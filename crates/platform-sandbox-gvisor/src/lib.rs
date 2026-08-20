//! Direct `runsc` runtime boundary for the first-release gVisor Sandbox executor.
//!
//! The dedicated executor is the only process allowed to use this crate. Commands are closed
//! argument vectors executed without a shell, with an empty environment and one immutable runtime
//! binary. There is deliberately no OCI/runc fallback.

use async_trait::async_trait;
use insight_platform_contracts::{ResourceId, ResourceKind, Sha256Digest};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use std::{
    collections::BTreeMap,
    ffi::OsString,
    path::{Path, PathBuf},
    process::Stdio,
    sync::Mutex,
    time::Duration,
};
use tokio::{
    io::AsyncReadExt,
    process::{ChildStderr, ChildStdout, Command},
    time::timeout,
};

pub const RUNSC_RUNTIME_NAME: &str = "runsc";
pub const MAX_RUNSC_OUTPUT_BYTES: usize = 64 * 1024;
pub const MAX_CONTAINER_ID_BYTES: usize = 128;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GvisorRuntimeConfig {
    pub runsc_path: PathBuf,
    pub runsc_version: String,
    pub runsc_binary_digest: Sha256Digest,
    pub runtime_root: PathBuf,
    pub command_timeout_milliseconds: u64,
}

impl GvisorRuntimeConfig {
    pub fn validate(&self) -> Result<(), GvisorRuntimeError> {
        if !self.runsc_path.is_absolute()
            || self.runsc_path.file_name().and_then(|value| value.to_str())
                != Some(RUNSC_RUNTIME_NAME)
            || !self.runtime_root.is_absolute()
            || self.runtime_root == Path::new("/")
            || self.runsc_version.is_empty()
            || self.runsc_version.len() > 128
            || !(1..=300_000).contains(&self.command_timeout_milliseconds)
        {
            return Err(GvisorRuntimeError::InvalidConfig);
        }
        Ok(())
    }

    fn timeout(&self) -> Duration {
        Duration::from_millis(self.command_timeout_milliseconds)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GvisorContainerIdentity {
    pub tenant_id: ResourceId,
    pub job_id: ResourceId,
    pub lease_generation: u64,
    pub worker_process_generation_id: ResourceId,
}

impl GvisorContainerIdentity {
    pub fn container_id(&self) -> Result<String, GvisorRuntimeError> {
        if self.tenant_id.kind() != ResourceKind::Tenant
            || self.job_id.kind() != ResourceKind::Job
            || self.worker_process_generation_id.kind() != ResourceKind::WorkerProcessGeneration
            || self.lease_generation == 0
        {
            return Err(GvisorRuntimeError::InvalidIdentity);
        }
        let digest = Sha256::digest(format!(
            "{}:{}:{}:{}",
            self.tenant_id, self.job_id, self.lease_generation, self.worker_process_generation_id
        ));
        let id = format!("ip-{}", hex_lower(&digest));
        if id.len() > MAX_CONTAINER_ID_BYTES {
            return Err(GvisorRuntimeError::InvalidIdentity);
        }
        Ok(id)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunscOperation {
    Version,
    Create,
    Start,
    Wait,
    Kill,
    Delete,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunscCommandPlan {
    pub operation: RunscOperation,
    pub executable: PathBuf,
    pub arguments: Vec<OsString>,
}

impl RunscCommandPlan {
    pub fn version(config: &GvisorRuntimeConfig) -> Result<Self, GvisorRuntimeError> {
        config.validate()?;
        Ok(Self::new(config, RunscOperation::Version, ["--version"]))
    }

    pub fn create(
        config: &GvisorRuntimeConfig,
        identity: &GvisorContainerIdentity,
        bundle: &Path,
    ) -> Result<Self, GvisorRuntimeError> {
        config.validate()?;
        let bundle = closed_bundle_path(bundle)?;
        let id = identity.container_id()?;
        Ok(Self::new(
            config,
            RunscOperation::Create,
            [
                format!("--root={}", config.runtime_root.display()),
                "--network=none".to_owned(),
                "--platform=systrap".to_owned(),
                "--directfs=false".to_owned(),
                "create".to_owned(),
                format!("--bundle={}", bundle.display()),
                id,
            ],
        ))
    }

    pub fn start(
        config: &GvisorRuntimeConfig,
        identity: &GvisorContainerIdentity,
    ) -> Result<Self, GvisorRuntimeError> {
        lifecycle(config, identity, RunscOperation::Start, "start", None)
    }

    pub fn wait(
        config: &GvisorRuntimeConfig,
        identity: &GvisorContainerIdentity,
    ) -> Result<Self, GvisorRuntimeError> {
        lifecycle(config, identity, RunscOperation::Wait, "wait", None)
    }

    pub fn kill(
        config: &GvisorRuntimeConfig,
        identity: &GvisorContainerIdentity,
        force: bool,
    ) -> Result<Self, GvisorRuntimeError> {
        lifecycle(
            config,
            identity,
            RunscOperation::Kill,
            "kill",
            Some(if force { "KILL" } else { "TERM" }),
        )
    }

    pub fn delete(
        config: &GvisorRuntimeConfig,
        identity: &GvisorContainerIdentity,
    ) -> Result<Self, GvisorRuntimeError> {
        config.validate()?;
        let id = identity.container_id()?;
        Ok(Self::new(
            config,
            RunscOperation::Delete,
            [
                format!("--root={}", config.runtime_root.display()),
                "delete".to_owned(),
                "--force".to_owned(),
                id,
            ],
        ))
    }

    fn new<I, S>(config: &GvisorRuntimeConfig, operation: RunscOperation, arguments: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<OsString>,
    {
        Self {
            operation,
            executable: config.runsc_path.clone(),
            arguments: arguments.into_iter().map(Into::into).collect(),
        }
    }
}

fn lifecycle(
    config: &GvisorRuntimeConfig,
    identity: &GvisorContainerIdentity,
    operation: RunscOperation,
    verb: &str,
    signal: Option<&str>,
) -> Result<RunscCommandPlan, GvisorRuntimeError> {
    config.validate()?;
    let mut arguments: Vec<OsString> = vec![
        format!("--root={}", config.runtime_root.display()).into(),
        verb.into(),
        identity.container_id()?.into(),
    ];
    if let Some(signal) = signal {
        arguments.push(signal.into());
    }
    Ok(RunscCommandPlan::new(config, operation, arguments))
}

fn closed_bundle_path(bundle: &Path) -> Result<PathBuf, GvisorRuntimeError> {
    if !bundle.is_absolute()
        || bundle == Path::new("/")
        || bundle
            .components()
            .any(|part| matches!(part, std::path::Component::ParentDir))
    {
        return Err(GvisorRuntimeError::InvalidBundle);
    }
    Ok(bundle.to_path_buf())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunscCommandOutput {
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
}

pub struct SystemRunscDriver {
    config: GvisorRuntimeConfig,
}

/// Production `runsc` lifecycle that preserves the root container's donated stdout/stderr
/// descriptors between the `create` and `wait` commands.
///
/// `runsc create` donates its current stdio descriptors to the sandbox process. Reading those
/// descriptors to EOF during `create` deadlocks because the sandbox intentionally keeps them open
/// until the guest exits. This runtime waits only for the short-lived create command, stores the
/// bounded read ends under the exact container identity, and drains them only alongside `wait`.
pub struct SystemGvisorRuntime {
    config: GvisorRuntimeConfig,
    driver: SystemRunscDriver,
    guest_io: Mutex<BTreeMap<String, CapturedGuestIo>>,
}

struct CapturedGuestIo {
    stdout: ChildStdout,
    stderr: ChildStderr,
}

#[async_trait]
pub trait RunscDriver: Send + Sync {
    async fn verify_runtime(&self) -> Result<(), GvisorRuntimeError>;

    async fn execute(
        &self,
        plan: RunscCommandPlan,
    ) -> Result<RunscCommandOutput, GvisorRuntimeError>;
}

impl SystemRunscDriver {
    pub fn new(config: GvisorRuntimeConfig) -> Result<Self, GvisorRuntimeError> {
        config.validate()?;
        Ok(Self { config })
    }

    async fn verify_runtime_inner(&self) -> Result<(), GvisorRuntimeError> {
        let output = self
            .execute(RunscCommandPlan::version(&self.config)?)
            .await?;
        let version =
            std::str::from_utf8(&output.stdout).map_err(|_| GvisorRuntimeError::RuntimeMismatch)?;
        if !version.contains(&self.config.runsc_version) {
            return Err(GvisorRuntimeError::RuntimeMismatch);
        }
        let bytes = tokio::fs::read(&self.config.runsc_path)
            .await
            .map_err(|_| GvisorRuntimeError::Unavailable)?;
        let actual = format!("sha256:{}", hex_lower(&Sha256::digest(bytes)));
        if actual != self.config.runsc_binary_digest.to_string() {
            return Err(GvisorRuntimeError::RuntimeMismatch);
        }
        Ok(())
    }

    async fn execute_inner(
        &self,
        plan: RunscCommandPlan,
    ) -> Result<RunscCommandOutput, GvisorRuntimeError> {
        if plan.executable != self.config.runsc_path {
            return Err(GvisorRuntimeError::InvalidCommand);
        }
        let mut child = Command::new(&plan.executable)
            .args(&plan.arguments)
            .env_clear()
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .map_err(|_| GvisorRuntimeError::Unavailable)?;
        let mut stdout = child.stdout.take().ok_or(GvisorRuntimeError::Unavailable)?;
        let mut stderr = child.stderr.take().ok_or(GvisorRuntimeError::Unavailable)?;
        let capture = async {
            let (stdout, stderr, status) = tokio::try_join!(
                read_bounded(&mut stdout),
                read_bounded(&mut stderr),
                child.wait()
            )
            .map_err(|_| GvisorRuntimeError::Unavailable)?;
            if !status.success() {
                return Err(GvisorRuntimeError::CommandFailed);
            }
            Ok(RunscCommandOutput { stdout, stderr })
        };
        timeout(self.config.timeout(), capture)
            .await
            .map_err(|_| GvisorRuntimeError::TimedOut)?
    }
}

impl SystemGvisorRuntime {
    pub fn new(config: GvisorRuntimeConfig) -> Result<Self, GvisorRuntimeError> {
        let driver = SystemRunscDriver::new(config.clone())?;
        Ok(Self {
            config,
            driver,
            guest_io: Mutex::new(BTreeMap::new()),
        })
    }

    async fn create_with_captured_guest_io(
        &self,
        identity: &GvisorContainerIdentity,
        bundle: &Path,
    ) -> Result<RunscCommandOutput, GvisorRuntimeError> {
        let container_id = identity.container_id()?;
        if self
            .guest_io
            .lock()
            .map_err(|_| GvisorRuntimeError::Unavailable)?
            .contains_key(&container_id)
        {
            return Err(GvisorRuntimeError::InvalidIdentity);
        }
        let plan = RunscCommandPlan::create(&self.config, identity, bundle)?;
        if plan.executable != self.config.runsc_path {
            return Err(GvisorRuntimeError::InvalidCommand);
        }
        let mut child = Command::new(&plan.executable)
            .args(&plan.arguments)
            .env_clear()
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .map_err(|_| GvisorRuntimeError::Unavailable)?;
        let stdout = child.stdout.take().ok_or(GvisorRuntimeError::Unavailable)?;
        let stderr = child.stderr.take().ok_or(GvisorRuntimeError::Unavailable)?;
        let status = timeout(self.config.timeout(), child.wait())
            .await
            .map_err(|_| GvisorRuntimeError::TimedOut)?
            .map_err(|_| GvisorRuntimeError::Unavailable)?;
        if !status.success() {
            return Err(GvisorRuntimeError::CommandFailed);
        }
        self.guest_io
            .lock()
            .map_err(|_| GvisorRuntimeError::Unavailable)?
            .insert(container_id, CapturedGuestIo { stdout, stderr });
        Ok(RunscCommandOutput {
            stdout: Vec::new(),
            stderr: Vec::new(),
        })
    }

    fn take_guest_io(
        &self,
        identity: &GvisorContainerIdentity,
    ) -> Result<CapturedGuestIo, GvisorRuntimeError> {
        self.guest_io
            .lock()
            .map_err(|_| GvisorRuntimeError::Unavailable)?
            .remove(&identity.container_id()?)
            .ok_or(GvisorRuntimeError::InvalidIdentity)
    }

    fn discard_guest_io(&self, identity: &GvisorContainerIdentity) {
        if let Ok(container_id) = identity.container_id() {
            if let Ok(mut guest_io) = self.guest_io.lock() {
                guest_io.remove(&container_id);
            }
        }
    }
}

#[async_trait]
impl GvisorRuntimePort for SystemGvisorRuntime {
    async fn verify(&self) -> Result<(), GvisorRuntimeError> {
        self.driver.verify_runtime().await
    }

    async fn create(
        &self,
        identity: &GvisorContainerIdentity,
        bundle: &Path,
    ) -> Result<RunscCommandOutput, GvisorRuntimeError> {
        let result = self.create_with_captured_guest_io(identity, bundle).await;
        if result.is_err() {
            if let Ok(plan) = RunscCommandPlan::delete(&self.config, identity) {
                let _ = self.driver.execute(plan).await;
            }
            self.discard_guest_io(identity);
        }
        result
    }

    async fn start(
        &self,
        identity: &GvisorContainerIdentity,
    ) -> Result<RunscCommandOutput, GvisorRuntimeError> {
        let result = self
            .driver
            .execute(RunscCommandPlan::start(&self.config, identity)?)
            .await;
        if result.is_err() {
            let _ = self.terminate(identity, true).await;
            let _ = self.destroy(identity).await;
        }
        result
    }

    async fn wait(
        &self,
        identity: &GvisorContainerIdentity,
    ) -> Result<RunscCommandOutput, GvisorRuntimeError> {
        let CapturedGuestIo {
            mut stdout,
            mut stderr,
        } = self.take_guest_io(identity)?;
        let wait = self
            .driver
            .execute(RunscCommandPlan::wait(&self.config, identity)?);
        let collect = async {
            let (stdout, stderr, waited) =
                tokio::join!(read_bounded(&mut stdout), read_bounded(&mut stderr), wait);
            let stdout = stdout.map_err(|_| GvisorRuntimeError::Unavailable)?;
            let stderr = stderr.map_err(|_| GvisorRuntimeError::Unavailable)?;
            waited?;
            Ok(RunscCommandOutput { stdout, stderr })
        };
        timeout(self.config.timeout(), collect)
            .await
            .map_err(|_| GvisorRuntimeError::TimedOut)?
    }

    async fn terminate(
        &self,
        identity: &GvisorContainerIdentity,
        force: bool,
    ) -> Result<RunscCommandOutput, GvisorRuntimeError> {
        self.driver
            .execute(RunscCommandPlan::kill(&self.config, identity, force)?)
            .await
    }

    async fn destroy(
        &self,
        identity: &GvisorContainerIdentity,
    ) -> Result<RunscCommandOutput, GvisorRuntimeError> {
        let result = self
            .driver
            .execute(RunscCommandPlan::delete(&self.config, identity)?)
            .await;
        if result.is_ok() {
            self.discard_guest_io(identity);
        }
        result
    }

    async fn recover_orphan(
        &self,
        identity: &GvisorContainerIdentity,
    ) -> Result<(), GvisorRuntimeError> {
        let _ = self.terminate(identity, true).await;
        let result = self.destroy(identity).await.map(|_| ());
        if result.is_ok() {
            self.discard_guest_io(identity);
        }
        result
    }
}

#[async_trait]
impl RunscDriver for SystemRunscDriver {
    async fn verify_runtime(&self) -> Result<(), GvisorRuntimeError> {
        self.verify_runtime_inner().await
    }

    async fn execute(
        &self,
        plan: RunscCommandPlan,
    ) -> Result<RunscCommandOutput, GvisorRuntimeError> {
        self.execute_inner(plan).await
    }
}

/// Closed single-Job lifecycle over one pinned `runsc` runtime.
///
/// The lifecycle never discovers an alternate runtime. A failed create/start is followed by a
/// bounded force-kill/delete attempt so that a partial container cannot become an untracked warm
/// sandbox. Cleanup failure never converts the original operation into success.
pub struct GvisorSingleJobRuntime<D> {
    config: GvisorRuntimeConfig,
    driver: D,
}

#[async_trait]
pub trait GvisorRuntimePort: Send + Sync {
    async fn verify(&self) -> Result<(), GvisorRuntimeError>;
    async fn create(
        &self,
        identity: &GvisorContainerIdentity,
        bundle: &Path,
    ) -> Result<RunscCommandOutput, GvisorRuntimeError>;
    async fn start(
        &self,
        identity: &GvisorContainerIdentity,
    ) -> Result<RunscCommandOutput, GvisorRuntimeError>;
    async fn wait(
        &self,
        identity: &GvisorContainerIdentity,
    ) -> Result<RunscCommandOutput, GvisorRuntimeError>;
    async fn terminate(
        &self,
        identity: &GvisorContainerIdentity,
        force: bool,
    ) -> Result<RunscCommandOutput, GvisorRuntimeError>;
    async fn destroy(
        &self,
        identity: &GvisorContainerIdentity,
    ) -> Result<RunscCommandOutput, GvisorRuntimeError>;
    async fn recover_orphan(
        &self,
        identity: &GvisorContainerIdentity,
    ) -> Result<(), GvisorRuntimeError>;
}

impl<D> GvisorSingleJobRuntime<D>
where
    D: RunscDriver,
{
    pub fn new(config: GvisorRuntimeConfig, driver: D) -> Result<Self, GvisorRuntimeError> {
        config.validate()?;
        Ok(Self { config, driver })
    }

    pub async fn verify(&self) -> Result<(), GvisorRuntimeError> {
        self.driver.verify_runtime().await
    }

    pub async fn create(
        &self,
        identity: &GvisorContainerIdentity,
        bundle: &Path,
    ) -> Result<RunscCommandOutput, GvisorRuntimeError> {
        let result = self
            .driver
            .execute(RunscCommandPlan::create(&self.config, identity, bundle)?)
            .await;
        if result.is_err() {
            self.best_effort_destroy(identity).await;
        }
        result
    }

    pub async fn start(
        &self,
        identity: &GvisorContainerIdentity,
    ) -> Result<RunscCommandOutput, GvisorRuntimeError> {
        let result = self
            .driver
            .execute(RunscCommandPlan::start(&self.config, identity)?)
            .await;
        if result.is_err() {
            self.best_effort_terminate(identity).await;
            self.best_effort_destroy(identity).await;
        }
        result
    }

    pub async fn wait(
        &self,
        identity: &GvisorContainerIdentity,
    ) -> Result<RunscCommandOutput, GvisorRuntimeError> {
        self.driver
            .execute(RunscCommandPlan::wait(&self.config, identity)?)
            .await
    }

    pub async fn terminate(
        &self,
        identity: &GvisorContainerIdentity,
        force: bool,
    ) -> Result<RunscCommandOutput, GvisorRuntimeError> {
        self.driver
            .execute(RunscCommandPlan::kill(&self.config, identity, force)?)
            .await
    }

    pub async fn destroy(
        &self,
        identity: &GvisorContainerIdentity,
    ) -> Result<RunscCommandOutput, GvisorRuntimeError> {
        self.driver
            .execute(RunscCommandPlan::delete(&self.config, identity)?)
            .await
    }

    pub async fn recover_orphan(
        &self,
        identity: &GvisorContainerIdentity,
    ) -> Result<(), GvisorRuntimeError> {
        let termination = self.terminate(identity, true).await;
        let destruction = self.destroy(identity).await;
        match (termination, destruction) {
            (_, Ok(_)) => Ok(()),
            (Err(error), Err(_)) | (Ok(_), Err(error)) => Err(error),
        }
    }

    async fn best_effort_terminate(&self, identity: &GvisorContainerIdentity) {
        let _ = self.terminate(identity, true).await;
    }

    async fn best_effort_destroy(&self, identity: &GvisorContainerIdentity) {
        let _ = self.destroy(identity).await;
    }
}

#[async_trait]
impl<D> GvisorRuntimePort for GvisorSingleJobRuntime<D>
where
    D: RunscDriver,
{
    async fn verify(&self) -> Result<(), GvisorRuntimeError> {
        GvisorSingleJobRuntime::verify(self).await
    }

    async fn create(
        &self,
        identity: &GvisorContainerIdentity,
        bundle: &Path,
    ) -> Result<RunscCommandOutput, GvisorRuntimeError> {
        GvisorSingleJobRuntime::create(self, identity, bundle).await
    }

    async fn start(
        &self,
        identity: &GvisorContainerIdentity,
    ) -> Result<RunscCommandOutput, GvisorRuntimeError> {
        GvisorSingleJobRuntime::start(self, identity).await
    }

    async fn wait(
        &self,
        identity: &GvisorContainerIdentity,
    ) -> Result<RunscCommandOutput, GvisorRuntimeError> {
        GvisorSingleJobRuntime::wait(self, identity).await
    }

    async fn terminate(
        &self,
        identity: &GvisorContainerIdentity,
        force: bool,
    ) -> Result<RunscCommandOutput, GvisorRuntimeError> {
        GvisorSingleJobRuntime::terminate(self, identity, force).await
    }

    async fn destroy(
        &self,
        identity: &GvisorContainerIdentity,
    ) -> Result<RunscCommandOutput, GvisorRuntimeError> {
        GvisorSingleJobRuntime::destroy(self, identity).await
    }

    async fn recover_orphan(
        &self,
        identity: &GvisorContainerIdentity,
    ) -> Result<(), GvisorRuntimeError> {
        GvisorSingleJobRuntime::recover_orphan(self, identity).await
    }
}

async fn read_bounded<R: tokio::io::AsyncRead + Unpin>(reader: &mut R) -> std::io::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    reader
        .take((MAX_RUNSC_OUTPUT_BYTES + 1) as u64)
        .read_to_end(&mut bytes)
        .await?;
    if bytes.len() > MAX_RUNSC_OUTPUT_BYTES {
        return Err(std::io::Error::other("runsc output exceeded bound"));
    }
    Ok(bytes)
}

fn hex_lower(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GvisorRuntimeError {
    InvalidConfig,
    InvalidIdentity,
    InvalidBundle,
    InvalidCommand,
    Unavailable,
    RuntimeMismatch,
    CommandFailed,
    TimedOut,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};
    use uuid::Uuid;

    struct RecordingDriver {
        operations: Arc<Mutex<Vec<RunscOperation>>>,
        fail_on: Option<RunscOperation>,
    }

    #[async_trait]
    impl RunscDriver for RecordingDriver {
        async fn verify_runtime(&self) -> Result<(), GvisorRuntimeError> {
            Ok(())
        }

        async fn execute(
            &self,
            plan: RunscCommandPlan,
        ) -> Result<RunscCommandOutput, GvisorRuntimeError> {
            self.operations.lock().unwrap().push(plan.operation);
            if self.fail_on == Some(plan.operation) {
                return Err(GvisorRuntimeError::CommandFailed);
            }
            Ok(RunscCommandOutput {
                stdout: Vec::new(),
                stderr: Vec::new(),
            })
        }
    }

    fn id(kind: ResourceKind, _byte: u8) -> ResourceId {
        ResourceId::from_uuid_v7(kind, Uuid::now_v7()).unwrap()
    }
    fn config() -> GvisorRuntimeConfig {
        GvisorRuntimeConfig {
            runsc_path: PathBuf::from("/opt/insight/bin/runsc"),
            runsc_version: "runsc version release-20260820.0".to_owned(),
            runsc_binary_digest: format!("sha256:{}", "a".repeat(64)).parse().unwrap(),
            runtime_root: PathBuf::from("/run/insight-platform/runsc"),
            command_timeout_milliseconds: 30_000,
        }
    }
    fn identity() -> GvisorContainerIdentity {
        GvisorContainerIdentity {
            tenant_id: id(ResourceKind::Tenant, 1),
            job_id: id(ResourceKind::Job, 2),
            lease_generation: 7,
            worker_process_generation_id: id(ResourceKind::WorkerProcessGeneration, 3),
        }
    }

    #[test]
    fn create_is_closed_to_runsc_systrap_and_network_none() {
        let plan = RunscCommandPlan::create(
            &config(),
            &identity(),
            Path::new("/var/lib/insight/bundles/job-1"),
        )
        .unwrap();
        let args = plan
            .arguments
            .iter()
            .map(|value| value.to_str().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(plan.executable, Path::new("/opt/insight/bin/runsc"));
        assert!(args.contains(&"--network=none"));
        assert!(args.contains(&"--platform=systrap"));
        // The official `create` subcommand rejects `--rootless`; the dedicated executor instead
        // relies on the publication-validated OCI user namespace and its node security boundary.
        assert!(!args.contains(&"--rootless=true"));
        assert!(args.contains(&"--directfs=false"));
        assert!(!args
            .iter()
            .any(|arg| arg.contains("runc") || arg.contains("--network=host")));
    }

    #[test]
    fn lifecycle_has_exact_identity_and_force_delete() {
        let identity = identity();
        let expected = identity.container_id().unwrap();
        let delete = RunscCommandPlan::delete(&config(), &identity).unwrap();
        assert_eq!(delete.arguments.last().unwrap(), &OsString::from(expected));
        assert!(delete.arguments.contains(&OsString::from("--force")));
        assert_eq!(
            RunscCommandPlan::kill(&config(), &identity, true)
                .unwrap()
                .arguments
                .last()
                .unwrap(),
            "KILL"
        );
    }

    #[test]
    fn invalid_paths_and_identity_fail_closed() {
        let mut invalid = config();
        invalid.runsc_path = PathBuf::from("runsc");
        assert_eq!(invalid.validate(), Err(GvisorRuntimeError::InvalidConfig));
        assert_eq!(
            RunscCommandPlan::create(&config(), &identity(), Path::new("../bundle")),
            Err(GvisorRuntimeError::InvalidBundle)
        );
        let mut invalid_identity = identity();
        invalid_identity.lease_generation = 0;
        assert_eq!(
            invalid_identity.container_id(),
            Err(GvisorRuntimeError::InvalidIdentity)
        );
    }

    #[tokio::test]
    async fn partial_create_is_force_deleted_without_runtime_fallback() {
        let operations = Arc::new(Mutex::new(Vec::new()));
        let runtime = GvisorSingleJobRuntime::new(
            config(),
            RecordingDriver {
                operations: Arc::clone(&operations),
                fail_on: Some(RunscOperation::Create),
            },
        )
        .unwrap();
        assert_eq!(
            runtime
                .create(&identity(), Path::new("/var/lib/insight/bundles/job-1"))
                .await,
            Err(GvisorRuntimeError::CommandFailed)
        );
        assert_eq!(
            *operations.lock().unwrap(),
            vec![RunscOperation::Create, RunscOperation::Delete]
        );
    }

    #[tokio::test]
    async fn failed_start_kills_and_deletes_the_exact_container() {
        let operations = Arc::new(Mutex::new(Vec::new()));
        let runtime = GvisorSingleJobRuntime::new(
            config(),
            RecordingDriver {
                operations: Arc::clone(&operations),
                fail_on: Some(RunscOperation::Start),
            },
        )
        .unwrap();
        assert_eq!(
            runtime.start(&identity()).await,
            Err(GvisorRuntimeError::CommandFailed)
        );
        assert_eq!(
            *operations.lock().unwrap(),
            vec![
                RunscOperation::Start,
                RunscOperation::Kill,
                RunscOperation::Delete
            ]
        );
    }

    #[tokio::test]
    async fn orphan_recovery_force_kills_before_delete() {
        let operations = Arc::new(Mutex::new(Vec::new()));
        let runtime = GvisorSingleJobRuntime::new(
            config(),
            RecordingDriver {
                operations: Arc::clone(&operations),
                fail_on: None,
            },
        )
        .unwrap();
        runtime.recover_orphan(&identity()).await.unwrap();
        assert_eq!(
            *operations.lock().unwrap(),
            vec![RunscOperation::Kill, RunscOperation::Delete]
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn system_runtime_preserves_donated_guest_streams_until_wait() {
        use std::os::unix::fs::PermissionsExt as _;

        let directory = std::env::temp_dir().join(format!("insight-gvisor-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&directory).unwrap();
        let runsc = directory.join("runsc");
        let script = br##"#!/bin/sh
for argument in "$@"; do
  case "$argument" in
    --version)
      printf 'runsc version test-release\n'
      exit 0
      ;;
    create)
      (sleep 0.15; printf 'guest-stdout'; printf 'guest-stderr' >&2) &
      exit 0
      ;;
    start|kill|delete)
      exit 0
      ;;
    wait)
      sleep 0.20
      printf '{"id":"ignored-runsc-status"}'
      exit 0
      ;;
  esac
done
exit 2
"##;
        std::fs::write(&runsc, script).unwrap();
        let mut permissions = std::fs::metadata(&runsc).unwrap().permissions();
        permissions.set_mode(0o700);
        std::fs::set_permissions(&runsc, permissions).unwrap();
        let runtime_config = GvisorRuntimeConfig {
            runsc_path: runsc,
            runsc_version: "runsc version test-release".to_owned(),
            runsc_binary_digest: format!("sha256:{}", hex_lower(&Sha256::digest(script)))
                .parse()
                .unwrap(),
            runtime_root: directory.join("runtime"),
            command_timeout_milliseconds: 2_000,
        };
        let runtime = SystemGvisorRuntime::new(runtime_config).unwrap();
        runtime.verify().await.unwrap();
        let identity = identity();
        tokio::time::timeout(
            Duration::from_millis(100),
            runtime.create(&identity, &directory),
        )
        .await
        .expect("create must not wait for donated guest streams")
        .unwrap();
        runtime.start(&identity).await.unwrap();
        let output = runtime.wait(&identity).await.unwrap();
        assert_eq!(output.stdout, b"guest-stdout");
        assert_eq!(output.stderr, b"guest-stderr");
        runtime.destroy(&identity).await.unwrap();
        std::fs::remove_dir_all(directory).unwrap();
    }
}
