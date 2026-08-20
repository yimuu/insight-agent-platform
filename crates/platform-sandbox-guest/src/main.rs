use chrono::Utc;
use insight_platform_artifact_rpc::{
    ArtifactGvisorGuestGrpcClient, ArtifactInternalRpcLimits, GvisorGuestArtifactSection,
};
use insight_platform_contracts::{
    canonical_json, parse_strict_json, JsonLimits, SandboxEntrypointKind, SandboxRuntimeFamily,
    ValueRef,
};
use insight_platform_sandbox::{
    GvisorGuestBootstrapRequest, GvisorGuestExecutionPlan, GvisorGuestPodIdentity,
    SandboxResourceUsage,
};
use serde::Serialize;
use std::{
    collections::BTreeSet,
    error::Error,
    fmt,
    fs::{self, OpenOptions},
    io::{Read as _, Write as _},
    path::{Component, Path, PathBuf},
    process::Stdio,
    str::FromStr,
    time::{Duration, Instant},
};
use tokio::{io::AsyncReadExt as _, process::Command};
use tonic::transport::{Certificate, ClientTlsConfig, Endpoint};

const ENDPOINT_ENV: &str = "INSIGHT_SANDBOX_BOOTSTRAP_ENDPOINT";
const TOKEN_PATH_ENV: &str = "INSIGHT_SANDBOX_BOOTSTRAP_TOKEN_PATH";
const CA_PATH_ENV: &str = "INSIGHT_SANDBOX_BOOTSTRAP_CA_PATH";
const TENANT_ID_ENV: &str = "INSIGHT_SANDBOX_TENANT_ID";
const JOB_ID_ENV: &str = "INSIGHT_SANDBOX_JOB_ID";
const REQUEST_DIGEST_ENV: &str = "INSIGHT_SANDBOX_REQUEST_DIGEST";
const POD_NAMESPACE_ENV: &str = "INSIGHT_SANDBOX_POD_NAMESPACE";
const POD_NAME_ENV: &str = "INSIGHT_SANDBOX_POD_NAME";
const POD_UID_ENV: &str = "INSIGHT_SANDBOX_POD_UID";
const SERVICE_ACCOUNT_ENV: &str = "INSIGHT_SANDBOX_SERVICE_ACCOUNT_NAME";
const PACKAGE_ROOT: &str = "/scratch/package";
const TERMINATION_LOG: &str = "/dev/termination-log";
const MAX_TOKEN_BYTES: usize = 16_384;
const MAX_CA_BYTES: usize = 1_048_576;
const MAX_TERMINATION_BYTES: usize = 4_096;
const TAR_BLOCK_BYTES: usize = 512;
const MAX_ARCHIVE_ENTRIES: usize = 100_000;

#[tokio::main]
async fn main() {
    if std::env::args().skip(1).collect::<Vec<_>>() != ["run"] {
        eprintln!("platform-sandbox-guest rejected command");
        std::process::exit(2);
    }
    if let Err(error) = run().await {
        eprintln!("platform-sandbox-guest failed: {error}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), GuestError> {
    let bootstrap = GvisorGuestBootstrapRequest {
        schema_version: 1,
        tenant_id: parsed_env(TENANT_ID_ENV)?,
        sandbox_job_id: parsed_env(JOB_ID_ENV)?,
        request_digest: parsed_env(REQUEST_DIGEST_ENV)?,
        pod: GvisorGuestPodIdentity {
            namespace: required(POD_NAMESPACE_ENV)?,
            pod_name: required(POD_NAME_ENV)?,
            pod_uid: required(POD_UID_ENV)?,
            service_account_name: required(SERVICE_ACCOUNT_ENV)?,
        },
    };
    bootstrap.validate().map_err(|_| GuestError::Denied)?;
    let token = read_bounded(&absolute_path(TOKEN_PATH_ENV)?, MAX_TOKEN_BYTES)?;
    let token = std::str::from_utf8(&token).map_err(|_| GuestError::Denied)?;
    if token.trim() != token {
        return Err(GuestError::Denied);
    }
    let endpoint_uri = required(ENDPOINT_ENV)?;
    let ca = read_bounded(&absolute_path(CA_PATH_ENV)?, MAX_CA_BYTES)?;
    let endpoint = Endpoint::from_shared(endpoint_uri)
        .map_err(|_| GuestError::InvalidConfiguration)?
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(30))
        .tls_config(ClientTlsConfig::new().ca_certificate(Certificate::from_pem(ca)))
        .map_err(|_| GuestError::InvalidConfiguration)?;
    let channel = endpoint
        .connect()
        .await
        .map_err(|_| GuestError::Unavailable)?;
    let client = ArtifactGvisorGuestGrpcClient::new(channel, ArtifactInternalRpcLimits::default());
    let plan = client
        .authorize(bootstrap.clone(), token)
        .await
        .map_err(|_| GuestError::Denied)?;
    let package = client
        .read_artifact(
            bootstrap.clone(),
            &plan,
            GvisorGuestArtifactSection::RuntimeBundle,
            token,
        )
        .await
        .map_err(|_| GuestError::Unavailable)?;
    let input = match &plan.input_ref {
        ValueRef::Inline { value } => canonical_json(value).map_err(|_| GuestError::Integrity)?,
        ValueRef::Artifact { .. } => client
            .read_artifact(
                bootstrap,
                &plan,
                GvisorGuestArtifactSection::InputValue,
                token,
            )
            .await
            .map_err(|_| GuestError::Unavailable)?,
    };
    validate_input(&input, &plan)?;
    let package_root = PathBuf::from(PACKAGE_ROOT);
    let extracted = extract_bundle(&package, &package_root, &plan)?;
    let started = Instant::now();
    let (stdout, stderr) = execute(&plan, &package_root, &input).await?;
    let value = validate_output(&stdout, &plan)?;
    let elapsed = u64::try_from(started.elapsed().as_millis())
        .unwrap_or(u64::MAX)
        .max(1)
        .min(plan.resources.wall_milliseconds);
    let result_bytes = u64::try_from(stdout.len()).map_err(|_| GuestError::LimitExceeded)?;
    let io_bytes = extracted
        .bytes
        .checked_add(u64::try_from(input.len()).map_err(|_| GuestError::LimitExceeded)?)
        .and_then(|bytes| bytes.checked_add(result_bytes))
        .ok_or(GuestError::LimitExceeded)?;
    if io_bytes > plan.resources.io_bytes {
        return Err(GuestError::LimitExceeded);
    }
    let envelope = GuestResultEnvelope {
        schema_version: 1,
        value,
        usage: SandboxResourceUsage {
            cpu_milliseconds: elapsed,
            peak_memory_mebibytes: 0,
            peak_pids: 1,
            files_created: extracted.files,
            io_bytes,
            stdout_bytes: result_bytes,
            stderr_bytes: u64::try_from(stderr.len()).map_err(|_| GuestError::LimitExceeded)?,
            result_bytes,
            artifact_output_bytes: 0,
            network_connections: 0,
            network_request_bytes: 0,
            network_response_bytes: 0,
            wall_milliseconds: elapsed,
            wasm_fuel_consumed: None,
        },
    };
    let bytes = canonical_json(&serde_json::to_value(envelope).map_err(|_| GuestError::Integrity)?)
        .map_err(|_| GuestError::Integrity)?;
    if bytes.len() > MAX_TERMINATION_BYTES {
        return Err(GuestError::LimitExceeded);
    }
    write_new(Path::new(TERMINATION_LOG), &bytes)?;
    Ok(())
}

#[derive(Serialize)]
#[serde(deny_unknown_fields)]
struct GuestResultEnvelope {
    schema_version: u32,
    value: serde_json::Value,
    usage: SandboxResourceUsage,
}

struct ExtractionUsage {
    files: u32,
    bytes: u64,
}

async fn execute(
    plan: &GvisorGuestExecutionPlan,
    package_root: &Path,
    input: &[u8],
) -> Result<(Vec<u8>, Vec<u8>), GuestError> {
    let mut command = match (plan.runtime_family, plan.entrypoint_kind) {
        (SandboxRuntimeFamily::Python, SandboxEntrypointKind::PythonModule) => {
            let mut command = Command::new("/usr/bin/python3");
            command.args(["-I", "-m", plan.entrypoint.as_str()]);
            command
        }
        (SandboxRuntimeFamily::NodeJs, SandboxEntrypointKind::NodeModule) => {
            let mut command = Command::new("/usr/bin/node");
            command.arg(exact_entrypoint(package_root, &plan.entrypoint, false)?);
            command
        }
        (SandboxRuntimeFamily::ReviewedShell, SandboxEntrypointKind::ReviewedExecutable) => {
            Command::new(exact_entrypoint(package_root, &plan.entrypoint, true)?)
        }
        _ => return Err(GuestError::Denied),
    };
    command
        .current_dir(package_root)
        .env_clear()
        .env("HOME", "/scratch")
        .env("LANG", "C.UTF-8")
        .env("PATH", "/usr/bin:/bin")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    let mut child = command.spawn().map_err(|_| GuestError::Execution)?;
    let mut stdin = child.stdin.take().ok_or(GuestError::Execution)?;
    let stdout = child.stdout.take().ok_or(GuestError::Execution)?;
    let stderr = child.stderr.take().ok_or(GuestError::Execution)?;
    let stdout_limit =
        usize::try_from(plan.resources.stdout_bytes).map_err(|_| GuestError::LimitExceeded)?;
    let stderr_limit =
        usize::try_from(plan.resources.stderr_bytes).map_err(|_| GuestError::LimitExceeded)?;
    let input = input.to_vec();
    let write_input = tokio::spawn(async move {
        use tokio::io::AsyncWriteExt as _;
        stdin.write_all(&input).await?;
        stdin.shutdown().await
    });
    let read_stdout = tokio::spawn(read_bounded_async(stdout, stdout_limit));
    let read_stderr = tokio::spawn(read_bounded_async(stderr, stderr_limit));
    let remaining = (plan.deadline - Utc::now())
        .to_std()
        .map_err(|_| GuestError::TimedOut)?;
    let status = tokio::time::timeout(remaining, child.wait())
        .await
        .map_err(|_| GuestError::TimedOut)?
        .map_err(|_| GuestError::Execution)?;
    write_input
        .await
        .map_err(|_| GuestError::Execution)?
        .map_err(|_| GuestError::Execution)?;
    let stdout = read_stdout.await.map_err(|_| GuestError::Execution)??;
    let stderr = read_stderr.await.map_err(|_| GuestError::Execution)??;
    if !status.success() {
        return Err(GuestError::Execution);
    }
    Ok((stdout, stderr))
}

async fn read_bounded_async<R>(reader: R, maximum: usize) -> Result<Vec<u8>, GuestError>
where
    R: tokio::io::AsyncRead + Unpin,
{
    let mut bytes = Vec::new();
    reader
        .take(u64::try_from(maximum).unwrap_or(u64::MAX) + 1)
        .read_to_end(&mut bytes)
        .await
        .map_err(|_| GuestError::Execution)?;
    if bytes.len() > maximum {
        return Err(GuestError::LimitExceeded);
    }
    Ok(bytes)
}

fn validate_input(bytes: &[u8], plan: &GvisorGuestExecutionPlan) -> Result<(), GuestError> {
    let maximum =
        usize::try_from(plan.resources.io_bytes).map_err(|_| GuestError::LimitExceeded)?;
    let value = strict_json(bytes, maximum)?;
    if canonical_json(&value).as_deref() != Ok(bytes) {
        return Err(GuestError::Integrity);
    }
    Ok(())
}

fn validate_output(
    bytes: &[u8],
    plan: &GvisorGuestExecutionPlan,
) -> Result<serde_json::Value, GuestError> {
    let maximum =
        usize::try_from(plan.resources.result_bytes).map_err(|_| GuestError::LimitExceeded)?;
    let value = strict_json(bytes, maximum)?;
    if canonical_json(&value).as_deref() != Ok(bytes) {
        return Err(GuestError::Integrity);
    }
    Ok(value)
}

fn strict_json(bytes: &[u8], maximum: usize) -> Result<serde_json::Value, GuestError> {
    parse_strict_json(
        bytes,
        JsonLimits {
            max_bytes: maximum,
            max_depth: 64,
            max_items_per_array: 4_096,
            max_properties_per_object: 4_096,
            max_string_bytes: maximum,
        },
    )
    .map_err(|_| GuestError::Integrity)
}

fn exact_entrypoint(
    root: &Path,
    entrypoint: &str,
    executable: bool,
) -> Result<PathBuf, GuestError> {
    let relative = Path::new(entrypoint);
    if relative.is_absolute()
        || relative
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(GuestError::Denied);
    }
    let path = root.join(relative);
    let metadata = fs::symlink_metadata(&path).map_err(|_| GuestError::Denied)?;
    if !metadata.is_file() || metadata.file_type().is_symlink() {
        return Err(GuestError::Denied);
    }
    #[cfg(unix)]
    if executable {
        use std::os::unix::fs::PermissionsExt as _;
        if metadata.permissions().mode() & 0o111 == 0 {
            return Err(GuestError::Denied);
        }
    }
    Ok(path)
}

fn extract_bundle(
    archive: &[u8],
    destination: &Path,
    plan: &GvisorGuestExecutionPlan,
) -> Result<ExtractionUsage, GuestError> {
    if archive.is_empty() || archive.len() % TAR_BLOCK_BYTES != 0 || destination.exists() {
        return Err(GuestError::Integrity);
    }
    fs::create_dir(destination).map_err(|_| GuestError::Unavailable)?;
    let result = extract_tar_entries(archive, destination, plan);
    if result.is_err() {
        let _ = fs::remove_dir_all(destination);
    }
    result
}

fn extract_tar_entries(
    archive: &[u8],
    destination: &Path,
    plan: &GvisorGuestExecutionPlan,
) -> Result<ExtractionUsage, GuestError> {
    let mut offset = 0usize;
    let mut entries = 0usize;
    let mut files = 0u32;
    let mut bytes = 0u64;
    let mut paths = BTreeSet::new();
    while offset + TAR_BLOCK_BYTES <= archive.len() {
        let header = &archive[offset..offset + TAR_BLOCK_BYTES];
        offset += TAR_BLOCK_BYTES;
        if header.iter().all(|byte| *byte == 0) {
            if archive[offset..].iter().any(|byte| *byte != 0) {
                return Err(GuestError::Integrity);
            }
            return Ok(ExtractionUsage { files, bytes });
        }
        entries = entries.checked_add(1).ok_or(GuestError::LimitExceeded)?;
        if entries > MAX_ARCHIVE_ENTRIES
            || entries > usize::try_from(plan.resources.files).unwrap_or(usize::MAX)
            || &header[257..263] != b"ustar\0"
            || &header[263..265] != b"00"
            || !valid_tar_checksum(header)
        {
            return Err(GuestError::Integrity);
        }
        let name = tar_path(header)?;
        if !paths.insert(name.clone()) {
            return Err(GuestError::Integrity);
        }
        let mode = parse_tar_octal(&header[100..108])?;
        let size = usize::try_from(parse_tar_octal(&header[124..136])?)
            .map_err(|_| GuestError::LimitExceeded)?;
        let padded = size
            .checked_add(TAR_BLOCK_BYTES - 1)
            .ok_or(GuestError::LimitExceeded)?
            / TAR_BLOCK_BYTES
            * TAR_BLOCK_BYTES;
        if offset
            .checked_add(padded)
            .is_none_or(|end| end > archive.len())
        {
            return Err(GuestError::Integrity);
        }
        let target = destination.join(&name);
        let parent = target.parent().ok_or(GuestError::Integrity)?;
        if parent != destination {
            let relative = parent
                .strip_prefix(destination)
                .map_err(|_| GuestError::Integrity)?;
            if !paths.contains(relative) || !parent.is_dir() {
                return Err(GuestError::Integrity);
            }
        }
        match header[156] {
            0 | b'0' => {
                files = files.checked_add(1).ok_or(GuestError::LimitExceeded)?;
                bytes = bytes
                    .checked_add(u64::try_from(size).map_err(|_| GuestError::LimitExceeded)?)
                    .ok_or(GuestError::LimitExceeded)?;
                if files > plan.resources.files || bytes > plan.resources.io_bytes {
                    return Err(GuestError::LimitExceeded);
                }
                let mut file = OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(&target)
                    .map_err(|_| GuestError::Unavailable)?;
                file.write_all(&archive[offset..offset + size])
                    .map_err(|_| GuestError::Unavailable)?;
                set_permissions(&target, mode, false)?;
            }
            b'5' if size == 0 => {
                fs::create_dir(&target).map_err(|_| GuestError::Unavailable)?;
                set_permissions(&target, mode, true)?;
            }
            _ => return Err(GuestError::Integrity),
        }
        offset += padded;
    }
    Err(GuestError::Integrity)
}

#[cfg(unix)]
fn set_permissions(path: &Path, archived_mode: u64, directory: bool) -> Result<(), GuestError> {
    use std::os::unix::fs::PermissionsExt as _;
    let mode = if directory {
        0o500
    } else if archived_mode & 0o111 != 0 {
        0o500
    } else {
        0o400
    };
    fs::set_permissions(path, fs::Permissions::from_mode(mode)).map_err(|_| GuestError::Unavailable)
}

#[cfg(not(unix))]
fn set_permissions(_path: &Path, _archived_mode: u64, _directory: bool) -> Result<(), GuestError> {
    Ok(())
}

fn tar_path(header: &[u8]) -> Result<PathBuf, GuestError> {
    let name = tar_text(&header[0..100])?;
    let prefix = tar_text(&header[345..500])?;
    let path = PathBuf::from(if prefix.is_empty() {
        name
    } else {
        format!("{prefix}/{name}")
    });
    if path.as_os_str().is_empty()
        || path.is_absolute()
        || path.components().any(|component| {
            !matches!(component, Component::Normal(_))
                || component.as_os_str().to_string_lossy().contains('\\')
        })
    {
        return Err(GuestError::Integrity);
    }
    Ok(path)
}

fn tar_text(bytes: &[u8]) -> Result<String, GuestError> {
    let end = bytes
        .iter()
        .position(|byte| *byte == 0)
        .unwrap_or(bytes.len());
    let value = std::str::from_utf8(&bytes[..end]).map_err(|_| GuestError::Integrity)?;
    if value.chars().any(char::is_control) {
        return Err(GuestError::Integrity);
    }
    Ok(value.to_owned())
}

fn parse_tar_octal(bytes: &[u8]) -> Result<u64, GuestError> {
    let text = tar_text(bytes)?;
    let text = text.trim_matches(' ');
    if text.is_empty() || !text.bytes().all(|byte| matches!(byte, b'0'..=b'7')) {
        return Err(GuestError::Integrity);
    }
    u64::from_str_radix(text, 8).map_err(|_| GuestError::Integrity)
}

fn valid_tar_checksum(header: &[u8]) -> bool {
    let Ok(expected) = parse_tar_octal(&header[148..156]) else {
        return false;
    };
    header
        .iter()
        .enumerate()
        .map(|(index, byte)| {
            if (148..156).contains(&index) {
                u64::from(b' ')
            } else {
                u64::from(*byte)
            }
        })
        .sum::<u64>()
        == expected
}

fn required(name: &'static str) -> Result<String, GuestError> {
    std::env::var(name)
        .ok()
        .filter(|value| {
            !value.is_empty() && value.len() <= 16_384 && !value.chars().any(char::is_control)
        })
        .ok_or(GuestError::MissingConfiguration(name))
}

fn parsed_env<T>(name: &'static str) -> Result<T, GuestError>
where
    T: FromStr,
{
    required(name)?
        .parse()
        .map_err(|_| GuestError::InvalidConfiguration)
}

fn absolute_path(name: &'static str) -> Result<PathBuf, GuestError> {
    let path = PathBuf::from(required(name)?);
    if !path.is_absolute()
        || path == Path::new("/")
        || path
            .components()
            .any(|part| matches!(part, Component::ParentDir))
    {
        return Err(GuestError::InvalidConfiguration);
    }
    Ok(path)
}

fn read_bounded(path: &Path, maximum: usize) -> Result<Vec<u8>, GuestError> {
    let file = fs::File::open(path).map_err(|_| GuestError::Unavailable)?;
    let metadata = file.metadata().map_err(|_| GuestError::Unavailable)?;
    if !metadata.is_file()
        || metadata.len() == 0
        || metadata.len() > u64::try_from(maximum).unwrap_or(u64::MAX)
    {
        return Err(GuestError::LimitExceeded);
    }
    let mut bytes = Vec::with_capacity(usize::try_from(metadata.len()).unwrap_or(maximum));
    file.take(u64::try_from(maximum).unwrap_or(u64::MAX) + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| GuestError::Unavailable)?;
    if bytes.is_empty() || bytes.len() > maximum {
        return Err(GuestError::LimitExceeded);
    }
    Ok(bytes)
}

fn write_new(path: &Path, bytes: &[u8]) -> Result<(), GuestError> {
    let mut file = OpenOptions::new()
        .write(true)
        .truncate(true)
        .open(path)
        .map_err(|_| GuestError::Unavailable)?;
    file.write_all(bytes).map_err(|_| GuestError::Unavailable)?;
    file.sync_all().map_err(|_| GuestError::Unavailable)
}

#[derive(Debug)]
enum GuestError {
    MissingConfiguration(&'static str),
    InvalidConfiguration,
    Denied,
    Unavailable,
    Integrity,
    LimitExceeded,
    TimedOut,
    Execution,
}

impl fmt::Display for GuestError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingConfiguration(name) => write!(formatter, "missing configuration {name}"),
            Self::InvalidConfiguration => formatter.write_str("invalid configuration"),
            Self::Denied => formatter.write_str("request denied"),
            Self::Unavailable => formatter.write_str("dependency unavailable"),
            Self::Integrity => formatter.write_str("invalid execution evidence"),
            Self::LimitExceeded => formatter.write_str("resource limit exceeded"),
            Self::TimedOut => formatter.write_str("execution timed out"),
            Self::Execution => formatter.write_str("guest process failed"),
        }
    }
}

impl Error for GuestError {}

#[cfg(test)]
mod tests {
    use super::*;

    fn tar_header(name: &str) -> [u8; TAR_BLOCK_BYTES] {
        let mut header = [0u8; TAR_BLOCK_BYTES];
        header[..name.len()].copy_from_slice(name.as_bytes());
        header[100..108].copy_from_slice(b"0000400\0");
        header[124..136].copy_from_slice(b"00000000000\0");
        header[148..156].fill(b' ');
        header[156] = b'0';
        header[257..263].copy_from_slice(b"ustar\0");
        header[263..265].copy_from_slice(b"00");
        let checksum: u64 = header.iter().map(|byte| u64::from(*byte)).sum();
        let encoded = format!("{checksum:06o}\0 ");
        header[148..156].copy_from_slice(encoded.as_bytes());
        header
    }

    #[test]
    fn tar_paths_reject_escape_absolute_and_windows_forms() {
        assert!(tar_path(&tar_header("package/main.js")).is_ok());
        assert!(tar_path(&tar_header("../escape")).is_err());
        assert!(tar_path(&tar_header("/absolute")).is_err());
        assert!(tar_path(&tar_header("windows\\escape")).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn exact_entrypoint_rejects_symlink() {
        use std::os::unix::fs::symlink;
        let root = tempfile::tempdir().unwrap();
        let target = root.path().join("target");
        fs::write(&target, b"payload").unwrap();
        symlink(&target, root.path().join("entry")).unwrap();
        assert!(exact_entrypoint(root.path(), "entry", false).is_err());
    }
}
