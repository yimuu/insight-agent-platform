use crate::{
    CollectedGvisorOutput, GvisorBrokerError, GvisorCleanupReceipt, GvisorWorkloadBroker,
    MaterializedGvisorBundle,
};
use async_trait::async_trait;
use insight_platform_contracts::{
    canonical_digest, canonical_json, parse_strict_json, JsonLimits, ResourceId,
    SandboxIsolationClass, Sha256Digest, ValueRef,
};
use insight_platform_sandbox::{
    RevokeWasiSandboxGrants, SandboxCompletedOutput, SandboxExecutionOutcome,
    SandboxExecutionRequest, SandboxResourceEnvelope, SandboxResourceUsage, WasiArtifactBroker,
    WasiArtifactBrokerError, WasiArtifactReadPurpose, WasiArtifactReadRequest,
    WasiGrantRevocationError, WasiGrantRevoker, WasiValueDirection, WasiValueValidationError,
    WasiValueValidationRequest, WasiValueValidator,
};
use insight_platform_sandbox_gvisor::RunscCommandOutput;
use serde::Deserialize;
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File, OpenOptions},
    io::{Read as _, Write as _},
    path::{Component, Path, PathBuf},
    sync::{Arc, Mutex},
    time::Instant,
};

const TAR_BLOCK_BYTES: usize = 512;
const MAX_BUNDLE_ENTRIES: usize = 100_000;
const MAX_OCI_CONFIG_BYTES: usize = 1024 * 1024;
const IMAGE_DIGEST_ANNOTATION: &str = "insight.platform/image-digest";
const PACKAGE_DIGEST_ANNOTATION: &str = "insight.platform/package-digest";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GrpcGvisorWorkloadBrokerConfig {
    pub bundle_root: PathBuf,
    pub maximum_bundle_bytes: usize,
    pub maximum_result_bytes: usize,
}

impl GrpcGvisorWorkloadBrokerConfig {
    fn validate(&self) -> bool {
        self.bundle_root.is_absolute()
            && self.bundle_root != Path::new("/")
            && !self
                .bundle_root
                .components()
                .any(|component| matches!(component, Component::ParentDir))
            && self.maximum_bundle_bytes > 0
            && self.maximum_result_bytes > 0
    }
}

pub struct GrpcGvisorWorkloadBroker {
    config: GrpcGvisorWorkloadBrokerConfig,
    artifacts: Arc<dyn WasiArtifactBroker>,
    value_validator: Arc<dyn WasiValueValidator>,
    grant_revoker: Arc<dyn WasiGrantRevoker>,
    started: Mutex<BTreeMap<Sha256Digest, Instant>>,
}

impl GrpcGvisorWorkloadBroker {
    pub fn new(
        config: GrpcGvisorWorkloadBrokerConfig,
        artifacts: Arc<dyn WasiArtifactBroker>,
        value_validator: Arc<dyn WasiValueValidator>,
        grant_revoker: Arc<dyn WasiGrantRevoker>,
    ) -> Result<Self, GvisorBrokerError> {
        if !config.validate() {
            return Err(GvisorBrokerError::Denied);
        }
        let root =
            fs::symlink_metadata(&config.bundle_root).map_err(|_| GvisorBrokerError::Denied)?;
        if !root.is_dir() || root.file_type().is_symlink() {
            return Err(GvisorBrokerError::Denied);
        }
        Ok(Self {
            config,
            artifacts,
            value_validator,
            grant_revoker,
            started: Mutex::new(BTreeMap::new()),
        })
    }

    fn bundle_path(&self, request: &SandboxExecutionRequest) -> Result<PathBuf, GvisorBrokerError> {
        let digest = request.request_digest.to_string();
        let suffix = digest
            .strip_prefix("sha256:")
            .filter(|value| value.len() == 64)
            .ok_or(GvisorBrokerError::Integrity)?;
        Ok(self.config.bundle_root.join(format!(
            "{}-{}-{}",
            suffix, request.attempt_no, request.lease_generation
        )))
    }

    async fn revoke(
        &self,
        request: &SandboxExecutionRequest,
        worker_process_generation_id: &ResourceId,
    ) -> Result<Sha256Digest, GvisorBrokerError> {
        self.grant_revoker
            .revoke_exact(RevokeWasiSandboxGrants {
                tenant_id: request.tenant_id.clone(),
                sandbox_job_id: request.sandbox_job_id.clone(),
                request_digest: request.request_digest.clone(),
                worker_process_generation_id: worker_process_generation_id.clone(),
                attempt_no: request.attempt_no,
                lease_generation: request.lease_generation,
            })
            .await
            .map(|evidence| evidence.evidence_digest)
            .map_err(|error| match error {
                WasiGrantRevocationError::Rejected => GvisorBrokerError::Denied,
                WasiGrantRevocationError::Unavailable => GvisorBrokerError::Unavailable,
            })
    }
}

#[async_trait]
impl GvisorWorkloadBroker for GrpcGvisorWorkloadBroker {
    async fn materialize_bundle(
        &self,
        request: &SandboxExecutionRequest,
        worker_process_generation_id: &ResourceId,
    ) -> Result<MaterializedGvisorBundle, GvisorBrokerError> {
        if request.isolation_class != SandboxIsolationClass::SandboxedContainer
            || request.package.runtime_bundle_artifact.byte_length()
                > u64::try_from(self.config.maximum_bundle_bytes)
                    .map_err(|_| GvisorBrokerError::Denied)?
        {
            return Err(GvisorBrokerError::Denied);
        }
        let bytes = self
            .artifacts
            .read_exact(WasiArtifactReadRequest {
                tenant_id: request.tenant_id.clone(),
                sandbox_job_id: request.sandbox_job_id.clone(),
                request_digest: request.request_digest.clone(),
                worker_process_generation_id: worker_process_generation_id.clone(),
                lease_generation: request.lease_generation,
                artifact: request.package.runtime_bundle_artifact.clone(),
                purpose: WasiArtifactReadPurpose::RuntimeBundle,
                read_grant: None,
                maximum_bytes: self.config.maximum_bundle_bytes,
                deadline: request.deadline,
            })
            .await
            .map_err(map_artifact_error)?;
        let bundle_path = self.bundle_path(request)?;
        let image_digest = request.runtime.image_or_module_digest.clone();
        let package_digest = request.package.package_digest.clone();
        let resources = request.resources.clone();
        let extraction_path = bundle_path.clone();
        tokio::task::spawn_blocking(move || {
            extract_reviewed_oci_bundle(
                &bytes,
                &extraction_path,
                &image_digest,
                &package_digest,
                &resources,
            )
        })
        .await
        .map_err(|_| GvisorBrokerError::Unavailable)??;
        self.started
            .lock()
            .map_err(|_| GvisorBrokerError::Unavailable)?
            .insert(request.request_digest.clone(), Instant::now());
        let materialization_evidence_digest = canonical_digest(&serde_json::json!({
            "schema_version": 1,
            "kind": "gvisor_oci_bundle_materialized",
            "request_digest": request.request_digest,
            "runtime_bundle_artifact": request.package.runtime_bundle_artifact,
            "image_digest": request.runtime.image_or_module_digest,
            "package_digest": request.package.package_digest,
        }))
        .map_err(|_| GvisorBrokerError::Integrity)?
        .parse()
        .map_err(|_| GvisorBrokerError::Integrity)?;
        Ok(MaterializedGvisorBundle {
            bundle_path,
            image_digest: request.runtime.image_or_module_digest.clone(),
            materialization_evidence_digest,
        })
    }

    async fn collect_output(
        &self,
        request: &SandboxExecutionRequest,
        worker_process_generation_id: &ResourceId,
        runtime_output: RunscCommandOutput,
    ) -> Result<CollectedGvisorOutput, GvisorBrokerError> {
        let started = self
            .started
            .lock()
            .map_err(|_| GvisorBrokerError::Unavailable)?
            .remove(&request.request_digest)
            .ok_or(GvisorBrokerError::Integrity)?;
        if runtime_output.stdout.len() > self.config.maximum_result_bytes
            || u64::try_from(runtime_output.stderr.len())
                .map_or(true, |bytes| bytes > request.resources.stderr_bytes)
        {
            return Err(GvisorBrokerError::Integrity);
        }
        let value = parse_strict_json(
            &runtime_output.stdout,
            JsonLimits {
                max_bytes: self.config.maximum_result_bytes,
                max_depth: 64,
                max_items_per_array: 4_096,
                max_properties_per_object: 4_096,
                max_string_bytes: self.config.maximum_result_bytes,
            },
        )
        .map_err(|_| GvisorBrokerError::Integrity)?;
        if canonical_json(&value).as_deref() != Ok(runtime_output.stdout.as_slice()) {
            return Err(GvisorBrokerError::Integrity);
        }
        let value_bytes = canonical_json(&value).map_err(|_| GvisorBrokerError::Integrity)?;
        if u64::try_from(value_bytes.len())
            .map_or(true, |bytes| bytes > request.resources.result_bytes)
        {
            return Err(GvisorBrokerError::Integrity);
        }
        let validation_evidence_digest = self
            .value_validator
            .validate(WasiValueValidationRequest {
                tenant_id: request.tenant_id.clone(),
                sandbox_job_id: request.sandbox_job_id.clone(),
                request_digest: request.request_digest.clone(),
                worker_process_generation_id: worker_process_generation_id.clone(),
                lease_generation: request.lease_generation,
                direction: WasiValueDirection::Output,
                schema_digest: request.output_schema_digest.clone(),
                classification: request.classification,
                value: value.clone(),
            })
            .await
            .map_err(|error| match error {
                WasiValueValidationError::Invalid => GvisorBrokerError::Integrity,
                WasiValueValidationError::Unavailable => GvisorBrokerError::Unavailable,
            })?;
        let content_digest = canonical_digest(&value)
            .map_err(|_| GvisorBrokerError::Integrity)?
            .parse()
            .map_err(|_| GvisorBrokerError::Integrity)?;
        let wall_milliseconds = u64::try_from(started.elapsed().as_millis())
            .unwrap_or(u64::MAX)
            .max(1)
            .min(request.resources.wall_milliseconds);
        let usage = SandboxResourceUsage {
            cpu_milliseconds: wall_milliseconds,
            peak_memory_mebibytes: 0,
            peak_pids: 1,
            files_created: 0,
            io_bytes: u64::try_from(value_bytes.len())
                .unwrap_or(u64::MAX)
                .min(request.resources.io_bytes),
            stdout_bytes: 0,
            stderr_bytes: u64::try_from(runtime_output.stderr.len()).unwrap_or(u64::MAX),
            result_bytes: u64::try_from(value_bytes.len()).unwrap_or(u64::MAX),
            artifact_output_bytes: 0,
            network_connections: 0,
            network_request_bytes: 0,
            network_response_bytes: 0,
            wall_milliseconds,
            wasm_fuel_consumed: None,
        };
        usage
            .validate_for(request)
            .map_err(|_| GvisorBrokerError::Integrity)?;
        let collection_evidence_digest = canonical_digest(&serde_json::json!({
            "schema_version": 1,
            "kind": "gvisor_output_collected",
            "request_digest": request.request_digest,
            "content_digest": content_digest,
            "validation_evidence_digest": validation_evidence_digest,
            "usage": usage,
        }))
        .map_err(|_| GvisorBrokerError::Integrity)?
        .parse()
        .map_err(|_| GvisorBrokerError::Integrity)?;
        Ok(CollectedGvisorOutput {
            outcome: SandboxExecutionOutcome::Completed(Box::new(SandboxCompletedOutput {
                value_id: request.output_value_id.clone(),
                classification: request.classification,
                schema_digest: request.output_schema_digest.clone(),
                content_digest,
                value: ValueRef::Inline { value },
                artifact_link_ids: Vec::new(),
                artifact_outputs: Vec::new(),
                validation_evidence_digest,
                usage: usage.clone(),
            })),
            usage,
            collection_evidence_digest,
        })
    }

    async fn revoke_grants(
        &self,
        request: &SandboxExecutionRequest,
        worker_process_generation_id: &ResourceId,
    ) -> Result<Sha256Digest, GvisorBrokerError> {
        self.revoke(request, worker_process_generation_id).await
    }

    async fn cleanup(
        &self,
        request: &SandboxExecutionRequest,
        worker_process_generation_id: &ResourceId,
        bundle: Option<&MaterializedGvisorBundle>,
    ) -> Result<GvisorCleanupReceipt, GvisorBrokerError> {
        let grants = self.revoke(request, worker_process_generation_id).await?;
        let path = bundle
            .map(|bundle| bundle.bundle_path.clone())
            .unwrap_or(self.bundle_path(request)?);
        if !path.starts_with(&self.config.bundle_root) || path == self.config.bundle_root {
            return Err(GvisorBrokerError::Integrity);
        }
        let cleanup_path = path.clone();
        tokio::task::spawn_blocking(move || remove_bundle_tree(&cleanup_path))
            .await
            .map_err(|_| GvisorBrokerError::Unavailable)??;
        if tokio::fs::try_exists(&path)
            .await
            .map_err(|_| GvisorBrokerError::Unavailable)?
        {
            return Err(GvisorBrokerError::Unavailable);
        }
        if let Ok(mut started) = self.started.lock() {
            started.remove(&request.request_digest);
        }
        let evidence_digest = canonical_digest(&serde_json::json!({
            "schema_version": 1,
            "kind": "gvisor_cleanup",
            "request_digest": request.request_digest,
            "grant_revocation_evidence_digest": grants,
            "ephemeral_storage_destroyed": true,
        }))
        .map_err(|_| GvisorBrokerError::Integrity)?
        .parse()
        .map_err(|_| GvisorBrokerError::Integrity)?;
        Ok(GvisorCleanupReceipt {
            grants_revoked: true,
            ephemeral_storage_destroyed: true,
            evidence_digest,
        })
    }
}

fn map_artifact_error(error: WasiArtifactBrokerError) -> GvisorBrokerError {
    match error {
        WasiArtifactBrokerError::Denied => GvisorBrokerError::Denied,
        WasiArtifactBrokerError::Integrity | WasiArtifactBrokerError::TooLarge => {
            GvisorBrokerError::Integrity
        }
        WasiArtifactBrokerError::Unavailable | WasiArtifactBrokerError::NotFound => {
            GvisorBrokerError::Unavailable
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReviewedOciConfig {
    #[serde(rename = "ociVersion")]
    oci_version: String,
    #[serde(default)]
    hostname: Option<String>,
    #[serde(default)]
    domainname: Option<String>,
    root: ReviewedOciRoot,
    process: ReviewedOciProcess,
    linux: ReviewedOciLinux,
    #[serde(default)]
    mounts: Vec<ReviewedOciMount>,
    #[serde(default)]
    hooks: Option<serde_json::Value>,
    annotations: BTreeMap<String, String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReviewedOciRoot {
    path: String,
    readonly: bool,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ReviewedOciProcess {
    terminal: bool,
    args: Vec<String>,
    cwd: String,
    no_new_privileges: bool,
    capabilities: ReviewedOciCapabilities,
    #[serde(default)]
    env: Vec<String>,
    user: ReviewedOciUser,
    #[serde(default)]
    rlimits: Vec<ReviewedOciRlimit>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ReviewedOciUser {
    uid: u32,
    gid: u32,
    #[serde(default)]
    additional_gids: Vec<u32>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReviewedOciRlimit {
    #[serde(rename = "type")]
    kind: String,
    hard: u64,
    soft: u64,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ReviewedOciCapabilities {
    bounding: Vec<String>,
    effective: Vec<String>,
    inheritable: Vec<String>,
    permitted: Vec<String>,
    ambient: Vec<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReviewedOciLinux {
    namespaces: Vec<ReviewedOciNamespace>,
    resources: ReviewedOciResources,
    #[serde(default)]
    masked_paths: Vec<String>,
    #[serde(default)]
    readonly_paths: Vec<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReviewedOciResources {
    memory: ReviewedOciMemory,
    cpu: ReviewedOciCpu,
    pids: ReviewedOciPids,
    unified: BTreeMap<String, String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReviewedOciMemory {
    limit: i64,
    swap: i64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReviewedOciCpu {
    quota: i64,
    period: u64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReviewedOciPids {
    limit: i64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReviewedOciNamespace {
    #[serde(rename = "type")]
    kind: String,
    #[serde(default)]
    path: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReviewedOciMount {
    destination: String,
    #[serde(rename = "type")]
    kind: String,
    source: String,
    #[serde(default)]
    options: Vec<String>,
}

fn extract_reviewed_oci_bundle(
    archive: &[u8],
    destination: &Path,
    expected_image_digest: &Sha256Digest,
    expected_package_digest: &Sha256Digest,
    resources: &SandboxResourceEnvelope,
) -> Result<(), GvisorBrokerError> {
    if archive.is_empty() || !archive.len().is_multiple_of(TAR_BLOCK_BYTES) || destination.exists()
    {
        return Err(GvisorBrokerError::Integrity);
    }
    fs::create_dir(destination).map_err(|_| GvisorBrokerError::Unavailable)?;
    set_owner_only_directory(destination)?;
    let result = extract_tar_entries(archive, destination).and_then(|_| {
        validate_reviewed_oci_config(
            destination,
            expected_image_digest,
            expected_package_digest,
            resources,
        )
    });
    if result.is_err() {
        let _ = remove_bundle_tree(destination);
    }
    result
}

#[cfg(unix)]
fn set_owner_only_directory(path: &Path) -> Result<(), GvisorBrokerError> {
    use std::os::unix::fs::PermissionsExt as _;
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))
        .map_err(|_| GvisorBrokerError::Unavailable)
}

#[cfg(not(unix))]
fn set_owner_only_directory(_path: &Path) -> Result<(), GvisorBrokerError> {
    Ok(())
}

fn remove_bundle_tree(path: &Path) -> Result<(), GvisorBrokerError> {
    if !path.exists() {
        return Ok(());
    }
    make_directories_owner_accessible(path)?;
    fs::remove_dir_all(path).map_err(|_| GvisorBrokerError::Unavailable)
}

fn make_directories_owner_accessible(path: &Path) -> Result<(), GvisorBrokerError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))
            .map_err(|_| GvisorBrokerError::Unavailable)?;
    }
    for entry in fs::read_dir(path).map_err(|_| GvisorBrokerError::Unavailable)? {
        let entry = entry.map_err(|_| GvisorBrokerError::Unavailable)?;
        let metadata = entry
            .file_type()
            .map_err(|_| GvisorBrokerError::Unavailable)?;
        if metadata.is_dir() {
            make_directories_owner_accessible(&entry.path())?;
        }
    }
    Ok(())
}

fn extract_tar_entries(archive: &[u8], destination: &Path) -> Result<(), GvisorBrokerError> {
    let mut offset = 0usize;
    let mut entries = 0usize;
    let mut paths = BTreeSet::new();
    let mut extracted_permissions = Vec::new();
    while offset + TAR_BLOCK_BYTES <= archive.len() {
        let header = &archive[offset..offset + TAR_BLOCK_BYTES];
        offset += TAR_BLOCK_BYTES;
        if header.iter().all(|byte| *byte == 0) {
            if archive[offset..].iter().any(|byte| *byte != 0) {
                return Err(GvisorBrokerError::Integrity);
            }
            extracted_permissions.sort_by_key(|(path, _, _): &(PathBuf, u64, bool)| {
                std::cmp::Reverse(path.components().count())
            });
            for (path, mode, directory) in extracted_permissions {
                set_extracted_permissions(&path, mode, directory)?;
            }
            return Ok(());
        }
        entries = entries.checked_add(1).ok_or(GvisorBrokerError::Integrity)?;
        if entries > MAX_BUNDLE_ENTRIES
            || &header[257..263] != b"ustar\0"
            || &header[263..265] != b"00"
            || !valid_tar_checksum(header)
        {
            return Err(GvisorBrokerError::Integrity);
        }
        let name = tar_path(header)?;
        if !paths.insert(name.clone()) {
            return Err(GvisorBrokerError::Integrity);
        }
        let mode = parse_tar_octal(&header[100..108])?;
        let size = parse_tar_octal(&header[124..136])?;
        let size = usize::try_from(size).map_err(|_| GvisorBrokerError::Integrity)?;
        let padded = size
            .checked_add(TAR_BLOCK_BYTES - 1)
            .ok_or(GvisorBrokerError::Integrity)?
            / TAR_BLOCK_BYTES
            * TAR_BLOCK_BYTES;
        if offset
            .checked_add(padded)
            .is_none_or(|end| end > archive.len())
        {
            return Err(GvisorBrokerError::Integrity);
        }
        let target = destination.join(&name);
        match header[156] {
            0 | b'0' => {
                let parent = target.parent().ok_or(GvisorBrokerError::Integrity)?;
                if parent != destination {
                    let relative_parent = parent
                        .strip_prefix(destination)
                        .map_err(|_| GvisorBrokerError::Integrity)?;
                    if !paths.contains(relative_parent) || !parent.is_dir() {
                        return Err(GvisorBrokerError::Integrity);
                    }
                }
                let mut file = OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(&target)
                    .map_err(|_| GvisorBrokerError::Unavailable)?;
                file.write_all(&archive[offset..offset + size])
                    .map_err(|_| GvisorBrokerError::Unavailable)?;
                file.sync_all()
                    .map_err(|_| GvisorBrokerError::Unavailable)?;
                extracted_permissions.push((target, mode, false));
            }
            b'5' if size == 0 => {
                let parent = target.parent().ok_or(GvisorBrokerError::Integrity)?;
                if parent != destination {
                    let relative_parent = parent
                        .strip_prefix(destination)
                        .map_err(|_| GvisorBrokerError::Integrity)?;
                    if !paths.contains(relative_parent) || !parent.is_dir() {
                        return Err(GvisorBrokerError::Integrity);
                    }
                }
                fs::create_dir(&target).map_err(|_| GvisorBrokerError::Unavailable)?;
                extracted_permissions.push((target, mode, true));
            }
            _ => return Err(GvisorBrokerError::Integrity),
        }
        offset += padded;
    }
    Err(GvisorBrokerError::Integrity)
}

#[cfg(unix)]
fn set_extracted_permissions(
    path: &Path,
    archived_mode: u64,
    directory: bool,
) -> Result<(), GvisorBrokerError> {
    use std::os::unix::fs::PermissionsExt as _;
    let mode = if directory || archived_mode & 0o111 != 0 {
        0o500
    } else {
        0o400
    };
    fs::set_permissions(path, fs::Permissions::from_mode(mode))
        .map_err(|_| GvisorBrokerError::Unavailable)
}

#[cfg(not(unix))]
fn set_extracted_permissions(
    _path: &Path,
    _archived_mode: u64,
    _directory: bool,
) -> Result<(), GvisorBrokerError> {
    Ok(())
}

fn tar_path(header: &[u8]) -> Result<PathBuf, GvisorBrokerError> {
    let name = tar_text(&header[0..100])?;
    let prefix = tar_text(&header[345..500])?;
    let combined = if prefix.is_empty() {
        name
    } else {
        format!("{prefix}/{name}")
    };
    let path = PathBuf::from(combined);
    if path.as_os_str().is_empty()
        || path.is_absolute()
        || path.components().any(|component| {
            !matches!(component, Component::Normal(_))
                || component.as_os_str().to_string_lossy().contains('\\')
        })
    {
        return Err(GvisorBrokerError::Integrity);
    }
    Ok(path)
}

fn tar_text(bytes: &[u8]) -> Result<String, GvisorBrokerError> {
    let end = bytes
        .iter()
        .position(|byte| *byte == 0)
        .unwrap_or(bytes.len());
    let value = std::str::from_utf8(&bytes[..end]).map_err(|_| GvisorBrokerError::Integrity)?;
    if value.chars().any(char::is_control) {
        return Err(GvisorBrokerError::Integrity);
    }
    Ok(value.to_owned())
}

fn parse_tar_octal(bytes: &[u8]) -> Result<u64, GvisorBrokerError> {
    let text = tar_text(bytes)?;
    let text = text.trim_matches(' ');
    if text.is_empty() || !text.bytes().all(|byte| matches!(byte, b'0'..=b'7')) {
        return Err(GvisorBrokerError::Integrity);
    }
    u64::from_str_radix(text, 8).map_err(|_| GvisorBrokerError::Integrity)
}

fn valid_tar_checksum(header: &[u8]) -> bool {
    let Ok(expected) = parse_tar_octal(&header[148..156]) else {
        return false;
    };
    let actual = header
        .iter()
        .enumerate()
        .map(|(index, byte)| {
            if (148..156).contains(&index) {
                u64::from(b' ')
            } else {
                u64::from(*byte)
            }
        })
        .sum::<u64>();
    actual == expected
}

fn validate_reviewed_oci_config(
    destination: &Path,
    expected_image_digest: &Sha256Digest,
    expected_package_digest: &Sha256Digest,
    resources: &SandboxResourceEnvelope,
) -> Result<(), GvisorBrokerError> {
    let config_path = destination.join("config.json");
    let mut file = File::open(&config_path).map_err(|_| GvisorBrokerError::Integrity)?;
    let mut bytes = Vec::new();
    std::io::Read::by_ref(&mut file)
        .take((MAX_OCI_CONFIG_BYTES + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(|_| GvisorBrokerError::Unavailable)?;
    if bytes.len() > MAX_OCI_CONFIG_BYTES {
        return Err(GvisorBrokerError::Integrity);
    }
    let value = parse_strict_json(
        &bytes,
        JsonLimits {
            max_bytes: MAX_OCI_CONFIG_BYTES,
            max_depth: 32,
            max_items_per_array: 4_096,
            max_properties_per_object: 256,
            max_string_bytes: 16_384,
        },
    )
    .map_err(|_| GvisorBrokerError::Integrity)?;
    let config: ReviewedOciConfig =
        serde_json::from_value(value).map_err(|_| GvisorBrokerError::Integrity)?;
    let capabilities_empty = config.process.capabilities.bounding.is_empty()
        && config.process.capabilities.effective.is_empty()
        && config.process.capabilities.inheritable.is_empty()
        && config.process.capabilities.permitted.is_empty()
        && config.process.capabilities.ambient.is_empty();
    let namespaces = config
        .linux
        .namespaces
        .iter()
        .map(|namespace| namespace.kind.as_str())
        .collect::<BTreeSet<_>>();
    let required_namespaces = ["ipc", "mount", "network", "pid", "user", "uts"]
        .into_iter()
        .collect::<BTreeSet<_>>();
    let rootfs = destination.join("rootfs");
    let mounts_closed = config.mounts.iter().all(|mount| {
        matches!(
            mount.kind.as_str(),
            "proc" | "tmpfs" | "devpts" | "mqueue" | "sysfs"
        ) && !mount.destination.is_empty()
            && !mount.source.is_empty()
            && !mount
                .options
                .iter()
                .any(|option| option == "rw" && mount.kind == "sysfs")
    });
    let cpu_period = 100_000_u64;
    let cpu_quota = u64::from(resources.cpu_millicores)
        .checked_mul(cpu_period)
        .and_then(|value| value.checked_div(1_000))
        .and_then(|value| i64::try_from(value).ok())
        .filter(|value| *value > 0)
        .ok_or(GvisorBrokerError::Integrity)?;
    let memory_bytes = u64::from(resources.memory_mebibytes)
        .checked_mul(1024 * 1024)
        .and_then(|value| i64::try_from(value).ok())
        .ok_or(GvisorBrokerError::Integrity)?;
    let pids = i64::from(resources.pids);
    let nofile = u64::from(resources.files)
        .checked_add(3)
        .ok_or(GvisorBrokerError::Integrity)?;
    let rlimits = config
        .process
        .rlimits
        .iter()
        .map(|limit| (limit.kind.as_str(), limit))
        .collect::<BTreeMap<_, _>>();
    let limits_closed = rlimits.len() == 2
        && rlimits.get("RLIMIT_FSIZE").is_some_and(|limit| {
            limit.soft == resources.io_bytes && limit.hard == resources.io_bytes
        })
        && rlimits
            .get("RLIMIT_NOFILE")
            .is_some_and(|limit| limit.soft == nofile && limit.hard == nofile);
    if config.oci_version != "1.1.0"
        || config
            .hostname
            .as_ref()
            .is_some_and(|value| value.len() > 253)
        || config
            .domainname
            .as_ref()
            .is_some_and(|value| value.len() > 253)
        || config.root.path != "rootfs"
        || !config.root.readonly
        || !rootfs.is_dir()
        || config.process.terminal
        || !config.process.no_new_privileges
        || config.process.args.is_empty()
        || !config.process.cwd.starts_with('/')
        || config.process.user.uid == 0
        || config.process.user.gid == 0
        || !config.process.user.additional_gids.is_empty()
        || !capabilities_empty
        || !limits_closed
        || config.hooks.is_some()
        || config
            .linux
            .namespaces
            .iter()
            .any(|namespace| namespace.path.is_some())
        || !required_namespaces.is_subset(&namespaces)
        || !mounts_closed
        || config.annotations.get(IMAGE_DIGEST_ANNOTATION)
            != Some(&expected_image_digest.to_string())
        || config.annotations.get(PACKAGE_DIGEST_ANNOTATION)
            != Some(&expected_package_digest.to_string())
        || config.process.env.iter().any(|entry| {
            entry.starts_with("AWS_")
                || entry.starts_with("GOOGLE_")
                || entry.starts_with("AZURE_")
                || entry.starts_with("KUBERNETES_")
        })
        || config.linux.resources.memory.limit != memory_bytes
        || config.linux.resources.memory.swap != memory_bytes
        || config.linux.resources.cpu.period != cpu_period
        || config.linux.resources.cpu.quota != cpu_quota
        || config.linux.resources.pids.limit != pids
        || config.linux.resources.unified.len() != 1
        || config.linux.resources.unified.get("memory.swap.max") != Some(&"0".to_owned())
        || config.linux.masked_paths.len() > 128
        || config.linux.readonly_paths.len() > 128
    {
        return Err(GvisorBrokerError::Integrity);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    fn digest(character: char) -> Sha256Digest {
        format!("sha256:{}", character.to_string().repeat(64))
            .parse()
            .unwrap()
    }

    fn resources() -> SandboxResourceEnvelope {
        SandboxResourceEnvelope {
            cpu_millicores: 500,
            memory_mebibytes: 128,
            pids: 32,
            files: 64,
            io_bytes: 1_048_576,
            stdout_bytes: 65_536,
            stderr_bytes: 65_536,
            result_bytes: 65_536,
            artifact_output_bytes: 0,
            network_connections: 0,
            network_request_bytes: 0,
            network_response_bytes: 0,
            startup_milliseconds: 1_000,
            idle_milliseconds: 1_000,
            wall_milliseconds: 10_000,
            cleanup_milliseconds: 1_000,
            wasm_fuel: None,
            wasm_memory_pages: None,
        }
    }

    fn config(
        image: &Sha256Digest,
        package: &Sha256Digest,
        resources: &SandboxResourceEnvelope,
    ) -> Vec<u8> {
        let memory_bytes = u64::from(resources.memory_mebibytes) * 1024 * 1024;
        let cpu_period = 100_000_u64;
        let cpu_quota = u64::from(resources.cpu_millicores) * cpu_period / 1_000;
        let nofile = u64::from(resources.files) + 3;
        serde_json::to_vec(&serde_json::json!({
            "ociVersion": "1.1.0",
            "root": {"path": "rootfs", "readonly": true},
            "process": {
                "terminal": false,
                "args": ["/bin/program"],
                "cwd": "/",
                "noNewPrivileges": true,
                "capabilities": {
                    "bounding": [], "effective": [], "inheritable": [],
                    "permitted": [], "ambient": []
                },
                "env": ["LANG=C.UTF-8"],
                "user": {"uid": 65532, "gid": 65532},
                "rlimits": [
                    {"type": "RLIMIT_FSIZE", "hard": resources.io_bytes, "soft": resources.io_bytes},
                    {"type": "RLIMIT_NOFILE", "hard": nofile, "soft": nofile}
                ]
            },
            "linux": {
                "namespaces": [
                    {"type": "ipc"}, {"type": "mount"}, {"type": "network"},
                    {"type": "pid"}, {"type": "user"}, {"type": "uts"}
                ],
                "resources": {
                    "memory": {"limit": memory_bytes, "swap": memory_bytes},
                    "cpu": {"quota": cpu_quota, "period": cpu_period},
                    "pids": {"limit": resources.pids},
                    "unified": {"memory.swap.max": "0"}
                },
                "masked_paths": [],
                "readonly_paths": []
            },
            "mounts": [],
            "annotations": {
                (IMAGE_DIGEST_ANNOTATION): image.to_string(),
                (PACKAGE_DIGEST_ANNOTATION): package.to_string()
            }
        }))
        .unwrap()
    }

    fn append_tar_entry(archive: &mut Vec<u8>, name: &str, kind: u8, mode: u64, body: &[u8]) {
        let mut header = [0_u8; TAR_BLOCK_BYTES];
        assert!(name.len() <= 100);
        header[..name.len()].copy_from_slice(name.as_bytes());
        write_octal(&mut header[100..108], mode);
        write_octal(&mut header[108..116], 0);
        write_octal(&mut header[116..124], 0);
        write_octal(&mut header[124..136], body.len() as u64);
        write_octal(&mut header[136..148], 0);
        header[148..156].fill(b' ');
        header[156] = kind;
        header[257..263].copy_from_slice(b"ustar\0");
        header[263..265].copy_from_slice(b"00");
        let checksum = header.iter().map(|byte| u64::from(*byte)).sum::<u64>();
        let encoded = format!("{checksum:06o}\0 ");
        header[148..156].copy_from_slice(encoded.as_bytes());
        archive.extend_from_slice(&header);
        archive.extend_from_slice(body);
        archive.resize(archive.len().div_ceil(TAR_BLOCK_BYTES) * TAR_BLOCK_BYTES, 0);
    }

    fn write_octal(target: &mut [u8], value: u64) {
        let encoded = format!("{:0width$o}\0", value, width = target.len() - 1);
        target.copy_from_slice(encoded.as_bytes());
    }

    fn bundle(
        image: &Sha256Digest,
        package: &Sha256Digest,
        resources: &SandboxResourceEnvelope,
    ) -> Vec<u8> {
        let mut archive = Vec::new();
        append_tar_entry(&mut archive, "rootfs", b'5', 0o755, &[]);
        append_tar_entry(&mut archive, "rootfs/bin", b'5', 0o755, &[]);
        append_tar_entry(
            &mut archive,
            "rootfs/bin/program",
            b'0',
            0o555,
            b"reviewed-program",
        );
        append_tar_entry(
            &mut archive,
            "config.json",
            b'0',
            0o444,
            &config(image, package, resources),
        );
        archive.resize(archive.len() + TAR_BLOCK_BYTES * 2, 0);
        archive
    }

    fn destination() -> PathBuf {
        std::env::temp_dir().join(format!("insight-gvisor-bundle-{}", Uuid::new_v4()))
    }

    #[test]
    fn reviewed_oci_bundle_extracts_only_regular_files_and_directories() {
        let image = digest('a');
        let package = digest('b');
        let resources = resources();
        serde_json::from_slice::<ReviewedOciConfig>(&config(&image, &package, &resources)).unwrap();
        let destination = destination();
        extract_reviewed_oci_bundle(
            &bundle(&image, &package, &resources),
            &destination,
            &image,
            &package,
            &resources,
        )
        .unwrap();
        assert_eq!(
            fs::read(destination.join("rootfs/bin/program")).unwrap(),
            b"reviewed-program"
        );
        remove_bundle_tree(&destination).unwrap();
    }

    #[test]
    fn archive_traversal_symlink_and_identity_drift_fail_closed() {
        let image = digest('a');
        let package = digest('b');
        let resources = resources();

        let mut traversal = Vec::new();
        append_tar_entry(&mut traversal, "../escape", b'0', 0o444, b"escape");
        traversal.resize(traversal.len() + TAR_BLOCK_BYTES * 2, 0);
        let traversal_destination = destination();
        assert_eq!(
            extract_reviewed_oci_bundle(
                &traversal,
                &traversal_destination,
                &image,
                &package,
                &resources,
            ),
            Err(GvisorBrokerError::Integrity)
        );
        assert!(!traversal_destination.exists());

        let mut symlink = Vec::new();
        append_tar_entry(&mut symlink, "rootfs/link", b'2', 0o777, &[]);
        symlink.resize(symlink.len() + TAR_BLOCK_BYTES * 2, 0);
        let symlink_destination = destination();
        assert_eq!(
            extract_reviewed_oci_bundle(
                &symlink,
                &symlink_destination,
                &image,
                &package,
                &resources,
            ),
            Err(GvisorBrokerError::Integrity)
        );
        assert!(!symlink_destination.exists());

        let drift_destination = destination();
        assert_eq!(
            extract_reviewed_oci_bundle(
                &bundle(&image, &package, &resources),
                &drift_destination,
                &digest('c'),
                &package,
                &resources,
            ),
            Err(GvisorBrokerError::Integrity)
        );
        assert!(!drift_destination.exists());

        let resource_drift_destination = destination();
        let mut drifted_resources = resources.clone();
        drifted_resources.memory_mebibytes += 1;
        assert_eq!(
            extract_reviewed_oci_bundle(
                &bundle(&image, &package, &resources),
                &resource_drift_destination,
                &image,
                &package,
                &drifted_resources,
            ),
            Err(GvisorBrokerError::Integrity)
        );
        assert!(!resource_drift_destination.exists());
    }
}
