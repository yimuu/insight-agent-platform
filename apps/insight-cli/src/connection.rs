//! Private public-client references. Installation and authentication remain external owners.
use crate::{public_client::PublicHttpClient, CliError};
use insight_platform_contracts::{parse_strict_json, JsonLimits, ResourceId, ResourceKind};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    ffi::OsString,
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
    time::Duration,
};

const FILE_NAME: &str = "connection.json";
const MAX_BYTES: u64 = 16_384;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Command {
    pub root: PathBuf,
    pub endpoint: String,
    pub runtime_endpoint: Option<String>,
    pub tenant_id: ResourceId,
    pub token_file: PathBuf,
    pub ca_file: Option<PathBuf>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ConnectionV1 {
    pub schema_version: u16,
    pub management_endpoint: String,
    pub runtime_endpoint: String,
    pub tenant_id: ResourceId,
    pub token_file: PathBuf,
    pub ca_file: Option<PathBuf>,
}

fn invalid(message: &'static str) -> CliError {
    CliError::RuntimeState(message.to_owned())
}

pub fn parse(args: &[OsString]) -> Result<Command, CliError> {
    let mut fields = BTreeMap::new();
    if !args.len().is_multiple_of(2) {
        return Err(CliError::Usage);
    }
    for pair in args.chunks_exact(2) {
        let key = pair[0].to_str().ok_or(CliError::Usage)?;
        if !matches!(
            key,
            "--path"
                | "--endpoint"
                | "--runtime-endpoint"
                | "--tenant"
                | "--token-file"
                | "--ca-file"
        ) || fields.insert(key, &pair[1]).is_some()
        {
            return Err(CliError::Usage);
        }
    }
    let text = |key| -> Result<String, CliError> {
        fields
            .get(key)
            .and_then(|s| s.to_str())
            .map(str::to_owned)
            .ok_or(CliError::Usage)
    };
    Ok(Command {
        root: fields
            .get("--path")
            .map_or_else(|| PathBuf::from("."), PathBuf::from),
        endpoint: text("--endpoint")?,
        runtime_endpoint: fields
            .get("--runtime-endpoint")
            .map(|_| text("--runtime-endpoint"))
            .transpose()?,
        tenant_id: ResourceId::parse_expected(&text("--tenant")?, ResourceKind::Tenant)
            .map_err(|_| CliError::Usage)?,
        token_file: PathBuf::from(fields.get("--token-file").ok_or(CliError::Usage)?),
        ca_file: fields.get("--ca-file").map(PathBuf::from),
    })
}

pub fn execute(command: Command, cwd: &Path) -> Result<String, CliError> {
    let root = canonical_root(&absolute(cwd, &command.root))?;
    let connection = ConnectionV1 {
        schema_version: 1,
        runtime_endpoint: command
            .runtime_endpoint
            .unwrap_or_else(|| command.endpoint.clone()),
        management_endpoint: command.endpoint,
        tenant_id: command.tenant_id,
        token_file: absolute_reference(cwd, &command.token_file)?,
        ca_file: command
            .ca_file
            .as_ref()
            .map(|p| absolute_reference(cwd, p))
            .transpose()?,
    };
    persist(&root, &connection, false)?;
    // This is a local binding report, never an assertion of server authentication/readiness.
    crate::render_json(&serde_json::json!({
        "schema_version": 1,
        "state": "connection_saved",
        "tenant_id": connection.tenant_id,
        "management_endpoint": connection.management_endpoint,
        "runtime_endpoint": connection.runtime_endpoint,
        "authentication_verified": false,
    }))
}

impl ConnectionV1 {
    fn validate(&self) -> Result<(), CliError> {
        if self.schema_version != 1
            || self.tenant_id.kind() != ResourceKind::Tenant
            || !crate::public_client::is_public_gateway_origin(&self.management_endpoint)
            || !crate::public_client::is_public_gateway_origin(&self.runtime_endpoint)
            || !valid_reference(&self.token_file)
            || self.ca_file.as_ref().is_some_and(|p| !valid_reference(p))
        {
            return Err(invalid("public connection configuration is invalid"));
        }
        Ok(())
    }

    fn same_target(&self, other: &Self) -> bool {
        self.management_endpoint == other.management_endpoint
            && self.runtime_endpoint == other.runtime_endpoint
            && self.tenant_id == other.tenant_id
    }

    pub(crate) fn client(&self, runtime: bool) -> Result<PublicHttpClient, CliError> {
        self.validate()?;
        PublicHttpClient::from_connection_files(
            if runtime {
                &self.runtime_endpoint
            } else {
                &self.management_endpoint
            },
            &self.token_file,
            self.ca_file.as_deref(),
            Some(&self.tenant_id),
            Duration::from_secs(5),
        )
        .map_err(CliError::RuntimeState)
    }
}

pub(crate) fn load(root: &Path) -> Result<ConnectionV1, CliError> {
    let root = canonical_root(root)?;
    let directory = root.join(crate::PROJECT_DIRECTORY);
    check_directory(&directory)?;
    let bytes = read_private(&directory.join(FILE_NAME), MAX_BYTES).map_err(|_| {
        invalid("no valid public connection; run `insight connect` for this workspace")
    })?;
    let value = parse_strict_json(
        &bytes,
        JsonLimits {
            max_bytes: MAX_BYTES as usize,
            max_depth: 2,
            max_properties_per_object: 7,
            max_items_per_array: 1,
            max_string_bytes: 4096,
        },
    )
    .map_err(|_| invalid("public connection JSON is invalid"))?;
    let connection: ConnectionV1 = serde_json::from_value(value)
        .map_err(|_| invalid("public connection fields are invalid"))?;
    connection.validate()?;
    Ok(connection)
}

fn persist(root: &Path, connection: &ConnectionV1, qualification: bool) -> Result<(), CliError> {
    connection.validate()?;
    // Validate referenced material before creating or changing any workspace file.
    connection.client(false)?;
    connection.client(true)?;
    let directory = root.join(crate::PROJECT_DIRECTORY);
    ensure_directory(&directory)?;
    let _lock = acquire_lock(&directory, "connection.lock")?;
    let path = directory.join(FILE_NAME);
    match fs::symlink_metadata(&path) {
        Ok(_) => {
            let previous = load(root)?;
            if !previous.same_target(connection) {
                return Err(invalid(
                    "this workspace is bound to another target; use a new workspace and preserve its recovery records",
                ));
            }
            if previous == *connection {
                return Ok(());
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            if !qualification {
                validate_first_binding(root, &directory)?;
            }
        }
        Err(_) => return Err(invalid("cannot inspect existing public connection")),
    }
    let bytes = serde_json::to_vec_pretty(connection)
        .map_err(|_| invalid("cannot encode public connection"))?;
    if bytes.len() as u64 > MAX_BYTES {
        return Err(invalid("public connection exceeds its byte bound"));
    }
    atomic_write(&path, &bytes)
}

fn validate_first_binding(root: &Path, directory: &Path) -> Result<(), CliError> {
    match fs::symlink_metadata(root.join("insight.lock")) {
        Ok(_) => {
            return Err(invalid(
                "existing publication identity has no connection binding; use a new workspace",
            ));
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(_) => return Err(invalid("cannot inspect existing publication identity")),
    }
    // Offline authoring inputs carry no public mutation intent. Everything else is rejected,
    // including future unknown journal kinds, so this is never an implicit state migration.
    for item in fs::read_dir(directory)
        .map_err(|_| invalid("cannot inspect workspace state"))?
        .take(7)
    {
        let item = item.map_err(|_| invalid("cannot inspect workspace state"))?;
        let name = item.file_name();
        if !matches!(
            name.to_str(),
            Some(
                "connection.lock"
                    | "agent-compiler-profile.json"
                    | "agent-binding-selections.json"
                    | "agent-exact-bindings.json"
                    | "agent-model-bindings.json"
            )
        ) || !item
            .file_type()
            .map_err(|_| invalid("cannot inspect workspace state"))?
            .is_file()
        {
            return Err(invalid(
                "existing workspace state has no connection binding; use a new workspace and preserve original records",
            ));
        }
    }
    Ok(())
}

/// Only the explicit qualification producer calls this after creating its actual environment.
/// Ordinary public clients never read LocalProjectState.
pub(crate) fn export_qualification(root: &Path) -> Result<(), CliError> {
    let root = canonical_root(root)?;
    let state = root.join(crate::PROJECT_DIRECTORY);
    let project = crate::load_local_project_state(&state)?;
    crate::validate_loaded_local_identity(&state, &project.identity)?;
    // This producer owns the just-created qualification directory. Tighten its old public
    // metadata mode before storing the private connection reference; normal connect never
    // changes permissions of an existing workspace.
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        let metadata = fs::symlink_metadata(&state)
            .map_err(|_| invalid("cannot inspect qualification workspace"))?;
        if !metadata.is_dir() || metadata.uid() != unsafe { libc::geteuid() } {
            return Err(invalid("qualification workspace ownership is invalid"));
        }
        fs::set_permissions(&state, fs::Permissions::from_mode(0o700))
            .map_err(|_| invalid("cannot protect qualification workspace"))?;
    }
    let profile = crate::read_runtime_profile_state(
        &state.join(crate::RUNTIME_DIRECTORY),
        &project.identity,
    )?
    .ok_or_else(|| invalid("qualification profile is absent"))?;
    persist(
        &root,
        &ConnectionV1 {
            schema_version: 1,
            management_endpoint: format!("http://127.0.0.1:{}", profile.ports.gateway_management),
            runtime_endpoint: format!("http://127.0.0.1:{}", profile.ports.gateway_runtime),
            tenant_id: ResourceId::parse_expected(
                &project.identity.tenant_id,
                ResourceKind::Tenant,
            )
            .map_err(|_| invalid("qualification Tenant identity is invalid"))?,
            token_file: state
                .join(crate::IDENTITY_DIRECTORY)
                .join(crate::IDENTITY_ACCESS_TOKEN_FILE),
            ca_file: None,
        },
        true,
    )
}

pub(crate) struct MutationLock {
    _file: File,
}

pub(crate) fn acquire_mutation_lock(root: &Path) -> Result<MutationLock, CliError> {
    let root = canonical_root(root)?;
    load(&root)?;
    acquire_lock(&root.join(crate::PROJECT_DIRECTORY), "public-mutation.lock")
}

fn acquire_lock(directory: &Path, name: &str) -> Result<MutationLock, CliError> {
    check_directory(directory)?;
    let path = directory.join(name);
    let mut options = private_options();
    options.read(true).write(true).create(true);
    let file = options
        .open(&path)
        .map_err(|_| invalid("cannot open private workspace lock"))?;
    check_file(&file, &path, None)?;
    #[cfg(unix)]
    {
        use std::os::fd::AsRawFd;
        if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
            return Err(invalid(
                "another command is mutating this workspace; retry after it finishes",
            ));
        }
    }
    #[cfg(not(unix))]
    return Err(invalid(
        "private workspace locking is unsupported on this host",
    ));
    Ok(MutationLock { _file: file })
}

fn check_directory(path: &Path) -> Result<(), CliError> {
    let metadata = fs::symlink_metadata(path)
        .map_err(|_| invalid("workspace requires a private .insight directory"))?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err(invalid("workspace state must be a physical directory"));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if metadata.mode() & 0o777 != 0o700 || metadata.uid() != unsafe { libc::geteuid() } {
            return Err(invalid(
                "workspace state requires current-user ownership and mode 0700",
            ));
        }
    }
    Ok(())
}

fn ensure_directory(path: &Path) -> Result<(), CliError> {
    let mut builder = fs::DirBuilder::new();
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
    }
    match builder.create(path) {
        Ok(()) => {
            sync_parent(path)?;
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(_) => return Err(invalid("cannot create private workspace state")),
    }
    check_directory(path)
}

fn private_options() -> OpenOptions {
    let mut options = OpenOptions::new();
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK);
    }
    options
}

fn check_file(file: &File, path: &Path, maximum: Option<u64>) -> Result<(), CliError> {
    let opened = file
        .metadata()
        .map_err(|_| invalid("cannot inspect private workspace file"))?;
    let current =
        fs::symlink_metadata(path).map_err(|_| invalid("cannot inspect private workspace file"))?;
    if !opened.is_file()
        || !current.is_file()
        || current.file_type().is_symlink()
        || maximum.is_some_and(|max| opened.len() == 0 || opened.len() > max)
    {
        return Err(invalid(
            "workspace file must be private, regular, and bounded",
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if opened.ino() != current.ino()
            || opened.dev() != current.dev()
            || opened.nlink() != 1
            || opened.uid() != unsafe { libc::geteuid() }
            || opened.mode() & 0o777 != 0o600
        {
            return Err(invalid(
                "workspace file identity or permissions are invalid",
            ));
        }
    }
    Ok(())
}

fn read_private(path: &Path, maximum: u64) -> Result<Vec<u8>, CliError> {
    let file = private_options()
        .read(true)
        .open(path)
        .map_err(|_| invalid("cannot open private workspace file"))?;
    check_file(&file, path, Some(maximum))?;
    let mut bytes = Vec::new();
    file.take(maximum + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| invalid("cannot read private workspace file"))?;
    if bytes.is_empty() || bytes.len() as u64 > maximum {
        return Err(invalid("workspace file exceeds its byte bound"));
    }
    Ok(bytes)
}

fn atomic_write(path: &Path, bytes: &[u8]) -> Result<(), CliError> {
    let temporary = path.with_file_name(format!(".connection-{}.tmp", uuid::Uuid::new_v4()));
    let result = (|| {
        let mut file = private_options()
            .write(true)
            .create_new(true)
            .open(&temporary)
            .map_err(|_| invalid("cannot create private connection replacement"))?;
        file.write_all(bytes)
            .and_then(|_| file.sync_all())
            .map_err(|_| invalid("cannot persist private connection"))?;
        fs::rename(&temporary, path).map_err(|_| invalid("cannot install private connection"))?;
        sync_parent(path)
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

fn sync_parent(path: &Path) -> Result<(), CliError> {
    File::open(
        path.parent()
            .ok_or_else(|| invalid("connection parent is absent"))?,
    )
    .and_then(|file| file.sync_all())
    .map_err(|_| invalid("cannot sync connection directory"))
}

fn canonical_root(root: &Path) -> Result<PathBuf, CliError> {
    let root =
        fs::canonicalize(root).map_err(|_| invalid("connection workspace must already exist"))?;
    if !root.is_dir() {
        return Err(invalid("connection workspace must be a directory"));
    }
    Ok(root)
}
fn absolute(cwd: &Path, path: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_owned()
    } else {
        cwd.join(path)
    }
}
fn absolute_reference(cwd: &Path, path: &Path) -> Result<PathBuf, CliError> {
    let path = absolute(cwd, path);
    let parent = fs::canonicalize(
        path.parent()
            .ok_or_else(|| invalid("connection reference has no parent"))?,
    )
    .map_err(|_| invalid("connection reference parent does not exist"))?;
    let result = parent.join(
        path.file_name()
            .ok_or_else(|| invalid("connection reference has no file name"))?,
    );
    if !valid_reference(&result) {
        return Err(invalid("connection reference is invalid"));
    }
    Ok(result)
}
fn valid_reference(path: &Path) -> bool {
    path.is_absolute()
        && path
            .to_str()
            .is_some_and(|s| s.len() <= 4096 && !s.chars().any(char::is_control))
        && path.components().all(|c| {
            !matches!(
                c,
                std::path::Component::ParentDir | std::path::Component::CurDir
            )
        })
}

#[cfg(test)]
mod tests;
