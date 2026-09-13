//! Per-role publication of renderer-owned bytes into exclusive installation volumes.
//! The private journal authorizes only recovery of this finite physical file inventory.
use crate::{
    private_state::InstallationDirectory, renderer::RenderedInstallationFiles, role_material, tls,
};
use insight_platform_contracts::{canonical_digest, parse_strict_json, JsonLimits, Sha256Digest};
use insight_platform_deployment_contracts::installation::{
    InstallationError as Error, InstallationIdentityV1, InstallationInputV1, InstallationTopology,
    INSTALLATION_LIMITS, INSTALLATION_MAX_BYTES,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
};

const JOURNAL: &str = "role-output.json";
const DEPENDENCY_JOURNAL: &str = "dependency-output.json";
const NATS_DATA_JOURNAL: &str = "nats-data-directory.json";
const NATS_CONFIGURATION: &[u8] = include_bytes!("../../../../deploy/dev/nats.conf");
const JOURNAL_LIMITS: JsonLimits = JsonLimits {
    max_bytes: 65_536,
    max_depth: 4,
    max_properties_per_object: 12,
    max_items_per_array: 32,
    max_string_bytes: 2048,
};

#[derive(Clone, PartialEq, Eq)]
pub enum RoleOutputMode {
    Provision,
    Verify,
    Upgrade,
    Rollout(Sha256Digest),
}
#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RoleOutputOwnership {
    ServingUsers,
    NativeCurrentUser,
    #[cfg(test)]
    CurrentUser,
}
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Pending {
    destination: String,
    bytes_digest: Sha256Digest,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Journal {
    schema_version: u32,
    input_digest: Sha256Digest,
    identity_digest: Sha256Digest,
    source_digest: Sha256Digest,
    files_digest: Sha256Digest,
    output_root: String,
    ownership: RoleOutputOwnership,
    complete: bool,
    pending: Option<Pending>,
}
impl Journal {
    fn save(&self, private: &InstallationDirectory, journal_name: &str) -> Result<(), Error> {
        private.replace(
            journal_name,
            &serde_json::to_vec(self).map_err(|_| Error::InvalidInput)?,
        )
    }
}
struct Role<'a> {
    owner: u32,
    files: BTreeMap<String, &'a [u8]>,
}
struct Publication<'a> {
    input: &'a InstallationInputV1,
    identity: &'a InstallationIdentityV1,
    private: &'a InstallationDirectory,
    root: &'a Path,
    mode: RoleOutputMode,
    ownership: RoleOutputOwnership,
    journal: &'static str,
    source_digest: Sha256Digest,
}
fn validate_private_identity(
    input: &InstallationInputV1,
    identity: &InstallationIdentityV1,
    private: &InstallationDirectory,
) -> Result<(), Error> {
    input.validate()?;
    identity.validate()?;
    if identity.input_digest != input.digest()? {
        return Err(Error::IdentityDrift);
    }
    let encoded = private
        .read("input.json", INSTALLATION_MAX_BYTES)?
        .ok_or(Error::Incomplete)?;
    if InstallationInputV1::decode(&encoded)?.digest()? != input.digest()? {
        return Err(Error::IdentityDrift);
    }
    let encoded = private
        .read("identity.json", INSTALLATION_MAX_BYTES)?
        .ok_or(Error::Incomplete)?;
    let installed: InstallationIdentityV1 = serde_json::from_value(
        parse_strict_json(&encoded, INSTALLATION_LIMITS).map_err(|_| Error::InvalidInput)?,
    )
    .map_err(|_| Error::InvalidInput)?;
    if installed.digest()? != identity.digest()? {
        return Err(Error::IdentityDrift);
    }
    Ok(())
}
fn bytes_digest(bytes: &[u8]) -> Sha256Digest {
    format!("sha256:{}", crate::lower_hex(&Sha256::digest(bytes)))
        .parse()
        .expect("actual SHA256")
}
fn canonical(value: &impl Serialize) -> Result<Sha256Digest, Error> {
    canonical_digest(&serde_json::to_value(value).map_err(|_| Error::InvalidInput)?)
        .map_err(|_| Error::InvalidInput)?
        .parse()
        .map_err(|_| Error::InvalidInput)
}
fn leaf(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 128
        && name != "."
        && name != ".."
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
}
fn checked_bytes(bytes: &[u8]) -> Result<(), Error> {
    if bytes.is_empty() || bytes.len() > INSTALLATION_MAX_BYTES {
        return Err(Error::InvalidInput);
    }
    Ok(())
}
fn inventory<'a>(
    input: &InstallationInputV1,
    identity: &InstallationIdentityV1,
    rendered: &'a RenderedInstallationFiles,
    ownership: RoleOutputOwnership,
) -> Result<BTreeMap<String, Role<'a>>, Error> {
    rendered.evidence.validate_for(input, identity)?;
    role_material::validate_credentials(&input.network, &input.credentials)?;
    if input.network.topology != InstallationTopology::Native
        && ownership == RoleOutputOwnership::NativeCurrentUser
    {
        return Err(Error::UnsupportedTopology);
    }
    let expected = input
        .network
        .processes
        .iter()
        .map(|entry| entry.process)
        .collect::<BTreeSet<_>>();
    if rendered.roles.keys().copied().collect::<BTreeSet<_>>() != expected {
        return Err(Error::InvalidRoleClosure);
    }
    let mut roles = BTreeMap::new();
    for (&process, rendered_role) in &rendered.roles {
        let evidence = rendered
            .evidence
            .processes
            .iter()
            .find(|entry| entry.process == process)
            .ok_or(Error::InvalidRoleClosure)?;
        let mut credential_names = input
            .credentials
            .files
            .iter()
            .filter(|entry| entry.process == process)
            .map(|entry| entry.file_name.clone())
            .collect::<BTreeSet<_>>();
        for (_, identity) in role_material::tls_identities(process) {
            credential_names.insert(identity.certificate.into());
            credential_names.insert(tls::RUNTIME_CA_CERTIFICATE_FILE.into());
        }
        if role_material::requires_aws_ca(&input.network, process) {
            credential_names.insert(tls::RUNTIME_CA_CERTIFICATE_FILE.into());
        }
        if let Some(role) = role_material::openbao_role(&input.network, process) {
            credential_names.insert(crate::openbao_profile::certificate_file(role));
        }
        if !leaf(&rendered_role.configuration_file)
            || evidence.configuration_file != rendered_role.configuration_file
            || rendered_role
                .credentials
                .keys()
                .cloned()
                .collect::<BTreeSet<_>>()
                != credential_names
            || evidence
                .credential_files
                .iter()
                .map(|entry| entry.file_name.clone())
                .collect::<BTreeSet<_>>()
                != credential_names
        {
            return Err(Error::CredentialInvalid);
        }
        checked_bytes(&rendered_role.configuration)?;
        checked_bytes(&rendered_role.environment)?;
        let configuration = parse_strict_json(&rendered_role.configuration, INSTALLATION_LIMITS)
            .map_err(|_| Error::ConfigurationDrift)?;
        if canonical(&configuration)? != evidence.configuration_digest
            || bytes_digest(&rendered_role.environment) != evidence.environment_bytes_digest
        {
            return Err(Error::ConfigurationDrift);
        }
        let mut files = BTreeMap::from([
            (
                format!("config/{}", rendered_role.configuration_file),
                rendered_role.configuration.as_slice(),
            ),
            ("environment".into(), rendered_role.environment.as_slice()),
        ]);
        for (name, bytes) in &rendered_role.credentials {
            checked_bytes(bytes)?;
            if !leaf(name)
                || evidence
                    .credential_files
                    .iter()
                    .find(|entry| &entry.file_name == name)
                    .is_none_or(|entry| entry.bytes_digest != bytes_digest(bytes))
            {
                return Err(Error::CredentialInvalid);
            }
            files.insert(format!("credentials/{name}"), bytes.as_slice());
        }
        roles.insert(
            process.name().to_owned(),
            Role {
                owner: target_uid(ownership, false),
                files,
            },
        );
    }
    checked_bytes(&rendered.console)?;
    if let Some(identity) = &rendered.local_identity {
        checked_bytes(identity)?;
        parse_strict_json(identity, INSTALLATION_LIMITS).map_err(|_| Error::ConfigurationDrift)?;
        roles.insert(
            "local-identity".into(),
            Role {
                owner: target_uid(ownership, true),
                files: BTreeMap::from([("config.json".into(), identity.as_slice())]),
            },
        );
    }
    parse_strict_json(&rendered.console, INSTALLATION_LIMITS)
        .map_err(|_| Error::ConfigurationDrift)?;
    roles.insert(
        "console".into(),
        Role {
            owner: target_uid(ownership, true),
            files: BTreeMap::from([("config.json".into(), rendered.console.as_slice())]),
        },
    );
    Ok(roles)
}
#[cfg(unix)]
fn target_uid(ownership: RoleOutputOwnership, console: bool) -> u32 {
    match ownership {
        RoleOutputOwnership::ServingUsers => {
            if console {
                1000
            } else {
                10001
            }
        }
        RoleOutputOwnership::NativeCurrentUser => unsafe { libc::geteuid() },
        #[cfg(test)]
        RoleOutputOwnership::CurrentUser => unsafe { libc::geteuid() },
    }
}
#[cfg(not(unix))]
fn target_uid(_: RoleOutputOwnership, _: bool) -> u32 {
    0
}

/// Native dependency adapters use the same local user as the private file publisher.
pub fn native_user_ids() -> Result<(u32, u32), Error> {
    #[cfg(unix)]
    {
        let (uid, gid) = (unsafe { libc::geteuid() }, unsafe { libc::getegid() });
        if uid == 0 || gid == 0 {
            return Err(Error::CredentialInvalid);
        }
        Ok((uid, gid))
    }
    #[cfg(not(unix))]
    {
        Err(Error::UnsupportedTopology)
    }
}

/// The caller holds the installation lock and has established exclusive identity-bound output
/// volumes. Root writes only the declared role leaves; serving containers mount one role read-only.
pub fn publish_role_outputs(
    input: &InstallationInputV1,
    identity: &InstallationIdentityV1,
    rendered: &RenderedInstallationFiles,
    private: &InstallationDirectory,
    output_root: &Path,
    mode: RoleOutputMode,
    ownership: RoleOutputOwnership,
) -> Result<(), Error> {
    #[cfg(unix)]
    {
        unix::publish(
            input,
            identity,
            rendered,
            private,
            output_root,
            mode,
            ownership,
        )
    }
    #[cfg(not(unix))]
    {
        let _ = (
            input,
            identity,
            rendered,
            private,
            output_root,
            mode,
            ownership,
        );
        Err(Error::UnsupportedTopology)
    }
}

/// Publish only each dependency's declared bootstrap or server material. This output
/// root is separate from all serving-role volumes and the installation's private identity volume.
pub fn publish_dependency_outputs(
    input: &InstallationInputV1,
    identity: &InstallationIdentityV1,
    private: &InstallationDirectory,
    output_root: &Path,
    mode: RoleOutputMode,
    ownership: RoleOutputOwnership,
) -> Result<(), Error> {
    #[cfg(unix)]
    {
        validate_private_identity(input, identity, private)?;
        let password = private
            .read("postgres-admin-password", 32)?
            .ok_or(Error::Incomplete)?;
        if password.len() != 32 || !password.iter().all(u8::is_ascii_hexdigit) {
            return Err(Error::CredentialInvalid);
        }
        let ca = private
            .read(tls::RUNTIME_CA_CERTIFICATE_FILE, INSTALLATION_MAX_BYTES)?
            .ok_or(Error::Incomplete)?;
        let certificate = private
            .read(
                tls::RUNTIME_NATS_SERVER_CERTIFICATE_FILE,
                INSTALLATION_MAX_BYTES,
            )?
            .ok_or(Error::Incomplete)?;
        let key = private
            .read(
                tls::RUNTIME_NATS_SERVER_PRIVATE_KEY_FILE,
                INSTALLATION_MAX_BYTES,
            )?
            .ok_or(Error::Incomplete)?;
        let user = target_uid(ownership, false);
        let postgres = if ownership == RoleOutputOwnership::ServingUsers {
            999
        } else {
            user
        };
        let mut roles = BTreeMap::from([
            (
                "postgres".into(),
                Role {
                    owner: postgres,
                    files: BTreeMap::from([("admin-password".into(), password.as_slice())]),
                },
            ),
            (
                "nats".into(),
                Role {
                    owner: user,
                    files: BTreeMap::from([
                        ("nats.conf".into(), NATS_CONFIGURATION),
                        ("tls/ca.pem".into(), ca.as_slice()),
                        ("tls/server.pem".into(), certificate.as_slice()),
                        ("tls/server-key.pem".into(), key.as_slice()),
                    ]),
                },
            ),
        ]);
        let mut provider_material = BTreeMap::<String, BTreeMap<String, Vec<u8>>>::new();
        let read = |name| {
            private
                .read(name, INSTALLATION_MAX_BYTES)?
                .ok_or(Error::Incomplete)
        };
        provider_material.insert(
            "openbao".into(),
            BTreeMap::from([
                ("ca.pem".into(), ca.clone()),
                (
                    "openbao-seal.key".into(),
                    read(crate::openbao_profile::OPENBAO_SEAL_FILE)?,
                ),
                (
                    "openbao-server.pem".into(),
                    read(crate::openbao_profile::OPENBAO_SERVER_CERTIFICATE)?,
                ),
                (
                    "openbao-server-key.pem".into(),
                    read(crate::openbao_profile::OPENBAO_SERVER_KEY)?,
                ),
                ("serve.json".into(), read("openbao-serve.json")?),
            ]),
        );
        let mut credentials = BTreeMap::new();
        for role in crate::s3_profile::S3IdentityRole::ALL {
            let mut bytes = read(role.credential_filename())?;
            let credential = crate::s3_profile::S3RoleCredentials::decode(&bytes);
            bytes.fill(0);
            credentials.insert(role, credential?);
        }
        let bucket = format!(
            "insight-platform-artifacts-{}",
            identity.installation_id.uuid().simple()
        );
        let profile = crate::s3_profile::render_s3_profile(&bucket, &credentials)?;
        provider_material.insert(
            "s3".into(),
            BTreeMap::from([
                ("ca.pem".into(), ca.clone()),
                ("server.crt".into(), read("s3-server.pem")?),
                ("server.key".into(), read("s3-server-key.pem")?),
                ("grpc-ca.pem".into(), read("s3-grpc-ca.pem")?),
                ("grpc-server.crt".into(), read("s3-grpc-server.crt")?),
                ("grpc-server.key".into(), read("s3-grpc-server.key")?),
                ("s3.json".into(), profile.configuration_json),
                ("security.toml".into(), profile.security_toml),
            ]),
        );
        for (name, files) in &provider_material {
            roles.insert(
                name.clone(),
                Role {
                    owner: user,
                    files: files
                        .iter()
                        .map(|(name, bytes)| (name.clone(), bytes.as_slice()))
                        .collect(),
                },
            );
        }
        let publication = Publication {
            input,
            identity,
            private,
            root: output_root,
            mode,
            ownership,
            journal: DEPENDENCY_JOURNAL,
            source_digest: bytes_digest(b"insight_installation_dependency_files_v2_s3_openbao"),
        };
        let result = unix::publish_files(&publication, &roles);
        drop(roles);
        for files in provider_material.values_mut() {
            for bytes in files.values_mut() {
                bytes.fill(0);
            }
        }
        result
    }
    #[cfg(not(unix))]
    {
        let _ = (input, identity, private, output_root, mode, ownership);
        Err(Error::UnsupportedTopology)
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DataDirectoryJournal {
    schema_version: u32,
    input_digest: Sha256Digest,
    identity_digest: Sha256Digest,
    directory: String,
    ownership: RoleOutputOwnership,
    owner: u32,
    complete: bool,
}

/// Initialize the root of the explicitly bound empty NATS data volume once. Completed checks
/// inspect only root metadata; JetStream's live data belongs to NATS and is never traversed here.
pub fn initialize_nats_data_directory(
    input: &InstallationInputV1,
    identity: &InstallationIdentityV1,
    private: &InstallationDirectory,
    directory: &Path,
    mode: RoleOutputMode,
    ownership: RoleOutputOwnership,
) -> Result<(), Error> {
    #[cfg(unix)]
    {
        unix::initialize_data(
            input,
            identity,
            private,
            directory,
            mode,
            ownership,
            DependencyDataDirectory::Nats,
        )
    }
    #[cfg(not(unix))]
    {
        let _ = (input, identity, private, directory, mode, ownership);
        Err(Error::UnsupportedTopology)
    }
}

#[derive(Clone, Copy)]
pub enum DependencyDataDirectory {
    Nats,
    S3,
    OpenBao,
    Postgres,
}
impl DependencyDataDirectory {
    fn journal(self) -> &'static str {
        match self {
            Self::Nats => NATS_DATA_JOURNAL,
            Self::S3 => "s3-data-directory.json",
            Self::OpenBao => "openbao-data-directory.json",
            Self::Postgres => "postgres-data-directory.json",
        }
    }
}
pub fn initialize_dependency_data_directory(
    input: &InstallationInputV1,
    identity: &InstallationIdentityV1,
    private: &InstallationDirectory,
    directory: &Path,
    mode: RoleOutputMode,
    ownership: RoleOutputOwnership,
    kind: DependencyDataDirectory,
) -> Result<(), Error> {
    #[cfg(unix)]
    {
        unix::initialize_data(input, identity, private, directory, mode, ownership, kind)
    }
    #[cfg(not(unix))]
    {
        let _ = (input, identity, private, directory, mode, ownership, kind);
        Err(Error::UnsupportedTopology)
    }
}

#[cfg(unix)]
mod unix {
    use super::*;
    include!("role_output_upgrade.rs");
    use std::{
        ffi::{CStr, CString},
        fs::{File, Metadata},
        io::{Read as _, Write as _},
        os::{
            fd::{AsRawFd as _, FromRawFd as _},
            unix::fs::MetadataExt as _,
        },
        path::Component,
    };

    struct Directory(File);
    fn name(value: &str) -> Result<CString, Error> {
        CString::new(value).map_err(|_| Error::InvalidPath)
    }
    fn code() -> i32 {
        std::io::Error::last_os_error().raw_os_error().unwrap_or(-1)
    }
    fn opened(fd: i32) -> Result<File, Error> {
        if fd < 0 {
            return Err(Error::CredentialInvalid);
        }
        // Successful open/dup returns a newly owned descriptor.
        Ok(unsafe { File::from_raw_fd(fd) })
    }
    impl Directory {
        fn absolute(path: &Path) -> Result<Self, Error> {
            if !path.is_absolute()
                || path
                    .components()
                    .any(|part| !matches!(part, Component::RootDir | Component::Normal(_)))
            {
                return Err(Error::InvalidPath);
            }
            let mut directory = Self(opened(unsafe {
                libc::open(
                    c"/".as_ptr(),
                    libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
                )
            })?);
            for component in path.components() {
                if let Component::Normal(value) = component {
                    directory = directory
                        .child(value.to_str().ok_or(Error::InvalidPath)?)?
                        .ok_or(Error::Incomplete)?;
                }
            }
            Ok(directory)
        }
        fn child(&self, leaf: &str) -> Result<Option<Self>, Error> {
            let leaf = name(leaf)?;
            let fd = unsafe {
                libc::openat(
                    self.0.as_raw_fd(),
                    leaf.as_ptr(),
                    libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
                )
            };
            if fd < 0 && code() == libc::ENOENT {
                return Ok(None);
            }
            Ok(Some(Self(opened(fd)?)))
        }
        fn create(&self, leaf: &str, owner: u32) -> Result<Self, Error> {
            let encoded = name(leaf)?;
            if unsafe { libc::mkdirat(self.0.as_raw_fd(), encoded.as_ptr(), 0o700) } != 0 {
                return Err(Error::Conflict);
            }
            let directory = self.child(leaf)?.ok_or(Error::Incomplete)?;
            directory.initialize_empty(owner)?;
            self.sync()?;
            Ok(directory)
        }
        fn sync(&self) -> Result<(), Error> {
            self.0.sync_all().map_err(|_| Error::Incomplete)
        }
        fn check(&self, owner: u32) -> Result<(), Error> {
            let metadata = self.0.metadata().map_err(|_| Error::CredentialInvalid)?;
            if !metadata.is_dir() || metadata.uid() != owner || metadata.mode() & 0o7777 != 0o700 {
                return Err(Error::CredentialInvalid);
            }
            Ok(())
        }
        fn initialize_empty(&self, owner: u32) -> Result<(), Error> {
            if !self.names()?.is_empty() {
                return Err(Error::ForeignState);
            }
            set_owner(&self.0, owner)?;
            if unsafe { libc::fchmod(self.0.as_raw_fd(), 0o700) } != 0 {
                return Err(Error::CredentialInvalid);
            }
            self.sync()?;
            self.check(owner)
        }
        fn names(&self) -> Result<BTreeSet<String>, Error> {
            let duplicate = unsafe { libc::dup(self.0.as_raw_fd()) };
            if duplicate < 0 {
                return Err(Error::Incomplete);
            }
            let stream = unsafe { libc::fdopendir(duplicate) };
            if stream.is_null() {
                unsafe {
                    libc::close(duplicate);
                }
                return Err(Error::Incomplete);
            }
            struct Entries(*mut libc::DIR);
            impl Drop for Entries {
                fn drop(&mut self) {
                    unsafe {
                        libc::closedir(self.0);
                    }
                }
            }
            let entries = Entries(stream);
            // A duplicate shares its directory offset; rewind each independent inventory pass.
            unsafe {
                libc::rewinddir(entries.0);
            }
            let mut names = BTreeSet::new();
            loop {
                #[cfg(target_os = "macos")]
                unsafe {
                    *libc::__error() = 0;
                }
                #[cfg(target_os = "linux")]
                unsafe {
                    *libc::__errno_location() = 0;
                }
                let entry = unsafe { libc::readdir(entries.0) };
                if entry.is_null() {
                    if code() != 0 {
                        return Err(Error::Incomplete);
                    }
                    break;
                }
                let value = unsafe { CStr::from_ptr((*entry).d_name.as_ptr()) }
                    .to_str()
                    .map_err(|_| Error::InvalidPath)?;
                if value != "." && value != ".." {
                    names.insert(value.into());
                }
                if names.len() > 64 {
                    return Err(Error::ForeignState);
                }
            }
            Ok(names)
        }
        fn read(&self, leaf: &str, expected: &[u8], owner: u32) -> Result<Option<Vec<u8>>, Error> {
            let leaf = name(leaf)?;
            let fd = unsafe {
                libc::openat(
                    self.0.as_raw_fd(),
                    leaf.as_ptr(),
                    libc::O_RDONLY | libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK,
                )
            };
            if fd < 0 && code() == libc::ENOENT {
                return Ok(None);
            }
            let file = opened(fd)?;
            check_file(
                &file.metadata().map_err(|_| Error::CredentialInvalid)?,
                expected.len(),
                owner,
            )?;
            let mut bytes = Vec::new();
            file.take(expected.len() as u64 + 1)
                .read_to_end(&mut bytes)
                .map_err(|_| Error::CredentialInvalid)?;
            if bytes.len() > expected.len() {
                return Err(Error::ConfigurationDrift);
            }
            Ok(Some(bytes))
        }
        fn remove(&self, leaf: &str) -> Result<(), Error> {
            if unsafe { libc::unlinkat(self.0.as_raw_fd(), name(leaf)?.as_ptr(), 0) } != 0 {
                return Err(Error::Incomplete);
            }
            self.sync()
        }
        fn publish(
            &self,
            leaf: &str,
            temporary: &str,
            bytes: &[u8],
            owner: u32,
        ) -> Result<(), Error> {
            if let Some(current) = self.read(leaf, bytes, owner)? {
                if current != bytes || self.read(temporary, bytes, owner)?.is_some() {
                    return Err(Error::ConfigurationDrift);
                }
                return Ok(());
            }
            if let Some(current) = self.read(temporary, bytes, owner)? {
                if current != bytes {
                    if !bytes.starts_with(&current) {
                        return Err(Error::ConfigurationDrift);
                    }
                    // Only this journal's pending destination permits this exact prefix temp.
                    self.remove(temporary)?;
                }
            }
            if self.read(temporary, bytes, owner)?.is_none() {
                let mut file = opened(unsafe {
                    libc::openat(
                        self.0.as_raw_fd(),
                        name(temporary)?.as_ptr(),
                        libc::O_WRONLY
                            | libc::O_CREAT
                            | libc::O_EXCL
                            | libc::O_NOFOLLOW
                            | libc::O_CLOEXEC,
                        0o600,
                    )
                })?;
                set_owner(&file, owner)?;
                check_file(
                    &file.metadata().map_err(|_| Error::CredentialInvalid)?,
                    bytes.len(),
                    owner,
                )?;
                file.write_all(bytes)
                    .and_then(|()| file.sync_all())
                    .map_err(|_| Error::Incomplete)?;
                self.sync()?;
            }
            let source = name(temporary)?;
            let target = name(leaf)?;
            #[cfg(target_os = "macos")]
            let result = unsafe {
                libc::renameatx_np(
                    self.0.as_raw_fd(),
                    source.as_ptr(),
                    self.0.as_raw_fd(),
                    target.as_ptr(),
                    libc::RENAME_EXCL,
                )
            };
            #[cfg(target_os = "linux")]
            let result = unsafe {
                libc::renameat2(
                    self.0.as_raw_fd(),
                    source.as_ptr(),
                    self.0.as_raw_fd(),
                    target.as_ptr(),
                    libc::RENAME_NOREPLACE,
                )
            };
            #[cfg(not(any(target_os = "linux", target_os = "macos")))]
            let result = -1;
            if result != 0 {
                return Err(Error::Conflict);
            }
            self.sync()
        }
    }
    fn set_owner(file: &File, owner: u32) -> Result<(), Error> {
        let effective = unsafe { libc::geteuid() };
        let group = if effective == 0 {
            owner
        } else {
            unsafe { libc::getegid() }
        };
        if unsafe { libc::fchown(file.as_raw_fd(), owner, group) } != 0 {
            return Err(Error::CredentialInvalid);
        }
        Ok(())
    }
    fn check_file(metadata: &Metadata, maximum: usize, owner: u32) -> Result<(), Error> {
        if !metadata.is_file()
            || metadata.nlink() != 1
            || metadata.uid() != owner
            || metadata.mode() & 0o7777 != 0o600
            || metadata.len() > maximum as u64
        {
            return Err(Error::CredentialInvalid);
        }
        Ok(())
    }
    fn temporary(bytes: &[u8]) -> String {
        format!(
            ".installation-{}.tmp",
            crate::lower_hex(&Sha256::digest(bytes))
        )
    }
    fn parent(relative: &str) -> (&str, &str) {
        relative.rsplit_once('/').unwrap_or(("", relative))
    }
    fn candidate(role: &Directory, relative: &str) -> Result<Directory, Error> {
        let (parent, _) = parent(relative);
        if parent.is_empty() {
            return Ok(Directory(opened(unsafe { libc::dup(role.0.as_raw_fd()) })?));
        }
        role.child(parent)?.ok_or(Error::Incomplete)
    }
    fn check_role(
        root: &Directory,
        name: &str,
        role: &Role<'_>,
        pending: Option<&Pending>,
        complete: bool,
    ) -> Result<(), Error> {
        let Some(directory) = root.child(name)? else {
            return if complete {
                Err(Error::Incomplete)
            } else {
                Ok(())
            };
        };
        let actual = directory.names()?;
        if !actual.is_empty() || complete {
            directory.check(role.owner)?;
        }
        let mut by_parent: BTreeMap<&str, BTreeSet<String>> = BTreeMap::new();
        for relative in role.files.keys() {
            let (parent, leaf) = parent(relative);
            by_parent.entry(parent).or_default().insert(leaf.into());
        }
        let mut root_names = by_parent.get("").cloned().unwrap_or_default();
        root_names.extend(
            by_parent
                .keys()
                .filter(|parent| !parent.is_empty())
                .map(|parent| (*parent).into()),
        );
        if let Some(pending) = pending {
            if let Some(relative) = pending.destination.strip_prefix(&format!("{name}/")) {
                let (parent_name, _) = parent(relative);
                let bytes = role.files.get(relative).ok_or(Error::IdentityDrift)?;
                if parent_name.is_empty() {
                    root_names.insert(temporary(bytes));
                } else {
                    by_parent
                        .entry(parent_name)
                        .or_default()
                        .insert(temporary(bytes));
                }
                if parent_name.is_empty() || directory.child(parent_name)?.is_some() {
                    let child = candidate(&directory, relative)?;
                    let (_, leaf) = parent(relative);
                    if let Some(current) = child.read(&temporary(bytes), bytes, role.owner)? {
                        if !bytes.starts_with(&current)
                            || child.read(leaf, bytes, role.owner)?.is_some()
                        {
                            return Err(Error::ConfigurationDrift);
                        }
                    }
                }
            }
        }
        if !actual.is_subset(&root_names) {
            return Err(Error::ForeignState);
        }
        for (parent, expected_names) in by_parent {
            if parent.is_empty() {
                continue;
            }
            let Some(child) = directory.child(parent)? else {
                if complete {
                    return Err(Error::Incomplete);
                }
                continue;
            };
            child.check(role.owner)?;
            if !child.names()?.is_subset(&expected_names) {
                return Err(Error::ForeignState);
            }
        }
        for (relative, expected) in &role.files {
            let (parent_name, leaf) = parent(relative);
            let child = if parent_name.is_empty() {
                candidate(&directory, relative)?
            } else {
                let Some(child) = directory.child(parent_name)? else {
                    continue;
                };
                child
            };
            match child.read(leaf, expected, role.owner)? {
                Some(actual) if actual == *expected => (),
                Some(_) => return Err(Error::ConfigurationDrift),
                None if complete => return Err(Error::Incomplete),
                None => (),
            }
        }
        Ok(())
    }

    pub(super) fn publish(
        input: &InstallationInputV1,
        identity: &InstallationIdentityV1,
        rendered: &RenderedInstallationFiles,
        private: &InstallationDirectory,
        output_root: &Path,
        mode: RoleOutputMode,
        ownership: RoleOutputOwnership,
    ) -> Result<(), Error> {
        let roles = inventory(input, identity, rendered, ownership)?;
        let publication = Publication {
            input,
            identity,
            private,
            root: output_root,
            mode,
            ownership,
            journal: JOURNAL,
            source_digest: canonical(&rendered.evidence)?,
        };
        publish_files(&publication, &roles)
    }
    pub(super) fn publish_files(
        publication: &Publication<'_>,
        roles: &BTreeMap<String, Role<'_>>,
    ) -> Result<(), Error> {
        let input = publication.input;
        let identity = publication.identity;
        let private = publication.private;
        let output_root = publication.root;
        let mode = publication.mode.clone();
        let ownership = publication.ownership;
        if matches!(mode, RoleOutputMode::Upgrade | RoleOutputMode::Rollout(_)) {
            return upgrade_files(publication, roles);
        }
        validate_private_identity(input, identity, private)?;
        if ownership == RoleOutputOwnership::NativeCurrentUser
            && input.network.topology != InstallationTopology::Native
        {
            return Err(Error::UnsupportedTopology);
        }
        let files = roles
            .iter()
            .map(|(name, role)| {
                (
                    name,
                    role.files
                        .iter()
                        .map(|(relative, bytes)| (relative, bytes_digest(bytes)))
                        .collect::<BTreeMap<_, _>>(),
                )
            })
            .collect::<BTreeMap<_, _>>();
        let expected = Journal {
            schema_version: 1,
            input_digest: input.digest()?,
            identity_digest: identity.digest()?,
            source_digest: publication.source_digest.clone(),
            files_digest: canonical(&files)?,
            output_root: output_root.to_str().ok_or(Error::InvalidPath)?.into(),
            ownership,
            complete: false,
            pending: None,
        };
        if expected.output_root.len() > 1024 || output_root == Path::new("/") {
            return Err(Error::InvalidPath);
        }
        let root = Directory::absolute(output_root)?;
        if !root.names()?.is_subset(&roles.keys().cloned().collect()) {
            return Err(Error::ForeignState);
        }
        let mut journal: Journal = match private
            .read(publication.journal, JOURNAL_LIMITS.max_bytes)?
        {
            Some(bytes) => serde_json::from_value(
                parse_strict_json(&bytes, JOURNAL_LIMITS).map_err(|_| Error::InvalidInput)?,
            )
            .map_err(|_| Error::InvalidInput)?,
            None if mode == RoleOutputMode::Verify => return Err(Error::Incomplete),
            None => {
                for name in roles.keys() {
                    if root.child(name)?.is_some_and(|directory| {
                        directory
                            .names()
                            .map_or(true, |entries| !entries.is_empty())
                    }) {
                        return Err(Error::ForeignState);
                    }
                }
                if ownership == RoleOutputOwnership::ServingUsers && unsafe { libc::geteuid() } != 0
                {
                    return Err(Error::CredentialInvalid);
                }
                expected.save(private, publication.journal)?;
                expected.clone()
            }
        };
        if journal.schema_version != 1
            || journal.input_digest != input.digest()?
            || journal.identity_digest != identity.digest()?
            || journal.source_digest != expected.source_digest
            || journal.files_digest != expected.files_digest
            || journal.output_root != output_root.to_str().ok_or(Error::InvalidPath)?
            || journal.ownership != ownership
            || (journal.complete && journal.pending.is_some())
        {
            return Err(Error::IdentityDrift);
        }
        if let Some(pending) = &journal.pending {
            let (role, relative) = pending
                .destination
                .split_once('/')
                .ok_or(Error::IdentityDrift)?;
            let bytes = roles
                .get(role)
                .and_then(|role| role.files.get(relative))
                .ok_or(Error::IdentityDrift)?;
            if bytes_digest(bytes) != pending.bytes_digest {
                return Err(Error::IdentityDrift);
            }
        }
        if mode == RoleOutputMode::Verify && !journal.complete {
            return Err(Error::Incomplete);
        }
        for (name, role) in roles {
            check_role(
                &root,
                name,
                role,
                journal.pending.as_ref(),
                journal.complete,
            )?;
        }
        if journal.complete {
            return Ok(());
        }
        if ownership == RoleOutputOwnership::ServingUsers && unsafe { libc::geteuid() } != 0 {
            return Err(Error::CredentialInvalid);
        }
        for (name, role) in roles {
            let directory = match root.child(name)? {
                Some(directory) => {
                    if directory.names()?.is_empty() {
                        directory.initialize_empty(role.owner)?;
                    } else {
                        directory.check(role.owner)?;
                    }
                    directory
                }
                None => root.create(name, role.owner)?,
            };
            for subdirectory in role
                .files
                .keys()
                .filter_map(|relative| relative.split_once('/').map(|(parent, _)| parent))
                .collect::<BTreeSet<_>>()
            {
                if let Some(child) = directory.child(subdirectory)? {
                    child.check(role.owner)?;
                } else {
                    directory.create(subdirectory, role.owner)?;
                }
            }
            for (relative, bytes) in &role.files {
                let destination = format!("{name}/{relative}");
                // A pending temp must finish before another destination can replace its intent.
                if journal
                    .pending
                    .as_ref()
                    .is_some_and(|pending| pending.destination != destination)
                {
                    continue;
                }
                let child = candidate(&directory, relative)?;
                let (_, leaf) = parent(relative);
                if child.read(leaf, bytes, role.owner)?.is_some() {
                    if journal.pending.is_some() {
                        child.publish(leaf, &temporary(bytes), bytes, role.owner)?;
                        journal.pending = None;
                        journal.save(private, publication.journal)?;
                    }
                    continue;
                }
                journal.pending = Some(Pending {
                    destination,
                    bytes_digest: bytes_digest(bytes),
                });
                journal.save(private, publication.journal)?;
                child.publish(leaf, &temporary(bytes), bytes, role.owner)?;
                journal.pending = None;
                journal.save(private, publication.journal)?;
            }
        }
        // A resumed pending destination may sort after other missing files. Make a second bounded
        // pass under the same journal, then require the complete exact inventory before success.
        let missing = roles.iter().try_fold(false, |missing, (name, role)| {
            match check_role(&root, name, role, None, true) {
                Ok(()) => Ok(missing),
                Err(Error::Incomplete) => Ok(true),
                Err(error) => Err(error),
            }
        })?;
        if missing {
            return publish_files(publication, roles);
        }
        journal.complete = true;
        journal.save(private, publication.journal)
    }

    pub(super) fn initialize_data(
        input: &InstallationInputV1,
        identity: &InstallationIdentityV1,
        private: &InstallationDirectory,
        path: &Path,
        mode: RoleOutputMode,
        ownership: RoleOutputOwnership,
        kind: DependencyDataDirectory,
    ) -> Result<(), Error> {
        validate_private_identity(input, identity, private)?;
        if ownership == RoleOutputOwnership::NativeCurrentUser
            && input.network.topology != InstallationTopology::Native
        {
            return Err(Error::UnsupportedTopology);
        }
        let encoded_path = path.to_str().ok_or(Error::InvalidPath)?;
        if encoded_path.len() > 1024 || path == Path::new("/") {
            return Err(Error::InvalidPath);
        }
        let directory = Directory::absolute(path)?;
        let owner = if matches!(kind, DependencyDataDirectory::Postgres)
            && ownership == RoleOutputOwnership::ServingUsers
        {
            999
        } else {
            target_uid(ownership, false)
        };
        let journal_name = kind.journal();
        let save = |journal: &DataDirectoryJournal| {
            private.replace(
                journal_name,
                &serde_json::to_vec(journal).map_err(|_| Error::InvalidInput)?,
            )
        };
        let mut journal: DataDirectoryJournal = match private
            .read(journal_name, JOURNAL_LIMITS.max_bytes)?
        {
            Some(bytes) => serde_json::from_value(
                parse_strict_json(&bytes, JOURNAL_LIMITS).map_err(|_| Error::InvalidInput)?,
            )
            .map_err(|_| Error::InvalidInput)?,
            None if mode == RoleOutputMode::Verify => return Err(Error::Incomplete),
            None => {
                if !directory.names()?.is_empty() {
                    return Err(Error::ForeignState);
                }
                if ownership == RoleOutputOwnership::ServingUsers && unsafe { libc::geteuid() } != 0
                {
                    return Err(Error::CredentialInvalid);
                }
                let journal = DataDirectoryJournal {
                    schema_version: 1,
                    input_digest: input.digest()?,
                    identity_digest: identity.digest()?,
                    directory: encoded_path.into(),
                    ownership,
                    owner,
                    complete: false,
                };
                save(&journal)?;
                journal
            }
        };
        if journal.schema_version != 1
            || journal.input_digest != input.digest()?
            || journal.identity_digest != identity.digest()?
            || journal.directory != encoded_path
            || journal.ownership != ownership
            || journal.owner != owner
        {
            return Err(Error::IdentityDrift);
        }
        if journal.complete {
            return directory.check(owner);
        }
        if mode == RoleOutputMode::Verify {
            return Err(Error::Incomplete);
        }
        if ownership == RoleOutputOwnership::ServingUsers && unsafe { libc::geteuid() } != 0 {
            return Err(Error::CredentialInvalid);
        }
        directory.initialize_empty(owner)?;
        journal.complete = true;
        save(&journal)
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use crate::{
        installation::{compose_input, PreparedInstallation},
        renderer::RenderedProcessFiles,
    };
    use insight_platform_deployment_contracts::installation::{
        RenderedCredentialFileV1, RenderedInstallationProcessV1, RenderedInstallationV1,
    };
    use std::{
        fs,
        os::unix::fs::{MetadataExt as _, PermissionsExt as _},
        path::PathBuf,
    };

    struct Fixture {
        _temp: tempfile::TempDir,
        input: InstallationInputV1,
        prepared: PreparedInstallation,
        rendered: RenderedInstallationFiles,
        root: PathBuf,
    }
    impl Fixture {
        fn authorize_upgrade(&self) {
            use insight_platform_deployment_contracts::installation_release::*;
            let release = InstallationReleaseV1 {
                schema_version: 1,
                installation_id: self.prepared.identity().installation_id.clone(),
                bootstrap_input_digest: self.input.digest().unwrap(),
                bootstrap_identity_digest: self.prepared.identity().digest().unwrap(),
                from_package_digest: self.input.package_digest.clone(),
                to_package_digest: format!("sha256:{}", "b".repeat(64)).parse().unwrap(),
                from_schema_version: SOURCE_SCHEMA_VERSION,
                to_schema_version: TARGET_SCHEMA_VERSION,
                from_inventory_digest: SOURCE_INVENTORY_DIGEST.parse().unwrap(),
                to_inventory_digest: format!("sha256:{}", "c".repeat(64)).parse().unwrap(),
            };
            self.prepared
                .directory()
                .write_immutable(UPGRADE_INTENT_FILE, &serde_json::to_vec(&release).unwrap())
                .unwrap();
        }
        fn change_environment(&mut self) -> (String, Vec<u8>, Vec<u8>) {
            let (process, role) = self.rendered.roles.first_key_value().unwrap();
            let process = *process;
            let old = role.environment.clone();
            let new = b"PLATFORM_FIXTURE='upgraded'\n".to_vec();
            self.rendered.roles.get_mut(&process).unwrap().environment = new.clone();
            self.rendered
                .evidence
                .processes
                .iter_mut()
                .find(|r| r.process == process)
                .unwrap()
                .environment_bytes_digest = bytes_digest(&new);
            (format!("{}/environment", process.name()), old, new)
        }
        fn new() -> Self {
            let temp = tempfile::tempdir().unwrap();
            let root = fs::canonicalize(temp.path()).unwrap();
            let input = compose_input(
                "role-output-test",
                format!("sha256:{}", "a".repeat(64)).parse().unwrap(),
            )
            .unwrap();
            let prepared = PreparedInstallation::prepare(&input, &root.join("private")).unwrap();
            let mut roles = BTreeMap::new();
            let mut processes = Vec::new();
            for entry in &input.network.processes {
                let process = entry.process;
                let mut names = input
                    .credentials
                    .files
                    .iter()
                    .filter(|entry| entry.process == process)
                    .map(|entry| entry.file_name.clone())
                    .collect::<BTreeSet<_>>();
                for (_, identity) in role_material::tls_identities(process) {
                    names.insert(identity.certificate.into());
                    names.insert(tls::RUNTIME_CA_CERTIFICATE_FILE.into());
                }
                if role_material::requires_aws_ca(&input.network, process) {
                    names.insert(tls::RUNTIME_CA_CERTIFICATE_FILE.into());
                }
                if let Some(role) = role_material::openbao_role(&input.network, process) {
                    names.insert(crate::openbao_profile::certificate_file(role));
                }
                let configuration_file = format!("{}.json", process.name());
                let configuration =
                    serde_json::json!({"schema_version":1,"fixture_role":process.name()});
                let environment = format!("PLATFORM_FIXTURE='{}'\n", process.name()).into_bytes();
                // These canaries prove file selection; the writer does not own config/credential semantics.
                let credentials = names
                    .into_iter()
                    .map(|name| {
                        let bytes =
                            format!("private fixture:{}:{name}", process.name()).into_bytes();
                        (name, bytes)
                    })
                    .collect::<BTreeMap<_, _>>();
                processes.push(RenderedInstallationProcessV1 {
                    process,
                    executable_digest: bytes_digest(b"fixture executable"),
                    configuration_file: configuration_file.clone(),
                    configuration_digest: canonical(&configuration).unwrap(),
                    environment_bytes_digest: bytes_digest(&environment),
                    credential_files: credentials
                        .iter()
                        .map(|(name, bytes)| RenderedCredentialFileV1 {
                            file_name: name.clone(),
                            bytes_digest: bytes_digest(bytes),
                        })
                        .collect(),
                });
                roles.insert(
                    process,
                    RenderedProcessFiles {
                        configuration_file,
                        configuration: serde_json::to_vec(&configuration).unwrap(),
                        environment,
                        credentials,
                    },
                );
            }
            let evidence = RenderedInstallationV1 {
                schema_version: 1,
                input_digest: input.digest().unwrap(),
                identity_digest: prepared.identity().digest().unwrap(),
                package_digest: input.package_digest.clone(),
                processes,
            };
            let rendered = RenderedInstallationFiles {
                local_identity: None,
                evidence,
                roles,
                console: br#"{"schema_version":1,"fixture":"console"}"#.to_vec(),
            };
            let output = root.join("outputs");
            fs::create_dir(&output).unwrap();
            Self {
                _temp: temp,
                input,
                prepared,
                rendered,
                root: output,
            }
        }
        fn publish(&self, mode: RoleOutputMode) -> Result<(), Error> {
            publish_role_outputs(
                &self.input,
                self.prepared.identity(),
                &self.rendered,
                self.prepared.directory(),
                &self.root,
                mode,
                RoleOutputOwnership::CurrentUser,
            )
        }
        fn journal(&self) -> Journal {
            serde_json::from_slice(&self.journal_bytes()).unwrap()
        }
        fn journal_bytes(&self) -> Vec<u8> {
            self.prepared
                .directory()
                .read(JOURNAL, JOURNAL_LIMITS.max_bytes)
                .unwrap()
                .unwrap()
        }
        fn chosen(&self) -> (String, Vec<u8>) {
            let (&process, role) = self.rendered.roles.first_key_value().unwrap();
            (
                format!("{}/environment", process.name()),
                role.environment.clone(),
            )
        }
        fn pending(&self, destination: &str, bytes: &[u8]) {
            let mut journal = self.journal();
            journal.complete = false;
            journal.pending = Some(Pending {
                destination: destination.into(),
                bytes_digest: bytes_digest(bytes),
            });
            journal.save(self.prepared.directory(), JOURNAL).unwrap();
        }
    }

    #[test]
    fn explicit_release_updates_exact_outputs_and_recovers_mixed_files() {
        let mut fixture = Fixture::new();
        fixture.publish(RoleOutputMode::Provision).unwrap();
        fixture.authorize_upgrade();
        let (path, old, new) = fixture.change_environment();
        fixture.publish(RoleOutputMode::Upgrade).unwrap();
        assert_eq!(fs::read(fixture.root.join(&path)).unwrap(), new);
        fixture.publish(RoleOutputMode::Verify).unwrap();
        // Replaying the fixed plan accepts a source/target mixture after an interrupted replacement.
        fs::write(fixture.root.join(&path), old).unwrap();
        let pending = fixture
            .root
            .join(&path)
            .parent()
            .unwrap()
            .join(format!(".upgrade-{}", &bytes_digest(&new).as_str()[7..]));
        fs::write(&pending, &new[..8]).unwrap();
        fs::set_permissions(&pending, fs::Permissions::from_mode(0o600)).unwrap();
        fixture.publish(RoleOutputMode::Upgrade).unwrap();
        fixture.publish(RoleOutputMode::Verify).unwrap();
        assert!(!pending.exists());
    }

    #[test]
    fn package_rollout_has_per_release_recovery_plan_and_rejects_unknown_edits() {
        use insight_platform_deployment_contracts::installation_release::*;
        let mut fixture = Fixture::new();
        fixture.publish(RoleOutputMode::Provision).unwrap();
        fixture.authorize_upgrade();
        let bytes = fixture
            .prepared
            .directory()
            .read(UPGRADE_INTENT_FILE, 65536)
            .unwrap()
            .unwrap();
        let mut previous: InstallationReleaseV1 = serde_json::from_slice(&bytes).unwrap();
        fixture
            .prepared
            .directory()
            .write_immutable(RELEASE_FILE, &bytes)
            .unwrap();
        for package in ['d', 'e'] {
            let mut target = previous.clone();
            target.to_package_digest = format!("sha256:{}", package.to_string().repeat(64))
                .parse()
                .unwrap();
            let intent = PackageRolloutIntentV1 {
                schema_version: 1,
                expected_previous_release_digest: previous.canonical_digest().unwrap(),
                previous_release: previous.clone(),
                target_release: target.clone(),
            };
            let digest = target.canonical_digest().unwrap();
            fixture
                .prepared
                .directory()
                .write_immutable(
                    &PackageRolloutIntentV1::filename(&digest),
                    &serde_json::to_vec(&intent).unwrap(),
                )
                .unwrap();
            let (path, old, new) = fixture.change_environment();
            fixture
                .publish(RoleOutputMode::Rollout(digest.clone()))
                .unwrap();
            fs::write(fixture.root.join(&path), old).unwrap();
            fixture
                .publish(RoleOutputMode::Rollout(digest.clone()))
                .unwrap();
            fs::write(fixture.root.join(&path), b"operator changed").unwrap();
            assert!(fixture
                .publish(RoleOutputMode::Rollout(digest.clone()))
                .is_err());
            assert_eq!(
                fs::read(fixture.root.join(&path)).unwrap(),
                b"operator changed"
            );
            fs::write(fixture.root.join(&path), new).unwrap();
            fixture.publish(RoleOutputMode::Rollout(digest)).unwrap();
            fixture
                .prepared
                .directory()
                .replace(RELEASE_FILE, &serde_json::to_vec(&target).unwrap())
                .unwrap();
            previous = target;
        }
    }

    #[test]
    fn upgrade_refuses_unreviewed_file_changes_and_missing_authorization() {
        let mut fixture = Fixture::new();
        fixture.publish(RoleOutputMode::Provision).unwrap();
        let (path, _, _) = fixture.change_environment();
        assert!(fixture.publish(RoleOutputMode::Upgrade).is_err());
        fixture.authorize_upgrade();
        fs::write(fixture.root.join(&path), b"operator edited this").unwrap();
        assert!(fixture.publish(RoleOutputMode::Upgrade).is_err());
        assert_eq!(
            fs::read(fixture.root.join(path)).unwrap(),
            b"operator edited this"
        );
    }
    fn private_file(path: &Path, bytes: &[u8]) {
        fs::write(path, bytes).unwrap();
        fs::set_permissions(path, fs::Permissions::from_mode(0o600)).unwrap();
    }
    fn temp_path(final_path: &Path, bytes: &[u8]) -> PathBuf {
        final_path.parent().unwrap().join(format!(
            ".installation-{}.tmp",
            crate::lower_hex(&Sha256::digest(bytes))
        ))
    }

    #[test]
    fn exact_role_publication_and_completed_verification_are_private_and_readonly() {
        let fixture = Fixture::new();
        fixture.publish(RoleOutputMode::Provision).unwrap();
        let expected = inventory(
            &fixture.input,
            fixture.prepared.identity(),
            &fixture.rendered,
            RoleOutputOwnership::CurrentUser,
        )
        .unwrap();
        for (name, role) in expected {
            let root = fixture.root.join(name);
            assert_eq!(fs::metadata(&root).unwrap().mode() & 0o777, 0o700);
            for (relative, bytes) in role.files {
                let path = root.join(relative);
                assert_eq!(fs::read(&path).unwrap(), bytes);
                let metadata = fs::metadata(path).unwrap();
                assert_eq!(metadata.mode() & 0o777, 0o600);
                assert_eq!(metadata.uid(), unsafe { libc::geteuid() });
                assert_eq!(metadata.nlink(), 1);
            }
        }
        assert_eq!(
            fs::read_dir(fixture.root.join("console")).unwrap().count(),
            1
        );
        let (chosen, _) = fixture.chosen();
        let before = fs::metadata(fixture.root.join(chosen)).unwrap();
        let journal = fixture.journal_bytes();
        fixture.publish(RoleOutputMode::Verify).unwrap();
        fixture.publish(RoleOutputMode::Provision).unwrap();
        assert_eq!(journal, fixture.journal_bytes());
        let (chosen, _) = fixture.chosen();
        let after = fs::metadata(fixture.root.join(chosen)).unwrap();
        assert_eq!(before.ino(), after.ino());
        assert_eq!(before.mtime(), after.mtime());
        assert!(fixture.journal().complete);
    }

    #[test]
    fn verify_without_intent_does_not_create_any_role_or_journal() {
        let fixture = Fixture::new();
        assert_eq!(
            fixture.publish(RoleOutputMode::Verify),
            Err(Error::Incomplete)
        );
        assert_eq!(fs::read_dir(&fixture.root).unwrap().count(), 0);
        assert!(fixture
            .prepared
            .directory()
            .read(JOURNAL, 65536)
            .unwrap()
            .is_none());
        assert_eq!(
            publish_role_outputs(
                &fixture.input,
                fixture.prepared.identity(),
                &fixture.rendered,
                fixture.prepared.directory(),
                &fixture.root,
                RoleOutputMode::Provision,
                RoleOutputOwnership::NativeCurrentUser
            ),
            Err(Error::UnsupportedTopology)
        );
    }

    #[test]
    fn initial_nonempty_volume_is_never_adopted_even_when_bytes_match() {
        let fixture = Fixture::new();
        let (chosen, bytes) = fixture.chosen();
        let path = fixture.root.join(chosen);
        fs::create_dir(path.parent().unwrap()).unwrap();
        private_file(&path, &bytes);
        assert_eq!(
            fixture.publish(RoleOutputMode::Provision),
            Err(Error::ForeignState)
        );
        assert!(fixture
            .prepared
            .directory()
            .read(JOURNAL, 65536)
            .unwrap()
            .is_none());
        assert_eq!(fs::read(path).unwrap(), bytes);
    }

    #[test]
    fn renderer_hashes_and_credential_scope_are_checked_before_writes() {
        let mut fixture = Fixture::new();
        fixture
            .rendered
            .roles
            .first_entry()
            .unwrap()
            .get_mut()
            .environment
            .push(b'x');
        assert_eq!(
            fixture.publish(RoleOutputMode::Provision),
            Err(Error::ConfigurationDrift)
        );
        assert_eq!(fs::read_dir(&fixture.root).unwrap().count(), 0);
        let mut fixture = Fixture::new();
        let (&process, _) = fixture.rendered.roles.first_key_value().unwrap();
        let bytes = b"another role private signing key";
        fixture
            .rendered
            .roles
            .get_mut(&process)
            .unwrap()
            .credentials
            .insert("installation-signing-key.pem".into(), bytes.to_vec());
        fixture
            .rendered
            .evidence
            .processes
            .iter_mut()
            .find(|entry| entry.process == process)
            .unwrap()
            .credential_files
            .push(RenderedCredentialFileV1 {
                file_name: "installation-signing-key.pem".into(),
                bytes_digest: bytes_digest(bytes),
            });
        assert_eq!(
            fixture.publish(RoleOutputMode::Provision),
            Err(Error::CredentialInvalid)
        );
        assert_eq!(fs::read_dir(&fixture.root).unwrap().count(), 0);
    }

    #[test]
    fn completed_byte_and_permission_drift_are_retained_without_repair() {
        let fixture = Fixture::new();
        fixture.publish(RoleOutputMode::Provision).unwrap();
        let (chosen, bytes) = fixture.chosen();
        let path = fixture.root.join(chosen);
        let journal = fixture.journal_bytes();
        private_file(&path, b"changed file");
        assert_eq!(
            fixture.publish(RoleOutputMode::Verify),
            Err(Error::ConfigurationDrift)
        );
        assert_eq!(fs::read(&path).unwrap(), b"changed file");
        private_file(&path, &bytes);
        fs::set_permissions(&path, fs::Permissions::from_mode(0o640)).unwrap();
        assert_eq!(
            fixture.publish(RoleOutputMode::Provision),
            Err(Error::CredentialInvalid)
        );
        assert_eq!(fs::metadata(&path).unwrap().mode() & 0o777, 0o640);
        assert_eq!(journal, fixture.journal_bytes());
    }

    #[test]
    fn symlink_hardlink_and_extra_files_are_rejected_without_following_them() {
        let fixture = Fixture::new();
        fixture.publish(RoleOutputMode::Provision).unwrap();
        let (chosen, bytes) = fixture.chosen();
        let path = fixture.root.join(chosen);
        let outside = fixture.root.parent().unwrap().join("outside");
        private_file(&outside, b"outside unchanged");
        fs::remove_file(&path).unwrap();
        std::os::unix::fs::symlink(&outside, &path).unwrap();
        assert_eq!(
            fixture.publish(RoleOutputMode::Verify),
            Err(Error::CredentialInvalid)
        );
        assert_eq!(fs::read(&outside).unwrap(), b"outside unchanged");
        fs::remove_file(&path).unwrap();
        private_file(&path, &bytes);
        let alias = fixture.root.parent().unwrap().join("hardlink");
        fs::hard_link(&path, &alias).unwrap();
        assert_eq!(
            fixture.publish(RoleOutputMode::Verify),
            Err(Error::CredentialInvalid)
        );
        fs::remove_file(alias).unwrap();
        private_file(&fixture.root.join("console/unexpected-key"), b"unexpected");
        assert_eq!(
            fixture.publish(RoleOutputMode::Verify),
            Err(Error::ForeignState)
        );
    }

    #[test]
    fn parent_directory_symlink_and_wrong_private_identity_are_rejected() {
        let fixture = Fixture::new();
        let link = fixture.root.parent().unwrap().join("output-link");
        std::os::unix::fs::symlink(&fixture.root, &link).unwrap();
        assert_eq!(
            publish_role_outputs(
                &fixture.input,
                fixture.prepared.identity(),
                &fixture.rendered,
                fixture.prepared.directory(),
                &link,
                RoleOutputMode::Provision,
                RoleOutputOwnership::CurrentUser
            ),
            Err(Error::CredentialInvalid)
        );
        fixture.publish(RoleOutputMode::Provision).unwrap();
        let mut journal = fixture.journal();
        journal.identity_digest = bytes_digest(b"foreign identity");
        journal.save(fixture.prepared.directory(), JOURNAL).unwrap();
        assert_eq!(
            fixture.publish(RoleOutputMode::Verify),
            Err(Error::IdentityDrift)
        );
    }

    #[test]
    fn only_recorded_complete_or_prefix_temporary_file_can_resume() {
        for prefix in [false, true] {
            let fixture = Fixture::new();
            fixture.publish(RoleOutputMode::Provision).unwrap();
            let (chosen, bytes) = fixture.chosen();
            let path = fixture.root.join(&chosen);
            fs::remove_file(&path).unwrap();
            fixture.pending(&chosen, &bytes);
            let temporary = temp_path(&path, &bytes);
            private_file(&temporary, if prefix { &bytes[..5] } else { &bytes });
            assert_eq!(
                fixture.publish(RoleOutputMode::Verify),
                Err(Error::Incomplete)
            );
            assert!(!path.exists());
            fixture.publish(RoleOutputMode::Provision).unwrap();
            assert_eq!(fs::read(&path).unwrap(), bytes);
            assert!(!temporary.exists());
            assert!(fixture.journal().complete);
        }
    }

    #[test]
    fn incomplete_intent_finishes_missing_files_and_retains_invalid_temp() {
        let fixture = Fixture::new();
        fixture.publish(RoleOutputMode::Provision).unwrap();
        let (chosen, bytes) = fixture.chosen();
        let path = fixture.root.join(&chosen);
        fs::remove_file(&path).unwrap();
        // Console sorts later than several roles: recovery must finish its pending temp first,
        // then revisit missing earlier files in one bounded additional pass.
        let console = fixture.root.join("console/config.json");
        fs::remove_file(&console).unwrap();
        fixture.pending("console/config.json", &fixture.rendered.console);
        let temporary = temp_path(&console, &fixture.rendered.console);
        private_file(&temporary, b"unowned bytes");
        assert_eq!(
            fixture.publish(RoleOutputMode::Provision),
            Err(Error::ConfigurationDrift)
        );
        assert!(!path.exists());
        assert_eq!(fs::read(&temporary).unwrap(), b"unowned bytes");
        private_file(&temporary, &fixture.rendered.console[..5]);
        fixture.publish(RoleOutputMode::Provision).unwrap();
        assert_eq!(fs::read(path).unwrap(), bytes);
        assert_eq!(fs::read(console).unwrap(), fixture.rendered.console);
        assert!(!temporary.exists());
    }

    #[test]
    fn dependency_outputs_have_only_owned_material_and_verify_readonly() {
        let fixture = Fixture::new();
        let publish = |mode| {
            publish_dependency_outputs(
                &fixture.input,
                fixture.prepared.identity(),
                fixture.prepared.directory(),
                &fixture.root,
                mode,
                RoleOutputOwnership::CurrentUser,
            )
        };
        publish(RoleOutputMode::Provision).unwrap();
        let actual = fs::read_dir(&fixture.root)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().into_string().unwrap())
            .collect::<BTreeSet<_>>();
        assert_eq!(
            actual,
            BTreeSet::from([
                "nats".into(),
                "postgres".into(),
                "s3".into(),
                "openbao".into()
            ])
        );
        for (output, source) in [
            ("postgres/admin-password", "postgres-admin-password"),
            ("nats/tls/ca.pem", tls::RUNTIME_CA_CERTIFICATE_FILE),
            (
                "nats/tls/server.pem",
                tls::RUNTIME_NATS_SERVER_CERTIFICATE_FILE,
            ),
            (
                "nats/tls/server-key.pem",
                tls::RUNTIME_NATS_SERVER_PRIVATE_KEY_FILE,
            ),
        ] {
            let path = fixture.root.join(output);
            assert!(
                fs::read(&path).unwrap()
                    == fixture
                        .prepared
                        .directory()
                        .read(source, INSTALLATION_MAX_BYTES)
                        .unwrap()
                        .unwrap()
            );
            let metadata = fs::metadata(path).unwrap();
            assert_eq!(metadata.mode() & 0o777, 0o600);
            assert_eq!(metadata.uid(), unsafe { libc::geteuid() });
        }
        assert_eq!(
            fs::read(fixture.root.join("nats/nats.conf")).unwrap(),
            NATS_CONFIGURATION
        );
        assert!(fixture
            .prepared
            .directory()
            .read(JOURNAL, 65536)
            .unwrap()
            .is_none());
        let before = fixture
            .prepared
            .directory()
            .read(DEPENDENCY_JOURNAL, 65536)
            .unwrap()
            .unwrap();
        publish(RoleOutputMode::Verify).unwrap();
        publish(RoleOutputMode::Provision).unwrap();
        assert_eq!(
            before,
            fixture
                .prepared
                .directory()
                .read(DEPENDENCY_JOURNAL, 65536)
                .unwrap()
                .unwrap()
        );
        private_file(
            &fixture.root.join("postgres/issuer-key.pem"),
            b"wrong scope",
        );
        assert_eq!(publish(RoleOutputMode::Verify), Err(Error::ForeignState));
    }

    #[test]
    fn dependency_key_prefix_recovery_uses_the_same_exact_atomic_writer() {
        let fixture = Fixture::new();
        let publish = |mode| {
            publish_dependency_outputs(
                &fixture.input,
                fixture.prepared.identity(),
                fixture.prepared.directory(),
                &fixture.root,
                mode,
                RoleOutputOwnership::CurrentUser,
            )
        };
        publish(RoleOutputMode::Provision).unwrap();
        let destination = "nats/tls/server-key.pem";
        let expected = fs::read(fixture.root.join(destination)).unwrap();
        fs::remove_file(fixture.root.join(destination)).unwrap();
        let encoded = fixture
            .prepared
            .directory()
            .read(DEPENDENCY_JOURNAL, 65536)
            .unwrap()
            .unwrap();
        let mut journal: Journal = serde_json::from_slice(&encoded).unwrap();
        journal.complete = false;
        journal.pending = Some(Pending {
            destination: destination.into(),
            bytes_digest: bytes_digest(&expected),
        });
        journal
            .save(fixture.prepared.directory(), DEPENDENCY_JOURNAL)
            .unwrap();
        let temporary = temp_path(&fixture.root.join(destination), &expected);
        private_file(&temporary, &expected[..8]);
        publish(RoleOutputMode::Provision).unwrap();
        assert!(fs::read(fixture.root.join(destination)).unwrap() == expected);
        assert!(!temporary.exists());
    }

    #[test]
    fn nats_data_requires_empty_initial_volume_and_readonly_checks_never_walk_data() {
        let fixture = Fixture::new();
        let initialize = |mode| {
            initialize_nats_data_directory(
                &fixture.input,
                fixture.prepared.identity(),
                fixture.prepared.directory(),
                &fixture.root,
                mode,
                RoleOutputOwnership::CurrentUser,
            )
        };
        assert_eq!(initialize(RoleOutputMode::Verify), Err(Error::Incomplete));
        private_file(&fixture.root.join("foreign-state"), b"retain");
        assert_eq!(
            initialize(RoleOutputMode::Provision),
            Err(Error::ForeignState)
        );
        assert!(fixture
            .prepared
            .directory()
            .read(NATS_DATA_JOURNAL, 65536)
            .unwrap()
            .is_none());
        fs::remove_file(fixture.root.join("foreign-state")).unwrap();
        initialize(RoleOutputMode::Provision).unwrap();
        let before = fixture
            .prepared
            .directory()
            .read(NATS_DATA_JOURNAL, 65536)
            .unwrap()
            .unwrap();
        let live = fixture.root.join("jetstream-owned");
        fs::create_dir(&live).unwrap();
        fs::set_permissions(&live, fs::Permissions::from_mode(0o000)).unwrap();
        std::os::unix::fs::symlink(
            "/nonexistent-nats-owned-entry",
            fixture.root.join("live-entry"),
        )
        .unwrap();
        initialize(RoleOutputMode::Verify).unwrap();
        initialize(RoleOutputMode::Provision).unwrap();
        assert_eq!(fs::metadata(&live).unwrap().mode() & 0o777, 0o000);
        assert_eq!(
            before,
            fixture
                .prepared
                .directory()
                .read(NATS_DATA_JOURNAL, 65536)
                .unwrap()
                .unwrap()
        );
        fs::set_permissions(&live, fs::Permissions::from_mode(0o700)).unwrap();
        fs::set_permissions(&fixture.root, fs::Permissions::from_mode(0o755)).unwrap();
        assert_eq!(
            initialize(RoleOutputMode::Provision),
            Err(Error::CredentialInvalid)
        );
        assert_eq!(fs::metadata(&fixture.root).unwrap().mode() & 0o777, 0o755);
    }

    #[test]
    fn nats_data_requested_recovery_never_changes_nonempty_unknown_data() {
        let fixture = Fixture::new();
        let initialize = |mode| {
            initialize_nats_data_directory(
                &fixture.input,
                fixture.prepared.identity(),
                fixture.prepared.directory(),
                &fixture.root,
                mode,
                RoleOutputOwnership::CurrentUser,
            )
        };
        initialize(RoleOutputMode::Provision).unwrap();
        let mut journal: DataDirectoryJournal = serde_json::from_slice(
            &fixture
                .prepared
                .directory()
                .read(NATS_DATA_JOURNAL, 65536)
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        journal.complete = false;
        fixture
            .prepared
            .directory()
            .replace(NATS_DATA_JOURNAL, &serde_json::to_vec(&journal).unwrap())
            .unwrap();
        fs::set_permissions(&fixture.root, fs::Permissions::from_mode(0o755)).unwrap();
        private_file(&fixture.root.join("unknown"), b"retain data");
        assert_eq!(
            initialize(RoleOutputMode::Provision),
            Err(Error::ForeignState)
        );
        assert_eq!(fs::metadata(&fixture.root).unwrap().mode() & 0o777, 0o755);
        assert_eq!(
            fs::read(fixture.root.join("unknown")).unwrap(),
            b"retain data"
        );
        fs::remove_file(fixture.root.join("unknown")).unwrap();
        initialize(RoleOutputMode::Provision).unwrap();
        assert_eq!(fs::metadata(&fixture.root).unwrap().mode() & 0o777, 0o700);
    }

    #[test]
    fn kubernetes_dependency_certificate_is_scoped_and_verify_preserves_all_bytes() {
        let temporary = tempfile::tempdir().unwrap();
        let root = fs::canonicalize(temporary.path()).unwrap();
        let input = crate::kubernetes::kubernetes_input(
            "tls-test",
            format!("sha256:{}", "a".repeat(64)).parse().unwrap(),
        )
        .unwrap();
        let prepared = PreparedInstallation::prepare(&input, &root.join("private")).unwrap();
        let directory = root.join("outputs");
        fs::create_dir(&directory).unwrap();
        let publish = |mode| {
            publish_dependency_outputs(
                &input,
                prepared.identity(),
                prepared.directory(),
                &directory,
                mode,
                RoleOutputOwnership::CurrentUser,
            )
        };
        publish(RoleOutputMode::Provision).unwrap();
        let file = directory.join("openbao/openbao-server.pem");
        let bytes = fs::read(&file).unwrap();
        let certificate = prepared
            .directory()
            .read(
                crate::openbao_profile::OPENBAO_SERVER_CERTIFICATE,
                INSTALLATION_MAX_BYTES,
            )
            .unwrap()
            .unwrap();
        assert_eq!(bytes, certificate);
        let before = fs::metadata(&file).unwrap();
        assert_eq!(before.mode() & 0o777, 0o600);
        assert_eq!(before.uid(), unsafe { libc::geteuid() });
        let (_, pem) = x509_parser::pem::parse_x509_pem(&certificate).unwrap();
        use x509_parser::prelude::FromDer as _;
        let (_, certificate) =
            x509_parser::certificate::X509Certificate::from_der(&pem.contents).unwrap();
        let names = &certificate
            .subject_alternative_name()
            .unwrap()
            .unwrap()
            .value
            .general_names;
        assert_eq!(
            names,
            &[x509_parser::extensions::GeneralName::DNSName(
                "openbao.tls-test.svc.cluster.local"
            )]
        );
        publish(RoleOutputMode::Verify).unwrap();
        assert_eq!(before.ino(), fs::metadata(&file).unwrap().ino());
        assert_eq!(bytes, fs::read(&file).unwrap());
        assert!(!directory.join("openbao/ca-key.pem").exists());
        use insight_platform_deployment_contracts::installation::InstallationProcess;
        assert!(role_material::requires_aws_ca(
            &input.network,
            InstallationProcess::ArtifactMaintenance
        ));
        assert!(!role_material::requires_aws_ca(
            &input.network,
            InstallationProcess::ModelWorker
        ));
        private_file(&directory.join("openbao/foreign"), b"retained");
        assert!(publish(RoleOutputMode::Provision).is_err());
        assert_eq!(bytes, fs::read(&file).unwrap());
    }
}
