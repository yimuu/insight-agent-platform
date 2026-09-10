//! Filesystem durability shared by local command journals; it grants no platform authority.
use std::{fs, io, path::Path};
pub fn sync_directory(path: &Path) -> io::Result<()> {
    fs::File::open(path)?.sync_all()
}
pub fn ensure_durable_directory(path: &Path) -> io::Result<()> {
    let mut missing = Vec::new();
    let mut cursor = path;
    loop {
        match fs::symlink_metadata(cursor) {
            Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink() => break,
            Ok(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "private state path is not a regular directory",
                ))
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                missing.push(cursor.to_owned());
                cursor = cursor.parent().ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "private state directory has no existing parent",
                    )
                })?;
            }
            Err(error) => return Err(error),
        }
    }
    for directory in missing.into_iter().rev() {
        let mut builder = fs::DirBuilder::new();
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt as _;
            builder.mode(0o700);
        }
        builder.create(&directory)?;
        sync_directory(&directory)?;
        sync_directory(directory.parent().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "private state directory has no parent",
            )
        })?)?;
    }
    Ok(())
}

/// Exclusive setup ownership. It conveys no database or platform permission.
pub struct InstallationDirectory {
    root: std::path::PathBuf,
    _lock: fs::File,
    read_only: bool,
}
impl InstallationDirectory {
    pub fn open(
        root: &Path,
        create: bool,
    ) -> Result<Self, insight_platform_deployment_contracts::installation::InstallationError> {
        Self::open_mode(root, create, false)
    }
    pub fn open_read_only(
        root: &Path,
    ) -> Result<Self, insight_platform_deployment_contracts::installation::InstallationError> {
        Self::open_mode(root, false, true)
    }
    fn open_mode(
        root: &Path,
        create: bool,
        read_only: bool,
    ) -> Result<Self, insight_platform_deployment_contracts::installation::InstallationError> {
        use insight_platform_deployment_contracts::installation::InstallationError as E;
        if !root.is_absolute()
            || root.components().any(|part| {
                matches!(
                    part,
                    std::path::Component::ParentDir | std::path::Component::CurDir
                )
            })
        {
            return Err(E::InvalidPath);
        }
        for ancestor in root.ancestors().skip(1) {
            let metadata = fs::symlink_metadata(ancestor).map_err(|_| E::InvalidPath)?;
            if !metadata.is_dir() || metadata.file_type().is_symlink() {
                return Err(E::InvalidPath);
            }
        }
        if create && !root.try_exists().map_err(|_| E::InvalidPath)? {
            let mut builder = fs::DirBuilder::new();
            #[cfg(unix)]
            {
                use std::os::unix::fs::DirBuilderExt as _;
                builder.mode(0o700);
            }
            builder.create(root).map_err(|_| E::Conflict)?;
            sync_directory(root.parent().ok_or(E::InvalidPath)?).map_err(|_| E::Incomplete)?;
        }
        let metadata = fs::symlink_metadata(root).map_err(|_| E::Incomplete)?;
        if !metadata.is_dir() || metadata.file_type().is_symlink() {
            return Err(E::InvalidPath);
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            if metadata.permissions().mode() & 0o777 != 0o700 {
                return Err(E::CredentialInvalid);
            }
        }
        let mut options = fs::OpenOptions::new();
        options.read(true).write(!read_only).create(create);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt as _;
            options
                .mode(0o600)
                .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
        }
        let lock = options
            .open(root.join("installation.lock"))
            .map_err(|_| E::CredentialInvalid)?;
        validate_private_metadata(&lock.metadata().map_err(|_| E::CredentialInvalid)?, None)?;
        lock.try_lock().map_err(|_| E::Conflict)?;
        if !read_only {
            sync_directory(root).map_err(|_| E::Incomplete)?;
        }
        Ok(Self {
            root: root.to_owned(),
            _lock: lock,
            read_only,
        })
    }
    pub fn require_uninitialized(
        &self,
    ) -> Result<(), insight_platform_deployment_contracts::installation::InstallationError> {
        use insight_platform_deployment_contracts::installation::InstallationError as E;
        for entry in fs::read_dir(&self.root).map_err(|_| E::ForeignState)? {
            let entry = entry.map_err(|_| E::ForeignState)?;
            if entry.file_name() != "installation.lock" {
                return Err(E::ForeignState);
            }
        }
        Ok(())
    }
    pub fn path(
        &self,
        name: &str,
    ) -> Result<
        std::path::PathBuf,
        insight_platform_deployment_contracts::installation::InstallationError,
    > {
        use insight_platform_deployment_contracts::installation::InstallationError as E;
        if name.is_empty()
            || name.len() > 128
            || name == "."
            || name == ".."
            || !name
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
        {
            return Err(E::InvalidPath);
        }
        Ok(self.root.join(name))
    }
    pub fn read(
        &self,
        name: &str,
        limit: usize,
    ) -> Result<
        Option<Vec<u8>>,
        insight_platform_deployment_contracts::installation::InstallationError,
    > {
        use insight_platform_deployment_contracts::installation::InstallationError as E;
        use std::io::Read as _;
        let path = self.path(name)?;
        let metadata = match fs::symlink_metadata(&path) {
            Ok(value) => value,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(_) => return Err(E::CredentialInvalid),
        };
        validate_private_metadata(&metadata, Some(limit))?;
        let mut options = fs::OpenOptions::new();
        options.read(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt as _;
            options.custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
        }
        let file = options.open(path).map_err(|_| E::CredentialInvalid)?;
        let opened = file.metadata().map_err(|_| E::CredentialInvalid)?;
        validate_private_metadata(&opened, Some(limit))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt as _;
            if opened.ino() != metadata.ino() || opened.dev() != metadata.dev() {
                return Err(E::CredentialInvalid);
            }
        }
        let mut bytes = Vec::new();
        file.take(limit as u64 + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| E::CredentialInvalid)?;
        if bytes.is_empty() || bytes.len() > limit {
            return Err(E::CredentialInvalid);
        }
        Ok(Some(bytes))
    }
    pub fn write_immutable(
        &self,
        name: &str,
        bytes: &[u8],
    ) -> Result<(), insight_platform_deployment_contracts::installation::InstallationError> {
        use insight_platform_deployment_contracts::installation::InstallationError as E;
        if self.read_only {
            return Err(E::InvalidInput);
        }
        if let Some(current) = self.read(name, bytes.len())? {
            return if current == bytes {
                Ok(())
            } else {
                Err(E::ConfigurationDrift)
            };
        }
        self.replace(name, bytes)
    }
    pub fn replace(
        &self,
        name: &str,
        bytes: &[u8],
    ) -> Result<(), insight_platform_deployment_contracts::installation::InstallationError> {
        use insight_platform_deployment_contracts::installation::InstallationError as E;
        use std::io::Write as _;
        if self.read_only {
            return Err(E::InvalidInput);
        }
        if bytes.is_empty() || bytes.len() > 262144 {
            return Err(E::InvalidInput);
        }
        let target = self.path(name)?;
        if let Ok(metadata) = fs::symlink_metadata(&target) {
            validate_private_metadata(&metadata, None)?;
        }
        let temporary = self.root.join(format!(".pending-{}", uuid::Uuid::new_v4()));
        let mut options = fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt as _;
            options
                .mode(0o600)
                .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
        }
        let mut file = options.open(&temporary).map_err(|_| E::Incomplete)?;
        file.write_all(bytes)
            .and_then(|()| file.sync_all())
            .map_err(|_| E::Incomplete)?;
        fs::rename(temporary, target).map_err(|_| E::Incomplete)?;
        sync_directory(&self.root).map_err(|_| E::Incomplete)
    }
}
fn validate_private_metadata(
    metadata: &fs::Metadata,
    limit: Option<usize>,
) -> Result<(), insight_platform_deployment_contracts::installation::InstallationError> {
    use insight_platform_deployment_contracts::installation::InstallationError as E;
    if !metadata.is_file()
        || metadata.file_type().is_symlink()
        || limit.is_some_and(|limit| metadata.len() == 0 || metadata.len() > limit as u64)
    {
        return Err(E::CredentialInvalid);
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt as _;
        if metadata.nlink() != 1 || metadata.mode() & 0o777 != 0o600 {
            return Err(E::CredentialInvalid);
        }
    }
    Ok(())
}
