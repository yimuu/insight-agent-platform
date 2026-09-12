//! Public deployment publication gates. They convey no database or provider permission.
use insight_platform_deployment_contracts::{installation::*, installation_provider::*};
use insight_platform_deployment_tooling::{
    installation::PreparedInstallation, private_state::sync_directory,
};
use std::{
    fs,
    io::{Read, Write},
    path::Path,
    time::Duration,
};

pub fn filename(phase: InstallationGatePhaseV1) -> &'static str {
    match phase {
        InstallationGatePhaseV1::Prepared => "prepared.json",
        InstallationGatePhaseV1::Ready => "ready.json",
    }
}

pub fn publish(
    prepared: &PreparedInstallation,
    root: &Path,
    phase: InstallationGatePhaseV1,
) -> Result<(), InstallationError> {
    use std::os::unix::{
        fs::MetadataExt,
        fs::{OpenOptionsExt, PermissionsExt},
    };
    if !root.is_absolute()
        || root
            .components()
            .any(|part| matches!(part, std::path::Component::ParentDir))
    {
        return Err(InstallationError::InvalidPath);
    }
    for ancestor in root.ancestors() {
        let metadata =
            fs::symlink_metadata(ancestor).map_err(|_| InstallationError::InvalidPath)?;
        if !metadata.is_dir() || metadata.file_type().is_symlink() {
            return Err(InstallationError::InvalidPath);
        }
    }
    if phase == InstallationGatePhaseV1::Ready
        && prepared.progress().phase != InstallationPhase::Ready
    {
        return Err(InstallationError::Incomplete);
    }
    let gate = InstallationGateV1 {
        schema_version: 1,
        phase,
        input_digest: prepared.input().digest()?,
        identity_digest: prepared.identity().digest()?,
    };
    let bytes = serde_json::to_vec(&gate).map_err(|_| InstallationError::InvalidInput)?;
    let destination = root.join(filename(phase));
    if let Ok(metadata) = fs::symlink_metadata(&destination) {
        if !metadata.is_file()
            || metadata.file_type().is_symlink()
            || metadata.nlink() != 1
            || metadata.len() as usize != bytes.len()
            || metadata.mode() & 0o777 != 0o444
            || fs::read(&destination).map_err(|_| InstallationError::Incomplete)? != bytes
        {
            return Err(InstallationError::ConfigurationDrift);
        }
        return Ok(());
    }
    fs::set_permissions(root, fs::Permissions::from_mode(0o755))
        .map_err(|_| InstallationError::CredentialInvalid)?;
    let temporary = root.join(format!(".pending-{}", uuid::Uuid::new_v4()));
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&temporary)
        .map_err(|_| InstallationError::Incomplete)?;
    file.write_all(&bytes)
        .map_err(|_| InstallationError::Incomplete)?;
    file.set_permissions(fs::Permissions::from_mode(0o444))
        .map_err(|_| InstallationError::Incomplete)?;
    file.sync_all().map_err(|_| InstallationError::Incomplete)?;
    fs::rename(&temporary, &destination).map_err(|_| InstallationError::Incomplete)?;
    sync_directory(root).map_err(|_| InstallationError::Incomplete)
}

fn observe(
    input: &InstallationInputV1,
    root: &Path,
    phase: InstallationGatePhaseV1,
) -> Result<bool, InstallationError> {
    use std::os::unix::fs::MetadataExt;
    let path = root.join(filename(phase));
    let metadata = match fs::symlink_metadata(&path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(_) => return Err(InstallationError::CredentialInvalid),
    };
    if !metadata.is_file()
        || metadata.file_type().is_symlink()
        || metadata.nlink() != 1
        || metadata.mode() & 0o777 != 0o444
        || metadata.len() as usize > INSTALLATION_GATE_MAX_BYTES
    {
        return Err(InstallationError::CredentialInvalid);
    }
    let mut bytes = Vec::new();
    fs::File::open(path)
        .map_err(|_| InstallationError::Incomplete)?
        .take(INSTALLATION_GATE_MAX_BYTES as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| InstallationError::Incomplete)?;
    InstallationGateV1::decode_for(&bytes, input, phase)?;
    Ok(true)
}

pub async fn wait(
    input: &InstallationInputV1,
    root: &Path,
    phase: InstallationGatePhaseV1,
) -> Result<(), InstallationError> {
    if !root.is_absolute() {
        return Err(InstallationError::InvalidPath);
    }
    let deadline = tokio::time::Instant::now() + Duration::from_secs(900);
    loop {
        if observe(input, root, phase)? {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(InstallationError::PrerequisiteUnavailable);
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn gates_reject_wrong_installations_partial_publication_and_mutation() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().canonicalize().unwrap();
        let input = insight_platform_deployment_tooling::installation::compose_input(
            "gate-test",
            format!("sha256:{}", "a".repeat(64)).parse().unwrap(),
        )
        .unwrap();
        let prepared = PreparedInstallation::prepare(&input, &path.join("private")).unwrap();
        fs::create_dir(path.join("gates")).unwrap();
        assert!(!observe(
            &input,
            &path.join("gates"),
            InstallationGatePhaseV1::Prepared
        )
        .unwrap());
        assert_eq!(
            publish(
                &prepared,
                &path.join("gates"),
                InstallationGatePhaseV1::Ready
            ),
            Err(InstallationError::Incomplete)
        );
        publish(
            &prepared,
            &path.join("gates"),
            InstallationGatePhaseV1::Prepared,
        )
        .unwrap();
        assert!(observe(
            &input,
            &path.join("gates"),
            InstallationGatePhaseV1::Prepared
        )
        .unwrap());
        let mut other = input.clone();
        other.name = "other".into();
        assert!(observe(
            &other,
            &path.join("gates"),
            InstallationGatePhaseV1::Prepared
        )
        .is_err());
        publish(
            &prepared,
            &path.join("gates"),
            InstallationGatePhaseV1::Prepared,
        )
        .unwrap();
    }
}
