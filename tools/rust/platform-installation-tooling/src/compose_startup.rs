//! Finite Compose preparation. Environment contains public options, never model credentials.
use insight_platform_contracts::{canonical_digest, Sha256Digest};
use insight_platform_deployment_contracts::installation::*;
use insight_platform_deployment_tooling::{
    installation::compose_input, private_state::InstallationDirectory,
};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    io::Read,
    path::Path,
};

fn invalid() -> InstallationError {
    InstallationError::InvalidInput
}

fn options(
    name: Option<String>,
    port: Option<String>,
) -> Result<LocalComposeOptionsV1, InstallationError> {
    let value = LocalComposeOptionsV1 {
        schema_version: 1,
        name: name.unwrap_or_else(|| "my-platform".into()),
        http_port: port
            .map(|p| p.parse().map_err(|_| invalid()))
            .transpose()?
            .unwrap_or(8088),
    };
    value.validate()?;
    Ok(value)
}

pub(crate) fn executable_inventory(directory: &Path) -> Result<Sha256Digest, InstallationError> {
    let mut names: BTreeSet<&str> = InstallationProcess::BASE
        .iter()
        .map(|p| p.binary())
        .collect();
    names.extend([
        "platform-installation",
        "platform-schema",
        "platform-dev-bootstrap",
        "platform-database-role",
        "platform-jetstream-provision",
    ]);
    let mut files = BTreeMap::new();
    for name in names {
        let path = directory.join(name);
        let metadata = std::fs::symlink_metadata(&path).map_err(|_| invalid())?;
        if !metadata.is_file() || metadata.len() == 0 || metadata.len() > 536_870_912 {
            return Err(invalid());
        }
        let mut file = std::fs::File::open(path).map_err(|_| invalid())?;
        let mut hasher = Sha256::new();
        let mut bytes = [0u8; 65536];
        let mut length = 0u64;
        loop {
            let n = file.read(&mut bytes).map_err(|_| invalid())?;
            if n == 0 {
                break;
            }
            length += n as u64;
            if length > metadata.len() {
                return Err(invalid());
            }
            hasher.update(&bytes[..n]);
        }
        if length != metadata.len() {
            return Err(invalid());
        }
        let hex: String = hasher
            .finalize()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();
        files.insert(name, format!("sha256:{hex}"));
    }
    canonical_digest(
        &serde_json::json!({"schema_version":1,"kind":"local_compose_executables","files":files}),
    )
    .map_err(|_| invalid())?
    .parse()
    .map_err(|_| invalid())
}

pub fn prepare() -> Result<(), InstallationError> {
    let read = |key| {
        std::env::var(key).map(Some).or_else(|e| match e {
            std::env::VarError::NotPresent => Ok(None),
            _ => Err(invalid()),
        })
    };
    let options = options(read("INSIGHT_NAME")?, read("INSIGHT_HTTP_PORT")?)?;
    let mut input = compose_input(
        &options.name,
        executable_inventory(Path::new("/usr/local/bin"))?,
    )?;
    input.network.console_origin =
        ServiceOrigin::parse(&format!("http://127.0.0.1:{}", options.http_port))?;
    input.validate()?;
    // Atomic immutable publication also rejects concurrent preparation and restart drift.
    let public = InstallationDirectory::open(Path::new("/installation-input/prepared"), true)?;
    if let Some(bytes) = public.read("input.json", INSTALLATION_MAX_BYTES)? {
        let installed = InstallationInputV1::decode(&bytes)?;
        if installed.digest()? != input.digest()? {
            crate::upgrade::verify_release(&installed, &input, Path::new("/installation/private"))?;
            input = installed;
        }
    } else {
        public.write_immutable(
            "input.json",
            &serde_json::to_vec_pretty(&input).map_err(|_| invalid())?,
        )?;
    }
    crate::workflow::prepare(
        &input,
        Path::new("/installation/private"),
        Path::new("/output"),
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn options_are_bounded_and_do_not_silently_replace_invalid_values() {
        assert_eq!(options(None, None).unwrap().http_port, 8088);
        assert_eq!(
            options(Some("example".into()), Some("8090".into()))
                .unwrap()
                .name,
            "example"
        );
        for port in ["0", "80", "65536", "x", "", " 8088"] {
            assert!(options(None, Some(port.into())).is_err());
        }
        for name in ["", "../other", "name with space"] {
            assert!(options(Some(name.into()), None).is_err());
        }
    }
    #[test]
    fn inventory_binds_actual_executables() {
        let root = tempfile::tempdir().unwrap();
        for name in InstallationProcess::BASE.iter().map(|p| p.binary()).chain([
            "platform-installation",
            "platform-schema",
            "platform-dev-bootstrap",
            "platform-database-role",
            "platform-jetstream-provision",
        ]) {
            std::fs::write(root.path().join(name), name).unwrap();
        }
        let first = executable_inventory(root.path()).unwrap();
        assert_eq!(first, executable_inventory(root.path()).unwrap());
        std::fs::write(root.path().join("platform-schema"), "changed").unwrap();
        assert_ne!(first, executable_inventory(root.path()).unwrap());
        std::fs::remove_file(root.path().join("platform-schema")).unwrap();
        assert!(executable_inventory(root.path()).is_err());
    }
}
