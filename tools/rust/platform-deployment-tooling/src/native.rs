//! Host deployment inputs and a read-only launch handoff; no identities or services are created.
use insight_platform_contracts::Sha256Digest;
use insight_platform_deployment_contracts::{installation::*, native_installation::*};
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, fs, io::Read, path::Path};

fn invalid() -> InstallationError {
    InstallationError::InvalidInput
}
fn path(value: &Path) -> Result<String, InstallationError> {
    let value = value.to_str().ok_or(InstallationError::InvalidPath)?;
    if !native_absolute_path(value) {
        return Err(InstallationError::InvalidPath);
    }
    Ok(value.into())
}

pub fn native_input(
    name: &str,
    package_digest: Sha256Digest,
    output: &Path,
    port_base: u16,
) -> Result<InstallationInputV1, InstallationError> {
    path(output)?;
    if !(1024..=64000).contains(&port_base) {
        return Err(InstallationError::InvalidEndpoint);
    }
    let mut input = crate::installation::compose_input(name, package_digest)?;
    input.network.topology = InstallationTopology::Native;
    input.network.database.host = "127.0.0.1".into();
    input.network.database.port = port_base;
    input.network.nats_host = "localhost".into();
    input.network.nats_port = port_base + 1;
    input.network.providers = ProviderNetworkV1::S3OpenBao {
        artifact: ServiceOrigin::parse(&format!("https://localhost:{}", port_base + 2))?,
        openbao: ServiceOrigin::parse(&format!("https://localhost:{}", port_base + 3))?,
    };
    input.network.console_origin =
        ServiceOrigin::parse(&format!("http://127.0.0.1:{}", port_base + 4))?;
    for (index, entry) in input.network.processes.iter_mut().enumerate() {
        let listen_port = port_base + 16 + u16::try_from(index).map_err(|_| invalid())? * 2;
        let observation_port = if matches!(
            entry.process,
            InstallationProcess::GatewayManagement | InstallationProcess::GatewayRuntime
        ) {
            listen_port
        } else {
            listen_port + 1
        };
        entry.observability_address = ([127, 0, 0, 1], observation_port).into();
        if let Some(origin) = &entry.service_origin {
            entry.listen_address = Some(([127, 0, 0, 1], listen_port).into());
            entry.service_origin = Some(ServiceOrigin::parse(&format!(
                "{}://localhost:{listen_port}",
                if origin.is_tls() { "https" } else { "http" }
            ))?);
        }
    }
    for entry in &mut input.paths {
        let role = output.join("roles").join(entry.process.name());
        entry.configuration_directory = path(&role.join("config"))?;
        entry.credential_directory = path(&role.join("credentials"))?;
        entry.temporary_directory = path(&output.join("temporary").join(entry.process.name()))?;
    }
    input.credentials = crate::role_material::credentials(&input.network);
    input.validate()?;
    Ok(input)
}

fn regular_file(file: &Path, executable: bool) -> Result<fs::File, InstallationError> {
    path(file)?;
    for ancestor in file.ancestors().skip(1) {
        if !fs::symlink_metadata(ancestor)
            .map_err(|_| invalid())?
            .is_dir()
        {
            return Err(InstallationError::InvalidPath);
        }
    }
    let metadata = fs::symlink_metadata(file).map_err(|_| invalid())?;
    if !metadata.is_file() || metadata.len() == 0 || metadata.len() > NATIVE_MAX_ARTIFACT_BYTES {
        return Err(invalid());
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
        if metadata.nlink() != 1
            || metadata.permissions().mode() & 0o022 != 0
            || (executable && metadata.permissions().mode() & 0o111 == 0)
        {
            return Err(invalid());
        }
        let source = fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK)
            .open(file)
            .map_err(|_| invalid())?;
        let actual = source.metadata().map_err(|_| invalid())?;
        if !actual.is_file()
            || actual.dev() != metadata.dev()
            || actual.ino() != metadata.ino()
            || actual.len() != metadata.len()
            || actual.mode() != metadata.mode()
            || actual.nlink() != 1
        {
            return Err(invalid());
        }
        Ok(source)
    }
    #[cfg(not(unix))]
    {
        let _ = executable;
        Err(InstallationError::UnsupportedTopology)
    }
}

fn matches_host(header: &[u8], host_os: &str, host_arch: &str) -> bool {
    if header.len() < 32 {
        return false;
    }
    match (host_os, host_arch) {
        ("linux", "x86_64" | "aarch64") => {
            let machine = if host_arch == "x86_64" { 62 } else { 183 };
            &header[..4] == b"\x7fELF"
                && header[4] == 2
                && header[5] == 1
                && matches!(u16::from_le_bytes([header[16], header[17]]), 2 | 3)
                && u16::from_le_bytes([header[18], header[19]]) == machine
        }
        ("macos", "x86_64" | "aarch64") => {
            let cpu = if host_arch == "x86_64" {
                0x0100_0007
            } else {
                0x0100_000c
            };
            header[..4] == [0xcf, 0xfa, 0xed, 0xfe]
                && u32::from_le_bytes(header[4..8].try_into().expect("fixed header")) == cpu
                && u32::from_le_bytes(header[12..16].try_into().expect("fixed header")) == 2
        }
        _ => false,
    }
}

fn artifact(file: &Path, executable: bool) -> Result<NativeArtifactV1, InstallationError> {
    let mut source = regular_file(file, executable)?;
    let before = source.metadata().map_err(|_| invalid())?;
    let mut buffer = [0u8; 65_536];
    let mut hash = Sha256::new();
    let mut total = 0;
    loop {
        let count = source.read(&mut buffer).map_err(|_| invalid())?;
        if count == 0 {
            break;
        }
        if total == 0
            && executable
            && !matches_host(
                &buffer[..count],
                std::env::consts::OS,
                std::env::consts::ARCH,
            )
        {
            return Err(InstallationError::UnsupportedTopology);
        }
        total += count as u64;
        if total > NATIVE_MAX_ARTIFACT_BYTES {
            return Err(invalid());
        }
        hash.update(&buffer[..count]);
    }
    let after = source.metadata().map_err(|_| invalid())?;
    let current = fs::symlink_metadata(file).map_err(|_| invalid())?;
    if total == 0 || total != before.len() || total != after.len() {
        return Err(InstallationError::ConfigurationDrift);
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        for metadata in [&after, &current] {
            if !metadata.is_file()
                || metadata.dev() != before.dev()
                || metadata.ino() != before.ino()
                || metadata.len() != before.len()
                || metadata.mode() != before.mode()
                || metadata.nlink() != 1
                || metadata.mtime() != before.mtime()
                || metadata.mtime_nsec() != before.mtime_nsec()
                || metadata.ctime() != before.ctime()
                || metadata.ctime_nsec() != before.ctime_nsec()
            {
                return Err(InstallationError::ConfigurationDrift);
            }
        }
    }
    Ok(NativeArtifactV1 {
        path: path(file)?,
        bytes_digest: format!("sha256:{}", crate::lower_hex(&hash.finalize()))
            .parse()
            .map_err(|_| invalid())?,
        executable,
    })
}

fn collect_assets(
    root: &Path,
    entries: &mut BTreeMap<String, NativeArtifactV1>,
    depth: u8,
    remaining: &mut usize,
) -> Result<(), InstallationError> {
    if depth > 4 || !fs::symlink_metadata(root).map_err(|_| invalid())?.is_dir() {
        return Err(invalid());
    }
    for entry in fs::read_dir(root).map_err(|_| invalid())? {
        let entry = entry.map_err(|_| invalid())?;
        *remaining = remaining.checked_sub(1).ok_or_else(invalid)?;
        if entry.file_type().map_err(|_| invalid())?.is_dir() {
            collect_assets(&entry.path(), entries, depth + 1, remaining)?;
        } else {
            let item = artifact(&entry.path(), false)?;
            entries.insert(item.path.clone(), item);
            if entries.len() > NATIVE_MAX_ARTIFACTS {
                return Err(invalid());
            }
        }
    }
    Ok(())
}

/// The filesystem is inspected before `prepare`, Docker startup, or any credential generation.
pub fn native_plan(
    input: &InstallationInputV1,
    output: &Path,
    binaries: &Path,
    console_directory: &Path,
    node_file: &Path,
) -> Result<NativeLaunchPlanV1, InstallationError> {
    input.validate()?;
    crate::installation::validate_remote_context_destinations(&input.remote_context_destinations)?;
    for directory in [output, binaries, console_directory] {
        path(directory)?;
    }
    if input.network.topology != InstallationTopology::Native {
        return Err(InstallationError::UnsupportedTopology);
    }
    let mut artifacts = BTreeMap::new();
    let mut processes = Vec::new();
    for entry in NATIVE_START_ORDER.iter().filter_map(|process| {
        input
            .network
            .processes
            .iter()
            .find(|entry| &entry.process == process)
    }) {
        let role = output.join("roles").join(entry.process.name());
        let paths = input
            .paths
            .iter()
            .find(|item| item.process == entry.process)
            .ok_or_else(invalid)?;
        if paths.configuration_directory != path(&role.join("config"))?
            || paths.credential_directory != path(&role.join("credentials"))?
            || paths.temporary_directory
                != path(&output.join("temporary").join(entry.process.name()))?
        {
            return Err(InstallationError::ConfigurationDrift);
        }
        let executable = binaries.join(entry.process.binary());
        if !artifacts.contains_key(&path(&executable)?) {
            let item = artifact(&executable, true)?;
            artifacts.insert(item.path.clone(), item);
        }
        processes.push(NativeProcessLaunchV1 {
            process: entry.process,
            executable_file: path(&executable)?,
            environment_file: path(&role.join("environment"))?,
            temporary_directory: paths.temporary_directory.clone(),
        });
    }
    for name in [
        "platform-installation",
        "platform-schema",
        "platform-database-role",
        "platform-dev-bootstrap",
        "platform-jetstream-provision",
    ] {
        let item = artifact(&binaries.join(name), true)?;
        artifacts.insert(item.path.clone(), item);
    }
    let node = artifact(node_file, true)?;
    artifacts.insert(node.path.clone(), node);
    let server = console_directory.join("server-dist");
    for name in ["main.js", "config.js", "gateway-server.js", "process.js"] {
        let item = artifact(&server.join(name), false)?;
        artifacts.insert(item.path.clone(), item);
    }
    let bundle = console_directory.join("dist");
    regular_file(&bundle.join("index.html"), false)?;
    collect_assets(&bundle, &mut artifacts, 0, &mut (NATIVE_MAX_ARTIFACTS * 2))?;
    if !artifacts
        .values()
        .any(|item| Path::new(&item.path).starts_with(&bundle) && item.path.ends_with(".wasm"))
        || !artifacts.values().any(|item| {
            Path::new(&item.path).starts_with(&bundle)
                && Path::new(&item.path)
                    .file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| {
                        name.starts_with("compiler.worker-") && name.ends_with(".js")
                    })
        })
    {
        return Err(invalid());
    }
    let plan = NativeLaunchPlanV1 {
        schema_version: NATIVE_PLAN_VERSION,
        input_digest: input.digest()?,
        host_os: std::env::consts::OS.into(),
        host_arch: std::env::consts::ARCH.into(),
        installation_binary: path(&binaries.join("platform-installation"))?,
        preparation_directories: std::iter::once(output.to_path_buf())
            .chain(
                [
                    "dependencies",
                    "roles",
                    "postgres-data",
                    "nats-data",
                    "s3-data",
                    "openbao-data",
                ]
                .into_iter()
                .map(|name| output.join(name)),
            )
            .map(|directory| path(&directory))
            .collect::<Result<Vec<_>, _>>()?,
        processes,
        console: NativeConsoleLaunchV1 {
            node_file: path(node_file)?,
            entrypoint_file: path(&server.join("main.js"))?,
            configuration_file: path(&output.join("roles/console/config.json"))?,
            bundle_directory: path(&bundle)?,
            minimum_node_version: NATIVE_MIN_NODE_VERSION,
            maximum_node_major: 24,
        },
        artifacts: artifacts.into_values().collect(),
    };
    plan.validate_for(input)?;
    Ok(plan)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn native_input_has_one_shared_identity_input_and_disjoint_loopback_ports() {
        let input = native_input(
            "native-test",
            format!("sha256:{}", "a".repeat(64)).parse().unwrap(),
            Path::new("/private/native/output"),
            18000,
        )
        .unwrap();
        input.validate().unwrap();
        assert!(matches!(
            input.network.providers,
            ProviderNetworkV1::S3OpenBao { .. }
        ));
        assert_eq!(input.network.database.port, 18000);
        assert!(input.paths.iter().all(|entry| {
            entry
                .configuration_directory
                .starts_with("/private/native/output/roles/")
        }));
        assert!(native_input(
            "native-test",
            input.package_digest,
            Path::new("/private/../foreign"),
            18000
        )
        .is_err());
    }
    #[test]
    fn host_preflight_rejects_other_architectures_and_scripts() {
        let mut elf = [0u8; 32];
        elf[..6].copy_from_slice(b"\x7fELF\x02\x01");
        elf[16] = 2;
        elf[18] = 62;
        assert!(matches_host(&elf, "linux", "x86_64"));
        assert!(!matches_host(&elf, "linux", "aarch64"));
        assert!(!matches_host(&elf, "macos", "x86_64"));
        assert!(!matches_host(b"#!/bin/sh\nexit 0\n", "linux", "x86_64"));
    }

    #[test]
    fn native_plan_binds_console_tree_and_rejects_missing_or_unsafe_artifacts_before_preparation() {
        use std::os::unix::fs::{symlink, PermissionsExt};
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().canonicalize().unwrap();
        let output = root.join("installation");
        let binaries = root.join("bin");
        let console = root.join("console");
        fs::create_dir(&binaries).unwrap();
        fs::create_dir_all(console.join("server-dist")).unwrap();
        fs::create_dir_all(console.join("dist/assets")).unwrap();
        let input = native_input(
            "native-artifacts",
            format!("sha256:{}", "a".repeat(64)).parse().unwrap(),
            &output,
            18000,
        )
        .unwrap();
        let mut header = [0u8; 64];
        if std::env::consts::OS == "macos" {
            header[..4].copy_from_slice(&[0xcf, 0xfa, 0xed, 0xfe]);
            header[4..8].copy_from_slice(
                &(if std::env::consts::ARCH == "aarch64" {
                    0x0100_000cu32
                } else {
                    0x0100_0007u32
                })
                .to_le_bytes(),
            );
            header[12] = 2;
        } else {
            header[..6].copy_from_slice(b"\x7fELF\x02\x01");
            header[16] = 2;
            header[18] = if std::env::consts::ARCH == "aarch64" {
                183
            } else {
                62
            };
        }
        for name in input
            .network
            .processes
            .iter()
            .map(|entry| entry.process.binary())
            .chain([
                "platform-installation",
                "platform-schema",
                "platform-database-role",
                "platform-dev-bootstrap",
                "platform-jetstream-provision",
                "node",
            ])
        {
            fs::write(binaries.join(name), header).unwrap();
            fs::set_permissions(binaries.join(name), fs::Permissions::from_mode(0o700)).unwrap();
        }
        for name in ["main.js", "config.js", "gateway-server.js", "process.js"] {
            fs::write(console.join("server-dist").join(name), b"export {}\n").unwrap();
        }
        fs::write(console.join("dist/index.html"), b"<html></html>").unwrap();
        fs::write(
            console.join("dist/assets/compiler.worker-test.js"),
            b"export {}\n",
        )
        .unwrap();
        let wasm = console.join("dist/assets/compiler.wasm");
        fs::write(&wasm, b"\0asm\x01\0\0\0").unwrap();
        let plan =
            native_plan(&input, &output, &binaries, &console, &binaries.join("node")).unwrap();
        assert!(!output.exists());
        let mut unordered = plan.clone();
        unordered.processes.reverse();
        assert!(unordered.validate_for(&input).is_err());
        let mut foreign_directory = plan.clone();
        foreign_directory
            .preparation_directories
            .push(root.join("foreign").display().to_string());
        assert!(foreign_directory.validate_for(&input).is_err());
        for field in [
            "config",
            "entrypoint",
            "bundle",
            "wasm",
            "worker",
            "index",
            "server",
        ] {
            let mut changed = plan.clone();
            match field {
                "config" => {
                    changed.console.configuration_file =
                        root.join("foreign.json").display().to_string()
                }
                "entrypoint" => {
                    changed.console.entrypoint_file =
                        console.join("server-dist/config.js").display().to_string()
                }
                "bundle" => {
                    changed.console.bundle_directory = root.join("foreign").display().to_string()
                }
                _ => changed.artifacts.retain(|entry| {
                    !entry.path.ends_with(match field {
                        "wasm" => "compiler.wasm",
                        "worker" => "compiler.worker-test.js",
                        "index" => "index.html",
                        _ => "process.js",
                    })
                }),
            }
            assert!(changed.validate_for(&input).is_err(), "{field}");
        }
        fs::remove_file(&wasm).unwrap();
        assert!(native_plan(&input, &output, &binaries, &console, &binaries.join("node")).is_err());
        symlink(console.join("dist/index.html"), &wasm).unwrap();
        assert!(native_plan(&input, &output, &binaries, &console, &binaries.join("node")).is_err());
        assert!(!output.exists());
    }
}
