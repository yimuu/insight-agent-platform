//! Test-only export of the real installation producer into an isolated provider fixture.
#![cfg(unix)]
use insight_platform_deployment_contracts::{
    installation::{InstallationInputV1, ProviderNetworkV1, ServiceOrigin},
    installation_provider::{InstallationProviderStateV1, OpenBaoInstallationRole},
};
use insight_platform_deployment_tooling::{
    installation::{compose_input, PreparedInstallation},
    openbao_profile::{OPENBAO_SEAL_FILE, OPENBAO_SERVER_CERTIFICATE, OPENBAO_SERVER_KEY},
};
use std::{
    fs::{self, File, OpenOptions},
    io::Write,
    os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt},
    path::{Component, Path},
};

fn private_file(path: &Path, bytes: &[u8]) -> Result<(), &'static str> {
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(path)
        .map_err(|_| "fixture_file_invalid")?;
    file.write_all(bytes)
        .and_then(|()| file.sync_all())
        .map_err(|_| "fixture_file_invalid")
}

fn export(root: &Path, port: u16) -> Result<(), &'static str> {
    if !(1024..=65535).contains(&port)
        || !root.is_absolute()
        || root
            .components()
            .any(|part| matches!(part, Component::CurDir | Component::ParentDir))
    {
        return Err("fixture_input_invalid");
    }
    for ancestor in root.ancestors() {
        if !fs::symlink_metadata(ancestor)
            .map_err(|_| "fixture_directory_invalid")?
            .is_dir()
        {
            return Err("fixture_directory_invalid");
        }
    }
    let metadata = fs::symlink_metadata(root).map_err(|_| "fixture_directory_invalid")?;
    if metadata.uid() != unsafe { libc::geteuid() }
        || metadata.permissions().mode() & 0o777 != 0o700
        || fs::read_dir(root)
            .map_err(|_| "fixture_directory_invalid")?
            .next()
            .is_some()
    {
        return Err("fixture_directory_must_be_private_empty");
    }
    // This fixture exercises provider bootstrap, not an installed runtime package.
    let mut input = compose_input(
        "openbao-qualification",
        format!("sha256:{}", "a".repeat(64)).parse().unwrap(),
    )
    .map_err(|_| "fixture_input_invalid")?;
    input.network.providers = ProviderNetworkV1::S3OpenBao {
        artifact: ServiceOrigin::parse("https://localhost:8333").unwrap(),
        openbao: ServiceOrigin::parse(&format!("https://localhost:{port}")).unwrap(),
    };
    input.credentials =
        insight_platform_deployment_tooling::role_material::credentials(&input.network);
    input.validate().map_err(|_| "fixture_input_invalid")?;
    private_file(
        &root.join("input.json"),
        &serde_json::to_vec(&input).unwrap(),
    )?;
    let prepared = PreparedInstallation::prepare(&input, &root.join("private"))
        .map_err(|_| "fixture_prepare_failed")?;
    let dependency = root.join("dependency");
    fs::create_dir(&dependency).map_err(|_| "fixture_directory_invalid")?;
    fs::set_permissions(&dependency, fs::Permissions::from_mode(0o700))
        .map_err(|_| "fixture_directory_invalid")?;
    let mut files = prepared
        .renderer_private_files()
        .map_err(|_| "fixture_material_invalid")?;
    for name in [
        "ca.pem",
        OPENBAO_SEAL_FILE,
        OPENBAO_SERVER_CERTIFICATE,
        OPENBAO_SERVER_KEY,
    ] {
        private_file(
            &dependency.join(name),
            files.get(name).ok_or("fixture_material_invalid")?,
        )?;
    }
    for bytes in files.values_mut() {
        bytes.fill(0);
    }
    let documents = prepared
        .provider_documents()
        .map_err(|_| "fixture_material_invalid")?;
    private_file(&dependency.join("initialize.json"), &documents.initialize)?;
    private_file(&dependency.join("serve.json"), &documents.serve)?;
    File::open(&dependency)
        .and_then(|directory| directory.sync_all())
        .map_err(|_| "fixture_sync_failed")?;
    File::open(root)
        .and_then(|directory| directory.sync_all())
        .map_err(|_| "fixture_sync_failed")
}

#[test]
#[ignore = "requires a new explicitly owned private OpenBao fixture directory and loopback port"]
fn export_real_installation_openbao_fixture() {
    let root = std::env::var_os("INSIGHT_OPENBAO_FIXTURE_DIRECTORY")
        .expect("private fixture directory required");
    let port = std::env::var("INSIGHT_OPENBAO_FIXTURE_PORT")
        .expect("loopback port required")
        .parse()
        .expect("bounded port");
    export(Path::new(&root), port).expect("OpenBao fixture export failed with safe code");
}

#[test]
#[ignore = "requires the same observed ProviderReady fixture; publishes private catalog paths only"]
fn export_observed_openbao_secret_catalog() {
    let root = std::env::var_os("INSIGHT_OPENBAO_FIXTURE_DIRECTORY")
        .expect("private fixture directory required");
    let root = Path::new(&root);
    let input = InstallationInputV1::decode(&fs::read(root.join("input.json")).unwrap()).unwrap();
    let prepared = PreparedInstallation::open(&input, &root.join("private")).unwrap();
    let InstallationProviderStateV1::ProviderReady { evidence } =
        prepared.provider_state().unwrap().state
    else {
        panic!("ProviderReady required before catalog publication");
    };
    let client = insight_platform_deployment_tooling::openbao_profile::role_client(
        &evidence,
        OpenBaoInstallationRole::EgressBroker,
        &root.join("private"),
    )
    .unwrap();
    let catalog =
        insight_platform_deployment_tooling::provider_config::openbao_secret_provider_catalog(
            &evidence,
            &client,
            &prepared.identity().secret_provider_id,
        )
        .unwrap();
    private_file(
        &root.join("secret-catalog.json"),
        &serde_json::to_vec(&catalog).unwrap(),
    )
    .unwrap();
}

#[test]
#[ignore = "requires the same observed Bao fixture and a separate qualified S3 fixture"]
fn export_observed_openbao_artifact_catalog() {
    #[derive(serde::Deserialize)]
    #[serde(deny_unknown_fields)]
    struct S3References {
        endpoint: String,
        bucket: String,
        ca_file: String,
        credentials_file: String,
    }
    let root = std::env::var_os("INSIGHT_OPENBAO_FIXTURE_DIRECTORY")
        .expect("private fixture directory required");
    let root = Path::new(&root);
    let path =
        std::env::var_os("INSIGHT_S3_FIXTURE_REFERENCES").expect("private S3 references required");
    let metadata = fs::symlink_metadata(&path).unwrap();
    assert!(metadata.is_file() && !metadata.file_type().is_symlink() && metadata.len() <= 32_768);
    assert_eq!(metadata.nlink(), 1);
    assert_eq!(metadata.permissions().mode() & 0o777, 0o600);
    let references: S3References = serde_json::from_slice(&fs::read(path).unwrap()).unwrap();
    for file in [&references.ca_file, &references.credentials_file] {
        assert!(Path::new(file).is_absolute());
    }
    let input = InstallationInputV1::decode(&fs::read(root.join("input.json")).unwrap()).unwrap();
    let prepared = PreparedInstallation::open(&input, &root.join("private")).unwrap();
    let InstallationProviderStateV1::ProviderReady { evidence } =
        prepared.provider_state().unwrap().state
    else {
        panic!("ProviderReady required before catalog publication");
    };
    let client = insight_platform_deployment_tooling::openbao_profile::role_client(
        &evidence,
        OpenBaoInstallationRole::ArtifactGateway,
        &root.join("private"),
    )
    .unwrap();
    let catalog =
        insight_platform_deployment_tooling::provider_config::openbao_artifact_provider_catalog(
            &ServiceOrigin::parse(&references.endpoint).unwrap(),
            &references.bucket,
            &client,
            &evidence.artifact_key,
        )
        .unwrap();
    private_file(
        &root.join("artifact-catalog.json"),
        &serde_json::to_vec(&catalog).unwrap(),
    )
    .unwrap();
}

#[test]
fn provider_dependency_contains_only_its_frozen_material_and_cannot_reprepare() {
    let temporary = tempfile::tempdir().unwrap();
    let root = temporary.path().canonicalize().unwrap().join("fixture");
    fs::create_dir(&root).unwrap();
    fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
    export(&root, 18200).unwrap();
    assert!(export(&root, 18200).is_err());
    let mut names = fs::read_dir(root.join("dependency"))
        .unwrap()
        .map(|entry| {
            let entry = entry.unwrap();
            let metadata = entry.metadata().unwrap();
            assert_eq!(metadata.nlink(), 1);
            assert_eq!(metadata.permissions().mode() & 0o777, 0o600);
            entry.file_name().into_string().unwrap()
        })
        .collect::<Vec<_>>();
    names.sort();
    assert_eq!(
        names,
        [
            "ca.pem",
            "initialize.json",
            "openbao-seal.key",
            "openbao-server-key.pem",
            "openbao-server.pem",
            "serve.json"
        ]
    );
    let serve: serde_json::Value =
        serde_json::from_slice(&fs::read(root.join("dependency/serve.json")).unwrap()).unwrap();
    assert!(serve.get("initialize").is_none());
}
