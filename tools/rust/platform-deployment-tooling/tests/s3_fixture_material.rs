//! Isolated S3 TLS evidence only. No CA private key or serving workload credential is exported.
#![cfg(unix)]

use insight_platform_deployment_tooling::tls::{
    authority_parameters, create_authority, create_leaf,
};
use rcgen::{ExtendedKeyUsagePurpose, Issuer, KeyPair};
use std::{
    fs::{self, File, OpenOptions},
    io::Write,
    os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt},
    path::{Component, Path},
};

fn export(root: &Path) -> Result<(), &'static str> {
    if !root.is_absolute()
        || root
            .components()
            .any(|part| matches!(part, Component::CurDir | Component::ParentDir))
    {
        return Err("fixture_directory_invalid");
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
    let api = create_authority().map_err(|_| "fixture_tls_invalid")?;
    let control = create_authority().map_err(|_| "fixture_tls_invalid")?;
    let api_issuer = Issuer::new(
        authority_parameters().map_err(|_| "fixture_tls_invalid")?,
        KeyPair::from_pem(&api.private_key_pem).map_err(|_| "fixture_tls_invalid")?,
    );
    let control_issuer = Issuer::new(
        authority_parameters().map_err(|_| "fixture_tls_invalid")?,
        KeyPair::from_pem(&control.private_key_pem).map_err(|_| "fixture_tls_invalid")?,
    );
    let api_server = create_leaf(
        &["localhost.localstack.cloud"],
        None,
        ExtendedKeyUsagePurpose::ServerAuth,
        &api_issuer,
    )
    .map_err(|_| "fixture_tls_invalid")?;
    let control_server = create_leaf(
        &["localhost.localstack.cloud"],
        None,
        ExtendedKeyUsagePurpose::ServerAuth,
        &control_issuer,
    )
    .map_err(|_| "fixture_tls_invalid")?;
    let ordinary_client = create_leaf(
        &[],
        Some("spiffe://insight.platform/qualification/ordinary-client"),
        ExtendedKeyUsagePurpose::ClientAuth,
        &api_issuer,
    )
    .map_err(|_| "fixture_tls_invalid")?;
    let control_client = create_leaf(
        &[],
        Some("spiffe://insight.platform/qualification/private-s3-control"),
        ExtendedKeyUsagePurpose::ClientAuth,
        &control_issuer,
    )
    .map_err(|_| "fixture_tls_invalid")?;
    for (name, contents) in [
        ("ca.pem", api.certificate_pem),
        ("server.crt", api_server.certificate_pem),
        ("server.key", api_server.private_key_pem),
        ("wrong-ca.pem", control.certificate_pem.clone()),
        ("grpc-ca.pem", control.certificate_pem),
        ("grpc-server.crt", control_server.certificate_pem),
        ("grpc-server.key", control_server.private_key_pem),
        ("grpc-client.crt", control_client.certificate_pem),
        ("grpc-client.key", control_client.private_key_pem),
        ("ordinary-client.crt", ordinary_client.certificate_pem),
        ("ordinary-client.key", ordinary_client.private_key_pem),
    ] {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(root.join(name))
            .map_err(|_| "fixture_write_failed")?;
        file.write_all(contents.as_bytes())
            .and_then(|()| file.sync_all())
            .map_err(|_| "fixture_write_failed")?;
    }
    File::open(root)
        .and_then(|directory| directory.sync_all())
        .map_err(|_| "fixture_sync_failed")
}

#[test]
#[ignore = "requires an explicitly owned, empty private S3 qualification output directory"]
fn export_isolated_s3_fixture_material() {
    let root = std::env::var_os("INSIGHT_S3_TLS_FIXTURE_DIRECTORY")
        .expect("explicit fixture output directory required");
    export(Path::new(&root)).expect("S3 fixture export failed with safe code");
}

#[test]
fn isolated_export_requires_private_empty_directory_and_exports_no_ca_keys() {
    let temporary = tempfile::tempdir().unwrap();
    let root = temporary.path().canonicalize().unwrap().join("tls");
    fs::create_dir(&root).unwrap();
    fs::set_permissions(&root, fs::Permissions::from_mode(0o755)).unwrap();
    assert!(export(&root).is_err());
    fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
    let alias = root.parent().unwrap().join("alias");
    std::os::unix::fs::symlink(&root, &alias).unwrap();
    assert!(export(&alias).is_err());
    export(&root).unwrap();
    assert!(export(&root).is_err());
    let mut names = fs::read_dir(&root)
        .unwrap()
        .map(|entry| {
            let entry = entry.unwrap();
            let metadata = entry.metadata().unwrap();
            assert_eq!(metadata.uid(), unsafe { libc::geteuid() });
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
            "grpc-ca.pem",
            "grpc-client.crt",
            "grpc-client.key",
            "grpc-server.crt",
            "grpc-server.key",
            "ordinary-client.crt",
            "ordinary-client.key",
            "server.crt",
            "server.key",
            "wrong-ca.pem"
        ]
    );
    assert_ne!(
        fs::read(root.join("ca.pem")).unwrap(),
        fs::read(root.join("grpc-ca.pem")).unwrap()
    );
}
