//! Test-only export for the isolated physical TLS qualification. No CA private key is exported.
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

fn export_material(root: &Path) -> Result<(), &'static str> {
    if !root.is_absolute()
        || root
            .components()
            .any(|part| matches!(part, Component::CurDir | Component::ParentDir))
    {
        return Err("fixture_directory_invalid");
    }
    for ancestor in root.ancestors() {
        let metadata = fs::symlink_metadata(ancestor).map_err(|_| "fixture_directory_invalid")?;
        if !metadata.is_dir() || metadata.file_type().is_symlink() {
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
        return Err("fixture_directory_must_be_owned_private_empty");
    }
    let authority = create_authority().map_err(|_| "fixture_material_invalid")?;
    let issuer = Issuer::new(
        authority_parameters().map_err(|_| "fixture_material_invalid")?,
        KeyPair::from_pem(&authority.private_key_pem).map_err(|_| "fixture_material_invalid")?,
    );
    let leaf = create_leaf(
        &[
            "localstack.tls-qualification.svc.cluster.local",
            "localhost.localstack.cloud",
        ],
        None,
        ExtendedKeyUsagePurpose::ServerAuth,
        &issuer,
    )
    .map_err(|_| "fixture_material_invalid")?;
    let wrong_authority = create_authority().map_err(|_| "fixture_material_invalid")?;
    for (name, contents) in [
        ("ca.pem", authority.certificate_pem),
        ("server.crt", leaf.certificate_pem),
        ("server.key", leaf.private_key_pem),
        ("wrong-ca.pem", wrong_authority.certificate_pem),
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
        .map_err(|_| "fixture_write_failed")
}

#[test]
#[ignore = "requires an explicit, empty, private qualification output directory"]
fn export_shared_producer_tls_fixture_material() {
    let directory = std::env::var_os("INSIGHT_TLS_FIXTURE_DIRECTORY")
        .expect("explicit fixture directory is required");
    export_material(Path::new(&directory)).expect("shared TLS fixture export failed");
}

#[test]
fn exporter_rejects_existing_files_public_directory_and_symlink_ancestors() {
    let temporary = tempfile::tempdir().unwrap();
    let base = temporary.path().canonicalize().unwrap();
    let root = base.join("private");
    fs::create_dir(&root).unwrap();
    fs::set_permissions(&root, fs::Permissions::from_mode(0o755)).unwrap();
    assert!(export_material(&root).is_err());
    fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
    fs::write(root.join("foreign"), b"unchanged").unwrap();
    assert!(export_material(&root).is_err());
    assert_eq!(fs::read_dir(&root).unwrap().count(), 1);
    fs::remove_file(root.join("foreign")).unwrap();
    std::os::unix::fs::symlink(&base, base.join("alias")).unwrap();
    assert!(export_material(&base.join("alias/private")).is_err());
    export_material(&root).unwrap();
    let mut names = fs::read_dir(&root)
        .unwrap()
        .map(|entry| {
            let entry = entry.unwrap();
            let metadata = entry.metadata().unwrap();
            assert!(metadata.is_file());
            assert_eq!(metadata.nlink(), 1);
            assert_eq!(metadata.uid(), unsafe { libc::geteuid() });
            assert_eq!(metadata.permissions().mode() & 0o777, 0o600);
            entry.file_name().into_string().unwrap()
        })
        .collect::<Vec<_>>();
    names.sort();
    assert_eq!(
        names,
        ["ca.pem", "server.crt", "server.key", "wrong-ca.pem"]
    );
    assert!(export_material(&root).is_err());
}
