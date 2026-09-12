//! Explicit deployment release advancement; bootstrap and provider identities never change.
use insight_platform_deployment_contracts::{installation::*, installation_release::*};
use insight_platform_deployment_tooling::{
    installation::PreparedInstallation, private_state::InstallationDirectory,
};
use insight_platform_storage_tooling::conversation_upgrade;
use std::path::Path;

fn invalid() -> InstallationError {
    InstallationError::ConfigurationDrift
}

pub fn verify_release(
    installed: &InstallationInputV1,
    candidate: &InstallationInputV1,
    root: &Path,
) -> Result<(), InstallationError> {
    let prepared = PreparedInstallation::open_read_only(installed, root)?;
    if prepared.progress().phase != InstallationPhase::Ready {
        return Err(InstallationError::Incomplete);
    }
    let bytes = prepared
        .directory()
        .read(RELEASE_FILE, INSTALLATION_MAX_BYTES)?
        .ok_or_else(invalid)?;
    let release: InstallationReleaseV1 = serde_json::from_slice(&bytes).map_err(|_| invalid())?;
    release.validate_for(installed, prepared.identity())?;
    let mut expected = installed.clone();
    expected.package_digest = release.to_package_digest.clone();
    if candidate.digest()? != expected.digest()?
        || release.to_inventory_digest.as_str() != conversation_upgrade::target_inventory_digest()
    {
        return Err(invalid());
    }
    if prepared
        .directory()
        .read(UPGRADE_INTENT_FILE, INSTALLATION_MAX_BYTES)?
        != Some(bytes)
    {
        let intent: PackageRolloutIntentV1 = serde_json::from_slice(
            &prepared
                .directory()
                .read(
                    &PackageRolloutIntentV1::filename(&release.canonical_digest()?),
                    INSTALLATION_MAX_BYTES,
                )?
                .ok_or_else(invalid)?,
        )
        .map_err(|_| invalid())?;
        intent.validate_for(installed, prepared.identity())?;
        if intent.target_release != release {
            return Err(invalid());
        }
    }
    Ok(())
}

fn publish_rollout(
    prepared: &PreparedInstallation,
    intent: &PackageRolloutIntentV1,
) -> Result<(), InstallationError> {
    intent.validate_for(prepared.input(), prepared.identity())?;
    let current: InstallationReleaseV1 = serde_json::from_slice(
        &prepared
            .directory()
            .read(RELEASE_FILE, INSTALLATION_MAX_BYTES)?
            .ok_or_else(invalid)?,
    )
    .map_err(|_| invalid())?;
    if current != intent.previous_release && current != intent.target_release {
        return Err(invalid());
    }
    prepared.directory().replace(
        RELEASE_FILE,
        &serde_json::to_vec_pretty(&intent.target_release).map_err(|_| invalid())?,
    )
}

pub fn release_digest() -> Result<(), InstallationError> {
    let public = InstallationDirectory::open(Path::new("/installation-input/prepared"), false)?;
    let input = InstallationInputV1::decode(
        &public
            .read("input.json", INSTALLATION_MAX_BYTES)?
            .ok_or_else(invalid)?,
    )?;
    let prepared = PreparedInstallation::open(&input, Path::new("/installation/private"))?;
    let release: InstallationReleaseV1 = serde_json::from_slice(
        &prepared
            .directory()
            .read(RELEASE_FILE, INSTALLATION_MAX_BYTES)?
            .ok_or_else(invalid)?,
    )
    .map_err(|_| invalid())?;
    release.validate_for(&input, prepared.identity())?;
    println!("{}", release.canonical_digest()?);
    Ok(())
}

pub async fn rollout(
    expected_previous: insight_platform_contracts::Sha256Digest,
) -> Result<(), InstallationError> {
    use insight_platform_deployment_tooling::role_output::RoleOutputMode;
    let public = InstallationDirectory::open(Path::new("/installation-input/prepared"), false)?;
    let input = InstallationInputV1::decode(
        &public
            .read("input.json", INSTALLATION_MAX_BYTES)?
            .ok_or_else(invalid)?,
    )?;
    let prepared = PreparedInstallation::open(&input, Path::new("/installation/private"))?;
    if prepared.progress().phase != InstallationPhase::Ready {
        return Err(InstallationError::Incomplete);
    }
    let current: InstallationReleaseV1 = serde_json::from_slice(
        &prepared
            .directory()
            .read(RELEASE_FILE, INSTALLATION_MAX_BYTES)?
            .ok_or_else(invalid)?,
    )
    .map_err(|_| invalid())?;
    current.validate_for(&input, prepared.identity())?;
    let mut target = current.clone();
    target.to_package_digest =
        crate::compose_startup::executable_inventory(Path::new("/usr/local/bin"))?;
    let target_digest = target.canonical_digest()?;
    let filename = PackageRolloutIntentV1::filename(&target_digest);
    let intent = if let Some(bytes) = prepared
        .directory()
        .read(&filename, INSTALLATION_MAX_BYTES)?
    {
        serde_json::from_slice::<PackageRolloutIntentV1>(&bytes).map_err(|_| invalid())?
    } else {
        PackageRolloutIntentV1 {
            schema_version: 1,
            expected_previous_release_digest: expected_previous.clone(),
            previous_release: current.clone(),
            target_release: target.clone(),
        }
    };
    intent.validate_for(&input, prepared.identity())?;
    if intent.expected_previous_release_digest != expected_previous
        || intent.target_release != target
        || (current != intent.previous_release && current != intent.target_release)
        || target.to_inventory_digest.as_str() != conversation_upgrade::target_inventory_digest()
    {
        return Err(invalid());
    }
    prepared.directory().write_immutable(
        &filename,
        &serde_json::to_vec_pretty(&intent).map_err(|_| invalid())?,
    )?;
    let password = zeroize::Zeroizing::new(
        prepared
            .directory()
            .read("postgres-admin-password", 32)?
            .ok_or_else(invalid)?,
    );
    let options = sqlx::postgres::PgConnectOptions::new()
        .host(&input.network.database.host)
        .port(input.network.database.port)
        .database(&input.network.database.database)
        .username("insight_installation_admin")
        .password(std::str::from_utf8(&password).map_err(|_| invalid())?)
        .ssl_mode(sqlx::postgres::PgSslMode::Disable);
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await
        .map_err(|_| InstallationError::PrerequisiteUnavailable)?;
    conversation_upgrade::rollout_package(&pool, &intent)
        .await
        .map_err(|error| {
            eprintln!("{error}");
            invalid()
        })?;
    pool.close().await;
    drop(prepared);
    let prepared = crate::workflow::configure_upgrade(
        &input,
        Path::new("/installation/private"),
        Path::new("/output"),
        Path::new("/usr/local/bin"),
        RoleOutputMode::Rollout(target_digest),
    )
    .await?;
    publish_rollout(&prepared, &intent)?;
    println!("Package rollout committed; schema, bootstrap identity and business data retained.");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn package_acceptance_requires_completed_exact_release_and_unchanged_bootstrap() {
        let temp = tempfile::tempdir().unwrap();
        let root = std::fs::canonicalize(temp.path()).unwrap().join("private");
        let input = insight_platform_deployment_tooling::installation::compose_input(
            "upgrade-test",
            format!("sha256:{}", "a".repeat(64)).parse().unwrap(),
        )
        .unwrap();
        let prepared = PreparedInstallation::prepare(&input, &root).unwrap();
        // Seed only the completed-phase evidence; this test exercises package publication,
        // while provider provisioning has its own integration qualification.
        let mut progress = prepared.progress().clone();
        progress.phase = InstallationPhase::Ready;
        prepared
            .directory()
            .replace("progress.json", &serde_json::to_vec(&progress).unwrap())
            .unwrap();
        let mut candidate = input.clone();
        candidate.package_digest = format!("sha256:{}", "b".repeat(64)).parse().unwrap();
        let release = InstallationReleaseV1 {
            schema_version: 1,
            installation_id: prepared.identity().installation_id.clone(),
            bootstrap_input_digest: input.digest().unwrap(),
            bootstrap_identity_digest: prepared.identity().digest().unwrap(),
            from_package_digest: input.package_digest.clone(),
            to_package_digest: candidate.package_digest.clone(),
            from_schema_version: 16,
            to_schema_version: 17,
            from_inventory_digest: SOURCE_INVENTORY_DIGEST.parse().unwrap(),
            to_inventory_digest: conversation_upgrade::target_inventory_digest()
                .parse()
                .unwrap(),
        };
        let bytes = serde_json::to_vec_pretty(&release).unwrap();
        prepared
            .directory()
            .write_immutable(UPGRADE_INTENT_FILE, &bytes)
            .unwrap();
        drop(prepared);
        assert!(verify_release(&input, &candidate, &root).is_err());
        let prepared = PreparedInstallation::open(&input, &root).unwrap();
        prepared
            .directory()
            .write_immutable(RELEASE_FILE, &bytes)
            .unwrap();
        drop(prepared);
        verify_release(&input, &candidate, &root).unwrap();
        candidate.package_digest = format!("sha256:{}", "c".repeat(64)).parse().unwrap();
        assert!(verify_release(&input, &candidate, &root).is_err());
        let prepared = PreparedInstallation::open(&input, &root).unwrap();
        let mut target = release.clone();
        target.to_package_digest = candidate.package_digest.clone();
        let intent = PackageRolloutIntentV1 {
            schema_version: 1,
            expected_previous_release_digest: release.canonical_digest().unwrap(),
            previous_release: release.clone(),
            target_release: target.clone(),
        };
        prepared
            .directory()
            .write_immutable(
                &PackageRolloutIntentV1::filename(&target.canonical_digest().unwrap()),
                &serde_json::to_vec(&intent).unwrap(),
            )
            .unwrap();
        let mut wrong = intent.clone();
        wrong.expected_previous_release_digest =
            format!("sha256:{}", "d".repeat(64)).parse().unwrap();
        assert!(publish_rollout(&prepared, &wrong).is_err());
        drop(prepared);
        assert!(verify_release(&input, &candidate, &root).is_err());
        let prepared = PreparedInstallation::open(&input, &root).unwrap();
        publish_rollout(&prepared, &intent).unwrap();
        publish_rollout(&prepared, &intent).unwrap();
        drop(prepared);
        verify_release(&input, &candidate, &root).unwrap();
        candidate.package_digest = target.to_package_digest;
        candidate.name = "different-installation".into();
        assert!(verify_release(&input, &candidate, &root).is_err());
    }
}

pub async fn run() -> Result<(), InstallationError> {
    let public = InstallationDirectory::open(Path::new("/installation-input/prepared"), false)?;
    let input = InstallationInputV1::decode(
        &public
            .read("input.json", INSTALLATION_MAX_BYTES)?
            .ok_or_else(invalid)?,
    )?;
    let prepared = PreparedInstallation::open(&input, Path::new("/installation/private"))?;
    if prepared.progress().phase != InstallationPhase::Ready {
        return Err(InstallationError::Incomplete);
    }
    let release = InstallationReleaseV1 {
        schema_version: 1,
        installation_id: prepared.identity().installation_id.clone(),
        bootstrap_input_digest: input.digest()?,
        bootstrap_identity_digest: prepared.identity().digest()?,
        from_package_digest: input.package_digest.clone(),
        to_package_digest: crate::compose_startup::executable_inventory(Path::new(
            "/usr/local/bin",
        ))?,
        from_schema_version: SOURCE_SCHEMA_VERSION,
        to_schema_version: TARGET_SCHEMA_VERSION,
        from_inventory_digest: SOURCE_INVENTORY_DIGEST.parse().map_err(|_| invalid())?,
        to_inventory_digest: conversation_upgrade::target_inventory_digest()
            .parse()
            .map_err(|_| invalid())?,
    };
    release.validate_for(&input, prepared.identity())?;
    if let Some(bytes) = prepared
        .directory()
        .read(RELEASE_FILE, INSTALLATION_MAX_BYTES)?
    {
        let current: InstallationReleaseV1 =
            serde_json::from_slice(&bytes).map_err(|_| invalid())?;
        if current != release {
            return Err(invalid());
        }
    }
    let bytes = serde_json::to_vec_pretty(&release).map_err(|_| invalid())?;
    prepared
        .directory()
        .write_immutable(UPGRADE_INTENT_FILE, &bytes)?;
    let password = zeroize::Zeroizing::new(
        prepared
            .directory()
            .read("postgres-admin-password", 32)?
            .ok_or_else(invalid)?,
    );
    let options = sqlx::postgres::PgConnectOptions::new()
        .host(&input.network.database.host)
        .port(input.network.database.port)
        .database(&input.network.database.database)
        .username("insight_installation_admin")
        .password(std::str::from_utf8(&password).map_err(|_| invalid())?)
        .ssl_mode(sqlx::postgres::PgSslMode::Disable);
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await
        .map_err(|_| InstallationError::PrerequisiteUnavailable)?;
    let mut evidence = Vec::new();
    for purpose in [
        "runtime",
        "local-identity",
        "outbox",
        "history",
        "security-authority",
        "artifact",
    ] {
        let name = format!("database-role-{purpose}.json");
        let old: InstallationDatabaseEvidenceV1 = serde_json::from_slice(
            &prepared
                .directory()
                .read(&name, INSTALLATION_MAX_BYTES)?
                .ok_or_else(invalid)?,
        )
        .map_err(|_| invalid())?;
        if old.input_digest != input.digest()?
            || old.identity_digest != prepared.identity().digest()?
        {
            return Err(invalid());
        }
        conversation_upgrade::refresh_role_evidence(&pool, &old)
            .await
            .map_err(|_| invalid())?;
        evidence.push((name, old));
    }
    conversation_upgrade::upgrade(&pool, &release)
        .await
        .map_err(|error| {
            eprintln!("{error}");
            invalid()
        })?;
    for (name, old) in evidence {
        let next = conversation_upgrade::refresh_role_evidence(&pool, &old)
            .await
            .map_err(|_| invalid())?;
        prepared
            .directory()
            .replace(&name, &serde_json::to_vec(&next).map_err(|_| invalid())?)?;
    }
    pool.close().await;
    drop(prepared);
    let prepared = crate::workflow::configure_upgrade(
        &input,
        Path::new("/installation/private"),
        Path::new("/output"),
        Path::new("/usr/local/bin"),
        insight_platform_deployment_tooling::role_output::RoleOutputMode::Upgrade,
    )
    .await?;
    // Release is the final durable publication; a crash earlier replays the exact intent.
    prepared.directory().write_immutable(RELEASE_FILE, &bytes)?;
    println!("Conversation schema upgrade committed; bootstrap identity and data retained.");
    Ok(())
}
