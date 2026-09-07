//! Local derived configuration replacement; profile.json remains the sole selected runtime owner.
//! Callers hold the runtime lifecycle lock and require all previously owned processes stopped.
use super::*;

const JOURNAL_FILE: &str = "config-transition.json";
const STAGING_PREFIX: &str = "config-transition-";
const MAX_TRANSITION_FILES: usize = 64;
const MAX_TRANSITION_TOTAL_BYTES: u64 = 16 * 1024 * 1024;

/// Current-only, bounded recovery evidence, never an alternative runtime authority.
/// File names and old/new digests derive exclusively from the two exact owning profiles.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RuntimeConfigTransitionV1 {
    schema_version: u32,
    previous_profile: RuntimeProfileState,
    target_profile: RuntimeProfileState,
    staged_directory: String,
}

fn invalid(detail: &str) -> CliError {
    CliError::RuntimeState(format!("runtime config transition rejected: {detail}"))
}
fn physical_directory(path: &Path) -> Result<(), CliError> {
    let metadata = fs::symlink_metadata(path).map_err(|_| invalid("directory unavailable"))?;
    if !metadata.file_type().is_dir() {
        return Err(invalid("directory is not physical"));
    }
    Ok(())
}
fn no_live_roles(
    runtime: &Path,
    identity: &LocalIdentityState,
    previous: &RuntimeProfileState,
) -> Result<(), CliError> {
    let binding = runtime_process_binding_for_cleanup(
        runtime,
        &identity.tenant_id,
        &compose_project_name(&identity.tenant_id)?,
        previous,
        identity,
    )?;
    if let Some(state) = read_runtime_process_state(runtime, &binding)? {
        for process in state.processes.values() {
            if observe_runtime_process(process)? == RuntimeProcessObservation::Owned {
                return Err(invalid(
                    "an owned role is live; run insight stop before dev",
                ));
            }
        }
    }
    Ok(())
}

fn config(path: &Path) -> Result<(serde_json::Value, String, u64), CliError> {
    // The shared reader is strict JSON, <=4 MiB, no symlink/hardlink, and inode-stable.
    let value =
        read_runtime_json::<serde_json::Value>(path)?.ok_or_else(|| invalid("config missing"))?;
    let bytes = serde_json::to_vec(&value).map_err(|_| invalid("config encoding"))?;
    let digest = canonical_digest(&value).map_err(|_| invalid("config digest"))?;
    let raw_bytes = fs::symlink_metadata(path)
        .map_err(|_| invalid("config byte count"))?
        .len();
    Ok((value, digest, raw_bytes.max(bytes.len() as u64)))
}
fn write_private(path: &Path, value: &impl Serialize) -> Result<(), CliError> {
    let bytes = serde_json::to_vec_pretty(value).map_err(|_| invalid("JSON encoding"))?;
    if bytes.len() > 4 * 1024 * 1024 {
        return Err(invalid("file byte bound"));
    }
    let temporary = path.with_extension(format!("{}.tmp", Uuid::now_v7()));
    let result = (|| {
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        options
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
        let mut file = options
            .open(&temporary)
            .map_err(|_| invalid("temporary file creation"))?;
        file.write_all(&bytes)
            .and_then(|_| file.sync_all())
            .map_err(|_| invalid("temporary file persistence"))?;
        fs::rename(&temporary, path).map_err(|_| invalid("atomic file replacement"))?;
        sync_directory(path.parent().ok_or_else(|| invalid("file parent"))?)
    })();
    if result.is_err() {
        let _ = fs::remove_file(temporary);
    }
    result
}
fn sync_directory(path: &Path) -> Result<(), CliError> {
    #[cfg(unix)]
    fs::File::open(path)
        .and_then(|file| file.sync_all())
        .map_err(|_| invalid("directory persistence"))?;
    Ok(())
}

/// Captured only after the complete prior profile has been validated for this local identity.
/// The immutable bootstrap is input to derived roles, not an independently selected generation.
pub(super) struct PreservedRuntimeInputs<'a> {
    pub(super) profile: &'a RuntimeProfileState,
    pub(super) artifact_bootstrap:
        insight_platform_deployment_contracts::development::DevelopmentArtifactAuthorityConfigV1,
    pub(super) artifact_bootstrap_value: serde_json::Value,
}
fn bootstrap_config(
    path: &Path,
    expected: &str,
) -> Result<
    (
        insight_platform_deployment_contracts::development::DevelopmentArtifactAuthorityConfigV1,
        serde_json::Value,
        u64,
    ),
    CliError,
> {
    use insight_platform_deployment_contracts::development::{
        DevelopmentArtifactAuthorityConfigV1, MAX_DEVELOPMENT_ARTIFACT_BOOTSTRAP_BYTES,
    };
    let bytes = read_runtime_file_bytes(path, MAX_DEVELOPMENT_ARTIFACT_BOOTSTRAP_BYTES as u64)?
        .ok_or_else(|| invalid("preserved bootstrap missing"))?;
    let expected = expected
        .parse()
        .map_err(|_| invalid("preserved bootstrap digest invalid"))?;
    let typed = DevelopmentArtifactAuthorityConfigV1::decode(&bytes, &expected)
        .map_err(|_| invalid("preserved bootstrap invalid or drifted"))?;
    // The owning decoder has validated these same original bytes, including duplicate keys.
    let value = serde_json::from_slice(&bytes).map_err(|_| invalid("preserved bootstrap JSON"))?;
    Ok((typed, value, bytes.len() as u64))
}
impl<'a> PreservedRuntimeInputs<'a> {
    fn capture(runtime: &Path, previous: &'a RuntimeProfileState) -> Result<Self, CliError> {
        let expected = previous
            .config_digests
            .get("artifact-bootstrap")
            .ok_or_else(|| invalid("preserved bootstrap digest missing"))?;
        let (artifact_bootstrap, artifact_bootstrap_value, _) = bootstrap_config(
            &runtime
                .join(RUNTIME_CONFIGURATION_DIRECTORY)
                .join(RUNTIME_ARTIFACT_BOOTSTRAP_CONFIG_FILE),
            expected,
        )?;
        Ok(Self {
            profile: previous,
            artifact_bootstrap,
            artifact_bootstrap_value,
        })
    }
}

pub(super) fn regenerate(
    state_directory: &Path,
    identity: &LocalIdentityState,
    previous: &RuntimeProfileState,
    selected: DevProfile,
    source: RuntimeProfileSource<'_>,
) -> Result<RuntimeProfileState, CliError> {
    let runtime = state_directory.join(RUNTIME_DIRECTORY);
    if runtime.join(JOURNAL_FILE).exists() {
        return Err(invalid("recover the existing journal first"));
    }
    validate_runtime_profile_state(
        &runtime.join(RUNTIME_PROFILE_STATE_FILE),
        previous,
        identity,
    )?;
    no_live_roles(&runtime, identity, previous)?;
    let preserved = PreservedRuntimeInputs::capture(&runtime, previous)?;
    let worker_builds = worker_profile::WorkerBuilds::read(source.binary_directory, selected)?;
    let stage_name = format!("{STAGING_PREFIX}{}", Uuid::now_v7());
    let stage = runtime.join(&stage_name);
    let mut builder = fs::DirBuilder::new();
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
    }
    builder
        .create(&stage)
        .map_err(|_| invalid("private staging creation"))?;
    let result = (|| {
        let target = prepare_runtime_profile_inner(
            state_directory,
            &stage,
            identity,
            &previous.kms_key_arn,
            &previous.secret_readiness_arn,
            RuntimeProfileSelection {
                preserved: Some(&preserved),
                worker_builds: &worker_builds,
                source_fingerprint: source.source_fingerprint,
                ports: &previous.ports,
                selected_profile: selected,
                release_identity: source.release_identity,
            },
        )?;
        let journal = RuntimeConfigTransitionV1 {
            schema_version: 1,
            previous_profile: previous.clone(),
            target_profile: target.clone(),
            staged_directory: stage_name,
        };
        preflight(&runtime, identity, &journal)?;
        // Every staged file is fully persisted before the durable journal advertises it.
        for entry in fs::read_dir(&stage).map_err(|_| invalid("staging enumeration"))? {
            fs::File::open(entry.map_err(|_| invalid("staging entry"))?.path())
                .and_then(|file| file.sync_all())
                .map_err(|_| invalid("staged file persistence"))?;
        }
        sync_directory(&stage)?;
        write_private(&runtime.join(JOURNAL_FILE), &journal)?;
        recover(&runtime, identity)?;
        Ok(target)
    })();
    if result.is_err() && !runtime.join(JOURNAL_FILE).exists() {
        let _ = fs::remove_dir_all(&stage);
    }
    result
}

/// Entire batch validation precedes the first mutation. Any drift leaves all files untouched.
fn preflight(
    runtime: &Path,
    identity: &LocalIdentityState,
    journal: &RuntimeConfigTransitionV1,
) -> Result<BTreeMap<String, serde_json::Value>, CliError> {
    if journal.schema_version != 1 {
        return Err(invalid("unsupported journal version"));
    }
    let suffix = journal
        .staged_directory
        .strip_prefix(STAGING_PREFIX)
        .ok_or_else(|| invalid("staging name"))?;
    let uuid = Uuid::parse_str(suffix).map_err(|_| invalid("staging identity"))?;
    if uuid.get_version_num() != 7 || uuid.hyphenated().to_string() != suffix {
        return Err(invalid("staging identity"));
    }
    physical_directory(runtime)?;
    let stage = runtime.join(&journal.staged_directory);
    physical_directory(&stage)?;
    #[cfg(unix)]
    if fs::symlink_metadata(&stage)
        .map_err(|_| invalid("staging metadata"))?
        .mode()
        & 0o077
        != 0
    {
        return Err(invalid("staging directory must be private"));
    }
    let path = runtime.join(RUNTIME_PROFILE_STATE_FILE);
    let previous = &journal.previous_profile;
    let target = &journal.target_profile;
    let before = validate_runtime_profile_state_for_cleanup(&path, previous, identity)?;
    let after = validate_runtime_profile_state_for_cleanup(&path, target, identity)?;
    validate_runtime_profile_transition(
        previous,
        after,
        &target.release_identity,
        &target.source_fingerprint,
    )?;
    if previous.ports != target.ports
        || previous.secret_provider_id != target.secret_provider_id
        || previous.capability_protocol_profile_revision_id
            != target.capability_protocol_profile_revision_id
        || previous.kms_key_arn != target.kms_key_arn
        || previous.secret_readiness_arn != target.secret_readiness_arn
        || previous.s3_bucket != target.s3_bucket
        || previous.config_digests.get("artifact-bootstrap")
            != target.config_digests.get("artifact-bootstrap")
    {
        return Err(invalid("stable local identity was rotated"));
    }
    let actual = read_runtime_json::<RuntimeProfileState>(&path)?
        .ok_or_else(|| invalid("journal cannot initialize a missing profile"))?;
    if &actual != previous && &actual != target {
        return Err(invalid("profile has an unknown generation"));
    }
    no_live_roles(runtime, identity, previous)?;
    let old_files = expected_runtime_closure(previous, before).config_files;
    let files = expected_runtime_closure(target, after).config_files;
    if files.len() > MAX_TRANSITION_FILES || !old_files.keys().all(|key| files.contains_key(key)) {
        return Err(invalid("config role bound or removal"));
    }
    let expected_names = files.values().copied().collect::<BTreeSet<_>>();
    let observed = fs::read_dir(&stage)
        .map_err(|_| invalid("staging enumeration"))?
        .map(|entry| {
            entry
                .map(|entry| entry.file_name().to_string_lossy().into_owned())
                .map_err(|_| invalid("staging entry"))
        })
        .collect::<Result<BTreeSet<_>, _>>()?;
    if observed.iter().map(String::as_str).collect::<BTreeSet<_>>() != expected_names {
        return Err(invalid("staging file closure differs"));
    }
    let config_dir = runtime.join(RUNTIME_CONFIGURATION_DIRECTORY);
    physical_directory(&config_dir)?;
    let current_names = fs::read_dir(&config_dir)
        .map_err(|_| invalid("config directory enumeration"))?
        .map(|entry| {
            entry
                .map(|entry| entry.file_name().to_string_lossy().into_owned())
                .map_err(|_| invalid("config directory entry"))
        })
        .collect::<Result<BTreeSet<_>, _>>()?;
    if current_names
        .iter()
        .any(|name| !expected_names.contains(name.as_str()))
    {
        return Err(invalid("unexpected existing config file"));
    }
    let mut total = 0u64;
    let mut staged = BTreeMap::new();
    for (role, name) in files {
        let (value, digest, bytes) = if role == "artifact-bootstrap" {
            let digest = target
                .config_digests
                .get(&role)
                .ok_or_else(|| invalid("target bootstrap digest missing"))?;
            let (_, value, bytes) = bootstrap_config(&stage.join(name), digest)?;
            (value, digest.clone(), bytes)
        } else {
            config(&stage.join(name))?
        };
        total = total
            .checked_add(bytes)
            .ok_or_else(|| invalid("aggregate bytes"))?;
        if total > MAX_TRANSITION_TOTAL_BYTES || target.config_digests.get(&role) != Some(&digest) {
            return Err(invalid("staging digest or aggregate byte bound"));
        }
        let current = config_dir.join(name);
        match fs::symlink_metadata(&current) {
            Ok(_) => {
                let observed = if role == "artifact-bootstrap" {
                    bootstrap_config(&current, &digest)?;
                    digest.clone()
                } else {
                    config(&current)?.1
                };
                if Some(&observed) != previous.config_digests.get(&role) && observed != digest {
                    return Err(invalid("config has an unknown old/new digest"));
                }
                if &actual == target && observed != digest {
                    return Err(invalid("committed profile config differs"));
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                if old_files.contains_key(&role) || &actual == target {
                    return Err(invalid("previous or committed config missing"));
                }
            }
            Err(_) => return Err(invalid("config metadata")),
        }
        staged.insert(name.to_owned(), value);
    }
    Ok(staged)
}

/// Unlocked readers never consume a partially replaced configuration generation.
pub(super) fn reject_pending(runtime: &Path) -> Result<(), CliError> {
    match fs::symlink_metadata(runtime.join(JOURNAL_FILE)) {
        Ok(_) => Err(invalid(
            "pending journal requires a locked insight status/start/dev/stop recovery",
        )),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(_) => Err(invalid("journal metadata unavailable")),
    }
}

pub(super) fn recover(runtime: &Path, identity: &LocalIdentityState) -> Result<(), CliError> {
    let path = runtime.join(JOURNAL_FILE);
    let Some(journal) = read_runtime_json::<RuntimeConfigTransitionV1>(&path)? else {
        return Ok(());
    };
    let staged = preflight(runtime, identity, &journal)?;
    for (name, value) in staged {
        // Preflight proved this stable input has the same previous/target/staged/current digest.
        // Never replace its original bytes when only derived runtime configuration changes.
        if name == RUNTIME_ARTIFACT_BOOTSTRAP_CONFIG_FILE {
            continue;
        }
        write_private(
            &runtime.join(RUNTIME_CONFIGURATION_DIRECTORY).join(name),
            &value,
        )?;
    }
    // The only commit point: readers select the exact target through the existing profile owner.
    write_private(
        &runtime.join(RUNTIME_PROFILE_STATE_FILE),
        &journal.target_profile,
    )?;
    validate_runtime_profile_state(
        &runtime.join(RUNTIME_PROFILE_STATE_FILE),
        &journal.target_profile,
        identity,
    )?;
    remove_runtime_state_file(&path)?;
    fs::remove_dir_all(runtime.join(journal.staged_directory))
        .map_err(|_| invalid("owned staging cleanup"))?;
    sync_directory(runtime)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[cfg(unix)]
    struct TestChild(std::process::Child);
    #[cfg(unix)]
    impl Drop for TestChild {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }

    fn setup() -> (TempDir, LocalIdentityState, RuntimeProfileState, PathBuf) {
        let directory = TempDir::new().unwrap();
        let project =
            initialize_project(directory.path(), Some("transition"), SystemTime::now()).unwrap();
        let binaries = worker_profile::fixture_binaries(directory.path());
        prepare_runtime_profile(
            directory.path(),
            "arn:aws:kms:us-east-1:000000000000:key/12345678-1234-1234-1234-123456789012",
            "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            &binaries,
        )
        .unwrap();
        let runtime = directory
            .path()
            .join(PROJECT_DIRECTORY)
            .join(RUNTIME_DIRECTORY);
        let profile = read_runtime_profile_state(&runtime, &project.identity)
            .unwrap()
            .unwrap();
        (directory, project.identity, profile, binaries)
    }
    fn staged(
        directory: &Path,
        identity: &LocalIdentityState,
        previous: &RuntimeProfileState,
        binaries: &Path,
        features: &str,
    ) -> RuntimeConfigTransitionV1 {
        let state_directory = directory.join(PROJECT_DIRECTORY);
        let runtime = state_directory.join(RUNTIME_DIRECTORY);
        let selected = DevProfile::parse(Some(features), false, true).unwrap();
        ensure_selected_feature_identity(
            &state_directory,
            selected,
            Some(&previous.tls_identity_digests),
        )
        .unwrap();
        let name = format!("{STAGING_PREFIX}{}", Uuid::now_v7());
        let stage = runtime.join(&name);
        let mut builder = fs::DirBuilder::new();
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            builder.mode(0o700);
        }
        builder.create(&stage).unwrap();
        let workers = worker_profile::WorkerBuilds::read(binaries, selected).unwrap();
        let preserved = PreservedRuntimeInputs::capture(&runtime, previous).unwrap();
        let target = prepare_runtime_profile_inner(
            &state_directory,
            &stage,
            identity,
            &previous.kms_key_arn,
            &previous.secret_readiness_arn,
            RuntimeProfileSelection {
                preserved: Some(&preserved),
                worker_builds: &workers,
                source_fingerprint: &previous.source_fingerprint,
                ports: &previous.ports,
                selected_profile: selected,
                release_identity: &previous.release_identity,
            },
        )
        .unwrap();
        RuntimeConfigTransitionV1 {
            schema_version: 1,
            previous_profile: previous.clone(),
            target_profile: target,
            staged_directory: name,
        }
    }
    fn stage_journal(
        runtime: &Path,
        identity: &LocalIdentityState,
        journal: &RuntimeConfigTransitionV1,
    ) {
        preflight(runtime, identity, journal).unwrap();
        write_private(&runtime.join(JOURNAL_FILE), journal).unwrap();
    }
    #[test]
    fn interrupted_batches_recover_before_and_after_the_profile_commit_point() {
        for partial in [0usize, 1, usize::MAX] {
            for committed in [false, true] {
                if committed && partial != usize::MAX {
                    continue;
                }
                let (directory, identity, previous, binaries) = setup();
                let _lock = acquire_runtime_lifecycle_lock(directory.path()).unwrap();
                let runtime = directory
                    .path()
                    .join(PROJECT_DIRECTORY)
                    .join(RUNTIME_DIRECTORY);
                let cursor = fs::read(runtime.join(RUNTIME_CURSOR_KEY_FILE)).unwrap();
                let journal = staged(
                    directory.path(),
                    &identity,
                    &previous,
                    &binaries,
                    "remote-capability,mcp",
                );
                let configs = preflight(&runtime, &identity, &journal).unwrap();
                stage_journal(&runtime, &identity, &journal);
                for (name, value) in configs.iter().take(partial) {
                    write_private(
                        &runtime.join(RUNTIME_CONFIGURATION_DIRECTORY).join(name),
                        value,
                    )
                    .unwrap();
                }
                if committed {
                    write_private(
                        &runtime.join(RUNTIME_PROFILE_STATE_FILE),
                        &journal.target_profile,
                    )
                    .unwrap();
                }
                recover(&runtime, &identity).unwrap();
                let actual = read_runtime_profile_state(&runtime, &identity)
                    .unwrap()
                    .unwrap();
                assert_eq!(actual, journal.target_profile);
                assert_eq!(
                    fs::read(runtime.join(RUNTIME_CURSOR_KEY_FILE)).unwrap(),
                    cursor
                );
                assert_eq!(actual.secret_provider_id, previous.secret_provider_id);
                assert_eq!(
                    actual.capability_protocol_profile_revision_id,
                    previous.capability_protocol_profile_revision_id
                );
                assert!(!runtime.join(JOURNAL_FILE).exists());
                assert!(!runtime.join(journal.staged_directory).exists());
                recover(&runtime, &identity).unwrap();
            }
        }
    }
    #[test]
    fn unknown_file_digest_rejects_the_entire_batch_without_partial_writes() {
        let (directory, identity, previous, binaries) = setup();
        let _lock = acquire_runtime_lifecycle_lock(directory.path()).unwrap();
        let runtime = directory
            .path()
            .join(PROJECT_DIRECTORY)
            .join(RUNTIME_DIRECTORY);
        let journal = staged(
            directory.path(),
            &identity,
            &previous,
            &binaries,
            "remote-capability",
        );
        stage_journal(&runtime, &identity, &journal);
        let path = runtime
            .join(RUNTIME_CONFIGURATION_DIRECTORY)
            .join(RUNTIME_GATEWAY_RUNTIME_CONFIG_FILE);
        let mut drift = read_runtime_json::<serde_json::Value>(&path)
            .unwrap()
            .unwrap();
        drift["unknown"] = serde_json::json!(true);
        write_private(&path, &drift).unwrap();
        let before = fs::read(runtime.join(RUNTIME_PROFILE_STATE_FILE)).unwrap();
        let original = fs::read(
            runtime
                .join(RUNTIME_CONFIGURATION_DIRECTORY)
                .join(RUNTIME_ARTIFACT_DATA_CONFIG_FILE),
        )
        .unwrap();
        assert!(recover(&runtime, &identity).is_err());
        assert_eq!(
            fs::read(runtime.join(RUNTIME_PROFILE_STATE_FILE)).unwrap(),
            before
        );
        assert_eq!(
            fs::read(
                runtime
                    .join(RUNTIME_CONFIGURATION_DIRECTORY)
                    .join(RUNTIME_ARTIFACT_DATA_CONFIG_FILE)
            )
            .unwrap(),
            original
        );
        assert!(runtime.join(JOURNAL_FILE).exists());
    }
    #[test]
    fn current_remote_profile_can_add_mcp_without_retaining_old_worker_capabilities() {
        let (directory, identity, previous, binaries) = setup();
        let _lock = acquire_runtime_lifecycle_lock(directory.path()).unwrap();
        let state_directory = directory.path().join(PROJECT_DIRECTORY);
        let runtime = state_directory.join(RUNTIME_DIRECTORY);
        let bootstrap_path = runtime
            .join(RUNTIME_CONFIGURATION_DIRECTORY)
            .join(RUNTIME_ARTIFACT_BOOTSTRAP_CONFIG_FILE);
        let original_bootstrap = fs::read(&bootstrap_path).unwrap();
        let remote = DevProfile::parse(Some("remote-capability"), false, true).unwrap();
        ensure_selected_feature_identity(
            &state_directory,
            remote,
            Some(&previous.tls_identity_digests),
        )
        .unwrap();
        let first = regenerate(
            &state_directory,
            &identity,
            &previous,
            remote,
            RuntimeProfileSource {
                source_fingerprint: &previous.source_fingerprint,
                release_identity: &previous.release_identity,
                binary_directory: &binaries,
            },
        )
        .unwrap();
        let path = runtime
            .join(RUNTIME_CONFIGURATION_DIRECTORY)
            .join(full_profile::CAPABILITY_REMOTE_CONFIG_FILE);
        let (before, _, _) = config(&path).unwrap();
        assert!(before["mcp_host"].is_null());
        let full = DevProfile::parse(Some("remote-capability,mcp"), false, true).unwrap();
        ensure_selected_feature_identity(&state_directory, full, Some(&first.tls_identity_digests))
            .unwrap();
        let second = regenerate(
            &state_directory,
            &identity,
            &first,
            full,
            RuntimeProfileSource {
                source_fingerprint: &first.source_fingerprint,
                release_identity: &first.release_identity,
                binary_directory: &binaries,
            },
        )
        .unwrap();
        let (after, _, _) = config(&path).unwrap();
        assert!(after["mcp_host"].is_object());
        assert!(!after["installed_mcp_codecs"].as_array().unwrap().is_empty());
        assert_ne!(
            before["worker_manifest"]["execution_capabilities"],
            after["worker_manifest"]["execution_capabilities"]
        );
        assert_eq!(fs::read(&bootstrap_path).unwrap(), original_bootstrap);
        assert_eq!(
            first.config_digests["artifact-bootstrap"],
            previous.config_digests["artifact-bootstrap"]
        );
        assert_eq!(
            second.config_digests["artifact-bootstrap"],
            previous.config_digests["artifact-bootstrap"]
        );
        assert_eq!(first.secret_provider_id, second.secret_provider_id);
        assert_eq!(first.ports, second.ports);
        assert_eq!(
            read_runtime_profile_state(&runtime, &identity).unwrap(),
            Some(second)
        );
    }
    #[test]
    fn changed_source_rebuilds_actual_executable_evidence_with_stable_local_ids() {
        let (directory, identity, previous, binaries) = setup();
        let _lock = acquire_runtime_lifecycle_lock(directory.path()).unwrap();
        let state_directory = directory.path().join(PROJECT_DIRECTORY);
        let bootstrap_path = state_directory
            .join(RUNTIME_DIRECTORY)
            .join(RUNTIME_CONFIGURATION_DIRECTORY)
            .join(RUNTIME_ARTIFACT_BOOTSTRAP_CONFIG_FILE);
        let mut original_bootstrap = fs::read(&bootstrap_path).unwrap();
        original_bootstrap.extend_from_slice(b" \n");
        fs::write(&bootstrap_path, &original_bootstrap).unwrap();
        fs::write(
            binaries.join("platform-orchestration-worker"),
            "new actual fixture executable bytes",
        )
        .unwrap();
        let fingerprint = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        let target = regenerate(
            &state_directory,
            &identity,
            &previous,
            DevProfile::parse(None, false, true).unwrap(),
            RuntimeProfileSource {
                source_fingerprint: fingerprint,
                release_identity: &format!("source:{fingerprint}"),
                binary_directory: &binaries,
            },
        )
        .unwrap();
        assert_eq!(fs::read(&bootstrap_path).unwrap(), original_bootstrap);
        assert_eq!(
            target.config_digests["artifact-bootstrap"],
            previous.config_digests["artifact-bootstrap"]
        );
        assert_eq!(target.source_fingerprint, fingerprint);
        assert_ne!(
            target.config_digests["orchestration"],
            previous.config_digests["orchestration"]
        );
        assert_eq!(target.secret_provider_id, previous.secret_provider_id);
        assert_eq!(
            target.capability_protocol_profile_revision_id,
            previous.capability_protocol_profile_revision_id
        );
        assert_eq!(target.ports, previous.ports);
    }

    #[test]
    fn journal_cannot_initialize_or_hide_unknown_versions_and_partial_reader_state() {
        for mode in [
            "missing-profile",
            "unknown-version",
            "unknown-staged-file",
            "pending-reader",
        ] {
            let (directory, identity, previous, binaries) = setup();
            let _lock = acquire_runtime_lifecycle_lock(directory.path()).unwrap();
            let runtime = directory
                .path()
                .join(PROJECT_DIRECTORY)
                .join(RUNTIME_DIRECTORY);
            let mut journal = staged(directory.path(), &identity, &previous, &binaries, "model");
            stage_journal(&runtime, &identity, &journal);
            match mode {
                "missing-profile" => {
                    fs::remove_file(runtime.join(RUNTIME_PROFILE_STATE_FILE)).unwrap()
                }
                "unknown-version" => {
                    journal.schema_version = 2;
                    write_private(&runtime.join(JOURNAL_FILE), &journal).unwrap();
                }
                "unknown-staged-file" => fs::write(
                    runtime.join(&journal.staged_directory).join("unknown.json"),
                    "{}",
                )
                .unwrap(),
                "pending-reader" => {
                    assert!(read_runtime_profile_state(&runtime, &identity).is_err());
                    recover(&runtime, &identity).unwrap();
                    continue;
                }
                _ => unreachable!(),
            }
            let original = fs::read(
                runtime
                    .join(RUNTIME_CONFIGURATION_DIRECTORY)
                    .join(RUNTIME_ARTIFACT_DATA_CONFIG_FILE),
            )
            .unwrap();
            assert!(recover(&runtime, &identity).is_err(), "{mode}");
            assert_eq!(
                fs::read(
                    runtime
                        .join(RUNTIME_CONFIGURATION_DIRECTORY)
                        .join(RUNTIME_ARTIFACT_DATA_CONFIG_FILE)
                )
                .unwrap(),
                original
            );
            assert!(runtime.join(JOURNAL_FILE).exists());
        }
    }

    #[cfg(unix)]
    #[test]
    fn symlinks_hardlinks_and_live_owned_processes_block_recovery() {
        use std::os::unix::{fs::symlink, process::CommandExt};
        for mode in ["symlink", "hardlink", "live"] {
            let (directory, identity, previous, binaries) = setup();
            let _lock = acquire_runtime_lifecycle_lock(directory.path()).unwrap();
            let runtime = directory
                .path()
                .join(PROJECT_DIRECTORY)
                .join(RUNTIME_DIRECTORY);
            let journal = staged(
                directory.path(),
                &identity,
                &previous,
                &binaries,
                "remote-capability",
            );
            stage_journal(&runtime, &identity, &journal);
            let mut child = None;
            let path = runtime
                .join(&journal.staged_directory)
                .join(RUNTIME_ARTIFACT_DATA_CONFIG_FILE);
            if mode == "live" {
                let generation = format!("{RUNTIME_PROCESS_GENERATION_PREFIX}{}", Uuid::now_v7());
                let process = ProcessCommand::new("/bin/sleep")
                    .arg0(&generation)
                    .arg("30")
                    .spawn()
                    .unwrap();
                let binding = runtime_process_binding_for_cleanup(
                    &runtime,
                    &identity.tenant_id,
                    &compose_project_name(&identity.tenant_id).unwrap(),
                    &previous,
                    &identity,
                )
                .unwrap();
                let (role, ready_address) = binding.expected_processes.iter().next().unwrap();
                let record = RuntimeProcessRecord {
                    pid: process.id(),
                    generation,
                    ready_address: ready_address.clone(),
                    log_file: format!("logs/{role}.log"),
                };
                child = Some(TestChild(process));
                for _ in 0..100 {
                    if observe_runtime_process(&record).unwrap() == RuntimeProcessObservation::Owned
                    {
                        break;
                    }
                    std::thread::sleep(Duration::from_millis(5));
                }
                assert_eq!(
                    observe_runtime_process(&record).unwrap(),
                    RuntimeProcessObservation::Owned
                );
                write_private(
                    &runtime.join(RUNTIME_PROCESS_STATE_FILE),
                    &RuntimeProcessState {
                        schema_version: RUNTIME_PROCESS_SCHEMA_VERSION,
                        kind: RUNTIME_PROCESS_KIND.into(),
                        tenant_id: identity.tenant_id.clone(),
                        profile: "starter".into(),
                        profile_digest: previous.profile_digest.clone(),
                        release_identity: previous.release_identity.clone(),
                        compose_project: compose_project_name(&identity.tenant_id).unwrap(),
                        source_fingerprint: previous.source_fingerprint.clone(),
                        lifecycle: RuntimeProcessLifecycle::Starting,
                        processes: BTreeMap::from([(role.clone(), record)]),
                    },
                )
                .unwrap();
            } else {
                fs::remove_file(&path).unwrap();
                let original = runtime
                    .join(RUNTIME_CONFIGURATION_DIRECTORY)
                    .join(RUNTIME_ARTIFACT_DATA_CONFIG_FILE);
                if mode == "symlink" {
                    symlink(original, &path).unwrap();
                } else {
                    fs::hard_link(original, &path).unwrap();
                }
            }
            let result = recover(&runtime, &identity);
            if let Some(mut process) = child {
                assert!(process.0.try_wait().unwrap().is_none());
            }
            assert!(result.is_err(), "{mode}");
            assert!(runtime.join(JOURNAL_FILE).exists());
        }
    }
    #[test]
    fn source_fingerprint_tracks_each_physical_source_owner_and_excludes_build_cache() {
        let directory = TempDir::new().unwrap();
        let root = directory.path();
        for file in [
            "Cargo.toml",
            "Cargo.lock",
            "crates/domain/lib.rs",
            "apps/services/worker/main.rs",
            "apps/insight-cli/src/lib.rs",
            "tools/rust/tool/src/main.rs",
            "contracts/manifest.json",
            "deploy/dev/compose.yaml",
            "deploy/release/profile.json",
        ] {
            let path = root.join(file);
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            fs::write(path, file).unwrap();
        }
        let mut fingerprint = workspace_fingerprint(root).unwrap();
        for file in [
            "apps/services/worker/main.rs",
            "apps/insight-cli/src/lib.rs",
            "tools/rust/tool/src/main.rs",
            "deploy/release/profile.json",
        ] {
            fs::write(root.join(file), format!("new {file}")).unwrap();
            let next = workspace_fingerprint(root).unwrap();
            assert_ne!(next, fingerprint);
            fingerprint = next;
        }
        for cache in [
            "target",
            ".git",
            "node_modules",
            "dist",
            ".cache",
            "cache",
            "build",
            "generated",
        ] {
            let path = root.join("apps/services").join(cache).join("noise");
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            fs::write(path, "generated").unwrap();
        }
        assert_eq!(workspace_fingerprint(root).unwrap(), fingerprint);
    }
    #[test]
    fn shared_bootstrap_decoder_closes_identity_policy_and_raw_byte_boundaries() {
        use insight_platform_deployment_contracts::development::{
            DevelopmentArtifactAuthorityConfigV1 as Config,
            MAX_DEVELOPMENT_ARTIFACT_BOOTSTRAP_BYTES,
        };
        let (directory, _, profile, _) = setup();
        let path = directory
            .path()
            .join(PROJECT_DIRECTORY)
            .join(RUNTIME_DIRECTORY)
            .join(RUNTIME_CONFIGURATION_DIRECTORY)
            .join(RUNTIME_ARTIFACT_BOOTSTRAP_CONFIG_FILE);
        let bytes = fs::read(path).unwrap();
        let digest = profile.config_digests["artifact-bootstrap"]
            .parse()
            .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        Config::decode(&bytes, &digest).unwrap();
        let wrong = "sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
            .parse()
            .unwrap();
        assert!(Config::decode(&bytes, &wrong).is_err());
        let mut duplicate = bytes.clone();
        duplicate.splice(1..1, b"\"schema_version\":1,".iter().copied());
        assert!(Config::decode(&duplicate, &digest).is_err());
        let mut oversized = bytes.clone();
        oversized.resize(MAX_DEVELOPMENT_ARTIFACT_BOOTSTRAP_BYTES + 1, b' ');
        assert!(Config::decode(&oversized, &digest).is_err());
        for (field, bad) in [
            ("schema_version", serde_json::json!(2)),
            ("environment_class", serde_json::json!("production")),
            ("unknown", serde_json::json!(true)),
            (
                "authoring_artifact_id",
                value["staging_quota_account_id"].clone(),
            ),
            (
                "artifact_io_policy_id",
                value["retention_policy_id"].clone(),
            ),
            (
                "orchestration_quota_account_id",
                value["staging_quota_account_id"].clone(),
            ),
            ("staging_quota_bytes", serde_json::json!(0)),
            ("orchestration_concurrent_jobs", serde_json::json!(-1)),
        ] {
            let mut invalid = value.clone();
            invalid[field] = bad;
            let current = canonical_digest(&invalid).unwrap().parse().unwrap();
            assert!(
                Config::decode(&serde_json::to_vec(&invalid).unwrap(), &current).is_err(),
                "{field}"
            );
        }
        for policy in [
            "retention_policy",
            "artifact_io_policy",
            "scheduling_policy",
        ] {
            let mut invalid = value.clone();
            invalid[policy]["unknown"] = serde_json::json!(true);
            let current = canonical_digest(&invalid).unwrap().parse().unwrap();
            assert!(
                Config::decode(&serde_json::to_vec(&invalid).unwrap(), &current).is_err(),
                "{policy}"
            );
        }
        for (policy, field, bad) in [
            ("retention_policy", "gc_grace_seconds", serde_json::json!(0)),
            (
                "artifact_io_policy",
                "deny_symlink",
                serde_json::json!(false),
            ),
            ("scheduling_policy", "weight", serde_json::json!(0)),
        ] {
            let mut invalid = value.clone();
            invalid[policy][field] = bad;
            let current = canonical_digest(&invalid).unwrap().parse().unwrap();
            assert!(
                Config::decode(&serde_json::to_vec(&invalid).unwrap(), &current).is_err(),
                "{policy}.{field}"
            );
        }
        let (another, _, other, _) = setup();
        assert_ne!(
            profile.config_digests["artifact-bootstrap"],
            other.config_digests["artifact-bootstrap"]
        );
        let fresh: serde_json::Value = read_runtime_json(
            &another
                .path()
                .join(PROJECT_DIRECTORY)
                .join(RUNTIME_DIRECTORY)
                .join(RUNTIME_CONFIGURATION_DIRECTORY)
                .join(RUNTIME_ARTIFACT_BOOTSTRAP_CONFIG_FILE),
        )
        .unwrap()
        .unwrap();
        for (key, original) in value.as_object().unwrap() {
            if key.ends_with("_id") {
                assert_ne!(original, &fresh[key], "fresh identity {key}");
            }
        }
    }

    #[test]
    fn invalid_preserved_bootstrap_never_falls_back_to_fresh_ids() {
        for mode in ["missing", "digest-drift", "typed-invalid", "policy-drift"] {
            let (directory, identity, mut previous, binaries) = setup();
            let _lock = acquire_runtime_lifecycle_lock(directory.path()).unwrap();
            let state = directory.path().join(PROJECT_DIRECTORY);
            let runtime = state.join(RUNTIME_DIRECTORY);
            let path = runtime
                .join(RUNTIME_CONFIGURATION_DIRECTORY)
                .join(RUNTIME_ARTIFACT_BOOTSTRAP_CONFIG_FILE);
            if mode == "missing" {
                fs::remove_file(&path).unwrap();
            } else {
                let mut value: serde_json::Value = read_runtime_json(&path).unwrap().unwrap();
                match mode {
                    "typed-invalid" => {
                        value["authoring_artifact_id"] = value["staging_quota_account_id"].clone()
                    }
                    "policy-drift" => {
                        value["artifact_io_policy"]["maximum_input_artifacts"] =
                            serde_json::json!(63)
                    }
                    _ => value["staging_quota_bytes"] = serde_json::json!(1),
                }
                write_private(&path, &value).unwrap();
                if mode != "digest-drift" {
                    previous.config_digests.insert(
                        "artifact-bootstrap".into(),
                        canonical_digest(&value).unwrap(),
                    );
                    refresh_runtime_profile_closure_digest(&mut previous).unwrap();
                    write_private(&runtime.join(RUNTIME_PROFILE_STATE_FILE), &previous).unwrap();
                }
            }
            let before = fs::read(&path).ok();
            let profile_before = fs::read(runtime.join(RUNTIME_PROFILE_STATE_FILE)).unwrap();
            assert!(
                regenerate(
                    &state,
                    &identity,
                    &previous,
                    DevProfile::parse(None, false, true).unwrap(),
                    RuntimeProfileSource {
                        source_fingerprint: &previous.source_fingerprint,
                        release_identity: &previous.release_identity,
                        binary_directory: &binaries
                    }
                )
                .is_err(),
                "{mode}"
            );
            assert_eq!(fs::read(&path).ok(), before, "{mode}");
            assert_eq!(
                fs::read(runtime.join(RUNTIME_PROFILE_STATE_FILE)).unwrap(),
                profile_before
            );
            assert!(!runtime.join(JOURNAL_FILE).exists());
        }
    }

    #[test]
    fn recovered_journal_cannot_replace_a_valid_bootstrap_with_new_ids() {
        let (directory, identity, previous, binaries) = setup();
        let _lock = acquire_runtime_lifecycle_lock(directory.path()).unwrap();
        let runtime = directory
            .path()
            .join(PROJECT_DIRECTORY)
            .join(RUNTIME_DIRECTORY);
        let mut journal = staged(directory.path(), &identity, &previous, &binaries, "model");
        let path = runtime
            .join(&journal.staged_directory)
            .join(RUNTIME_ARTIFACT_BOOTSTRAP_CONFIG_FILE);
        let mut value: serde_json::Value = read_runtime_json(&path).unwrap().unwrap();
        value["authoring_artifact_id"] =
            serde_json::json!(fresh_resource_id(ResourceKind::Artifact));
        write_private(&path, &value).unwrap();
        journal.target_profile.config_digests.insert(
            "artifact-bootstrap".into(),
            canonical_digest(&value).unwrap(),
        );
        refresh_runtime_profile_closure_digest(&mut journal.target_profile).unwrap();
        let original_path = runtime
            .join(RUNTIME_CONFIGURATION_DIRECTORY)
            .join(RUNTIME_ARTIFACT_BOOTSTRAP_CONFIG_FILE);
        let before = fs::read(&original_path).unwrap();
        write_private(&runtime.join(JOURNAL_FILE), &journal).unwrap();
        assert!(recover(&runtime, &identity).is_err());
        assert_eq!(fs::read(original_path).unwrap(), before);
        assert!(runtime.join(JOURNAL_FILE).exists());
    }
    #[test]
    fn bootstrap_raw_byte_bound_applies_to_both_staged_and_current_recovery_inputs() {
        use insight_platform_deployment_contracts::development::MAX_DEVELOPMENT_ARTIFACT_BOOTSTRAP_BYTES;
        for staged_input in [false, true] {
            let (directory, identity, previous, binaries) = setup();
            let _lock = acquire_runtime_lifecycle_lock(directory.path()).unwrap();
            let runtime = directory
                .path()
                .join(PROJECT_DIRECTORY)
                .join(RUNTIME_DIRECTORY);
            let journal = staged(directory.path(), &identity, &previous, &binaries, "model");
            let config_directory = if staged_input {
                runtime.join(&journal.staged_directory)
            } else {
                runtime.join(RUNTIME_CONFIGURATION_DIRECTORY)
            };
            let path = config_directory.join(RUNTIME_ARTIFACT_BOOTSTRAP_CONFIG_FILE);
            let mut bytes = fs::read(&path).unwrap();
            bytes.resize(MAX_DEVELOPMENT_ARTIFACT_BOOTSTRAP_BYTES + 1, b' ');
            fs::write(&path, &bytes).unwrap();
            let before = fs::read(runtime.join(RUNTIME_PROFILE_STATE_FILE)).unwrap();
            assert!(preflight(&runtime, &identity, &journal).is_err());
            assert_eq!(
                fs::read(runtime.join(RUNTIME_PROFILE_STATE_FILE)).unwrap(),
                before
            );
            assert_eq!(fs::read(path).unwrap(), bytes);
        }
    }
}
