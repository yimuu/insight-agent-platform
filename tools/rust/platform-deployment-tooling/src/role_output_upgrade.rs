// Included inside the descriptor-based publication module: no path-based traversal or writes.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReleaseOutputPlan {
    release_digest: Sha256Digest,
    before: BTreeMap<String, BTreeMap<String, Sha256Digest>>,
    after_digest: Sha256Digest,
    source_digest: Sha256Digest,
}

fn upgrade_files(
    publication: &Publication<'_>,
    roles: &BTreeMap<String, Role<'_>>,
) -> Result<(), Error> {
    use insight_platform_deployment_contracts::installation_release::*;
    validate_private_identity(publication.input, publication.identity, publication.private)?;
    let (intent, release, plan_name) = match &publication.mode {
        RoleOutputMode::Upgrade => {
            let bytes = publication
                .private
                .read(UPGRADE_INTENT_FILE, INSTALLATION_MAX_BYTES)?
                .ok_or(Error::Incomplete)?;
            let release = serde_json::from_slice::<InstallationReleaseV1>(&bytes)
                .map_err(|_| Error::InvalidInput)?;
            (bytes, release, format!("upgrade-{}", publication.journal))
        }
        RoleOutputMode::Rollout(digest) => {
            let bytes = publication
                .private
                .read(
                    &PackageRolloutIntentV1::filename(digest),
                    INSTALLATION_MAX_BYTES,
                )?
                .ok_or(Error::Incomplete)?;
            let intent = serde_json::from_slice::<PackageRolloutIntentV1>(&bytes)
                .map_err(|_| Error::InvalidInput)?;
            intent.validate_for(publication.input, publication.identity)?;
            if intent.target_release.canonical_digest()? != *digest {
                return Err(Error::IdentityDrift);
            }
            let current: InstallationReleaseV1 = serde_json::from_slice(
                &publication
                    .private
                    .read(RELEASE_FILE, INSTALLATION_MAX_BYTES)?
                    .ok_or(Error::Incomplete)?,
            )
            .map_err(|_| Error::InvalidInput)?;
            if current != intent.previous_release && current != intent.target_release {
                return Err(Error::IdentityDrift);
            }
            (
                bytes,
                intent.target_release,
                format!("rollout-{}-{}", &digest.as_str()[7..], publication.journal),
            )
        }
        _ => return Err(Error::InvalidInput),
    };
    release.validate_for(publication.input, publication.identity)?;
    let release_digest = bytes_digest(&intent);
    let root = Directory::absolute(publication.root)?;
    if root.names()? != roles.keys().cloned().collect() {
        return Err(Error::ForeignState);
    }
    let target: BTreeMap<String, BTreeMap<String, Sha256Digest>> = roles
        .iter()
        .map(|(name, role)| {
            (
                name.clone(),
                role.files
                    .iter()
                    .map(|(path, bytes)| (path.clone(), bytes_digest(bytes)))
                    .collect(),
            )
        })
        .collect();
    let after_digest = canonical(&target)?;
    let old: Journal = serde_json::from_slice(
        &publication
            .private
            .read(publication.journal, 65536)?
            .ok_or(Error::Incomplete)?,
    )
    .map_err(|_| Error::InvalidInput)?;
    if old.schema_version != 1
        || old.input_digest != publication.input.digest()?
        || old.identity_digest != publication.identity.digest()?
        || old.output_root != publication.root.to_str().ok_or(Error::InvalidPath)?
        || old.ownership != publication.ownership
        || !old.complete
        || old.pending.is_some()
    {
        return Err(Error::IdentityDrift);
    }
    let existing = publication.private.read(&plan_name, 262144)?;
    let plan: ReleaseOutputPlan = if let Some(bytes) = existing {
        serde_json::from_slice(&bytes).map_err(|_| Error::InvalidInput)?
    } else {
        let before = observe_upgrade_files(&root, roles, false)?;
        if canonical(&before)? != old.files_digest {
            return Err(Error::ConfigurationDrift);
        }
        let plan = ReleaseOutputPlan {
            release_digest: release_digest.clone(),
            before,
            after_digest: after_digest.clone(),
            source_digest: publication.source_digest.clone(),
        };
        publication.private.write_immutable(
            &plan_name,
            &serde_json::to_vec(&plan).map_err(|_| Error::InvalidInput)?,
        )?;
        plan
    };
    if plan.release_digest != release_digest
        || plan.after_digest != after_digest
        || plan.source_digest != publication.source_digest
        || (old.files_digest != canonical(&plan.before)? && old.files_digest != after_digest)
    {
        return Err(Error::IdentityDrift);
    }
    let actual = observe_upgrade_files(&root, roles, true)?;
    for (name, role) in roles {
        let directory = root.child(name)?.ok_or(Error::Incomplete)?;
        directory.check(role.owner)?;
        for (path, bytes) in &role.files {
            let before = plan.before.get(name).and_then(|v| v.get(path));
            let found = actual.get(name).and_then(|v| v.get(path));
            let after = &target[name][path];
            if found != before && found != Some(after) {
                return Err(Error::ConfigurationDrift);
            }
            let (parent_name, leaf) = parent(path);
            if !parent_name.is_empty() && directory.child(parent_name)?.is_none() {
                directory.create(parent_name, role.owner)?;
            }
            let child = candidate(&directory, path)?;
            let tmp = format!(".upgrade-{}", &after.as_str()[7..]);
            if let Some(partial) = child.read(&tmp, bytes, role.owner)? {
                if !bytes.starts_with(&partial) {
                    return Err(Error::ConfigurationDrift);
                }
                child.remove(&tmp)?;
            }
            if found == Some(after) {
                continue;
            }
            let mut file = opened(unsafe {
                libc::openat(
                    child.0.as_raw_fd(),
                    name_c(&tmp)?.as_ptr(),
                    libc::O_WRONLY
                        | libc::O_CREAT
                        | libc::O_EXCL
                        | libc::O_NOFOLLOW
                        | libc::O_CLOEXEC,
                    0o600,
                )
            })?;
            set_owner(&file, role.owner)?;
            file.write_all(bytes)
                .and_then(|_| file.sync_all())
                .map_err(|_| Error::Incomplete)?;
            if unsafe {
                libc::renameat(
                    child.0.as_raw_fd(),
                    name_c(&tmp)?.as_ptr(),
                    child.0.as_raw_fd(),
                    name_c(leaf)?.as_ptr(),
                )
            } != 0
            {
                return Err(Error::Incomplete);
            }
            child.sync()?;
        }
    }
    for (name, role) in roles {
        check_role(&root, name, role, None, true)?;
    }
    Journal {
        schema_version: 1,
        input_digest: old.input_digest,
        identity_digest: old.identity_digest,
        source_digest: publication.source_digest.clone(),
        files_digest: after_digest,
        output_root: old.output_root,
        ownership: old.ownership,
        complete: true,
        pending: None,
    }
    .save(publication.private, publication.journal)
}

fn name_c(value: &str) -> Result<std::ffi::CString, Error> {
    name(value)
}

fn observe_upgrade_files(
    root: &Directory,
    roles: &BTreeMap<String, Role<'_>>,
    allow_temporary: bool,
) -> Result<BTreeMap<String, BTreeMap<String, Sha256Digest>>, Error> {
    let mut all = BTreeMap::new();
    for (role_name, role) in roles {
        let dir = root.child(role_name)?.ok_or(Error::Incomplete)?;
        dir.check(role.owner)?;
        let mut files = BTreeMap::new();
        for entry in dir.names()? {
            if role
                .files
                .keys()
                .any(|path| path.starts_with(&format!("{entry}/")))
            {
                let sub = dir.child(&entry)?.ok_or(Error::Incomplete)?;
                sub.check(role.owner)?;
                for leaf in sub.names()? {
                    observe_upgrade_leaf(
                        &sub,
                        role,
                        &format!("{entry}/{leaf}"),
                        &leaf,
                        allow_temporary,
                        &mut files,
                    )?;
                }
            } else {
                observe_upgrade_leaf(&dir, role, &entry, &entry, allow_temporary, &mut files)?;
            }
        }
        all.insert(role_name.clone(), files);
    }
    Ok(all)
}
fn observe_upgrade_leaf(
    dir: &Directory,
    role: &Role<'_>,
    relative: &str,
    leaf: &str,
    allow_temporary: bool,
    files: &mut BTreeMap<String, Sha256Digest>,
) -> Result<(), Error> {
    if allow_temporary && leaf.starts_with(".upgrade-") {
        let (p, _) = parent(relative);
        let allowed = role.files.iter().any(|(path, bytes)| {
            parent(path).0 == p
                && leaf == format!(".upgrade-{}", &bytes_digest(bytes).as_str()[7..])
        });
        if allowed {
            return Ok(());
        }
    }
    if !role.files.contains_key(relative) {
        return Err(Error::ForeignState);
    }
    let maximum = vec![0u8; 262144];
    let bytes = dir
        .read(leaf, &maximum, role.owner)?
        .ok_or(Error::Incomplete)?;
    files.insert(relative.into(), bytes_digest(&bytes));
    Ok(())
}
