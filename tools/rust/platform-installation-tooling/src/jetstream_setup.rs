//! Direct invocation of the separately credentialed JetStream deployment owner.
use insight_platform_contracts::{parse_strict_json, Sha256Digest};
use insight_platform_deployment_contracts::installation::{
    InstallationError as Error, InstallationIdentityV1, InstallationInputV1, INSTALLATION_LIMITS,
};
use insight_platform_deployment_tooling::private_state::InstallationDirectory;
use serde::{Deserialize, Serialize};
use std::{path::Path, process::Stdio, time::Duration};

const JOURNAL: &str = "jetstream-setup.json";
const CONTRACT: &str = "committed-events.json";
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    Provision,
    Verify,
}
#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Phase {
    Requested,
    Verified,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Journal {
    schema_version: u32,
    input_digest: Sha256Digest,
    identity_digest: Sha256Digest,
    phase: Phase,
}

pub async fn ensure_stream(
    input: &InstallationInputV1,
    identity: &InstallationIdentityV1,
    private: &InstallationDirectory,
    binary_directory: &Path,
    mode: Mode,
) -> Result<(), Error> {
    input.validate()?;
    identity.validate()?;
    let contract = insight_platform_deployment_tooling::base_profile::outbox_stream_contract();
    let bytes = serde_json::to_vec(&contract).map_err(|_| Error::InvalidInput)?;
    let prior = private.read(JOURNAL, 4096)?;
    let fresh = prior.is_none();
    let mut journal: Journal = match prior {
        Some(bytes) => serde_json::from_value(
            parse_strict_json(&bytes, INSTALLATION_LIMITS).map_err(|_| Error::InvalidInput)?,
        )
        .map_err(|_| Error::InvalidInput)?,
        None if mode == Mode::Provision => Journal {
            schema_version: 1,
            input_digest: input.digest()?,
            identity_digest: identity.digest()?,
            phase: Phase::Requested,
        },
        None => return Err(Error::Incomplete),
    };
    if journal.schema_version != 1
        || journal.input_digest != input.digest()?
        || journal.identity_digest != identity.digest()?
        || identity.input_digest != journal.input_digest
    {
        return Err(Error::IdentityDrift);
    }
    if mode == Mode::Verify && journal.phase != Phase::Verified {
        return Err(Error::Incomplete);
    }
    match private.read(CONTRACT, 16384)? {
        Some(existing) if existing == bytes => (),
        None if fresh => private.write_immutable(CONTRACT, &bytes)?,
        _ => return Err(Error::ConfigurationDrift),
    }
    let binary = binary_directory.join(format!(
        "platform-jetstream-provision{}",
        std::env::consts::EXE_SUFFIX
    ));
    if !binary_directory.is_absolute()
        || binary_directory.components().any(|part| {
            matches!(
                part,
                std::path::Component::ParentDir | std::path::Component::CurDir
            )
        })
    {
        return Err(Error::InvalidPath);
    }
    for path in binary.ancestors() {
        let metadata =
            std::fs::symlink_metadata(path).map_err(|_| Error::PrerequisiteUnavailable)?;
        if metadata.file_type().is_symlink()
            || path == binary && (!metadata.is_file() || metadata.len() == 0)
        {
            return Err(Error::InvalidPath);
        }
    }
    for name in [
        "ca.pem",
        "outbox-provision-client.pem",
        "outbox-provision-client-key.pem",
    ] {
        private.read(name, 16384)?.ok_or(Error::Incomplete)?;
    }
    if fresh {
        private.replace(
            JOURNAL,
            &serde_json::to_vec(&journal).map_err(|_| Error::InvalidInput)?,
        )?;
    }
    let mut command = tokio::process::Command::new(binary);
    command
        .args([
            if fresh { "create" } else { "verify" },
            private.path(CONTRACT)?.to_str().ok_or(Error::InvalidPath)?,
        ])
        .env_clear()
        .env(
            "PLATFORM_OUTBOX_PROVISION_NATS_URL",
            format!(
                "tls://{}:{}",
                input.network.nats_host, input.network.nats_port
            ),
        )
        .env("PLATFORM_OUTBOX_PROVISION_CA_PATH", private.path("ca.pem")?)
        .env(
            "PLATFORM_OUTBOX_PROVISION_CERT_PATH",
            private.path("outbox-provision-client.pem")?,
        )
        .env(
            "PLATFORM_OUTBOX_PROVISION_KEY_PATH",
            private.path("outbox-provision-client-key.pem")?,
        )
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    let status = match crate::process_control::execute(command, Duration::from_secs(35)).await? {
        crate::process_control::ChildOutcome::Exited(status) => status,
        crate::process_control::ChildOutcome::Interrupted => {
            return Err(Error::ExternalOutcomeUnknown)
        }
    };
    if !status.success() {
        // The binary does not expose a typed distinction between rejected create and lost ACK.
        // Preserve Requested; a later invocation can only read back, never create again.
        return Err(if fresh || status.code().is_none() {
            Error::ExternalOutcomeUnknown
        } else {
            Error::PrerequisiteUnavailable
        });
    }
    if journal.phase != Phase::Verified {
        journal.phase = Phase::Verified;
        private.replace(
            JOURNAL,
            &serde_json::to_vec(&journal).map_err(|_| Error::InvalidInput)?,
        )?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use insight_platform_deployment_tooling::installation::{compose_input, PreparedInstallation};
    use std::os::unix::fs::PermissionsExt;

    struct Fixture {
        _root: tempfile::TempDir,
        input: InstallationInputV1,
        prepared: PreparedInstallation,
        binaries: std::path::PathBuf,
    }
    impl Fixture {
        fn new(script: &str) -> Self {
            let root = tempfile::tempdir().unwrap();
            let path = root.path().canonicalize().unwrap();
            let input = compose_input(
                "stream-recovery",
                format!("sha256:{}", "a".repeat(64)).parse().unwrap(),
            )
            .unwrap();
            let prepared = PreparedInstallation::prepare(&input, &path.join("private")).unwrap();
            let binaries = path.join("binaries with spaces");
            std::fs::create_dir(&binaries).unwrap();
            let binary = binaries.join("platform-jetstream-provision");
            std::fs::write(&binary, script).unwrap();
            std::fs::set_permissions(&binary, std::fs::Permissions::from_mode(0o700)).unwrap();
            Self {
                _root: root,
                input,
                prepared,
                binaries,
            }
        }
        async fn run(&self, mode: Mode) -> Result<(), Error> {
            ensure_stream(
                &self.input,
                self.prepared.identity(),
                self.prepared.directory(),
                &self.binaries,
                mode,
            )
            .await
        }
    }

    #[tokio::test]
    async fn lost_create_ack_resumes_readonly_and_completed_verify_does_not_rewrite() {
        let fixture = Fixture::new(
            r#"#!/bin/sh
set -eu
root=${0%/*}
printf '%s\n' "$1" >> "$root/calls"
[ "$#" = 2 ] && [ -f "$2" ]
[ -n "$PLATFORM_OUTBOX_PROVISION_NATS_URL" ] && [ -f "$PLATFORM_OUTBOX_PROVISION_KEY_PATH" ]
if [ "$1" = create ]; then
  printf '%s' accepted > "$root/stream"
  exit 1
fi
[ "$1" = verify ] && [ -f "$root/stream" ]
"#,
        );
        assert!(matches!(
            fixture.run(Mode::Provision).await,
            Err(Error::ExternalOutcomeUnknown)
        ));
        let requested = fixture
            .prepared
            .directory()
            .read(JOURNAL, 4096)
            .unwrap()
            .unwrap();
        assert!(String::from_utf8_lossy(&requested).contains("requested"));
        fixture.run(Mode::Provision).await.unwrap();
        let completed = fixture
            .prepared
            .directory()
            .read(JOURNAL, 4096)
            .unwrap()
            .unwrap();
        fixture.run(Mode::Verify).await.unwrap();
        assert_eq!(
            fixture
                .prepared
                .directory()
                .read(JOURNAL, 4096)
                .unwrap()
                .unwrap(),
            completed
        );
        assert_eq!(
            std::fs::read_to_string(fixture.binaries.join("calls")).unwrap(),
            "create\nverify\nverify\n"
        );
        std::fs::remove_file(fixture.binaries.join("stream")).unwrap();
        assert!(matches!(
            fixture.run(Mode::Provision).await,
            Err(Error::PrerequisiteUnavailable)
        ));
        assert!(!fixture.binaries.join("stream").exists());
        assert_eq!(
            std::fs::read_to_string(fixture.binaries.join("calls")).unwrap(),
            "create\nverify\nverify\nverify\n"
        );
        assert_eq!(
            fixture
                .prepared
                .directory()
                .read(JOURNAL, 4096)
                .unwrap()
                .unwrap(),
            completed
        );
    }

    #[tokio::test]
    async fn missing_or_foreign_stream_journal_stops_before_any_child() {
        let fixture = Fixture::new("#!/bin/sh\nprintf called > \"${0%/*}/calls\"\n");
        assert!(matches!(
            fixture.run(Mode::Verify).await,
            Err(Error::Incomplete)
        ));
        assert!(!fixture.binaries.join("calls").exists());
        fixture.run(Mode::Provision).await.unwrap();
        std::fs::remove_file(fixture.binaries.join("calls")).unwrap();
        let bytes = fixture
            .prepared
            .directory()
            .read(JOURNAL, 4096)
            .unwrap()
            .unwrap();
        let mut journal: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        journal["identity_digest"] = serde_json::json!(format!("sha256:{}", "b".repeat(64)));
        fixture
            .prepared
            .directory()
            .replace(JOURNAL, &serde_json::to_vec(&journal).unwrap())
            .unwrap();
        assert!(matches!(
            fixture.run(Mode::Provision).await,
            Err(Error::IdentityDrift)
        ));
        assert!(!fixture.binaries.join("calls").exists());
    }
}
