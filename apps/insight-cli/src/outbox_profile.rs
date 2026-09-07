//! Development-only Outbox composition. No runtime business authority lives in the CLI.
use super::*;

pub(crate) const CONFIG_FILE: &str = "outbox-worker.json";
pub(crate) const STREAM_FILE: &str = "outbox-stream.json";
pub(crate) const NATS_CONFIG_FILE: &str = "nats.conf";
pub(crate) const CERTIFICATE_FILE: &str = "outbox-client.pem";
pub(crate) const PRIVATE_KEY_FILE: &str = "outbox-client-key.pem";
pub(crate) const PROVISION_CERTIFICATE_FILE: &str = "outbox-provision-client.pem";
pub(crate) const PROVISION_PRIVATE_KEY_FILE: &str = "outbox-provision-client-key.pem";
const PASSWORD_FILE: &str = "outbox-database-password";
const PROVISION_IDENTITY: &str = "spiffe://insight.platform/workload/local-outbox-provisioner";
const NATS_CONFIGURATION: &[u8] = include_bytes!("../../../deploy/dev/nats.conf");
const STREAM_CONTRACT: &[u8] = include_bytes!("../../../deploy/jetstream/committed-events-v1.json");

pub(crate) fn config(port: u16) -> serde_json::Value {
    serde_json::json!({
        "schema_version": 1, "observability_listen_address": loopback_address(port),
        "database_max_connections": 4, "database_acquire_timeout_milliseconds": 5000,
        "nats_servers": ["tls://localhost:4222"], "nats_connect_timeout_milliseconds": 5000,
        "nats_publish_timeout_milliseconds": 3000, "maximum_pending_messages": 64,
        "poll_interval_milliseconds": 1000, "claim_batch": 4, "lease_milliseconds": 60000,
        "retry_base_milliseconds": 1000, "retry_maximum_milliseconds": 60000,
        "stream": serde_json::from_slice::<serde_json::Value>(STREAM_CONTRACT).expect("embedded transport contract")
    })
}
pub(crate) fn identity_specs() -> [LocalTlsIdentitySpec; 2] {
    [
        LocalTlsIdentitySpec {
            certificate: CERTIFICATE_FILE,
            private_key: PRIVATE_KEY_FILE,
            dns_names: &[],
            workload_identity: Some(insight_platform_contracts::OUTBOX_WORKER_WORKLOAD_IDENTITY),
            usage: LocalTlsUsage::Client,
        },
        LocalTlsIdentitySpec {
            certificate: PROVISION_CERTIFICATE_FILE,
            private_key: PROVISION_PRIVATE_KEY_FILE,
            dns_names: &[],
            workload_identity: Some(PROVISION_IDENTITY),
            usage: LocalTlsUsage::Client,
        },
    ]
}
pub(crate) fn prepare_files(runtime: &Path) -> Result<(), CliError> {
    for (name, bytes) in [
        (NATS_CONFIG_FILE, NATS_CONFIGURATION),
        (STREAM_FILE, STREAM_CONTRACT),
    ] {
        let path = runtime.join(name);
        if path.exists() {
            if fs::read(&path).map_err(|source| CliError::InitializeProject {
                path: path.display().to_string(),
                source,
            })? != bytes
            {
                return Err(CliError::RuntimeState(format!(
                    "installed {name} differs from the current deployment contract"
                )));
            }
        } else {
            write_new(&path, bytes)?;
        }
    }
    let password = runtime.join(PASSWORD_FILE);
    if !password.exists() {
        write_sensitive_new(&password, Uuid::new_v4().simple().to_string().as_bytes())?;
    }
    read_password(runtime)?;
    Ok(())
}
fn read_password(runtime: &Path) -> Result<String, CliError> {
    let bytes = read_bounded_identity_file(&runtime.join(PASSWORD_FILE))?;
    if bytes.len() != 32 || !bytes.iter().all(u8::is_ascii_hexdigit) {
        return Err(CliError::RuntimeState(
            "Outbox database credential is invalid".to_owned(),
        ));
    }
    String::from_utf8(bytes)
        .map_err(|_| CliError::RuntimeState("Outbox database credential is invalid".to_owned()))
}
pub(crate) fn provision(binary_directory: &Path, runtime: &Path) -> Result<(), CliError> {
    let binary = binary_directory.join(format!(
        "platform-jetstream-provision{}",
        std::env::consts::EXE_SUFFIX
    ));
    let mut database = ProcessCommand::new(binary_directory.join(format!(
        "platform-database-role{}",
        std::env::consts::EXE_SUFFIX
    )));
    database
        .args(["--purpose", "outbox"])
        .arg(runtime.join(PASSWORD_FILE))
        .env(
            "PLATFORM_DATABASE_ROLE_ADMIN_URL",
            "postgres://insight:insight@127.0.0.1:5432/insight_platform",
        );
    run_external(database, "provision development Outbox role")?;
    let tls = runtime.join(RUNTIME_TLS_DIRECTORY);
    let mut stream = ProcessCommand::new(binary);
    stream
        .arg("create")
        .arg(runtime.join(STREAM_FILE))
        .env("PLATFORM_OUTBOX_PROVISION_NATS_URL", "tls://localhost:4222")
        .env(
            "PLATFORM_OUTBOX_PROVISION_CA_PATH",
            tls.join(RUNTIME_CA_CERTIFICATE_FILE),
        )
        .env(
            "PLATFORM_OUTBOX_PROVISION_CERT_PATH",
            tls.join(PROVISION_CERTIFICATE_FILE),
        )
        .env(
            "PLATFORM_OUTBOX_PROVISION_KEY_PATH",
            tls.join(PROVISION_PRIVATE_KEY_FILE),
        );
    run_external(stream, "provision committed Event stream")?;
    Ok(())
}
pub(crate) fn launch(
    binary_directory: &Path,
    runtime: &Path,
    profile: &RuntimeProfileState,
) -> Result<RuntimeLaunchSpec, CliError> {
    let tls = runtime.join(RUNTIME_TLS_DIRECTORY);
    let config_digest = profile
        .config_digests
        .get("outbox")
        .ok_or_else(|| CliError::RuntimeState("Outbox configuration digest missing".to_owned()))?;
    Ok(RuntimeLaunchSpec {
        role: "outbox",
        binary: binary_directory.join(format!(
            "platform-outbox-worker{}",
            std::env::consts::EXE_SUFFIX
        )),
        ready_address: loopback_address(profile.ports.full.outbox_observability),
        environment: vec![
            (
                "PLATFORM_OUTBOX_CONFIG",
                runtime
                    .join(RUNTIME_CONFIGURATION_DIRECTORY)
                    .join(CONFIG_FILE)
                    .display()
                    .to_string(),
            ),
            ("PLATFORM_OUTBOX_CONFIG_DIGEST", config_digest.clone()),
            (
                "PLATFORM_OUTBOX_DATABASE_URL",
                format!(
                    "postgres://insight_outbox_dev:{}@127.0.0.1:5432/insight_platform",
                    read_password(runtime)?
                ),
            ),
            (
                "PLATFORM_OUTBOX_NATS_CA_PATH",
                tls.join(RUNTIME_CA_CERTIFICATE_FILE).display().to_string(),
            ),
            (
                "PLATFORM_OUTBOX_NATS_CERT_PATH",
                tls.join(CERTIFICATE_FILE).display().to_string(),
            ),
            (
                "PLATFORM_OUTBOX_NATS_KEY_PATH",
                tls.join(PRIVATE_KEY_FILE).display().to_string(),
            ),
        ],
        extra_environment: Vec::new(),
    })
}
