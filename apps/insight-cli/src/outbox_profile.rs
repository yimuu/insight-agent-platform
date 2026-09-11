//! Development-only Outbox composition. No runtime business authority lives in the CLI.
use super::*;

pub(crate) const CONFIG_FILE: &str = "outbox-worker.json";
pub(crate) const STREAM_FILE: &str = "outbox-stream.json";
pub(crate) const NATS_CONFIG_FILE: &str = "nats.conf";
pub(crate) const PROVISION_CERTIFICATE_FILE: &str = "outbox-provision-client.pem";
pub(crate) const PROVISION_PRIVATE_KEY_FILE: &str = "outbox-provision-client-key.pem";
const PASSWORD_FILE: &str = "outbox-database-password";
const NATS_CONFIGURATION: &[u8] = include_bytes!("../../../deploy/dev/nats.conf");
const STREAM_CONTRACT: &[u8] = include_bytes!("../../../deploy/jetstream/committed-events-v1.json");

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
pub(crate) fn read_password(runtime: &Path) -> Result<String, CliError> {
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
