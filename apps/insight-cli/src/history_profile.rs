//! Development composition for the isolated history maintenance role.
use super::*;
pub(crate) const CONFIG_FILE: &str = "history-maintenance.json";
const PASSWORD_FILE: &str = "history-database-password";

pub(crate) fn config(
    port: u16,
    builds: &worker_profile::WorkerBuilds,
) -> Result<serde_json::Value, CliError> {
    let executable = builds
        .executable("platform-history-maintenance")
        .ok_or_else(|| {
            CliError::RuntimeState("History maintenance executable is missing".to_owned())
        })?;
    let value = serde_json::json!({
        "schema_version":1,"component_role":"history_maintenance","executable_digest":executable,
        "observability_listen_address":loopback_address(port),"database_max_connections":2,"database_acquire_timeout_milliseconds":5000,
        "poll_interval_milliseconds":30000,"maximum_runs":16,"maximum_events_per_run":128,
        "retention_policy":{"schema_version":2,"public_event_minimum_seconds":604800,"audit_event_minimum_seconds":604800,
            "cleanup_minimum_seconds":604800,"receipt_minimum_seconds":604800,"published_outbox_minimum_seconds":604800}
    });
    let config: insight_platform_deployment_contracts::history::HistoryMaintenanceConfigV1 =
        serde_json::from_value(value.clone())
            .map_err(|_| CliError::RuntimeState("History config is invalid".to_owned()))?;
    config
        .validate()
        .map_err(|error| CliError::RuntimeState(error.to_owned()))?;
    Ok(value)
}
fn password(runtime: &Path) -> Result<String, CliError> {
    let bytes = read_bounded_identity_file(&runtime.join(PASSWORD_FILE))?;
    if bytes.len() != 32 || !bytes.iter().all(u8::is_ascii_hexdigit) {
        return Err(CliError::RuntimeState(
            "History database credential is invalid".to_owned(),
        ));
    }
    String::from_utf8(bytes)
        .map_err(|_| CliError::RuntimeState("History database credential is invalid".to_owned()))
}
pub(crate) fn prepare(runtime: &Path) -> Result<(), CliError> {
    let path = runtime.join(PASSWORD_FILE);
    if !path.exists() {
        write_sensitive_new(&path, Uuid::new_v4().simple().to_string().as_bytes())?;
    }
    password(runtime)?;
    Ok(())
}
pub(crate) fn provision(binaries: &Path, runtime: &Path) -> Result<(), CliError> {
    let mut command = ProcessCommand::new(binaries.join(format!(
        "platform-database-role{}",
        std::env::consts::EXE_SUFFIX
    )));
    command
        .args(["--purpose", "history"])
        .arg(runtime.join(PASSWORD_FILE))
        .env(
            "PLATFORM_DATABASE_ROLE_ADMIN_URL",
            "postgres://insight:insight@127.0.0.1:5432/insight_platform",
        );
    run_external(command, "provision independent history maintenance role")?;
    Ok(())
}
pub(crate) fn launch(
    binaries: &Path,
    runtime: &Path,
    profile: &RuntimeProfileState,
) -> Result<RuntimeLaunchSpec, CliError> {
    let digest = profile
        .config_digests
        .get("history-maintenance")
        .ok_or_else(|| CliError::RuntimeState("History config digest missing".to_owned()))?;
    Ok(RuntimeLaunchSpec {
        role: "history-maintenance",
        binary: binaries.join(format!(
            "platform-history-maintenance{}",
            std::env::consts::EXE_SUFFIX
        )),
        ready_address: loopback_address(profile.ports.full.history_observability),
        extra_environment: Vec::new(),
        environment: vec![
            (
                "PLATFORM_HISTORY_MAINTENANCE_CONFIG",
                runtime
                    .join(RUNTIME_CONFIGURATION_DIRECTORY)
                    .join(CONFIG_FILE)
                    .display()
                    .to_string(),
            ),
            ("PLATFORM_HISTORY_MAINTENANCE_CONFIG_DIGEST", digest.clone()),
            (
                "PLATFORM_HISTORY_MAINTENANCE_DATABASE_URL",
                format!(
                    "postgres://insight_history_dev:{}@127.0.0.1:5432/insight_platform",
                    password(runtime)?
                ),
            ),
        ],
    })
}
