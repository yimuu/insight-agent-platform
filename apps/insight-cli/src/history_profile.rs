//! Development composition for the isolated history maintenance role.
use super::*;
pub(crate) const CONFIG_FILE: &str = "history-maintenance.json";
const PASSWORD_FILE: &str = "history-database-password";

pub(crate) fn password(runtime: &Path) -> Result<String, CliError> {
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
