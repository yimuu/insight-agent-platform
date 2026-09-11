//! Model setup is a public client of the existing Credential, Artifact and Registry owners.
mod configuration;
mod management;
mod quota;
mod workflow;
use crate::{public_client::PublicHttpClient, CliError};
use insight_platform_contracts::{
    parse_strict_json, JsonLimits, RegistryResourceKind, ResourceId, ResourceKind,
};
use std::{
    ffi::OsString,
    path::{Path, PathBuf},
    time::Duration,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Action {
    Configure,
    Sources,
    List,
    Default,
    Probe,
    Credential,
    Revoke,
    Source,
    Get,
    Quota,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Command {
    pub action: Action,
    pub endpoint: String,
    pub tenant: ResourceId,
    pub token_file: PathBuf,
    pub ca_file: Option<PathBuf>,
    pub state_dir: PathBuf,
    pub file: Option<PathBuf>,
    pub new_attempt: bool,
    pub model: Option<String>,
    pub source: Option<String>,
    pub binding: Option<ResourceId>,
    pub clear: bool,
}
pub fn parse(args: &[OsString]) -> Result<Command, CliError> {
    let action=match args.first().and_then(|arg|arg.to_str()) {Some("configure")=>Action::Configure,Some("sources")=>Action::Sources,Some("list")=>Action::List,Some("default")=>Action::Default,Some("probe")=>Action::Probe,Some("credential")=>Action::Credential,Some("revoke")=>Action::Revoke,Some("source")=>Action::Source,Some("get")=>Action::Get,Some("quota")=>Action::Quota,_=>return Err(CliError::ModelConfiguration("usage: insight model <configure|sources|source|list|get|default|probe|credential|revoke|quota> --endpoint <origin> --tenant <tenant ID> --token-file <private file> [--file <configuration.json>] [--ca-file <public PEM bundle>] [--state-dir <private directory>] [--model <alias or ID>] [--source <alias or ID>] [--binding <binding ID>] [--clear] [--new-attempt]".to_owned()))};
    let mut fields = std::collections::BTreeMap::new();
    let mut new_attempt = false;
    let mut clear = false;
    let mut index = 1;
    while index < args.len() {
        let key = args[index].to_str().ok_or(CliError::Usage)?;
        if key == "--new-attempt"
            && matches!(action, Action::Configure | Action::Default | Action::Quota)
            && !new_attempt
        {
            new_attempt = true;
            index += 1;
            continue;
        }
        if key == "--clear" && action == Action::Default && !clear {
            clear = true;
            index += 1;
            continue;
        }
        if !matches!(
            key,
            "--endpoint"
                | "--tenant"
                | "--token-file"
                | "--ca-file"
                | "--state-dir"
                | "--file"
                | "--model"
                | "--source"
                | "--binding"
        ) || fields.contains_key(key)
        {
            return Err(CliError::Usage);
        }
        let value = args.get(index + 1).ok_or(CliError::Usage)?.clone();
        fields.insert(key.to_owned(), value);
        index += 2;
    }
    let text = |name: &str| {
        fields
            .get(name)
            .and_then(|value| value.to_str())
            .map(str::to_owned)
            .ok_or(CliError::Usage)
    };
    let file = fields.get("--file").map(PathBuf::from);
    let model = fields.get("--model").map(|_| text("--model")).transpose()?;
    let source = fields
        .get("--source")
        .map(|_| text("--source"))
        .transpose()?;
    let binding = fields
        .get("--binding")
        .map(|_| {
            ResourceId::parse_expected(&text("--binding")?, ResourceKind::SecretBinding)
                .map_err(|_| CliError::Usage)
        })
        .transpose()?;
    if (action == Action::Configure && file.is_none())
        || file.is_some() && !matches!(action, Action::Configure | Action::Quota)
        || fields.contains_key("--state-dir")
            && !matches!(
                action,
                Action::Configure | Action::Default | Action::Revoke | Action::Quota
            )
        || model.is_some()
            && !matches!(
                action,
                Action::Default | Action::Probe | Action::Get | Action::Quota
            )
        || matches!(action, Action::Probe | Action::Get | Action::Quota) && model.is_none()
        || source.is_some() != (action == Action::Source)
        || binding.is_some() != matches!(action, Action::Credential | Action::Revoke)
        || action == Action::Quota
            && file.is_none()
            && (new_attempt || fields.contains_key("--state-dir"))
        || clear && model.is_some()
        || new_attempt && action == Action::Default && model.is_none() && !clear
    {
        return Err(CliError::Usage);
    }
    for (selector, kind) in [
        (model.as_ref(), ResourceKind::ModelProfile),
        (source.as_ref(), ResourceKind::ModelProvider),
    ] {
        if let Some(value) = selector {
            if ResourceId::parse_expected(value, kind).is_err()
                && !(action == Action::Quota
                    && ResourceId::parse_expected(value, ResourceKind::ModelDeployment).is_ok())
                && value
                    .parse::<insight_platform_contracts::ResourceAlias>()
                    .is_err()
            {
                return Err(CliError::Usage);
            }
        }
    }
    Ok(Command {
        action,
        endpoint: text("--endpoint")?,
        tenant: ResourceId::parse_expected(&text("--tenant")?, ResourceKind::Tenant)
            .map_err(|_| CliError::Usage)?,
        token_file: PathBuf::from(fields.get("--token-file").ok_or(CliError::Usage)?),
        ca_file: fields.get("--ca-file").map(PathBuf::from),
        state_dir: fields
            .get("--state-dir")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(".insight-models")),
        file,
        new_attempt,
        model,
        source,
        binding,
        clear,
    })
}
pub fn execute(command: Command, cwd: &Path) -> Result<String, CliError> {
    execute_inner(command, cwd).map_err(CliError::ModelConfiguration)
}
fn execute_inner(command: Command, cwd: &Path) -> Result<String, String> {
    let ca_file = command.ca_file.as_ref().map(|path| absolute(cwd, path));
    let client = PublicHttpClient::from_connection_files(
        &command.endpoint,
        &absolute(cwd, &command.token_file),
        ca_file.as_deref(),
        None,
        Duration::from_secs(35),
    )?;
    let default = if matches!(
        command.action,
        Action::Credential | Action::Revoke | Action::Quota
    ) {
        None
    } else {
        Some(workflow::default(&client, &command.tenant)?)
    };
    let value = match command.action {
        Action::Sources => {
            serde_json::json!({"schema_version":1,"items":workflow::list(&client,RegistryResourceKind::ModelProvider)?})
        }
        Action::List => {
            serde_json::json!({"schema_version":1,"items":workflow::list(&client,RegistryResourceKind::ModelProfile)?})
        }
        Action::Default if command.model.is_none() && !command.clear => {
            serde_json::to_value(default.ok_or("default authority unavailable")?.body)
                .map_err(|_| "invalid model default response")?
        }
        Action::Default => {
            management::set_default(&client, &command, &absolute(cwd, &command.state_dir))?
        }
        Action::Probe => management::probe(&client, &command)?,
        Action::Credential => serde_json::to_value(management::credential(
            &client,
            &command.tenant,
            command.binding.as_ref().ok_or("binding required")?,
        )?)
        .map_err(|_| "invalid credential metadata")?,
        Action::Revoke => {
            management::revoke(&client, &command, &absolute(cwd, &command.state_dir))?
        }
        Action::Source => serde_json::to_value(management::resource(
            &client,
            RegistryResourceKind::ModelProvider,
            command.source.as_deref().ok_or("source required")?,
        )?)
        .map_err(|_| "invalid source metadata")?,
        Action::Get => serde_json::to_value(management::resource(
            &client,
            RegistryResourceKind::ModelProfile,
            command.model.as_deref().ok_or("model required")?,
        )?)
        .map_err(|_| "invalid model metadata")?,
        Action::Quota => quota::execute(&client, &command, cwd)?,
        Action::Configure => {
            let file = absolute(
                cwd,
                command
                    .file
                    .as_ref()
                    .ok_or("configuration file is required")?,
            );
            let metadata = std::fs::symlink_metadata(&file)
                .map_err(|_| "cannot read model configuration file")?;
            if !metadata.is_file()
                || metadata.file_type().is_symlink()
                || metadata.len() == 0
                || metadata.len() > 65_536
            {
                return Err("model configuration must be a bounded regular JSON file".to_owned());
            }
            let bytes = std::fs::read(&file).map_err(|_| "cannot read model configuration file")?;
            let value = parse_strict_json(
                &bytes,
                JsonLimits {
                    max_bytes: 65_536,
                    max_depth: 8,
                    max_items_per_array: 32,
                    max_properties_per_object: 16,
                    max_string_bytes: 2048,
                },
            )
            .map_err(|_| "model configuration JSON is not closed and bounded")?;
            let mut file_config: configuration::ConfigurationFileV1 = serde_json::from_value(value)
                .map_err(|_| "model configuration fields or environment mapping are invalid")?;
            let base = file
                .parent()
                .ok_or("configuration file parent is unavailable")?;
            for source in &mut file_config.sources {
                if let Some(path) = &mut source.api_key_file {
                    *path = absolute(base, Path::new(path))
                        .to_str()
                        .ok_or("credential file path encoding is invalid")?
                        .to_owned();
                }
            }
            workflow::configure(
                &client,
                &command,
                &file_config,
                &absolute(cwd, &command.state_dir),
            )?
        }
    };
    serde_json::to_string_pretty(&value)
        .map_err(|_| "cannot encode model command report".to_owned())
}
fn absolute(cwd: &Path, path: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_owned()
    } else {
        cwd.join(path)
    }
}

fn read_private_key_file(
    path: &Path,
) -> Result<insight_platform_contracts::SensitiveModelApiKey, String> {
    use std::io::Read;
    let initial =
        std::fs::symlink_metadata(path).map_err(|_| "cannot inspect mapped credential file")?;
    if !initial.is_file() || initial.file_type().is_symlink() {
        return Err("credential file must be regular".to_owned());
    }
    let mut options = std::fs::OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK);
    }
    let file = options
        .open(path)
        .map_err(|_| "cannot open mapped credential file")?;
    let metadata = file
        .metadata()
        .map_err(|_| "cannot inspect mapped credential file")?;
    if !metadata.is_file() || metadata.len() == 0 || metadata.len() > 4097 {
        return Err("credential file must be bounded and regular".to_owned());
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if metadata.ino() != initial.ino()
            || metadata.dev() != initial.dev()
            || metadata.nlink() != 1
            || !matches!(metadata.mode() & 0o777, 0o400 | 0o600)
        {
            return Err(
                "credential file requires 0400 or 0600 permissions and one link".to_owned(),
            );
        }
    }
    let mut bytes = Vec::new();
    file.take(4098)
        .read_to_end(&mut bytes)
        .map_err(|_| "cannot read credential file")?;
    if bytes.last() == Some(&b'\n') {
        bytes.pop();
    }
    insight_platform_contracts::SensitiveModelApiKey::new(bytes).map_err(|_| {
        "credential file must contain one visible-ASCII key with an optional final newline"
            .to_owned()
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn model_commands_reject_inline_secrets_and_ambiguous_selection() {
        let common = [
            "--endpoint",
            "http://127.0.0.1:8080",
            "--tenant",
            "ten_0198f1cc-32e4-75e1-a9e8-000000000001",
            "--token-file",
            "session.token",
        ];
        let command = |action: &str, extra: &[&str]| {
            std::iter::once(action)
                .chain(common)
                .chain(extra.iter().copied())
                .map(OsString::from)
                .collect::<Vec<_>>()
        };
        assert!(parse(&command("probe", &["--model", "work.chat"])).is_ok());
        assert!(parse(&command(
            "quota",
            &[
                "--model",
                "work.chat",
                "--file",
                "limits.json",
                "--new-attempt"
            ]
        ))
        .is_ok());
        assert!(parse(&command(
            "quota",
            &["--model", "mdep_0198f1cc-32e4-75e1-a9e8-000000000002"]
        ))
        .is_ok());
        assert!(parse(&command(
            "quota",
            &["--model", "work.chat", "--new-attempt"]
        ))
        .is_err());
        assert!(parse(&command("default", &["--clear", "--state-dir", "state"])).is_ok());
        for (action, args) in [
            ("probe", vec![]),
            ("default", vec!["--clear", "--model", "work.chat"]),
            ("sources", vec!["--api-key", "secret"]),
            ("get", vec!["--model", "../bad"]),
            ("default", vec!["--new-attempt"]),
        ] {
            assert!(parse(&command(action, &args)).is_err());
        }
    }
    #[cfg(unix)]
    #[test]
    fn mounted_keys_require_regular_private_files_and_bounded_single_line() {
        use std::os::unix::fs::{symlink, PermissionsExt};
        let root =
            std::env::temp_dir().join(format!("insight-model-key-test-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&root).unwrap();
        let path = root.join("key");
        std::fs::write(&path, b"test-private-key\n").unwrap();
        for mode in [0o400, 0o600] {
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(mode)).unwrap();
            assert_eq!(
                read_private_key_file(&path).unwrap().expose(),
                b"test-private-key"
            );
        }
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert!(read_private_key_file(&path).is_err());
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        let link = root.join("symlink");
        symlink(&path, &link).unwrap();
        assert!(read_private_key_file(&link).is_err());
        let hard = root.join("hard");
        std::fs::hard_link(&path, &hard).unwrap();
        assert!(read_private_key_file(&path).is_err());
        std::fs::remove_file(hard).unwrap();
        for bytes in [vec![b'a'; 4098], b"first\nsecond".to_vec(), vec![]] {
            std::fs::write(&path, bytes).unwrap();
            assert!(read_private_key_file(&path).is_err());
        }
        assert!(read_private_key_file(&root).is_err());
        std::fs::remove_dir_all(root).unwrap();
    }
}
