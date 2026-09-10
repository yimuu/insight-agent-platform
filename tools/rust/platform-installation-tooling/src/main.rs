//! Direct one-shot installation commands; this process never supervises serving roles.
mod jetstream_setup;
mod model_setup;
mod openbao_setup;
mod process_control;
mod readiness;
mod s3_setup;
mod storage_setup;
mod workflow;
use insight_platform_deployment_contracts::installation::{
    InstallationError, InstallationInputV1, INSTALLATION_MAX_BYTES,
};
use insight_platform_deployment_tooling::installation::{compose_input, PreparedInstallation};
use std::{io::Read as _, path::PathBuf};
#[tokio::main]
async fn main() {
    if let Err(error) = process_control::run(run()).await {
        eprintln!("{error}");
        std::process::exit(1);
    }
}
async fn run() -> Result<(), InstallationError> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    let (args, remote_context_file) = input_destination_options(&args)?;
    let remote_context_destinations = remote_context_file
        .map(|file| {
            insight_platform_deployment_tooling::installation::read_remote_context_destinations(
                std::path::Path::new(file),
            )
        })
        .transpose()?
        .unwrap_or_default();
    if let [command, name, digest, output_flag, output, port_flag, port] = args.as_slice() {
        if command == "native-input" && output_flag == "--output" && port_flag == "--port-base" {
            let input = insight_platform_deployment_tooling::native::native_input(
                name,
                digest
                    .parse()
                    .map_err(|_| InstallationError::InvalidInput)?,
                std::path::Path::new(output),
                port.parse().map_err(|_| InstallationError::InvalidInput)?,
            )?;
            let input = insight_platform_deployment_tooling::installation::with_remote_context_destinations(
                input, remote_context_destinations,
            )?;
            println!(
                "{}",
                serde_json::to_string_pretty(&input)
                    .map_err(|_| InstallationError::InvalidInput)?
            );
            return Ok(());
        }
    }
    if let [command, input_flag, input_file, output_flag, output, binaries_flag, binaries, console_flag, console, node_flag, node] =
        args.as_slice()
    {
        if command == "native-plan"
            && input_flag == "--input"
            && output_flag == "--output"
            && binaries_flag == "--binaries"
            && console_flag == "--console-directory"
            && node_flag == "--node"
        {
            let input = read_input(std::path::Path::new(input_file))?;
            let plan = insight_platform_deployment_tooling::native::native_plan(
                &input,
                std::path::Path::new(output),
                std::path::Path::new(binaries),
                std::path::Path::new(console),
                std::path::Path::new(node),
            )?;
            println!(
                "{}",
                serde_json::to_string_pretty(&plan).map_err(|_| InstallationError::InvalidInput)?
            );
            return Ok(());
        }
    }
    if let [command, input_flag, input_file, output_flag, output, uid_flag, uid, gid_flag, gid] =
        args.as_slice()
    {
        if command == "dependencies"
            && input_flag == "--input"
            && output_flag == "--output"
            && uid_flag == "--uid"
            && gid_flag == "--gid"
        {
            let input = read_input(std::path::Path::new(input_file))?;
            let uid = uid.parse().map_err(|_| InstallationError::InvalidInput)?;
            let gid = gid.parse().map_err(|_| InstallationError::InvalidInput)?;
            if insight_platform_deployment_tooling::role_output::native_user_ids()? != (uid, gid) {
                return Err(InstallationError::CredentialInvalid);
            }
            let document =
                insight_platform_deployment_tooling::dependency_profile::native_document(
                    &input,
                    std::path::Path::new(output),
                    uid,
                    gid,
                )?;
            println!(
                "{}",
                serde_json::to_string_pretty(&document)
                    .map_err(|_| InstallationError::InvalidInput)?
            );
            return Ok(());
        }
    }
    if let [command, input_flag, input_file, process_flag, process] = args.as_slice() {
        if command == "ready" && input_flag == "--input" && process_flag == "--process" {
            readiness::wait_process(&read_input(std::path::Path::new(input_file))?, process)
                .await?;
            println!("Selected installed process readiness probe passed");
            return Ok(());
        }
    }
    if let [command, input_flag, input_file] = args.as_slice() {
        if command == "ready" && input_flag == "--input" {
            readiness::wait(&read_input(std::path::Path::new(input_file))?).await?;
            println!("All installed process readiness probes passed");
            return Ok(());
        }
    }
    if let [command, name, package_digest] = args.as_slice() {
        if matches!(command.as_str(), "compose-input" | "kubernetes-input") {
            let package_digest = package_digest
                .parse()
                .map_err(|_| InstallationError::InvalidInput)?;
            let input = if command == "compose-input" {
                compose_input(name, package_digest)?
            } else {
                insight_platform_deployment_tooling::kubernetes::kubernetes_input(
                    name,
                    package_digest,
                )?
            };
            let input = insight_platform_deployment_tooling::installation::with_remote_context_destinations(
                input, remote_context_destinations,
            )?;
            println!(
                "{}",
                serde_json::to_string_pretty(&input)
                    .map_err(|_| InstallationError::InvalidInput)?
            );
            return Ok(());
        }
    }
    if let [command, input_flag, input_file, runtime_flag, runtime, console_flag, console] =
        args.as_slice()
    {
        if matches!(command.as_str(), "compose" | "helm-plan")
            && input_flag == "--input"
            && runtime_flag == "--runtime-image"
            && console_flag == "--console-image"
        {
            let input_file = PathBuf::from(input_file);
            let input = read_input(&input_file)?;
            let document = if command == "compose" {
                insight_platform_deployment_tooling::compose::compose_document(
                    &input,
                    &input_file,
                    runtime,
                    console,
                )?
            } else {
                insight_platform_deployment_tooling::kubernetes::helm_plan(
                    &input, runtime, console,
                )?
            };
            println!(
                "{}",
                serde_json::to_string_pretty(&document)
                    .map_err(|_| InstallationError::InvalidInput)?
            );
            return Ok(());
        }
    }
    let (common, output, binaries) = match args.as_slice() {
        [common @ .., output_flag, output, binary_flag, binaries]
            if common.len() == 5 && output_flag == "--output" && binary_flag == "--binaries" =>
        {
            (
                common,
                Some(PathBuf::from(output)),
                Some(PathBuf::from(binaries)),
            )
        }
        [common @ .., output_flag, output] if common.len() == 5 && output_flag == "--output" => {
            (common, Some(PathBuf::from(output)), None)
        }
        common if common.len() == 5 => (common, None, None),
        _ => return Err(InstallationError::InvalidInput),
    };
    let [command, input_flag, input_file, state_flag, state_directory] = common else {
        return Err(InstallationError::InvalidInput);
    };
    if !valid_command_options(command, output.is_some(), binaries.is_some())
        || input_flag != "--input"
        || state_flag != "--state"
    {
        return Err(InstallationError::InvalidInput);
    }
    let input_file = PathBuf::from(input_file);
    let state = PathBuf::from(state_directory);
    if !input_file.is_absolute() || !state.is_absolute() {
        return Err(InstallationError::InvalidPath);
    }
    let input = read_input(&input_file)?;
    match command.as_str() {
        "provider-observe" => {
            let prepared = PreparedInstallation::open(&input, &state)?;
            let evidence = openbao_setup::observe(&prepared, &state).await?;
            prepared.complete_provider(evidence)?;
            println!(
                "{}",
                serde_json::json!({
                    "schema_version":1,"input_digest":prepared.progress().input_digest,
                    "identity_digest":prepared.progress().identity_digest,"phase":"provider_ready",
                })
            );
        }
        "provider-start" => {
            let prepared = PreparedInstallation::open(&input, &state)?;
            let mode = prepared.request_provider_start()?;
            println!(
                "{}",
                serde_json::json!({
                    "schema_version":1,"input_digest":prepared.progress().input_digest,
                    "identity_digest":prepared.progress().identity_digest,"mode":mode,
                })
            );
        }
        "prepare" => {
            let prepared = workflow::prepare(
                &input,
                &state,
                output.as_deref().ok_or(InstallationError::InvalidInput)?,
            )?;
            println!(
                "{}",
                serde_json::json!({"schema_version":1,"phase":prepared.progress().phase,"input_digest":prepared.progress().input_digest,"identity_digest":prepared.progress().identity_digest})
            );
        }
        "provision" | "verify" => {
            let prepared = workflow::configure(
                &input,
                &state,
                output.as_deref().ok_or(InstallationError::InvalidInput)?,
                binaries.as_deref().ok_or(InstallationError::InvalidInput)?,
                command == "verify",
            )
            .await?;
            println!(
                "{}",
                serde_json::json!({"schema_version":1,"phase":prepared.progress().phase,"input_digest":prepared.progress().input_digest,"identity_digest":prepared.progress().identity_digest})
            );
        }
        "public-trust" => {
            let prepared = PreparedInstallation::open_read_only(&input, &state)?;
            let trust = prepared.public_trust()?;
            println!(
                "{}",
                serde_json::to_string(&trust).map_err(|_| InstallationError::InvalidInput)?
            );
        }
        "session" => {
            let prepared = PreparedInstallation::open(&input, &state)?;
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_err(|_| InstallationError::InvalidInput)?
                .as_secs();
            let file = prepared.issue_session(now)?;
            let delivery = insight_platform_deployment_contracts::installation::InstallationSessionDeliveryV1 {
                schema_version: 1,
                input_digest: prepared.progress().input_digest.clone(),
                identity_digest: prepared.progress().identity_digest.clone(),
                session_file: file.display().to_string(),
                tenant_id: prepared.identity().session.tenant_id.clone(),
                endpoint: input.network.console_origin.clone(),
                expires_at_unix_seconds: now + insight_platform_deployment_contracts::installation::INSTALLATION_SESSION_SECONDS,
            };
            delivery.validate_for(&input, prepared.identity(), now)?;
            println!(
                "{}",
                serde_json::to_string(&delivery).map_err(|_| InstallationError::InvalidInput)?
            );
        }
        _ => return Err(InstallationError::InvalidInput),
    }
    Ok(())
}

fn read_input(path: &std::path::Path) -> Result<InstallationInputV1, InstallationError> {
    if !path.is_absolute() {
        return Err(InstallationError::InvalidPath);
    }
    let metadata = std::fs::symlink_metadata(path).map_err(|_| InstallationError::InvalidInput)?;
    if !metadata.is_file()
        || metadata.file_type().is_symlink()
        || metadata.len() > INSTALLATION_MAX_BYTES as u64
    {
        return Err(InstallationError::InvalidInput);
    }
    let mut bytes = Vec::new();
    std::fs::File::open(path)
        .map_err(|_| InstallationError::InvalidInput)?
        .take(INSTALLATION_MAX_BYTES as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| InstallationError::InvalidInput)?;
    InstallationInputV1::decode(&bytes)
}

fn input_destination_options(
    args: &[String],
) -> Result<(Vec<String>, Option<&str>), InstallationError> {
    if matches!(
        args.first().map(String::as_str),
        Some("compose-input" | "kubernetes-input" | "native-input")
    ) && args.len() >= 2
        && args[args.len() - 2] == "--remote-context-destinations"
    {
        return Ok((args[..args.len() - 2].to_vec(), Some(&args[args.len() - 1])));
    }
    Ok((args.to_vec(), None))
}

fn valid_command_options(command: &str, output: bool, binaries: bool) -> bool {
    matches!(
        (command, output, binaries),
        ("prepare", true, false)
            | ("provision" | "verify", true, true)
            | (
                "session" | "public-trust" | "provider-start" | "provider-observe",
                false,
                false
            )
    )
}

#[cfg(test)]
mod command_tests {
    use super::{input_destination_options, valid_command_options};
    #[test]
    fn optional_destination_file_belongs_only_to_input_declarations() {
        for command in ["compose-input", "kubernetes-input", "native-input"] {
            let args = [
                command,
                "name",
                "digest",
                "--remote-context-destinations",
                "/public/destinations.json",
            ]
            .map(str::to_owned);
            let (selected, file) = input_destination_options(&args).unwrap();
            assert_eq!(selected, args[..3]);
            assert_eq!(file, Some("/public/destinations.json"));
        }
        for command in [
            "prepare",
            "provision",
            "verify",
            "session",
            "public-trust",
            "unknown",
        ] {
            let args = [
                command,
                "--remote-context-destinations",
                "/public/destinations.json",
            ]
            .map(str::to_owned);
            let (selected, file) = input_destination_options(&args).unwrap();
            assert_eq!(selected, args);
            assert!(file.is_none());
        }
    }
    #[test]
    fn commands_reject_irrelevant_or_missing_effect_inputs() {
        for command in [
            "prepare",
            "provision",
            "verify",
            "session",
            "public-trust",
            "provider-start",
            "provider-observe",
            "unknown",
        ] {
            for output in [false, true] {
                for binaries in [false, true] {
                    let expected = match command {
                        "prepare" => output && !binaries,
                        "provision" | "verify" => output && binaries,
                        "session" | "public-trust" | "provider-start" | "provider-observe" => {
                            !output && !binaries
                        }
                        _ => false,
                    };
                    assert_eq!(valid_command_options(command, output, binaries), expected);
                }
            }
        }
    }
}
