//! Entry-level authoring regressions: local source failures precede every HTTP request.
use super::*;
use std::net::TcpListener;
use tempfile::TempDir;

pub(crate) fn project() -> TempDir {
    let directory = TempDir::new().unwrap();
    initialize_project(directory.path(), Some("authoring-entry"), SystemTime::now()).unwrap();
    prepare_runtime_profile(
        directory.path(),
        "arn:aws:kms:us-east-1:000000000000:key/12345678-1234-1234-1234-123456789012",
        "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        &worker_profile::fixture_binaries(directory.path()),
    )
    .unwrap();
    let corpus = workspace_assets::workspace_path("contracts/product-experience/agent-compiler/v2");
    fs::copy(
        corpus.join("deterministic.json"),
        directory.path().join("agent.json"),
    )
    .unwrap();
    fs::copy(
        corpus.join("schema-message.json"),
        directory.path().join("schema-message.json"),
    )
    .unwrap();
    directory
}

#[test]
fn actual_entries_preflight_invalid_sources_before_profile_http_or_journal() {
    for invalid in ["agent.json", "schema-message.json"] {
        let directory = project();
        let runtime = directory
            .path()
            .join(PROJECT_DIRECTORY)
            .join(RUNTIME_DIRECTORY);
        let local = load_local_project_state(&directory.path().join(PROJECT_DIRECTORY)).unwrap();
        let profile = read_runtime_profile_state(&runtime, &local.identity)
            .unwrap()
            .unwrap();
        let listener = TcpListener::bind(("127.0.0.1", profile.ports.gateway_management)).unwrap();
        listener.set_nonblocking(true).unwrap();
        let original = fs::read(runtime.join(RUNTIME_PROFILE_STATE_FILE)).unwrap();
        fs::write(directory.path().join(invalid), b"{ invalid local source").unwrap();
        let errors = [
            execute(
                CliCommand::AgentValidate {
                    root: directory.path().to_owned(),
                    file: "agent.json".into(),
                    online: true,
                    output: agent::AgentOutputOptions::default(),
                },
                directory.path(),
                &SystemDoctorProbe,
            )
            .unwrap_err(),
            execute(
                CliCommand::AgentPublish {
                    root: directory.path().to_owned(),
                    file: "agent.json".into(),
                    wait: true,
                    output: agent::AgentOutputOptions::default(),
                },
                directory.path(),
                &SystemDoctorProbe,
            )
            .unwrap_err(),
        ];
        for error in errors {
            assert!(
                matches!(
                    error,
                    CliError::Agent(agent::AgentCommandError::Compiler(_))
                        | CliError::Agent(agent::AgentCommandError::InvalidLocalState(_))
                ),
                "source error must win: {error}"
            );
        }
        assert_eq!(
            listener.accept().unwrap_err().kind(),
            std::io::ErrorKind::WouldBlock
        );
        assert_eq!(
            fs::read(runtime.join(RUNTIME_PROFILE_STATE_FILE)).unwrap(),
            original
        );
        assert!(!directory.path().join(".insight/agent-publication").exists());
        assert!(!directory.path().join("insight.lock").exists());
    }
}

#[test]
fn captured_source_is_used_even_when_disk_changes_after_inspection() {
    let directory = project();
    let captured =
        agent::capture_project_sources(directory.path(), Path::new("agent.json")).unwrap();
    let original = fs::read_to_string(directory.path().join("agent.json")).unwrap();
    let profile = agent::offline_compiler_profile(directory.path()).unwrap();
    fs::write(
        directory.path().join("agent.json"),
        b"{ invalid replacement",
    )
    .unwrap();
    fs::write(
        directory.path().join("schema-message.json"),
        b"{ invalid replacement",
    )
    .unwrap();
    let compilation = agent::compile_project(directory.path(), captured, profile).unwrap();
    let bundle: insight_platform_agent_compiler::AgentSourceBundleV1 =
        serde_json::from_slice(&compilation.source_bundle_bytes).unwrap();
    assert_eq!(bundle.sources.files["agent.json"], original);
    assert!(agent::capture_project_sources(directory.path(), Path::new("agent.json")).is_err());
}

#[test]
fn authoring_reads_full_shared_bootstrap_and_rejects_current_profile_drift() {
    let directory = project();
    let profile = agent::offline_compiler_profile(directory.path()).unwrap();
    let runtime = directory
        .path()
        .join(PROJECT_DIRECTORY)
        .join(RUNTIME_DIRECTORY);
    let path = runtime
        .join(RUNTIME_CONFIGURATION_DIRECTORY)
        .join(RUNTIME_ARTIFACT_BOOTSTRAP_CONFIG_FILE);
    let mut bootstrap: serde_json::Value =
        serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
    assert_eq!(
        profile.execution_profile.revision.revision_id.to_string(),
        bootstrap["scheduling_policy_revision_id"]
    );
    bootstrap["unknown_authoring_field"] = serde_json::json!(true);
    fs::write(&path, serde_json::to_vec(&bootstrap).unwrap()).unwrap();
    assert!(agent::offline_compiler_profile(directory.path()).is_err());
}

#[test]
fn dispatcher_keeps_documented_cwd_paths_inside_the_selected_project() {
    let directory = project();
    let parent = directory.path().parent().unwrap();
    let name = directory.path().file_name().unwrap();
    for file in [
        PathBuf::from(name).join("agent.json"),
        directory.path().join("agent.json"),
    ] {
        execute(
            CliCommand::AgentValidate {
                root: PathBuf::from(name),
                file,
                online: false,
                output: agent::AgentOutputOptions::default(),
            },
            parent,
            &SystemDoctorProbe,
        )
        .unwrap();
    }
    for file in [
        directory.path().join("../agent.json"),
        parent.join("outside.json"),
    ] {
        assert!(execute(
            CliCommand::AgentValidate {
                root: directory.path().to_owned(),
                file,
                online: false,
                output: agent::AgentOutputOptions::default()
            },
            parent,
            &SystemDoctorProbe
        )
        .is_err());
    }
    #[cfg(unix)]
    {
        std::os::unix::fs::symlink(
            directory.path().join("agent.json"),
            directory.path().join("linked.json"),
        )
        .unwrap();
        assert!(execute(
            CliCommand::AgentValidate {
                root: directory.path().to_owned(),
                file: directory.path().join("linked.json"),
                online: false,
                output: agent::AgentOutputOptions::default()
            },
            parent,
            &SystemDoctorProbe
        )
        .is_err());
    }
}
