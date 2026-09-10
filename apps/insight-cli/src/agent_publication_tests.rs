use super::*;
use insight_platform_agent_compiler::ArtifactIntent;
use insight_platform_contracts::{ArtifactRef, ArtifactState};
use std::{net::TcpListener, thread};
fn id(kind: ResourceKind) -> ResourceId {
    ResourceId::from_uuid_v7(kind, Uuid::now_v7()).unwrap()
}
fn authority(intent: &ArtifactIntent) -> ArtifactAuthority {
    ArtifactAuthority {
        purpose: intent.purpose,
        state: ArtifactState::Ready,
        artifact: ArtifactRef::new(
            id(ResourceKind::Artifact),
            intent.content_digest.clone(),
            intent.byte_length,
            intent.media_type.clone(),
            intent.classification,
            intent.display_name.clone(),
        )
        .unwrap(),
    }
}
fn journal(compilation: &AgentCompilationV1, agent: ResourceId) -> AgentUpdateJournalV3 {
    AgentUpdateJournalV3 {
        attempt_id: id(ResourceKind::ServerRequest),
        base_resource_version: 7,
        base_active_deployment_id: None,
        source_bundle_digest: compilation.source_bundle_digest.clone(),
        schema_version: 3,
        kind: "insight.platform.agent-update-journal/v3".into(),
        manifest_digest: compilation.compiled.manifest_digest.clone(),
        agent_id: agent,
        updated_resource_etag: None,
        validation_operation_id: None,
        validated_resource_etag: None,
        draft_generation: None,
        published_versions: None,
        published_resource_etag: None,
        deployment_id: None,
        deployed_resource_etag: None,
        final_resource_etag: None,
    }
}
fn compilation(root: &Path) -> AgentCompilationV1 {
    compile_project(
        root,
        capture_project_sources(root, Path::new("agent.json")).unwrap(),
        offline_compiler_profile(root).unwrap(),
    )
    .unwrap()
}
#[test]
fn update_attempts_have_distinct_receipts_and_strict_phase_identity() {
    let directory = crate::agent_entry_tests::project();
    let compilation = compilation(directory.path());
    let agent = id(ResourceKind::Agent);
    let first = journal(&compilation, agent.clone());
    let second = journal(&compilation, agent.clone());
    assert_ne!(
        publication_receipt(&first.attempt_id, "publish"),
        publication_receipt(&second.attempt_id, "publish")
    );
    validate_update_journal(&first, &compilation, &agent).unwrap();
    for mutate in 0..5 {
        let mut bad = first.clone();
        match mutate {
            0 => bad.attempt_id = id(ResourceKind::Job),
            1 => bad.base_resource_version = 0,
            2 => bad.validation_operation_id = Some(id(ResourceKind::Job)),
            3 => bad.updated_resource_etag = Some(format!("\"{agent}-08\"")),
            _ => {
                bad.final_resource_etag =
                    Some(insight_platform_api::resource::resource_etag(&agent, 9))
            }
        }
        assert!(validate_update_journal(&bad, &compilation, &agent).is_err());
    }
}
#[test]
fn lost_update_response_reuses_original_attempt_body_and_base_etag_over_http() {
    let directory = crate::agent_entry_tests::project();
    let compilation = compilation(directory.path());
    let source = authority(&compilation.compiled.resource_intent.authoring_artifact);
    let plan = authority(&compilation.compiled.resource_intent.typed_plan_artifact);
    let document = compilation.materialize(&source, &plan).unwrap();
    let agent = id(ResourceKind::Agent);
    let tenant = id(ResourceKind::Tenant);
    let pending = journal(&compilation, agent.clone());
    let expected_receipt = publication_receipt(&pending.attempt_id, "update");
    let expected_etag = insight_platform_api::resource::resource_etag(&agent, 7);
    let response = ResourceViewV1 {
        schema_version: 1,
        resource_id: agent.clone(),
        active_deployment_id: None,
        resource_kind: RegistryResourceKind::Agent,
        lifecycle_state: EntityLifecycle::Active,
        gate_state: AdministrativeGate::Suspended,
        draft_generation: 4,
        version: 8,
        draft: ResourceDraftPayload {
            alias: None,
            display_name: compilation.compiled.resource_intent.display_name.clone(),
            document: document.clone(),
            validation: None,
        },
        etag: insight_platform_api::resource::resource_etag(&agent, 8),
    };
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let client = PublicHttpClient::new(
        format!("http://{}", listener.local_addr().unwrap()),
        "fixture".into(),
        Duration::from_secs(3),
    )
    .unwrap();
    let server = thread::spawn(move || {
        let mut first_request = None;
        for request_no in 0..3 {
            listener.set_nonblocking(true).unwrap();
            let until = std::time::Instant::now() + Duration::from_secs(5);
            let (mut stream, _) = loop {
                match listener.accept() {
                    Ok(connection) => break connection,
                    Err(error)
                        if error.kind() == std::io::ErrorKind::WouldBlock
                            && std::time::Instant::now() < until =>
                    {
                        thread::sleep(Duration::from_millis(5))
                    }
                    Err(error) => panic!("bounded publication HTTP fixture accept: {error}"),
                }
            };
            stream
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let mut bytes = Vec::new();
            let (header_end, content_length) = loop {
                let mut one = [0u8; 1];
                stream.read_exact(&mut one).unwrap();
                bytes.push(one[0]);
                assert!(bytes.len() < 1_048_576);
                if bytes.ends_with(b"\r\n\r\n") {
                    let headers = std::str::from_utf8(&bytes).unwrap();
                    let length = headers
                        .lines()
                        .find_map(|line| {
                            line.to_ascii_lowercase()
                                .strip_prefix("content-length: ")
                                .map(|value| value.parse::<usize>().unwrap())
                        })
                        .unwrap_or(0);
                    break (bytes.len(), length);
                }
            };
            bytes.resize(header_end + content_length, 0);
            stream.read_exact(&mut bytes[header_end..]).unwrap();
            let headers = std::str::from_utf8(&bytes[..header_end])
                .unwrap()
                .to_ascii_lowercase();
            if request_no < 2 {
                assert!(headers.starts_with("put "));
                assert!(headers.contains(&format!("idempotency-key: {expected_receipt}")));
                assert!(headers.contains(&format!("if-match: {expected_etag}")));
                let body: serde_json::Value = serde_json::from_slice(&bytes[header_end..]).unwrap();
                if let Some(first) = &first_request {
                    assert_eq!(first, &body);
                } else {
                    first_request = Some(body);
                }
            } else {
                assert!(headers.starts_with("post ") && headers.contains("/draft:validate"));
            }
            let trace = headers
                .lines()
                .find_map(|line| line.strip_prefix("traceparent: "))
                .unwrap()
                .split('-')
                .nth(1)
                .unwrap();
            let (status, body, etag) = if request_no == 1 {
                (
                    "200 OK",
                    serde_json::to_vec(&response).unwrap(),
                    response.etag.clone(),
                )
            } else {
                (
                    "503 Service Unavailable",
                    b"{}".to_vec(),
                    "\"fixture\"".into(),
                )
            };
            write!(stream, "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nETag: {etag}\r\nTrace-Id: {trace}\r\nCache-Control: no-store, private, max-age=0\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", body.len()).unwrap();
            stream.write_all(&body).unwrap();
        }
    });
    let entry = AgentLockEntryV2 {
        source_bundle_digest: compilation.source_bundle_digest.clone(),
        manifest_digest: compilation.compiled.manifest_digest.clone(),
        agent_id: agent.clone(),
        interface_revision_id: None,
        plan_revision_id: None,
        active_deployment_id: None,
        environment: Some("development".into()),
        last_success_at: UtcTimestamp::from_datetime(Utc::now()),
        latest_run_id: None,
    };
    let cache = directory.path().join("attempt");
    fs::create_dir(&cache).unwrap();
    for previous in [Some(pending.clone()), None] {
        let previous = previous.or_else(|| {
            load_update_journal(&cache.join("update.json"), &compilation, &agent).unwrap()
        });
        assert!(update_agent(
            directory.path(),
            &client,
            &tenant,
            &compilation,
            document.clone(),
            plan.artifact.artifact_id(),
            entry.clone(),
            Duration::from_secs(1),
            &cache,
            previous
        )
        .is_err());
    }
    server.join().unwrap();
    let saved = load_update_journal(&cache.join("update.json"), &compilation, &agent)
        .unwrap()
        .unwrap();
    assert_eq!(saved.attempt_id, pending.attempt_id);
    assert_eq!(saved.base_resource_version, 7);
    assert_eq!(
        saved.updated_resource_etag,
        Some(insight_platform_api::resource::resource_etag(&agent, 8))
    );
}

#[test]
fn current_authoring_profile_controls_environment_and_exact_execution_binding() {
    let corpus = workspace_fixture_profile();
    let profile = insight_platform_api::product::AgentAuthoringProfileV1::build_for_installation(
        "staging".to_owned(),
        corpus.execution_profile.clone(),
        vec![],
    )
    .unwrap();
    assert_eq!(compiler_profile(&profile).default_environment, "staging");
    assert_eq!(
        compiler_profile(&profile).execution_profile,
        corpus.execution_profile
    );
    let mut bad = profile;
    bad.default_environment = "production".to_owned();
    assert!(bad.validate().is_err());
}
fn workspace_fixture_profile() -> AgentCompilerProfile {
    let directory = crate::agent_entry_tests::project();
    offline_compiler_profile(directory.path()).unwrap()
}
