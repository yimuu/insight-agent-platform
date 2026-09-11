use super::*;
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use insight_platform_contracts::{RunState, UtcTimestamp};
use std::{net::TcpListener, thread, time::Instant};
use tempfile::TempDir;

fn id(kind: ResourceKind) -> ResourceId {
    ResourceId::from_uuid_v7(kind, uuid::Uuid::now_v7()).unwrap()
}
fn token(tenant: &ResourceId, expiry: u64) -> String {
    format!(
        "e30.{}.c2ln",
        URL_SAFE_NO_PAD.encode(
            serde_json::to_vec(&serde_json::json!({"tenant_id":tenant,"exp":expiry})).unwrap()
        )
    )
}
fn valid_token(tenant: &ResourceId) -> String {
    token(
        tenant,
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 900,
    )
}
fn write_private(path: &Path, bytes: &[u8]) {
    let mut options = private_options();
    let mut f = options
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)
        .unwrap();
    f.write_all(bytes).unwrap();
}
fn fixture(endpoint: &str, runtime: Option<&str>) -> (TempDir, Command) {
    let directory = TempDir::new().unwrap();
    let tenant = id(ResourceKind::Tenant);
    let file = directory.path().join("session.jwt");
    write_private(&file, valid_token(&tenant).as_bytes());
    let command = Command {
        root: directory.path().to_owned(),
        endpoint: endpoint.into(),
        runtime_endpoint: runtime.map(str::to_owned),
        tenant_id: tenant,
        token_file: file,
        ca_file: None,
    };
    (directory, command)
}
fn response(stream: &mut (impl Read + Write), body: &str, etag: &str) -> String {
    let mut request = Vec::new();
    while !request.ends_with(b"\r\n\r\n") {
        let mut byte = [0];
        if stream.read(&mut byte).unwrap_or(0) == 0 {
            return String::new();
        }
        request.push(byte[0]);
        assert!(request.len() < 70_000);
    }
    write!(stream,"HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncache-control: no-store, private, max-age=0\r\ntrace-id: 1234567890abcdef1234567890abcdef\r\netag: {etag}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",body.len()).unwrap();
    String::from_utf8(request).unwrap()
}
fn accept(listener: &TcpListener) -> std::net::TcpStream {
    listener.set_nonblocking(true).unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match listener.accept() {
            Ok((stream, _)) => {
                stream.set_nonblocking(false).unwrap();
                stream
                    .set_read_timeout(Some(Duration::from_secs(5)))
                    .unwrap();
                stream
                    .set_write_timeout(Some(Duration::from_secs(5)))
                    .unwrap();
                return stream;
            }
            Err(error)
                if error.kind() == std::io::ErrorKind::WouldBlock && Instant::now() < deadline =>
            {
                thread::sleep(Duration::from_millis(2))
            }
            Err(error) => panic!("fixture accept failed: {error}"),
        }
    }
}
fn server(body: String, etag: String) -> (String, thread::JoinHandle<String>) {
    let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    (
        endpoint,
        thread::spawn(move || response(&mut accept(&listener), &body, &etag)),
    )
}

#[test]
fn parser_requires_explicit_bounded_connection_inputs() {
    let tenant = id(ResourceKind::Tenant).to_string();
    let args = [
        "--endpoint",
        "http://127.0.0.1:8123",
        "--tenant",
        &tenant,
        "--token-file",
        "session.jwt",
    ];
    let parsed = parse(&args.map(OsString::from)).unwrap();
    assert_eq!(parsed.runtime_endpoint, None);
    for extra in [
        vec!["--endpoint", "http://127.0.0.1:9"],
        vec!["--token", "secret"],
        vec!["--ca-file"],
    ] {
        let mut input = args.map(OsString::from).to_vec();
        input.extend(extra.into_iter().map(OsString::from));
        assert!(parse(&input).is_err());
    }
    let mut wrong = args.map(OsString::from);
    wrong[3] = id(ResourceKind::Agent).to_string().into();
    assert!(parse(&wrong).is_err());
}

#[test]
fn public_entries_route_both_surfaces_without_local_installation_state() {
    let run_id = id(ResourceKind::Run);
    let time: UtcTimestamp = "2026-09-10T00:00:00.000000Z".parse().unwrap();
    let run = crate::run::RunViewV1 {
        schema_version: 1,
        run_id: run_id.clone(),
        agent_deployment_id: id(ResourceKind::AgentDeployment),
        state: RunState::Queued,
        version: 1,
        input_value_id: id(ResourceKind::RunValue),
        output_value_id: None,
        pause_generation: 0,
        cancel_generation: 0,
        deadline: time.clone(),
        started_at: None,
        terminal_at: None,
        created_at: time.clone(),
        updated_at: time,
        etag: format!("\"{run_id}-1\""),
    };
    let (management, management_server) = server("{\"ok\":true}".into(), "\"fixture-1\"".into());
    let (runtime, runtime_server) = server(serde_json::to_string(&run).unwrap(), run.etag.clone());
    let (directory, command) = fixture(&management, Some(&runtime));
    let report = execute(command.clone(), directory.path()).unwrap();
    assert!(report.contains("connection_saved"));
    assert!(report.contains("\"authentication_verified\": false"));
    let (client, tenant) = crate::local_public_http_client(directory.path()).unwrap();
    assert_eq!(tenant, command.tenant_id);
    let observed: crate::public_client::PublicJsonResponse<serde_json::Value> = client
        .get_json("/v1/agents", reqwest::StatusCode::OK)
        .unwrap();
    assert_eq!(observed.body["ok"], true);
    let body = crate::read_local_run(directory.path(), run_id.to_string().as_str()).unwrap();
    assert!(body.contains(run_id.to_string().as_str()));
    assert!(management_server
        .join()
        .unwrap()
        .starts_with("GET /v1/agents HTTP/1.1"));
    assert!(runtime_server
        .join()
        .unwrap()
        .starts_with(&format!("GET /v1/runs/{run_id} HTTP/1.1")));
    for forbidden in ["project.json", "runtime", "identity"] {
        assert!(!directory.path().join(".insight").join(forbidden).exists());
    }
    let stored = fs::read_to_string(directory.path().join(".insight/connection.json")).unwrap();
    assert!(!stored.contains(&valid_token(&tenant)));
    assert!(!stored.contains("principal_kind"));
}

#[test]
fn connection_is_idempotent_references_refresh_but_target_and_journals_never_rebind() {
    let (directory, mut command) = fixture("http://127.0.0.1:8123", None);
    execute(command.clone(), directory.path()).unwrap();
    let path = directory.path().join(".insight/connection.json");
    let original = fs::read(&path).unwrap();
    let metadata = fs::metadata(&path).unwrap().modified().unwrap();
    execute(command.clone(), directory.path()).unwrap();
    assert_eq!(fs::metadata(&path).unwrap().modified().unwrap(), metadata);
    let journal = directory.path().join(".insight/task-control");
    fs::create_dir(&journal).unwrap();
    let intent = b"original receipt and CAS intent";
    fs::write(journal.join("intent.json"), intent).unwrap();
    for mutate in 0..3 {
        let mut wrong = command.clone();
        match mutate {
            0 => wrong.endpoint = "http://127.0.0.1:8124".into(),
            1 => wrong.runtime_endpoint = Some("http://127.0.0.1:8124".into()),
            _ => {
                wrong.tenant_id = id(ResourceKind::Tenant);
                write_private(&wrong.token_file, valid_token(&wrong.tenant_id).as_bytes());
            }
        }
        assert!(execute(wrong, directory.path()).is_err());
        assert_eq!(fs::read(&path).unwrap(), original);
        assert_eq!(fs::read(journal.join("intent.json")).unwrap(), intent);
    }
    command.token_file = directory.path().join("renewed.jwt");
    write_private(
        &command.token_file,
        valid_token(&command.tenant_id).as_bytes(),
    );
    execute(command.clone(), directory.path()).unwrap();
    assert_eq!(
        load(directory.path()).unwrap().token_file,
        fs::canonicalize(command.token_file).unwrap()
    );
    assert_eq!(fs::read(journal.join("intent.json")).unwrap(), intent);
}

#[test]
fn token_refresh_is_read_each_time_and_wrong_tenant_or_expiry_makes_zero_requests() {
    let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    listener.set_nonblocking(true).unwrap();
    let (directory, command) = fixture(&format!("http://{}", listener.local_addr().unwrap()), None);
    execute(command.clone(), directory.path()).unwrap();
    for value in [
        valid_token(&id(ResourceKind::Tenant)),
        token(&command.tenant_id, 1),
        "not.a.jwt".into(),
        format!(
            "e30.{}.c2ln",
            URL_SAFE_NO_PAD.encode(format!(
                "{{\"tenant_id\":\"{}\",\"tenant_id\":\"{}\",\"exp\":9999999999}}",
                command.tenant_id, command.tenant_id
            ))
        ),
    ] {
        write_private(&command.token_file, value.as_bytes());
        let error = crate::local_public_http_client(directory.path())
            .unwrap_err()
            .to_string();
        assert!(!error.contains(&value));
        assert_eq!(
            listener.accept().unwrap_err().kind(),
            std::io::ErrorKind::WouldBlock
        );
    }
    write_private(
        &command.token_file,
        valid_token(&command.tenant_id).as_bytes(),
    );
    crate::local_runtime_http_client(directory.path()).unwrap();
    fs::remove_file(&command.token_file).unwrap();
    assert!(load(directory.path()).unwrap().client(false).is_err());
}

#[test]
fn first_binding_and_general_clients_never_adopt_existing_state() {
    for entry in [
        "project.json",
        "identity",
        "runtime",
        "artifact-upload",
        "agent-publication",
        "unknown-future-journal",
        "insight.lock",
    ] {
        let (directory, command) = fixture("http://127.0.0.1:8123", None);
        ensure_directory(&directory.path().join(".insight")).unwrap();
        let path = if entry == "insight.lock" {
            directory.path().join(entry)
        } else {
            directory.path().join(".insight").join(entry)
        };
        fs::write(&path, b"preserve this original state").unwrap();
        assert!(execute(command, directory.path()).is_err());
        assert_eq!(fs::read(&path).unwrap(), b"preserve this original state");
        assert!(!directory.path().join(".insight/connection.json").exists());
        assert!(crate::local_public_http_client(directory.path()).is_err());
    }
    let qualification = crate::agent_entry_tests::project();
    assert!(crate::local_public_http_client(qualification.path()).is_err());
    export_qualification(qualification.path()).unwrap();
    crate::local_public_http_client(qualification.path()).unwrap();
    let before = fs::read(qualification.path().join(".insight/connection.json")).unwrap();
    export_qualification(qualification.path()).unwrap();
    assert_eq!(
        fs::read(qualification.path().join(".insight/connection.json")).unwrap(),
        before
    );
}

#[test]
fn workspace_mutation_lock_needs_no_runtime_and_cannot_be_shared_concurrently() {
    let (directory, command) = fixture("http://127.0.0.1:8123", None);
    execute(command, directory.path()).unwrap();
    let first = acquire_mutation_lock(directory.path()).unwrap();
    assert!(acquire_mutation_lock(directory.path()).is_err());
    assert!(!directory.path().join(".insight/runtime").exists());
    drop(first);
    acquire_mutation_lock(directory.path()).unwrap();
}

#[test]
fn connection_fields_and_private_files_are_strictly_closed() {
    let (directory, command) = fixture("http://127.0.0.1:8123", None);
    execute(command, directory.path()).unwrap();
    let path = directory.path().join(".insight/connection.json");
    let original = fs::read(&path).unwrap();
    let base: serde_json::Value = serde_json::from_slice(&original).unwrap();
    for (key, value) in [
        ("schema_version", serde_json::json!(2)),
        ("unknown", serde_json::json!(true)),
        (
            "management_endpoint",
            serde_json::json!("http://example.com"),
        ),
        (
            "runtime_endpoint",
            serde_json::json!("https://user:secret@example.com"),
        ),
        ("token_file", serde_json::json!("relative")),
        ("tenant_id", serde_json::json!(id(ResourceKind::Agent))),
    ] {
        let mut changed = base.clone();
        changed[key] = value;
        write_private(&path, &serde_json::to_vec(&changed).unwrap());
        assert!(load(directory.path()).is_err());
    }
    write_private(&path, &[b' '; MAX_BYTES as usize + 1]);
    assert!(load(directory.path()).is_err());
    let duplicate =
        String::from_utf8(original.clone())
            .unwrap()
            .replacen('{', "{\"schema_version\":1,", 1);
    write_private(&path, duplicate.as_bytes());
    assert!(load(directory.path()).is_err());
    write_private(&path, &original);
    #[cfg(unix)]
    {
        use std::os::unix::fs::{symlink, PermissionsExt};
        fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();
        assert!(load(directory.path()).is_err());
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
        let sibling = path.with_extension("other");
        fs::hard_link(&path, &sibling).unwrap();
        assert!(load(directory.path()).is_err());
        fs::remove_file(&sibling).unwrap();
        fs::rename(&path, &sibling).unwrap();
        symlink(&sibling, &path).unwrap();
        assert!(load(directory.path()).is_err());
    }
}

#[cfg(unix)]
#[test]
fn unsafe_or_missing_token_is_rejected_before_workspace_creation() {
    use std::os::unix::fs::{symlink, PermissionsExt};
    for case in 0..5 {
        let (directory, command) = fixture("http://127.0.0.1:8123", None);
        match case {
            0 => fs::remove_file(&command.token_file).unwrap(),
            1 => {
                fs::set_permissions(&command.token_file, fs::Permissions::from_mode(0o644)).unwrap()
            }
            2 => {
                fs::hard_link(&command.token_file, directory.path().join("second.jwt")).unwrap();
            }
            3 => {
                let real = directory.path().join("real.jwt");
                fs::rename(&command.token_file, &real).unwrap();
                symlink(real, &command.token_file).unwrap();
            }
            _ => write_private(&command.token_file, &[b'x'; 65_538]),
        }
        assert!(execute(command, directory.path()).is_err());
        assert!(!directory.path().join(".insight").exists());
    }
}

#[test]
fn explicit_ca_supports_https_and_wrong_san_fails_without_http() {
    let certificate = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    for (wrong_san, missing_ca) in [(false, false), (true, false), (false, true)] {
        let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let port = listener.local_addr().unwrap().port();
        let config = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(
                vec![certificate.cert.der().clone()],
                rustls::pki_types::PrivateKeyDer::Pkcs8(
                    certificate.signing_key.serialize_der().into(),
                ),
            )
            .unwrap();
        let worker = thread::spawn(move || {
            let stream = accept(&listener);
            let connection = rustls::ServerConnection::new(std::sync::Arc::new(config)).unwrap();
            let mut stream = rustls::StreamOwned::new(connection, stream);
            response(&mut stream, "{\"ok\":true}", "\"fixture-1\"")
        });
        let (directory, mut command) = fixture(
            &format!(
                "https://{}:{port}",
                if wrong_san { "127.0.0.1" } else { "localhost" }
            ),
            None,
        );
        let ca = directory.path().join("ca.pem");
        fs::write(&ca, certificate.cert.pem()).unwrap();
        command.ca_file = (!missing_ca).then_some(ca);
        execute(command, directory.path()).unwrap();
        let client = crate::local_public_http_client(directory.path()).unwrap().0;
        let result = client.get_json::<serde_json::Value>("/v1/agents", reqwest::StatusCode::OK);
        assert_eq!(result.is_ok(), !wrong_san && !missing_ca, "{result:?}");
        assert_eq!(worker.join().unwrap().is_empty(), wrong_san || missing_ca);
    }
}

#[test]
fn explicit_connection_ca_reaches_artifact_and_agent_publication_uploads() {
    fn request(stream: &mut (impl Read + Write)) -> Option<(String, Vec<u8>)> {
        let mut bytes = Vec::new();
        let end = loop {
            let mut block = [0; 4096];
            let length = stream.read(&mut block).ok()?;
            if length == 0 {
                return None;
            }
            bytes.extend_from_slice(&block[..length]);
            assert!(bytes.len() <= 131_072);
            if let Some(position) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
                break position + 4;
            }
        };
        let head = String::from_utf8(bytes[..end].to_vec()).unwrap();
        let length = head
            .lines()
            .find_map(|line| {
                let (name, value) = line.split_once(':')?;
                name.eq_ignore_ascii_case("content-length")
                    .then(|| value.trim().parse::<usize>().unwrap())
            })
            .unwrap_or(0);
        assert!(end + length <= 131_072);
        while bytes.len() < end + length {
            let mut block = [0; 4096];
            let length = stream.read(&mut block).unwrap();
            assert!(length > 0);
            bytes.extend_from_slice(&block[..length]);
        }
        Some((head, bytes[end..end + length].to_vec()))
    }

    let mut failures = Vec::new();
    for publication in [false, true] {
        for (missing_ca, wrong_san) in [(false, false), (true, false), (false, true)] {
            let object_listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
            let object_port = object_listener.local_addr().unwrap().port();
            let certificate = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
            let tls = rustls::ServerConfig::builder()
                .with_no_client_auth()
                .with_single_cert(
                    vec![certificate.cert.der().clone()],
                    rustls::pki_types::PrivateKeyDer::Pkcs8(
                        certificate.signing_key.serialize_der().into(),
                    ),
                )
                .unwrap();
            let object_worker = thread::spawn(move || {
                let socket = accept(&object_listener);
                let connection = rustls::ServerConnection::new(std::sync::Arc::new(tls)).unwrap();
                let mut stream = rustls::StreamOwned::new(connection, socket);
                let received = request(&mut stream);
                if received.is_some() {
                    // Stop after proving the actual isolated HTTPS PUT. No synthetic complete
                    // or Ready result pretends to qualify the rest of the Artifact pipeline.
                    stream.write_all(b"HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").unwrap();
                }
                received
            });
            let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
            let endpoint = format!("http://{}", listener.local_addr().unwrap());
            let (directory, mut command) = fixture(&endpoint, None);
            let tenant = command.tenant_id.clone();
            let ca = directory.path().join("ca.pem");
            write_private(&ca, certificate.cert.pem().as_bytes());
            command.ca_file = (!missing_ca).then_some(ca);
            execute(command, directory.path()).unwrap();

            let artifact = id(ResourceKind::Artifact);
            let etag = format!("\"{artifact}-1\"");
            let prepared = serde_json::json!({
                "schema_version":1,"artifact_id":artifact,"operation_id":id(ResourceKind::Job),
                "upload_grant_id":id(ResourceKind::ArtifactGrant),"artifact_etag":etag,
                "upload_target":{"url":format!("https://{}:{object_port}/object?signature=fixture",if wrong_san {"127.0.0.1"} else {"localhost"}),
                    "completion_proof":"proof_123.safe"},
                "upload_expires_at":UtcTimestamp::from_datetime(chrono::Utc::now()+chrono::Duration::minutes(5)),
            });
            let api_worker = thread::spawn(move || {
                let mut stream = accept(&listener);
                let (head, body) = request(&mut stream).unwrap();
                assert!(head.starts_with("POST /v1/artifacts:prepare-upload HTTP/1.1\r\n"));
                let trace = head
                    .lines()
                    .find(|line| line.to_ascii_lowercase().starts_with("traceparent:"))
                    .unwrap()
                    .split('-')
                    .nth(1)
                    .unwrap();
                let body_text = serde_json::to_string(&prepared).unwrap();
                write!(stream,"HTTP/1.1 201 Created\r\nContent-Type: application/json\r\nCache-Control: no-store, private, max-age=0\r\nTrace-Id: {trace}\r\nETag: {etag}\r\nLocation: /v1/artifacts/{artifact}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body_text}",body_text.len()).unwrap();
                serde_json::from_slice::<serde_json::Value>(&body).unwrap()
            });
            let expected_bytes = if publication {
                let corpus = crate::workspace_assets::workspace_path(
                    "contracts/product-experience/agent-compiler/v2",
                );
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
                let corpus: serde_json::Value =
                    serde_json::from_slice(&fs::read(corpus.join("corpus.json")).unwrap()).unwrap();
                let compiled = crate::agent::compile_project(
                    directory.path(),
                    crate::agent::capture_project_sources(
                        directory.path(),
                        Path::new("agent.json"),
                    )
                    .unwrap(),
                    serde_json::from_value(corpus["profile"].clone()).unwrap(),
                )
                .unwrap();
                let client = crate::local_runtime_http_client(directory.path())
                    .unwrap()
                    .0;
                assert!(crate::agent::publish_agent(
                    directory.path(),
                    &client,
                    &client,
                    &tenant,
                    &compiled,
                    Duration::from_secs(3)
                )
                .is_err());
                compiled.source_bundle_bytes
            } else {
                let bytes = b"explicit connection CA upload".to_vec();
                let source = directory.path().join("upload.txt");
                fs::write(&source, &bytes).unwrap();
                assert!(crate::upload_local_artifact(
                    directory.path(),
                    &source,
                    "run_input",
                    "internal",
                    Some("text/plain".into()),
                    None,
                    3
                )
                .is_err());
                bytes
            };
            let prepared_request = api_worker.join().unwrap();
            assert_eq!(
                prepared_request["expected_size_bytes"],
                expected_bytes.len()
            );
            let actual = object_worker.join().unwrap();
            if actual.is_some() != (!missing_ca && !wrong_san) {
                failures.push(format!(
                    "publication={publication},missing_ca={missing_ca},wrong_san={wrong_san}"
                ));
            }
            if let Some((head, body)) = actual {
                assert!(head.starts_with("PUT /object?signature=fixture HTTP/1.1\r\n"));
                assert!(!head.to_ascii_lowercase().contains("authorization:"));
                assert!(!head.to_ascii_lowercase().contains("traceparent:"));
                assert_eq!(body, expected_bytes);
            }
        }
    }
    assert!(
        failures.is_empty(),
        "explicit CA propagation failed: {failures:?}"
    );
}
