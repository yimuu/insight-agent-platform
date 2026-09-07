//! Public recovery commands. A local intent journal contains metadata only;
//! the management API and PostgreSQL Receipt remain the outcome authority.
use super::{local_public_http_client, render_json, CliCommand, CliError, PROJECT_DIRECTORY};
use insight_platform_api::recovery::{
    parse_recovery_request, RecoveryResultV1, MAX_RECOVERY_REQUEST_BYTES,
};
use insight_platform_contracts::canonical_digest;
use std::{
    ffi::OsString,
    fs,
    io::Read,
    path::{Path, PathBuf},
};

pub(crate) fn parse(arguments: &[OsString]) -> Result<CliCommand, CliError> {
    let action = arguments
        .first()
        .and_then(|value| value.to_str())
        .ok_or(CliError::Usage)?;
    let action = match action {
        "hold-place" => "run-history-holds:place",
        "hold-release" => "run-history-holds:release",
        "pkce-recover" => "mcp-pkce-cleanup:recover",
        _ => return Err(CliError::Usage),
    };
    let mut root = None;
    let mut file = None;
    let mut index = 1;
    while index < arguments.len() {
        let flag = arguments[index].to_str().ok_or(CliError::Usage)?;
        let target = match flag {
            "--path" => &mut root,
            "--file" => &mut file,
            _ => return Err(CliError::UnsupportedOption(flag.to_owned())),
        };
        if target.is_some() {
            return Err(CliError::Usage);
        }
        *target = Some(PathBuf::from(
            arguments.get(index + 1).ok_or(CliError::Usage)?,
        ));
        index += 2;
    }
    Ok(CliCommand::Recovery {
        root: root.unwrap_or_else(|| PathBuf::from(".")),
        file: file.ok_or(CliError::MissingValue("--file"))?,
        action: action.to_owned(),
    })
}
pub(crate) fn execute(root: &Path, file: &Path, action: &str) -> Result<String, CliError> {
    let metadata = fs::metadata(file)
        .map_err(|_| CliError::RuntimeState("cannot read recovery request file".into()))?;
    if !metadata.is_file()
        || metadata.len() == 0
        || metadata.len() > MAX_RECOVERY_REQUEST_BYTES as u64
    {
        return Err(CliError::RuntimeState(
            "recovery request exceeds its bounded file contract".into(),
        ));
    }
    let mut bytes = Vec::new();
    fs::File::open(file)
        .and_then(|file| {
            file.take(MAX_RECOVERY_REQUEST_BYTES as u64 + 1)
                .read_to_end(&mut bytes)
        })
        .map_err(|_| CliError::RuntimeState("cannot read recovery request file".into()))?;
    parse_recovery_request(action, &bytes)
        .map_err(|_| CliError::RuntimeState("invalid recovery request".into()))?;
    let (client, tenant_id) = local_public_http_client(root)?;
    execute_with_client(root, &client, &tenant_id, action, &bytes)
}
fn execute_with_client(
    root: &Path,
    client: &super::public_client::PublicHttpClient,
    tenant_id: &insight_platform_contracts::ResourceId,
    action: &str,
    bytes: &[u8],
) -> Result<String, CliError> {
    let request = parse_recovery_request(action, bytes)
        .map_err(|_| CliError::RuntimeState("invalid recovery request".into()))?;
    let body: serde_json::Value = serde_json::from_slice(bytes).map_err(|_| CliError::Usage)?;
    let identity = serde_json::json!({"schema_version":1,"tenant_id":tenant_id,"operation":request.operation(),"request":body});
    let request_digest = canonical_digest(&identity).map_err(|_| CliError::Usage)?;
    let key = format!(
        "recovery-{}",
        request_digest
            .strip_prefix("sha256:")
            .ok_or(CliError::Usage)?
    );
    let journal=serde_json::to_vec(&serde_json::json!({"schema_version":1,"tenant_id":tenant_id,"operation":request.operation(),"target":request.target(),"request_digest":request_digest,"idempotency_key":key})).map_err(|_|CliError::Usage)?;
    let path = root
        .join(PROJECT_DIRECTORY)
        .join("recovery-intents")
        .join(format!("{key}.json"));
    super::run_journal::save_bytes(&path, &journal)
        .map_err(|error| CliError::RuntimeState(error.to_string()))?;
    let response = client
        .post_json::<_, RecoveryResultV1>(
            &format!("/v1/recovery/{action}"),
            &body,
            reqwest::StatusCode::OK,
            &key,
            None,
        )
        .map_err(CliError::PublicClient)?;
    response
        .body
        .validate_for(&request, chrono::Utc::now())
        .map_err(|_| {
            CliError::RuntimeState("recovery response does not match the command".into())
        })?;
    let result = serde_json::to_value(&response.body).map_err(|_| CliError::Usage)?;
    if response.etag
        != format!(
            "\"{}\"",
            canonical_digest(&result).map_err(|_| CliError::Usage)?
        )
    {
        return Err(CliError::RuntimeState(
            "recovery response ETag does not match its projection".into(),
        ));
    }
    render_json(&response.body)
}

#[cfg(test)]
mod tests {
    use super::*;
    use insight_platform_contracts::{ResourceId, ResourceKind};
    use std::{io::Write, net::TcpListener, time::Duration};
    fn id(kind: ResourceKind) -> ResourceId {
        ResourceId::from_uuid_v7(kind, uuid::Uuid::now_v7()).unwrap()
    }

    #[test]
    fn retry_reuses_intent_but_reads_current_authority_and_rejects_bad_etag() {
        let tenant = id(ResourceKind::Tenant);
        let task = id(ResourceKind::Interaction);
        let request = serde_json::json!({"schema_version":1,"task_id":task,"expected_task_version":3,"expected_task_generation":1,"previous_job_id":id(ResourceKind::Job),"attempt_limit":2,"recovery_evidence_digest":format!("sha256:{}","e".repeat(64))});
        let request_bytes = serde_json::to_vec(&request).unwrap();
        let result = serde_json::json!({"kind":"pkce_cleanup","schema_version":1,"task_id":task,"cleanup_job_id":id(ResourceKind::Job)});
        let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let client = super::super::public_client::PublicHttpClient::new(
            format!("http://{}", listener.local_addr().unwrap()),
            "fixture-token".into(),
            Duration::from_secs(3),
        )
        .unwrap();
        let server = std::thread::spawn(move || {
            let mut keys = Vec::new();
            for step in 0..3 {
                let (mut socket, _) = listener.accept().unwrap();
                socket
                    .set_read_timeout(Some(Duration::from_secs(5)))
                    .unwrap();
                let mut bytes = Vec::new();
                let mut byte = [0u8];
                while !bytes.ends_with(b"\r\n\r\n") {
                    socket.read_exact(&mut byte).unwrap();
                    bytes.push(byte[0]);
                    assert!(bytes.len() < 8192);
                }
                let head = String::from_utf8(bytes).unwrap();
                assert!(head.starts_with("POST /v1/recovery/mcp-pkce-cleanup:recover HTTP/1.1"));
                let header = |name: &str| {
                    head.lines()
                        .find_map(|line| {
                            line.split_once(':')
                                .filter(|(key, _)| key.eq_ignore_ascii_case(name))
                                .map(|(_, value)| value.trim().to_owned())
                        })
                        .unwrap()
                };
                assert_eq!(header("authorization"), "Bearer fixture-token");
                let trace = header("traceparent").split('-').nth(1).unwrap().to_owned();
                keys.push(header("idempotency-key"));
                let mut body = vec![0; header("content-length").parse().unwrap()];
                socket.read_exact(&mut body).unwrap();
                assert_eq!(
                    serde_json::from_slice::<serde_json::Value>(&body).unwrap(),
                    request
                );
                let response = serde_json::to_vec(&result).unwrap();
                let etag = if step == 2 {
                    format!("sha256:{}", "b".repeat(64))
                } else {
                    canonical_digest(&result).unwrap()
                };
                write!(socket,"HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\ncache-control: no-store, private, max-age=0\r\netag: \"{}\"\r\ntrace-id: {}\r\nconnection: close\r\n\r\n",response.len(),etag,trace).unwrap();
                socket.write_all(&response).unwrap();
            }
            assert!(keys.windows(2).all(|pair| pair[0] == pair[1]));
        });
        let directory = tempfile::TempDir::new().unwrap();
        let first = execute_with_client(
            directory.path(),
            &client,
            &tenant,
            "mcp-pkce-cleanup:recover",
            &request_bytes,
        )
        .unwrap();
        let retry = execute_with_client(
            directory.path(),
            &client,
            &tenant,
            "mcp-pkce-cleanup:recover",
            &request_bytes,
        )
        .unwrap();
        assert_eq!(first, retry);
        assert!(execute_with_client(
            directory.path(),
            &client,
            &tenant,
            "mcp-pkce-cleanup:recover",
            &request_bytes
        )
        .is_err());
        server.join().unwrap();
        let journals = fs::read_dir(
            directory
                .path()
                .join(PROJECT_DIRECTORY)
                .join("recovery-intents"),
        )
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect::<Vec<_>>();
        assert_eq!(journals.len(), 1);
        let journal = fs::read_to_string(&journals[0]).unwrap();
        assert!(!journal.contains("fixture-token"));
        assert!(!journal.contains("recovery_evidence_digest"));
        assert!(!journal.contains("cleanup_job_id"));
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                fs::metadata(&journals[0]).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }
    }
}
