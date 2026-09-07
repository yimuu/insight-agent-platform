//! Ordinary Run signal command. The public Receipt remains the outcome authority.
use crate::{public_client::PublicHttpClient, CliCommand, CliError};
use insight_platform_api::run::{valid_signal_key, SignalRunRequestV1, MAX_RUN_REQUEST_BYTES};
use insight_platform_contracts::{
    canonical_digest, parse_strict_json, JsonLimits, ResourceId, ResourceKind, ValueRef,
};
use std::{
    ffi::OsString,
    fs,
    io::Read,
    path::{Path, PathBuf},
};

pub(crate) fn parse(arguments: &[OsString]) -> Result<CliCommand, CliError> {
    let run_id = arguments
        .first()
        .and_then(|value| value.to_str())
        .and_then(|value| ResourceId::parse_expected(value, ResourceKind::Run).ok())
        .ok_or(CliError::Usage)?;
    let key = arguments
        .get(1)
        .and_then(|value| value.to_str())
        .filter(|value| valid_signal_key(value))
        .ok_or(CliError::Usage)?
        .to_owned();
    let mut root = None;
    let mut file = None;
    let mut index = 2;
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
    Ok(CliCommand::AgentSignal {
        root: root.unwrap_or_else(|| PathBuf::from(".")),
        run_id,
        key,
        file,
    })
}
pub(crate) fn execute(
    root: &Path,
    run_id: &ResourceId,
    key: &str,
    file: Option<&Path>,
) -> Result<String, CliError> {
    let bytes = if let Some(file) = file {
        let mut bytes = Vec::new();
        fs::File::open(file)
            .and_then(|file| {
                file.take(MAX_RUN_REQUEST_BYTES as u64 + 1)
                    .read_to_end(&mut bytes)
            })
            .map_err(|_| CliError::RuntimeState("cannot read bounded signal request".into()))?;
        bytes
    } else {
        b"{\"payload\":null}".to_vec()
    };
    let request = parse_request(&bytes)?;
    let (client, tenant) = crate::local_runtime_http_client(root)?;
    send(&client, &tenant, run_id, key, &request)?;
    crate::render_json(
        &serde_json::json!({"schema_version":1,"run_id":run_id,"signal_key":key,"accepted":true}),
    )
}
fn parse_request(bytes: &[u8]) -> Result<SignalRunRequestV1, CliError> {
    let limits = JsonLimits {
        max_bytes: MAX_RUN_REQUEST_BYTES,
        max_depth: 32,
        max_properties_per_object: 1024,
        max_items_per_array: 4096,
        max_string_bytes: 65536,
    };
    let invalid = || {
        CliError::RuntimeState("signal request violates the bounded typed public contract".into())
    };
    let request: SignalRunRequestV1 =
        serde_json::from_value(parse_strict_json(bytes, limits).map_err(|_| invalid())?)
            .map_err(|_| invalid())?;
    if let Some(payload) = &request.payload {
        payload.value.validate(limits).map_err(|_| invalid())?;
        if let ValueRef::Artifact { artifact } = &payload.value {
            if artifact.classification() != payload.classification {
                return Err(invalid());
            }
        }
    }
    Ok(request)
}
fn send(
    client: &PublicHttpClient,
    tenant: &ResourceId,
    run_id: &ResourceId,
    key: &str,
    request: &SignalRunRequestV1,
) -> Result<(), CliError> {
    if tenant.kind() != ResourceKind::Tenant
        || run_id.kind() != ResourceKind::Run
        || !valid_signal_key(key)
    {
        return Err(CliError::Usage);
    }
    let digest=canonical_digest(&serde_json::json!({"schema_version":1,"tenant_id":tenant,"run_id":run_id,"signal_key":key,"request":request})).map_err(|_|CliError::Usage)?;
    let receipt = format!(
        "insight-signal-v1-{}",
        digest.strip_prefix("sha256:").ok_or(CliError::Usage)?
    );
    // No local response cache: even a repeated accepted intent rechecks current server authorization.
    client
        .post_json_no_content(
            &format!("/v1/runs/{run_id}/signals/{key}"),
            request,
            &receipt,
        )
        .map_err(CliError::PublicClient)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{io::Write, net::TcpListener, time::Duration};
    fn id(kind: ResourceKind) -> ResourceId {
        ResourceId::from_uuid_v7(kind, uuid::Uuid::now_v7()).unwrap()
    }
    #[test]
    fn normal_cli_signal_parses_closed_request_before_contacting_server() {
        let run = id(ResourceKind::Run);
        let args = [
            "agent".into(),
            "signal".into(),
            run.to_string().into(),
            "approval_received".into(),
            "--file".into(),
            "signal.json".into(),
        ];
        assert!(
            matches!(crate::parse_command(&args).unwrap(),CliCommand::AgentSignal{run_id,key,file:Some(_),..}if run_id==run&&key=="approval_received")
        );
        assert!(parse_request(br#"{"payload":null}"#).is_ok());
        for bytes in [br#"{"payload":null,"payload":null}"#.as_slice(),br#"{"payload":null,"admin":true}"#,br#"{"payload":{"classification":"public","schema_digest":"bad","value":{"storage":"inline","value":1}}}"#] {assert!(parse_request(bytes).is_err());}
        assert!(parse_request(&vec![b' '; MAX_RUN_REQUEST_BYTES + 1]).is_err());
        let bad = [run.to_string().into(), "../../commands".into()];
        assert!(parse(&bad).is_err());
    }
    #[test]
    fn lost_signal_response_reuses_receipt_and_current_denial_still_fails() {
        use insight_platform_contracts::{ApiProblem, ApiProblemCode};
        let tenant = id(ResourceKind::Tenant);
        let run = id(ResourceKind::Run);
        let server_run = run.clone();
        let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let client = PublicHttpClient::new(
            format!("http://{}", listener.local_addr().unwrap()),
            "fixture-token".into(),
            Duration::from_secs(3),
        )
        .unwrap();
        let server = std::thread::spawn(move || {
            let mut receipts = Vec::new();
            for step in 0..3 {
                let (mut socket, _) = listener.accept().unwrap();
                socket
                    .set_read_timeout(Some(Duration::from_secs(5)))
                    .unwrap();
                let mut head = Vec::new();
                let mut byte = [0u8];
                while !head.ends_with(b"\r\n\r\n") {
                    socket.read_exact(&mut byte).unwrap();
                    head.push(byte[0]);
                    assert!(head.len() < 8192)
                }
                let head = String::from_utf8(head).unwrap();
                assert!(head.starts_with(&format!(
                    "POST /v1/runs/{server_run}/signals/ready HTTP/1.1"
                )));
                let header = |key: &str| {
                    head.lines()
                        .find_map(|line| {
                            line.split_once(':')
                                .filter(|(name, _)| name.eq_ignore_ascii_case(key))
                                .map(|(_, value)| value.trim().to_owned())
                        })
                        .unwrap()
                };
                assert_eq!(header("authorization"), "Bearer fixture-token");
                receipts.push(header("idempotency-key"));
                let trace = header("traceparent").split('-').nth(1).unwrap().to_owned();
                let mut body = vec![0; header("content-length").parse().unwrap()];
                socket.read_exact(&mut body).unwrap();
                assert_eq!(
                    serde_json::from_slice::<serde_json::Value>(&body).unwrap(),
                    serde_json::json!({"payload":null})
                );
                if step == 0 {
                    continue;
                }
                if step == 1 {
                    write!(socket,"HTTP/1.1 204 No Content\r\ncache-control: no-store, private, max-age=0\r\ntrace-id: {trace}\r\nconnection: close\r\n\r\n").unwrap();
                } else {
                    let problem = ApiProblem {
                        type_uri: "urn:insight:problem:permission_denied".into(),
                        title: "Permission denied".into(),
                        status: 403,
                        code: ApiProblemCode::PermissionDenied,
                        detail: None,
                        request_id: id(ResourceKind::ServerRequest),
                        trace_id: trace.parse().unwrap(),
                        retryable: false,
                        retry_after_ms: None,
                        field_errors: vec![],
                    };
                    let body = serde_json::to_vec(&problem).unwrap();
                    write!(socket,"HTTP/1.1 403 Forbidden\r\ncontent-type: application/json\r\ncache-control: no-store, private, max-age=0\r\ntrace-id: {trace}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",body.len()).unwrap();
                    socket.write_all(&body).unwrap();
                }
            }
            assert!(receipts.windows(2).all(|pair| pair[0] == pair[1]));
        });
        let request = SignalRunRequestV1 { payload: None };
        assert!(send(&client, &tenant, &run, "ready", &request).is_err());
        send(&client, &tenant, &run, "ready", &request).unwrap();
        assert!(
            matches!(send(&client,&tenant,&run,"ready",&request),Err(CliError::PublicClient(crate::public_client::PublicClientError::Problem(problem)))if problem.status==403)
        );
        server.join().unwrap();
    }
}
