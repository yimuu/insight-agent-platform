//! Explicit live protocol qualification, without Context/Policy IDs or synthetic authority.
use super::wire::{decode_wire, encode_wire, RemoteSearchWireResponse};
use crate::{
    capability_http::capability_url, is_public_destination_ip, parse_endpoint_host,
    EgressDnsResolver, ParsedEndpointHost, TokioEgressDnsResolver,
};
use futures::StreamExt;
use insight_platform_contracts::{
    canonical_digest, canonical_json, parse_strict_json, DataClassification,
    InstalledRemoteContextDestinationV1, JsonLimits, Sha256Digest,
};
use serde_json::{json, Value};
use std::{
    fs,
    io::{Read, Write},
    net::SocketAddr,
    path::{Path, PathBuf},
    time::Duration,
};

fn digest(bytes: &[u8]) -> String {
    format!(
        "sha256:{}",
        ring::digest::digest(&ring::digest::SHA256, bytes)
            .as_ref()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
    )
}
fn read(path: &Path, maximum: usize) -> Result<Vec<u8>, &'static str> {
    let metadata = fs::symlink_metadata(path).map_err(|_| "input_unavailable")?;
    if !metadata.is_file() || metadata.len() > maximum as u64 {
        return Err("input_invalid");
    }
    let mut options = fs::OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
    }
    let file = options.open(path).map_err(|_| "input_unavailable")?;
    let opened = file.metadata().map_err(|_| "input_unavailable")?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if opened.dev() != metadata.dev() || opened.ino() != metadata.ino() {
            return Err("input_changed");
        }
    }
    if !opened.is_file() || opened.len() != metadata.len() {
        return Err("input_changed");
    }
    let mut bytes = Vec::new();
    (&file)
        .take(maximum as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| "input_unavailable")?;
    if bytes.len() > maximum
        || file
            .metadata()
            .map_err(|_| "input_unavailable")?
            .modified()
            .ok()
            != opened.modified().ok()
    {
        return Err("input_changed");
    }
    Ok(bytes)
}
struct Attempt {
    progress: fs::File,
}
impl Attempt {
    fn begin(
        output: &Path,
        destination: &InstalledRemoteContextDestinationV1,
    ) -> Result<Self, &'static str> {
        let mut attempt = output.as_os_str().to_os_string();
        attempt.push(".attempt.json");
        let mut progress = output.as_os_str().to_os_string();
        progress.push(".progress.jsonl");
        for path in [output, Path::new(&attempt), Path::new(&progress)] {
            if fs::symlink_metadata(path).is_ok() {
                return Err("attempt_already_exists");
            }
        }
        let initial = json!({"schema_version":1,"phase":"prepared","scope":"remote_search_wire_tls_corpus","endpoint_identity_digest":destination.endpoint_identity_digest,"destination_digest":canonical_digest(&serde_json::to_value(destination).map_err(|_|"input_invalid")?).map_err(|_|"input_invalid")?});
        let mut file = new_file(Path::new(&attempt))?;
        file.write_all(&canonical_json(&initial).map_err(|_| "report_invalid")?)
            .map_err(|_| "report_io")?;
        file.sync_all().map_err(|_| "report_io")?;
        let progress = new_file(Path::new(&progress))?;
        progress.sync_all().map_err(|_| "report_io")?;
        fs::File::open(output.parent().ok_or("report_path")?)
            .map_err(|_| "report_io")?
            .sync_all()
            .map_err(|_| "report_io")?;
        Ok(Self { progress })
    }
    fn record(&mut self, value: &Value) -> Result<(), &'static str> {
        let mut bytes = canonical_json(value).map_err(|_| "report_invalid")?;
        if bytes.len() > 4096 {
            return Err("report_invalid");
        }
        bytes.push(b'\n');
        self.progress.write_all(&bytes).map_err(|_| "report_io")?;
        self.progress.sync_all().map_err(|_| "report_io")
    }
}
fn new_file(path: &Path) -> Result<fs::File, &'static str> {
    let mut options = fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
    }
    options
        .open(path)
        .map_err(|_| "report_path_exists_or_unavailable")
}
fn json_bytes(bytes: &[u8], maximum: usize) -> Result<Value, &'static str> {
    parse_strict_json(
        bytes,
        JsonLimits {
            max_bytes: maximum,
            max_depth: 16,
            max_items_per_array: 64,
            max_properties_per_object: 32,
            max_string_bytes: 16_384,
        },
    )
    .map_err(|_| "input_invalid")
}
fn corpus() -> Result<(PathBuf, Value, Sha256Digest), &'static str> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .find(|root| root.join("contracts/platform-v1/manifest.json").is_file())
        .ok_or("workspace_unavailable")?;
    let directory = root.join("examples/productization/document-review/corpus");
    let value = json_bytes(&read(&directory.join("manifest.json"), 8192)?, 8192)?;
    let sha = canonical_digest(&value)
        .map_err(|_| "corpus_invalid")?
        .parse()
        .map_err(|_| "corpus_invalid")?;
    Ok((directory, value, sha))
}
fn verify_corpus(
    response: &RemoteSearchWireResponse,
    expected: usize,
    directory: &Path,
    manifest: &Value,
    manifest_digest: &Sha256Digest,
) -> Result<(), &'static str> {
    if response.items.len() != expected
        || response.next_cursor_digest.is_some()
        || response.remote_revision_digest.as_ref() != Some(manifest_digest)
    {
        return Err("corpus_mismatch");
    }
    for item in &response.items {
        let fields = item
            .structured_fields
            .as_object()
            .filter(|fields| fields.len() == 5)
            .ok_or("corpus_mismatch")?;
        let uri = fields
            .get("source_uri")
            .and_then(Value::as_str)
            .ok_or("corpus_mismatch")?;
        let document = manifest["documents"]
            .as_array()
            .ok_or("corpus_invalid")?
            .iter()
            .find(|document| document["source_uri"] == uri)
            .ok_or("corpus_mismatch")?;
        let path = document["path"]
            .as_str()
            .filter(|path| {
                matches!(
                    *path,
                    "docs/current/architecture.md" | "docs/current/agent-authoring.md"
                )
            })
            .ok_or("corpus_invalid")?;
        let source = read(
            &directory.join(Path::new(path).file_name().ok_or("corpus_invalid")?),
            65_536,
        )?;
        if document["content_digest"] != digest(&source)
            || document["utf8_bytes"] != source.len()
            || fields.get("raw_content_digest") != Some(&document["content_digest"])
            || fields.get("source_revision") != Some(&document["source_revision"])
        {
            return Err("corpus_mismatch");
        }
        let source = String::from_utf8(source).map_err(|_| "corpus_invalid")?;
        let start = fields
            .get("start_line")
            .and_then(Value::as_u64)
            .ok_or("corpus_mismatch")? as usize;
        let end = fields
            .get("end_line")
            .and_then(Value::as_u64)
            .ok_or("corpus_mismatch")? as usize;
        if start == 0 || end < start || end > source.lines().count() {
            return Err("corpus_mismatch");
        }
        let excerpt = source
            .split_inclusive('\n')
            .skip(start - 1)
            .take(end - start + 1)
            .collect::<String>();
        let locator = format!("{uri}#L{start}-L{end}");
        if item.content != excerpt
            || item.source_identity != locator
            || item.locator != locator
            || item.classification != DataClassification::Public
        {
            return Err("corpus_mismatch");
        }
    }
    Ok(())
}
async fn run(
    destination: InstalledRemoteContextDestinationV1,
    attempt: &mut Attempt,
    deadline: tokio::time::Instant,
) -> Result<Value, &'static str> {
    if !destination.validate_shape()
        || !destination.credential_injections.is_empty()
        || destination.endpoint.base_path != "/v1/query"
    {
        return Err("destination_invalid");
    }
    let (host, mut addresses) =
        match parse_endpoint_host(&destination.endpoint.host).map_err(|_| "destination_invalid")? {
            ParsedEndpointHost::Address(address) => (
                address.to_string(),
                vec![SocketAddr::new(address, destination.endpoint.port)],
            ),
            ParsedEndpointHost::Name(host) => {
                let addresses = TokioEgressDnsResolver
                    .resolve(&host, destination.endpoint.port)
                    .await
                    .map_err(|_| "dns_rejected")?;
                (host, addresses)
            }
        };
    addresses.sort_unstable();
    addresses.dedup();
    if addresses.is_empty()
        || addresses.len() > 16
        || addresses.iter().any(|address| {
            address.port() != destination.endpoint.port || !is_public_destination_ip(address.ip())
        })
    {
        return Err("destination_denied");
    }
    let root = reqwest::Certificate::from_pem(destination.trusted_root_pem.as_bytes())
        .map_err(|_| "trust_invalid")?;
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .referer(false)
        .no_proxy()
        .https_only(true)
        .tls_certs_only([root])
        .connect_timeout(Duration::from_secs(5))
        .timeout(Duration::from_secs(10))
        .pool_max_idle_per_host(0)
        .resolve_to_addrs(&host, &addresses)
        .build()
        .map_err(|_| "transport_invalid")?;
    let url = capability_url(&destination.endpoint).map_err(|_| "destination_invalid")?;
    let (directory, manifest, manifest_digest) = corpus()?;
    let mut cases = Vec::new();
    for (name, question, expected, negative) in [
        ("source_match", "持久状态 PostgreSQL", 1, None),
        ("escaped_query", "人工确认 \"PostgreSQL\" \\ 原文", 1, None),
        ("empty_match", "zzzzunmatchedzzzz", 0, None),
        ("unknown_field", "PostgreSQL", 0, Some("unknown")),
        (
            "unsupported_projection",
            "PostgreSQL",
            0,
            Some("projection"),
        ),
        ("duplicate_field", "PostgreSQL", 0, Some("duplicate")),
    ] {
        let query = json!({"question":question});
        let query_digest = canonical_digest(&query)
            .map_err(|_| "request_invalid")?
            .parse()
            .map_err(|_| "request_invalid")?;
        let filter = canonical_digest(&json!({"schema_version":1,"filter":null}))
            .map_err(|_| "request_invalid")?
            .parse()
            .map_err(|_| "request_invalid")?;
        let mut body = encode_wire(
            &query,
            &query_digest,
            &filter,
            &[],
            1,
            &None,
            destination.maximum_request_bytes,
        )
        .map_err(|_| "request_invalid")?;
        if let Some(negative) = negative {
            let mut value: Value = serde_json::from_slice(&body).map_err(|_| "request_invalid")?;
            match negative {
                "unknown" => value["unknown"] = json!(true),
                "projection" => value["requested_projection"] = json!(["not_installed"]),
                "duplicate" => {}
                _ => return Err("request_invalid"),
            }
            body = canonical_json(&value).map_err(|_| "request_invalid")?;
            if negative == "duplicate" {
                body = String::from_utf8(body)
                    .map_err(|_| "request_invalid")?
                    .replacen(
                        "\"schema_version\":1",
                        "\"schema_version\":1,\"schema_version\":1",
                        1,
                    )
                    .into_bytes();
            }
        }
        if body.len() > destination.maximum_request_bytes as usize {
            return Err("request_invalid");
        }
        let request_digest = digest(&body);
        attempt.record(
            &json!({"phase":"maybe_dispatched","case":name,"request_digest":request_digest}),
        )?;
        if tokio::time::Instant::now() >= deadline {
            return Err("deadline_exceeded");
        }
        let response = client
            .post(url.clone())
            .header("content-type", "application/json")
            .header("accept", "application/json")
            .body(body)
            .send()
            .await
            .map_err(|_| "transport_unavailable")?;
        let status = response.status();
        let maximum = destination.maximum_response_bytes.min(65_536) as usize;
        if status.as_u16() != if negative.is_some() { 400 } else { 200 }
            || response.headers().get_all("content-length").iter().count() > 1
            || response
                .headers()
                .get("content-type")
                .is_none_or(|value| value.as_bytes() != b"application/json")
            || response
                .headers()
                .get("content-encoding")
                .is_some_and(|value| value.as_bytes() != b"identity")
            || response
                .content_length()
                .is_some_and(|length| length > maximum as u64)
        {
            return Err("http_response_rejected");
        }
        let mut bytes = Vec::new();
        let mut stream = response.bytes_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|_| "transport_unavailable")?;
            if bytes.len().saturating_add(chunk.len()) > maximum {
                return Err("response_too_large");
            }
            bytes.extend_from_slice(&chunk);
        }
        let response_digest = if negative.is_none() {
            let wire = decode_wire(&bytes, 1, DataClassification::Public, maximum as u32)
                .map_err(|_| "wire_rejected")?;
            verify_corpus(
                &wire.response,
                expected,
                &directory,
                &manifest,
                &manifest_digest,
            )?;
            wire.canonical_response_digest.to_string()
        } else {
            let value = json_bytes(&bytes, maximum)?;
            if value != json!({"error":"invalid_request"}) {
                return Err("negative_response_rejected");
            }
            canonical_digest(&value).map_err(|_| "wire_rejected")?
        };
        attempt.record(&json!({"phase":"case_completed","case":name,"http_status":status.as_u16(),"request_digest":request_digest,"response_digest":response_digest}))?;
        cases.push(json!({"case":name,"http_status":status.as_u16(),"request_digest":request_digest,"response_digest":response_digest,"passed":true}));
    }
    Ok(
        json!({"schema_version":1,"kind":"insight.document-review.remote-search-qualification/v1","scope":"remote_search_wire_tls_corpus","observed_at":chrono::Utc::now().to_rfc3339(),"endpoint_identity_digest":destination.endpoint_identity_digest,"region":destination.region,"protocol_contract_digest":destination.protocol_contract_digest,"result_mapping_digest":destination.result_mapping_digest,"public_root_pem_file_digest":digest(destination.trusted_root_pem.as_bytes()),"corpus_manifest_digest":manifest_digest,"cases":cases,"business_authorization_qualified":false,"human_response_observed":false}),
    )
}
#[tokio::test]
#[ignore = "requires an explicitly deployed public HTTPS document-review endpoint and new private report path"]
async fn live_document_provider_protocol_conformance() {
    let input = std::env::var_os("PLATFORM_DOCUMENT_REVIEW_DESTINATION_FILE")
        .expect("explicit destination file is required");
    let output = std::env::var_os("PLATFORM_DOCUMENT_REVIEW_REPORT_FILE")
        .expect("new private report file is required");
    let destination = serde_json::from_value(
        json_bytes(
            &read(Path::new(&input), 65_536).expect("bounded destination"),
            65_536,
        )
        .expect("strict destination"),
    )
    .expect("typed destination");
    let output = Path::new(&output);
    let parent = output.parent().expect("report parent");
    let metadata = fs::symlink_metadata(parent).expect("existing report directory");
    assert!(
        metadata.is_dir() && !output.exists(),
        "new report path required"
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            metadata.permissions().mode() & 0o077,
            0,
            "private report directory required"
        );
    }
    let mut attempt = Attempt::begin(output, &destination).expect("new attempt only");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    let outcome = tokio::time::timeout_at(deadline, run(destination, &mut attempt, deadline)).await;
    let result = match outcome {
        Ok(Ok(result)) => result,
        Ok(Err(code)) => {
            attempt
                .record(&json!({"phase":"failed","failure":code}))
                .expect("failure evidence fsync");
            panic!("protocol qualification failed: {code}");
        }
        Err(_) => {
            attempt
                .record(&json!({"phase":"failed","failure":"deadline_exceeded"}))
                .expect("timeout evidence fsync");
            panic!("protocol qualification deadline exceeded");
        }
    };
    attempt
        .record(&json!({"phase":"protocol_cases_passed"}))
        .expect("completion evidence fsync");
    let bytes = canonical_json(&result).expect("safe report");
    let mut options = fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(output).expect("new report only");
    file.write_all(&bytes).expect("report write");
    file.sync_all().expect("report fsync");
    fs::File::open(parent)
        .expect("parent directory")
        .sync_all()
        .expect("directory fsync");
}

#[test]
fn attempt_is_reserved_before_dispatch_and_failure_preserves_completed_case_evidence() {
    use std::time::{SystemTime, UNIX_EPOCH};
    let directory = std::env::temp_dir().join(format!(
        "insight-document-attempt-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let mut options = fs::DirBuilder::new();
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        options.mode(0o700);
    }
    options.create(&directory).unwrap();
    let output = directory.join("report.json");
    let endpoint = insight_platform_contracts::CanonicalHttpEndpoint {
        scheme: insight_platform_contracts::CapabilityEndpointScheme::Https,
        host: "documents.example.test".into(),
        port: 443,
        base_path: "/v1/query".into(),
    };
    let destination = InstalledRemoteContextDestinationV1 {
        schema_version: 1,
        protocol_contract_digest:
            insight_platform_contracts::remote_context_protocol_contract_digest(),
        result_mapping_digest: insight_platform_contracts::remote_context_result_mapping_digest(),
        endpoint_identity_digest: endpoint.canonical_digest().unwrap(),
        endpoint,
        region: "global".parse().unwrap(),
        credential_injections: vec![],
        trusted_root_pem: "public-file-fixture-only".into(),
        maximum_request_bytes: 8192,
        maximum_response_bytes: 65_536,
    };
    let mut attempt = Attempt::begin(&output, &destination).unwrap();
    attempt.record(&json!({"phase":"maybe_dispatched","case":"source_match","request_digest":digest(b"query")})).unwrap();
    attempt.record(&json!({"phase":"case_completed","case":"source_match","response_digest":digest(b"actual response")})).unwrap();
    attempt.record(&json!({"phase":"maybe_dispatched","case":"empty_match","request_digest":digest(b"next query")})).unwrap();
    attempt
        .record(&json!({"phase":"failed","failure":"deadline_exceeded"}))
        .unwrap();
    drop(attempt);
    assert!(
        Attempt::begin(&output, &destination).is_err(),
        "an unknown attempt is never reused"
    );
    assert!(
        !output.exists(),
        "failure cannot publish a passed conformance report"
    );
    let progress = read(&directory.join("report.json.progress.jsonl"), 8192).unwrap();
    let records: Vec<Value> = String::from_utf8(progress.clone())
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(records.len(), 4);
    assert_eq!(records[1]["phase"], "case_completed");
    assert_eq!(records[2]["phase"], "maybe_dispatched");
    assert_eq!(records[3]["failure"], "deadline_exceeded");
    assert_eq!(
        read(&directory.join("report.json.progress.jsonl"), 8192).unwrap(),
        progress
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::symlink;
        symlink(
            directory.join("report.json.attempt.json"),
            directory.join("public-link"),
        )
        .unwrap();
        assert!(read(&directory.join("public-link"), 8192).is_err());
    }
    fs::remove_dir_all(directory).unwrap();
}
