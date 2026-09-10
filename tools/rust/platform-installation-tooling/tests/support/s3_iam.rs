//! Independent physical IAM conformance, sharing only explicit SDK/TLS setup with the S3 fixture.
use super::*;
use std::{collections::BTreeMap, io::Write, os::unix::fs::OpenOptionsExt};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CredentialsInput {
    #[serde(rename = "accessKey")]
    access: String,
    #[serde(rename = "secretKey")]
    secret: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct IamInput {
    base: Input,
    credentials: BTreeMap<String, CredentialsInput>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct IamEvidence {
    empty_delete_status: u16,
    empty_delete_marker: bool,
    original_versions_retained: bool,
    denied_status: u16,
    first: String,
    second: String,
    marker: String,
    data: String,
    presigned: String,
}

fn denied<T, E>(result: std::result::Result<T, aws_sdk_s3::error::SdkError<E>>) -> Result<()> {
    if result.err().and_then(|error| {
        error
            .raw_response()
            .map(|response| response.status().as_u16())
    }) != Some(403)
    {
        return Err("iam_denial");
    }
    Ok(())
}

fn presigning() -> Result<PresigningConfig> {
    PresigningConfig::expires_in(Duration::from_secs(60)).map_err(|_| "iam_presign")
}

fn read_error<E>(
    object: &str,
    operation: &str,
    error: &aws_sdk_s3::error::SdkError<E>,
) -> &'static str {
    let class = match error
        .raw_response()
        .map(|response| response.status().as_u16())
    {
        Some(403) => "forbidden",
        Some(404) => "not_found",
        Some(500..=599) => "service",
        Some(_) => "other_status",
        None => match error {
            aws_sdk_s3::error::SdkError::TimeoutError(_) => "timeout",
            aws_sdk_s3::error::SdkError::DispatchFailure(_) => "dispatch",
            _ => "unknown",
        },
    };
    eprintln!("IAM_READ object={object} operation={operation} class={class}");
    "iam_restarted_read"
}

async fn restart_exact(
    client: &Client,
    input: &Input,
    key: &str,
    generation: &str,
    bytes: &[u8],
    object: &str,
) -> Result<()> {
    let head = client
        .head_object()
        .bucket(&input.bucket)
        .key(key)
        .version_id(generation)
        .send()
        .await
        .map_err(|error| read_error(object, "head", &error))?;
    if head.version_id() != Some(generation) || head.content_length() != Some(bytes.len() as i64) {
        eprintln!("IAM_READ object={object} operation=head class=evidence");
        return Err("iam_restarted_read");
    }
    let mut read = client
        .get_object()
        .bucket(&input.bucket)
        .key(key)
        .version_id(generation)
        .send()
        .await
        .map_err(|error| read_error(object, "get", &error))?;
    if read.version_id() != Some(generation) || read.content_length() != Some(bytes.len() as i64) {
        eprintln!("IAM_READ object={object} operation=get class=evidence");
        return Err("iam_restarted_read");
    }
    let mut actual = Vec::new();
    while let Some(chunk) = read.body.next().await {
        let chunk = chunk.map_err(|_| "iam_restarted_read")?;
        if chunk.len() > bytes.len().saturating_sub(actual.len()) {
            return Err("iam_restarted_read");
        }
        actual.extend_from_slice(&chunk);
    }
    if actual != bytes {
        eprintln!("IAM_READ object={object} operation=body class=evidence");
        return Err("iam_restarted_read");
    }
    Ok(())
}

async fn request(
    http: &reqwest::Client,
    signed: &aws_sdk_s3::presigning::PresignedRequest,
) -> Result<reqwest::Response> {
    let method =
        reqwest::Method::from_bytes(signed.method().as_bytes()).map_err(|_| "iam_presign")?;
    let mut request = http.request(method, signed.uri());
    for (name, value) in signed.headers() {
        request = request.header(name, value);
    }
    request.send().await.map_err(|_| "iam_http")
}

async fn clients(input: &IamInput) -> BTreeMap<&str, Client> {
    let mut clients = BTreeMap::new();
    for (role, credentials) in &input.credentials {
        let mut base = input.base.clone();
        base.access_key.clone_from(&credentials.access);
        base.secret_key.clone_from(&credentials.secret);
        clients.insert(role.as_str(), client(&base, false).await);
    }
    clients
}

async fn denied_matrix(
    clients: &BTreeMap<&str, Client>,
    input: &Input,
    generation: &str,
) -> Result<()> {
    for (role, client) in clients {
        denied(
            client
                .list_objects_v2()
                .bucket(&input.bucket)
                .max_keys(1)
                .send()
                .await,
        )?;
        denied(
            client
                .get_object()
                .bucket(format!("{}-foreign", input.bucket))
                .key("v1/object")
                .version_id(generation)
                .send()
                .await,
        )?;
        denied(
            client
                .put_object()
                .bucket(&input.bucket)
                .key("outside/closed-prefix")
                .body(ByteStream::from_static(FIRST))
                .send()
                .await,
        )?;
        denied(
            client
                .delete_object()
                .bucket(&input.bucket)
                .key("v1/object")
                .send()
                .await,
        )?;
        denied(
            client
                .put_bucket_policy()
                .bucket(&input.bucket)
                .policy("{\"Version\":\"2012-10-17\",\"Statement\":[]}")
                .send()
                .await,
        )?;
        if *role != "initializer" {
            denied(
                client
                    .put_bucket_versioning()
                    .bucket(&input.bucket)
                    .versioning_configuration(
                        VersioningConfiguration::builder()
                            .status(BucketVersioningStatus::Suspended)
                            .build(),
                    )
                    .send()
                    .await,
            )?;
        }
        if !matches!(*role, "artifact-gateway" | "artifact-data") {
            denied(
                client
                    .put_object()
                    .bucket(&input.bucket)
                    .key("v1/forbidden")
                    .body(ByteStream::from_static(FIRST))
                    .send()
                    .await,
            )?;
        }
        if *role != "artifact-maintenance" {
            denied(
                client
                    .delete_object()
                    .bucket(&input.bucket)
                    .key("v1/object")
                    .version_id(generation)
                    .send()
                    .await,
            )?;
            denied(
                client
                    .delete_object()
                    .bucket(&input.bucket)
                    .key("v1/object")
                    .version_id("")
                    .send()
                    .await,
            )?;
        }
        if matches!(*role, "artifact-gateway" | "artifact-data") {
            // Seaweed implicitly maps multipart permissions to PutObject. The fixed PUT method
            // condition must still reject POST initiation and DELETE abort, preserving verb classes.
            denied(
                client
                    .create_multipart_upload()
                    .bucket(&input.bucket)
                    .key("v1/multipart")
                    .send()
                    .await,
            )?;
            denied(
                client
                    .abort_multipart_upload()
                    .bucket(&input.bucket)
                    .key("v1/multipart")
                    .upload_id("qualification-missing")
                    .send()
                    .await,
            )?;
        }
    }
    Ok(())
}

pub(super) async fn run() -> Result<()> {
    let path = std::env::var("INSIGHT_S3_IAM_FIXTURE_INPUT").map_err(|_| "iam_input")?;
    let input: IamInput =
        serde_json::from_slice(&read_private(Path::new(&path))?).map_err(|_| "iam_input")?;
    let root = Path::new(&path).parent().ok_or("iam_input")?;
    let url = reqwest::Url::parse(&input.base.endpoint).map_err(|_| "iam_input")?;
    let roles = [
        "artifact-data",
        "artifact-gateway",
        "artifact-maintenance",
        "initializer",
        "qualification-reader",
    ];
    if input.base.schema_version != 1
        || input
            .credentials
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>()
            != roles
        || input.credentials.values().any(|credential| {
            credential.access.len() != 32
                || credential.secret.len() != 64
                || !credential
                    .access
                    .bytes()
                    .chain(credential.secret.bytes())
                    .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
        })
        || !input
            .base
            .bucket
            .strip_prefix("insight-platform-artifacts-")
            .is_some_and(|nonce| {
                nonce.len() == 32 && nonce.bytes().all(|byte| byte.is_ascii_hexdigit())
            })
        || url.scheme() != "https"
        || url.host_str() != Some("localhost.localstack.cloud")
        || !url.username().is_empty()
        || url.password().is_some()
        || url.port().is_none_or(|port| port < 1024)
        || url.path() != "/"
        || url.query().is_some()
        || url.fragment().is_some()
        || Path::new(&input.base.ca_file) != root.join("tls/ca.pem")
        || Path::new(&input.base.evidence_file) != root.join("evidence.json")
        || !matches!(input.base.mode.as_str(), "seed" | "verify")
    {
        return Err("iam_input");
    }
    let clients = clients(&input).await;
    let base = &input.base;
    let initializer = &clients["initializer"];
    let gateway = &clients["artifact-gateway"];
    let data = &clients["artifact-data"];
    let maintenance = &clients["artifact-maintenance"];
    let deadline = tokio::time::Instant::now() + Duration::from_secs(90);
    loop {
        let result = initializer.head_bucket().bucket(&base.bucket).send().await;
        let ready = if base.mode == "seed" {
            result.err().and_then(|error| {
                error
                    .raw_response()
                    .map(|response| response.status().as_u16())
            }) == Some(404)
        } else {
            result.is_ok()
        };
        if ready {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            return Err("iam_readiness");
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    if base.mode == "verify" {
        let before = read_private(Path::new(&base.evidence_file))?;
        let evidence: IamEvidence = serde_json::from_slice(&before).map_err(|_| "iam_evidence")?;
        restart_exact(gateway, base, "v1/object", &evidence.second, SECOND, "main")
            .await
            .map_err(|_| "iam_restarted_read")?;
        restart_exact(data, base, "v1/data", &evidence.data, FIRST, "data")
            .await
            .map_err(|_| "iam_restarted_read")?;
        restart_exact(
            gateway,
            base,
            "v1/presigned",
            &evidence.presigned,
            FIRST,
            "presigned",
        )
        .await
        .map_err(|_| "iam_restarted_read")?;
        restart_exact(
            maintenance,
            base,
            "v1/object",
            &evidence.second,
            SECOND,
            "maintenance",
        )
        .await
        .map_err(|_| "iam_restarted_read")?;
        absent(gateway, base, "v1/object", &evidence.first)
            .await
            .map_err(|_| "iam_restarted_read")?;
        denied_matrix(&clients, base, &evidence.second).await?;
        if read_private(Path::new(&base.evidence_file))? != before {
            return Err("iam_readonly_evidence");
        }
        return Ok(());
    }
    initializer
        .create_bucket()
        .bucket(&base.bucket)
        .send()
        .await
        .map_err(|_| "iam_bucket_create")?;
    initializer
        .put_bucket_versioning()
        .bucket(&base.bucket)
        .versioning_configuration(
            VersioningConfiguration::builder()
                .status(BucketVersioningStatus::Enabled)
                .build(),
        )
        .send()
        .await
        .map_err(|_| "iam_versioning")?;
    initializer
        .put_bucket_tagging()
        .bucket(&base.bucket)
        .tagging(
            Tagging::builder()
                .tag_set(
                    Tag::builder()
                        .key("insight-qualification-owner")
                        .value(&base.bucket)
                        .build()
                        .map_err(|_| "iam_tagging")?,
                )
                .build()
                .map_err(|_| "iam_tagging")?,
        )
        .send()
        .await
        .map_err(|_| "iam_tagging")?;
    initializer
        .put_bucket_cors()
        .bucket(&base.bucket)
        .cors_configuration(
            CorsConfiguration::builder()
                .cors_rules(
                    CorsRule::builder()
                        .allowed_methods("PUT")
                        .allowed_origins(ORIGIN)
                        .allowed_headers("content-type")
                        .build()
                        .map_err(|_| "iam_cors")?,
                )
                .build()
                .map_err(|_| "iam_cors")?,
        )
        .send()
        .await
        .map_err(|_| "iam_cors")?;
    metadata(initializer, base)
        .await
        .map_err(|_| "iam_readiness_role")?;
    for client in clients.values() {
        client
            .head_bucket()
            .bucket(&base.bucket)
            .send()
            .await
            .map_err(|_| "iam_readiness_role")?;
        client
            .get_bucket_versioning()
            .bucket(&base.bucket)
            .send()
            .await
            .map_err(|_| "iam_readiness_role")?;
    }
    let first = version(
        gateway
            .put_object()
            .bucket(&base.bucket)
            .key("v1/object")
            .if_none_match("*")
            .body(ByteStream::from_static(FIRST))
            .send()
            .await
            .map_err(|_| "iam_put")?
            .version_id(),
    )?;
    let replay = gateway
        .put_object()
        .bucket(&base.bucket)
        .key("v1/object")
        .if_none_match("*")
        .body(ByteStream::from_static(FIRST))
        .send()
        .await;
    if replay.err().and_then(|error| {
        error
            .raw_response()
            .map(|response| response.status().as_u16())
    }) != Some(412)
    {
        return Err("iam_replay");
    }
    let second = version(
        gateway
            .put_object()
            .bucket(&base.bucket)
            .key("v1/object")
            .body(ByteStream::from_static(SECOND))
            .send()
            .await
            .map_err(|_| "iam_put")?
            .version_id(),
    )?;
    let data_version = version(
        data.put_object()
            .bucket(&base.bucket)
            .key("v1/data")
            .if_none_match("*")
            .body(ByteStream::from_static(FIRST))
            .send()
            .await
            .map_err(|_| "iam_put")?
            .version_id(),
    )?;
    for role in [
        "artifact-gateway",
        "artifact-data",
        "artifact-maintenance",
        "qualification-reader",
    ] {
        exact_bytes(&clients[role], base, "v1/object", &first, FIRST)
            .await
            .map_err(|_| "iam_exact_read")?;
        exact_bytes(&clients[role], base, "v1/object", &second, SECOND)
            .await
            .map_err(|_| "iam_exact_read")?;
    }
    for client in [gateway, data] {
        if client
            .head_object()
            .bucket(&base.bucket)
            .key("v1/object")
            .send()
            .await
            .map_err(|_| "iam_head_latest")?
            .version_id()
            != Some(second.as_str())
        {
            return Err("iam_head_latest");
        }
    }
    denied_matrix(&clients, base, &first).await?;
    let http = http_client(base).await?;
    let put = gateway
        .put_object()
        .bucket(&base.bucket)
        .key("v1/presigned")
        .if_none_match("*")
        .content_type("application/octet-stream")
        .presigned(presigning()?)
        .await
        .map_err(|_| "iam_presign")?;
    let mut upload = http.put(put.uri()).body(FIRST.to_vec());
    for (name, value) in put.headers() {
        upload = upload.header(name, value);
    }
    let uploaded = upload.send().await.map_err(|_| "iam_http")?;
    if uploaded.status().as_u16() != 200 {
        return Err("iam_put");
    }
    let presigned_version = version(
        uploaded
            .headers()
            .get("x-amz-version-id")
            .and_then(|value| value.to_str().ok()),
    )?;
    exact_bytes(gateway, base, "v1/presigned", &presigned_version, FIRST)
        .await
        .map_err(|_| "iam_exact_read")?;
    let signed = maintenance
        .delete_object()
        .bucket(&base.bucket)
        .key("v1/object")
        .version_id(&first)
        .presigned(presigning()?)
        .await
        .map_err(|_| "iam_presign")?;
    let mut tampered = reqwest::Url::parse(signed.uri()).map_err(|_| "iam_presign")?;
    let pairs = tampered
        .query_pairs()
        .map(|(key, value)| (key.into_owned(), value.into_owned()))
        .collect::<Vec<_>>();
    tampered
        .query_pairs_mut()
        .clear()
        .extend_pairs(pairs.iter().map(|(key, value)| {
            (
                key.as_str(),
                if key == "versionId" {
                    ""
                } else {
                    value.as_str()
                },
            )
        }));
    let mut tampered_request = http.delete(tampered);
    for (name, value) in signed.headers() {
        tampered_request = tampered_request.header(name, value);
    }
    if tampered_request
        .send()
        .await
        .map_err(|_| "iam_http")?
        .status()
        .as_u16()
        != 403
    {
        return Err("iam_signature_query");
    }
    if http
        .get(format!(
            "{}/{}/v1/object?versionId={first}",
            base.endpoint, base.bucket
        ))
        .send()
        .await
        .map_err(|_| "iam_http")?
        .status()
        .as_u16()
        != 403
    {
        return Err("iam_anonymous");
    }
    let empty = maintenance
        .delete_object()
        .bucket(&base.bucket)
        .key("v1/object")
        .version_id("")
        .presigned(presigning()?)
        .await
        .map_err(|_| "iam_presign")?;
    if !reqwest::Url::parse(empty.uri())
        .map_err(|_| "iam_presign")?
        .query_pairs()
        .any(|(key, value)| key == "versionId" && value.is_empty())
    {
        return Err("iam_empty_query_missing");
    }
    let response = request(&http, &empty).await?;
    if response.status().as_u16() != 204
        || response
            .headers()
            .get("x-amz-delete-marker")
            .and_then(|value| value.to_str().ok())
            != Some("true")
    {
        return Err("iam_empty_delete");
    }
    let marker = version(
        response
            .headers()
            .get("x-amz-version-id")
            .and_then(|value| value.to_str().ok()),
    )
    .map_err(|_| "iam_marker_missing")?;
    exact_bytes(gateway, base, "v1/object", &first, FIRST)
        .await
        .map_err(|_| "iam_empty_version_destroyed")?;
    exact_bytes(gateway, base, "v1/object", &second, SECOND)
        .await
        .map_err(|_| "iam_empty_version_destroyed")?;
    let removed = maintenance
        .delete_object()
        .bucket(&base.bucket)
        .key("v1/object")
        .version_id(&first)
        .send()
        .await
        .map_err(|_| "iam_exact_delete")?;
    if removed.version_id() != Some(first.as_str()) || removed.delete_marker() == Some(true) {
        return Err("iam_exact_delete");
    }
    absent(gateway, base, "v1/object", &first)
        .await
        .map_err(|_| "iam_exact_delete")?;
    exact_bytes(gateway, base, "v1/object", &second, SECOND)
        .await
        .map_err(|_| "iam_other_version_destroyed")?;
    let evidence = IamEvidence {
        empty_delete_status: 204,
        empty_delete_marker: true,
        original_versions_retained: true,
        denied_status: 403,
        first,
        second,
        marker,
        data: data_version,
        presigned: presigned_version,
    };
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&base.evidence_file)
        .map_err(|_| "iam_evidence")?;
    file.write_all(&serde_json::to_vec(&evidence).map_err(|_| "iam_evidence")?)
        .and_then(|()| file.sync_all())
        .map_err(|_| "iam_evidence")?;
    fs::File::open(root)
        .and_then(|directory| directory.sync_all())
        .map_err(|_| "iam_evidence")?;
    Ok(())
}
