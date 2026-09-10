//! S3-only physical qualification. Never constructs a KMS client or a business authority.
#![cfg(unix)]

use aws_config::{retry::RetryConfig, timeout::TimeoutConfig, BehaviorVersion};
use aws_sdk_s3::{
    config::Credentials,
    presigning::PresigningConfig,
    primitives::ByteStream,
    types::{
        BucketVersioningStatus, CorsConfiguration, CorsRule, Tag, Tagging, VersioningConfiguration,
    },
    Client,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    fs,
    os::unix::fs::{MetadataExt, PermissionsExt},
    path::Path,
    time::Duration,
};

type Result<T> = std::result::Result<T, &'static str>;
const ORIGIN: &str = "https://console.s3-qualification.invalid";
const FIRST: &[u8] = b"physical-s3-canary-generation-one";
const SECOND: &[u8] = b"physical-s3-canary-generation-two";

#[path = "support/s3_iam.rs"]
mod s3_iam;

#[tokio::test]
#[ignore = "requires the owning isolated static-IAM S3 qualification harness"]
async fn actual_s3_static_iam_contract() {
    s3_iam::run()
        .await
        .expect("S3 IAM qualification failed with safe code");
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct Input {
    schema_version: u32,
    endpoint: String,
    bucket: String,
    access_key: String,
    secret_key: String,
    ca_file: String,
    evidence_file: String,
    mode: String,
}

#[derive(Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct Evidence {
    schema_version: u32,
    deleted_version: String,
    versions: Vec<(String, String, String)>,
}

fn read_private(path: &Path) -> Result<Vec<u8>> {
    for parent in path.ancestors().skip(1) {
        if !fs::symlink_metadata(parent)
            .map_err(|_| "private_parent")?
            .is_dir()
        {
            return Err("private_parent");
        }
    }
    let metadata = fs::symlink_metadata(path).map_err(|_| "private_file")?;
    let parent = fs::symlink_metadata(path.parent().ok_or("private_parent")?)
        .map_err(|_| "private_parent")?;
    if !metadata.is_file()
        || metadata.nlink() != 1
        || metadata.len() > 65_536
        || metadata.permissions().mode() & 0o777 != 0o600
        || parent.permissions().mode() & 0o777 != 0o700
        || parent.uid() != metadata.uid()
    {
        return Err("private_file");
    }
    fs::read(path).map_err(|_| "private_file")
}

async fn client(input: &Input, wrong_credential: bool) -> Client {
    let configuration = aws_config::defaults(BehaviorVersion::latest())
        .region(aws_sdk_s3::config::Region::new("us-east-1"))
        .credentials_provider(Credentials::new(
            input.access_key.clone(),
            if wrong_credential {
                "deliberately-wrong-fixture-credential".into()
            } else {
                input.secret_key.clone()
            },
            None,
            None,
            "isolated-s3-qualification",
        ))
        .retry_config(RetryConfig::standard().with_max_attempts(1))
        .timeout_config(
            TimeoutConfig::builder()
                .connect_timeout(Duration::from_secs(3))
                .operation_timeout(Duration::from_secs(10))
                .operation_attempt_timeout(Duration::from_secs(10))
                .build(),
        )
        .load()
        .await;
    Client::from_conf(
        aws_sdk_s3::config::Builder::from(&configuration)
            .endpoint_url(&input.endpoint)
            .force_path_style(true)
            .build(),
    )
}

fn version(value: Option<&str>) -> Result<String> {
    value
        .filter(|value| !value.is_empty() && *value != "null" && value.len() <= 256)
        .map(str::to_owned)
        .ok_or("version_missing")
}

async fn metadata(client: &Client, input: &Input) -> Result<()> {
    let tags = client
        .get_bucket_tagging()
        .bucket(&input.bucket)
        .send()
        .await
        .map_err(|_| "tag_read")?;
    if tags.tag_set().len() != 1
        || tags.tag_set()[0].key() != "insight-qualification-owner"
        || tags.tag_set()[0].value() != input.bucket
    {
        return Err("tag_drift");
    }
    let configuration = client
        .get_bucket_versioning()
        .bucket(&input.bucket)
        .send()
        .await
        .map_err(|_| "versioning_read")?;
    if configuration.status() != Some(&BucketVersioningStatus::Enabled) {
        return Err("versioning_drift");
    }
    let cors = client
        .get_bucket_cors()
        .bucket(&input.bucket)
        .send()
        .await
        .map_err(|_| "cors_read")?;
    if cors.cors_rules().len() != 1 {
        return Err("cors_drift");
    }
    let rule = &cors.cors_rules()[0];
    if rule.allowed_origins() != [ORIGIN]
        || rule.allowed_methods() != ["PUT"]
        || rule.allowed_headers() != ["content-type"]
        || !rule.expose_headers().is_empty()
    {
        return Err("cors_drift");
    }
    Ok(())
}

async fn inventory(client: &Client, input: &Input) -> Result<Vec<(String, String, String)>> {
    let output = client
        .list_object_versions()
        .bucket(&input.bucket)
        .max_keys(20)
        .send()
        .await
        .map_err(|_| "versions_read")?;
    if output.is_truncated() == Some(true) || !output.delete_markers().is_empty() {
        return Err("versions_incomplete");
    }
    let mut versions = output
        .versions()
        .iter()
        .map(|item| {
            Ok((
                item.key().ok_or("key_missing")?.to_owned(),
                version(item.version_id())?,
                item.e_tag().ok_or("etag_missing")?.to_owned(),
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    versions.sort();
    Ok(versions)
}

async fn exact_bytes(
    client: &Client,
    input: &Input,
    key: &str,
    generation: &str,
    expected: &[u8],
) -> Result<()> {
    let head = client
        .head_object()
        .bucket(&input.bucket)
        .key(key)
        .version_id(generation)
        .send()
        .await
        .map_err(|_| "head_exact")?;
    if head.version_id() != Some(generation) || head.content_length() != Some(expected.len() as i64)
    {
        return Err("head_evidence");
    }
    let mut output = client
        .get_object()
        .bucket(&input.bucket)
        .key(key)
        .version_id(generation)
        .send()
        .await
        .map_err(|_| "get_exact")?;
    if output.version_id() != Some(generation)
        || output.content_length() != Some(expected.len() as i64)
    {
        return Err("get_evidence");
    }
    let mut bytes = Vec::with_capacity(expected.len());
    while let Some(chunk) = output.body.next().await {
        let chunk = chunk.map_err(|_| "body_read")?;
        if chunk.len() > expected.len().saturating_sub(bytes.len()) {
            return Err("body_drift");
        }
        bytes.extend_from_slice(&chunk);
    }
    if bytes != expected || Sha256::digest(&bytes) != Sha256::digest(expected) {
        return Err("body_drift");
    }
    Ok(())
}

async fn absent(client: &Client, input: &Input, key: &str, generation: &str) -> Result<()> {
    let result = client
        .head_object()
        .bucket(&input.bucket)
        .key(key)
        .version_id(generation)
        .send()
        .await;
    if result.err().and_then(|error| {
        error
            .raw_response()
            .map(|response| response.status().as_u16())
    }) != Some(404)
    {
        return Err("deleted_version_readable");
    }
    Ok(())
}

async fn http_client(input: &Input) -> Result<reqwest::Client> {
    let certificate = reqwest::Certificate::from_pem(&read_private(Path::new(&input.ca_file))?)
        .map_err(|_| "ca_invalid")?;
    reqwest::Client::builder()
        .no_proxy()
        .retry(reqwest::retry::never())
        .add_root_certificate(certificate)
        .timeout(Duration::from_secs(10))
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .map_err(|_| "http_configuration")
}

async fn negatives(client: &Client, input: &Input) -> Result<()> {
    let wrong = client
        .get_object()
        .bucket(format!("foreign-{}", &input.bucket[0..32]))
        .key("canary")
        .send()
        .await;
    if wrong.err().and_then(|error| {
        error
            .raw_response()
            .map(|response| response.status().as_u16())
    }) != Some(403)
    {
        return Err("cross_bucket_not_denied");
    }
    let wrong = self::client(input, true)
        .await
        .head_bucket()
        .bucket(&input.bucket)
        .send()
        .await;
    if wrong.err().and_then(|error| {
        error
            .raw_response()
            .map(|response| response.status().as_u16())
    }) != Some(403)
    {
        return Err("wrong_credential_not_denied");
    }
    let http = http_client(input).await?;
    let url = format!("{}/{}/presigned", input.endpoint, input.bucket);
    if http
        .get(&url)
        .send()
        .await
        .map_err(|_| "unsigned_transport")?
        .status()
        .as_u16()
        != 403
    {
        return Err("unsigned_not_denied");
    }
    for (origin, method, header, allowed) in [
        (ORIGIN, "PUT", "content-type", true),
        (
            "https://foreign.s3-qualification.invalid",
            "PUT",
            "content-type",
            false,
        ),
        (ORIGIN, "GET", "content-type", false),
        (ORIGIN, "PUT", "authorization", false),
    ] {
        let response = http
            .request(reqwest::Method::OPTIONS, &url)
            .header("origin", origin)
            .header("access-control-request-method", method)
            .header("access-control-request-headers", header)
            .send()
            .await
            .map_err(|_| "cors_transport")?;
        let headers = response.headers();
        let permits = response.status().is_success()
            && headers
                .get("access-control-allow-origin")
                .and_then(|x| x.to_str().ok())
                == Some(ORIGIN)
            && headers
                .get("access-control-allow-methods")
                .and_then(|x| x.to_str().ok())
                == Some("PUT")
            && headers
                .get("access-control-allow-headers")
                .and_then(|x| x.to_str().ok())
                == Some("content-type");
        if permits != allowed
            || headers.contains_key("access-control-allow-credentials")
            || headers.contains_key("access-control-expose-headers")
        {
            return Err("cors_boundary");
        }
    }
    Ok(())
}

async fn seed(client: &Client, input: &Input) -> Result<Evidence> {
    client
        .create_bucket()
        .bucket(&input.bucket)
        .send()
        .await
        .map_err(|_| "bucket_create")?;
    client
        .put_bucket_tagging()
        .bucket(&input.bucket)
        .tagging(
            Tagging::builder()
                .tag_set(
                    Tag::builder()
                        .key("insight-qualification-owner")
                        .value(&input.bucket)
                        .build()
                        .map_err(|_| "tag_configuration")?,
                )
                .build()
                .map_err(|_| "tag_configuration")?,
        )
        .send()
        .await
        .map_err(|_| "tag_write")?;
    client
        .put_bucket_versioning()
        .bucket(&input.bucket)
        .versioning_configuration(
            VersioningConfiguration::builder()
                .status(BucketVersioningStatus::Enabled)
                .build(),
        )
        .send()
        .await
        .map_err(|_| "versioning_write")?;
    client
        .put_bucket_cors()
        .bucket(&input.bucket)
        .cors_configuration(
            CorsConfiguration::builder()
                .cors_rules(
                    CorsRule::builder()
                        .allowed_origins(ORIGIN)
                        .allowed_methods("PUT")
                        .allowed_headers("content-type")
                        .build()
                        .map_err(|_| "cors_configuration")?,
                )
                .build()
                .map_err(|_| "cors_configuration")?,
        )
        .send()
        .await
        .map_err(|_| "cors_write")?;
    metadata(client, input).await?;
    let http = http_client(input).await?;
    let signed = client
        .put_object()
        .bucket(&input.bucket)
        .key("presigned")
        .content_type("application/octet-stream")
        .presigned(
            PresigningConfig::expires_in(Duration::from_secs(60))
                .map_err(|_| "presign_configuration")?,
        )
        .await
        .map_err(|_| "presign")?;
    let mut request = http.put(signed.uri()).body(FIRST.to_vec());
    for (name, value) in signed.headers() {
        request = request.header(name, value);
    }
    let response = request.send().await.map_err(|_| "presigned_put")?;
    if !response.status().is_success() {
        return Err("presigned_rejected");
    }
    let presigned_version = version(
        response
            .headers()
            .get("x-amz-version-id")
            .and_then(|value| value.to_str().ok()),
    )?;
    exact_bytes(client, input, "presigned", &presigned_version, FIRST).await?;
    let put = || {
        client
            .put_object()
            .bucket(&input.bucket)
            .key("raced")
            .if_none_match("*")
            .body(ByteStream::from_static(FIRST))
            .send()
    };
    let (left, right) = tokio::join!(put(), put());
    let (success, failure) = match (left, right) {
        (Ok(success), Err(failure)) | (Err(failure), Ok(success)) => (success, failure),
        _ => return Err("conditional_concurrency"),
    };
    if failure
        .raw_response()
        .map(|response| response.status().as_u16())
        != Some(412)
    {
        return Err("conditional_status");
    }
    let raced_version = version(success.version_id())?;
    exact_bytes(client, input, "raced", &raced_version, FIRST).await?;
    let replay = put().await;
    if replay.err().and_then(|error| {
        error
            .raw_response()
            .map(|response| response.status().as_u16())
    }) != Some(412)
    {
        return Err("conditional_replay");
    }
    let first = client
        .put_object()
        .bucket(&input.bucket)
        .key("versioned")
        .body(ByteStream::from_static(FIRST))
        .send()
        .await
        .map_err(|_| "first_put")?;
    let first = version(first.version_id())?;
    let second = client
        .put_object()
        .bucket(&input.bucket)
        .key("versioned")
        .body(ByteStream::from_static(SECOND))
        .send()
        .await
        .map_err(|_| "second_put")?;
    let second = version(second.version_id())?;
    if first == second {
        return Err("generation_reused");
    }
    exact_bytes(client, input, "versioned", &first, FIRST).await?;
    exact_bytes(client, input, "versioned", &second, SECOND).await?;
    let deleted = client
        .delete_object()
        .bucket(&input.bucket)
        .key("versioned")
        .version_id(&first)
        .send()
        .await
        .map_err(|_| "delete_exact")?;
    if deleted.version_id() != Some(first.as_str()) || deleted.delete_marker() == Some(true) {
        return Err("delete_evidence");
    }
    absent(client, input, "versioned", &first).await?;
    exact_bytes(client, input, "versioned", &second, SECOND).await?;
    negatives(client, input).await?;
    let versions = inventory(client, input).await?;
    if versions.len() != 3 || versions.iter().filter(|entry| entry.0 == "raced").count() != 1 {
        return Err("unexpected_generation_count");
    }
    Ok(Evidence {
        schema_version: 1,
        deleted_version: first,
        versions,
    })
}

async fn verify(client: &Client, input: &Input, evidence: &Evidence) -> Result<()> {
    metadata(client, input).await?;
    if inventory(client, input).await? != evidence.versions {
        return Err("restart_version_drift");
    }
    for (key, version, _) in &evidence.versions {
        exact_bytes(
            client,
            input,
            key,
            version,
            if key == "versioned" { SECOND } else { FIRST },
        )
        .await?;
    }
    absent(client, input, "versioned", &evidence.deleted_version).await?;
    if inventory(client, input).await? != evidence.versions {
        return Err("readonly_inventory_drift");
    }
    Ok(())
}

async fn run() -> Result<()> {
    let path = std::env::var("INSIGHT_S3_FIXTURE_INPUT").map_err(|_| "fixture_input_missing")?;
    let input: Input = serde_json::from_slice(&read_private(Path::new(&path))?)
        .map_err(|_| "fixture_input_invalid")?;
    let endpoint = reqwest::Url::parse(&input.endpoint).map_err(|_| "fixture_input_invalid")?;
    let root = Path::new(&path).parent().ok_or("fixture_input_invalid")?;
    if input.schema_version != 1
        || input
            .bucket
            .strip_prefix("insight-s3-qualification-")
            .is_none_or(|suffix| {
                suffix.len() != 32 || !suffix.bytes().all(|byte| byte.is_ascii_hexdigit())
            })
        || endpoint.scheme() != "https"
        || !matches!(
            endpoint.host_str(),
            Some("localhost.localstack.cloud" | "127.0.0.1")
        )
        || !endpoint.username().is_empty()
        || endpoint.password().is_some()
        || endpoint.port().is_none_or(|port| port < 1024)
        || endpoint.path() != "/"
        || endpoint.query().is_some()
        || endpoint.fragment().is_some()
        || Path::new(&input.ca_file) != root.join("tls/ca.pem")
        || Path::new(&input.evidence_file) != root.join("evidence.json")
        || input.access_key.len() != 32
        || input.secret_key.len() != 64
        || !matches!(input.mode.as_str(), "seed" | "verify" | "tls-negative")
    {
        return Err("fixture_input_invalid");
    }
    let client = client(&input, false).await;
    if input.mode == "tls-negative" {
        let result = client.head_bucket().bucket(&input.bucket).send().await;
        if !matches!(result, Err(aws_sdk_s3::error::SdkError::DispatchFailure(_))) {
            return Err("sdk_tls_not_rejected");
        }
        return Ok(());
    }
    if input.mode == "seed" {
        // Readiness probes use only signed, bounded reads; mutations below are never retried.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(90);
        loop {
            let result = client.head_bucket().bucket(&input.bucket).send().await;
            if result.err().is_some_and(|error| {
                error
                    .raw_response()
                    .is_some_and(|response| response.status().as_u16() == 404)
            }) {
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                return Err("signed_readiness_timeout");
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        let evidence = seed(&client, &input).await?;
        use std::{io::Write, os::unix::fs::OpenOptionsExt};
        let mut file = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&input.evidence_file)
            .map_err(|_| "evidence_create")?;
        file.write_all(&serde_json::to_vec(&evidence).map_err(|_| "evidence_encode")?)
            .and_then(|()| file.sync_all())
            .map_err(|_| "evidence_write")?;
        fs::File::open(
            Path::new(&input.evidence_file)
                .parent()
                .ok_or("evidence_parent")?,
        )
        .and_then(|directory| directory.sync_all())
        .map_err(|_| "evidence_sync")?;
    } else {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(90);
        loop {
            if client
                .head_bucket()
                .bucket(&input.bucket)
                .send()
                .await
                .is_ok()
            {
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                return Err("signed_readiness_timeout");
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        let evidence = serde_json::from_slice(&read_private(Path::new(&input.evidence_file))?)
            .map_err(|_| "evidence_decode")?;
        verify(&client, &input, &evidence).await?;
    }
    Ok(())
}

#[tokio::test]
#[ignore = "requires the owning isolated S3-only qualification harness"]
async fn actual_s3_versioned_contract() {
    run()
        .await
        .expect("S3 physical qualification failed with safe code");
}

#[test]
fn private_reader_rejects_public_linked_and_oversized_files() {
    let temporary = tempfile::tempdir().unwrap();
    let root = temporary.path().canonicalize().unwrap();
    fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
    let path = root.join("input");
    fs::write(&path, b"fixture").unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
    assert!(read_private(&path).is_ok());
    fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();
    assert!(read_private(&path).is_err());
    fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
    let alias = root.join("alias");
    std::os::unix::fs::symlink(&path, &alias).unwrap();
    assert!(read_private(&alias).is_err());
    fs::remove_file(&alias).unwrap();
    fs::hard_link(&path, &alias).unwrap();
    assert!(read_private(&path).is_err());
    fs::remove_file(&alias).unwrap();
    fs::write(&path, vec![0; 65_537]).unwrap();
    assert!(read_private(&path).is_err());
}
