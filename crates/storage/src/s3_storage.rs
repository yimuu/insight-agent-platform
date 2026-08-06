//! Concrete S3 protocol storage used with RustFS and other compatible servers.

use std::{collections::BTreeMap, sync::Arc, time::Duration};

use async_trait::async_trait;
use aws_credential_types::Credentials;
use aws_sdk_s3::{
    config::{BehaviorVersion, Region},
    presigning::PresigningConfig,
    primitives::ByteStream,
    types::ChecksumMode,
    Client,
};
use aws_smithy_http_client::Builder as SmithyHttpClientBuilder;
use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use sha2::{Digest, Sha256};

use insight_engine::{
    artifact_store::{adapter as artifact_store_adapter, ArtifactStoreDeploymentContract},
    repository::{RepositoryError, StorageLocator},
    ArtifactRef, ContentHash,
};

use crate::artifact_store::WorkerArtifactStore;
use crate::repository::RepositoryErrorExt as _;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct S3StorageConfig {
    pub endpoint: String,
    pub public_endpoint: String,
    pub region: String,
    pub bucket: String,
    pub force_path_style: bool,
    pub access_key: String,
    pub secret_key: String,
    pub connect_timeout: Duration,
    pub request_timeout: Duration,
    pub presign_upload_ttl: Duration,
    pub presign_download_ttl: Duration,
    pub artifact_namespace: String,
    pub artifact_inline_threshold_bytes: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PresignedS3Request {
    pub method: String,
    pub url: String,
    pub headers: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct S3ObjectMetadata {
    pub size_bytes: u64,
    pub etag: Option<String>,
    pub version_id: Option<String>,
    pub media_type: Option<String>,
    pub checksum_sha256: Option<String>,
}

#[derive(Clone)]
pub struct S3Storage {
    client: Client,
    presign_client: Client,
    bucket: Arc<str>,
    request_timeout: Duration,
    presign_upload_ttl: Duration,
    presign_download_ttl: Duration,
    artifact_namespace: Arc<str>,
    artifact_store_id: Arc<str>,
    artifact_inline_threshold_bytes: usize,
}

impl std::fmt::Debug for S3Storage {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("S3Storage")
            .field("bucket", &"<redacted>")
            .field("artifact_namespace", &self.artifact_namespace)
            .finish_non_exhaustive()
    }
}

impl S3Storage {
    pub fn new(config: S3StorageConfig) -> Result<Self, RepositoryError> {
        if !valid_endpoint(&config.endpoint)
            || !valid_endpoint(&config.public_endpoint)
            || config.region.trim().is_empty()
            || config.bucket.trim().is_empty()
            || config.access_key.is_empty()
            || config.secret_key.is_empty()
            || config.connect_timeout.is_zero()
            || config.request_timeout.is_zero()
            || config.connect_timeout > config.request_timeout
            || config.presign_upload_ttl.is_zero()
            || config.presign_download_ttl.is_zero()
            || config.artifact_inline_threshold_bytes == 0
            || !valid_namespace(&config.artifact_namespace)
        {
            return Err(RepositoryError::invalid_configuration());
        }
        let credentials = Credentials::new(
            config.access_key,
            config.secret_key,
            None,
            None,
            "insight-agent-platform-s3",
        );
        let build = |endpoint: &str, networked: bool| {
            let timeout = aws_sdk_s3::config::timeout::TimeoutConfig::builder()
                .connect_timeout(config.connect_timeout)
                .operation_timeout(config.request_timeout)
                .build();
            let mut sdk = aws_sdk_s3::Config::builder()
                .behavior_version(BehaviorVersion::latest())
                .endpoint_url(endpoint)
                .region(Region::new(config.region.clone()))
                .credentials_provider(credentials.clone())
                .force_path_style(config.force_path_style)
                .timeout_config(timeout);
            // RustFS commonly uses a private plain-HTTP endpoint. Selecting an
            // HTTP-only connector avoids loading native TLS roots for a
            // protocol that will never use TLS, which also keeps minimal
            // containers without a system CA bundle operational.
            if endpoint.starts_with("http://") || !networked {
                sdk = sdk.http_client(SmithyHttpClientBuilder::new().build_http());
            }
            let sdk = sdk.build();
            Client::from_conf(sdk)
        };
        let identity = format!(
            "{}\0{}\0{}",
            config.endpoint, config.bucket, config.artifact_namespace
        );
        let digest = Sha256::digest(identity.as_bytes());
        let store_id = format!(
            "artifact_store_{}",
            digest[..16]
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>()
        );
        Ok(Self {
            client: build(&config.endpoint, true),
            // This client only constructs signed requests; it never opens a
            // connection to the public endpoint.
            presign_client: build(&config.public_endpoint, false),
            bucket: Arc::from(config.bucket),
            request_timeout: config.request_timeout,
            presign_upload_ttl: config.presign_upload_ttl,
            presign_download_ttl: config.presign_download_ttl,
            artifact_namespace: Arc::from(config.artifact_namespace),
            artifact_store_id: Arc::from(store_id),
            artifact_inline_threshold_bytes: config.artifact_inline_threshold_bytes,
        })
    }

    pub async fn check_readiness(&self) -> Result<(), RepositoryError> {
        self.with_timeout(self.client.head_bucket().bucket(&*self.bucket).send())
            .await
            .map(|_| ())
    }

    pub async fn put_bytes(
        &self,
        key: &str,
        bytes: Vec<u8>,
        media_type: Option<&str>,
    ) -> Result<S3ObjectMetadata, RepositoryError> {
        validate_key(key)?;
        let checksum_sha256 = BASE64_STANDARD.encode(Sha256::digest(&bytes));
        let mut request = self
            .client
            .put_object()
            .bucket(&*self.bucket)
            .key(key)
            .checksum_sha256(checksum_sha256)
            .body(ByteStream::from(bytes));
        if let Some(media_type) = media_type {
            request = request.content_type(media_type);
        }
        self.with_timeout(request.send()).await?;
        self.head_object(key).await
    }

    pub async fn head_object(&self, key: &str) -> Result<S3ObjectMetadata, RepositoryError> {
        self.find_object(key)
            .await?
            .ok_or_else(RepositoryError::invalid_data)
    }

    pub(crate) async fn find_object(
        &self,
        key: &str,
    ) -> Result<Option<S3ObjectMetadata>, RepositoryError> {
        validate_key(key)?;
        let response = tokio::time::timeout(
            self.request_timeout,
            self.client
                .head_object()
                .bucket(&*self.bucket)
                .key(key)
                .checksum_mode(ChecksumMode::Enabled)
                .send(),
        )
        .await
        .map_err(|_| storage_failure())?;
        let output = match response {
            Ok(output) => output,
            Err(error)
                if error
                    .as_service_error()
                    .is_some_and(|error| error.is_not_found()) =>
            {
                return Ok(None);
            }
            Err(_) => return Err(storage_failure()),
        };
        let size_bytes = output
            .content_length()
            .and_then(|value| u64::try_from(value).ok())
            .ok_or_else(RepositoryError::invalid_data)?;
        Ok(Some(S3ObjectMetadata {
            size_bytes,
            etag: output.e_tag().map(str::to_owned),
            version_id: output.version_id().map(str::to_owned),
            media_type: output.content_type().map(str::to_owned),
            checksum_sha256: match output.checksum_sha256() {
                Some(checksum) => Some(base64_checksum_to_hex(checksum)?),
                None => output
                    .metadata()
                    .and_then(|metadata| metadata.get("sha256"))
                    .cloned(),
            },
        }))
    }

    pub async fn get_bytes(&self, key: &str, max_bytes: usize) -> Result<Vec<u8>, RepositoryError> {
        let metadata = self.head_object(key).await?;
        if metadata.size_bytes > u64::try_from(max_bytes).unwrap_or(u64::MAX) {
            return Err(RepositoryError::invalid_data());
        }
        let output = self
            .with_timeout(
                self.client
                    .get_object()
                    .bucket(&*self.bucket)
                    .key(key)
                    .send(),
            )
            .await?;
        let collected = self.with_timeout(output.body.collect()).await?;
        let bytes = collected.into_bytes().to_vec();
        if bytes.len() > max_bytes || u64::try_from(bytes.len()).ok() != Some(metadata.size_bytes) {
            return Err(RepositoryError::invalid_data());
        }
        Ok(bytes)
    }

    /// Reads at most `max_bytes` beginning at `offset` using the S3 Range
    /// contract. This is the only partial-read primitive exposed by the
    /// product, so callers cannot accidentally issue an unbounded object read.
    pub async fn get_bytes_range(
        &self,
        key: &str,
        offset: u64,
        max_bytes: usize,
    ) -> Result<Vec<u8>, RepositoryError> {
        validate_key(key)?;
        if max_bytes == 0 {
            return Err(RepositoryError::invalid_data());
        }
        let byte_count = u64::try_from(max_bytes).map_err(|_| RepositoryError::invalid_data())?;
        let end = offset
            .checked_add(byte_count - 1)
            .ok_or_else(RepositoryError::invalid_data)?;
        let output = self
            .with_timeout(
                self.client
                    .get_object()
                    .bucket(&*self.bucket)
                    .key(key)
                    .range(format!("bytes={offset}-{end}"))
                    .send(),
            )
            .await?;
        let collected = self.with_timeout(output.body.collect()).await?;
        let bytes = collected.into_bytes().to_vec();
        if bytes.is_empty() || bytes.len() > max_bytes {
            return Err(RepositoryError::invalid_data());
        }
        Ok(bytes)
    }

    pub async fn delete_object(&self, key: &str) -> Result<(), RepositoryError> {
        self.delete_object_if_identity(key, None, None).await
    }

    pub async fn delete_object_if_identity(
        &self,
        key: &str,
        etag: Option<&str>,
        version_id: Option<&str>,
    ) -> Result<(), RepositoryError> {
        validate_key(key)?;
        let mut request = self
            .client
            .delete_object()
            .bucket(&*self.bucket)
            .key(key)
            .set_version_id(version_id.map(str::to_owned));
        if let Some(etag) = etag {
            request = request.if_match(etag);
        }
        self.with_timeout(request.send()).await.map(|_| ())
    }

    pub async fn presign_put(
        &self,
        key: &str,
        size_bytes: u64,
        media_type: &str,
        checksum_sha256: Option<&str>,
    ) -> Result<PresignedS3Request, RepositoryError> {
        validate_key(key)?;
        let length = i64::try_from(size_bytes).map_err(|_| RepositoryError::invalid_data())?;
        let mut request = self
            .presign_client
            .put_object()
            .bucket(&*self.bucket)
            .key(key)
            .content_length(length)
            .content_type(media_type)
            .if_none_match("*");
        if let Some(checksum_sha256) = checksum_sha256 {
            request = request
                .checksum_sha256(hex_checksum_to_base64(checksum_sha256)?)
                .metadata("sha256", checksum_sha256);
        }
        let request = request
            .presigned(
                PresigningConfig::expires_in(self.presign_upload_ttl)
                    .map_err(|_| RepositoryError::invalid_configuration())?,
            )
            .await
            .map_err(|_| storage_failure())?;
        presigned_request(request.method(), request.uri(), request.headers())
    }

    pub async fn presign_get(&self, key: &str) -> Result<PresignedS3Request, RepositoryError> {
        validate_key(key)?;
        let request = self
            .presign_client
            .get_object()
            .bucket(&*self.bucket)
            .key(key)
            .presigned(
                PresigningConfig::expires_in(self.presign_download_ttl)
                    .map_err(|_| RepositoryError::invalid_configuration())?,
            )
            .await
            .map_err(|_| storage_failure())?;
        presigned_request(request.method(), request.uri(), request.headers())
    }

    pub const fn presign_download_ttl(&self) -> Duration {
        self.presign_download_ttl
    }

    pub const fn presign_upload_ttl(&self) -> Duration {
        self.presign_upload_ttl
    }

    async fn with_timeout<F, T, E>(&self, future: F) -> Result<T, RepositoryError>
    where
        F: std::future::Future<Output = Result<T, E>>,
    {
        tokio::time::timeout(self.request_timeout, future)
            .await
            .map_err(|_| storage_failure())?
            .map_err(|_| storage_failure())
    }

    fn artifact_key(&self, artifact: &ArtifactRef) -> String {
        format!(
            "run-artifacts/{}/{}/content",
            self.artifact_namespace,
            artifact.artifact_id().as_str()
        )
    }

    fn tenant_artifact_key(
        &self,
        namespace: &str,
        tenant_id: &str,
        artifact: &ArtifactRef,
    ) -> Result<String, RepositoryError> {
        tenant_artifact_key(namespace, tenant_id, artifact)
    }

    fn validate_artifact_locator(
        &self,
        artifact: &ArtifactRef,
        locator: &StorageLocator,
    ) -> Result<String, RepositoryError> {
        let key = locator.expose_to_storage_adapter();
        if key == self.artifact_key(artifact) {
            return Ok(key.to_owned());
        }
        let segments = key.split('/').collect::<Vec<_>>();
        let valid = segments.len() == 4
            && matches!(segments[0], "run-artifacts" | "conversation-content")
            && segments[1].len() == 64
            && segments[1].bytes().all(|byte| byte.is_ascii_hexdigit())
            && segments[2] == artifact.artifact_id().as_str()
            && segments[3] == "content";
        valid
            .then(|| key.to_owned())
            .ok_or_else(RepositoryError::invalid_data)
    }
}

fn tenant_artifact_key(
    namespace: &str,
    tenant_id: &str,
    artifact: &ArtifactRef,
) -> Result<String, RepositoryError> {
    if !matches!(namespace, "run-artifacts" | "conversation-content")
        || tenant_id.is_empty()
        || tenant_id.len() > 256
        || tenant_id.chars().any(char::is_control)
    {
        return Err(RepositoryError::invalid_configuration());
    }
    let tenant_hash = Sha256::digest(tenant_id.as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    Ok(format!(
        "{namespace}/{tenant_hash}/{}/content",
        artifact.artifact_id().as_str()
    ))
}

#[async_trait]
impl WorkerArtifactStore for S3Storage {
    fn inline_threshold_bytes(&self) -> usize {
        self.artifact_inline_threshold_bytes
    }

    fn deployment_contract(&self) -> ArtifactStoreDeploymentContract {
        artifact_store_adapter::s3_deployment_contract(
            self.artifact_store_id.to_string(),
            self.artifact_namespace.to_string(),
        )
    }

    fn artifact_for_bytes(
        &self,
        bytes: &[u8],
        media_type: Option<String>,
    ) -> Result<ArtifactRef, RepositoryError> {
        let hash = ContentHash::from_bytes(bytes);
        let artifact_id = insight_engine::ArtifactId::new(format!(
            "artifact_{}",
            hash.as_str().trim_start_matches("sha256:")
        ))
        .map_err(|_| RepositoryError::invalid_data())?;
        ArtifactRef::new(
            artifact_id,
            hash,
            u64::try_from(bytes.len()).map_err(|_| RepositoryError::invalid_data())?,
            media_type,
        )
        .map_err(|_| RepositoryError::invalid_data())
    }

    fn storage_locator(&self, artifact: &ArtifactRef) -> Result<StorageLocator, RepositoryError> {
        StorageLocator::new(self.artifact_key(artifact))
    }

    fn storage_locator_for_tenant(
        &self,
        namespace: &str,
        tenant_id: &str,
        artifact: &ArtifactRef,
    ) -> Result<StorageLocator, RepositoryError> {
        StorageLocator::new(self.tenant_artifact_key(namespace, tenant_id, artifact)?)
    }

    async fn put_and_verify(
        &self,
        artifact: &ArtifactRef,
        bytes: &[u8],
    ) -> Result<(ContentHash, u64), RepositoryError> {
        let key = self.artifact_key(artifact);
        self.put_bytes(&key, bytes.to_vec(), artifact.media_type())
            .await?;
        let stored = self.get_bytes(&key, bytes.len()).await?;
        let hash = ContentHash::from_bytes(&stored);
        let size = u64::try_from(stored.len()).map_err(|_| RepositoryError::invalid_data())?;
        if &hash != artifact.content_hash() || size != artifact.size_bytes() {
            return Err(RepositoryError::invalid_data());
        }
        Ok((hash, size))
    }

    async fn put_and_verify_at(
        &self,
        artifact: &ArtifactRef,
        locator: &StorageLocator,
        bytes: &[u8],
    ) -> Result<(ContentHash, u64), RepositoryError> {
        let key = self.validate_artifact_locator(artifact, locator)?;
        self.put_bytes(&key, bytes.to_vec(), artifact.media_type())
            .await?;
        let stored = self.get_bytes(&key, bytes.len()).await?;
        let hash = ContentHash::from_bytes(&stored);
        let size = u64::try_from(stored.len()).map_err(|_| RepositoryError::invalid_data())?;
        if &hash != artifact.content_hash() || size != artifact.size_bytes() {
            return Err(RepositoryError::invalid_data());
        }
        Ok((hash, size))
    }

    async fn read_and_verify(
        &self,
        artifact: &ArtifactRef,
        locator: &StorageLocator,
        max_bytes: usize,
    ) -> Result<Vec<u8>, RepositoryError> {
        let key = self.validate_artifact_locator(artifact, locator)?;
        let bytes = self.get_bytes(&key, max_bytes).await?;
        if ContentHash::from_bytes(&bytes) != *artifact.content_hash()
            || u64::try_from(bytes.len()).ok() != Some(artifact.size_bytes())
        {
            return Err(RepositoryError::invalid_data());
        }
        Ok(bytes)
    }

    async fn delete(
        &self,
        artifact: &ArtifactRef,
        locator: &StorageLocator,
    ) -> Result<(), RepositoryError> {
        let key = self.validate_artifact_locator(artifact, locator)?;
        self.delete_object(&key).await
    }
}

fn presigned_request<'a>(
    method: &str,
    uri: &str,
    headers: impl IntoIterator<Item = (&'a str, &'a str)>,
) -> Result<PresignedS3Request, RepositoryError> {
    let headers = headers
        .into_iter()
        .map(|(name, value)| Ok((name.to_owned(), value.to_owned())))
        .collect::<Result<BTreeMap<_, _>, RepositoryError>>()?;
    Ok(PresignedS3Request {
        method: method.to_owned(),
        url: uri.to_owned(),
        headers,
    })
}

fn valid_endpoint(value: &str) -> bool {
    value.starts_with("http://") || value.starts_with("https://")
}

fn hex_checksum_to_base64(checksum: &str) -> Result<String, RepositoryError> {
    if checksum.len() != 64
        || checksum
            .bytes()
            .any(|byte| !byte.is_ascii_digit() && !(b'a'..=b'f').contains(&byte))
    {
        return Err(RepositoryError::invalid_data());
    }
    let bytes = checksum
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let high = hex_nibble(pair[0])?;
            let low = hex_nibble(pair[1])?;
            Ok((high << 4) | low)
        })
        .collect::<Result<Vec<_>, RepositoryError>>()?;
    Ok(BASE64_STANDARD.encode(bytes))
}

fn base64_checksum_to_hex(checksum: &str) -> Result<String, RepositoryError> {
    let bytes = BASE64_STANDARD
        .decode(checksum)
        .map_err(|_| RepositoryError::invalid_data())?;
    if bytes.len() != 32 {
        return Err(RepositoryError::invalid_data());
    }
    Ok(bytes.iter().map(|byte| format!("{byte:02x}")).collect())
}

fn hex_nibble(byte: u8) -> Result<u8, RepositoryError> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        _ => Err(RepositoryError::invalid_data()),
    }
}

fn valid_namespace(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
}

fn validate_key(key: &str) -> Result<(), RepositoryError> {
    if key.is_empty()
        || key.len() > 1024
        || key.starts_with('/')
        || key
            .split('/')
            .any(|segment| segment.is_empty() || segment == "..")
        || key.chars().any(char::is_control)
    {
        return Err(RepositoryError::invalid_configuration());
    }
    Ok(())
}

fn storage_failure() -> RepositoryError {
    insight_engine::repository::adapter::repository_error(
        insight_engine::repository::REPOSITORY_STORAGE_FAILURE,
        "S3 object storage operation failed",
    )
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use insight_engine::{ArtifactId, ArtifactRef, ContentHash};
    use sha2::{Digest, Sha256};

    use super::{
        base64_checksum_to_hex, hex_checksum_to_base64, tenant_artifact_key, S3Storage,
        S3StorageConfig,
    };

    #[test]
    fn checksum_wire_encoding_round_trips_exact_sha256_bytes() {
        let checksum = Sha256::digest(b"content")
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        let encoded = hex_checksum_to_base64(&checksum).unwrap();
        assert_eq!(base64_checksum_to_hex(&encoded).unwrap(), checksum);
        assert!(hex_checksum_to_base64(&"g".repeat(64)).is_err());
        assert!(base64_checksum_to_hex("not-base64").is_err());
    }

    #[test]
    fn plain_http_rustfs_configuration_does_not_require_native_tls_roots() {
        S3Storage::new(S3StorageConfig {
            endpoint: "http://rustfs.internal:9000".to_owned(),
            public_endpoint: "https://files.example.test".to_owned(),
            region: "us-east-1".to_owned(),
            bucket: "platform".to_owned(),
            force_path_style: true,
            access_key: "access".to_owned(),
            secret_key: "secret".to_owned(),
            connect_timeout: Duration::from_secs(1),
            request_timeout: Duration::from_secs(2),
            presign_upload_ttl: Duration::from_secs(60),
            presign_download_ttl: Duration::from_secs(60),
            artifact_namespace: "test".to_owned(),
            artifact_inline_threshold_bytes: 1024,
        })
        .unwrap();
    }

    #[test]
    fn artifact_locator_uses_closed_namespace_and_irreversible_tenant_hash() {
        let artifact = ArtifactRef::new(
            ArtifactId::new("artifact_s3_locator_contract").unwrap(),
            ContentHash::from_bytes(b"private"),
            7,
            Some("application/json".to_owned()),
        )
        .unwrap();
        let first = tenant_artifact_key("run-artifacts", "tenant-a", &artifact).unwrap();
        let second = tenant_artifact_key("run-artifacts", "tenant-b", &artifact).unwrap();
        assert_ne!(first, second);
        let key = first.as_str();
        assert!(!key.contains("tenant-a"));
        let segments = key.split('/').collect::<Vec<_>>();
        assert_eq!(segments.len(), 4);
        assert_eq!(segments[0], "run-artifacts");
        assert_eq!(segments[1].len(), 64);
        assert_eq!(segments[2], artifact.artifact_id().as_str());
        assert_eq!(segments[3], "content");
        assert!(tenant_artifact_key("not-a-product-namespace", "tenant-a", &artifact).is_err());
    }
}
