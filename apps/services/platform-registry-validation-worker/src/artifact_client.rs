//! The validation worker receives exact owner-bound bytes through the Artifact gateway.
//! It has no object-store credentials, locators or upload interface.
use crate::RegistryValidationWorkerError;
use insight_platform_artifacts::{
    RegistryArtifactReadRequestV1, RegistryArtifactReadResponseV1,
    MAX_REGISTRY_ARTIFACT_RESPONSE_BYTES,
};
use insight_platform_contracts::{parse_strict_json, JsonLimits, ResourceId};

#[derive(Clone)]
pub struct RegistryArtifactClient {
    client: reqwest::Client,
    endpoint: String,
}

impl RegistryArtifactClient {
    pub fn install(
        endpoint: &str,
        ca: &[u8],
        certificate: &[u8],
        key: &[u8],
    ) -> Result<Self, RegistryValidationWorkerError> {
        let url = reqwest::Url::parse(endpoint)
            .map_err(|_| RegistryValidationWorkerError::InvalidConfiguration)?;
        if url.scheme() != "https"
            || url.host_str().is_none()
            || url.port().is_none()
            || url.username() != ""
            || url.password().is_some()
            || url.path() != "/"
            || url.query().is_some()
            || url.fragment().is_some()
            || [ca, certificate, key]
                .iter()
                .any(|bytes| bytes.is_empty() || bytes.len() > 1_048_576)
        {
            return Err(RegistryValidationWorkerError::InvalidConfiguration);
        }
        let mut identity = certificate.to_vec();
        identity.extend_from_slice(key);
        let client = reqwest::Client::builder()
            .https_only(true)
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(std::time::Duration::from_secs(5))
            .timeout(std::time::Duration::from_secs(10))
            .add_root_certificate(
                reqwest::Certificate::from_pem(ca)
                    .map_err(|_| RegistryValidationWorkerError::InvalidConfiguration)?,
            )
            .identity(
                reqwest::Identity::from_pem(&identity)
                    .map_err(|_| RegistryValidationWorkerError::InvalidConfiguration)?,
            )
            .build()
            .map_err(|_| RegistryValidationWorkerError::InvalidConfiguration)?;
        Ok(Self {
            client,
            endpoint: endpoint.trim_end_matches('/').to_owned(),
        })
    }

    pub async fn read(
        &self,
        tenant: &ResourceId,
        principal: &ResourceId,
        request: &RegistryArtifactReadRequestV1,
    ) -> Result<RegistryArtifactReadResponseV1, RegistryValidationWorkerError> {
        request
            .validate_at(chrono::Utc::now())
            .map_err(|_| RegistryValidationWorkerError::ArtifactReadRejected)?;
        let mut response = self
            .client
            .post(format!(
                "{}/internal/v1/registry-validation/artifacts:read",
                self.endpoint
            ))
            .header("x-insight-verified-tenant-id", tenant.to_string())
            .header("x-insight-verified-principal-id", principal.to_string())
            .header("x-insight-verified-principal-kind", "service_identity")
            .json(request)
            .send()
            .await
            .map_err(|_| RegistryValidationWorkerError::ArtifactReadUnavailable)?;
        if !response.status().is_success() {
            return Err(if response.status().is_server_error() {
                RegistryValidationWorkerError::ArtifactReadUnavailable
            } else {
                RegistryValidationWorkerError::ArtifactReadRejected
            });
        }
        if response
            .content_length()
            .is_some_and(|size| size > MAX_REGISTRY_ARTIFACT_RESPONSE_BYTES as u64)
        {
            return Err(RegistryValidationWorkerError::ArtifactReadRejected);
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| RegistryValidationWorkerError::ArtifactReadUnavailable)?
        {
            if bytes.len().saturating_add(chunk.len()) > MAX_REGISTRY_ARTIFACT_RESPONSE_BYTES {
                return Err(RegistryValidationWorkerError::ArtifactReadRejected);
            }
            bytes.extend_from_slice(&chunk);
        }
        let value = parse_strict_json(
            &bytes,
            JsonLimits {
                max_bytes: MAX_REGISTRY_ARTIFACT_RESPONSE_BYTES,
                max_depth: 8,
                max_properties_per_object: 24,
                max_items_per_array: 1,
                max_string_bytes: MAX_REGISTRY_ARTIFACT_RESPONSE_BYTES,
            },
        )
        .map_err(|_| RegistryValidationWorkerError::ArtifactReadRejected)?;
        let result: RegistryArtifactReadResponseV1 = serde_json::from_value(value)
            .map_err(|_| RegistryValidationWorkerError::ArtifactReadRejected)?;
        result
            .decode_verified()
            .map_err(|_| RegistryValidationWorkerError::ArtifactReadRejected)?;
        Ok(result)
    }
}
