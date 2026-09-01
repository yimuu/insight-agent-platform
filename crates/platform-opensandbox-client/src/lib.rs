//! Strict OpenSandbox v0.2.3 Kubernetes lifecycle adapter for CR-216.
//!
//! The adapter owns only physical lifecycle and the fixed runner proxy. It has no Platform
//! repository dependency and cannot write Job, Run, or Invocation state.

use async_trait::async_trait;
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use chrono::{DateTime, Utc};
use futures::StreamExt as _;
use insight_platform_contracts::{
    canonical_json, parse_strict_json, JsonLimits, ResourceId, Sha256Digest,
};
use insight_platform_sandbox::opensandbox::{
    BoundedCandidatePageV1, CandidateCursorV1, OpenSandboxCreateV1, OpenSandboxId,
    OpenSandboxObservationV1, OpenSandboxProvider, SandboxActivationFrameV1,
    SandboxCandidateMetadataV1, SandboxCandidateV1, SandboxNetworkMode, SandboxProviderError,
    SandboxRunnerPhaseV1, SandboxRunnerStateFrameV1, MAX_SANDBOX_CANDIDATE_PAGE_ITEMS,
    MAX_SANDBOX_INPUT_BYTES, SANDBOX_CONTRACT_SCHEMA_VERSION, SANDBOX_RUNNER_CONFIG_DIGEST_ENV,
    SANDBOX_RUNNER_CONFIG_ENV, SANDBOX_RUNNER_PORT,
};
use reqwest::{header, Client, Method, RequestBuilder, StatusCode};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::{collections::BTreeMap, fmt, str::FromStr, time::Duration};
use url::Url;

const API_KEY_HEADER: &str = "OPEN-SANDBOX-API-KEY";
const METADATA_SCHEMA: &str = "platform.insight.dev/schema";
const METADATA_TENANT: &str = "platform.insight.dev/tenant";
const METADATA_JOB: &str = "platform.insight.dev/job";
const METADATA_ATTEMPT: &str = "platform.insight.dev/attempt";
const METADATA_PROVISIONING: &str = "platform.insight.dev/provision";
const METADATA_REQUEST: &str = "platform.insight.dev/request";
const METADATA_RUNTIME: &str = "platform.insight.dev/runtime";
const METADATA_PROFILE: &str = "platform.insight.dev/profile";
const METADATA_NETWORK: &str = "platform.insight.dev/network";
const METADATA_SCHEMA_VALUE: &str = "v1";
const MAX_LIFECYCLE_RESPONSE_BYTES: usize = 262_144;
const MAX_API_KEY_BYTES: usize = 256;
const MIN_API_KEY_BYTES: usize = 32;
const MAX_PAGE_NUMBER: u32 = 1_000_000;

#[derive(Clone, PartialEq, Eq)]
pub struct OpenSandboxApiKey(String);

impl fmt::Debug for OpenSandboxApiKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("OpenSandboxApiKey([redacted])")
    }
}

impl OpenSandboxApiKey {
    pub fn parse(value: impl Into<String>) -> Result<Self, OpenSandboxClientConfigError> {
        let value = value.into();
        if !(MIN_API_KEY_BYTES..=MAX_API_KEY_BYTES).contains(&value.len())
            || !value.is_ascii()
            || value.bytes().any(|byte| byte.is_ascii_control())
        {
            return Err(OpenSandboxClientConfigError::InvalidApiKey);
        }
        Ok(Self(value))
    }

    fn expose(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone)]
pub struct OpenSandboxHttpClientConfig {
    pub lifecycle_base_url: Url,
    pub api_key: OpenSandboxApiKey,
    pub request_timeout_milliseconds: u32,
    pub connect_timeout_milliseconds: u32,
    pub candidate_page_items: u8,
}

impl OpenSandboxHttpClientConfig {
    pub fn validate(&self) -> Result<(), OpenSandboxClientConfigError> {
        let base = &self.lifecycle_base_url;
        if base.scheme() != "http"
            || !base.username().is_empty()
            || base.password().is_some()
            || base.host_str().is_none()
            || base.query().is_some()
            || base.fragment().is_some()
            || base.path() != "/v1/"
            || !(100..=120_000).contains(&self.request_timeout_milliseconds)
            || !(100..=10_000).contains(&self.connect_timeout_milliseconds)
            || self.connect_timeout_milliseconds > self.request_timeout_milliseconds
            || !(1..=MAX_SANDBOX_CANDIDATE_PAGE_ITEMS).contains(&self.candidate_page_items)
        {
            return Err(OpenSandboxClientConfigError::InvalidConfiguration);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpenSandboxClientConfigError {
    InvalidConfiguration,
    InvalidApiKey,
}

impl fmt::Display for OpenSandboxClientConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidConfiguration => "OpenSandbox client configuration is invalid",
            Self::InvalidApiKey => "OpenSandbox API key is invalid",
        })
    }
}

impl std::error::Error for OpenSandboxClientConfigError {}

#[derive(Clone)]
pub struct OpenSandboxHttpClient {
    client: Client,
    config: OpenSandboxHttpClientConfig,
}

impl OpenSandboxHttpClient {
    pub fn new(config: OpenSandboxHttpClientConfig) -> Result<Self, OpenSandboxClientConfigError> {
        config.validate()?;
        let client = Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_millis(u64::from(
                config.connect_timeout_milliseconds,
            )))
            .timeout(Duration::from_millis(u64::from(
                config.request_timeout_milliseconds,
            )))
            .http1_only()
            .user_agent("Insight-Agent-Platform-OpenSandbox/1")
            .build()
            .map_err(|_| OpenSandboxClientConfigError::InvalidConfiguration)?;
        Ok(Self { client, config })
    }

    fn lifecycle_url(&self, suffix: &str) -> Result<Url, SandboxProviderError> {
        self.config
            .lifecycle_base_url
            .join(suffix)
            .map_err(|_| SandboxProviderError::InvalidResponse)
    }

    fn request(&self, method: Method, url: Url) -> RequestBuilder {
        self.client
            .request(method, url)
            .header(API_KEY_HEADER, self.config.api_key.expose())
            .header(header::ACCEPT, "application/json")
    }

    async fn exchange(
        &self,
        request: RequestBuilder,
        maximum_bytes: usize,
    ) -> Result<HttpExchange, SandboxProviderError> {
        let response = request.send().await.map_err(classify_reqwest_error)?;
        let status = response.status();
        let content_type = response
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
        let declared = response.content_length();
        if declared.is_some_and(|length| {
            usize::try_from(length).map_or(true, |length| length > maximum_bytes)
        }) {
            return Err(SandboxProviderError::InvalidResponse);
        }
        let mut stream = response.bytes_stream();
        let mut body = Vec::with_capacity(
            declared
                .and_then(|length| usize::try_from(length).ok())
                .unwrap_or(0)
                .min(maximum_bytes),
        );
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(classify_reqwest_error)?;
            if body.len().saturating_add(chunk.len()) > maximum_bytes {
                return Err(SandboxProviderError::InvalidResponse);
            }
            body.extend_from_slice(&chunk);
        }
        if declared.is_some_and(|length| usize::try_from(length).ok() != Some(body.len())) {
            return Err(SandboxProviderError::InvalidResponse);
        }
        Ok(HttpExchange {
            status,
            content_type,
            declared_length: declared,
            body,
        })
    }

    async fn json_exchange<T: DeserializeOwned>(
        &self,
        request: RequestBuilder,
        expected_status: StatusCode,
        maximum_bytes: usize,
    ) -> Result<T, SandboxProviderError> {
        let exchange = self.exchange(request, maximum_bytes).await?;
        if exchange.status != expected_status {
            return Err(classify_status(exchange.status));
        }
        exchange.validate_json_body()?;
        parse_json(&exchange.body, maximum_bytes)
    }

    fn runner_url(
        &self,
        sandbox_id: &OpenSandboxId,
        operation: &str,
    ) -> Result<Url, SandboxProviderError> {
        self.lifecycle_url(&format!(
            "sandboxes/{}/proxy/{}/v1/{operation}",
            sandbox_id.as_str(),
            SANDBOX_RUNNER_PORT
        ))
    }

    async fn observe_internal(
        &self,
        sandbox_id: &OpenSandboxId,
    ) -> Result<OpenSandboxObservationV1, SandboxProviderError> {
        let url = self.lifecycle_url(&format!("sandboxes/{}", sandbox_id.as_str()))?;
        let exchange = self
            .exchange(self.request(Method::GET, url), MAX_LIFECYCLE_RESPONSE_BYTES)
            .await?;
        let present = match exchange.status {
            StatusCode::OK => {
                exchange.validate_json_body()?;
                let sandbox: VendorSandbox =
                    parse_json(&exchange.body, MAX_LIFECYCLE_RESPONSE_BYTES)?;
                if sandbox.id != sandbox_id.as_str() {
                    return Err(SandboxProviderError::InvalidResponse);
                }
                true
            }
            StatusCode::NOT_FOUND => false,
            status => return Err(classify_status(status)),
        };
        OpenSandboxObservationV1 {
            schema_version: SANDBOX_CONTRACT_SCHEMA_VERSION,
            sandbox_id: sandbox_id.clone(),
            present,
            observed_at: Utc::now(),
            observation_digest: zero_digest(),
        }
        .seal()
        .map_err(|_| SandboxProviderError::InvalidResponse)
    }
}

#[async_trait]
impl OpenSandboxProvider for OpenSandboxHttpClient {
    async fn create_candidate(
        &self,
        request: OpenSandboxCreateV1,
    ) -> Result<SandboxCandidateV1, SandboxProviderError> {
        request
            .validate()
            .map_err(|_| SandboxProviderError::InvalidResponse)?;
        let runner_config = request
            .runner_config
            .canonical_json()
            .map_err(|_| SandboxProviderError::InvalidResponse)?;
        let runner_config_digest = request
            .runner_config
            .canonical_digest()
            .map_err(|_| SandboxProviderError::InvalidResponse)?;
        let expected_metadata = request.metadata.clone();
        let expected_entrypoint = request.entrypoint.clone();
        let body = VendorCreateRequest {
            image: VendorImageRequest {
                uri: request.image_uri,
            },
            timeout: request.ttl_seconds,
            resource_limits: VendorResourceLimits::from_limits(&request.resource_limits),
            resource_requests: VendorResourceLimits::from_limits(&request.resource_limits),
            env: BTreeMap::from([
                (SANDBOX_RUNNER_CONFIG_ENV.to_owned(), runner_config),
                (
                    SANDBOX_RUNNER_CONFIG_DIGEST_ENV.to_owned(),
                    runner_config_digest.to_string(),
                ),
            ]),
            metadata: encode_metadata(&request.metadata)?,
            entrypoint: request.entrypoint,
            secure_access: false,
        };
        let bytes = canonical_body(&body)?;
        let url = self.lifecycle_url("sandboxes")?;
        let response: VendorCreateResponse = self
            .json_exchange(
                self.request(Method::POST, url)
                    .header(header::CONTENT_TYPE, "application/json")
                    .header(header::CONTENT_LENGTH, bytes.len())
                    .body(bytes),
                StatusCode::ACCEPTED,
                MAX_LIFECYCLE_RESPONSE_BYTES,
            )
            .await?;
        if response.status.state != VendorSandboxState::Running
            || response.entrypoint != expected_entrypoint
            || decode_metadata(&response.metadata)? != expected_metadata
        {
            return Err(SandboxProviderError::InvalidResponse);
        }
        Ok(SandboxCandidateV1 {
            schema_version: SANDBOX_CONTRACT_SCHEMA_VERSION,
            sandbox_id: OpenSandboxId::parse(response.id)
                .map_err(|_| SandboxProviderError::InvalidResponse)?,
            metadata: expected_metadata,
            observed_at: Utc::now(),
        })
    }

    async fn list_candidates(
        &self,
        token_digest: Sha256Digest,
        cursor: CandidateCursorV1,
    ) -> Result<BoundedCandidatePageV1, SandboxProviderError> {
        if cursor.schema_version != SANDBOX_CONTRACT_SCHEMA_VERSION {
            return Err(SandboxProviderError::InvalidResponse);
        }
        let page = decode_cursor(&cursor)?;
        let metadata_filter = format!("{METADATA_PROVISIONING}={}", encode_digest(&token_digest)?);
        let mut url = self.lifecycle_url("sandboxes")?;
        url.query_pairs_mut()
            .append_pair("metadata", &metadata_filter)
            .append_pair("page", &page.to_string())
            .append_pair("pageSize", &self.config.candidate_page_items.to_string());
        let response: VendorListResponse = self
            .json_exchange(
                self.request(Method::GET, url),
                StatusCode::OK,
                MAX_LIFECYCLE_RESPONSE_BYTES,
            )
            .await?;
        response.pagination.validate(
            page,
            self.config.candidate_page_items,
            response.items.len(),
        )?;
        let mut items = Vec::with_capacity(response.items.len());
        for sandbox in response.items {
            let metadata = decode_metadata(&sandbox.metadata)?;
            if metadata.provisioning_token_digest != token_digest {
                return Err(SandboxProviderError::InvalidResponse);
            }
            items.push(SandboxCandidateV1 {
                schema_version: SANDBOX_CONTRACT_SCHEMA_VERSION,
                sandbox_id: OpenSandboxId::parse(sandbox.id)
                    .map_err(|_| SandboxProviderError::InvalidResponse)?,
                metadata,
                observed_at: Utc::now(),
            });
        }
        let next = response
            .pagination
            .has_next_page
            .then(|| CandidateCursorV1 {
                schema_version: SANDBOX_CONTRACT_SCHEMA_VERSION,
                opaque: Some((page + 1).to_string()),
            });
        Ok(BoundedCandidatePageV1 {
            schema_version: SANDBOX_CONTRACT_SCHEMA_VERSION,
            items,
            next,
        })
    }

    async fn observe(
        &self,
        sandbox_id: &OpenSandboxId,
    ) -> Result<OpenSandboxObservationV1, SandboxProviderError> {
        self.observe_internal(sandbox_id).await
    }

    async fn runner_state(
        &self,
        sandbox_id: &OpenSandboxId,
    ) -> Result<SandboxRunnerStateFrameV1, SandboxProviderError> {
        let url = self.runner_url(sandbox_id, "state")?;
        let frame: SandboxRunnerStateFrameV1 = self
            .json_exchange(self.request(Method::GET, url), StatusCode::OK, 65_536)
            .await?;
        frame
            .validate()
            .map_err(|_| SandboxProviderError::InvalidResponse)?;
        if &frame.sandbox_id != sandbox_id {
            return Err(SandboxProviderError::InvalidResponse);
        }
        Ok(frame)
    }

    async fn activate(
        &self,
        sandbox_id: &OpenSandboxId,
        frame: SandboxActivationFrameV1,
    ) -> Result<SandboxRunnerStateFrameV1, SandboxProviderError> {
        frame
            .validate_wire(MAX_SANDBOX_INPUT_BYTES)
            .map_err(|_| SandboxProviderError::InvalidResponse)?;
        let bytes = canonical_body(&frame)?;
        let maximum = bytes.len().saturating_add(65_536);
        let url = self.runner_url(sandbox_id, "activate")?;
        let response: SandboxRunnerStateFrameV1 = self
            .json_exchange(
                self.request(Method::POST, url)
                    .header(header::CONTENT_TYPE, "application/json")
                    .header(header::CONTENT_LENGTH, bytes.len())
                    .body(bytes),
                StatusCode::OK,
                maximum,
            )
            .await?;
        response
            .validate()
            .map_err(|_| SandboxProviderError::InvalidResponse)?;
        if &response.sandbox_id != sandbox_id
            || response.execution_request_digest != frame.execution_request_digest
            || !matches!(
                response.phase,
                SandboxRunnerPhaseV1::ActivationLatched
                    | SandboxRunnerPhaseV1::Started
                    | SandboxRunnerPhaseV1::Succeeded
                    | SandboxRunnerPhaseV1::Failed
                    | SandboxRunnerPhaseV1::UnknownPriorActivation
            )
        {
            return Err(SandboxProviderError::InvalidResponse);
        }
        Ok(response)
    }

    async fn read_result(
        &self,
        sandbox_id: &OpenSandboxId,
        maximum_bytes: u64,
    ) -> Result<Vec<u8>, SandboxProviderError> {
        let maximum =
            usize::try_from(maximum_bytes).map_err(|_| SandboxProviderError::InvalidResponse)?;
        let url = self.runner_url(sandbox_id, "result")?;
        let exchange = self
            .exchange(self.request(Method::GET, url), maximum)
            .await?;
        if exchange.status == StatusCode::TOO_EARLY {
            return Err(SandboxProviderError::NotReady);
        }
        if exchange.status != StatusCode::OK {
            return Err(classify_status(exchange.status));
        }
        exchange.validate_json_body()?;
        Ok(exchange.body)
    }

    async fn terminate(
        &self,
        sandbox_id: &OpenSandboxId,
    ) -> Result<OpenSandboxObservationV1, SandboxProviderError> {
        let url = self.lifecycle_url(&format!("sandboxes/{}", sandbox_id.as_str()))?;
        let exchange = self.exchange(self.request(Method::DELETE, url), 0).await?;
        match exchange.status {
            StatusCode::NO_CONTENT | StatusCode::NOT_FOUND => {}
            status => return Err(classify_status(status)),
        }
        if !exchange.body.is_empty() {
            return Err(SandboxProviderError::InvalidResponse);
        }
        self.observe_internal(sandbox_id).await
    }

    async fn prove_absent(
        &self,
        sandbox_id: &OpenSandboxId,
    ) -> Result<OpenSandboxObservationV1, SandboxProviderError> {
        self.observe_internal(sandbox_id).await
    }
}

#[derive(Debug)]
struct HttpExchange {
    status: StatusCode,
    content_type: Option<String>,
    declared_length: Option<u64>,
    body: Vec<u8>,
}

impl HttpExchange {
    fn validate_json_body(&self) -> Result<(), SandboxProviderError> {
        if self.body.is_empty()
            || self.declared_length.is_none()
            || self.content_type.as_deref().map(media_type) != Some("application/json")
        {
            return Err(SandboxProviderError::InvalidResponse);
        }
        Ok(())
    }
}

fn media_type(value: &str) -> &str {
    value.split(';').next().unwrap_or("").trim()
}

fn classify_reqwest_error(error: reqwest::Error) -> SandboxProviderError {
    if error.is_timeout() {
        SandboxProviderError::Timeout
    } else {
        SandboxProviderError::Unavailable
    }
}

fn classify_status(status: StatusCode) -> SandboxProviderError {
    match status {
        StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => SandboxProviderError::Unauthorized,
        StatusCode::CONFLICT => SandboxProviderError::Conflict,
        StatusCode::TOO_MANY_REQUESTS => SandboxProviderError::Capacity,
        StatusCode::REQUEST_TIMEOUT | StatusCode::GATEWAY_TIMEOUT => SandboxProviderError::Timeout,
        status if status.is_server_error() => SandboxProviderError::Unavailable,
        _ => SandboxProviderError::InvalidResponse,
    }
}

fn canonical_body<T: Serialize>(value: &T) -> Result<Vec<u8>, SandboxProviderError> {
    let value = serde_json::to_value(value).map_err(|_| SandboxProviderError::InvalidResponse)?;
    canonical_json(&value).map_err(|_| SandboxProviderError::InvalidResponse)
}

fn parse_json<T: DeserializeOwned>(
    bytes: &[u8],
    maximum_bytes: usize,
) -> Result<T, SandboxProviderError> {
    let value = parse_strict_json(
        bytes,
        JsonLimits {
            max_bytes: maximum_bytes,
            max_depth: 32,
            max_items_per_array: 256,
            max_properties_per_object: 64,
            max_string_bytes: 65_536,
        },
    )
    .map_err(|_| SandboxProviderError::InvalidResponse)?;
    serde_json::from_value(value).map_err(|_| SandboxProviderError::InvalidResponse)
}

fn encode_metadata(
    metadata: &SandboxCandidateMetadataV1,
) -> Result<BTreeMap<String, String>, SandboxProviderError> {
    metadata
        .validate_shape()
        .map_err(|_| SandboxProviderError::InvalidResponse)?;
    Ok(BTreeMap::from([
        (METADATA_SCHEMA.to_owned(), METADATA_SCHEMA_VALUE.to_owned()),
        (METADATA_TENANT.to_owned(), metadata.tenant_id.to_string()),
        (METADATA_JOB.to_owned(), metadata.job_id.to_string()),
        (
            METADATA_ATTEMPT.to_owned(),
            metadata.physical_attempt.to_string(),
        ),
        (
            METADATA_PROVISIONING.to_owned(),
            encode_digest(&metadata.provisioning_token_digest)?,
        ),
        (
            METADATA_REQUEST.to_owned(),
            encode_digest(&metadata.execution_request_digest)?,
        ),
        (
            METADATA_RUNTIME.to_owned(),
            encode_digest(&metadata.runtime_contract_digest)?,
        ),
        (
            METADATA_PROFILE.to_owned(),
            encode_digest(&metadata.profile_deployment_digest)?,
        ),
        (
            METADATA_NETWORK.to_owned(),
            match metadata.network_mode {
                SandboxNetworkMode::Disabled => "disabled",
                SandboxNetworkMode::Direct => "direct",
            }
            .to_owned(),
        ),
    ]))
}

fn decode_metadata(
    metadata: &BTreeMap<String, String>,
) -> Result<SandboxCandidateMetadataV1, SandboxProviderError> {
    if metadata.len() != 9
        || metadata.get(METADATA_SCHEMA).map(String::as_str) != Some(METADATA_SCHEMA_VALUE)
    {
        return Err(SandboxProviderError::InvalidResponse);
    }
    let get = |key: &str| {
        metadata
            .get(key)
            .map(String::as_str)
            .ok_or(SandboxProviderError::InvalidResponse)
    };
    let network_mode = match get(METADATA_NETWORK)? {
        "disabled" => SandboxNetworkMode::Disabled,
        "direct" => SandboxNetworkMode::Direct,
        _ => return Err(SandboxProviderError::InvalidResponse),
    };
    let decoded = SandboxCandidateMetadataV1 {
        schema_version: SANDBOX_CONTRACT_SCHEMA_VERSION,
        tenant_id: get(METADATA_TENANT)?
            .parse::<ResourceId>()
            .map_err(|_| SandboxProviderError::InvalidResponse)?,
        job_id: get(METADATA_JOB)?
            .parse::<ResourceId>()
            .map_err(|_| SandboxProviderError::InvalidResponse)?,
        physical_attempt: get(METADATA_ATTEMPT)?
            .parse::<u32>()
            .map_err(|_| SandboxProviderError::InvalidResponse)?,
        provisioning_token_digest: decode_digest(get(METADATA_PROVISIONING)?)?,
        execution_request_digest: decode_digest(get(METADATA_REQUEST)?)?,
        runtime_contract_digest: decode_digest(get(METADATA_RUNTIME)?)?,
        profile_deployment_digest: decode_digest(get(METADATA_PROFILE)?)?,
        network_mode,
    };
    decoded
        .validate_shape()
        .map_err(|_| SandboxProviderError::InvalidResponse)?;
    Ok(decoded)
}

fn encode_digest(digest: &Sha256Digest) -> Result<String, SandboxProviderError> {
    let hex = digest
        .as_str()
        .strip_prefix("sha256:")
        .ok_or(SandboxProviderError::InvalidResponse)?;
    let mut bytes = Vec::with_capacity(32);
    for pair in hex.as_bytes().chunks_exact(2) {
        let pair = std::str::from_utf8(pair).map_err(|_| SandboxProviderError::InvalidResponse)?;
        bytes
            .push(u8::from_str_radix(pair, 16).map_err(|_| SandboxProviderError::InvalidResponse)?);
    }
    Ok(format!("v1-{}", URL_SAFE_NO_PAD.encode(bytes)))
}

fn decode_digest(value: &str) -> Result<Sha256Digest, SandboxProviderError> {
    let encoded = value
        .strip_prefix("v1-")
        .ok_or(SandboxProviderError::InvalidResponse)?;
    let bytes = URL_SAFE_NO_PAD
        .decode(encoded)
        .map_err(|_| SandboxProviderError::InvalidResponse)?;
    if bytes.len() != 32 {
        return Err(SandboxProviderError::InvalidResponse);
    }
    let mut value = String::with_capacity(71);
    value.push_str("sha256:");
    for byte in bytes {
        use fmt::Write as _;
        write!(&mut value, "{byte:02x}").map_err(|_| SandboxProviderError::InvalidResponse)?;
    }
    Sha256Digest::from_str(&value).map_err(|_| SandboxProviderError::InvalidResponse)
}

fn decode_cursor(cursor: &CandidateCursorV1) -> Result<u32, SandboxProviderError> {
    match cursor.opaque.as_deref() {
        None => Ok(1),
        Some(value)
            if !value.is_empty()
                && value.len() <= 7
                && value.bytes().all(|byte| byte.is_ascii_digit()) =>
        {
            let page = value
                .parse::<u32>()
                .map_err(|_| SandboxProviderError::InvalidResponse)?;
            if (1..=MAX_PAGE_NUMBER).contains(&page) {
                Ok(page)
            } else {
                Err(SandboxProviderError::InvalidResponse)
            }
        }
        Some(_) => Err(SandboxProviderError::InvalidResponse),
    }
}

fn zero_digest() -> Sha256Digest {
    format!("sha256:{}", "0".repeat(64))
        .parse()
        .expect("zero digest has a valid shape")
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct VendorCreateRequest {
    image: VendorImageRequest,
    timeout: u32,
    resource_limits: VendorResourceLimits,
    resource_requests: VendorResourceLimits,
    env: BTreeMap<String, String>,
    metadata: BTreeMap<String, String>,
    entrypoint: Vec<String>,
    secure_access: bool,
}

#[derive(Debug, Serialize)]
#[serde(deny_unknown_fields)]
struct VendorImageRequest {
    uri: String,
}

#[derive(Debug, Serialize)]
#[serde(transparent)]
struct VendorResourceLimits(BTreeMap<String, String>);

impl VendorResourceLimits {
    fn from_limits(
        limits: &insight_platform_sandbox::opensandbox::SandboxResourceLimitsV1,
    ) -> Self {
        Self(BTreeMap::from([
            ("cpu".to_owned(), format!("{}m", limits.cpu_millicores)),
            (
                "memory".to_owned(),
                format!("{}Mi", limits.memory_mebibytes),
            ),
            (
                "ephemeral-storage".to_owned(),
                limits.ephemeral_storage_bytes.to_string(),
            ),
        ]))
    }
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct VendorCreateResponse {
    id: String,
    status: VendorSandboxStatus,
    #[serde(default)]
    metadata: BTreeMap<String, String>,
    #[serde(default)]
    extensions: BTreeMap<String, String>,
    #[serde(default)]
    platform: Option<VendorPlatform>,
    #[serde(default)]
    expires_at: Option<DateTime<Utc>>,
    created_at: DateTime<Utc>,
    entrypoint: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct VendorListResponse {
    items: Vec<VendorSandbox>,
    pagination: VendorPagination,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct VendorPagination {
    page: u32,
    page_size: u8,
    total_items: u64,
    total_pages: u32,
    has_next_page: bool,
}

impl VendorPagination {
    fn validate(
        &self,
        expected_page: u32,
        expected_page_size: u8,
        actual_items: usize,
    ) -> Result<(), SandboxProviderError> {
        let pages_consistent = if self.total_items == 0 {
            self.total_pages == 0 && !self.has_next_page
        } else {
            self.total_pages >= 1
                && self.page <= self.total_pages
                && self.has_next_page == (self.page < self.total_pages)
        };
        if self.page != expected_page
            || self.page_size != expected_page_size
            || actual_items > usize::from(expected_page_size)
            || u64::try_from(actual_items).map_or(true, |items| items > self.total_items)
            || !pages_consistent
        {
            return Err(SandboxProviderError::InvalidResponse);
        }
        Ok(())
    }
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct VendorSandbox {
    id: String,
    #[serde(default)]
    image: Option<VendorImage>,
    #[serde(default)]
    snapshot_id: Option<String>,
    #[serde(default)]
    platform: Option<VendorPlatform>,
    status: VendorSandboxStatus,
    #[serde(default)]
    metadata: BTreeMap<String, String>,
    #[serde(default)]
    extensions: BTreeMap<String, String>,
    #[serde(default)]
    allocation: Option<VendorAllocation>,
    entrypoint: Vec<String>,
    #[serde(default)]
    expires_at: Option<DateTime<Utc>>,
    created_at: DateTime<Utc>,
    #[serde(default)]
    updated_at: Option<DateTime<Utc>>,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct VendorImage {
    uri: String,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct VendorPlatform {
    os: String,
    arch: String,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct VendorAllocation {
    mode: String,
    pool_ref: String,
    state: String,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct VendorSandboxStatus {
    state: VendorSandboxState,
    #[serde(default)]
    reason: Option<String>,
    #[serde(default)]
    message: Option<String>,
    #[serde(default)]
    last_transition_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
enum VendorSandboxState {
    Pending,
    Allocated,
    Running,
    Pausing,
    Paused,
    Resuming,
    Stopping,
    Terminated,
    Failed,
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::{to_bytes, Body},
        extract::{Request, State},
        http::Response,
        routing::any,
        Router,
    };
    use chrono::TimeZone as _;
    use insight_platform_contracts::{DataClassification, ResourceId, ResourceKind};
    use insight_platform_sandbox::opensandbox::{
        OpaqueActivationToken, RunnerBootId, SandboxExecutionRequestV1, SandboxPhysicalEvidenceV1,
        SandboxProvisioningTokenV1, SandboxResourceLimitsV1, SandboxRunnerConfigV1,
        SandboxRunnerOutcomeV1, SandboxRunnerResultFrameV1, OPENSANDBOX_ID_ENV,
    };
    use serde_json::{json, Value};
    use std::sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    };
    use tokio::sync::Mutex;
    use uuid::Uuid;

    const TEST_API_KEY: &str = "test-opensandbox-api-key-00000001";

    #[derive(Debug, Clone)]
    struct RecordedRequest {
        method: Method,
        path: String,
        query: Option<String>,
        body: Vec<u8>,
    }

    #[derive(Clone)]
    struct MockState {
        records: Arc<Mutex<Vec<RecordedRequest>>>,
        create_metadata: Arc<Mutex<Option<Value>>>,
        create_entrypoint: Arc<Mutex<Option<Value>>>,
        present: Arc<AtomicBool>,
        unexpected_get_field: Arc<AtomicBool>,
        result_ready: Arc<AtomicBool>,
        runner_state: Arc<Mutex<SandboxRunnerStateFrameV1>>,
        result: Arc<Vec<u8>>,
    }

    async fn mock_handler(State(state): State<MockState>, request: Request) -> Response<Body> {
        let method = request.method().clone();
        let path = request.uri().path().to_owned();
        let query = request.uri().query().map(str::to_owned);
        let authorized = request
            .headers()
            .get(API_KEY_HEADER)
            .and_then(|value| value.to_str().ok())
            == Some(TEST_API_KEY);
        let body = to_bytes(request.into_body(), 2 * 1024 * 1024)
            .await
            .unwrap()
            .to_vec();
        state.records.lock().await.push(RecordedRequest {
            method: method.clone(),
            path: path.clone(),
            query,
            body: body.clone(),
        });
        if !authorized {
            return json_response(
                StatusCode::UNAUTHORIZED,
                json!({"code":"UNAUTHORIZED","message":"denied"}),
            );
        }

        match (method, path.as_str()) {
            (Method::POST, "/v1/sandboxes") => {
                let value: Value = serde_json::from_slice(&body).unwrap();
                *state.create_metadata.lock().await = value.get("metadata").cloned();
                *state.create_entrypoint.lock().await = value.get("entrypoint").cloned();
                state.present.store(true, Ordering::SeqCst);
                json_response(
                    StatusCode::ACCEPTED,
                    json!({
                        "id":"sandbox-one",
                        "status":{"state":"Running"},
                        "metadata":value["metadata"],
                        "extensions":{},
                        "expiresAt":"2033-05-18T03:33:20Z",
                        "createdAt":"2033-05-18T03:32:20Z",
                        "entrypoint":value["entrypoint"]
                    }),
                )
            }
            (Method::GET, "/v1/sandboxes") => {
                let metadata = state
                    .create_metadata
                    .lock()
                    .await
                    .clone()
                    .unwrap_or_else(|| json!({}));
                let entrypoint = state
                    .create_entrypoint
                    .lock()
                    .await
                    .clone()
                    .unwrap_or_else(|| json!(["/runner"]));
                json_response(
                    StatusCode::OK,
                    json!({
                        "items":[{
                            "id":"sandbox-one",
                            "status":{"state":"Running"},
                            "metadata":metadata,
                            "extensions":{},
                            "entrypoint":entrypoint,
                            "expiresAt":"2033-05-18T03:33:20Z",
                            "createdAt":"2033-05-18T03:32:20Z",
                            "updatedAt":"2033-05-18T03:32:30Z"
                        }],
                        "pagination":{
                            "page":1,
                            "pageSize":4,
                            "totalItems":1,
                            "totalPages":1,
                            "hasNextPage":false
                        }
                    }),
                )
            }
            (Method::GET, "/v1/sandboxes/sandbox-one") => {
                if !state.present.load(Ordering::SeqCst) {
                    return json_response(
                        StatusCode::NOT_FOUND,
                        json!({"code":"NOT_FOUND","message":"absent"}),
                    );
                }
                let metadata = state
                    .create_metadata
                    .lock()
                    .await
                    .clone()
                    .unwrap_or_else(|| json!({}));
                let mut value = json!({
                    "id":"sandbox-one",
                    "status":{"state":"Running"},
                    "metadata":metadata,
                    "extensions":{},
                    "entrypoint":["/usr/local/bin/platform-sandbox-runner"],
                    "createdAt":"2033-05-18T03:32:20Z"
                });
                if state.unexpected_get_field.load(Ordering::SeqCst) {
                    value["providerLeak"] = Value::Bool(true);
                }
                json_response(StatusCode::OK, value)
            }
            (Method::DELETE, "/v1/sandboxes/sandbox-one") => {
                state.present.store(false, Ordering::SeqCst);
                Response::builder()
                    .status(StatusCode::NO_CONTENT)
                    .body(Body::empty())
                    .unwrap()
            }
            (Method::GET, "/v1/sandboxes/sandbox-one/proxy/18080/v1/state") => json_response(
                StatusCode::OK,
                serde_json::to_value(state.runner_state.lock().await.clone()).unwrap(),
            ),
            (Method::POST, "/v1/sandboxes/sandbox-one/proxy/18080/v1/activate") => {
                let activation: SandboxActivationFrameV1 = serde_json::from_slice(&body).unwrap();
                let current = state.runner_state.lock().await.clone();
                let next = SandboxRunnerStateFrameV1 {
                    magic: String::new(),
                    schema_version: SANDBOX_CONTRACT_SCHEMA_VERSION,
                    sandbox_id: current.sandbox_id,
                    boot_id: activation.boot_id,
                    execution_request_digest: activation.execution_request_digest,
                    phase: SandboxRunnerPhaseV1::ActivationLatched,
                    frame_digest: zero_digest(),
                }
                .seal()
                .unwrap();
                *state.runner_state.lock().await = next.clone();
                json_response(StatusCode::OK, serde_json::to_value(next).unwrap())
            }
            (Method::GET, "/v1/sandboxes/sandbox-one/proxy/18080/v1/result") => {
                if !state.result_ready.load(Ordering::SeqCst) {
                    return json_response(
                        StatusCode::TOO_EARLY,
                        json!({"code":"sandbox_result_not_ready"}),
                    );
                }
                Response::builder()
                    .status(StatusCode::OK)
                    .header(header::CONTENT_TYPE, "application/json")
                    .header(header::CONTENT_LENGTH, state.result.len())
                    .body(Body::from(state.result.as_ref().clone()))
                    .unwrap()
            }
            _ => json_response(
                StatusCode::NOT_FOUND,
                json!({"code":"NOT_FOUND","message":"missing"}),
            ),
        }
    }

    fn json_response(status: StatusCode, value: Value) -> Response<Body> {
        let body = serde_json::to_vec(&value).unwrap();
        Response::builder()
            .status(status)
            .header(header::CONTENT_TYPE, "application/json")
            .header(header::CONTENT_LENGTH, body.len())
            .body(Body::from(body))
            .unwrap()
    }

    fn id(kind: ResourceKind, sequence: u128) -> ResourceId {
        let raw = (sequence & ((1_u128 << 74) - 1)) | (7_u128 << 76) | (2_u128 << 62);
        ResourceId::from_uuid_v7(kind, Uuid::from_u128(raw)).unwrap()
    }

    fn digest(character: char) -> Sha256Digest {
        format!("sha256:{}", character.to_string().repeat(64))
            .parse()
            .unwrap()
    }

    fn limits() -> SandboxResourceLimitsV1 {
        SandboxResourceLimitsV1 {
            maximum_input_bytes: 1_048_576,
            maximum_output_bytes: 1_048_576,
            cpu_millicores: 500,
            memory_mebibytes: 512,
            pids: 64,
            ephemeral_storage_bytes: 67_108_864,
            wall_milliseconds: 30_000,
            cleanup_milliseconds: 10_000,
        }
    }

    fn execution_request() -> SandboxExecutionRequestV1 {
        SandboxExecutionRequestV1 {
            schema_version: SANDBOX_CONTRACT_SCHEMA_VERSION,
            tenant_id: id(ResourceKind::Tenant, 1),
            invocation_id: id(ResourceKind::CapabilityInvocation, 2),
            job_id: id(ResourceKind::Job, 3),
            lease_generation: 1,
            physical_attempt: 1,
            worker_process_generation_id: id(ResourceKind::WorkerProcessGeneration, 7),
            package_version_id: id(ResourceKind::SandboxPackageRevision, 4),
            image_uri: format!("registry.invalid/package@sha256:{}", "a".repeat(64)),
            runtime_version_id: id(ResourceKind::SandboxRuntimeRevision, 5),
            runtime_contract_digest: digest('d'),
            sandbox_profile_deployment_id: id(ResourceKind::SandboxProfileDeployment, 6),
            profile_deployment_digest: digest('e'),
            runner_argv: vec!["/usr/local/bin/platform-sandbox-runner".to_owned()],
            package_argv: vec!["/opt/insight/package".to_owned()],
            input_value_id: id(ResourceKind::RunValue, 8),
            output_value_id: id(ResourceKind::RunValue, 9),
            classification: DataClassification::Internal,
            input: json!({"question":"answer"}),
            input_schema_digest: digest('b'),
            input_digest: zero_digest(),
            output_schema_digest: digest('c'),
            network_mode: SandboxNetworkMode::Direct,
            limits: limits(),
            deadline_at: Utc.timestamp_opt(2_000_000_000, 0).unwrap(),
            trace: insight_platform_contracts::TraceIdentityV1::generate(),
            request_digest: zero_digest(),
        }
        .seal()
        .unwrap()
    }

    fn create_request() -> (OpenSandboxCreateV1, SandboxPhysicalEvidenceV1) {
        let request = execution_request();
        let evidence = SandboxPhysicalEvidenceV1::begin(
            &request,
            OpaqueActivationToken::parse("1".repeat(64)).unwrap(),
        )
        .unwrap();
        let metadata = SandboxCandidateMetadataV1 {
            schema_version: SANDBOX_CONTRACT_SCHEMA_VERSION,
            tenant_id: request.tenant_id.clone(),
            job_id: request.job_id.clone(),
            physical_attempt: request.physical_attempt,
            provisioning_token_digest: SandboxProvisioningTokenV1::from_request(&request)
                .digest()
                .unwrap(),
            execution_request_digest: request.request_digest.clone(),
            runtime_contract_digest: request.runtime_contract_digest.clone(),
            profile_deployment_digest: request.profile_deployment_digest.clone(),
            network_mode: request.network_mode,
        };
        let runner_config = SandboxRunnerConfigV1::from_request(
            &request,
            evidence.activation_token_digest.clone(),
            8_192,
        )
        .unwrap();
        (
            OpenSandboxCreateV1 {
                schema_version: SANDBOX_CONTRACT_SCHEMA_VERSION,
                image_uri: request.image_uri,
                entrypoint: request.runner_argv,
                metadata,
                runner_config,
                resource_limits: request.limits,
                ttl_seconds: 60,
            },
            evidence,
        )
    }

    async fn fixture() -> (OpenSandboxHttpClient, MockState) {
        let (create, _) = create_request();
        let sandbox_id = OpenSandboxId::parse("sandbox-one").unwrap();
        let boot_id = RunnerBootId::parse("boot-one").unwrap();
        let runner_state = SandboxRunnerStateFrameV1 {
            magic: String::new(),
            schema_version: SANDBOX_CONTRACT_SCHEMA_VERSION,
            sandbox_id,
            boot_id: boot_id.clone(),
            execution_request_digest: create.runner_config.execution_request_digest.clone(),
            phase: SandboxRunnerPhaseV1::Armed,
            frame_digest: zero_digest(),
        }
        .seal()
        .unwrap();
        let result = SandboxRunnerResultFrameV1 {
            magic: String::new(),
            schema_version: SANDBOX_CONTRACT_SCHEMA_VERSION,
            execution_request_digest: create.runner_config.execution_request_digest,
            boot_id,
            result: SandboxRunnerOutcomeV1::Succeeded {
                output: json!({"ok":true}),
                output_schema_digest: digest('c'),
                output_digest: zero_digest(),
                declared_output_bytes: 0,
            },
            frame_digest: zero_digest(),
        }
        .seal()
        .unwrap()
        .canonical_bytes()
        .unwrap();
        let state = MockState {
            records: Arc::new(Mutex::new(Vec::new())),
            create_metadata: Arc::new(Mutex::new(None)),
            create_entrypoint: Arc::new(Mutex::new(None)),
            present: Arc::new(AtomicBool::new(false)),
            unexpected_get_field: Arc::new(AtomicBool::new(false)),
            result_ready: Arc::new(AtomicBool::new(false)),
            runner_state: Arc::new(Mutex::new(runner_state)),
            result: Arc::new(result),
        };
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let app = Router::new()
            .fallback(any(mock_handler))
            .with_state(state.clone());
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let client = OpenSandboxHttpClient::new(OpenSandboxHttpClientConfig {
            lifecycle_base_url: Url::parse(&format!("http://{address}/v1/")).unwrap(),
            api_key: OpenSandboxApiKey::parse(TEST_API_KEY).unwrap(),
            request_timeout_milliseconds: 5_000,
            connect_timeout_milliseconds: 1_000,
            candidate_page_items: 4,
        })
        .unwrap();
        (client, state)
    }

    #[tokio::test]
    async fn lifecycle_mapping_is_closed_and_response_loss_discovers_inert_candidate() {
        let (client, state) = fixture().await;
        let (create, _) = create_request();
        let token_digest = create.metadata.provisioning_token_digest.clone();
        let candidate = client.create_candidate(create).await.unwrap();
        assert_eq!(candidate.sandbox_id.as_str(), "sandbox-one");

        let page = client
            .list_candidates(
                token_digest,
                CandidateCursorV1 {
                    schema_version: SANDBOX_CONTRACT_SCHEMA_VERSION,
                    opaque: None,
                },
            )
            .await
            .unwrap();
        assert_eq!(page.items.len(), 1);
        assert!(page.next.is_none());

        let records = state.records.lock().await;
        let create_record = records
            .iter()
            .find(|record| record.method == Method::POST && record.path == "/v1/sandboxes")
            .unwrap();
        let body: Value = serde_json::from_slice(&create_record.body).unwrap();
        assert_eq!(body["secureAccess"], Value::Bool(false));
        assert!(body.get("networkPolicy").is_none());
        assert!(body.get("volumes").is_none());
        assert!(body.get("extensions").is_none());
        assert!(body["env"].get(OPENSANDBOX_ID_ENV).is_none());
        assert!(!String::from_utf8_lossy(&create_record.body).contains("answer"));
        assert!(body["metadata"]
            .as_object()
            .unwrap()
            .values()
            .all(|value| value.as_str().unwrap().len() <= 63));
        let list_record = records
            .iter()
            .find(|record| record.method == Method::GET && record.path == "/v1/sandboxes")
            .unwrap();
        let query = list_record.query.as_deref().unwrap();
        assert!(query.contains("page=1"));
        assert!(query.contains("pageSize=4"));
    }

    #[tokio::test]
    async fn fixed_runner_proxy_activation_result_and_absence_are_validated() {
        let (client, state) = fixture().await;
        let (create, evidence) = create_request();
        let sandbox_id = client
            .create_candidate(create.clone())
            .await
            .unwrap()
            .sandbox_id;
        let armed = client.runner_state(&sandbox_id).await.unwrap();
        assert_eq!(armed.phase, SandboxRunnerPhaseV1::Armed);
        let request = execution_request();
        let frame = SandboxActivationFrameV1 {
            magic: String::new(),
            schema_version: SANDBOX_CONTRACT_SCHEMA_VERSION,
            activation_token: evidence.activation_token,
            boot_id: armed.boot_id.clone(),
            execution_request_digest: request.request_digest,
            input_schema_digest: request.input_schema_digest,
            input_digest: request.input_digest,
            declared_input_bytes: 0,
            input: request.input,
            frame_digest: zero_digest(),
        }
        .seal()
        .unwrap();
        let activated = client.activate(&sandbox_id, frame).await.unwrap();
        assert_eq!(activated.phase, SandboxRunnerPhaseV1::ActivationLatched);
        assert_eq!(
            client.read_result(&sandbox_id, 1_114_112).await,
            Err(SandboxProviderError::NotReady)
        );
        state.result_ready.store(true, Ordering::SeqCst);
        assert_eq!(
            client.read_result(&sandbox_id, 16).await,
            Err(SandboxProviderError::InvalidResponse)
        );
        let result = client.read_result(&sandbox_id, 1_114_112).await.unwrap();
        assert_eq!(result, client_result(&create, &armed.boot_id));

        let observation = client.terminate(&sandbox_id).await.unwrap();
        assert!(!observation.present);
        observation.validate().unwrap();
        assert!(!client.prove_absent(&sandbox_id).await.unwrap().present);
    }

    fn client_result(create: &OpenSandboxCreateV1, boot_id: &RunnerBootId) -> Vec<u8> {
        SandboxRunnerResultFrameV1 {
            magic: String::new(),
            schema_version: SANDBOX_CONTRACT_SCHEMA_VERSION,
            execution_request_digest: create.runner_config.execution_request_digest.clone(),
            boot_id: boot_id.clone(),
            result: SandboxRunnerOutcomeV1::Succeeded {
                output: json!({"ok":true}),
                output_schema_digest: digest('c'),
                output_digest: zero_digest(),
                declared_output_bytes: 0,
            },
            frame_digest: zero_digest(),
        }
        .seal()
        .unwrap()
        .canonical_bytes()
        .unwrap()
    }

    #[tokio::test]
    async fn wrong_credential_unknown_response_field_and_illegal_config_fail_closed() {
        let (client, state) = fixture().await;
        let sandbox_id = OpenSandboxId::parse("sandbox-one").unwrap();
        state.present.store(true, Ordering::SeqCst);
        state.unexpected_get_field.store(true, Ordering::SeqCst);
        assert_eq!(
            client.observe(&sandbox_id).await,
            Err(SandboxProviderError::InvalidResponse)
        );

        let wrong = OpenSandboxHttpClient::new(OpenSandboxHttpClientConfig {
            lifecycle_base_url: client.config.lifecycle_base_url.clone(),
            api_key: OpenSandboxApiKey::parse("wrong-opensandbox-api-key-0000001").unwrap(),
            request_timeout_milliseconds: 5_000,
            connect_timeout_milliseconds: 1_000,
            candidate_page_items: 4,
        })
        .unwrap();
        assert_eq!(
            wrong.observe(&sandbox_id).await,
            Err(SandboxProviderError::Unauthorized)
        );
        assert!(OpenSandboxApiKey::parse("short").is_err());
        let mut invalid = client.config.clone();
        invalid.lifecycle_base_url = Url::parse("https://public.example/v1/").unwrap();
        assert!(OpenSandboxHttpClient::new(invalid).is_err());
        assert!(decode_cursor(&CandidateCursorV1 {
            schema_version: SANDBOX_CONTRACT_SCHEMA_VERSION,
            opaque: Some("1000001".to_owned()),
        })
        .is_err());
    }
}
