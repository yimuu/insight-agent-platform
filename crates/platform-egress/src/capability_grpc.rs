use super::{
    is_public_destination_ip, parse_endpoint_host, DnsResolutionError, EgressCapacitySnapshot,
    EgressConfigurationError, EgressDnsResolver, ParsedEndpointHost, ResolvedSecretMaterial,
    SecretMaterialResolutionError, SecretMaterialResolver, MAX_DNS_ANSWERS_HARD,
    MAX_EGRESS_IN_FLIGHT_HARD, MAX_SECRET_MATERIAL_BYTES_HARD,
};
use async_trait::async_trait;
use chrono::Utc;
use http::{HeaderMap, HeaderName, HeaderValue, Request, Version};
use http_body_util::{BodyExt, Full};
use hyper::body::Bytes;
use hyper_rustls::HttpsConnectorBuilder;
use hyper_util::{
    client::legacy::{connect::dns::Name, connect::HttpConnector, Client},
    rt::TokioExecutor,
};
use insight_platform_capability_adapters::{
    CapabilityAdapterFailure, CapabilityAdapterFailureClass, CapabilityTransportCancelOutcome,
    CapabilityTransportCancelRequest, CapabilityTransportRequestIdentity, GrpcNetworkTransport,
    GrpcTransportRequest, GrpcTransportResponse, SafeGrpcMetadata, MAX_GRPC_ADAPTER_METADATA,
};
use insight_platform_contracts::{
    canonical_digest, CanonicalHttpEndpoint, CapabilityBackendKind, CapabilityBackendLimits,
    CapabilityEndpointScheme, CapabilityIdempotencyKind, Effect, ExactDeploymentRef,
    ExactSecretBindingRef, ExactVersionRef, SecretPurpose, Sha256Digest,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    future::{ready, Ready},
    io,
    net::{IpAddr, SocketAddr},
    sync::{Arc, Mutex},
    task::{Context, Poll},
    time::Duration,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::CancellationToken;
use tower::Service;

pub const MAX_INSTALLED_CAPABILITY_GRPC_ENDPOINTS: usize = 4_096;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapabilityGrpcEgressLimits {
    pub maximum_in_flight: usize,
    pub maximum_dns_answers: usize,
    pub maximum_secret_material_bytes: usize,
}

impl CapabilityGrpcEgressLimits {
    pub fn validate(self) -> Result<(), EgressConfigurationError> {
        if self.maximum_in_flight == 0
            || self.maximum_in_flight > MAX_EGRESS_IN_FLIGHT_HARD
            || self.maximum_dns_answers == 0
            || self.maximum_dns_answers > MAX_DNS_ANSWERS_HARD
            || self.maximum_secret_material_bytes == 0
            || self.maximum_secret_material_bytes > MAX_SECRET_MATERIAL_BYTES_HARD
        {
            return Err(EgressConfigurationError::InvalidLimits);
        }
        Ok(())
    }
}

impl Default for CapabilityGrpcEgressLimits {
    fn default() -> Self {
        Self {
            maximum_in_flight: 256,
            maximum_dns_answers: 16,
            maximum_secret_material_bytes: 8_192,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum InstalledGrpcCredentialInjection {
    BearerAuthorization { purpose: SecretPurpose },
    Metadata { purpose: SecretPurpose, key: String },
}

impl InstalledGrpcCredentialInjection {
    fn purpose(&self) -> &SecretPurpose {
        match self {
            Self::BearerAuthorization { purpose } | Self::Metadata { purpose, .. } => purpose,
        }
    }

    fn key(&self) -> &str {
        match self {
            Self::BearerAuthorization { .. } => "authorization",
            Self::Metadata { key, .. } => key,
        }
    }

    fn validate(&self) -> Result<(), EgressConfigurationError> {
        match self {
            Self::BearerAuthorization { .. } => Ok(()),
            Self::Metadata { key, .. } => {
                if key != &key.to_ascii_lowercase()
                    || key.ends_with("-bin")
                    || HeaderName::from_bytes(key.as_bytes()).is_err()
                    || matches!(
                        key.as_str(),
                        "authorization"
                            | "connection"
                            | "content-length"
                            | "cookie"
                            | "grpc-encoding"
                            | "grpc-message"
                            | "grpc-status"
                            | "grpc-timeout"
                            | "host"
                            | "proxy-authorization"
                            | "te"
                            | "transfer-encoding"
                    )
                {
                    return Err(EgressConfigurationError::InvalidEndpoint);
                }
                Ok(())
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstalledCapabilityGrpcEndpoint {
    pub schema_version: u32,
    pub capability_deployment: ExactDeploymentRef,
    pub backend_contract_digest: Sha256Digest,
    pub effect: Effect,
    pub idempotency_kind: CapabilityIdempotencyKind,
    pub service_name: String,
    pub method_name: String,
    pub endpoint: CanonicalHttpEndpoint,
    pub endpoint_identity_digest: Sha256Digest,
    pub network_policy: ExactVersionRef,
    pub tls_policy: ExactVersionRef,
    pub trust_policy: ExactVersionRef,
    pub secret_bindings: Vec<ExactSecretBindingRef>,
    pub credential_injections: Vec<InstalledGrpcCredentialInjection>,
    pub limits: CapabilityBackendLimits,
}

impl InstalledCapabilityGrpcEndpoint {
    pub fn validate(&self) -> Result<(), EgressConfigurationError> {
        self.capability_deployment
            .validate()
            .map_err(|_| EgressConfigurationError::InvalidEndpoint)?;
        self.endpoint
            .validate()
            .map_err(|_| EgressConfigurationError::InvalidEndpoint)?;
        self.limits
            .validate()
            .map_err(|_| EgressConfigurationError::InvalidLimits)?;
        if self.schema_version != 1
            || self.capability_deployment.resource_kind
                != insight_platform_contracts::ResourceKind::CapabilityDeployment
            || self.endpoint.scheme != CapabilityEndpointScheme::Https
            || self.endpoint.canonical_digest().as_ref() != Ok(&self.endpoint_identity_digest)
            || parse_endpoint_host(&self.endpoint.host).is_err()
            || !valid_grpc_component(&self.service_name)
            || !valid_grpc_component(&self.method_name)
        {
            return Err(EgressConfigurationError::InvalidEndpoint);
        }
        let policies = [&self.network_policy, &self.tls_policy, &self.trust_policy];
        let mut policy_ids = BTreeSet::new();
        for policy in policies {
            policy
                .validate()
                .map_err(|_| EgressConfigurationError::InvalidEndpoint)?;
            if policy.resource_kind != insight_platform_contracts::ResourceKind::PolicyRevision
                || !policy_ids.insert(policy.revision_id.clone())
            {
                return Err(EgressConfigurationError::InvalidEndpoint);
            }
        }
        let mut prior = None;
        for binding in &self.secret_bindings {
            binding
                .validate()
                .map_err(|_| EgressConfigurationError::InvalidEndpoint)?;
            let key = (&binding.purpose, &binding.secret_binding_id);
            if prior.is_some_and(|value| value >= key) {
                return Err(EgressConfigurationError::InvalidEndpoint);
            }
            prior = Some(key);
        }
        let mut purposes = BTreeSet::new();
        let mut keys = BTreeSet::new();
        for injection in &self.credential_injections {
            injection.validate()?;
            if !purposes.insert(injection.purpose().clone())
                || !keys.insert(injection.key().to_owned())
                || self
                    .secret_bindings
                    .iter()
                    .filter(|binding| binding.purpose == *injection.purpose())
                    .count()
                    != 1
            {
                return Err(EgressConfigurationError::InvalidEndpoint);
            }
        }
        if self.secret_bindings.len() != self.credential_injections.len() {
            return Err(EgressConfigurationError::InvalidEndpoint);
        }
        grpc_uri(self)?;
        Ok(())
    }

    fn matches(&self, request: &GrpcTransportRequest) -> bool {
        request.identity.backend_kind == CapabilityBackendKind::Grpc
            && request.identity.capability_deployment_id == self.capability_deployment.deployment_id
            && request.identity.capability_deployment_digest
                == self.capability_deployment.deployment_digest
            && request.backend_contract_digest == self.backend_contract_digest
            && request.effect == self.effect
            && request.idempotency_kind == self.idempotency_kind
            && request.service_name == self.service_name
            && request.method_name == self.method_name
            && request.endpoint == self.endpoint
            && request.endpoint_identity_digest == self.endpoint_identity_digest
            && request.network_policy == self.network_policy
            && request.tls_policy == self.tls_policy
            && request.trust_policy == self.trust_policy
            && request.secret_bindings == self.secret_bindings
            && request.limits == self.limits
    }
}

#[derive(Debug, Clone)]
pub struct InstalledCapabilityGrpcEndpointCatalog {
    entries: BTreeMap<
        (insight_platform_contracts::ResourceId, Sha256Digest),
        InstalledCapabilityGrpcEndpoint,
    >,
}

impl InstalledCapabilityGrpcEndpointCatalog {
    pub fn new(
        entries: Vec<InstalledCapabilityGrpcEndpoint>,
    ) -> Result<Self, EgressConfigurationError> {
        if entries.is_empty() || entries.len() > MAX_INSTALLED_CAPABILITY_GRPC_ENDPOINTS {
            return Err(EgressConfigurationError::InvalidEndpointCatalog);
        }
        let mut catalog = BTreeMap::new();
        for entry in entries {
            entry.validate()?;
            let key = (
                entry.capability_deployment.deployment_id.clone(),
                entry.capability_deployment.deployment_digest.clone(),
            );
            if catalog.insert(key, entry).is_some() {
                return Err(EgressConfigurationError::DuplicateEndpoint);
            }
        }
        Ok(Self { entries: catalog })
    }

    fn resolve(
        &self,
        request: &GrpcTransportRequest,
    ) -> Result<InstalledCapabilityGrpcEndpoint, CapabilityAdapterFailure> {
        let key = (
            request.identity.capability_deployment_id.clone(),
            request.identity.capability_deployment_digest.clone(),
        );
        self.entries
            .get(&key)
            .filter(|entry| entry.matches(request))
            .cloned()
            .ok_or_else(|| before_dispatch("capability_grpc_endpoint_not_installed", false))
    }
}

struct PinnedCapabilityGrpcRequest {
    uri: http::Uri,
    dns_host: String,
    addresses: Vec<SocketAddr>,
    headers: HeaderMap,
    framed_message: Vec<u8>,
    connect_timeout: Duration,
    first_byte_timeout: Duration,
    idle_timeout: Duration,
    total_timeout: Duration,
    maximum_response_bytes: u64,
    cancellation: CancellationToken,
    transport_evidence_digest: Sha256Digest,
    effect: Effect,
    idempotency_kind: CapabilityIdempotencyKind,
}

struct PinnedCapabilityGrpcResponse {
    status_code: u16,
    trailing_metadata: Vec<SafeGrpcMetadata>,
    message: Vec<u8>,
}

#[async_trait]
trait PinnedCapabilityGrpcTransport: Send + Sync {
    async fn unary(
        &self,
        request: PinnedCapabilityGrpcRequest,
    ) -> Result<PinnedCapabilityGrpcResponse, CapabilityAdapterFailure>;
}

#[derive(Clone)]
struct PinnedResolver {
    expected_host: String,
    addresses: Arc<Vec<SocketAddr>>,
}

impl Service<Name> for PinnedResolver {
    type Response = std::vec::IntoIter<SocketAddr>;
    type Error = io::Error;
    type Future = Ready<Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, name: Name) -> Self::Future {
        if name.as_str() != self.expected_host {
            return ready(Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "connector host does not match the pinned endpoint",
            )));
        }
        ready(Ok(self.addresses.as_ref().clone().into_iter()))
    }
}

#[derive(Default)]
struct HyperPinnedCapabilityGrpcTransport;

#[async_trait]
impl PinnedCapabilityGrpcTransport for HyperPinnedCapabilityGrpcTransport {
    async fn unary(
        &self,
        request: PinnedCapabilityGrpcRequest,
    ) -> Result<PinnedCapabilityGrpcResponse, CapabilityAdapterFailure> {
        let started_at = tokio::time::Instant::now();
        let resolver = PinnedResolver {
            expected_host: request.dns_host,
            addresses: Arc::new(request.addresses),
        };
        let mut http = HttpConnector::new_with_resolver(resolver);
        http.enforce_http(false);
        http.set_connect_timeout(Some(request.connect_timeout));
        http.set_happy_eyeballs_timeout(None);
        http.set_nodelay(true);
        let https = HttpsConnectorBuilder::new()
            .with_native_roots()
            .map_err(|_| before_dispatch("capability_grpc_tls_roots_unavailable", false))?
            .https_only()
            .enable_http2()
            .wrap_connector(http);
        let mut builder = Client::builder(TokioExecutor::new());
        builder.http2_only(true);
        builder.pool_max_idle_per_host(0);
        let client: Client<_, Full<Bytes>> = builder.build(https);
        let outbound = Request::builder()
            .method("POST")
            .uri(request.uri)
            .version(Version::HTTP_2)
            .body(Full::new(Bytes::from(request.framed_message)))
            .map_err(|_| before_dispatch("capability_grpc_request_build_failed", false))?;
        let (mut parts, body) = outbound.into_parts();
        parts.headers = request.headers;
        let outbound = Request::from_parts(parts, body);
        let response = tokio::select! {
            biased;
            _ = request.cancellation.cancelled() => {
                return Err(uncertain_after_dispatch(
                    "capability_grpc_cancelled",
                    false,
                    request.transport_evidence_digest,
                ));
            }
            response = tokio::time::timeout(request.first_byte_timeout, client.request(outbound)) => {
                match response {
                    Ok(Ok(response)) => response,
                    Ok(Err(_)) => return Err(dispatch_failure(
                        "capability_grpc_transport_failed",
                        false,
                        request.transport_evidence_digest,
                        request.effect,
                        request.idempotency_kind,
                    )),
                    Err(_) => return Err(dispatch_failure(
                        "capability_grpc_first_byte_timeout",
                        true,
                        request.transport_evidence_digest,
                        request.effect,
                        request.idempotency_kind,
                    )),
                }
            },
        };
        if response.version() != Version::HTTP_2
            || response.status().as_u16() != 200
            || response.headers().get_all("content-type").iter().count() != 1
            || response.headers().contains_key("grpc-encoding")
            || !response
                .headers()
                .get("content-type")
                .and_then(|value| value.to_str().ok())
                .is_some_and(valid_grpc_content_type)
        {
            return Err(dispatch_failure(
                "capability_grpc_invalid_http_response",
                false,
                request.transport_evidence_digest,
                request.effect,
                request.idempotency_kind,
            ));
        }
        let (parts, mut incoming) = response.into_parts();
        let initial_headers = parts.headers;
        let remaining_total = request
            .total_timeout
            .checked_sub(started_at.elapsed())
            .ok_or_else(|| {
                dispatch_failure(
                    "capability_grpc_total_timeout",
                    true,
                    request.transport_evidence_digest.clone(),
                    request.effect,
                    request.idempotency_kind,
                )
            })?;
        let maximum_framed_bytes =
            usize::try_from(request.maximum_response_bytes.saturating_add(5)).unwrap_or(usize::MAX);
        let collected = tokio::select! {
            biased;
            _ = request.cancellation.cancelled() => {
                return Err(uncertain_after_dispatch(
                    "capability_grpc_cancelled",
                    false,
                    request.transport_evidence_digest,
                ));
            }
            collected = tokio::time::timeout(remaining_total, async {
                let mut data = Vec::new();
                let mut trailers = None;
                loop {
                    let frame = tokio::time::timeout(request.idle_timeout, incoming.frame())
                        .await
                        .map_err(|_| "capability_grpc_idle_timeout")?;
                    let Some(frame) = frame else {
                        break;
                    };
                    let frame = frame.map_err(|_| "capability_grpc_response_failed")?;
                    match frame.into_data() {
                        Ok(bytes) => {
                            if trailers.is_some()
                                || data.len().saturating_add(bytes.len()) > maximum_framed_bytes
                            {
                                return Err("capability_grpc_response_too_large");
                            }
                            data.extend_from_slice(&bytes);
                        }
                        Err(frame) => {
                            let received = frame
                                .into_trailers()
                                .map_err(|_| "capability_grpc_invalid_frame")?;
                            if trailers.replace(received).is_some() {
                                return Err("capability_grpc_invalid_trailers");
                            }
                        }
                    }
                }
                Ok::<_, &'static str>((data, trailers))
            }) => {
                let result = collected
                    .map_err(|_| dispatch_failure(
                        "capability_grpc_total_timeout",
                        true,
                        request.transport_evidence_digest.clone(),
                        request.effect,
                        request.idempotency_kind,
                    ))?;
                result.map_err(|code| dispatch_failure(
                    code,
                    code.ends_with("_timeout"),
                    request.transport_evidence_digest.clone(),
                    request.effect,
                    request.idempotency_kind,
                ))?
            }
        };
        let (framed_message, trailers) = collected;
        let (status_code, trailing_metadata) =
            parse_response_status(&initial_headers, trailers.as_ref()).map_err(|code| {
                dispatch_failure(
                    code,
                    false,
                    request.transport_evidence_digest.clone(),
                    request.effect,
                    request.idempotency_kind,
                )
            })?;
        let message = if framed_message.is_empty() && status_code != 0 {
            Vec::new()
        } else {
            decode_unary_frame(&framed_message, request.maximum_response_bytes).map_err(|code| {
                dispatch_failure(
                    code,
                    false,
                    request.transport_evidence_digest.clone(),
                    request.effect,
                    request.idempotency_kind,
                )
            })?
        };
        Ok(PinnedCapabilityGrpcResponse {
            status_code,
            trailing_metadata,
            message,
        })
    }
}

struct CapabilityGrpcInFlightRegistration {
    identity: CapabilityTransportRequestIdentity,
    active: Arc<Mutex<BTreeMap<CapabilityTransportRequestIdentity, CancellationToken>>>,
    cancellation: CancellationToken,
    _permit: OwnedSemaphorePermit,
}

impl Drop for CapabilityGrpcInFlightRegistration {
    fn drop(&mut self) {
        if let Ok(mut active) = self.active.lock() {
            active.remove(&self.identity);
        }
    }
}

pub struct HyperCapabilityGrpcEgressTransport {
    catalog: InstalledCapabilityGrpcEndpointCatalog,
    secrets: Arc<dyn SecretMaterialResolver>,
    dns: Arc<dyn EgressDnsResolver>,
    transport: Arc<dyn PinnedCapabilityGrpcTransport>,
    limits: CapabilityGrpcEgressLimits,
    permits: Arc<Semaphore>,
    active: Arc<Mutex<BTreeMap<CapabilityTransportRequestIdentity, CancellationToken>>>,
}

impl HyperCapabilityGrpcEgressTransport {
    pub fn new(
        catalog: InstalledCapabilityGrpcEndpointCatalog,
        secrets: Arc<dyn SecretMaterialResolver>,
        dns: Arc<dyn EgressDnsResolver>,
        limits: CapabilityGrpcEgressLimits,
    ) -> Result<Self, EgressConfigurationError> {
        Self::with_transport(
            catalog,
            secrets,
            dns,
            Arc::new(HyperPinnedCapabilityGrpcTransport),
            limits,
        )
    }

    fn with_transport(
        catalog: InstalledCapabilityGrpcEndpointCatalog,
        secrets: Arc<dyn SecretMaterialResolver>,
        dns: Arc<dyn EgressDnsResolver>,
        transport: Arc<dyn PinnedCapabilityGrpcTransport>,
        limits: CapabilityGrpcEgressLimits,
    ) -> Result<Self, EgressConfigurationError> {
        limits.validate()?;
        Ok(Self {
            catalog,
            secrets,
            dns,
            transport,
            limits,
            permits: Arc::new(Semaphore::new(limits.maximum_in_flight)),
            active: Arc::new(Mutex::new(BTreeMap::new())),
        })
    }

    pub fn capacity_snapshot(&self) -> EgressCapacitySnapshot {
        EgressCapacitySnapshot {
            maximum_in_flight: self.limits.maximum_in_flight,
            available: self.permits.available_permits(),
        }
    }

    fn register(
        &self,
        identity: CapabilityTransportRequestIdentity,
    ) -> Result<CapabilityGrpcInFlightRegistration, CapabilityAdapterFailure> {
        let permit = self
            .permits
            .clone()
            .try_acquire_owned()
            .map_err(|_| before_dispatch("capability_grpc_capacity", true))?;
        let cancellation = CancellationToken::new();
        let mut active = self
            .active
            .lock()
            .map_err(|_| before_dispatch("capability_grpc_registry_unavailable", false))?;
        if active.contains_key(&identity) {
            return Err(before_dispatch("capability_grpc_duplicate_request", false));
        }
        active.insert(identity.clone(), cancellation.clone());
        drop(active);
        Ok(CapabilityGrpcInFlightRegistration {
            identity,
            active: self.active.clone(),
            cancellation,
            _permit: permit,
        })
    }

    async fn resolve_addresses(
        &self,
        endpoint: &CanonicalHttpEndpoint,
        cancellation: &CancellationToken,
    ) -> Result<(String, Vec<SocketAddr>), CapabilityAdapterFailure> {
        let host = parse_endpoint_host(&endpoint.host)
            .map_err(|_| before_dispatch("capability_grpc_invalid_endpoint", false))?;
        let (dns_host, mut addresses) = match host {
            ParsedEndpointHost::Address(address) => (
                match address {
                    IpAddr::V4(address) => address.to_string(),
                    IpAddr::V6(address) => address.to_string(),
                },
                vec![SocketAddr::new(address, endpoint.port)],
            ),
            ParsedEndpointHost::Name(host) => {
                let resolved = tokio::select! {
                    biased;
                    _ = cancellation.cancelled() => {
                        return Err(before_dispatch("capability_grpc_cancelled", false));
                    }
                    result = self.dns.resolve(&host, endpoint.port) => result,
                }
                .map_err(|failure| match failure {
                    DnsResolutionError::Unavailable => {
                        before_dispatch("capability_grpc_dns_unavailable", true)
                    }
                    DnsResolutionError::NoAddresses | DnsResolutionError::TooManyAddresses => {
                        before_dispatch("capability_grpc_dns_rejected", false)
                    }
                })?;
                (host, resolved)
            }
        };
        addresses.sort_unstable();
        addresses.dedup();
        if addresses.is_empty()
            || addresses.len() > self.limits.maximum_dns_answers
            || addresses.iter().any(|address| {
                address.port() != endpoint.port || !is_public_destination_ip(address.ip())
            })
        {
            return Err(before_dispatch("capability_grpc_destination_denied", false));
        }
        Ok((dns_host, addresses))
    }

    async fn resolve_metadata(
        &self,
        request: &GrpcTransportRequest,
        entry: &InstalledCapabilityGrpcEndpoint,
        cancellation: &CancellationToken,
    ) -> Result<HeaderMap, CapabilityAdapterFailure> {
        let mut headers = HeaderMap::new();
        headers.insert("content-type", HeaderValue::from_static("application/grpc"));
        headers.insert("te", HeaderValue::from_static("trailers"));
        for metadata in &request.metadata {
            metadata
                .validate()
                .map_err(|_| before_dispatch("capability_grpc_invalid_metadata", false))?;
            let name = HeaderName::from_bytes(metadata.key.as_bytes())
                .map_err(|_| before_dispatch("capability_grpc_invalid_metadata", false))?;
            let value = HeaderValue::from_str(&metadata.value)
                .map_err(|_| before_dispatch("capability_grpc_invalid_metadata", false))?;
            if headers.insert(name, value).is_some() {
                return Err(before_dispatch("capability_grpc_duplicate_metadata", false));
            }
        }
        if let Some(idempotency) = &request.idempotency {
            let name = HeaderName::from_bytes(idempotency.metadata_key.as_bytes())
                .map_err(|_| before_dispatch("capability_grpc_invalid_idempotency", false))?;
            let value = HeaderValue::from_str(idempotency.value_digest.as_str())
                .map_err(|_| before_dispatch("capability_grpc_invalid_idempotency", false))?;
            if headers.insert(name, value).is_some() {
                return Err(before_dispatch("capability_grpc_duplicate_metadata", false));
            }
        }
        for injection in &entry.credential_injections {
            let binding = request
                .secret_bindings
                .iter()
                .find(|binding| binding.purpose == *injection.purpose())
                .ok_or_else(|| before_dispatch("capability_grpc_secret_missing", false))?;
            let resolved = tokio::select! {
                biased;
                _ = cancellation.cancelled() => {
                    return Err(before_dispatch("capability_grpc_cancelled", false));
                }
                result = self.secrets.resolve(&request.identity.tenant_id, binding) => result,
            }
            .map_err(|failure| match failure {
                SecretMaterialResolutionError::Unavailable => {
                    before_dispatch("capability_grpc_secret_unavailable", true)
                }
                SecretMaterialResolutionError::NotFound
                | SecretMaterialResolutionError::Revoked
                | SecretMaterialResolutionError::InvalidEvidence => {
                    before_dispatch("capability_grpc_secret_rejected", false)
                }
            })?;
            if !resolved.validate_for(binding, self.limits.maximum_secret_material_bytes) {
                return Err(before_dispatch("capability_grpc_secret_rejected", false));
            }
            insert_credential(&mut headers, injection, &resolved)?;
        }
        Ok(headers)
    }
}

#[async_trait]
impl GrpcNetworkTransport for HyperCapabilityGrpcEgressTransport {
    async fn unary(
        &self,
        request: GrpcTransportRequest,
    ) -> Result<GrpcTransportResponse, CapabilityAdapterFailure> {
        request
            .validate_at(Utc::now())
            .map_err(|_| before_dispatch("capability_grpc_invalid_request", false))?;
        let entry = self.catalog.resolve(&request)?;
        let registration = self.register(request.identity.clone())?;
        let (dns_host, addresses) = self
            .resolve_addresses(&entry.endpoint, &registration.cancellation)
            .await?;
        let mut headers = self
            .resolve_metadata(&request, &entry, &registration.cancellation)
            .await?;
        let now = Utc::now();
        let remaining = (request.deadline - now)
            .to_std()
            .map_err(|_| before_dispatch("capability_grpc_deadline_elapsed", false))?;
        let total_timeout = remaining.min(Duration::from_millis(
            request.limits.total_timeout_milliseconds,
        ));
        let connect_timeout =
            Duration::from_millis(request.limits.connect_timeout_milliseconds).min(total_timeout);
        let first_byte_timeout =
            Duration::from_millis(request.limits.first_byte_timeout_milliseconds)
                .min(total_timeout);
        let idle_timeout =
            Duration::from_millis(request.limits.idle_timeout_milliseconds).min(total_timeout);
        if total_timeout.is_zero()
            || connect_timeout.is_zero()
            || first_byte_timeout.is_zero()
            || idle_timeout.is_zero()
        {
            return Err(before_dispatch("capability_grpc_deadline_elapsed", false));
        }
        headers.insert(
            "grpc-timeout",
            grpc_timeout_header(total_timeout)
                .map_err(|_| before_dispatch("capability_grpc_deadline_elapsed", false))?,
        );
        let evidence_digest =
            transport_evidence_digest(&entry, &addresses, &request.admission_digest);
        let response = self
            .transport
            .unary(PinnedCapabilityGrpcRequest {
                uri: grpc_uri(&entry)
                    .map_err(|_| before_dispatch("capability_grpc_invalid_endpoint", false))?,
                dns_host,
                addresses,
                headers,
                framed_message: encode_unary_frame(&request.message)?,
                connect_timeout,
                first_byte_timeout,
                idle_timeout,
                total_timeout,
                maximum_response_bytes: u64::from(request.limits.maximum_response_bytes),
                cancellation: registration.cancellation.clone(),
                transport_evidence_digest: evidence_digest.clone(),
                effect: request.effect,
                idempotency_kind: request.idempotency_kind,
            })
            .await?;
        drop(registration);
        if response.status_code > 16
            || response.message.len() > request.limits.maximum_response_bytes as usize
            || response.trailing_metadata.len() > MAX_GRPC_ADAPTER_METADATA
            || response
                .trailing_metadata
                .iter()
                .any(|metadata| metadata.validate().is_err())
        {
            return Err(dispatch_failure(
                "capability_grpc_invalid_response",
                false,
                evidence_digest,
                request.effect,
                request.idempotency_kind,
            ));
        }
        Ok(GrpcTransportResponse {
            status_code: response.status_code,
            trailing_metadata: response.trailing_metadata,
            message: response.message,
            transport_evidence_digest: evidence_digest,
        })
    }

    async fn cancel(
        &self,
        request: CapabilityTransportCancelRequest,
    ) -> Result<CapabilityTransportCancelOutcome, CapabilityAdapterFailure> {
        request
            .validate_at(Utc::now())
            .map_err(|_| before_dispatch("capability_grpc_invalid_cancel", false))?;
        if request.identity.backend_kind != CapabilityBackendKind::Grpc {
            return Err(before_dispatch("capability_grpc_invalid_cancel", false));
        }
        let cancellation = self
            .active
            .lock()
            .map_err(|_| before_dispatch("capability_grpc_registry_unavailable", false))?
            .get(&request.identity)
            .cloned();
        let Some(cancellation) = cancellation else {
            return Ok(CapabilityTransportCancelOutcome::AlreadyTerminal);
        };
        cancellation.cancel();
        Ok(CapabilityTransportCancelOutcome::Accepted)
    }
}

fn encode_unary_frame(message: &[u8]) -> Result<Vec<u8>, CapabilityAdapterFailure> {
    let length = u32::try_from(message.len())
        .map_err(|_| before_dispatch("capability_grpc_request_too_large", false))?;
    let mut framed = Vec::with_capacity(5 + message.len());
    framed.push(0);
    framed.extend_from_slice(&length.to_be_bytes());
    framed.extend_from_slice(message);
    Ok(framed)
}

fn decode_unary_frame(framed: &[u8], maximum_response_bytes: u64) -> Result<Vec<u8>, &'static str> {
    if framed.len() < 5 || framed[0] != 0 {
        return Err("capability_grpc_invalid_frame");
    }
    let length = u32::from_be_bytes([framed[1], framed[2], framed[3], framed[4]]) as usize;
    if length > maximum_response_bytes as usize || framed.len() != length.saturating_add(5) {
        return Err("capability_grpc_invalid_frame");
    }
    Ok(framed[5..].to_vec())
}

fn parse_response_status(
    initial_headers: &HeaderMap,
    trailers: Option<&HeaderMap>,
) -> Result<(u16, Vec<SafeGrpcMetadata>), &'static str> {
    let initial_status_count = initial_headers.get_all("grpc-status").iter().count();
    let trailer_status_count = trailers
        .map(|values| values.get_all("grpc-status").iter().count())
        .unwrap_or(0);
    if initial_status_count + trailer_status_count != 1
        || initial_status_count == 1 && trailers.is_some()
    {
        return Err("capability_grpc_invalid_trailers");
    }
    let status_source = if initial_status_count == 1 {
        initial_headers
    } else {
        trailers.ok_or("capability_grpc_missing_trailers")?
    };
    let status_code = status_source
        .get("grpc-status")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u16>().ok())
        .filter(|status| *status <= 16)
        .ok_or("capability_grpc_invalid_trailers")?;
    let mut metadata = Vec::new();
    let mut keys = BTreeSet::new();
    for (name, value) in trailers.into_iter().flatten() {
        let key = name.as_str();
        if matches!(key, "grpc-status" | "grpc-message") {
            continue;
        }
        if metadata.len() == MAX_GRPC_ADAPTER_METADATA || !keys.insert(key.to_owned()) {
            return Err("capability_grpc_invalid_trailers");
        }
        let value = value
            .to_str()
            .ok()
            .filter(|value| value.len() <= 4_096)
            .ok_or("capability_grpc_invalid_trailers")?;
        let item = SafeGrpcMetadata {
            key: key.to_owned(),
            value: value.to_owned(),
        };
        item.validate()
            .map_err(|_| "capability_grpc_invalid_trailers")?;
        metadata.push(item);
    }
    Ok((status_code, metadata))
}

fn insert_credential(
    headers: &mut HeaderMap,
    injection: &InstalledGrpcCredentialInjection,
    credential: &ResolvedSecretMaterial,
) -> Result<(), CapabilityAdapterFailure> {
    let (name, raw) = match injection {
        InstalledGrpcCredentialInjection::BearerAuthorization { .. } => {
            let mut raw = Vec::with_capacity(7 + credential.material.as_bytes().len());
            raw.extend_from_slice(b"Bearer ");
            raw.extend_from_slice(credential.material.as_bytes());
            (HeaderName::from_static("authorization"), raw)
        }
        InstalledGrpcCredentialInjection::Metadata { key, .. } => (
            HeaderName::from_bytes(key.as_bytes())
                .map_err(|_| before_dispatch("capability_grpc_invalid_credential", false))?,
            credential.material.as_bytes().to_vec(),
        ),
    };
    let parsed = HeaderValue::from_bytes(&raw);
    let mut raw = raw;
    raw.fill(0);
    let mut value =
        parsed.map_err(|_| before_dispatch("capability_grpc_invalid_credential", false))?;
    value.set_sensitive(true);
    if headers.insert(name, value).is_some() {
        return Err(before_dispatch("capability_grpc_duplicate_metadata", false));
    }
    Ok(())
}

fn grpc_uri(
    entry: &InstalledCapabilityGrpcEndpoint,
) -> Result<http::Uri, EgressConfigurationError> {
    let base = entry.endpoint.base_path.trim_end_matches('/');
    let path = format!("{base}/{}/{}", entry.service_name, entry.method_name);
    format!(
        "https://{}:{}{}",
        entry.endpoint.host, entry.endpoint.port, path
    )
    .parse()
    .map_err(|_| EgressConfigurationError::InvalidEndpoint)
}

fn valid_grpc_component(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 256
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
}

fn valid_grpc_content_type(value: &str) -> bool {
    matches!(value, "application/grpc" | "application/grpc+proto")
}

fn grpc_timeout_header(timeout: Duration) -> Result<HeaderValue, http::header::InvalidHeaderValue> {
    let milliseconds = timeout.as_millis().max(1);
    HeaderValue::from_str(&format!("{milliseconds}m"))
}

fn transport_evidence_digest(
    entry: &InstalledCapabilityGrpcEndpoint,
    addresses: &[SocketAddr],
    admission_digest: &Sha256Digest,
) -> Sha256Digest {
    canonical_digest(&serde_json::json!({
        "schema_version": 1,
        "endpoint_identity_digest": entry.endpoint_identity_digest,
        "tls_policy": entry.tls_policy,
        "trust_policy": entry.trust_policy,
        "pinned_addresses": addresses.iter().map(ToString::to_string).collect::<Vec<_>>(),
        "admission_digest": admission_digest,
        "grpc_service": entry.service_name,
        "grpc_method": entry.method_name,
    }))
    .expect("closed gRPC transport evidence is canonical")
    .parse()
    .expect("canonical gRPC transport evidence is SHA-256")
}

fn before_dispatch(code: &str, retryable: bool) -> CapabilityAdapterFailure {
    CapabilityAdapterFailure {
        class: if retryable {
            CapabilityAdapterFailureClass::RetryableBeforeDispatch
        } else {
            CapabilityAdapterFailureClass::RejectedBeforeDispatch
        },
        safe_code: code.to_owned(),
        safe_message: "Capability gRPC Egress rejected the request before dispatch".to_owned(),
        evidence_digest: static_digest(code),
        external_identity_digest: None,
    }
}

fn dispatch_failure(
    code: &str,
    timed_out: bool,
    external_identity_digest: Sha256Digest,
    effect: Effect,
    idempotency_kind: CapabilityIdempotencyKind,
) -> CapabilityAdapterFailure {
    let safe_replay = effect.risk_rank() <= Effect::ReadOnly.risk_rank()
        || effect == Effect::IdempotentWrite
            && matches!(
                idempotency_kind,
                CapabilityIdempotencyKind::Intrinsic | CapabilityIdempotencyKind::CallerKey
            );
    CapabilityAdapterFailure {
        class: if safe_replay {
            CapabilityAdapterFailureClass::RetryableAfterDispatch
        } else if timed_out {
            CapabilityAdapterFailureClass::TimedOutUncertain
        } else {
            CapabilityAdapterFailureClass::Uncertain
        },
        safe_code: code.to_owned(),
        safe_message: "Capability gRPC Egress could not prove the remote outcome".to_owned(),
        evidence_digest: static_digest(code),
        external_identity_digest: Some(external_identity_digest),
    }
}

fn uncertain_after_dispatch(
    code: &str,
    timed_out: bool,
    external_identity_digest: Sha256Digest,
) -> CapabilityAdapterFailure {
    CapabilityAdapterFailure {
        class: if timed_out {
            CapabilityAdapterFailureClass::TimedOutUncertain
        } else {
            CapabilityAdapterFailureClass::Uncertain
        },
        safe_code: code.to_owned(),
        safe_message: "Capability gRPC Egress could not prove the remote outcome".to_owned(),
        evidence_digest: static_digest(code),
        external_identity_digest: Some(external_identity_digest),
    }
}

fn static_digest(code: &str) -> Sha256Digest {
    canonical_digest(&serde_json::json!({"domain": code, "schema_version": 1}))
        .expect("static Capability gRPC Egress evidence is canonical")
        .parse()
        .expect("canonical Capability gRPC Egress evidence is SHA-256")
}

#[cfg(test)]
mod tests {
    use super::*;
    use insight_platform_capability_adapters::{
        CapabilityTransportRequestIdentity, GrpcIdempotencyBinding,
    };
    use insight_platform_contracts::{ResourceId, ResourceKind, SecretResolutionPolicy};
    use std::{
        net::Ipv4Addr,
        str::FromStr,
        sync::{
            atomic::{AtomicUsize, Ordering},
            Mutex,
        },
    };
    use tokio::sync::Notify;

    fn digest(character: char) -> Sha256Digest {
        format!("sha256:{}", character.to_string().repeat(64))
            .parse()
            .unwrap()
    }

    fn id(kind: ResourceKind, suffix: u16) -> ResourceId {
        format!(
            "{}_0198f1c9-32e4-75e1-a9e8-d95ca0f5{suffix:04x}",
            kind.descriptor().prefix
        )
        .parse()
        .unwrap()
    }

    fn exact_version(kind: ResourceKind, suffix: u16, character: char) -> ExactVersionRef {
        ExactVersionRef::new(id(kind, suffix), digest(character)).unwrap()
    }

    fn exact_secret() -> ExactSecretBindingRef {
        ExactSecretBindingRef::build(
            id(ResourceKind::SecretBinding, 10),
            3,
            id(ResourceKind::SecretProvider, 11),
            SecretPurpose::from_str("service.grpc_api_key").unwrap(),
            SecretResolutionPolicy::Pinned {
                opaque_version_identity_digest: digest('9'),
            },
        )
        .unwrap()
    }

    fn backend_limits() -> CapabilityBackendLimits {
        CapabilityBackendLimits {
            maximum_request_bytes: 4_096,
            maximum_response_bytes: 4_096,
            maximum_diagnostic_bytes: 1_024,
            connect_timeout_milliseconds: 100,
            first_byte_timeout_milliseconds: 200,
            idle_timeout_milliseconds: 300,
            total_timeout_milliseconds: 1_000,
        }
    }

    struct Fixture {
        entry: InstalledCapabilityGrpcEndpoint,
        request: GrpcTransportRequest,
    }

    fn fixture() -> Fixture {
        let endpoint = CanonicalHttpEndpoint {
            scheme: CapabilityEndpointScheme::Https,
            host: "grpc.example.com".to_owned(),
            port: 443,
            base_path: "/api".to_owned(),
        };
        let deployment =
            ExactDeploymentRef::new(id(ResourceKind::CapabilityDeployment, 1), digest('1'))
                .unwrap();
        let network_policy = exact_version(ResourceKind::PolicyRevision, 2, '2');
        let tls_policy = exact_version(ResourceKind::PolicyRevision, 3, '3');
        let trust_policy = exact_version(ResourceKind::PolicyRevision, 4, '4');
        let binding = exact_secret();
        let limits = backend_limits();
        Fixture {
            entry: InstalledCapabilityGrpcEndpoint {
                schema_version: 1,
                capability_deployment: deployment.clone(),
                backend_contract_digest: digest('5'),
                effect: Effect::ReadOnly,
                idempotency_kind: CapabilityIdempotencyKind::CallerKey,
                service_name: "fixture.v1.LookupService".to_owned(),
                method_name: "Lookup".to_owned(),
                endpoint: endpoint.clone(),
                endpoint_identity_digest: endpoint.canonical_digest().unwrap(),
                network_policy: network_policy.clone(),
                tls_policy: tls_policy.clone(),
                trust_policy: trust_policy.clone(),
                secret_bindings: vec![binding.clone()],
                credential_injections: vec![
                    InstalledGrpcCredentialInjection::BearerAuthorization {
                        purpose: binding.purpose.clone(),
                    },
                ],
                limits,
            },
            request: GrpcTransportRequest {
                identity: CapabilityTransportRequestIdentity {
                    backend_kind: CapabilityBackendKind::Grpc,
                    tenant_id: id(ResourceKind::Tenant, 20),
                    invocation_id: id(ResourceKind::CapabilityInvocation, 21),
                    job_id: id(ResourceKind::Job, 22),
                    worker_process_generation_id: id(ResourceKind::WorkerProcessGeneration, 23),
                    capability_deployment_id: deployment.deployment_id,
                    capability_deployment_digest: deployment.deployment_digest,
                    physical_attempt: 1,
                    lease_generation: 1,
                },
                admission_digest: digest('6'),
                deadline: Utc::now() + chrono::Duration::seconds(30),
                effect: Effect::ReadOnly,
                idempotency_kind: CapabilityIdempotencyKind::CallerKey,
                backend_contract_digest: digest('5'),
                endpoint: endpoint.clone(),
                endpoint_identity_digest: endpoint.canonical_digest().unwrap(),
                service_name: "fixture.v1.LookupService".to_owned(),
                method_name: "Lookup".to_owned(),
                network_policy,
                tls_policy,
                trust_policy,
                secret_bindings: vec![binding],
                idempotency: Some(GrpcIdempotencyBinding {
                    metadata_key: "idempotency-key".to_owned(),
                    value_digest: digest('7'),
                }),
                limits,
                metadata: vec![SafeGrpcMetadata {
                    key: "x-fixture".to_owned(),
                    value: "yes".to_owned(),
                }],
                message: vec![0x0a, 0x00],
            },
        }
    }

    struct FixtureSecrets {
        calls: AtomicUsize,
    }

    #[async_trait]
    impl SecretMaterialResolver for FixtureSecrets {
        async fn resolve(
            &self,
            _tenant_id: &ResourceId,
            binding: &ExactSecretBindingRef,
        ) -> Result<ResolvedSecretMaterial, SecretMaterialResolutionError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            ResolvedSecretMaterial::new(
                binding.secret_binding_id.clone(),
                binding.provider_id.clone(),
                binding.purpose.clone(),
                3,
                digest('9'),
                b"top-secret".to_vec(),
            )
            .map_err(|_| SecretMaterialResolutionError::InvalidEvidence)
        }
    }

    struct FixtureDns {
        addresses: Vec<SocketAddr>,
        calls: AtomicUsize,
    }

    #[async_trait]
    impl EgressDnsResolver for FixtureDns {
        async fn resolve(
            &self,
            _host: &str,
            _port: u16,
        ) -> Result<Vec<SocketAddr>, DnsResolutionError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(self.addresses.clone())
        }
    }

    #[derive(Debug, Clone, Copy)]
    enum FixtureMode {
        Complete,
        WaitForCancel,
        FailAfterDispatch,
    }

    struct FixtureTransport {
        mode: FixtureMode,
        started: Notify,
        observed_evidence: Mutex<Option<Sha256Digest>>,
    }

    impl FixtureTransport {
        fn new(mode: FixtureMode) -> Self {
            Self {
                mode,
                started: Notify::new(),
                observed_evidence: Mutex::new(None),
            }
        }
    }

    #[async_trait]
    impl PinnedCapabilityGrpcTransport for FixtureTransport {
        async fn unary(
            &self,
            request: PinnedCapabilityGrpcRequest,
        ) -> Result<PinnedCapabilityGrpcResponse, CapabilityAdapterFailure> {
            assert_eq!(
                request.uri.to_string(),
                "https://grpc.example.com:443/api/fixture.v1.LookupService/Lookup"
            );
            assert_eq!(request.dns_host, "grpc.example.com");
            assert_eq!(
                request.addresses,
                vec![SocketAddr::new(Ipv4Addr::new(8, 8, 8, 8).into(), 443)]
            );
            assert_eq!(request.framed_message, vec![0, 0, 0, 0, 2, 0x0a, 0x00]);
            assert_eq!(
                request.headers.get("authorization").unwrap().as_bytes(),
                b"Bearer top-secret"
            );
            assert_eq!(
                request.headers.get("content-type").unwrap(),
                "application/grpc"
            );
            assert_eq!(request.headers.get("te").unwrap(), "trailers");
            assert!(request.headers.get("grpc-timeout").is_some());
            *self.observed_evidence.lock().unwrap() =
                Some(request.transport_evidence_digest.clone());
            self.started.notify_waiters();
            match self.mode {
                FixtureMode::Complete => Ok(PinnedCapabilityGrpcResponse {
                    status_code: 0,
                    trailing_metadata: vec![SafeGrpcMetadata {
                        key: "x-evidence".to_owned(),
                        value: "ready".to_owned(),
                    }],
                    message: vec![0x0a, 0x00],
                }),
                FixtureMode::WaitForCancel => {
                    request.cancellation.cancelled().await;
                    Err(uncertain_after_dispatch(
                        "capability_grpc_cancelled",
                        false,
                        request.transport_evidence_digest,
                    ))
                }
                FixtureMode::FailAfterDispatch => Err(dispatch_failure(
                    "capability_grpc_transport_failed",
                    false,
                    request.transport_evidence_digest,
                    request.effect,
                    request.idempotency_kind,
                )),
            }
        }
    }

    fn build_transport(
        fixture: &Fixture,
        secrets: Arc<FixtureSecrets>,
        dns: Arc<FixtureDns>,
        backend: Arc<FixtureTransport>,
    ) -> Arc<HyperCapabilityGrpcEgressTransport> {
        Arc::new(
            HyperCapabilityGrpcEgressTransport::with_transport(
                InstalledCapabilityGrpcEndpointCatalog::new(vec![fixture.entry.clone()]).unwrap(),
                secrets,
                dns,
                backend,
                CapabilityGrpcEgressLimits {
                    maximum_in_flight: 2,
                    maximum_dns_answers: 4,
                    maximum_secret_material_bytes: 128,
                },
            )
            .unwrap(),
        )
    }

    fn public_dns() -> Arc<FixtureDns> {
        Arc::new(FixtureDns {
            addresses: vec![SocketAddr::new(Ipv4Addr::new(8, 8, 8, 8).into(), 443)],
            calls: AtomicUsize::new(0),
        })
    }

    fn secrets() -> Arc<FixtureSecrets> {
        Arc::new(FixtureSecrets {
            calls: AtomicUsize::new(0),
        })
    }

    #[test]
    fn grpc_frames_trailers_and_trailers_only_are_closed() {
        let framed = encode_unary_frame(&[0x0a, 0x00]).unwrap();
        assert_eq!(framed, vec![0, 0, 0, 0, 2, 0x0a, 0x00]);
        assert_eq!(decode_unary_frame(&framed, 8).unwrap(), vec![0x0a, 0x00]);
        assert!(decode_unary_frame(&[1, 0, 0, 0, 0], 8).is_err());

        let initial = HeaderMap::new();
        let trailers = HeaderMap::from_iter([(
            HeaderName::from_static("grpc-status"),
            HeaderValue::from_static("0"),
        )]);
        assert_eq!(
            parse_response_status(&initial, Some(&trailers)).unwrap().0,
            0
        );
        let initial = HeaderMap::from_iter([(
            HeaderName::from_static("grpc-status"),
            HeaderValue::from_static("7"),
        )]);
        assert_eq!(parse_response_status(&initial, None).unwrap().0, 7);
        assert!(parse_response_status(&initial, Some(&trailers)).is_err());
    }

    #[tokio::test]
    async fn exact_catalog_secret_dns_frame_and_response_are_composed() {
        let fixture = fixture();
        let secret_resolver = secrets();
        let dns = public_dns();
        let backend = Arc::new(FixtureTransport::new(FixtureMode::Complete));
        let transport = build_transport(
            &fixture,
            secret_resolver.clone(),
            dns.clone(),
            backend.clone(),
        );
        let response = transport.unary(fixture.request.clone()).await.unwrap();
        assert_eq!(response.status_code, 0);
        assert_eq!(response.message, vec![0x0a, 0x00]);
        assert_eq!(
            response.transport_evidence_digest,
            backend.observed_evidence.lock().unwrap().clone().unwrap()
        );
        assert_eq!(secret_resolver.calls.load(Ordering::SeqCst), 1);
        assert_eq!(dns.calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn drift_private_dns_and_stale_cancel_fail_closed() {
        let fixture = fixture();
        let secret_resolver = secrets();
        let dns = public_dns();
        let transport = build_transport(
            &fixture,
            secret_resolver.clone(),
            dns.clone(),
            Arc::new(FixtureTransport::new(FixtureMode::Complete)),
        );
        let mut drifted = fixture.request.clone();
        drifted.method_name = "Delete".to_owned();
        assert_eq!(
            transport.unary(drifted).await.unwrap_err().safe_code,
            "capability_grpc_endpoint_not_installed"
        );
        assert_eq!(dns.calls.load(Ordering::SeqCst), 0);
        assert_eq!(secret_resolver.calls.load(Ordering::SeqCst), 0);

        let private_dns = Arc::new(FixtureDns {
            addresses: vec![SocketAddr::new(Ipv4Addr::new(10, 0, 0, 1).into(), 443)],
            calls: AtomicUsize::new(0),
        });
        let transport = build_transport(
            &fixture,
            secret_resolver.clone(),
            private_dns,
            Arc::new(FixtureTransport::new(FixtureMode::Complete)),
        );
        assert_eq!(
            transport
                .unary(fixture.request.clone())
                .await
                .unwrap_err()
                .safe_code,
            "capability_grpc_destination_denied"
        );
        assert_eq!(secret_resolver.calls.load(Ordering::SeqCst), 0);

        let backend = Arc::new(FixtureTransport::new(FixtureMode::WaitForCancel));
        let transport = build_transport(&fixture, secrets(), public_dns(), backend.clone());
        let request = fixture.request.clone();
        let task_transport = transport.clone();
        let task = tokio::spawn(async move { task_transport.unary(request).await });
        backend.started.notified().await;
        let mut stale = fixture.request.identity.clone();
        stale.lease_generation += 1;
        assert_eq!(
            transport
                .cancel(CapabilityTransportCancelRequest {
                    identity: stale,
                    deadline: Utc::now() + chrono::Duration::seconds(1),
                })
                .await
                .unwrap(),
            CapabilityTransportCancelOutcome::AlreadyTerminal
        );
        assert!(!task.is_finished());
        assert_eq!(
            transport
                .cancel(CapabilityTransportCancelRequest {
                    identity: fixture.request.identity,
                    deadline: Utc::now() + chrono::Duration::seconds(1),
                })
                .await
                .unwrap(),
            CapabilityTransportCancelOutcome::Accepted
        );
        assert_eq!(
            task.await.unwrap().unwrap_err().safe_code,
            "capability_grpc_cancelled"
        );
    }

    #[tokio::test]
    async fn dispatch_failure_respects_frozen_effect_and_idempotency() {
        let read_fixture = fixture();
        let transport = build_transport(
            &read_fixture,
            secrets(),
            public_dns(),
            Arc::new(FixtureTransport::new(FixtureMode::FailAfterDispatch)),
        );
        assert_eq!(
            transport
                .unary(read_fixture.request)
                .await
                .unwrap_err()
                .class,
            CapabilityAdapterFailureClass::RetryableAfterDispatch
        );
        let mut write_fixture = fixture();
        write_fixture.entry.effect = Effect::NonIdempotentWrite;
        write_fixture.entry.idempotency_kind = CapabilityIdempotencyKind::None;
        write_fixture.request.effect = Effect::NonIdempotentWrite;
        write_fixture.request.idempotency_kind = CapabilityIdempotencyKind::None;
        write_fixture.request.idempotency = None;
        let transport = build_transport(
            &write_fixture,
            secrets(),
            public_dns(),
            Arc::new(FixtureTransport::new(FixtureMode::FailAfterDispatch)),
        );
        assert_eq!(
            transport
                .unary(write_fixture.request)
                .await
                .unwrap_err()
                .class,
            CapabilityAdapterFailureClass::Uncertain
        );
    }
}
