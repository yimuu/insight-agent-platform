use super::{
    is_public_destination_ip, parse_endpoint_host, DnsResolutionError, EgressConfigurationError,
    EgressDnsResolver, ParsedEndpointHost, ResolvedSecretMaterial, SecretMaterialResolutionError,
    SecretMaterialResolver, MAX_DNS_ANSWERS_HARD, MAX_EGRESS_IN_FLIGHT_HARD,
    MAX_SECRET_MATERIAL_BYTES_HARD,
};
use async_trait::async_trait;
use chrono::Utc;
use futures::StreamExt;
use insight_platform_capability_adapters::{
    CapabilityAdapterFailure, CapabilityAdapterFailureClass, CapabilityTransportCancelOutcome,
    CapabilityTransportCancelRequest, CapabilityTransportRequestIdentity, HttpNetworkTransport,
    HttpTransportRequest, HttpTransportResponse, SafeHttpHeader, MAX_HTTP_ADAPTER_HEADERS,
};
use insight_platform_contracts::{
    canonical_digest, CanonicalHttpEndpoint, CapabilityBackendKind, CapabilityBackendLimits,
    CapabilityEndpointScheme, CapabilityIdempotencyKind, Effect, ExactDeploymentRef,
    ExactSecretBindingRef, ExactVersionRef, HttpCapabilityMethod, SecretPurpose, Sha256Digest,
};
use reqwest::{
    header::{HeaderMap, HeaderName, HeaderValue, CONTENT_ENCODING, CONTENT_LENGTH},
    Method,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    net::{IpAddr, SocketAddr},
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::CancellationToken;
use url::Url;

pub const MAX_INSTALLED_CAPABILITY_HTTP_ENDPOINTS: usize = 4_096;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapabilityHttpEgressLimits {
    pub maximum_in_flight: usize,
    pub maximum_dns_answers: usize,
    pub maximum_secret_material_bytes: usize,
}

impl CapabilityHttpEgressLimits {
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

impl Default for CapabilityHttpEgressLimits {
    fn default() -> Self {
        Self {
            maximum_in_flight: 256,
            maximum_dns_answers: 16,
            maximum_secret_material_bytes: 8_192,
        }
    }
}

/// Credential injection is installed with the trusted Deployment catalog, never supplied by a
/// worker request or declarative protocol codec.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum InstalledHttpCredentialInjection {
    BearerAuthorization {
        purpose: SecretPurpose,
    },
    Header {
        purpose: SecretPurpose,
        name: String,
    },
}

impl InstalledHttpCredentialInjection {
    fn purpose(&self) -> &SecretPurpose {
        match self {
            Self::BearerAuthorization { purpose } | Self::Header { purpose, .. } => purpose,
        }
    }

    fn validate(&self) -> Result<(), EgressConfigurationError> {
        match self {
            Self::BearerAuthorization { .. } => Ok(()),
            Self::Header { name, .. } => {
                let lower = name.to_ascii_lowercase();
                if name != &lower
                    || HeaderName::from_bytes(name.as_bytes()).is_err()
                    || matches!(
                        lower.as_str(),
                        "authorization"
                            | "connection"
                            | "content-length"
                            | "cookie"
                            | "host"
                            | "proxy-authorization"
                            | "set-cookie"
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
pub struct InstalledCapabilityHttpEndpoint {
    pub schema_version: u32,
    pub capability_deployment: ExactDeploymentRef,
    pub backend_contract_digest: Sha256Digest,
    pub effect: Effect,
    pub idempotency_kind: CapabilityIdempotencyKind,
    pub method: HttpCapabilityMethod,
    pub endpoint: CanonicalHttpEndpoint,
    pub endpoint_identity_digest: Sha256Digest,
    pub network_policy: ExactVersionRef,
    pub tls_policy: ExactVersionRef,
    pub trust_policy: ExactVersionRef,
    pub secret_bindings: Vec<ExactSecretBindingRef>,
    pub credential_injections: Vec<InstalledHttpCredentialInjection>,
    pub limits: CapabilityBackendLimits,
}

impl InstalledCapabilityHttpEndpoint {
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
        let mut prior_binding = None;
        for binding in &self.secret_bindings {
            binding
                .validate()
                .map_err(|_| EgressConfigurationError::InvalidEndpoint)?;
            let key = (&binding.purpose, &binding.secret_binding_id);
            if prior_binding.is_some_and(|prior| prior >= key) {
                return Err(EgressConfigurationError::InvalidEndpoint);
            }
            prior_binding = Some(key);
        }
        let mut purposes = BTreeSet::new();
        let mut names = BTreeSet::new();
        for injection in &self.credential_injections {
            injection.validate()?;
            if !purposes.insert(injection.purpose().clone())
                || !names.insert(match injection {
                    InstalledHttpCredentialInjection::BearerAuthorization { .. } => {
                        "authorization".to_owned()
                    }
                    InstalledHttpCredentialInjection::Header { name, .. } => name.clone(),
                })
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
        capability_url(&self.endpoint)?;
        Ok(())
    }

    fn matches(&self, request: &HttpTransportRequest) -> bool {
        request.identity.backend_kind == CapabilityBackendKind::Http
            && request.identity.capability_deployment_id == self.capability_deployment.deployment_id
            && request.identity.capability_deployment_digest
                == self.capability_deployment.deployment_digest
            && request.backend_contract_digest == self.backend_contract_digest
            && request.effect == self.effect
            && request.idempotency_kind == self.idempotency_kind
            && request.method == self.method
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
pub struct InstalledCapabilityHttpEndpointCatalog {
    entries: BTreeMap<
        (insight_platform_contracts::ResourceId, Sha256Digest),
        InstalledCapabilityHttpEndpoint,
    >,
}

impl InstalledCapabilityHttpEndpointCatalog {
    pub fn new(
        entries: Vec<InstalledCapabilityHttpEndpoint>,
    ) -> Result<Self, EgressConfigurationError> {
        if entries.is_empty() || entries.len() > MAX_INSTALLED_CAPABILITY_HTTP_ENDPOINTS {
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
        request: &HttpTransportRequest,
    ) -> Result<InstalledCapabilityHttpEndpoint, CapabilityAdapterFailure> {
        let key = (
            request.identity.capability_deployment_id.clone(),
            request.identity.capability_deployment_digest.clone(),
        );
        self.entries
            .get(&key)
            .filter(|entry| entry.matches(request))
            .cloned()
            .ok_or_else(|| before_dispatch("capability_egress_endpoint_not_installed", false))
    }
}

struct PinnedCapabilityHttpRequest {
    method: Method,
    url: Url,
    dns_host: String,
    addresses: Vec<SocketAddr>,
    headers: HeaderMap,
    body: Vec<u8>,
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

struct PinnedCapabilityHttpResponse {
    status: u16,
    headers: Vec<SafeHttpHeader>,
    body: Vec<u8>,
}

#[async_trait]
trait PinnedCapabilityHttpTransport: Send + Sync {
    async fn round_trip(
        &self,
        request: PinnedCapabilityHttpRequest,
    ) -> Result<PinnedCapabilityHttpResponse, CapabilityAdapterFailure>;
}

#[derive(Default)]
struct ReqwestPinnedCapabilityHttpTransport;

#[async_trait]
impl PinnedCapabilityHttpTransport for ReqwestPinnedCapabilityHttpTransport {
    async fn round_trip(
        &self,
        request: PinnedCapabilityHttpRequest,
    ) -> Result<PinnedCapabilityHttpResponse, CapabilityAdapterFailure> {
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .referer(false)
            .no_proxy()
            .https_only(true)
            .connect_timeout(request.connect_timeout)
            .timeout(request.total_timeout)
            .pool_max_idle_per_host(0)
            .resolve_to_addrs(&request.dns_host, &request.addresses)
            .build()
            .map_err(|_| before_dispatch("capability_egress_client_build_failed", false))?;
        let outbound = client
            .request(request.method, request.url)
            .headers(request.headers)
            .body(request.body)
            .build()
            .map_err(|_| before_dispatch("capability_egress_request_build_failed", false))?;
        let response = tokio::select! {
            biased;
            _ = request.cancellation.cancelled() => {
                return Err(uncertain_after_dispatch("capability_egress_cancelled", false, request.transport_evidence_digest));
            }
            response = tokio::time::timeout(request.first_byte_timeout, client.execute(outbound)) => {
                match response {
                    Ok(Ok(response)) => response,
                    Ok(Err(_)) => return Err(dispatch_failure(
                        "capability_egress_transport_failed",
                        false,
                        request.transport_evidence_digest.clone(),
                        request.effect,
                        request.idempotency_kind,
                    )),
                    Err(_) => return Err(dispatch_failure(
                        "capability_egress_first_byte_timeout",
                        true,
                        request.transport_evidence_digest.clone(),
                        request.effect,
                        request.idempotency_kind,
                    )),
                }
            },
        };
        if response.headers().get_all(CONTENT_LENGTH).iter().count() > 1
            || response
                .headers()
                .get(CONTENT_ENCODING)
                .is_some_and(|value| value.as_bytes() != b"identity")
            || response
                .content_length()
                .is_some_and(|length| length > request.maximum_response_bytes)
        {
            return Err(dispatch_failure(
                "capability_egress_invalid_response_metadata",
                false,
                request.transport_evidence_digest,
                request.effect,
                request.idempotency_kind,
            ));
        }
        let status = response.status().as_u16();
        let mut headers = Vec::new();
        for (name, value) in response.headers() {
            let lower = name.as_str().to_ascii_lowercase();
            if matches!(
                lower.as_str(),
                "authorization"
                    | "connection"
                    | "content-length"
                    | "cookie"
                    | "proxy-authenticate"
                    | "proxy-authorization"
                    | "set-cookie"
                    | "transfer-encoding"
            ) {
                continue;
            }
            let Some(value) = value.to_str().ok().filter(|value| value.len() <= 4_096) else {
                continue;
            };
            let header = SafeHttpHeader {
                name: lower,
                value: value.to_owned(),
            };
            if header.validate().is_ok() {
                headers.push(header);
            }
            if headers.len() == MAX_HTTP_ADAPTER_HEADERS {
                break;
            }
        }
        let mut body = Vec::new();
        let mut stream = response.bytes_stream();
        loop {
            let next = tokio::select! {
                biased;
                _ = request.cancellation.cancelled() => {
                    return Err(uncertain_after_dispatch(
                        "capability_egress_cancelled",
                        false,
                        request.transport_evidence_digest,
                    ));
                }
                next = tokio::time::timeout(request.idle_timeout, stream.next()) => next,
            };
            let item = match next {
                Ok(Some(Ok(bytes))) => bytes,
                Ok(Some(Err(_))) => {
                    return Err(dispatch_failure(
                        "capability_egress_response_failed",
                        false,
                        request.transport_evidence_digest,
                        request.effect,
                        request.idempotency_kind,
                    ))
                }
                Ok(None) => break,
                Err(_) => {
                    return Err(dispatch_failure(
                        "capability_egress_idle_timeout",
                        true,
                        request.transport_evidence_digest,
                        request.effect,
                        request.idempotency_kind,
                    ))
                }
            };
            if body.len().saturating_add(item.len()) > request.maximum_response_bytes as usize {
                return Err(dispatch_failure(
                    "capability_egress_response_too_large",
                    false,
                    request.transport_evidence_digest,
                    request.effect,
                    request.idempotency_kind,
                ));
            }
            body.extend_from_slice(&item);
        }
        Ok(PinnedCapabilityHttpResponse {
            status,
            headers,
            body,
        })
    }
}

struct CapabilityInFlightRegistration {
    identity: CapabilityTransportRequestIdentity,
    active: Arc<Mutex<BTreeMap<CapabilityTransportRequestIdentity, CancellationToken>>>,
    cancellation: CancellationToken,
    _permit: OwnedSemaphorePermit,
}

impl Drop for CapabilityInFlightRegistration {
    fn drop(&mut self) {
        if let Ok(mut active) = self.active.lock() {
            active.remove(&self.identity);
        }
    }
}

pub struct ReqwestCapabilityHttpEgressTransport {
    catalog: InstalledCapabilityHttpEndpointCatalog,
    secrets: Arc<dyn SecretMaterialResolver>,
    dns: Arc<dyn EgressDnsResolver>,
    transport: Arc<dyn PinnedCapabilityHttpTransport>,
    limits: CapabilityHttpEgressLimits,
    permits: Arc<Semaphore>,
    active: Arc<Mutex<BTreeMap<CapabilityTransportRequestIdentity, CancellationToken>>>,
}

impl ReqwestCapabilityHttpEgressTransport {
    pub fn new(
        catalog: InstalledCapabilityHttpEndpointCatalog,
        secrets: Arc<dyn SecretMaterialResolver>,
        dns: Arc<dyn EgressDnsResolver>,
        limits: CapabilityHttpEgressLimits,
    ) -> Result<Self, EgressConfigurationError> {
        Self::with_transport(
            catalog,
            secrets,
            dns,
            Arc::new(ReqwestPinnedCapabilityHttpTransport),
            limits,
        )
    }

    fn with_transport(
        catalog: InstalledCapabilityHttpEndpointCatalog,
        secrets: Arc<dyn SecretMaterialResolver>,
        dns: Arc<dyn EgressDnsResolver>,
        transport: Arc<dyn PinnedCapabilityHttpTransport>,
        limits: CapabilityHttpEgressLimits,
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

    fn register(
        &self,
        identity: CapabilityTransportRequestIdentity,
    ) -> Result<CapabilityInFlightRegistration, CapabilityAdapterFailure> {
        let permit = self
            .permits
            .clone()
            .try_acquire_owned()
            .map_err(|_| before_dispatch("capability_egress_capacity", true))?;
        let cancellation = CancellationToken::new();
        let mut active = self
            .active
            .lock()
            .map_err(|_| before_dispatch("capability_egress_registry_unavailable", false))?;
        if active.contains_key(&identity) {
            return Err(before_dispatch(
                "capability_egress_duplicate_request",
                false,
            ));
        }
        active.insert(identity.clone(), cancellation.clone());
        drop(active);
        Ok(CapabilityInFlightRegistration {
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
            .map_err(|_| before_dispatch("capability_egress_invalid_endpoint", false))?;
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
                        return Err(before_dispatch("capability_egress_cancelled", false));
                    }
                    result = self.dns.resolve(&host, endpoint.port) => result,
                }
                .map_err(|failure| match failure {
                    DnsResolutionError::Unavailable => {
                        before_dispatch("capability_egress_dns_unavailable", true)
                    }
                    DnsResolutionError::NoAddresses | DnsResolutionError::TooManyAddresses => {
                        before_dispatch("capability_egress_dns_rejected", false)
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
            return Err(before_dispatch(
                "capability_egress_destination_denied",
                false,
            ));
        }
        Ok((dns_host, addresses))
    }

    async fn resolve_headers(
        &self,
        request: &HttpTransportRequest,
        entry: &InstalledCapabilityHttpEndpoint,
        cancellation: &CancellationToken,
    ) -> Result<HeaderMap, CapabilityAdapterFailure> {
        let mut headers = HeaderMap::new();
        for header in &request.headers {
            header
                .validate()
                .map_err(|_| before_dispatch("capability_egress_invalid_request_header", false))?;
            let name = HeaderName::from_bytes(header.name.as_bytes())
                .map_err(|_| before_dispatch("capability_egress_invalid_request_header", false))?;
            let value = HeaderValue::from_str(&header.value)
                .map_err(|_| before_dispatch("capability_egress_invalid_request_header", false))?;
            if headers.insert(name, value).is_some() {
                return Err(before_dispatch(
                    "capability_egress_duplicate_request_header",
                    false,
                ));
            }
        }
        if let Some(idempotency) = &request.idempotency {
            let name = HeaderName::from_bytes(idempotency.header_name.as_bytes())
                .map_err(|_| before_dispatch("capability_egress_invalid_idempotency", false))?;
            let value = HeaderValue::from_str(idempotency.value_digest.as_str())
                .map_err(|_| before_dispatch("capability_egress_invalid_idempotency", false))?;
            if headers.insert(name, value).is_some() {
                return Err(before_dispatch(
                    "capability_egress_duplicate_request_header",
                    false,
                ));
            }
        }
        for injection in &entry.credential_injections {
            let binding = request
                .secret_bindings
                .iter()
                .find(|binding| binding.purpose == *injection.purpose())
                .ok_or_else(|| {
                    before_dispatch("capability_egress_secret_binding_missing", false)
                })?;
            let resolved = tokio::select! {
                biased;
                _ = cancellation.cancelled() => {
                    return Err(before_dispatch("capability_egress_cancelled", false));
                }
                result = self.secrets.resolve(&request.identity.tenant_id, binding) => result,
            }
            .map_err(|failure| match failure {
                SecretMaterialResolutionError::Unavailable => {
                    before_dispatch("capability_egress_secret_unavailable", true)
                }
                SecretMaterialResolutionError::NotFound
                | SecretMaterialResolutionError::Revoked
                | SecretMaterialResolutionError::InvalidEvidence => {
                    before_dispatch("capability_egress_secret_rejected", false)
                }
            })?;
            if !resolved.validate_for(binding, self.limits.maximum_secret_material_bytes) {
                return Err(before_dispatch("capability_egress_secret_rejected", false));
            }
            insert_credential(&mut headers, injection, &resolved)?;
        }
        Ok(headers)
    }
}

#[async_trait]
impl HttpNetworkTransport for ReqwestCapabilityHttpEgressTransport {
    async fn round_trip(
        &self,
        request: HttpTransportRequest,
    ) -> Result<HttpTransportResponse, CapabilityAdapterFailure> {
        request
            .validate_at(Utc::now())
            .map_err(|_| before_dispatch("capability_egress_invalid_transport_request", false))?;
        let entry = self.catalog.resolve(&request)?;
        let registration = self.register(request.identity.clone())?;
        let (dns_host, addresses) = self
            .resolve_addresses(&entry.endpoint, &registration.cancellation)
            .await?;
        let headers = self
            .resolve_headers(&request, &entry, &registration.cancellation)
            .await?;
        let now = Utc::now();
        let remaining = (request.deadline - now)
            .to_std()
            .map_err(|_| before_dispatch("capability_egress_deadline_elapsed", false))?;
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
            return Err(before_dispatch("capability_egress_deadline_elapsed", false));
        }
        let evidence_digest =
            transport_evidence_digest(&entry, &addresses, &request.admission_digest);
        let response = self
            .transport
            .round_trip(PinnedCapabilityHttpRequest {
                method: http_method(request.method),
                url: capability_url(&entry.endpoint)
                    .map_err(|_| before_dispatch("capability_egress_invalid_endpoint", false))?,
                dns_host,
                addresses: addresses.clone(),
                headers,
                body: request.body,
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
        Ok(HttpTransportResponse {
            status: response.status,
            headers: response.headers,
            body: response.body,
            transport_evidence_digest: evidence_digest,
        })
    }

    async fn cancel(
        &self,
        request: CapabilityTransportCancelRequest,
    ) -> Result<CapabilityTransportCancelOutcome, CapabilityAdapterFailure> {
        request
            .validate_at(Utc::now())
            .map_err(|_| before_dispatch("capability_egress_invalid_cancel", false))?;
        if request.identity.backend_kind != CapabilityBackendKind::Http {
            return Err(before_dispatch("capability_egress_invalid_cancel", false));
        }
        let cancellation = self
            .active
            .lock()
            .map_err(|_| before_dispatch("capability_egress_registry_unavailable", false))?
            .get(&request.identity)
            .cloned();
        let Some(cancellation) = cancellation else {
            return Ok(CapabilityTransportCancelOutcome::AlreadyTerminal);
        };
        cancellation.cancel();
        Ok(CapabilityTransportCancelOutcome::Accepted)
    }
}

fn insert_credential(
    headers: &mut HeaderMap,
    injection: &InstalledHttpCredentialInjection,
    credential: &ResolvedSecretMaterial,
) -> Result<(), CapabilityAdapterFailure> {
    let (name, raw) = match injection {
        InstalledHttpCredentialInjection::BearerAuthorization { .. } => {
            let mut raw = Vec::with_capacity(7 + credential.material.as_bytes().len());
            raw.extend_from_slice(b"Bearer ");
            raw.extend_from_slice(credential.material.as_bytes());
            (HeaderName::from_static("authorization"), raw)
        }
        InstalledHttpCredentialInjection::Header { name, .. } => (
            HeaderName::from_bytes(name.as_bytes())
                .map_err(|_| before_dispatch("capability_egress_invalid_credential", false))?,
            credential.material.as_bytes().to_vec(),
        ),
    };
    let parsed = HeaderValue::from_bytes(&raw);
    let mut raw = raw;
    raw.fill(0);
    let mut value =
        parsed.map_err(|_| before_dispatch("capability_egress_invalid_credential", false))?;
    value.set_sensitive(true);
    if headers.insert(name, value).is_some() {
        return Err(before_dispatch(
            "capability_egress_duplicate_request_header",
            false,
        ));
    }
    Ok(())
}

fn http_method(method: HttpCapabilityMethod) -> Method {
    match method {
        HttpCapabilityMethod::Get => Method::GET,
        HttpCapabilityMethod::Post => Method::POST,
        HttpCapabilityMethod::Put => Method::PUT,
        HttpCapabilityMethod::Patch => Method::PATCH,
        HttpCapabilityMethod::Delete => Method::DELETE,
    }
}

fn capability_url(endpoint: &CanonicalHttpEndpoint) -> Result<Url, EgressConfigurationError> {
    let raw = format!(
        "https://{}:{}{}",
        endpoint.host, endpoint.port, endpoint.base_path
    );
    let url = Url::parse(&raw).map_err(|_| EgressConfigurationError::InvalidEndpoint)?;
    if url.scheme() != "https"
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || url.port_or_known_default() != Some(endpoint.port)
    {
        return Err(EgressConfigurationError::InvalidEndpoint);
    }
    Ok(url)
}

fn transport_evidence_digest(
    entry: &InstalledCapabilityHttpEndpoint,
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
    }))
    .expect("closed transport evidence is canonical")
    .parse()
    .expect("canonical transport evidence is SHA-256")
}

fn before_dispatch(code: &str, retryable: bool) -> CapabilityAdapterFailure {
    CapabilityAdapterFailure {
        class: if retryable {
            CapabilityAdapterFailureClass::RetryableBeforeDispatch
        } else {
            CapabilityAdapterFailureClass::RejectedBeforeDispatch
        },
        safe_code: code.to_owned(),
        safe_message: "Capability Egress rejected the request before dispatch".to_owned(),
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
        safe_message: "Capability Egress could not prove the remote outcome".to_owned(),
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
        safe_message: "Capability Egress could not prove the remote outcome".to_owned(),
        evidence_digest: static_digest(code),
        external_identity_digest: Some(external_identity_digest),
    }
}

fn static_digest(code: &str) -> Sha256Digest {
    canonical_digest(&serde_json::json!({"domain": code, "schema_version": 1}))
        .expect("static Capability Egress evidence is canonical")
        .parse()
        .expect("canonical Capability Egress evidence is SHA-256")
}

#[cfg(test)]
mod tests {
    use super::*;
    use insight_platform_capability_adapters::{
        CapabilityTransportRequestIdentity, HttpIdempotencyBinding,
    };
    use insight_platform_contracts::{ResourceId, ResourceKind, SecretResolutionPolicy};
    use std::{
        net::Ipv4Addr,
        str::FromStr,
        sync::{
            atomic::{AtomicBool, AtomicUsize, Ordering},
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
            SecretPurpose::from_str("service.api_key").unwrap(),
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
        entry: InstalledCapabilityHttpEndpoint,
        request: HttpTransportRequest,
    }

    fn fixture() -> Fixture {
        let endpoint = CanonicalHttpEndpoint {
            scheme: CapabilityEndpointScheme::Https,
            host: "api.example.com".to_owned(),
            port: 443,
            base_path: "/v1/invoke".to_owned(),
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
            entry: InstalledCapabilityHttpEndpoint {
                schema_version: 1,
                capability_deployment: deployment.clone(),
                backend_contract_digest: digest('5'),
                effect: Effect::ReadOnly,
                idempotency_kind: CapabilityIdempotencyKind::CallerKey,
                method: HttpCapabilityMethod::Post,
                endpoint: endpoint.clone(),
                endpoint_identity_digest: endpoint.canonical_digest().unwrap(),
                network_policy: network_policy.clone(),
                tls_policy: tls_policy.clone(),
                trust_policy: trust_policy.clone(),
                secret_bindings: vec![binding.clone()],
                credential_injections: vec![
                    InstalledHttpCredentialInjection::BearerAuthorization {
                        purpose: binding.purpose.clone(),
                    },
                ],
                limits,
            },
            request: HttpTransportRequest {
                identity: CapabilityTransportRequestIdentity {
                    backend_kind: CapabilityBackendKind::Http,
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
                method: HttpCapabilityMethod::Post,
                endpoint: endpoint.clone(),
                endpoint_identity_digest: endpoint.canonical_digest().unwrap(),
                network_policy,
                tls_policy,
                trust_policy,
                secret_bindings: vec![binding],
                idempotency: Some(HttpIdempotencyBinding {
                    header_name: "idempotency-key".to_owned(),
                    value_digest: digest('7'),
                }),
                limits,
                headers: vec![SafeHttpHeader {
                    name: "content-type".to_owned(),
                    value: "application/json".to_owned(),
                }],
                body: br#"{"query":"ready"}"#.to_vec(),
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
        validated: AtomicBool,
        observed_evidence: Mutex<Option<Sha256Digest>>,
    }

    impl FixtureTransport {
        fn new(mode: FixtureMode) -> Self {
            Self {
                mode,
                started: Notify::new(),
                validated: AtomicBool::new(false),
                observed_evidence: Mutex::new(None),
            }
        }
    }

    #[async_trait]
    impl PinnedCapabilityHttpTransport for FixtureTransport {
        async fn round_trip(
            &self,
            request: PinnedCapabilityHttpRequest,
        ) -> Result<PinnedCapabilityHttpResponse, CapabilityAdapterFailure> {
            assert_eq!(request.method, Method::POST);
            assert_eq!(request.url.as_str(), "https://api.example.com/v1/invoke");
            assert_eq!(request.dns_host, "api.example.com");
            assert_eq!(
                request.addresses,
                vec![SocketAddr::new(Ipv4Addr::new(8, 8, 8, 8).into(), 443)]
            );
            assert_eq!(request.body, br#"{"query":"ready"}"#);
            assert_eq!(
                request.headers.get("authorization").unwrap().as_bytes(),
                b"Bearer top-secret"
            );
            assert!(request.headers.get("authorization").unwrap().is_sensitive());
            if request.idempotency_kind != CapabilityIdempotencyKind::None {
                assert_eq!(
                    request.headers.get("idempotency-key").unwrap().as_bytes(),
                    digest('7').as_str().as_bytes()
                );
            } else {
                assert!(request.headers.get("idempotency-key").is_none());
            }
            *self.observed_evidence.lock().unwrap() =
                Some(request.transport_evidence_digest.clone());
            self.validated.store(true, Ordering::SeqCst);
            self.started.notify_waiters();
            match self.mode {
                FixtureMode::Complete => Ok(PinnedCapabilityHttpResponse {
                    status: 200,
                    headers: vec![SafeHttpHeader {
                        name: "content-type".to_owned(),
                        value: "application/json".to_owned(),
                    }],
                    body: br#"{"ok":true}"#.to_vec(),
                }),
                FixtureMode::WaitForCancel => {
                    request.cancellation.cancelled().await;
                    Err(uncertain_after_dispatch(
                        "capability_egress_cancelled",
                        false,
                        request.transport_evidence_digest,
                    ))
                }
                FixtureMode::FailAfterDispatch => Err(dispatch_failure(
                    "capability_egress_transport_failed",
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
    ) -> Arc<ReqwestCapabilityHttpEgressTransport> {
        Arc::new(
            ReqwestCapabilityHttpEgressTransport::with_transport(
                InstalledCapabilityHttpEndpointCatalog::new(vec![fixture.entry.clone()]).unwrap(),
                secrets,
                dns,
                backend,
                CapabilityHttpEgressLimits {
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

    #[tokio::test]
    async fn exact_catalog_dns_secret_and_bounded_transport_are_composed() {
        let fixture = fixture();
        let secrets = secrets();
        let dns = public_dns();
        let backend = Arc::new(FixtureTransport::new(FixtureMode::Complete));
        let transport = build_transport(&fixture, secrets.clone(), dns.clone(), backend.clone());

        let response = transport.round_trip(fixture.request.clone()).await.unwrap();
        assert_eq!(response.status, 200);
        assert_eq!(response.body, br#"{"ok":true}"#);
        assert_eq!(
            response.transport_evidence_digest,
            backend.observed_evidence.lock().unwrap().clone().unwrap()
        );
        assert_eq!(secrets.calls.load(Ordering::SeqCst), 1);
        assert_eq!(dns.calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn catalog_drift_and_private_dns_fail_before_secret_resolution() {
        let fixture = fixture();
        let secrets = secrets();
        let dns = public_dns();
        let backend = Arc::new(FixtureTransport::new(FixtureMode::Complete));
        let transport = build_transport(&fixture, secrets.clone(), dns.clone(), backend);
        let mut drifted = fixture.request.clone();
        drifted.backend_contract_digest = digest('0');
        let failure = transport.round_trip(drifted).await.unwrap_err();
        assert_eq!(
            failure.safe_code,
            "capability_egress_endpoint_not_installed"
        );
        assert_eq!(dns.calls.load(Ordering::SeqCst), 0);
        assert_eq!(secrets.calls.load(Ordering::SeqCst), 0);

        let private_dns = Arc::new(FixtureDns {
            addresses: vec![SocketAddr::new(Ipv4Addr::new(10, 0, 0, 1).into(), 443)],
            calls: AtomicUsize::new(0),
        });
        let transport = build_transport(
            &fixture,
            secrets.clone(),
            private_dns,
            Arc::new(FixtureTransport::new(FixtureMode::Complete)),
        );
        let failure = transport
            .round_trip(fixture.request.clone())
            .await
            .unwrap_err();
        assert_eq!(failure.safe_code, "capability_egress_destination_denied");
        assert_eq!(secrets.calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn cancel_is_exact_to_worker_generation_and_live_request() {
        let fixture = fixture();
        let backend = Arc::new(FixtureTransport::new(FixtureMode::WaitForCancel));
        let transport = build_transport(&fixture, secrets(), public_dns(), backend.clone());
        let request = fixture.request.clone();
        let task_transport = transport.clone();
        let task = tokio::spawn(async move { task_transport.round_trip(request).await });
        backend.started.notified().await;

        let mut stale = fixture.request.identity.clone();
        stale.worker_process_generation_id = id(ResourceKind::WorkerProcessGeneration, 99);
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
                    identity: fixture.request.identity.clone(),
                    deadline: Utc::now() + chrono::Duration::seconds(1),
                })
                .await
                .unwrap(),
            CapabilityTransportCancelOutcome::Accepted
        );
        let failure = task.await.unwrap().unwrap_err();
        assert_eq!(failure.safe_code, "capability_egress_cancelled");
        assert_eq!(
            transport
                .cancel(CapabilityTransportCancelRequest {
                    identity: fixture.request.identity,
                    deadline: Utc::now() + chrono::Duration::seconds(1),
                })
                .await
                .unwrap(),
            CapabilityTransportCancelOutcome::AlreadyTerminal
        );
    }

    #[tokio::test]
    async fn after_dispatch_retry_depends_on_frozen_effect_and_idempotency() {
        let read_fixture = fixture();
        let transport = build_transport(
            &read_fixture,
            secrets(),
            public_dns(),
            Arc::new(FixtureTransport::new(FixtureMode::FailAfterDispatch)),
        );
        let failure = transport
            .round_trip(read_fixture.request.clone())
            .await
            .unwrap_err();
        assert_eq!(
            failure.class,
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
        let failure = transport
            .round_trip(write_fixture.request)
            .await
            .unwrap_err();
        assert_eq!(failure.class, CapabilityAdapterFailureClass::Uncertain);
        assert!(failure.external_identity_digest.is_some());
    }
}
