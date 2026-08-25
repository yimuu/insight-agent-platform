use super::{
    capability_http::{capability_url, insert_credential, InstalledHttpCredentialInjection},
    is_public_destination_ip, parse_endpoint_host, DnsResolutionError, EgressConfigurationError,
    EgressDnsResolver, ParsedEndpointHost, SecretMaterialResolutionError, SecretMaterialResolver,
    MAX_DNS_ANSWERS_HARD, MAX_EGRESS_IN_FLIGHT_HARD, MAX_SECRET_MATERIAL_BYTES_HARD,
};
use async_trait::async_trait;
use chrono::Utc;
use futures::StreamExt;
use insight_platform_context::{
    RemoteContextFailure, RemoteContextFailureClass, RemoteContextItem,
    RemoteContextSearchConnector, RemoteContextSearchRequest, RemoteContextSearchResponse,
    REMOTE_CONTEXT_PROTOCOL_VERSION,
};
use insight_platform_contracts::{
    canonical_digest, canonical_json, parse_strict_json, CapabilityEndpointScheme,
    ExactDeploymentRef, ExactSecretBindingRef, ExactVersionRef, JsonLimits, ResourceKind,
    Sha256Digest,
};
use reqwest::header::{
    HeaderMap, HeaderValue, ACCEPT, CONTENT_ENCODING, CONTENT_LENGTH, CONTENT_TYPE,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    net::SocketAddr,
    sync::Arc,
    time::Duration,
};
use tokio::sync::Semaphore;

pub const MAX_INSTALLED_REMOTE_CONTEXT_ENDPOINTS: usize = 4_096;
pub const MAX_REMOTE_CONTEXT_TRUST_BUNDLE_BYTES: usize = 262_144;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RemoteContextEgressLimits {
    pub maximum_in_flight: usize,
    pub maximum_dns_answers: usize,
    pub maximum_secret_material_bytes: usize,
    pub connect_timeout_milliseconds: u64,
    pub first_byte_timeout_milliseconds: u64,
    pub idle_timeout_milliseconds: u64,
}

impl RemoteContextEgressLimits {
    pub fn validate(self) -> Result<(), EgressConfigurationError> {
        if self.maximum_in_flight == 0
            || self.maximum_in_flight > MAX_EGRESS_IN_FLIGHT_HARD
            || self.maximum_dns_answers == 0
            || self.maximum_dns_answers > MAX_DNS_ANSWERS_HARD
            || self.maximum_secret_material_bytes == 0
            || self.maximum_secret_material_bytes > MAX_SECRET_MATERIAL_BYTES_HARD
            || self.connect_timeout_milliseconds == 0
            || self.first_byte_timeout_milliseconds == 0
            || self.idle_timeout_milliseconds == 0
        {
            return Err(EgressConfigurationError::InvalidLimits);
        }
        Ok(())
    }
}

impl Default for RemoteContextEgressLimits {
    fn default() -> Self {
        Self {
            maximum_in_flight: 128,
            maximum_dns_answers: 16,
            maximum_secret_material_bytes: 8_192,
            connect_timeout_milliseconds: 5_000,
            first_byte_timeout_milliseconds: 15_000,
            idle_timeout_milliseconds: 10_000,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstalledRemoteContextEndpoint {
    pub schema_version: u32,
    pub context_deployment: ExactDeploymentRef,
    pub implementation_revision: ExactVersionRef,
    pub protocol_contract_digest: Sha256Digest,
    pub result_mapping_digest: Sha256Digest,
    pub endpoint: insight_platform_contracts::CanonicalHttpEndpoint,
    pub endpoint_identity_digest: Sha256Digest,
    pub region: insight_platform_contracts::DataRegion,
    pub network_policy: ExactVersionRef,
    pub tls_policy: ExactVersionRef,
    pub trust_policy: ExactVersionRef,
    pub secret_bindings: Vec<ExactSecretBindingRef>,
    pub credential_injections: Vec<InstalledHttpCredentialInjection>,
    pub trusted_root_pem: String,
    pub maximum_request_bytes: u32,
    pub maximum_response_bytes: u32,
}

impl InstalledRemoteContextEndpoint {
    pub fn validate(&self) -> Result<(), EgressConfigurationError> {
        self.context_deployment
            .validate()
            .map_err(|_| EgressConfigurationError::InvalidEndpoint)?;
        self.implementation_revision
            .validate()
            .map_err(|_| EgressConfigurationError::InvalidEndpoint)?;
        self.endpoint
            .validate()
            .map_err(|_| EgressConfigurationError::InvalidEndpoint)?;
        if self.schema_version != REMOTE_CONTEXT_PROTOCOL_VERSION
            || self.context_deployment.resource_kind != ResourceKind::ContextDeployment
            || self.implementation_revision.resource_kind
                != ResourceKind::ContextSourceImplementationRevision
            || self.endpoint.scheme != CapabilityEndpointScheme::Https
            || self.endpoint.canonical_digest().as_ref() != Ok(&self.endpoint_identity_digest)
            || parse_endpoint_host(&self.endpoint.host).is_err()
            || self.maximum_request_bytes == 0
            || self.maximum_response_bytes == 0
            || self.trusted_root_pem.is_empty()
            || self.trusted_root_pem.len() > MAX_REMOTE_CONTEXT_TRUST_BUNDLE_BYTES
            || reqwest::Certificate::from_pem(self.trusted_root_pem.as_bytes()).is_err()
        {
            return Err(EgressConfigurationError::InvalidEndpoint);
        }
        let policies = [&self.network_policy, &self.tls_policy, &self.trust_policy];
        let mut policy_ids = BTreeSet::new();
        if policies.iter().any(|policy| {
            policy.validate().is_err()
                || policy.resource_kind != ResourceKind::PolicyRevision
                || !policy_ids.insert(policy.revision_id.clone())
        }) {
            return Err(EgressConfigurationError::InvalidEndpoint);
        }
        let mut purposes = BTreeSet::new();
        for binding in &self.secret_bindings {
            binding
                .validate()
                .map_err(|_| EgressConfigurationError::InvalidEndpoint)?;
            if !purposes.insert(binding.purpose.clone()) {
                return Err(EgressConfigurationError::InvalidEndpoint);
            }
        }
        if self.secret_bindings.len() != self.credential_injections.len()
            || self.credential_injections.iter().any(|injection| {
                injection.validate().is_err()
                    || self
                        .secret_bindings
                        .iter()
                        .filter(|binding| binding.purpose == *injection.purpose())
                        .count()
                        != 1
            })
        {
            return Err(EgressConfigurationError::InvalidEndpoint);
        }
        capability_url(&self.endpoint)?;
        Ok(())
    }

    fn matches(&self, request: &RemoteContextSearchRequest) -> bool {
        request.context_deployment == self.context_deployment
            && request.implementation_revision == self.implementation_revision
            && request.protocol_contract_digest == self.protocol_contract_digest
            && request.result_mapping_digest == self.result_mapping_digest
            && request.endpoint == self.endpoint
            && request.endpoint_identity_digest == self.endpoint_identity_digest
            && request.region == self.region
            && request.network_policy == self.network_policy
            && request.tls_policy == self.tls_policy
            && request.trust_policy == self.trust_policy
            && request.secret_bindings == self.secret_bindings
            && request.maximum_response_bytes <= self.maximum_response_bytes
    }
}

#[derive(Debug, Clone)]
pub struct InstalledRemoteContextEndpointCatalog {
    entries: BTreeMap<
        (insight_platform_contracts::ResourceId, Sha256Digest),
        InstalledRemoteContextEndpoint,
    >,
}

impl InstalledRemoteContextEndpointCatalog {
    pub fn new(
        entries: Vec<InstalledRemoteContextEndpoint>,
    ) -> Result<Self, EgressConfigurationError> {
        if entries.is_empty() || entries.len() > MAX_INSTALLED_REMOTE_CONTEXT_ENDPOINTS {
            return Err(EgressConfigurationError::InvalidEndpointCatalog);
        }
        let mut catalog = BTreeMap::new();
        for entry in entries {
            entry.validate()?;
            let key = (
                entry.context_deployment.deployment_id.clone(),
                entry.context_deployment.deployment_digest.clone(),
            );
            if catalog.insert(key, entry).is_some() {
                return Err(EgressConfigurationError::DuplicateEndpoint);
            }
        }
        Ok(Self { entries: catalog })
    }

    fn resolve(
        &self,
        request: &RemoteContextSearchRequest,
    ) -> Result<InstalledRemoteContextEndpoint, RemoteContextFailure> {
        self.entries
            .get(&(
                request.context_deployment.deployment_id.clone(),
                request.context_deployment.deployment_digest.clone(),
            ))
            .filter(|entry| entry.matches(request))
            .cloned()
            .ok_or_else(|| before_dispatch("context_egress_endpoint_not_installed", false))
    }
}

pub struct ReqwestRemoteContextSearchConnector {
    catalog: InstalledRemoteContextEndpointCatalog,
    secrets: Arc<dyn SecretMaterialResolver>,
    dns: Arc<dyn EgressDnsResolver>,
    limits: RemoteContextEgressLimits,
    permits: Arc<Semaphore>,
    #[cfg(test)]
    allow_loopback_for_protocol_fixture: bool,
}

impl ReqwestRemoteContextSearchConnector {
    pub fn new(
        catalog: InstalledRemoteContextEndpointCatalog,
        secrets: Arc<dyn SecretMaterialResolver>,
        dns: Arc<dyn EgressDnsResolver>,
        limits: RemoteContextEgressLimits,
    ) -> Result<Self, EgressConfigurationError> {
        limits.validate()?;
        Ok(Self {
            catalog,
            secrets,
            dns,
            limits,
            permits: Arc::new(Semaphore::new(limits.maximum_in_flight)),
            #[cfg(test)]
            allow_loopback_for_protocol_fixture: false,
        })
    }

    #[cfg(test)]
    fn allow_loopback_for_protocol_fixture(mut self) -> Self {
        self.allow_loopback_for_protocol_fixture = true;
        self
    }

    fn destination_allowed(&self, address: &SocketAddr) -> bool {
        if is_public_destination_ip(address.ip()) {
            return true;
        }
        #[cfg(test)]
        if self.allow_loopback_for_protocol_fixture && address.ip().is_loopback() {
            return true;
        }
        false
    }

    async fn addresses(
        &self,
        entry: &InstalledRemoteContextEndpoint,
    ) -> Result<(String, Vec<SocketAddr>), RemoteContextFailure> {
        let host = parse_endpoint_host(&entry.endpoint.host)
            .map_err(|_| before_dispatch("context_egress_invalid_endpoint", false))?;
        let (dns_host, mut addresses) = match host {
            ParsedEndpointHost::Address(address) => (
                address.to_string(),
                vec![SocketAddr::new(address, entry.endpoint.port)],
            ),
            ParsedEndpointHost::Name(host) => {
                let addresses =
                    self.dns
                        .resolve(&host, entry.endpoint.port)
                        .await
                        .map_err(|failure| match failure {
                            DnsResolutionError::Unavailable => {
                                before_dispatch("context_egress_dns_unavailable", true)
                            }
                            DnsResolutionError::NoAddresses
                            | DnsResolutionError::TooManyAddresses => {
                                before_dispatch("context_egress_dns_rejected", false)
                            }
                        })?;
                (host, addresses)
            }
        };
        addresses.sort_unstable();
        addresses.dedup();
        if addresses.is_empty()
            || addresses.len() > self.limits.maximum_dns_answers
            || addresses.iter().any(|address| {
                address.port() != entry.endpoint.port || !self.destination_allowed(address)
            })
        {
            return Err(before_dispatch("context_egress_destination_denied", false));
        }
        Ok((dns_host, addresses))
    }

    async fn headers(
        &self,
        request: &RemoteContextSearchRequest,
        entry: &InstalledRemoteContextEndpoint,
    ) -> Result<HeaderMap, RemoteContextFailure> {
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        headers.insert(ACCEPT, HeaderValue::from_static("application/json"));
        for injection in &entry.credential_injections {
            let binding = request
                .secret_bindings
                .iter()
                .find(|binding| binding.purpose == *injection.purpose())
                .ok_or_else(|| before_dispatch("context_egress_secret_binding_missing", false))?;
            let resolved = self
                .secrets
                .resolve(&request.tenant_id, binding)
                .await
                .map_err(|failure| match failure {
                    SecretMaterialResolutionError::Unavailable => {
                        before_dispatch("context_egress_secret_unavailable", true)
                    }
                    SecretMaterialResolutionError::NotFound
                    | SecretMaterialResolutionError::Revoked
                    | SecretMaterialResolutionError::InvalidEvidence => {
                        before_dispatch("context_egress_secret_rejected", false)
                    }
                })?;
            if !resolved.validate_for(binding, self.limits.maximum_secret_material_bytes) {
                return Err(before_dispatch("context_egress_secret_rejected", false));
            }
            insert_credential(&mut headers, injection, &resolved)
                .map_err(|_| before_dispatch("context_egress_invalid_credential", false))?;
        }
        Ok(headers)
    }
}

#[derive(Serialize)]
#[serde(deny_unknown_fields)]
struct RemoteSearchWireRequest<'a> {
    schema_version: u32,
    query: &'a serde_json::Value,
    normalized_query_digest: &'a Sha256Digest,
    normalized_filter_digest: &'a Sha256Digest,
    requested_projection: &'a [String],
    page_size: u32,
    cursor_digest: &'a Option<Sha256Digest>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RemoteSearchWireResponse {
    schema_version: u32,
    items: Vec<RemoteSearchWireItem>,
    next_cursor_digest: Option<Sha256Digest>,
    remote_revision_digest: Option<Sha256Digest>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RemoteSearchWireItem {
    source_identity: String,
    content: serde_json::Value,
    structured_fields: serde_json::Value,
    score_millionths: Option<i32>,
    locator: String,
    display_label: String,
    classification: insight_platform_contracts::DataClassification,
}

fn encode_remote_search_body(
    request: &RemoteContextSearchRequest,
    maximum_request_bytes: u32,
) -> Result<Vec<u8>, RemoteContextFailure> {
    let query = match &request.query_input {
        insight_platform_contracts::ValueRef::Inline { value } => value,
        insight_platform_contracts::ValueRef::Artifact { .. } => {
            return Err(before_dispatch(
                "context_egress_artifact_input_unsupported",
                false,
            ));
        }
    };
    let body = canonical_json(
        &serde_json::to_value(RemoteSearchWireRequest {
            schema_version: REMOTE_CONTEXT_PROTOCOL_VERSION,
            query,
            normalized_query_digest: &request.normalized_query_digest,
            normalized_filter_digest: &request.normalized_filter_digest,
            requested_projection: &request.requested_projection,
            page_size: request.page_size,
            cursor_digest: &request.cursor_digest,
        })
        .map_err(|_| before_dispatch("context_egress_request_invalid", false))?,
    )
    .map_err(|_| before_dispatch("context_egress_request_invalid", false))?;
    if body.len() > maximum_request_bytes as usize {
        return Err(before_dispatch("context_egress_request_too_large", false));
    }
    Ok(body)
}

fn normalize_remote_search_response(
    request: &RemoteContextSearchRequest,
    bytes: &[u8],
    evidence: Sha256Digest,
) -> Result<RemoteContextSearchResponse, RemoteContextFailure> {
    let response_digest: Sha256Digest = canonical_digest(
        &parse_strict_json(
            bytes,
            JsonLimits {
                max_bytes: request.maximum_response_bytes as usize,
                max_depth: 32,
                max_items_per_array: usize::try_from(request.page_size).unwrap_or(0),
                max_properties_per_object: 16,
                max_string_bytes: request.maximum_response_bytes as usize,
            },
        )
        .map_err(|_| after_dispatch("context_egress_response_invalid", false, evidence.clone()))?,
    )
    .map_err(|_| after_dispatch("context_egress_response_invalid", false, evidence.clone()))?
    .parse()
    .map_err(|_| after_dispatch("context_egress_response_invalid", false, evidence.clone()))?;
    let wire: RemoteSearchWireResponse = serde_json::from_slice(bytes)
        .map_err(|_| after_dispatch("context_egress_response_invalid", false, evidence.clone()))?;
    if wire.schema_version != REMOTE_CONTEXT_PROTOCOL_VERSION
        || wire.items.len() > request.page_size as usize
        || wire
            .items
            .iter()
            .any(|item| item.classification.rank() > request.maximum_classification.rank())
    {
        return Err(after_dispatch(
            "context_egress_response_invalid",
            false,
            evidence,
        ));
    }
    let authorization_evidence_digest = closed_digest(&serde_json::json!({
        "context_deployment": request.context_deployment,
        "network_policy": request.network_policy,
        "tls_policy": request.tls_policy,
        "trust_policy": request.trust_policy,
        "response_digest": response_digest,
    }));
    let items = wire
        .items
        .into_iter()
        .map(|item| RemoteContextItem {
            source_item_identity_digest: closed_digest(&item.source_identity),
            content: item.content,
            structured_fields: item.structured_fields,
            score_millionths: item.score_millionths,
            locator_digest: closed_digest(&item.locator),
            authorization_evidence_digest: authorization_evidence_digest.clone(),
            display_label: item.display_label,
            classification: item.classification,
        })
        .collect();
    let observed_at = Utc::now();
    let normalized = RemoteContextSearchResponse {
        schema_version: REMOTE_CONTEXT_PROTOCOL_VERSION,
        items,
        next_cursor_digest: wire.next_cursor_digest,
        backend_request_digest: request.normalized_query_digest.clone(),
        backend_response_digest: response_digest.clone(),
        ranking_evidence_digest: closed_digest(&serde_json::json!({
            "mapping": request.result_mapping_digest,
            "response": response_digest,
        })),
        remote_revision_digest: wire.remote_revision_digest,
        observed_at,
    };
    normalized
        .validate_for(request, observed_at)
        .map_err(|_| after_dispatch("context_egress_response_invalid", false, evidence))?;
    Ok(normalized)
}

#[async_trait]
impl RemoteContextSearchConnector for ReqwestRemoteContextSearchConnector {
    async fn query(
        &self,
        request: RemoteContextSearchRequest,
    ) -> Result<RemoteContextSearchResponse, RemoteContextFailure> {
        request
            .validate_at(Utc::now())
            .map_err(|_| before_dispatch("context_egress_request_invalid", false))?;
        let _permit = self
            .permits
            .clone()
            .try_acquire_owned()
            .map_err(|_| before_dispatch("context_egress_capacity", true))?;
        let entry = self.catalog.resolve(&request)?;
        let body = encode_remote_search_body(&request, entry.maximum_request_bytes)?;
        let headers = self.headers(&request, &entry).await?;
        let (dns_host, addresses) = self.addresses(&entry).await?;
        let root = reqwest::Certificate::from_pem(entry.trusted_root_pem.as_bytes())
            .map_err(|_| before_dispatch("context_egress_trust_invalid", false))?;
        let remaining = (request.deadline - Utc::now())
            .to_std()
            .map_err(|_| before_dispatch("context_egress_deadline_elapsed", false))?;
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .referer(false)
            .no_proxy()
            .https_only(true)
            .tls_certs_only([root])
            .connect_timeout(
                Duration::from_millis(self.limits.connect_timeout_milliseconds).min(remaining),
            )
            .timeout(remaining)
            .pool_max_idle_per_host(0)
            .resolve_to_addrs(&dns_host, &addresses)
            .build()
            .map_err(|_| before_dispatch("context_egress_client_build_failed", false))?;
        let outbound = client
            .post(
                capability_url(&entry.endpoint)
                    .map_err(|_| before_dispatch("context_egress_invalid_endpoint", false))?,
            )
            .headers(headers)
            .body(body)
            .build()
            .map_err(|_| before_dispatch("context_egress_request_build_failed", false))?;
        let evidence = transport_evidence(&request, &entry, &addresses);
        let response = tokio::time::timeout(
            Duration::from_millis(self.limits.first_byte_timeout_milliseconds).min(remaining),
            client.execute(outbound),
        )
        .await
        .map_err(|_| after_dispatch("context_egress_first_byte_timeout", true, evidence.clone()))?
        .map_err(|_| after_dispatch("context_egress_transport_failed", true, evidence.clone()))?;
        if response.status() != reqwest::StatusCode::OK
            || response.headers().get_all(CONTENT_LENGTH).iter().count() > 1
            || response
                .headers()
                .get(CONTENT_ENCODING)
                .is_some_and(|value| value.as_bytes() != b"identity")
            || response
                .content_length()
                .is_some_and(|length| length > u64::from(request.maximum_response_bytes))
        {
            return Err(after_dispatch(
                "context_egress_response_rejected",
                false,
                evidence,
            ));
        }
        let mut bytes = Vec::new();
        let mut stream = response.bytes_stream();
        while let Some(item) = tokio::time::timeout(
            Duration::from_millis(self.limits.idle_timeout_milliseconds).min(remaining),
            stream.next(),
        )
        .await
        .map_err(|_| after_dispatch("context_egress_idle_timeout", true, evidence.clone()))?
        {
            let item = item.map_err(|_| {
                after_dispatch("context_egress_response_failed", true, evidence.clone())
            })?;
            if bytes.len().saturating_add(item.len()) > request.maximum_response_bytes as usize {
                return Err(after_dispatch(
                    "context_egress_response_too_large",
                    false,
                    evidence,
                ));
            }
            bytes.extend_from_slice(&item);
        }
        normalize_remote_search_response(&request, &bytes, evidence)
    }
}

fn transport_evidence(
    request: &RemoteContextSearchRequest,
    entry: &InstalledRemoteContextEndpoint,
    addresses: &[SocketAddr],
) -> Sha256Digest {
    closed_digest(&serde_json::json!({
        "schema_version": 1,
        "context_query_id": request.context_query_id,
        "job_id": request.job_id,
        "physical_attempt": request.physical_attempt,
        "lease_generation": request.lease_generation,
        "endpoint_identity_digest": entry.endpoint_identity_digest,
        "tls_policy": entry.tls_policy,
        "trust_policy": entry.trust_policy,
        "pinned_addresses": addresses.iter().map(ToString::to_string).collect::<Vec<_>>(),
    }))
}

fn closed_digest<T: Serialize>(value: &T) -> Sha256Digest {
    let value = serde_json::to_value(value).expect("closed Egress evidence serializes");
    canonical_digest(&value)
        .expect("closed Egress evidence is canonical")
        .parse()
        .expect("canonical digest is SHA-256")
}

fn before_dispatch(code: &str, retryable: bool) -> RemoteContextFailure {
    RemoteContextFailure {
        code: code.to_owned(),
        class: if retryable {
            RemoteContextFailureClass::RetryableBeforeDispatch
        } else {
            RemoteContextFailureClass::RejectedBeforeDispatch
        },
        safe_message: "Remote Context Egress rejected the request before dispatch".to_owned(),
        dispatch_evidence_digest: None,
    }
}

fn after_dispatch(code: &str, retryable: bool, evidence: Sha256Digest) -> RemoteContextFailure {
    RemoteContextFailure {
        code: code.to_owned(),
        class: if retryable {
            RemoteContextFailureClass::RetryableAfterDispatch
        } else {
            RemoteContextFailureClass::PermanentAfterDispatch
        },
        safe_message: "Remote Context Egress failed after dispatch".to_owned(),
        dispatch_evidence_digest: Some(evidence),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration as ChronoDuration;
    use insight_platform_contracts::{
        CapabilityEndpointScheme, DataClassification, DataRegion, ResourceId, ValueRef,
    };
    use rcgen::{
        BasicConstraints, CertificateParams, CertifiedIssuer, ExtendedKeyUsagePurpose, IsCa,
        KeyPair, SanType,
    };
    use rustls::{pki_types::PrivatePkcs8KeyDer, ServerConfig};
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    use tokio_rustls::TlsAcceptor;

    fn id(kind: ResourceKind, suffix: u16) -> ResourceId {
        format!(
            "{}_0198f1c9-32e4-75e1-a9e8-d95ca0f6{suffix:04x}",
            kind.descriptor().prefix
        )
        .parse()
        .unwrap()
    }

    fn digest(marker: char) -> Sha256Digest {
        format!("sha256:{}", marker.to_string().repeat(64))
            .parse()
            .unwrap()
    }

    fn exact(kind: ResourceKind, suffix: u16, marker: char) -> ExactVersionRef {
        ExactVersionRef::new(id(kind, suffix), digest(marker)).unwrap()
    }

    fn root_pem() -> String {
        let mut parameters = CertificateParams::default();
        parameters.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        CertifiedIssuer::self_signed(parameters, KeyPair::generate().unwrap())
            .unwrap()
            .pem()
    }

    fn fixture() -> (InstalledRemoteContextEndpoint, RemoteContextSearchRequest) {
        let endpoint = insight_platform_contracts::CanonicalHttpEndpoint {
            scheme: CapabilityEndpointScheme::Https,
            host: "search.example.test".to_owned(),
            port: 443,
            base_path: "/v1/query".to_owned(),
        };
        let deployment =
            ExactDeploymentRef::new(id(ResourceKind::ContextDeployment, 1), digest('1')).unwrap();
        let implementation = exact(ResourceKind::ContextSourceImplementationRevision, 2, '2');
        let network = exact(ResourceKind::PolicyRevision, 3, '3');
        let tls = exact(ResourceKind::PolicyRevision, 4, '4');
        let trust = exact(ResourceKind::PolicyRevision, 5, '5');
        let installed = InstalledRemoteContextEndpoint {
            schema_version: REMOTE_CONTEXT_PROTOCOL_VERSION,
            context_deployment: deployment.clone(),
            implementation_revision: implementation.clone(),
            protocol_contract_digest: digest('6'),
            result_mapping_digest: digest('7'),
            endpoint_identity_digest: endpoint.canonical_digest().unwrap(),
            endpoint: endpoint.clone(),
            region: "cn-east-1".parse::<DataRegion>().unwrap(),
            network_policy: network.clone(),
            tls_policy: tls.clone(),
            trust_policy: trust.clone(),
            secret_bindings: vec![],
            credential_injections: vec![],
            trusted_root_pem: root_pem(),
            maximum_request_bytes: 65_536,
            maximum_response_bytes: 1_048_576,
        };
        let request = RemoteContextSearchRequest {
            schema_version: REMOTE_CONTEXT_PROTOCOL_VERSION,
            tenant_id: id(ResourceKind::Tenant, 6),
            context_query_id: id(ResourceKind::ContextQuery, 7),
            job_id: id(ResourceKind::Job, 8),
            physical_attempt: 1,
            lease_generation: 1,
            context_deployment: deployment,
            implementation_revision: implementation,
            protocol_contract_digest: digest('6'),
            result_mapping_digest: digest('7'),
            endpoint_identity_digest: endpoint.canonical_digest().unwrap(),
            endpoint,
            region: "cn-east-1".parse().unwrap(),
            secret_bindings: vec![],
            network_policy: network,
            tls_policy: tls,
            trust_policy: trust,
            query_input: ValueRef::Inline {
                value: serde_json::json!({"query": "bounded"}),
            },
            normalized_query_digest: digest('8'),
            normalized_filter_digest: digest('9'),
            requested_projection: vec!["title".to_owned()],
            maximum_classification: DataClassification::Confidential,
            page_size: 10,
            cursor_digest: None,
            maximum_response_bytes: 1_048_576,
            deadline: Utc::now() + ChronoDuration::minutes(1),
        };
        (installed, request)
    }

    struct EmptySecrets;

    #[async_trait]
    impl SecretMaterialResolver for EmptySecrets {
        async fn resolve(
            &self,
            _tenant_id: &ResourceId,
            _binding: &ExactSecretBindingRef,
        ) -> Result<crate::ResolvedSecretMaterial, SecretMaterialResolutionError> {
            Err(SecretMaterialResolutionError::NotFound)
        }
    }

    struct FixtureDns(SocketAddr);

    #[async_trait]
    impl EgressDnsResolver for FixtureDns {
        async fn resolve(
            &self,
            host: &str,
            port: u16,
        ) -> Result<Vec<SocketAddr>, DnsResolutionError> {
            assert_eq!(host, "search.example.test");
            assert_eq!(port, self.0.port());
            Ok(vec![self.0])
        }
    }

    async fn start_remote_search_https_fixture() -> (SocketAddr, String, tokio::task::JoinHandle<()>)
    {
        let mut ca_parameters = CertificateParams::default();
        ca_parameters.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        let ca = CertifiedIssuer::self_signed(ca_parameters, KeyPair::generate().unwrap()).unwrap();
        let mut server_parameters = CertificateParams::default();
        server_parameters.subject_alt_names =
            vec![SanType::DnsName("search.example.test".try_into().unwrap())];
        server_parameters.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
        let server_key = KeyPair::generate().unwrap();
        let server_certificate = server_parameters.signed_by(&server_key, &ca).unwrap();
        let tls = ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(
                vec![server_certificate.der().clone()],
                PrivatePkcs8KeyDer::from(server_key.serialize_der()).into(),
            )
            .unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut stream = TlsAcceptor::from(Arc::new(tls))
                .accept(stream)
                .await
                .unwrap();
            let mut request = Vec::new();
            let header_end = loop {
                let mut chunk = [0_u8; 1_024];
                let read = stream.read(&mut chunk).await.unwrap();
                assert!(read > 0);
                request.extend_from_slice(&chunk[..read]);
                if let Some(position) = request.windows(4).position(|part| part == b"\r\n\r\n") {
                    break position + 4;
                }
            };
            let headers = String::from_utf8(request[..header_end].to_vec()).unwrap();
            assert!(
                headers.starts_with("POST /v1/query HTTP/1.1\r\n"),
                "{headers}"
            );
            assert!(headers
                .to_ascii_lowercase()
                .contains("host: search.example.test:"));
            let content_length = headers
                .lines()
                .find_map(|line| {
                    line.to_ascii_lowercase()
                        .strip_prefix("content-length: ")
                        .map(str::to_owned)
                })
                .unwrap()
                .parse::<usize>()
                .unwrap();
            while request.len() - header_end < content_length {
                let mut chunk = [0_u8; 1_024];
                let read = stream.read(&mut chunk).await.unwrap();
                assert!(read > 0);
                request.extend_from_slice(&chunk[..read]);
            }
            let wire_request = parse_strict_json(
                &request[header_end..header_end + content_length],
                JsonLimits {
                    max_bytes: 65_536,
                    max_depth: 16,
                    max_items_per_array: 32,
                    max_properties_per_object: 16,
                    max_string_bytes: 16_384,
                },
            )
            .unwrap();
            assert_eq!(
                wire_request["query"],
                serde_json::json!({"query": "bounded"})
            );
            let body = canonical_json(&serde_json::json!({
                "schema_version": 1,
                "items": [{
                    "source_identity": "record-https-1",
                    "content": {"title": "TLS result"},
                    "structured_fields": {},
                    "score_millionths": 910000,
                    "locator": "opaque-https-1",
                    "display_label": "TLS result",
                    "classification": "internal"
                }],
                "next_cursor_digest": null,
                "remote_revision_digest": digest('c')
            }))
            .unwrap();
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                body.len()
            );
            stream.write_all(response.as_bytes()).await.unwrap();
            stream.write_all(&body).await.unwrap();
            stream.shutdown().await.unwrap();
        });
        (address, ca.pem(), task)
    }

    #[test]
    fn installed_remote_context_catalog_matches_the_complete_frozen_closure() {
        let (installed, request) = fixture();
        installed.validate().unwrap();
        let catalog = InstalledRemoteContextEndpointCatalog::new(vec![installed]).unwrap();
        catalog.resolve(&request).unwrap();

        let mut drifted = request;
        drifted.trust_policy = exact(ResourceKind::PolicyRevision, 9, 'a');
        assert!(matches!(
            catalog.resolve(&drifted),
            Err(RemoteContextFailure {
                class: RemoteContextFailureClass::RejectedBeforeDispatch,
                dispatch_evidence_digest: None,
                ..
            })
        ));
    }

    #[test]
    fn remote_search_wire_is_canonical_bounded_and_closed() {
        let (installed, request) = fixture();
        let body = encode_remote_search_body(&request, installed.maximum_request_bytes).unwrap();
        assert_eq!(
            body,
            canonical_json(&serde_json::json!({
                "cursor_digest": null,
                "normalized_filter_digest": request.normalized_filter_digest,
                "normalized_query_digest": request.normalized_query_digest,
                "page_size": 10,
                "query": {"query": "bounded"},
                "requested_projection": ["title"],
                "schema_version": 1
            }))
            .unwrap()
        );
        assert!(matches!(
            encode_remote_search_body(&request, 8),
            Err(RemoteContextFailure {
                code,
                class: RemoteContextFailureClass::RejectedBeforeDispatch,
                dispatch_evidence_digest: None,
                ..
            }) if code == "context_egress_request_too_large"
        ));

        let evidence = digest('a');
        let response = canonical_json(&serde_json::json!({
            "schema_version": 1,
            "items": [{
                "source_identity": "record-1",
                "content": {"title": "bounded result"},
                "structured_fields": {},
                "score_millionths": 900000,
                "locator": "opaque-record-1",
                "display_label": "bounded result",
                "classification": "internal"
            }],
            "next_cursor_digest": null,
            "remote_revision_digest": digest('b')
        }))
        .unwrap();
        let normalized =
            normalize_remote_search_response(&request, &response, evidence.clone()).unwrap();
        assert_eq!(normalized.items.len(), 1);
        assert_eq!(
            normalized.backend_request_digest,
            request.normalized_query_digest
        );
        assert_eq!(
            normalized.items[0].classification,
            DataClassification::Internal
        );

        let unknown = br#"{"items":[],"next_cursor_digest":null,"remote_revision_digest":null,"schema_version":1,"unexpected":true}"#;
        assert!(matches!(
            normalize_remote_search_response(&request, unknown, evidence.clone()),
            Err(RemoteContextFailure {
                code,
                class: RemoteContextFailureClass::PermanentAfterDispatch,
                dispatch_evidence_digest: Some(_),
                ..
            }) if code == "context_egress_response_invalid"
        ));
        let duplicate = br#"{"schema_version":1,"schema_version":1,"items":[],"next_cursor_digest":null,"remote_revision_digest":null}"#;
        assert!(normalize_remote_search_response(&request, duplicate, evidence).is_err());
    }

    #[tokio::test]
    async fn remote_search_https_last_hop_pins_dns_and_explicit_trust() {
        let (address, root_pem, server) = start_remote_search_https_fixture().await;
        let (mut installed, mut request) = fixture();
        installed.endpoint.port = address.port();
        installed.endpoint_identity_digest = installed.endpoint.canonical_digest().unwrap();
        installed.trusted_root_pem = root_pem;
        request.endpoint = installed.endpoint.clone();
        request.endpoint_identity_digest = installed.endpoint_identity_digest.clone();
        let connector = ReqwestRemoteContextSearchConnector::new(
            InstalledRemoteContextEndpointCatalog::new(vec![installed]).unwrap(),
            Arc::new(EmptySecrets),
            Arc::new(FixtureDns(address)),
            RemoteContextEgressLimits::default(),
        )
        .unwrap()
        .allow_loopback_for_protocol_fixture();
        let response = connector.query(request.clone()).await.unwrap();
        assert_eq!(response.items.len(), 1);
        assert_eq!(response.items[0].display_label, "TLS result");
        assert_eq!(
            response.backend_request_digest,
            request.normalized_query_digest
        );
        server.await.unwrap();
    }
}
