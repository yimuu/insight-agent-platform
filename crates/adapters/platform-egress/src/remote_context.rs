#[path = "remote_context_wire.rs"]
mod wire;

#[cfg(test)]
#[path = "remote_context_document_qualification.rs"]
mod document_qualification;

use super::{
    capability_http::{capability_url, insert_credential},
    is_public_destination_ip, parse_endpoint_host, DnsResolutionError, EgressCapacitySnapshot,
    EgressConfigurationError, EgressDnsResolver, ParsedEndpointHost, SecretMaterialResolutionError,
    SecretMaterialResolver, MAX_DNS_ANSWERS_HARD, MAX_EGRESS_IN_FLIGHT_HARD,
    MAX_SECRET_MATERIAL_BYTES_HARD,
};
use async_trait::async_trait;
use chrono::Utc;
use futures::StreamExt;
use insight_platform_context::{
    RemoteContextFailure, RemoteContextFailureClass, RemoteContextItem,
    RemoteContextSearchConnector, RemoteContextSearchRequest, RemoteContextSearchResponse,
    REMOTE_CONTEXT_PROTOCOL_VERSION,
};
use insight_platform_contracts::{canonical_digest, Sha256Digest};
use reqwest::header::{
    HeaderMap, HeaderValue, ACCEPT, CONTENT_ENCODING, CONTENT_LENGTH, CONTENT_TYPE,
};
use serde::{Deserialize, Serialize};
use std::{net::SocketAddr, sync::Arc, time::Duration};
use tokio::sync::Semaphore;

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

pub use insight_platform_contracts::InstalledRemoteContextDestinationV1;

#[derive(Debug, Clone)]
pub struct InstalledRemoteContextDestinationCatalog {
    entries: Vec<InstalledRemoteContextDestinationV1>,
}

fn validate_remote_context_roots(pem: &str) -> Result<(), EgressConfigurationError> {
    // With rustls, Certificate::from_pem only stores bytes; build the real trust store here.
    // Client construction performs no network request and installs no system trust.
    let roots = reqwest::Certificate::from_pem_bundle(pem.as_bytes())
        .map_err(|_| EgressConfigurationError::InvalidEndpoint)?;
    if roots.is_empty() {
        return Err(EgressConfigurationError::InvalidEndpoint);
    }
    reqwest::Client::builder()
        .no_proxy()
        .https_only(true)
        .tls_certs_only(roots)
        .build()
        .map_err(|_| EgressConfigurationError::InvalidEndpoint)?;
    Ok(())
}

impl InstalledRemoteContextDestinationCatalog {
    pub fn new(
        entries: Vec<InstalledRemoteContextDestinationV1>,
    ) -> Result<Self, EgressConfigurationError> {
        if entries.len() > insight_platform_contracts::MAX_REMOTE_CONTEXT_INSTALLATION_DESTINATIONS
        {
            return Err(EgressConfigurationError::InvalidEndpointCatalog);
        }
        for (index, entry) in entries.iter().enumerate() {
            if !entry.validate_shape()
                || parse_endpoint_host(&entry.endpoint.host).is_err()
                || validate_remote_context_roots(&entry.trusted_root_pem).is_err()
            {
                return Err(EgressConfigurationError::InvalidEndpoint);
            }
            capability_url(&entry.endpoint)?;
            if entries[..index]
                .iter()
                .any(|other| entry.same_selector(other))
            {
                return Err(EgressConfigurationError::DuplicateEndpoint);
            }
        }
        Ok(Self { entries })
    }
    fn resolve(
        &self,
        request: &RemoteContextSearchRequest,
    ) -> Result<InstalledRemoteContextDestinationV1, RemoteContextFailure> {
        self.entries
            .iter()
            .find(|entry| {
                request.protocol_contract_digest == entry.protocol_contract_digest
                    && request.result_mapping_digest == entry.result_mapping_digest
                    && request.endpoint == entry.endpoint
                    && request.endpoint_identity_digest == entry.endpoint_identity_digest
                    && request.region == entry.region
                    && request.maximum_response_bytes <= entry.maximum_response_bytes
                    && request.secret_bindings.len() == entry.credential_injections.len()
                    && entry.credential_injections.iter().all(|injection| {
                        request
                            .secret_bindings
                            .iter()
                            .filter(|binding| binding.purpose == *injection.purpose())
                            .count()
                            == 1
                    })
            })
            .cloned()
            .ok_or_else(|| before_dispatch("context_egress_destination_not_installed", false))
    }
}

pub struct ReqwestRemoteContextSearchConnector {
    catalog: InstalledRemoteContextDestinationCatalog,
    authority: Arc<dyn insight_platform_security::ContextDispatchAuthority>,
    secrets: Arc<dyn SecretMaterialResolver>,
    dns: Arc<dyn EgressDnsResolver>,
    limits: RemoteContextEgressLimits,
    permits: Arc<Semaphore>,
    #[cfg(test)]
    allow_loopback_for_protocol_fixture: bool,
}

impl ReqwestRemoteContextSearchConnector {
    pub fn new(
        catalog: InstalledRemoteContextDestinationCatalog,
        authority: Arc<dyn insight_platform_security::ContextDispatchAuthority>,
        secrets: Arc<dyn SecretMaterialResolver>,
        dns: Arc<dyn EgressDnsResolver>,
        limits: RemoteContextEgressLimits,
    ) -> Result<Self, EgressConfigurationError> {
        limits.validate()?;
        Ok(Self {
            catalog,
            authority,
            secrets,
            dns,
            limits,
            permits: Arc::new(Semaphore::new(limits.maximum_in_flight)),
            #[cfg(test)]
            allow_loopback_for_protocol_fixture: false,
        })
    }

    pub fn capacity_snapshot(&self) -> EgressCapacitySnapshot {
        EgressCapacitySnapshot {
            maximum_in_flight: self.limits.maximum_in_flight,
            available: self.permits.available_permits(),
        }
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
        entry: &InstalledRemoteContextDestinationV1,
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
        entry: &InstalledRemoteContextDestinationV1,
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
    wire::encode_wire(
        query,
        &request.normalized_query_digest,
        &request.normalized_filter_digest,
        &request.requested_projection,
        request.page_size,
        &request.cursor_digest,
        maximum_request_bytes,
    )
    .map_err(|error| match error {
        wire::RemoteSearchWireError::RequestTooLarge => {
            before_dispatch("context_egress_request_too_large", false)
        }
        _ => before_dispatch("context_egress_request_invalid", false),
    })
}

fn normalize_remote_search_response(
    request: &RemoteContextSearchRequest,
    bytes: &[u8],
    evidence: Sha256Digest,
) -> Result<RemoteContextSearchResponse, RemoteContextFailure> {
    let decoded = wire::decode_wire(
        bytes,
        request.page_size,
        request.maximum_classification,
        request.maximum_response_bytes,
    )
    .map_err(|_| after_dispatch("context_egress_response_invalid", false, evidence.clone()))?;
    let wire = decoded.response;
    let response_digest = decoded.canonical_response_digest;
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
        let authorization = request
            .dispatch_authorization()
            .map_err(|_| before_dispatch("context_egress_request_invalid", false))?;
        let predispatch_deadline = tokio::time::Instant::now()
            .checked_add(
                (request.deadline - Utc::now())
                    .to_std()
                    .map_err(|_| before_dispatch("context_egress_deadline_elapsed", false))?,
            )
            .ok_or_else(|| before_dispatch("context_egress_deadline_elapsed", false))?;
        let authorization_permit = tokio::time::timeout_at(
            predispatch_deadline,
            self.authority.authorize_context_dispatch(&authorization),
        )
        .await
        .map_err(|_| before_dispatch("context_egress_authorization_unavailable", true))?
        .map_err(|error| match error {
            insight_platform_contracts::ContextDispatchAuthorizationError::Rejected => {
                before_dispatch("context_egress_authorization_rejected", false)
            }
            insight_platform_contracts::ContextDispatchAuthorizationError::Unavailable => {
                before_dispatch("context_egress_authorization_unavailable", true)
            }
        })?;
        if !authorization_permit.validate_for(&authorization, Utc::now()) {
            return Err(before_dispatch(
                "context_egress_authorization_rejected",
                false,
            ));
        }
        let body = encode_remote_search_body(
            &request,
            entry
                .maximum_request_bytes
                .min(request.maximum_request_bytes),
        )?;
        let (headers, (dns_host, addresses)) =
            tokio::time::timeout_at(predispatch_deadline, async {
                let headers = self.headers(&request, &entry).await?;
                let addresses = self.addresses(&entry).await?;
                Ok::<_, RemoteContextFailure>((headers, addresses))
            })
            .await
            .map_err(|_| before_dispatch("context_egress_deadline_elapsed", false))??;
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
        if !authorization_permit.validate_for(&authorization, Utc::now()) {
            return Err(before_dispatch(
                "context_egress_authorization_expired",
                false,
            ));
        }
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
    entry: &InstalledRemoteContextDestinationV1,
    addresses: &[SocketAddr],
) -> Sha256Digest {
    closed_digest(&serde_json::json!({
        "schema_version": 1,
        "context_query_id": request.context_query_id,
        "job_id": request.job_id,
        "physical_attempt": request.physical_attempt,
        "lease_generation": request.lease_generation,
        "endpoint_identity_digest": entry.endpoint_identity_digest,
        "tls_policy": request.tls_policy,
        "trust_policy": request.trust_policy,
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
        canonical_json, parse_strict_json, CapabilityEndpointScheme, DataClassification,
        DataRegion, ExactDeploymentRef, ExactSecretBindingRef, ExactVersionRef, JsonLimits,
        ResourceId, ResourceKind, ValueRef,
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

    fn fixture() -> (
        InstalledRemoteContextDestinationV1,
        RemoteContextSearchRequest,
    ) {
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
        let installed = InstalledRemoteContextDestinationV1 {
            schema_version: REMOTE_CONTEXT_PROTOCOL_VERSION,
            protocol_contract_digest:
                insight_platform_context::remote_context_protocol_contract_digest(),
            result_mapping_digest: insight_platform_context::remote_context_result_mapping_digest(),
            endpoint_identity_digest: endpoint.canonical_digest().unwrap(),
            endpoint: endpoint.clone(),
            region: "cn-east-1".parse::<DataRegion>().unwrap(),
            credential_injections: vec![],
            trusted_root_pem: root_pem(),
            maximum_request_bytes: 65_536,
            maximum_response_bytes: 1_048_576,
        };
        let request = RemoteContextSearchRequest {
            schema_version: insight_platform_contracts::REMOTE_CONTEXT_EXECUTION_SCHEMA_VERSION,
            tenant_id: id(ResourceKind::Tenant, 6),
            context_query_id: id(ResourceKind::ContextQuery, 7),
            job_id: id(ResourceKind::Job, 8),
            worker_process_generation_id: id(ResourceKind::WorkerProcessGeneration, 15),
            physical_attempt: 1,
            lease_generation: 1,
            lease_token_digest: digest('d'),
            admission_digest: digest('e'),
            context_deployment: deployment,
            implementation_revision: implementation,
            protocol_contract_digest:
                insight_platform_context::remote_context_protocol_contract_digest(),
            result_mapping_digest: insight_platform_context::remote_context_result_mapping_digest(),
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
            maximum_request_bytes: 65_536,
            maximum_response_bytes: 1_048_576,
            deadline: Utc::now() + ChronoDuration::minutes(1),
        };
        (installed, request)
    }

    struct ExactAuthority(insight_platform_contracts::ContextDispatchAuthorizationV1);
    #[async_trait]
    impl insight_platform_security::ContextDispatchAuthority for ExactAuthority {
        async fn authorize_context_dispatch(
            &self,
            request: &insight_platform_contracts::ContextDispatchAuthorizationV1,
        ) -> Result<
            insight_platform_contracts::ContextDispatchPermitV1,
            insight_platform_contracts::ContextDispatchAuthorizationError,
        > {
            if request != &self.0 {
                return Err(
                    insight_platform_contracts::ContextDispatchAuthorizationError::Rejected,
                );
            }
            Ok(insight_platform_contracts::ContextDispatchPermitV1 {
                schema_version: 1,
                request_digest: closed_digest(request),
                valid_until: request.deadline,
            })
        }
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
    fn installed_remote_context_catalog_matches_only_the_physical_destination() {
        let (installed, request) = fixture();
        assert!(installed.validate_shape());
        let catalog = InstalledRemoteContextDestinationCatalog::new(vec![installed]).unwrap();
        catalog.resolve(&request).unwrap();

        let mut drifted = request;
        drifted.region = "cn-west-1".parse().unwrap();
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
        let (address, serving_root_pem, server) = start_remote_search_https_fixture().await;
        let (mut installed, mut request) = fixture();
        installed.endpoint.port = address.port();
        installed.endpoint_identity_digest = installed.endpoint.canonical_digest().unwrap();
        // The serving root is second: this proves the complete bundle is trusted.
        installed.trusted_root_pem = format!("{}{}", root_pem(), serving_root_pem);
        request.endpoint = installed.endpoint.clone();
        request.endpoint_identity_digest = installed.endpoint_identity_digest.clone();
        let connector = ReqwestRemoteContextSearchConnector::new(
            InstalledRemoteContextDestinationCatalog::new(vec![installed]).unwrap(),
            Arc::new(ExactAuthority(request.dispatch_authorization().unwrap())),
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

    #[test]
    fn installed_roots_require_nonempty_real_der_before_any_dispatch() {
        let (installed, _) = fixture();
        assert!(InstalledRemoteContextDestinationCatalog::new(vec![installed.clone()]).is_ok());
        for pem in [
            "not a certificate",
            "-----BEGIN CERTIFICATE-----\nYWJj\n-----END CERTIFICATE-----\n",
        ] {
            let mut wrong = installed.clone();
            wrong.trusted_root_pem = pem.into();
            assert!(InstalledRemoteContextDestinationCatalog::new(vec![wrong]).is_err());
        }
    }

    #[test]
    fn public_document_provider_uses_the_actual_encoder_and_result_mapping() {
        use std::{io::Write as _, path::Path, process::Stdio, time::Instant};

        let workspace = Path::new(env!("CARGO_MANIFEST_DIR"))
            .ancestors()
            .find(|candidate| {
                candidate.join("Cargo.toml").is_file()
                    && candidate
                        .join("contracts/platform-v1/manifest.json")
                        .is_file()
            })
            .expect("provider protocol test is inside the marked workspace");
        let example = workspace.join("examples/productization/document-review");
        for (question, expected_items) in [
            ("持久状态 PostgreSQL", 1),
            ("人工确认 \"PostgreSQL\" \\ 原文", 1),
            ("zzzzunmatchedzzzz", 0),
        ] {
            let (_, mut request) = fixture();
            let query = serde_json::json!({"question": question});
            request.normalized_query_digest = closed_digest(&query);
            request.query_input = ValueRef::Inline { value: query };
            request.normalized_filter_digest = closed_digest(&serde_json::json!({
                "schema_version": 1, "filter": null
            }));
            request.requested_projection.clear();
            request.page_size = 1;
            request.maximum_response_bytes = 65_536;
            let body = encode_remote_search_body(&request, 8_192).unwrap();
            let mut child = std::process::Command::new("python3")
                .arg(example.join("server.py"))
                .arg("--query-stdin")
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .unwrap();
            child.stdin.take().unwrap().write_all(&body).unwrap();
            // A single bounded paragraph fits the pipe; kill a broken provider instead of hanging CI.
            let deadline = Instant::now() + Duration::from_secs(5);
            while child.try_wait().unwrap().is_none() {
                if Instant::now() >= deadline {
                    let _ = child.kill();
                    let _ = child.wait();
                    panic!("document provider exceeded the local protocol-check deadline");
                }
                std::thread::sleep(Duration::from_millis(10));
            }
            let output = child.wait_with_output().unwrap();
            assert!(output.status.success(), "{:?}", output.stderr);
            assert!(output.stderr.is_empty());
            let normalized =
                normalize_remote_search_response(&request, &output.stdout, digest('a')).unwrap();
            assert_eq!(normalized.items.len(), expected_items);
            assert_eq!(
                normalized.backend_request_digest,
                request.normalized_query_digest
            );
            assert!(normalized.next_cursor_digest.is_none());
            for item in normalized.items {
                let fields = item.structured_fields;
                let uri = fields["source_uri"].as_str().unwrap();
                let revision = fields["source_revision"].as_str().unwrap();
                assert_eq!(revision, "b8d9a6e2a4043945eb94cf1bcab52df7c91d3963");
                let path = uri.split(&format!("/{revision}/")).nth(1).unwrap();
                assert!(matches!(
                    path,
                    "docs/current/architecture.md" | "docs/current/agent-authoring.md"
                ));
                let source = std::fs::read(
                    example
                        .join("corpus")
                        .join(Path::new(path).file_name().unwrap()),
                )
                .unwrap();
                let source_digest = format!(
                    "sha256:{}",
                    ring::digest::digest(&ring::digest::SHA256, &source)
                        .as_ref()
                        .iter()
                        .map(|byte| format!("{byte:02x}"))
                        .collect::<String>()
                );
                assert_eq!(fields["raw_content_digest"], source_digest);
                let source = String::from_utf8(source).unwrap();
                let start = usize::try_from(fields["start_line"].as_u64().unwrap()).unwrap();
                let end = usize::try_from(fields["end_line"].as_u64().unwrap()).unwrap();
                assert!(start > 0 && end >= start);
                let excerpt = source
                    .split_inclusive('\n')
                    .skip(start - 1)
                    .take(end - start + 1)
                    .collect::<String>();
                assert_eq!(item.content, excerpt);
                assert_ne!(canonical_digest(&item.content).unwrap(), source_digest);
                assert_eq!(item.classification, DataClassification::Public);
            }
        }
    }

    struct CountingDns {
        calls: Arc<std::sync::atomic::AtomicUsize>,
        delay: bool,
    }
    #[async_trait]
    impl EgressDnsResolver for CountingDns {
        async fn resolve(&self, _: &str, port: u16) -> Result<Vec<SocketAddr>, DnsResolutionError> {
            self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if self.delay {
                tokio::time::sleep(Duration::from_millis(80)).await;
            }
            Ok(vec![SocketAddr::new("8.8.8.8".parse().unwrap(), port)])
        }
    }
    struct RecordingDispatchAuthority {
        calls: Arc<std::sync::atomic::AtomicUsize>,
        mode: u8,
    }
    #[async_trait]
    impl insight_platform_security::ContextDispatchAuthority for RecordingDispatchAuthority {
        async fn authorize_context_dispatch(
            &self,
            request: &insight_platform_contracts::ContextDispatchAuthorizationV1,
        ) -> Result<
            insight_platform_contracts::ContextDispatchPermitV1,
            insight_platform_contracts::ContextDispatchAuthorizationError,
        > {
            use insight_platform_contracts::ContextDispatchAuthorizationError as Failure;
            self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            match self.mode {
                0 => return Err(Failure::Rejected),
                1 => return Err(Failure::Unavailable),
                5 => {
                    std::future::pending::<()>().await;
                    unreachable!()
                }
                _ => {}
            }
            Ok(insight_platform_contracts::ContextDispatchPermitV1 {
                schema_version: 1,
                request_digest: if self.mode == 3 {
                    digest('0')
                } else {
                    closed_digest(request)
                },
                valid_until: match self.mode {
                    2 => Utc::now() - ChronoDuration::seconds(1),
                    4 => Utc::now() + ChronoDuration::milliseconds(40),
                    _ => request.deadline,
                },
            })
        }
    }
    #[tokio::test]
    async fn current_authorization_failure_or_expiry_never_opens_http_or_retries() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        for mode in 0..=5 {
            let (installed, mut request) = fixture();
            if mode == 5 {
                request.deadline = Utc::now() + ChronoDuration::milliseconds(40);
            }
            let authorizations = Arc::new(AtomicUsize::new(0));
            let dns = Arc::new(AtomicUsize::new(0));
            let connector = ReqwestRemoteContextSearchConnector::new(
                InstalledRemoteContextDestinationCatalog::new(vec![installed]).unwrap(),
                Arc::new(RecordingDispatchAuthority {
                    calls: authorizations.clone(),
                    mode,
                }),
                Arc::new(EmptySecrets),
                Arc::new(CountingDns {
                    calls: dns.clone(),
                    delay: mode == 4,
                }),
                RemoteContextEgressLimits::default(),
            )
            .unwrap();
            let failure = connector.query(request).await.unwrap_err();
            assert!(matches!(
                failure.class,
                RemoteContextFailureClass::RejectedBeforeDispatch
                    | RemoteContextFailureClass::RetryableBeforeDispatch
            ));
            assert!(failure.dispatch_evidence_digest.is_none());
            assert_eq!(authorizations.load(Ordering::SeqCst), 1);
            assert_eq!(dns.load(Ordering::SeqCst), usize::from(mode == 4));
            assert!(failure.code.starts_with("context_egress_authorization_"));
            assert_eq!(
                connector.capacity_snapshot().available,
                RemoteContextEgressLimits::default().maximum_in_flight
            );
        }
    }
    #[tokio::test]
    async fn changed_inline_body_or_business_metadata_is_rejected_before_dns() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        for body_change in [true, false] {
            let (installed, mut request) = fixture();
            let expected = request.dispatch_authorization().unwrap();
            if body_change {
                request.query_input = ValueRef::Inline {
                    value: serde_json::json!({"query":"body-canary"}),
                };
            } else {
                request.trust_policy = exact(ResourceKind::PolicyRevision, 19, 'a');
            }
            let dns = Arc::new(AtomicUsize::new(0));
            let connector = ReqwestRemoteContextSearchConnector::new(
                InstalledRemoteContextDestinationCatalog::new(vec![installed]).unwrap(),
                Arc::new(ExactAuthority(expected)),
                Arc::new(EmptySecrets),
                Arc::new(CountingDns {
                    calls: dns.clone(),
                    delay: false,
                }),
                RemoteContextEgressLimits::default(),
            )
            .unwrap();
            let failure = connector.query(request).await.unwrap_err();
            assert_eq!(failure.code, "context_egress_authorization_rejected");
            assert_eq!(dns.load(Ordering::SeqCst), 0);
            assert!(failure.dispatch_evidence_digest.is_none());
            assert!(!format!("{failure:?}").contains("body-canary"));
        }
    }
    #[tokio::test]
    async fn authorized_request_still_obeys_its_frozen_body_limit_before_dns() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let (installed, mut request) = fixture();
        request.maximum_request_bytes = 8;
        let dns = Arc::new(AtomicUsize::new(0));
        let connector = ReqwestRemoteContextSearchConnector::new(
            InstalledRemoteContextDestinationCatalog::new(vec![installed]).unwrap(),
            Arc::new(ExactAuthority(request.dispatch_authorization().unwrap())),
            Arc::new(EmptySecrets),
            Arc::new(CountingDns {
                calls: dns.clone(),
                delay: false,
            }),
            RemoteContextEgressLimits::default(),
        )
        .unwrap();
        let failure = connector.query(request).await.unwrap_err();
        assert_eq!(failure.code, "context_egress_request_too_large");
        assert_eq!(dns.load(Ordering::SeqCst), 0);
        assert!(failure.dispatch_evidence_digest.is_none());
    }
}
