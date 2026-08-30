use super::*;
use futures::StreamExt;
use insight_platform_contracts::SecretResolutionPolicy;
#[cfg(any())]
use insight_platform_mcp_host::ManagedMcpSandboxSessionIdentity;
#[cfg(any())]
use insight_platform_sandbox::{
    AuthorizedManagedMcpSandboxSecretDelivery, ManagedMcpSandboxSecretCommitOutcome,
    ManagedMcpSandboxSecretDeliveryAuthority, ManagedMcpSandboxSecretDeliveryError,
    ManagedMcpSandboxSecretDeliveryEvidence, ManagedMcpSandboxSecretDeliveryRequest,
    ManagedMcpSandboxSecretReservationOutcome, PreparedManagedMcpSandboxSession, ScopedSecretGrant,
};
use rcgen::{BasicConstraints, CertificateParams, CertifiedIssuer, IsCa, KeyPair};
use std::{
    net::{Ipv4Addr, Ipv6Addr},
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

fn exact_deployment(kind: ResourceKind, suffix: u16, character: char) -> ExactDeploymentRef {
    ExactDeploymentRef::new(id(kind, suffix), digest(character)).unwrap()
}

fn binding(policy: SecretResolutionPolicy) -> ExactSecretBindingRef {
    ExactSecretBindingRef::build(
        id(ResourceKind::SecretBinding, 20),
        3,
        id(ResourceKind::SecretProvider, 21),
        "model_api_key".parse().unwrap(),
        policy,
    )
    .unwrap()
}

struct Fixture {
    entry: InstalledModelProviderEndpoint,
    request: ModelProviderWireRequest,
}

fn fixture(policy: SecretResolutionPolicy) -> Fixture {
    let endpoint = CanonicalHttpEndpoint {
        scheme: CapabilityEndpointScheme::Https,
        host: "api.example.com".to_owned(),
        port: 443,
        base_path: "/".to_owned(),
    };
    let endpoint_identity_digest = endpoint.canonical_digest().unwrap();
    let provider_deployment = exact_deployment(ResourceKind::ModelProviderDeployment, 10, 'a');
    let provider_revision = exact_version(ResourceKind::ModelProviderRevision, 11, 'b');
    let network_policy = exact_version(ResourceKind::PolicyRevision, 12, 'c');
    let tls_policy = exact_version(ResourceKind::PolicyRevision, 13, 'd');
    let trust_policy = exact_version(ResourceKind::PolicyRevision, 14, 'e');
    let data_policy = exact_version(ResourceKind::PolicyRevision, 15, 'f');
    let region: DataRegion = "us-east".parse().unwrap();
    let secret_binding = binding(policy);
    let request_body = serde_json::json!({"model": "qualified-model", "stream": true});
    let request_body_digest = canonical_digest(&request_body).unwrap().parse().unwrap();
    Fixture {
        entry: InstalledModelProviderEndpoint {
            schema_version: 1,
            protocol: ModelProviderWireProtocol::OpenAiResponses,
            provider_deployment: provider_deployment.clone(),
            provider_revision: provider_revision.clone(),
            endpoint,
            endpoint_identity_digest: endpoint_identity_digest.clone(),
            credential_purpose: "model_api_key".parse().unwrap(),
            network_policy: network_policy.clone(),
            tls_policy: tls_policy.clone(),
            trust_policy: trust_policy.clone(),
            data_policy: data_policy.clone(),
            region: region.clone(),
            development_loopback: false,
            trusted_root_pem: None,
        },
        request: ModelProviderWireRequest {
            schema_version: 2,
            protocol: ModelProviderWireProtocol::OpenAiResponses,
            tenant_id: id(ResourceKind::Tenant, 1),
            model_turn_id: id(ResourceKind::ModelTurn, 2),
            job_id: id(ResourceKind::Job, 3),
            worker_process_generation_id: id(ResourceKind::WorkerProcessGeneration, 4),
            attempt_no: 1,
            lease_generation: 1,
            admission_digest: digest('7'),
            model_request_digest: digest('8'),
            provider_deployment,
            provider_revision,
            endpoint_identity_digest,
            secret_bindings: vec![secret_binding],
            network_policy,
            tls_policy,
            trust_policy,
            data_policy,
            region,
            request_body,
            request_body_digest,
            maximum_request_bytes: 4_096,
            maximum_response_bytes: 4_096,
            connect_timeout_milliseconds: 1_000,
            total_timeout_milliseconds: 10_000,
            deadline: Utc::now() + chrono::Duration::seconds(30),
        },
    }
}

struct FixtureSecretResolver {
    generation: u64,
    version_digest: Sha256Digest,
    calls: AtomicUsize,
}

#[cfg(any())]
struct FixtureSandboxSecretAuthority {
    delivered: AtomicBool,
    commits: AtomicUsize,
}

#[cfg(any())]
#[async_trait]
impl ManagedMcpSandboxSecretDeliveryAuthority for FixtureSandboxSecretAuthority {
    async fn reserve_managed_mcp_sandbox_secret_delivery(
        &self,
        request: &ManagedMcpSandboxSecretDeliveryRequest,
    ) -> Result<ManagedMcpSandboxSecretReservationOutcome, ManagedMcpSandboxSecretDeliveryError>
    {
        if self.delivered.load(Ordering::SeqCst) {
            return Ok(ManagedMcpSandboxSecretReservationOutcome::AlreadyDelivered);
        }
        let authorization = AuthorizedManagedMcpSandboxSecretDelivery {
            schema_version: 1,
            receipt_id: request.receipt_id.clone(),
            tenant_id: request.identity.tenant_id.clone(),
            sandbox_job_id: request.identity.sandbox_job_id.clone(),
            secret_binding: request.secret_grant.secret_binding.clone(),
            resolved_binding_generation: request.secret_grant.resolved_binding_generation,
            delivery_request_digest: request.canonical_digest.clone(),
            expires_at: request.secret_grant.expires_at,
            authorization_digest: digest('0'),
        }
        .seal()
        .unwrap();
        Ok(ManagedMcpSandboxSecretReservationOutcome::Authorized(
            Box::new(authorization),
        ))
    }

    async fn commit_managed_mcp_sandbox_secret_delivery(
        &self,
        request: &ManagedMcpSandboxSecretDeliveryRequest,
        authorization: &AuthorizedManagedMcpSandboxSecretDelivery,
        resolution_evidence_digest: &Sha256Digest,
    ) -> Result<ManagedMcpSandboxSecretCommitOutcome, ManagedMcpSandboxSecretDeliveryError> {
        self.commits.fetch_add(1, Ordering::SeqCst);
        self.delivered.store(true, Ordering::SeqCst);
        let evidence = ManagedMcpSandboxSecretDeliveryEvidence {
            schema_version: 1,
            receipt_id: request.receipt_id.clone(),
            tenant_id: request.identity.tenant_id.clone(),
            sandbox_job_id: request.identity.sandbox_job_id.clone(),
            secret_binding_id: request
                .secret_grant
                .secret_binding
                .secret_binding_id
                .clone(),
            resolved_binding_generation: request.secret_grant.resolved_binding_generation,
            authorization_digest: authorization.authorization_digest.clone(),
            resolution_evidence_digest: resolution_evidence_digest.clone(),
            delivered_at: Utc::now(),
            evidence_digest: digest('0'),
        }
        .seal()
        .unwrap();
        Ok(ManagedMcpSandboxSecretCommitOutcome::Delivered(evidence))
    }
}

#[cfg(any())]
fn managed_mcp_sandbox_secret_delivery() -> ManagedMcpSandboxSecretDeliveryRequest {
    let physical_job_id = id(ResourceKind::Job, 903);
    let sandbox_job_id =
        ResourceId::from_uuid_v7(ResourceKind::Job, physical_job_id.uuid()).unwrap();
    let tenant_id = id(ResourceKind::Tenant, 900);
    let identity = ManagedMcpSandboxSessionIdentity {
        schema_version: 1,
        tenant_id: tenant_id.clone(),
        subscription_id: id(ResourceKind::McpOperation, 901),
        logical_job_id: id(ResourceKind::Job, 902),
        admitted_subscription_version: 1,
        admitted_logical_job_version: 1,
        session_generation: 1,
        sandbox_job_id: sandbox_job_id.clone(),
        physical_job_id,
        subscription_binding_digest: digest('1'),
        canonical_digest: digest('2'),
    };
    let worker = id(ResourceKind::WorkerProcessGeneration, 904);
    let request_digest = digest('3');
    let fence = JobFence {
        expected_version: 4,
        worker_process_generation_id: worker.clone(),
        lease_generation: 1,
        token_digest: digest('4'),
    };
    let prepared = PreparedManagedMcpSandboxSession {
        schema_version: 1,
        identity: identity.clone(),
        request_digest: request_digest.clone(),
        worker_process_generation_id: worker,
        provider_process_generation_id: id(ResourceKind::WorkerProcessGeneration, 905),
        lease_generation: 1,
        executor_identity_digest: digest('5'),
        sandbox_identity_digest: digest('6'),
        prepare_evidence_digest: digest('7'),
        canonical_digest: digest('0'),
    }
    .seal()
    .unwrap();
    let secret_binding = ExactSecretBindingRef::build(
        id(ResourceKind::SecretBinding, 906),
        1,
        id(ResourceKind::SecretProvider, 907),
        "mcp_access_token".parse::<SecretPurpose>().unwrap(),
        SecretResolutionPolicy::Pinned {
            opaque_version_identity_digest: digest('8'),
        },
    )
    .unwrap();
    let expires_at = Utc::now() + chrono::Duration::minutes(5);
    let secret_grant = ScopedSecretGrant {
        schema_version: 1,
        tenant_id,
        sandbox_job_id,
        secret_binding,
        resolved_binding_generation: 1,
        runtime_digest: digest('9'),
        maximum_reads: 1,
        expires_at,
        grant_digest: digest('0'),
    }
    .seal()
    .unwrap();
    ManagedMcpSandboxSecretDeliveryRequest {
        schema_version: 1,
        receipt_id: id(ResourceKind::Receipt, 908),
        event_id: id(ResourceKind::Event, 909),
        outbox_id: id(ResourceKind::OutboxEvent, 910),
        idempotency_key_digest: digest('a'),
        identity,
        request_digest,
        fence,
        prepared,
        secret_grant,
        canonical_digest: digest('0'),
    }
    .seal()
    .unwrap()
}

#[async_trait]
impl SecretMaterialResolver for FixtureSecretResolver {
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
            self.generation,
            self.version_digest.clone(),
            b"top-secret".to_vec(),
        )
        .map_err(|_| SecretMaterialResolutionError::InvalidEvidence)
    }
}

#[cfg(any())]
#[tokio::test]
async fn managed_mcp_sandbox_secret_is_released_only_after_fresh_commit() {
    let request = managed_mcp_sandbox_secret_delivery();
    let authority = Arc::new(FixtureSandboxSecretAuthority {
        delivered: AtomicBool::new(false),
        commits: AtomicUsize::new(0),
    });
    let resolver = Arc::new(FixtureSecretResolver {
        generation: 1,
        version_digest: digest('8'),
        calls: AtomicUsize::new(0),
    });
    let broker = BrokeredManagedMcpSandboxSecretDelivery::new(
        Arc::clone(&authority),
        resolver.clone(),
        1_024,
    )
    .unwrap();
    let delivered = broker.deliver(request.clone()).await.unwrap();
    assert_eq!(delivered.into_material(), b"top-secret");
    assert_eq!(authority.commits.load(Ordering::SeqCst), 1);
    assert_eq!(resolver.calls.load(Ordering::SeqCst), 1);

    assert!(matches!(
        broker.deliver(request).await,
        Err(ManagedMcpSandboxSecretBrokerError::OutcomeUncertain)
    ));
    assert_eq!(authority.commits.load(Ordering::SeqCst), 1);
    assert_eq!(resolver.calls.load(Ordering::SeqCst), 1);
}

struct FixtureDnsResolver {
    addresses: Vec<SocketAddr>,
}

#[async_trait]
impl EgressDnsResolver for FixtureDnsResolver {
    async fn resolve(
        &self,
        _host: &str,
        _port: u16,
    ) -> Result<Vec<SocketAddr>, DnsResolutionError> {
        Ok(self.addresses.clone())
    }
}

#[derive(Debug, Clone, Copy)]
enum TransportMode {
    Complete,
    WaitForCancel,
}

struct FixtureTransport {
    mode: TransportMode,
    started: Notify,
    validated: AtomicBool,
    chunks: Mutex<Option<Vec<Vec<u8>>>>,
}

struct LoopbackFixtureTransport {
    expected_port: u16,
    called: AtomicBool,
}

#[async_trait]
impl PinnedModelProviderHttpTransport for LoopbackFixtureTransport {
    async fn open(
        &self,
        request: PinnedHttpRequest,
    ) -> Result<PinnedHttpResponse, ModelAdapterFailure> {
        assert_eq!(request.url.host_str(), Some("localhost"));
        assert_eq!(request.url.port(), Some(self.expected_port));
        assert_eq!(request.dns_host, "localhost");
        assert_eq!(
            request.addresses,
            vec![SocketAddr::new(
                Ipv4Addr::LOCALHOST.into(),
                self.expected_port
            )]
        );
        assert!(request.trusted_root_pem.is_some());
        self.called.store(true, Ordering::SeqCst);
        Ok(PinnedHttpResponse {
            status_code: 200,
            content_type: "text/event-stream".to_owned(),
            body: stream::empty().boxed(),
        })
    }
}

fn fixture_root_pem() -> String {
    let mut parameters = CertificateParams::default();
    parameters.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    CertifiedIssuer::self_signed(parameters, KeyPair::generate().unwrap())
        .unwrap()
        .pem()
}

impl FixtureTransport {
    fn complete(chunks: Vec<Vec<u8>>) -> Self {
        Self {
            mode: TransportMode::Complete,
            started: Notify::new(),
            validated: AtomicBool::new(false),
            chunks: Mutex::new(Some(chunks)),
        }
    }

    fn waiting() -> Self {
        Self {
            mode: TransportMode::WaitForCancel,
            started: Notify::new(),
            validated: AtomicBool::new(false),
            chunks: Mutex::new(Some(Vec::new())),
        }
    }
}

#[async_trait]
impl PinnedModelProviderHttpTransport for FixtureTransport {
    async fn open(
        &self,
        request: PinnedHttpRequest,
    ) -> Result<PinnedHttpResponse, ModelAdapterFailure> {
        assert_eq!(request.url.as_str(), "https://api.example.com/v1/responses");
        assert_eq!(request.dns_host, "api.example.com");
        assert_eq!(
            request.addresses,
            vec![SocketAddr::new(Ipv4Addr::new(8, 8, 8, 8).into(), 443)]
        );
        assert_eq!(
            request.body,
            br#"{"model":"qualified-model","stream":true}"#
        );
        assert_eq!(request.headers.len(), 3);
        let authorization = request.headers.get(AUTHORIZATION).unwrap();
        assert_eq!(authorization.as_bytes(), b"Bearer top-secret");
        assert!(authorization.is_sensitive());
        self.validated.store(true, Ordering::SeqCst);
        self.started.notify_waiters();
        if matches!(self.mode, TransportMode::WaitForCancel) {
            request.cancellation.cancelled().await;
            return Err(permanent_failure("model_egress_cancelled", true));
        }
        let chunks = self.chunks.lock().unwrap().take().unwrap();
        Ok(PinnedHttpResponse {
            status_code: 200,
            content_type: "text/event-stream".to_owned(),
            body: stream::iter(chunks.into_iter().map(Ok)).boxed(),
        })
    }
}

fn broker(
    fixture: &Fixture,
    secrets: Arc<FixtureSecretResolver>,
    dns: Arc<FixtureDnsResolver>,
    transport: Arc<FixtureTransport>,
) -> Arc<ReqwestModelProviderEgressBroker> {
    Arc::new(
        ReqwestModelProviderEgressBroker::with_transport(
            InstalledModelProviderEndpointCatalog::new(vec![fixture.entry.clone()]).unwrap(),
            secrets,
            dns,
            transport,
            ModelProviderEgressLimits {
                maximum_in_flight: 2,
                maximum_dns_answers: 4,
                maximum_secret_material_bytes: 128,
            },
        )
        .unwrap(),
    )
}

fn pinned_fixture() -> Fixture {
    fixture(SecretResolutionPolicy::Pinned {
        opaque_version_identity_digest: digest('9'),
    })
}

fn successful_secrets() -> Arc<FixtureSecretResolver> {
    Arc::new(FixtureSecretResolver {
        generation: 3,
        version_digest: digest('9'),
        calls: AtomicUsize::new(0),
    })
}

fn public_dns() -> Arc<FixtureDnsResolver> {
    Arc::new(FixtureDnsResolver {
        addresses: vec![SocketAddr::new(Ipv4Addr::new(8, 8, 8, 8).into(), 443)],
    })
}

fn cancel_for(request: &ModelProviderWireRequest) -> ModelAdapterCancelRequest {
    ModelAdapterCancelRequest {
        tenant_id: request.tenant_id.clone(),
        model_turn_id: request.model_turn_id.clone(),
        job_id: request.job_id.clone(),
        worker_process_generation_id: request.worker_process_generation_id.clone(),
        provider_deployment: request.provider_deployment.clone(),
        attempt_no: request.attempt_no,
        lease_generation: request.lease_generation,
        deadline: Utc::now() + chrono::Duration::seconds(5),
    }
}

fn expect_failure<T>(result: Result<T, ModelAdapterFailure>) -> ModelAdapterFailure {
    match result {
        Ok(_) => panic!("expected egress failure"),
        Err(failure) => failure,
    }
}

#[test]
fn public_destination_policy_rejects_local_private_special_and_documentation_ranges() {
    for address in [
        Ipv4Addr::LOCALHOST,
        Ipv4Addr::new(10, 0, 0, 1),
        Ipv4Addr::new(100, 64, 0, 1),
        Ipv4Addr::new(169, 254, 169, 254),
        Ipv4Addr::new(172, 16, 0, 1),
        Ipv4Addr::new(192, 0, 2, 1),
        Ipv4Addr::new(192, 168, 1, 1),
        Ipv4Addr::new(198, 51, 100, 1),
        Ipv4Addr::new(203, 0, 113, 1),
        Ipv4Addr::BROADCAST,
    ] {
        assert!(!is_public_destination_ip(address.into()), "{address}");
    }
    for address in [Ipv4Addr::new(1, 1, 1, 1), Ipv4Addr::new(8, 8, 8, 8)] {
        assert!(is_public_destination_ip(address.into()), "{address}");
    }
    assert!(!is_public_destination_ip(Ipv6Addr::LOCALHOST.into()));
    assert!(!is_public_destination_ip("fc00::1".parse().unwrap()));
    assert!(!is_public_destination_ip("fe80::1".parse().unwrap()));
    assert!(!is_public_destination_ip("2001:db8::1".parse().unwrap()));
    assert!(is_public_destination_ip(
        "2606:4700:4700::1111".parse().unwrap()
    ));
}

#[test]
fn catalog_rejects_http_ambiguous_paths_and_duplicate_exact_deployments() {
    let fixture = pinned_fixture();
    let mut http = fixture.entry.clone();
    http.endpoint.scheme = CapabilityEndpointScheme::Http;
    http.endpoint_identity_digest = http.endpoint.canonical_digest().unwrap();
    assert_eq!(
        http.validate(),
        Err(EgressConfigurationError::InvalidEndpoint)
    );

    let mut encoded_path = fixture.entry.clone();
    encoded_path.endpoint.base_path = "/api/%2e%2e".to_owned();
    encoded_path.endpoint_identity_digest = encoded_path.endpoint.canonical_digest().unwrap();
    assert_eq!(
        encoded_path.validate(),
        Err(EgressConfigurationError::InvalidEndpoint)
    );

    assert_eq!(
        InstalledModelProviderEndpointCatalog::new(vec![fixture.entry.clone(), fixture.entry])
            .unwrap_err(),
        EgressConfigurationError::DuplicateEndpoint
    );
}

#[tokio::test]
async fn mixed_public_and_private_dns_answers_fail_closed_before_secret_resolution() {
    let fixture = pinned_fixture();
    let secrets = successful_secrets();
    let transport = Arc::new(FixtureTransport::complete(vec![]));
    let broker = broker(
        &fixture,
        secrets.clone(),
        Arc::new(FixtureDnsResolver {
            addresses: vec![
                SocketAddr::new(Ipv4Addr::new(8, 8, 8, 8).into(), 443),
                SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 443),
            ],
        }),
        transport.clone(),
    );
    let failure = expect_failure(broker.open(fixture.request).await);
    assert_eq!(failure.safe_code, "model_egress_destination_denied");
    assert_eq!(
        failure.class,
        ModelAdapterFailureClass::RejectedBeforeDispatch
    );
    assert_eq!(secrets.calls.load(Ordering::SeqCst), 0);
    assert!(!transport.validated.load(Ordering::SeqCst));
}

#[tokio::test]
async fn explicit_development_endpoint_allows_only_root_bound_localhost() {
    let mut fixture = pinned_fixture();
    let port = 18_443;
    fixture.entry.endpoint.host = "localhost".to_owned();
    fixture.entry.endpoint.port = port;
    fixture.entry.endpoint_identity_digest = fixture.entry.endpoint.canonical_digest().unwrap();
    fixture.entry.development_loopback = true;
    fixture.entry.trusted_root_pem = Some(fixture_root_pem());
    fixture.request.endpoint_identity_digest = fixture.entry.endpoint_identity_digest.clone();
    let transport = Arc::new(LoopbackFixtureTransport {
        expected_port: port,
        called: AtomicBool::new(false),
    });
    let broker = ReqwestModelProviderEgressBroker::with_transport(
        InstalledModelProviderEndpointCatalog::new(vec![fixture.entry.clone()]).unwrap(),
        successful_secrets(),
        Arc::new(FixtureDnsResolver {
            addresses: vec![SocketAddr::new(Ipv4Addr::LOCALHOST.into(), port)],
        }),
        transport.clone(),
        ModelProviderEgressLimits {
            maximum_in_flight: 2,
            maximum_dns_answers: 4,
            maximum_secret_material_bytes: 128,
        },
    )
    .unwrap();
    let response = broker.open(fixture.request).await.unwrap();
    assert_eq!(response.status_code, 200);
    assert!(transport.called.load(Ordering::SeqCst));

    let mut missing_root = fixture.entry;
    missing_root.trusted_root_pem = None;
    assert_eq!(
        missing_root.validate(),
        Err(EgressConfigurationError::InvalidEndpoint)
    );
    missing_root.trusted_root_pem = Some(fixture_root_pem());
    missing_root.endpoint.host = "api.example.com".to_owned();
    missing_root.endpoint_identity_digest = missing_root.endpoint.canonical_digest().unwrap();
    assert_eq!(
        missing_root.validate(),
        Err(EgressConfigurationError::InvalidEndpoint)
    );
}

#[tokio::test]
async fn exact_endpoint_policy_mismatch_fails_before_dns_or_secret() {
    let mut fixture = pinned_fixture();
    fixture.request.network_policy.semantic_digest = digest('0');
    let secrets = successful_secrets();
    let transport = Arc::new(FixtureTransport::complete(vec![]));
    let broker = broker(&fixture, secrets.clone(), public_dns(), transport.clone());
    let failure = expect_failure(broker.open(fixture.request).await);
    assert_eq!(failure.safe_code, "model_egress_endpoint_not_installed");
    assert_eq!(secrets.calls.load(Ordering::SeqCst), 0);
    assert!(!transport.validated.load(Ordering::SeqCst));
}

#[tokio::test]
async fn pinned_secret_requires_exact_generation_and_opaque_version() {
    let fixture = pinned_fixture();
    let generation_mismatch_broker = broker(
        &fixture,
        Arc::new(FixtureSecretResolver {
            generation: 4,
            version_digest: digest('9'),
            calls: AtomicUsize::new(0),
        }),
        public_dns(),
        Arc::new(FixtureTransport::complete(vec![])),
    );
    let failure = expect_failure(generation_mismatch_broker.open(fixture.request).await);
    assert_eq!(failure.safe_code, "model_egress_secret_rejected");

    let fixture = pinned_fixture();
    let version_mismatch_broker = broker(
        &fixture,
        Arc::new(FixtureSecretResolver {
            generation: 3,
            version_digest: digest('0'),
            calls: AtomicUsize::new(0),
        }),
        public_dns(),
        Arc::new(FixtureTransport::complete(vec![])),
    );
    let failure = expect_failure(version_mismatch_broker.open(fixture.request).await);
    assert_eq!(failure.safe_code, "model_egress_secret_rejected");
}

#[tokio::test]
async fn follow_rotation_accepts_a_newer_generation_under_the_frozen_policy() {
    let fixture = fixture(SecretResolutionPolicy::FollowProviderRotation {
        rotation_policy_revision_id: id(ResourceKind::PolicyRevision, 30),
    });
    let transport = Arc::new(FixtureTransport::complete(vec![b"event".to_vec()]));
    let broker = broker(
        &fixture,
        Arc::new(FixtureSecretResolver {
            generation: 4,
            version_digest: digest('0'),
            calls: AtomicUsize::new(0),
        }),
        public_dns(),
        transport,
    );
    let mut response = broker.open(fixture.request).await.unwrap();
    assert_eq!(response.body.next().await.unwrap().unwrap(), b"event");
    assert!(response.body.next().await.is_none());
}

#[tokio::test]
async fn broker_sends_only_fixed_headers_and_releases_registration_after_stream() {
    let fixture = pinned_fixture();
    let transport = Arc::new(FixtureTransport::complete(vec![
        b"first".to_vec(),
        b"second".to_vec(),
    ]));
    let broker = broker(
        &fixture,
        successful_secrets(),
        public_dns(),
        transport.clone(),
    );
    let mut response = broker.open(fixture.request).await.unwrap();
    assert_eq!(response.status_code, 200);
    assert_eq!(response.content_type, "text/event-stream");
    assert_eq!(response.body.next().await.unwrap().unwrap(), b"first");
    assert_eq!(response.body.next().await.unwrap().unwrap(), b"second");
    assert!(response.body.next().await.is_none());
    assert!(transport.validated.load(Ordering::SeqCst));
    assert!(broker.active.lock().unwrap().is_empty());
}

#[tokio::test]
async fn response_limit_is_enforced_inside_the_broker() {
    let mut fixture = pinned_fixture();
    fixture.request.maximum_response_bytes = 4;
    let broker = broker(
        &fixture,
        successful_secrets(),
        public_dns(),
        Arc::new(FixtureTransport::complete(vec![b"12345".to_vec()])),
    );
    let mut response = broker.open(fixture.request).await.unwrap();
    let failure = response.body.next().await.unwrap().unwrap_err();
    assert_eq!(failure.safe_code, "model_egress_response_too_large");
    assert!(response.body.next().await.is_none());
    assert!(broker.active.lock().unwrap().is_empty());
}

#[tokio::test]
async fn cancellation_is_bound_to_protocol_worker_and_job_fence() {
    let fixture = pinned_fixture();
    let request = fixture.request.clone();
    let transport = Arc::new(FixtureTransport::waiting());
    let broker = broker(
        &fixture,
        successful_secrets(),
        public_dns(),
        transport.clone(),
    );
    let started = transport.started.notified();
    let task_broker = broker.clone();
    let task = tokio::spawn(async move { task_broker.open(request).await });
    started.await;

    let mut stale = cancel_for(&fixture.request);
    stale.worker_process_generation_id = id(ResourceKind::WorkerProcessGeneration, 5);
    assert_eq!(
        broker
            .cancel(ModelProviderWireProtocol::OpenAiResponses, stale)
            .await
            .unwrap(),
        ModelAdapterCancelOutcome::AlreadyTerminal
    );
    assert_eq!(
        broker
            .cancel(
                ModelProviderWireProtocol::AnthropicMessages,
                cancel_for(&fixture.request)
            )
            .await
            .unwrap(),
        ModelAdapterCancelOutcome::AlreadyTerminal
    );
    assert_eq!(
        broker
            .cancel(
                ModelProviderWireProtocol::OpenAiResponses,
                cancel_for(&fixture.request)
            )
            .await
            .unwrap(),
        ModelAdapterCancelOutcome::Accepted
    );
    let failure = expect_failure(task.await.unwrap());
    assert_eq!(failure.safe_code, "model_egress_cancelled");
    assert!(broker.active.lock().unwrap().is_empty());
}

#[tokio::test]
async fn duplicate_exact_physical_request_is_rejected_while_first_is_active() {
    let fixture = pinned_fixture();
    let request = fixture.request.clone();
    let transport = Arc::new(FixtureTransport::waiting());
    let broker = broker(
        &fixture,
        successful_secrets(),
        public_dns(),
        transport.clone(),
    );
    let started = transport.started.notified();
    let task_broker = broker.clone();
    let task_request = request.clone();
    let task = tokio::spawn(async move { task_broker.open(task_request).await });
    started.await;

    let failure = expect_failure(broker.open(request.clone()).await);
    assert_eq!(failure.safe_code, "model_egress_duplicate_request");
    broker
        .cancel(
            ModelProviderWireProtocol::OpenAiResponses,
            cancel_for(&request),
        )
        .await
        .unwrap();
    expect_failure(task.await.unwrap());
}

#[test]
fn resolved_secret_debug_output_never_contains_material() {
    let resolved = ResolvedSecretMaterial::new(
        id(ResourceKind::SecretBinding, 40),
        id(ResourceKind::SecretProvider, 41),
        "model_api_key".parse().unwrap(),
        1,
        digest('1'),
        b"never-print-this".to_vec(),
    )
    .unwrap();
    let debug = format!("{resolved:?}");
    assert!(!debug.contains("never-print-this"));
    assert!(!debug.contains("material"));
}

#[test]
fn empty_installed_catalogs_are_explicit_deny_all_closures() {
    InstalledModelProviderEndpointCatalog::new(Vec::new()).unwrap();
    InstalledCapabilityHttpEndpointCatalog::new(Vec::new()).unwrap();
    InstalledCapabilityGrpcEndpointCatalog::new(Vec::new()).unwrap();
    InstalledRemoteContextEndpointCatalog::new(Vec::new()).unwrap();
    InstalledMcpStreamableHttpEndpointCatalog::new(Vec::new()).unwrap();
    InstalledMcpOAuthVerificationCatalog::new(Vec::new()).unwrap();
}
