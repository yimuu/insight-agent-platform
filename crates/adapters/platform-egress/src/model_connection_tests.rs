use super::*;
use insight_platform_contracts::*;
use insight_platform_security::{ModelConnectionProbe, ModelConnectionProbeAuthority};

fn target(f: &Fixture) -> ModelConnectionTargetV1 {
    let provider = ModelProviderDeploymentClosure {
        endpoint: f.entry.endpoint.clone(),
        provider_revision: f.request.provider_revision.clone(),
        endpoint_identity_digest: f.entry.endpoint_identity_digest.clone(),
        secret_bindings: f.request.secret_bindings.clone(),
        protocol_policy: exact_version(ResourceKind::PolicyRevision, 60, '8'),
        network_policy: f.entry.network_policy.clone(),
        tls_policy: f.entry.tls_policy.clone(),
        trust_policy: f.entry.trust_policy.clone(),
        data_policy: f.entry.data_policy.clone(),
        region: f.entry.region.clone(),
        admission_evidence: ModelAdmissionEvidence {
            basis: ModelEvidenceBasis::OperatorDeclaration,
            artifact: ArtifactRef::new(
                id(ResourceKind::Artifact, 62),
                digest('6'),
                16,
                "application/json",
                DataClassification::Internal,
                None,
            )
            .unwrap(),
        },
    };
    let payload =
        TypedPayload::new(1, &DeploymentClosure::ModelProvider(provider.clone())).unwrap();
    ModelConnectionTargetV1 {
        schema_version: 1,
        model_deployment: exact_deployment(ResourceKind::ModelDeployment, 64, '5'),
        profile_revision: exact_version(ResourceKind::ModelProfileRevision, 65, '4'),
        provider_deployment: ExactDeploymentRef::new(
            f.request.provider_deployment.deployment_id.clone(),
            payload.digest.parse().unwrap(),
        )
        .unwrap(),
        provider,
        model_identity: ProviderModelIdentity {
            value: "configured-model".to_owned(),
            stability: ModelIdentityStability::ExternallyMutable,
        },
        protocol: f.entry.protocol,
        installed_adapter: InstalledModelAdapter {
            qualified_name: f.entry.protocol.qualified_name().to_owned(),
            worker_manifest_digest: digest('9'),
            adapter_contract_digest: f.entry.protocol.adapter_contract_digest(),
        },
        request_limits: ProviderRequestLimits {
            maximum_request_bytes: 4096,
            maximum_response_bytes: 65536,
            maximum_messages: 1,
            maximum_parts: 1,
            maximum_tools: 0,
            maximum_parallel_tool_calls: 0,
            maximum_stream_delta_bytes: 4096,
            connect_timeout_milliseconds: 1000,
            first_byte_timeout_milliseconds: 1000,
            idle_timeout_milliseconds: 1000,
            total_timeout_milliseconds: 10000,
        },
        maximum_output_tokens: 32,
        maximum_input_text_bytes: 1024,
        credential_generation: 3,
    }
}
#[test]
fn probe_encoders_preserve_both_protocols_fixed_prompt_and_declared_output_bound() {
    for protocol in [
        ModelProviderWireProtocol::OpenAiResponses,
        ModelProviderWireProtocol::AnthropicMessages,
    ] {
        let mut t = target(&pinned_fixture());
        t.protocol = protocol;
        t.installed_adapter.qualified_name = protocol.qualified_name().to_owned();
        t.installed_adapter.adapter_contract_digest = protocol.adapter_contract_digest();
        t.maximum_output_tokens = 7;
        let bytes = insight_platform_model_adapters::model_connection_request(&t).unwrap();
        let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        let expected = match protocol {
            ModelProviderWireProtocol::OpenAiResponses => serde_json::json!({
                "model":"configured-model", "input":[{"role":"user","content":[{
                    "type":"input_text", "text":"Reply with OK."
                }]}], "max_output_tokens":7, "stream":false, "store":false
            }),
            ModelProviderWireProtocol::AnthropicMessages => serde_json::json!({
                "model":"configured-model", "messages":[{"role":"user","content":[{
                    "type":"text", "text":"Reply with OK."
                }]}], "max_tokens":7, "stream":false
            }),
        };
        assert_eq!(body, expected);
        t.request_limits.maximum_request_bytes = 1;
        assert_eq!(
            insight_platform_model_adapters::model_connection_request(&t).unwrap_err(),
            ModelConnectionError::Rejected
        );
    }
}
fn request(target: &ModelConnectionTargetV1) -> ModelConnectionProbeAuthorizationV1 {
    ModelConnectionProbeAuthorizationV1 {
        schema_version: 1,
        request_id: id(ResourceKind::ServerRequest, 70),
        tenant_id: id(ResourceKind::Tenant, 1),
        principal_id: id(ResourceKind::Principal, 71),
        principal_kind: PrincipalKind::TenantAdmin,
        installation_digest: digest('7'),
        model_deployment: target.model_deployment.clone(),
        environment: "development".to_owned(),
        deadline: UtcTimestamp::from_datetime(Utc::now() + chrono::Duration::seconds(29)),
    }
}
struct ProbeAuthority {
    target: ModelConnectionTargetV1,
    calls: AtomicUsize,
    reject_at: usize,
    drift: bool,
    secrets: Arc<FixtureSecretResolver>,
}
#[async_trait]
impl ModelConnectionProbeAuthority for ProbeAuthority {
    async fn authorize_model_connection_probe(
        &self,
        r: &ModelConnectionProbeAuthorizationV1,
    ) -> Result<ModelConnectionProbePermitV1, ModelConnectionError> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
        assert_eq!(
            self.secrets.calls.load(Ordering::SeqCst),
            if call == 1 { 0 } else { 1 }
        );
        if self.reject_at == call {
            return Err(ModelConnectionError::Rejected);
        }
        let mut target = self.target.clone();
        if self.drift && call == 2 {
            target.maximum_output_tokens = 16;
        }
        Ok(ModelConnectionProbePermitV1 {
            schema_version: 1,
            request_digest: r.canonical_digest()?,
            target_digest: target.canonical_digest()?,
            target,
            valid_until: r.deadline.clone(),
        })
    }
}
struct ProbeTransport {
    calls: AtomicUsize,
    mode: u8,
}
#[async_trait]
impl PinnedModelProviderHttpTransport for ProbeTransport {
    async fn open(&self, r: PinnedHttpRequest) -> Result<PinnedHttpResponse, ModelAdapterFailure> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        assert_eq!(r.url.as_str(), "https://api.example.com/v1/responses");
        assert_eq!(
            r.addresses,
            vec![SocketAddr::new(Ipv4Addr::new(8, 8, 8, 8).into(), 443)]
        );
        let body: serde_json::Value = serde_json::from_slice(&r.body).unwrap();
        assert_eq!(body["input"][0]["content"][0]["text"], MODEL_PROBE_PROMPT);
        assert_eq!(body["max_output_tokens"], 32);
        assert_eq!(body["stream"], false);
        assert!(body.get("tools").is_none());
        assert_eq!(r.headers.get(AUTHORIZATION).unwrap(), "Bearer top-secret");
        assert!(r.headers.get(AUTHORIZATION).unwrap().is_sensitive());
        let body=br#"{"id":"resp_1","object":"response","model":"actual-revision","status":"completed","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"different text"}]}]}"#.to_vec();
        Ok(PinnedHttpResponse {
            status_code: if self.mode == 3 { 401 } else { 200 },
            content_type: "application/json".to_owned(),
            body: match self.mode {
                1 => stream::pending().boxed(),
                2 => stream::once(async { Ok(vec![b'x'; 65537]) }).boxed(),
                _ => stream::once(async { Ok(body) }).boxed(),
            },
        })
    }
}
#[tokio::test]
async fn probe_reauthorizes_after_secret_and_never_sends_after_denial_or_target_drift() {
    for (reject, drift) in [(0, false), (1, false), (2, false), (0, true)] {
        let f = pinned_fixture();
        let target = target(&f);
        let request = request(&target);
        assert!(target.validate());
        let secrets = successful_secrets();
        let authority = Arc::new(ProbeAuthority {
            target,
            calls: AtomicUsize::new(0),
            reject_at: reject,
            drift,
            secrets: secrets.clone(),
        });
        let transport = Arc::new(ProbeTransport {
            calls: AtomicUsize::new(0),
            mode: 0,
        });
        let broker = ReqwestModelProviderEgressBroker::with_transport(
            InstalledModelDestinationCatalog::new(vec![f.entry.clone()]).unwrap(),
            secrets.clone(),
            public_dns(),
            transport.clone(),
            Arc::new(FixtureDispatchAuthority(f.entry.endpoint.clone())),
            ModelProviderEgressLimits::default(),
        )
        .unwrap()
        .with_model_connection_authority(authority.clone());
        let result = broker.probe_model_connection(request).await;
        if reject == 0 && !drift {
            assert_eq!(
                result.unwrap().outcome,
                ModelConnectionOutcome::ResponseReceived
            );
            assert_eq!(transport.calls.load(Ordering::SeqCst), 1)
        } else {
            assert_eq!(result.unwrap_err(), ModelConnectionError::Rejected);
            assert_eq!(transport.calls.load(Ordering::SeqCst), 0)
        }
        assert_eq!(
            secrets.calls.load(Ordering::SeqCst),
            if reject == 1 { 0 } else { 1 }
        );
        assert_eq!(
            broker.capacity_snapshot().available,
            broker.capacity_snapshot().maximum_in_flight
        );
    }
}
fn public_dns() -> Arc<FixtureDnsResolver> {
    Arc::new(FixtureDnsResolver {
        addresses: vec![SocketAddr::new(Ipv4Addr::new(8, 8, 8, 8).into(), 443)],
    })
}
#[tokio::test]
async fn probe_body_deadline_size_and_status_are_closed_and_release_capacity() {
    for (mode, expected) in [
        (1, ModelConnectionOutcome::TimedOut),
        (2, ModelConnectionOutcome::InvalidResponse),
        (3, ModelConnectionOutcome::CredentialsRejected),
    ] {
        let f = pinned_fixture();
        let target = target(&f);
        let mut request = request(&target);
        request.deadline =
            UtcTimestamp::from_datetime(Utc::now() + chrono::Duration::milliseconds(70));
        let secrets = successful_secrets();
        let authority = Arc::new(ProbeAuthority {
            target,
            calls: AtomicUsize::new(0),
            reject_at: 0,
            drift: false,
            secrets: secrets.clone(),
        });
        let broker = ReqwestModelProviderEgressBroker::with_transport(
            InstalledModelDestinationCatalog::new(vec![f.entry.clone()]).unwrap(),
            secrets,
            public_dns(),
            Arc::new(ProbeTransport {
                calls: AtomicUsize::new(0),
                mode,
            }),
            Arc::new(FixtureDispatchAuthority(f.entry.endpoint.clone())),
            ModelProviderEgressLimits::default(),
        )
        .unwrap()
        .with_model_connection_authority(authority);
        assert_eq!(
            broker
                .probe_model_connection(request)
                .await
                .unwrap()
                .outcome,
            expected
        );
        assert_eq!(broker.probe_permits.available_permits(), 4);
        assert_eq!(
            broker.permits.available_permits(),
            broker.limits.maximum_in_flight
        );
    }
}
#[test]
fn probe_permit_binds_complete_target_request_and_original_deadline() {
    let target = target(&pinned_fixture());
    let r = request(&target);
    let p = ModelConnectionProbePermitV1 {
        schema_version: 1,
        request_digest: r.canonical_digest().unwrap(),
        target_digest: target.canonical_digest().unwrap(),
        target,
        valid_until: r.deadline.clone(),
    };
    assert!(p.validate_for(&r, Utc::now()));
    let mut q = p.clone();
    q.target.maximum_output_tokens = 16;
    assert!(!q.validate_for(&r, Utc::now()));
    let mut q = p.clone();
    q.request_digest = digest('a');
    assert!(!q.validate_for(&r, Utc::now()));
    let mut q = p.clone();
    q.valid_until =
        UtcTimestamp::from_datetime(r.deadline_at() + chrono::Duration::microseconds(1));
    assert!(!q.validate_for(&r, Utc::now()));
    let mut q = r.clone();
    q.tenant_id = id(ResourceKind::Tenant, 90);
    assert!(!p.validate_for(&q, Utc::now()));
    let mut value = serde_json::to_value(&r).unwrap();
    value["actor"] = serde_json::json!("foreign");
    assert!(serde_json::from_value::<ModelConnectionProbeAuthorizationV1>(value).is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn probe_actual_https_uses_pinned_address_ca_san_and_nonstream_body() {
    use rcgen::{ExtendedKeyUsagePurpose, SanType};
    use rustls::pki_types::PrivatePkcs8KeyDer;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    struct ServerTask(tokio::task::JoinHandle<()>);
    impl Drop for ServerTask {
        fn drop(&mut self) {
            self.0.abort();
        }
    }
    for wrong_san in [false, true] {
        let mut ca_params = CertificateParams::default();
        ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        let ca = CertifiedIssuer::self_signed(ca_params, KeyPair::generate().unwrap()).unwrap();
        let mut params = CertificateParams::default();
        params.subject_alt_names = vec![SanType::DnsName(
            if wrong_san { "other.test" } else { "localhost" }
                .try_into()
                .unwrap(),
        )];
        params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
        let key = KeyPair::generate().unwrap();
        let certificate = params.signed_by(&key, &ca).unwrap();
        let tls = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(
                vec![certificate.der().clone()],
                PrivatePkcs8KeyDer::from(key.serialize_der()).into(),
            )
            .unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let accepted = Arc::new(AtomicBool::new(false));
        let seen = accepted.clone();
        let mut server = ServerTask(tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let Ok(mut stream) = tokio_rustls::TlsAcceptor::from(Arc::new(tls))
                .accept(socket)
                .await
            else {
                return;
            };
            let mut bytes = vec![];
            let (end, length) = loop {
                let mut chunk = [0u8; 1024];
                let n = stream.read(&mut chunk).await.unwrap();
                assert!(n > 0);
                assert!(bytes.len() + n <= 8192);
                bytes.extend_from_slice(&chunk[..n]);
                if let Some(end) = bytes.windows(4).position(|p| p == b"\r\n\r\n") {
                    let h = std::str::from_utf8(&bytes[..end])
                        .unwrap()
                        .to_ascii_lowercase();
                    assert!(h.starts_with("post /v1/responses http/1.1\r\n"));
                    assert!(h.contains("accept: application/json\r\n"));
                    assert!(h.contains("authorization: bearer top-secret\r\n"));
                    let length = h
                        .lines()
                        .find_map(|l| l.strip_prefix("content-length: "))
                        .unwrap()
                        .parse::<usize>()
                        .unwrap();
                    break (end + 4, length);
                }
            };
            while bytes.len() < end + length {
                let mut chunk = [0u8; 1024];
                let n = stream.read(&mut chunk).await.unwrap();
                assert!(n > 0);
                assert!(bytes.len() + n <= 8192);
                bytes.extend_from_slice(&chunk[..n]);
            }
            let request: serde_json::Value = serde_json::from_slice(&bytes[end..]).unwrap();
            assert_eq!(request["stream"], false);
            assert_eq!(request["max_output_tokens"], 32);
            bytes.fill(0);
            seen.store(true, Ordering::SeqCst);
            let body=br#"{"id":"resp","object":"response","model":"reported-model","status":"completed","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"test"}]}]}"#;
            stream.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",body.len()).as_bytes()).await.unwrap();
            stream.write_all(body).await.unwrap();
            stream.shutdown().await.unwrap();
        }));
        let mut f = pinned_fixture();
        f.entry.endpoint.host = "localhost".to_owned();
        f.entry.endpoint.port = port;
        f.entry.endpoint_identity_digest = f.entry.endpoint.canonical_digest().unwrap();
        f.entry.development_loopback = true;
        f.entry.trusted_root_pem = Some(ca.pem());
        let mut target = target(&f);
        // Give real platform trust verification a bounded fixture budget while
        // retaining the broker's production connect cap and the request deadline.
        target.request_limits.connect_timeout_milliseconds = MODEL_PROBE_CONNECT_MILLISECONDS;
        target.request_limits.total_timeout_milliseconds = 20_000;
        let request = request(&target);
        let secrets = successful_secrets();
        let authority = Arc::new(ProbeAuthority {
            target,
            calls: AtomicUsize::new(0),
            reject_at: 0,
            drift: false,
            secrets: secrets.clone(),
        });
        let broker = ReqwestModelProviderEgressBroker::new(
            InstalledModelDestinationCatalog::new(vec![f.entry.clone()]).unwrap(),
            secrets,
            Arc::new(FixtureDnsResolver {
                addresses: vec![SocketAddr::new(Ipv4Addr::LOCALHOST.into(), port)],
            }),
            Arc::new(FixtureDispatchAuthority(f.entry.endpoint.clone())),
            ModelProviderEgressLimits::default(),
        )
        .unwrap()
        .with_model_connection_authority(authority);
        // Platform trust verification on macOS can exceed the old fixture budget
        // in the full workspace suite. Keep this watchdog inside the request's
        // 29-second deadline; certificate and hostname verification stay enabled.
        let observation = tokio::time::timeout(
            Duration::from_secs(25),
            broker.probe_model_connection(request),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(
            observation.outcome,
            if wrong_san {
                ModelConnectionOutcome::TransportUnavailable
            } else {
                ModelConnectionOutcome::ResponseReceived
            }
        );
        assert_eq!(accepted.load(Ordering::SeqCst), !wrong_san);
        tokio::time::timeout(Duration::from_secs(2), &mut server.0)
            .await
            .unwrap()
            .unwrap();
    }
}

struct CapacityAuthority(ModelConnectionTargetV1);
#[async_trait]
impl ModelConnectionProbeAuthority for CapacityAuthority {
    async fn authorize_model_connection_probe(
        &self,
        r: &ModelConnectionProbeAuthorizationV1,
    ) -> Result<ModelConnectionProbePermitV1, ModelConnectionError> {
        Ok(ModelConnectionProbePermitV1 {
            schema_version: 1,
            request_digest: r.canonical_digest()?,
            target_digest: self.0.canonical_digest()?,
            target: self.0.clone(),
            valid_until: r.deadline.clone(),
        })
    }
}
#[tokio::test]
async fn probe_capacity_is_four_shared_with_business_and_held_until_body_completion() {
    let f = pinned_fixture();
    let target = target(&f);
    let mut request = request(&target);
    request.deadline =
        UtcTimestamp::from_datetime(Utc::now() + chrono::Duration::milliseconds(400));
    let transport = Arc::new(ProbeTransport {
        calls: AtomicUsize::new(0),
        mode: 1,
    });
    let broker = Arc::new(
        ReqwestModelProviderEgressBroker::with_transport(
            InstalledModelDestinationCatalog::new(vec![f.entry.clone()]).unwrap(),
            successful_secrets(),
            public_dns(),
            transport.clone(),
            Arc::new(FixtureDispatchAuthority(f.entry.endpoint.clone())),
            ModelProviderEgressLimits::default(),
        )
        .unwrap()
        .with_model_connection_authority(Arc::new(CapacityAuthority(target))),
    );
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..4 {
        let b = broker.clone();
        let r = request.clone();
        tasks.spawn(async move { b.probe_model_connection(r).await });
    }
    tokio::time::timeout(Duration::from_millis(250), async {
        while transport.calls.load(Ordering::SeqCst) < 4 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(broker.probe_permits.available_permits(), 0);
    assert_eq!(
        broker.permits.available_permits(),
        broker.limits.maximum_in_flight - 4
    );
    assert_eq!(
        broker.probe_model_connection(request).await.unwrap_err(),
        ModelConnectionError::Unavailable
    );
    assert_eq!(transport.calls.load(Ordering::SeqCst), 4);
    while let Some(result) = tasks.join_next().await {
        assert_eq!(
            result.unwrap().unwrap().outcome,
            ModelConnectionOutcome::TimedOut
        )
    }
    assert_eq!(broker.probe_permits.available_permits(), 4);
    assert_eq!(
        broker.permits.available_permits(),
        broker.limits.maximum_in_flight
    );
}
