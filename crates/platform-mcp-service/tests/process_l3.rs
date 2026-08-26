use async_trait::async_trait;
use chrono::{Duration as ChronoDuration, Utc};
use insight_platform_capability_adapters::{
    CapabilityAdapterFailure, CapabilityTransportCancelOutcome, CapabilityTransportCancelRequest,
    GrpcNetworkTransport, GrpcTransportRequest, GrpcTransportResponse, HttpNetworkTransport,
    HttpTransportRequest, HttpTransportResponse,
};
use insight_platform_contracts::{
    canonical_digest, AllowedMcpServerCapabilities, ArtifactRef, CanonicalHttpEndpoint,
    CapabilityEndpointScheme, CapabilityIdempotencyKind, ClosedJsonValue, DataClassification,
    Effect, ExactDeploymentRef, ExactSecretBindingRef, ExactVersionRef,
    McpAuthorizationPrincipalKind, McpClientCapabilities, McpMetadataPolicy, McpMethodLimits,
    McpNegotiatedCapabilities, McpProtocolPolicyDocument, McpServerExecutionContract,
    McpServerLimits, McpTransportBinding, McpTransportFeatures, PrincipalKind, PublishedMcpMethod,
    ResourceId, ResourceKind, SecretPurpose, SecretResolutionPolicy, Sha256Digest, TraceFlags,
    TraceIdentityV1, MCP_PROTOCOL_BASELINE,
};
use insight_platform_egress_rpc::{
    proto::egress_broker_service_server::EgressBrokerServiceServer, EgressBrokerGrpcService,
    EgressCallerWorkloadIdentity, EgressInternalRpcLimits, MCP_HOST_WORKLOAD_IDENTITY,
};
use insight_platform_mcp_host::{
    McpAuthorizationContext, McpHostError, McpHostExecutionContract, McpOperationOutcome,
    McpOperationRequest, McpRemoteTaskCancelOutcome, McpStreamableHttpConnector,
    McpStreamableHttpRequest, McpTransportFailure, NewMcpAuthorizationContext,
    NewMcpHostExecutionContract,
};
use insight_platform_mcp_rpc::{McpHostGrpcClient, McpHostInternalRpcLimits};
use insight_platform_model_adapters::{
    ModelAdapterCancelOutcome, ModelAdapterCancelRequest, ModelAdapterFailure,
    ModelProviderWireConnector, ModelProviderWireProtocol, ModelProviderWireRequest,
    ModelProviderWireStream,
};
use insight_platform_rpc_trace::{scope_trace, RpcTraceContext};
use rcgen::{
    BasicConstraints, CertificateParams, CertifiedIssuer, ExtendedKeyUsagePurpose, IsCa, KeyPair,
    KeyUsagePurpose, SanType,
};
use std::{
    collections::BTreeMap,
    path::PathBuf,
    process::Stdio,
    str::FromStr,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::sync::Notify;
use tonic::transport::server::TcpIncoming;
use tonic::transport::{Certificate, ClientTlsConfig, Endpoint, Identity, Server, ServerTlsConfig};

fn id(kind: ResourceKind, suffix: u16) -> ResourceId {
    format!(
        "{}_0198f1c9-32e4-75e1-a9e8-d95ca0f6{suffix:04x}",
        kind.descriptor().prefix
    )
    .parse()
    .unwrap()
}

fn sha(character: char) -> Sha256Digest {
    format!("sha256:{}", character.to_string().repeat(64))
        .parse()
        .unwrap()
}

fn exact(kind: ResourceKind, suffix: u16, character: char) -> ExactVersionRef {
    ExactVersionRef::new(id(kind, suffix), sha(character)).unwrap()
}

fn method_limits() -> McpMethodLimits {
    McpMethodLimits {
        maximum_request_bytes: 4_096,
        maximum_response_bytes: 4_096,
        maximum_metadata_entries: 16,
        maximum_progress_events: 32,
        maximum_pages: 16,
        minimum_poll_milliseconds: 10,
        maximum_poll_milliseconds: 1_000,
    }
}

fn execution_fixture() -> (McpHostExecutionContract, McpOperationRequest) {
    let now = Utc::now();
    let tenant_id = id(ResourceKind::Tenant, 1);
    let deployment = ExactDeploymentRef::new(id(ResourceKind::McpDeployment, 2), sha('2')).unwrap();
    let server_revision = exact(ResourceKind::McpServerRevision, 3, '3');
    let protocol = McpProtocolPolicyDocument {
        schema_version: 1,
        offered_versions: vec![MCP_PROTOCOL_BASELINE.to_owned()],
        transport_features: McpTransportFeatures {
            streamable_http_get: true,
            streamable_http_sse: true,
            resumable_stream: true,
            session_affinity: true,
        },
        client_capabilities: McpClientCapabilities {
            elicitation_form: true,
            elicitation_url: false,
            tasks_elicitation_create: false,
            sampling: false,
            roots: false,
        },
        allowed_server_capabilities: AllowedMcpServerCapabilities {
            tools: true,
            resources: false,
            prompts: false,
            logging: false,
            tasks: false,
            subscriptions: false,
        },
        experimental_features: vec![],
        method_limits: BTreeMap::from([(PublishedMcpMethod::ToolsCall, method_limits())]),
        metadata_policy: McpMetadataPolicy {
            maximum_server_name_bytes: 128,
            maximum_server_version_bytes: 64,
            maximum_instruction_bytes: 4_096,
            maximum_object_name_bytes: 128,
            maximum_description_bytes: 8_192,
            maximum_icon_bytes: 1_048_576,
        },
    };
    let protocol_policy = ExactVersionRef::new(
        id(ResourceKind::PolicyRevision, 4),
        protocol.canonical_digest().unwrap(),
    )
    .unwrap();
    let endpoint = CanonicalHttpEndpoint {
        scheme: CapabilityEndpointScheme::Https,
        host: "mcp.remote.test".to_owned(),
        port: 443,
        base_path: "/v1/mcp".to_owned(),
    };
    let server_identity_digest = endpoint.canonical_digest().unwrap();
    let trust_policy = exact(ResourceKind::PolicyRevision, 7, '7');
    let closure = insight_platform_contracts::McpDeploymentClosure {
        server_revision: server_revision.clone(),
        server_identity_digest: server_identity_digest.clone(),
        transport: McpTransportBinding::StreamableHttp {
            endpoint,
            endpoint_identity_digest: server_identity_digest.clone(),
            network_policy: exact(ResourceKind::PolicyRevision, 5, '5'),
            tls_policy: exact(ResourceKind::PolicyRevision, 6, '6'),
        },
        protocol_policy: protocol_policy.clone(),
        trust_policy,
        auth_policy: Some(exact(ResourceKind::PolicyRevision, 8, '8')),
        secret_bindings: vec![],
        conformance_evidence: ArtifactRef::new(
            id(ResourceKind::Artifact, 10),
            sha('a'),
            128,
            "application/json",
            DataClassification::Internal,
            Some("mcp-conformance.json".to_owned()),
        )
        .unwrap(),
    };
    let purpose = SecretPurpose::from_str("mcp.oauth").unwrap();
    let server = McpServerExecutionContract::build(
        server_revision.clone(),
        insight_platform_contracts::McpTransportKind::StreamableHttp,
        protocol_policy.clone(),
        vec![],
        Some(purpose.clone()),
        McpServerLimits {
            maximum_message_bytes: 8_192,
            maximum_response_bytes: 8_192,
            maximum_headers: 32,
            maximum_sse_event_bytes: 4_096,
            maximum_in_flight: 8,
            maximum_connections: 4,
            maximum_sessions: 4,
            maximum_session_milliseconds: 3_600_000,
            idle_timeout_milliseconds: 5_000,
            initialize_timeout_milliseconds: 5_000,
            request_timeout_milliseconds: 30_000,
            total_timeout_milliseconds: 30_000,
        },
    )
    .unwrap();
    let authorization = McpAuthorizationContext::build(NewMcpAuthorizationContext {
        tenant_id: tenant_id.clone(),
        authorization_binding_id: id(ResourceKind::McpAuthorizationBinding, 11),
        mcp_deployment: deployment.clone(),
        principal_kind: McpAuthorizationPrincipalKind::PerUser,
        principal_id: id(ResourceKind::Principal, 12),
        principal_identity_kind: PrincipalKind::AgentRunner,
        principal_binding_generation: 1,
        audience_identity_digest: server_identity_digest,
        granted_scopes: vec!["tools.call".to_owned(), "tools.read".to_owned()],
        token_secret_binding: ExactSecretBindingRef::build(
            id(ResourceKind::SecretBinding, 9),
            1,
            id(ResourceKind::SecretProvider, 9),
            purpose,
            SecretResolutionPolicy::Pinned {
                opaque_version_identity_digest: sha('9'),
            },
        )
        .unwrap(),
        generation: 1,
        expires_at: now + ChronoDuration::hours(1),
    })
    .unwrap();
    let discovery = insight_platform_contracts::McpDiscoverySnapshot::build(
        id(ResourceKind::McpDiscoverySnapshot, 14),
        deployment.clone(),
        server_revision,
        protocol_policy,
        authorization.canonical_digest.clone(),
        MCP_PROTOCOL_BASELINE.to_owned(),
        McpNegotiatedCapabilities {
            tools: true,
            resources: false,
            prompts: false,
            logging: false,
            tasks: false,
            tasks_list: false,
            tasks_cancel: false,
            tasks_tools_call: false,
            elicitation: true,
            sampling: false,
            roots: false,
            subscriptions: false,
        },
        ArtifactRef::new(
            id(ResourceKind::Artifact, 13),
            sha('d'),
            256,
            "application/json",
            DataClassification::Internal,
            Some("mcp-discovery.json".to_owned()),
        )
        .unwrap(),
        now - ChronoDuration::seconds(1),
        now + ChronoDuration::minutes(30),
    )
    .unwrap();
    let contract = McpHostExecutionContract::build(NewMcpHostExecutionContract {
        deployment,
        deployment_closure: closure,
        server,
        protocol_profile: protocol,
        authorization,
        discovery,
    })
    .unwrap();
    let request = McpOperationRequest {
        schema_version: 1,
        mcp_operation_id: id(ResourceKind::McpOperation, 15),
        tenant_id,
        invocation_id: id(ResourceKind::CapabilityInvocation, 16),
        job_id: id(ResourceKind::Job, 17),
        worker_process_generation_id: id(ResourceKind::WorkerProcessGeneration, 18),
        lease_generation: 1,
        physical_attempt: 1,
        snapshot_id: contract.discovery.snapshot_id.clone(),
        authorization_binding_id: contract.authorization.authorization_binding_id.clone(),
        method: PublishedMcpMethod::ToolsCall,
        params: ClosedJsonValue::build(sha('e'), serde_json::json!({"query": "hello"})).unwrap(),
        task_requested: false,
        continuation: None,
        idempotency_key_digest: sha('f'),
        idempotency: CapabilityIdempotencyKind::None,
        effect: Effect::ReadOnly,
        deadline: now + ChronoDuration::minutes(5),
    };
    (contract, request)
}

struct EmptyModel;

#[async_trait]
impl ModelProviderWireConnector for EmptyModel {
    async fn open(
        &self,
        _request: ModelProviderWireRequest,
    ) -> Result<ModelProviderWireStream, ModelAdapterFailure> {
        Ok(Box::pin(futures::stream::empty()))
    }

    async fn cancel(
        &self,
        _protocol: ModelProviderWireProtocol,
        _request: ModelAdapterCancelRequest,
    ) -> Result<ModelAdapterCancelOutcome, ModelAdapterFailure> {
        Ok(ModelAdapterCancelOutcome::Unsupported)
    }
}

struct EmptyHttp;

#[async_trait]
impl HttpNetworkTransport for EmptyHttp {
    async fn round_trip(
        &self,
        _request: HttpTransportRequest,
    ) -> Result<HttpTransportResponse, CapabilityAdapterFailure> {
        unreachable!()
    }

    async fn cancel(
        &self,
        _request: CapabilityTransportCancelRequest,
    ) -> Result<CapabilityTransportCancelOutcome, CapabilityAdapterFailure> {
        Ok(CapabilityTransportCancelOutcome::Accepted)
    }
}

struct EmptyGrpc;

#[async_trait]
impl GrpcNetworkTransport for EmptyGrpc {
    async fn unary(
        &self,
        _request: GrpcTransportRequest,
    ) -> Result<GrpcTransportResponse, CapabilityAdapterFailure> {
        unreachable!()
    }

    async fn cancel(
        &self,
        _request: CapabilityTransportCancelRequest,
    ) -> Result<CapabilityTransportCancelOutcome, CapabilityAdapterFailure> {
        Ok(CapabilityTransportCancelOutcome::Accepted)
    }
}

struct DelayedFirstMcp {
    calls: AtomicUsize,
    first_started: Notify,
}

#[async_trait]
impl McpStreamableHttpConnector for DelayedFirstMcp {
    async fn execute(
        &self,
        _request: McpStreamableHttpRequest,
    ) -> Result<McpOperationOutcome, McpTransportFailure> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        if call == 0 {
            self.first_started.notify_one();
            tokio::time::sleep(Duration::from_secs(30)).await;
        }
        Ok(McpOperationOutcome::Completed {
            result: ClosedJsonValue::build(sha('1'), serde_json::json!({"ok": true})).unwrap(),
            evidence_digest: sha('2'),
        })
    }

    async fn cancel_remote_task(
        &self,
        _request: McpStreamableHttpRequest,
    ) -> Result<McpRemoteTaskCancelOutcome, McpTransportFailure> {
        Ok(McpRemoteTaskCancelOutcome::Accepted)
    }
}

struct TlsFixture {
    ca: String,
    egress_cert: String,
    egress_key: String,
    host_cert: String,
    host_key: String,
    host_client_cert: String,
    host_client_key: String,
    capability_cert: String,
    capability_key: String,
}

fn tls_fixture() -> TlsFixture {
    let mut params = CertificateParams::default();
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params.key_usages = vec![
        KeyUsagePurpose::DigitalSignature,
        KeyUsagePurpose::KeyCertSign,
        KeyUsagePurpose::CrlSign,
    ];
    let ca = CertifiedIssuer::self_signed(params, KeyPair::generate().unwrap()).unwrap();
    let issue = |sans, usage| {
        let mut params = CertificateParams::default();
        params.subject_alt_names = sans;
        params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        params.extended_key_usages = vec![usage];
        let key = KeyPair::generate().unwrap();
        let cert = params.signed_by(&key, &ca).unwrap();
        (cert.pem(), key.serialize_pem())
    };
    let (egress_cert, egress_key) = issue(
        vec![SanType::DnsName("egress.test".try_into().unwrap())],
        ExtendedKeyUsagePurpose::ServerAuth,
    );
    let (host_cert, host_key) = issue(
        vec![SanType::DnsName("mcp-host.test".try_into().unwrap())],
        ExtendedKeyUsagePurpose::ServerAuth,
    );
    let (host_client_cert, host_client_key) = issue(
        vec![SanType::URI(MCP_HOST_WORKLOAD_IDENTITY.try_into().unwrap())],
        ExtendedKeyUsagePurpose::ClientAuth,
    );
    let (capability_cert, capability_key) = issue(
        vec![SanType::URI(
            insight_platform_mcp_rpc::CAPABILITY_WORKER_WORKLOAD_IDENTITY
                .try_into()
                .unwrap(),
        )],
        ExtendedKeyUsagePurpose::ClientAuth,
    );
    TlsFixture {
        ca: ca.pem(),
        egress_cert,
        egress_key,
        host_cert,
        host_key,
        host_client_cert,
        host_client_key,
        capability_cert,
        capability_key,
    }
}

async fn read_line(stderr: std::process::ChildStderr) -> String {
    tokio::task::spawn_blocking(move || {
        let mut line = String::new();
        std::io::BufRead::read_line(&mut std::io::BufReader::new(stderr), &mut line).unwrap();
        line
    })
    .await
    .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn production_host_kill_and_restart_preserves_safe_replay_boundary() {
    let Ok(binary) = std::env::var("PLATFORM_MCP_HOST_BIN") else {
        eprintln!("PLATFORM_MCP_HOST_BIN is unset; MCP Host process L3 skipped");
        return;
    };
    let tls = tls_fixture();
    let incoming = TcpIncoming::bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let egress_address = incoming.local_addr().unwrap();
    let connector = Arc::new(DelayedFirstMcp {
        calls: AtomicUsize::new(0),
        first_started: Notify::new(),
    });
    let limits = EgressInternalRpcLimits::new(65_536, 1_048_576).unwrap();
    let service = EgressBrokerServiceServer::new(
        EgressBrokerGrpcService::new(
            Arc::new(EmptyModel),
            Arc::new(EmptyHttp),
            Arc::new(EmptyGrpc),
            limits,
        )
        .with_mcp_streamable_http(connector.clone()),
    );
    let service =
        tonic::service::interceptor::InterceptedService::new(service, EgressCallerWorkloadIdentity);
    let (egress_shutdown, egress_shutdown_rx) = tokio::sync::oneshot::channel();
    let egress_server = tokio::spawn(
        Server::builder()
            .tls_config(
                ServerTlsConfig::new()
                    .identity(Identity::from_pem(&tls.egress_cert, &tls.egress_key))
                    .client_ca_root(Certificate::from_pem(tls.ca.clone())),
            )
            .unwrap()
            .add_service(service)
            .serve_with_incoming_shutdown(incoming, async move {
                let _ = egress_shutdown_rx.await;
            }),
    );

    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let host_address = listener.local_addr().unwrap();
    drop(listener);
    let observability_listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let observability_address = observability_listener.local_addr().unwrap();
    drop(observability_listener);
    let prefix = format!("/tmp/platform-mcp-host-process-l3-{}", std::process::id());
    let config_path = PathBuf::from(format!("{prefix}.json"));
    let ca_path = PathBuf::from(format!("{prefix}-ca.pem"));
    let host_cert_path = PathBuf::from(format!("{prefix}-host.pem"));
    let host_key_path = PathBuf::from(format!("{prefix}-host-key.pem"));
    let egress_cert_path = PathBuf::from(format!("{prefix}-egress-client.pem"));
    let egress_key_path = PathBuf::from(format!("{prefix}-egress-client-key.pem"));
    let config = serde_json::json!({
        "schema_version": 1,
        "listen_address": host_address.to_string(),
        "observability_listen_address": observability_address.to_string(),
        "tls_server_name": "mcp-host.test",
        "maximum_rpc_message_bytes": 1048576,
        "maximum_in_flight_requests": 8,
        "egress": {
            "endpoint": format!("https://{egress_address}/"),
            "tls_server_name": "egress.test",
            "connect_timeout_milliseconds": 1000,
            "request_timeout_milliseconds": 30000,
            "maximum_rpc_metadata_bytes": 65536,
            "maximum_rpc_payload_bytes": 1048576
        },
        "drain_grace_milliseconds": 1000
    });
    std::fs::write(&config_path, serde_json::to_vec(&config).unwrap()).unwrap();
    std::fs::write(&ca_path, &tls.ca).unwrap();
    std::fs::write(&host_cert_path, &tls.host_cert).unwrap();
    std::fs::write(&host_key_path, &tls.host_key).unwrap();
    std::fs::write(&egress_cert_path, &tls.host_client_cert).unwrap();
    std::fs::write(&egress_key_path, &tls.host_client_key).unwrap();
    let config_digest = canonical_digest(&config).unwrap();
    let spawn_host = || {
        std::process::Command::new(&binary)
            .env("PLATFORM_MCP_HOST_CONFIG", &config_path)
            .env("PLATFORM_MCP_HOST_CONFIG_DIGEST", &config_digest)
            .env("PLATFORM_MCP_HOST_SERVER_CLIENT_CA_PATH", &ca_path)
            .env("PLATFORM_MCP_HOST_SERVER_CERT_PATH", &host_cert_path)
            .env("PLATFORM_MCP_HOST_SERVER_KEY_PATH", &host_key_path)
            .env("PLATFORM_MCP_HOST_EGRESS_CA_PATH", &ca_path)
            .env("PLATFORM_MCP_HOST_EGRESS_CERT_PATH", &egress_cert_path)
            .env("PLATFORM_MCP_HOST_EGRESS_KEY_PATH", &egress_key_path)
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap()
    };
    let connect_client = || async {
        let tls_config = ClientTlsConfig::new()
            .domain_name("mcp-host.test")
            .ca_certificate(Certificate::from_pem(tls.ca.clone()))
            .identity(Identity::from_pem(
                &tls.capability_cert,
                &tls.capability_key,
            ));
        let mut last = None;
        for _ in 0..100 {
            match Endpoint::from_shared(format!("https://{host_address}/"))
                .unwrap()
                .tls_config(tls_config.clone())
                .unwrap()
                .connect()
                .await
            {
                Ok(channel) => return channel,
                Err(error) => last = Some(error),
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("MCP Host did not accept mTLS: {:?}", last.unwrap());
    };

    let mut first = spawn_host();
    let startup = read_line(first.stderr.take().unwrap()).await;
    assert!(startup.contains("platform-mcp-host started"), "{startup}");
    let client = McpHostGrpcClient::new(
        connect_client().await,
        McpHostInternalRpcLimits::new(1_048_576).unwrap(),
    );
    let (contract, request) = execution_fixture();
    let mut pending = tokio::spawn({
        let client = client.clone();
        let contract = contract.clone();
        let request = request.clone();
        async move {
            let trace = RpcTraceContext::start(TraceIdentityV1::generate(), TraceFlags::NotSampled)
                .unwrap();
            scope_trace(
                trace,
                insight_platform_mcp_host::McpHostClient::execute(&client, &contract, &request),
            )
            .await
        }
    });
    let reached_egress = tokio::select! {
        _ = connector.first_started.notified() => Ok(()),
        outcome = &mut pending => Err(format!(
            "MCP Host RPC completed before reaching Egress: {outcome:?}"
        )),
        _ = tokio::time::sleep(Duration::from_secs(10)) => Err(
            "MCP Host RPC did not reach Egress within 10 seconds".to_owned()
        ),
    };
    if let Err(failure) = reached_egress {
        pending.abort();
        let _ = first.kill();
        let _ = first.wait();
        egress_server.abort();
        for path in [
            &config_path,
            &ca_path,
            &host_cert_path,
            &host_key_path,
            &egress_cert_path,
            &egress_key_path,
        ] {
            let _ = std::fs::remove_file(path);
        }
        panic!("{failure}");
    }
    first.kill().unwrap();
    first.wait().unwrap();
    assert_eq!(pending.await.unwrap(), Err(McpHostError::CompletionUnknown));
    assert_eq!(connector.calls.load(Ordering::SeqCst), 1);

    let mut second = spawn_host();
    let startup = read_line(second.stderr.take().unwrap()).await;
    assert!(startup.contains("platform-mcp-host started"), "{startup}");
    let client = McpHostGrpcClient::new(
        connect_client().await,
        McpHostInternalRpcLimits::new(1_048_576).unwrap(),
    );
    let trace =
        RpcTraceContext::start(TraceIdentityV1::generate(), TraceFlags::NotSampled).unwrap();
    let outcome = scope_trace(
        trace,
        insight_platform_mcp_host::McpHostClient::execute(&client, &contract, &request),
    )
    .await
    .unwrap();
    assert!(matches!(outcome, McpOperationOutcome::Completed { .. }));
    assert_eq!(connector.calls.load(Ordering::SeqCst), 2);
    second.kill().unwrap();
    second.wait().unwrap();

    let _ = egress_shutdown.send(());
    egress_server.await.unwrap().unwrap();
    for path in [
        config_path,
        ca_path,
        host_cert_path,
        host_key_path,
        egress_cert_path,
        egress_key_path,
    ] {
        std::fs::remove_file(path).unwrap();
    }
}
