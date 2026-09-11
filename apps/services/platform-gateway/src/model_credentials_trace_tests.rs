//! Actual public HTTP adapters -> production Gateway wrapper -> authenticated internal TLS.
use super::*;
use axum::{
    body::Body,
    http::{Request as HttpRequest, StatusCode},
    Extension,
};
use insight_platform_api::{
    authentication::{AuthenticatedPrincipal, SystemAuthenticationClock},
    model_credentials::{build_model_credential_router, ModelCredentialHttpState},
};
use insight_platform_contracts::*;
use insight_platform_egress_rpc::{
    proto::{
        egress_broker_service_server::{EgressBrokerService, EgressBrokerServiceServer},
        ClosedEgressEnvelope,
    },
    EgressCallerRole, EgressCallerWorkloadIdentity,
};
use rcgen::{
    BasicConstraints, CertificateParams, CertifiedIssuer, ExtendedKeyUsagePurpose, IsCa, KeyPair,
    SanType,
};
use std::{pin::Pin, sync::Mutex};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Request, Response, Status};
use tower::ServiceExt;

type WireStream = Pin<Box<dyn futures::Stream<Item = Result<ClosedEgressEnvelope, Status>> + Send>>;
#[derive(Clone)]
struct FixtureEgress(Arc<Mutex<Vec<(String, W3cTraceParent)>>>);
impl FixtureEgress {
    fn observe(&self, request: &Request<ClosedEgressEnvelope>) {
        assert_eq!(
            request.extensions().get::<EgressCallerRole>(),
            Some(&EgressCallerRole::Gateway)
        );
        let parent = request
            .metadata()
            .get("traceparent")
            .unwrap()
            .to_str()
            .unwrap()
            .parse()
            .unwrap();
        self.0
            .lock()
            .unwrap()
            .push((request.get_ref().operation.clone(), parent));
    }
}
#[async_trait]
impl EgressBrokerService for FixtureEgress {
    type OpenModelProviderStream = WireStream;
    type StreamMcpStreamableHttpSubscriptionStream = WireStream;
    async fn import_model_credential(
        &self,
        request: Request<ClosedEgressEnvelope>,
    ) -> Result<Response<ClosedEgressEnvelope>, Status> {
        self.observe(&request);
        assert_eq!(request.get_ref().payload, b"synthetic-trace-test-key");
        Err(Status::unavailable(
            "fixture stops before credential storage",
        ))
    }
    async fn probe_model_connection(
        &self,
        request: Request<ClosedEgressEnvelope>,
    ) -> Result<Response<ClosedEgressEnvelope>, Status> {
        self.observe(&request);
        assert!(request.get_ref().payload.is_empty());
        Err(Status::unavailable("fixture stops before model invocation"))
    }
    async fn cancel_model_provider(
        &self,
        _: Request<ClosedEgressEnvelope>,
    ) -> Result<Response<ClosedEgressEnvelope>, Status> {
        Err(Status::unimplemented("outside fixture"))
    }
    async fn round_trip_capability_http(
        &self,
        _: Request<ClosedEgressEnvelope>,
    ) -> Result<Response<ClosedEgressEnvelope>, Status> {
        Err(Status::unimplemented("outside fixture"))
    }
    async fn cancel_capability_http(
        &self,
        _: Request<ClosedEgressEnvelope>,
    ) -> Result<Response<ClosedEgressEnvelope>, Status> {
        Err(Status::unimplemented("outside fixture"))
    }
    async fn unary_capability_grpc(
        &self,
        _: Request<ClosedEgressEnvelope>,
    ) -> Result<Response<ClosedEgressEnvelope>, Status> {
        Err(Status::unimplemented("outside fixture"))
    }
    async fn cancel_capability_grpc(
        &self,
        _: Request<ClosedEgressEnvelope>,
    ) -> Result<Response<ClosedEgressEnvelope>, Status> {
        Err(Status::unimplemented("outside fixture"))
    }
    async fn query_remote_context(
        &self,
        _: Request<ClosedEgressEnvelope>,
    ) -> Result<Response<ClosedEgressEnvelope>, Status> {
        Err(Status::unimplemented("outside fixture"))
    }
    async fn exchange_mcp_o_auth_authorization_code(
        &self,
        _: Request<ClosedEgressEnvelope>,
    ) -> Result<Response<ClosedEgressEnvelope>, Status> {
        Err(Status::unimplemented("outside fixture"))
    }
    async fn delete_mcp_o_auth_pkce_secret(
        &self,
        _: Request<ClosedEgressEnvelope>,
    ) -> Result<Response<ClosedEgressEnvelope>, Status> {
        Err(Status::unimplemented("outside fixture"))
    }
    async fn discover_mcp_streamable_http(
        &self,
        _: Request<ClosedEgressEnvelope>,
    ) -> Result<Response<ClosedEgressEnvelope>, Status> {
        Err(Status::unimplemented("outside fixture"))
    }
    async fn execute_mcp_streamable_http(
        &self,
        _: Request<ClosedEgressEnvelope>,
    ) -> Result<Response<ClosedEgressEnvelope>, Status> {
        Err(Status::unimplemented("outside fixture"))
    }
    async fn refresh_mcp_resources(
        &self,
        _: Request<ClosedEgressEnvelope>,
    ) -> Result<Response<ClosedEgressEnvelope>, Status> {
        Err(Status::unimplemented("outside fixture"))
    }
    async fn cancel_mcp_remote_task(
        &self,
        _: Request<ClosedEgressEnvelope>,
    ) -> Result<Response<ClosedEgressEnvelope>, Status> {
        Err(Status::unimplemented("outside fixture"))
    }
    async fn open_model_provider(
        &self,
        _: Request<ClosedEgressEnvelope>,
    ) -> Result<Response<Self::OpenModelProviderStream>, Status> {
        Err(Status::unimplemented("outside fixture"))
    }
    async fn stream_mcp_streamable_http_subscription(
        &self,
        _: Request<tonic::Streaming<ClosedEgressEnvelope>>,
    ) -> Result<Response<Self::StreamMcpStreamableHttpSubscriptionStream>, Status> {
        Err(Status::unimplemented("outside fixture"))
    }
}
fn fixture_id(kind: ResourceKind) -> ResourceId {
    ResourceId::from_uuid_v7(kind, uuid::Uuid::now_v7()).unwrap()
}
struct ProbeApp(Arc<dyn ModelManagementEgress>);
#[async_trait]
impl insight_platform_api::model_connection::ModelConnectionApplication for ProbeApp {
    async fn probe(
        &self,
        intent: insight_platform_api::model_connection::ModelConnectionIntent,
    ) -> Result<
        ModelConnectionObservationV1,
        insight_platform_api::resource::ResourceApplicationError,
    > {
        self.0
            .probe_model_connection(ModelConnectionProbeAuthorizationV1 {
                schema_version: 1,
                request_id: fixture_id(ResourceKind::ServerRequest),
                tenant_id: intent.principal.tenant_id,
                principal_id: intent.principal.principal_id,
                principal_kind: intent.principal.principal_kind,
                installation_digest: intent.request.installation_digest,
                model_deployment: intent.request.model_deployment,
                environment: "development".to_owned(),
                deadline: UtcTimestamp::from_datetime(intent.deadline),
            })
            .await
            .map_err(|error| match error {
                ModelConnectionError::Rejected => {
                    insight_platform_api::resource::ResourceApplicationError::Denied
                }
                _ => insight_platform_api::resource::ResourceApplicationError::Unavailable,
            })
    }
}
struct OwnedServer(tokio::task::JoinHandle<Result<(), tonic::transport::Error>>);
impl Drop for OwnedServer {
    fn drop(&mut self) {
        self.0.abort();
    }
}
#[tokio::test]
async fn public_model_management_trace_reaches_real_authenticated_egress_without_test_scope() {
    let mut ca_parameters = CertificateParams::default();
    ca_parameters.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    let ca = CertifiedIssuer::self_signed(ca_parameters, KeyPair::generate().unwrap()).unwrap();
    let issue = |sans: Vec<SanType>, usage: ExtendedKeyUsagePurpose| {
        let mut parameters = CertificateParams::default();
        parameters.subject_alt_names = sans;
        parameters.extended_key_usages = vec![usage];
        let key = KeyPair::generate().unwrap();
        let certificate = parameters.signed_by(&key, &ca).unwrap();
        (certificate.pem(), key.serialize_pem())
    };
    let (server_certificate, server_key) = issue(
        vec![SanType::DnsName("egress.test".try_into().unwrap())],
        ExtendedKeyUsagePurpose::ServerAuth,
    );
    let (client_certificate, client_key) = issue(
        vec![SanType::URI(
            insight_platform_egress_rpc::GATEWAY_WORKLOAD_IDENTITY
                .try_into()
                .unwrap(),
        )],
        ExtendedKeyUsagePurpose::ClientAuth,
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let observed = Arc::new(Mutex::new(vec![]));
    let fixture = FixtureEgress(observed.clone());
    let (shutdown, stopped) = tokio::sync::oneshot::channel();
    let server = tonic::transport::Server::builder()
        .tls_config(
            tonic::transport::ServerTlsConfig::new()
                .identity(Identity::from_pem(server_certificate, server_key))
                .client_ca_root(Certificate::from_pem(ca.pem())),
        )
        .unwrap()
        .add_service(EgressBrokerServiceServer::with_interceptor(
            fixture,
            EgressCallerWorkloadIdentity,
        ));
    let mut server = OwnedServer(tokio::spawn(server.serve_with_incoming_shutdown(
        TcpListenerStream::new(listener),
        async {
            let _ = stopped.await;
        },
    )));
    let channel = Endpoint::from_shared(format!("https://{address}"))
        .unwrap()
        .tls_config(
            ClientTlsConfig::new()
                .domain_name("egress.test")
                .ca_certificate(Certificate::from_pem(ca.pem()))
                .identity(Identity::from_pem(client_certificate, client_key)),
        )
        .unwrap()
        .connect()
        .await
        .unwrap();
    let client = public_management_egress(Arc::new(EgressBrokerGrpcClient::new(
        channel,
        EgressInternalRpcLimits::new(4096, 4096).unwrap(),
    )));
    let principal = AuthenticatedPrincipal {
        trace: TraceIdentityV1::generate(),
        tenant_id: fixture_id(ResourceKind::Tenant),
        principal_id: fixture_id(ResourceKind::Principal),
        principal_kind: PrincipalKind::TenantAdmin,
        permissions: PermissionSet::new(vec![Permission::SecretBind, Permission::ModelRead])
            .unwrap(),
        authn_strength: AuthnStrength::MultiFactor,
        principal_version: 1,
        binding_generation: 1,
        binding_version: 1,
        credential_digest: format!("sha256:{}", "a".repeat(64)).parse().unwrap(),
        credential_expires_at: chrono::Utc::now() + chrono::Duration::minutes(2),
    };
    let import = build_model_credential_router(ModelCredentialHttpState::new(
        client.clone(),
        Arc::new(SystemAuthenticationClock),
    ));
    let probe = insight_platform_api::model_connection::build_model_connection_router(
        insight_platform_api::model_connection::ModelConnectionHttpState::new(
            Arc::new(ProbeApp(client)),
            Arc::new(SystemAuthenticationClock),
        ),
    );
    let router = import
        .merge(probe)
        .layer(Extension(principal))
        .layer(axum::middleware::from_fn(
            insight_platform_api::trace::establish_public_trace,
        ));
    let trace = TraceIdentityV1::generate();
    let parent = W3cTraceParent::new(trace.trace_id, SpanId::new(), TraceFlags::NotSampled);
    let inputs = [
        (
            "/v1/model-credentials",
            serde_json::json!({"schema_version":1,"operation_id":uuid::Uuid::new_v4().to_string(),"provider_id":fixture_id(ResourceKind::SecretProvider),"api_key":"synthetic-trace-test-key"}),
        ),
        (
            "/v1/model-configuration:probe",
            serde_json::json!({"schema_version":1,"installation_digest":format!("sha256:{}","a".repeat(64)),"model_deployment":{"resource_kind":"model_deployment","deployment_id":fixture_id(ResourceKind::ModelDeployment),"deployment_digest":format!("sha256:{}","b".repeat(64))}}),
        ),
    ];
    for (path, body) in inputs {
        assert!(insight_platform_execution_context::current_trace().is_err());
        let response = router
            .clone()
            .oneshot(
                HttpRequest::post(path)
                    .header("traceparent", parent.to_string())
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_vec(&body).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            StatusCode::SERVICE_UNAVAILABLE,
            "the fixture must be reached through authenticated internal RPC"
        );
        assert_eq!(response.headers()["trace-id"], trace.trace_id.to_string());
        assert!(insight_platform_execution_context::current_trace().is_err());
    }
    {
        let actual = observed.lock().unwrap();
        assert_eq!(actual.len(), 2);
        assert_eq!(actual[0].0, "model_credential.import/v1");
        assert_eq!(actual[1].0, "model_connection.probe/v1");
        for (_, outbound) in actual.iter() {
            assert_eq!(outbound.trace_id, trace.trace_id);
            assert_ne!(outbound.parent_span_id, parent.parent_span_id);
        }
    }
    let _ = shutdown.send(());
    tokio::time::timeout(Duration::from_secs(2), &mut server.0)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}
