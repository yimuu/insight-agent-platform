//! Versioned internal gRPC boundary for the independently deployed Artifact Broker.
//!
//! Model Workers submit one exact, credential-free read authority request. The Broker returns
//! bytes only after PostgreSQL authorization, exact-version object verification and a second
//! authorization. The wire is bounded, canonical and chunked; it never carries an object locator,
//! storage credential or generic operation name supplied by the caller.

use async_trait::async_trait;
use futures::{stream, Stream};
use insight_platform_contracts::{parse_strict_json, JsonLimits, Sha256Digest};
use insight_platform_models::{
    ModelArtifactBroker, ModelArtifactBrokerError, ModelArtifactReadRequest, ModelTurnLimits,
};
use serde::{de::DeserializeOwned, Serialize};
use sha2::{Digest as _, Sha256};
use std::{error::Error, fmt, pin::Pin, sync::Arc};
use tonic::{Request, Response, Status};
use x509_parser::{extensions::GeneralName, parse_x509_certificate};

pub mod proto {
    tonic::include_proto!("insight.platform.v1");
}

use proto::{
    artifact_model_broker_service_client::ArtifactModelBrokerServiceClient,
    artifact_model_broker_service_server::ArtifactModelBrokerService, ArtifactReadChunk,
    ClosedArtifactReadRequest,
};

pub const ARTIFACT_INTERNAL_RPC_SCHEMA_VERSION: u32 = 1;
pub const MODEL_WORKER_WORKLOAD_IDENTITY: &str = "spiffe://insight.platform/workload/model-worker";
pub const MAX_ARTIFACT_RPC_REQUEST_BYTES_HARD: usize = 1_048_576;
pub const MAX_ARTIFACT_RPC_CHUNK_BYTES_HARD: usize = 262_144;
const MODEL_REQUEST_READ_OPERATION: &str = "artifact.model_request.read/v1";
const MODEL_REQUEST_CHUNK_OPERATION: &str = "artifact.model_request.chunk/v1";
const RPC_MESSAGE_OVERHEAD_BYTES: usize = 8_192;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ArtifactInternalRpcLimits {
    maximum_request_bytes: usize,
    maximum_chunk_bytes: usize,
}

impl ArtifactInternalRpcLimits {
    pub fn new(
        maximum_request_bytes: usize,
        maximum_chunk_bytes: usize,
    ) -> Result<Self, ArtifactRpcError> {
        if !(1..=MAX_ARTIFACT_RPC_REQUEST_BYTES_HARD).contains(&maximum_request_bytes)
            || !(1..=MAX_ARTIFACT_RPC_CHUNK_BYTES_HARD).contains(&maximum_chunk_bytes)
        {
            return Err(ArtifactRpcError::InvalidConfiguration);
        }
        Ok(Self {
            maximum_request_bytes,
            maximum_chunk_bytes,
        })
    }

    pub const fn maximum_request_bytes(self) -> usize {
        self.maximum_request_bytes
    }

    pub const fn maximum_chunk_bytes(self) -> usize {
        self.maximum_chunk_bytes
    }

    pub const fn maximum_message_bytes(self) -> usize {
        if self.maximum_request_bytes > self.maximum_chunk_bytes {
            self.maximum_request_bytes + RPC_MESSAGE_OVERHEAD_BYTES
        } else {
            self.maximum_chunk_bytes + RPC_MESSAGE_OVERHEAD_BYTES
        }
    }
}

impl Default for ArtifactInternalRpcLimits {
    fn default() -> Self {
        Self {
            maximum_request_bytes: MAX_ARTIFACT_RPC_REQUEST_BYTES_HARD,
            maximum_chunk_bytes: MAX_ARTIFACT_RPC_CHUNK_BYTES_HARD,
        }
    }
}

/// Endpoint-role authorization after tonic/rustls has verified the client certificate chain.
#[derive(Debug, Clone, Copy, Default)]
pub struct ModelWorkerWorkloadIdentity;

impl tonic::service::Interceptor for ModelWorkerWorkloadIdentity {
    fn call(&mut self, request: Request<()>) -> Result<Request<()>, Status> {
        let certificates = request
            .peer_certs()
            .ok_or_else(|| Status::unauthenticated("client certificate is required"))?;
        let leaf = certificates
            .first()
            .ok_or_else(|| Status::unauthenticated("client certificate is required"))?;
        require_exact_workload_uri(leaf.as_ref(), MODEL_WORKER_WORKLOAD_IDENTITY)?;
        Ok(request)
    }
}

fn require_exact_workload_uri(certificate: &[u8], expected: &str) -> Result<(), Status> {
    let (remainder, certificate) = parse_x509_certificate(certificate)
        .map_err(|_| Status::unauthenticated("client certificate is invalid"))?;
    if !remainder.is_empty() {
        return Err(Status::unauthenticated("client certificate is invalid"));
    }
    let alternative_names = certificate
        .subject_alternative_name()
        .map_err(|_| Status::unauthenticated("client certificate identity is invalid"))?
        .ok_or_else(|| Status::permission_denied("workload identity is not authorized"))?;
    let mut uris = alternative_names
        .value
        .general_names
        .iter()
        .filter_map(|name| match name {
            GeneralName::URI(uri) => Some(*uri),
            _ => None,
        });
    if uris.next() != Some(expected) || uris.next().is_some() {
        return Err(Status::permission_denied(
            "workload identity is not authorized",
        ));
    }
    Ok(())
}

#[derive(Clone)]
pub struct ArtifactModelBrokerGrpcClient {
    client: ArtifactModelBrokerServiceClient<tonic::transport::Channel>,
    rpc_limits: ArtifactInternalRpcLimits,
    model_limits: ModelTurnLimits,
}

impl ArtifactModelBrokerGrpcClient {
    pub fn new(
        channel: tonic::transport::Channel,
        rpc_limits: ArtifactInternalRpcLimits,
        model_limits: ModelTurnLimits,
    ) -> Self {
        let maximum = rpc_limits.maximum_message_bytes();
        Self {
            client: ArtifactModelBrokerServiceClient::new(channel)
                .max_encoding_message_size(maximum)
                .max_decoding_message_size(maximum),
            rpc_limits,
            model_limits,
        }
    }
}

#[async_trait]
impl ModelArtifactBroker for ArtifactModelBrokerGrpcClient {
    async fn read_exact(
        &self,
        request: ModelArtifactReadRequest,
    ) -> Result<Vec<u8>, ModelArtifactBrokerError> {
        request
            .validate(self.model_limits)
            .map_err(|_| ModelArtifactBrokerError::Denied)?;
        let artifact = request
            .artifact()
            .cloned()
            .ok_or(ModelArtifactBrokerError::Denied)?;
        let envelope = encode_request(MODEL_REQUEST_READ_OPERATION, &request, self.rpc_limits)
            .map_err(|_| ModelArtifactBrokerError::Integrity)?;
        let request_digest = envelope.request_digest.clone();
        let mut client = self.client.clone();
        let mut response = client
            .read_model_request(envelope)
            .await
            .map_err(map_client_status)?
            .into_inner();
        let expected_length = usize::try_from(artifact.byte_length())
            .map_err(|_| ModelArtifactBrokerError::TooLarge)?;
        if expected_length != request.maximum_bytes {
            return Err(ModelArtifactBrokerError::Integrity);
        }
        let mut bytes = Vec::with_capacity(expected_length);
        let mut expected_sequence = 0_u64;
        let mut observed_terminal = false;
        while let Some(chunk) = response.message().await.map_err(map_client_status)? {
            if observed_terminal {
                return Err(ModelArtifactBrokerError::Integrity);
            }
            validate_chunk(
                &chunk,
                &request_digest,
                expected_sequence,
                &artifact,
                self.rpc_limits,
            )?;
            let next_length = bytes
                .len()
                .checked_add(chunk.payload.len())
                .ok_or(ModelArtifactBrokerError::TooLarge)?;
            if next_length > expected_length {
                return Err(ModelArtifactBrokerError::TooLarge);
            }
            bytes.extend_from_slice(&chunk.payload);
            expected_sequence = expected_sequence
                .checked_add(1)
                .ok_or(ModelArtifactBrokerError::Integrity)?;
            observed_terminal = chunk.terminal;
        }
        if !observed_terminal
            || bytes.len() != expected_length
            || digest_bytes(&bytes) != *artifact.content_digest()
        {
            bytes.fill(0);
            return Err(ModelArtifactBrokerError::Integrity);
        }
        Ok(bytes)
    }
}

fn validate_chunk(
    chunk: &ArtifactReadChunk,
    request_digest: &str,
    expected_sequence: u64,
    artifact: &insight_platform_contracts::ArtifactRef,
    limits: ArtifactInternalRpcLimits,
) -> Result<(), ModelArtifactBrokerError> {
    let expected_length = artifact.byte_length();
    if chunk.schema_version != ARTIFACT_INTERNAL_RPC_SCHEMA_VERSION
        || chunk.operation != MODEL_REQUEST_CHUNK_OPERATION
        || chunk.request_digest != request_digest
        || chunk.sequence != expected_sequence
        || chunk.payload.is_empty()
        || chunk.payload.len() > limits.maximum_chunk_bytes()
        || (!chunk.terminal && chunk.payload.len() != limits.maximum_chunk_bytes())
        || chunk.total_length != expected_length
        || chunk.content_digest != artifact.content_digest().to_string()
        || chunk.payload_digest != digest_bytes(&chunk.payload).to_string()
    {
        return Err(ModelArtifactBrokerError::Integrity);
    }
    Ok(())
}

pub struct ArtifactModelBrokerGrpcService<B> {
    broker: Arc<B>,
    rpc_limits: ArtifactInternalRpcLimits,
    model_limits: ModelTurnLimits,
}

impl<B> ArtifactModelBrokerGrpcService<B> {
    pub fn new(
        broker: Arc<B>,
        rpc_limits: ArtifactInternalRpcLimits,
        model_limits: ModelTurnLimits,
    ) -> Self {
        Self {
            broker,
            rpc_limits,
            model_limits,
        }
    }
}

#[tonic::async_trait]
impl<B> ArtifactModelBrokerService for ArtifactModelBrokerGrpcService<B>
where
    B: ModelArtifactBroker + 'static,
{
    type ReadModelRequestStream =
        Pin<Box<dyn Stream<Item = Result<ArtifactReadChunk, Status>> + Send + 'static>>;

    async fn read_model_request(
        &self,
        request: Request<ClosedArtifactReadRequest>,
    ) -> Result<Response<Self::ReadModelRequestStream>, Status> {
        let envelope = request.into_inner();
        let request_digest = envelope.request_digest.clone();
        let read: ModelArtifactReadRequest =
            decode_request(envelope, MODEL_REQUEST_READ_OPERATION, self.rpc_limits)
                .map_err(Status::from)?;
        read.validate(self.model_limits)
            .map_err(|_| Status::invalid_argument("invalid Artifact read request"))?;
        let artifact = read
            .artifact()
            .cloned()
            .ok_or_else(|| Status::invalid_argument("invalid Artifact read request"))?;
        let bytes = self
            .broker
            .read_exact(read)
            .await
            .map_err(map_server_error)?;
        if u64::try_from(bytes.len()).ok() != Some(artifact.byte_length())
            || digest_bytes(&bytes) != *artifact.content_digest()
        {
            return Err(Status::data_loss("Artifact Broker returned invalid bytes"));
        }
        let chunks = encode_chunks(
            &bytes,
            &request_digest,
            &artifact,
            self.rpc_limits.maximum_chunk_bytes(),
        )?;
        Ok(Response::new(Box::pin(stream::iter(
            chunks.into_iter().map(Ok),
        ))))
    }
}

fn encode_request<T: Serialize>(
    operation: &str,
    value: &T,
    limits: ArtifactInternalRpcLimits,
) -> Result<ClosedArtifactReadRequest, ArtifactRpcError> {
    let canonical_request_json =
        serde_jcs::to_vec(value).map_err(|_| ArtifactRpcError::InvalidEnvelope)?;
    if canonical_request_json.is_empty()
        || canonical_request_json.len() > limits.maximum_request_bytes()
    {
        return Err(ArtifactRpcError::InvalidEnvelope);
    }
    let request_digest = digest_bytes(&canonical_request_json).to_string();
    Ok(ClosedArtifactReadRequest {
        schema_version: ARTIFACT_INTERNAL_RPC_SCHEMA_VERSION,
        operation: operation.to_owned(),
        canonical_request_json,
        request_digest,
    })
}

fn decode_request<T: DeserializeOwned>(
    envelope: ClosedArtifactReadRequest,
    expected_operation: &str,
    limits: ArtifactInternalRpcLimits,
) -> Result<T, ArtifactRpcError> {
    if envelope.schema_version != ARTIFACT_INTERNAL_RPC_SCHEMA_VERSION
        || envelope.operation != expected_operation
        || envelope.canonical_request_json.is_empty()
        || envelope.canonical_request_json.len() > limits.maximum_request_bytes()
        || envelope.request_digest != digest_bytes(&envelope.canonical_request_json).to_string()
    {
        return Err(ArtifactRpcError::InvalidEnvelope);
    }
    let value = parse_strict_json(
        &envelope.canonical_request_json,
        JsonLimits {
            max_bytes: limits.maximum_request_bytes(),
            max_depth: 24,
            max_items_per_array: 64,
            max_properties_per_object: 64,
            max_string_bytes: 16_384,
        },
    )
    .map_err(|_| ArtifactRpcError::InvalidEnvelope)?;
    if serde_jcs::to_vec(&value).map_err(|_| ArtifactRpcError::InvalidEnvelope)?
        != envelope.canonical_request_json
    {
        return Err(ArtifactRpcError::InvalidEnvelope);
    }
    serde_json::from_value(value).map_err(|_| ArtifactRpcError::InvalidEnvelope)
}

fn encode_chunks(
    bytes: &[u8],
    request_digest: &str,
    artifact: &insight_platform_contracts::ArtifactRef,
    maximum_chunk_bytes: usize,
) -> Result<Vec<ArtifactReadChunk>, Status> {
    if bytes.is_empty() || maximum_chunk_bytes == 0 {
        return Err(Status::data_loss("Artifact Broker returned invalid bytes"));
    }
    let chunk_count = bytes.len().div_ceil(maximum_chunk_bytes);
    let mut chunks = Vec::with_capacity(chunk_count);
    for (index, payload) in bytes.chunks(maximum_chunk_bytes).enumerate() {
        chunks.push(ArtifactReadChunk {
            schema_version: ARTIFACT_INTERNAL_RPC_SCHEMA_VERSION,
            operation: MODEL_REQUEST_CHUNK_OPERATION.to_owned(),
            request_digest: request_digest.to_owned(),
            sequence: u64::try_from(index)
                .map_err(|_| Status::resource_exhausted("too many Artifact chunks"))?,
            payload: payload.to_vec(),
            payload_digest: digest_bytes(payload).to_string(),
            content_digest: artifact.content_digest().to_string(),
            total_length: artifact.byte_length(),
            terminal: index + 1 == chunk_count,
        });
    }
    Ok(chunks)
}

fn digest_bytes(bytes: &[u8]) -> Sha256Digest {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    let mut encoded = String::with_capacity(71);
    encoded.push_str("sha256:");
    for byte in hasher.finalize() {
        use fmt::Write as _;
        let _ = write!(encoded, "{byte:02x}");
    }
    encoded.parse().expect("SHA-256 encoding is canonical")
}

fn map_server_error(error: ModelArtifactBrokerError) -> Status {
    match error {
        ModelArtifactBrokerError::Unavailable => Status::unavailable("Artifact Broker unavailable"),
        ModelArtifactBrokerError::Denied => Status::permission_denied("Artifact read denied"),
        ModelArtifactBrokerError::NotFound => Status::not_found("Artifact not found"),
        ModelArtifactBrokerError::TooLarge => {
            Status::resource_exhausted("Artifact exceeds the read limit")
        }
        ModelArtifactBrokerError::Integrity => {
            Status::data_loss("Artifact integrity verification failed")
        }
    }
}

fn map_client_status(status: Status) -> ModelArtifactBrokerError {
    match status.code() {
        tonic::Code::Unavailable | tonic::Code::DeadlineExceeded => {
            ModelArtifactBrokerError::Unavailable
        }
        tonic::Code::PermissionDenied | tonic::Code::Unauthenticated => {
            ModelArtifactBrokerError::Denied
        }
        tonic::Code::NotFound => ModelArtifactBrokerError::NotFound,
        tonic::Code::ResourceExhausted => ModelArtifactBrokerError::TooLarge,
        _ => ModelArtifactBrokerError::Integrity,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArtifactRpcError {
    InvalidConfiguration,
    InvalidEnvelope,
}

impl fmt::Display for ArtifactRpcError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidConfiguration => "Artifact RPC configuration is invalid",
            Self::InvalidEnvelope => "Artifact RPC envelope is invalid",
        })
    }
}

impl Error for ArtifactRpcError {}

impl From<ArtifactRpcError> for Status {
    fn from(_: ArtifactRpcError) -> Self {
        Status::invalid_argument("invalid Artifact RPC envelope")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Duration, Utc};
    use insight_platform_contracts::{
        checked_in_hard_limit_profile, ArtifactRef, DataClassification, ResourceId, ResourceKind,
    };
    use insight_platform_models::{ExactInvocationValueRef, InvocationValueStorage, JobFence};
    use rcgen::{
        BasicConstraints, CertificateParams, CertifiedIssuer, ExtendedKeyUsagePurpose, IsCa,
        KeyPair, KeyUsagePurpose, SanType,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::oneshot;
    use tonic::transport::{
        server::TcpIncoming, Certificate, ClientTlsConfig, Endpoint, Identity, Server,
        ServerTlsConfig,
    };

    fn id(kind: ResourceKind) -> ResourceId {
        ResourceId::from_uuid_v7(kind, uuid::Uuid::now_v7()).unwrap()
    }

    fn digest(character: char) -> Sha256Digest {
        format!("sha256:{}", character.to_string().repeat(64))
            .parse()
            .unwrap()
    }

    fn request(bytes: &[u8]) -> ModelArtifactReadRequest {
        let artifact = ArtifactRef::new(
            id(ResourceKind::Artifact),
            digest_bytes(bytes),
            u64::try_from(bytes.len()).unwrap(),
            "application/json",
            DataClassification::Internal,
            Some("model-request.json".to_owned()),
        )
        .unwrap();
        ModelArtifactReadRequest {
            schema_version: 1,
            tenant_id: id(ResourceKind::Tenant),
            model_turn_id: id(ResourceKind::ModelTurn),
            job_id: id(ResourceKind::Job),
            exact: ExactInvocationValueRef {
                schema_version: 1,
                value_id: id(ResourceKind::RunValue),
                run_id: id(ResourceKind::Run),
                producing_node_id: Some(id(ResourceKind::NodeExecution)),
                value_kind: "model_request".to_owned(),
                classification: DataClassification::Internal,
                schema_digest: digest('a'),
                content_digest: digest_bytes(bytes),
                storage: InvocationValueStorage::Artifact { artifact },
            },
            artifact_link_id: id(ResourceKind::ArtifactLink),
            fence: JobFence {
                expected_version: 2,
                worker_process_generation_id: id(ResourceKind::WorkerProcessGeneration),
                lease_generation: 1,
                token_digest: digest('b'),
            },
            request_digest: digest('c'),
            maximum_bytes: bytes.len(),
            deadline: Utc::now() + Duration::minutes(1),
        }
    }

    struct RecordingBroker {
        bytes: Vec<u8>,
        calls: AtomicUsize,
    }

    #[async_trait]
    impl ModelArtifactBroker for RecordingBroker {
        async fn read_exact(
            &self,
            _request: ModelArtifactReadRequest,
        ) -> Result<Vec<u8>, ModelArtifactBrokerError> {
            self.calls.fetch_add(1, Ordering::AcqRel);
            Ok(self.bytes.clone())
        }
    }

    struct MtlsFixture {
        ca_pem: String,
        server_certificate_pem: String,
        server_key_pem: String,
        model_certificate_pem: String,
        model_key_pem: String,
        wrong_certificate_pem: String,
        wrong_key_pem: String,
    }

    fn mtls_fixture() -> MtlsFixture {
        let mut ca_parameters = CertificateParams::default();
        ca_parameters.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        ca_parameters.key_usages = vec![
            KeyUsagePurpose::DigitalSignature,
            KeyUsagePurpose::KeyCertSign,
            KeyUsagePurpose::CrlSign,
        ];
        let ca = CertifiedIssuer::self_signed(ca_parameters, KeyPair::generate().unwrap()).unwrap();
        let issue = |subject_alt_names, extended_key_usage| {
            let mut parameters = CertificateParams::default();
            parameters.subject_alt_names = subject_alt_names;
            parameters.key_usages = vec![KeyUsagePurpose::DigitalSignature];
            parameters.extended_key_usages = vec![extended_key_usage];
            let key = KeyPair::generate().unwrap();
            let certificate = parameters.signed_by(&key, &ca).unwrap();
            (certificate.pem(), key.serialize_pem())
        };
        let (server_certificate_pem, server_key_pem) = issue(
            vec![SanType::DnsName("localhost".try_into().unwrap())],
            ExtendedKeyUsagePurpose::ServerAuth,
        );
        let (model_certificate_pem, model_key_pem) = issue(
            vec![SanType::URI(
                MODEL_WORKER_WORKLOAD_IDENTITY.try_into().unwrap(),
            )],
            ExtendedKeyUsagePurpose::ClientAuth,
        );
        let (wrong_certificate_pem, wrong_key_pem) = issue(
            vec![SanType::URI(
                "spiffe://insight.platform/workload/capability-worker"
                    .try_into()
                    .unwrap(),
            )],
            ExtendedKeyUsagePurpose::ClientAuth,
        );
        MtlsFixture {
            ca_pem: ca.pem(),
            server_certificate_pem,
            server_key_pem,
            model_certificate_pem,
            model_key_pem,
            wrong_certificate_pem,
            wrong_key_pem,
        }
    }

    async fn channel(
        endpoint: &str,
        fixture: &MtlsFixture,
        certificate: &str,
        key: &str,
    ) -> tonic::transport::Channel {
        Endpoint::from_shared(endpoint.to_owned())
            .unwrap()
            .tls_config(
                ClientTlsConfig::new()
                    .domain_name("localhost")
                    .ca_certificate(Certificate::from_pem(&fixture.ca_pem))
                    .identity(Identity::from_pem(certificate, key)),
            )
            .unwrap()
            .connect()
            .await
            .unwrap()
    }

    #[test]
    fn request_envelope_is_canonical_and_digest_bound() {
        let limits = ArtifactInternalRpcLimits::default();
        let read = request(br#"{"messages":[]}"#);
        let envelope = encode_request(MODEL_REQUEST_READ_OPERATION, &read, limits).unwrap();
        let decoded: ModelArtifactReadRequest =
            decode_request(envelope.clone(), MODEL_REQUEST_READ_OPERATION, limits).unwrap();
        assert_eq!(decoded, read);

        let mut whitespace = envelope.clone();
        whitespace.canonical_request_json.push(b' ');
        assert!(decode_request::<ModelArtifactReadRequest>(
            whitespace,
            MODEL_REQUEST_READ_OPERATION,
            limits
        )
        .is_err());

        let mut drift = envelope;
        drift.operation = "artifact.generic.read/v1".to_owned();
        assert!(decode_request::<ModelArtifactReadRequest>(
            drift,
            MODEL_REQUEST_READ_OPERATION,
            limits
        )
        .is_err());
    }

    #[tokio::test]
    async fn real_mtls_streams_exact_bytes_and_rejects_wrong_role() {
        let fixture = mtls_fixture();
        let bytes = br#"{"messages":[]}"#.to_vec();
        let broker = Arc::new(RecordingBroker {
            bytes: bytes.clone(),
            calls: AtomicUsize::new(0),
        });
        let rpc_limits = ArtifactInternalRpcLimits::new(65_536, 5).unwrap();
        let model_limits = ModelTurnLimits::from_profile(&checked_in_hard_limit_profile()).unwrap();
        let incoming = TcpIncoming::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let address = incoming.local_addr().unwrap();
        let (shutdown_sender, shutdown_receiver) = oneshot::channel::<()>();
        let service =
            proto::artifact_model_broker_service_server::ArtifactModelBrokerServiceServer::new(
                ArtifactModelBrokerGrpcService::new(Arc::clone(&broker), rpc_limits, model_limits),
            )
            .max_encoding_message_size(rpc_limits.maximum_message_bytes())
            .max_decoding_message_size(rpc_limits.maximum_message_bytes());
        let service = tonic::service::interceptor::InterceptedService::new(
            service,
            ModelWorkerWorkloadIdentity,
        );
        let tls = ServerTlsConfig::new()
            .identity(Identity::from_pem(
                &fixture.server_certificate_pem,
                &fixture.server_key_pem,
            ))
            .client_ca_root(Certificate::from_pem(&fixture.ca_pem));
        let server = tokio::spawn(async move {
            Server::builder()
                .tls_config(tls)
                .unwrap()
                .add_service(service)
                .serve_with_incoming_shutdown(incoming, async {
                    let _ = shutdown_receiver.await;
                })
                .await
                .unwrap();
        });
        let endpoint = format!("https://localhost:{}", address.port());

        let accepted_channel = channel(
            &endpoint,
            &fixture,
            &fixture.model_certificate_pem,
            &fixture.model_key_pem,
        )
        .await;
        let client = ArtifactModelBrokerGrpcClient::new(accepted_channel, rpc_limits, model_limits);
        assert_eq!(client.read_exact(request(&bytes)).await.unwrap(), bytes);
        assert_eq!(broker.calls.load(Ordering::Acquire), 1);

        let wrong_channel = channel(
            &endpoint,
            &fixture,
            &fixture.wrong_certificate_pem,
            &fixture.wrong_key_pem,
        )
        .await;
        let mut wrong_client = ArtifactModelBrokerServiceClient::new(wrong_channel);
        let rejected = wrong_client
            .read_model_request(
                encode_request(MODEL_REQUEST_READ_OPERATION, &request(&bytes), rpc_limits).unwrap(),
            )
            .await
            .unwrap_err();
        assert_eq!(rejected.code(), tonic::Code::PermissionDenied);
        assert_eq!(broker.calls.load(Ordering::Acquire), 1);

        drop(client);
        drop(wrong_client);
        shutdown_sender.send(()).unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), server)
            .await
            .unwrap()
            .unwrap();
    }
}
