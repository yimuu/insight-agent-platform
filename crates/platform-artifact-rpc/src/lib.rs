//! Versioned internal gRPC boundary for the independently deployed Artifact Broker.
//!
//! The Sandbox Controller submits exact, credential-free read authority
//! requests. The Broker returns bytes only after PostgreSQL authorization, exact-version object
//! verification and a second authorization. The wire is bounded, canonical and chunked; it never
//! carries an object locator, storage credential or generic operation name supplied by the caller.

use async_trait::async_trait;
use futures::Stream;
use insight_platform_contracts::{
    parse_strict_json, ArtifactRef, JsonLimits, ResourceKind, Sha256Digest,
};
use insight_platform_sandbox::{WasiArtifactBroker, WasiArtifactReadPurpose};
pub use insight_platform_sandbox::{WasiArtifactBrokerError, WasiArtifactReadRequest};
use serde::{de::DeserializeOwned, Serialize};
use sha2::{Digest as _, Sha256};
use std::{
    error::Error,
    fmt,
    future::Future,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering as AtomicOrdering},
        Arc, Mutex,
    },
    task::{Context, Poll},
    time::{Duration as StdDuration, SystemTime},
};
use tonic::{Request, Response, Status};
use x509_parser::{extensions::GeneralName, parse_x509_certificate};

pub mod proto {
    tonic::include_proto!("insight.platform.v1");
}

use proto::{
    artifact_sandbox_broker_service_client::ArtifactSandboxBrokerServiceClient,
    artifact_sandbox_broker_service_server::ArtifactSandboxBrokerService, ArtifactReadChunk,
    ClosedArtifactReadRequest,
};

pub const ARTIFACT_INTERNAL_RPC_SCHEMA_VERSION: u32 = 1;
pub const MODEL_WORKER_WORKLOAD_IDENTITY: &str = "spiffe://insight.platform/workload/model-worker";
pub const SANDBOX_CONTROLLER_WORKLOAD_IDENTITY: &str =
    "spiffe://insight.platform/workload/sandbox-controller";
pub const MAX_ARTIFACT_RPC_REQUEST_BYTES_HARD: usize = 1_048_576;
pub const MAX_ARTIFACT_RPC_CHUNK_BYTES_HARD: usize = 262_144;
const WASI_ARTIFACT_READ_OPERATION: &str = "artifact.sandbox.wasi.read/v1";
const WASI_ARTIFACT_CHUNK_OPERATION: &str = "artifact.sandbox.wasi.chunk/v1";
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

/// Authorizes only the Sandbox Controller at the Sandbox materialization boundary.
#[derive(Debug, Clone, Copy, Default)]
pub struct SandboxControllerWorkloadIdentity;

impl tonic::service::Interceptor for SandboxControllerWorkloadIdentity {
    fn call(&mut self, request: Request<()>) -> Result<Request<()>, Status> {
        let certificates = request
            .peer_certs()
            .ok_or_else(|| Status::unauthenticated("client certificate is required"))?;
        let leaf = certificates
            .first()
            .ok_or_else(|| Status::unauthenticated("client certificate is required"))?;
        require_exact_workload_uri(leaf.as_ref(), SANDBOX_CONTROLLER_WORKLOAD_IDENTITY)?;
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ArtifactClientReadError {
    Unavailable,
    Denied,
    NotFound,
    TooLarge,
    Integrity,
}

async fn collect_artifact_stream(
    response: &mut tonic::Streaming<ArtifactReadChunk>,
    request_digest: &str,
    chunk_operation: &str,
    artifact: &ArtifactRef,
    maximum_bytes: usize,
    limits: ArtifactInternalRpcLimits,
    deadline: SystemTime,
) -> Result<Vec<u8>, ArtifactClientReadError> {
    let expected_length =
        usize::try_from(artifact.byte_length()).map_err(|_| ArtifactClientReadError::TooLarge)?;
    if expected_length > maximum_bytes {
        return Err(ArtifactClientReadError::TooLarge);
    }
    let mut bytes = ZeroizingBytes::with_capacity(expected_length);
    let mut expected_sequence = 0_u64;
    let mut observed_terminal = false;
    while let Some(chunk) = await_rpc_before_deadline(deadline, response.message()).await? {
        if observed_terminal {
            return Err(ArtifactClientReadError::Integrity);
        }
        validate_chunk(
            &chunk,
            request_digest,
            chunk_operation,
            expected_sequence,
            artifact,
            limits,
        )?;
        let next_length = bytes
            .len()
            .checked_add(chunk.payload.len())
            .ok_or(ArtifactClientReadError::TooLarge)?;
        if next_length > expected_length || next_length > maximum_bytes {
            return Err(ArtifactClientReadError::TooLarge);
        }
        if (chunk.terminal && next_length != expected_length)
            || (!chunk.terminal && next_length >= expected_length)
        {
            return Err(ArtifactClientReadError::Integrity);
        }
        bytes.extend_from_slice(&chunk.payload);
        expected_sequence = expected_sequence
            .checked_add(1)
            .ok_or(ArtifactClientReadError::Integrity)?;
        observed_terminal = chunk.terminal;
    }
    if !observed_terminal
        || bytes.len() != expected_length
        || digest_bytes(bytes.as_slice()) != *artifact.content_digest()
    {
        return Err(ArtifactClientReadError::Integrity);
    }
    Ok(bytes.into_inner())
}

struct ZeroizingBytes {
    value: Vec<u8>,
}

impl ZeroizingBytes {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            value: Vec::with_capacity(capacity),
        }
    }

    fn len(&self) -> usize {
        self.value.len()
    }

    fn as_slice(&self) -> &[u8] {
        &self.value
    }

    fn extend_from_slice(&mut self, value: &[u8]) {
        self.value.extend_from_slice(value);
    }

    fn into_inner(mut self) -> Vec<u8> {
        std::mem::take(&mut self.value)
    }
}

impl Drop for ZeroizingBytes {
    fn drop(&mut self) {
        self.value.fill(0);
    }
}

fn validate_chunk(
    chunk: &ArtifactReadChunk,
    request_digest: &str,
    chunk_operation: &str,
    expected_sequence: u64,
    artifact: &ArtifactRef,
    limits: ArtifactInternalRpcLimits,
) -> Result<(), ArtifactClientReadError> {
    let expected_length = artifact.byte_length();
    let empty_terminal = chunk.terminal && expected_length == 0 && expected_sequence == 0;
    if chunk.schema_version != ARTIFACT_INTERNAL_RPC_SCHEMA_VERSION
        || chunk.operation != chunk_operation
        || chunk.request_digest != request_digest
        || chunk.sequence != expected_sequence
        || (chunk.payload.is_empty() && !empty_terminal)
        || chunk.payload.len() > limits.maximum_chunk_bytes()
        || (!chunk.terminal && chunk.payload.len() != limits.maximum_chunk_bytes())
        || chunk.total_length != expected_length
        || chunk.content_digest != artifact.content_digest().to_string()
        || chunk.payload_digest != digest_bytes(&chunk.payload).to_string()
    {
        return Err(ArtifactClientReadError::Integrity);
    }
    Ok(())
}

/// Credential-free client used only by the Sandbox Controller. The closed WASI method keeps
/// runtime selection out of untrusted requests.
#[derive(Clone)]
pub struct ArtifactSandboxBrokerGrpcClient {
    client: ArtifactSandboxBrokerServiceClient<tonic::transport::Channel>,
    rpc_limits: ArtifactInternalRpcLimits,
}

impl ArtifactSandboxBrokerGrpcClient {
    pub fn new(channel: tonic::transport::Channel, rpc_limits: ArtifactInternalRpcLimits) -> Self {
        let maximum = rpc_limits.maximum_message_bytes();
        Self {
            client: ArtifactSandboxBrokerServiceClient::new(channel)
                .max_encoding_message_size(maximum)
                .max_decoding_message_size(maximum),
            rpc_limits,
        }
    }
}

#[async_trait]
impl WasiArtifactBroker for ArtifactSandboxBrokerGrpcClient {
    async fn read_exact(
        &self,
        request: WasiArtifactReadRequest,
    ) -> Result<Vec<u8>, WasiArtifactBrokerError> {
        validate_wasi_read_request(&request)?;
        let artifact = request.artifact.clone();
        let maximum_bytes = request.maximum_bytes;
        let envelope = encode_request(WASI_ARTIFACT_READ_OPERATION, &request, self.rpc_limits)
            .map_err(|_| WasiArtifactBrokerError::Integrity)?;
        let request_digest = envelope.request_digest.clone();
        let deadline = SystemTime::from(request.deadline);
        let request =
            request_with_domain_deadline(envelope, deadline).map_err(map_wasi_client_read_error)?;
        let mut client = self.client.clone();
        let mut response = await_rpc_before_deadline(deadline, client.read_wasi_artifact(request))
            .await
            .map_err(map_wasi_client_read_error)?
            .into_inner();
        collect_artifact_stream(
            &mut response,
            &request_digest,
            WASI_ARTIFACT_CHUNK_OPERATION,
            &artifact,
            maximum_bytes,
            self.rpc_limits,
            deadline,
        )
        .await
        .map_err(map_wasi_client_read_error)
    }
}

fn validate_wasi_read_request(
    request: &WasiArtifactReadRequest,
) -> Result<(), WasiArtifactBrokerError> {
    let grant_shape_valid = match request.purpose {
        WasiArtifactReadPurpose::RuntimeBundle => request.read_grant.is_none(),
        WasiArtifactReadPurpose::InputValue => request.read_grant.is_some(),
    };
    if request.tenant_id.kind() != ResourceKind::Tenant
        || request.sandbox_job_id.kind() != ResourceKind::Job
        || request.worker_process_generation_id.kind() != ResourceKind::WorkerProcessGeneration
        || request.lease_generation == 0
        || request.maximum_bytes == 0
        || request.artifact.validate().is_err()
        || u64::try_from(request.maximum_bytes)
            .ok()
            .is_none_or(|maximum| maximum < request.artifact.byte_length())
        || !grant_shape_valid
    {
        return Err(WasiArtifactBrokerError::Denied);
    }
    Ok(())
}

fn remaining_until(deadline: SystemTime) -> Option<StdDuration> {
    deadline
        .duration_since(SystemTime::now())
        .ok()
        .filter(|remaining| !remaining.is_zero())
}

fn request_with_domain_deadline<T>(
    message: T,
    deadline: SystemTime,
) -> Result<Request<T>, ArtifactClientReadError> {
    let remaining = remaining_until(deadline).ok_or(ArtifactClientReadError::Unavailable)?;
    let mut request = Request::new(message);
    request.set_timeout(remaining);
    Ok(request)
}

async fn await_rpc_before_deadline<T, F>(
    deadline: SystemTime,
    future: F,
) -> Result<T, ArtifactClientReadError>
where
    F: Future<Output = Result<T, Status>>,
{
    let remaining = remaining_until(deadline).ok_or(ArtifactClientReadError::Unavailable)?;
    tokio::time::timeout(remaining, future)
        .await
        .map_err(|_| ArtifactClientReadError::Unavailable)?
        .map_err(classify_client_status)
}

fn server_deadline_budget(deadline: SystemTime) -> Result<StdDuration, Status> {
    remaining_until(deadline)
        .ok_or_else(|| Status::deadline_exceeded("Artifact read deadline elapsed"))
}

/// Exact bytes plus the opaque audience-capacity lease that authorized their materialization.
/// The server moves both into one response stream so a slow or abandoned response continues to
/// consume the same permit until the stream completes or is dropped.
pub struct LeasedArtifactBytes {
    bytes: Vec<u8>,
    lease: Option<Box<dyn Send + 'static>>,
}

impl LeasedArtifactBytes {
    pub fn new<L>(bytes: Vec<u8>, lease: L) -> Self
    where
        L: Send + 'static,
    {
        Self {
            bytes,
            lease: Some(Box::new(lease)),
        }
    }

    fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    fn into_parts(mut self) -> (Vec<u8>, Box<dyn Send + 'static>) {
        let bytes = std::mem::take(&mut self.bytes);
        let lease = self
            .lease
            .take()
            .expect("Artifact response lease is present until the stream takes ownership");
        (bytes, lease)
    }
}

impl Drop for LeasedArtifactBytes {
    fn drop(&mut self) {
        self.bytes.fill(0);
    }
}

#[async_trait]
pub trait WasiArtifactResponseBroker: Send + Sync {
    async fn read_wasi_for_response(
        &self,
        request: WasiArtifactReadRequest,
    ) -> Result<LeasedArtifactBytes, WasiArtifactBrokerError>;
}

/// Server adapter for the exact Sandbox materialization authority. Endpoint-role
/// authorization is installed by the process around the generated service and therefore runs
/// before either method decodes a request or invokes the domain broker.
pub struct ArtifactSandboxBrokerGrpcService<B> {
    broker: Arc<B>,
    rpc_limits: ArtifactInternalRpcLimits,
}

impl<B> ArtifactSandboxBrokerGrpcService<B> {
    pub fn new(broker: Arc<B>, rpc_limits: ArtifactInternalRpcLimits) -> Self {
        Self { broker, rpc_limits }
    }

    async fn read_wasi(
        &self,
        envelope: ClosedArtifactReadRequest,
    ) -> Result<Response<ArtifactReadStream>, Status>
    where
        B: WasiArtifactResponseBroker + 'static,
    {
        let request_digest = envelope.request_digest.clone();
        let read: WasiArtifactReadRequest =
            decode_request(envelope, WASI_ARTIFACT_READ_OPERATION, self.rpc_limits)
                .map_err(Status::from)?;
        validate_wasi_read_request(&read).map_err(map_wasi_server_error)?;
        let artifact = read.artifact.clone();
        let maximum_bytes = read.maximum_bytes;
        let deadline = SystemTime::from(read.deadline);
        let response_read = tokio::time::timeout(
            server_deadline_budget(deadline)?,
            self.broker.read_wasi_for_response(read),
        )
        .await
        .map_err(|_| Status::deadline_exceeded("Artifact read deadline elapsed"))?
        .map_err(map_wasi_server_error)?;
        require_broker_bytes(response_read.as_bytes(), &artifact, maximum_bytes)?;
        artifact_read_stream(
            response_read,
            &request_digest,
            WASI_ARTIFACT_CHUNK_OPERATION,
            &artifact,
            self.rpc_limits.maximum_chunk_bytes(),
            deadline,
        )
    }
}

type ArtifactReadStream =
    Pin<Box<dyn Stream<Item = Result<ArtifactReadChunk, Status>> + Send + 'static>>;

#[tonic::async_trait]
impl<B> ArtifactSandboxBrokerService for ArtifactSandboxBrokerGrpcService<B>
where
    B: WasiArtifactResponseBroker + 'static,
{
    type ReadWasiArtifactStream = ArtifactReadStream;

    async fn read_wasi_artifact(
        &self,
        request: Request<ClosedArtifactReadRequest>,
    ) -> Result<Response<Self::ReadWasiArtifactStream>, Status> {
        self.read_wasi(request.into_inner()).await
    }
}

fn require_broker_bytes(
    bytes: &[u8],
    artifact: &ArtifactRef,
    maximum_bytes: usize,
) -> Result<(), Status> {
    if bytes.len() > maximum_bytes
        || u64::try_from(bytes.len()).ok() != Some(artifact.byte_length())
        || digest_bytes(bytes) != *artifact.content_digest()
    {
        return Err(Status::data_loss("Artifact Broker returned invalid bytes"));
    }
    Ok(())
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

fn artifact_read_stream(
    read: LeasedArtifactBytes,
    request_digest: &str,
    chunk_operation: &'static str,
    artifact: &ArtifactRef,
    maximum_chunk_bytes: usize,
    deadline: SystemTime,
) -> Result<Response<ArtifactReadStream>, Status> {
    if maximum_chunk_bytes == 0 {
        return Err(Status::data_loss("Artifact Broker returned invalid bytes"));
    }
    let maximum_bytes = usize::try_from(artifact.byte_length())
        .map_err(|_| Status::resource_exhausted("Artifact exceeds the read limit"))?;
    require_broker_bytes(read.as_bytes(), artifact, maximum_bytes)?;
    let deadline_budget = server_deadline_budget(deadline)?;
    let (bytes, lease) = read.into_parts();
    let response = Arc::new(Mutex::new(LeasedArtifactResponse {
        bytes,
        lease: Some(lease),
    }));
    let deadline_expired = Arc::new(AtomicBool::new(false));
    let deadline_response = Arc::clone(&response);
    let deadline_flag = Arc::clone(&deadline_expired);
    let deadline_task = tokio::spawn(async move {
        tokio::time::sleep(deadline_budget).await;
        let mut response = deadline_response
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        response.release();
        deadline_flag.store(true, AtomicOrdering::Release);
    });
    Ok(Response::new(Box::pin(ArtifactChunkStream {
        response,
        offset: 0,
        sequence: 0,
        terminal_emitted: false,
        request_digest: request_digest.to_owned(),
        chunk_operation,
        content_digest: artifact.content_digest().to_string(),
        total_length: artifact.byte_length(),
        maximum_chunk_bytes,
        deadline_expired,
        deadline_task: Some(deadline_task),
    })))
}

struct LeasedArtifactResponse {
    bytes: Vec<u8>,
    lease: Option<Box<dyn Send + 'static>>,
}

impl LeasedArtifactResponse {
    fn release(&mut self) {
        let mut bytes = std::mem::take(&mut self.bytes);
        bytes.fill(0);
        drop(bytes);
        self.lease.take();
    }
}

struct ArtifactChunkStream {
    response: Arc<Mutex<LeasedArtifactResponse>>,
    offset: usize,
    sequence: u64,
    terminal_emitted: bool,
    request_digest: String,
    chunk_operation: &'static str,
    content_digest: String,
    total_length: u64,
    maximum_chunk_bytes: usize,
    deadline_expired: Arc<AtomicBool>,
    deadline_task: Option<tokio::task::JoinHandle<()>>,
}

impl ArtifactChunkStream {
    fn release_response(&mut self) {
        if let Some(deadline_task) = self.deadline_task.take() {
            deadline_task.abort();
        }
        self.response
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .release();
    }
}

impl Stream for ArtifactChunkStream {
    type Item = Result<ArtifactReadChunk, Status>;

    fn poll_next(mut self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.terminal_emitted {
            self.release_response();
            return Poll::Ready(None);
        }

        let response = Arc::clone(&self.response);
        let mut response = response
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if self.deadline_expired.load(AtomicOrdering::Acquire) {
            drop(response);
            self.terminal_emitted = true;
            self.release_response();
            return Poll::Ready(Some(Err(Status::deadline_exceeded(
                "Artifact read deadline elapsed",
            ))));
        }

        let start = self.offset;
        let end = start
            .saturating_add(self.maximum_chunk_bytes)
            .min(response.bytes.len());
        let payload = response.bytes[start..end].to_vec();
        response.bytes[start..end].fill(0);
        let terminal = end == response.bytes.len();
        drop(response);
        let chunk = ArtifactReadChunk {
            schema_version: ARTIFACT_INTERNAL_RPC_SCHEMA_VERSION,
            operation: self.chunk_operation.to_owned(),
            request_digest: self.request_digest.clone(),
            sequence: self.sequence,
            payload_digest: digest_bytes(&payload).to_string(),
            payload,
            content_digest: self.content_digest.clone(),
            total_length: self.total_length,
            terminal,
        };
        self.offset = end;
        self.sequence = match self.sequence.checked_add(1) {
            Some(sequence) => sequence,
            None => {
                self.terminal_emitted = true;
                return Poll::Ready(Some(Err(Status::resource_exhausted(
                    "too many Artifact chunks",
                ))));
            }
        };
        self.terminal_emitted = terminal;
        Poll::Ready(Some(Ok(chunk)))
    }
}

impl Drop for ArtifactChunkStream {
    fn drop(&mut self) {
        self.release_response();
    }
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

fn map_wasi_server_error(error: WasiArtifactBrokerError) -> Status {
    match error {
        WasiArtifactBrokerError::Unavailable => Status::unavailable("Artifact Broker unavailable"),
        WasiArtifactBrokerError::Denied => Status::permission_denied("Artifact read denied"),
        WasiArtifactBrokerError::NotFound => Status::not_found("Artifact not found"),
        WasiArtifactBrokerError::TooLarge => {
            Status::resource_exhausted("Artifact exceeds the read limit")
        }
        WasiArtifactBrokerError::Integrity => {
            Status::data_loss("Artifact integrity verification failed")
        }
    }
}

fn classify_client_status(status: Status) -> ArtifactClientReadError {
    match status.code() {
        tonic::Code::Unavailable | tonic::Code::DeadlineExceeded => {
            ArtifactClientReadError::Unavailable
        }
        tonic::Code::PermissionDenied | tonic::Code::Unauthenticated => {
            ArtifactClientReadError::Denied
        }
        tonic::Code::NotFound => ArtifactClientReadError::NotFound,
        tonic::Code::ResourceExhausted => ArtifactClientReadError::TooLarge,
        _ => ArtifactClientReadError::Integrity,
    }
}

fn map_wasi_client_read_error(error: ArtifactClientReadError) -> WasiArtifactBrokerError {
    match error {
        ArtifactClientReadError::Unavailable => WasiArtifactBrokerError::Unavailable,
        ArtifactClientReadError::Denied => WasiArtifactBrokerError::Denied,
        ArtifactClientReadError::NotFound => WasiArtifactBrokerError::NotFound,
        ArtifactClientReadError::TooLarge => WasiArtifactBrokerError::TooLarge,
        ArtifactClientReadError::Integrity => WasiArtifactBrokerError::Integrity,
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
    use futures::StreamExt as _;
    use insight_platform_contracts::{ArtifactRef, DataClassification, ResourceId, ResourceKind};
    use rcgen::{
        BasicConstraints, CertificateParams, CertifiedIssuer, ExtendedKeyUsagePurpose, IsCa,
        KeyPair, KeyUsagePurpose, SanType,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::{oneshot, Semaphore};
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

    fn artifact(bytes: &[u8], display_name: &str) -> ArtifactRef {
        ArtifactRef::new(
            id(ResourceKind::Artifact),
            digest_bytes(bytes),
            u64::try_from(bytes.len()).unwrap(),
            "application/octet-stream",
            DataClassification::Internal,
            Some(display_name.to_owned()),
        )
        .unwrap()
    }

    fn wasi_request(bytes: &[u8]) -> WasiArtifactReadRequest {
        WasiArtifactReadRequest {
            tenant_id: id(ResourceKind::Tenant),
            sandbox_job_id: id(ResourceKind::Job),
            request_digest: digest('d'),
            worker_process_generation_id: id(ResourceKind::WorkerProcessGeneration),
            lease_generation: 2,
            artifact: artifact(bytes, "runtime.wasm"),
            purpose: WasiArtifactReadPurpose::RuntimeBundle,
            read_grant: None,
            maximum_bytes: bytes.len(),
            deadline: Utc::now() + Duration::minutes(1),
        }
    }

    struct RecordingSandboxBroker {
        bytes: Vec<u8>,
        wasi_calls: AtomicUsize,
    }

    #[async_trait]
    impl WasiArtifactResponseBroker for RecordingSandboxBroker {
        async fn read_wasi_for_response(
            &self,
            _request: WasiArtifactReadRequest,
        ) -> Result<LeasedArtifactBytes, WasiArtifactBrokerError> {
            self.wasi_calls.fetch_add(1, Ordering::AcqRel);
            Ok(LeasedArtifactBytes::new(self.bytes.clone(), ()))
        }
    }

    struct AudienceCapacityBroker {
        bytes: Vec<u8>,
        sandbox: Arc<Semaphore>,
    }

    #[async_trait]
    impl WasiArtifactResponseBroker for AudienceCapacityBroker {
        async fn read_wasi_for_response(
            &self,
            _request: WasiArtifactReadRequest,
        ) -> Result<LeasedArtifactBytes, WasiArtifactBrokerError> {
            let permit = Arc::clone(&self.sandbox)
                .try_acquire_owned()
                .map_err(|_| WasiArtifactBrokerError::Unavailable)?;
            Ok(LeasedArtifactBytes::new(self.bytes.clone(), permit))
        }
    }

    struct MtlsFixture {
        ca_pem: String,
        server_certificate_pem: String,
        server_key_pem: String,
        model_certificate_pem: String,
        model_key_pem: String,
        sandbox_controller_certificate_pem: String,
        sandbox_controller_key_pem: String,
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
        let (sandbox_controller_certificate_pem, sandbox_controller_key_pem) = issue(
            vec![SanType::URI(
                SANDBOX_CONTROLLER_WORKLOAD_IDENTITY.try_into().unwrap(),
            )],
            ExtendedKeyUsagePurpose::ClientAuth,
        );
        MtlsFixture {
            ca_pem: ca.pem(),
            server_certificate_pem,
            server_key_pem,
            model_certificate_pem,
            model_key_pem,
            sandbox_controller_certificate_pem,
            sandbox_controller_key_pem,
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
        let wasi = wasi_request(b"wasi-runtime");
        let envelope = encode_request(WASI_ARTIFACT_READ_OPERATION, &wasi, limits).unwrap();
        let decoded: WasiArtifactReadRequest =
            decode_request(envelope.clone(), WASI_ARTIFACT_READ_OPERATION, limits).unwrap();
        assert_eq!(decoded, wasi);

        let mut whitespace = envelope.clone();
        whitespace.canonical_request_json.push(b' ');
        whitespace.request_digest = digest_bytes(&whitespace.canonical_request_json).to_string();
        assert!(decode_request::<WasiArtifactReadRequest>(
            whitespace,
            WASI_ARTIFACT_READ_OPERATION,
            limits
        )
        .is_err());

        let mut digest_tamper = envelope.clone();
        digest_tamper.request_digest = digest('9').to_string();
        assert!(decode_request::<WasiArtifactReadRequest>(
            digest_tamper,
            WASI_ARTIFACT_READ_OPERATION,
            limits,
        )
        .is_err());

        let mut drift = envelope;
        drift.operation = "artifact.generic.read/v1".to_owned();
        assert!(decode_request::<WasiArtifactReadRequest>(
            drift,
            WASI_ARTIFACT_READ_OPERATION,
            limits
        )
        .is_err());
    }

    #[test]
    fn grpc_timeout_is_positive_and_does_not_exceed_the_domain_deadline() {
        fn decode_grpc_timeout(value: &str) -> StdDuration {
            let (number, unit) = value.split_at(value.len() - 1);
            let number = number.parse::<u64>().unwrap();
            match unit {
                "H" => StdDuration::from_secs(number * 60 * 60),
                "M" => StdDuration::from_secs(number * 60),
                "S" => StdDuration::from_secs(number),
                "m" => StdDuration::from_millis(number),
                "u" => StdDuration::from_micros(number),
                "n" => StdDuration::from_nanos(number),
                _ => panic!("unexpected gRPC timeout unit"),
            }
        }

        let domain_budget = StdDuration::from_secs(5);
        let request =
            request_with_domain_deadline((), SystemTime::now().checked_add(domain_budget).unwrap())
                .unwrap();
        let encoded = request
            .metadata()
            .get("grpc-timeout")
            .unwrap()
            .to_str()
            .unwrap();
        let timeout = decode_grpc_timeout(encoded);
        assert!(!timeout.is_zero());
        assert!(timeout <= domain_budget);
        assert!(matches!(
            request_with_domain_deadline((), SystemTime::now()),
            Err(ArtifactClientReadError::Unavailable)
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn local_absolute_deadline_cancels_a_stalled_rpc_body_future() {
        let deadline = SystemTime::now()
            .checked_add(StdDuration::from_secs(30))
            .unwrap();
        let stalled = tokio::spawn(async move {
            await_rpc_before_deadline(deadline, futures::future::pending::<Result<(), Status>>())
                .await
        });
        tokio::task::yield_now().await;
        tokio::time::advance(StdDuration::from_secs(31)).await;
        assert_eq!(
            stalled.await.unwrap(),
            Err(ArtifactClientReadError::Unavailable)
        );
    }

    #[tokio::test]
    async fn slow_streams_hold_sandbox_audience_capacity() {
        let bytes = br#"{"messages":[]}"#.to_vec();
        let broker = Arc::new(AudienceCapacityBroker {
            bytes: bytes.clone(),
            sandbox: Arc::new(Semaphore::new(1)),
        });
        let rpc_limits = ArtifactInternalRpcLimits::new(65_536, 5).unwrap();
        let sandbox_service =
            ArtifactSandboxBrokerGrpcService::new(Arc::clone(&broker), rpc_limits);

        let wasi_envelope = || {
            Request::new(
                encode_request(
                    WASI_ARTIFACT_READ_OPERATION,
                    &wasi_request(&bytes),
                    rpc_limits,
                )
                .unwrap(),
            )
        };
        let mut sandbox_stream = sandbox_service
            .read_wasi_artifact(wasi_envelope())
            .await
            .unwrap()
            .into_inner();
        let rejected_concurrent = sandbox_service
            .read_wasi_artifact(wasi_envelope())
            .await
            .err()
            .unwrap();
        assert_eq!(rejected_concurrent.code(), tonic::Code::Unavailable);

        while let Some(chunk) = sandbox_stream.next().await {
            chunk.unwrap();
        }
        assert_eq!(broker.sandbox.available_permits(), 1);
        let released_sandbox = sandbox_service
            .read_wasi_artifact(wasi_envelope())
            .await
            .unwrap();
        drop(released_sandbox);
    }

    #[tokio::test(start_paused = true)]
    async fn stalled_stream_releases_lease_at_domain_deadline_without_poll_or_drop() {
        let bytes = br#"{"messages":[]}"#.to_vec();
        let sandbox_capacity = Arc::new(Semaphore::new(1));
        let broker = Arc::new(AudienceCapacityBroker {
            bytes: bytes.clone(),
            sandbox: Arc::clone(&sandbox_capacity),
        });
        let rpc_limits = ArtifactInternalRpcLimits::new(65_536, 5).unwrap();
        let service = ArtifactSandboxBrokerGrpcService::new(Arc::clone(&broker), rpc_limits);

        let mut stalled_request = wasi_request(&bytes);
        stalled_request.deadline = Utc::now() + Duration::seconds(30);
        let mut stalled_stream = service
            .read_wasi_artifact(Request::new(
                encode_request(WASI_ARTIFACT_READ_OPERATION, &stalled_request, rpc_limits).unwrap(),
            ))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(sandbox_capacity.available_permits(), 0);

        // Let the independently spawned deadline task register its timer, then advance time while
        // retaining the stream without polling it. HTTP/2 backpressure has the same ownership
        // shape: the stream and its buffered body remain live, but the capacity lease must not.
        tokio::task::yield_now().await;
        tokio::time::advance(StdDuration::from_secs(31)).await;
        tokio::task::yield_now().await;
        assert_eq!(sandbox_capacity.available_permits(), 1);

        let released_capacity = service
            .read_wasi_artifact(Request::new(
                encode_request(
                    WASI_ARTIFACT_READ_OPERATION,
                    &wasi_request(&bytes),
                    rpc_limits,
                )
                .unwrap(),
            ))
            .await
            .unwrap();
        drop(released_capacity);

        let expired = stalled_stream.next().await.unwrap().unwrap_err();
        assert_eq!(expired.code(), tonic::Code::DeadlineExceeded);
    }

    #[tokio::test]
    async fn sandbox_mtls_streams_wasi_and_rejects_wrong_role_before_authority() {
        let fixture = mtls_fixture();
        let bytes = b"sandbox-runtime-material".to_vec();
        let broker = Arc::new(RecordingSandboxBroker {
            bytes: bytes.clone(),
            wasi_calls: AtomicUsize::new(0),
        });
        let rpc_limits = ArtifactInternalRpcLimits::new(65_536, 5).unwrap();
        let incoming = TcpIncoming::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let address = incoming.local_addr().unwrap();
        let (shutdown_sender, shutdown_receiver) = oneshot::channel::<()>();
        let service =
            proto::artifact_sandbox_broker_service_server::ArtifactSandboxBrokerServiceServer::new(
                ArtifactSandboxBrokerGrpcService::new(Arc::clone(&broker), rpc_limits),
            )
            .max_encoding_message_size(rpc_limits.maximum_message_bytes())
            .max_decoding_message_size(rpc_limits.maximum_message_bytes());
        let service = tonic::service::interceptor::InterceptedService::new(
            service,
            SandboxControllerWorkloadIdentity,
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
            &fixture.sandbox_controller_certificate_pem,
            &fixture.sandbox_controller_key_pem,
        )
        .await;
        let client = ArtifactSandboxBrokerGrpcClient::new(accepted_channel, rpc_limits);
        assert_eq!(
            WasiArtifactBroker::read_exact(&client, wasi_request(&bytes))
                .await
                .unwrap(),
            bytes
        );
        assert_eq!(broker.wasi_calls.load(Ordering::Acquire), 1);

        let wrong_channel = channel(
            &endpoint,
            &fixture,
            &fixture.model_certificate_pem,
            &fixture.model_key_pem,
        )
        .await;
        let mut wrong_client = ArtifactSandboxBrokerServiceClient::new(wrong_channel);
        let rejected_wasi = wrong_client
            .read_wasi_artifact(
                encode_request(
                    WASI_ARTIFACT_READ_OPERATION,
                    &wasi_request(&bytes),
                    rpc_limits,
                )
                .unwrap(),
            )
            .await
            .unwrap_err();
        assert_eq!(rejected_wasi.code(), tonic::Code::PermissionDenied);
        assert_eq!(broker.wasi_calls.load(Ordering::Acquire), 1);

        drop(client);
        drop(wrong_client);
        shutdown_sender.send(()).unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), server)
            .await
            .unwrap()
            .unwrap();
    }
}
