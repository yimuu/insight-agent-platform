//! Independent Model Worker claim and execution composition.
//!
//! This crate owns process-local Model capacity and orchestration only. PostgreSQL remains the
//! durable claim/commit authority, Provider traffic remains behind the Egress connector, and
//! Model request and response bytes remain bounded Inline values.

use async_trait::async_trait;
use chrono::{Duration as ChronoDuration, Utc};
use insight_platform_contracts::{
    canonical_digest, parse_strict_json, HardLimitProfile, JobState, ModelTurnState, ResourceId,
    ResourceIdError, ResourceKind, Sha256Digest, WorkClass,
};
use insight_platform_jobs::JobFence;
use insight_platform_model_adapters::{
    ModelAdapterCancelOutcome, ModelAdapterCancelRequest, ModelAdapterFailure,
    ModelAdapterFailureClass, ModelAdapterWorker, ModelExecutionAuthority, ModelLiveDeltaSink,
    ModelOutputMaterializer, ModelProviderWireConnector, ModelProviderWireProtocol,
    ANTHROPIC_MESSAGES_ADAPTER_NAME, OPENAI_RESPONSES_ADAPTER_NAME,
};
use insight_platform_models::{
    CanonicalModelRequest, ClaimModelJobs, CommitModelCancellationOutcome, ModelAttemptMeasurement,
    ModelClaimSlot, ModelExecutionInput, ModelExecutionInputMaterial, ModelLiveTextDelta,
    ModelTurnLimits, ModelTurnStore, ModelTurnTransaction, ModelWorkerAudit, NormalizedModelDelta,
    NormalizedModelFrame, MODEL_QUOTA_LINES,
};
use insight_platform_postgres::{
    model_turn_repository::{ClaimedModelExecution, ControlledModelExecution},
    repository::{PgRepository, RepositoryError},
};
use insight_platform_worker::{
    ClaimBatchHardLimit, ClaimedJobIdentity, LocalWorkerPoolError, LocalWorkerPools,
};
use sha2::{Digest as _, Sha256};
use std::{
    collections::BTreeSet,
    error::Error,
    fmt,
    fmt::Write as _,
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{
    sync::{mpsc, OwnedSemaphorePermit, Semaphore},
    task::{JoinError, JoinSet},
    time::{Instant, MissedTickBehavior},
};
use tokio_util::sync::CancellationToken;
use url::Url;
use uuid::Uuid;

pub const MODEL_WORKER_ROLE: &str = "model-worker";
pub const MODEL_LIVE_NATS_SUBJECT_PREFIX: &str = "insight.platform.v1.run.live";
const MAX_LIVE_DELTA_PUBLISH_BATCH_MESSAGES: usize = 64;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelLiveDeltaNatsSettings {
    pub servers: Vec<String>,
    pub namespace: String,
    pub root_certificate: PathBuf,
    pub client_certificate: PathBuf,
    pub client_private_key: PathBuf,
    pub connect_timeout: Duration,
    pub publish_timeout: Duration,
    pub reconnect_backoff: Duration,
    pub drain_timeout: Duration,
    pub maximum_pending_messages: usize,
    pub maximum_pending_bytes: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelLiveDeltaNatsConfig {
    settings: ModelLiveDeltaNatsSettings,
    pub maximum_payload_bytes: usize,
}

impl ModelLiveDeltaNatsConfig {
    pub fn from_profile(
        profile: &HardLimitProfile,
        settings: ModelLiveDeltaNatsSettings,
    ) -> Result<Self, ModelLiveDeltaError> {
        profile
            .validate()
            .map_err(|_| ModelLiveDeltaError::InvalidConfig)?;
        let maximum_payload_bytes =
            usize::try_from(profile.control_data.nats_payload_bytes.q1_default)
                .map_err(|_| ModelLiveDeltaError::InvalidConfig)?;
        let maximum_buffer_events =
            usize::try_from(profile.control_data.telemetry_buffer_events.q1_default)
                .map_err(|_| ModelLiveDeltaError::InvalidConfig)?;
        if settings.maximum_pending_messages > maximum_buffer_events {
            return Err(ModelLiveDeltaError::InvalidConfig);
        }
        let config = Self {
            settings,
            maximum_payload_bytes,
        };
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> Result<(), ModelLiveDeltaError> {
        let settings = &self.settings;
        if settings.servers.is_empty()
            || settings.servers.len() > 16
            || settings.servers.iter().collect::<BTreeSet<_>>().len() != settings.servers.len()
            || settings
                .servers
                .iter()
                .any(|server| !valid_nats_server(server))
            || !valid_nats_namespace(&settings.namespace)
            || !settings.root_certificate.is_absolute()
            || !settings.client_certificate.is_absolute()
            || !settings.client_private_key.is_absolute()
            || settings.connect_timeout.is_zero()
            || settings.connect_timeout > Duration::from_secs(30)
            || settings.publish_timeout.is_zero()
            || settings.publish_timeout > Duration::from_secs(5)
            || settings.reconnect_backoff.is_zero()
            || settings.reconnect_backoff > Duration::from_secs(30)
            || settings.drain_timeout.is_zero()
            || settings.drain_timeout > Duration::from_secs(30)
            || settings.maximum_pending_messages == 0
            || settings.maximum_pending_bytes < self.maximum_payload_bytes
            || settings.maximum_pending_bytes > 1_073_741_824
            || self.maximum_payload_bytes < 256
            || self.maximum_payload_bytes > 1_048_576
            || settings.maximum_pending_bytes > Semaphore::MAX_PERMITS
            || settings.maximum_pending_bytes > u32::MAX as usize
        {
            return Err(ModelLiveDeltaError::InvalidConfig);
        }
        Ok(())
    }
}

fn valid_nats_server(value: &str) -> bool {
    let Ok(url) = Url::parse(value) else {
        return false;
    };
    url.scheme() == "tls"
        && url.username().is_empty()
        && url.password().is_none()
        && url.host_str().is_some()
        && url.port().is_some()
        && matches!(url.path(), "" | "/")
        && url.query().is_none()
        && url.fragment().is_none()
}

fn valid_nats_namespace(value: &str) -> bool {
    let bytes = value.as_bytes();
    (1..=64).contains(&bytes.len())
        && bytes
            .first()
            .is_some_and(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit())
        && bytes
            .last()
            .is_some_and(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit())
        && bytes.iter().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'-' | b'_')
        })
}

pub fn model_live_delta_subject(
    namespace: &str,
    tenant_id: &ResourceId,
    run_id: &ResourceId,
) -> Result<String, ModelLiveDeltaError> {
    if !valid_nats_namespace(namespace)
        || tenant_id.kind() != ResourceKind::Tenant
        || run_id.kind() != ResourceKind::Run
    {
        return Err(ModelLiveDeltaError::InvalidEnvelope);
    }
    let mut hasher = Sha256::new();
    hasher.update(b"insight.platform/v1/run-live-subject\0");
    hasher.update(tenant_id.to_string().as_bytes());
    hasher.update(b"\0");
    hasher.update(run_id.to_string().as_bytes());
    let mut key = String::with_capacity(64);
    for byte in hasher.finalize() {
        let _ = write!(key, "{byte:02x}");
    }
    Ok(format!(
        "{MODEL_LIVE_NATS_SUBJECT_PREFIX}.{namespace}.{key}"
    ))
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ModelLiveDeltaReport {
    pub enqueued: u64,
    pub published: u64,
    pub dropped_non_public: u64,
    pub dropped_invalid: u64,
    pub dropped_oversized: u64,
    pub dropped_backpressure: u64,
    pub transport_failures: u64,
}

#[derive(Default)]
struct ModelLiveDeltaCounters {
    enqueued: AtomicU64,
    published: AtomicU64,
    dropped_non_public: AtomicU64,
    dropped_invalid: AtomicU64,
    dropped_oversized: AtomicU64,
    dropped_backpressure: AtomicU64,
    transport_failures: AtomicU64,
}

impl ModelLiveDeltaCounters {
    fn snapshot(&self) -> ModelLiveDeltaReport {
        ModelLiveDeltaReport {
            enqueued: self.enqueued.load(Ordering::Relaxed),
            published: self.published.load(Ordering::Relaxed),
            dropped_non_public: self.dropped_non_public.load(Ordering::Relaxed),
            dropped_invalid: self.dropped_invalid.load(Ordering::Relaxed),
            dropped_oversized: self.dropped_oversized.load(Ordering::Relaxed),
            dropped_backpressure: self.dropped_backpressure.load(Ordering::Relaxed),
            transport_failures: self.transport_failures.load(Ordering::Relaxed),
        }
    }
}

struct QueuedModelLiveDelta {
    subject: String,
    payload: Vec<u8>,
    message_permit: OwnedSemaphorePermit,
    byte_permit: OwnedSemaphorePermit,
}

#[derive(Clone)]
pub struct BufferedNatsModelLiveDeltaSink {
    sender: mpsc::Sender<QueuedModelLiveDelta>,
    message_capacity: Arc<Semaphore>,
    byte_capacity: Arc<Semaphore>,
    namespace: String,
    limits: ModelTurnLimits,
    maximum_payload_bytes: usize,
    counters: Arc<ModelLiveDeltaCounters>,
}

pub struct NatsModelLiveDeltaDriver {
    receiver: mpsc::Receiver<QueuedModelLiveDelta>,
    config: ModelLiveDeltaNatsConfig,
    counters: Arc<ModelLiveDeltaCounters>,
}

impl BufferedNatsModelLiveDeltaSink {
    pub fn new(
        config: ModelLiveDeltaNatsConfig,
        limits: ModelTurnLimits,
    ) -> Result<(Self, NatsModelLiveDeltaDriver), ModelLiveDeltaError> {
        config.validate()?;
        let (sender, receiver) = mpsc::channel(config.settings.maximum_pending_messages);
        let counters = Arc::new(ModelLiveDeltaCounters::default());
        let sink = Self {
            sender,
            message_capacity: Arc::new(Semaphore::new(config.settings.maximum_pending_messages)),
            byte_capacity: Arc::new(Semaphore::new(config.settings.maximum_pending_bytes)),
            namespace: config.settings.namespace.clone(),
            limits,
            maximum_payload_bytes: config.maximum_payload_bytes,
            counters: Arc::clone(&counters),
        };
        let driver = NatsModelLiveDeltaDriver {
            receiver,
            config,
            counters,
        };
        Ok((sink, driver))
    }

    pub fn report(&self) -> ModelLiveDeltaReport {
        self.counters.snapshot()
    }

    fn enqueue(&self, event: ModelLiveTextDelta) {
        if event.validate(self.limits).is_err() {
            self.counters
                .dropped_invalid
                .fetch_add(1, Ordering::Relaxed);
            return;
        }
        let payload = match serde_jcs::to_vec(&event) {
            Ok(payload) if payload.len() <= self.maximum_payload_bytes => payload,
            Ok(_) => {
                self.counters
                    .dropped_oversized
                    .fetch_add(1, Ordering::Relaxed);
                return;
            }
            Err(_) => {
                self.counters
                    .dropped_invalid
                    .fetch_add(1, Ordering::Relaxed);
                return;
            }
        };
        let subject =
            match model_live_delta_subject(&self.namespace, &event.tenant_id, &event.run_id) {
                Ok(subject) => subject,
                Err(_) => {
                    self.counters
                        .dropped_invalid
                        .fetch_add(1, Ordering::Relaxed);
                    return;
                }
            };
        let Ok(byte_count) = u32::try_from(payload.len()) else {
            self.counters
                .dropped_oversized
                .fetch_add(1, Ordering::Relaxed);
            return;
        };
        let Ok(message_permit) = Arc::clone(&self.message_capacity).try_acquire_owned() else {
            self.counters
                .dropped_backpressure
                .fetch_add(1, Ordering::Relaxed);
            return;
        };
        let Ok(byte_permit) = Arc::clone(&self.byte_capacity).try_acquire_many_owned(byte_count)
        else {
            self.counters
                .dropped_backpressure
                .fetch_add(1, Ordering::Relaxed);
            return;
        };
        if self
            .sender
            .try_send(QueuedModelLiveDelta {
                subject,
                payload,
                message_permit,
                byte_permit,
            })
            .is_err()
        {
            self.counters
                .dropped_backpressure
                .fetch_add(1, Ordering::Relaxed);
            return;
        }
        self.counters.enqueued.fetch_add(1, Ordering::Relaxed);
    }
}

#[async_trait]
impl ModelLiveDeltaSink for BufferedNatsModelLiveDeltaSink {
    async fn publish(
        &self,
        execution: &insight_platform_model_adapters::ModelAdapterExecutionRequest,
        frame: &NormalizedModelFrame,
    ) {
        let NormalizedModelDelta::Text(text) = &frame.delta else {
            self.counters
                .dropped_non_public
                .fetch_add(1, Ordering::Relaxed);
            return;
        };
        if frame.model_turn_id != execution.model_turn_id
            || frame.attempt_no != execution.attempt_no
            || frame.lease_generation != execution.lease_generation
        {
            self.counters
                .dropped_invalid
                .fetch_add(1, Ordering::Relaxed);
            return;
        }
        self.enqueue(ModelLiveTextDelta {
            schema_version: 1,
            tenant_id: execution.tenant_id.clone(),
            run_id: execution.run_id.clone(),
            model_turn_id: execution.model_turn_id.clone(),
            job_id: execution.job_id.clone(),
            worker_process_generation_id: execution.worker_process_generation_id.clone(),
            attempt_no: execution.attempt_no,
            lease_generation: execution.lease_generation,
            transport_sequence: frame.transport_sequence,
            request_digest: execution.request_digest.clone(),
            classification: execution.request.classification,
            text: text.clone(),
        });
    }
}

impl NatsModelLiveDeltaDriver {
    pub async fn run(
        mut self,
        cancellation: CancellationToken,
    ) -> Result<ModelLiveDeltaReport, ModelLiveDeltaError> {
        loop {
            let connection_config = self.config.clone();
            let connect = Self::connect(&connection_config);
            tokio::pin!(connect);
            let client = tokio::select! {
                biased;
                _ = cancellation.cancelled() => return Ok(self.counters.snapshot()),
                result = &mut connect => match result {
                    Ok(client) => client,
                    Err(()) => {
                        self.counters.transport_failures.fetch_add(1, Ordering::Relaxed);
                        tokio::select! {
                            _ = cancellation.cancelled() => return Ok(self.counters.snapshot()),
                            _ = tokio::time::sleep(self.config.settings.reconnect_backoff) => continue,
                        }
                    }
                }
            };
            loop {
                tokio::select! {
                    biased;
                    _ = cancellation.cancelled() => {
                        let _ = tokio::time::timeout(self.config.settings.drain_timeout, client.drain()).await;
                        return Ok(self.counters.snapshot());
                    }
                    item = self.receiver.recv() => {
                        let Some(item) = item else {
                            return Err(ModelLiveDeltaError::QueueClosed);
                        };
                        let mut batch = Vec::with_capacity(
                            MAX_LIVE_DELTA_PUBLISH_BATCH_MESSAGES.min(
                                self.config.settings.maximum_pending_messages,
                            ),
                        );
                        batch.push(item);
                        while batch.len() < batch.capacity() {
                            match self.receiver.try_recv() {
                                Ok(item) => batch.push(item),
                                Err(mpsc::error::TryRecvError::Empty | mpsc::error::TryRecvError::Disconnected) => break,
                            }
                        }
                        let published = batch.len();
                        if Self::publish_batch(
                            &client,
                            batch,
                            self.config.settings.publish_timeout,
                        )
                        .await
                        .is_ok()
                        {
                            self.counters
                                .published
                                .fetch_add(published as u64, Ordering::Relaxed);
                        } else {
                            self.counters.transport_failures.fetch_add(1, Ordering::Relaxed);
                            break;
                        }
                    }
                }
            }
        }
    }

    async fn publish_batch(
        client: &async_nats::Client,
        batch: Vec<QueuedModelLiveDelta>,
        publish_timeout: Duration,
    ) -> Result<(), ()> {
        let publish = async {
            let mut permits = Vec::with_capacity(batch.len());
            for item in batch {
                client
                    .publish(item.subject, item.payload.into())
                    .await
                    .map_err(|_| ())?;
                permits.push((item.message_permit, item.byte_permit));
            }
            client.flush().await.map_err(|_| ())?;
            drop(permits);
            Ok(())
        };
        tokio::time::timeout(publish_timeout, publish)
            .await
            .map_err(|_| ())?
    }

    async fn connect(config: &ModelLiveDeltaNatsConfig) -> Result<async_nats::Client, ()> {
        let servers = config
            .settings
            .servers
            .iter()
            .map(|server| server.parse::<async_nats::ServerAddr>())
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| ())?;
        let options = async_nats::ConnectOptions::new()
            .name("insight-platform-model-worker-live-v1")
            .connection_timeout(config.settings.connect_timeout)
            .max_reconnects(None)
            .client_capacity(config.settings.maximum_pending_messages)
            .require_tls(true)
            .add_root_certificates(config.settings.root_certificate.clone())
            .add_client_certificate(
                config.settings.client_certificate.clone(),
                config.settings.client_private_key.clone(),
            );
        let client =
            tokio::time::timeout(config.settings.connect_timeout, options.connect(servers))
                .await
                .map_err(|_| ())?
                .map_err(|_| ())?;
        if client.max_payload() < config.maximum_payload_bytes {
            return Err(());
        }
        Ok(client)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModelLiveDeltaError {
    InvalidConfig,
    InvalidEnvelope,
    QueueClosed,
}

impl fmt::Display for ModelLiveDeltaError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidConfig => "Model live delta NATS configuration is invalid",
            Self::InvalidEnvelope => "Model live delta envelope is invalid",
            Self::QueueClosed => "Model live delta publisher queue closed unexpectedly",
        })
    }
}

impl Error for ModelLiveDeltaError {}

pub trait ModelWorkerIdentityFactory: Send + Sync {
    fn new_resource_id(&self, kind: ResourceKind) -> Result<ResourceId, ModelWorkerIdentityError>;
    fn new_opaque_digest(&self) -> Result<Sha256Digest, ModelWorkerIdentityError>;
}

#[derive(Debug, Clone, Copy, Default)]
pub struct UuidModelWorkerIdentityFactory;

impl ModelWorkerIdentityFactory for UuidModelWorkerIdentityFactory {
    fn new_resource_id(&self, kind: ResourceKind) -> Result<ResourceId, ModelWorkerIdentityError> {
        ResourceId::from_uuid_v7(kind, Uuid::now_v7()).map_err(ModelWorkerIdentityError::ResourceId)
    }

    fn new_opaque_digest(&self) -> Result<Sha256Digest, ModelWorkerIdentityError> {
        let mut hasher = Sha256::new();
        hasher.update(b"insight.platform/v1/model-worker/opaque-identity\0");
        hasher.update(Uuid::new_v4().as_bytes());
        format!("sha256:{}", lower_hex(&hasher.finalize()))
            .parse()
            .map_err(|_| ModelWorkerIdentityError::Digest)
    }
}

fn lower_hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(char::from(DIGITS[usize::from(byte >> 4)]));
        encoded.push(char::from(DIGITS[usize::from(byte & 0x0f)]));
    }
    encoded
}

#[derive(Debug)]
pub enum ModelWorkerIdentityError {
    ResourceId(ResourceIdError),
    Digest,
}

impl fmt::Display for ModelWorkerIdentityError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ResourceId(failure) => failure.fmt(formatter),
            Self::Digest => formatter.write_str("generated Model Worker digest is invalid"),
        }
    }
}

impl Error for ModelWorkerIdentityError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModelClaimFailure {
    Unavailable,
    FirstWinnerLost,
    Invariant,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelClaimBinding {
    pub tenant_id: ResourceId,
    pub job_id: ResourceId,
    pub worker_process_generation_id: ResourceId,
    pub worker_manifest_digest: Sha256Digest,
    pub expected_job_version: u64,
    pub lease_generation: u64,
    pub lease_token_digest: Sha256Digest,
    pub request_digest: Sha256Digest,
    pub quota_reservation_entry_ids: Vec<ResourceId>,
}

pub trait ClaimedModelJob: Send + 'static {
    fn claim_binding(&self) -> Result<ModelClaimBinding, ModelClaimFailure>;
}

impl ClaimedModelJob for ClaimedModelExecution {
    fn claim_binding(&self) -> Result<ModelClaimBinding, ModelClaimFailure> {
        self.request_input
            .validate()
            .map_err(|_| ModelClaimFailure::Invariant)?;
        let projection = self
            .job_projection()
            .map_err(|_| ModelClaimFailure::Invariant)?;
        if projection.state != insight_platform_contracts::JobState::Running
            || projection.version != self.fence.expected_version
            || projection.lease_generation != self.fence.lease_generation
            || projection.lease.as_ref().is_none_or(|lease| {
                lease.worker_process_generation_id != self.fence.worker_process_generation_id
                    || lease.token_digest != self.fence.token_digest
            })
        {
            return Err(ModelClaimFailure::Invariant);
        }
        Ok(ModelClaimBinding {
            tenant_id: self.turn.tenant_id.clone(),
            job_id: projection.job_id,
            worker_process_generation_id: self.fence.worker_process_generation_id.clone(),
            worker_manifest_digest: self
                .turn
                .payload
                .admission
                .provider
                .installed_adapter
                .worker_manifest_digest
                .clone(),
            expected_job_version: self.fence.expected_version,
            lease_generation: self.fence.lease_generation,
            lease_token_digest: self.fence.token_digest.clone(),
            request_digest: self.turn.payload.admission.request_digest.clone(),
            quota_reservation_entry_ids: self.quota_entry_ids.clone(),
        })
    }
}

#[async_trait]
pub trait ModelClaimAuthority: Send + Sync + 'static {
    type Claim: ClaimedModelJob;

    async fn claim_model_jobs(
        &self,
        command: ClaimModelJobs,
    ) -> Result<Vec<Self::Claim>, ModelClaimFailure>;
}

#[async_trait]
impl ModelClaimAuthority for PgRepository {
    type Claim = ClaimedModelExecution;

    async fn claim_model_jobs(
        &self,
        command: ClaimModelJobs,
    ) -> Result<Vec<Self::Claim>, ModelClaimFailure> {
        PgRepository::claim_model_jobs(self, command)
            .await
            .map_err(|failure| match failure {
                insight_platform_postgres::repository::RepositoryError::Database(_) => {
                    ModelClaimFailure::Unavailable
                }
                insight_platform_postgres::repository::RepositoryError::Conflict(_)
                | insight_platform_postgres::repository::RepositoryError::StaleFence
                | insight_platform_postgres::repository::RepositoryError::LeaseExpired => {
                    ModelClaimFailure::FirstWinnerLost
                }
                _ => ModelClaimFailure::Invariant,
            })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModelHeartbeatFailure {
    Unavailable,
    FenceLost,
    Invariant,
}

#[async_trait]
pub trait ModelHeartbeatAuthority: Send + Sync + 'static {
    async fn heartbeat_model_execution(
        &self,
        tenant_id: &ResourceId,
        job_id: &ResourceId,
        fence: &JobFence,
        lease_milliseconds: u64,
    ) -> Result<JobFence, ModelHeartbeatFailure>;
}

#[async_trait]
impl ModelHeartbeatAuthority for PgRepository {
    async fn heartbeat_model_execution(
        &self,
        tenant_id: &ResourceId,
        job_id: &ResourceId,
        fence: &JobFence,
        lease_milliseconds: u64,
    ) -> Result<JobFence, ModelHeartbeatFailure> {
        let record = self
            .heartbeat_job(insight_platform_postgres::repository::HeartbeatJob {
                fence: insight_platform_postgres::repository::JobFence {
                    tenant_id: tenant_id.to_string(),
                    job_id: job_id.to_string(),
                    worker_id: fence.worker_process_generation_id.clone(),
                    lease_epoch: i64::try_from(fence.lease_generation)
                        .map_err(|_| ModelHeartbeatFailure::Invariant)?,
                    expected_job_version: i64::try_from(fence.expected_version)
                        .map_err(|_| ModelHeartbeatFailure::Invariant)?,
                    lease_token_digest: fence.token_digest.clone(),
                },
                lease_milliseconds: i64::try_from(lease_milliseconds)
                    .map_err(|_| ModelHeartbeatFailure::Invariant)?,
            })
            .await
            .map_err(|failure| match failure {
                insight_platform_postgres::repository::RepositoryError::Database(_) => {
                    ModelHeartbeatFailure::Unavailable
                }
                insight_platform_postgres::repository::RepositoryError::Conflict(_)
                | insight_platform_postgres::repository::RepositoryError::StaleFence
                | insight_platform_postgres::repository::RepositoryError::LeaseExpired
                | insight_platform_postgres::repository::RepositoryError::NotFound(_) => {
                    ModelHeartbeatFailure::FenceLost
                }
                _ => ModelHeartbeatFailure::Invariant,
            })?;
        let worker_id = fence.worker_process_generation_id.to_string();
        if record.tenant_id != tenant_id.to_string()
            || record.job_id != job_id.to_string()
            || record.worker_id.as_deref() != Some(worker_id.as_str())
            || record.lease_token_digest.as_deref() != Some(fence.token_digest.as_str())
            || u64::try_from(record.lease_epoch).ok() != Some(fence.lease_generation)
            || record.state != "running"
            || u64::try_from(record.version)
                .ok()
                .is_none_or(|version| version <= fence.expected_version)
            || record.heartbeat_at.is_none()
            || record.lease_expires_at.is_none()
        {
            return Err(ModelHeartbeatFailure::Invariant);
        }
        Ok(JobFence {
            expected_version: u64::try_from(record.version)
                .map_err(|_| ModelHeartbeatFailure::Invariant)?,
            worker_process_generation_id: fence.worker_process_generation_id.clone(),
            lease_generation: fence.lease_generation,
            token_digest: fence.token_digest.clone(),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModelCancellationSourceFailure {
    Unavailable,
    Invariant,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModelCancellationCommitFailure {
    Unavailable,
    FirstWinnerLost,
    Invariant,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ModelCancellationCandidate {
    pub tenant_id: ResourceId,
    pub model_turn_id: ResourceId,
    pub job_id: ResourceId,
    pub worker_process_generation_id: ResourceId,
    pub worker_manifest_digest: Sha256Digest,
    pub expected_turn_version: u64,
    pub fence: JobFence,
    pub usage_reservation_id: ResourceId,
    pub protocol: ModelProviderWireProtocol,
    pub provider_deployment: insight_platform_contracts::ExactDeploymentRef,
    pub attempt_no: u32,
    pub deadline: chrono::DateTime<Utc>,
    pub request_digest: Sha256Digest,
    pub measurement: ModelAttemptMeasurement,
}

impl ModelCancellationCandidate {
    fn from_controlled(
        controlled: ControlledModelExecution,
        limits: ModelTurnLimits,
    ) -> Result<Self, ModelCancellationSourceFailure> {
        controlled
            .turn
            .validate(limits)
            .map_err(|_| ModelCancellationSourceFailure::Invariant)?;
        let projection = controlled
            .job_projection()
            .map_err(|_| ModelCancellationSourceFailure::Invariant)?
            .ok_or(ModelCancellationSourceFailure::Invariant)?;
        let job = controlled
            .job
            .as_ref()
            .ok_or(ModelCancellationSourceFailure::Invariant)?;
        let lease = projection
            .lease
            .as_ref()
            .ok_or(ModelCancellationSourceFailure::Invariant)?;
        let usage_reservation_id: ResourceId = job
            .quota_reservation_id
            .as_deref()
            .ok_or(ModelCancellationSourceFailure::Invariant)?
            .parse()
            .map_err(|_| ModelCancellationSourceFailure::Invariant)?;
        let installed = &controlled.turn.payload.admission.provider.installed_adapter;
        let protocol = match installed.qualified_name.as_str() {
            OPENAI_RESPONSES_ADAPTER_NAME => ModelProviderWireProtocol::OpenAiResponses,
            ANTHROPIC_MESSAGES_ADAPTER_NAME => ModelProviderWireProtocol::AnthropicMessages,
            _ => return Err(ModelCancellationSourceFailure::Invariant),
        };
        if controlled.turn.state != ModelTurnState::Cancelling
            || projection.state != JobState::Cancelling
            || controlled.turn.payload.current_job_id.as_ref() != Some(&projection.job_id)
            || controlled.turn.tenant_id != projection.tenant_id
            || controlled.turn.model_turn_id != projection.owner.owner_id
            || projection.work_class != WorkClass::Model
            || usage_reservation_id.kind() != ResourceKind::UsageReservation
        {
            return Err(ModelCancellationSourceFailure::Invariant);
        }
        let request_digest = controlled.turn.payload.admission.request_digest.clone();
        let measurement = ModelAttemptMeasurement::conservative_dispatched(
            &controlled.turn.payload.admission,
            limits,
        )
        .map_err(|_| ModelCancellationSourceFailure::Invariant)?;
        let candidate = Self {
            tenant_id: projection.tenant_id,
            model_turn_id: controlled.turn.model_turn_id,
            job_id: projection.job_id,
            worker_process_generation_id: lease.worker_process_generation_id.clone(),
            worker_manifest_digest: installed.worker_manifest_digest.clone(),
            expected_turn_version: controlled.turn.version,
            fence: JobFence {
                expected_version: projection.version,
                worker_process_generation_id: lease.worker_process_generation_id.clone(),
                lease_generation: lease.lease_generation,
                token_digest: lease.token_digest.clone(),
            },
            usage_reservation_id,
            protocol,
            provider_deployment: controlled
                .turn
                .payload
                .admission
                .provider_deployment
                .clone(),
            attempt_no: projection.attempt_count,
            deadline: controlled.turn.deadline,
            request_digest,
            measurement,
        };
        Ok(candidate)
    }

    pub fn cancel_request(&self) -> ModelAdapterCancelRequest {
        ModelAdapterCancelRequest {
            tenant_id: self.tenant_id.clone(),
            model_turn_id: self.model_turn_id.clone(),
            job_id: self.job_id.clone(),
            worker_process_generation_id: self.worker_process_generation_id.clone(),
            provider_deployment: self.provider_deployment.clone(),
            attempt_no: self.attempt_no,
            lease_generation: self.fence.lease_generation,
            deadline: self.deadline,
        }
    }
}

#[async_trait]
pub trait ModelCancellationSource: Send + Sync + 'static {
    async fn scan_model_cancellations(
        &self,
        worker_process_generation_id: &ResourceId,
        limit: u16,
        limits: ModelTurnLimits,
    ) -> Result<Vec<ModelCancellationCandidate>, ModelCancellationSourceFailure>;
}

#[async_trait]
impl ModelCancellationSource for PgRepository {
    async fn scan_model_cancellations(
        &self,
        worker_process_generation_id: &ResourceId,
        limit: u16,
        limits: ModelTurnLimits,
    ) -> Result<Vec<ModelCancellationCandidate>, ModelCancellationSourceFailure> {
        let controlled = self
            .scan_cancelling_model_executions(worker_process_generation_id, limit)
            .await
            .map_err(map_cancellation_source_repository_error)?;
        let now = Utc::now();
        let mut candidates = Vec::with_capacity(controlled.len());
        for controlled in controlled {
            if controlled.turn.deadline <= now {
                continue;
            }
            let candidate = ModelCancellationCandidate::from_controlled(controlled, limits)?;
            candidate
                .cancel_request()
                .validate_shape_at(now)
                .map_err(|_| ModelCancellationSourceFailure::Invariant)?;
            candidates.push(candidate);
        }
        Ok(candidates)
    }
}

fn map_cancellation_source_repository_error(
    failure: RepositoryError,
) -> ModelCancellationSourceFailure {
    match failure {
        RepositoryError::Database(_) => ModelCancellationSourceFailure::Unavailable,
        _ => ModelCancellationSourceFailure::Invariant,
    }
}

#[async_trait]
pub trait ModelCancellationAuthority: Send + Sync + 'static {
    async fn commit_model_cancellation(
        &self,
        command: CommitModelCancellationOutcome,
    ) -> Result<(), ModelCancellationCommitFailure>;
}

#[async_trait]
impl ModelCancellationAuthority for PgRepository {
    async fn commit_model_cancellation(
        &self,
        command: CommitModelCancellationOutcome,
    ) -> Result<(), ModelCancellationCommitFailure> {
        let mut transaction = self
            .begin_model_turn()
            .await
            .map_err(map_cancellation_commit_repository_error)?;
        transaction
            .commit_model_cancellation_outcome(command)
            .await
            .map_err(map_cancellation_commit_repository_error)?;
        transaction
            .commit()
            .await
            .map_err(map_cancellation_commit_repository_error)
    }
}

fn map_cancellation_commit_repository_error(
    failure: RepositoryError,
) -> ModelCancellationCommitFailure {
    match failure {
        RepositoryError::Database(_) => ModelCancellationCommitFailure::Unavailable,
        RepositoryError::Conflict(_)
        | RepositoryError::StaleFence
        | RepositoryError::LeaseExpired
        | RepositoryError::NotFound(_) => ModelCancellationCommitFailure::FirstWinnerLost,
        _ => ModelCancellationCommitFailure::Invariant,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ModelCancellationDriverConfig {
    scan_limit: u16,
    receipt_ttl: Duration,
    scan_interval: Duration,
    failure_backoff: Duration,
    limits: ModelTurnLimits,
}

impl ModelCancellationDriverConfig {
    pub fn from_profile(
        profile: &HardLimitProfile,
        receipt_ttl: Duration,
        scan_interval: Duration,
        failure_backoff: Duration,
    ) -> Result<Self, ModelCancellationDriverError> {
        profile
            .validate()
            .map_err(|_| ModelCancellationDriverError::InvalidConfig)?;
        let scan_limit = u16::try_from(profile.control_data.recovery_batch.q1_default)
            .map_err(|_| ModelCancellationDriverError::InvalidConfig)?;
        if receipt_ttl <= Duration::from_millis(profile.run_scheduler.lease_milliseconds.q1_default)
        {
            return Err(ModelCancellationDriverError::InvalidConfig);
        }
        let config = Self {
            scan_limit,
            receipt_ttl,
            scan_interval,
            failure_backoff,
            limits: ModelTurnLimits::from_profile(profile)
                .map_err(|_| ModelCancellationDriverError::InvalidConfig)?,
        };
        config.validate()?;
        Ok(config)
    }

    fn validate(self) -> Result<(), ModelCancellationDriverError> {
        if self.scan_limit == 0
            || self.receipt_ttl.is_zero()
            || ChronoDuration::from_std(self.receipt_ttl).is_err()
            || self.scan_interval.is_zero()
            || self.failure_backoff.is_zero()
            || self.failure_backoff > self.scan_interval
        {
            return Err(ModelCancellationDriverError::InvalidConfig);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ModelCancellationCycleReport {
    pub discovered: u64,
    pub cancelled: u64,
    pub first_winner_lost: u64,
    pub deferred: u64,
}

pub struct ModelCancellationDriver<S, A, P, I> {
    source: Arc<S>,
    authority: Arc<A>,
    port: Arc<P>,
    identities: Arc<I>,
    pools: LocalWorkerPools,
    config: ModelCancellationDriverConfig,
}

impl<S, A, P, I> ModelCancellationDriver<S, A, P, I>
where
    S: ModelCancellationSource,
    A: ModelCancellationAuthority,
    P: ModelProviderWireConnector + 'static,
    I: ModelWorkerIdentityFactory + 'static,
{
    pub fn new(
        source: Arc<S>,
        authority: Arc<A>,
        port: Arc<P>,
        identities: Arc<I>,
        pools: LocalWorkerPools,
        config: ModelCancellationDriverConfig,
    ) -> Result<Self, ModelCancellationDriverError> {
        config.validate()?;
        let pool = pools.snapshot();
        if pool.worker_role != MODEL_WORKER_ROLE || pool.work_class != WorkClass::Model {
            return Err(ModelCancellationDriverError::WrongWorkerManifest);
        }
        Ok(Self {
            source,
            authority,
            port,
            identities,
            pools,
            config,
        })
    }

    pub async fn drive_once(
        &self,
    ) -> Result<ModelCancellationCycleReport, ModelCancellationDriverError> {
        let Some(_permit) = self
            .pools
            .try_acquire_critical_control()
            .map_err(ModelCancellationDriverError::LocalPool)?
        else {
            return Ok(ModelCancellationCycleReport::default());
        };
        let pool = self.pools.snapshot();
        let candidates = self
            .source
            .scan_model_cancellations(
                &pool.worker_process_generation_id,
                self.config.scan_limit,
                self.config.limits,
            )
            .await
            .map_err(ModelCancellationDriverError::Source)?;
        let mut report = ModelCancellationCycleReport {
            discovered: candidates.len() as u64,
            ..Default::default()
        };
        let mut jobs = BTreeSet::new();
        for candidate in candidates {
            if candidate.worker_process_generation_id != pool.worker_process_generation_id
                || candidate.worker_manifest_digest != pool.worker_manifest_digest
                || candidate.fence.worker_process_generation_id != pool.worker_process_generation_id
                || !jobs.insert(candidate.job_id.clone())
            {
                return Err(ModelCancellationDriverError::CorruptCandidate);
            }
            let now = Utc::now();
            if candidate.deadline <= now {
                report.first_winner_lost = report.first_winner_lost.saturating_add(1);
                continue;
            }
            match self
                .port
                .cancel(candidate.protocol, candidate.cancel_request())
                .await
            {
                Ok(
                    ModelAdapterCancelOutcome::Accepted
                    | ModelAdapterCancelOutcome::AlreadyTerminal,
                ) => {}
                Ok(ModelAdapterCancelOutcome::Unsupported) => {
                    return Err(ModelCancellationDriverError::InvalidEgressOutcome);
                }
                Err(failure) => {
                    failure
                        .validate_wire_shape(candidate.deadline, Utc::now())
                        .map_err(|_| ModelCancellationDriverError::InvalidEgressOutcome)?;
                    if matches!(
                        failure.class,
                        ModelAdapterFailureClass::RetryableBeforeDispatch
                            | ModelAdapterFailureClass::RetryableAfterDispatch
                    ) {
                        report.deferred = report.deferred.saturating_add(1);
                        continue;
                    }
                    return Err(ModelCancellationDriverError::InvalidEgressOutcome);
                }
            }
            let command = self.commit_command(candidate)?;
            match self.authority.commit_model_cancellation(command).await {
                Ok(()) => report.cancelled = report.cancelled.saturating_add(1),
                Err(ModelCancellationCommitFailure::FirstWinnerLost) => {
                    report.first_winner_lost = report.first_winner_lost.saturating_add(1)
                }
                Err(failure) => return Err(ModelCancellationDriverError::Commit(failure)),
            }
        }
        Ok(report)
    }

    fn commit_command(
        &self,
        candidate: ModelCancellationCandidate,
    ) -> Result<CommitModelCancellationOutcome, ModelCancellationDriverError> {
        let now = Utc::now();
        let receipt_expires_at = now
            .checked_add_signed(
                ChronoDuration::from_std(self.config.receipt_ttl)
                    .map_err(|_| ModelCancellationDriverError::InvalidConfig)?,
            )
            .ok_or(ModelCancellationDriverError::InvalidGeneratedCommand)?;
        let audit = ModelWorkerAudit {
            tenant_id: candidate.tenant_id.clone(),
            worker_process_generation_id: candidate.worker_process_generation_id.clone(),
            receipt_id: self.new_id(ResourceKind::Receipt)?,
            event_id: self.new_id(ResourceKind::Event)?,
            outbox_id: self.new_id(ResourceKind::OutboxEvent)?,
            idempotency_key_digest: self.new_digest()?,
            request_digest: candidate.request_digest,
            receipt_expires_at,
        };
        let command = CommitModelCancellationOutcome {
            audit,
            model_turn_id: candidate.model_turn_id,
            job_id: candidate.job_id,
            expected_turn_version: candidate.expected_turn_version,
            fence: Some(candidate.fence),
            usage_reservation_id: candidate.usage_reservation_id,
            quota_entry_ids: self.quota_entry_ids()?,
            measurement: candidate.measurement,
        };
        command
            .validate_at(now)
            .map_err(|_| ModelCancellationDriverError::InvalidGeneratedCommand)?;
        Ok(command)
    }

    fn quota_entry_ids(&self) -> Result<Vec<ResourceId>, ModelCancellationDriverError> {
        let mut ids = (0..MODEL_QUOTA_LINES)
            .map(|_| self.new_id(ResourceKind::QuotaLedgerEntry))
            .collect::<Result<Vec<_>, _>>()?;
        ids.sort();
        if ids.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(ModelCancellationDriverError::InvalidGeneratedCommand);
        }
        Ok(ids)
    }

    fn new_id(&self, kind: ResourceKind) -> Result<ResourceId, ModelCancellationDriverError> {
        self.identities
            .new_resource_id(kind)
            .map_err(ModelCancellationDriverError::Identity)
    }

    fn new_digest(&self) -> Result<Sha256Digest, ModelCancellationDriverError> {
        self.identities
            .new_opaque_digest()
            .map_err(ModelCancellationDriverError::Identity)
    }

    pub async fn run(
        self,
        cancellation: CancellationToken,
    ) -> Result<ModelCancellationCycleReport, ModelCancellationDriverError> {
        let mut report = ModelCancellationCycleReport::default();
        let mut scan = tokio::time::interval_at(Instant::now(), self.config.scan_interval);
        scan.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                biased;
                _ = cancellation.cancelled() => return Ok(report),
                _ = scan.tick() => {}
            }
            match self.drive_once().await {
                Ok(cycle) => {
                    report.discovered = report.discovered.saturating_add(cycle.discovered);
                    report.cancelled = report.cancelled.saturating_add(cycle.cancelled);
                    report.first_winner_lost = report
                        .first_winner_lost
                        .saturating_add(cycle.first_winner_lost);
                    report.deferred = report.deferred.saturating_add(cycle.deferred);
                }
                Err(ModelCancellationDriverError::Source(
                    ModelCancellationSourceFailure::Unavailable,
                ))
                | Err(ModelCancellationDriverError::Commit(
                    ModelCancellationCommitFailure::Unavailable,
                )) => {
                    tokio::select! {
                        _ = cancellation.cancelled() => return Ok(report),
                        _ = tokio::time::sleep(self.config.failure_backoff) => {}
                    }
                }
                Err(failure) => return Err(failure),
            }
        }
    }
}

#[derive(Debug)]
pub enum ModelCancellationDriverError {
    InvalidConfig,
    WrongWorkerManifest,
    CorruptCandidate,
    InvalidGeneratedCommand,
    InvalidEgressOutcome,
    Identity(ModelWorkerIdentityError),
    LocalPool(LocalWorkerPoolError),
    Source(ModelCancellationSourceFailure),
    Commit(ModelCancellationCommitFailure),
}

impl fmt::Display for ModelCancellationDriverError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidConfig => "Model cancellation driver configuration is invalid",
            Self::WrongWorkerManifest => "Model cancellation driver manifest is invalid",
            Self::CorruptCandidate => "Model cancellation scan returned a corrupt candidate",
            Self::InvalidGeneratedCommand => {
                "Model cancellation driver generated an invalid command"
            }
            Self::InvalidEgressOutcome => "Model cancellation Egress outcome is invalid",
            Self::Identity(_) => "Model cancellation identity generation failed",
            Self::LocalPool(_) => "Model cancellation local control pool failed",
            Self::Source(_) => "Model cancellation scan failed",
            Self::Commit(_) => "Model cancellation commit failed",
        })
    }
}

impl Error for ModelCancellationDriverError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ModelWorkerDriverTiming {
    pub safety_scan_interval: Duration,
    pub claim_failure_backoff: Duration,
    pub drain_grace: Duration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ModelWorkerDriverConfig {
    requested_claim_size: usize,
    claim_batch_hard_limit: ClaimBatchHardLimit,
    lease_milliseconds: u64,
    heartbeat_interval: Duration,
    receipt_ttl: Duration,
    timing: ModelWorkerDriverTiming,
    limits: ModelTurnLimits,
}

impl ModelWorkerDriverConfig {
    pub fn from_profile(
        profile: &HardLimitProfile,
        receipt_ttl: Duration,
        timing: ModelWorkerDriverTiming,
    ) -> Result<Self, ModelWorkerDriverError> {
        profile
            .validate()
            .map_err(|failure| ModelWorkerDriverError::InvalidConfig(failure.to_string()))?;
        let config = Self {
            requested_claim_size: usize::try_from(profile.run_scheduler.claim_batch.q1_default)
                .map_err(|_| {
                    ModelWorkerDriverError::InvalidConfig(
                        "Model claim batch exceeds local address space".to_owned(),
                    )
                })?,
            claim_batch_hard_limit: ClaimBatchHardLimit::from_profile(profile)
                .map_err(ModelWorkerDriverError::LocalPool)?,
            lease_milliseconds: profile.run_scheduler.lease_milliseconds.q1_default,
            heartbeat_interval: Duration::from_millis(
                profile.run_scheduler.heartbeat_milliseconds.q1_default,
            ),
            receipt_ttl,
            timing,
            limits: ModelTurnLimits::from_profile(profile)
                .map_err(|failure| ModelWorkerDriverError::InvalidConfig(failure.to_string()))?,
        };
        config.validate()?;
        Ok(config)
    }

    fn validate(self) -> Result<(), ModelWorkerDriverError> {
        if self.requested_claim_size == 0
            || self.lease_milliseconds == 0
            || self.heartbeat_interval.is_zero()
            || self.heartbeat_interval >= Duration::from_millis(self.lease_milliseconds / 3)
            || self.receipt_ttl.is_zero()
            || self.receipt_ttl <= Duration::from_millis(self.lease_milliseconds)
            || ChronoDuration::from_std(self.receipt_ttl).is_err()
            || self.timing.safety_scan_interval.is_zero()
            || self.timing.claim_failure_backoff.is_zero()
            || self.timing.claim_failure_backoff > self.timing.safety_scan_interval
            || self.timing.drain_grace.is_zero()
        {
            return Err(ModelWorkerDriverError::InvalidConfig(
                "Model Worker timing is invalid".to_owned(),
            ));
        }
        Ok(())
    }
}

pub struct ExecuteClaimedModelJob<C> {
    pub claim: C,
    pub audit: ModelWorkerAudit,
    pub quota_settlement_entry_ids: Vec<ResourceId>,
    pub worker_manifest_digest: Sha256Digest,
    pub limits: ModelTurnLimits,
    pub heartbeat_interval: Duration,
    pub lease_milliseconds: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModelExecutionDisposition {
    Settled,
    Abandoned,
}

#[async_trait]
pub trait ModelJobCommandExecutor<C>: Send + Sync + 'static {
    async fn execute(&self, command: ExecuteClaimedModelJob<C>) -> ModelExecutionDisposition;
}

#[async_trait]
pub trait ModelRequestMaterializer: Send + Sync {
    async fn materialize(
        &self,
        input: &ModelExecutionInput,
        limits: ModelTurnLimits,
    ) -> Result<CanonicalModelRequest, ModelRequestMaterializationError>;
}

#[derive(Debug, Clone, Copy, Default)]
pub struct InlineModelRequestMaterializer;

#[async_trait]
impl ModelRequestMaterializer for InlineModelRequestMaterializer {
    async fn materialize(
        &self,
        input: &ModelExecutionInput,
        limits: ModelTurnLimits,
    ) -> Result<CanonicalModelRequest, ModelRequestMaterializationError> {
        input
            .validate()
            .map_err(|_| ModelRequestMaterializationError::InvalidMaterial)?;
        let ModelExecutionInputMaterial::Inline { value } = &input.material;
        let bytes = serde_jcs::to_vec(value)
            .map_err(|_| ModelRequestMaterializationError::InvalidMaterial)?;
        decode_canonical_model_request(&bytes, input, limits)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModelRequestMaterializationError {
    InvalidMaterial,
}

fn decode_canonical_model_request(
    bytes: &[u8],
    input: &ModelExecutionInput,
    limits: ModelTurnLimits,
) -> Result<CanonicalModelRequest, ModelRequestMaterializationError> {
    if bytes.len() > limits.maximum_request_bytes() {
        return Err(ModelRequestMaterializationError::InvalidMaterial);
    }
    let value = parse_strict_json(bytes, limits.request_json_limits())
        .map_err(|_| ModelRequestMaterializationError::InvalidMaterial)?;
    let canonical =
        serde_jcs::to_vec(&value).map_err(|_| ModelRequestMaterializationError::InvalidMaterial)?;
    if canonical != bytes {
        return Err(ModelRequestMaterializationError::InvalidMaterial);
    }
    let digest: Sha256Digest = canonical_digest(&value)
        .map_err(|_| ModelRequestMaterializationError::InvalidMaterial)?
        .parse()
        .map_err(|_| ModelRequestMaterializationError::InvalidMaterial)?;
    if digest != input.exact.content_digest {
        return Err(ModelRequestMaterializationError::InvalidMaterial);
    }
    serde_json::from_value(value).map_err(|_| ModelRequestMaterializationError::InvalidMaterial)
}

pub struct RegisteredModelJobExecutor<R, O, A, H> {
    request_materializer: R,
    worker: Arc<ModelAdapterWorker<O, A>>,
    heartbeat_authority: Arc<H>,
}

impl<R, O, A, H> RegisteredModelJobExecutor<R, O, A, H> {
    pub fn new(
        request_materializer: R,
        worker: Arc<ModelAdapterWorker<O, A>>,
        heartbeat_authority: Arc<H>,
    ) -> Self {
        Self {
            request_materializer,
            worker,
            heartbeat_authority,
        }
    }
}

#[async_trait]
impl<R, O, A, H> ModelJobCommandExecutor<ClaimedModelExecution>
    for RegisteredModelJobExecutor<R, O, A, H>
where
    R: ModelRequestMaterializer + 'static,
    O: ModelOutputMaterializer + 'static,
    A: ModelExecutionAuthority + Send + Sync + 'static,
    A::Error: Send,
    H: ModelHeartbeatAuthority,
{
    async fn execute(
        &self,
        command: ExecuteClaimedModelJob<ClaimedModelExecution>,
    ) -> ModelExecutionDisposition {
        let binding = match command.claim.claim_binding() {
            Ok(binding) => binding,
            Err(_) => return ModelExecutionDisposition::Abandoned,
        };
        let mut fence = JobFence {
            expected_version: binding.expected_job_version,
            worker_process_generation_id: binding.worker_process_generation_id.clone(),
            lease_generation: binding.lease_generation,
            token_digest: binding.lease_token_digest.clone(),
        };
        let heartbeat_interval = command.heartbeat_interval;
        let lease_milliseconds = command.lease_milliseconds;
        let prepare = async {
            let request = self
                .request_materializer
                .materialize(&command.claim.request_input, command.limits)
                .await
                .map_err(|_| ())?;
            let adapter_job = command
                .claim
                .adapter_job(
                    request,
                    command.worker_manifest_digest,
                    command.audit,
                    command.quota_settlement_entry_ids,
                    command.limits,
                )
                .map_err(|_| ())?;
            self.worker.prepare(adapter_job).await.map_err(|_| ())
        };
        tokio::pin!(prepare);
        let mut heartbeat = tokio::time::interval_at(
            Instant::now()
                .checked_add(heartbeat_interval)
                .unwrap_or_else(Instant::now),
            heartbeat_interval,
        );
        heartbeat.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let mut prepared = loop {
            tokio::select! {
                result = &mut prepare => match result {
                    Ok(prepared) => break prepared,
                    Err(()) => return ModelExecutionDisposition::Abandoned,
                },
                _ = heartbeat.tick() => {
                    match self.heartbeat_authority.heartbeat_model_execution(
                        &binding.tenant_id,
                        &binding.job_id,
                        &fence,
                        lease_milliseconds,
                    ).await {
                        Ok(next) => fence = next,
                        Err(ModelHeartbeatFailure::Unavailable) => {}
                        Err(ModelHeartbeatFailure::FenceLost | ModelHeartbeatFailure::Invariant) => {
                            return ModelExecutionDisposition::Abandoned;
                        }
                    }
                }
            }
        };
        match self
            .heartbeat_authority
            .heartbeat_model_execution(
                &binding.tenant_id,
                &binding.job_id,
                &fence,
                lease_milliseconds,
            )
            .await
        {
            Ok(next) => fence = next,
            Err(ModelHeartbeatFailure::Unavailable) => {}
            Err(ModelHeartbeatFailure::FenceLost | ModelHeartbeatFailure::Invariant) => {
                return ModelExecutionDisposition::Abandoned;
            }
        }
        if prepared.refresh_fence(fence).is_err() {
            return ModelExecutionDisposition::Abandoned;
        }
        match self.worker.commit(prepared).await {
            Ok(_) => ModelExecutionDisposition::Settled,
            Err(_) => ModelExecutionDisposition::Abandoned,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ModelWorkerCycleReport {
    pub claimed: u64,
    pub settled: u64,
    pub abandoned: u64,
}

pub struct ModelWorkerDriver<S, E, I> {
    claim_authority: Arc<S>,
    executor: Arc<E>,
    identities: Arc<I>,
    pools: LocalWorkerPools,
    config: ModelWorkerDriverConfig,
}

impl<S, E, I> ModelWorkerDriver<S, E, I>
where
    S: ModelClaimAuthority,
    E: ModelJobCommandExecutor<S::Claim>,
    I: ModelWorkerIdentityFactory + 'static,
{
    pub fn new(
        claim_authority: Arc<S>,
        executor: Arc<E>,
        identities: Arc<I>,
        pools: LocalWorkerPools,
        config: ModelWorkerDriverConfig,
    ) -> Result<Self, ModelWorkerDriverError> {
        config.validate()?;
        let pool = pools.snapshot();
        if pool.worker_role != MODEL_WORKER_ROLE || pool.work_class != WorkClass::Model {
            return Err(ModelWorkerDriverError::WrongWorkerManifest);
        }
        Ok(Self {
            claim_authority,
            executor,
            identities,
            pools,
            config,
        })
    }

    pub async fn drive_once(
        &self,
        active: &mut JoinSet<ModelExecutionDisposition>,
    ) -> Result<usize, ModelWorkerDriverError> {
        let Some(reservation) = self
            .pools
            .reserve_claim_capacity(
                WorkClass::Model,
                self.config.requested_claim_size,
                self.config.claim_batch_hard_limit,
            )
            .map_err(ModelWorkerDriverError::LocalPool)?
        else {
            return Ok(0);
        };
        let claim_limit = reservation.claim_limit();
        let slots = (0..claim_limit)
            .map(|_| self.claim_slot())
            .collect::<Result<Vec<_>, _>>()?;
        let expected_tokens = slots
            .iter()
            .map(|slot| slot.lease_token_digest.clone())
            .collect::<BTreeSet<_>>();
        let claimed = self
            .claim_authority
            .claim_model_jobs(ClaimModelJobs {
                worker_process_generation_id: self.pools.snapshot().worker_process_generation_id,
                limit: u16::try_from(claim_limit)
                    .map_err(|_| ModelWorkerDriverError::InvalidGeneratedCommand)?,
                lease_milliseconds: self.config.lease_milliseconds,
                slots,
            })
            .await
            .map_err(ModelWorkerDriverError::Claim)?;
        if claimed.is_empty() {
            return Ok(0);
        }
        let pool = self.pools.snapshot();
        let bindings = validate_claims(&claimed, &pool, &expected_tokens)?;
        let permits = reservation
            .bind_claimed_jobs(
                bindings
                    .iter()
                    .map(|binding| ClaimedJobIdentity {
                        job_id: binding.job_id.clone(),
                        lease_generation: binding.lease_generation,
                    })
                    .collect(),
            )
            .map_err(ModelWorkerDriverError::LocalPool)?;
        if permits.len() != claimed.len() {
            return Err(ModelWorkerDriverError::CorruptClaim);
        }
        let count = claimed.len();
        for ((claim, binding), permit) in claimed.into_iter().zip(bindings).zip(permits) {
            let command = self.execution_command(claim, &binding)?;
            let executor = Arc::clone(&self.executor);
            active.spawn(async move {
                let _permit = permit;
                executor.execute(command).await
            });
        }
        Ok(count)
    }

    fn claim_slot(&self) -> Result<ModelClaimSlot, ModelWorkerDriverError> {
        Ok(ModelClaimSlot {
            lease_token_digest: self.new_digest()?,
            usage_reservation_id: self.new_id(ResourceKind::UsageReservation)?,
            quota_entry_ids: self.quota_entry_ids()?,
            event_id: self.new_id(ResourceKind::Event)?,
            outbox_id: self.new_id(ResourceKind::OutboxEvent)?,
            resume_mutations: Some(insight_platform_contracts::ExternalLeafResumeMutationIds {
                continuation_node_execution_id: self.new_id(ResourceKind::NodeExecution)?,
                continuation_job_id: self.new_id(ResourceKind::Job)?,
                run_event_id: self.new_id(ResourceKind::Event)?,
                run_outbox_id: self.new_id(ResourceKind::OutboxEvent)?,
                leaf_node_event_id: self.new_id(ResourceKind::Event)?,
                leaf_node_outbox_id: self.new_id(ResourceKind::OutboxEvent)?,
                continuation_node_event_id: self.new_id(ResourceKind::Event)?,
                continuation_node_outbox_id: self.new_id(ResourceKind::OutboxEvent)?,
                continuation_job_event_id: self.new_id(ResourceKind::Event)?,
                continuation_job_outbox_id: self.new_id(ResourceKind::OutboxEvent)?,
            }),
            failure_mutations: Some(insight_platform_contracts::ExternalLeafFailureMutationIds {
                convergence_job_id: self.new_id(ResourceKind::Job)?,
                run_event_id: self.new_id(ResourceKind::Event)?,
                run_outbox_id: self.new_id(ResourceKind::OutboxEvent)?,
                leaf_node_event_id: self.new_id(ResourceKind::Event)?,
                leaf_node_outbox_id: self.new_id(ResourceKind::OutboxEvent)?,
                convergence_job_event_id: self.new_id(ResourceKind::Event)?,
                convergence_job_outbox_id: self.new_id(ResourceKind::OutboxEvent)?,
            }),
            tool_continuation_mutations: Some(
                insight_platform_models::ModelToolContinuationMutationIds {
                    continuation_job_id: self.new_id(ResourceKind::Job)?,
                    run_event_id: self.new_id(ResourceKind::Event)?,
                    run_outbox_id: self.new_id(ResourceKind::OutboxEvent)?,
                    node_event_id: self.new_id(ResourceKind::Event)?,
                    node_outbox_id: self.new_id(ResourceKind::OutboxEvent)?,
                    continuation_job_event_id: self.new_id(ResourceKind::Event)?,
                    continuation_job_outbox_id: self.new_id(ResourceKind::OutboxEvent)?,
                },
            ),
        })
    }

    fn execution_command(
        &self,
        claim: S::Claim,
        binding: &ModelClaimBinding,
    ) -> Result<ExecuteClaimedModelJob<S::Claim>, ModelWorkerDriverError> {
        let receipt_expires_at = Utc::now()
            .checked_add_signed(
                ChronoDuration::from_std(self.config.receipt_ttl)
                    .map_err(|_| ModelWorkerDriverError::InvalidGeneratedCommand)?,
            )
            .ok_or(ModelWorkerDriverError::InvalidGeneratedCommand)?;
        let audit = ModelWorkerAudit {
            tenant_id: binding.tenant_id.clone(),
            worker_process_generation_id: binding.worker_process_generation_id.clone(),
            receipt_id: self.new_id(ResourceKind::Receipt)?,
            event_id: self.new_id(ResourceKind::Event)?,
            outbox_id: self.new_id(ResourceKind::OutboxEvent)?,
            idempotency_key_digest: self.new_digest()?,
            request_digest: binding.request_digest.clone(),
            receipt_expires_at,
        };
        audit
            .validate_at(Utc::now())
            .map_err(|_| ModelWorkerDriverError::InvalidGeneratedCommand)?;
        let quota_settlement_entry_ids = self.quota_entry_ids()?;
        if quota_settlement_entry_ids
            .iter()
            .any(|identity| binding.quota_reservation_entry_ids.contains(identity))
        {
            return Err(ModelWorkerDriverError::InvalidGeneratedCommand);
        }
        Ok(ExecuteClaimedModelJob {
            claim,
            audit,
            quota_settlement_entry_ids,
            worker_manifest_digest: binding.worker_manifest_digest.clone(),
            limits: self.config.limits,
            heartbeat_interval: self.config.heartbeat_interval,
            lease_milliseconds: self.config.lease_milliseconds,
        })
    }

    fn quota_entry_ids(&self) -> Result<Vec<ResourceId>, ModelWorkerDriverError> {
        let mut ids = (0..MODEL_QUOTA_LINES)
            .map(|_| self.new_id(ResourceKind::QuotaLedgerEntry))
            .collect::<Result<Vec<_>, _>>()?;
        ids.sort();
        if ids.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(ModelWorkerDriverError::InvalidGeneratedCommand);
        }
        Ok(ids)
    }

    fn new_id(&self, kind: ResourceKind) -> Result<ResourceId, ModelWorkerDriverError> {
        self.identities
            .new_resource_id(kind)
            .map_err(ModelWorkerDriverError::Identity)
    }

    fn new_digest(&self) -> Result<Sha256Digest, ModelWorkerDriverError> {
        self.identities
            .new_opaque_digest()
            .map_err(ModelWorkerDriverError::Identity)
    }

    pub async fn run(
        self,
        cancellation: CancellationToken,
    ) -> Result<ModelWorkerCycleReport, ModelWorkerDriverError> {
        let mut active = JoinSet::new();
        let mut report = ModelWorkerCycleReport::default();
        let mut scan =
            tokio::time::interval_at(Instant::now(), self.config.timing.safety_scan_interval);
        scan.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                biased;
                _ = cancellation.cancelled() => break,
                joined = active.join_next(), if !active.is_empty() => {
                    observe_join(joined, &mut report);
                }
                _ = self.pools.wait_for_business_capacity(), if self.pools.snapshot().business_available == 0 => {}
                _ = scan.tick() => {
                    match self.drive_once(&mut active).await {
                        Ok(claimed) => report.claimed = report.claimed.saturating_add(claimed as u64),
                        Err(ModelWorkerDriverError::Claim(
                            ModelClaimFailure::Unavailable | ModelClaimFailure::FirstWinnerLost,
                        )) => {
                            tokio::select! {
                                _ = cancellation.cancelled() => break,
                                _ = tokio::time::sleep(self.config.timing.claim_failure_backoff) => {}
                            }
                        }
                        Err(failure) => return Err(failure),
                    }
                }
            }
        }
        let drain = async {
            while let Some(joined) = active.join_next().await {
                observe_join(Some(joined), &mut report);
            }
        };
        if tokio::time::timeout(self.config.timing.drain_grace, drain)
            .await
            .is_err()
        {
            return Err(ModelWorkerDriverError::DrainRequiresProcessTermination);
        }
        Ok(report)
    }
}

fn validate_claims<C: ClaimedModelJob>(
    claimed: &[C],
    pool: &insight_platform_worker::LocalWorkerPoolSnapshot,
    expected_tokens: &BTreeSet<Sha256Digest>,
) -> Result<Vec<ModelClaimBinding>, ModelWorkerDriverError> {
    if claimed.len() > expected_tokens.len() {
        return Err(ModelWorkerDriverError::CorruptClaim);
    }
    let mut jobs = BTreeSet::new();
    let mut tokens = BTreeSet::new();
    let mut bindings = Vec::with_capacity(claimed.len());
    for claim in claimed {
        let binding = claim
            .claim_binding()
            .map_err(|_| ModelWorkerDriverError::CorruptClaim)?;
        if binding.worker_process_generation_id != pool.worker_process_generation_id
            || binding.worker_manifest_digest != pool.worker_manifest_digest
            || !expected_tokens.contains(&binding.lease_token_digest)
            || !jobs.insert(binding.job_id.clone())
            || !tokens.insert(binding.lease_token_digest.clone())
            || binding.quota_reservation_entry_ids.len() != MODEL_QUOTA_LINES
            || binding
                .quota_reservation_entry_ids
                .iter()
                .any(|id| id.kind() != ResourceKind::QuotaLedgerEntry)
            || binding
                .quota_reservation_entry_ids
                .iter()
                .collect::<BTreeSet<_>>()
                .len()
                != binding.quota_reservation_entry_ids.len()
        {
            return Err(ModelWorkerDriverError::CorruptClaim);
        }
        bindings.push(binding);
    }
    Ok(bindings)
}

fn observe_join(
    joined: Option<Result<ModelExecutionDisposition, JoinError>>,
    report: &mut ModelWorkerCycleReport,
) {
    match joined {
        Some(Ok(ModelExecutionDisposition::Settled)) => {
            report.settled = report.settled.saturating_add(1)
        }
        Some(Ok(ModelExecutionDisposition::Abandoned)) | Some(Err(_)) | None => {
            report.abandoned = report.abandoned.saturating_add(1)
        }
    }
}

#[derive(Debug)]
pub enum ModelWorkerDriverError {
    InvalidConfig(String),
    WrongWorkerManifest,
    InvalidGeneratedCommand,
    CorruptClaim,
    DrainRequiresProcessTermination,
    Identity(ModelWorkerIdentityError),
    LocalPool(LocalWorkerPoolError),
    Claim(ModelClaimFailure),
}

impl fmt::Display for ModelWorkerDriverError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidConfig(_) => "Model Worker driver configuration is invalid",
            Self::WrongWorkerManifest => "Model Worker manifest is invalid",
            Self::InvalidGeneratedCommand => "Model Worker generated an invalid command",
            Self::CorruptClaim => "Model claim response is inconsistent",
            Self::DrainRequiresProcessTermination => {
                "Model Worker drain grace expired; process-generation termination is required"
            }
            Self::Identity(_) => "Model Worker identity generation failed",
            Self::LocalPool(_) => "Model Worker local pool failed",
            Self::Claim(_) => "Model claim authority failed",
        })
    }
}

impl Error for ModelWorkerDriverError {}

fn materialization_failure(code: &'static str, request_sent: bool) -> ModelAdapterFailure {
    let evidence_digest = canonical_digest(&serde_json::json!({
        "code": code,
        "domain": "model_worker_materialization",
        "schema_version": 1,
    }))
    .ok()
    .and_then(|digest| digest.parse().ok())
    .unwrap_or_else(|| {
        "sha256:0000000000000000000000000000000000000000000000000000000000000000"
            .parse()
            .expect("static SHA-256 digest is valid")
    });
    ModelAdapterFailure {
        class: ModelAdapterFailureClass::Permanent,
        safe_code: code.to_owned(),
        safe_message: "Model value materialization failed".to_owned(),
        evidence_digest,
        request_sent,
        retry_at: None,
    }
}

const INLINE_MODEL_RESPONSE_ENVELOPE_OVERHEAD_BYTES: usize = 65_536;

/// Bounded first-release Inline output materializer. Oversized output fails with the stable
/// `model_output_too_large` code and is never truncated or staged as an Artifact.
pub struct InlineModelOutputMaterializer<I> {
    identities: Arc<I>,
    limits: ModelTurnLimits,
}

impl<I> InlineModelOutputMaterializer<I> {
    pub fn new(identities: Arc<I>, limits: ModelTurnLimits) -> Self {
        Self { identities, limits }
    }
}

#[async_trait]
impl<I> ModelOutputMaterializer for InlineModelOutputMaterializer<I>
where
    I: ModelWorkerIdentityFactory,
{
    fn validate_execution(
        &self,
        execution: &insight_platform_model_adapters::ModelAdapterExecutionRequest,
    ) -> Result<(), ModelAdapterFailure> {
        let maximum = usize::try_from(execution.provider.request_limits.maximum_response_bytes)
            .ok()
            .and_then(|bytes| bytes.checked_add(INLINE_MODEL_RESPONSE_ENVELOPE_OVERHEAD_BYTES));
        if maximum.is_none_or(|maximum| maximum > self.limits.inline_value_limits().max_bytes) {
            let mut failure = materialization_failure("model_output_too_large", false);
            failure.class = ModelAdapterFailureClass::RejectedBeforeDispatch;
            return Err(failure);
        }
        Ok(())
    }

    async fn materialize(
        &self,
        execution: &insight_platform_model_adapters::ModelAdapterExecutionRequest,
        success: insight_platform_model_adapters::ModelAdapterSuccess,
    ) -> Result<insight_platform_models::ModelOutputValue, ModelAdapterFailure> {
        let value = serde_json::to_value(&success.response)
            .map_err(|_| materialization_failure("model_output_invalid", true))?;
        insight_platform_contracts::ValueRef::Inline {
            value: value.clone(),
        }
        .validate(self.limits.inline_value_limits())
        .map_err(|_| materialization_failure("model_output_too_large", true))?;
        let content_digest: Sha256Digest = canonical_digest(&value)
            .map_err(|_| materialization_failure("model_output_invalid", true))?
            .parse()
            .map_err(|_| materialization_failure("model_output_invalid", true))?;
        let validation_evidence_digest: Sha256Digest = canonical_digest(&serde_json::json!({
            "content_digest": content_digest,
            "domain": "model_inline_output_validation",
            "schema_digest": execution.request.response_contract.output_schema_digest,
            "stream_evidence": {
                "accepted_delta_bytes": success.stream_evidence.accepted_delta_bytes,
                "accepted_delta_count": success.stream_evidence.accepted_delta_count,
                "terminal_sequence": success.stream_evidence.terminal_sequence,
            },
            "schema_version": 1,
        }))
        .map_err(|_| materialization_failure("model_output_invalid", true))?
        .parse()
        .map_err(|_| materialization_failure("model_output_invalid", true))?;
        Ok(insight_platform_models::ModelOutputValue {
            value_id: self
                .identities
                .new_resource_id(ResourceKind::RunValue)
                .map_err(|_| materialization_failure("model_output_identity_failed", true))?,
            classification: execution.request.classification,
            schema_digest: execution
                .request
                .response_contract
                .output_schema_digest
                .clone(),
            content_digest,
            value: insight_platform_contracts::ValueRef::Inline { value },
            response: *success.response,
            validation_evidence_digest,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt as _;
    use insight_platform_contracts::{
        checked_in_hard_limit_profile, ExactDeploymentRef, ResourceKind, WorkerManifest,
        WORKER_MANIFEST_VERSION, WORKER_PROTOCOL_VERSION,
    };
    use insight_platform_model_adapters::{ModelProviderWireRequest, ModelProviderWireStream};
    use insight_platform_models::{AccountingQuality, ModelObservation, ModelUsage};
    use std::sync::Mutex;

    #[derive(Debug, Clone)]
    struct FakeClaim(ModelClaimBinding);

    impl ClaimedModelJob for FakeClaim {
        fn claim_binding(&self) -> Result<ModelClaimBinding, ModelClaimFailure> {
            Ok(self.0.clone())
        }
    }

    #[derive(Default)]
    struct EmptyAuthority {
        claim: Mutex<Option<ClaimModelJobs>>,
    }

    #[async_trait]
    impl ModelClaimAuthority for EmptyAuthority {
        type Claim = FakeClaim;

        async fn claim_model_jobs(
            &self,
            command: ClaimModelJobs,
        ) -> Result<Vec<Self::Claim>, ModelClaimFailure> {
            *self.claim.lock().unwrap() = Some(command);
            Ok(Vec::new())
        }
    }

    struct UnusedExecutor;

    #[async_trait]
    impl ModelJobCommandExecutor<FakeClaim> for UnusedExecutor {
        async fn execute(
            &self,
            _command: ExecuteClaimedModelJob<FakeClaim>,
        ) -> ModelExecutionDisposition {
            panic!("empty authority must not dispatch")
        }
    }

    struct EchoAuthority {
        worker_manifest_digest: Sha256Digest,
        corrupt_manifest: bool,
    }

    #[async_trait]
    impl ModelClaimAuthority for EchoAuthority {
        type Claim = FakeClaim;

        async fn claim_model_jobs(
            &self,
            command: ClaimModelJobs,
        ) -> Result<Vec<Self::Claim>, ModelClaimFailure> {
            let slot = command.slots.into_iter().next().unwrap();
            Ok(vec![FakeClaim(ModelClaimBinding {
                tenant_id: ResourceId::from_uuid_v7(ResourceKind::Tenant, Uuid::now_v7()).unwrap(),
                job_id: ResourceId::from_uuid_v7(ResourceKind::Job, Uuid::now_v7()).unwrap(),
                worker_process_generation_id: command.worker_process_generation_id,
                worker_manifest_digest: if self.corrupt_manifest {
                    "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
                        .parse()
                        .unwrap()
                } else {
                    self.worker_manifest_digest.clone()
                },
                expected_job_version: 2,
                lease_generation: 1,
                lease_token_digest: slot.lease_token_digest,
                request_digest:
                    "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
                        .parse()
                        .unwrap(),
                quota_reservation_entry_ids: slot.quota_entry_ids,
            })])
        }
    }

    struct SettlingExecutor;

    #[async_trait]
    impl ModelJobCommandExecutor<FakeClaim> for SettlingExecutor {
        async fn execute(
            &self,
            command: ExecuteClaimedModelJob<FakeClaim>,
        ) -> ModelExecutionDisposition {
            assert_eq!(command.audit.tenant_id, command.claim.0.tenant_id);
            assert_eq!(command.quota_settlement_entry_ids.len(), MODEL_QUOTA_LINES);
            ModelExecutionDisposition::Settled
        }
    }

    fn model_manifest(max_concurrency: u16) -> WorkerManifest {
        WorkerManifest {
            manifest_version: WORKER_MANIFEST_VERSION,
            worker_role: MODEL_WORKER_ROLE.to_owned(),
            work_class: WorkClass::Model,
            adapter_runtime_digest:
                "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                    .parse()
                    .unwrap(),
            protocol_version: WORKER_PROTOCOL_VERSION,
            max_concurrency,
            critical_control_reserved_slots: 1,
        }
    }

    fn config() -> ModelWorkerDriverConfig {
        ModelWorkerDriverConfig::from_profile(
            &checked_in_hard_limit_profile(),
            Duration::from_secs(300),
            ModelWorkerDriverTiming {
                safety_scan_interval: Duration::from_millis(100),
                claim_failure_backoff: Duration::from_millis(10),
                drain_grace: Duration::from_secs(1),
            },
        )
        .unwrap()
    }

    fn cancellation_config() -> ModelCancellationDriverConfig {
        ModelCancellationDriverConfig::from_profile(
            &checked_in_hard_limit_profile(),
            Duration::from_secs(300),
            Duration::from_millis(100),
            Duration::from_millis(10),
        )
        .unwrap()
    }

    fn resource(kind: ResourceKind) -> ResourceId {
        ResourceId::from_uuid_v7(kind, Uuid::now_v7()).unwrap()
    }

    fn digest(character: char) -> Sha256Digest {
        format!("sha256:{}", character.to_string().repeat(64))
            .parse()
            .unwrap()
    }

    fn cancellation_candidate(pools: &LocalWorkerPools) -> ModelCancellationCandidate {
        let snapshot = pools.snapshot();
        ModelCancellationCandidate {
            tenant_id: resource(ResourceKind::Tenant),
            model_turn_id: resource(ResourceKind::ModelTurn),
            job_id: resource(ResourceKind::Job),
            worker_process_generation_id: snapshot.worker_process_generation_id.clone(),
            worker_manifest_digest: snapshot.worker_manifest_digest,
            expected_turn_version: 3,
            fence: JobFence {
                expected_version: 4,
                worker_process_generation_id: snapshot.worker_process_generation_id,
                lease_generation: 1,
                token_digest: digest('d'),
            },
            usage_reservation_id: resource(ResourceKind::UsageReservation),
            protocol: ModelProviderWireProtocol::OpenAiResponses,
            provider_deployment: ExactDeploymentRef::new(
                resource(ResourceKind::ModelProviderDeployment),
                digest('e'),
            )
            .unwrap(),
            attempt_no: 1,
            deadline: Utc::now() + ChronoDuration::minutes(1),
            request_digest: digest('f'),
            measurement: ModelAttemptMeasurement {
                usage: Some(ModelUsage {
                    input_tokens: Some(20),
                    output_tokens: Some(0),
                    cached_input_tokens: None,
                    reasoning_tokens: None,
                    provider_reported_cost: None,
                    accounting_quality: AccountingQuality::Reconciled,
                }),
                observation: ModelObservation {
                    request_sent: true,
                    provider_response_digest: None,
                    actual_model_identity: None,
                    model_fingerprint: None,
                    possible_duplicate_charge: true,
                    stream_delta_count: 0,
                    stream_bytes: 0,
                },
            },
        }
    }

    fn live_delta_config(
        maximum_pending_messages: usize,
        maximum_pending_bytes: usize,
    ) -> ModelLiveDeltaNatsConfig {
        ModelLiveDeltaNatsConfig::from_profile(
            &checked_in_hard_limit_profile(),
            ModelLiveDeltaNatsSettings {
                servers: vec!["tls://nats.platform-runtime.svc:4222".to_owned()],
                namespace: "candidate".to_owned(),
                root_certificate: PathBuf::from("/tmp/nats-ca.pem"),
                client_certificate: PathBuf::from("/tmp/nats-client.pem"),
                client_private_key: PathBuf::from("/tmp/nats-client-key.pem"),
                connect_timeout: Duration::from_secs(1),
                publish_timeout: Duration::from_millis(100),
                reconnect_backoff: Duration::from_millis(100),
                drain_timeout: Duration::from_secs(1),
                maximum_pending_messages,
                maximum_pending_bytes,
            },
        )
        .unwrap()
    }

    fn live_delta(text: String) -> ModelLiveTextDelta {
        ModelLiveTextDelta {
            schema_version: 1,
            tenant_id: resource(ResourceKind::Tenant),
            run_id: resource(ResourceKind::Run),
            model_turn_id: resource(ResourceKind::ModelTurn),
            job_id: resource(ResourceKind::Job),
            worker_process_generation_id: resource(ResourceKind::WorkerProcessGeneration),
            attempt_no: 1,
            lease_generation: 1,
            transport_sequence: 1,
            request_digest: digest('7'),
            classification: insight_platform_contracts::DataClassification::Internal,
            text,
        }
    }

    struct CancellationSource {
        expected_generation: ResourceId,
        candidates: Mutex<Option<Vec<ModelCancellationCandidate>>>,
    }

    #[async_trait]
    impl ModelCancellationSource for CancellationSource {
        async fn scan_model_cancellations(
            &self,
            worker_process_generation_id: &ResourceId,
            limit: u16,
            _limits: ModelTurnLimits,
        ) -> Result<Vec<ModelCancellationCandidate>, ModelCancellationSourceFailure> {
            assert_eq!(worker_process_generation_id, &self.expected_generation);
            assert!(limit > 0);
            Ok(self.candidates.lock().unwrap().take().unwrap_or_default())
        }
    }

    #[derive(Clone)]
    enum CancelResponse {
        Outcome(ModelAdapterCancelOutcome),
        Failure(ModelAdapterFailure),
    }

    struct CancellationPort {
        response: CancelResponse,
        requests: Mutex<Vec<(ModelProviderWireProtocol, ModelAdapterCancelRequest)>>,
    }

    #[async_trait]
    impl ModelProviderWireConnector for CancellationPort {
        async fn open(
            &self,
            _request: ModelProviderWireRequest,
        ) -> Result<ModelProviderWireStream, ModelAdapterFailure> {
            panic!("cancellation driver must not open a Provider stream")
        }

        async fn cancel(
            &self,
            protocol: ModelProviderWireProtocol,
            request: ModelAdapterCancelRequest,
        ) -> Result<ModelAdapterCancelOutcome, ModelAdapterFailure> {
            self.requests.lock().unwrap().push((protocol, request));
            match &self.response {
                CancelResponse::Outcome(outcome) => Ok(*outcome),
                CancelResponse::Failure(failure) => Err(failure.clone()),
            }
        }
    }

    struct CancellationAuthority {
        commands: Mutex<Vec<CommitModelCancellationOutcome>>,
    }

    #[async_trait]
    impl ModelCancellationAuthority for CancellationAuthority {
        async fn commit_model_cancellation(
            &self,
            command: CommitModelCancellationOutcome,
        ) -> Result<(), ModelCancellationCommitFailure> {
            self.commands.lock().unwrap().push(command);
            Ok(())
        }
    }

    #[tokio::test]
    async fn reserves_local_model_capacity_before_bounded_claim() {
        let authority = Arc::new(EmptyAuthority::default());
        let process_generation =
            ResourceId::from_uuid_v7(ResourceKind::WorkerProcessGeneration, Uuid::now_v7())
                .unwrap();
        let pools = LocalWorkerPools::new(model_manifest(3), process_generation.clone()).unwrap();
        let driver = ModelWorkerDriver::new(
            Arc::clone(&authority),
            Arc::new(UnusedExecutor),
            Arc::new(UuidModelWorkerIdentityFactory),
            pools,
            config(),
        )
        .unwrap();
        let mut active = JoinSet::new();
        assert_eq!(driver.drive_once(&mut active).await.unwrap(), 0);
        let claim = authority.claim.lock().unwrap().clone().unwrap();
        assert_eq!(claim.worker_process_generation_id, process_generation);
        assert_eq!(claim.limit, 3);
        assert_eq!(claim.slots.len(), 3);
        assert!(claim.slots.iter().all(|slot| {
            slot.quota_entry_ids.len() == MODEL_QUOTA_LINES
                && slot
                    .quota_entry_ids
                    .windows(2)
                    .all(|pair| pair[0] < pair[1])
        }));
    }

    #[tokio::test]
    async fn saturated_model_pool_does_not_touch_claim_authority() {
        let authority = Arc::new(EmptyAuthority::default());
        let process_generation =
            ResourceId::from_uuid_v7(ResourceKind::WorkerProcessGeneration, Uuid::now_v7())
                .unwrap();
        let pools = LocalWorkerPools::new(model_manifest(1), process_generation).unwrap();
        let reservation = pools
            .reserve_claim_capacity(
                WorkClass::Model,
                1,
                ClaimBatchHardLimit::from_profile(&checked_in_hard_limit_profile()).unwrap(),
            )
            .unwrap()
            .unwrap();
        let driver = ModelWorkerDriver::new(
            Arc::clone(&authority),
            Arc::new(UnusedExecutor),
            Arc::new(UuidModelWorkerIdentityFactory),
            pools,
            config(),
        )
        .unwrap();
        let mut active = JoinSet::new();
        assert_eq!(driver.drive_once(&mut active).await.unwrap(), 0);
        assert!(authority.claim.lock().unwrap().is_none());
        drop(reservation);
    }

    #[tokio::test]
    async fn exact_claim_is_bound_to_one_reserved_permit_before_dispatch() {
        let process_generation =
            ResourceId::from_uuid_v7(ResourceKind::WorkerProcessGeneration, Uuid::now_v7())
                .unwrap();
        let pools = LocalWorkerPools::new(model_manifest(2), process_generation).unwrap();
        let authority = Arc::new(EchoAuthority {
            worker_manifest_digest: pools.snapshot().worker_manifest_digest,
            corrupt_manifest: false,
        });
        let driver = ModelWorkerDriver::new(
            authority,
            Arc::new(SettlingExecutor),
            Arc::new(UuidModelWorkerIdentityFactory),
            pools.clone(),
            config(),
        )
        .unwrap();
        let mut active = JoinSet::new();
        assert_eq!(driver.drive_once(&mut active).await.unwrap(), 1);
        assert_eq!(
            active.join_next().await.unwrap().unwrap(),
            ModelExecutionDisposition::Settled
        );
        assert_eq!(pools.snapshot().business_available, 2);
    }

    #[tokio::test]
    async fn manifest_drift_in_claim_response_fails_closed() {
        let process_generation =
            ResourceId::from_uuid_v7(ResourceKind::WorkerProcessGeneration, Uuid::now_v7())
                .unwrap();
        let pools = LocalWorkerPools::new(model_manifest(1), process_generation).unwrap();
        let authority = Arc::new(EchoAuthority {
            worker_manifest_digest: pools.snapshot().worker_manifest_digest,
            corrupt_manifest: true,
        });
        let driver = ModelWorkerDriver::new(
            authority,
            Arc::new(SettlingExecutor),
            Arc::new(UuidModelWorkerIdentityFactory),
            pools,
            config(),
        )
        .unwrap();
        let mut active = JoinSet::new();
        assert!(matches!(
            driver.drive_once(&mut active).await,
            Err(ModelWorkerDriverError::CorruptClaim)
        ));
        assert!(active.is_empty());
    }

    #[tokio::test]
    async fn cancellation_uses_reserved_control_capacity_when_business_is_saturated() {
        let process_generation = resource(ResourceKind::WorkerProcessGeneration);
        let pools = LocalWorkerPools::new(model_manifest(1), process_generation.clone()).unwrap();
        let business = pools
            .reserve_claim_capacity(
                WorkClass::Model,
                1,
                ClaimBatchHardLimit::from_profile(&checked_in_hard_limit_profile()).unwrap(),
            )
            .unwrap()
            .unwrap();
        let candidate = cancellation_candidate(&pools);
        let expected_request = candidate.cancel_request();
        let source = Arc::new(CancellationSource {
            expected_generation: process_generation,
            candidates: Mutex::new(Some(vec![candidate])),
        });
        let authority = Arc::new(CancellationAuthority {
            commands: Mutex::new(Vec::new()),
        });
        let port = Arc::new(CancellationPort {
            response: CancelResponse::Outcome(ModelAdapterCancelOutcome::Accepted),
            requests: Mutex::new(Vec::new()),
        });
        let driver = ModelCancellationDriver::new(
            source,
            Arc::clone(&authority),
            Arc::clone(&port),
            Arc::new(UuidModelWorkerIdentityFactory),
            pools,
            cancellation_config(),
        )
        .unwrap();

        let report = driver.drive_once().await.unwrap();
        assert_eq!(report.discovered, 1);
        assert_eq!(report.cancelled, 1);
        assert_eq!(
            port.requests.lock().unwrap().as_slice(),
            &[(ModelProviderWireProtocol::OpenAiResponses, expected_request,)]
        );
        let commands = authority.commands.lock().unwrap();
        assert_eq!(commands.len(), 1);
        assert!(commands[0].measurement.observation.request_sent);
        assert!(
            commands[0]
                .measurement
                .observation
                .possible_duplicate_charge
        );
        assert_eq!(commands[0].quota_entry_ids.len(), MODEL_QUOTA_LINES);
        assert!(commands[0]
            .quota_entry_ids
            .windows(2)
            .all(|pair| pair[0] < pair[1]));
        drop(business);
    }

    #[tokio::test]
    async fn retryable_cancel_failure_defers_without_terminal_commit() {
        let process_generation = resource(ResourceKind::WorkerProcessGeneration);
        let pools = LocalWorkerPools::new(model_manifest(1), process_generation.clone()).unwrap();
        let candidate = cancellation_candidate(&pools);
        let deadline = candidate.deadline;
        let source = Arc::new(CancellationSource {
            expected_generation: process_generation,
            candidates: Mutex::new(Some(vec![candidate])),
        });
        let authority = Arc::new(CancellationAuthority {
            commands: Mutex::new(Vec::new()),
        });
        let port = Arc::new(CancellationPort {
            response: CancelResponse::Failure(ModelAdapterFailure {
                class: ModelAdapterFailureClass::RetryableAfterDispatch,
                safe_code: "model_cancel_temporarily_unavailable".to_owned(),
                safe_message: "Model cancellation is temporarily unavailable".to_owned(),
                evidence_digest: digest('9'),
                request_sent: true,
                retry_at: Some(deadline - ChronoDuration::seconds(1)),
            }),
            requests: Mutex::new(Vec::new()),
        });
        let driver = ModelCancellationDriver::new(
            source,
            Arc::clone(&authority),
            port,
            Arc::new(UuidModelWorkerIdentityFactory),
            pools,
            cancellation_config(),
        )
        .unwrap();

        let report = driver.drive_once().await.unwrap();
        assert_eq!(report.discovered, 1);
        assert_eq!(report.deferred, 1);
        assert!(authority.commands.lock().unwrap().is_empty());
    }

    #[test]
    fn live_delta_pipeline_is_canonical_tenant_scoped_and_end_to_end_bounded() {
        let limits = ModelTurnLimits::from_profile(&checked_in_hard_limit_profile()).unwrap();
        let (sink, mut driver) =
            BufferedNatsModelLiveDeltaSink::new(live_delta_config(1, 1_048_576), limits).unwrap();
        let first = live_delta("first".to_owned());
        let expected_subject =
            model_live_delta_subject("candidate", &first.tenant_id, &first.run_id).unwrap();
        sink.enqueue(first.clone());
        sink.enqueue(live_delta("second".to_owned()));

        let queued = driver.receiver.try_recv().unwrap();
        sink.enqueue(live_delta("still-held-after-dequeue".to_owned()));
        assert_eq!(queued.subject, expected_subject);
        assert_eq!(
            serde_json::from_slice::<ModelLiveTextDelta>(&queued.payload).unwrap(),
            first
        );
        assert_eq!(serde_jcs::to_vec(&first).unwrap(), queued.payload);
        assert_eq!(
            sink.report(),
            ModelLiveDeltaReport {
                enqueued: 1,
                dropped_backpressure: 2,
                ..Default::default()
            }
        );
        drop(queued);
        sink.enqueue(live_delta("capacity-released".to_owned()));
        assert!(driver.receiver.try_recv().is_ok());
        assert_eq!(sink.report().enqueued, 2);
    }

    #[test]
    fn live_delta_envelope_over_nats_limit_is_dropped() {
        let limits = ModelTurnLimits::from_profile(&checked_in_hard_limit_profile()).unwrap();
        let (sink, mut driver) =
            BufferedNatsModelLiveDeltaSink::new(live_delta_config(1, 1_048_576), limits).unwrap();
        sink.enqueue(live_delta(
            "x".repeat(limits.maximum_delta_bytes().saturating_sub(2)),
        ));
        assert!(driver.receiver.try_recv().is_err());
        assert_eq!(sink.report().dropped_oversized, 1);
    }

    #[tokio::test]
    async fn real_tls_nats_publishes_live_delta_when_configured() {
        let (Ok(server), Ok(ca), Ok(certificate), Ok(private_key)) = (
            std::env::var("PLATFORM_TEST_NATS_URL"),
            std::env::var("PLATFORM_TEST_NATS_CA_PATH"),
            std::env::var("PLATFORM_TEST_NATS_CERT_PATH"),
            std::env::var("PLATFORM_TEST_NATS_KEY_PATH"),
        ) else {
            return;
        };
        let limits = ModelTurnLimits::from_profile(&checked_in_hard_limit_profile()).unwrap();
        let config = ModelLiveDeltaNatsConfig::from_profile(
            &checked_in_hard_limit_profile(),
            ModelLiveDeltaNatsSettings {
                servers: vec![server],
                namespace: "model_worker_fixture".to_owned(),
                root_certificate: PathBuf::from(ca),
                client_certificate: PathBuf::from(certificate),
                client_private_key: PathBuf::from(private_key),
                connect_timeout: Duration::from_secs(5),
                publish_timeout: Duration::from_secs(1),
                reconnect_backoff: Duration::from_millis(100),
                drain_timeout: Duration::from_secs(1),
                maximum_pending_messages: 16,
                maximum_pending_bytes: 1_048_576,
            },
        )
        .unwrap();
        let subscriber_client = NatsModelLiveDeltaDriver::connect(&config).await.unwrap();
        let event = live_delta("real NATS delta".to_owned());
        let subject =
            model_live_delta_subject("model_worker_fixture", &event.tenant_id, &event.run_id)
                .unwrap();
        let mut subscriber = subscriber_client.subscribe(subject).await.unwrap();
        subscriber_client.flush().await.unwrap();
        let (sink, driver) = BufferedNatsModelLiveDeltaSink::new(config, limits).unwrap();
        let cancellation = CancellationToken::new();
        let task = tokio::spawn(driver.run(cancellation.child_token()));
        sink.enqueue(event.clone());

        let message = tokio::time::timeout(Duration::from_secs(5), subscriber.next())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            serde_json::from_slice::<ModelLiveTextDelta>(&message.payload).unwrap(),
            event
        );
        cancellation.cancel();
        tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }

    #[test]
    fn wrong_worker_role_is_rejected_at_composition() {
        let authority = Arc::new(EmptyAuthority::default());
        let process_generation =
            ResourceId::from_uuid_v7(ResourceKind::WorkerProcessGeneration, Uuid::now_v7())
                .unwrap();
        let mut manifest = model_manifest(1);
        manifest.worker_role = "capability-worker".to_owned();
        let pools = LocalWorkerPools::new(manifest, process_generation).unwrap();
        assert!(matches!(
            ModelWorkerDriver::new(
                authority,
                Arc::new(UnusedExecutor),
                Arc::new(UuidModelWorkerIdentityFactory),
                pools,
                config(),
            ),
            Err(ModelWorkerDriverError::WrongWorkerManifest)
        ));
    }
}
