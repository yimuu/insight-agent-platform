//! Terminal-only Run admission, owner lease, execution, and Conversation host.
//!
//! This path is intentionally separate from [`crate::RunService`]'s durable
//! coordinator. It owns only process-local active state and the two lightweight
//! backend-neutral storage ports from `insight-durable`; concrete database
//! adapters remain outside the runtime crate.

use std::{
    collections::{hash_map::DefaultHasher, BTreeMap, BTreeSet},
    hash::{Hash, Hasher},
    panic::AssertUnwindSafe,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex, RwLock, Weak,
    },
    time::Duration,
};

use chrono::{DateTime, Utc};
use futures::FutureExt;
use insight_durable::terminal_store::{
    AckContentDeletionJob, AckTerminalArtifactStage, AdmissionConversation, ArchiveOutcome,
    BoundedRetention, ClaimContentDeletionJobs, ClaimTerminalArtifactStages,
    CommitConversationTurn, Conversation, ConversationContent, ConversationContextQuery,
    ConversationMessage, ConversationQuery, ConversationRole, ConversationStore,
    ConversationTurnOutcome, CreateConversationOutcome, FullConversationTurnQuery,
    HeartbeatRuntimeInstance, MessageCursor, MessagePageQuery, NewConversation,
    NewConversationMessage, NewConversationTurn, NewTerminalArtifactStage, NewTerminalRunAdmission,
    NewTerminalRunResult, OwnerLeaseHeartbeat, PrivacyDeleteOutcome, RegisterRuntimeInstance,
    ResolveTerminalArtifactStage, RuntimeOwner, TerminalArtifactSourceKind,
    TerminalArtifactStageDisposition, TerminalArtifactStagingStore, TerminalContentDeletionStore,
    TerminalRunDerivedState, TerminalRunQuery, TerminalRunRequestQuery, TerminalRunStore,
    TerminalRunView, TerminalState, CONVERSATION_ARCHIVED,
    CONVERSATION_NOT_FOUND as STORE_CONVERSATION_NOT_FOUND, CONVERSATION_OWNERSHIP_MISMATCH,
    TERMINAL_RUN_OWNER_LEASE_LOST, TERMINAL_SCOPED_ARTIFACT_MEDIA_TYPE,
};
use insight_durable::PublicationHead;
use insight_engine::{
    artifact_store::WorkerArtifactStore,
    history::types::{RunAttachment, RunLifecycle as PublicRunLifecycle, RunRecord, StopError},
    outcome::{FailureKind, RunFailure, RunOutput},
    repository::{
        REPOSITORY_CONSTRAINT_CONFLICT, REPOSITORY_INTENT_CONFLICT, REPOSITORY_STORAGE_FAILURE,
    },
    run_stream::{
        adapter::durable_run_stream_snapshot_new, DurableRunStreamSnapshot, LiveRunStreamBroker,
        LiveRunStreamSubscriber,
    },
    ContentHash, PersistenceMode, RunId, RuntimeValue,
};
use serde::Serialize;
use serde_json::{json, Value};
use tokio::{
    sync::{watch, Mutex as AsyncMutex, Notify, OwnedMutexGuard, OwnedSemaphorePermit, Semaphore},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::{
    catalog::DeployedAgent,
    run_service::{
        cancel_conversation_streams, ConversationResponseVisibilityGuard,
        ConversationStreamPrivacy, ConversationStreamPrivacyInner, DeployedAgentCatalog,
        ServiceError,
    },
    terminal_execution::{
        execute_terminal_plan, TerminalExecutionConfig, TerminalExecutionOutcome,
        TerminalFailureKind,
    },
};

const DEFAULT_TENANT_ID: &str = "default";
pub(crate) const ADMIN_DEBUG_TENANT_ID: &str = "__admin_debug";
const TERMINAL_ONLY_UNAVAILABLE: &str = "TERMINAL_ONLY_UNAVAILABLE";
const TERMINAL_ONLY_OWNER_LOST: &str = "TERMINAL_ONLY_OWNER_LOST";
const CONVERSATION_INPUT_INVALID: &str = "CONVERSATION_INPUT_INVALID";
const CONVERSATION_CONTENT_UNAVAILABLE: &str = "CONVERSATION_CONTENT_UNAVAILABLE";
const TERMINAL_OUTPUT_UNAVAILABLE: &str = "TERMINAL_OUTPUT_UNAVAILABLE";
const RUN_CAPABILITY_UNAVAILABLE: &str = "RUN_CAPABILITY_UNAVAILABLE";
const SCOPED_ARTIFACT_KIND: &str = "insight_terminal_object";
pub(crate) const SUMMARY_MAX_ENTRIES: usize = 64;
const SUMMARY_PREVIEW_MAX_CHARS: usize = 256;
const CONVERSATION_MUTATION_STRIPES: usize = 64;
const QUALIFICATION_ADMISSION_DELAY_ENV: &str = "INSIGHT_TERMINAL_QUALIFICATION_ADMISSION_DELAY_MS";
const QUALIFICATION_POST_COMMIT_DELAY_ENV: &str =
    "INSIGHT_TERMINAL_QUALIFICATION_POST_COMMIT_DELAY_MS";
const QUALIFICATION_SUMMARY_DELAY_ENV: &str = "INSIGHT_TERMINAL_QUALIFICATION_SUMMARY_DELAY_MS";
const QUALIFICATION_ENABLED_ENV: &str = "INSIGHT_QUALIFICATION_ENABLED";
const MAX_QUALIFICATION_DELAY_MS: u64 = 30_000;
const TERMINAL_MAINTENANCE_CADENCE: Duration = Duration::from_secs(60);
const TERMINAL_MAINTENANCE_BATCH_SIZE: u32 = 100;
const MAX_TERMINAL_MAINTENANCE_BATCHES_PER_CYCLE: usize = 64;
const TERMINAL_MAINTENANCE_CYCLE_BUDGET: Duration = Duration::from_secs(45);

pub trait TerminalOnlyStore:
    TerminalRunStore + ConversationStore + TerminalContentDeletionStore + TerminalArtifactStagingStore
{
}

impl<T> TerminalOnlyStore for T where
    T: TerminalRunStore
        + ConversationStore
        + TerminalContentDeletionStore
        + TerminalArtifactStagingStore
{
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RecoveryCapability {
    Full,
    RestartOnly,
    None,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct RunPersistenceCapability {
    pub persistence_mode: PersistenceMode,
    pub recovery_capability: RecoveryCapability,
    pub event_replay: bool,
}

impl RunPersistenceCapability {
    pub const FULL: Self = Self {
        persistence_mode: PersistenceMode::Full,
        recovery_capability: RecoveryCapability::Full,
        event_replay: true,
    };
    pub const FULL_CONVERSATION: Self = Self {
        persistence_mode: PersistenceMode::Full,
        recovery_capability: RecoveryCapability::RestartOnly,
        event_replay: true,
    };
    pub const TERMINAL_ONLY: Self = Self {
        persistence_mode: PersistenceMode::TerminalOnly,
        recovery_capability: RecoveryCapability::None,
        event_replay: false,
    };
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TerminalOnlyRunConfig {
    pub owner_lease: Duration,
    pub owner_heartbeat: Duration,
    pub terminal_commit_retry: Duration,
    pub run_timeout: Duration,
    pub max_concurrent_runs: usize,
    pub conversations_enabled: bool,
    pub inline_content_max_bytes: usize,
    pub message_page_size_default: u32,
    pub message_page_size_max: u32,
    pub recent_context_messages: u32,
    pub summary_trigger_messages: u32,
    pub summary_trigger_tokens: usize,
    pub run_retention: Duration,
    pub conversation_retention: Duration,
    pub terminal_barrier_timeout: Duration,
    pub outbound_write_timeout: Duration,
}

impl TerminalOnlyRunConfig {
    pub fn validate(self) -> Result<Self, ServiceError> {
        if self.owner_lease.is_zero()
            || self.owner_heartbeat.is_zero()
            || self.owner_heartbeat > self.owner_lease / 3
            || self.terminal_commit_retry.is_zero()
            || self.run_timeout.is_zero()
            || self.run_timeout > self.owner_lease
            || self.max_concurrent_runs == 0
            || !(256..=1024 * 1024).contains(&self.inline_content_max_bytes)
            || !(1..=200).contains(&self.message_page_size_default)
            || !(self.message_page_size_default..=200).contains(&self.message_page_size_max)
            || !(1..=200).contains(&self.recent_context_messages)
            || !(2..=10_000).contains(&self.summary_trigger_messages)
            || self.recent_context_messages > self.summary_trigger_messages
            || !(256..=1_000_000).contains(&self.summary_trigger_tokens)
            || self.run_retention.is_zero()
            || self.conversation_retention.is_zero()
            || self.terminal_barrier_timeout.is_zero()
            || self.outbound_write_timeout.is_zero()
        {
            return Err(ServiceError::new(
                "RUN_SERVICE_CONFIG_INVALID",
                "terminal-only Run configuration is invalid",
            ));
        }
        Ok(self)
    }
}

#[derive(Debug, Clone)]
pub struct ConversationDetachedTurn {
    pub user_message: ConversationMessageView,
    pub run: RunRecord,
    pub replayed: bool,
}

pub struct ConversationAttachedTurn {
    pub user_message: ConversationMessageView,
    pub attached: TerminalAttachedRun,
    pub replayed: bool,
}

/// Exact mutable safety evidence for a public terminal-only admission. Empty
/// authority is reserved for explicit immutable/admin paths.
#[derive(Debug, Clone, Default)]
pub(crate) struct TerminalAdmissionAuthority {
    pub publication_head: Option<PublicationHead>,
    pub mcp_server_fences: BTreeMap<String, u64>,
    pub provider_fences: BTreeMap<String, u64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ConversationMessageView {
    pub message_id: String,
    pub conversation_id: String,
    pub message_order: i64,
    pub role: ConversationRole,
    pub run_id: Option<RunId>,
    pub content: Value,
    pub content_hash: ContentHash,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ConversationMessagePageView {
    pub messages: Vec<ConversationMessageView>,
    pub next_cursor: Option<MessageCursor>,
}

pub struct TerminalAttachedRun {
    pub run_id: String,
    pub response_id: String,
    pub request_id: String,
    pub subscription: TerminalRunSubscription,
    pub live_run_stream: Box<dyn LiveRunStreamSubscriber>,
    pub live_run_stream_broker: Arc<dyn LiveRunStreamBroker>,
    pub terminal_barrier_timeout: Duration,
    pub outbound_write_timeout: Duration,
    pub conversation_privacy: Option<ConversationStreamPrivacy>,
}

/// Opaque privacy fence held while a terminal-only Conversation stream
/// publishes its terminal frame. It deliberately carries no user content.
pub struct ConversationVisibilityGuard {
    _mutation: OwnedMutexGuard<()>,
}

pub struct TerminalRunSubscription {
    run_id: RunId,
    receiver: watch::Receiver<Option<DurableRunStreamSnapshot>>,
    cancellation: CancellationToken,
    terminal: bool,
}

impl TerminalRunSubscription {
    pub fn run_id(&self) -> &RunId {
        &self.run_id
    }

    pub async fn recv_terminal(&mut self) -> Result<DurableRunStreamSnapshot, ServiceError> {
        if self.terminal {
            return self
                .receiver
                .borrow()
                .clone()
                .ok_or_else(terminal_unavailable);
        }
        loop {
            if let Some(snapshot) = self.receiver.borrow().clone() {
                self.terminal = true;
                return Ok(snapshot);
            }
            self.receiver
                .changed()
                .await
                .map_err(|_| terminal_unavailable())?;
        }
    }

    pub fn load_run_stream_snapshot(&self) -> Option<DurableRunStreamSnapshot> {
        self.receiver.borrow().clone()
    }
}

impl Drop for TerminalRunSubscription {
    fn drop(&mut self) {
        if !self.terminal {
            self.cancellation.cancel();
        }
    }
}

#[derive(Debug, Default)]
struct TerminalOnlyMetrics {
    admissions: AtomicU64,
    results: AtomicU64,
    succeeded: AtomicU64,
    failed: AtomicU64,
    cancelled: AtomicU64,
    timed_out: AtomicU64,
    interrupted: AtomicU64,
    commit_retries: AtomicU64,
    lease_expired: AtomicU64,
    conversation_user_messages: AtomicU64,
    conversation_assistant_messages: AtomicU64,
    conversation_context_messages: AtomicU64,
    conversation_context_tokens: AtomicU64,
    summary_succeeded: AtomicU64,
    summary_failed: AtomicU64,
    retention_conversations_deleted: AtomicU64,
    retention_runs_deleted: AtomicU64,
    retention_batches: AtomicU64,
    retention_backlog_pending: AtomicBool,
    content_deletion_batches: AtomicU64,
    content_deletion_backlog_pending: AtomicBool,
    content_deletion_oldest_age_seconds: AtomicU64,
    artifact_staging_batches: AtomicU64,
    artifact_staging_backlog_pending: AtomicBool,
    artifact_staging_oldest_age_seconds: AtomicU64,
}

struct ActiveTerminalRun {
    tenant_id: String,
    conversation_id: Option<String>,
    owner: RuntimeOwner,
    cancellation: CancellationToken,
    terminal: watch::Sender<Option<DurableRunStreamSnapshot>>,
    attachment: RunAttachment,
}

#[derive(Debug, Clone, Copy)]
struct ConfirmedOwnerLease {
    deadline: tokio::time::Instant,
}

impl ConfirmedOwnerLease {
    fn new(deadline: tokio::time::Instant) -> Self {
        Self { deadline }
    }

    fn renew(&mut self, deadline: tokio::time::Instant) {
        self.deadline = deadline;
    }

    fn error_requires_retirement(self, now: tokio::time::Instant, heartbeat: Duration) -> bool {
        self.deadline.saturating_duration_since(now) <= heartbeat
    }

    fn heartbeat_timeout(self, now: tokio::time::Instant, heartbeat: Duration) -> Option<Duration> {
        let before_retirement_threshold = self
            .deadline
            .saturating_duration_since(now)
            .saturating_sub(heartbeat);
        (!before_retirement_threshold.is_zero())
            .then_some(before_retirement_threshold.min(heartbeat))
    }
}

#[derive(Clone)]
pub struct TerminalOnlyRunEngine {
    inner: Arc<TerminalOnlyRunEngineInner>,
}

struct TerminalOnlyRunEngineInner {
    store: Arc<dyn TerminalOnlyStore>,
    agents: DeployedAgentCatalog,
    workers: Arc<insight_engine::worker::WorkerExecutorRegistry>,
    artifact_store: Arc<dyn WorkerArtifactStore>,
    live_run_stream_broker: Arc<dyn LiveRunStreamBroker>,
    owner: RwLock<RuntimeOwner>,
    endpoint: String,
    config: TerminalOnlyRunConfig,
    qualification_admission_delay: Duration,
    qualification_post_commit_delay: Duration,
    qualification_summary_delay: Duration,
    permits: Arc<Semaphore>,
    accepting: AtomicBool,
    lease_healthy: AtomicBool,
    confirmed_owner_lease: Mutex<ConfirmedOwnerLease>,
    owner_recovery: Notify,
    shutdown: CancellationToken,
    active: Mutex<BTreeMap<String, ActiveTerminalRun>>,
    background: AsyncMutex<Vec<JoinHandle<()>>>,
    conversation_mutations: Vec<Arc<AsyncMutex<()>>>,
    conversation_streams: Mutex<BTreeMap<String, Vec<Weak<ConversationStreamPrivacyInner>>>>,
    summary_jobs: Mutex<BTreeSet<String>>,
    deleting_conversations: Mutex<BTreeSet<String>>,
    metrics: TerminalOnlyMetrics,
}

impl std::fmt::Debug for TerminalOnlyRunEngine {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TerminalOnlyRunEngine")
            .field("owner", &self.owner())
            .field("accepting", &self.inner.accepting.load(Ordering::Acquire))
            .finish_non_exhaustive()
    }
}

impl TerminalOnlyRunEngine {
    #[allow(clippy::too_many_arguments)]
    pub async fn start(
        store: Arc<dyn TerminalOnlyStore>,
        agents: DeployedAgentCatalog,
        workers: Arc<insight_engine::worker::WorkerExecutorRegistry>,
        artifact_store: Arc<dyn WorkerArtifactStore>,
        live_run_stream_broker: Arc<dyn LiveRunStreamBroker>,
        endpoint: String,
        config: TerminalOnlyRunConfig,
    ) -> Result<Self, ServiceError> {
        let config = config.validate()?;
        let qualification_enabled = qualification_enabled_from_environment()?;
        let qualification_admission_delay = qualification_delay_from_environment(
            QUALIFICATION_ADMISSION_DELAY_ENV,
            qualification_enabled,
        )?;
        let qualification_post_commit_delay = qualification_delay_from_environment(
            QUALIFICATION_POST_COMMIT_DELAY_ENV,
            qualification_enabled,
        )?;
        let qualification_summary_delay = qualification_delay_from_environment(
            QUALIFICATION_SUMMARY_DELAY_ENV,
            qualification_enabled,
        )?;
        if endpoint.is_empty() || endpoint.chars().any(char::is_control) {
            return Err(terminal_unavailable());
        }
        let now = Utc::now();
        let confirmed_owner_lease =
            ConfirmedOwnerLease::new(tokio::time::Instant::now() + config.owner_lease);
        let owner = RuntimeOwner {
            instance_id: format!("terminal-{}", Uuid::new_v4().simple()),
            owner_epoch: now.timestamp_micros().max(1),
        };
        store
            .register_runtime_instance(RegisterRuntimeInstance {
                owner: owner.clone(),
                endpoint: endpoint.clone(),
                lease_expires_at: now + chrono_duration(config.owner_lease)?,
                started_at: now,
            })
            .await
            .map_err(store_unavailable)?;

        let engine = Self {
            inner: Arc::new(TerminalOnlyRunEngineInner {
                store,
                agents,
                workers,
                artifact_store,
                live_run_stream_broker,
                owner: RwLock::new(owner),
                endpoint,
                config,
                qualification_admission_delay,
                qualification_post_commit_delay,
                qualification_summary_delay,
                permits: Arc::new(Semaphore::new(config.max_concurrent_runs)),
                accepting: AtomicBool::new(true),
                lease_healthy: AtomicBool::new(true),
                confirmed_owner_lease: Mutex::new(confirmed_owner_lease),
                owner_recovery: Notify::new(),
                shutdown: CancellationToken::new(),
                active: Mutex::new(BTreeMap::new()),
                background: AsyncMutex::new(Vec::new()),
                conversation_mutations: (0..CONVERSATION_MUTATION_STRIPES)
                    .map(|_| Arc::new(AsyncMutex::new(())))
                    .collect(),
                conversation_streams: Mutex::new(BTreeMap::new()),
                summary_jobs: Mutex::new(BTreeSet::new()),
                deleting_conversations: Mutex::new(BTreeSet::new()),
                metrics: TerminalOnlyMetrics::default(),
            }),
        };
        engine.spawn_owner_heartbeat().await;
        engine.spawn_retention_pump().await;
        engine.spawn_content_deletion_pump().await;
        engine.spawn_artifact_staging_pump().await;
        Ok(engine)
    }

    pub fn owner(&self) -> RuntimeOwner {
        self.current_owner()
    }

    pub fn active_run_count(&self) -> usize {
        self.inner
            .active
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len()
    }

    #[doc(hidden)]
    pub async fn tracked_background_task_count(&self) -> usize {
        self.inner.background.lock().await.len()
    }

    pub fn is_ready(&self) -> bool {
        self.inner.accepting.load(Ordering::Acquire)
            && self.inner.lease_healthy.load(Ordering::Acquire)
            && !self.inner.shutdown.is_cancelled()
    }

    pub fn conversation_page_limits(&self) -> Option<(u32, u32)> {
        self.inner.config.conversations_enabled.then_some((
            self.inner.config.message_page_size_default,
            self.inner.config.message_page_size_max,
        ))
    }

    pub fn supports_run(&self, agent: &DeployedAgent) -> bool {
        agent.persistence_mode() == PersistenceMode::TerminalOnly
    }

    pub async fn create_detached(
        &self,
        agent_id: &str,
        input: Value,
        request_id: String,
    ) -> Result<RunRecord, ServiceError> {
        self.create_detached_for_tenant(DEFAULT_TENANT_ID, agent_id, input, request_id)
            .await
    }

    pub async fn create_detached_for_tenant(
        &self,
        tenant_id: &str,
        agent_id: &str,
        input: Value,
        request_id: String,
    ) -> Result<RunRecord, ServiceError> {
        let agent = self.terminal_agent(agent_id)?;
        self.create_detached_with_agent(
            tenant_id,
            agent,
            input,
            request_id,
            TerminalAdmissionAuthority::default(),
        )
        .await
    }

    pub(crate) async fn create_detached_for_tenant_with_authority(
        &self,
        tenant_id: &str,
        agent: Arc<DeployedAgent>,
        input: Value,
        request_id: String,
        authority: TerminalAdmissionAuthority,
    ) -> Result<RunRecord, ServiceError> {
        self.create_detached_with_agent(tenant_id, agent, input, request_id, authority)
            .await
    }

    pub async fn create_detached_from_deployment(
        &self,
        tenant_id: &str,
        agent_id: &str,
        deployment_revision_id: &str,
        input: Value,
        request_id: String,
    ) -> Result<RunRecord, ServiceError> {
        let agent = self
            .inner
            .agents
            .get_deployment(deployment_revision_id)
            .filter(|agent| {
                agent.published().metadata().id == agent_id
                    && agent.persistence_mode() == PersistenceMode::TerminalOnly
            })
            .ok_or_else(|| {
                ServiceError::new(
                    "GRAPH_DEPLOYMENT_NOT_FOUND",
                    "graph deployment revision was not found for this agent",
                )
            })?;
        self.create_detached_with_agent(
            tenant_id,
            agent,
            input,
            request_id,
            TerminalAdmissionAuthority::default(),
        )
        .await
    }

    pub(crate) async fn create_admin_debug_detached_from_deployment(
        &self,
        agent_id: &str,
        deployment_revision_id: &str,
        run_id: RunId,
        input: Value,
        request_id: String,
    ) -> Result<RunRecord, ServiceError> {
        let agent = self
            .inner
            .agents
            .get_deployment(deployment_revision_id)
            .filter(|agent| {
                agent.published().metadata().id == agent_id
                    && agent.persistence_mode() == PersistenceMode::TerminalOnly
            })
            .ok_or_else(|| {
                ServiceError::new(
                    "AGENT_DEBUG_SOURCE_NOT_FOUND",
                    "Debug deployment was not found for this Agent",
                )
            })?;
        let normalized = agent
            .published()
            .normalize_input(input)
            .map_err(|_| ServiceError::new("INPUT_INVALID", "agent input is invalid"))?;
        let admission = self
            .admit(
                ADMIN_DEBUG_TENANT_ID.to_owned(),
                request_id,
                agent,
                normalized,
                None,
                RunAttachment::Detached,
                None,
                Some(run_id),
                TerminalAdmissionAuthority::default(),
            )
            .await?;
        Ok(admission.run)
    }

    pub(crate) async fn open_admin_debug_stream(
        &self,
        run_id: &str,
    ) -> Result<TerminalAttachedRun, ServiceError> {
        let run_id = RunId::new(run_id.to_owned())
            .map_err(|_| ServiceError::new("RUN_NOT_FOUND", "Run not found"))?;
        let view = self
            .inner
            .store
            .get_terminal_run(TerminalRunQuery {
                tenant_id: ADMIN_DEBUG_TENANT_ID.to_owned(),
                run_id: run_id.clone(),
                observed_at: Utc::now(),
            })
            .await
            .map_err(store_unavailable)?
            .ok_or_else(|| ServiceError::new("RUN_NOT_FOUND", "Run not found"))?;
        let active = {
            let active = self
                .inner
                .active
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            active
                .get(run_id.as_str())
                .filter(|run| run.tenant_id == ADMIN_DEBUG_TENANT_ID)
                .map(|run| (run.terminal.subscribe(), run.cancellation.clone()))
        };
        if let Some((receiver, cancellation)) = active {
            let live_run_stream = self
                .inner
                .live_run_stream_broker
                .subscribe(run_id.clone())
                .await
                .map_err(|_| terminal_unavailable())?;
            return Ok(TerminalAttachedRun {
                run_id: run_id.as_str().to_owned(),
                response_id: response_id(&run_id),
                request_id: view.admission.request_id.clone(),
                subscription: TerminalRunSubscription {
                    run_id,
                    receiver,
                    cancellation,
                    terminal: false,
                },
                live_run_stream,
                live_run_stream_broker: Arc::clone(&self.inner.live_run_stream_broker),
                terminal_barrier_timeout: self.inner.config.terminal_barrier_timeout,
                outbound_write_timeout: self.inner.config.outbound_write_timeout,
                conversation_privacy: None,
            });
        }
        self.attach_terminal_replay(&view.admission)
            .await?
            .ok_or_else(|| ServiceError::new("RUN_NOT_FOUND", "Run not found"))
    }

    pub(crate) async fn get_admin_debug_run(
        &self,
        run_id: &str,
    ) -> Result<RunRecord, ServiceError> {
        self.get_run(ADMIN_DEBUG_TENANT_ID, run_id).await
    }

    pub(crate) async fn cancel_admin_debug_run(
        &self,
        run_id: &str,
    ) -> Result<RunRecord, ServiceError> {
        self.cancel(ADMIN_DEBUG_TENANT_ID, run_id).await
    }

    async fn create_detached_with_agent(
        &self,
        tenant_id: &str,
        agent: Arc<DeployedAgent>,
        input: Value,
        request_id: String,
        authority: TerminalAdmissionAuthority,
    ) -> Result<RunRecord, ServiceError> {
        let normalized = agent
            .published()
            .normalize_input(input)
            .map_err(|_| ServiceError::new("INPUT_INVALID", "agent input is invalid"))?;
        let admission = self
            .admit(
                tenant_id.to_owned(),
                request_id,
                agent,
                normalized,
                None,
                RunAttachment::Detached,
                None,
                None,
                authority,
            )
            .await?;
        Ok(admission.run)
    }

    pub async fn create_attached(
        &self,
        tenant_id: &str,
        agent_id: &str,
        input: Value,
        request_id: String,
    ) -> Result<TerminalAttachedRun, ServiceError> {
        let agent = self.terminal_agent(agent_id)?;
        self.create_attached_with_authority(
            tenant_id,
            agent,
            input,
            request_id,
            TerminalAdmissionAuthority::default(),
        )
        .await
    }

    pub(crate) async fn create_attached_with_authority(
        &self,
        tenant_id: &str,
        agent: Arc<DeployedAgent>,
        input: Value,
        request_id: String,
        authority: TerminalAdmissionAuthority,
    ) -> Result<TerminalAttachedRun, ServiceError> {
        let normalized = agent
            .published()
            .normalize_input(input)
            .map_err(|_| ServiceError::new("INPUT_INVALID", "agent input is invalid"))?;
        let admission = self
            .admit(
                tenant_id.to_owned(),
                request_id,
                agent,
                normalized,
                None,
                RunAttachment::Attached,
                None,
                None,
                authority,
            )
            .await?;
        admission
            .attached
            .ok_or_else(|| ServiceError::new("RUN_CONFLICT", "attached Run replay is unavailable"))
    }

    pub async fn get_run(&self, tenant_id: &str, run_id: &str) -> Result<RunRecord, ServiceError> {
        self.get_run_for_principal(tenant_id, None, run_id).await
    }

    pub async fn get_run_for_principal(
        &self,
        tenant_id: &str,
        user_id: Option<&str>,
        run_id: &str,
    ) -> Result<RunRecord, ServiceError> {
        let view = self.authorized_run_view(tenant_id, user_id, run_id).await?;
        self.record_from_view(view).await
    }

    async fn authorized_run_view(
        &self,
        tenant_id: &str,
        user_id: Option<&str>,
        run_id: &str,
    ) -> Result<TerminalRunView, ServiceError> {
        let run_id = RunId::new(run_id.to_owned())
            .map_err(|_| ServiceError::new("RUN_NOT_FOUND", "Run not found"))?;
        let view = self
            .inner
            .store
            .get_terminal_run(TerminalRunQuery {
                tenant_id: tenant_id.to_owned(),
                run_id,
                observed_at: Utc::now(),
            })
            .await
            .map_err(store_unavailable)?
            .ok_or_else(|| ServiceError::new("RUN_NOT_FOUND", "Run not found"))?;
        if let Some(conversation) = &view.admission.conversation {
            let user_id =
                user_id.ok_or_else(|| ServiceError::new("RUN_NOT_FOUND", "Run not found"))?;
            if self
                .inner
                .store
                .get_conversation(conversation_query(
                    &conversation.conversation_id,
                    tenant_id,
                    user_id,
                ))
                .await
                .map_err(conversation_store_error)?
                .is_none()
            {
                return Err(ServiceError::new("RUN_NOT_FOUND", "Run not found"));
            }
        }
        Ok(view)
    }

    /// Acquires the exact privacy-delete mutation stripe and then revalidates
    /// the tenant/user Conversation authority for this Run. Holding the
    /// returned content-free guard orders terminal-frame publication before
    /// privacy deletion, or observes deletion and denies publication.
    pub async fn acquire_conversation_visibility_guard(
        &self,
        tenant_id: &str,
        user_id: &str,
        run_id: &str,
    ) -> Result<ConversationVisibilityGuard, ServiceError> {
        self.ensure_conversations_enabled()?;
        let run_id = RunId::new(run_id.to_owned())
            .map_err(|_| ServiceError::new("RUN_NOT_FOUND", "Run not found"))?;
        let initial = self
            .inner
            .store
            .get_terminal_run(TerminalRunQuery {
                tenant_id: tenant_id.to_owned(),
                run_id: run_id.clone(),
                observed_at: Utc::now(),
            })
            .await
            .map_err(store_unavailable)?
            .ok_or_else(|| ServiceError::new("RUN_NOT_FOUND", "Run not found"))?;
        let conversation_id = initial
            .admission
            .conversation
            .as_ref()
            .map(|conversation| conversation.conversation_id.clone())
            .ok_or_else(|| ServiceError::new("RUN_NOT_FOUND", "Run not found"))?;
        let mutation = self
            .conversation_mutation(&conversation_id)
            .lock_owned()
            .await;

        // Re-read after acquiring the same fence used by privacy deletion.
        // The first lookup is only routing data and never authorizes output.
        let current = self
            .inner
            .store
            .get_terminal_run(TerminalRunQuery {
                tenant_id: tenant_id.to_owned(),
                run_id,
                observed_at: Utc::now(),
            })
            .await
            .map_err(store_unavailable)?
            .ok_or_else(|| ServiceError::new("RUN_NOT_FOUND", "Run not found"))?;
        if current
            .admission
            .conversation
            .as_ref()
            .is_none_or(|conversation| conversation.conversation_id != conversation_id)
            || self
                .inner
                .store
                .get_conversation(conversation_query(&conversation_id, tenant_id, user_id))
                .await
                .map_err(conversation_store_error)?
                .is_none()
        {
            return Err(ServiceError::new("RUN_NOT_FOUND", "Run not found"));
        }
        Ok(ConversationVisibilityGuard {
            _mutation: mutation,
        })
    }

    /// Acquires the privacy-delete stripe for a finite Conversation response
    /// and revalidates the exact principal after the lock is held. API bodies
    /// use the returned cancellation handle to count only a frame actively
    /// handed to transport; idle bodies never block privacy deletion.
    pub async fn acquire_conversation_response_visibility(
        &self,
        conversation_id: &str,
        tenant_id: &str,
        user_id: &str,
    ) -> Result<ConversationResponseVisibilityGuard, ServiceError> {
        self.ensure_conversations_enabled()?;
        let _mutation = self
            .conversation_mutation(conversation_id)
            .lock_owned()
            .await;
        if self
            .inner
            .store
            .get_conversation(conversation_query(conversation_id, tenant_id, user_id))
            .await
            .map_err(conversation_store_error)?
            .is_none()
        {
            return Err(ServiceError::new(
                "CONVERSATION_NOT_FOUND",
                "conversation was not found",
            ));
        }
        let privacy = self.register_conversation_stream(conversation_id);
        Ok(ConversationResponseVisibilityGuard::new(privacy))
    }

    /// Returns a response guard only for a Conversation-bound Run. The first
    /// lookup supplies routing data; the existing run guard re-reads all
    /// authority while holding the deletion stripe.
    pub async fn acquire_run_response_visibility(
        &self,
        tenant_id: &str,
        user_id: Option<&str>,
        run_id: &str,
    ) -> Result<Option<ConversationResponseVisibilityGuard>, ServiceError> {
        let run_id_model = RunId::new(run_id.to_owned())
            .map_err(|_| ServiceError::new("RUN_NOT_FOUND", "Run not found"))?;
        let view = self
            .inner
            .store
            .get_terminal_run(TerminalRunQuery {
                tenant_id: tenant_id.to_owned(),
                run_id: run_id_model,
                observed_at: Utc::now(),
            })
            .await
            .map_err(store_unavailable)?
            .ok_or_else(|| ServiceError::new("RUN_NOT_FOUND", "Run not found"))?;
        let Some(conversation) = view.admission.conversation.as_ref() else {
            return Ok(None);
        };
        let user_id = user_id.ok_or_else(|| ServiceError::new("RUN_NOT_FOUND", "Run not found"))?;
        let conversation_id = conversation.conversation_id.clone();
        let _mutation = self
            .conversation_mutation(&conversation_id)
            .lock_owned()
            .await;
        let current = self
            .inner
            .store
            .get_terminal_run(TerminalRunQuery {
                tenant_id: tenant_id.to_owned(),
                run_id: RunId::new(run_id.to_owned())
                    .map_err(|_| ServiceError::new("RUN_NOT_FOUND", "Run not found"))?,
                observed_at: Utc::now(),
            })
            .await
            .map_err(store_unavailable)?
            .ok_or_else(|| ServiceError::new("RUN_NOT_FOUND", "Run not found"))?;
        if current
            .admission
            .conversation
            .as_ref()
            .is_none_or(|binding| binding.conversation_id != conversation_id)
            || self
                .inner
                .store
                .get_conversation(conversation_query(&conversation_id, tenant_id, user_id))
                .await
                .map_err(conversation_store_error)?
                .is_none()
        {
            return Err(ServiceError::new("RUN_NOT_FOUND", "Run not found"));
        }
        let privacy = self.register_conversation_stream(&conversation_id);
        Ok(Some(ConversationResponseVisibilityGuard::new(privacy)))
    }

    pub async fn cancel(&self, tenant_id: &str, run_id: &str) -> Result<RunRecord, ServiceError> {
        self.cancel_for_principal(tenant_id, None, run_id).await
    }

    pub async fn cancel_for_principal(
        &self,
        tenant_id: &str,
        user_id: Option<&str>,
        run_id: &str,
    ) -> Result<RunRecord, ServiceError> {
        self.authorized_run_view(tenant_id, user_id, run_id).await?;
        let cancellation = self
            .inner
            .active
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(run_id)
            .filter(|active| active.tenant_id == tenant_id)
            .map(|active| active.cancellation.clone());
        if let Some(cancellation) = cancellation {
            cancellation.cancel();
        }
        self.get_run_for_principal(tenant_id, user_id, run_id).await
    }

    pub async fn create_conversation(
        &self,
        tenant_id: &str,
        user_id: &str,
        agent_id: &str,
        request_id: &str,
    ) -> Result<Conversation, ServiceError> {
        self.ensure_conversations_enabled()?;
        let agent = self.terminal_agent(agent_id)?;
        validate_request_identity(tenant_id, user_id, request_id)?;
        let conversation_id = deterministic_public_id("conv", &[tenant_id, user_id, request_id]);
        let outcome: CreateConversationOutcome = self
            .inner
            .store
            .create_conversation(NewConversation {
                conversation_id,
                tenant_id: tenant_id.to_owned(),
                user_id: user_id.to_owned(),
                agent_id: agent_id.to_owned(),
                persistence_mode: PersistenceMode::TerminalOnly,
                deployment_revision_id: agent.deployment_revision_id().clone(),
                created_at: Utc::now(),
            })
            .await
            .map_err(conversation_store_error)?;
        Ok(outcome.conversation)
    }

    pub async fn get_conversation(
        &self,
        conversation_id: &str,
        tenant_id: &str,
        user_id: &str,
    ) -> Result<Conversation, ServiceError> {
        self.ensure_conversations_enabled()?;
        self.inner
            .store
            .get_conversation(conversation_query(conversation_id, tenant_id, user_id))
            .await
            .map_err(conversation_store_error)?
            .ok_or_else(|| {
                ServiceError::new("CONVERSATION_NOT_FOUND", "conversation was not found")
            })
    }

    pub async fn list_conversation_messages(
        &self,
        conversation_id: &str,
        tenant_id: &str,
        user_id: &str,
        before: Option<MessageCursor>,
        limit: u32,
    ) -> Result<ConversationMessagePageView, ServiceError> {
        self.ensure_conversations_enabled()?;
        if limit == 0 || limit > self.inner.config.message_page_size_max {
            return Err(ServiceError::new(
                "CONVERSATION_REQUEST_INVALID",
                "conversation message page size is invalid",
            ));
        }
        let page = self
            .inner
            .store
            .page_conversation_messages(MessagePageQuery {
                conversation: conversation_query(conversation_id, tenant_id, user_id),
                before,
                limit,
            })
            .await
            .map_err(conversation_store_error)?
            .ok_or_else(|| {
                ServiceError::new("CONVERSATION_NOT_FOUND", "conversation was not found")
            })?;
        let mut messages = Vec::with_capacity(page.messages.len());
        for message in page.messages {
            messages.push(self.conversation_message_view(message).await?);
        }
        Ok(ConversationMessagePageView {
            messages,
            next_cursor: page.next_cursor,
        })
    }

    pub async fn create_conversation_detached_turn(
        &self,
        conversation_id: &str,
        tenant_id: &str,
        user_id: &str,
        request_id: &str,
        content: Value,
    ) -> Result<ConversationDetachedTurn, ServiceError> {
        self.create_conversation_detached_turn_with_authority(
            conversation_id,
            tenant_id,
            user_id,
            request_id,
            content,
            TerminalAdmissionAuthority::default(),
        )
        .await
    }

    pub(crate) async fn create_conversation_detached_turn_with_authority(
        &self,
        conversation_id: &str,
        tenant_id: &str,
        user_id: &str,
        request_id: &str,
        content: Value,
        authority: TerminalAdmissionAuthority,
    ) -> Result<ConversationDetachedTurn, ServiceError> {
        self.ensure_conversations_enabled()?;
        let conversation = self
            .get_conversation(conversation_id, tenant_id, user_id)
            .await?;
        let admitted = self
            .admit_conversation_turn(
                conversation,
                request_id,
                content,
                RunAttachment::Detached,
                authority,
            )
            .await?;
        Ok(ConversationDetachedTurn {
            user_message: self
                .conversation_message_view(admitted.user_message)
                .await?,
            run: admitted.run,
            replayed: admitted.replayed,
        })
    }

    pub async fn create_conversation_attached_turn(
        &self,
        conversation_id: &str,
        tenant_id: &str,
        user_id: &str,
        request_id: &str,
        content: Value,
    ) -> Result<ConversationAttachedTurn, ServiceError> {
        self.create_conversation_attached_turn_with_authority(
            conversation_id,
            tenant_id,
            user_id,
            request_id,
            content,
            TerminalAdmissionAuthority::default(),
        )
        .await
    }

    pub(crate) async fn create_conversation_attached_turn_with_authority(
        &self,
        conversation_id: &str,
        tenant_id: &str,
        user_id: &str,
        request_id: &str,
        content: Value,
        authority: TerminalAdmissionAuthority,
    ) -> Result<ConversationAttachedTurn, ServiceError> {
        self.ensure_conversations_enabled()?;
        let conversation = self
            .get_conversation(conversation_id, tenant_id, user_id)
            .await?;
        let admitted = self
            .admit_conversation_turn(
                conversation,
                request_id,
                content,
                RunAttachment::Attached,
                authority,
            )
            .await?;
        let attached = admitted.attached.ok_or_else(|| {
            ServiceError::new("RUN_CONFLICT", "attached Run replay is unavailable")
        })?;
        Ok(ConversationAttachedTurn {
            user_message: self
                .conversation_message_view(admitted.user_message)
                .await?,
            attached,
            replayed: admitted.replayed,
        })
    }

    fn register_conversation_stream(&self, conversation_id: &str) -> ConversationStreamPrivacy {
        let privacy = ConversationStreamPrivacy::new();
        let mut streams = self
            .inner
            .conversation_streams
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let streams = streams.entry(conversation_id.to_owned()).or_default();
        streams.retain(|stream| stream.strong_count() > 0);
        streams.push(privacy.downgrade());
        privacy
    }

    pub async fn archive_conversation(
        &self,
        conversation_id: &str,
        tenant_id: &str,
        user_id: &str,
    ) -> Result<Conversation, ServiceError> {
        self.ensure_conversations_enabled()?;
        match self
            .inner
            .store
            .archive_conversation(
                conversation_query(conversation_id, tenant_id, user_id),
                Utc::now(),
            )
            .await
            .map_err(conversation_store_error)?
        {
            ArchiveOutcome::Archived { conversation, .. } => Ok(conversation),
            ArchiveOutcome::NotFound => Err(ServiceError::new(
                "CONVERSATION_NOT_FOUND",
                "conversation was not found",
            )),
        }
    }

    pub async fn delete_conversation(
        &self,
        conversation_id: &str,
        tenant_id: &str,
        user_id: &str,
    ) -> Result<bool, ServiceError> {
        self.ensure_conversations_enabled()?;
        if self
            .inner
            .store
            .get_conversation(conversation_query(conversation_id, tenant_id, user_id))
            .await
            .map_err(conversation_store_error)?
            .is_none()
        {
            return Ok(false);
        }
        tokio::time::timeout(
            self.inner.config.terminal_commit_retry,
            cancel_conversation_streams(&self.inner.conversation_streams, conversation_id),
        )
        .await
        .map_err(|_| terminal_unavailable())?;
        let _mutation = self
            .conversation_mutation(conversation_id)
            .lock_owned()
            .await;
        tokio::time::timeout(
            self.inner.config.terminal_commit_retry,
            cancel_conversation_streams(&self.inner.conversation_streams, conversation_id),
        )
        .await
        .map_err(|_| terminal_unavailable())?;
        if self
            .inner
            .store
            .get_conversation(conversation_query(conversation_id, tenant_id, user_id))
            .await
            .map_err(conversation_store_error)?
            .is_none()
        {
            return Ok(false);
        }
        self.inner
            .deleting_conversations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(conversation_id.to_owned());
        for run in self
            .inner
            .active
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .values()
            .filter(|run| {
                run.tenant_id == tenant_id
                    && run.conversation_id.as_deref() == Some(conversation_id)
            })
        {
            run.cancellation.cancel();
        }
        let quiesced = tokio::time::timeout(self.inner.config.terminal_commit_retry, async {
            loop {
                let active = self
                    .inner
                    .active
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .values()
                    .any(|run| {
                        run.tenant_id == tenant_id
                            && run.conversation_id.as_deref() == Some(conversation_id)
                    });
                let summarizing = self
                    .inner
                    .summary_jobs
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .contains(conversation_id);
                if !active && !summarizing {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .is_ok();
        if !quiesced {
            self.inner
                .deleting_conversations
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .remove(conversation_id);
            return Err(terminal_unavailable());
        }
        let deleted = self
            .inner
            .store
            .delete_conversation(conversation_query(conversation_id, tenant_id, user_id))
            .await
            .map_err(conversation_store_error);
        self.inner
            .deleting_conversations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(conversation_id);
        match deleted? {
            PrivacyDeleteOutcome::Deleted { .. } => {
                // The storage transaction has already enqueued every object
                // reference. Try promptly, but leave the durable job for the
                // background worker when the object store is unavailable.
                let _ = self.run_content_deletion_batch().await;
                Ok(true)
            }
            PrivacyDeleteOutcome::NotFound => Ok(false),
        }
    }

    pub fn capability_unavailable(&self) -> ServiceError {
        ServiceError::new(
            RUN_CAPABILITY_UNAVAILABLE,
            "this run does not persist recovery checkpoints",
        )
    }

    pub fn begin_shutdown(&self) {
        self.inner.accepting.store(false, Ordering::Release);
        self.inner.shutdown.cancel();
        for active in self
            .inner
            .active
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .values()
        {
            active.cancellation.cancel();
        }
    }

    pub async fn shutdown(&self, grace: Duration) -> Result<(), ServiceError> {
        self.begin_shutdown();
        let handles = std::mem::take(&mut *self.inner.background.lock().await);
        tokio::time::timeout(grace, async {
            for handle in handles {
                handle.await.map_err(|_| terminal_unavailable())?;
            }
            while self.active_run_count() != 0
                || !self
                    .inner
                    .summary_jobs
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .is_empty()
            {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            Ok::<(), ServiceError>(())
        })
        .await
        .map_err(|_| ServiceError::new("RUN_SERVICE_STOPPING", "shutdown grace elapsed"))??;
        self.inner
            .store
            .unregister_runtime_instance(self.current_owner())
            .await
            .map_err(store_unavailable)?;
        Ok(())
    }

    pub fn prometheus_metrics(&self) -> String {
        let metrics = &self.inner.metrics;
        format!(
            concat!(
                "terminal_run_active {}\n",
                "terminal_run_admissions_total {}\n",
                "terminal_run_results_total{{terminal_state=\"succeeded\"}} {}\n",
                "terminal_run_results_total{{terminal_state=\"failed\"}} {}\n",
                "terminal_run_results_total{{terminal_state=\"cancelled\"}} {}\n",
                "terminal_run_results_total{{terminal_state=\"timed_out\"}} {}\n",
                "terminal_run_interrupted_total{{reason=\"owner_lost\"}} {}\n",
                "terminal_run_terminal_commit_retries_total {}\n",
                "terminal_runtime_owner_lease_expired_total {}\n",
                "conversation_messages_total{{role=\"user\",persistence_mode=\"terminal_only\"}} {}\n",
                "conversation_messages_total{{role=\"assistant\",persistence_mode=\"terminal_only\"}} {}\n",
                "conversation_context_messages{{persistence_mode=\"terminal_only\"}} {}\n",
                "conversation_context_tokens{{persistence_mode=\"terminal_only\"}} {}\n",
                "conversation_summary_jobs_active{{persistence_mode=\"terminal_only\"}} {}\n",
                "conversation_summary_jobs_total{{result=\"succeeded\",persistence_mode=\"terminal_only\"}} {}\n",
                "conversation_summary_jobs_total{{result=\"failed\",persistence_mode=\"terminal_only\"}} {}\n",
                "conversation_retention_deleted_total{{kind=\"conversation\",persistence_mode=\"terminal_only\"}} {}\n",
                "conversation_retention_deleted_total{{kind=\"run\",persistence_mode=\"terminal_only\"}} {}\n",
                "conversation_retention_batches_total{{persistence_mode=\"terminal_only\"}} {}\n",
                "conversation_retention_backlog_pending{{persistence_mode=\"terminal_only\"}} {}\n",
                "conversation_maintenance_batches_total{{queue=\"content_deletion\",persistence_mode=\"terminal_only\"}} {}\n",
                "conversation_maintenance_batches_total{{queue=\"artifact_staging\",persistence_mode=\"terminal_only\"}} {}\n",
                "conversation_maintenance_backlog_pending{{queue=\"content_deletion\",persistence_mode=\"terminal_only\"}} {}\n",
                "conversation_maintenance_backlog_pending{{queue=\"artifact_staging\",persistence_mode=\"terminal_only\"}} {}\n",
                "conversation_maintenance_oldest_age_seconds{{queue=\"content_deletion\",persistence_mode=\"terminal_only\"}} {}\n",
                "conversation_maintenance_oldest_age_seconds{{queue=\"artifact_staging\",persistence_mode=\"terminal_only\"}} {}\n"
            ),
            self.active_run_count(),
            metrics.admissions.load(Ordering::Relaxed),
            metrics.succeeded.load(Ordering::Relaxed),
            metrics.failed.load(Ordering::Relaxed),
            metrics.cancelled.load(Ordering::Relaxed),
            metrics.timed_out.load(Ordering::Relaxed),
            metrics.interrupted.load(Ordering::Relaxed),
            metrics.commit_retries.load(Ordering::Relaxed),
            metrics.lease_expired.load(Ordering::Relaxed),
            metrics.conversation_user_messages.load(Ordering::Relaxed),
            metrics
                .conversation_assistant_messages
                .load(Ordering::Relaxed),
            metrics
                .conversation_context_messages
                .load(Ordering::Relaxed),
            metrics.conversation_context_tokens.load(Ordering::Relaxed),
            self.inner
                .summary_jobs
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .len(),
            metrics.summary_succeeded.load(Ordering::Relaxed),
            metrics.summary_failed.load(Ordering::Relaxed),
            metrics
                .retention_conversations_deleted
                .load(Ordering::Relaxed),
            metrics.retention_runs_deleted.load(Ordering::Relaxed),
            metrics.retention_batches.load(Ordering::Relaxed),
            u8::from(metrics.retention_backlog_pending.load(Ordering::Relaxed)),
            metrics.content_deletion_batches.load(Ordering::Relaxed),
            metrics.artifact_staging_batches.load(Ordering::Relaxed),
            u8::from(
                metrics
                    .content_deletion_backlog_pending
                    .load(Ordering::Relaxed)
            ),
            u8::from(
                metrics
                    .artifact_staging_backlog_pending
                    .load(Ordering::Relaxed)
            ),
            metrics
                .content_deletion_oldest_age_seconds
                .load(Ordering::Relaxed),
            metrics
                .artifact_staging_oldest_age_seconds
                .load(Ordering::Relaxed),
        )
    }

    async fn admit_conversation_turn(
        &self,
        conversation: Conversation,
        request_id: &str,
        content: Value,
        attachment: RunAttachment,
        authority: TerminalAdmissionAuthority,
    ) -> Result<ConversationAdmission, ServiceError> {
        let _mutation = self
            .conversation_mutation(&conversation.conversation_id)
            .lock_owned()
            .await;
        let conversation = self
            .inner
            .store
            .get_conversation(conversation_query(
                &conversation.conversation_id,
                &conversation.tenant_id,
                &conversation.user_id,
            ))
            .await
            .map_err(conversation_store_error)?
            .ok_or_else(|| {
                ServiceError::new("CONVERSATION_NOT_FOUND", "conversation was not found")
            })?;
        if self
            .inner
            .deleting_conversations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .contains(&conversation.conversation_id)
        {
            return Err(ServiceError::new(
                "CONVERSATION_NOT_FOUND",
                "conversation was not found",
            ));
        }
        if conversation.archived_at.is_some() {
            return Err(ServiceError::new(
                "CONVERSATION_ARCHIVED",
                "conversation is archived",
            ));
        }
        let agent = self
            .inner
            .agents
            .get_deployment(conversation.deployment_revision_id.as_str())
            .filter(|agent| {
                agent.published().metadata().id == conversation.agent_id
                    && agent.persistence_mode() == PersistenceMode::TerminalOnly
                    && conversation.persistence_mode == PersistenceMode::TerminalOnly
            })
            .ok_or_else(|| {
                ServiceError::new(
                    TERMINAL_ONLY_UNAVAILABLE,
                    "Conversation deployment is unavailable",
                )
            })?;
        let message_hash = canonical_hash(&content)?;
        if let Some(outcome) = self
            .inner
            .store
            .get_terminal_conversation_turn(FullConversationTurnQuery {
                tenant_id: conversation.tenant_id.clone(),
                request_id: request_id.to_owned(),
                conversation_id: conversation.conversation_id.clone(),
                user_id: conversation.user_id.clone(),
            })
            .await
            .map_err(conversation_store_error)?
        {
            if outcome.admission.agent_id != agent.published().metadata().id
                || outcome.admission.definition_revision_id
                    != *agent.published().definition_revision_id()
                || outcome.admission.deployment_revision_id != *agent.deployment_revision_id()
                || outcome.user_message.content_hash != message_hash
            {
                return Err(ServiceError::new(
                    "CONVERSATION_CONFLICT",
                    "conversation request conflicts with existing intent",
                ));
            }
            let mut attached = if attachment == RunAttachment::Attached {
                self.attach_active(&outcome.admission).await?
            } else {
                None
            };
            if let Some(attached) = &mut attached {
                attached.conversation_privacy =
                    Some(self.register_conversation_stream(&conversation.conversation_id));
            }
            let run = self
                .get_run_for_principal(
                    &outcome.admission.tenant_id,
                    Some(&conversation.user_id),
                    outcome.admission.run_id.as_str(),
                )
                .await?;
            return Ok(ConversationAdmission {
                user_message: outcome.user_message,
                run,
                attached,
                replayed: true,
            });
        }
        let slot = self.reserve_slot()?;
        let context_query = conversation_query(
            &conversation.conversation_id,
            &conversation.tenant_id,
            &conversation.user_id,
        );
        let context = self
            .inner
            .store
            .load_conversation_context(ConversationContextQuery {
                conversation: context_query.clone(),
                recent_message_limit: self.inner.config.recent_context_messages,
            })
            .await
            .map_err(conversation_store_error)?;
        let context_value = self.context_value(context, &context_query).await?;
        let input_value = json!({
            "message": content,
            "conversation_context": context_value,
        });
        let normalized = agent
            .published()
            .normalize_input(input_value)
            .map_err(|_| {
                ServiceError::new(
                    CONVERSATION_INPUT_INVALID,
                    "agent does not accept the terminal Conversation input contract",
                )
            })?;
        let selected_context_hash = canonical_hash(&context_value)?;
        let run_id = RunId::random();
        let user_message_id = deterministic_public_id(
            "msg",
            &[
                &conversation.conversation_id,
                &conversation.user_id,
                request_id,
                "user",
            ],
        );
        let message = self
            .new_message(
                &conversation.tenant_id,
                TerminalArtifactSourceKind::UserMessage,
                user_message_id.clone(),
                content,
                Utc::now(),
            )
            .await?;
        let command = NewConversationTurn {
            user_id: conversation.user_id.clone(),
            message,
            admission: self.new_admission(
                run_id,
                conversation.tenant_id.clone(),
                request_id.to_owned(),
                agent.as_ref(),
                &normalized,
                Some(AdmissionConversation {
                    conversation_id: conversation.conversation_id.clone(),
                    user_message_id,
                }),
                Some(selected_context_hash),
                slot.owner.clone(),
                authority,
            )?,
        };
        let candidate_run_id = command.admission.run_id.clone();
        let outcome: ConversationTurnOutcome = match persist_admission_with_recovery(|| {
            self.inner.store.create_conversation_turn(command.clone())
        })
        .await
        {
            Ok(outcome) => outcome,
            Err(failure) => {
                if failure.ambiguous {
                    self.lose_owner(&slot.owner);
                }
                return Err(conversation_store_error(failure.error));
            }
        };
        let owns_admission = outcome.admission.run_id == candidate_run_id;
        if !owns_admission {
            drop(slot.permit);
            let mut attached = if attachment == RunAttachment::Attached {
                self.attach_active(&outcome.admission).await?
            } else {
                None
            };
            if let Some(attached) = &mut attached {
                attached.conversation_privacy =
                    Some(self.register_conversation_stream(&conversation.conversation_id));
            }
            let run = self
                .get_run_for_principal(
                    &outcome.admission.tenant_id,
                    Some(&conversation.user_id),
                    outcome.admission.run_id.as_str(),
                )
                .await?;
            return Ok(ConversationAdmission {
                user_message: outcome.user_message,
                run,
                attached,
                replayed: true,
            });
        }
        self.inner
            .metrics
            .conversation_user_messages
            .fetch_add(1, Ordering::Relaxed);
        let stream_conversation_id = conversation.conversation_id.clone();
        let pending = PendingConversationCommit {
            assistant_message_id: deterministic_public_id(
                "msg",
                &[
                    &conversation.conversation_id,
                    &conversation.user_id,
                    request_id,
                    "assistant",
                ],
            ),
            conversation,
        };
        let mut admitted = self
            .launch_admitted(
                outcome.admission,
                agent,
                normalized,
                attachment,
                Some(pending),
                slot.permit,
            )
            .await?;
        if let Some(attached) = &mut admitted.attached {
            attached.conversation_privacy =
                Some(self.register_conversation_stream(&stream_conversation_id));
        }
        Ok(ConversationAdmission {
            user_message: outcome.user_message,
            run: admitted.run,
            attached: admitted.attached,
            replayed: false,
        })
    }

    #[allow(clippy::too_many_arguments)]
    async fn admit(
        &self,
        tenant_id: String,
        request_id: String,
        agent: Arc<DeployedAgent>,
        normalized: RuntimeValue,
        conversation: Option<AdmissionConversation>,
        attachment: RunAttachment,
        pending_conversation: Option<PendingConversationCommit>,
        run_id_override: Option<RunId>,
        authority: TerminalAdmissionAuthority,
    ) -> Result<AdmittedRun, ServiceError> {
        let expected_input_hash = canonical_hash(normalized.value())?;
        if let Some(view) = self
            .inner
            .store
            .get_terminal_run_by_request(TerminalRunRequestQuery {
                tenant_id: tenant_id.clone(),
                request_id: request_id.clone(),
                observed_at: Utc::now(),
            })
            .await
            .map_err(run_store_error)?
        {
            let expected_conversation_id = conversation
                .as_ref()
                .map(|conversation| conversation.conversation_id.as_str());
            if view.admission.agent_id != agent.published().metadata().id
                || view.admission.definition_revision_id
                    != *agent.published().definition_revision_id()
                || view.admission.deployment_revision_id != *agent.deployment_revision_id()
                || view
                    .admission
                    .conversation
                    .as_ref()
                    .map(|conversation| conversation.conversation_id.as_str())
                    != expected_conversation_id
                || view.admission.input_hash != expected_input_hash
                || view.admission.selected_context_hash.is_some()
            {
                return Err(ServiceError::new(
                    "RUN_CONFLICT",
                    "run request conflicts with existing intent",
                ));
            }
            let attached = if attachment == RunAttachment::Attached {
                self.attach_active(&view.admission).await?
            } else {
                None
            };
            let run = self
                .get_run(&view.admission.tenant_id, view.admission.run_id.as_str())
                .await?;
            return Ok(AdmittedRun { run, attached });
        }
        let slot = self.reserve_slot()?;
        let command = self.new_admission(
            run_id_override.unwrap_or_else(RunId::random),
            tenant_id,
            request_id,
            agent.as_ref(),
            &normalized,
            conversation,
            None,
            slot.owner.clone(),
            authority,
        )?;
        let candidate_run_id = command.run_id.clone();
        let outcome = match persist_admission_with_recovery(|| {
            self.inner.store.admit_terminal_run(command.clone())
        })
        .await
        {
            Ok(outcome) => outcome,
            Err(failure) => {
                if failure.ambiguous {
                    self.lose_owner(&slot.owner);
                }
                return Err(run_store_error(failure.error));
            }
        };
        let owns_admission = outcome.admission.run_id == candidate_run_id;
        if !owns_admission {
            drop(slot.permit);
            let attached = if attachment == RunAttachment::Attached {
                self.attach_active(&outcome.admission).await?
            } else {
                None
            };
            let run = self
                .get_run(
                    &outcome.admission.tenant_id,
                    outcome.admission.run_id.as_str(),
                )
                .await?;
            return Ok(AdmittedRun { run, attached });
        }
        self.launch_admitted(
            outcome.admission,
            agent,
            normalized,
            attachment,
            pending_conversation,
            slot.permit,
        )
        .await
    }

    async fn attach_active(
        &self,
        admission: &insight_durable::terminal_store::TerminalRunAdmission,
    ) -> Result<Option<TerminalAttachedRun>, ServiceError> {
        let active = {
            let active = self
                .inner
                .active
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            active
                .get(admission.run_id.as_str())
                .filter(|run| {
                    run.tenant_id == admission.tenant_id
                        && run.attachment == RunAttachment::Attached
                })
                .map(|run| (run.terminal.subscribe(), run.cancellation.clone()))
        };
        let Some((receiver, cancellation)) = active else {
            return self.attach_terminal_replay(admission).await;
        };
        let live_run_stream = self
            .inner
            .live_run_stream_broker
            .subscribe(admission.run_id.clone())
            .await
            .map_err(|_| terminal_unavailable())?;
        Ok(Some(TerminalAttachedRun {
            run_id: admission.run_id.as_str().to_owned(),
            response_id: response_id(&admission.run_id),
            request_id: admission.request_id.clone(),
            subscription: TerminalRunSubscription {
                run_id: admission.run_id.clone(),
                receiver,
                cancellation,
                terminal: false,
            },
            live_run_stream,
            live_run_stream_broker: Arc::clone(&self.inner.live_run_stream_broker),
            terminal_barrier_timeout: self.inner.config.terminal_barrier_timeout,
            outbound_write_timeout: self.inner.config.outbound_write_timeout,
            conversation_privacy: None,
        }))
    }

    async fn attach_terminal_replay(
        &self,
        admission: &insight_durable::terminal_store::TerminalRunAdmission,
    ) -> Result<Option<TerminalAttachedRun>, ServiceError> {
        let Some(view) = self
            .inner
            .store
            .get_terminal_run(TerminalRunQuery {
                tenant_id: admission.tenant_id.clone(),
                run_id: admission.run_id.clone(),
                observed_at: Utc::now(),
            })
            .await
            .map_err(store_unavailable)?
        else {
            return Ok(None);
        };
        let outcome = match view.state {
            TerminalRunDerivedState::Active => return Ok(None),
            TerminalRunDerivedState::Interrupted => TerminalExecutionOutcome::Failed {
                failure_kind: TerminalFailureKind::Infrastructure,
                error_code: "RUN_INTERRUPTED".to_owned(),
                safe_message: None,
                usage: None,
                tool_results: Vec::new(),
            },
            TerminalRunDerivedState::Succeeded => {
                let result = view.result.as_ref().ok_or_else(terminal_unavailable)?;
                let output = match result.output_ref.as_deref() {
                    Some(reference) => self.read_artifact_value(reference).await?,
                    None => Value::Null,
                };
                TerminalExecutionOutcome::Succeeded {
                    output,
                    usage: result.usage_json.clone(),
                    tool_results: result.tool_results.clone(),
                }
            }
            TerminalRunDerivedState::Failed => {
                let result = view.result.as_ref().ok_or_else(terminal_unavailable)?;
                TerminalExecutionOutcome::Failed {
                    failure_kind: TerminalFailureKind::Operation,
                    error_code: result
                        .error_code
                        .clone()
                        .unwrap_or_else(|| "RUN_FAILED".to_owned()),
                    safe_message: None,
                    usage: result.usage_json.clone(),
                    tool_results: result.tool_results.clone(),
                }
            }
            TerminalRunDerivedState::Cancelled => TerminalExecutionOutcome::Cancelled {
                tool_results: view
                    .result
                    .as_ref()
                    .ok_or_else(terminal_unavailable)?
                    .tool_results
                    .clone(),
            },
            TerminalRunDerivedState::TimedOut => TerminalExecutionOutcome::TimedOut {
                tool_results: view
                    .result
                    .as_ref()
                    .ok_or_else(terminal_unavailable)?
                    .tool_results
                    .clone(),
            },
        };
        let snapshot = terminal_snapshot(&admission.run_id, &outcome)?;
        let (_terminal, receiver) = watch::channel(Some(snapshot));
        let live_run_stream = self
            .inner
            .live_run_stream_broker
            .subscribe(admission.run_id.clone())
            .await
            .map_err(|_| terminal_unavailable())?;
        let _ = self
            .inner
            .live_run_stream_broker
            .close_run(&admission.run_id);
        Ok(Some(TerminalAttachedRun {
            run_id: admission.run_id.as_str().to_owned(),
            response_id: response_id(&admission.run_id),
            request_id: admission.request_id.clone(),
            subscription: TerminalRunSubscription {
                run_id: admission.run_id.clone(),
                receiver,
                cancellation: CancellationToken::new(),
                terminal: true,
            },
            live_run_stream,
            live_run_stream_broker: Arc::clone(&self.inner.live_run_stream_broker),
            terminal_barrier_timeout: self.inner.config.terminal_barrier_timeout,
            outbound_write_timeout: self.inner.config.outbound_write_timeout,
            conversation_privacy: None,
        }))
    }

    async fn launch_admitted(
        &self,
        admission: insight_durable::terminal_store::TerminalRunAdmission,
        agent: Arc<DeployedAgent>,
        normalized: RuntimeValue,
        attachment: RunAttachment,
        pending_conversation: Option<PendingConversationCommit>,
        permit: OwnedSemaphorePermit,
    ) -> Result<AdmittedRun, ServiceError> {
        self.inner
            .metrics
            .admissions
            .fetch_add(1, Ordering::Relaxed);
        let cancellation = self.inner.shutdown.child_token();
        let (terminal, receiver) = watch::channel(None);
        let run_owner = admission.owner.clone();
        self.inner
            .active
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(
                admission.run_id.as_str().to_owned(),
                ActiveTerminalRun {
                    tenant_id: admission.tenant_id.clone(),
                    conversation_id: admission
                        .conversation
                        .as_ref()
                        .map(|conversation| conversation.conversation_id.clone()),
                    owner: run_owner.clone(),
                    cancellation: cancellation.clone(),
                    terminal: terminal.clone(),
                    attachment,
                },
            );
        let response_id = response_id(&admission.run_id);
        let run = record_from_admission(&admission, attachment);
        let engine = self.clone();
        let run_id = admission.run_id.clone();
        let request_id = admission.request_id.clone();
        let attached_cancellation = cancellation.clone();
        let live_run_stream_broker = Arc::clone(&self.inner.live_run_stream_broker);
        let cleanup = ActiveRunCleanup {
            inner: Arc::downgrade(&self.inner),
            run_id: admission.run_id.clone(),
        };
        let _task = tokio::spawn(async move {
            let _permit = permit;
            let _cleanup = cleanup;
            let execution = AssertUnwindSafe(engine.execute_and_commit(
                admission,
                agent,
                normalized,
                pending_conversation,
                cancellation,
                terminal,
            ))
            .catch_unwind()
            .await;
            if !matches!(execution, Ok(Ok(()))) {
                engine.lose_owner(&run_owner);
            }
        });
        // Starting execution before attaching is deliberate. A transient
        // broker subscription failure may fail this request, but it can never
        // strand the already-durable admission in Active under a healthy
        // owner: execution still reaches a fenced terminal commit.
        let live_run_stream = if attachment == RunAttachment::Attached {
            Some(
                self.inner
                    .live_run_stream_broker
                    .subscribe(run_id.clone())
                    .await
                    .map_err(|_| terminal_unavailable())?,
            )
        } else {
            None
        };
        let attached = live_run_stream.map(|live_run_stream| TerminalAttachedRun {
            run_id: run_id.as_str().to_owned(),
            response_id,
            request_id,
            subscription: TerminalRunSubscription {
                run_id,
                receiver,
                cancellation: attached_cancellation,
                terminal: false,
            },
            live_run_stream,
            live_run_stream_broker,
            terminal_barrier_timeout: self.inner.config.terminal_barrier_timeout,
            outbound_write_timeout: self.inner.config.outbound_write_timeout,
            conversation_privacy: None,
        });
        Ok(AdmittedRun { run, attached })
    }

    async fn execute_and_commit(
        &self,
        admission: insight_durable::terminal_store::TerminalRunAdmission,
        agent: Arc<DeployedAgent>,
        normalized: RuntimeValue,
        pending_conversation: Option<PendingConversationCommit>,
        cancellation: CancellationToken,
        terminal: watch::Sender<Option<DurableRunStreamSnapshot>>,
    ) -> Result<(), ServiceError> {
        // These delays are deliberately private qualification failpoints.
        // Production defaults are zero; the Helm Gate C harness temporarily
        // enables them to kill at otherwise unobservable transaction edges.
        qualification_delay_or_cancellation(
            self.inner.qualification_admission_delay,
            &cancellation,
        )
        .await;
        let started_at = Utc::now();
        // Keep a non-zero duration so the executor can publish a durable
        // TimedOut result even when admission/qualification consumed the
        // entire frozen budget.
        let remaining_budget = remaining_terminal_execution_budget(
            self.inner.config.run_timeout,
            agent.persistence_policy().execution_budget_ms(),
            admission.accepted_at,
            Utc::now(),
        );
        let execution_config = match TerminalExecutionConfig::new(remaining_budget) {
            Ok(config) => config,
            Err(_) => {
                return Err(terminal_unavailable());
            }
        };
        let mut outcome = match execute_terminal_plan(
            agent,
            Arc::clone(&self.inner.workers),
            admission.run_id.clone(),
            response_id(&admission.run_id),
            normalized,
            cancellation.clone(),
            execution_config,
            Arc::clone(&self.inner.live_run_stream_broker),
        )
        .await
        {
            Ok(outcome) => outcome,
            Err(error) => TerminalExecutionOutcome::Failed {
                failure_kind: error.kind(),
                error_code: error.code().to_owned(),
                safe_message: None,
                usage: None,
                tool_results: Vec::new(),
            },
        };
        let (output_ref, output_hash) = match outcome.output() {
            Some(output) => match self
                .put_scoped_artifact_value(
                    &admission.tenant_id,
                    TerminalArtifactSourceKind::RunOutput,
                    admission.run_id.as_str(),
                    format!("run-output:{}", admission.run_id.as_str()),
                    output,
                )
                .await
            {
                Ok((reference, hash)) => (Some(reference), Some(hash)),
                Err(_) => {
                    let tool_results = outcome.tool_results().to_vec();
                    outcome = TerminalExecutionOutcome::Failed {
                        failure_kind: TerminalFailureKind::Infrastructure,
                        error_code: TERMINAL_OUTPUT_UNAVAILABLE.to_owned(),
                        safe_message: None,
                        usage: None,
                        tool_results,
                    };
                    (None, None)
                }
            },
            None => (None, None),
        };
        let terminal_at = Utc::now();
        let command = NewTerminalRunResult {
            run_id: admission.run_id.clone(),
            owner: admission.owner.clone(),
            terminal_state: terminal_state(&outcome),
            response_id: response_id(&admission.run_id),
            output_ref,
            output_hash,
            error_code: outcome.error_code().map(str::to_owned),
            usage_json: outcome.usage().cloned(),
            tool_results: outcome.tool_results().to_vec(),
            started_at,
            terminal_at,
        };
        let committed = self
            .commit_with_retry(
                command,
                &admission.tenant_id,
                pending_conversation.as_ref(),
                outcome.output().cloned(),
            )
            .await;
        let result = committed?;
        self.record_terminal_metric(result.terminal_state);
        let snapshot = terminal_snapshot(&admission.run_id, &outcome).ok();
        qualification_delay_or_cancellation(
            self.inner.qualification_post_commit_delay,
            &cancellation,
        )
        .await;
        let _ = self
            .inner
            .live_run_stream_broker
            .close_run(&admission.run_id);
        if let Some(snapshot) = snapshot {
            let _ = terminal.send(Some(snapshot));
        }
        if let Some(pending) = pending_conversation {
            self.maybe_schedule_summary(pending.conversation).await;
        }
        Ok(())
    }

    async fn commit_with_retry(
        &self,
        command: NewTerminalRunResult,
        tenant_id: &str,
        conversation: Option<&PendingConversationCommit>,
        assistant_output: Option<Value>,
    ) -> Result<insight_durable::terminal_store::TerminalRunResult, ServiceError> {
        let assistant = match conversation {
            Some(conversation) => match self
                .new_message(
                    tenant_id,
                    TerminalArtifactSourceKind::AssistantMessage,
                    conversation.assistant_message_id.clone(),
                    assistant_output.unwrap_or(Value::Null),
                    command.terminal_at,
                )
                .await
            {
                Ok(message) => Some(message),
                Err(error) => {
                    self.lose_owner(&command.owner);
                    return Err(error);
                }
            },
            None => None,
        };
        let deadline = tokio::time::Instant::now() + self.inner.config.terminal_commit_retry;
        loop {
            let committed = match assistant.as_ref() {
                Some(assistant) => self
                    .inner
                    .store
                    .commit_conversation_turn(CommitConversationTurn {
                        result: command.clone(),
                        assistant_message: assistant.clone(),
                    })
                    .await
                    .map(|outcome| {
                        self.inner
                            .metrics
                            .conversation_assistant_messages
                            .fetch_add(u64::from(!outcome.replayed), Ordering::Relaxed);
                        outcome.result
                    }),
                None => self
                    .inner
                    .store
                    .commit_terminal_result(command.clone())
                    .await
                    .map(|outcome| outcome.result),
            };
            match committed {
                Ok(result) => return Ok(result),
                Err(error) if error.code() == TERMINAL_RUN_OWNER_LEASE_LOST => {
                    self.lose_owner(&command.owner);
                    return Err(ServiceError::new(
                        TERMINAL_ONLY_OWNER_LOST,
                        "terminal runtime owner lease was lost",
                    ));
                }
                Err(_) if tokio::time::Instant::now() < deadline => {
                    self.inner
                        .metrics
                        .commit_retries
                        .fetch_add(1, Ordering::Relaxed);
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                Err(error) => {
                    // A durable admission cannot remain Active while its owner
                    // continues to advertise a healthy lease after all
                    // terminal commit attempts fail. Retiring this owner makes
                    // every such admission derive Interrupted and fences late
                    // commits before a fresh owner starts accepting.
                    self.lose_owner(&command.owner);
                    return Err(store_unavailable(error));
                }
            }
        }
    }

    async fn record_from_view(&self, view: TerminalRunView) -> Result<RunRecord, ServiceError> {
        let attachment = self
            .inner
            .active
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(view.admission.run_id.as_str())
            .map_or(RunAttachment::Detached, |active| active.attachment);
        let response_id = view.result.as_ref().map_or_else(
            || response_id(&view.admission.run_id),
            |value| value.response_id.clone(),
        );
        let lifecycle = match view.state {
            TerminalRunDerivedState::Active => PublicRunLifecycle::Running,
            TerminalRunDerivedState::Interrupted => PublicRunLifecycle::Interrupted {
                error: StopError {
                    code: "RUN_INTERRUPTED".to_owned(),
                    message: "terminal-only runtime owner is no longer active".to_owned(),
                },
            },
            TerminalRunDerivedState::Succeeded => {
                let result = view.result.as_ref().ok_or_else(terminal_unavailable)?;
                let output = match result.output_ref.as_deref() {
                    Some(reference) => self.read_artifact_value(reference).await?,
                    None => Value::Null,
                };
                PublicRunLifecycle::Completed {
                    output: public_output(output),
                }
            }
            TerminalRunDerivedState::Failed => {
                let result = view.result.as_ref().ok_or_else(terminal_unavailable)?;
                PublicRunLifecycle::Failed {
                    error: RunFailure {
                        kind: FailureKind::Operation,
                        code: result
                            .error_code
                            .clone()
                            .unwrap_or_else(|| "RUN_FAILED".to_owned()),
                        message: "terminal-only Run failed".to_owned(),
                    },
                }
            }
            TerminalRunDerivedState::Cancelled => PublicRunLifecycle::Cancelled {
                error: StopError {
                    code: "RUN_CANCELLED".to_owned(),
                    message: "terminal-only Run was cancelled".to_owned(),
                },
            },
            TerminalRunDerivedState::TimedOut => PublicRunLifecycle::Failed {
                error: RunFailure {
                    kind: FailureKind::Timeout,
                    code: "RUN_TIMEOUT".to_owned(),
                    message: "terminal-only Run timed out".to_owned(),
                },
            },
        };
        let started_at = view.result.as_ref().map(|result| result.started_at);
        let ended_at = view.result.as_ref().map(|result| result.terminal_at);
        let updated_at = ended_at.unwrap_or(view.admission.accepted_at);
        Ok(RunRecord {
            run_id: view.admission.run_id.as_str().to_owned(),
            response_id,
            projection_version: 0,
            request_id: view.admission.request_id,
            agent_id: view.admission.agent_id,
            agent_version: view.admission.deployment_revision_id.as_str().to_owned(),
            attachment,
            lifecycle,
            started_at,
            ended_at,
            updated_at,
            input_summary: json!({
                "content_hash": view.admission.input_hash.as_str(),
                "terminal_only": true,
            }),
        })
    }

    fn terminal_agent(&self, agent_id: &str) -> Result<Arc<DeployedAgent>, ServiceError> {
        let agent = self
            .inner
            .agents
            .get(agent_id)
            .ok_or_else(|| ServiceError::new("AGENT_NOT_FOUND", "agent not found"))?;
        if agent.persistence_mode() != PersistenceMode::TerminalOnly {
            return Err(ServiceError::new(
                TERMINAL_ONLY_UNAVAILABLE,
                "deployment does not use terminal-only persistence",
            ));
        }
        Ok(agent)
    }

    fn ensure_conversations_enabled(&self) -> Result<(), ServiceError> {
        self.inner
            .config
            .conversations_enabled
            .then_some(())
            .ok_or_else(|| {
                ServiceError::new("CONVERSATION_UNAVAILABLE", "Conversation API is disabled")
            })
    }

    fn reserve_slot(&self) -> Result<AdmissionSlot, ServiceError> {
        if !self.inner.accepting.load(Ordering::Acquire)
            || !self.inner.lease_healthy.load(Ordering::Acquire)
        {
            return Err(ServiceError::new(
                "RUN_SERVICE_STOPPING",
                "terminal-only Run service is not accepting work",
            ));
        }
        let owner = self.current_owner();
        let permit = Arc::clone(&self.inner.permits)
            .try_acquire_owned()
            .map_err(|_| {
                ServiceError::new("RUN_CAPACITY_EXCEEDED", "active Run capacity is exhausted")
            })?;
        if !self.inner.accepting.load(Ordering::Acquire)
            || !self.inner.lease_healthy.load(Ordering::Acquire)
            || self.current_owner() != owner
        {
            drop(permit);
            return Err(ServiceError::new(
                "RUN_SERVICE_STOPPING",
                "terminal-only Run service is not accepting work",
            ));
        }
        Ok(AdmissionSlot { owner, permit })
    }

    #[allow(clippy::too_many_arguments)]
    fn new_admission(
        &self,
        run_id: RunId,
        tenant_id: String,
        request_id: String,
        agent: &DeployedAgent,
        input: &RuntimeValue,
        conversation: Option<AdmissionConversation>,
        selected_context_hash: Option<ContentHash>,
        owner: RuntimeOwner,
        authority: TerminalAdmissionAuthority,
    ) -> Result<NewTerminalRunAdmission, ServiceError> {
        if !valid_request_identity_component(&tenant_id)
            || !valid_request_identity_component(&request_id)
        {
            return Err(ServiceError::new(
                "INPUT_INVALID",
                "request identity is invalid",
            ));
        }
        Ok(NewTerminalRunAdmission {
            run_id,
            tenant_id,
            request_id,
            agent_id: agent.published().metadata().id.clone(),
            definition_revision_id: agent.published().definition_revision_id().clone(),
            deployment_revision_id: agent.deployment_revision_id().clone(),
            conversation,
            input_ref: None,
            input_hash: canonical_hash(input.value())?,
            selected_context_hash,
            expected_publication_head: authority.publication_head,
            expected_mcp_server_fences: authority.mcp_server_fences,
            expected_provider_fences: authority.provider_fences,
            owner,
            accepted_at: Utc::now(),
        })
    }

    async fn new_message(
        &self,
        tenant_id: &str,
        source_kind: TerminalArtifactSourceKind,
        message_id: String,
        value: Value,
        created_at: DateTime<Utc>,
    ) -> Result<NewConversationMessage, ServiceError> {
        let bytes = serde_jcs::to_vec(&value)
            .map_err(|_| ServiceError::new("INPUT_INVALID", "message content is invalid"))?;
        let content_hash = ContentHash::from_bytes(&bytes);
        let content = if bytes.len() <= self.inner.config.inline_content_max_bytes {
            ConversationContent::Inline(value)
        } else {
            let (reference, _) = self
                .put_scoped_artifact_value(
                    tenant_id,
                    source_kind,
                    &message_id,
                    format!("message:{message_id}"),
                    &value,
                )
                .await?;
            ConversationContent::Ref(reference)
        };
        Ok(NewConversationMessage {
            message_id,
            content,
            content_hash,
            created_at,
        })
    }

    async fn context_value(
        &self,
        context: Option<insight_durable::terminal_store::ConversationContext>,
        conversation: &ConversationQuery,
    ) -> Result<Value, ServiceError> {
        let Some(mut context) = context else {
            self.inner
                .metrics
                .conversation_context_messages
                .store(0, Ordering::Relaxed);
            self.inner
                .metrics
                .conversation_context_tokens
                .store(0, Ordering::Relaxed);
            return Ok(json!({"summary": null, "messages": []}));
        };
        let summary = match context.summary {
            Some(summary) => match self.read_artifact_value(&summary.summary_ref).await {
                Ok(value) => Some(value),
                Err(_) => {
                    // Summaries are an optimization, never an availability
                    // dependency for a new turn. Recent durable messages remain
                    // authoritative context when a summary object is missing or
                    // corrupt.
                    self.inner
                        .metrics
                        .summary_failed
                        .fetch_add(1, Ordering::Relaxed);
                    let page = self
                        .inner
                        .store
                        .page_conversation_messages(MessagePageQuery {
                            conversation: conversation.clone(),
                            before: None,
                            limit: self.inner.config.recent_context_messages,
                        })
                        .await
                        .map_err(conversation_store_error)?
                        .ok_or_else(|| {
                            ServiceError::new(
                                "CONVERSATION_NOT_FOUND",
                                "conversation was not found",
                            )
                        })?;
                    context.messages = page.messages;
                    context.messages.reverse();
                    None
                }
            },
            None => None,
        };
        let token_budget = self.inner.config.summary_trigger_tokens;
        let mut selected_summary = None;
        let empty = json!({"summary": null, "messages": []});
        if let Some(summary) = summary {
            let candidate = json!({"summary": summary, "messages": []});
            if estimated_json_tokens(&candidate)? <= token_budget {
                selected_summary = candidate.get("summary").cloned();
            } else {
                let compact = compact_summary_for_context(&summary);
                let candidate = json!({"summary": compact, "messages": []});
                if estimated_json_tokens(&candidate)? <= token_budget {
                    selected_summary = candidate.get("summary").cloned();
                }
            }
        }
        let mut newest_first = Vec::with_capacity(context.messages.len());
        for message in context.messages.into_iter().rev() {
            let content = self.read_conversation_content(&message.content).await?;
            let item = json!({
                "message_order": message.message_order,
                "role": match message.role {
                    ConversationRole::User => "user",
                    ConversationRole::Assistant => "assistant",
                },
                "content_hash": message.content_hash.as_str(),
                "content": content,
            });
            newest_first.push(item);
            let mut chronological = newest_first.clone();
            chronological.reverse();
            let candidate = json!({
                "summary": selected_summary,
                "messages": chronological,
            });
            if estimated_json_tokens(&candidate)? > token_budget {
                newest_first.pop();
                break;
            }
        }
        newest_first.reverse();
        let messages = newest_first;
        let context_message_count = messages.len();
        let context_value = if selected_summary.is_none() && messages.is_empty() {
            empty
        } else {
            json!({"summary": selected_summary, "messages": messages})
        };
        let context_tokens = estimated_json_tokens(&context_value)?;
        self.inner.metrics.conversation_context_messages.store(
            u64::try_from(context_message_count).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        self.inner.metrics.conversation_context_tokens.store(
            u64::try_from(context_tokens).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        Ok(context_value)
    }

    async fn read_conversation_content(
        &self,
        content: &ConversationContent,
    ) -> Result<Value, ServiceError> {
        match content {
            ConversationContent::Inline(value) => Ok(value.clone()),
            ConversationContent::Ref(reference) => self.read_artifact_value(reference).await,
        }
    }

    async fn conversation_message_view(
        &self,
        message: ConversationMessage,
    ) -> Result<ConversationMessageView, ServiceError> {
        let content = self.read_conversation_content(&message.content).await?;
        Ok(ConversationMessageView {
            message_id: message.message_id,
            conversation_id: message.conversation_id,
            message_order: message.message_order,
            role: message.role,
            run_id: message.run_id,
            content,
            content_hash: message.content_hash,
            created_at: message.created_at,
        })
    }

    async fn put_scoped_artifact_value(
        &self,
        tenant_id: &str,
        source_kind: TerminalArtifactSourceKind,
        source_id: &str,
        scope: String,
        value: &Value,
    ) -> Result<(String, ContentHash), ServiceError> {
        // Every object referenced by terminal-only metadata is wrapped here.
        // The full runtime's worker artifacts remain plain values, so terminal
        // privacy/retention GC can delete a scoped object without ever
        // deleting a content-identical full-runtime artifact. The deterministic
        // source scope also makes retries stable while preventing sharing
        // across Runs, messages, summaries, Conversations, or tenants.
        let envelope = ScopedArtifactEnvelope {
            kind: SCOPED_ARTIFACT_KIND,
            scope: format!("tenant:{tenant_id}:{scope}"),
            value,
        };
        let bytes = serde_jcs::to_vec(&envelope).map_err(|_| {
            ServiceError::new(TERMINAL_OUTPUT_UNAVAILABLE, "terminal output is invalid")
        })?;
        self.put_artifact_bytes(
            tenant_id,
            source_kind,
            source_id,
            &bytes,
            TERMINAL_SCOPED_ARTIFACT_MEDIA_TYPE,
        )
        .await
    }

    async fn put_artifact_bytes(
        &self,
        tenant_id: &str,
        source_kind: TerminalArtifactSourceKind,
        source_id: &str,
        bytes: &[u8],
        media_type: &str,
    ) -> Result<(String, ContentHash), ServiceError> {
        let artifact = self
            .inner
            .artifact_store
            .artifact_for_bytes(bytes, Some(media_type.to_owned()))
            .map_err(|_| terminal_unavailable())?;
        let reference = serde_json::to_string(&artifact).map_err(|_| terminal_unavailable())?;
        let now = Utc::now();
        let grace = self
            .inner
            .config
            .terminal_commit_retry
            .saturating_mul(2)
            .max(Duration::from_secs(1));
        self.inner
            .store
            .stage_terminal_artifact(NewTerminalArtifactStage {
                tenant_id: tenant_id.to_owned(),
                content_ref: reference.clone(),
                content_hash: artifact.content_hash().clone(),
                source_kind,
                source_id: source_id.to_owned(),
                available_at: now + chrono_duration(grace)?,
                created_at: now,
            })
            .await
            .map_err(store_unavailable)?;
        let (hash, size) = self
            .inner
            .artifact_store
            .put_and_verify(&artifact, bytes)
            .await
            .map_err(|_| terminal_unavailable())?;
        if hash != *artifact.content_hash() || size != artifact.size_bytes() {
            return Err(terminal_unavailable());
        }
        Ok((reference, hash))
    }

    async fn read_artifact_value(&self, reference: &str) -> Result<Value, ServiceError> {
        let artifact: insight_engine::ArtifactRef =
            serde_json::from_str(reference).map_err(|_| {
                ServiceError::new(CONVERSATION_CONTENT_UNAVAILABLE, "content ref invalid")
            })?;
        let locator = self
            .inner
            .artifact_store
            .storage_locator(&artifact)
            .map_err(|_| terminal_unavailable())?;
        let bytes = self
            .inner
            .artifact_store
            .read_and_verify(&artifact, &locator, 64 * 1024 * 1024)
            .await
            .map_err(|_| {
                ServiceError::new(
                    CONVERSATION_CONTENT_UNAVAILABLE,
                    "conversation content is unavailable",
                )
            })?;
        let stored: Value = serde_json::from_slice(&bytes).map_err(|_| {
            ServiceError::new(
                CONVERSATION_CONTENT_UNAVAILABLE,
                "conversation content is invalid",
            )
        })?;
        if artifact.media_type() != Some(TERMINAL_SCOPED_ARTIFACT_MEDIA_TYPE) {
            // Backward compatibility for terminal objects written before
            // source-scoped envelopes were introduced.
            return Ok(stored);
        }
        decode_scoped_artifact(stored)
    }

    async fn delete_artifact_reference(&self, reference: &str) -> Result<(), ServiceError> {
        let artifact: insight_engine::ArtifactRef =
            serde_json::from_str(reference).map_err(|_| terminal_unavailable())?;
        let locator = self
            .inner
            .artifact_store
            .storage_locator(&artifact)
            .map_err(|_| terminal_unavailable())?;
        self.inner
            .artifact_store
            .delete(&artifact, &locator)
            .await
            .map_err(|_| terminal_unavailable())
    }

    async fn spawn_owner_heartbeat(&self) {
        let engine = self.clone();
        let handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(engine.inner.config.owner_heartbeat);
            interval.tick().await;
            loop {
                tokio::select! {
                    _ = engine.inner.shutdown.cancelled() => break,
                    _ = interval.tick() => {}
                    () = engine.inner.owner_recovery.notified() => {}
                }
                if engine.inner.shutdown.is_cancelled() {
                    break;
                }
                if !engine.inner.lease_healthy.load(Ordering::Acquire) {
                    if !engine.recover_owner().await {
                        break;
                    }
                    interval.reset();
                    continue;
                }
                let owner = engine.current_owner();
                let heartbeat_timeout = engine
                    .inner
                    .confirmed_owner_lease
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .heartbeat_timeout(
                        tokio::time::Instant::now(),
                        engine.inner.config.owner_heartbeat,
                    );
                let Some(heartbeat_timeout) = heartbeat_timeout else {
                    engine.lose_owner(&owner);
                    continue;
                };
                let proposed_deadline =
                    tokio::time::Instant::now() + engine.inner.config.owner_lease;
                let expires = match chrono_duration(engine.inner.config.owner_lease) {
                    Ok(duration) => Utc::now() + duration,
                    Err(_) => {
                        engine.lose_owner(&owner);
                        continue;
                    }
                };
                let heartbeat =
                    engine
                        .inner
                        .store
                        .heartbeat_runtime_instance(HeartbeatRuntimeInstance {
                            owner: owner.clone(),
                            lease_expires_at: expires,
                        });
                match tokio::time::timeout(heartbeat_timeout, heartbeat).await {
                    Ok(Ok(OwnerLeaseHeartbeat::Renewed { .. })) => {
                        engine
                            .inner
                            .confirmed_owner_lease
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .renew(proposed_deadline);
                    }
                    Ok(Ok(OwnerLeaseHeartbeat::Lost)) => {
                        engine.lose_owner(&owner);
                    }
                    Ok(Err(_)) | Err(_) => {
                        let retire = engine
                            .inner
                            .confirmed_owner_lease
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .error_requires_retirement(
                                tokio::time::Instant::now(),
                                engine.inner.config.owner_heartbeat,
                            );
                        if retire {
                            engine.lose_owner(&owner);
                        }
                    }
                }
            }
        });
        self.inner.background.lock().await.push(handle);
    }

    async fn recover_owner(&self) -> bool {
        let mut previous_epoch = self.current_owner().owner_epoch;
        let mut backoff = Duration::from_millis(25);
        let max_backoff = self
            .inner
            .config
            .owner_heartbeat
            .max(Duration::from_millis(25))
            .min(Duration::from_secs(1));
        loop {
            if self.inner.shutdown.is_cancelled() {
                return false;
            }
            let now = Utc::now();
            let owner_epoch = now
                .timestamp_micros()
                .max(previous_epoch.saturating_add(1))
                .max(1);
            previous_epoch = owner_epoch;
            let owner = RuntimeOwner {
                instance_id: format!("terminal-{}", Uuid::new_v4().simple()),
                owner_epoch,
            };
            let proposed_deadline = tokio::time::Instant::now() + self.inner.config.owner_lease;
            let lease_expires_at = match chrono_duration(self.inner.config.owner_lease) {
                Ok(duration) => now + duration,
                Err(_) => return false,
            };
            let registration =
                self.inner
                    .store
                    .register_runtime_instance(RegisterRuntimeInstance {
                        owner: owner.clone(),
                        endpoint: self.inner.endpoint.clone(),
                        lease_expires_at,
                        started_at: now,
                    });
            let registered = tokio::select! {
                _ = self.inner.shutdown.cancelled() => return false,
                result = registration => result,
            };
            if registered.is_ok() {
                if self.inner.shutdown.is_cancelled() {
                    let _ = self.inner.store.unregister_runtime_instance(owner).await;
                    return false;
                }
                *self
                    .inner
                    .owner
                    .write()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) = owner;
                self.inner
                    .confirmed_owner_lease
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .renew(proposed_deadline);
                self.inner.lease_healthy.store(true, Ordering::Release);
                self.inner.accepting.store(true, Ordering::Release);
                if self.inner.shutdown.is_cancelled() {
                    self.inner.accepting.store(false, Ordering::Release);
                    self.inner.lease_healthy.store(false, Ordering::Release);
                    let _ = self
                        .inner
                        .store
                        .unregister_runtime_instance(self.current_owner())
                        .await;
                    return false;
                }
                return true;
            }
            tokio::select! {
                _ = self.inner.shutdown.cancelled() => return false,
                () = tokio::time::sleep(backoff) => {}
            }
            backoff = backoff.saturating_mul(2).min(max_backoff);
        }
    }

    async fn spawn_retention_pump(&self) {
        let engine = self.clone();
        let handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(TERMINAL_MAINTENANCE_CADENCE);
            loop {
                tokio::select! {
                    _ = engine.inner.shutdown.cancelled() => break,
                    _ = interval.tick() => {
                        let _ = engine.run_retention_batch().await;
                    }
                }
            }
        });
        self.inner.background.lock().await.push(handle);
    }

    async fn spawn_content_deletion_pump(&self) {
        let engine = self.clone();
        let handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(5));
            loop {
                tokio::select! {
                    _ = engine.inner.shutdown.cancelled() => break,
                    _ = interval.tick() => {
                        let _ = engine.run_content_deletion_drain().await;
                    }
                }
            }
        });
        self.inner.background.lock().await.push(handle);
    }

    async fn spawn_artifact_staging_pump(&self) {
        let engine = self.clone();
        let handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(5));
            loop {
                tokio::select! {
                    _ = engine.inner.shutdown.cancelled() => break,
                    _ = interval.tick() => {
                        let _ = engine.run_artifact_staging_drain_at(Utc::now()).await;
                    }
                }
            }
        });
        self.inner.background.lock().await.push(handle);
    }

    async fn run_retention_batch(&self) -> Result<(), ServiceError> {
        let observed_at = Utc::now();
        let conversation_before =
            observed_at - chrono_duration(self.inner.config.conversation_retention)?;
        let run_before = observed_at - chrono_duration(self.inner.config.run_retention)?;
        let started = tokio::time::Instant::now();
        self.inner
            .metrics
            .retention_backlog_pending
            .store(true, Ordering::Relaxed);
        let mut backlog_pending = false;
        for batch_index in 0..MAX_TERMINAL_MAINTENANCE_BATCHES_PER_CYCLE {
            let conversations = self
                .inner
                .store
                .delete_archived_conversations_before(BoundedRetention {
                    before: conversation_before,
                    limit: TERMINAL_MAINTENANCE_BATCH_SIZE,
                })
                .await
                .map_err(conversation_store_error)?;
            let runs = self
                .inner
                .store
                .delete_terminal_runs_before(BoundedRetention {
                    before: run_before,
                    limit: TERMINAL_MAINTENANCE_BATCH_SIZE,
                })
                .await
                .map_err(store_unavailable)?;
            self.inner
                .metrics
                .retention_conversations_deleted
                .fetch_add(conversations.deleted, Ordering::Relaxed);
            self.inner
                .metrics
                .retention_runs_deleted
                .fetch_add(runs.deleted, Ordering::Relaxed);
            self.inner
                .metrics
                .retention_batches
                .fetch_add(1, Ordering::Relaxed);

            let saturated = conversations.deleted >= u64::from(TERMINAL_MAINTENANCE_BATCH_SIZE)
                || runs.deleted >= u64::from(TERMINAL_MAINTENANCE_BATCH_SIZE);
            if !saturated {
                break;
            }
            if batch_index + 1 >= MAX_TERMINAL_MAINTENANCE_BATCHES_PER_CYCLE
                || started.elapsed() >= TERMINAL_MAINTENANCE_CYCLE_BUDGET
            {
                backlog_pending = true;
                break;
            }
        }
        self.inner
            .metrics
            .retention_backlog_pending
            .store(backlog_pending, Ordering::Relaxed);
        self.run_content_deletion_drain().await?;
        Ok(())
    }

    async fn run_content_deletion_batch(&self) -> Result<(usize, bool), ServiceError> {
        let observed_at = Utc::now();
        let claims = self
            .inner
            .store
            .claim_content_deletion_jobs(ClaimContentDeletionJobs {
                claimed_by: self.current_owner().instance_id,
                observed_at,
                claim_expires_at: observed_at + chrono::Duration::seconds(30),
                limit: TERMINAL_MAINTENANCE_BATCH_SIZE,
            })
            .await
            .map_err(store_unavailable)?;
        self.inner
            .metrics
            .content_deletion_batches
            .fetch_add(1, Ordering::Relaxed);
        self.inner
            .metrics
            .content_deletion_oldest_age_seconds
            .store(
                oldest_claim_age_seconds(
                    observed_at,
                    claims.iter().map(|claim| claim.job.created_at),
                ),
                Ordering::Relaxed,
            );
        let claimed = claims.len();
        let mut had_failure = false;
        for claim in claims {
            if self
                .delete_artifact_reference(&claim.job.content_ref)
                .await
                .is_err()
            {
                // The lease expires and makes the job claimable again. Object
                // deletion is idempotent, so a crash before ack is also safe.
                had_failure = true;
                continue;
            }
            self.inner
                .store
                .ack_content_deletion_job(AckContentDeletionJob {
                    deletion_job_id: claim.job.deletion_job_id,
                    claim_token: claim.claim_token,
                })
                .await
                .map_err(store_unavailable)?;
        }
        Ok((claimed, had_failure))
    }

    async fn run_content_deletion_drain(&self) -> Result<u64, ServiceError> {
        let started = tokio::time::Instant::now();
        let mut total_claimed = 0_u64;
        let mut had_failure = false;
        self.inner
            .metrics
            .content_deletion_backlog_pending
            .store(true, Ordering::Relaxed);
        let mut backlog_pending = false;
        for batch_index in 0..MAX_TERMINAL_MAINTENANCE_BATCHES_PER_CYCLE {
            let (claimed, batch_had_failure) = self.run_content_deletion_batch().await?;
            had_failure |= batch_had_failure;
            total_claimed =
                total_claimed.saturating_add(u64::try_from(claimed).unwrap_or(u64::MAX));
            if claimed < usize::try_from(TERMINAL_MAINTENANCE_BATCH_SIZE).unwrap_or(usize::MAX) {
                backlog_pending = had_failure;
                break;
            }
            if batch_index + 1 >= MAX_TERMINAL_MAINTENANCE_BATCHES_PER_CYCLE
                || started.elapsed() >= TERMINAL_MAINTENANCE_CYCLE_BUDGET
            {
                backlog_pending = true;
                break;
            }
        }
        self.inner
            .metrics
            .content_deletion_backlog_pending
            .store(backlog_pending, Ordering::Relaxed);
        Ok(total_claimed)
    }

    #[doc(hidden)]
    pub async fn run_artifact_staging_batch_at(
        &self,
        observed_at: DateTime<Utc>,
    ) -> Result<u64, ServiceError> {
        self.run_artifact_staging_claim_batch_at(observed_at)
            .await
            .map(|(resolved, _, _)| resolved)
    }

    async fn run_artifact_staging_claim_batch_at(
        &self,
        observed_at: DateTime<Utc>,
    ) -> Result<(u64, usize, bool), ServiceError> {
        let claims = self
            .inner
            .store
            .claim_terminal_artifact_stages(ClaimTerminalArtifactStages {
                claimed_by: self.current_owner().instance_id,
                observed_at,
                claim_expires_at: observed_at + chrono::Duration::seconds(30),
                limit: TERMINAL_MAINTENANCE_BATCH_SIZE,
            })
            .await
            .map_err(store_unavailable)?;
        self.inner
            .metrics
            .artifact_staging_batches
            .fetch_add(1, Ordering::Relaxed);
        self.inner
            .metrics
            .artifact_staging_oldest_age_seconds
            .store(
                oldest_claim_age_seconds(
                    observed_at,
                    claims.iter().map(|claim| claim.stage.created_at),
                ),
                Ordering::Relaxed,
            );
        let claimed = claims.len();
        let mut resolved = 0_u64;
        let mut had_failure = false;
        for claim in claims {
            match self
                .inner
                .store
                .resolve_terminal_artifact_stage(ResolveTerminalArtifactStage {
                    staging_id: claim.stage.staging_id.clone(),
                    claim_token: claim.claim_token.clone(),
                })
                .await
                .map_err(store_unavailable)?
            {
                TerminalArtifactStageDisposition::Authoritative => {
                    resolved = resolved.saturating_add(1);
                }
                TerminalArtifactStageDisposition::Lost => {}
                TerminalArtifactStageDisposition::DeleteOrphan => {
                    let artifact: insight_engine::ArtifactRef =
                        serde_json::from_str(&claim.stage.content_ref)
                            .map_err(|_| terminal_unavailable())?;
                    if artifact.media_type() != Some(TERMINAL_SCOPED_ARTIFACT_MEDIA_TYPE) {
                        // Registration validates this invariant. Corrupt
                        // staging state fails closed instead of deleting a
                        // full-runtime or foreign object.
                        had_failure = true;
                        continue;
                    }
                    if self
                        .delete_artifact_reference(&claim.stage.content_ref)
                        .await
                        .is_err()
                    {
                        had_failure = true;
                        continue;
                    }
                    if self
                        .inner
                        .store
                        .ack_terminal_artifact_stage(AckTerminalArtifactStage {
                            staging_id: claim.stage.staging_id,
                            claim_token: claim.claim_token,
                        })
                        .await
                        .map_err(store_unavailable)?
                    {
                        resolved = resolved.saturating_add(1);
                    } else {
                        had_failure = true;
                    }
                }
            }
        }
        Ok((resolved, claimed, had_failure))
    }

    #[doc(hidden)]
    pub async fn run_artifact_staging_drain_at(
        &self,
        observed_at: DateTime<Utc>,
    ) -> Result<u64, ServiceError> {
        let started = tokio::time::Instant::now();
        let mut total_resolved = 0_u64;
        let mut had_failure = false;
        self.inner
            .metrics
            .artifact_staging_backlog_pending
            .store(true, Ordering::Relaxed);
        let mut backlog_pending = false;
        for batch_index in 0..MAX_TERMINAL_MAINTENANCE_BATCHES_PER_CYCLE {
            let (resolved, claimed, batch_had_failure) = self
                .run_artifact_staging_claim_batch_at(observed_at)
                .await?;
            had_failure |= batch_had_failure;
            total_resolved = total_resolved.saturating_add(resolved);
            if claimed < usize::try_from(TERMINAL_MAINTENANCE_BATCH_SIZE).unwrap_or(usize::MAX) {
                backlog_pending = had_failure;
                break;
            }
            if batch_index + 1 >= MAX_TERMINAL_MAINTENANCE_BATCHES_PER_CYCLE
                || started.elapsed() >= TERMINAL_MAINTENANCE_CYCLE_BUDGET
            {
                backlog_pending = true;
                break;
            }
        }
        self.inner
            .metrics
            .artifact_staging_backlog_pending
            .store(backlog_pending, Ordering::Relaxed);
        Ok(total_resolved)
    }

    fn current_owner(&self) -> RuntimeOwner {
        self.inner
            .owner
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    fn conversation_mutation(&self, conversation_id: &str) -> Arc<AsyncMutex<()>> {
        let mut hasher = DefaultHasher::new();
        conversation_id.hash(&mut hasher);
        let index =
            usize::try_from(hasher.finish()).unwrap_or(0) % self.inner.conversation_mutations.len();
        Arc::clone(&self.inner.conversation_mutations[index])
    }

    fn lose_owner(&self, expected_owner: &RuntimeOwner) {
        let owner = self
            .inner
            .owner
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if *owner != *expected_owner {
            return;
        }
        if self.inner.lease_healthy.swap(false, Ordering::AcqRel) {
            self.inner.accepting.store(false, Ordering::Release);
            self.inner
                .metrics
                .lease_expired
                .fetch_add(1, Ordering::Relaxed);
            let mut active = self
                .inner
                .active
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let interrupted = active
                .values()
                .filter(|run| run.owner == *expected_owner)
                .count();
            self.inner.metrics.interrupted.fetch_add(
                u64::try_from(interrupted).unwrap_or(u64::MAX),
                Ordering::Relaxed,
            );
            active.retain(|_, run| {
                if run.owner == *expected_owner {
                    run.cancellation.cancel();
                    false
                } else {
                    true
                }
            });
            drop(active);
            drop(owner);
            self.inner.owner_recovery.notify_one();
        }
    }

    fn record_terminal_metric(&self, state: TerminalState) {
        self.inner.metrics.results.fetch_add(1, Ordering::Relaxed);
        let counter = match state {
            TerminalState::Succeeded => &self.inner.metrics.succeeded,
            TerminalState::Failed => &self.inner.metrics.failed,
            TerminalState::Cancelled => &self.inner.metrics.cancelled,
            TerminalState::TimedOut => &self.inner.metrics.timed_out,
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }

    async fn maybe_schedule_summary(&self, conversation: Conversation) {
        if self
            .inner
            .deleting_conversations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .contains(&conversation.conversation_id)
        {
            return;
        }
        let inserted = self
            .inner
            .summary_jobs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(conversation.conversation_id.clone());
        if !inserted {
            return;
        }
        let engine = self.clone();
        let conversation_id = conversation.conversation_id.clone();
        let _task = tokio::spawn(async move {
            if !engine.inner.qualification_summary_delay.is_zero() {
                tokio::select! {
                    _ = engine.inner.shutdown.cancelled() => {
                        engine
                            .inner
                            .summary_jobs
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .remove(&conversation_id);
                        return;
                    }
                    () = tokio::time::sleep(engine.inner.qualification_summary_delay) => {}
                }
            }
            let succeeded = tokio::select! {
                _ = engine.inner.shutdown.cancelled() => false,
                result = AssertUnwindSafe(engine.generate_summary(conversation)).catch_unwind() => {
                    result.is_ok_and(|result| result.is_ok())
                }
            };
            let counter = if succeeded {
                &engine.inner.metrics.summary_succeeded
            } else {
                &engine.inner.metrics.summary_failed
            };
            counter.fetch_add(1, Ordering::Relaxed);
            engine
                .inner
                .summary_jobs
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .remove(&conversation_id);
        });
    }

    async fn generate_summary(&self, conversation: Conversation) -> Result<(), ServiceError> {
        let query = conversation_query(
            &conversation.conversation_id,
            &conversation.tenant_id,
            &conversation.user_id,
        );
        let previous = self
            .inner
            .store
            .latest_conversation_summary(query.clone())
            .await
            .map_err(conversation_store_error)?;
        let previous_boundary = previous
            .as_ref()
            .map_or(0, |summary| summary.through_message_order);
        let mut before = None;
        let mut messages = Vec::new();
        let backlog_limit = usize::try_from(self.inner.config.summary_trigger_messages)
            .unwrap_or(usize::MAX)
            .saturating_add(usize::try_from(self.inner.config.recent_context_messages).unwrap_or(0))
            .max(200);
        'pages: loop {
            let page = self
                .inner
                .store
                .page_conversation_messages(MessagePageQuery {
                    conversation: query.clone(),
                    before,
                    limit: 200,
                })
                .await
                .map_err(conversation_store_error)?
                .ok_or_else(|| {
                    ServiceError::new("CONVERSATION_NOT_FOUND", "conversation was not found")
                })?;
            for message in page.messages {
                if message.message_order <= previous_boundary {
                    break 'pages;
                }
                messages.push(message);
                if messages.len() > backlog_limit {
                    return Err(ServiceError::new(
                        "CONVERSATION_SUMMARY_BACKLOG",
                        "conversation summary backlog exceeds the bounded job size",
                    ));
                }
            }
            match page.next_cursor {
                Some(cursor) => before = Some(cursor),
                None => break,
            }
        }

        let mut selected = Vec::with_capacity(messages.len());
        let mut estimated_tokens = 0_usize;
        for message in messages {
            let content = self.read_conversation_content(&message.content).await?;
            let content_bytes = serde_jcs::to_vec(&content).map_err(|_| {
                ServiceError::new(
                    "CONVERSATION_SUMMARY_INVALID",
                    "conversation summary input is invalid",
                )
            })?;
            estimated_tokens =
                estimated_tokens.saturating_add(content_bytes.len().saturating_add(3) / 4);
            selected.push((message, content));
        }
        if selected.len()
            < usize::try_from(self.inner.config.summary_trigger_messages).unwrap_or(usize::MAX)
            && estimated_tokens < self.inner.config.summary_trigger_tokens
        {
            return Ok(());
        }
        let keep = usize::try_from(self.inner.config.recent_context_messages)
            .unwrap_or(usize::MAX)
            .min(selected.len());
        let summarized = &selected[keep..];
        let Some(boundary) = summarized.first() else {
            return Ok(());
        };
        let mut new_items = Vec::with_capacity(summarized.len());
        for (message, content) in summarized.iter().rev() {
            new_items.push(json!({
                "role": match message.role {
                    ConversationRole::User => "user",
                    ConversationRole::Assistant => "assistant",
                },
                "content_hash": message.content_hash.as_str(),
                "preview": summary_preview(content),
            }));
        }
        let mut entries = match previous.as_ref() {
            Some(summary) => {
                let previous_value = self.read_artifact_value(&summary.summary_ref).await?;
                normalized_summary_entries(&previous_value)
            }
            None => Vec::new(),
        };
        entries.extend(new_items);
        if entries.len() > SUMMARY_MAX_ENTRIES {
            entries.drain(..entries.len() - SUMMARY_MAX_ENTRIES);
        }
        let summary_budget = self
            .inner
            .config
            .summary_trigger_tokens
            .saturating_div(2)
            .max(64);
        let mut summary_value = flat_summary_value(
            previous_boundary,
            boundary.0.message_order,
            entries.as_slice(),
        );
        while !entries.is_empty() && estimated_json_tokens(&summary_value)? > summary_budget {
            entries.remove(0);
            summary_value = flat_summary_value(
                previous_boundary,
                boundary.0.message_order,
                entries.as_slice(),
            );
        }
        if estimated_json_tokens(&summary_value)? > summary_budget {
            return Err(ServiceError::new(
                "CONVERSATION_SUMMARY_INVALID",
                "conversation summary exceeds its hard size budget",
            ));
        }
        let (summary_ref, summary_hash) = self
            .put_scoped_artifact_value(
                &conversation.tenant_id,
                TerminalArtifactSourceKind::ConversationSummary,
                &format!(
                    "{}:{}",
                    conversation.conversation_id, boundary.0.message_order
                ),
                format!(
                    "conversation-summary:{}:{}",
                    conversation.conversation_id, boundary.0.message_order
                ),
                &summary_value,
            )
            .await?;
        let put = self
            .inner
            .store
            .put_conversation_summary(insight_durable::terminal_store::NewConversationSummary {
                conversation: query.clone(),
                through_message_order: boundary.0.message_order,
                summary_ref: summary_ref.clone(),
                summary_hash,
                model_revision: "extractive-terminal-v2".to_owned(),
                created_at: Utc::now(),
            })
            .await;
        if let Err(error) = put {
            match self.inner.store.latest_conversation_summary(query).await {
                Ok(Some(summary)) if summary.summary_ref == summary_ref => {}
                Ok(_) => {
                    let _ = self.delete_artifact_reference(&summary_ref).await;
                }
                Err(_) => {
                    // Preserve the object when authority cannot be read back;
                    // deleting after an ambiguous commit would risk data loss.
                }
            }
            return Err(conversation_store_error(error));
        }
        Ok(())
    }
}

struct AdmittedRun {
    run: RunRecord,
    attached: Option<TerminalAttachedRun>,
}

struct AdmissionSlot {
    owner: RuntimeOwner,
    permit: OwnedSemaphorePermit,
}

struct ActiveRunCleanup {
    inner: Weak<TerminalOnlyRunEngineInner>,
    run_id: RunId,
}

impl Drop for ActiveRunCleanup {
    fn drop(&mut self) {
        if let Some(inner) = self.inner.upgrade() {
            inner
                .active
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .remove(self.run_id.as_str());
            let _ = inner.live_run_stream_broker.close_run(&self.run_id);
        }
    }
}

#[derive(Serialize)]
struct ScopedArtifactEnvelope<'a> {
    kind: &'static str,
    scope: String,
    value: &'a Value,
}

struct ConversationAdmission {
    user_message: ConversationMessage,
    run: RunRecord,
    attached: Option<TerminalAttachedRun>,
    replayed: bool,
}

#[derive(Clone)]
struct PendingConversationCommit {
    conversation: Conversation,
    assistant_message_id: String,
}

fn record_from_admission(
    admission: &insight_durable::terminal_store::TerminalRunAdmission,
    attachment: RunAttachment,
) -> RunRecord {
    RunRecord {
        run_id: admission.run_id.as_str().to_owned(),
        response_id: response_id(&admission.run_id),
        projection_version: 0,
        request_id: admission.request_id.clone(),
        agent_id: admission.agent_id.clone(),
        agent_version: admission.deployment_revision_id.as_str().to_owned(),
        attachment,
        lifecycle: PublicRunLifecycle::Created,
        started_at: None,
        ended_at: None,
        updated_at: admission.accepted_at,
        input_summary: json!({
            "content_hash": admission.input_hash.as_str(),
            "terminal_only": true,
        }),
    }
}

fn terminal_state(outcome: &TerminalExecutionOutcome) -> TerminalState {
    match outcome {
        TerminalExecutionOutcome::Succeeded { .. } => TerminalState::Succeeded,
        TerminalExecutionOutcome::Failed { .. } => TerminalState::Failed,
        TerminalExecutionOutcome::Cancelled { .. } => TerminalState::Cancelled,
        TerminalExecutionOutcome::TimedOut { .. } => TerminalState::TimedOut,
    }
}

fn terminal_snapshot(
    run_id: &RunId,
    outcome: &TerminalExecutionOutcome,
) -> Result<DurableRunStreamSnapshot, ServiceError> {
    use insight_durable::common::adapter::{
        build_terminal_run_stream_snapshot, TerminalRunStreamSnapshotInput,
    };
    let (lifecycle, output, error) = match outcome {
        TerminalExecutionOutcome::Succeeded { output, .. } => {
            (insight_engine::RunLifecycle::Succeeded, Some(output), None)
        }
        TerminalExecutionOutcome::Failed { error_code, .. } => (
            insight_engine::RunLifecycle::Failed,
            None,
            Some(error_code.as_str()),
        ),
        TerminalExecutionOutcome::Cancelled { .. } => {
            (insight_engine::RunLifecycle::Cancelled, None, None)
        }
        TerminalExecutionOutcome::TimedOut { .. } => {
            (insight_engine::RunLifecycle::TimedOut, None, None)
        }
    };
    let pending = build_terminal_run_stream_snapshot(TerminalRunStreamSnapshotInput {
        run_id,
        lifecycle,
        output,
        error_code: error,
        items: Vec::new(),
        model_calls: Vec::new(),
        tool_results: outcome.tool_results().to_vec(),
        retrievals: Vec::new(),
        interactions: Vec::new(),
    })
    .map_err(|_| terminal_unavailable())?;
    durable_run_stream_snapshot_new(
        pending.run_id,
        pending.terminal_kind,
        pending.run,
        pending.manifest,
        pending.snapshot_hash,
    )
    .map_err(|_| terminal_unavailable())
}

fn conversation_query(conversation_id: &str, tenant_id: &str, user_id: &str) -> ConversationQuery {
    ConversationQuery {
        conversation_id: conversation_id.to_owned(),
        tenant_id: tenant_id.to_owned(),
        user_id: user_id.to_owned(),
    }
}

fn canonical_hash(value: &Value) -> Result<ContentHash, ServiceError> {
    let bytes = serde_jcs::to_vec(value)
        .map_err(|_| ServiceError::new("INPUT_INVALID", "value is not canonical JSON"))?;
    Ok(ContentHash::from_bytes(&bytes))
}

fn deterministic_public_id(prefix: &str, values: &[&str]) -> String {
    let mut bytes = format!("terminal-only/{prefix}/v1").into_bytes();
    for value in values {
        bytes.push(0);
        bytes.extend_from_slice(value.as_bytes());
    }
    format!(
        "{prefix}_{}",
        ContentHash::from_bytes(&bytes)
            .as_str()
            .trim_start_matches("sha256:")
    )
}

fn validate_request_identity(
    tenant_id: &str,
    user_id: &str,
    request_id: &str,
) -> Result<(), ServiceError> {
    if [tenant_id, user_id, request_id]
        .iter()
        .any(|value| !valid_request_identity_component(value))
    {
        return Err(ServiceError::new(
            "INPUT_INVALID",
            "request identity is invalid",
        ));
    }
    Ok(())
}

fn valid_request_identity_component(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 256
        && !value
            .chars()
            .any(|character| character.is_control() || character.is_whitespace())
}

fn response_id(run_id: &RunId) -> String {
    format!("resp_{}", run_id.as_str())
}

fn public_output(value: Value) -> RunOutput {
    match value {
        Value::String(text) => RunOutput {
            content: Some(text.clone()),
            format: Some("text".to_owned()),
            data: Value::String(text),
        },
        value => RunOutput {
            content: None,
            format: None,
            data: value,
        },
    }
}

pub(crate) fn summary_preview(value: &Value) -> String {
    let encoded = match value {
        Value::String(value) => value.clone(),
        value => serde_json::to_string(value).unwrap_or_else(|_| "<invalid>".to_owned()),
    };
    let mut characters = encoded.chars();
    let mut preview = characters
        .by_ref()
        .take(SUMMARY_PREVIEW_MAX_CHARS)
        .collect::<String>();
    if characters.next().is_some() {
        preview.push('…');
    }
    preview
}

fn decode_scoped_artifact(stored: Value) -> Result<Value, ServiceError> {
    let Value::Object(mut object) = stored else {
        return Err(ServiceError::new(
            CONVERSATION_CONTENT_UNAVAILABLE,
            "scoped terminal content is invalid",
        ));
    };
    if object.len() != 3
        || object.get("kind").and_then(Value::as_str) != Some(SCOPED_ARTIFACT_KIND)
        || object
            .get("scope")
            .and_then(Value::as_str)
            .is_none_or(str::is_empty)
    {
        return Err(ServiceError::new(
            CONVERSATION_CONTENT_UNAVAILABLE,
            "scoped terminal content is invalid",
        ));
    }
    object.remove("value").ok_or_else(|| {
        ServiceError::new(
            CONVERSATION_CONTENT_UNAVAILABLE,
            "scoped terminal content is invalid",
        )
    })
}

pub(crate) fn estimated_json_tokens(value: &Value) -> Result<usize, ServiceError> {
    serde_jcs::to_vec(value)
        .map(|bytes| bytes.len().saturating_add(3) / 4)
        .map_err(|_| {
            ServiceError::new(
                CONVERSATION_CONTENT_UNAVAILABLE,
                "conversation context is invalid",
            )
        })
}

pub(crate) fn normalized_summary_entries(previous: &Value) -> Vec<Value> {
    let entries = previous
        .as_object()
        .filter(|object| object.get("kind").and_then(Value::as_str) == Some("conversation_summary"))
        .and_then(|object| object.get("entries"))
        .and_then(Value::as_array);
    let Some(entries) = entries else {
        return vec![json!({
            "role": "summary",
            "content_hash": canonical_hash(previous)
                .map_or_else(|_| "legacy".to_owned(), |hash| hash.as_str().to_owned()),
            "preview": summary_preview(previous),
        })];
    };
    entries
        .iter()
        .filter_map(|entry| {
            let object = entry.as_object()?;
            let role = object.get("role")?.as_str()?;
            let content_hash = object.get("content_hash")?.as_str()?;
            let preview = object.get("preview")?.as_str()?;
            Some(json!({
                "role": truncate_chars(role, 16),
                "content_hash": truncate_chars(content_hash, 80),
                "preview": truncate_chars(preview, SUMMARY_PREVIEW_MAX_CHARS),
            }))
        })
        .rev()
        .take(SUMMARY_MAX_ENTRIES)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect()
}

pub(crate) fn flat_summary_value(
    previous_boundary: i64,
    through_message_order: i64,
    entries: &[Value],
) -> Value {
    json!({
        "kind": "conversation_summary",
        "version": 2,
        "previous_through_message_order": previous_boundary,
        "through_message_order": through_message_order,
        "entries": entries,
    })
}

fn compact_summary_for_context(summary: &Value) -> Value {
    let mut entries = normalized_summary_entries(summary);
    if entries.len() > 4 {
        entries.drain(..entries.len() - 4);
    }
    json!({
        "kind": "conversation_summary",
        "version": 2,
        "entries": entries,
    })
}

fn truncate_chars(value: &str, max_chars: usize) -> String {
    let mut characters = value.chars();
    let mut truncated = characters.by_ref().take(max_chars).collect::<String>();
    if characters.next().is_some() {
        truncated.push('…');
    }
    truncated
}

fn qualification_enabled_from_environment() -> Result<bool, ServiceError> {
    match std::env::var(QUALIFICATION_ENABLED_ENV) {
        Err(std::env::VarError::NotPresent) => Ok(false),
        Ok(value) if value == "true" => Ok(true),
        Ok(value) if value == "false" => Ok(false),
        Ok(_) | Err(std::env::VarError::NotUnicode(_)) => Err(ServiceError::new(
            "RUN_SERVICE_CONFIG_INVALID",
            "qualification enable flag is invalid",
        )),
    }
}

fn qualification_delay_from_environment(
    name: &str,
    qualification_enabled: bool,
) -> Result<Duration, ServiceError> {
    let raw = match std::env::var(name) {
        Ok(raw) => raw,
        Err(std::env::VarError::NotPresent) => return Ok(Duration::ZERO),
        Err(std::env::VarError::NotUnicode(_)) => {
            return Err(ServiceError::new(
                "RUN_SERVICE_CONFIG_INVALID",
                "terminal-only qualification delay is invalid",
            ));
        }
    };
    let milliseconds = raw.parse::<u64>().ok().filter(|milliseconds| {
        *milliseconds <= MAX_QUALIFICATION_DELAY_MS
            && !raw.is_empty()
            && raw.bytes().all(|byte| byte.is_ascii_digit())
    });
    let delay = milliseconds.map(Duration::from_millis).ok_or_else(|| {
        ServiceError::new(
            "RUN_SERVICE_CONFIG_INVALID",
            "terminal-only qualification delay is invalid",
        )
    })?;
    if !qualification_enabled && !delay.is_zero() {
        return Err(ServiceError::new(
            "RUN_SERVICE_CONFIG_INVALID",
            "terminal-only qualification delay requires qualification mode",
        ));
    }
    Ok(delay)
}

pub(crate) fn qualification_summary_delay_from_environment() -> Result<Duration, ServiceError> {
    qualification_delay_from_environment(
        QUALIFICATION_SUMMARY_DELAY_ENV,
        qualification_enabled_from_environment()?,
    )
}

async fn qualification_delay_or_cancellation(delay: Duration, cancellation: &CancellationToken) {
    if delay.is_zero() {
        return;
    }
    tokio::select! {
        _ = tokio::time::sleep(delay) => {}
        _ = cancellation.cancelled() => {}
    }
}

fn chrono_duration(duration: Duration) -> Result<chrono::Duration, ServiceError> {
    chrono::Duration::from_std(duration).map_err(|_| terminal_unavailable())
}

fn oldest_claim_age_seconds(
    observed_at: DateTime<Utc>,
    created_at: impl Iterator<Item = DateTime<Utc>>,
) -> u64 {
    created_at
        .min()
        .map(|oldest| {
            observed_at
                .signed_duration_since(oldest)
                .num_seconds()
                .max(0)
        })
        .and_then(|seconds| u64::try_from(seconds).ok())
        .unwrap_or(0)
}

fn remaining_terminal_execution_budget(
    configured_timeout: Duration,
    frozen_execution_budget_ms: u64,
    accepted_at: DateTime<Utc>,
    observed_at: DateTime<Utc>,
) -> Duration {
    let hard_budget = configured_timeout.min(Duration::from_millis(frozen_execution_budget_ms));
    let elapsed_since_admission = observed_at
        .signed_duration_since(accepted_at)
        .to_std()
        .unwrap_or_default();
    hard_budget
        .saturating_sub(elapsed_since_admission)
        .max(Duration::from_nanos(1))
}

fn terminal_unavailable() -> ServiceError {
    ServiceError::new(
        TERMINAL_ONLY_UNAVAILABLE,
        "terminal-only Run service is unavailable",
    )
}

#[derive(Debug)]
struct AdmissionPersistenceFailure {
    error: insight_engine::repository::RepositoryError,
    ambiguous: bool,
}

async fn persist_admission_with_recovery<T, F, Fut>(
    mut attempt: F,
) -> Result<T, AdmissionPersistenceFailure>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, insight_engine::repository::RepositoryError>>,
{
    match attempt().await {
        Ok(outcome) => Ok(outcome),
        Err(error) if error.code() == REPOSITORY_STORAGE_FAILURE => {
            attempt()
                .await
                .map_err(|error| AdmissionPersistenceFailure {
                    error,
                    // The first call may have committed even when its response was
                    // lost. A second inconclusive result therefore requires owner
                    // retirement so the candidate can never remain durably Active.
                    ambiguous: true,
                })
        }
        Err(error) => Err(AdmissionPersistenceFailure {
            error,
            ambiguous: false,
        }),
    }
}

fn store_unavailable(_: insight_engine::repository::RepositoryError) -> ServiceError {
    terminal_unavailable()
}

fn run_store_error(error: insight_engine::repository::RepositoryError) -> ServiceError {
    match error.code() {
        REPOSITORY_CONSTRAINT_CONFLICT | REPOSITORY_INTENT_CONFLICT => {
            ServiceError::new("RUN_CONFLICT", "run request conflicts with existing intent")
        }
        _ => terminal_unavailable(),
    }
}

fn conversation_store_error(error: insight_engine::repository::RepositoryError) -> ServiceError {
    match error.code() {
        REPOSITORY_CONSTRAINT_CONFLICT | REPOSITORY_INTENT_CONFLICT => ServiceError::new(
            "CONVERSATION_CONFLICT",
            "conversation request conflicts with existing intent",
        ),
        CONVERSATION_ARCHIVED => {
            ServiceError::new("CONVERSATION_ARCHIVED", "conversation is archived")
        }
        STORE_CONVERSATION_NOT_FOUND | CONVERSATION_OWNERSHIP_MISMATCH => {
            ServiceError::new("CONVERSATION_NOT_FOUND", "conversation was not found")
        }
        _ => terminal_unavailable(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use insight_engine::repository::adapter::repository_error;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn transient_heartbeat_error_preserves_then_renewal_extends_confirmed_lease() {
        let now = tokio::time::Instant::now();
        let heartbeat = Duration::from_secs(1);
        let mut lease = ConfirmedOwnerLease::new(now + Duration::from_secs(3));
        assert!(!lease.error_requires_retirement(now + heartbeat, heartbeat));

        lease.renew(now + Duration::from_secs(4));
        assert!(!lease.error_requires_retirement(now + Duration::from_secs(2), heartbeat));
    }

    #[test]
    fn repeated_heartbeat_errors_retire_inside_one_heartbeat_of_deadline() {
        let now = tokio::time::Instant::now();
        let heartbeat = Duration::from_secs(1);
        let lease = ConfirmedOwnerLease::new(now + Duration::from_secs(3));
        assert!(!lease.error_requires_retirement(now + Duration::from_secs(1), heartbeat));
        assert!(lease.error_requires_retirement(now + Duration::from_millis(2_001), heartbeat));
    }

    #[test]
    fn execution_budget_is_frozen_cap_less_time_spent_since_admission() {
        let accepted_at = Utc::now();
        assert_eq!(
            remaining_terminal_execution_budget(
                Duration::from_secs(5),
                3_000,
                accepted_at,
                accepted_at + chrono::Duration::seconds(2),
            ),
            Duration::from_secs(1)
        );
        assert_eq!(
            remaining_terminal_execution_budget(
                Duration::from_secs(2),
                3_000,
                accepted_at,
                accepted_at + chrono::Duration::seconds(3),
            ),
            Duration::from_nanos(1)
        );
    }

    #[tokio::test]
    async fn ambiguous_admission_is_retried_once_and_recovers_committed_authority() {
        let attempts = AtomicUsize::new(0);
        let outcome = persist_admission_with_recovery(|| {
            let attempt = attempts.fetch_add(1, Ordering::Relaxed);
            async move {
                if attempt == 0 {
                    Err(repository_error(
                        REPOSITORY_STORAGE_FAILURE,
                        "injected ambiguous response loss",
                    ))
                } else {
                    Ok("authoritative")
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(outcome, "authoritative");
        assert_eq!(attempts.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn repeated_storage_failure_is_marked_for_owner_retirement() {
        let attempts = AtomicUsize::new(0);
        let failure = persist_admission_with_recovery(|| {
            attempts.fetch_add(1, Ordering::Relaxed);
            async {
                Err::<(), _>(repository_error(
                    REPOSITORY_STORAGE_FAILURE,
                    "injected ambiguous response loss",
                ))
            }
        })
        .await
        .unwrap_err();
        assert!(failure.ambiguous);
        assert_eq!(attempts.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn definitive_conflict_is_not_retried_or_marked_ambiguous() {
        let attempts = AtomicUsize::new(0);
        let failure = persist_admission_with_recovery(|| {
            attempts.fetch_add(1, Ordering::Relaxed);
            async {
                Err::<(), _>(repository_error(
                    REPOSITORY_CONSTRAINT_CONFLICT,
                    "injected intent conflict",
                ))
            }
        })
        .await
        .unwrap_err();
        assert!(!failure.ambiguous);
        assert_eq!(attempts.load(Ordering::Relaxed), 1);
    }
}
