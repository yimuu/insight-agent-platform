//! Production host for the durable scheduler.
//!
//! This service owns only process-local admission, live subscriptions and
//! background pumps. Canonical Plan, Run state, task delivery, scheduler
//! checkpoints and terminal authority remain in `DurableRepository`.

use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error,
    fmt,
    fmt::Write as _,
    panic::AssertUnwindSafe,
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicU8, AtomicUsize, Ordering},
        Arc, Mutex, RwLock, Weak,
    },
    time::Duration,
};

use async_trait::async_trait;
use futures::FutureExt;
use insight_dsl::{
    GraphAuthorDocument, GraphSemanticEditBatch, StoredGraphView, TraceOverlay, ViewDocument,
    GRAPH_EDIT_CONFLICT,
};
use insight_durable::model::adapter as durable_model_adapter;
use insight_durable::production::adapter as production_contract_adapter;
use insight_durable::recovery_repository::adapter as recovery_contract_adapter;
use insight_durable::{
    AcknowledgeArtifactDeletionCommand, BeginMigrationCommand, BindArtifactStoreAuthorityCommand,
    ClaimHumanWorkItemCommand, ClaimSchedulerRunCommand, CompleteHumanWorkItemCommand,
    ContinueAsNewCommand, CreateRunCommand, DurableResponseSnapshot, FencedSchedulerRunCommand,
    FinalizeMigrationCommand, FireTimerCommand, ForkRunCommand, FullConversationRunAdmission,
    HumanTaskPrincipal, HumanWorkItem, HumanWorkItemId, MigrationMappingCompatibility,
    ModelToolBatchActivation, ModelToolTaskClaim, NoSchedulerCrash, OrderedPublicEventRead,
    OrphanSweepCommand, PlanPublicationOutcome, PublicEventPosition, PublicRunAttachment,
    PublicationHead, PublicationOrigin, PublishVersionedPlanCommand, ReceiveSignalCommand,
    RecoveryRevisionSpec, RecoveryRunReceipt, RedriveRunCommand, RepositoryError,
    ResolveSignalCommand, RunProjection, SchedulerDurableRepository, SchedulerTaskClaim,
    SignalInboxState, VersionedPlan, VersionedPlanCatalog, WorkClass,
    REPOSITORY_ARTIFACT_STORE_CONFLICT, REPOSITORY_CONSTRAINT_CONFLICT, REPOSITORY_INTENT_CONFLICT,
    REPOSITORY_REDRIVE_REQUIRES_FORK,
};
use insight_engine::{
    artifact_store::{ArtifactStoreDeploymentCapability, WorkerArtifactStore},
    events::protocol::{RunEvent, RunEventScope, RunEventType},
    history::types::{summarize_input, RunAttachment, RunLifecycle, RunRecord, StopError},
    outcome::{FailureKind, RunFailure, RunOutput},
    plan::{DataPort, DataPortId, NodeKind, PlanIndex, PortDirection},
    response::public_failure_message,
    worker::WorkerExecutorRegistry,
    AdmissionState, ArtifactId, ArtifactRef, ContentHash, DefinitionRevisionId,
    ExecutionEventContext, ExecutionEventPayload, ExecutionRevisionPin, MigrationNodeMapping,
    NodeId, PendingExecutionEvent, PersistenceMode, PlannedSchedulerAction, PublicEventEnvelope,
    PublicEventPayload, PublicFailureKind, PublicFailureSummary, RunId,
    RunLifecycle as EngineRunLifecycle, RunLineageKind, RuntimeValue, SchedulerAction,
    SchedulerCheckpointId, TerminationReason, TransitionKey, TransitionOutcome,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::{
    sync::{broadcast, Mutex as AsyncMutex, Notify, OwnedMutexGuard},
    task::{JoinHandle, JoinSet},
};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

pub use insight_durable::{PendingMigrationWait, ProductionRunRepository, RunRepositoryCapability};

use crate::{
    catalog::{
        subflow_registry_for_available, subflow_registry_for_stored, AgentStreamingSourceKind,
        DeployedAgent, DeploymentPersistencePolicy, DeploymentRiskDiagnostic,
        LeafDeploymentResolver, PublishedAgent,
    },
    scheduler_runtime::{
        consume_claimed_model_tool_task_with_observer,
        consume_claimed_scheduler_task_with_artifact_store_and_retrieval_observer,
        drive_scheduler_until_quiescent_with_preparer, FrozenSchedulerWorkerFailurePolicy,
        SchedulerActionPreparer, SchedulerRecoveryOutcome, SchedulerWorkerPumpOutcome,
    },
    terminal_only::{
        estimated_json_tokens, flat_summary_value, normalized_summary_entries,
        qualification_summary_delay_from_environment, summary_preview, ConversationAttachedTurn,
        ConversationDetachedTurn, ConversationMessagePageView, ConversationMessageView,
        ConversationVisibilityGuard, RunPersistenceCapability, TerminalAttachedRun,
        TerminalOnlyRunConfig, TerminalOnlyRunEngine, TerminalOnlyStore, SUMMARY_MAX_ENTRIES,
    },
};

use super::{
    CompletedFunctionCallTailPublication, InMemoryLiveResponseBroker, LiveResponseBroker,
    LiveResponseBrokerCapability, LiveResponseBrokerError, LiveResponseByteLimits,
    LiveResponseDelivery, LiveResponseItemIdentity, LiveResponseSubscriber,
};

const PRODUCTION_TRANSITION_DOMAIN: &str = "production.run-service";
const DEFAULT_PUMP_INTERVAL: Duration = Duration::from_millis(25);
const DEFAULT_IDLE_POLL_MIN_INTERVAL: Duration = Duration::from_millis(100);
const DEFAULT_IDLE_POLL_MAX_INTERVAL: Duration = Duration::from_secs(2);
const DEFAULT_SAFETY_POLL_INTERVAL: Duration = Duration::from_secs(5);
const DEFAULT_NOTIFICATION_RECONNECT_INTERVAL: Duration = Duration::from_millis(250);
const DEFAULT_CLAIM_BATCH_SIZE: u32 = 8;
const MAX_RECOVERY_BATCH: u32 = 256;
const MAX_PUBLIC_EVENT_BATCH: u32 = 256;
const MAX_PUBLIC_EVENT_PRUNE_BATCH: u32 = 256;
const MAX_PUBLIC_EVENT_RETENTION: Duration = Duration::from_secs(10 * 365 * 24 * 60 * 60);
const MAX_RUNTIME_INGRESS_BATCH: u32 = 256;
const FULL_CONVERSATION_SUMMARY_TIMEOUT: Duration = Duration::from_secs(30);
const FULL_CONVERSATION_SUMMARY_CLAIM_LEASE: Duration = Duration::from_secs(35);
const FULL_CONVERSATION_RETENTION_CADENCE: Duration = Duration::from_secs(60);
const FULL_CONVERSATION_RETENTION_BATCH_SIZE: u32 = 100;
const MAX_FULL_CONVERSATION_RETENTION_BATCHES_PER_CYCLE: usize = 64;
const FULL_CONVERSATION_RETENTION_BATCH_BUDGET: usize = 22;
const FULL_CONVERSATION_CONTENT_DELETION_BATCH_BUDGET: usize = 21;
const FULL_CONVERSATION_STAGING_BATCH_BUDGET: usize = 21;
const FULL_CONVERSATION_RETENTION_CYCLE_BUDGET: Duration = Duration::from_secs(45);
const FULL_CONVERSATION_MAINTENANCE_CLAIM_LEASE: chrono::Duration = chrono::Duration::seconds(60);
const MAX_HUMAN_TASK_LIST: u32 = 1_024;
const MAX_SIGNAL_PAYLOAD_BYTES: usize = 1024 * 1024;
const MAX_PUBLIC_ARTIFACT_SNAPSHOT_VALUES: usize = 100_000;
const MAX_PUBLIC_ARTIFACT_SNAPSHOT_DEPTH: usize = 64;
const PUBLIC_EVENT_RECONNECT_DELAY: Duration = Duration::from_millis(250);
const MAX_TERMINATION_CAS_ATTEMPTS: usize = 32;
const MAX_PUBLICATION_ADMISSION_ATTEMPTS: usize = 16;
const PLATFORM_PRODUCTION_REQUIRES_POSTGRES: &str = "PLATFORM_PRODUCTION_REQUIRES_POSTGRES";
const PLATFORM_PRODUCTION_REQUIRES_ARTIFACT_STORE: &str =
    "PLATFORM_PRODUCTION_REQUIRES_ARTIFACT_STORE";
const PLATFORM_PRODUCTION_REQUIRES_SHARED_ARTIFACT_STORE: &str =
    "PLATFORM_PRODUCTION_REQUIRES_SHARED_ARTIFACT_STORE";
const PLATFORM_ARTIFACT_STORE_AUTHORITY_CONFLICT: &str =
    "PLATFORM_ARTIFACT_STORE_AUTHORITY_CONFLICT";
const PLATFORM_PRODUCTION_REQUIRES_SHARED_LIVE_RESPONSE_BROKER: &str =
    "PLATFORM_PRODUCTION_REQUIRES_SHARED_LIVE_RESPONSE_BROKER";
const WORK_SCHEDULER_TASK: u8 = 1 << 0;
const WORK_MODEL_TOOL_TASK: u8 = 1 << 1;
const WORK_RUNTIME_INGRESS: u8 = 1 << 2;
const WORK_PUBLIC_EVENT: u8 = 1 << 3;
const WORK_RECOVERY: u8 = 1 << 4;
const WORK_ALL: u8 = WORK_SCHEDULER_TASK
    | WORK_MODEL_TOOL_TASK
    | WORK_RUNTIME_INGRESS
    | WORK_PUBLIC_EVENT
    | WORK_RECOVERY;

#[derive(Default)]
struct RunServiceMetrics {
    admission_accepted: AtomicU64,
    admission_capacity_rejected: AtomicU64,
    wakeup_notify: AtomicU64,
    wakeup_deadline: AtomicU64,
    wakeup_safety: AtomicU64,
    wakeup_completion: AtomicU64,
    scheduler_poll_claimed: AtomicU64,
    scheduler_poll_empty: AtomicU64,
    scheduler_claimed_nanos: AtomicU64,
    scheduler_empty_nanos: AtomicU64,
    model_tool_poll_claimed: AtomicU64,
    model_tool_poll_empty: AtomicU64,
    model_tool_claimed_nanos: AtomicU64,
    model_tool_empty_nanos: AtomicU64,
    executing_scheduler: AtomicUsize,
    executing_model_tool: AtomicUsize,
    executing_recovery: AtomicUsize,
    notification_listener_connected: AtomicBool,
    notification_listener_reconnects: AtomicU64,
    notification_publish_requests: AtomicU64,
    notification_published: AtomicU64,
    notification_publish_errors: AtomicU64,
    full_conversation_user_messages: AtomicU64,
    full_conversation_assistant_messages: AtomicU64,
    full_conversation_context_messages: AtomicU64,
    full_conversation_context_tokens: AtomicU64,
    full_conversation_summary_succeeded: AtomicU64,
    full_conversation_summary_failed: AtomicU64,
    full_conversation_retention_conversations_deleted: AtomicU64,
    full_conversation_retention_runs_deleted: AtomicU64,
    full_conversation_retention_batches: AtomicU64,
    full_conversation_retention_backlog_pending: AtomicBool,
    full_conversation_content_deletion_batches: AtomicU64,
    full_conversation_content_deletion_backlog_pending: AtomicBool,
    full_conversation_staging_batches: AtomicU64,
    full_conversation_staging_backlog_pending: AtomicBool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CoordinatorWakeupSource {
    Notify,
    Deadline,
    Safety,
    Completion,
}

impl RunServiceMetrics {
    fn record_wakeup(&self, source: CoordinatorWakeupSource) {
        let counter = match source {
            CoordinatorWakeupSource::Notify => &self.wakeup_notify,
            CoordinatorWakeupSource::Deadline => &self.wakeup_deadline,
            CoordinatorWakeupSource::Safety => &self.wakeup_safety,
            CoordinatorWakeupSource::Completion => &self.wakeup_completion,
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }

    fn record_claim(&self, class: WorkClass, claimed: bool, elapsed: Duration) {
        let nanos = u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX);
        let (count, total) = match (class, claimed) {
            (WorkClass::SchedulerTask, true) => {
                (&self.scheduler_poll_claimed, &self.scheduler_claimed_nanos)
            }
            (WorkClass::SchedulerTask, false) => {
                (&self.scheduler_poll_empty, &self.scheduler_empty_nanos)
            }
            (WorkClass::ModelToolTask, true) => (
                &self.model_tool_poll_claimed,
                &self.model_tool_claimed_nanos,
            ),
            (WorkClass::ModelToolTask, false) => {
                (&self.model_tool_poll_empty, &self.model_tool_empty_nanos)
            }
            _ => return,
        };
        count.fetch_add(1, Ordering::Relaxed);
        total.fetch_add(nanos, Ordering::Relaxed);
    }

    fn executing_counter(&self, class: WorkClass) -> &AtomicUsize {
        match class {
            WorkClass::ModelToolTask => &self.executing_model_tool,
            WorkClass::Recovery => &self.executing_recovery,
            _ => &self.executing_scheduler,
        }
    }
}

fn has_public_streaming_source(agent: &PublishedAgent) -> bool {
    !agent.public_streaming_contract().sources().is_empty()
}

fn has_public_llm_streaming_source(agent: &PublishedAgent) -> bool {
    agent
        .public_streaming_contract()
        .sources()
        .iter()
        .any(|source| source.kind() == AgentStreamingSourceKind::Llm)
}

#[derive(Default)]
struct DeployedAgentCatalogState {
    current_by_agent_id: BTreeMap<String, Arc<DeployedAgent>>,
    deployment_archive: BTreeMap<String, Arc<DeployedAgent>>,
}

#[derive(Clone, Default)]
pub struct DeployedAgentCatalog {
    state: Arc<RwLock<DeployedAgentCatalogState>>,
}

impl DeployedAgentCatalog {
    /// Build a catalog whose entries are both the current public routes and
    /// the initial immutable deployment archive.
    pub fn new(current: Vec<Arc<DeployedAgent>>) -> Result<Self, ServiceError> {
        Self::from_current_and_archive(current, Vec::new())
    }

    /// Separates mutable public routing from immutable Run execution lookup.
    ///
    /// `current` must contain at most one deployment for each public agent ID.
    /// `archive` may contain any number of older deployments for that same ID.
    /// Every deployment revision across both collections remains unique and is
    /// addressable for Runs pinned before an alias/adapter/worker update.
    pub fn from_current_and_archive(
        current: Vec<Arc<DeployedAgent>>,
        archive: Vec<Arc<DeployedAgent>>,
    ) -> Result<Self, ServiceError> {
        let mut current_by_agent_id = BTreeMap::new();
        let mut deployment_archive = BTreeMap::new();
        for agent in current {
            let agent_id = agent.published().metadata().id.clone();
            let deployment = agent.deployment_revision_id().as_str().to_owned();
            if current_by_agent_id
                .insert(agent_id, Arc::clone(&agent))
                .is_some()
            {
                return Err(ServiceError::new(
                    "AGENT_DEPLOYMENT_CONFLICT",
                    "current agent routes contain a duplicate agent ID",
                ));
            }
            insert_archived_deployment(&mut deployment_archive, deployment, agent)?;
        }
        for agent in archive {
            let deployment = agent.deployment_revision_id().as_str().to_owned();
            insert_archived_deployment(&mut deployment_archive, deployment, agent)?;
        }
        Ok(Self {
            state: Arc::new(RwLock::new(DeployedAgentCatalogState {
                current_by_agent_id,
                deployment_archive,
            })),
        })
    }

    pub fn get(&self, agent_id: &str) -> Option<Arc<DeployedAgent>> {
        self.read().current_by_agent_id.get(agent_id).cloned()
    }

    pub fn get_deployment(&self, deployment_revision: &str) -> Option<Arc<DeployedAgent>> {
        self.read()
            .deployment_archive
            .get(deployment_revision)
            .cloned()
    }

    pub fn list(&self) -> impl Iterator<Item = Arc<DeployedAgent>> {
        self.read()
            .current_by_agent_id
            .values()
            .cloned()
            .collect::<Vec<_>>()
            .into_iter()
    }

    pub fn archived_deployments(&self) -> impl Iterator<Item = Arc<DeployedAgent>> {
        self.read()
            .deployment_archive
            .values()
            .cloned()
            .collect::<Vec<_>>()
            .into_iter()
    }

    fn install_current(&self, agent: Arc<DeployedAgent>) -> Result<(), ServiceError> {
        let deployment = agent.deployment_revision_id().as_str().to_owned();
        let agent_id = agent.published().metadata().id.clone();
        let mut state = self.write();
        if let Some(existing) = state.deployment_archive.get(&deployment) {
            if existing.versioned_plan() != agent.versioned_plan() {
                return Err(ServiceError::new(
                    "GRAPH_PUBLICATION_CONFLICT",
                    "deployment revision collides with different immutable content",
                ));
            }
        } else {
            state
                .deployment_archive
                .insert(deployment, Arc::clone(&agent));
        }
        state.current_by_agent_id.insert(agent_id, agent);
        Ok(())
    }

    fn install_archive(&self, agent: Arc<DeployedAgent>) -> Result<(), ServiceError> {
        let deployment = agent.deployment_revision_id().as_str().to_owned();
        let mut state = self.write();
        if let Some(existing) = state.deployment_archive.get(&deployment) {
            if existing.versioned_plan() != agent.versioned_plan() {
                return Err(ServiceError::new(
                    "AGENT_DEPLOYMENT_CONFLICT",
                    "deployment revision collides with different immutable content",
                ));
            }
            return Ok(());
        }
        state.deployment_archive.insert(deployment, agent);
        Ok(())
    }

    fn read(&self) -> std::sync::RwLockReadGuard<'_, DeployedAgentCatalogState> {
        self.state
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn write(&self) -> std::sync::RwLockWriteGuard<'_, DeployedAgentCatalogState> {
        self.state
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

fn insert_archived_deployment(
    archive: &mut BTreeMap<String, Arc<DeployedAgent>>,
    deployment_revision: String,
    agent: Arc<DeployedAgent>,
) -> Result<(), ServiceError> {
    if archive.insert(deployment_revision, agent).is_some() {
        return Err(ServiceError::new(
            "AGENT_DEPLOYMENT_CONFLICT",
            "deployment archive contains a duplicate immutable revision",
        ));
    }
    Ok(())
}

fn restore_persisted_catalog(
    agents: &DeployedAgentCatalog,
    workers: &WorkerExecutorRegistry,
    stored: &VersionedPlanCatalog,
) -> Result<(), ServiceError> {
    for versioned in stored.plans() {
        restore_persisted_deployment(
            agents,
            workers,
            stored,
            versioned.deployment_revision_id().as_str(),
            &mut BTreeSet::new(),
        )?;
    }

    for head in stored.heads() {
        let deployment = restore_persisted_head(agents, workers, stored, head)?;
        agents.install_current(deployment)?;
    }
    Ok(())
}

/// Resolves one durable publication head from a single catalog snapshot. The
/// returned deployment is exact and executable in this runtime; process-local
/// `current` routes are deliberately not treated as publication authority.
fn restore_persisted_head(
    agents: &DeployedAgentCatalog,
    workers: &WorkerExecutorRegistry,
    stored: &VersionedPlanCatalog,
    head: &PublicationHead,
) -> Result<Arc<DeployedAgent>, ServiceError> {
    let deployment = restore_persisted_deployment(
        agents,
        workers,
        stored,
        head.deployment_revision_id().as_str(),
        &mut BTreeSet::new(),
    )?;
    if deployment.versioned_plan().agent_id() != head.agent_id()
        || deployment.versioned_plan().definition_id() != head.definition_id()
        || deployment.versioned_plan().definition_revision_id() != head.definition_revision_id()
        || deployment.versioned_plan().deployment_revision_id() != head.deployment_revision_id()
    {
        return Err(ServiceError::new(
            "STORED_DEPLOYMENT_INVALID",
            "durable publication head disagrees with its immutable deployment",
        ));
    }
    Ok(deployment)
}

/// Restore one exact immutable deployment and only its frozen subflow
/// dependency closure from a repository snapshot. This is also used by online
/// alias admission when another runtime published a revision after startup.
fn restore_persisted_deployment(
    agents: &DeployedAgentCatalog,
    workers: &WorkerExecutorRegistry,
    stored: &VersionedPlanCatalog,
    deployment_revision_id: &str,
    visiting: &mut BTreeSet<String>,
) -> Result<Arc<DeployedAgent>, ServiceError> {
    let versioned = stored
        .plans()
        .iter()
        .find(|plan| plan.deployment_revision_id().as_str() == deployment_revision_id)
        .cloned()
        .ok_or_else(|| {
            ServiceError::new(
                "RUN_DEPLOYMENT_UNAVAILABLE",
                "durable publication dependency is absent from its catalog snapshot",
            )
        })?;
    if let Some(existing) = agents.get_deployment(deployment_revision_id) {
        if existing.versioned_plan() != &versioned {
            return Err(ServiceError::new(
                "STORED_DEPLOYMENT_INVALID",
                "durable deployment collides with different in-memory content",
            ));
        }
        if !existing.workers_available(workers) {
            return Err(ServiceError::new(
                "RUN_DEPLOYMENT_UNAVAILABLE",
                "an exact worker version required by a durable deployment is unavailable",
            ));
        }
        return Ok(existing);
    }
    if !visiting.insert(deployment_revision_id.to_owned()) {
        return Err(ServiceError::new(
            "STORED_DEPLOYMENT_INVALID",
            "durable subflow deployment dependencies contain a cycle",
        ));
    }

    let result = (|| {
        let published = PublishedAgent::from_stored_versioned_plan(&versioned).map_err(|_| {
            ServiceError::new(
                "STORED_DEPLOYMENT_INVALID",
                "durable author metadata failed authoritative verification",
            )
        })?;
        for dependency in stored_subflow_dependencies(&published, &versioned)? {
            restore_persisted_deployment(agents, workers, stored, &dependency, visiting)?;
        }
        let subflows =
            subflow_registry_for_stored(&published, &versioned, agents.archived_deployments())
                .map_err(|error| {
                    if error.code() == "STORED_SUBFLOW_DEPENDENCY_UNAVAILABLE" {
                        ServiceError::new(
                            "RUN_DEPLOYMENT_UNAVAILABLE",
                            "durable subflow dependency is unavailable",
                        )
                    } else {
                        ServiceError::new(
                            "STORED_DEPLOYMENT_INVALID",
                            "durable subflow binding failed authoritative verification",
                        )
                    }
                })?;
        let deployed = Arc::new(DeployedAgent::restore_frozen(versioned, subflows).map_err(
            |_| {
                ServiceError::new(
                    "STORED_DEPLOYMENT_INVALID",
                    "durable deployment contracts failed authoritative verification",
                )
            },
        )?);
        if !deployed.workers_available(workers) {
            return Err(ServiceError::new(
                "RUN_DEPLOYMENT_UNAVAILABLE",
                "an exact worker version required by a durable deployment is unavailable",
            ));
        }
        agents.install_archive(Arc::clone(&deployed))?;
        Ok(deployed)
    })();
    visiting.remove(deployment_revision_id);
    result
}

fn stored_subflow_dependencies(
    published: &PublishedAgent,
    stored: &VersionedPlan,
) -> Result<Vec<String>, ServiceError> {
    let bindings = durable_model_adapter::versioned_plan_resolved_bindings(stored)
        .as_array()
        .ok_or_else(|| {
            ServiceError::new(
                "STORED_DEPLOYMENT_INVALID",
                "durable deployment bindings are invalid",
            )
        })?;
    let mut dependencies = Vec::new();
    for node in published.plan().nodes() {
        if !matches!(node.kind(), NodeKind::SubflowCall(_)) {
            continue;
        }
        let matches = bindings
            .iter()
            .filter(|document| {
                document.get("node_id").and_then(Value::as_str) == Some(node.id().as_str())
            })
            .collect::<Vec<_>>();
        let [document] = matches.as_slice() else {
            return Err(ServiceError::new(
                "STORED_DEPLOYMENT_INVALID",
                "durable subflow binding identity is ambiguous",
            ));
        };
        let binding = document
            .get("binding")
            .and_then(Value::as_object)
            .ok_or_else(|| {
                ServiceError::new(
                    "STORED_DEPLOYMENT_INVALID",
                    "durable subflow binding is invalid",
                )
            })?;
        let dependency = binding
            .get("deployment_revision_id")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                ServiceError::new(
                    "STORED_DEPLOYMENT_INVALID",
                    "durable subflow deployment revision is missing",
                )
            })?;
        dependencies.push(dependency.to_owned());
    }
    dependencies.sort();
    dependencies.dedup();
    Ok(dependencies)
}

/// Selects every direct subflow exclusively from one durable catalog
/// snapshot. Process-local current/archive state may accelerate exact restore,
/// but it never participates in deciding which child route is current.
fn durable_publication_subflows(
    parent: &PublishedAgent,
    agents: &DeployedAgentCatalog,
    workers: &WorkerExecutorRegistry,
    stored: &VersionedPlanCatalog,
) -> Result<(Vec<Arc<DeployedAgent>>, Vec<PublicationHead>), ServiceError> {
    let required_revisions = parent
        .plan()
        .nodes()
        .iter()
        .filter_map(|node| match node.kind() {
            NodeKind::SubflowCall(call) => Some(call.definition_revision_id.clone()),
            _ => None,
        })
        .collect::<BTreeSet<_>>();
    let mut deployments = Vec::with_capacity(required_revisions.len());
    let mut heads = Vec::with_capacity(required_revisions.len());
    for revision in required_revisions {
        let matches = stored
            .heads()
            .iter()
            .filter(|head| head.definition_revision_id() == &revision)
            .collect::<Vec<_>>();
        let [head] = matches.as_slice() else {
            return Err(ServiceError::new(
                "GRAPH_PUBLICATION_INVALID",
                "graph subflow definition does not identify one durable publication head",
            ));
        };
        let deployment = restore_persisted_deployment(
            agents,
            workers,
            stored,
            head.deployment_revision_id().as_str(),
            &mut BTreeSet::new(),
        )?;
        deployments.push(deployment);
        heads.push((*head).clone());
    }
    heads.sort_by(|left, right| left.agent_id().cmp(right.agent_id()));
    Ok((deployments, heads))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RunServiceDeploymentMode {
    Production,
    SingleProcessDevelopment,
}

#[derive(Debug, Clone, Copy)]
pub struct WorkCoordinatorConfig {
    pub active_poll_interval: Duration,
    pub idle_poll_min_interval: Duration,
    pub idle_poll_max_interval: Duration,
    pub safety_poll_interval: Duration,
    pub claim_batch_size: u32,
    pub notification_reconnect_interval: Duration,
}

impl Default for WorkCoordinatorConfig {
    fn default() -> Self {
        Self {
            active_poll_interval: DEFAULT_PUMP_INTERVAL,
            idle_poll_min_interval: DEFAULT_IDLE_POLL_MIN_INTERVAL,
            idle_poll_max_interval: DEFAULT_IDLE_POLL_MAX_INTERVAL,
            safety_poll_interval: DEFAULT_SAFETY_POLL_INTERVAL,
            claim_batch_size: DEFAULT_CLAIM_BATCH_SIZE,
            notification_reconnect_interval: DEFAULT_NOTIFICATION_RECONNECT_INTERVAL,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct RunServiceConfig {
    deployment_mode: RunServiceDeploymentMode,
    pub max_concurrent_runs: usize,
    pub max_concurrent_operations: usize,
    pub max_concurrent_operations_per_run: usize,
    pub subscriber_capacity: usize,
    pub scheduler_lease_seconds: u32,
    pub task_claim_seconds: u32,
    pub scheduler_action_budget: u32,
    pub work_coordinator: WorkCoordinatorConfig,
    /// Legacy cadence retained for non-production tests and maintenance
    /// pumps. Production scheduler discovery uses `work_coordinator`.
    pub pump_interval: Duration,
    pub run_timeout: Duration,
    pub artifact_orphan_retention: Duration,
    pub artifact_reference_retention: Duration,
    pub artifact_gc_interval: Duration,
    pub artifact_deletion_claim_seconds: u32,
    pub artifact_read_max_bytes: usize,
    pub public_event_nonterminal_retention: Duration,
    pub public_event_prune_interval: Duration,
    pub live_response_body_queue_capacity: usize,
    pub live_response_control_queue_capacity: usize,
    pub live_response_max_frame_bytes: usize,
    pub live_response_max_item_bytes: usize,
    pub live_response_max_run_bytes: usize,
    pub terminal_barrier_timeout: Duration,
    pub outbound_write_timeout: Duration,
    pub graph_persistence_policy: DeploymentPersistencePolicy,
}

impl RunServiceConfig {
    pub fn production(
        max_concurrent_runs: usize,
        max_concurrent_operations: usize,
        max_concurrent_operations_per_run: usize,
        subscriber_capacity: usize,
    ) -> Self {
        Self {
            deployment_mode: RunServiceDeploymentMode::Production,
            max_concurrent_runs,
            max_concurrent_operations,
            max_concurrent_operations_per_run,
            subscriber_capacity,
            scheduler_lease_seconds: 30,
            task_claim_seconds: 60,
            scheduler_action_budget: 512,
            work_coordinator: WorkCoordinatorConfig::default(),
            pump_interval: DEFAULT_PUMP_INTERVAL,
            run_timeout: Duration::from_secs(5 * 60),
            artifact_orphan_retention: Duration::from_secs(24 * 60 * 60),
            artifact_reference_retention: Duration::from_secs(30 * 24 * 60 * 60),
            artifact_gc_interval: Duration::from_secs(60),
            artifact_deletion_claim_seconds: 60,
            artifact_read_max_bytes: 64 * 1024 * 1024,
            public_event_nonterminal_retention: Duration::from_secs(24 * 60 * 60),
            public_event_prune_interval: Duration::from_secs(60),
            live_response_body_queue_capacity: 256,
            live_response_control_queue_capacity: 32,
            live_response_max_frame_bytes: 4 * 1_024,
            live_response_max_item_bytes: 4 * 1_024 * 1_024,
            live_response_max_run_bytes: 16 * 1_024 * 1_024,
            terminal_barrier_timeout: Duration::from_secs(2),
            outbound_write_timeout: Duration::from_secs(10),
            graph_persistence_policy: DeploymentPersistencePolicy::full(),
        }
    }

    /// Local single-process profile. It intentionally shares scheduler
    /// defaults with production; the distinct constructor makes the weaker
    /// repository ownership contract explicit at the binary boundary.
    pub fn single_process_development(
        max_concurrent_runs: usize,
        max_concurrent_operations: usize,
        max_concurrent_operations_per_run: usize,
        subscriber_capacity: usize,
    ) -> Self {
        let mut config = Self::production(
            max_concurrent_runs,
            max_concurrent_operations,
            max_concurrent_operations_per_run,
            subscriber_capacity,
        );
        config.deployment_mode = RunServiceDeploymentMode::SingleProcessDevelopment;
        config
    }

    pub fn with_artifact_gc(
        mut self,
        orphan_retention: Duration,
        reference_retention: Duration,
        gc_interval: Duration,
        deletion_claim_seconds: u32,
    ) -> Self {
        self.artifact_orphan_retention = orphan_retention;
        self.artifact_reference_retention = reference_retention;
        self.artifact_gc_interval = gc_interval;
        self.artifact_deletion_claim_seconds = deletion_claim_seconds;
        self
    }

    pub fn with_artifact_read_limit(mut self, max_bytes: usize) -> Self {
        self.artifact_read_max_bytes = max_bytes;
        self
    }

    pub fn with_run_timeout(mut self, timeout: Duration) -> Self {
        self.run_timeout = timeout;
        self
    }

    pub fn with_work_coordinator(mut self, config: WorkCoordinatorConfig) -> Self {
        self.work_coordinator = config;
        self
    }

    fn effective_work_coordinator(&self) -> WorkCoordinatorConfig {
        if self.pump_interval == DEFAULT_PUMP_INTERVAL {
            return self.work_coordinator;
        }
        // Existing integration tests use a non-default pump interval as an
        // injected clock. Preserve that behavior until those tests move to a
        // dedicated coordinator clock; production configuration never enters
        // this compatibility branch.
        let idle_poll_max_interval = if self.pump_interval > DEFAULT_IDLE_POLL_MAX_INTERVAL {
            self.pump_interval
        } else {
            DEFAULT_IDLE_POLL_MAX_INTERVAL
        };
        WorkCoordinatorConfig {
            active_poll_interval: self.pump_interval,
            idle_poll_min_interval: self.pump_interval,
            idle_poll_max_interval,
            safety_poll_interval: idle_poll_max_interval.max(DEFAULT_SAFETY_POLL_INTERVAL),
            claim_batch_size: self.work_coordinator.claim_batch_size,
            notification_reconnect_interval: self.work_coordinator.notification_reconnect_interval,
        }
    }

    pub fn with_public_event_retention(
        mut self,
        nonterminal_retention: Duration,
        prune_interval: Duration,
    ) -> Self {
        self.public_event_nonterminal_retention = nonterminal_retention;
        self.public_event_prune_interval = prune_interval;
        self
    }

    pub fn with_live_response_limits(
        mut self,
        body_queue_capacity: usize,
        control_queue_capacity: usize,
        terminal_barrier_timeout: Duration,
        outbound_write_timeout: Duration,
    ) -> Self {
        self.live_response_body_queue_capacity = body_queue_capacity;
        self.live_response_control_queue_capacity = control_queue_capacity;
        self.terminal_barrier_timeout = terminal_barrier_timeout;
        self.outbound_write_timeout = outbound_write_timeout;
        self
    }

    pub fn with_live_response_byte_limits(
        mut self,
        max_frame_bytes: usize,
        max_item_bytes: usize,
        max_run_bytes: usize,
    ) -> Self {
        self.live_response_max_frame_bytes = max_frame_bytes;
        self.live_response_max_item_bytes = max_item_bytes;
        self.live_response_max_run_bytes = max_run_bytes;
        self
    }

    pub fn with_graph_persistence_policy(mut self, policy: DeploymentPersistencePolicy) -> Self {
        self.graph_persistence_policy = policy;
        self
    }

    fn validate(self) -> Result<Self, ServiceError> {
        if self.max_concurrent_runs == 0
            || self.max_concurrent_operations == 0
            || self.max_concurrent_operations > 1_000
            || self.max_concurrent_operations_per_run == 0
            || self.subscriber_capacity == 0
            || self.scheduler_lease_seconds == 0
            || self.task_claim_seconds == 0
            || self.scheduler_action_budget == 0
            || self.work_coordinator.active_poll_interval.is_zero()
            || self.work_coordinator.idle_poll_min_interval.is_zero()
            || self.work_coordinator.idle_poll_max_interval.is_zero()
            || self.work_coordinator.safety_poll_interval.is_zero()
            || self
                .work_coordinator
                .notification_reconnect_interval
                .is_zero()
            || self.work_coordinator.active_poll_interval
                > self.work_coordinator.idle_poll_min_interval
            || self.work_coordinator.idle_poll_min_interval
                > self.work_coordinator.idle_poll_max_interval
            || self.work_coordinator.idle_poll_max_interval
                > self.work_coordinator.safety_poll_interval
            || !(1..=256).contains(&self.work_coordinator.claim_batch_size)
            || self.pump_interval.is_zero()
            || self.run_timeout.is_zero()
            || self.artifact_orphan_retention.is_zero()
            || self.artifact_reference_retention < Duration::from_secs(1)
            || self.artifact_reference_retention > Duration::from_secs(10 * 365 * 24 * 60 * 60)
            || self.artifact_gc_interval.is_zero()
            || self.artifact_deletion_claim_seconds == 0
            || self.artifact_deletion_claim_seconds > 3_600
            || self.artifact_read_max_bytes == 0
            || self.artifact_read_max_bytes > 256 * 1024 * 1024
            || self.public_event_nonterminal_retention < Duration::from_secs(1)
            || self.public_event_nonterminal_retention > MAX_PUBLIC_EVENT_RETENTION
            || self.public_event_prune_interval.is_zero()
            || self.live_response_body_queue_capacity == 0
            || self.live_response_control_queue_capacity == 0
            || self.live_response_max_frame_bytes == 0
            || self.live_response_max_item_bytes < self.live_response_max_frame_bytes
            || self.live_response_max_run_bytes < self.live_response_max_item_bytes
            || self.terminal_barrier_timeout.is_zero()
            || self.outbound_write_timeout.is_zero()
        {
            return Err(ServiceError::new(
                "RUN_SERVICE_CONFIG_INVALID",
                "Run service configuration is invalid",
            ));
        }
        Ok(self)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServiceError {
    code: &'static str,
    message: &'static str,
}

impl ServiceError {
    pub fn new(code: &'static str, message: &'static str) -> Self {
        Self { code, message }
    }

    pub fn code(&self) -> &'static str {
        self.code
    }
}

impl fmt::Display for ServiceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.message)
    }
}

impl Error for ServiceError {}

impl From<RepositoryError> for ServiceError {
    fn from(_: RepositoryError) -> Self {
        Self::new(
            "RUN_SERVICE_UNAVAILABLE",
            "durable Run service is unavailable",
        )
    }
}

/// Verified bytes for one ArtifactRef already published by a Run's durable
/// terminal response. The body is intentionally neither serializable nor
/// printable so logs cannot accidentally capture Artifact contents.
#[derive(PartialEq, Eq)]
pub struct PublicArtifact {
    artifact: ArtifactRef,
    bytes: Vec<u8>,
}

impl PublicArtifact {
    fn new(artifact: ArtifactRef, bytes: Vec<u8>) -> Self {
        Self { artifact, bytes }
    }

    pub fn artifact(&self) -> &ArtifactRef {
        &self.artifact
    }

    pub fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }
}

impl fmt::Debug for PublicArtifact {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PublicArtifact")
            .field("artifact", &self.artifact)
            .field("body", &"<redacted>")
            .finish()
    }
}

fn public_artifact_reference(
    snapshot: &DurableResponseSnapshot,
    run_id: &RunId,
    artifact_id: &ArtifactId,
) -> Result<Option<ArtifactRef>, ServiceError> {
    if snapshot.workflow().get("run_id").and_then(Value::as_str) != Some(run_id.as_str()) {
        return Err(ServiceError::new(
            "RUN_SERVICE_UNAVAILABLE",
            "durable response snapshot is invalid",
        ));
    }

    let mut pending = vec![(snapshot.response(), 0usize), (snapshot.workflow(), 0usize)];
    let mut visited = 0usize;
    let mut found: Option<ArtifactRef> = None;
    while let Some((value, depth)) = pending.pop() {
        visited = visited.checked_add(1).ok_or_else(|| {
            ServiceError::new(
                "RUN_SERVICE_UNAVAILABLE",
                "durable response snapshot is invalid",
            )
        })?;
        if visited > MAX_PUBLIC_ARTIFACT_SNAPSHOT_VALUES
            || depth > MAX_PUBLIC_ARTIFACT_SNAPSHOT_DEPTH
        {
            return Err(ServiceError::new(
                "RUN_SERVICE_UNAVAILABLE",
                "durable response snapshot exceeds Artifact authorization bounds",
            ));
        }
        match value {
            Value::Array(values) => {
                pending.extend(values.iter().map(|value| (value, depth + 1)));
            }
            Value::Object(object) => {
                if object.len() == 4
                    && object.contains_key("artifact_id")
                    && object.contains_key("content_hash")
                    && object.contains_key("size_bytes")
                    && object.contains_key("media_type")
                {
                    if let Ok(reference) = serde_json::from_value::<ArtifactRef>(value.clone()) {
                        if reference.artifact_id() == artifact_id {
                            if found
                                .as_ref()
                                .is_some_and(|existing| existing != &reference)
                            {
                                return Err(ServiceError::new(
                                    "RUN_SERVICE_UNAVAILABLE",
                                    "durable response snapshot has conflicting Artifact references",
                                ));
                            }
                            found = Some(reference);
                        }
                    }
                }
                pending.extend(object.values().map(|value| (value, depth + 1)));
            }
            Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
        }
    }
    Ok(found)
}

fn graph_surface_unavailable(_: RepositoryError) -> ServiceError {
    ServiceError::new(
        "GRAPH_SURFACE_UNAVAILABLE",
        "graph author surface is unavailable",
    )
}

fn terminal_only_not_installed() -> ServiceError {
    ServiceError::new(
        "TERMINAL_ONLY_UNAVAILABLE",
        "terminal-only Run service is unavailable",
    )
}

fn require_default_full_tenant(tenant_id: &str) -> Result<(), ServiceError> {
    if tenant_id == "default" {
        Ok(())
    } else {
        Err(ServiceError::new("RUN_NOT_FOUND", "Run not found"))
    }
}

fn validate_full_conversation_identity(value: &str) -> Result<(), ServiceError> {
    if value.is_empty()
        || value.len() > 256
        || value
            .chars()
            .any(|character| character.is_control() || character.is_whitespace())
    {
        return Err(ServiceError::new(
            "CONVERSATION_REQUEST_INVALID",
            "conversation request identity is invalid",
        ));
    }
    Ok(())
}

fn canonical_full_conversation_hash(value: &Value) -> Result<ContentHash, ServiceError> {
    serde_jcs::to_vec(value)
        .map(|bytes| ContentHash::from_bytes(&bytes))
        .map_err(|_| {
            ServiceError::new(
                "CONVERSATION_INPUT_INVALID",
                "conversation content is invalid",
            )
        })
}

fn full_conversation_public_id(prefix: &str, values: &[&str]) -> String {
    let mut bytes = format!("full-conversation/{prefix}/v1").into_bytes();
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

fn full_conversation_mutation_index(conversation_id: &str, stripe_count: usize) -> usize {
    let identity = full_conversation_public_id("lock", &[conversation_id]);
    let suffix = identity.rsplit('_').next().unwrap_or_default();
    let value = u64::from_str_radix(&suffix[..suffix.len().min(16)], 16).unwrap_or_default();
    usize::try_from(value).unwrap_or_default() % stripe_count.max(1)
}

fn estimated_full_conversation_tokens(value: &Value) -> Result<usize, ServiceError> {
    serde_jcs::to_vec(value)
        .map(|bytes| bytes.len().saturating_add(3) / 4)
        .map_err(|_| {
            ServiceError::new(
                "CONVERSATION_CONTENT_UNAVAILABLE",
                "conversation context is invalid",
            )
        })
}

fn full_conversation_store_error(error: RepositoryError) -> ServiceError {
    match error.code() {
        insight_durable::terminal_store::CONVERSATION_NOT_FOUND => {
            ServiceError::new("CONVERSATION_NOT_FOUND", "conversation was not found")
        }
        insight_durable::terminal_store::CONVERSATION_ARCHIVED => {
            ServiceError::new("CONVERSATION_ARCHIVED", "conversation is archived")
        }
        insight_durable::terminal_store::CONVERSATION_OWNERSHIP_MISMATCH => ServiceError::new(
            "CONVERSATION_OWNERSHIP_MISMATCH",
            "conversation ownership does not match the requested principal",
        ),
        REPOSITORY_INTENT_CONFLICT | REPOSITORY_CONSTRAINT_CONFLICT => ServiceError::new(
            "RUN_CONFLICT",
            "Conversation request conflicts with stored authority",
        ),
        _ => ServiceError::new(
            "CONVERSATION_UNAVAILABLE",
            "Conversation storage is unavailable",
        ),
    }
}

fn redact_deleted_conversation_run(record: &mut RunRecord) {
    record.input_summary = json!({"redacted": true});
    match &mut record.lifecycle {
        RunLifecycle::Completed { output } => {
            output.content = None;
            output.format = None;
            output.data = Value::Null;
        }
        RunLifecycle::Failed { error } => {
            error.code = "RUN_CONTENT_REDACTED".to_owned();
            error.message = "Run content was deleted".to_owned();
        }
        RunLifecycle::Cancelled { error } | RunLifecycle::Interrupted { error } => {
            error.code = "RUN_CONTENT_REDACTED".to_owned();
            error.message = "Run content was deleted".to_owned();
        }
        RunLifecycle::Created | RunLifecycle::Running => {}
    }
}

#[derive(Debug, Clone, Default)]
pub struct RequestMetadata {
    pub request_id: Option<String>,
}

/// Immutable identities minted by one successful Graph Author publication.
#[derive(Debug, Clone, Serialize)]
pub struct GraphPublication {
    pub agent_id: String,
    pub graph_document_id: String,
    pub definition_revision_id: String,
    pub deployment_revision_id: String,
    pub semantic_hash: String,
    pub risks: Vec<DeploymentRiskDiagnostic>,
}

/// Closed recovery policy exposed by the production API. Even when compatible
/// reuse is requested, candidates and every proof are derived server-side.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecoveryReusePolicy {
    ReuseCompatible,
    Reexecute,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveryRequestMetadata {
    pub request_id: String,
    pub expected_source_projection_version: u64,
    pub reuse_policy: RecoveryReusePolicy,
}

/// Human-authored selector for one cross-revision node mapping. Hashes and
/// compatibility proofs are deliberately absent; the service derives them
/// from the two immutable linked deployments.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MigrationNodeMappingRequest {
    pub source_node_id: String,
    pub target_node_id: String,
    #[serde(default)]
    pub ports: Option<BTreeMap<String, String>>,
    #[serde(default)]
    pub rebuild_signal_wait: bool,
    #[serde(default)]
    pub rebuild_timer: bool,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct ForkRecoveryOptions {
    /// Exact loaded Deployment Revision to fork onto. Omission retains the
    /// source pin; the service and repository both verify interface safety.
    pub target_deployment_revision_id: Option<String>,
    /// Durable checkpoint identity, never a caller-supplied content hash.
    pub checkpoint_id: Option<String>,
    /// Public input override normalized against the selected target revision.
    pub input_override: Option<Value>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RecoveryOperation {
    Redrive,
    Fork,
    Migrate,
    ContinueAsNew,
}

impl RecoveryOperation {
    const fn as_identity(self) -> &'static str {
        match self {
            Self::Redrive => "redrive",
            Self::Fork => "fork",
            Self::Migrate => "migrate",
            Self::ContinueAsNew => "continue_as_new",
        }
    }

    const fn lineage_kind(self) -> RunLineageKind {
        match self {
            Self::Redrive => RunLineageKind::Redrive,
            Self::Fork => RunLineageKind::Fork,
            Self::Migrate => RunLineageKind::Migrate,
            Self::ContinueAsNew => RunLineageKind::ContinueAsNew,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct RecoveryRunResult {
    pub operation: RecoveryOperation,
    pub request_id: String,
    pub reuse_policy: RecoveryReusePolicy,
    pub candidates_created: u32,
    pub source: RunRecord,
    pub target: RunRecord,
}

pub struct AttachedRun {
    pub run_id: String,
    pub response_id: String,
    pub request_id: String,
    pub subscription: RunSubscription,
    pub live_response: Box<dyn LiveResponseSubscriber>,
    pub live_response_broker: Arc<dyn LiveResponseBroker>,
    pub terminal_barrier_timeout: Duration,
    pub outbound_write_timeout: Duration,
    pub conversation_privacy: Option<ConversationStreamPrivacy>,
}

#[derive(Clone)]
pub struct ConversationStreamPrivacy {
    inner: Arc<ConversationStreamPrivacyInner>,
}

pub(crate) struct ConversationStreamPrivacyInner {
    cancellation: CancellationToken,
    in_flight_deliveries: AtomicUsize,
    delivery_quiesced: Notify,
}

pub struct ConversationStreamDelivery {
    inner: Arc<ConversationStreamPrivacyInner>,
}

/// Opaque response-lifetime privacy read lease. Unlike the exclusive
/// Conversation mutation stripe, leases are concurrent; DELETE cancels every
/// registered lease and waits only for frames already handed to transport. An
/// unpolled body is cancelled without blocking DELETE.
pub struct ConversationResponseVisibilityGuard {
    privacy: ConversationStreamPrivacy,
}

impl ConversationResponseVisibilityGuard {
    pub(crate) fn new(privacy: ConversationStreamPrivacy) -> Self {
        Self { privacy }
    }

    #[doc(hidden)]
    pub fn is_cancelled(&self) -> bool {
        self.privacy.is_cancelled()
    }

    #[doc(hidden)]
    pub fn try_begin_delivery(&self) -> Option<ConversationStreamDelivery> {
        self.privacy.try_begin_delivery()
    }
}

impl Drop for ConversationStreamDelivery {
    fn drop(&mut self) {
        if self
            .inner
            .in_flight_deliveries
            .fetch_sub(1, Ordering::AcqRel)
            == 1
        {
            self.inner.delivery_quiesced.notify_waiters();
        }
    }
}

impl ConversationStreamPrivacy {
    #[doc(hidden)]
    pub fn new() -> Self {
        Self {
            inner: Arc::new(ConversationStreamPrivacyInner {
                cancellation: CancellationToken::new(),
                in_flight_deliveries: AtomicUsize::new(0),
                delivery_quiesced: Notify::new(),
            }),
        }
    }

    pub fn is_cancelled(&self) -> bool {
        self.inner.cancellation.is_cancelled()
    }

    pub async fn cancelled(&self) {
        self.inner.cancellation.cancelled().await;
    }

    #[doc(hidden)]
    pub fn cancel(&self) {
        self.inner.cancellation.cancel();
    }

    pub fn try_begin_delivery(&self) -> Option<ConversationStreamDelivery> {
        if self.is_cancelled() {
            return None;
        }
        self.inner
            .in_flight_deliveries
            .fetch_add(1, Ordering::AcqRel);
        if self.is_cancelled() {
            drop(ConversationStreamDelivery {
                inner: Arc::clone(&self.inner),
            });
            return None;
        }
        Some(ConversationStreamDelivery {
            inner: Arc::clone(&self.inner),
        })
    }

    #[doc(hidden)]
    pub async fn cancel_and_wait(&self) {
        self.cancel();
        loop {
            let notified = self.inner.delivery_quiesced.notified();
            if self.inner.in_flight_deliveries.load(Ordering::Acquire) == 0 {
                break;
            }
            notified.await;
        }
    }

    pub(crate) fn downgrade(&self) -> Weak<ConversationStreamPrivacyInner> {
        Arc::downgrade(&self.inner)
    }
}

pub(crate) async fn cancel_conversation_streams(
    streams: &Mutex<BTreeMap<String, Vec<Weak<ConversationStreamPrivacyInner>>>>,
    conversation_id: &str,
) {
    let streams = streams
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .remove(conversation_id)
        .unwrap_or_default();
    let streams = streams
        .into_iter()
        .filter_map(|stream| stream.upgrade())
        .map(|inner| ConversationStreamPrivacy { inner })
        .collect::<Vec<_>>();
    for stream in &streams {
        stream.cancel();
    }
    for stream in streams {
        stream.cancel_and_wait().await;
    }
}

/// Opaque, content-free fence held while a full Conversation stream publishes
/// its terminal tail and terminal frame.
pub struct FullConversationVisibilityGuard {
    _mutation: OwnedMutexGuard<()>,
}

enum FullRunResponseVisibility {
    Standalone,
    Visible(ConversationResponseVisibilityGuard),
    Deleted,
}

#[derive(Clone, Copy)]
enum FullConversationRunAuthorization {
    Standalone,
    Bound { deleted: bool },
}

impl FullConversationRunAuthorization {
    const fn is_bound(self) -> bool {
        matches!(self, Self::Bound { .. })
    }

    const fn is_deleted(self) -> bool {
        matches!(self, Self::Bound { deleted: true })
    }
}

/// Attached creation result selected from the immutable Deployment Revision's
/// persistence policy. Full and terminal-only streaming remain separate
/// internally so terminal-only Runs never acquire a durable event-log
/// subscription.
pub enum AnyAttachedRun {
    Full(AttachedRun),
    TerminalOnly(TerminalAttachedRun),
}

pub struct AnyConversationAttachedTurn {
    pub user_message: ConversationMessageView,
    pub attached: AnyAttachedRun,
    pub replayed: bool,
}

struct FullConversationAdmissionResult {
    user_message: ConversationMessageView,
    projection: RunProjection,
    receiver: Option<broadcast::Receiver<()>>,
    live_response: Option<Box<dyn LiveResponseSubscriber>>,
    conversation_privacy: Option<ConversationStreamPrivacy>,
    replayed: bool,
}

struct ReplayWithoutLiveTail {
    run_id: RunId,
}

#[async_trait]
impl LiveResponseSubscriber for ReplayWithoutLiveTail {
    fn run_id(&self) -> &RunId {
        &self.run_id
    }

    async fn recv(&mut self) -> Result<LiveResponseDelivery, LiveResponseBrokerError> {
        Err(LiveResponseBrokerError::new(
            "LIVE_RESPONSE_REPLAY",
            "replayed attached Run has no second live response tail",
        ))
    }
}

#[derive(Debug)]
pub struct SubscriptionError {
    code: &'static str,
}

impl SubscriptionError {
    pub fn code(&self) -> &'static str {
        self.code
    }
}

pub struct RunSubscription {
    pub run_id: String,
    run_id_model: RunId,
    receiver: broadcast::Receiver<()>,
    owner: Weak<RunServiceInner>,
    position: Option<PublicEventPosition>,
    last_seq: u64,
    terminal: bool,
}

impl RunSubscription {
    pub fn last_seq(&self) -> u64 {
        self.last_seq
    }

    pub async fn load_response_snapshot(
        &self,
    ) -> Result<DurableResponseSnapshot, SubscriptionError> {
        let owner = self.owner.upgrade().ok_or(SubscriptionError {
            code: "RUN_SERVICE_UNAVAILABLE",
        })?;
        owner
            .repository
            .load_response_snapshot(&self.run_id_model)
            .await
            .map_err(|_| SubscriptionError {
                code: "RUN_SERVICE_UNAVAILABLE",
            })?
            .ok_or(SubscriptionError {
                code: "RESPONSE_SNAPSHOT_UNAVAILABLE",
            })
    }

    pub async fn recv(&mut self) -> Result<RunEvent, SubscriptionError> {
        if self.terminal {
            return Err(SubscriptionError {
                code: "SUBSCRIPTION_TERMINAL",
            });
        }
        loop {
            let Some(owner) = self.owner.upgrade() else {
                return Err(SubscriptionError {
                    code: "RUN_SERVICE_UNAVAILABLE",
                });
            };
            if owner.shutdown.is_cancelled() {
                return Err(SubscriptionError {
                    code: "RUN_SERVICE_UNAVAILABLE",
                });
            }
            match owner
                .repository
                .load_next_public_event(&self.run_id_model, self.position.as_ref())
                .await
                .map_err(|_| SubscriptionError {
                    code: "RUN_SERVICE_UNAVAILABLE",
                })? {
                OrderedPublicEventRead::Event(published) => {
                    let event = owner
                        .run_event(published.run_id(), published.safe_envelope())
                        .await
                        .map_err(|_| SubscriptionError {
                            code: "RUN_SERVICE_UNAVAILABLE",
                        })?;
                    self.last_seq = event.seq;
                    self.position = Some(published.position().clone());
                    self.terminal = published.is_terminal();
                    if self.terminal {
                        // Closing the wake sender is observable to every local
                        // receiver, so do it only after this subscription has
                        // drained every durable predecessor of the terminal
                        // position.
                        owner.remove_live_run(&self.run_id_model);
                    }
                    return Ok(event);
                }
                OrderedPublicEventRead::RetentionGap => {
                    return Err(SubscriptionError {
                        code: "SUBSCRIBER_LAGGED",
                    });
                }
                OrderedPublicEventRead::Pending | OrderedPublicEventRead::UpToDate => {}
            }

            // Broadcast and PostgreSQL notifications are wake-up hints only.
            // Lagged, duplicated, reversed or closed hint streams cannot alter
            // the durable per-Run order selected above. A short poll also
            // closes the listen-before-query race and recovers lost hints.
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_millis(100)) => {}
                received = self.receiver.recv() => match received {
                    Ok(()) | Err(broadcast::error::RecvError::Lagged(_)) => {}
                    Err(broadcast::error::RecvError::Closed) => {
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                },
            }
        }
    }
}

impl Drop for RunSubscription {
    fn drop(&mut self) {
        let Some(owner) = self.owner.upgrade() else {
            return;
        };
        owner.remove_live_run(&self.run_id_model);
        if self.terminal {
            return;
        }
        if !owner.accepting.load(Ordering::Acquire) || owner.shutdown.is_cancelled() {
            return;
        }
        let run_id = self.run_id_model.clone();
        tokio::spawn(async move {
            let _ = RunService { inner: owner }.cancel_model(run_id).await;
        });
    }
}

#[derive(Clone)]
pub struct RunService {
    inner: Arc<RunServiceInner>,
}

impl fmt::Debug for RunService {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RunService")
            .field("owner", &self.inner.owner)
            .field("accepting", &self.inner.accepting.load(Ordering::Acquire))
            .finish_non_exhaustive()
    }
}

struct RunServiceInner {
    agents: DeployedAgentCatalog,
    repository: Arc<dyn ProductionRunRepository>,
    workers: Arc<WorkerExecutorRegistry>,
    live_response_broker: Arc<dyn LiveResponseBroker>,
    artifact_store: Option<Arc<dyn WorkerArtifactStore>>,
    graph_publication_resolver: Option<Arc<dyn LeafDeploymentResolver>>,
    config: RunServiceConfig,
    owner: String,
    accepting: AtomicBool,
    /// Alternates ordinary scheduler work and model-tool work across cycles.
    /// Both queues therefore share the same process-global worker pool without
    /// allowing either continuously-ready queue to starve the other.
    model_tool_turn: AtomicBool,
    pending_work: AtomicU8,
    work_wakeup: Notify,
    work_notification_publish_pending: AtomicBool,
    work_notification_publish_wakeup: Notify,
    metrics: RunServiceMetrics,
    shutdown: CancellationToken,
    background: AsyncMutex<Vec<JoinHandle<()>>>,
    live: Mutex<BTreeMap<String, broadcast::Sender<()>>>,
    active_runs: Mutex<BTreeSet<String>>,
    terminal: RwLock<Option<TerminalOnlyRunEngine>>,
    conversation_store: RwLock<Option<Arc<dyn TerminalOnlyStore>>>,
    conversation_config: RwLock<Option<TerminalOnlyRunConfig>>,
    full_conversation_mutations: Vec<Arc<AsyncMutex<()>>>,
    full_conversation_streams: Mutex<BTreeMap<String, Vec<Weak<ConversationStreamPrivacyInner>>>>,
    /// Process-local coalescing state. `true` records a trigger that arrived
    /// while the Conversation worker was already active and requests one more
    /// bounded pass before the worker retires.
    full_conversation_summary_jobs: Mutex<BTreeMap<String, bool>>,
    full_conversation_summary_delay: RwLock<Duration>,
}

impl RunService {
    pub async fn start(
        agents: DeployedAgentCatalog,
        repository: Arc<dyn ProductionRunRepository>,
        workers: WorkerExecutorRegistry,
        config: RunServiceConfig,
    ) -> Result<Self, ServiceError> {
        Self::start_inner(agents, repository, workers, None, None, None, config).await
    }

    pub async fn start_with_artifact_store(
        agents: DeployedAgentCatalog,
        repository: Arc<dyn ProductionRunRepository>,
        workers: WorkerExecutorRegistry,
        artifact_store: Arc<dyn WorkerArtifactStore>,
        config: RunServiceConfig,
    ) -> Result<Self, ServiceError> {
        Self::start_inner(
            agents,
            repository,
            workers,
            None,
            Some(artifact_store),
            None,
            config,
        )
        .await
    }

    pub async fn start_with_graph_publication(
        agents: DeployedAgentCatalog,
        repository: Arc<dyn ProductionRunRepository>,
        workers: WorkerExecutorRegistry,
        graph_publication_resolver: Arc<dyn LeafDeploymentResolver>,
        config: RunServiceConfig,
    ) -> Result<Self, ServiceError> {
        Self::start_inner(
            agents,
            repository,
            workers,
            None,
            None,
            Some(graph_publication_resolver),
            config,
        )
        .await
    }

    pub async fn start_with_artifact_store_and_graph_publication(
        agents: DeployedAgentCatalog,
        repository: Arc<dyn ProductionRunRepository>,
        workers: WorkerExecutorRegistry,
        artifact_store: Arc<dyn WorkerArtifactStore>,
        graph_publication_resolver: Arc<dyn LeafDeploymentResolver>,
        config: RunServiceConfig,
    ) -> Result<Self, ServiceError> {
        Self::start_inner(
            agents,
            repository,
            workers,
            None,
            Some(artifact_store),
            Some(graph_publication_resolver),
            config,
        )
        .await
    }

    pub async fn start_with_artifact_store_graph_publication_and_live_response(
        agents: DeployedAgentCatalog,
        repository: Arc<dyn ProductionRunRepository>,
        workers: WorkerExecutorRegistry,
        live_response_broker: Arc<dyn LiveResponseBroker>,
        artifact_store: Arc<dyn WorkerArtifactStore>,
        graph_publication_resolver: Arc<dyn LeafDeploymentResolver>,
        config: RunServiceConfig,
    ) -> Result<Self, ServiceError> {
        Self::start_inner(
            agents,
            repository,
            workers,
            Some(live_response_broker),
            Some(artifact_store),
            Some(graph_publication_resolver),
            config,
        )
        .await
    }

    async fn start_inner(
        agents: DeployedAgentCatalog,
        repository: Arc<dyn ProductionRunRepository>,
        workers: WorkerExecutorRegistry,
        live_response_broker: Option<Arc<dyn LiveResponseBroker>>,
        artifact_store: Option<Arc<dyn WorkerArtifactStore>>,
        graph_publication_resolver: Option<Arc<dyn LeafDeploymentResolver>>,
        config: RunServiceConfig,
    ) -> Result<Self, ServiceError> {
        let config = config.validate()?;
        let live_response_broker = match live_response_broker {
            Some(broker) => broker,
            None => Arc::new(
                InMemoryLiveResponseBroker::new_with_limits(
                    config.live_response_body_queue_capacity,
                    config.live_response_control_queue_capacity,
                    LiveResponseByteLimits::new(
                        config.live_response_max_frame_bytes,
                        config.live_response_max_item_bytes,
                        config.live_response_max_run_bytes,
                    )
                    .map_err(|_| {
                        ServiceError::new(
                            "RUN_SERVICE_CONFIG_INVALID",
                            "live response broker byte limits are invalid",
                        )
                    })?,
                )
                .map_err(|_| {
                    ServiceError::new(
                        "RUN_SERVICE_CONFIG_INVALID",
                        "live response broker configuration is invalid",
                    )
                })?,
            ) as Arc<dyn LiveResponseBroker>,
        };
        if config.deployment_mode == RunServiceDeploymentMode::Production
            && repository.run_repository_capability() != RunRepositoryCapability::Production
        {
            return Err(ServiceError::new(
                PLATFORM_PRODUCTION_REQUIRES_POSTGRES,
                "production Run service requires a production-capable PostgreSQL repository",
            ));
        }
        if agents
            .list()
            .any(|agent| has_public_llm_streaming_source(agent.published()))
            && !workers.supports_public_llm_response()
        {
            return Err(ServiceError::new(
                "RUN_DEPLOYMENT_UNAVAILABLE",
                "public streaming Agents require an LLM worker bound to the live response broker",
            ));
        }
        if config.deployment_mode == RunServiceDeploymentMode::Production
            && live_response_broker.deployment_capability() != LiveResponseBrokerCapability::Shared
            && agents
                .list()
                .any(|agent| has_public_streaming_source(agent.published()))
        {
            return Err(ServiceError::new(
                PLATFORM_PRODUCTION_REQUIRES_SHARED_LIVE_RESPONSE_BROKER,
                "production Agents with public streaming sources require a shared live response broker",
            ));
        }
        if config.deployment_mode == RunServiceDeploymentMode::Production {
            let store = artifact_store.as_deref().ok_or_else(|| {
                ServiceError::new(
                    PLATFORM_PRODUCTION_REQUIRES_ARTIFACT_STORE,
                    "production Run service requires an explicit Artifact store",
                )
            })?;
            let contract = store.deployment_contract();
            if contract.capability() != ArtifactStoreDeploymentCapability::SharedFilesystem {
                return Err(ServiceError::new(
                    PLATFORM_PRODUCTION_REQUIRES_SHARED_ARTIFACT_STORE,
                    "production Run service requires a shared-filesystem Artifact store",
                ));
            }
            let command = BindArtifactStoreAuthorityCommand::shared_filesystem(
                contract.namespace().ok_or_else(|| {
                    ServiceError::new(
                        PLATFORM_PRODUCTION_REQUIRES_SHARED_ARTIFACT_STORE,
                        "shared Artifact store deployment contract is incomplete",
                    )
                })?,
                contract.store_id().ok_or_else(|| {
                    ServiceError::new(
                        PLATFORM_PRODUCTION_REQUIRES_SHARED_ARTIFACT_STORE,
                        "shared Artifact store deployment contract is incomplete",
                    )
                })?,
            )
            .map_err(|_| {
                ServiceError::new(
                    PLATFORM_PRODUCTION_REQUIRES_SHARED_ARTIFACT_STORE,
                    "shared Artifact store deployment contract is invalid",
                )
            })?;
            repository
                .bind_artifact_store_authority(command)
                .await
                .map_err(|error| {
                    if error.code() == REPOSITORY_ARTIFACT_STORE_CONFLICT {
                        ServiceError::new(
                            PLATFORM_ARTIFACT_STORE_AUTHORITY_CONFLICT,
                            "durable repository is bound to a different Artifact store",
                        )
                    } else {
                        ServiceError::from(error)
                    }
                })?;
        }
        let supplied_current = agents.list().collect::<Vec<_>>();
        let stored = repository.load_versioned_plan_catalog().await?;
        restore_persisted_catalog(&agents, &workers, &stored)?;
        for agent in agents.archived_deployments() {
            repository
                .install_versioned_plan(agent.versioned_plan())
                .await?;
        }
        for agent in supplied_current {
            if !agent.workers_available(&workers) {
                return Err(ServiceError::new(
                    "RUN_DEPLOYMENT_UNAVAILABLE",
                    "an exact worker version required by a built-in deployment is unavailable",
                ));
            }
            let head = repository
                .publish_builtin_versioned_plan(agent.versioned_plan())
                .await?;
            if head.deployment_revision_id() == agent.versioned_plan().deployment_revision_id() {
                agents.install_current(agent)?;
            } else if head.origin() != PublicationOrigin::Graph {
                return Err(ServiceError::new(
                    "RUN_DEPLOYMENT_UNAVAILABLE",
                    "durable built-in publication head failed to advance atomically",
                ));
            }
        }
        let durable_coordinator_enabled = config.graph_persistence_policy.persistence_mode()
            == PersistenceMode::Full
            || agents
                .archived_deployments()
                .any(|agent| agent.persistence_mode() == PersistenceMode::Full);
        let service = Self {
            inner: Arc::new(RunServiceInner {
                agents,
                repository,
                workers: Arc::new(workers),
                live_response_broker,
                artifact_store,
                graph_publication_resolver,
                config,
                owner: format!("runtime_{}", Uuid::new_v4().simple()),
                accepting: AtomicBool::new(true),
                model_tool_turn: AtomicBool::new(true),
                pending_work: AtomicU8::new(0),
                work_wakeup: Notify::new(),
                work_notification_publish_pending: AtomicBool::new(false),
                work_notification_publish_wakeup: Notify::new(),
                metrics: RunServiceMetrics::default(),
                shutdown: CancellationToken::new(),
                background: AsyncMutex::new(Vec::new()),
                live: Mutex::new(BTreeMap::new()),
                active_runs: Mutex::new(BTreeSet::new()),
                terminal: RwLock::new(None),
                conversation_store: RwLock::new(None),
                conversation_config: RwLock::new(None),
                full_conversation_mutations: (0..64)
                    .map(|_| Arc::new(AsyncMutex::new(())))
                    .collect(),
                full_conversation_streams: Mutex::new(BTreeMap::new()),
                full_conversation_summary_jobs: Mutex::new(BTreeMap::new()),
                full_conversation_summary_delay: RwLock::new(Duration::ZERO),
            }),
        };
        if durable_coordinator_enabled {
            service.reconcile_startup().await?;
            service.spawn_background().await;
            service
                .inner
                .wake_work(WORK_ALL, CoordinatorWakeupSource::Safety);
        }
        Ok(service)
    }

    pub fn agents(&self) -> &DeployedAgentCatalog {
        &self.inner.agents
    }

    /// Installs the process-local terminal-only runtime alongside the durable
    /// coordinator. The reader/Conversation surface remains installed even
    /// when the startup catalog has no current terminal-only revision: graph
    /// publication can add one later, and historical terminal results must
    /// remain queryable until retention removes them.
    pub async fn enable_terminal_only(
        &self,
        store: Arc<dyn TerminalOnlyStore>,
        endpoint: String,
        config: TerminalOnlyRunConfig,
    ) -> Result<(), ServiceError> {
        let summary_delay = qualification_summary_delay_from_environment()?;
        if self.terminal_engine().is_some() {
            return Err(ServiceError::new(
                "RUN_SERVICE_CONFIG_INVALID",
                "terminal-only runtime is already installed",
            ));
        }
        let artifact_store = self.inner.artifact_store.clone().ok_or_else(|| {
            ServiceError::new(
                PLATFORM_PRODUCTION_REQUIRES_ARTIFACT_STORE,
                "terminal-only Run service requires an Artifact store",
            )
        })?;
        let conversation_store = Arc::clone(&store);
        let engine = TerminalOnlyRunEngine::start(
            store,
            self.inner.agents.clone(),
            Arc::clone(&self.inner.workers),
            artifact_store,
            Arc::clone(&self.inner.live_response_broker),
            endpoint,
            config,
        )
        .await?;
        let rejected_engine = {
            let mut terminal = self
                .inner
                .terminal
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if terminal.is_some() {
                Some(engine)
            } else {
                *terminal = Some(engine);
                None
            }
        };
        if let Some(engine) = rejected_engine {
            engine.shutdown(Duration::from_secs(1)).await?;
            return Err(ServiceError::new(
                "RUN_SERVICE_CONFIG_INVALID",
                "terminal-only runtime is already installed",
            ));
        }
        *self
            .inner
            .conversation_store
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(conversation_store);
        *self
            .inner
            .conversation_config
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(config);
        *self
            .inner
            .full_conversation_summary_delay
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = summary_delay;
        Ok(())
    }

    /// Installs the shared Conversation repository for full-persistence
    /// deployments when the terminal-only execution engine itself is disabled.
    pub fn enable_conversations(
        &self,
        store: Arc<dyn TerminalOnlyStore>,
        config: TerminalOnlyRunConfig,
    ) -> Result<(), ServiceError> {
        let config = config.validate()?;
        let summary_delay = qualification_summary_delay_from_environment()?;
        if self
            .inner
            .conversation_store
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_some()
        {
            return Err(ServiceError::new(
                "RUN_SERVICE_CONFIG_INVALID",
                "Conversation repository is already installed",
            ));
        }
        *self
            .inner
            .conversation_store
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(store);
        *self
            .inner
            .conversation_config
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(config);
        *self
            .inner
            .full_conversation_summary_delay
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = summary_delay;
        Ok(())
    }

    /// Runs one bounded retention/deletion/staging cycle for a full-only
    /// Conversation deployment. This is a no-op while the terminal-only
    /// engine is installed, because that engine owns the same maintenance
    /// queues.
    #[doc(hidden)]
    pub async fn run_full_conversation_maintenance_once(&self) -> Result<(), ServiceError> {
        self.inner.run_full_conversation_maintenance(true).await
    }

    fn terminal_engine(&self) -> Option<TerminalOnlyRunEngine> {
        self.inner
            .terminal
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    /// Renders bounded-cardinality runtime metrics in the Prometheus text
    /// exposition format. Durable queue depth and PostgreSQL lock/pool
    /// telemetry remain database-exporter concerns; this surface deliberately
    /// contains no Run, task, request or tenant identifiers.
    pub fn prometheus_metrics(&self) -> String {
        let metrics = &self.inner.metrics;
        let active_runs = self
            .inner
            .active_runs
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .len();
        let pending = self.inner.pending_work.load(Ordering::Acquire);
        let backend = match self.inner.repository.run_repository_capability() {
            RunRepositoryCapability::Production => "postgres",
            RunRepositoryCapability::SingleProcessOnly => "sqlite",
        };
        let mut output = String::with_capacity(4096);
        output.push_str(
            "# HELP insight_runtime_active_runs Process-local admitted nonterminal Runs.\n\
             # TYPE insight_runtime_active_runs gauge\n",
        );
        let _ = writeln!(
            output,
            "insight_runtime_active_runs{{lifecycle_class=\"nonterminal\"}} {active_runs}"
        );
        output.push_str(
            "# HELP insight_runtime_runnable_operations Coalesced runnable work-class hints; durable queue rows remain authoritative.\n\
             # TYPE insight_runtime_runnable_operations gauge\n",
        );
        for (class, bit) in [
            ("scheduler_task", WORK_SCHEDULER_TASK),
            ("model_tool_task", WORK_MODEL_TOOL_TASK),
            ("runtime_ingress", WORK_RUNTIME_INGRESS),
            ("public_event", WORK_PUBLIC_EVENT),
            ("recovery", WORK_RECOVERY),
        ] {
            let _ = writeln!(
                output,
                "insight_runtime_runnable_operations{{work_class=\"{class}\"}} {}",
                u8::from(pending & bit != 0)
            );
        }
        output.push_str(
            "# HELP insight_runtime_executing_operations Operations currently holding a process-global execution permit.\n\
             # TYPE insight_runtime_executing_operations gauge\n",
        );
        let _ = writeln!(
            output,
            "insight_runtime_executing_operations{{work_class=\"scheduler_task\"}} {}",
            metrics.executing_scheduler.load(Ordering::Relaxed)
        );
        let _ = writeln!(
            output,
            "insight_runtime_executing_operations{{work_class=\"model_tool_task\"}} {}",
            metrics.executing_model_tool.load(Ordering::Relaxed)
        );
        let _ = writeln!(
            output,
            "insight_runtime_executing_operations{{work_class=\"recovery\"}} {}",
            metrics.executing_recovery.load(Ordering::Relaxed)
        );
        output.push_str(
            "# HELP insight_runtime_admission_total Run admission outcomes.\n\
             # TYPE insight_runtime_admission_total counter\n",
        );
        let _ = writeln!(
            output,
            "insight_runtime_admission_total{{outcome=\"accepted\"}} {}",
            metrics.admission_accepted.load(Ordering::Relaxed)
        );
        let _ = writeln!(
            output,
            "insight_runtime_admission_total{{outcome=\"capacity_rejected\"}} {}",
            metrics.admission_capacity_rejected.load(Ordering::Relaxed)
        );
        output.push_str(
            "# HELP insight_runtime_coordinator_wakeups_total WorkCoordinator wakeups by bounded source.\n\
             # TYPE insight_runtime_coordinator_wakeups_total counter\n",
        );
        for (source, counter) in [
            ("notify", &metrics.wakeup_notify),
            ("deadline", &metrics.wakeup_deadline),
            ("safety", &metrics.wakeup_safety),
            ("completion", &metrics.wakeup_completion),
        ] {
            let _ = writeln!(
                output,
                "insight_runtime_coordinator_wakeups_total{{source=\"{source}\"}} {}",
                counter.load(Ordering::Relaxed)
            );
        }
        output.push_str(
            "# HELP insight_runtime_coordinator_poll_cycles_total Durable queue poll outcomes.\n\
             # TYPE insight_runtime_coordinator_poll_cycles_total counter\n",
        );
        for (class, outcome, counter) in [
            ("scheduler_task", "claimed", &metrics.scheduler_poll_claimed),
            ("scheduler_task", "empty", &metrics.scheduler_poll_empty),
            (
                "model_tool_task",
                "claimed",
                &metrics.model_tool_poll_claimed,
            ),
            ("model_tool_task", "empty", &metrics.model_tool_poll_empty),
        ] {
            let _ = writeln!(
                output,
                "insight_runtime_coordinator_poll_cycles_total{{work_class=\"{class}\",outcome=\"{outcome}\"}} {}",
                counter.load(Ordering::Relaxed)
            );
        }
        output.push_str(
            "# HELP insight_runtime_claim_latency_seconds Durable claim latency summary.\n\
             # TYPE insight_runtime_claim_latency_seconds summary\n",
        );
        for (class, outcome, count, nanos) in [
            (
                "scheduler_task",
                "claimed",
                &metrics.scheduler_poll_claimed,
                &metrics.scheduler_claimed_nanos,
            ),
            (
                "scheduler_task",
                "empty",
                &metrics.scheduler_poll_empty,
                &metrics.scheduler_empty_nanos,
            ),
            (
                "model_tool_task",
                "claimed",
                &metrics.model_tool_poll_claimed,
                &metrics.model_tool_claimed_nanos,
            ),
            (
                "model_tool_task",
                "empty",
                &metrics.model_tool_poll_empty,
                &metrics.model_tool_empty_nanos,
            ),
        ] {
            let _ = writeln!(
                output,
                "insight_runtime_claim_latency_seconds_count{{work_class=\"{class}\",outcome=\"{outcome}\"}} {}",
                count.load(Ordering::Relaxed)
            );
            let _ = writeln!(
                output,
                "insight_runtime_claim_latency_seconds_sum{{work_class=\"{class}\",outcome=\"{outcome}\"}} {:.9}",
                nanos.load(Ordering::Relaxed) as f64 / 1_000_000_000_f64
            );
        }
        output.push_str(
            "# HELP insight_runtime_notification_listener_state Durable work listener connection state.\n\
             # TYPE insight_runtime_notification_listener_state gauge\n",
        );
        let _ = writeln!(
            output,
            "insight_runtime_notification_listener_state{{backend=\"{backend}\"}} {}",
            u8::from(
                backend == "sqlite"
                    || metrics
                        .notification_listener_connected
                        .load(Ordering::Relaxed)
            )
        );
        output.push_str(
            "# HELP insight_runtime_notification_listener_reconnects_total Durable work listener reconnect attempts.\n\
             # TYPE insight_runtime_notification_listener_reconnects_total counter\n",
        );
        let _ = writeln!(
            output,
            "insight_runtime_notification_listener_reconnects_total{{backend=\"{backend}\"}} {}",
            metrics
                .notification_listener_reconnects
                .load(Ordering::Relaxed)
        );
        output.push_str(
            "# HELP insight_runtime_notification_publisher_total Process-coalesced cross-process work hints.\n\
             # TYPE insight_runtime_notification_publisher_total counter\n",
        );
        for (outcome, counter) in [
            ("requested", &metrics.notification_publish_requests),
            ("published", &metrics.notification_published),
            ("error", &metrics.notification_publish_errors),
        ] {
            let _ = writeln!(
                output,
                "insight_runtime_notification_publisher_total{{backend=\"{backend}\",outcome=\"{outcome}\"}} {}",
                counter.load(Ordering::Relaxed)
            );
        }
        output.push_str(
            "# HELP conversation_messages_total Conversation messages committed by persistence mode and role.\n\
             # TYPE conversation_messages_total counter\n",
        );
        let _ = writeln!(
            output,
            "conversation_messages_total{{role=\"user\",persistence_mode=\"full\"}} {}",
            metrics
                .full_conversation_user_messages
                .load(Ordering::Relaxed)
        );
        let _ = writeln!(
            output,
            "conversation_messages_total{{role=\"assistant\",persistence_mode=\"full\"}} {}",
            metrics
                .full_conversation_assistant_messages
                .load(Ordering::Relaxed)
        );
        output.push_str(
            "# HELP conversation_context_messages Messages in the most recently assembled Conversation context.\n\
             # TYPE conversation_context_messages gauge\n",
        );
        let _ = writeln!(
            output,
            "conversation_context_messages{{persistence_mode=\"full\"}} {}",
            metrics
                .full_conversation_context_messages
                .load(Ordering::Relaxed)
        );
        output.push_str(
            "# HELP conversation_context_tokens Estimated tokens in the most recently assembled Conversation context.\n\
             # TYPE conversation_context_tokens gauge\n",
        );
        let _ = writeln!(
            output,
            "conversation_context_tokens{{persistence_mode=\"full\"}} {}",
            metrics
                .full_conversation_context_tokens
                .load(Ordering::Relaxed)
        );
        output.push_str(
            "# HELP conversation_summary_jobs_active Process-local coalesced Conversation summary jobs.\n\
             # TYPE conversation_summary_jobs_active gauge\n",
        );
        let summary_jobs_active = self
            .inner
            .full_conversation_summary_jobs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len();
        let _ = writeln!(
            output,
            "conversation_summary_jobs_active{{persistence_mode=\"full\"}} {summary_jobs_active}"
        );
        output.push_str(
            "# HELP conversation_summary_jobs_total Conversation summary job outcomes.\n\
             # TYPE conversation_summary_jobs_total counter\n",
        );
        let _ = writeln!(
            output,
            "conversation_summary_jobs_total{{result=\"succeeded\",persistence_mode=\"full\"}} {}",
            metrics
                .full_conversation_summary_succeeded
                .load(Ordering::Relaxed)
        );
        let _ = writeln!(
            output,
            "conversation_summary_jobs_total{{result=\"failed\",persistence_mode=\"full\"}} {}",
            metrics
                .full_conversation_summary_failed
                .load(Ordering::Relaxed)
        );
        output.push_str(
            "# HELP conversation_retention_deleted_total Rows removed by bounded full-persistence Conversation retention.\n\
             # TYPE conversation_retention_deleted_total counter\n",
        );
        let _ = writeln!(
            output,
            "conversation_retention_deleted_total{{kind=\"conversation\",persistence_mode=\"full\"}} {}",
            metrics
                .full_conversation_retention_conversations_deleted
                .load(Ordering::Relaxed)
        );
        let _ = writeln!(
            output,
            "conversation_retention_deleted_total{{kind=\"run\",persistence_mode=\"full\"}} {}",
            metrics
                .full_conversation_retention_runs_deleted
                .load(Ordering::Relaxed)
        );
        output.push_str(
            "# HELP conversation_retention_batches_total Bounded full-persistence retention batch pairs attempted.\n\
             # TYPE conversation_retention_batches_total counter\n",
        );
        let _ = writeln!(
            output,
            "conversation_retention_batches_total{{persistence_mode=\"full\"}} {}",
            metrics
                .full_conversation_retention_batches
                .load(Ordering::Relaxed)
        );
        output.push_str(
            "# HELP conversation_retention_backlog_pending Whether the last bounded full-persistence retention cycle exhausted its drain budget.\n\
             # TYPE conversation_retention_backlog_pending gauge\n",
        );
        let _ = writeln!(
            output,
            "conversation_retention_backlog_pending{{persistence_mode=\"full\"}} {}",
            u8::from(
                metrics
                    .full_conversation_retention_backlog_pending
                    .load(Ordering::Relaxed)
            )
        );
        output.push_str(
            "# HELP conversation_maintenance_batches_total Bounded full-persistence object maintenance batches attempted.\n\
             # TYPE conversation_maintenance_batches_total counter\n",
        );
        let _ = writeln!(
            output,
            "conversation_maintenance_batches_total{{queue=\"content_deletion\",persistence_mode=\"full\"}} {}",
            metrics
                .full_conversation_content_deletion_batches
                .load(Ordering::Relaxed)
        );
        let _ = writeln!(
            output,
            "conversation_maintenance_batches_total{{queue=\"artifact_staging\",persistence_mode=\"full\"}} {}",
            metrics
                .full_conversation_staging_batches
                .load(Ordering::Relaxed)
        );
        output.push_str(
            "# HELP conversation_maintenance_backlog_pending Whether the last bounded object maintenance cycle left a saturated or failed queue.\n\
             # TYPE conversation_maintenance_backlog_pending gauge\n",
        );
        let _ = writeln!(
            output,
            "conversation_maintenance_backlog_pending{{queue=\"content_deletion\",persistence_mode=\"full\"}} {}",
            u8::from(
                metrics
                    .full_conversation_content_deletion_backlog_pending
                    .load(Ordering::Relaxed)
            )
        );
        let _ = writeln!(
            output,
            "conversation_maintenance_backlog_pending{{queue=\"artifact_staging\",persistence_mode=\"full\"}} {}",
            u8::from(
                metrics
                    .full_conversation_staging_backlog_pending
                    .load(Ordering::Relaxed)
            )
        );
        if let Some(terminal) = self.terminal_engine() {
            output.push_str(&terminal.prometheus_metrics());
        }
        output
    }

    /// Lists the effective public Agent routes from one durable publication
    /// snapshot. This is the discovery authority in multi-runtime deployments;
    /// the process-local catalog remains only an executable deployment cache.
    pub async fn list_current_agents(&self) -> Result<Vec<Arc<DeployedAgent>>, ServiceError> {
        let stored = self.inner.repository.load_versioned_plan_catalog().await?;
        let mut current = stored
            .heads()
            .iter()
            .map(|head| {
                restore_persisted_head(
                    &self.inner.agents,
                    self.inner.workers.as_ref(),
                    &stored,
                    head,
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        current.sort_by(|left, right| {
            left.published()
                .metadata()
                .id
                .cmp(&right.published().metadata().id)
        });
        Ok(current)
    }

    /// Resolves one effective public Agent route from the durable publication
    /// head without requiring a Run admission to refresh process-local state.
    pub async fn get_current_agent(
        &self,
        agent_id: &str,
    ) -> Result<Arc<DeployedAgent>, ServiceError> {
        let stored = self.inner.repository.load_versioned_plan_catalog().await?;
        let head = stored
            .heads()
            .iter()
            .find(|head| head.agent_id() == agent_id)
            .ok_or_else(|| ServiceError::new("AGENT_NOT_FOUND", "agent not found"))?;
        restore_persisted_head(
            &self.inner.agents,
            self.inner.workers.as_ref(),
            &stored,
            head,
        )
    }

    pub fn begin_shutdown(&self) {
        self.inner.accepting.store(false, Ordering::Release);
        if let Some(terminal) = self.terminal_engine() {
            terminal.begin_shutdown();
        }
    }

    pub async fn shutdown(&self, grace: Duration) -> Result<(), ServiceError> {
        self.begin_shutdown();
        if let Some(terminal) = self.terminal_engine() {
            terminal.shutdown(grace).await?;
        }
        self.inner.shutdown.cancel();
        let handles = std::mem::take(&mut *self.inner.background.lock().await);
        tokio::time::timeout(grace, async {
            for handle in handles {
                handle.await.map_err(|_| {
                    ServiceError::new("RUN_SERVICE_UNAVAILABLE", "background pump panicked")
                })?;
            }
            while !self
                .inner
                .full_conversation_summary_jobs
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .is_empty()
            {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            self.inner
                .live_response_broker
                .shutdown(grace)
                .await
                .map_err(|_| {
                    ServiceError::new(
                        "RUN_SERVICE_UNAVAILABLE",
                        "live response broker shutdown failed",
                    )
                })?;
            Ok(())
        })
        .await
        .map_err(|_| ServiceError::new("RUN_SERVICE_UNAVAILABLE", "Run service drain timed out"))?
    }

    pub async fn check_readiness(&self, timeout: Duration) -> Result<(), ServiceError> {
        if !self.inner.accepting.load(Ordering::Acquire) {
            return Err(ServiceError::new(
                "RUN_SERVICE_STOPPING",
                "Run service is stopping",
            ));
        }
        tokio::time::timeout(timeout, self.inner.repository.check_production_health())
            .await
            .map_err(|_| {
                ServiceError::new("RUN_SERVICE_UNAVAILABLE", "repository probe timed out")
            })??;
        self.inner
            .live_response_broker
            .check_readiness(timeout)
            .await
            .map_err(|_| {
                ServiceError::new(
                    "RUN_SERVICE_UNAVAILABLE",
                    "live response broker readiness probe failed",
                )
            })?;
        if !self.inner.accepting.load(Ordering::Acquire) {
            return Err(ServiceError::new(
                "RUN_SERVICE_STOPPING",
                "Run service is stopping",
            ));
        }
        if self
            .terminal_engine()
            .is_some_and(|terminal| !terminal.is_ready())
        {
            return Err(ServiceError::new(
                "RUN_SERVICE_UNAVAILABLE",
                "terminal-only runtime owner lease is unavailable",
            ));
        }
        Ok(())
    }

    /// Publishes an explicit Graph Author document as an immutable Definition
    /// and Deployment Revision, then makes that deployment addressable for
    /// pinned Run admission. View state is intentionally absent from this path.
    pub async fn publish_graph(
        &self,
        agent_id: &str,
        graph: GraphAuthorDocument,
    ) -> Result<GraphPublication, ServiceError> {
        self.publish_graph_inner(agent_id, graph, None).await
    }

    /// Applies a closed semantic edit transaction to the current durable Graph
    /// head and publishes the verified candidate under a new Definition
    /// Revision. Both the semantic hash and durable route are CAS-guarded.
    pub async fn apply_graph_semantic_edits(
        &self,
        agent_id: &str,
        base_definition_revision_id: &str,
        batch: GraphSemanticEditBatch,
    ) -> Result<GraphPublication, ServiceError> {
        let base_revision = DefinitionRevisionId::new(base_definition_revision_id.to_owned())
            .map_err(|_| {
                ServiceError::new("GRAPH_DOCUMENT_NOT_FOUND", "graph revision was not found")
            })?;
        let mut graph = self
            .inner
            .repository
            .load_graph_author(agent_id, &base_revision)
            .await
            .map_err(graph_surface_unavailable)?
            .ok_or_else(|| {
                ServiceError::new("GRAPH_DOCUMENT_NOT_FOUND", "graph revision was not found")
            })?;
        graph.apply_semantic_edit_batch(batch).map_err(|error| {
            if error.code() == GRAPH_EDIT_CONFLICT {
                ServiceError::new(
                    "GRAPH_EDIT_CONFLICT",
                    "graph semantic edit base hash no longer matches",
                )
            } else {
                ServiceError::new(
                    "GRAPH_EDIT_INVALID",
                    "graph semantic edit candidate is invalid",
                )
            }
        })?;

        let catalog = self
            .inner
            .repository
            .load_versioned_plan_catalog()
            .await
            .map_err(|_| {
                ServiceError::new(
                    "GRAPH_PUBLICATION_UNAVAILABLE",
                    "graph semantic edit could not load the durable publication head",
                )
            })?;
        let expected_head = catalog
            .heads()
            .iter()
            .find(|head| head.agent_id() == agent_id)
            .filter(|head| {
                head.definition_revision_id() == &base_revision
                    && head.origin() == PublicationOrigin::Graph
            })
            .cloned()
            .ok_or_else(|| {
                ServiceError::new(
                    "GRAPH_EDIT_CONFLICT",
                    "graph semantic edit base is no longer the published revision",
                )
            })?;

        self.publish_graph_inner(agent_id, graph, Some(expected_head))
            .await
    }

    async fn publish_graph_inner(
        &self,
        agent_id: &str,
        graph: GraphAuthorDocument,
        expected_target_head: Option<PublicationHead>,
    ) -> Result<GraphPublication, ServiceError> {
        if !self.inner.accepting.load(Ordering::Acquire) {
            return Err(ServiceError::new(
                "RUN_SERVICE_STOPPING",
                "Run service is stopping",
            ));
        }
        let resolver = self
            .inner
            .graph_publication_resolver
            .as_ref()
            .ok_or_else(|| {
                ServiceError::new(
                    "GRAPH_PUBLICATION_UNAVAILABLE",
                    "graph publication is not configured",
                )
            })?;
        graph.validate().map_err(|_| {
            ServiceError::new("GRAPH_DOCUMENT_INVALID", "graph author document is invalid")
        })?;
        let published = Arc::new(
            PublishedAgent::from_verified_graph(agent_id, agent_id, "Graph-authored agent", graph)
                .map_err(|_| {
                    ServiceError::new(
                        "GRAPH_PUBLICATION_INVALID",
                        "graph publication cannot be compiled",
                    )
                })?,
        );
        let durable_catalog = self
            .inner
            .repository
            .load_versioned_plan_catalog()
            .await
            .map_err(|_| {
                ServiceError::new(
                    "GRAPH_PUBLICATION_UNAVAILABLE",
                    "graph publication could not load durable dependency routes",
                )
            })?;
        let (subflow_candidates, dependency_heads) = durable_publication_subflows(
            &published,
            &self.inner.agents,
            &self.inner.workers,
            &durable_catalog,
        )?;
        let subflows =
            subflow_registry_for_available(&published, subflow_candidates).map_err(|_| {
                ServiceError::new(
                    "GRAPH_PUBLICATION_INVALID",
                    "graph publication cannot link its subflow contracts",
                )
            })?;
        let deployed = Arc::new(
            DeployedAgent::publish_with_persistence_policy(
                Arc::clone(&published),
                resolver.as_ref(),
                subflows,
                self.inner.config.graph_persistence_policy,
            )
            .map_err(|_| {
                ServiceError::new(
                    "GRAPH_PUBLICATION_INVALID",
                    "graph publication cannot resolve its deployment bindings",
                )
            })?,
        );
        if self.inner.config.deployment_mode == RunServiceDeploymentMode::Production
            && self.inner.live_response_broker.deployment_capability()
                != LiveResponseBrokerCapability::Shared
            && has_public_streaming_source(deployed.published())
        {
            return Err(ServiceError::new(
                PLATFORM_PRODUCTION_REQUIRES_SHARED_LIVE_RESPONSE_BROKER,
                "production Graph publication with public streaming sources requires a shared live response broker",
            ));
        }
        if has_public_llm_streaming_source(deployed.published())
            && !self.inner.workers.supports_public_llm_response()
        {
            return Err(ServiceError::new(
                "GRAPH_PUBLICATION_INVALID",
                "graph publication requires a live-response-capable LLM worker",
            ));
        }
        if !deployed.workers_available(self.inner.workers.as_ref()) {
            return Err(ServiceError::new(
                "GRAPH_PUBLICATION_INVALID",
                "graph publication requires an unavailable exact worker version",
            ));
        }
        let command =
            PublishVersionedPlanCommand::new(deployed.versioned_plan().clone(), dependency_heads)
                .map_err(|_| {
                ServiceError::new(
                    "GRAPH_PUBLICATION_INVALID",
                    "graph publication dependency proposal is invalid",
                )
            })?;
        let guarded_target_publication = expected_target_head.is_some();
        let command = match expected_target_head {
            Some(head) => command
                .with_expected_target_publication_head(head)
                .map_err(|_| {
                    ServiceError::new(
                        "GRAPH_EDIT_INVALID",
                        "graph semantic edit publication guard is invalid",
                    )
                })?,
            None => command,
        };
        let publication = self
            .inner
            .repository
            .publish_versioned_plan(command)
            .await
            .map_err(|_| {
                ServiceError::new(
                    "GRAPH_PUBLICATION_UNAVAILABLE",
                    "graph publication could not be stored",
                )
            })?;
        if publication == PlanPublicationOutcome::DependencyHeadChanged {
            return Err(ServiceError::new(
                if guarded_target_publication {
                    "GRAPH_EDIT_CONFLICT"
                } else {
                    "GRAPH_PUBLICATION_CONFLICT"
                },
                "a guarded publication head changed while graph publication was in progress",
            ));
        }
        self.inner.agents.install_current(Arc::clone(&deployed))?;
        let graph = published
            .graph_author_document()
            .expect("graph-native publication retains its author document");
        Ok(GraphPublication {
            agent_id: agent_id.to_owned(),
            graph_document_id: graph.document_id().as_str().to_owned(),
            definition_revision_id: published.definition_revision_id().as_str().to_owned(),
            deployment_revision_id: deployed.deployment_revision_id().as_str().to_owned(),
            semantic_hash: graph.semantic_hash().as_str().to_owned(),
            risks: deployed.risk_diagnostics().to_vec(),
        })
    }

    pub async fn graph_author(
        &self,
        agent_id: &str,
        definition_revision_id: &str,
    ) -> Result<GraphAuthorDocument, ServiceError> {
        let revision =
            DefinitionRevisionId::new(definition_revision_id.to_owned()).map_err(|_| {
                ServiceError::new("GRAPH_DOCUMENT_NOT_FOUND", "graph revision was not found")
            })?;
        self.inner
            .repository
            .load_graph_author(agent_id, &revision)
            .await
            .map_err(graph_surface_unavailable)?
            .ok_or_else(|| {
                ServiceError::new("GRAPH_DOCUMENT_NOT_FOUND", "graph revision was not found")
            })
    }

    pub async fn graph_view(
        &self,
        agent_id: &str,
        definition_revision_id: &str,
    ) -> Result<StoredGraphView, ServiceError> {
        let revision =
            DefinitionRevisionId::new(definition_revision_id.to_owned()).map_err(|_| {
                ServiceError::new("GRAPH_DOCUMENT_NOT_FOUND", "graph revision was not found")
            })?;
        self.inner
            .repository
            .load_graph_view(agent_id, &revision)
            .await
            .map_err(graph_surface_unavailable)?
            .ok_or_else(|| ServiceError::new("GRAPH_VIEW_NOT_FOUND", "graph view was not found"))
    }

    pub async fn save_graph_view(
        &self,
        agent_id: &str,
        definition_revision_id: &str,
        expected_version: u64,
        view: ViewDocument,
    ) -> Result<StoredGraphView, ServiceError> {
        let graph = self.graph_author(agent_id, definition_revision_id).await?;
        view.validate_against(&graph).map_err(|_| {
            ServiceError::new("GRAPH_VIEW_INVALID", "graph view document is invalid")
        })?;
        match self
            .inner
            .repository
            .save_graph_view(agent_id, &graph, expected_version, &view)
            .await
            .map_err(graph_surface_unavailable)?
        {
            TransitionOutcome::Committed { result } => Ok(result),
            TransitionOutcome::ExactReplay { authoritative } => Ok(authoritative),
            TransitionOutcome::StateConflict | TransitionOutcome::StaleLease => Err(
                ServiceError::new("GRAPH_VIEW_CONFLICT", "graph view update conflicted"),
            ),
        }
    }

    pub async fn execution_graph(&self, run_id: &str) -> Result<GraphAuthorDocument, ServiceError> {
        self.execution_graph_for_principal("default", None, run_id)
            .await
    }

    pub async fn execution_graph_for_principal(
        &self,
        tenant_id: &str,
        user_id: Option<&str>,
        run_id: &str,
    ) -> Result<GraphAuthorDocument, ServiceError> {
        let run_id = RunId::new(run_id.to_owned())
            .map_err(|_| ServiceError::new("RUN_NOT_FOUND", "Run not found"))?;
        self.run_persistence_capability_for_principal(tenant_id, user_id, run_id.as_str())
            .await?;
        let projection = self
            .inner
            .repository
            .load_run(&run_id)
            .await
            .map_err(graph_surface_unavailable)?
            .ok_or_else(|| ServiceError::new("RUN_NOT_FOUND", "Run not found"))?;
        let deployment = self.inner.load_pinned_run_deployment(&projection).await?;
        if let Some(graph) = deployment.published().graph_author_document() {
            return Ok(graph.clone());
        }
        GraphAuthorDocument::from_execution_plan(deployment.published().plan().clone()).map_err(
            |_| {
                ServiceError::new(
                    "GRAPH_SURFACE_UNAVAILABLE",
                    "Run's Canonical Plan cannot be projected as a read-only execution graph",
                )
            },
        )
    }

    pub async fn trace_overlay(&self, run_id: &str) -> Result<TraceOverlay, ServiceError> {
        self.trace_overlay_for_tenant("default", run_id).await
    }

    pub async fn trace_overlay_for_tenant(
        &self,
        tenant_id: &str,
        run_id: &str,
    ) -> Result<TraceOverlay, ServiceError> {
        self.trace_overlay_for_principal(tenant_id, None, run_id)
            .await
    }

    pub async fn trace_overlay_for_principal(
        &self,
        tenant_id: &str,
        user_id: Option<&str>,
        run_id: &str,
    ) -> Result<TraceOverlay, ServiceError> {
        let run_id = RunId::new(run_id.to_owned())
            .map_err(|_| ServiceError::new("RUN_NOT_FOUND", "Run not found"))?;
        if self
            .authorize_full_conversation_principal(tenant_id, user_id, &run_id)
            .await?
            .is_deleted()
        {
            return Err(ServiceError::new("RUN_NOT_FOUND", "Run not found"));
        }
        let graph = self
            .execution_graph_for_principal(tenant_id, user_id, run_id.as_str())
            .await?;
        self.inner
            .repository
            .load_trace_overlay(graph.document_id(), &run_id)
            .await
            .map_err(graph_surface_unavailable)?
            .ok_or_else(|| ServiceError::new("GRAPH_TRACE_NOT_FOUND", "Run trace was not found"))
    }

    pub async fn reconcile_startup(&self) -> Result<u64, ServiceError> {
        self.inner.runtime_ingress_cycle().await?;
        let runs = self
            .inner
            .repository
            .list_recoverable_scheduler_runs(MAX_RECOVERY_BATCH)
            .await?;
        let count = u64::try_from(runs.len()).unwrap_or(u64::MAX);
        for run_id in runs {
            self.inner.resume_run(&run_id, false).await?;
        }
        Ok(count)
    }

    pub async fn create_detached(
        &self,
        agent_id: &str,
        input: Value,
        request: RequestMetadata,
    ) -> Result<RunRecord, ServiceError> {
        self.create_detached_for_tenant("default", agent_id, input, request)
            .await
    }

    pub async fn create_detached_for_tenant(
        &self,
        tenant_id: &str,
        agent_id: &str,
        input: Value,
        request: RequestMetadata,
    ) -> Result<RunRecord, ServiceError> {
        if self
            .inner
            .agents
            .get(agent_id)
            .is_some_and(|agent| agent.persistence_mode() == PersistenceMode::TerminalOnly)
        {
            return self
                .terminal_engine()
                .ok_or_else(terminal_only_not_installed)?
                .create_detached_for_tenant(
                    tenant_id,
                    agent_id,
                    input,
                    request
                        .request_id
                        .unwrap_or_else(|| format!("req_{}", Uuid::new_v4().simple())),
                )
                .await;
        }
        require_default_full_tenant(tenant_id)?;
        let (projection, _, _) = self
            .create_run(
                agent_id,
                input,
                request,
                PublicRunAttachment::Detached,
                false,
            )
            .await?;
        self.inner.record(&projection).await
    }

    /// Creates a Run from one exact immutable deployment instead of resolving
    /// the mutable current-agent route. This is the admission contract used by
    /// Graph Author immediately after publication.
    pub async fn create_detached_from_deployment(
        &self,
        agent_id: &str,
        deployment_revision_id: &str,
        input: Value,
        request: RequestMetadata,
    ) -> Result<RunRecord, ServiceError> {
        self.create_detached_from_deployment_for_tenant(
            "default",
            agent_id,
            deployment_revision_id,
            input,
            request,
        )
        .await
    }

    pub async fn create_detached_from_deployment_for_tenant(
        &self,
        tenant_id: &str,
        agent_id: &str,
        deployment_revision_id: &str,
        input: Value,
        request: RequestMetadata,
    ) -> Result<RunRecord, ServiceError> {
        if self
            .inner
            .agents
            .get_deployment(deployment_revision_id)
            .is_some_and(|agent| {
                agent.published().metadata().id == agent_id
                    && agent.persistence_mode() == PersistenceMode::TerminalOnly
            })
        {
            return self
                .terminal_engine()
                .ok_or_else(terminal_only_not_installed)?
                .create_detached_from_deployment(
                    tenant_id,
                    agent_id,
                    deployment_revision_id,
                    input,
                    request
                        .request_id
                        .unwrap_or_else(|| format!("req_{}", Uuid::new_v4().simple())),
                )
                .await;
        }
        require_default_full_tenant(tenant_id)?;
        let agent = self
            .inner
            .load_exact_durable_deployment(deployment_revision_id)
            .await?;
        if agent.published().metadata().id != agent_id {
            return Err(ServiceError::new(
                "GRAPH_DEPLOYMENT_NOT_FOUND",
                "graph deployment revision was not found for this agent",
            ));
        }
        let (projection, _, _) = self
            .create_run_with_agent(
                agent,
                input,
                request,
                PublicRunAttachment::Detached,
                false,
                None,
                None,
                None,
            )
            .await?;
        self.inner.record(&projection).await
    }

    pub async fn create_attached(
        &self,
        agent_id: &str,
        input: Value,
        request: RequestMetadata,
    ) -> Result<AttachedRun, ServiceError> {
        let (projection, receiver, live_response) = self
            .create_run(
                agent_id,
                input,
                request,
                PublicRunAttachment::Attached,
                true,
            )
            .await?;
        let receiver = receiver.expect("attached creation opens a live receiver");
        let live_response =
            live_response.expect("attached creation opens a live response receiver");
        Ok(AttachedRun {
            run_id: projection.run_id().as_str().to_owned(),
            response_id: projection.response_id().to_owned(),
            request_id: projection.request_id().to_owned(),
            subscription: RunSubscription {
                run_id: projection.run_id().as_str().to_owned(),
                run_id_model: projection.run_id().clone(),
                receiver,
                owner: Arc::downgrade(&self.inner),
                position: None,
                last_seq: 0,
                terminal: false,
            },
            live_response,
            live_response_broker: Arc::clone(&self.inner.live_response_broker),
            terminal_barrier_timeout: self.inner.config.terminal_barrier_timeout,
            outbound_write_timeout: self.inner.config.outbound_write_timeout,
            conversation_privacy: None,
        })
    }

    /// Creates an attached Run through the persistence engine frozen into the
    /// selected Deployment Revision.
    pub async fn create_attached_any(
        &self,
        tenant_id: &str,
        agent_id: &str,
        input: Value,
        request: RequestMetadata,
    ) -> Result<AnyAttachedRun, ServiceError> {
        if self
            .inner
            .agents
            .get(agent_id)
            .is_some_and(|agent| agent.persistence_mode() == PersistenceMode::TerminalOnly)
        {
            let attached = self
                .terminal_engine()
                .ok_or_else(terminal_only_not_installed)?
                .create_attached(
                    tenant_id,
                    agent_id,
                    input,
                    request
                        .request_id
                        .unwrap_or_else(|| format!("req_{}", Uuid::new_v4().simple())),
                )
                .await?;
            return Ok(AnyAttachedRun::TerminalOnly(attached));
        }
        require_default_full_tenant(tenant_id)?;
        self.create_attached(agent_id, input, request)
            .await
            .map(AnyAttachedRun::Full)
    }

    async fn create_run(
        &self,
        agent_id: &str,
        input: Value,
        request: RequestMetadata,
        attachment: PublicRunAttachment,
        subscribe: bool,
    ) -> Result<
        (
            RunProjection,
            Option<broadcast::Receiver<()>>,
            Option<Box<dyn LiveResponseSubscriber>>,
        ),
        ServiceError,
    > {
        if !self.inner.accepting.load(Ordering::Acquire) {
            return Err(ServiceError::new(
                "RUN_SERVICE_STOPPING",
                "Run service is stopping",
            ));
        }
        for _ in 0..MAX_PUBLICATION_ADMISSION_ATTEMPTS {
            let (agent, head) = self.resolve_durable_alias(agent_id).await?;
            match self
                .create_run_with_agent(
                    agent,
                    input.clone(),
                    request.clone(),
                    attachment,
                    subscribe,
                    Some(head),
                    None,
                    None,
                )
                .await
            {
                Err(error) if error.code() == "AGENT_PUBLICATION_CHANGED" => continue,
                result => return result,
            }
        }
        Err(ServiceError::new(
            "RUN_CONFLICT",
            "agent publication kept changing during Run admission",
        ))
    }

    async fn resolve_durable_alias(
        &self,
        agent_id: &str,
    ) -> Result<(Arc<DeployedAgent>, PublicationHead), ServiceError> {
        let stored = self.inner.repository.load_versioned_plan_catalog().await?;
        let head = stored
            .heads()
            .iter()
            .find(|head| head.agent_id() == agent_id)
            .cloned()
            .ok_or_else(|| ServiceError::new("AGENT_NOT_FOUND", "agent not found"))?;
        let deployed = restore_persisted_head(
            &self.inner.agents,
            self.inner.workers.as_ref(),
            &stored,
            &head,
        )?;
        Ok((deployed, head))
    }

    #[allow(clippy::too_many_arguments)]
    async fn create_run_with_agent(
        &self,
        agent: Arc<DeployedAgent>,
        input: Value,
        request: RequestMetadata,
        attachment: PublicRunAttachment,
        subscribe: bool,
        expected_publication_head: Option<PublicationHead>,
        run_id_override: Option<RunId>,
        full_conversation: Option<FullConversationRunAdmission>,
    ) -> Result<
        (
            RunProjection,
            Option<broadcast::Receiver<()>>,
            Option<Box<dyn LiveResponseSubscriber>>,
        ),
        ServiceError,
    > {
        if self.inner.config.deployment_mode == RunServiceDeploymentMode::Production
            && self.inner.live_response_broker.deployment_capability()
                != LiveResponseBrokerCapability::Shared
            && has_public_streaming_source(agent.published())
        {
            return Err(ServiceError::new(
                PLATFORM_PRODUCTION_REQUIRES_SHARED_LIVE_RESPONSE_BROKER,
                "public streaming admission requires a shared live response broker",
            ));
        }
        if has_public_llm_streaming_source(agent.published())
            && !self.inner.workers.supports_public_llm_response()
        {
            return Err(ServiceError::new(
                "RUN_DEPLOYMENT_UNAVAILABLE",
                "public streaming admission requires a live-response-capable LLM worker",
            ));
        }
        let normalized = agent
            .published()
            .normalize_input(input)
            .map_err(|_| ServiceError::new("INPUT_INVALID", "agent input is invalid"))?;
        let run_id = match run_id_override {
            Some(run_id) => run_id,
            None => RunId::new(format!("run_{}", Uuid::new_v4().simple())).map_err(|_| {
                ServiceError::new("RUN_CONFLICT", "failed to allocate Run identity")
            })?,
        };
        match self.inner.reserve_active_run(&run_id, false) {
            ActiveRunReservation::NewlyReserved => {}
            ActiveRunReservation::Existing => {
                return Err(ServiceError::new(
                    "RUN_CONFLICT",
                    "new Run identity is already active",
                ));
            }
            ActiveRunReservation::CapacityFull => {
                self.inner
                    .metrics
                    .admission_capacity_rejected
                    .fetch_add(1, Ordering::Relaxed);
                return Err(ServiceError::new(
                    "RUN_CAPACITY_EXCEEDED",
                    "Run capacity is exhausted",
                ));
            }
        }
        let mut active_run = ActiveRunGuard::new(&self.inner, &run_id);
        let request_id = request
            .request_id
            .filter(|value| {
                !value.is_empty() && value.len() <= 256 && !value.chars().any(char::is_control)
            })
            .unwrap_or_else(|| format!("req_{}", Uuid::new_v4().simple()));
        let receiver = subscribe.then(|| self.inner.open_subscription(&run_id));
        let live_response = if subscribe {
            Some(
                self.inner
                    .live_response_broker
                    .subscribe(run_id.clone())
                    .await
                    .map_err(|_| {
                        ServiceError::new(
                            "LIVE_RESPONSE_UNAVAILABLE",
                            "live response broker subscription is unavailable",
                        )
                    })?,
            )
        } else {
            None
        };
        let mut live_run = subscribe.then(|| LiveRunGuard::new(&self.inner, &run_id));
        let alias_admission = expected_publication_head.is_some();
        let command = CreateRunCommand::new(
            run_id.clone(),
            agent.versioned_plan(),
            normalized.value().clone(),
        )?
        .with_run_timeout(self.inner.config.run_timeout)?
        .with_artifact_reference_retention(self.inner.config.artifact_reference_retention)?
        .with_public_metadata(request_id, attachment)?;
        let command = match expected_publication_head {
            Some(head) => command.with_expected_publication_head(head)?,
            None => command,
        };
        let command = match full_conversation {
            Some(conversation) => command.with_full_conversation(conversation)?,
            None => command,
        };
        let create_key = transition_key("create", &[run_id.as_str()])?;
        match self
            .inner
            .repository
            .create_run(create_key, command)
            .await?
        {
            TransitionOutcome::Committed { .. } | TransitionOutcome::ExactReplay { .. } => {}
            TransitionOutcome::StateConflict | TransitionOutcome::StaleLease => {
                self.inner.remove_live_run(&run_id);
                return Err(if alias_admission {
                    ServiceError::new(
                        "AGENT_PUBLICATION_CHANGED",
                        "agent publication changed during Run admission",
                    )
                } else {
                    ServiceError::new("RUN_CONFLICT", "Run creation conflicted")
                });
            }
        }
        self.inner
            .metrics
            .admission_accepted
            .fetch_add(1, Ordering::Relaxed);
        if attachment == PublicRunAttachment::Detached {
            // A detached request acknowledges the durable admission boundary.
            // Execution belongs to the supervised WorkCoordinator after that
            // commit: its recovery worker contains failures, observes
            // shutdown, and is backed by durable safety discovery. Keeping
            // resume out of this request future also prevents a fast terminal
            // failure from racing the HTTP 202 response.
            active_run.retain();
            self.inner.wake_work(
                WORK_RECOVERY | WORK_PUBLIC_EVENT,
                CoordinatorWakeupSource::Completion,
            );
            let projection = self
                .inner
                .repository
                .load_run(&run_id)
                .await?
                .ok_or_else(|| {
                    ServiceError::new("RUN_NOT_FOUND", "Run disappeared after creation")
                })?;
            if projection.lifecycle().is_terminal() {
                self.inner.release_active_run(&run_id);
            }
            if let Some(live_run) = &mut live_run {
                live_run.retain();
            }
            return Ok((projection, receiver, live_response));
        }
        // Attached SSE is live-only: publish the durable admission event while
        // its receiver is already open, before Run start can produce a later
        // sequence. This makes `run.created` the observable first event
        // without introducing a replay API.
        self.inner.flush_public_events().await?;
        self.inner.resume_run(&run_id, false).await?;
        self.inner.flush_public_events().await?;
        let projection = self
            .inner
            .repository
            .load_run(&run_id)
            .await?
            .ok_or_else(|| ServiceError::new("RUN_NOT_FOUND", "Run disappeared after creation"))?;
        if !projection.lifecycle().is_terminal() {
            active_run.retain();
        }
        if let Some(live_run) = &mut live_run {
            live_run.retain();
        }
        Ok((projection, receiver, live_response))
    }

    /// Create a new Run pinned to the source's exact immutable revision. When
    /// requested, the server derives closed candidates from authoritative
    /// checkpoints; the scheduler still revalidates them at exact admission.
    pub async fn redrive(
        &self,
        source_run_id: &str,
        request: RecoveryRequestMetadata,
    ) -> Result<RecoveryRunResult, ServiceError> {
        self.validate_recovery_request(&request)?;
        let source = self.load_recovery_source(source_run_id).await?;
        let source_revision = self.load_source_revision(&source).await?;
        let target_run_id = recovery_target_id(
            RecoveryOperation::Redrive,
            source.run_id(),
            &request.request_id,
        )?;
        let candidates = if request.reuse_policy == RecoveryReusePolicy::ReuseCompatible {
            self.inner
                .repository
                .derive_compatible_reuse_candidates(
                    source.run_id(),
                    &target_run_id,
                    request.expected_source_projection_version,
                    &source_revision,
                    None,
                )
                .await
                .map_err(recovery_repository_error)?
                .ok_or_else(recovery_conflict)?
        } else {
            Vec::new()
        };
        let newly_reserved = self.inner.reserve_recovery_target(&target_run_id)?;
        let mut reservation =
            newly_reserved.then(|| ActiveRunGuard::new(&self.inner, &target_run_id));
        let key = recovery_transition_key(
            RecoveryOperation::Redrive,
            "commit",
            source.run_id(),
            &request.request_id,
        )?;
        let command = RedriveRunCommand::new(
            source.run_id().clone(),
            target_run_id.clone(),
            request.expected_source_projection_version,
            candidates,
        )
        .and_then(|command| command.with_run_timeout(self.inner.config.run_timeout))
        .map_err(|_| recovery_request_invalid())?;
        let receipt = match self
            .inner
            .repository
            .redrive_run(key, command)
            .await
            .map_err(recovery_repository_error)?
        {
            TransitionOutcome::Committed { result } => result,
            TransitionOutcome::ExactReplay { authoritative } => authoritative,
            TransitionOutcome::StateConflict | TransitionOutcome::StaleLease => {
                return Err(recovery_conflict())
            }
        };
        let result = self
            .finish_recovery(
                RecoveryOperation::Redrive,
                &request,
                source.run_id(),
                &target_run_id,
                &receipt,
            )
            .await?;
        if let Some(reservation) = &mut reservation {
            reservation.retain();
        }
        Ok(result)
    }

    /// Fork from the newest repository-verified scheduler checkpoint while
    /// retaining the source revision and input.
    pub async fn fork(
        &self,
        source_run_id: &str,
        request: RecoveryRequestMetadata,
    ) -> Result<RecoveryRunResult, ServiceError> {
        self.fork_with_options(source_run_id, ForkRecoveryOptions::default(), request)
            .await
    }

    /// Fork using public selectors. A checkpoint ID selects durable history,
    /// a Deployment Revision selects an installed target, and input is always
    /// normalized against that target. None of these fields is accepted as a
    /// compatibility or content-hash proof.
    pub async fn fork_with_options(
        &self,
        source_run_id: &str,
        options: ForkRecoveryOptions,
        request: RecoveryRequestMetadata,
    ) -> Result<RecoveryRunResult, ServiceError> {
        self.validate_recovery_request(&request)?;
        let source = self.load_recovery_source(source_run_id).await?;
        let source_deployment = self.load_source_deployment(&source).await?;
        let target_deployment = match options.target_deployment_revision_id.as_deref() {
            Some(revision) => self
                .inner
                .load_exact_durable_deployment(revision)
                .await
                .map_err(|error| {
                    if error.code() == "RUN_DEPLOYMENT_UNAVAILABLE" {
                        ServiceError::new(
                            "FORK_REVISION_UNAVAILABLE",
                            "selected fork Deployment Revision is unavailable",
                        )
                    } else {
                        error
                    }
                })?,
            None => Arc::clone(&source_deployment),
        };
        let source_plan = source_deployment.published().plan();
        let target_plan = target_deployment.published().plan();
        if source_plan.metadata().input_contract() != target_plan.metadata().input_contract()
            || source_plan.metadata().output_type() != target_plan.metadata().output_type()
        {
            return Err(ServiceError::new(
                "FORK_REVISION_INCOMPATIBLE",
                "selected fork revision has an incompatible public interface",
            ));
        }
        let raw_target_input = match options.input_override {
            Some(value) => value,
            None => self
                .inner
                .repository
                .load_production_run_input(source.run_id())
                .await?
                .ok_or_else(|| {
                    ServiceError::new("RUN_NOT_FOUND", "source Run input disappeared")
                })?,
        };
        let target_input = target_deployment
            .published()
            .normalize_input(raw_target_input)
            .map_err(|_| ServiceError::new("INPUT_INVALID", "fork input is invalid"))?;
        let target_revision = recovery_revision_for_agent(target_deployment.as_ref())?;
        let checkpoint = match options.checkpoint_id {
            Some(checkpoint_id) => {
                let checkpoint_id = SchedulerCheckpointId::parse(checkpoint_id)
                    .map_err(|_| recovery_request_invalid())?;
                self.inner
                    .repository
                    .authoritative_scheduler_checkpoint_by_id(
                        source.run_id(),
                        request.expected_source_projection_version,
                        &checkpoint_id,
                    )
                    .await
                    .map_err(recovery_repository_error)?
            }
            None => self
                .inner
                .repository
                .latest_authoritative_scheduler_checkpoint(
                    source.run_id(),
                    request.expected_source_projection_version,
                )
                .await
                .map_err(recovery_repository_error)?,
        }
        .ok_or_else(|| {
            ServiceError::new(
                "RECOVERY_CHECKPOINT_UNAVAILABLE",
                "the selected authoritative scheduler checkpoint is unavailable",
            )
        })?;
        let target_run_id = recovery_target_id(
            RecoveryOperation::Fork,
            source.run_id(),
            &request.request_id,
        )?;
        let candidates = if request.reuse_policy == RecoveryReusePolicy::ReuseCompatible {
            self.inner
                .repository
                .derive_compatible_reuse_candidates(
                    source.run_id(),
                    &target_run_id,
                    request.expected_source_projection_version,
                    &target_revision,
                    Some(&checkpoint),
                )
                .await
                .map_err(recovery_repository_error)?
                .ok_or_else(recovery_conflict)?
        } else {
            Vec::new()
        };
        let newly_reserved = self.inner.reserve_recovery_target(&target_run_id)?;
        let mut reservation =
            newly_reserved.then(|| ActiveRunGuard::new(&self.inner, &target_run_id));
        let key = recovery_transition_key(
            RecoveryOperation::Fork,
            "commit",
            source.run_id(),
            &request.request_id,
        )?;
        let command = ForkRunCommand::new(
            source.run_id().clone(),
            target_run_id.clone(),
            request.expected_source_projection_version,
            target_revision,
            checkpoint,
            candidates,
        )
        .and_then(|command| command.with_target_input(target_input.value().clone()))
        .and_then(|command| command.with_run_timeout(self.inner.config.run_timeout))
        .map_err(|_| recovery_request_invalid())?;
        let receipt = match self
            .inner
            .repository
            .fork_run(key, command)
            .await
            .map_err(recovery_repository_error)?
        {
            TransitionOutcome::Committed { result } => result,
            TransitionOutcome::ExactReplay { authoritative } => authoritative,
            TransitionOutcome::StateConflict | TransitionOutcome::StaleLease => {
                return Err(recovery_conflict())
            }
        };
        let result = self
            .finish_recovery(
                RecoveryOperation::Fork,
                &request,
                source.run_id(),
                &target_run_id,
                &receipt,
            )
            .await?;
        if let Some(reservation) = &mut reservation {
            reservation.retain();
        }
        Ok(result)
    }

    /// Atomically cross a generation boundary on the same immutable revision.
    /// The source must already be paused and fully drained; no Wait, Timer,
    /// Signal, Activation, or effect state is transferred.
    pub async fn continue_as_new(
        &self,
        source_run_id: &str,
        target_input: Value,
        request: RecoveryRequestMetadata,
    ) -> Result<RecoveryRunResult, ServiceError> {
        self.validate_recovery_request(&request)?;
        if request.reuse_policy != RecoveryReusePolicy::Reexecute {
            return Err(recovery_request_invalid());
        }
        let source = self.load_recovery_source(source_run_id).await?;
        let deployed = self.load_source_deployment(&source).await?;
        let normalized = deployed
            .published()
            .normalize_input(target_input)
            .map_err(|_| ServiceError::new("INPUT_INVALID", "agent input is invalid"))?;
        let target_run_id = recovery_target_id(
            RecoveryOperation::ContinueAsNew,
            source.run_id(),
            &request.request_id,
        )?;
        let newly_reserved = self.inner.reserve_recovery_target(&target_run_id)?;
        let mut reservation =
            newly_reserved.then(|| ActiveRunGuard::new(&self.inner, &target_run_id));
        let key = recovery_transition_key(
            RecoveryOperation::ContinueAsNew,
            "commit",
            source.run_id(),
            &request.request_id,
        )?;
        let command = ContinueAsNewCommand::new(
            source.run_id().clone(),
            target_run_id.clone(),
            request.expected_source_projection_version,
            normalized.value().clone(),
            Vec::new(),
        )
        .and_then(|command| command.with_run_timeout(self.inner.config.run_timeout))
        .map_err(|_| recovery_request_invalid())?;
        let receipt = match self
            .inner
            .repository
            .continue_as_new(key, command)
            .await
            .map_err(recovery_repository_error)?
        {
            TransitionOutcome::Committed { result } => result,
            TransitionOutcome::ExactReplay { authoritative } => authoritative,
            TransitionOutcome::StateConflict | TransitionOutcome::StaleLease => {
                return Err(recovery_not_ready())
            }
        };
        let result = self
            .finish_recovery(
                RecoveryOperation::ContinueAsNew,
                &request,
                source.run_id(),
                &target_run_id,
                &receipt,
            )
            .await?;
        if let Some(reservation) = &mut reservation {
            reservation.retain();
        }
        Ok(result)
    }

    /// Perform the durable two-phase migrate handoff. A retry with the same
    /// request identity first replays the begin receipt and then safely retries
    /// finalize, including after a process failure between the two commits.
    pub async fn migrate(
        &self,
        source_run_id: &str,
        target_agent_id: &str,
        target_input: Value,
        mapping_requests: Vec<MigrationNodeMappingRequest>,
        request: RecoveryRequestMetadata,
    ) -> Result<RecoveryRunResult, ServiceError> {
        self.validate_recovery_request(&request)?;
        let source = self.load_recovery_source(source_run_id).await?;
        let source_agent = self.load_source_deployment(&source).await?;
        let target_run_id = recovery_target_id(
            RecoveryOperation::Migrate,
            source.run_id(),
            &request.request_id,
        )?;
        let begin_key = recovery_transition_key(
            RecoveryOperation::Migrate,
            "begin",
            source.run_id(),
            &request.request_id,
        )?;
        let pending = self
            .inner
            .repository
            .load_pending_migration_intent(source.run_id(), &begin_key)
            .await
            .map_err(recovery_repository_error)?;
        let begin = if let Some(pending) = pending {
            // A committed begin is the immutable authority after a crash. Load
            // its old deployment even if the public alias has since advanced,
            // then interpret the retry against that old schema. Rebuilding the
            // complete command also keeps changed caller input/mappings as an
            // idempotency conflict instead of silently adopting them.
            let target_agent = self
                .load_pending_migration_target(&pending, target_agent_id)
                .await?;
            let normalized = target_agent
                .published()
                .normalize_input(target_input)
                .map_err(|_| recovery_conflict())?;
            let mappings = derive_migration_mappings(
                source_agent.as_ref(),
                target_agent.as_ref(),
                &mapping_requests,
            )
            .map_err(|_| recovery_conflict())?;
            let replay = BeginMigrationCommand::new(
                source.run_id().clone(),
                target_run_id.clone(),
                request.expected_source_projection_version,
                recovery_revision_for_agent(target_agent.as_ref())?,
                normalized.value().clone(),
                mappings,
                pending.reuse_candidates().to_vec(),
            )
            .and_then(|command| {
                command.with_run_timeout(Duration::from_millis(pending.target_timeout_ms()))
            })
            .map_err(|_| recovery_conflict())?;
            let replay = if let Some(agent_id) = pending.expected_target_publication_agent() {
                recovery_contract_adapter::with_expected_target_publication_agent(
                    replay,
                    agent_id.to_owned(),
                )
                .map_err(|_| recovery_conflict())?
            } else {
                replay
            };
            if replay != pending {
                return Err(recovery_conflict());
            }
            replay
        } else {
            if request.reuse_policy == RecoveryReusePolicy::ReuseCompatible
                && mapping_requests.is_empty()
            {
                return Err(migration_mapping_incompatible());
            }
            // Resolve the mutable target alias from one durable catalog
            // snapshot. The exact head is carried into begin_migration and
            // compared under the repository transaction lock, so a stale
            // runtime cannot silently pin its process-local deployment.
            let (target_agent, target_head) = self.resolve_durable_alias(target_agent_id).await?;
            let normalized = target_agent
                .published()
                .normalize_input(target_input)
                .map_err(|_| ServiceError::new("INPUT_INVALID", "agent input is invalid"))?;
            let target_revision = recovery_revision_for_agent(target_agent.as_ref())?;
            let mappings = derive_migration_mappings(
                source_agent.as_ref(),
                target_agent.as_ref(),
                &mapping_requests,
            )?;
            let pending_waits = self
                .inner
                .repository
                .load_pending_migration_waits(source.run_id())
                .await
                .map_err(recovery_repository_error)?;
            validate_pending_migration_waits(&pending_waits, &mappings)?;
            let candidates = if request.reuse_policy == RecoveryReusePolicy::ReuseCompatible {
                self.inner
                    .repository
                    .derive_migration_reuse_candidates(
                        source.run_id(),
                        &target_run_id,
                        request.expected_source_projection_version,
                        &target_revision,
                        &mappings,
                    )
                    .await
                    .map_err(recovery_repository_error)?
                    .ok_or_else(recovery_conflict)?
            } else {
                Vec::new()
            };
            BeginMigrationCommand::new(
                source.run_id().clone(),
                target_run_id.clone(),
                request.expected_source_projection_version,
                target_revision,
                normalized.value().clone(),
                mappings,
                candidates,
            )
            .and_then(|command| command.with_run_timeout(self.inner.config.run_timeout))
            .and_then(|command| command.with_expected_target_publication_head(target_head))
            .map_err(|_| recovery_request_invalid())?
        };
        let newly_reserved = self.inner.reserve_recovery_target(&target_run_id)?;
        let mut reservation =
            newly_reserved.then(|| ActiveRunGuard::new(&self.inner, &target_run_id));
        let intent = match self
            .inner
            .repository
            .begin_migration(begin_key.clone(), begin)
            .await
            .map_err(recovery_repository_error)?
        {
            TransitionOutcome::Committed { result } => result,
            TransitionOutcome::ExactReplay { authoritative } => authoritative,
            TransitionOutcome::StateConflict | TransitionOutcome::StaleLease => {
                return Err(recovery_not_ready())
            }
        };
        let finalize_key = recovery_transition_key(
            RecoveryOperation::Migrate,
            "finalize",
            source.run_id(),
            &request.request_id,
        )?;
        let finalize = FinalizeMigrationCommand::new(
            source.run_id().clone(),
            target_run_id.clone(),
            intent.source_event().projection_version(),
            begin_key,
        )
        .map_err(|_| recovery_request_invalid())?;
        let receipt = match self
            .inner
            .repository
            .finalize_migration(finalize_key, finalize)
            .await
            .map_err(recovery_repository_error)?
        {
            TransitionOutcome::Committed { result } => result,
            TransitionOutcome::ExactReplay { authoritative } => authoritative,
            TransitionOutcome::StateConflict | TransitionOutcome::StaleLease => {
                return Err(recovery_conflict())
            }
        };
        let result = self
            .finish_recovery(
                RecoveryOperation::Migrate,
                &request,
                source.run_id(),
                &target_run_id,
                &receipt,
            )
            .await?;
        if let Some(reservation) = &mut reservation {
            reservation.retain();
        }
        Ok(result)
    }

    fn validate_recovery_request(
        &self,
        request: &RecoveryRequestMetadata,
    ) -> Result<(), ServiceError> {
        if !self.inner.accepting.load(Ordering::Acquire) {
            return Err(ServiceError::new(
                "RUN_SERVICE_STOPPING",
                "Run service is stopping",
            ));
        }
        if request.request_id.is_empty()
            || request.request_id.len() > 256
            || request.request_id.chars().any(char::is_control)
        {
            return Err(recovery_request_invalid());
        }
        Ok(())
    }

    async fn load_recovery_source(&self, run_id: &str) -> Result<RunProjection, ServiceError> {
        let run_id = RunId::new(run_id.to_owned())
            .map_err(|_| ServiceError::new("RUN_NOT_FOUND", "Run not found"))?;
        self.inner
            .repository
            .load_run(&run_id)
            .await?
            .ok_or_else(|| ServiceError::new("RUN_NOT_FOUND", "Run not found"))
    }

    async fn load_source_deployment(
        &self,
        source: &RunProjection,
    ) -> Result<Arc<DeployedAgent>, ServiceError> {
        self.inner.load_pinned_run_deployment(source).await
    }

    async fn load_pending_migration_target(
        &self,
        pending: &BeginMigrationCommand,
        requested_agent_id: &str,
    ) -> Result<Arc<DeployedAgent>, ServiceError> {
        let deployment_revision = pending
            .target_revision()
            .revision()
            .deployment_revision_id()
            .as_str();
        let deployed = self
            .inner
            .load_exact_durable_deployment(deployment_revision)
            .await?;
        if deployed.published().metadata().id != requested_agent_id
            || recovery_revision_for_agent(deployed.as_ref())? != *pending.target_revision()
        {
            return Err(recovery_conflict());
        }
        if !deployed.workers_available(self.inner.workers.as_ref()) {
            return Err(ServiceError::new(
                "RUN_DEPLOYMENT_UNAVAILABLE",
                "an exact worker version required by the pending migration is unavailable",
            ));
        }
        Ok(deployed)
    }

    async fn load_source_revision(
        &self,
        source: &RunProjection,
    ) -> Result<RecoveryRevisionSpec, ServiceError> {
        let deployed = self.load_source_deployment(source).await?;
        recovery_revision_for_agent(deployed.as_ref())
    }

    async fn finish_recovery(
        &self,
        operation: RecoveryOperation,
        request: &RecoveryRequestMetadata,
        source_run_id: &RunId,
        target_run_id: &RunId,
        receipt: &RecoveryRunReceipt,
    ) -> Result<RecoveryRunResult, ServiceError> {
        if receipt.target_created().run_id() != target_run_id
            || receipt.lineage().source_run_id() != source_run_id
            || receipt.lineage().target_run_id() != target_run_id
            || receipt.lineage().kind() != operation.lineage_kind()
        {
            return Err(ServiceError::new(
                "RUN_SERVICE_UNAVAILABLE",
                "recovery receipt does not match the requested lineage",
            ));
        }
        self.inner.resume_run(target_run_id, false).await?;
        self.inner.flush_public_events().await?;
        let source = self
            .inner
            .repository
            .load_run(source_run_id)
            .await?
            .ok_or_else(|| ServiceError::new("RUN_NOT_FOUND", "source Run disappeared"))?;
        let target = self
            .inner
            .repository
            .load_run(target_run_id)
            .await?
            .ok_or_else(|| ServiceError::new("RUN_NOT_FOUND", "target Run disappeared"))?;
        if source.lifecycle().is_terminal() {
            self.inner.release_active_run(source_run_id);
        }
        Ok(RecoveryRunResult {
            operation,
            request_id: request.request_id.clone(),
            reuse_policy: request.reuse_policy,
            candidates_created: receipt.candidates_created(),
            source: self.inner.record(&source).await?,
            target: self.inner.record(&target).await?,
        })
    }

    pub async fn get_run(&self, run_id: &str) -> Result<RunRecord, ServiceError> {
        self.get_run_for_tenant("default", run_id).await
    }

    pub async fn get_run_for_tenant(
        &self,
        tenant_id: &str,
        run_id: &str,
    ) -> Result<RunRecord, ServiceError> {
        self.get_run_for_principal(tenant_id, None, run_id).await
    }

    pub async fn get_run_for_principal(
        &self,
        tenant_id: &str,
        user_id: Option<&str>,
        run_id: &str,
    ) -> Result<RunRecord, ServiceError> {
        let run_id = RunId::new(run_id.to_owned())
            .map_err(|_| ServiceError::new("RUN_NOT_FOUND", "Run not found"))?;
        if let Some(projection) = self.inner.repository.load_run(&run_id).await? {
            let authorization = self
                .authorize_full_conversation_principal(tenant_id, user_id, &run_id)
                .await?;
            let mut record = self.inner.record(&projection).await?;
            if authorization.is_deleted() {
                redact_deleted_conversation_run(&mut record);
            }
            return Ok(record);
        }
        self.terminal_engine()
            .ok_or_else(|| ServiceError::new("RUN_NOT_FOUND", "Run not found"))?
            .get_run_for_principal(tenant_id, user_id, run_id.as_str())
            .await
    }

    pub async fn run_persistence_capability(
        &self,
        tenant_id: &str,
        run_id: &str,
    ) -> Result<RunPersistenceCapability, ServiceError> {
        self.run_persistence_capability_for_principal(tenant_id, None, run_id)
            .await
    }

    pub async fn run_persistence_capability_for_principal(
        &self,
        tenant_id: &str,
        user_id: Option<&str>,
        run_id: &str,
    ) -> Result<RunPersistenceCapability, ServiceError> {
        let run_id = RunId::new(run_id.to_owned())
            .map_err(|_| ServiceError::new("RUN_NOT_FOUND", "Run not found"))?;
        if self.inner.repository.load_run(&run_id).await?.is_some() {
            let authorization = self
                .authorize_full_conversation_principal(tenant_id, user_id, &run_id)
                .await?;
            return Ok(if authorization.is_bound() {
                RunPersistenceCapability::FULL_CONVERSATION
            } else {
                RunPersistenceCapability::FULL
            });
        }
        self.terminal_engine()
            .ok_or_else(|| ServiceError::new("RUN_NOT_FOUND", "Run not found"))?
            .get_run_for_principal(tenant_id, user_id, run_id.as_str())
            .await?;
        Ok(RunPersistenceCapability::TERMINAL_ONLY)
    }

    /// Fails with the stable public 422 capability code when a terminal-only
    /// or restart-only Run is sent to an operator recovery surface.
    pub async fn require_full_recovery_capability(
        &self,
        tenant_id: &str,
        run_id: &str,
    ) -> Result<(), ServiceError> {
        self.require_full_recovery_capability_for_principal(tenant_id, None, run_id)
            .await
    }

    pub async fn require_full_recovery_capability_for_principal(
        &self,
        tenant_id: &str,
        user_id: Option<&str>,
        run_id: &str,
    ) -> Result<(), ServiceError> {
        if self
            .run_persistence_capability_for_principal(tenant_id, user_id, run_id)
            .await?
            .persistence_mode
            == PersistenceMode::TerminalOnly
        {
            return Err(self
                .terminal_engine()
                .map_or_else(terminal_only_not_installed, |engine| {
                    engine.capability_unavailable()
                }));
        }
        let run_id = RunId::new(run_id.to_owned())
            .map_err(|_| ServiceError::new("RUN_NOT_FOUND", "Run not found"))?;
        let authorization = self
            .authorize_full_conversation_principal(tenant_id, user_id, &run_id)
            .await?;
        if authorization.is_deleted() {
            return Err(ServiceError::new("RUN_NOT_FOUND", "Run not found"));
        }
        if authorization.is_bound() {
            return Err(ServiceError::new(
                "RUN_FULL_CONVERSATION_CAPABILITY_UNAVAILABLE",
                "Conversation-bound full Runs do not support recovery lineage",
            ));
        }
        Ok(())
    }

    pub async fn create_conversation(
        &self,
        tenant_id: &str,
        user_id: &str,
        agent_id: &str,
        request_id: &str,
    ) -> Result<insight_durable::terminal_store::Conversation, ServiceError> {
        let agent = self
            .inner
            .agents
            .get(agent_id)
            .ok_or_else(|| ServiceError::new("AGENT_NOT_FOUND", "agent not found"))?;
        if agent.persistence_mode() == PersistenceMode::TerminalOnly {
            return self
                .terminal_engine()
                .ok_or_else(terminal_only_not_installed)?
                .create_conversation(tenant_id, user_id, agent_id, request_id)
                .await;
        }
        validate_full_conversation_identity(tenant_id)?;
        validate_full_conversation_identity(user_id)?;
        validate_full_conversation_identity(request_id)?;
        let (store, config) = self.conversation_components()?;
        if !config.conversations_enabled {
            return Err(ServiceError::new(
                "CONVERSATION_UNAVAILABLE",
                "Conversation API is disabled",
            ));
        }
        store
            .create_conversation(insight_durable::terminal_store::NewConversation {
                conversation_id: full_conversation_public_id(
                    "conv",
                    &[tenant_id, user_id, request_id],
                ),
                tenant_id: tenant_id.to_owned(),
                user_id: user_id.to_owned(),
                agent_id: agent_id.to_owned(),
                persistence_mode: PersistenceMode::Full,
                deployment_revision_id: agent.deployment_revision_id().clone(),
                created_at: chrono::Utc::now(),
            })
            .await
            .map(|outcome| outcome.conversation)
            .map_err(full_conversation_store_error)
    }

    pub fn conversation_page_limits(&self) -> Result<(u32, u32), ServiceError> {
        let (_, config) = self.conversation_components()?;
        config
            .conversations_enabled
            .then_some((
                config.message_page_size_default,
                config.message_page_size_max,
            ))
            .ok_or_else(|| {
                ServiceError::new("CONVERSATION_UNAVAILABLE", "Conversation API is disabled")
            })
    }

    pub async fn get_conversation(
        &self,
        conversation_id: &str,
        tenant_id: &str,
        user_id: &str,
    ) -> Result<insight_durable::terminal_store::Conversation, ServiceError> {
        if let Some(engine) = self.terminal_engine() {
            return engine
                .get_conversation(conversation_id, tenant_id, user_id)
                .await;
        }
        let (store, config) = self.conversation_components()?;
        if !config.conversations_enabled {
            return Err(ServiceError::new(
                "CONVERSATION_UNAVAILABLE",
                "Conversation API is disabled",
            ));
        }
        store
            .get_conversation(insight_durable::terminal_store::ConversationQuery {
                conversation_id: conversation_id.to_owned(),
                tenant_id: tenant_id.to_owned(),
                user_id: user_id.to_owned(),
            })
            .await
            .map_err(full_conversation_store_error)?
            .ok_or_else(|| {
                ServiceError::new("CONVERSATION_NOT_FOUND", "conversation was not found")
            })
    }

    pub async fn list_conversation_messages(
        &self,
        conversation_id: &str,
        tenant_id: &str,
        user_id: &str,
        before: Option<insight_durable::terminal_store::MessageCursor>,
        limit: u32,
    ) -> Result<ConversationMessagePageView, ServiceError> {
        if let Some(engine) = self.terminal_engine() {
            return engine
                .list_conversation_messages(conversation_id, tenant_id, user_id, before, limit)
                .await;
        }
        let (store, config) = self.conversation_components()?;
        if !config.conversations_enabled || limit == 0 || limit > config.message_page_size_max {
            return Err(ServiceError::new(
                "CONVERSATION_REQUEST_INVALID",
                "conversation message page size is invalid",
            ));
        }
        let page = store
            .page_conversation_messages(insight_durable::terminal_store::MessagePageQuery {
                conversation: insight_durable::terminal_store::ConversationQuery {
                    conversation_id: conversation_id.to_owned(),
                    tenant_id: tenant_id.to_owned(),
                    user_id: user_id.to_owned(),
                },
                before,
                limit,
            })
            .await
            .map_err(full_conversation_store_error)?
            .ok_or_else(|| {
                ServiceError::new("CONVERSATION_NOT_FOUND", "conversation was not found")
            })?;
        let mut messages = Vec::with_capacity(page.messages.len());
        for message in page.messages {
            messages.push(self.full_conversation_message_view(message).await?);
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
        let conversation = self
            .get_conversation(conversation_id, tenant_id, user_id)
            .await?;
        if conversation.persistence_mode == PersistenceMode::Full {
            return self
                .create_full_conversation_detached_turn(conversation, request_id, content)
                .await;
        }
        self.terminal_engine()
            .ok_or_else(terminal_only_not_installed)?
            .create_conversation_detached_turn(
                conversation_id,
                tenant_id,
                user_id,
                request_id,
                content,
            )
            .await
    }

    pub async fn create_conversation_attached_turn(
        &self,
        conversation_id: &str,
        tenant_id: &str,
        user_id: &str,
        request_id: &str,
        content: Value,
    ) -> Result<AnyConversationAttachedTurn, ServiceError> {
        let conversation = self
            .get_conversation(conversation_id, tenant_id, user_id)
            .await?;
        if conversation.persistence_mode == PersistenceMode::Full {
            return self
                .create_full_conversation_attached_turn(conversation, request_id, content)
                .await;
        }
        let turn: ConversationAttachedTurn = self
            .terminal_engine()
            .ok_or_else(terminal_only_not_installed)?
            .create_conversation_attached_turn(
                conversation_id,
                tenant_id,
                user_id,
                request_id,
                content,
            )
            .await?;
        Ok(AnyConversationAttachedTurn {
            user_message: turn.user_message,
            attached: AnyAttachedRun::TerminalOnly(turn.attached),
            replayed: turn.replayed,
        })
    }

    async fn create_full_conversation_detached_turn(
        &self,
        conversation: insight_durable::terminal_store::Conversation,
        request_id: &str,
        content: Value,
    ) -> Result<ConversationDetachedTurn, ServiceError> {
        let summary_conversation = conversation.clone();
        let admitted = self
            .admit_full_conversation_turn(
                conversation,
                request_id,
                content,
                PublicRunAttachment::Detached,
                false,
            )
            .await?;
        if !admitted.replayed {
            self.schedule_full_conversation_summary_after_run(
                summary_conversation,
                admitted.projection.run_id().clone(),
            );
        }
        Ok(ConversationDetachedTurn {
            user_message: admitted.user_message,
            run: self.inner.record(&admitted.projection).await?,
            replayed: admitted.replayed,
        })
    }

    async fn create_full_conversation_attached_turn(
        &self,
        conversation: insight_durable::terminal_store::Conversation,
        request_id: &str,
        content: Value,
    ) -> Result<AnyConversationAttachedTurn, ServiceError> {
        let summary_conversation = conversation.clone();
        let admitted = self
            .admit_full_conversation_turn(
                conversation,
                request_id,
                content,
                PublicRunAttachment::Attached,
                true,
            )
            .await?;
        if !admitted.replayed {
            self.schedule_full_conversation_summary_after_run(
                summary_conversation,
                admitted.projection.run_id().clone(),
            );
        }
        let replayed = admitted.replayed;
        let user_message = admitted.user_message.clone();
        let attached = self.full_attached_from_admission(admitted)?;
        Ok(AnyConversationAttachedTurn {
            user_message,
            attached: AnyAttachedRun::Full(attached),
            replayed,
        })
    }

    fn register_full_conversation_stream(
        &self,
        conversation_id: &str,
    ) -> ConversationStreamPrivacy {
        let privacy = ConversationStreamPrivacy::new();
        let mut streams = self
            .inner
            .full_conversation_streams
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let streams = streams.entry(conversation_id.to_owned()).or_default();
        streams.retain(|stream| stream.strong_count() > 0);
        streams.push(privacy.downgrade());
        privacy
    }

    async fn admit_full_conversation_turn(
        &self,
        conversation: insight_durable::terminal_store::Conversation,
        request_id: &str,
        content: Value,
        attachment: PublicRunAttachment,
        subscribe: bool,
    ) -> Result<FullConversationAdmissionResult, ServiceError> {
        validate_full_conversation_identity(request_id)?;
        let (store, config) = self.conversation_components()?;
        if !config.conversations_enabled {
            return Err(ServiceError::new(
                "CONVERSATION_UNAVAILABLE",
                "Conversation API is disabled",
            ));
        }
        let user_content_hash = canonical_full_conversation_hash(&content)?;
        let mutation = full_conversation_mutation_index(
            &conversation.conversation_id,
            self.inner.full_conversation_mutations.len(),
        );
        let _mutation = Arc::clone(&self.inner.full_conversation_mutations[mutation])
            .lock_owned()
            .await;
        let turn_query = insight_durable::terminal_store::FullConversationTurnQuery {
            tenant_id: conversation.tenant_id.clone(),
            request_id: request_id.to_owned(),
            conversation_id: conversation.conversation_id.clone(),
            user_id: conversation.user_id.clone(),
        };
        if let Some(replayed) = store
            .get_full_conversation_turn(turn_query.clone())
            .await
            .map_err(full_conversation_store_error)?
        {
            if replayed.user_message.content_hash != user_content_hash {
                return Err(ServiceError::new(
                    "RUN_CONFLICT",
                    "Conversation request identity was reused with different content",
                ));
            }
            let projection = self
                .inner
                .repository
                .load_run(&replayed.run_id)
                .await?
                .ok_or_else(|| {
                    ServiceError::new("RUN_NOT_FOUND", "full Conversation Run was not found")
                })?;
            if projection.agent_id() != conversation.agent_id {
                return Err(ServiceError::new(
                    "CONVERSATION_OWNERSHIP_MISMATCH",
                    "conversation Run does not match its bound agent",
                ));
            }
            let (receiver, live_response) = if subscribe {
                let live_response = self
                    .inner
                    .live_response_broker
                    .subscribe(replayed.run_id.clone())
                    .await
                    .unwrap_or_else(|_| {
                        Box::new(ReplayWithoutLiveTail {
                            run_id: replayed.run_id.clone(),
                        })
                    });
                (
                    Some(self.inner.open_subscription(&replayed.run_id)),
                    Some(live_response),
                )
            } else {
                (None, None)
            };
            return Ok(FullConversationAdmissionResult {
                user_message: self
                    .full_conversation_message_view(replayed.user_message)
                    .await?,
                projection,
                receiver,
                live_response,
                conversation_privacy: subscribe
                    .then(|| self.register_full_conversation_stream(&conversation.conversation_id)),
                replayed: true,
            });
        }
        if conversation.archived_at.is_some() {
            return Err(ServiceError::new(
                "CONVERSATION_ARCHIVED",
                "conversation is archived",
            ));
        }
        // A crashed process may miss the post-terminal background trigger.
        // Enqueue catch-up without joining it: the current turn always uses
        // the latest already-committed summary plus recent durable messages.
        self.enqueue_full_conversation_summary(conversation.clone());
        let run_id = RunId::new(full_conversation_public_id(
            "run",
            &[
                &conversation.tenant_id,
                &conversation.conversation_id,
                request_id,
            ],
        ))
        .map_err(|_| ServiceError::new("RUN_CONFLICT", "failed to allocate Run identity"))?;
        let user_message_id = full_conversation_public_id(
            "msg",
            &[
                &conversation.conversation_id,
                &conversation.user_id,
                request_id,
                "user",
            ],
        );
        let assistant_message_id = full_conversation_public_id(
            "msg",
            &[
                &conversation.conversation_id,
                &conversation.user_id,
                request_id,
                "assistant",
            ],
        );
        let content_bytes = serde_jcs::to_vec(&content).map_err(|_| {
            ServiceError::new("CONVERSATION_REQUEST_INVALID", "message content is invalid")
        })?;
        let (user_content_inline, user_content_ref) =
            if content_bytes.len() > config.inline_content_max_bytes {
                let (reference, _) = self
                    .put_full_conversation_artifact(
                        &conversation.tenant_id,
                        insight_durable::terminal_store::TerminalArtifactSourceKind::UserMessage,
                        &user_message_id,
                        format!(
                            "conversation-message:{}:{}",
                            conversation.conversation_id, user_message_id
                        ),
                        &content,
                    )
                    .await?;
                (None, Some(reference))
            } else {
                (Some(content.clone()), None)
            };
        for _ in 0..MAX_PUBLICATION_ADMISSION_ATTEMPTS {
            let context_query = insight_durable::terminal_store::ConversationQuery {
                conversation_id: conversation.conversation_id.clone(),
                tenant_id: conversation.tenant_id.clone(),
                user_id: conversation.user_id.clone(),
            };
            let context = store
                .load_conversation_context(
                    insight_durable::terminal_store::ConversationContextQuery {
                        conversation: context_query.clone(),
                        recent_message_limit: config.recent_context_messages,
                    },
                )
                .await
                .map_err(full_conversation_store_error)?
                .ok_or_else(|| {
                    ServiceError::new("CONVERSATION_NOT_FOUND", "conversation was not found")
                })?;
            let context_message_order = context
                .messages
                .iter()
                .map(|message| message.message_order)
                .chain(
                    context
                        .summary
                        .iter()
                        .map(|summary| summary.through_message_order),
                )
                .max()
                .unwrap_or(0);
            let context_value = self
                .full_conversation_context_value(
                    context,
                    &context_query,
                    config.recent_context_messages,
                    config.summary_trigger_tokens,
                )
                .await?;
            let selected_context_hash = canonical_full_conversation_hash(&context_value)?;
            let full_conversation = FullConversationRunAdmission::new(
                conversation.conversation_id.clone(),
                conversation.tenant_id.clone(),
                conversation.user_id.clone(),
                conversation.agent_id.clone(),
                user_message_id.clone(),
                assistant_message_id.clone(),
                user_content_inline.clone(),
                user_content_ref.clone(),
                user_content_hash.clone(),
                selected_context_hash,
                context_message_order,
                chrono::Utc::now(),
            )?;
            let input = json!({
                "message": content.clone(),
                "conversation_context": context_value,
            });
            let agent = self
                .inner
                .load_exact_durable_deployment(conversation.deployment_revision_id.as_str())
                .await?;
            if agent.persistence_mode() != PersistenceMode::Full
                || agent.published().metadata().id != conversation.agent_id
            {
                return Err(ServiceError::new(
                    "CONVERSATION_OWNERSHIP_MISMATCH",
                    "conversation is not bound to a full-persistence deployment",
                ));
            }
            if agent
                .published()
                .plan()
                .nodes()
                .iter()
                .any(|node| matches!(node.kind(), NodeKind::SubflowCall(_)))
            {
                return Err(ServiceError::new(
                    "RUN_CAPABILITY_UNAVAILABLE",
                    "Conversation-bound full Runs do not support subflow lineage",
                ));
            }
            match self
                .create_run_with_agent(
                    agent,
                    input.clone(),
                    RequestMetadata {
                        request_id: Some(request_id.to_owned()),
                    },
                    attachment,
                    subscribe,
                    None,
                    Some(run_id.clone()),
                    Some(full_conversation.clone()),
                )
                .await
            {
                Err(error) if error.code() == "AGENT_PUBLICATION_CHANGED" => continue,
                Err(error) if error.code() == "RUN_CONFLICT" => {
                    let replayed = store
                        .get_full_conversation_turn(turn_query.clone())
                        .await
                        .map_err(full_conversation_store_error)?;
                    let Some(replayed) = replayed else {
                        continue;
                    };
                    if replayed.user_message.content_hash != user_content_hash {
                        return Err(error);
                    }
                    let projection = self
                        .inner
                        .repository
                        .load_run(&replayed.run_id)
                        .await?
                        .ok_or_else(|| {
                            ServiceError::new(
                                "RUN_NOT_FOUND",
                                "full Conversation Run was not found",
                            )
                        })?;
                    let (receiver, live_response) = if subscribe {
                        let live_response = self
                            .inner
                            .live_response_broker
                            .subscribe(replayed.run_id.clone())
                            .await
                            .unwrap_or_else(|_| {
                                Box::new(ReplayWithoutLiveTail {
                                    run_id: replayed.run_id.clone(),
                                })
                            });
                        (
                            Some(self.inner.open_subscription(&replayed.run_id)),
                            Some(live_response),
                        )
                    } else {
                        (None, None)
                    };
                    return Ok(FullConversationAdmissionResult {
                        user_message: self
                            .full_conversation_message_view(replayed.user_message)
                            .await?,
                        projection,
                        receiver,
                        live_response,
                        conversation_privacy: subscribe.then(|| {
                            self.register_full_conversation_stream(&conversation.conversation_id)
                        }),
                        replayed: true,
                    });
                }
                Err(error) => return Err(error),
                Ok((projection, receiver, live_response)) => {
                    let stored = store
                        .get_full_conversation_turn(turn_query.clone())
                        .await
                        .map_err(full_conversation_store_error)?
                        .ok_or_else(|| {
                            ServiceError::new(
                                "RUN_CONFLICT",
                                "full Conversation admission was not committed",
                            )
                        })?;
                    if stored.run_id != run_id {
                        return Err(ServiceError::new(
                            "RUN_CONFLICT",
                            "Conversation request identity conflicts with another Run",
                        ));
                    }
                    self.inner
                        .metrics
                        .full_conversation_user_messages
                        .fetch_add(1, Ordering::Relaxed);
                    return Ok(FullConversationAdmissionResult {
                        user_message: self
                            .full_conversation_message_view(stored.user_message)
                            .await?,
                        projection,
                        receiver,
                        live_response,
                        conversation_privacy: subscribe.then(|| {
                            self.register_full_conversation_stream(&conversation.conversation_id)
                        }),
                        replayed: false,
                    });
                }
            }
        }
        Err(ServiceError::new(
            "RUN_CONFLICT",
            "agent publication kept changing during Conversation admission",
        ))
    }

    fn full_attached_from_admission(
        &self,
        mut admitted: FullConversationAdmissionResult,
    ) -> Result<AttachedRun, ServiceError> {
        let receiver = admitted
            .receiver
            .take()
            .ok_or_else(|| ServiceError::new("RUN_CONFLICT", "attached subscription missing"))?;
        let live_response = admitted.live_response.take().ok_or_else(|| {
            ServiceError::new(
                "RUN_CONFLICT",
                "attached live response subscription missing",
            )
        })?;
        let conversation_privacy = admitted.conversation_privacy.take().ok_or_else(|| {
            ServiceError::new(
                "RUN_CONFLICT",
                "attached Conversation privacy registration missing",
            )
        })?;
        Ok(AttachedRun {
            run_id: admitted.projection.run_id().as_str().to_owned(),
            response_id: admitted.projection.response_id().to_owned(),
            request_id: admitted.projection.request_id().to_owned(),
            subscription: RunSubscription {
                run_id: admitted.projection.run_id().as_str().to_owned(),
                run_id_model: admitted.projection.run_id().clone(),
                receiver,
                owner: Arc::downgrade(&self.inner),
                position: None,
                last_seq: 0,
                terminal: false,
            },
            live_response,
            live_response_broker: Arc::clone(&self.inner.live_response_broker),
            terminal_barrier_timeout: self.inner.config.terminal_barrier_timeout,
            outbound_write_timeout: self.inner.config.outbound_write_timeout,
            conversation_privacy: Some(conversation_privacy),
        })
    }

    async fn full_conversation_message_view(
        &self,
        message: insight_durable::terminal_store::ConversationMessage,
    ) -> Result<ConversationMessageView, ServiceError> {
        let content = match &message.content {
            insight_durable::terminal_store::ConversationContent::Inline(value) => value.clone(),
            insight_durable::terminal_store::ConversationContent::Ref(reference) => {
                self.read_full_conversation_artifact(reference).await?
            }
        };
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

    async fn full_conversation_context_value(
        &self,
        mut context: insight_durable::terminal_store::ConversationContext,
        conversation: &insight_durable::terminal_store::ConversationQuery,
        recent_message_limit: u32,
        token_budget: usize,
    ) -> Result<Value, ServiceError> {
        let summary = match context.summary {
            Some(summary) => match self
                .read_full_conversation_artifact(&summary.summary_ref)
                .await
            {
                Ok(value) => Some(value),
                Err(_) => {
                    let (store, _) = self.conversation_components()?;
                    let page = store
                        .page_conversation_messages(
                            insight_durable::terminal_store::MessagePageQuery {
                                conversation: conversation.clone(),
                                before: None,
                                limit: recent_message_limit,
                            },
                        )
                        .await
                        .map_err(full_conversation_store_error)?
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
        let mut selected = Vec::new();
        for message in context.messages.into_iter().rev() {
            let content = match &message.content {
                insight_durable::terminal_store::ConversationContent::Inline(value) => {
                    value.clone()
                }
                insight_durable::terminal_store::ConversationContent::Ref(reference) => {
                    self.read_full_conversation_artifact(reference).await?
                }
            };
            let candidate = json!({
                "message_order": message.message_order,
                "role": message.role,
                "content": content,
            });
            let mut next = selected.clone();
            next.push(candidate.clone());
            next.reverse();
            let context = json!({"summary": summary.clone(), "messages": next});
            if estimated_full_conversation_tokens(&context)? > token_budget {
                break;
            }
            selected.push(candidate);
        }
        selected.reverse();
        let selected_count = selected.len();
        let context = json!({"summary": summary, "messages": selected});
        let context_tokens = estimated_full_conversation_tokens(&context)?;
        if context_tokens <= token_budget {
            self.inner.metrics.full_conversation_context_messages.store(
                u64::try_from(selected_count).unwrap_or(u64::MAX),
                Ordering::Relaxed,
            );
            self.inner.metrics.full_conversation_context_tokens.store(
                u64::try_from(context_tokens).unwrap_or(u64::MAX),
                Ordering::Relaxed,
            );
            Ok(context)
        } else {
            self.inner
                .metrics
                .full_conversation_context_messages
                .store(0, Ordering::Relaxed);
            self.inner
                .metrics
                .full_conversation_context_tokens
                .store(0, Ordering::Relaxed);
            Ok(json!({"summary": null, "messages": []}))
        }
    }

    async fn read_full_conversation_artifact(
        &self,
        reference: &str,
    ) -> Result<Value, ServiceError> {
        let artifact: ArtifactRef = serde_json::from_str(reference).map_err(|_| {
            ServiceError::new(
                "CONVERSATION_CONTENT_UNAVAILABLE",
                "conversation content ref is invalid",
            )
        })?;
        let store = self.inner.artifact_store.as_ref().ok_or_else(|| {
            ServiceError::new(
                "CONVERSATION_CONTENT_UNAVAILABLE",
                "conversation content store is unavailable",
            )
        })?;
        let locator = store
            .storage_locator(&artifact)
            .map_err(ServiceError::from)?;
        let bytes = store
            .read_and_verify(&artifact, &locator, 64 * 1024 * 1024)
            .await
            .map_err(ServiceError::from)?;
        let mut value: Value = serde_json::from_slice(&bytes).map_err(|_| {
            ServiceError::new(
                "CONVERSATION_CONTENT_UNAVAILABLE",
                "conversation content is invalid",
            )
        })?;
        if artifact.media_type() == Some("application/vnd.insight.terminal-object.v1+json") {
            let object = value.as_object_mut().ok_or_else(|| {
                ServiceError::new(
                    "CONVERSATION_CONTENT_UNAVAILABLE",
                    "scoped conversation content is invalid",
                )
            })?;
            if object.get("kind").and_then(Value::as_str) != Some("insight_terminal_object") {
                return Err(ServiceError::new(
                    "CONVERSATION_CONTENT_UNAVAILABLE",
                    "scoped conversation content is invalid",
                ));
            }
            return object.remove("value").ok_or_else(|| {
                ServiceError::new(
                    "CONVERSATION_CONTENT_UNAVAILABLE",
                    "scoped conversation content is invalid",
                )
            });
        }
        Ok(value)
    }

    fn schedule_full_conversation_summary_after_run(
        &self,
        conversation: insight_durable::terminal_store::Conversation,
        run_id: RunId,
    ) {
        if self.inner.shutdown.is_cancelled() {
            return;
        }
        let service = self.clone();
        tokio::spawn(async move {
            let timeout = service
                .conversation_components()
                .map(|(_, config)| config.run_timeout.saturating_add(Duration::from_secs(1)))
                .unwrap_or(Duration::from_secs(1));
            let terminal = tokio::select! {
                _ = service.inner.shutdown.cancelled() => false,
                terminal = tokio::time::timeout(timeout, async {
                    loop {
                        match service.inner.repository.load_run(&run_id).await {
                            Ok(Some(projection)) if projection.lifecycle().is_terminal() => {
                                break true;
                            }
                            Ok(Some(_)) => tokio::time::sleep(Duration::from_millis(20)).await,
                            _ => break false,
                        }
                    }
                }) => terminal.unwrap_or(false),
            };
            if terminal {
                service
                    .inner
                    .metrics
                    .full_conversation_assistant_messages
                    .fetch_add(1, Ordering::Relaxed);
                service.enqueue_full_conversation_summary(conversation);
            }
        });
    }

    fn enqueue_full_conversation_summary(
        &self,
        conversation: insight_durable::terminal_store::Conversation,
    ) {
        if self.inner.shutdown.is_cancelled() {
            return;
        }
        let conversation_id = conversation.conversation_id.clone();
        let should_spawn = {
            let mut jobs = self
                .inner
                .full_conversation_summary_jobs
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            match jobs.entry(conversation_id.clone()) {
                std::collections::btree_map::Entry::Vacant(entry) => {
                    entry.insert(false);
                    true
                }
                std::collections::btree_map::Entry::Occupied(mut entry) => {
                    *entry.get_mut() = true;
                    false
                }
            }
        };
        if !should_spawn {
            return;
        }
        let service = self.clone();
        tokio::spawn(async move {
            let worker =
                AssertUnwindSafe(service.run_full_conversation_summary_worker(conversation))
                    .catch_unwind()
                    .await;
            if worker.is_err() {
                service
                    .inner
                    .metrics
                    .full_conversation_summary_failed
                    .fetch_add(1, Ordering::Relaxed);
                service
                    .inner
                    .full_conversation_summary_jobs
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .remove(&conversation_id);
                tracing::warn!(conversation_id, "full Conversation summary worker panicked");
            }
        });
    }

    async fn run_full_conversation_summary_worker(
        &self,
        conversation: insight_durable::terminal_store::Conversation,
    ) {
        loop {
            if self.inner.shutdown.is_cancelled() {
                self.finish_full_conversation_summary_pass(&conversation.conversation_id, false);
                return;
            }
            let summary_delay = *self
                .inner
                .full_conversation_summary_delay
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if !summary_delay.is_zero() {
                tokio::select! {
                    _ = self.inner.shutdown.cancelled() => {
                        self.finish_full_conversation_summary_pass(
                            &conversation.conversation_id,
                            false,
                        );
                        return;
                    }
                    () = tokio::time::sleep(summary_delay) => {}
                }
            }
            match self
                .run_claimed_full_conversation_summary(conversation.clone())
                .await
            {
                Ok(Some(())) => {
                    self.inner
                        .metrics
                        .full_conversation_summary_succeeded
                        .fetch_add(1, Ordering::Relaxed);
                }
                Ok(None) => {}
                Err(error) => {
                    self.inner
                        .metrics
                        .full_conversation_summary_failed
                        .fetch_add(1, Ordering::Relaxed);
                    tracing::warn!(
                        conversation_id = conversation.conversation_id.as_str(),
                        code = error.code(),
                        "full Conversation summary job failed"
                    );
                }
            }
            if !self.finish_full_conversation_summary_pass(&conversation.conversation_id, true) {
                return;
            }
        }
    }

    fn finish_full_conversation_summary_pass(
        &self,
        conversation_id: &str,
        allow_repeat: bool,
    ) -> bool {
        let mut jobs = self
            .inner
            .full_conversation_summary_jobs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if allow_repeat && jobs.get(conversation_id).copied() == Some(true) {
            jobs.insert(conversation_id.to_owned(), false);
            true
        } else {
            jobs.remove(conversation_id);
            false
        }
    }

    async fn run_claimed_full_conversation_summary(
        &self,
        conversation: insight_durable::terminal_store::Conversation,
    ) -> Result<Option<()>, ServiceError> {
        let (store, _) = self.conversation_components()?;
        let eligible = tokio::select! {
            _ = self.inner.shutdown.cancelled() => {
                return Err(ServiceError::new(
                    "RUN_SERVICE_STOPPING",
                    "Run service is stopping",
                ));
            }
            result = tokio::time::timeout(
                FULL_CONVERSATION_SUMMARY_TIMEOUT,
                self.full_conversation_summary_is_eligible(&conversation),
            ) => result.map_err(|_| {
                ServiceError::new(
                    "CONVERSATION_SUMMARY_TIMEOUT",
                    "Conversation summary eligibility preflight timed out",
                )
            })??,
        };
        if !eligible {
            return Ok(None);
        }
        if self.inner.shutdown.is_cancelled() {
            return Err(ServiceError::new(
                "RUN_SERVICE_STOPPING",
                "Run service is stopping",
            ));
        }
        let query = insight_durable::terminal_store::ConversationQuery {
            conversation_id: conversation.conversation_id.clone(),
            tenant_id: conversation.tenant_id.clone(),
            user_id: conversation.user_id.clone(),
        };
        let claim_token = format!("summary_{}", Uuid::new_v4().simple());
        let claimed_at = chrono::Utc::now();
        let claim_expires_at = claimed_at
            + chrono::Duration::from_std(FULL_CONVERSATION_SUMMARY_CLAIM_LEASE).map_err(|_| {
                ServiceError::new(
                    "RUN_SERVICE_CONFIG_INVALID",
                    "Conversation summary claim lease is invalid",
                )
            })?;
        let claimed = store
            .try_claim_conversation_summary_job(
                insight_durable::terminal_store::ClaimConversationSummaryJob {
                    conversation: query,
                    claim_token: claim_token.clone(),
                    claimed_by: self.inner.owner.clone(),
                    claim_expires_at,
                    created_at: claimed_at,
                },
            )
            .await
            .map_err(full_conversation_store_error)?;
        if !claimed {
            return Ok(None);
        }

        let conversation_id = conversation.conversation_id.clone();
        let generation = tokio::select! {
            _ = self.inner.shutdown.cancelled() => Err(ServiceError::new(
                "RUN_SERVICE_STOPPING",
                "Run service is stopping",
            )),
            result = tokio::time::timeout(
                FULL_CONVERSATION_SUMMARY_TIMEOUT,
                AssertUnwindSafe(self.generate_full_conversation_summary(conversation))
                    .catch_unwind(),
            ) => match result {
                Ok(Ok(result)) => result,
                Ok(Err(_)) => Err(ServiceError::new(
                    "CONVERSATION_SUMMARY_FAILED",
                    "Conversation summary worker panicked",
                )),
                Err(_) => Err(ServiceError::new(
                    "CONVERSATION_SUMMARY_TIMEOUT",
                    "Conversation summary job timed out",
                )),
            },
        };
        let released = store
            .release_conversation_summary_job(
                insight_durable::terminal_store::ReleaseConversationSummaryJob {
                    conversation_id,
                    claim_token,
                    claimed_by: self.inner.owner.clone(),
                },
            )
            .await
            .map_err(full_conversation_store_error)?;
        if !released {
            return Err(ServiceError::new(
                "CONVERSATION_SUMMARY_CLAIM_LOST",
                "Conversation summary claim was lost before release",
            ));
        }
        generation.map(Some)
    }

    async fn full_conversation_summary_is_eligible(
        &self,
        conversation: &insight_durable::terminal_store::Conversation,
    ) -> Result<bool, ServiceError> {
        let (store, config) = self.conversation_components()?;
        let query = insight_durable::terminal_store::ConversationQuery {
            conversation_id: conversation.conversation_id.clone(),
            tenant_id: conversation.tenant_id.clone(),
            user_id: conversation.user_id.clone(),
        };
        let previous_boundary = store
            .latest_conversation_summary(query.clone())
            .await
            .map_err(full_conversation_store_error)?
            .as_ref()
            .map_or(0, |summary| summary.through_message_order);
        let mut before = None;
        let mut message_count = 0_usize;
        let mut token_count = 0_usize;
        loop {
            let page = store
                .page_conversation_messages(insight_durable::terminal_store::MessagePageQuery {
                    conversation: query.clone(),
                    before,
                    limit: 200,
                })
                .await
                .map_err(full_conversation_store_error)?
                .ok_or_else(|| {
                    ServiceError::new("CONVERSATION_NOT_FOUND", "conversation was not found")
                })?;
            for message in page.messages {
                if message.message_order <= previous_boundary {
                    return Ok(false);
                }
                let content = match &message.content {
                    insight_durable::terminal_store::ConversationContent::Inline(value) => {
                        value.clone()
                    }
                    insight_durable::terminal_store::ConversationContent::Ref(reference) => {
                        self.read_full_conversation_artifact(reference).await?
                    }
                };
                message_count = message_count.saturating_add(1);
                token_count =
                    token_count.saturating_add(estimated_full_conversation_tokens(&content)?);
                if message_count
                    >= usize::try_from(config.summary_trigger_messages).unwrap_or(usize::MAX)
                    || token_count >= config.summary_trigger_tokens
                {
                    return Ok(true);
                }
            }
            match page.next_cursor {
                Some(cursor) => before = Some(cursor),
                None => return Ok(false),
            }
        }
    }

    async fn generate_full_conversation_summary(
        &self,
        conversation: insight_durable::terminal_store::Conversation,
    ) -> Result<(), ServiceError> {
        let (store, config) = self.conversation_components()?;
        let query = insight_durable::terminal_store::ConversationQuery {
            conversation_id: conversation.conversation_id.clone(),
            tenant_id: conversation.tenant_id.clone(),
            user_id: conversation.user_id.clone(),
        };
        let previous = store
            .latest_conversation_summary(query.clone())
            .await
            .map_err(full_conversation_store_error)?;
        let previous_boundary = previous
            .as_ref()
            .map_or(0, |summary| summary.through_message_order);
        let backlog_limit = usize::try_from(config.summary_trigger_messages)
            .unwrap_or(usize::MAX)
            .saturating_add(usize::try_from(config.recent_context_messages).unwrap_or(0))
            .max(200);
        let mut before = None;
        let mut messages = Vec::new();
        'pages: loop {
            let page = store
                .page_conversation_messages(insight_durable::terminal_store::MessagePageQuery {
                    conversation: query.clone(),
                    before,
                    limit: 200,
                })
                .await
                .map_err(full_conversation_store_error)?
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
        let mut token_count = 0_usize;
        for message in messages {
            let content = match &message.content {
                insight_durable::terminal_store::ConversationContent::Inline(value) => {
                    value.clone()
                }
                insight_durable::terminal_store::ConversationContent::Ref(reference) => {
                    self.read_full_conversation_artifact(reference).await?
                }
            };
            token_count = token_count.saturating_add(estimated_full_conversation_tokens(&content)?);
            selected.push((message, content));
        }
        if selected.len() < usize::try_from(config.summary_trigger_messages).unwrap_or(usize::MAX)
            && token_count < config.summary_trigger_tokens
        {
            return Ok(());
        }
        let keep = usize::try_from(config.recent_context_messages)
            .unwrap_or(usize::MAX)
            .min(selected.len());
        let summarized = &selected[keep..];
        let Some(boundary) = summarized.first() else {
            return Ok(());
        };
        let mut new_entries = Vec::with_capacity(summarized.len());
        for (message, content) in summarized.iter().rev() {
            new_entries.push(json!({
                "role": match message.role {
                    insight_durable::terminal_store::ConversationRole::User => "user",
                    insight_durable::terminal_store::ConversationRole::Assistant => "assistant",
                },
                "content_hash": message.content_hash.as_str(),
                "preview": summary_preview(content),
            }));
        }
        let mut entries = match previous.as_ref() {
            Some(summary) => {
                let previous = self
                    .read_full_conversation_artifact(&summary.summary_ref)
                    .await?;
                normalized_summary_entries(&previous)
            }
            None => Vec::new(),
        };
        entries.extend(new_entries);
        if entries.len() > SUMMARY_MAX_ENTRIES {
            entries.drain(..entries.len() - SUMMARY_MAX_ENTRIES);
        }
        let summary_budget = config.summary_trigger_tokens.saturating_div(2).max(64);
        let mut summary_value =
            flat_summary_value(previous_boundary, boundary.0.message_order, &entries);
        while !entries.is_empty() && estimated_json_tokens(&summary_value)? > summary_budget {
            entries.remove(0);
            summary_value =
                flat_summary_value(previous_boundary, boundary.0.message_order, &entries);
        }
        if estimated_json_tokens(&summary_value)? > summary_budget {
            return Err(ServiceError::new(
                "CONVERSATION_SUMMARY_INVALID",
                "conversation summary exceeds its hard size budget",
            ));
        }
        let source_id = format!(
            "{}:{}",
            conversation.conversation_id, boundary.0.message_order
        );
        let (summary_ref, summary_hash) = self
            .put_full_conversation_artifact(
                &conversation.tenant_id,
                insight_durable::terminal_store::TerminalArtifactSourceKind::ConversationSummary,
                &source_id,
                format!(
                    "conversation-summary:{}:{}",
                    conversation.conversation_id, boundary.0.message_order
                ),
                &summary_value,
            )
            .await?;
        store
            .put_conversation_summary(insight_durable::terminal_store::NewConversationSummary {
                conversation: query,
                through_message_order: boundary.0.message_order,
                summary_ref,
                summary_hash,
                model_revision: "extractive-full-v1".to_owned(),
                created_at: chrono::Utc::now(),
            })
            .await
            .map(|_| ())
            .map_err(full_conversation_store_error)
    }

    async fn put_full_conversation_artifact(
        &self,
        tenant_id: &str,
        source_kind: insight_durable::terminal_store::TerminalArtifactSourceKind,
        source_id: &str,
        scope: String,
        value: &Value,
    ) -> Result<(String, ContentHash), ServiceError> {
        stage_full_conversation_artifact(
            self.inner.as_ref(),
            tenant_id,
            source_kind,
            source_id,
            scope,
            value,
            None,
        )
        .await
    }

    fn conversation_components(
        &self,
    ) -> Result<(Arc<dyn TerminalOnlyStore>, TerminalOnlyRunConfig), ServiceError> {
        let store = self
            .inner
            .conversation_store
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
            .ok_or_else(|| {
                ServiceError::new("CONVERSATION_UNAVAILABLE", "Conversation API is disabled")
            })?;
        let config = self
            .inner
            .conversation_config
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .copied()
            .ok_or_else(|| {
                ServiceError::new("CONVERSATION_UNAVAILABLE", "Conversation API is disabled")
            })?;
        Ok((store, config))
    }

    pub async fn archive_conversation(
        &self,
        conversation_id: &str,
        tenant_id: &str,
        user_id: &str,
    ) -> Result<insight_durable::terminal_store::Conversation, ServiceError> {
        if let Some(engine) = self.terminal_engine() {
            return engine
                .archive_conversation(conversation_id, tenant_id, user_id)
                .await;
        }
        let (store, config) = self.conversation_components()?;
        if !config.conversations_enabled {
            return Err(ServiceError::new(
                "CONVERSATION_UNAVAILABLE",
                "Conversation API is disabled",
            ));
        }
        match store
            .archive_conversation(
                insight_durable::terminal_store::ConversationQuery {
                    conversation_id: conversation_id.to_owned(),
                    tenant_id: tenant_id.to_owned(),
                    user_id: user_id.to_owned(),
                },
                chrono::Utc::now(),
            )
            .await
            .map_err(full_conversation_store_error)?
        {
            insight_durable::terminal_store::ArchiveOutcome::Archived { conversation, .. } => {
                Ok(conversation)
            }
            insight_durable::terminal_store::ArchiveOutcome::NotFound => Err(ServiceError::new(
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
        let (store, config) = self.conversation_components()?;
        if !config.conversations_enabled {
            return Err(ServiceError::new(
                "CONVERSATION_UNAVAILABLE",
                "Conversation API is disabled",
            ));
        }
        let query = insight_durable::terminal_store::ConversationQuery {
            conversation_id: conversation_id.to_owned(),
            tenant_id: tenant_id.to_owned(),
            user_id: user_id.to_owned(),
        };
        if store
            .get_conversation(query.clone())
            .await
            .map_err(full_conversation_store_error)?
            .is_none()
        {
            return Ok(false);
        }
        tokio::time::timeout(
            config.terminal_commit_retry,
            cancel_conversation_streams(&self.inner.full_conversation_streams, conversation_id),
        )
        .await
        .map_err(|_| {
            ServiceError::new(
                "CONVERSATION_UNAVAILABLE",
                "Conversation streams could not be quiesced before deletion",
            )
        })?;
        let mutation = full_conversation_mutation_index(
            conversation_id,
            self.inner.full_conversation_mutations.len(),
        );
        let _mutation = Arc::clone(&self.inner.full_conversation_mutations[mutation])
            .lock_owned()
            .await;
        tokio::time::timeout(
            config.terminal_commit_retry,
            cancel_conversation_streams(&self.inner.full_conversation_streams, conversation_id),
        )
        .await
        .map_err(|_| {
            ServiceError::new(
                "CONVERSATION_UNAVAILABLE",
                "Conversation streams could not be quiesced before deletion",
            )
        })?;
        if store
            .get_conversation(query.clone())
            .await
            .map_err(full_conversation_store_error)?
            .is_none()
        {
            return Ok(false);
        }
        let run_ids = store
            .list_full_conversation_run_ids(query.clone())
            .await
            .map_err(full_conversation_store_error)?;
        tokio::time::timeout(config.terminal_commit_retry, async {
            loop {
                let mut pending = false;
                for run_id in &run_ids {
                    let Some(projection) = self.inner.repository.load_run(run_id).await? else {
                        continue;
                    };
                    if projection.lifecycle().is_terminal() {
                        continue;
                    }
                    pending = true;
                    self.cancel_model(run_id.clone()).await?;
                }
                if !pending {
                    return Ok::<(), ServiceError>(());
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .map_err(|_| {
            ServiceError::new(
                "CONVERSATION_UNAVAILABLE",
                "Conversation Runs could not be quiesced before deletion",
            )
        })??;
        if let Some(engine) = self.terminal_engine() {
            return engine
                .delete_conversation(conversation_id, tenant_id, user_id)
                .await;
        }
        match store
            .delete_conversation(query)
            .await
            .map_err(full_conversation_store_error)?
        {
            insight_durable::terminal_store::PrivacyDeleteOutcome::Deleted { .. } => Ok(true),
            insight_durable::terminal_store::PrivacyDeleteOutcome::NotFound => Ok(false),
        }
    }

    /// Reads one Artifact only when its exact ArtifactRef is already present
    /// in this Run's hashed durable public terminal snapshot. The repository
    /// independently enforces Run ownership, referenced state and retention;
    /// the store then revalidates size and content hash under a hard bound.
    pub async fn read_public_artifact(
        &self,
        run_id: &str,
        artifact_id: &str,
    ) -> Result<PublicArtifact, ServiceError> {
        self.read_public_artifact_for_tenant("default", run_id, artifact_id)
            .await
    }

    pub async fn read_public_artifact_for_tenant(
        &self,
        tenant_id: &str,
        run_id: &str,
        artifact_id: &str,
    ) -> Result<PublicArtifact, ServiceError> {
        self.read_public_artifact_for_principal(tenant_id, None, run_id, artifact_id)
            .await
    }

    pub async fn read_public_artifact_for_principal(
        &self,
        tenant_id: &str,
        user_id: Option<&str>,
        run_id: &str,
        artifact_id: &str,
    ) -> Result<PublicArtifact, ServiceError> {
        let not_found = || ServiceError::new("ARTIFACT_NOT_FOUND", "Artifact not found");
        let run_id = RunId::new(run_id.to_owned()).map_err(|_| not_found())?;
        let artifact_id = ArtifactId::new(artifact_id.to_owned()).map_err(|_| not_found())?;
        match self
            .authorize_full_conversation_principal(tenant_id, user_id, &run_id)
            .await
        {
            Ok(authorization) if !authorization.is_deleted() => {}
            Ok(_) => return Err(not_found()),
            Err(error) if error.code() == "RUN_NOT_FOUND" => return Err(not_found()),
            Err(error) => return Err(error),
        }
        let snapshot = self
            .inner
            .repository
            .load_response_snapshot(&run_id)
            .await?
            .ok_or_else(not_found)?;
        let published =
            public_artifact_reference(&snapshot, &run_id, &artifact_id)?.ok_or_else(not_found)?;
        let retained = self
            .inner
            .repository
            .get_retained_artifact(&run_id, &artifact_id)
            .await?
            .ok_or_else(not_found)?;
        if retained.run_id() != &run_id || retained.artifact() != &published {
            return Err(not_found());
        }
        if retained.artifact().size_bytes()
            > u64::try_from(self.inner.config.artifact_read_max_bytes).unwrap_or(u64::MAX)
        {
            return Err(ServiceError::new(
                "ARTIFACT_TOO_LARGE",
                "Artifact exceeds the authorized read size",
            ));
        }
        let store = self.inner.artifact_store.as_deref().ok_or_else(|| {
            ServiceError::new("RUN_SERVICE_UNAVAILABLE", "Artifact storage is unavailable")
        })?;
        let bytes = store
            .read_and_verify(
                retained.artifact(),
                retained.storage_locator(),
                self.inner.config.artifact_read_max_bytes,
            )
            .await?;
        Ok(PublicArtifact::new(published, bytes))
    }

    /// Submit one typed durable signal. `message_id` is the public
    /// idempotency key; the target Activation and Signal identity are always
    /// resolved from scheduler-owned wait state.
    pub async fn signal(
        &self,
        run_id: &str,
        signal_name: &str,
        message_id: &str,
        value: Value,
    ) -> Result<RunRecord, ServiceError> {
        if !self.inner.accepting.load(Ordering::Acquire) {
            return Err(ServiceError::new(
                "RUN_SERVICE_STOPPING",
                "Run service is stopping",
            ));
        }
        let canonical = serde_jcs::to_vec(&value)
            .map_err(|_| ServiceError::new("SIGNAL_INVALID", "signal payload is invalid"))?;
        if canonical.len() > MAX_SIGNAL_PAYLOAD_BYTES {
            return Err(ServiceError::new(
                "SIGNAL_INVALID",
                "signal payload exceeds the supported size",
            ));
        }
        let runtime_value = RuntimeValue::new(value.clone())
            .map_err(|_| ServiceError::new("SIGNAL_INVALID", "signal payload is invalid"))?;
        let run_id = RunId::new(run_id.to_owned())
            .map_err(|_| ServiceError::new("RUN_NOT_FOUND", "Run not found"))?;
        self.inner
            .repository
            .load_run(&run_id)
            .await?
            .ok_or_else(|| ServiceError::new("RUN_NOT_FOUND", "Run not found"))?;

        let existing = self
            .inner
            .repository
            .find_existing_signal_submission(&run_id, message_id)
            .await
            .map_err(|_| ServiceError::new("SIGNAL_INVALID", "signal identity is invalid"))?;
        let target = if let Some(existing) = &existing {
            existing.target().clone()
        } else {
            let targets = self
                .inner
                .repository
                .find_open_signal_waits(&run_id, signal_name)
                .await
                .map_err(|_| ServiceError::new("SIGNAL_INVALID", "signal name is invalid"))?;
            match targets.as_slice() {
                [target] => target.clone(),
                [] => {
                    return Err(ServiceError::new(
                        "SIGNAL_NOT_WAITING",
                        "Run is not waiting for this signal",
                    ))
                }
                _ => {
                    return Err(ServiceError::new(
                        "SIGNAL_AMBIGUOUS",
                        "signal name addresses more than one open wait",
                    ))
                }
            }
        };
        if !runtime_value.matches(target.payload_type()) {
            return Err(ServiceError::new(
                "SIGNAL_TYPE_MISMATCH",
                "signal payload does not match the wait contract",
            ));
        }
        let command = ReceiveSignalCommand::new(
            run_id.clone(),
            target.signal_id().clone(),
            message_id,
            signal_name,
            target.activation_id().clone(),
            value,
        )
        .map_err(|_| ServiceError::new("SIGNAL_INVALID", "signal request is invalid"))?;
        match self.inner.repository.receive_signal(command).await? {
            TransitionOutcome::Committed { .. } | TransitionOutcome::ExactReplay { .. } => {}
            TransitionOutcome::StateConflict | TransitionOutcome::StaleLease => {
                return Err(ServiceError::new(
                    "SIGNAL_CONFLICT",
                    "signal idempotency or wait state conflicts",
                ))
            }
        }
        if existing
            .as_ref()
            .is_some_and(|submission| submission.state() == SignalInboxState::Consumed)
        {
            return self.get_run(run_id.as_str()).await;
        }
        if existing.as_ref().is_some_and(|submission| {
            matches!(
                submission.state(),
                SignalInboxState::Rejected | SignalInboxState::Expired
            )
        }) {
            return Err(ServiceError::new(
                "SIGNAL_CONFLICT",
                "signal previously lost the durable wait race",
            ));
        }
        let key = transition_key(
            "signal.resolve",
            &[run_id.as_str(), target.signal_id().as_str()],
        )?;
        match self
            .inner
            .repository
            .resolve_wait_signal(
                key,
                ResolveSignalCommand::new(
                    run_id.clone(),
                    target.activation_id().clone(),
                    target.signal_id().clone(),
                    target.activation_projection_version(),
                ),
            )
            .await?
        {
            TransitionOutcome::Committed { .. } | TransitionOutcome::ExactReplay { .. } => {}
            TransitionOutcome::StateConflict | TransitionOutcome::StaleLease => {
                return Err(ServiceError::new(
                    "SIGNAL_CONFLICT",
                    "signal lost the durable wait race",
                ))
            }
        }
        self.inner.resume_run(&run_id, false).await?;
        self.inner.flush_public_events().await?;
        self.get_run(run_id.as_str()).await
    }

    /// Lists work currently claimable by one authenticated human identity.
    /// Assignment is checked again under the claim row lock, so this view is
    /// advisory and never grants completion authority by itself.
    pub async fn list_human_tasks(
        &self,
        identity: &str,
        groups: Vec<String>,
        limit: u32,
    ) -> Result<Vec<HumanWorkItem>, ServiceError> {
        if limit == 0 || limit > MAX_HUMAN_TASK_LIST {
            return Err(ServiceError::new(
                "HUMAN_TASK_REQUEST_INVALID",
                "human task list limit is invalid",
            ));
        }
        let principal = HumanTaskPrincipal::new(identity, groups).map_err(|_| {
            ServiceError::new(
                "HUMAN_TASK_IDENTITY_INVALID",
                "human task identity is invalid",
            )
        })?;
        self.inner
            .repository
            .list_human_work_items(&principal, limit)
            .await
            .map_err(Into::into)
    }

    pub async fn claim_human_task(
        &self,
        work_item_id: &str,
        identity: &str,
        groups: Vec<String>,
        request_id: &str,
    ) -> Result<HumanWorkItem, ServiceError> {
        if !self.inner.accepting.load(Ordering::Acquire) {
            return Err(ServiceError::new(
                "RUN_SERVICE_STOPPING",
                "Run service is stopping",
            ));
        }
        let principal = HumanTaskPrincipal::new(identity, groups).map_err(|_| {
            ServiceError::new(
                "HUMAN_TASK_IDENTITY_INVALID",
                "human task identity is invalid",
            )
        })?;
        let work_item_id = HumanWorkItemId::new(work_item_id).map_err(|_| {
            ServiceError::new("HUMAN_TASK_NOT_FOUND", "human work item was not found")
        })?;
        let command =
            ClaimHumanWorkItemCommand::new(work_item_id, principal, request_id).map_err(|_| {
                ServiceError::new(
                    "HUMAN_TASK_REQUEST_INVALID",
                    "human task request is invalid",
                )
            })?;
        match self.inner.repository.claim_human_work_item(command).await? {
            TransitionOutcome::Committed { result } => Ok(result.work_item().clone()),
            TransitionOutcome::ExactReplay { authoritative } => {
                Ok(authoritative.work_item().clone())
            }
            TransitionOutcome::StateConflict | TransitionOutcome::StaleLease => {
                Err(ServiceError::new(
                    "HUMAN_TASK_CLAIM_CONFLICT",
                    "human work item cannot be claimed",
                ))
            }
        }
    }

    /// Completes a fenced claim with a typed approval. The durable work-item
    /// reservation is replayable independently of the completion signal, and
    /// the existing signal/timer CAS remains the only scheduler winner.
    pub async fn complete_human_task(
        &self,
        work_item_id: &str,
        identity: &str,
        groups: Vec<String>,
        request_id: &str,
        claim_fence: u64,
        value: Value,
    ) -> Result<HumanWorkItem, ServiceError> {
        if !self.inner.accepting.load(Ordering::Acquire) {
            return Err(ServiceError::new(
                "RUN_SERVICE_STOPPING",
                "Run service is stopping",
            ));
        }
        let principal = HumanTaskPrincipal::new(identity, groups).map_err(|_| {
            ServiceError::new(
                "HUMAN_TASK_IDENTITY_INVALID",
                "human task identity is invalid",
            )
        })?;
        let work_item_id = HumanWorkItemId::new(work_item_id).map_err(|_| {
            ServiceError::new("HUMAN_TASK_NOT_FOUND", "human work item was not found")
        })?;
        let command = CompleteHumanWorkItemCommand::new(
            work_item_id.clone(),
            principal,
            request_id,
            claim_fence,
            value,
        )
        .map_err(|_| {
            ServiceError::new(
                "HUMAN_TASK_REQUEST_INVALID",
                "human task completion is invalid",
            )
        })?;
        let authority = match self
            .inner
            .repository
            .complete_human_work_item(command)
            .await?
        {
            TransitionOutcome::Committed { result } => result,
            TransitionOutcome::ExactReplay { authoritative } => authoritative,
            TransitionOutcome::StateConflict | TransitionOutcome::StaleLease => {
                return Err(ServiceError::new(
                    "HUMAN_TASK_COMPLETION_CONFLICT",
                    "human task claim or approval conflicts",
                ))
            }
        };
        let item = authority.work_item();
        let receive = ReceiveSignalCommand::new(
            item.run_id().clone(),
            authority.signal_id().clone(),
            authority.message_id(),
            item.signal_name(),
            item.activation_id().clone(),
            authority.value().clone(),
        )
        .map_err(|_| {
            ServiceError::new(
                "HUMAN_TASK_REQUEST_INVALID",
                "human task completion is invalid",
            )
        })?;
        match self.inner.repository.receive_signal(receive).await? {
            TransitionOutcome::Committed { .. } | TransitionOutcome::ExactReplay { .. } => {}
            TransitionOutcome::StateConflict | TransitionOutcome::StaleLease => {
                return Err(ServiceError::new(
                    "HUMAN_TASK_COMPLETION_CONFLICT",
                    "human task completion lost the durable wait race",
                ))
            }
        }
        let submission = self
            .inner
            .repository
            .find_existing_signal_submission(item.run_id(), authority.message_id())
            .await?
            .ok_or_else(|| {
                ServiceError::new(
                    "RUN_SERVICE_UNAVAILABLE",
                    "human task completion authority disappeared",
                )
            })?;
        if matches!(
            submission.state(),
            SignalInboxState::Rejected | SignalInboxState::Expired
        ) {
            return Err(ServiceError::new(
                "HUMAN_TASK_COMPLETION_CONFLICT",
                "human task completion lost the durable wait race",
            ));
        }
        if submission.state() == SignalInboxState::Pending {
            let target = submission.target();
            // Foreground completion and the reserved-completion ingress pump
            // are two entry points for the same durable wait transition. They
            // must derive the same idempotency key so a concurrent winner is
            // an ExactReplay, never a false human-completion conflict.
            let key = transition_key(
                "signal.resolve",
                &[item.run_id().as_str(), authority.signal_id().as_str()],
            )?;
            if !matches!(
                self.inner
                    .repository
                    .resolve_wait_signal(
                        key,
                        ResolveSignalCommand::new(
                            item.run_id().clone(),
                            item.activation_id().clone(),
                            authority.signal_id().clone(),
                            target.activation_projection_version(),
                        ),
                    )
                    .await?,
                TransitionOutcome::Committed { .. } | TransitionOutcome::ExactReplay { .. }
            ) {
                return Err(ServiceError::new(
                    "HUMAN_TASK_COMPLETION_CONFLICT",
                    "human task completion lost the durable wait race",
                ));
            }
        }
        if !self
            .inner
            .repository
            .finalize_human_work_item(&work_item_id, request_id)
            .await?
        {
            return Err(ServiceError::new(
                "RUN_SERVICE_UNAVAILABLE",
                "human task completion could not be finalized",
            ));
        }
        let run_is_terminal = self
            .inner
            .repository
            .load_run(item.run_id())
            .await?
            .is_some_and(|run| run.lifecycle().is_terminal());
        if !run_is_terminal {
            self.inner.resume_run(item.run_id(), false).await?;
        }
        self.inner.flush_public_events().await?;
        self.inner
            .repository
            .load_human_work_item(&work_item_id)
            .await?
            .ok_or_else(|| {
                ServiceError::new("HUMAN_TASK_NOT_FOUND", "human work item was not found")
            })
    }

    pub async fn pause(&self, run_id: &str) -> Result<RunRecord, ServiceError> {
        self.change_admission(run_id, AdmissionState::Paused).await
    }

    pub async fn resume(&self, run_id: &str) -> Result<RunRecord, ServiceError> {
        self.change_admission(run_id, AdmissionState::Open).await
    }

    async fn change_admission(
        &self,
        run_id: &str,
        target: AdmissionState,
    ) -> Result<RunRecord, ServiceError> {
        let run_id = RunId::new(run_id.to_owned())
            .map_err(|_| ServiceError::new("RUN_NOT_FOUND", "Run not found"))?;
        for _ in 0..8 {
            let projection = self
                .inner
                .repository
                .load_run(&run_id)
                .await?
                .ok_or_else(|| ServiceError::new("RUN_NOT_FOUND", "Run not found"))?;
            if projection.lifecycle().is_terminal()
                || matches!(
                    projection.lifecycle(),
                    EngineRunLifecycle::Completing | EngineRunLifecycle::Terminating
                )
                || !matches!(
                    projection.admission(),
                    AdmissionState::Open | AdmissionState::Paused
                )
            {
                return Err(ServiceError::new(
                    "RUN_CONFLICT",
                    "Run admission can no longer be changed",
                ));
            }
            if projection.admission() == target {
                return self.inner.record(&projection).await;
            }
            let event = PendingExecutionEvent::new(
                ExecutionEventContext::for_run(run_id.clone()),
                ExecutionEventPayload::RunAdmissionChanged { admission: target },
            )
            .map_err(|_| ServiceError::new("RUN_CONFLICT", "admission event is invalid"))?;
            let command = durable_model_adapter::run_transition_nonterminal(
                run_id.clone(),
                projection.projection_version(),
                projection.lifecycle(),
                projection.admission(),
                projection.lifecycle(),
                target,
                event,
                None,
            )?;
            let version = projection.projection_version().to_string();
            let key = transition_key("admission", &[run_id.as_str(), target.as_str(), &version])?;
            match self
                .inner
                .repository
                .commit_run_transition(key, command)
                .await?
            {
                TransitionOutcome::Committed { .. } | TransitionOutcome::ExactReplay { .. } => {
                    break
                }
                TransitionOutcome::StateConflict | TransitionOutcome::StaleLease => continue,
            }
        }
        if target == AdmissionState::Open {
            self.inner.resume_run(&run_id, false).await?;
        }
        self.inner.flush_public_events().await?;
        let projection = self
            .inner
            .repository
            .load_run(&run_id)
            .await?
            .ok_or_else(|| ServiceError::new("RUN_NOT_FOUND", "Run not found"))?;
        if projection.admission() != target
            && !(target == AdmissionState::Open && projection.lifecycle().is_terminal())
        {
            return Err(ServiceError::new(
                "RUN_CONFLICT",
                "Run admission changed concurrently",
            ));
        }
        self.inner.record(&projection).await
    }

    pub async fn cancel(&self, run_id: &str) -> Result<RunRecord, ServiceError> {
        self.cancel_for_tenant("default", run_id).await
    }

    pub async fn cancel_for_tenant(
        &self,
        tenant_id: &str,
        run_id: &str,
    ) -> Result<RunRecord, ServiceError> {
        self.cancel_for_principal(tenant_id, None, run_id).await
    }

    pub async fn cancel_for_principal(
        &self,
        tenant_id: &str,
        user_id: Option<&str>,
        run_id: &str,
    ) -> Result<RunRecord, ServiceError> {
        let run_id = RunId::new(run_id.to_owned())
            .map_err(|_| ServiceError::new("RUN_NOT_FOUND", "Run not found"))?;
        if self.inner.repository.load_run(&run_id).await?.is_none() {
            return self
                .terminal_engine()
                .ok_or_else(|| ServiceError::new("RUN_NOT_FOUND", "Run not found"))?
                .cancel_for_principal(tenant_id, user_id, run_id.as_str())
                .await;
        }
        let authorization = self
            .authorize_full_conversation_principal(tenant_id, user_id, &run_id)
            .await?;
        let mut record = self.cancel_model(run_id).await?;
        if authorization.is_deleted() {
            redact_deleted_conversation_run(&mut record);
        }
        Ok(record)
    }

    async fn authorize_full_conversation_principal(
        &self,
        tenant_id: &str,
        user_id: Option<&str>,
        run_id: &RunId,
    ) -> Result<FullConversationRunAuthorization, ServiceError> {
        let store = self
            .inner
            .conversation_store
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let Some(store) = store else {
            return require_default_full_tenant(tenant_id)
                .map(|()| FullConversationRunAuthorization::Standalone);
        };
        let bound_tenant = store
            .full_conversation_run_tenant(run_id)
            .await
            .map_err(full_conversation_store_error)?;
        match bound_tenant.as_deref() {
            Some(bound) if bound != tenant_id => {
                return Err(ServiceError::new("RUN_NOT_FOUND", "Run not found"));
            }
            Some(_) => {}
            None => {
                require_default_full_tenant(tenant_id)?;
                return Ok(FullConversationRunAuthorization::Standalone);
            }
        }
        let expected_user =
            user_id.ok_or_else(|| ServiceError::new("RUN_NOT_FOUND", "Run not found"))?;
        if store
            .full_conversation_run_user(run_id)
            .await
            .map_err(full_conversation_store_error)?
            .as_deref()
            != Some(expected_user)
        {
            return Err(ServiceError::new("RUN_NOT_FOUND", "Run not found"));
        }
        let deleted = store
            .full_conversation_run_is_deleted(run_id)
            .await
            .map(|deleted| deleted.unwrap_or(false))
            .map_err(full_conversation_store_error)?;
        Ok(FullConversationRunAuthorization::Bound { deleted })
    }

    /// Acquires the same content-free mutation fence used by full
    /// Conversation privacy deletion, then revalidates exact
    /// conversation/tenant/user/run membership.
    pub async fn acquire_visible_full_conversation_run(
        &self,
        conversation_id: &str,
        tenant_id: &str,
        user_id: &str,
        run_id: &str,
    ) -> Result<FullConversationVisibilityGuard, ServiceError> {
        let run_id = RunId::new(run_id.to_owned())
            .map_err(|_| ServiceError::new("RUN_NOT_FOUND", "Run not found"))?;
        self.acquire_visible_full_conversation_response(
            insight_durable::terminal_store::ConversationQuery {
                conversation_id: conversation_id.to_owned(),
                tenant_id: tenant_id.to_owned(),
                user_id: user_id.to_owned(),
            },
            Some(&run_id),
        )
        .await
    }

    async fn acquire_visible_full_conversation_response(
        &self,
        query: insight_durable::terminal_store::ConversationQuery,
        expected_run_id: Option<&RunId>,
    ) -> Result<FullConversationVisibilityGuard, ServiceError> {
        let (store, config) = self.conversation_components()?;
        if !config.conversations_enabled {
            return Err(ServiceError::new(
                "CONVERSATION_UNAVAILABLE",
                "Conversation API is disabled",
            ));
        }
        let mutation = full_conversation_mutation_index(
            &query.conversation_id,
            self.inner.full_conversation_mutations.len(),
        );
        let mutation = Arc::clone(&self.inner.full_conversation_mutations[mutation])
            .lock_owned()
            .await;
        if store
            .get_conversation(query.clone())
            .await
            .map_err(full_conversation_store_error)?
            .is_none()
        {
            return Err(if expected_run_id.is_some() {
                ServiceError::new("RUN_NOT_FOUND", "Run not found")
            } else {
                ServiceError::new("CONVERSATION_NOT_FOUND", "conversation was not found")
            });
        }
        if let Some(expected_run_id) = expected_run_id {
            if !store
                .list_full_conversation_run_ids(query)
                .await
                .map_err(full_conversation_store_error)?
                .contains(expected_run_id)
            {
                return Err(ServiceError::new("RUN_NOT_FOUND", "Run not found"));
            }
        }
        Ok(FullConversationVisibilityGuard {
            _mutation: mutation,
        })
    }

    async fn acquire_full_conversation_response_visibility(
        &self,
        query: insight_durable::terminal_store::ConversationQuery,
    ) -> Result<ConversationResponseVisibilityGuard, ServiceError> {
        let (store, config) = self.conversation_components()?;
        if !config.conversations_enabled {
            return Err(ServiceError::new(
                "CONVERSATION_UNAVAILABLE",
                "Conversation API is disabled",
            ));
        }
        let mutation = full_conversation_mutation_index(
            &query.conversation_id,
            self.inner.full_conversation_mutations.len(),
        );
        let _mutation = Arc::clone(&self.inner.full_conversation_mutations[mutation])
            .lock_owned()
            .await;
        if store
            .get_conversation(query.clone())
            .await
            .map_err(full_conversation_store_error)?
            .is_none()
        {
            return Err(ServiceError::new(
                "CONVERSATION_NOT_FOUND",
                "conversation was not found",
            ));
        }
        let privacy = self.register_full_conversation_stream(&query.conversation_id);
        Ok(ConversationResponseVisibilityGuard::new(privacy))
    }

    /// Revalidates a finite Conversation response under its persistence
    /// engine's exact privacy-delete stripe.
    pub async fn acquire_conversation_response_visibility(
        &self,
        conversation_id: &str,
        tenant_id: &str,
        user_id: &str,
    ) -> Result<ConversationResponseVisibilityGuard, ServiceError> {
        let (store, config) = self.conversation_components()?;
        if !config.conversations_enabled {
            return Err(ServiceError::new(
                "CONVERSATION_UNAVAILABLE",
                "Conversation API is disabled",
            ));
        }
        let query = insight_durable::terminal_store::ConversationQuery {
            conversation_id: conversation_id.to_owned(),
            tenant_id: tenant_id.to_owned(),
            user_id: user_id.to_owned(),
        };
        let conversation = store
            .get_conversation(query.clone())
            .await
            .map_err(full_conversation_store_error)?
            .ok_or_else(|| {
                ServiceError::new("CONVERSATION_NOT_FOUND", "conversation was not found")
            })?;
        match conversation.persistence_mode {
            PersistenceMode::Full => {
                self.acquire_full_conversation_response_visibility(query)
                    .await
            }
            PersistenceMode::TerminalOnly => {
                self.terminal_engine()
                    .ok_or_else(terminal_only_not_installed)?
                    .acquire_conversation_response_visibility(conversation_id, tenant_id, user_id)
                    .await
            }
        }
    }

    async fn full_run_response_visibility(
        &self,
        tenant_id: &str,
        user_id: Option<&str>,
        run_id: &RunId,
    ) -> Result<FullRunResponseVisibility, ServiceError> {
        let store = self
            .inner
            .conversation_store
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let Some(store) = store else {
            require_default_full_tenant(tenant_id)?;
            return Ok(FullRunResponseVisibility::Standalone);
        };
        let Some(conversation_id) = store
            .full_conversation_run_conversation_id(run_id)
            .await
            .map_err(full_conversation_store_error)?
        else {
            require_default_full_tenant(tenant_id)?;
            return Ok(FullRunResponseVisibility::Standalone);
        };
        let mutation = full_conversation_mutation_index(
            &conversation_id,
            self.inner.full_conversation_mutations.len(),
        );
        let _mutation = Arc::clone(&self.inner.full_conversation_mutations[mutation])
            .lock_owned()
            .await;

        if store
            .full_conversation_run_tenant(run_id)
            .await
            .map_err(full_conversation_store_error)?
            .as_deref()
            != Some(tenant_id)
        {
            return Err(ServiceError::new("RUN_NOT_FOUND", "Run not found"));
        }
        let expected_user =
            user_id.ok_or_else(|| ServiceError::new("RUN_NOT_FOUND", "Run not found"))?;
        if store
            .full_conversation_run_user(run_id)
            .await
            .map_err(full_conversation_store_error)?
            .as_deref()
            != Some(expected_user)
        {
            return Err(ServiceError::new("RUN_NOT_FOUND", "Run not found"));
        }
        if store
            .full_conversation_run_is_deleted(run_id)
            .await
            .map_err(full_conversation_store_error)?
            .unwrap_or(true)
        {
            return Ok(FullRunResponseVisibility::Deleted);
        }
        let query = insight_durable::terminal_store::ConversationQuery {
            conversation_id,
            tenant_id: tenant_id.to_owned(),
            user_id: expected_user.to_owned(),
        };
        if store
            .get_conversation(query.clone())
            .await
            .map_err(full_conversation_store_error)?
            .is_none()
            || !store
                .list_full_conversation_run_ids(query.clone())
                .await
                .map_err(full_conversation_store_error)?
                .contains(run_id)
        {
            return Err(ServiceError::new("RUN_NOT_FOUND", "Run not found"));
        }
        let privacy = self.register_full_conversation_stream(&query.conversation_id);
        Ok(FullRunResponseVisibility::Visible(
            ConversationResponseVisibilityGuard::new(privacy),
        ))
    }

    /// Final privacy calibration for a finite Run response. If full
    /// Conversation deletion won the stripe, content is redacted before the
    /// response can be serialized. Otherwise the returned guard must live
    /// through body delivery.
    pub async fn finalize_run_response_visibility(
        &self,
        tenant_id: &str,
        user_id: Option<&str>,
        run: &mut RunRecord,
    ) -> Result<Option<ConversationResponseVisibilityGuard>, ServiceError> {
        let run_id = RunId::new(run.run_id.clone())
            .map_err(|_| ServiceError::new("RUN_NOT_FOUND", "Run not found"))?;
        if self.inner.repository.load_run(&run_id).await?.is_some() {
            return match self
                .full_run_response_visibility(tenant_id, user_id, &run_id)
                .await?
            {
                FullRunResponseVisibility::Standalone => Ok(None),
                FullRunResponseVisibility::Visible(guard) => Ok(Some(guard)),
                FullRunResponseVisibility::Deleted => {
                    redact_deleted_conversation_run(run);
                    Ok(None)
                }
            };
        }
        self.terminal_engine()
            .ok_or_else(|| ServiceError::new("RUN_NOT_FOUND", "Run not found"))?
            .acquire_run_response_visibility(tenant_id, user_id, run_id.as_str())
            .await
    }

    /// Acquires a privacy fence for content surfaces that cannot be redacted
    /// (trace and Artifact bytes). Deleted Conversation content is therefore
    /// indistinguishable from a missing Run.
    pub async fn acquire_run_content_response_visibility(
        &self,
        tenant_id: &str,
        user_id: Option<&str>,
        run_id: &str,
    ) -> Result<Option<ConversationResponseVisibilityGuard>, ServiceError> {
        let run_id = RunId::new(run_id.to_owned())
            .map_err(|_| ServiceError::new("RUN_NOT_FOUND", "Run not found"))?;
        if self.inner.repository.load_run(&run_id).await?.is_some() {
            return match self
                .full_run_response_visibility(tenant_id, user_id, &run_id)
                .await?
            {
                FullRunResponseVisibility::Standalone => Ok(None),
                FullRunResponseVisibility::Visible(guard) => Ok(Some(guard)),
                FullRunResponseVisibility::Deleted => {
                    Err(ServiceError::new("RUN_NOT_FOUND", "Run not found"))
                }
            };
        }
        self.terminal_engine()
            .ok_or_else(|| ServiceError::new("RUN_NOT_FOUND", "Run not found"))?
            .acquire_run_response_visibility(tenant_id, user_id, run_id.as_str())
            .await
    }

    /// Acquires a content-free privacy fence immediately before a
    /// terminal-only Conversation SSE stream publishes its terminal frame.
    pub async fn acquire_visible_terminal_conversation_run(
        &self,
        tenant_id: &str,
        user_id: &str,
        run_id: &str,
    ) -> Result<ConversationVisibilityGuard, ServiceError> {
        self.terminal_engine()
            .ok_or_else(terminal_only_not_installed)?
            .acquire_conversation_visibility_guard(tenant_id, user_id, run_id)
            .await
    }

    async fn cancel_model(&self, run_id: RunId) -> Result<RunRecord, ServiceError> {
        self.terminate_model(run_id, TerminationReason::Cancelled, "cancel")
            .await
    }

    /// Records a runtime-owned interruption through the same durable,
    /// first-winner termination authority as cancellation and root timeout.
    /// A terminal Run is returned unchanged.
    pub async fn interrupt(&self, run_id: &str) -> Result<RunRecord, ServiceError> {
        let run_id = RunId::new(run_id.to_owned())
            .map_err(|_| ServiceError::new("RUN_NOT_FOUND", "Run not found"))?;
        self.terminate_model(run_id, TerminationReason::Interrupted, "interrupt")
            .await
    }

    async fn terminate_model(
        &self,
        run_id: RunId,
        reason: TerminationReason,
        operation: &str,
    ) -> Result<RunRecord, ServiceError> {
        self.inner
            .claim_termination(&run_id, reason, operation)
            .await?;
        // Control termination is a draining operation, so it must not be
        // stranded by the ordinary admission limit. A process may temporarily
        // exceed the limit only to drive an existing Run toward terminal.
        self.inner.resume_run(&run_id, true).await?;
        self.inner.flush_public_events().await?;
        let projection = self
            .inner
            .repository
            .load_run(&run_id)
            .await?
            .ok_or_else(|| ServiceError::new("RUN_NOT_FOUND", "Run not found"))?;
        self.inner.record(&projection).await
    }

    async fn spawn_background(&self) {
        let mut tasks = self.inner.background.lock().await;
        tasks.push(spawn_worker_coordinator(Arc::clone(&self.inner)));
        tasks.push(spawn_public_event_prune_pump(Arc::clone(&self.inner)));
        tasks.push(spawn_public_event_notification_pump(Arc::clone(
            &self.inner,
        )));
        tasks.push(spawn_work_notification_pump(Arc::clone(&self.inner)));
        tasks.push(spawn_work_notification_publisher(Arc::clone(&self.inner)));
        if self.inner.artifact_store.is_some() {
            tasks.push(spawn_artifact_gc_pump(Arc::clone(&self.inner)));
            tasks.push(spawn_full_conversation_maintenance_pump(Arc::clone(
                &self.inner,
            )));
        }
    }
}

fn full_conversation_terminal_staging_error() -> RepositoryError {
    insight_engine::repository::adapter::repository_error(
        insight_engine::repository::REPOSITORY_STORAGE_FAILURE,
        "full Conversation terminal artifact staging failed",
    )
}

async fn stage_full_conversation_artifact(
    inner: &RunServiceInner,
    tenant_id: &str,
    source_kind: insight_durable::terminal_store::TerminalArtifactSourceKind,
    source_id: &str,
    scope: String,
    value: &Value,
    stage_anchor: Option<(chrono::DateTime<chrono::Utc>, chrono::DateTime<chrono::Utc>)>,
) -> Result<(String, ContentHash), ServiceError> {
    let envelope = json!({
        "kind": "insight_terminal_object",
        "scope": format!("tenant:{tenant_id}:{scope}"),
        "value": value,
    });
    let bytes = serde_jcs::to_vec(&envelope).map_err(|_| {
        ServiceError::new(
            "CONVERSATION_CONTENT_UNAVAILABLE",
            "conversation content is invalid",
        )
    })?;
    let artifact_store = inner.artifact_store.as_ref().ok_or_else(|| {
        ServiceError::new(
            "CONVERSATION_CONTENT_UNAVAILABLE",
            "conversation content store is unavailable",
        )
    })?;
    let artifact = artifact_store
        .artifact_for_bytes(
            &bytes,
            Some(insight_durable::terminal_store::TERMINAL_SCOPED_ARTIFACT_MEDIA_TYPE.to_owned()),
        )
        .map_err(|_| {
            ServiceError::new(
                "CONVERSATION_CONTENT_UNAVAILABLE",
                "conversation artifact is invalid",
            )
        })?;
    let reference = serde_json::to_string(&artifact).map_err(|_| {
        ServiceError::new(
            "CONVERSATION_CONTENT_UNAVAILABLE",
            "conversation artifact reference is invalid",
        )
    })?;
    let config = inner
        .conversation_config
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .as_ref()
        .copied()
        .ok_or_else(|| {
            ServiceError::new("CONVERSATION_UNAVAILABLE", "Conversation API is disabled")
        })?;
    let store = inner
        .conversation_store
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone()
        .ok_or_else(|| {
            ServiceError::new("CONVERSATION_UNAVAILABLE", "Conversation API is disabled")
        })?;
    let retry_grace = config
        .terminal_commit_retry
        .saturating_mul(2)
        .max(Duration::from_secs(1));
    let grace = chrono::Duration::from_std(if stage_anchor.is_some() {
        retry_grace.max(config.run_timeout)
    } else {
        retry_grace
    })
    .map_err(|_| {
        ServiceError::new(
            "CONVERSATION_CONTENT_UNAVAILABLE",
            "conversation artifact grace is invalid",
        )
    })?;
    let (created_at, available_at) = match stage_anchor {
        Some((created_at, deadline_at)) => (created_at, deadline_at + grace),
        None => {
            let created_at = chrono::Utc::now();
            (created_at, created_at + grace)
        }
    };
    store
        .stage_terminal_artifact(insight_durable::terminal_store::NewTerminalArtifactStage {
            tenant_id: tenant_id.to_owned(),
            content_ref: reference.clone(),
            content_hash: artifact.content_hash().clone(),
            source_kind,
            source_id: source_id.to_owned(),
            available_at,
            created_at,
        })
        .await
        .map_err(full_conversation_store_error)?;
    let (hash, size) = artifact_store
        .put_and_verify(&artifact, &bytes)
        .await
        .map_err(|_| {
            ServiceError::new(
                "CONVERSATION_CONTENT_UNAVAILABLE",
                "conversation artifact write failed",
            )
        })?;
    if hash != *artifact.content_hash() || size != artifact.size_bytes() {
        return Err(ServiceError::new(
            "CONVERSATION_CONTENT_UNAVAILABLE",
            "conversation artifact verification failed",
        ));
    }
    Ok((reference, hash))
}

#[async_trait]
impl SchedulerActionPreparer for RunServiceInner {
    async fn prepare_scheduler_action(
        &self,
        action: &PlannedSchedulerAction,
    ) -> Result<(), RepositoryError> {
        let SchedulerAction::CompleteRun { output, .. } = action.intent().action() else {
            return Ok(());
        };
        let Some(store) = self
            .conversation_store
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
        else {
            return Ok(());
        };
        let Some(config) = self
            .conversation_config
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .copied()
        else {
            return Ok(());
        };
        let tenant_id = store
            .full_conversation_run_tenant(action.intent().run_id())
            .await
            .map_err(|_| full_conversation_terminal_staging_error())?;
        let Some(tenant_id) = tenant_id else {
            return Ok(());
        };
        let projection = self
            .repository
            .load_run(action.intent().run_id())
            .await?
            .ok_or_else(full_conversation_terminal_staging_error)?;
        let bytes = serde_jcs::to_vec(output.value())
            .map_err(|_| full_conversation_terminal_staging_error())?;
        if bytes.len() <= config.inline_content_max_bytes {
            return Ok(());
        }
        let deadline_at = match projection.deadline_at() {
            Some(deadline_at) => deadline_at,
            None => {
                projection.created_at()
                    + chrono::Duration::from_std(config.run_timeout)
                        .map_err(|_| full_conversation_terminal_staging_error())?
            }
        };
        stage_full_conversation_artifact(
            self,
            &tenant_id,
            insight_durable::terminal_store::TerminalArtifactSourceKind::AssistantMessage,
            action.intent().run_id().as_str(),
            format!(
                "conversation-assistant:{}",
                action.intent().run_id().as_str()
            ),
            output.value(),
            Some((projection.created_at(), deadline_at)),
        )
        .await
        .map(|_| ())
        .map_err(|_| full_conversation_terminal_staging_error())
    }
}

struct ActiveRunGuard<'a> {
    owner: &'a RunServiceInner,
    run_id: &'a RunId,
    release: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ActiveRunReservation {
    Existing,
    NewlyReserved,
    CapacityFull,
}

enum CoordinatedWorkClaim {
    Scheduler(Box<SchedulerTaskClaim>),
    ModelTool(Box<ModelToolTaskClaim>),
    Recovery(RunId),
}

type WorkerExecutionResult = Result<Result<(), ServiceError>, Box<dyn std::any::Any + Send>>;
type WorkerCompletion =
    Result<(usize, Option<RunId>, WorkerExecutionResult), tokio::task::JoinError>;

impl CoordinatedWorkClaim {
    const fn work_class(&self) -> WorkClass {
        match self {
            Self::Scheduler(_) => WorkClass::SchedulerTask,
            Self::ModelTool(_) => WorkClass::ModelToolTask,
            Self::Recovery(_) => WorkClass::Recovery,
        }
    }
}

impl<'a> ActiveRunGuard<'a> {
    fn new(owner: &'a RunServiceInner, run_id: &'a RunId) -> Self {
        Self {
            owner,
            run_id,
            release: true,
        }
    }

    fn retain(&mut self) {
        self.release = false;
    }
}

impl Drop for ActiveRunGuard<'_> {
    fn drop(&mut self) {
        if self.release {
            self.owner.release_active_run(self.run_id);
        }
    }
}

struct LiveRunGuard<'a> {
    owner: &'a RunServiceInner,
    run_id: &'a RunId,
    remove: bool,
}

struct FullConversationMaintenanceComponents {
    store: Arc<dyn TerminalOnlyStore>,
    artifact_store: Arc<dyn WorkerArtifactStore>,
    config: TerminalOnlyRunConfig,
}

impl<'a> LiveRunGuard<'a> {
    fn new(owner: &'a RunServiceInner, run_id: &'a RunId) -> Self {
        Self {
            owner,
            run_id,
            remove: true,
        }
    }

    fn retain(&mut self) {
        self.remove = false;
    }
}

impl Drop for LiveRunGuard<'_> {
    fn drop(&mut self) {
        if self.remove {
            self.owner.remove_live_run(self.run_id);
        }
    }
}

impl RunServiceInner {
    fn full_conversation_retention_cadence(&self) -> Option<Duration> {
        if self
            .terminal
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_some()
        {
            return None;
        }
        let config = self
            .conversation_config
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .copied()?;
        let _ = config;
        Some(FULL_CONVERSATION_RETENTION_CADENCE)
    }

    fn full_conversation_maintenance_components(
        &self,
    ) -> Option<FullConversationMaintenanceComponents> {
        if self
            .terminal
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_some()
        {
            return None;
        }
        let store = self
            .conversation_store
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()?;
        let artifact_store = self.artifact_store.clone()?;
        let config = self
            .conversation_config
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .copied()?;
        Some(FullConversationMaintenanceComponents {
            store,
            artifact_store,
            config,
        })
    }

    async fn delete_full_conversation_artifact(
        artifact_store: &dyn WorkerArtifactStore,
        reference: &str,
    ) -> Result<(), ServiceError> {
        let artifact: ArtifactRef = serde_json::from_str(reference).map_err(|_| {
            ServiceError::new(
                "CONVERSATION_CONTENT_UNAVAILABLE",
                "conversation artifact reference is invalid",
            )
        })?;
        let locator = artifact_store
            .storage_locator(&artifact)
            .map_err(ServiceError::from)?;
        artifact_store
            .delete(&artifact, &locator)
            .await
            .map_err(ServiceError::from)
    }

    async fn run_full_conversation_maintenance(
        &self,
        include_retention: bool,
    ) -> Result<(), ServiceError> {
        let Some(FullConversationMaintenanceComponents {
            store,
            artifact_store,
            config,
        }) = self.full_conversation_maintenance_components()
        else {
            return Ok(());
        };
        let cycle_started = tokio::time::Instant::now();
        let mut batches_used = 0_usize;
        let retention_observed_at = chrono::Utc::now();
        if include_retention {
            self.metrics
                .full_conversation_retention_backlog_pending
                .store(true, Ordering::Relaxed);
            let conversation_retention = chrono::Duration::from_std(config.conversation_retention)
                .map_err(|_| {
                    ServiceError::new(
                        "RUN_SERVICE_CONFIG_INVALID",
                        "Conversation retention duration is invalid",
                    )
                })?;
            let run_retention = chrono::Duration::from_std(config.run_retention).map_err(|_| {
                ServiceError::new(
                    "RUN_SERVICE_CONFIG_INVALID",
                    "terminal Run retention duration is invalid",
                )
            })?;
            let conversation_before = retention_observed_at - conversation_retention;
            let run_before = retention_observed_at - run_retention;
            let mut backlog_pending = true;
            let mut retention_batches_used = 0_usize;
            while retention_batches_used < FULL_CONVERSATION_RETENTION_BATCH_BUDGET
                && batches_used < MAX_FULL_CONVERSATION_RETENTION_BATCHES_PER_CYCLE
                && cycle_started.elapsed() < FULL_CONVERSATION_RETENTION_CYCLE_BUDGET
            {
                retention_batches_used += 1;
                batches_used += 1;
                let conversations = store
                    .delete_conversations_before(
                        insight_durable::terminal_store::BoundedRetention {
                            before: conversation_before,
                            limit: FULL_CONVERSATION_RETENTION_BATCH_SIZE,
                        },
                    )
                    .await
                    .map_err(full_conversation_store_error)?;
                let runs = store
                    .delete_terminal_runs_before(
                        insight_durable::terminal_store::BoundedRetention {
                            before: run_before,
                            limit: FULL_CONVERSATION_RETENTION_BATCH_SIZE,
                        },
                    )
                    .await
                    .map_err(full_conversation_store_error)?;
                self.metrics
                    .full_conversation_retention_conversations_deleted
                    .fetch_add(conversations.deleted, Ordering::Relaxed);
                self.metrics
                    .full_conversation_retention_runs_deleted
                    .fetch_add(runs.deleted, Ordering::Relaxed);
                self.metrics
                    .full_conversation_retention_batches
                    .fetch_add(1, Ordering::Relaxed);

                let saturated = conversations.deleted
                    >= u64::from(FULL_CONVERSATION_RETENTION_BATCH_SIZE)
                    || runs.deleted >= u64::from(FULL_CONVERSATION_RETENTION_BATCH_SIZE);
                if !saturated {
                    backlog_pending = false;
                    break;
                }
            }
            self.metrics
                .full_conversation_retention_backlog_pending
                .store(backlog_pending, Ordering::Relaxed);
        }

        // Retention and privacy deletion enqueue object work transactionally;
        // consume those jobs in the same maintenance cycle before sweeping
        // producer stages.
        let observed_at = chrono::Utc::now();
        let claim_expires_at = observed_at + FULL_CONVERSATION_MAINTENANCE_CLAIM_LEASE;
        self.metrics
            .full_conversation_content_deletion_backlog_pending
            .store(true, Ordering::Relaxed);
        let mut content_deletion_backlog = true;
        let content_deletion_batch_budget = if include_retention {
            FULL_CONVERSATION_CONTENT_DELETION_BATCH_BUDGET
        } else {
            MAX_FULL_CONVERSATION_RETENTION_BATCHES_PER_CYCLE / 2
        };
        let mut content_deletion_batches_used = 0_usize;
        while content_deletion_batches_used < content_deletion_batch_budget
            && batches_used < MAX_FULL_CONVERSATION_RETENTION_BATCHES_PER_CYCLE
            && cycle_started.elapsed() < FULL_CONVERSATION_RETENTION_CYCLE_BUDGET
        {
            content_deletion_batches_used += 1;
            batches_used += 1;
            self.metrics
                .full_conversation_content_deletion_batches
                .fetch_add(1, Ordering::Relaxed);
            let claims = store
                .claim_content_deletion_jobs(
                    insight_durable::terminal_store::ClaimContentDeletionJobs {
                        claimed_by: self.owner.clone(),
                        observed_at,
                        claim_expires_at,
                        limit: FULL_CONVERSATION_RETENTION_BATCH_SIZE,
                    },
                )
                .await
                .map_err(full_conversation_store_error)?;
            let saturated = claims.len()
                >= usize::try_from(FULL_CONVERSATION_RETENTION_BATCH_SIZE).unwrap_or(usize::MAX);
            let mut failed = false;
            for claim in claims {
                if Self::delete_full_conversation_artifact(
                    artifact_store.as_ref(),
                    &claim.job.content_ref,
                )
                .await
                .is_err()
                {
                    failed = true;
                    break;
                }
                store
                    .ack_content_deletion_job(
                        insight_durable::terminal_store::AckContentDeletionJob {
                            deletion_job_id: claim.job.deletion_job_id,
                            claim_token: claim.claim_token,
                        },
                    )
                    .await
                    .map_err(full_conversation_store_error)?;
            }
            if failed {
                break;
            }
            if !saturated {
                content_deletion_backlog = false;
                break;
            }
        }
        self.metrics
            .full_conversation_content_deletion_backlog_pending
            .store(content_deletion_backlog, Ordering::Relaxed);

        self.metrics
            .full_conversation_staging_backlog_pending
            .store(true, Ordering::Relaxed);
        let mut staging_backlog = true;
        let staging_batch_budget = if include_retention {
            FULL_CONVERSATION_STAGING_BATCH_BUDGET
        } else {
            MAX_FULL_CONVERSATION_RETENTION_BATCHES_PER_CYCLE / 2
        };
        let mut staging_batches_used = 0_usize;
        while staging_batches_used < staging_batch_budget
            && batches_used < MAX_FULL_CONVERSATION_RETENTION_BATCHES_PER_CYCLE
            && cycle_started.elapsed() < FULL_CONVERSATION_RETENTION_CYCLE_BUDGET
        {
            staging_batches_used += 1;
            batches_used += 1;
            self.metrics
                .full_conversation_staging_batches
                .fetch_add(1, Ordering::Relaxed);
            let claims = store
                .claim_terminal_artifact_stages(
                    insight_durable::terminal_store::ClaimTerminalArtifactStages {
                        claimed_by: self.owner.clone(),
                        observed_at,
                        claim_expires_at,
                        limit: FULL_CONVERSATION_RETENTION_BATCH_SIZE,
                    },
                )
                .await
                .map_err(full_conversation_store_error)?;
            let saturated = claims.len()
                >= usize::try_from(FULL_CONVERSATION_RETENTION_BATCH_SIZE).unwrap_or(usize::MAX);
            let mut failed = false;
            for claim in claims {
                match store
                    .resolve_terminal_artifact_stage(
                        insight_durable::terminal_store::ResolveTerminalArtifactStage {
                            staging_id: claim.stage.staging_id.clone(),
                            claim_token: claim.claim_token.clone(),
                        },
                    )
                    .await
                    .map_err(full_conversation_store_error)?
                {
                    insight_durable::terminal_store::TerminalArtifactStageDisposition::Authoritative
                    | insight_durable::terminal_store::TerminalArtifactStageDisposition::Lost => {}
                    insight_durable::terminal_store::TerminalArtifactStageDisposition::DeleteOrphan => {
                        let artifact =
                            match serde_json::from_str::<ArtifactRef>(&claim.stage.content_ref) {
                                Ok(artifact) => artifact,
                                Err(_) => {
                                    failed = true;
                                    break;
                                }
                            };
                        if artifact.media_type()
                            != Some(insight_durable::terminal_store::TERMINAL_SCOPED_ARTIFACT_MEDIA_TYPE)
                            || Self::delete_full_conversation_artifact(
                                artifact_store.as_ref(),
                                &claim.stage.content_ref,
                            )
                            .await
                            .is_err()
                        {
                            failed = true;
                            break;
                        }
                        store
                            .ack_terminal_artifact_stage(
                                insight_durable::terminal_store::AckTerminalArtifactStage {
                                    staging_id: claim.stage.staging_id,
                                    claim_token: claim.claim_token,
                                },
                            )
                            .await
                            .map_err(full_conversation_store_error)?;
                    }
                }
            }
            if failed {
                break;
            }
            if !saturated {
                staging_backlog = false;
                break;
            }
        }
        self.metrics
            .full_conversation_staging_backlog_pending
            .store(staging_backlog, Ordering::Relaxed);
        Ok(())
    }

    fn wake_work(&self, work: u8, source: CoordinatorWakeupSource) {
        if source == CoordinatorWakeupSource::Completion {
            self.request_cross_process_work_hint();
        }
        if self.config.pump_interval != DEFAULT_PUMP_INTERVAL {
            return;
        }
        self.metrics.record_wakeup(source);
        self.pending_work.fetch_or(work, Ordering::Release);
        self.work_wakeup.notify_one();
    }

    fn request_cross_process_work_hint(&self) {
        if self.repository.run_repository_capability() != RunRepositoryCapability::Production
            || self.shutdown.is_cancelled()
        {
            return;
        }
        self.metrics
            .notification_publish_requests
            .fetch_add(1, Ordering::Relaxed);
        self.work_notification_publish_pending
            .store(true, Ordering::Release);
        self.work_notification_publish_wakeup.notify_one();
    }

    /// Publishes the completed function-call item only after its durable
    /// checkpoint and tool-batch activation have committed. Exact activation
    /// replay carries the same frozen identity and canonical arguments, so
    /// the live broker can deterministically deduplicate every frame and seal.
    /// Observation remains best-effort and never changes durable execution.
    fn publish_completed_function_calls(&self, activation: &ModelToolBatchActivation) {
        for task in activation.tasks() {
            let (Some(public_item), Some(arguments_jcs), Some(seal_index)) = (
                task.public_item(),
                task.public_arguments_jcs(),
                task.public_seal_index(),
            ) else {
                continue;
            };
            let Ok(identity) = LiveResponseItemIdentity::new(
                activation.run_id().clone(),
                activation.activation_id().clone(),
                activation.parent_attempt_no(),
                activation.model_call_no(),
                public_item.item_id(),
                public_item.output_index(),
            ) else {
                continue;
            };
            let Ok(publication) = CompletedFunctionCallTailPublication::build(
                identity,
                task.call_id(),
                task.action().name(),
                arguments_jcs,
                seal_index,
            ) else {
                continue;
            };
            let (frames, seal) = publication.into_parts();
            for frame in frames {
                let _ = self.live_response_broker.publish(frame);
            }
            let _ = self.live_response_broker.seal(seal);
        }
    }

    /// Resolve one immutable deployment from the process-local archive or,
    /// when another runtime published it after startup, from one durable
    /// catalog snapshot. Restoring from that snapshot recursively installs the
    /// exact subflow deployment closure frozen in the root binding.
    async fn load_exact_durable_deployment(
        &self,
        deployment_revision_id: &str,
    ) -> Result<Arc<DeployedAgent>, ServiceError> {
        if let Some(deployed) = self.agents.get_deployment(deployment_revision_id) {
            if !deployed.workers_available(self.workers.as_ref()) {
                return Err(ServiceError::new(
                    "RUN_DEPLOYMENT_UNAVAILABLE",
                    "an exact worker version required by a durable deployment is unavailable",
                ));
            }
            return Ok(deployed);
        }
        let stored = self.repository.load_versioned_plan_catalog().await?;
        restore_persisted_deployment(
            &self.agents,
            self.workers.as_ref(),
            &stored,
            deployment_revision_id,
            &mut BTreeSet::new(),
        )
    }

    async fn load_pinned_run_deployment(
        &self,
        projection: &RunProjection,
    ) -> Result<Arc<DeployedAgent>, ServiceError> {
        let deployed = self
            .load_exact_durable_deployment(projection.deployment_revision_id().as_str())
            .await?;
        let plan = deployed.versioned_plan();
        if plan.agent_id() != projection.agent_id()
            || plan.definition_id() != projection.definition_id()
            || plan.definition_revision_id() != projection.definition_revision_id()
            || plan.deployment_revision_id() != projection.deployment_revision_id()
        {
            return Err(ServiceError::new(
                "RUN_DEPLOYMENT_UNAVAILABLE",
                "Run deployment revision does not match its durable pin",
            ));
        }
        Ok(deployed)
    }

    async fn claim_termination(
        &self,
        run_id: &RunId,
        reason: TerminationReason,
        operation: &str,
    ) -> Result<(), ServiceError> {
        for _ in 0..MAX_TERMINATION_CAS_ATTEMPTS {
            let projection = self
                .repository
                .load_run(run_id)
                .await?
                .ok_or_else(|| ServiceError::new("RUN_NOT_FOUND", "Run not found"))?;
            if projection.lifecycle().is_terminal()
                || projection.lifecycle() == EngineRunLifecycle::Terminating
            {
                return Ok(());
            }
            let event = PendingExecutionEvent::new(
                ExecutionEventContext::for_run(run_id.clone()),
                ExecutionEventPayload::RunTerminationClaimed { reason },
            )
            .map_err(|_| ServiceError::new("RUN_CONFLICT", "termination intent is invalid"))?;
            let command = durable_model_adapter::run_transition_termination_intent(
                run_id.clone(),
                projection.projection_version(),
                projection.lifecycle(),
                projection.admission(),
                reason,
                event,
                None,
            )?;
            let key = transition_key(operation, &[run_id.as_str()])?;
            match self.repository.commit_run_transition(key, command).await? {
                TransitionOutcome::Committed { .. } | TransitionOutcome::ExactReplay { .. } => {
                    return Ok(())
                }
                TransitionOutcome::StateConflict | TransitionOutcome::StaleLease => continue,
            }
        }
        Err(ServiceError::new(
            "RUN_CONFLICT",
            "Run termination kept changing concurrently",
        ))
    }

    fn reserve_active_run(&self, run_id: &RunId, draining: bool) -> ActiveRunReservation {
        let mut active = self
            .active_runs
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if active.contains(run_id.as_str()) {
            return ActiveRunReservation::Existing;
        }
        if !draining && active.len() >= self.config.max_concurrent_runs {
            return ActiveRunReservation::CapacityFull;
        }
        active.insert(run_id.as_str().to_owned());
        ActiveRunReservation::NewlyReserved
    }

    /// Reserve capacity before a recovery transaction creates its target Run.
    /// `Ok(false)` means this exact target already owns a slot, which is the
    /// normal idempotent-retry case and must not be released by a new guard.
    fn reserve_recovery_target(&self, run_id: &RunId) -> Result<bool, ServiceError> {
        let mut active = self
            .active_runs
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if active.contains(run_id.as_str()) {
            return Ok(false);
        }
        if active.len() >= self.config.max_concurrent_runs {
            self.metrics
                .admission_capacity_rejected
                .fetch_add(1, Ordering::Relaxed);
            return Err(ServiceError::new(
                "RUN_CAPACITY_EXCEEDED",
                "Run capacity is exhausted",
            ));
        }
        active.insert(run_id.as_str().to_owned());
        Ok(true)
    }

    fn release_active_run(&self, run_id: &RunId) {
        self.active_runs
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(run_id.as_str());
    }

    fn open_subscription(&self, run_id: &RunId) -> broadcast::Receiver<()> {
        let mut live = self
            .live
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        live.entry(run_id.as_str().to_owned())
            .or_insert_with(|| broadcast::channel(self.config.subscriber_capacity).0)
            .subscribe()
    }

    fn remove_live_run(&self, run_id: &RunId) {
        self.live
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(run_id.as_str());
    }

    async fn resume_run(&self, run_id: &RunId, draining: bool) -> Result<(), ServiceError> {
        let Some(mut projection) = self.repository.load_run(run_id).await? else {
            return Ok(());
        };
        if projection.lifecycle().is_terminal() {
            self.release_active_run(run_id);
            return Ok(());
        }
        if projection.admission() == AdmissionState::Paused && !draining {
            return Ok(());
        }

        // Resolve and verify the immutable execution identity before consuming
        // process-local capacity or changing Created -> Active. A runtime that
        // started before this deployment was published restores it, including
        // its frozen subflow closure, from the durable catalog here.
        let agent = self.load_pinned_run_deployment(&projection).await?;
        agent
            .linked_plan()
            .map_err(|_| ServiceError::new("RUN_DEPLOYMENT_UNAVAILABLE", "Plan link failed"))?;

        let mut reservation = match self.reserve_active_run(run_id, draining) {
            ActiveRunReservation::Existing => None,
            ActiveRunReservation::NewlyReserved => Some(ActiveRunGuard::new(self, run_id)),
            ActiveRunReservation::CapacityFull => return Ok(()),
        };
        if projection.lifecycle() == EngineRunLifecycle::Created {
            let event = PendingExecutionEvent::new(
                ExecutionEventContext::for_run(run_id.clone()),
                ExecutionEventPayload::RunLifecycleChanged {
                    lifecycle: EngineRunLifecycle::Active,
                },
            )
            .map_err(|_| ServiceError::new("RUN_CONFLICT", "Run start event is invalid"))?;
            let command = durable_model_adapter::run_transition_nonterminal(
                run_id.clone(),
                projection.projection_version(),
                EngineRunLifecycle::Created,
                AdmissionState::Open,
                EngineRunLifecycle::Active,
                AdmissionState::Open,
                event,
                Some(durable_model_adapter::public_event_intent(
                    PublicEventPayload::RunStarted,
                )),
            )?;
            let key = transition_key(
                "start",
                &[
                    run_id.as_str(),
                    projection.deployment_revision_id().as_str(),
                ],
            )?;
            match self.repository.commit_run_transition(key, command).await? {
                TransitionOutcome::Committed { .. } | TransitionOutcome::ExactReplay { .. } => {}
                TransitionOutcome::StateConflict | TransitionOutcome::StaleLease => {}
            }
            let Some(reloaded) = self.repository.load_run(run_id).await? else {
                return Ok(());
            };
            projection = reloaded;
        }
        if projection.lifecycle().is_terminal() {
            self.release_active_run(run_id);
            return Ok(());
        }
        if projection.admission() == AdmissionState::Paused && !draining {
            if let Some(reservation) = &mut reservation {
                reservation.retain();
            }
            return Ok(());
        }
        let nonce = Uuid::new_v4().simple().to_string();
        let claim = ClaimSchedulerRunCommand::new(
            run_id.clone(),
            self.owner.clone(),
            self.config.scheduler_lease_seconds,
        )?;
        let key = transition_key("claim", &[run_id.as_str(), &self.owner, &nonce])?;
        let Some(fence) = self
            .repository
            .claim_production_scheduler_run(key, claim)
            .await?
        else {
            if let Some(reservation) = &mut reservation {
                reservation.retain();
            }
            return Ok(());
        };
        let linked = agent
            .linked_plan()
            .map_err(|_| ServiceError::new("RUN_DEPLOYMENT_UNAVAILABLE", "Plan link failed"))?;
        let projection_version_before_drive = projection.projection_version();
        let drive = drive_scheduler_until_quiescent_with_preparer(
            self.repository.as_ref(),
            &linked,
            &fence,
            &NoSchedulerCrash,
            self.config.scheduler_action_budget,
            self,
        )
        .await;
        self.release_lease(fence).await?;
        match drive? {
            SchedulerRecoveryOutcome::Quiescent(_)
            | SchedulerRecoveryOutcome::ActionBudgetExhausted => {
                let projection = self.repository.load_run(run_id).await?.ok_or_else(|| {
                    ServiceError::new("RUN_NOT_FOUND", "Run disappeared during scheduling")
                })?;
                if projection.lifecycle().is_terminal() {
                    self.release_active_run(run_id);
                } else if let Some(reservation) = &mut reservation {
                    reservation.retain();
                }
                let mut wake = WORK_SCHEDULER_TASK
                    | WORK_MODEL_TOOL_TASK
                    | WORK_RUNTIME_INGRESS
                    | WORK_PUBLIC_EVENT;
                // PostgreSQL gets this hint from the workflow_runs trigger.
                // SQLite has no cross-process listener; one progress-driven
                // recovery hint closes local subflow/child continuation
                // without turning every quiescent wait into a poll loop.
                if self.config.deployment_mode == RunServiceDeploymentMode::SingleProcessDevelopment
                    && projection.projection_version() > projection_version_before_drive
                {
                    wake |= WORK_RECOVERY;
                }
                self.wake_work(wake, CoordinatorWakeupSource::Completion);
                Ok(())
            }
            SchedulerRecoveryOutcome::Fenced => {
                if let Some(reservation) = &mut reservation {
                    reservation.retain();
                }
                Ok(())
            }
        }
    }

    async fn release_lease(&self, fence: FencedSchedulerRunCommand) -> Result<(), ServiceError> {
        let nonce = Uuid::new_v4().simple().to_string();
        let key = transition_key("release", &[fence.run_id().as_str(), &self.owner, &nonce])?;
        self.repository
            .release_production_scheduler_run(key, fence)
            .await?;
        Ok(())
    }

    async fn runtime_ingress_cycle(&self) -> Result<(), ServiceError> {
        // Both operations are first-winner CAS transactions. Query order is
        // not authority: whichever transaction reaches the durable wait lock
        // first wins, and the other becomes an auditable no-op/rejection.
        let deadlines = self
            .repository
            .list_due_run_deadlines(MAX_RUNTIME_INGRESS_BATCH)
            .await?;
        for run_id in deadlines {
            self.claim_termination(&run_id, TerminationReason::TimedOut, "root_deadline")
                .await?;
            // Root timeout is control termination. It bypasses paused
            // admission and the ordinary new-Run capacity limit so drain can
            // always make forward progress.
            self.resume_run(&run_id, true).await?;
        }

        // Completion reservation is a durable outbox. A process may die
        // after reserving a fenced response but before publishing its signal;
        // replay it here without requiring the human client to retry.
        let completions = self
            .repository
            .list_pending_human_work_item_completions(MAX_RUNTIME_INGRESS_BATCH)
            .await?;
        for completion in completions {
            let item = completion.work_item();
            let command = ReceiveSignalCommand::new(
                item.run_id().clone(),
                completion.signal_id().clone(),
                completion.message_id(),
                item.signal_name(),
                item.activation_id().clone(),
                completion.value().clone(),
            )?;
            if !matches!(
                self.repository.receive_signal(command).await?,
                TransitionOutcome::Committed { .. } | TransitionOutcome::ExactReplay { .. }
            ) {
                return Err(ServiceError::new(
                    "HUMAN_TASK_COMPLETION_CONFLICT",
                    "reserved human completion lost its durable wait race",
                ));
            }
        }

        let signals = self
            .repository
            .list_pending_signal_resolutions(MAX_RUNTIME_INGRESS_BATCH)
            .await?;
        for pending in signals {
            let target = pending.target();
            let key = transition_key(
                "signal.resolve",
                &[target.run_id().as_str(), target.signal_id().as_str()],
            )?;
            if matches!(
                self.repository
                    .resolve_wait_signal(
                        key,
                        ResolveSignalCommand::new(
                            target.run_id().clone(),
                            target.activation_id().clone(),
                            target.signal_id().clone(),
                            target.activation_projection_version(),
                        ),
                    )
                    .await?,
                TransitionOutcome::Committed { .. } | TransitionOutcome::ExactReplay { .. }
            ) {
                self.resume_run(target.run_id(), false).await?;
            }
        }

        self.repository
            .reconcile_human_work_items(MAX_RUNTIME_INGRESS_BATCH)
            .await?;

        let timers = self
            .repository
            .list_due_runtime_timers(MAX_RUNTIME_INGRESS_BATCH)
            .await?;
        for timer in timers {
            let key = transition_key(
                "timer.fire",
                &[timer.run_id().as_str(), timer.timer_id().as_str()],
            )?;
            if matches!(
                self.repository
                    .fire_timer(
                        key,
                        FireTimerCommand::new(
                            timer.run_id().clone(),
                            timer.timer_id().clone(),
                            None,
                        ),
                    )
                    .await?,
                TransitionOutcome::Committed { .. } | TransitionOutcome::ExactReplay { .. }
            ) {
                self.resume_run(timer.run_id(), false).await?;
            }
        }
        self.repository
            .reconcile_wait_late_audits(MAX_RUNTIME_INGRESS_BATCH)
            .await?;
        Ok(())
    }

    async fn claim_worker_task(
        &self,
        worker_index: usize,
        requested_work: u8,
    ) -> Result<Option<CoordinatedWorkClaim>, ServiceError> {
        let claimant = format!("{}_worker_{worker_index}", self.owner);
        // A per-Run limit larger than the process-global pool is valid and
        // naturally collapses to the global limit. This keeps independently
        // tuned configurations usable when the global pool is reduced.
        let effective_per_run = self
            .config
            .max_concurrent_operations_per_run
            .min(self.config.max_concurrent_operations);
        let per_run = u32::try_from(effective_per_run).map_err(|_| {
            ServiceError::new(
                "RUN_SERVICE_CONFIG_INVALID",
                "per-Run worker concurrency is invalid",
            )
        })?;
        let model_tool_first = self.model_tool_turn.fetch_xor(true, Ordering::AcqRel);
        if model_tool_first && requested_work & WORK_MODEL_TOOL_TASK != 0 {
            let started = std::time::Instant::now();
            let mut claims = SchedulerDurableRepository::claim_model_tool_calls(
                self.repository.as_ref(),
                &claimant,
                self.config.task_claim_seconds,
                1,
                per_run,
            )
            .await?;
            self.metrics.record_claim(
                WorkClass::ModelToolTask,
                !claims.is_empty(),
                started.elapsed(),
            );
            if let Some(claim) = claims.pop() {
                return Ok(Some(CoordinatedWorkClaim::ModelTool(Box::new(claim))));
            }
        }
        if requested_work & WORK_SCHEDULER_TASK != 0 {
            let started = std::time::Instant::now();
            let mut claims = SchedulerDurableRepository::claim_scheduler_tasks_with_run_limit(
                self.repository.as_ref(),
                &claimant,
                self.config.task_claim_seconds,
                1,
                per_run,
            )
            .await?;
            self.metrics.record_claim(
                WorkClass::SchedulerTask,
                !claims.is_empty(),
                started.elapsed(),
            );
            if let Some(claim) = claims.pop() {
                return Ok(Some(CoordinatedWorkClaim::Scheduler(Box::new(claim))));
            }
        }
        if !model_tool_first && requested_work & WORK_MODEL_TOOL_TASK != 0 {
            let started = std::time::Instant::now();
            let mut claims = SchedulerDurableRepository::claim_model_tool_calls(
                self.repository.as_ref(),
                &claimant,
                self.config.task_claim_seconds,
                1,
                per_run,
            )
            .await?;
            self.metrics.record_claim(
                WorkClass::ModelToolTask,
                !claims.is_empty(),
                started.elapsed(),
            );
            if let Some(claim) = claims.pop() {
                return Ok(Some(CoordinatedWorkClaim::ModelTool(Box::new(claim))));
            }
        }
        Ok(None)
    }

    async fn execute_worker_claim(&self, claim: CoordinatedWorkClaim) -> Result<(), ServiceError> {
        match claim {
            CoordinatedWorkClaim::Scheduler(claim) => {
                let outcome =
                    consume_claimed_scheduler_task_with_artifact_store_and_retrieval_observer(
                        self.repository.as_ref(),
                        self.workers.as_ref(),
                        &FrozenSchedulerWorkerFailurePolicy,
                        self.config.task_claim_seconds,
                        self.shutdown.child_token(),
                        self.artifact_store.as_deref(),
                        self.live_response_broker.as_ref(),
                        &NoSchedulerCrash,
                        *claim,
                    )
                    .await?;
                if let SchedulerWorkerPumpOutcome::SuspendedForTools { activation, .. } = &outcome {
                    self.publish_completed_function_calls(activation);
                }
            }
            CoordinatedWorkClaim::ModelTool(claim) => {
                consume_claimed_model_tool_task_with_observer(
                    self.repository.as_ref(),
                    self.workers.as_ref(),
                    self.config.task_claim_seconds,
                    self.shutdown.child_token(),
                    self.live_response_broker.as_ref(),
                    &NoSchedulerCrash,
                    *claim,
                )
                .await?;
            }
            CoordinatedWorkClaim::Recovery(run_id) => {
                self.resume_run(&run_id, false).await?;
            }
        }
        Ok(())
    }

    async fn artifact_gc_cycle(&self) -> Result<(), ServiceError> {
        let Some(store) = self.artifact_store.as_deref() else {
            return Ok(());
        };
        // Every Run admitted by this schema registers Artifact retention in
        // the same transaction as terminal state. Looking for the absence of
        // that receipt is an unbounded anti-join over all closed Runs, so it
        // must not run as periodic online maintenance. The repository scanner
        // and legacy command remain available to an explicit pre-cutover
        // verifier/repair tool, where a historical full scan is intentional.
        let orphan_retention_seconds =
            u32::try_from(self.config.artifact_orphan_retention.as_secs()).map_err(|_| {
                ServiceError::new(
                    "RUN_SERVICE_CONFIG_INVALID",
                    "artifact orphan retention is invalid",
                )
            })?;
        let nonce = Uuid::new_v4().simple().to_string();
        let command = OrphanSweepCommand::new(
            orphan_retention_seconds,
            format!("{}_artifact_gc", self.owner),
            self.config.artifact_deletion_claim_seconds,
            MAX_RUNTIME_INGRESS_BATCH,
        )?;
        let key = transition_key("artifact.gc", &[&self.owner, &nonce])?;
        let batch = match self.repository.sweep_orphan_artifacts(key, command).await? {
            TransitionOutcome::Committed { result } => result,
            TransitionOutcome::ExactReplay { authoritative } => authoritative,
            TransitionOutcome::StaleLease | TransitionOutcome::StateConflict => return Ok(()),
        };
        for claim in batch.claims() {
            store
                .delete(claim.artifact(), claim.storage_locator())
                .await?;
            match self
                .repository
                .acknowledge_artifact_deleted(AcknowledgeArtifactDeletionCommand::from_claim(claim))
                .await?
            {
                TransitionOutcome::Committed { .. } | TransitionOutcome::ExactReplay { .. } => {}
                TransitionOutcome::StaleLease | TransitionOutcome::StateConflict => {}
            }
        }
        Ok(())
    }

    async fn flush_public_events(&self) -> Result<(), ServiceError> {
        let retention_seconds = u32::try_from(
            self.config.public_event_nonterminal_retention.as_secs(),
        )
        .map_err(|_| {
            ServiceError::new(
                "RUN_SERVICE_CONFIG_INVALID",
                "public event retention is invalid",
            )
        })?;
        let claims = self
            .repository
            .claim_public_events(
                &format!("{}_public", self.owner),
                self.config.task_claim_seconds,
                MAX_PUBLIC_EVENT_BATCH,
            )
            .await?;
        for claim in claims {
            if self
                .repository
                .publish_public_event(&claim, retention_seconds)
                .await?
            {
                // Publication claims carry a safe envelope for dispatch, but
                // live delivery always crosses the same durable-by-ID read
                // boundary as a remote PostgreSQL notification.
                self.deliver_published_public_event(claim.public_event_id())
                    .await?;
            }
        }
        Ok(())
    }

    async fn public_event_prune_cycle(&self) -> Result<(), ServiceError> {
        self.repository
            .prune_expired_public_events(MAX_PUBLIC_EVENT_PRUNE_BATCH)
            .await?;
        Ok(())
    }

    async fn deliver_published_public_event(
        &self,
        public_event_id: &str,
    ) -> Result<(), ServiceError> {
        let Some(published) = self
            .repository
            .load_published_public_event(public_event_id)
            .await?
        else {
            // A malformed, stale or transactionally uncommitted notification
            // is only a hint and has no observable effect.
            return Ok(());
        };
        let sender = self
            .live
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(published.run_id().as_str())
            .cloned();
        if let Some(sender) = sender {
            let _ = sender.send(());
        }
        if published.is_terminal() {
            self.release_active_run(published.run_id());
        }
        Ok(())
    }

    async fn run_event(
        &self,
        run_id: &RunId,
        envelope: &PublicEventEnvelope,
    ) -> Result<RunEvent, ServiceError> {
        let projection = self
            .repository
            .load_run(run_id)
            .await?
            .ok_or_else(|| ServiceError::new("RUN_NOT_FOUND", "Run not found"))?;
        let kind = RunEventType::parse(envelope.kind().as_str())
            .ok_or_else(|| ServiceError::new("RUN_SERVICE_UNAVAILABLE", "public event invalid"))?;
        let seq = envelope.seq().get();
        let payload = serde_json::to_value(envelope.payload())
            .map_err(|_| ServiceError::new("RUN_SERVICE_UNAVAILABLE", "public event invalid"))?;
        let scope = RunEventScope::for_run(
            projection.request_id(),
            run_id.as_str(),
            projection.agent_id(),
            projection.deployment_revision_id().as_str(),
        );
        let timestamp = envelope.occurred_at().to_owned();
        let failure_code =
            public_failure(envelope.payload()).map(|failure| failure.code.as_str().to_owned());
        let (code, message) = match kind {
            RunEventType::RunCancelled => ("RUN_CANCELLED".to_owned(), "run cancelled"),
            RunEventType::RunInterrupted => ("RUN_INTERRUPTED".to_owned(), "run interrupted"),
            RunEventType::RunFailed if projection.lifecycle() == EngineRunLifecycle::TimedOut => {
                ("RUN_TIMEOUT".to_owned(), "run timed out")
            }
            RunEventType::RunFailed | RunEventType::OperationFailed => {
                let code = failure_code.unwrap_or_else(|| "RUN_FAILED".to_owned());
                let message = public_failure_message(&code);
                (code, message)
            }
            _ => ("OK".to_owned(), "ok"),
        };
        let event = if code == "OK" {
            RunEvent::ok_at(kind, seq, scope, timestamp, payload)
        } else {
            RunEvent::error_at(kind, seq, scope, timestamp, code, message, payload)
        };
        Ok(event)
    }

    async fn record(&self, projection: &RunProjection) -> Result<RunRecord, ServiceError> {
        if projection.lifecycle().is_terminal() {
            self.release_active_run(projection.run_id());
        }
        let input = self
            .repository
            .load_production_run_input(projection.run_id())
            .await?
            .ok_or_else(|| {
                ServiceError::new(
                    "RUN_SERVICE_UNAVAILABLE",
                    "Run input payload is unavailable",
                )
            })?;
        let terminal_envelope = if projection.lifecycle().is_terminal() {
            self.repository
                .load_terminal_public_event_authority(projection.run_id())
                .await?
        } else {
            None
        };
        let lifecycle = match projection.lifecycle() {
            EngineRunLifecycle::Created => RunLifecycle::Created,
            EngineRunLifecycle::Active
            | EngineRunLifecycle::Waiting
            | EngineRunLifecycle::Completing
            | EngineRunLifecycle::Terminating => RunLifecycle::Running,
            EngineRunLifecycle::Succeeded => RunLifecycle::Completed {
                output: public_output(projection.output().cloned().unwrap_or(Value::Null)),
            },
            EngineRunLifecycle::Failed => {
                let code = terminal_failure_code(terminal_envelope.as_ref())
                    .unwrap_or_else(|| "RUN_FAILED".to_owned());
                RunLifecycle::Failed {
                    error: RunFailure {
                        kind: terminal_failure_kind(terminal_envelope.as_ref()),
                        message: public_failure_message(&code).to_owned(),
                        code,
                    },
                }
            }
            EngineRunLifecycle::TimedOut => RunLifecycle::Failed {
                error: RunFailure {
                    kind: FailureKind::Timeout,
                    code: "RUN_TIMEOUT".to_owned(),
                    message: "run timed out".to_owned(),
                },
            },
            EngineRunLifecycle::Cancelled => RunLifecycle::Cancelled {
                error: StopError {
                    code: "RUN_CANCELLED".to_owned(),
                    message: "run cancelled".to_owned(),
                },
            },
            EngineRunLifecycle::Interrupted => RunLifecycle::Interrupted {
                error: StopError {
                    code: "RUN_INTERRUPTED".to_owned(),
                    message: "run interrupted".to_owned(),
                },
            },
        };
        Ok(RunRecord {
            run_id: projection.run_id().as_str().to_owned(),
            response_id: projection.response_id().to_owned(),
            projection_version: projection.projection_version(),
            request_id: projection.request_id().to_owned(),
            agent_id: projection.agent_id().to_owned(),
            agent_version: projection.deployment_revision_id().as_str().to_owned(),
            attachment: match projection.attachment() {
                PublicRunAttachment::Attached => RunAttachment::Attached,
                PublicRunAttachment::Detached => RunAttachment::Detached,
            },
            lifecycle,
            started_at: projection.started_at(),
            ended_at: projection.terminal_at(),
            updated_at: projection.updated_at(),
            input_summary: summarize_input(&input),
        })
    }
}

fn public_failure(payload: &PublicEventPayload) -> Option<&PublicFailureSummary> {
    match payload {
        PublicEventPayload::OperationFailed { failure, .. }
        | PublicEventPayload::RunFailed { failure }
        | PublicEventPayload::RunCancelled { failure }
        | PublicEventPayload::RunInterrupted { failure } => Some(failure),
        PublicEventPayload::RunCreated
        | PublicEventPayload::RunStarted
        | PublicEventPayload::OperationStarted { .. }
        | PublicEventPayload::OperationCompleted { .. }
        | PublicEventPayload::RunCompleted => None,
    }
}

fn terminal_failure_code(envelope: Option<&PublicEventEnvelope>) -> Option<String> {
    public_failure(envelope?.payload()).map(|failure| failure.code.as_str().to_owned())
}

fn terminal_failure_kind(envelope: Option<&PublicEventEnvelope>) -> FailureKind {
    match envelope
        .and_then(|value| public_failure(value.payload()))
        .map(|failure| failure.kind)
    {
        Some(PublicFailureKind::Workflow) => FailureKind::Workflow,
        Some(PublicFailureKind::Timeout) => FailureKind::Timeout,
        Some(PublicFailureKind::Infrastructure) => FailureKind::Infrastructure,
        Some(PublicFailureKind::Operation | PublicFailureKind::Stop) | None => {
            FailureKind::Operation
        }
    }
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

fn recovery_revision_for_agent(
    agent: &DeployedAgent,
) -> Result<RecoveryRevisionSpec, ServiceError> {
    let plan = agent.versioned_plan();
    RecoveryRevisionSpec::new(
        plan.definition_id(),
        ExecutionRevisionPin::new(
            plan.definition_revision_id().clone(),
            plan.deployment_revision_id().clone(),
            plan.plan_hash().clone(),
            plan.binding_hash().clone(),
        ),
    )
    .map_err(|_| {
        ServiceError::new(
            "RUN_DEPLOYMENT_UNAVAILABLE",
            "deployment revision cannot form a recovery pin",
        )
    })
}

fn recovery_target_id(
    operation: RecoveryOperation,
    source_run_id: &RunId,
    request_id: &str,
) -> Result<RunId, ServiceError> {
    let identity = transition_key(
        "recovery.target",
        &[operation.as_identity(), source_run_id.as_str(), request_id],
    )?;
    let hash = identity
        .as_str()
        .strip_prefix("transition_")
        .ok_or_else(recovery_request_invalid)?;
    RunId::new(format!("run_recovery_{hash}"))
        .map_err(|_| ServiceError::new("RUN_CONFLICT", "failed to derive recovery Run identity"))
}

fn recovery_transition_key(
    operation: RecoveryOperation,
    phase: &str,
    source_run_id: &RunId,
    request_id: &str,
) -> Result<TransitionKey, ServiceError> {
    transition_key(
        "recovery",
        &[
            operation.as_identity(),
            phase,
            source_run_id.as_str(),
            request_id,
        ],
    )
}

fn derive_migration_mappings(
    source: &DeployedAgent,
    target: &DeployedAgent,
    requests: &[MigrationNodeMappingRequest],
) -> Result<Vec<MigrationMappingCompatibility>, ServiceError> {
    if requests.len() > 10_000 {
        return Err(migration_mapping_incompatible());
    }
    let source_plan = source.published().plan();
    let target_plan = target.published().plan();
    if source_plan.metadata().input_contract() != target_plan.metadata().input_contract()
        || source_plan.metadata().output_type() != target_plan.metadata().output_type()
    {
        return Err(migration_mapping_incompatible());
    }
    let source_linked = source
        .linked_plan()
        .map_err(|_| migration_mapping_incompatible())?;
    let target_linked = target
        .linked_plan()
        .map_err(|_| migration_mapping_incompatible())?;
    let mut source_nodes = BTreeSet::new();
    let mut target_nodes = BTreeSet::new();
    let mut mappings = Vec::with_capacity(requests.len());

    for request in requests {
        let source_node_id = NodeId::new(request.source_node_id.clone())
            .map_err(|_| migration_mapping_incompatible())?;
        let target_node_id = NodeId::new(request.target_node_id.clone())
            .map_err(|_| migration_mapping_incompatible())?;
        if !source_nodes.insert(source_node_id.clone())
            || !target_nodes.insert(target_node_id.clone())
        {
            return Err(migration_mapping_incompatible());
        }
        let source_node = source_linked
            .index()
            .node(&source_node_id)
            .ok_or_else(migration_mapping_incompatible)?;
        let target_node = target_linked
            .index()
            .node(&target_node_id)
            .ok_or_else(migration_mapping_incompatible)?;
        if source_node.kind().name() != target_node.kind().name() {
            return Err(migration_mapping_incompatible());
        }
        let signal_wait_pair = matches!(
            (source_node.kind(), target_node.kind()),
            (NodeKind::HumanTask(_), NodeKind::HumanTask(_))
                | (NodeKind::WaitSignal(_), NodeKind::WaitSignal(_))
        );
        let timer_pair = matches!(
            (source_node.kind(), target_node.kind()),
            (NodeKind::Timer(_), NodeKind::Timer(_))
        );
        if (request.rebuild_signal_wait && !signal_wait_pair)
            || (request.rebuild_timer && !timer_pair)
        {
            return Err(migration_mapping_incompatible());
        }

        let port_mapping = derive_migration_port_mapping(
            source_linked.index(),
            target_linked.index(),
            &source_node_id,
            &target_node_id,
            request.ports.as_ref(),
        )?;
        let source_mapping = MigrationNodeMapping::new(
            source_node_id.clone(),
            target_node_id.clone(),
            port_mapping.clone(),
            migration_schema_hash(
                source_linked.index(),
                &source_node_id,
                PortDirection::Input,
                Some(&port_mapping),
            )?,
            migration_schema_hash(
                source_linked.index(),
                &source_node_id,
                PortDirection::Output,
                Some(&port_mapping),
            )?,
            migration_effect_policy_hash(&source_linked, &source_node_id)?,
            request.rebuild_signal_wait,
            request.rebuild_timer,
        );
        let target_mapping = MigrationNodeMapping::new(
            source_node_id,
            target_node_id.clone(),
            port_mapping,
            migration_schema_hash(
                target_linked.index(),
                &target_node_id,
                PortDirection::Input,
                None,
            )?,
            migration_schema_hash(
                target_linked.index(),
                &target_node_id,
                PortDirection::Output,
                None,
            )?,
            migration_effect_policy_hash(&target_linked, &target_node_id)?,
            request.rebuild_signal_wait,
            request.rebuild_timer,
        );
        mappings.push(
            MigrationMappingCompatibility::new(source_mapping, target_mapping)
                .map_err(|_| migration_mapping_incompatible())?,
        );
    }
    Ok(mappings)
}

fn derive_migration_port_mapping(
    source: &PlanIndex<'_>,
    target: &PlanIndex<'_>,
    source_node_id: &NodeId,
    target_node_id: &NodeId,
    requested: Option<&BTreeMap<String, String>>,
) -> Result<BTreeMap<DataPortId, DataPortId>, ServiceError> {
    let source_ports = migration_node_ports(source, source_node_id)?;
    let target_ports = migration_node_ports(target, target_node_id)?;
    if source_ports.len() != target_ports.len() {
        return Err(migration_mapping_incompatible());
    }
    let mut mapping = BTreeMap::new();
    let mut used_targets = BTreeSet::new();
    if let Some(requested) = requested {
        if requested.len() != source_ports.len() {
            return Err(migration_mapping_incompatible());
        }
        for (source_name, target_name) in requested {
            let source_port = unique_migration_port_named(&source_ports, source_name)?;
            let target_port = unique_migration_port_named(&target_ports, target_name)?;
            validate_migration_port_pair(source_port, target_port)?;
            if mapping
                .insert(source_port.id().clone(), target_port.id().clone())
                .is_some()
                || !used_targets.insert(target_port.id().clone())
            {
                return Err(migration_mapping_incompatible());
            }
        }
    } else {
        for source_port in &source_ports {
            let matches = target_ports
                .iter()
                .copied()
                .filter(|target_port| {
                    target_port.direction() == source_port.direction()
                        && target_port.name() == source_port.name()
                })
                .collect::<Vec<_>>();
            let [target_port] = matches.as_slice() else {
                return Err(migration_mapping_incompatible());
            };
            validate_migration_port_pair(source_port, target_port)?;
            if !used_targets.insert(target_port.id().clone()) {
                return Err(migration_mapping_incompatible());
            }
            mapping.insert(source_port.id().clone(), target_port.id().clone());
        }
    }
    if mapping.len() != source_ports.len() || used_targets.len() != target_ports.len() {
        return Err(migration_mapping_incompatible());
    }
    Ok(mapping)
}

fn migration_node_ports<'a>(
    index: &'a PlanIndex<'_>,
    node_id: &NodeId,
) -> Result<Vec<&'a DataPort>, ServiceError> {
    index
        .data_inputs(node_id)
        .iter()
        .chain(index.data_outputs(node_id))
        .map(|port_id| {
            index
                .data_port(port_id)
                .ok_or_else(migration_mapping_incompatible)
        })
        .collect()
}

fn unique_migration_port_named<'a>(
    ports: &[&'a DataPort],
    name: &str,
) -> Result<&'a DataPort, ServiceError> {
    let matches = ports
        .iter()
        .copied()
        .filter(|port| port.name().as_str() == name)
        .collect::<Vec<_>>();
    let [port] = matches.as_slice() else {
        return Err(migration_mapping_incompatible());
    };
    Ok(port)
}

fn validate_migration_port_pair(source: &DataPort, target: &DataPort) -> Result<(), ServiceError> {
    if source.direction() != target.direction()
        || source.value_type() != target.value_type()
        || source.required() != target.required()
    {
        return Err(migration_mapping_incompatible());
    }
    Ok(())
}

fn migration_schema_hash(
    index: &PlanIndex<'_>,
    node_id: &NodeId,
    direction: PortDirection,
    remap: Option<&BTreeMap<DataPortId, DataPortId>>,
) -> Result<ContentHash, ServiceError> {
    let ids = match direction {
        PortDirection::Input => index.data_inputs(node_id),
        PortDirection::Output => index.data_outputs(node_id),
    };
    let mut projection = ids
        .iter()
        .map(|port_id| {
            let port = index
                .data_port(port_id)
                .ok_or_else(migration_mapping_incompatible)?;
            let projected_id = remap
                .map(|mapping| {
                    mapping
                        .get(port_id)
                        .ok_or_else(migration_mapping_incompatible)
                })
                .transpose()?
                .unwrap_or(port_id);
            Ok(json!({
                "port_id": projected_id,
                "value_type": port.value_type(),
                "required": port.required(),
            }))
        })
        .collect::<Result<Vec<_>, ServiceError>>()?;
    projection.sort_by(|left, right| left["port_id"].as_str().cmp(&right["port_id"].as_str()));
    migration_contract_hash(&projection)
}

fn migration_effect_policy_hash(
    linked: &insight_engine::plan::LinkedPlan<'_>,
    node_id: &NodeId,
) -> Result<ContentHash, ServiceError> {
    if let Some(descriptor) = linked.descriptor(node_id) {
        migration_contract_hash(descriptor.worker().effect_policy())
    } else {
        let node = linked
            .index()
            .node(node_id)
            .ok_or_else(migration_mapping_incompatible)?;
        migration_contract_hash(&json!({
            "execution": "scheduler_owned",
            "node_kind": node.kind().name(),
        }))
    }
}

fn migration_contract_hash<T: Serialize + ?Sized>(value: &T) -> Result<ContentHash, ServiceError> {
    serde_jcs::to_vec(value)
        .map(|encoded| ContentHash::from_bytes(&encoded))
        .map_err(|_| migration_mapping_incompatible())
}

fn validate_pending_migration_waits(
    pending: &[PendingMigrationWait],
    mappings: &[MigrationMappingCompatibility],
) -> Result<(), ServiceError> {
    for wait in pending {
        let (node_id, has_signal, has_timer) =
            production_contract_adapter::pending_migration_wait_parts(wait);
        let mapping = mappings
            .iter()
            .find(|mapping| mapping.source().source_node_id() == node_id)
            .ok_or_else(migration_mapping_incompatible)?;
        if (has_signal
            && (!mapping.source().signal_wait_rebuild_declared()
                || !mapping.target().signal_wait_rebuild_declared()))
            || (has_timer
                && (!mapping.source().timer_rebuild_declared()
                    || !mapping.target().timer_rebuild_declared()))
        {
            return Err(migration_mapping_incompatible());
        }
    }
    Ok(())
}

fn migration_mapping_incompatible() -> ServiceError {
    ServiceError::new(
        "MIGRATION_MAPPING_INCOMPATIBLE",
        "migration node mappings are incomplete or incompatible",
    )
}

fn recovery_request_invalid() -> ServiceError {
    ServiceError::new(
        "RECOVERY_REQUEST_INVALID",
        "recovery request identity or contract is invalid",
    )
}

fn recovery_conflict() -> ServiceError {
    ServiceError::new(
        "RECOVERY_CONFLICT",
        "recovery request conflicts with the durable source state",
    )
}

fn recovery_not_ready() -> ServiceError {
    ServiceError::new(
        "RECOVERY_NOT_READY",
        "recovery requires a paused active or waiting Run with all work drained",
    )
}

fn redrive_requires_fork() -> ServiceError {
    ServiceError::new(
        "REDRIVE_REQUIRES_FORK",
        "redrive requires a fork with a new effect lineage",
    )
}

fn recovery_repository_error(error: RepositoryError) -> ServiceError {
    tracing::warn!(
        repository_code = error.code(),
        "durable recovery repository operation failed"
    );
    match error.code() {
        REPOSITORY_INTENT_CONFLICT | REPOSITORY_CONSTRAINT_CONFLICT => recovery_conflict(),
        REPOSITORY_REDRIVE_REQUIRES_FORK => redrive_requires_fork(),
        _ => ServiceError::new(
            "RUN_SERVICE_UNAVAILABLE",
            "durable Run service is unavailable",
        ),
    }
}

fn transition_key(operation: &str, parts: &[&str]) -> Result<TransitionKey, ServiceError> {
    let mut all = Vec::with_capacity(parts.len() + 1);
    all.push(operation);
    all.extend_from_slice(parts);
    TransitionKey::derive(PRODUCTION_TRANSITION_DOMAIN, &all)
        .map_err(|_| ServiceError::new("RUN_CONFLICT", "transition identity is invalid"))
}

fn spawn_worker_coordinator(inner: Arc<RunServiceInner>) -> JoinHandle<()> {
    tokio::spawn(async move {
        let coordinator = inner.config.effective_work_coordinator();
        let mut workers = JoinSet::new();
        let mut in_flight_recovery = BTreeSet::new();
        let mut worker_index = 0_usize;
        let mut idle_interval = coordinator.idle_poll_min_interval;
        let mut jitter_sequence = 0_u64;
        let mut ready_work = if inner.config.pump_interval == DEFAULT_PUMP_INTERVAL {
            WORK_ALL
        } else {
            0
        };
        let mut next_ingress_deadline = None;
        let mut next_safety_deadline =
            tokio::time::Instant::now() + coordinator.safety_poll_interval;
        loop {
            ready_work |= inner.pending_work.swap(0, Ordering::AcqRel);
            let now = tokio::time::Instant::now();
            if now >= next_safety_deadline {
                inner.metrics.record_wakeup(CoordinatorWakeupSource::Safety);
                ready_work |= WORK_ALL;
                next_safety_deadline = now + coordinator.safety_poll_interval;
            }
            if next_ingress_deadline.is_some_and(|deadline| now >= deadline) {
                inner
                    .metrics
                    .record_wakeup(CoordinatorWakeupSource::Deadline);
                ready_work |= WORK_RUNTIME_INGRESS;
                next_ingress_deadline = None;
            }

            if ready_work & WORK_RUNTIME_INGRESS != 0 {
                if let Err(error) = inner.runtime_ingress_cycle().await {
                    tracing::warn!(code = error.code(), "timer/signal ingress cycle failed");
                }
                next_ingress_deadline = match inner.repository.next_runtime_ingress_delay().await {
                    Ok(Some(delay)) => tokio::time::Instant::now().checked_add(delay),
                    Ok(None) => None,
                    Err(error) => {
                        tracing::warn!(code = error.code(), "next timer/deadline discovery failed");
                        tokio::time::Instant::now().checked_add(coordinator.safety_poll_interval)
                    }
                };
            }
            if ready_work & WORK_PUBLIC_EVENT != 0 {
                if let Err(error) = inner.flush_public_events().await {
                    tracing::warn!(code = error.code(), "public event cycle failed");
                }
            }

            let mut claimed_any = false;
            if ready_work & WORK_RECOVERY != 0 {
                let available_permits = inner
                    .config
                    .max_concurrent_operations
                    .saturating_sub(workers.len());
                let recovery_budget = available_permits
                    .min(coordinator.claim_batch_size as usize)
                    .min(MAX_RECOVERY_BATCH as usize);
                if recovery_budget > 0 {
                    let discovery_limit = recovery_budget
                        .saturating_add(in_flight_recovery.len())
                        .min(MAX_RECOVERY_BATCH as usize);
                    match inner
                        .repository
                        .list_recoverable_scheduler_runs(discovery_limit as u32)
                        .await
                    {
                        Ok(runs) => {
                            for run_id in runs
                                .into_iter()
                                .filter(|run_id| in_flight_recovery.insert(run_id.clone()))
                                .take(recovery_budget)
                            {
                                claimed_any = true;
                                let claim = CoordinatedWorkClaim::Recovery(run_id.clone());
                                let work_class = claim.work_class();
                                let task_inner = Arc::clone(&inner);
                                let task_worker_index = worker_index;
                                workers.spawn(async move {
                                    let executing =
                                        task_inner.metrics.executing_counter(work_class);
                                    executing.fetch_add(1, Ordering::Relaxed);
                                    let result = std::panic::AssertUnwindSafe(
                                        task_inner.execute_worker_claim(claim),
                                    )
                                    .catch_unwind()
                                    .await;
                                    executing.fetch_sub(1, Ordering::Relaxed);
                                    (task_worker_index, Some(run_id), result)
                                });
                                worker_index = worker_index.wrapping_add(1);
                            }
                        }
                        Err(error) => {
                            tracing::warn!(
                                code = error.code(),
                                "scheduler recovery discovery failed"
                            );
                        }
                    }
                }
            }

            let requested_worker_work = ready_work & (WORK_SCHEDULER_TASK | WORK_MODEL_TOOL_TASK);
            if requested_worker_work != 0 {
                let available_permits = inner
                    .config
                    .max_concurrent_operations
                    .saturating_sub(workers.len());
                let claim_budget = available_permits.min(coordinator.claim_batch_size as usize);
                for _ in 0..claim_budget {
                    let claim = match std::panic::AssertUnwindSafe(
                        inner.claim_worker_task(worker_index, requested_worker_work),
                    )
                    .catch_unwind()
                    .await
                    {
                        Ok(Ok(claim)) => claim,
                        Ok(Err(error)) => {
                            tracing::warn!(code = error.code(), "scheduler work discovery failed");
                            break;
                        }
                        Err(_) => {
                            tracing::error!("scheduler work discovery panicked");
                            break;
                        }
                    };
                    let Some(claim) = claim else {
                        break;
                    };
                    claimed_any = true;
                    let work_class = claim.work_class();
                    let task_inner = Arc::clone(&inner);
                    let task_worker_index = worker_index;
                    workers.spawn(async move {
                        let executing = task_inner.metrics.executing_counter(work_class);
                        executing.fetch_add(1, Ordering::Relaxed);
                        let result =
                            std::panic::AssertUnwindSafe(task_inner.execute_worker_claim(claim))
                                .catch_unwind()
                                .await;
                        executing.fetch_sub(1, Ordering::Relaxed);
                        (task_worker_index, None, result)
                    });
                    worker_index = worker_index.wrapping_add(1);
                }
                if claimed_any {
                    idle_interval = coordinator.idle_poll_min_interval;
                } else {
                    idle_interval = idle_interval
                        .checked_mul(2)
                        .unwrap_or(coordinator.idle_poll_max_interval)
                        .min(coordinator.idle_poll_max_interval);
                }
            }
            ready_work = 0;

            let poll_interval = if workers.is_empty() {
                jitter_sequence = jitter_sequence.wrapping_add(1);
                jittered_poll_interval(idle_interval, jitter_sequence)
            } else {
                coordinator.active_poll_interval
            };
            let queue_poll_deadline = (workers.len() < inner.config.max_concurrent_operations)
                .then(|| tokio::time::Instant::now() + poll_interval);
            let mut wake_deadline = next_safety_deadline;
            if let Some(deadline) = queue_poll_deadline {
                wake_deadline = wake_deadline.min(deadline);
            }
            if let Some(deadline) = next_ingress_deadline {
                wake_deadline = wake_deadline.min(deadline);
            }

            let completed = if workers.is_empty() {
                tokio::select! {
                    _ = inner.shutdown.cancelled() => None,
                    _ = inner.work_wakeup.notified() => Some(None),
                    _ = tokio::time::sleep_until(wake_deadline) => Some(None),
                }
            } else {
                tokio::select! {
                    _ = inner.shutdown.cancelled() => None,
                    _ = inner.work_wakeup.notified() => Some(None),
                    completed = workers.join_next() => Some(completed),
                    _ = tokio::time::sleep_until(wake_deadline) => Some(None),
                }
            };
            let Some(completed) = completed else {
                while let Some(completed) = workers.join_next().await {
                    remove_completed_recovery(&completed, &mut in_flight_recovery);
                    supervise_worker_completion(completed);
                }
                return;
            };
            if let Some(completed) = completed {
                remove_completed_recovery(&completed, &mut in_flight_recovery);
                inner
                    .metrics
                    .record_wakeup(CoordinatorWakeupSource::Completion);
                inner.request_cross_process_work_hint();
                ready_work |= supervise_worker_completion(completed);
            } else if queue_poll_deadline
                .is_some_and(|deadline| tokio::time::Instant::now() >= deadline)
            {
                ready_work |= if inner.config.pump_interval == DEFAULT_PUMP_INTERVAL {
                    WORK_SCHEDULER_TASK | WORK_MODEL_TOOL_TASK
                } else {
                    WORK_ALL
                };
            }
        }
    })
}

fn jittered_poll_interval(base: Duration, sequence: u64) -> Duration {
    // A deterministic process-local sequence avoids pulling payload-bearing
    // randomness into scheduler logs while still de-synchronizing repeated
    // safety polls. The multiplier spans the required inclusive ±20% range.
    let percent = 80 + sequence.wrapping_mul(17) % 41;
    base.mul_f64(percent as f64 / 100.0)
}

fn remove_completed_recovery(
    completed: &WorkerCompletion,
    in_flight_recovery: &mut BTreeSet<RunId>,
) {
    if let Ok((_, Some(run_id), _)) = completed {
        in_flight_recovery.remove(run_id);
    }
}

fn supervise_worker_completion(completed: WorkerCompletion) -> u8 {
    match completed {
        Ok((_, _, Ok(Ok(())))) => {
            WORK_SCHEDULER_TASK | WORK_MODEL_TOOL_TASK | WORK_PUBLIC_EVENT | WORK_RECOVERY
        }
        Ok((_, _, Ok(Err(error)))) => {
            tracing::warn!(code = error.code(), "scheduler worker execution failed");
            0
        }
        Ok((worker_index, _, Err(_))) => {
            tracing::error!(worker_index, "scheduler worker execution panicked");
            0
        }
        Err(error) => {
            if error.is_cancelled() {
                tracing::warn!("scheduler worker execution was cancelled");
            } else {
                tracing::error!("scheduler worker task failed");
            }
            0
        }
    }
}

fn spawn_public_event_prune_pump(inner: Arc<RunServiceInner>) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = inner.shutdown.cancelled() => return,
                _ = tokio::time::sleep(inner.config.public_event_prune_interval) => {}
            }
            if let Err(error) = inner.public_event_prune_cycle().await {
                tracing::warn!(code = error.code(), "public event retention prune failed");
            }
        }
    })
}

fn spawn_public_event_notification_pump(inner: Arc<RunServiceInner>) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            if inner.shutdown.is_cancelled() {
                return;
            }
            let mut stream = match inner
                .repository
                .open_public_event_notification_stream()
                .await
            {
                Ok(Some(stream)) => stream,
                Ok(None) => return,
                Err(error) => {
                    tracing::warn!(
                        code = error.code(),
                        "public event notification listener failed to connect"
                    );
                    tokio::select! {
                        _ = inner.shutdown.cancelled() => return,
                        _ = tokio::time::sleep(PUBLIC_EVENT_RECONNECT_DELAY) => continue,
                    }
                }
            };

            loop {
                let received = tokio::select! {
                    _ = inner.shutdown.cancelled() => return,
                    received = stream.recv() => received,
                };
                match received {
                    Ok(public_event_id) => {
                        if let Err(error) =
                            inner.deliver_published_public_event(&public_event_id).await
                        {
                            tracing::warn!(
                                code = error.code(),
                                "public event notification delivery failed"
                            );
                        }
                    }
                    Err(error) => {
                        tracing::warn!(
                            code = error.code(),
                            "public event notification listener disconnected"
                        );
                        break;
                    }
                }
            }

            tokio::select! {
                _ = inner.shutdown.cancelled() => return,
                _ = tokio::time::sleep(PUBLIC_EVENT_RECONNECT_DELAY) => {}
            }
        }
    })
}

fn spawn_work_notification_pump(inner: Arc<RunServiceInner>) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            if inner.shutdown.is_cancelled() {
                return;
            }
            let mut stream = match inner.repository.open_work_notification_stream().await {
                Ok(Some(stream)) => {
                    inner
                        .metrics
                        .notification_listener_connected
                        .store(true, Ordering::Release);
                    stream
                }
                Ok(None) => return,
                Err(error) => {
                    inner
                        .metrics
                        .notification_listener_connected
                        .store(false, Ordering::Release);
                    inner
                        .metrics
                        .notification_listener_reconnects
                        .fetch_add(1, Ordering::Relaxed);
                    tracing::warn!(
                        code = error.code(),
                        "durable work notification listener failed to connect"
                    );
                    tokio::select! {
                        _ = inner.shutdown.cancelled() => return,
                        _ = tokio::time::sleep(
                            inner.config.work_coordinator.notification_reconnect_interval
                        ) => continue,
                    }
                }
            };
            // Anything committed while the listener was disconnected is
            // recovered by an immediate all-class safety scan.
            inner.wake_work(WORK_ALL, CoordinatorWakeupSource::Safety);
            loop {
                let received = tokio::select! {
                    _ = inner.shutdown.cancelled() => return,
                    received = stream.recv() => received,
                };
                match received {
                    Ok(work) => {
                        inner.wake_work(work_class_bit(work), CoordinatorWakeupSource::Notify)
                    }
                    Err(error) => {
                        inner
                            .metrics
                            .notification_listener_connected
                            .store(false, Ordering::Release);
                        inner
                            .metrics
                            .notification_listener_reconnects
                            .fetch_add(1, Ordering::Relaxed);
                        tracing::warn!(
                            code = error.code(),
                            "durable work notification listener disconnected"
                        );
                        break;
                    }
                }
            }
            tokio::select! {
                _ = inner.shutdown.cancelled() => return,
                _ = tokio::time::sleep(
                    inner.config.work_coordinator.notification_reconnect_interval
                ) => {}
            }
        }
    })
}

fn spawn_work_notification_publisher(inner: Arc<RunServiceInner>) -> JoinHandle<()> {
    tokio::spawn(async move {
        if inner.repository.run_repository_capability() != RunRepositoryCapability::Production {
            return;
        }
        let debounce = inner
            .config
            .work_coordinator
            .notification_reconnect_interval;
        loop {
            tokio::select! {
                _ = inner.shutdown.cancelled() => return,
                _ = inner.work_notification_publish_wakeup.notified() => {}
            }
            tokio::select! {
                _ = inner.shutdown.cancelled() => return,
                _ = tokio::time::sleep(debounce) => {}
            }
            if !inner
                .work_notification_publish_pending
                .swap(false, Ordering::AcqRel)
            {
                continue;
            }
            match inner
                .repository
                .publish_work_notification(WorkClass::Maintenance)
                .await
            {
                Ok(()) => {
                    inner
                        .metrics
                        .notification_published
                        .fetch_add(1, Ordering::Relaxed);
                }
                Err(error) => {
                    inner
                        .metrics
                        .notification_publish_errors
                        .fetch_add(1, Ordering::Relaxed);
                    inner
                        .work_notification_publish_pending
                        .store(true, Ordering::Release);
                    inner.work_notification_publish_wakeup.notify_one();
                    tracing::warn!(
                        code = error.code(),
                        "cross-process durable work notification publish failed"
                    );
                }
            }
        }
    })
}

const fn work_class_bit(work: WorkClass) -> u8 {
    match work {
        WorkClass::SchedulerTask => WORK_SCHEDULER_TASK,
        WorkClass::ModelToolTask => WORK_MODEL_TOOL_TASK,
        WorkClass::RuntimeIngress => WORK_RUNTIME_INGRESS,
        WorkClass::PublicEvent => WORK_PUBLIC_EVENT,
        WorkClass::Recovery => WORK_RECOVERY,
        WorkClass::Maintenance => WORK_ALL,
    }
}

fn spawn_artifact_gc_pump(inner: Arc<RunServiceInner>) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = inner.shutdown.cancelled() => return,
                _ = tokio::time::sleep(inner.config.artifact_gc_interval) => {}
            }
            if let Err(error) = inner.artifact_gc_cycle().await {
                tracing::warn!(code = error.code(), "artifact GC cycle failed");
            }
        }
    })
}

fn spawn_full_conversation_maintenance_pump(inner: Arc<RunServiceInner>) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut artifact_interval = tokio::time::interval(Duration::from_secs(5));
        artifact_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut next_retention = tokio::time::Instant::now();
        loop {
            tokio::select! {
                _ = inner.shutdown.cancelled() => return,
                _ = artifact_interval.tick() => {}
            }
            let now = tokio::time::Instant::now();
            let retention_cadence = inner.full_conversation_retention_cadence();
            let include_retention = match retention_cadence {
                Some(cadence) if now >= next_retention => {
                    // This deadline is advanced before I/O so a failing store
                    // cannot turn the 5-second object-maintenance loop into a
                    // retention scan loop.
                    next_retention = now + cadence;
                    true
                }
                Some(_) | None => false,
            };
            if let Err(error) = inner
                .run_full_conversation_maintenance(include_retention)
                .await
            {
                tracing::warn!(code = error.code(), "full Conversation maintenance failed");
            }
        }
    })
}

pub(crate) async fn test_run_event(
    service: &RunService,
    run_id: &RunId,
    envelope: &PublicEventEnvelope,
) -> Result<RunEvent, ServiceError> {
    service.inner.run_event(run_id, envelope).await
}

pub(crate) fn test_subscription(service: &RunService, run_id: &RunId) -> RunSubscription {
    RunSubscription {
        run_id: run_id.as_str().to_owned(),
        run_id_model: run_id.clone(),
        receiver: service.inner.open_subscription(run_id),
        owner: Arc::downgrade(&service.inner),
        position: None,
        last_seq: 0,
        terminal: false,
    }
}

pub(crate) async fn test_deliver_published_public_event(
    service: &RunService,
    public_event_id: &str,
) -> Result<(), ServiceError> {
    service
        .inner
        .deliver_published_public_event(public_event_id)
        .await
}

pub(crate) async fn test_flush_public_events(service: &RunService) -> Result<(), ServiceError> {
    service.inner.flush_public_events().await
}

pub(crate) fn test_has_live_subscription(service: &RunService, run_id: &RunId) -> bool {
    service
        .inner
        .live
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .contains_key(run_id.as_str())
}

#[cfg(test)]
mod conversation_stream_privacy_tests {
    use super::*;

    fn private_run(lifecycle: RunLifecycle) -> RunRecord {
        let now = chrono::Utc::now();
        RunRecord {
            run_id: "run_private_redaction".to_owned(),
            response_id: "resp_private_redaction".to_owned(),
            projection_version: 1,
            request_id: "request-redaction".to_owned(),
            agent_id: "agent-redaction".to_owned(),
            agent_version: "1".to_owned(),
            attachment: RunAttachment::Detached,
            lifecycle,
            started_at: Some(now),
            ended_at: Some(now),
            updated_at: now,
            input_summary: json!({"private": "deleted-content-marker"}),
        }
    }

    #[test]
    fn deleted_conversation_redaction_clears_success_and_failure_payloads() {
        let terminal_states = [
            RunLifecycle::Completed {
                output: RunOutput {
                    content: Some("deleted-content-marker".to_owned()),
                    format: Some("text".to_owned()),
                    data: json!({"private": "deleted-content-marker"}),
                },
            },
            RunLifecycle::Failed {
                error: RunFailure {
                    kind: FailureKind::Workflow,
                    code: "deleted-content-marker".to_owned(),
                    message: "deleted-content-marker".to_owned(),
                },
            },
            RunLifecycle::Cancelled {
                error: StopError {
                    code: "deleted-content-marker".to_owned(),
                    message: "deleted-content-marker".to_owned(),
                },
            },
            RunLifecycle::Interrupted {
                error: StopError {
                    code: "deleted-content-marker".to_owned(),
                    message: "deleted-content-marker".to_owned(),
                },
            },
        ];

        for lifecycle in terminal_states {
            let expected_status = lifecycle.status();
            let mut record = private_run(lifecycle);
            redact_deleted_conversation_run(&mut record);
            assert_eq!(record.status(), expected_status);
            assert!(
                !serde_json::to_string(&record)
                    .unwrap()
                    .contains("deleted-content-marker"),
                "redacted terminal state still exposed deleted content"
            );
        }
    }

    #[tokio::test]
    async fn second_delete_sweep_cancels_a_stream_registered_after_preflight() {
        let streams = Mutex::new(BTreeMap::new());
        cancel_conversation_streams(&streams, "conversation-race").await;

        let late = ConversationStreamPrivacy::new();
        streams
            .lock()
            .unwrap()
            .entry("conversation-race".to_owned())
            .or_insert_with(Vec::new)
            .push(late.downgrade());
        cancel_conversation_streams(&streams, "conversation-race").await;

        assert!(
            late.is_cancelled(),
            "the in-lock delete sweep must cancel registration that raced preflight"
        );
    }
}

#[cfg(test)]
mod public_artifact_authorization_tests {
    use serde_json::json;

    use insight_engine::response::adapter::durable_response_snapshot_new;

    use super::*;
    use insight_durable::{ResponseTerminalKind, ResponseUsageStatus};

    fn snapshot(run_id: &RunId, workflow: Value) -> DurableResponseSnapshot {
        let response_id = format!("resp_{}", run_id.as_str());
        let response = json!({
            "id": response_id,
            "object": "response",
            "status": "completed",
            "output": [],
            "usage": null,
            "error": null
        });
        let manifest = json!([]);
        let projection = json!({
            "response_id": response_id,
            "terminal_kind": "response.completed",
            "response": response,
            "workflow": workflow,
            "public_item_manifest": manifest,
            "usage": Value::Null,
            "usage_status": "unavailable",
        });
        let hash = ContentHash::from_bytes(&serde_jcs::to_vec(&projection).unwrap());
        durable_response_snapshot_new(
            response_id,
            ResponseTerminalKind::Completed,
            response,
            workflow,
            manifest,
            None,
            ResponseUsageStatus::Unavailable,
            hash,
        )
        .unwrap()
    }

    #[test]
    fn only_exact_refs_in_the_hashed_public_snapshot_authorize_a_handle() {
        let run_id = RunId::new("run_public_artifact").unwrap();
        let bytes = b"image bytes";
        let reference = ArtifactRef::new(
            ArtifactId::new("artifact_public_image").unwrap(),
            ContentHash::from_bytes(bytes),
            bytes.len() as u64,
            Some("image/png".to_owned()),
        )
        .unwrap();
        let snapshot = snapshot(
            &run_id,
            json!({
                "run_id": run_id.as_str(),
                "result": {"answer": "done"},
                "tool_results": [{
                    "content": [{"type": "output_image", "artifact": reference}]
                }],
                "retrievals": [],
                "usage_status": "unavailable"
            }),
        );
        assert_eq!(
            public_artifact_reference(&snapshot, &run_id, reference.artifact_id()).unwrap(),
            Some(reference.clone())
        );
        assert!(public_artifact_reference(
            &snapshot,
            &run_id,
            &ArtifactId::new("artifact_other").unwrap()
        )
        .unwrap()
        .is_none());

        let other_run = RunId::new("run_other").unwrap();
        assert_eq!(
            public_artifact_reference(&snapshot, &other_run, reference.artifact_id())
                .unwrap_err()
                .code(),
            "RUN_SERVICE_UNAVAILABLE"
        );
    }

    #[test]
    fn conflicting_public_refs_fail_closed_and_artifact_body_is_not_debuggable() {
        let run_id = RunId::new("run_conflicting_artifact").unwrap();
        let artifact_id = ArtifactId::new("artifact_conflict").unwrap();
        let first = ArtifactRef::new(
            artifact_id.clone(),
            ContentHash::from_bytes(b"first"),
            5,
            Some("text/plain".to_owned()),
        )
        .unwrap();
        let second = ArtifactRef::new(
            artifact_id.clone(),
            ContentHash::from_bytes(b"second"),
            6,
            Some("text/plain".to_owned()),
        )
        .unwrap();
        let snapshot = snapshot(
            &run_id,
            json!({
                "run_id": run_id.as_str(),
                "result": [first, second],
                "tool_results": [],
                "retrievals": [],
                "usage_status": "unavailable"
            }),
        );
        assert_eq!(
            public_artifact_reference(&snapshot, &run_id, &artifact_id)
                .unwrap_err()
                .code(),
            "RUN_SERVICE_UNAVAILABLE"
        );

        let body = b"body must stay private".to_vec();
        let public = PublicArtifact::new(
            ArtifactRef::new(
                artifact_id,
                ContentHash::from_bytes(&body),
                body.len() as u64,
                None,
            )
            .unwrap(),
            body,
        );
        let debug = format!("{public:?}");
        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("body must stay private"));
    }
}
