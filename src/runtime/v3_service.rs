//! Production host for the durable v3 scheduler.
//!
//! This service owns only process-local admission, live subscriptions and
//! background pumps. Canonical Plan, Run state, task delivery, scheduler
//! checkpoints and terminal authority remain in `DurableRepository`.

use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error,
    fmt,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex, RwLock, Weak,
    },
    time::Duration,
};

use crate::engine::repository::{
    PublicEventIntentAdapter as _, RunTransitionCommandAdapter as _, VersionedPlanAdapter as _,
};
use async_trait::async_trait;
use futures::FutureExt;
use insight_durable::production::adapter as production_contract_adapter;
use insight_durable::recovery_repository::adapter as recovery_contract_adapter;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sqlx::Row;
use tokio::{
    sync::{broadcast, Mutex as AsyncMutex},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::engine::repository::RepositoryErrorExt as _;

pub use insight_durable::{PendingMigrationWait, ProductionRunRepository, RunRepositoryCapability};

use crate::{
    catalog_v3::{
        subflow_registry_for_available, subflow_registry_for_stored, AgentStreamingSourceKind,
        DeployedV3Agent, DeploymentRiskDiagnostic, LeafDeploymentResolver, PublishedV3Agent,
    },
    dsl::v3::{
        GraphAuthorDocument, GraphSemanticEditBatch, StoredGraphView, TraceOverlay, ViewDocument,
        GRAPH_EDIT_CONFLICT,
    },
    engine::{
        plan::{DataPort, DataPortId, NodeKind, PlanIndex, PortDirection},
        repository::{
            consume_model_tool_task_once_with_observer,
            consume_scheduler_task_once_with_artifact_store_and_retrieval_observer,
            consume_scheduler_task_once_with_retrieval_observer, drive_scheduler_until_quiescent,
            AcknowledgeArtifactDeletionCommand, BeginMigrationCommand,
            BindArtifactStoreAuthorityCommand, ClaimHumanWorkItemCommand, ClaimSchedulerRunCommand,
            CompleteHumanWorkItemCommand, ContinueAsNewCommand, CreateRunCommand,
            DurableResponseSnapshot, FencedSchedulerRunCommand, FinalizeMigrationCommand,
            FireTimerCommand, ForkRunCommand, FrozenSchedulerWorkerFailurePolicy,
            HumanTaskPrincipal, HumanWorkItem, HumanWorkItemId, MigrationMappingCompatibility,
            ModelToolBatchActivation, ModelToolWorkerPumpOutcome, NoSchedulerCrash,
            OrderedPublicEventRead, OrphanSweepCommand, PlanPublicationOutcome,
            PublicEventPosition, PublicRunAttachment, PublicationHead, PublicationOrigin,
            PublishVersionedPlanCommand, ReceiveSignalCommand, RecoveryRevisionSpec,
            RecoveryRunReceipt, RedriveRunCommand, ReleaseRunArtifactRetentionCommand,
            RepositoryError, ResolveSignalCommand, RunProjection, RunTransitionCommand,
            SchedulerLeaseRepository, SchedulerRecoveryOutcome, SchedulerWorkerPumpOutcome,
            SignalInboxState, VersionedPlanCatalog, REPOSITORY_ARTIFACT_STORE_CONFLICT,
            REPOSITORY_CONSTRAINT_CONFLICT, REPOSITORY_INTENT_CONFLICT,
            REPOSITORY_REDRIVE_REQUIRES_FORK,
        },
        AdmissionState, ArtifactId, ArtifactRef, ArtifactStoreDeploymentCapability, ContentHash,
        DefinitionRevisionId, ExecutionEventContext, ExecutionEventPayload, ExecutionRevisionPin,
        MigrationNodeMapping, NodeId, PendingExecutionEvent, PublicEventEnvelope,
        PublicEventPayload, PublicFailureKind, RunId, RunLifecycle as EngineRunLifecycle,
        RunLineageKind, RuntimeValue, SchedulerCheckpointId, TerminationReason, TransitionKey,
        TransitionOutcome, WorkerArtifactStore, WorkerExecutorRegistry,
    },
    events::protocol::{RunEvent, RunEventScope, RunEventType},
    history::types::{summarize_input, RunAttachment, RunLifecycle, RunRecord, StopError},
    outcome::{FailureKind, RunFailure, RunOutput},
};

use super::{
    CompletedFunctionCallTailPublication, InMemoryLiveResponseBroker, LiveResponseBroker,
    LiveResponseBrokerCapability, LiveResponseByteLimits, LiveResponseItemIdentity,
    LiveResponseSubscriber,
};

const PRODUCTION_TRANSITION_DOMAIN: &str = "production.v3.run-service";
const DEFAULT_PUMP_INTERVAL: Duration = Duration::from_millis(25);
const MAX_RECOVERY_BATCH: u32 = 256;
const MAX_PUBLIC_EVENT_BATCH: u32 = 256;
const MAX_PUBLIC_EVENT_PRUNE_BATCH: u32 = 256;
const MAX_PUBLIC_EVENT_RETENTION: Duration = Duration::from_secs(10 * 365 * 24 * 60 * 60);
const MAX_RUNTIME_INGRESS_BATCH: u32 = 256;
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

fn has_public_streaming_source(agent: &PublishedV3Agent) -> bool {
    !agent.public_streaming_contract().sources().is_empty()
}

fn has_public_llm_streaming_source(agent: &PublishedV3Agent) -> bool {
    agent
        .public_streaming_contract()
        .sources()
        .iter()
        .any(|source| source.kind() == AgentStreamingSourceKind::Llm)
}

#[async_trait]
impl ProductionRunRepository for crate::engine::repository::SqliteDurableRepository {
    fn run_repository_capability(&self) -> RunRepositoryCapability {
        RunRepositoryCapability::SingleProcessOnly
    }

    async fn check_production_health(&self) -> Result<(), RepositoryError> {
        self.check_health().await
    }

    async fn load_production_run_input(
        &self,
        run_id: &RunId,
    ) -> Result<Option<Value>, RepositoryError> {
        let row = sqlx::query(
            "SELECT p.encoding,p.inline_value FROM workflow_runs r
             JOIN payloads p ON p.run_id=r.run_id AND p.payload_id=r.input_payload_id
             WHERE r.run_id=?",
        )
        .bind(run_id.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(RepositoryError::storage)?;
        row.map(|row| {
            if row
                .try_get::<String, _>("encoding")
                .map_err(|_| RepositoryError::invalid_data())?
                != "json_jcs"
            {
                return Err(RepositoryError::invalid_data());
            }
            serde_json::from_str(
                &row.try_get::<String, _>("inline_value")
                    .map_err(|_| RepositoryError::invalid_data())?,
            )
            .map_err(|_| RepositoryError::invalid_data())
        })
        .transpose()
    }

    async fn claim_production_scheduler_run(
        &self,
        _transition_key: TransitionKey,
        command: ClaimSchedulerRunCommand,
    ) -> Result<Option<FencedSchedulerRunCommand>, RepositoryError> {
        // SQLite is deliberately a single-process backend. Its writer mutex is
        // the authority; these persisted fields only satisfy the scheduler's
        // fenced CAS contract and make an ordinary single-process restart
        // immediately recoverable. They do not provide multi-process safety.
        let _writer = self.writer.lock().await;
        let token = format!("sqlite_local_{}", Uuid::new_v4().simple());
        let row = sqlx::query(
            "UPDATE workflow_runs
             SET scheduler_lease_epoch = scheduler_lease_epoch + 1,
                 scheduler_lease_owner = ?, scheduler_fencing_token = ?,
                 scheduler_lease_expires_at = datetime('now', '+' || ? || ' seconds'),
                 scheduler_heartbeat_at = CURRENT_TIMESTAMP
             WHERE run_id = ? AND lifecycle IN ('active','waiting','terminating')
               AND (admission_state = 'open' OR lifecycle = 'terminating')
             RETURNING scheduler_lease_epoch",
        )
        .bind(command.owner())
        .bind(&token)
        .bind(command.lease_seconds())
        .bind(command.run_id().as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(RepositoryError::storage)?;
        let Some(row) = row else {
            return Ok(None);
        };
        let epoch = u64::try_from(
            row.try_get::<i64, _>("scheduler_lease_epoch")
                .map_err(|_| RepositoryError::invalid_data())?,
        )
        .map_err(|_| RepositoryError::invalid_data())?;
        Ok(Some(FencedSchedulerRunCommand::new(
            command.run_id().clone(),
            command.owner(),
            epoch,
            token,
        )?))
    }

    async fn release_production_scheduler_run(
        &self,
        _transition_key: TransitionKey,
        fence: FencedSchedulerRunCommand,
    ) -> Result<(), RepositoryError> {
        let _writer = self.writer.lock().await;
        sqlx::query(
            "UPDATE workflow_runs
             SET scheduler_lease_owner = NULL, scheduler_fencing_token = NULL,
                 scheduler_lease_expires_at = NULL, scheduler_heartbeat_at = NULL
             WHERE run_id = ? AND scheduler_lease_owner = ?
               AND scheduler_lease_epoch = ? AND scheduler_fencing_token = ?",
        )
        .bind(fence.run_id().as_str())
        .bind(fence.owner())
        .bind(i64::try_from(fence.lease_epoch()).map_err(|_| RepositoryError::invalid_data())?)
        .bind(fence.fencing_token())
        .execute(&self.pool)
        .await
        .map_err(RepositoryError::storage)?;
        Ok(())
    }

    async fn load_pending_migration_waits(
        &self,
        run_id: &RunId,
    ) -> Result<Vec<PendingMigrationWait>, RepositoryError> {
        let rows = sqlx::query(
            "SELECT w.node_id AS wait_node_id,a.node_id AS activation_node_id,
                    w.signal_id,w.timer_id
             FROM scheduler_wait_registrations w
             JOIN node_activations a
               ON a.run_id=w.run_id AND a.activation_id=w.activation_id
             WHERE w.run_id=? AND w.winner_kind IS NULL
             ORDER BY w.node_id,w.wait_id",
        )
        .bind(run_id.as_str())
        .fetch_all(&self.pool)
        .await
        .map_err(RepositoryError::storage)?;
        rows.into_iter()
            .map(|row| {
                let wait_node = row
                    .try_get::<String, _>("wait_node_id")
                    .map_err(|_| RepositoryError::invalid_data())?;
                if wait_node
                    != row
                        .try_get::<String, _>("activation_node_id")
                        .map_err(|_| RepositoryError::invalid_data())?
                {
                    return Err(RepositoryError::invalid_data());
                }
                production_contract_adapter::pending_migration_wait(
                    NodeId::new(wait_node).map_err(|_| RepositoryError::invalid_data())?,
                    row.try_get::<Option<String>, _>("signal_id")
                        .map_err(|_| RepositoryError::invalid_data())?
                        .is_some(),
                    row.try_get::<Option<String>, _>("timer_id")
                        .map_err(|_| RepositoryError::invalid_data())?
                        .is_some(),
                )
            })
            .collect()
    }
}

#[async_trait]
impl ProductionRunRepository for crate::engine::repository::PostgresDurableRepository {
    fn run_repository_capability(&self) -> RunRepositoryCapability {
        RunRepositoryCapability::Production
    }

    async fn check_production_health(&self) -> Result<(), RepositoryError> {
        self.check_health().await
    }

    async fn load_production_run_input(
        &self,
        run_id: &RunId,
    ) -> Result<Option<Value>, RepositoryError> {
        let row = sqlx::query(
            "SELECT p.encoding,p.inline_value FROM workflow_runs r
             JOIN payloads p ON p.run_id=r.run_id AND p.payload_id=r.input_payload_id
             WHERE r.run_id=$1",
        )
        .bind(run_id.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(RepositoryError::storage)?;
        row.map(|row| {
            if row
                .try_get::<String, _>("encoding")
                .map_err(|_| RepositoryError::invalid_data())?
                != "json_jcs"
            {
                return Err(RepositoryError::invalid_data());
            }
            row.try_get::<Value, _>("inline_value")
                .map_err(|_| RepositoryError::invalid_data())
        })
        .transpose()
    }

    async fn claim_production_scheduler_run(
        &self,
        transition_key: TransitionKey,
        command: ClaimSchedulerRunCommand,
    ) -> Result<Option<FencedSchedulerRunCommand>, RepositoryError> {
        match SchedulerLeaseRepository::claim_scheduler_run(self, transition_key, command).await? {
            TransitionOutcome::Committed { result } => Ok(Some(result.fence()?)),
            TransitionOutcome::ExactReplay { authoritative } => Ok(Some(authoritative.fence()?)),
            TransitionOutcome::StateConflict | TransitionOutcome::StaleLease => Ok(None),
        }
    }

    async fn release_production_scheduler_run(
        &self,
        transition_key: TransitionKey,
        fence: FencedSchedulerRunCommand,
    ) -> Result<(), RepositoryError> {
        let _ =
            SchedulerLeaseRepository::release_scheduler_run(self, transition_key, fence).await?;
        Ok(())
    }

    async fn load_pending_migration_waits(
        &self,
        run_id: &RunId,
    ) -> Result<Vec<PendingMigrationWait>, RepositoryError> {
        let rows = sqlx::query(
            "SELECT w.node_id AS wait_node_id,a.node_id AS activation_node_id,
                    w.signal_id,w.timer_id
             FROM scheduler_wait_registrations w
             JOIN node_activations a
               ON a.run_id=w.run_id AND a.activation_id=w.activation_id
             WHERE w.run_id=$1 AND w.winner_kind IS NULL
             ORDER BY w.node_id,w.wait_id",
        )
        .bind(run_id.as_str())
        .fetch_all(&self.pool)
        .await
        .map_err(RepositoryError::storage)?;
        rows.into_iter()
            .map(|row| {
                let wait_node = row
                    .try_get::<String, _>("wait_node_id")
                    .map_err(|_| RepositoryError::invalid_data())?;
                if wait_node
                    != row
                        .try_get::<String, _>("activation_node_id")
                        .map_err(|_| RepositoryError::invalid_data())?
                {
                    return Err(RepositoryError::invalid_data());
                }
                production_contract_adapter::pending_migration_wait(
                    NodeId::new(wait_node).map_err(|_| RepositoryError::invalid_data())?,
                    row.try_get::<Option<String>, _>("signal_id")
                        .map_err(|_| RepositoryError::invalid_data())?
                        .is_some(),
                    row.try_get::<Option<String>, _>("timer_id")
                        .map_err(|_| RepositoryError::invalid_data())?
                        .is_some(),
                )
            })
            .collect()
    }
}

#[derive(Default)]
struct DeployedAgentCatalogState {
    current_by_agent_id: BTreeMap<String, Arc<DeployedV3Agent>>,
    deployment_archive: BTreeMap<String, Arc<DeployedV3Agent>>,
}

#[derive(Clone, Default)]
pub struct DeployedAgentCatalog {
    state: Arc<RwLock<DeployedAgentCatalogState>>,
}

impl DeployedAgentCatalog {
    /// Build a catalog whose entries are both the current public routes and
    /// the initial immutable deployment archive.
    pub fn new(current: Vec<Arc<DeployedV3Agent>>) -> Result<Self, ServiceError> {
        Self::from_current_and_archive(current, Vec::new())
    }

    /// Separates mutable public routing from immutable Run execution lookup.
    ///
    /// `current` must contain at most one deployment for each public agent ID.
    /// `archive` may contain any number of older deployments for that same ID.
    /// Every deployment revision across both collections remains unique and is
    /// addressable for Runs pinned before an alias/adapter/worker update.
    pub fn from_current_and_archive(
        current: Vec<Arc<DeployedV3Agent>>,
        archive: Vec<Arc<DeployedV3Agent>>,
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
                    "current v3 agent routes contain a duplicate agent ID",
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

    pub fn get(&self, agent_id: &str) -> Option<Arc<DeployedV3Agent>> {
        self.read().current_by_agent_id.get(agent_id).cloned()
    }

    pub fn get_deployment(&self, deployment_revision: &str) -> Option<Arc<DeployedV3Agent>> {
        self.read()
            .deployment_archive
            .get(deployment_revision)
            .cloned()
    }

    pub fn list(&self) -> impl Iterator<Item = Arc<DeployedV3Agent>> {
        self.read()
            .current_by_agent_id
            .values()
            .cloned()
            .collect::<Vec<_>>()
            .into_iter()
    }

    pub fn archived_deployments(&self) -> impl Iterator<Item = Arc<DeployedV3Agent>> {
        self.read()
            .deployment_archive
            .values()
            .cloned()
            .collect::<Vec<_>>()
            .into_iter()
    }

    fn install_current(&self, agent: Arc<DeployedV3Agent>) -> Result<(), ServiceError> {
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

    fn install_archive(&self, agent: Arc<DeployedV3Agent>) -> Result<(), ServiceError> {
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
    archive: &mut BTreeMap<String, Arc<DeployedV3Agent>>,
    deployment_revision: String,
    agent: Arc<DeployedV3Agent>,
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
) -> Result<Arc<DeployedV3Agent>, ServiceError> {
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
) -> Result<Arc<DeployedV3Agent>, ServiceError> {
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
        let published = PublishedV3Agent::from_stored_versioned_plan(&versioned).map_err(|_| {
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
        let deployed = Arc::new(
            DeployedV3Agent::restore_frozen(versioned, subflows).map_err(|_| {
                ServiceError::new(
                    "STORED_DEPLOYMENT_INVALID",
                    "durable deployment contracts failed authoritative verification",
                )
            })?,
        );
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
    published: &PublishedV3Agent,
    stored: &crate::engine::repository::VersionedPlan,
) -> Result<Vec<String>, ServiceError> {
    let bindings = stored.resolved_bindings().as_array().ok_or_else(|| {
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
    parent: &PublishedV3Agent,
    agents: &DeployedAgentCatalog,
    workers: &WorkerExecutorRegistry,
    stored: &VersionedPlanCatalog,
) -> Result<(Vec<Arc<DeployedV3Agent>>, Vec<PublicationHead>), ServiceError> {
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
pub struct RunServiceConfig {
    deployment_mode: RunServiceDeploymentMode,
    pub max_concurrent_runs: usize,
    pub max_concurrent_operations: usize,
    pub max_concurrent_operations_per_run: usize,
    pub subscriber_capacity: usize,
    pub scheduler_lease_seconds: u32,
    pub task_claim_seconds: u32,
    pub scheduler_action_budget: u32,
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

    fn validate(self) -> Result<Self, ServiceError> {
        if self.max_concurrent_runs == 0
            || self.max_concurrent_operations == 0
            || self.max_concurrent_operations > 1_000
            || self.max_concurrent_operations_per_run == 0
            || self.subscriber_capacity == 0
            || self.scheduler_lease_seconds == 0
            || self.task_claim_seconds == 0
            || self.scheduler_action_budget == 0
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
                "v3 Run service configuration is invalid",
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
    shutdown: CancellationToken,
    background: AsyncMutex<Vec<JoinHandle<()>>>,
    live: Mutex<BTreeMap<String, broadcast::Sender<()>>>,
    active_runs: Mutex<BTreeSet<String>>,
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
                shutdown: CancellationToken::new(),
                background: AsyncMutex::new(Vec::new()),
                live: Mutex::new(BTreeMap::new()),
                active_runs: Mutex::new(BTreeSet::new()),
            }),
        };
        service.reconcile_startup().await?;
        service.spawn_background().await;
        Ok(service)
    }

    pub fn agents(&self) -> &DeployedAgentCatalog {
        &self.inner.agents
    }

    /// Lists the effective public Agent routes from one durable publication
    /// snapshot. This is the discovery authority in multi-runtime deployments;
    /// the process-local catalog remains only an executable deployment cache.
    pub async fn list_current_agents(&self) -> Result<Vec<Arc<DeployedV3Agent>>, ServiceError> {
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
    ) -> Result<Arc<DeployedV3Agent>, ServiceError> {
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
    }

    pub async fn shutdown(&self, grace: Duration) -> Result<(), ServiceError> {
        self.begin_shutdown();
        self.inner.shutdown.cancel();
        let handles = std::mem::take(&mut *self.inner.background.lock().await);
        tokio::time::timeout(grace, async {
            for handle in handles {
                handle.await.map_err(|_| {
                    ServiceError::new("RUN_SERVICE_UNAVAILABLE", "v3 background pump panicked")
                })?;
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
        .map_err(|_| {
            ServiceError::new("RUN_SERVICE_UNAVAILABLE", "v3 Run service drain timed out")
        })?
    }

    pub async fn check_readiness(&self, timeout: Duration) -> Result<(), ServiceError> {
        if !self.inner.accepting.load(Ordering::Acquire) {
            return Err(ServiceError::new(
                "RUN_SERVICE_STOPPING",
                "v3 Run service is stopping",
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
                "v3 Run service is stopping",
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
                "v3 Run service is stopping",
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
            PublishedV3Agent::from_verified_graph(
                agent_id,
                agent_id,
                "Graph-authored agent",
                graph,
            )
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
            DeployedV3Agent::publish(Arc::clone(&published), resolver.as_ref(), subflows).map_err(
                |_| {
                    ServiceError::new(
                        "GRAPH_PUBLICATION_INVALID",
                        "graph publication cannot resolve its deployment bindings",
                    )
                },
            )?,
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
        let run_id = RunId::new(run_id.to_owned())
            .map_err(|_| ServiceError::new("RUN_NOT_FOUND", "Run not found"))?;
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
        let run_id = RunId::new(run_id.to_owned())
            .map_err(|_| ServiceError::new("RUN_NOT_FOUND", "Run not found"))?;
        let graph = self.execution_graph(run_id.as_str()).await?;
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
        })
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
                "v3 Run service is stopping",
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
    ) -> Result<(Arc<DeployedV3Agent>, PublicationHead), ServiceError> {
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

    async fn create_run_with_agent(
        &self,
        agent: Arc<DeployedV3Agent>,
        input: Value,
        request: RequestMetadata,
        attachment: PublicRunAttachment,
        subscribe: bool,
        expected_publication_head: Option<PublicationHead>,
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
        let run_id = RunId::new(format!("run_{}", Uuid::new_v4().simple()))
            .map_err(|_| ServiceError::new("RUN_CONFLICT", "failed to allocate Run identity"))?;
        match self.inner.reserve_active_run(&run_id, false) {
            ActiveRunReservation::NewlyReserved => {}
            ActiveRunReservation::Existing => {
                return Err(ServiceError::new(
                    "RUN_CONFLICT",
                    "new Run identity is already active",
                ));
            }
            ActiveRunReservation::CapacityFull => {
                return Err(ServiceError::new(
                    "RUN_CAPACITY_EXCEEDED",
                    "v3 Run capacity is exhausted",
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
                "v3 Run service is stopping",
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
    ) -> Result<Arc<DeployedV3Agent>, ServiceError> {
        self.inner.load_pinned_run_deployment(source).await
    }

    async fn load_pending_migration_target(
        &self,
        pending: &BeginMigrationCommand,
        requested_agent_id: &str,
    ) -> Result<Arc<DeployedV3Agent>, ServiceError> {
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
        let run_id = RunId::new(run_id.to_owned())
            .map_err(|_| ServiceError::new("RUN_NOT_FOUND", "Run not found"))?;
        let projection = self
            .inner
            .repository
            .load_run(&run_id)
            .await?
            .ok_or_else(|| ServiceError::new("RUN_NOT_FOUND", "Run not found"))?;
        self.inner.record(&projection).await
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
        let not_found = || ServiceError::new("ARTIFACT_NOT_FOUND", "Artifact not found");
        let run_id = RunId::new(run_id.to_owned()).map_err(|_| not_found())?;
        let artifact_id = ArtifactId::new(artifact_id.to_owned()).map_err(|_| not_found())?;
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
                "v3 Run service is stopping",
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
                "v3 Run service is stopping",
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
                "v3 Run service is stopping",
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
            let command = RunTransitionCommand::nonterminal(
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
        let run_id = RunId::new(run_id.to_owned())
            .map_err(|_| ServiceError::new("RUN_NOT_FOUND", "Run not found"))?;
        self.cancel_model(run_id).await
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
        tasks.push(spawn_recovery_pump(Arc::clone(&self.inner)));
        for worker_index in 0..self.inner.config.max_concurrent_operations {
            tasks.push(spawn_worker_pump(Arc::clone(&self.inner), worker_index));
        }
        tasks.push(spawn_runtime_ingress_pump(Arc::clone(&self.inner)));
        tasks.push(spawn_public_event_pump(Arc::clone(&self.inner)));
        tasks.push(spawn_public_event_prune_pump(Arc::clone(&self.inner)));
        tasks.push(spawn_public_event_notification_pump(Arc::clone(
            &self.inner,
        )));
        if self.inner.artifact_store.is_some() {
            tasks.push(spawn_artifact_gc_pump(Arc::clone(&self.inner)));
        }
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
    ) -> Result<Arc<DeployedV3Agent>, ServiceError> {
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
    ) -> Result<Arc<DeployedV3Agent>, ServiceError> {
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
            let command = RunTransitionCommand::termination_intent(
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
            return Err(ServiceError::new(
                "RUN_CAPACITY_EXCEEDED",
                "v3 Run capacity is exhausted",
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
            let command = RunTransitionCommand::nonterminal(
                run_id.clone(),
                projection.projection_version(),
                EngineRunLifecycle::Created,
                AdmissionState::Open,
                EngineRunLifecycle::Active,
                AdmissionState::Open,
                event,
                Some(crate::engine::repository::PublicEventIntent::new(
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
        let drive = drive_scheduler_until_quiescent(
            self.repository.as_ref(),
            &linked,
            &fence,
            &NoSchedulerCrash,
            self.config.scheduler_action_budget,
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

    async fn recovery_cycle(&self) -> Result<(), ServiceError> {
        let runs = self
            .repository
            .list_recoverable_scheduler_runs(MAX_RECOVERY_BATCH)
            .await?;
        for run_id in runs {
            self.resume_run(&run_id, false).await?;
        }
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

    async fn worker_cycle(&self, worker_index: usize) -> Result<(), ServiceError> {
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
        if model_tool_first {
            let outcome = consume_model_tool_task_once_with_observer(
                self.repository.as_ref(),
                self.workers.as_ref(),
                &claimant,
                self.config.task_claim_seconds,
                per_run,
                self.shutdown.child_token(),
                self.live_response_broker.as_ref(),
                &NoSchedulerCrash,
            )
            .await?;
            if !matches!(outcome, ModelToolWorkerPumpOutcome::NoTask) {
                self.recovery_cycle().await?;
                return Ok(());
            }
        }
        let outcome = match self.artifact_store.as_deref() {
            Some(store) => {
                consume_scheduler_task_once_with_artifact_store_and_retrieval_observer(
                    self.repository.as_ref(),
                    self.workers.as_ref(),
                    &FrozenSchedulerWorkerFailurePolicy,
                    &claimant,
                    self.config.task_claim_seconds,
                    per_run,
                    self.shutdown.child_token(),
                    store,
                    self.live_response_broker.as_ref(),
                    &NoSchedulerCrash,
                )
                .await?
            }
            None => {
                consume_scheduler_task_once_with_retrieval_observer(
                    self.repository.as_ref(),
                    self.workers.as_ref(),
                    &FrozenSchedulerWorkerFailurePolicy,
                    &claimant,
                    self.config.task_claim_seconds,
                    per_run,
                    self.shutdown.child_token(),
                    self.live_response_broker.as_ref(),
                    &NoSchedulerCrash,
                )
                .await?
            }
        };
        if let SchedulerWorkerPumpOutcome::SuspendedForTools { activation, .. } = &outcome {
            self.publish_completed_function_calls(activation);
        }
        if !matches!(outcome, SchedulerWorkerPumpOutcome::NoTask) {
            self.recovery_cycle().await?;
            return Ok(());
        }
        if !model_tool_first {
            let outcome = consume_model_tool_task_once_with_observer(
                self.repository.as_ref(),
                self.workers.as_ref(),
                &claimant,
                self.config.task_claim_seconds,
                per_run,
                self.shutdown.child_token(),
                self.live_response_broker.as_ref(),
                &NoSchedulerCrash,
            )
            .await?;
            if !matches!(outcome, ModelToolWorkerPumpOutcome::NoTask) {
                self.recovery_cycle().await?;
            }
        }
        Ok(())
    }

    async fn artifact_gc_cycle(&self) -> Result<(), ServiceError> {
        let Some(store) = self.artifact_store.as_deref() else {
            return Ok(());
        };
        let retention_seconds = u32::try_from(self.config.artifact_reference_retention.as_secs())
            .map_err(|_| {
            ServiceError::new(
                "RUN_SERVICE_CONFIG_INVALID",
                "artifact reference retention is invalid",
            )
        })?;
        for run_id in self
            .repository
            .list_unreleased_terminal_artifact_runs(MAX_RUNTIME_INGRESS_BATCH)
            .await?
        {
            let key = transition_key("artifact.retention.release", &[run_id.as_str()])?;
            let command = ReleaseRunArtifactRetentionCommand::new(run_id, retention_seconds)?;
            match self
                .repository
                .release_run_artifact_retention(key, command)
                .await?
            {
                TransitionOutcome::Committed { .. }
                | TransitionOutcome::ExactReplay { .. }
                | TransitionOutcome::StaleLease
                | TransitionOutcome::StateConflict => {}
            }
        }
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
            RunEventType::RunFailed | RunEventType::OperationFailed => (
                failure_code.unwrap_or_else(|| "RUN_FAILED".to_owned()),
                "run failed",
            ),
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
            EngineRunLifecycle::Failed => RunLifecycle::Failed {
                error: RunFailure {
                    kind: terminal_failure_kind(terminal_envelope.as_ref()),
                    code: terminal_failure_code(terminal_envelope.as_ref())
                        .unwrap_or_else(|| "RUN_FAILED".to_owned()),
                    message: "run failed".to_owned(),
                },
            },
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

fn public_failure(payload: &PublicEventPayload) -> Option<&crate::engine::PublicFailureSummary> {
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
    agent: &DeployedV3Agent,
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
    source: &DeployedV3Agent,
    target: &DeployedV3Agent,
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
            (
                crate::engine::plan::NodeKind::HumanTask(_),
                crate::engine::plan::NodeKind::HumanTask(_)
            ) | (
                crate::engine::plan::NodeKind::WaitSignal(_),
                crate::engine::plan::NodeKind::WaitSignal(_)
            )
        );
        let timer_pair = matches!(
            (source_node.kind(), target_node.kind()),
            (
                crate::engine::plan::NodeKind::Timer(_),
                crate::engine::plan::NodeKind::Timer(_)
            )
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
    linked: &crate::engine::plan::LinkedPlan<'_>,
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

fn spawn_recovery_pump(inner: Arc<RunServiceInner>) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = inner.shutdown.cancelled() => return,
                _ = tokio::time::sleep(inner.config.pump_interval) => {}
            }
            if let Err(error) = inner.recovery_cycle().await {
                tracing::warn!(code = error.code(), "v3 scheduler recovery cycle failed");
            }
        }
    })
}

fn spawn_worker_pump(inner: Arc<RunServiceInner>, worker_index: usize) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = inner.shutdown.cancelled() => return,
                _ = tokio::time::sleep(inner.config.pump_interval) => {}
            }
            // Keep the long-lived pump supervised even if repository or host
            // adapter code panics outside the executor boundary. A claimed
            // task remains protected by its durable lease and is reclaimed by
            // a later cycle; the runtime must not silently lose capacity.
            match std::panic::AssertUnwindSafe(inner.worker_cycle(worker_index))
                .catch_unwind()
                .await
            {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    tracing::warn!(code = error.code(), "v3 scheduler worker cycle failed");
                }
                Err(_) => {
                    tracing::error!(worker_index, "v3 scheduler worker cycle panicked");
                }
            }
        }
    })
}

fn spawn_runtime_ingress_pump(inner: Arc<RunServiceInner>) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = inner.shutdown.cancelled() => return,
                _ = tokio::time::sleep(inner.config.pump_interval) => {}
            }
            if let Err(error) = inner.runtime_ingress_cycle().await {
                tracing::warn!(code = error.code(), "v3 timer/signal ingress cycle failed");
            }
        }
    })
}

fn spawn_public_event_pump(inner: Arc<RunServiceInner>) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = inner.shutdown.cancelled() => return,
                _ = tokio::time::sleep(inner.config.pump_interval) => {}
            }
            if let Err(error) = inner.flush_public_events().await {
                tracing::warn!(code = error.code(), "v3 public event cycle failed");
            }
        }
    })
}

fn spawn_public_event_prune_pump(inner: Arc<RunServiceInner>) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = inner.shutdown.cancelled() => return,
                _ = tokio::time::sleep(inner.config.public_event_prune_interval) => {}
            }
            if let Err(error) = inner.public_event_prune_cycle().await {
                tracing::warn!(
                    code = error.code(),
                    "v3 public event retention prune failed"
                );
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
                        "v3 public event notification listener failed to connect"
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
                                "v3 public event notification delivery failed"
                            );
                        }
                    }
                    Err(error) => {
                        tracing::warn!(
                            code = error.code(),
                            "v3 public event notification listener disconnected"
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

fn spawn_artifact_gc_pump(inner: Arc<RunServiceInner>) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = inner.shutdown.cancelled() => return,
                _ = tokio::time::sleep(inner.config.artifact_gc_interval) => {}
            }
            if let Err(error) = inner.artifact_gc_cycle().await {
                tracing::warn!(code = error.code(), "v3 artifact GC cycle failed");
            }
        }
    })
}

#[cfg(test)]
mod public_artifact_authorization_tests {
    use serde_json::json;

    use insight_engine::response::adapter::durable_response_snapshot_new;

    use super::*;
    use crate::engine::repository::{ResponseTerminalKind, ResponseUsageStatus};

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

#[cfg(test)]
mod public_event_multiruntime_tests {
    use std::time::Instant;

    use serde_json::json;
    use sqlx::{postgres::PgPoolOptions, AssertSqlSafe, PgPool};

    use super::*;
    use crate::engine::{
        repository::{
            DurableRepository, PostgresDurableRepository, PublicEventIntent,
            PublicEventOutboxRepository, SqliteDurableRepository, VersionedPlan,
        },
        ContentHash, DefinitionRevisionId, DeploymentRevisionId,
        LocalContentAddressedArtifactStore,
    };

    fn plan(suffix: &str) -> VersionedPlan {
        insight_durable::model::adapter::versioned_plan_for_test(
            format!("definition_public_listener_{suffix}"),
            format!("agent_public_listener_{suffix}"),
            "Public listener test",
            DefinitionRevisionId::new(format!("definition_revision_{suffix}")).unwrap(),
            DeploymentRevisionId::new(format!("deployment_revision_{suffix}")).unwrap(),
            ContentHash::from_bytes(format!("public-listener-plan-{suffix}").as_bytes()),
            ContentHash::from_bytes(format!("public-listener-binding-{suffix}").as_bytes()),
            "compiler-3.0.0",
            "expression-3.0.0",
            json!({"kind": "structured"}),
            json!({"nodes": []}),
            json!({}),
            json!({"model": "fixed"}),
            json!({"worker": "v1"}),
        )
        .unwrap()
    }

    fn config() -> RunServiceConfig {
        let mut config = RunServiceConfig::production(8, 1, 1, 32);
        config.pump_interval = Duration::from_secs(3_600);
        config.run_timeout = Duration::from_secs(3_600);
        config
    }

    fn single_process_development_config() -> RunServiceConfig {
        let mut config = RunServiceConfig::single_process_development(8, 1, 1, 32);
        config.pump_interval = Duration::from_secs(3_600);
        config.run_timeout = Duration::from_secs(3_600);
        config
    }

    fn key(suffix: &str, label: &str) -> TransitionKey {
        TransitionKey::derive("runtime.public-event.pg.test", &[suffix, label]).unwrap()
    }

    fn lifecycle_event(run_id: &RunId, lifecycle: EngineRunLifecycle) -> PendingExecutionEvent {
        PendingExecutionEvent::new(
            ExecutionEventContext::for_run(run_id.clone()),
            ExecutionEventPayload::RunLifecycleChanged { lifecycle },
        )
        .unwrap()
    }

    async fn wait_for_listener_pid(
        admin: &PgPool,
        application_name: &str,
        previous_pid: Option<i32>,
    ) -> i32 {
        let deadline = Instant::now() + Duration::from_secs(8);
        loop {
            let pid = sqlx::query_scalar::<_, i32>(
                "SELECT pid FROM pg_stat_activity
                 WHERE datname = current_database() AND application_name = $1
                   AND query ILIKE '%insight_agent_public_events_v3%'
                 ORDER BY backend_start DESC LIMIT 1",
            )
            .bind(application_name)
            .fetch_optional(admin)
            .await
            .unwrap();
            if let Some(pid) = pid.filter(|pid| Some(*pid) != previous_pid) {
                return pid;
            }
            assert!(
                Instant::now() < deadline,
                "PostgreSQL listener did not connect"
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    #[tokio::test]
    async fn sqlite_live_event_uses_canonical_envelope_occurred_at() {
        let repository = Arc::new(SqliteDurableRepository::in_memory().await.unwrap());
        let service = RunService::start(
            DeployedAgentCatalog::new(Vec::new()).unwrap(),
            repository.clone() as Arc<dyn ProductionRunRepository>,
            WorkerExecutorRegistry::new(),
            single_process_development_config(),
        )
        .await
        .unwrap();

        let suffix = Uuid::new_v4().simple().to_string();
        let plan = plan(&suffix);
        repository.install_versioned_plan(&plan).await.unwrap();
        let run_id = RunId::new(format!("run_public_time_{suffix}")).unwrap();
        repository
            .create_run(
                key(&suffix, "time.create"),
                CreateRunCommand::new(run_id.clone(), &plan, json!({"question": "time"}))
                    .unwrap()
                    .with_public_metadata("request-public-time", PublicRunAttachment::Attached)
                    .unwrap(),
            )
            .await
            .unwrap();
        let committed = repository
            .commit_run_transition(
                key(&suffix, "time.start"),
                RunTransitionCommand::nonterminal(
                    run_id.clone(),
                    0,
                    EngineRunLifecycle::Created,
                    AdmissionState::Open,
                    EngineRunLifecycle::Active,
                    AdmissionState::Open,
                    lifecycle_event(&run_id, EngineRunLifecycle::Active),
                    Some(PublicEventIntent::new(PublicEventPayload::RunStarted)),
                )
                .unwrap(),
            )
            .await
            .unwrap();
        let public_event_id = committed
            .committed_result()
            .unwrap()
            .public_event_id()
            .unwrap()
            .to_owned();
        let created_claim = repository
            .claim_public_events("sqlite-time-dispatcher", 30, 1)
            .await
            .unwrap()
            .pop()
            .unwrap();
        assert_eq!(
            created_claim.safe_envelope().payload(),
            &PublicEventPayload::RunCreated
        );
        assert!(repository
            .publish_public_event(&created_claim, 60)
            .await
            .unwrap());
        let claim = repository
            .claim_public_events("sqlite-time-dispatcher", 30, 1)
            .await
            .unwrap()
            .pop()
            .unwrap();
        assert!(repository.publish_public_event(&claim, 60).await.unwrap());
        let published = repository
            .load_published_public_event(&public_event_id)
            .await
            .unwrap()
            .unwrap();

        sqlx::query(
            "UPDATE workflow_runs SET updated_at='2099-01-01T00:00:00.000000Z' WHERE run_id=?",
        )
        .bind(run_id.as_str())
        .execute(&repository.pool)
        .await
        .unwrap();
        let projection = repository.load_run(&run_id).await.unwrap().unwrap();
        let live = service
            .inner
            .run_event(&run_id, published.safe_envelope())
            .await
            .unwrap();
        assert_eq!(
            live.timestamp,
            published.safe_envelope().occurred_at().to_owned()
        );
        assert_ne!(live.timestamp, projection.updated_at());
        service.shutdown(Duration::from_secs(1)).await.unwrap();
    }

    #[tokio::test]
    async fn sqlite_subscription_uses_durable_order_when_terminal_hint_arrives_first() {
        let repository = Arc::new(SqliteDurableRepository::in_memory().await.unwrap());
        let service = RunService::start(
            DeployedAgentCatalog::new(Vec::new()).unwrap(),
            repository.clone() as Arc<dyn ProductionRunRepository>,
            WorkerExecutorRegistry::new(),
            single_process_development_config(),
        )
        .await
        .unwrap();

        let suffix = Uuid::new_v4().simple().to_string();
        let plan = plan(&suffix);
        repository.install_versioned_plan(&plan).await.unwrap();
        let run_id = RunId::new(format!("run_public_reverse_{suffix}")).unwrap();
        repository
            .create_run(
                key(&suffix, "reverse.create"),
                CreateRunCommand::new(run_id.clone(), &plan, json!({"question": "reverse"}))
                    .unwrap()
                    .with_public_metadata("request-public-reverse", PublicRunAttachment::Attached)
                    .unwrap(),
            )
            .await
            .unwrap();
        repository
            .commit_run_transition(
                key(&suffix, "reverse.start"),
                RunTransitionCommand::nonterminal(
                    run_id.clone(),
                    0,
                    EngineRunLifecycle::Created,
                    AdmissionState::Open,
                    EngineRunLifecycle::Active,
                    AdmissionState::Open,
                    lifecycle_event(&run_id, EngineRunLifecycle::Active),
                    Some(PublicEventIntent::new(PublicEventPayload::RunStarted)),
                )
                .unwrap(),
            )
            .await
            .unwrap();
        repository
            .commit_run_transition(
                key(&suffix, "reverse.completing"),
                RunTransitionCommand::nonterminal(
                    run_id.clone(),
                    1,
                    EngineRunLifecycle::Active,
                    AdmissionState::Open,
                    EngineRunLifecycle::Completing,
                    AdmissionState::Draining,
                    lifecycle_event(&run_id, EngineRunLifecycle::Completing),
                    None,
                )
                .unwrap(),
            )
            .await
            .unwrap();
        let terminal = repository
            .commit_run_transition(
                key(&suffix, "reverse.terminal"),
                RunTransitionCommand::terminal_success(
                    run_id.clone(),
                    2,
                    json!({"answer": "done"}),
                    lifecycle_event(&run_id, EngineRunLifecycle::Succeeded),
                    PublicEventIntent::new(PublicEventPayload::RunCompleted),
                )
                .unwrap(),
            )
            .await
            .unwrap();
        let terminal_id = terminal
            .committed_result()
            .unwrap()
            .public_event_id()
            .unwrap()
            .to_owned();

        for expected in [
            PublicEventPayload::RunCreated,
            PublicEventPayload::RunStarted,
            PublicEventPayload::RunCompleted,
        ] {
            let claim = repository
                .claim_public_events("sqlite-reverse-dispatcher", 30, 10)
                .await
                .unwrap()
                .pop()
                .unwrap();
            assert_eq!(claim.safe_envelope().payload(), &expected);
            assert!(repository.publish_public_event(&claim, 60).await.unwrap());
        }

        let receiver = service.inner.open_subscription(&run_id);
        let mut subscription = RunSubscription {
            run_id: run_id.as_str().to_owned(),
            run_id_model: run_id.clone(),
            receiver,
            owner: Arc::downgrade(&service.inner),
            position: None,
            last_seq: 0,
            terminal: false,
        };

        // Simulate the worst dispatcher race: the terminal hint wins and all
        // earlier hints are lost. The subscriber must still drain the durable
        // order, and the wake sender must remain open until that drain reaches
        // the terminal position.
        service
            .inner
            .deliver_published_public_event(&terminal_id)
            .await
            .unwrap();
        assert!(service
            .inner
            .live
            .lock()
            .unwrap()
            .contains_key(run_id.as_str()));
        let created = subscription.recv().await.unwrap();
        let started = subscription.recv().await.unwrap();
        assert!(service
            .inner
            .live
            .lock()
            .unwrap()
            .contains_key(run_id.as_str()));
        let completed = subscription.recv().await.unwrap();
        assert_eq!(created.event_type, RunEventType::RunCreated);
        assert_eq!(started.event_type, RunEventType::RunStarted);
        assert_eq!(completed.event_type, RunEventType::RunCompleted);
        assert!(created.seq < started.seq && started.seq < completed.seq);
        assert_eq!(subscription.last_seq(), completed.seq);
        assert!(!service
            .inner
            .live
            .lock()
            .unwrap()
            .contains_key(run_id.as_str()));
        assert_eq!(
            subscription.recv().await.unwrap_err().code(),
            "SUBSCRIPTION_TERMINAL"
        );

        service.shutdown(Duration::from_secs(1)).await.unwrap();
    }

    #[tokio::test]
    async fn postgres_listener_reconnects_and_reversed_remote_hints_remain_ordered() {
        let Some(database_url) = std::env::var("V3_TEST_POSTGRES_URL").ok() else {
            return;
        };
        let suffix = Uuid::new_v4().simple().to_string();
        let schema = format!("public_listener_{}", &suffix[..16]);
        let app_a = format!("pub_a_{}", &suffix[..12]);
        let app_b = format!("pub_b_{}", &suffix[..12]);
        let admin = PgPoolOptions::new()
            .max_connections(2)
            .connect(&database_url)
            .await
            .unwrap();
        let version: String = sqlx::query_scalar("SHOW server_version_num")
            .fetch_one(&admin)
            .await
            .unwrap();
        let version = version.parse::<u32>().unwrap();
        assert!(
            (160_000..170_000).contains(&version),
            "public event multi-runtime gate requires PostgreSQL 16"
        );
        sqlx::query(AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
            .execute(&admin)
            .await
            .unwrap();
        let separator = if database_url.contains('?') { '&' } else { '?' };
        let scoped = format!("{database_url}{separator}options=-csearch_path%3D{schema}");
        let repository_a = Arc::new(
            PostgresDurableRepository::connect(&format!("{scoped}&application_name={app_a}"))
                .await
                .unwrap(),
        );
        repository_a.initialize_schema().await.unwrap();
        let repository_b = Arc::new(
            PostgresDurableRepository::connect(&format!("{scoped}&application_name={app_b}"))
                .await
                .unwrap(),
        );

        let artifact_directory = tempfile::tempdir().unwrap();
        let artifact_store = Arc::new(
            LocalContentAddressedArtifactStore::open_shared(
                artifact_directory.path().join("objects"),
                1,
                "public_listener",
            )
            .await
            .unwrap(),
        );
        let service_a = RunService::start_with_artifact_store(
            DeployedAgentCatalog::new(Vec::new()).unwrap(),
            repository_a.clone() as Arc<dyn ProductionRunRepository>,
            WorkerExecutorRegistry::new(),
            artifact_store.clone(),
            config(),
        )
        .await
        .unwrap();
        let service_b = RunService::start_with_artifact_store(
            DeployedAgentCatalog::new(Vec::new()).unwrap(),
            repository_b.clone() as Arc<dyn ProductionRunRepository>,
            WorkerExecutorRegistry::new(),
            artifact_store,
            config(),
        )
        .await
        .unwrap();
        let listener_pid = wait_for_listener_pid(&admin, &app_b, None).await;

        let plan = plan(&suffix);
        repository_a.install_versioned_plan(&plan).await.unwrap();
        let run_id = RunId::new(format!("run_public_listener_{suffix}")).unwrap();
        repository_a
            .create_run(
                key(&suffix, "create"),
                CreateRunCommand::new(run_id.clone(), &plan, json!({"question": "listen"}))
                    .unwrap()
                    .with_public_metadata("request-public-listener", PublicRunAttachment::Attached)
                    .unwrap(),
            )
            .await
            .unwrap();
        let receiver = service_b.inner.open_subscription(&run_id);
        let mut subscription = RunSubscription {
            run_id: run_id.as_str().to_owned(),
            run_id_model: run_id.clone(),
            receiver,
            owner: Arc::downgrade(&service_b.inner),
            position: None,
            last_seq: 0,
            terminal: false,
        };

        let start = repository_a
            .commit_run_transition(
                key(&suffix, "start"),
                RunTransitionCommand::nonterminal(
                    run_id.clone(),
                    0,
                    EngineRunLifecycle::Created,
                    AdmissionState::Open,
                    EngineRunLifecycle::Active,
                    AdmissionState::Open,
                    lifecycle_event(&run_id, EngineRunLifecycle::Active),
                    Some(PublicEventIntent::new(PublicEventPayload::RunStarted)),
                )
                .unwrap(),
            )
            .await
            .unwrap();
        let start_id = match start {
            TransitionOutcome::Committed { result } => result.public_event_id().unwrap().to_owned(),
            TransitionOutcome::ExactReplay { authoritative } => {
                authoritative.public_event_id().unwrap().to_owned()
            }
            TransitionOutcome::StateConflict | TransitionOutcome::StaleLease => {
                panic!("start transition must commit")
            }
        };
        service_a.inner.flush_public_events().await.unwrap();
        service_a.inner.flush_public_events().await.unwrap();

        assert!(
            sqlx::query_scalar::<_, bool>("SELECT pg_terminate_backend($1)")
                .bind(listener_pid)
                .fetch_one(&admin)
                .await
                .unwrap()
        );
        let _reconnected_pid = wait_for_listener_pid(&admin, &app_b, Some(listener_pid)).await;

        repository_a
            .commit_run_transition(
                key(&suffix, "completing"),
                RunTransitionCommand::nonterminal(
                    run_id.clone(),
                    1,
                    EngineRunLifecycle::Active,
                    AdmissionState::Open,
                    EngineRunLifecycle::Completing,
                    AdmissionState::Draining,
                    lifecycle_event(&run_id, EngineRunLifecycle::Completing),
                    None,
                )
                .unwrap(),
            )
            .await
            .unwrap();
        let terminal = repository_a
            .commit_run_transition(
                key(&suffix, "terminal"),
                RunTransitionCommand::terminal_success(
                    run_id.clone(),
                    2,
                    json!({"answer": "done"}),
                    lifecycle_event(&run_id, EngineRunLifecycle::Succeeded),
                    PublicEventIntent::new(PublicEventPayload::RunCompleted),
                )
                .unwrap(),
            )
            .await
            .unwrap();
        let terminal_id = match terminal {
            TransitionOutcome::Committed { result } => result.public_event_id().unwrap().to_owned(),
            TransitionOutcome::ExactReplay { authoritative } => {
                authoritative.public_event_id().unwrap().to_owned()
            }
            TransitionOutcome::StateConflict | TransitionOutcome::StaleLease => {
                panic!("terminal transition must commit")
            }
        };
        service_a.inner.flush_public_events().await.unwrap();

        // Force the remote runtime's terminal handler to win locally, then
        // emit PostgreSQL hints in terminal-before-start order. Publication
        // notifications may also be duplicated or already buffered; none of
        // those process-local arrival orders may control subscriber order.
        service_b
            .inner
            .deliver_published_public_event(&terminal_id)
            .await
            .unwrap();
        sqlx::query("SELECT pg_notify($1, $2)")
            .bind(crate::engine::repository::PUBLIC_EVENT_NOTIFY_CHANNEL)
            .bind(&terminal_id)
            .execute(&admin)
            .await
            .unwrap();
        sqlx::query("SELECT pg_notify($1, $2)")
            .bind(crate::engine::repository::PUBLIC_EVENT_NOTIFY_CHANNEL)
            .bind(&start_id)
            .execute(&admin)
            .await
            .unwrap();

        let created = tokio::time::timeout(Duration::from_secs(5), subscription.recv())
            .await
            .unwrap()
            .unwrap();
        let started = tokio::time::timeout(Duration::from_secs(5), subscription.recv())
            .await
            .unwrap()
            .unwrap();
        let completed = tokio::time::timeout(Duration::from_secs(5), subscription.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(created.event_type, RunEventType::RunCreated);
        assert_eq!(started.event_type, RunEventType::RunStarted);
        assert_eq!(completed.event_type, RunEventType::RunCompleted);
        assert!(created.seq < started.seq && started.seq < completed.seq);
        assert_eq!(
            subscription.recv().await.unwrap_err().code(),
            "SUBSCRIPTION_TERMINAL"
        );

        service_a.shutdown(Duration::from_secs(2)).await.unwrap();
        service_b.shutdown(Duration::from_secs(2)).await.unwrap();
        drop(service_a);
        drop(service_b);
        drop(repository_a);
        drop(repository_b);
        sqlx::query(AssertSqlSafe(format!("DROP SCHEMA {schema} CASCADE")))
            .execute(&admin)
            .await
            .unwrap();
    }
}
