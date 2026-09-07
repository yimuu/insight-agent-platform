//! Durable Run and controller boundary projections.
use crate::*;
use chrono::{DateTime, Utc};
use insight_platform_contracts::{RunBindingsSnapshot, TraceIdentityV1};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Safe exact definition references derived from one Run and its immutable deployment.
/// Reading the referenced authoring content requires a separate current permission check.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunDefinitionRecord {
    pub run_id: ResourceId,
    pub agent_id: ResourceId,
    pub agent_deployment: ExactDeploymentRef,
    pub agent_interface: ExactVersionRef,
    pub plan: ExactVersionRef,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunRecord {
    pub tenant_id: String,
    pub run_id: String,
    pub root_run_id: String,
    pub parent_run_id: Option<String>,
    pub parent_node_id: Option<String>,
    pub agent_deployment_id: String,
    pub principal_id: String,
    pub trace: TraceIdentityV1,
    pub state: String,
    pub version: i64,
    pub bindings: RunBindingsSnapshot,
    pub execution_requirement: insight_platform_contracts::ExecutionRequirement,
    pub current: RunCurrentSnapshot,
    pub input_value_id: Option<String>,
    pub output_value_id: Option<String>,
    pub depth: i32,
    pub descendant_count: i32,
    pub active_work_count: i32,
    pub pause_generation: i64,
    pub cancel_generation: i64,
    pub timeout_generation: i64,
    pub public_sequence: i64,
    pub public_replay_floor: i64,
    pub retry_at: Option<DateTime<Utc>>,
    pub deadline: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub terminal_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

use insight_platform_contracts::*;
use insight_platform_invocations::{
    CapabilityInvocationRecord, InvocationPolicyDecisionBundle, McpCapabilityRuntimeRequest,
};
use insight_platform_jobs::store::{
    validate_safety_scan_request, JobCommandFence as JobFence, JobRecord, SafetyScanCursor,
    SafetyScanShard, MAX_JOB_LEASE_MILLISECONDS,
};
use insight_platform_jobs::WakeSource;
use insight_platform_models::ModelRequestValue;
use insight_platform_plan::HumanTaskDefinition as RuntimeHumanTaskDefinition;
use insight_platform_scheduler::SchedulerHardLimits;
use insight_platform_tasks::store::TaskRecord;
use insight_platform_tasks::{TaskDefinition, TaskKind, TaskState};
use std::collections::BTreeSet;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ControllerStoreError {
    InvalidInput(String),
}
impl std::fmt::Display for ControllerStoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidInput(message) => write!(f, "invalid controller command: {message}"),
        }
    }
}
impl std::error::Error for ControllerStoreError {}
impl From<insight_platform_jobs::JobError> for ControllerStoreError {
    fn from(error: insight_platform_jobs::JobError) -> Self {
        Self::InvalidInput(error.to_string())
    }
}
impl From<insight_platform_sandbox::contracts::SandboxContractError> for ControllerStoreError {
    fn from(error: insight_platform_sandbox::contracts::SandboxContractError) -> Self {
        Self::InvalidInput(error.to_string())
    }
}
pub const MAX_ORCHESTRATION_QUOTA_LINES: usize = 4;
pub const MAX_CHILD_RUN_DEPTH: u16 = 32;
pub const MAX_DESCENDANT_RUNS: u32 = 500;
pub const MAX_CHILD_INPUT_SOURCES: usize = 512;
#[derive(Debug, Clone)]
pub struct OrchestrationClaimSlot {
    pub lease_token_digest: Sha256Digest,
    pub quota_reservation_id: ResourceId,
    pub quota_entry_ids: Vec<ResourceId>,
    pub run_event_id: ResourceId,
    pub run_outbox_id: ResourceId,
    pub node_event_id: ResourceId,
    pub node_outbox_id: ResourceId,
    pub job_event_id: ResourceId,
    pub job_outbox_id: ResourceId,
}
impl OrchestrationClaimSlot {
    pub fn validate(&self) -> Result<(), ControllerStoreError> {
        if self.quota_reservation_id.kind() != ResourceKind::UsageReservation
            || self.quota_entry_ids.len() != MAX_ORCHESTRATION_QUOTA_LINES
            || self
                .quota_entry_ids
                .iter()
                .any(|id| id.kind() != ResourceKind::QuotaLedgerEntry)
            || [&self.run_event_id, &self.node_event_id, &self.job_event_id]
                .into_iter()
                .any(|id| id.kind() != ResourceKind::Event)
            || [
                &self.run_outbox_id,
                &self.node_outbox_id,
                &self.job_outbox_id,
            ]
            .into_iter()
            .any(|id| id.kind() != ResourceKind::OutboxEvent)
        {
            return Err(ControllerStoreError::InvalidInput(
                "orchestration claim slot identity is invalid".to_owned(),
            ));
        }
        let mut identities = BTreeSet::new();
        for identity in self.quota_entry_ids.iter().chain([
            &self.run_event_id,
            &self.run_outbox_id,
            &self.node_event_id,
            &self.node_outbox_id,
            &self.job_event_id,
            &self.job_outbox_id,
        ]) {
            if !identities.insert(identity.to_string()) {
                return Err(ControllerStoreError::InvalidInput(
                    "orchestration claim slot contains a duplicate identity".to_owned(),
                ));
            }
        }
        Ok(())
    }
}
#[derive(Debug, Clone)]
pub struct ClaimOrchestrationJobs {
    pub worker_manifest: insight_platform_contracts::WorkerManifest,
    pub worker_id: ResourceId,
    pub limit: u16,
    pub lease_milliseconds: i64,
    pub slots: Vec<OrchestrationClaimSlot>,
}
impl ClaimOrchestrationJobs {
    pub fn validate(&self, limits: SchedulerHardLimits) -> Result<(), ControllerStoreError> {
        self.worker_manifest
            .validate()
            .map_err(|error| ControllerStoreError::InvalidInput(error.to_string()))?;
        if self.worker_manifest.work_class != WorkClass::Orchestration {
            return Err(ControllerStoreError::InvalidInput(
                "orchestration worker manifest has a different work class".into(),
            ));
        }
        if self.worker_id.kind() != ResourceKind::WorkerProcessGeneration
            || self.limit == 0
            || self.limit > limits.maximum_batch
            || self.lease_milliseconds <= 0
            || self.lease_milliseconds > MAX_JOB_LEASE_MILLISECONDS
            || self.slots.len() != usize::from(self.limit)
        {
            return Err(ControllerStoreError::InvalidInput(
                "orchestration claim is outside the platform bound".to_owned(),
            ));
        }
        let mut tokens = BTreeSet::new();
        let mut reservations = BTreeSet::new();
        let mut mutation_ids = BTreeSet::new();
        for slot in &self.slots {
            slot.validate()?;
            if !tokens.insert(slot.lease_token_digest.to_string())
                || !reservations.insert(slot.quota_reservation_id.to_string())
            {
                return Err(ControllerStoreError::InvalidInput(
                    "claim lease tokens and quota reservations must be unique".to_owned(),
                ));
            }
            for identity in slot.quota_entry_ids.iter().chain([
                &slot.run_event_id,
                &slot.run_outbox_id,
                &slot.node_event_id,
                &slot.node_outbox_id,
                &slot.job_event_id,
                &slot.job_outbox_id,
            ]) {
                if !mutation_ids.insert(identity.to_string()) {
                    return Err(ControllerStoreError::InvalidInput(
                        "claim mutation identities must be globally unique".to_owned(),
                    ));
                }
            }
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct ClaimedOrchestrationJob {
    pub job: JobRecord,
    pub run_version: i64,
    pub node_version: i64,
    pub quota_reservation_id: String,
    pub quota_account_ids: Vec<String>,
}
#[derive(Debug, Clone)]
pub struct StartOrchestrationJob {
    pub fence: JobFence,
    pub receipt_id: ResourceId,
    pub idempotency_key_digest: Sha256Digest,
    pub request_digest: Sha256Digest,
    pub receipt_expires_at: DateTime<Utc>,
    pub job_event_id: ResourceId,
    pub job_outbox_id: ResourceId,
    pub node_event_id: ResourceId,
    pub node_outbox_id: ResourceId,
}
impl StartOrchestrationJob {
    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), ControllerStoreError> {
        self.fence.validate()?;
        if self.receipt_id.kind() != ResourceKind::Receipt
            || self.job_event_id.kind() != ResourceKind::Event
            || self.job_outbox_id.kind() != ResourceKind::OutboxEvent
            || self.node_event_id.kind() != ResourceKind::Event
            || self.node_outbox_id.kind() != ResourceKind::OutboxEvent
            || self.job_event_id == self.node_event_id
            || self.job_outbox_id == self.node_outbox_id
            || self.receipt_expires_at <= now
        {
            return Err(ControllerStoreError::InvalidInput(
                "orchestration start audit is invalid".to_owned(),
            ));
        }
        Ok(())
    }
}
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OrchestrationYield {
    TimerWait { plan: RuntimePlan },
    SignalWait { plan: RuntimePlan },
    Retry { retry_at: DateTime<Utc> },
}
impl OrchestrationYield {
    pub const fn state(&self) -> JobState {
        match self {
            Self::TimerWait { .. } | Self::SignalWait { .. } => JobState::Waiting,
            Self::Retry { .. } => JobState::RetryScheduled,
        }
    }
}
#[derive(Debug, Clone)]
pub struct OrchestrationYieldMutationIds {
    pub receipt_id: ResourceId,
    pub quota_entry_ids: Vec<ResourceId>,
    pub run_event_id: ResourceId,
    pub run_outbox_id: ResourceId,
    pub node_event_id: ResourceId,
    pub node_outbox_id: ResourceId,
    pub job_event_id: ResourceId,
    pub job_outbox_id: ResourceId,
}
impl OrchestrationYieldMutationIds {
    pub fn validate(&self) -> Result<(), ControllerStoreError> {
        if self.receipt_id.kind() != ResourceKind::Receipt
            || self.quota_entry_ids.len() != MAX_ORCHESTRATION_QUOTA_LINES
            || self
                .quota_entry_ids
                .iter()
                .any(|identity| identity.kind() != ResourceKind::QuotaLedgerEntry)
            || [&self.run_event_id, &self.node_event_id, &self.job_event_id]
                .into_iter()
                .any(|identity| identity.kind() != ResourceKind::Event)
            || [
                &self.run_outbox_id,
                &self.node_outbox_id,
                &self.job_outbox_id,
            ]
            .into_iter()
            .any(|identity| identity.kind() != ResourceKind::OutboxEvent)
        {
            return Err(ControllerStoreError::InvalidInput(
                "orchestration yield mutation identity is invalid".to_owned(),
            ));
        }
        let mut unique = BTreeSet::new();
        for identity in self.quota_entry_ids.iter().chain([
            &self.receipt_id,
            &self.run_event_id,
            &self.run_outbox_id,
            &self.node_event_id,
            &self.node_outbox_id,
            &self.job_event_id,
            &self.job_outbox_id,
        ]) {
            if !unique.insert(identity.to_string()) {
                return Err(ControllerStoreError::InvalidInput(
                    "orchestration yield mutation identities must be unique".to_owned(),
                ));
            }
        }
        Ok(())
    }
}
#[derive(Debug, Clone)]
pub struct YieldOrchestrationJob {
    pub fence: JobFence,
    pub outcome: OrchestrationYield,
    pub idempotency_key_digest: Sha256Digest,
    pub request_digest: Sha256Digest,
    pub receipt_expires_at: DateTime<Utc>,
    pub mutations: OrchestrationYieldMutationIds,
}
impl YieldOrchestrationJob {
    pub fn validate_at(
        &self,
        now: DateTime<Utc>,
        plan_limits: PlanLimits,
    ) -> Result<(), ControllerStoreError> {
        self.fence.validate()?;
        self.mutations.validate()?;
        if let OrchestrationYield::TimerWait { plan } | OrchestrationYield::SignalWait { plan } =
            &self.outcome
        {
            plan.validate(plan_limits)
                .map_err(|failure| ControllerStoreError::InvalidInput(failure.to_string()))?;
        }
        if self.receipt_expires_at <= now {
            return Err(ControllerStoreError::InvalidInput(
                "orchestration yield command is invalid".to_owned(),
            ));
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct YieldedOrchestrationJob {
    pub run: RunRecord,
    pub job: JobRecord,
    pub node_id: String,
    pub node_version: i64,
    pub settled_quota_account_ids: Vec<String>,
}
#[derive(Debug, Clone, Serialize)]
pub struct ControllerActivationSlot {
    pub node_execution_id: ResourceId,
    pub orchestration_job_id: ResourceId,
    pub scope: Option<ControllerScopeSlot>,
    pub node_event_id: ResourceId,
    pub node_outbox_id: ResourceId,
    pub job_event_id: ResourceId,
    pub job_outbox_id: ResourceId,
}
impl ControllerActivationSlot {
    pub fn validate(&self) -> Result<(), ControllerStoreError> {
        if self.node_execution_id.kind() != ResourceKind::NodeExecution
            || self.orchestration_job_id.kind() != ResourceKind::Job
            || [&self.node_event_id, &self.job_event_id]
                .into_iter()
                .any(|id| id.kind() != ResourceKind::Event)
            || [&self.node_outbox_id, &self.job_outbox_id]
                .into_iter()
                .any(|id| id.kind() != ResourceKind::OutboxEvent)
        {
            return Err(ControllerStoreError::InvalidInput(
                "controller activation identities are invalid".to_owned(),
            ));
        }
        let mut unique = BTreeSet::new();
        for id in [
            &self.node_execution_id,
            &self.orchestration_job_id,
            &self.node_event_id,
            &self.node_outbox_id,
            &self.job_event_id,
            &self.job_outbox_id,
        ] {
            if !unique.insert(id.to_string()) {
                return Err(ControllerStoreError::InvalidInput(
                    "controller activation identities must be unique".to_owned(),
                ));
            }
        }
        if let Some(scope) = &self.scope {
            scope.validate()?;
            for id in [
                &scope.scope_instance_id,
                &scope.scope_event_id,
                &scope.scope_outbox_id,
            ] {
                if !unique.insert(id.to_string()) {
                    return Err(ControllerStoreError::InvalidInput(
                        "controller activation scope identities must be unique".to_owned(),
                    ));
                }
            }
        }
        Ok(())
    }
}
#[derive(Debug, Clone, Serialize)]
pub struct ControllerScopeSlot {
    pub scope_instance_id: ResourceId,
    pub scope_event_id: ResourceId,
    pub scope_outbox_id: ResourceId,
}
impl ControllerScopeSlot {
    pub fn validate(&self) -> Result<(), ControllerStoreError> {
        if self.scope_instance_id.kind() != ResourceKind::ScopeInstance
            || self.scope_event_id.kind() != ResourceKind::Event
            || self.scope_outbox_id.kind() != ResourceKind::OutboxEvent
        {
            return Err(ControllerStoreError::InvalidInput(
                "controller Scope identities are invalid".to_owned(),
            ));
        }
        Ok(())
    }
}
#[derive(Debug, Clone, Serialize)]
pub struct ControllerPendingNodeSlot {
    pub node_execution_id: ResourceId,
    pub node_event_id: ResourceId,
    pub node_outbox_id: ResourceId,
}
impl ControllerPendingNodeSlot {
    pub fn validate(&self) -> Result<(), ControllerStoreError> {
        if self.node_execution_id.kind() != ResourceKind::NodeExecution
            || self.node_event_id.kind() != ResourceKind::Event
            || self.node_outbox_id.kind() != ResourceKind::OutboxEvent
        {
            return Err(ControllerStoreError::InvalidInput(
                "controller pending Node identities are invalid".to_owned(),
            ));
        }
        Ok(())
    }
}
#[derive(Debug, Clone, Serialize)]
pub struct ControllerStructuralExitSlot {
    pub scope_closing_event_id: ResourceId,
    pub scope_closing_outbox_id: ResourceId,
    pub scope_terminal_event_id: ResourceId,
    pub scope_terminal_outbox_id: ResourceId,
    pub loop_rollover: Option<ControllerLoopRolloverSlot>,
}
impl ControllerStructuralExitSlot {
    pub fn validate(&self) -> Result<(), ControllerStoreError> {
        if [&self.scope_closing_event_id, &self.scope_terminal_event_id]
            .into_iter()
            .any(|id| id.kind() != ResourceKind::Event)
            || [
                &self.scope_closing_outbox_id,
                &self.scope_terminal_outbox_id,
            ]
            .into_iter()
            .any(|id| id.kind() != ResourceKind::OutboxEvent)
        {
            return Err(ControllerStoreError::InvalidInput(
                "controller structural-exit identities are invalid".to_owned(),
            ));
        }
        if let Some(rollover) = &self.loop_rollover {
            rollover.scope.validate()?;
            if rollover
                .carried_value_ids
                .iter()
                .any(|id| id.kind() != ResourceKind::RunValue)
                || rollover
                    .carried_value_ids
                    .iter()
                    .collect::<BTreeSet<_>>()
                    .len()
                    != rollover.carried_value_ids.len()
            {
                return Err(ControllerStoreError::InvalidInput(
                    "Loop rollover identities are invalid".to_owned(),
                ));
            }
        }
        Ok(())
    }
}
#[derive(Debug, Clone, Serialize)]
pub struct ControllerLoopRolloverSlot {
    pub scope: ControllerScopeSlot,
    pub carried_value_ids: Vec<ResourceId>,
}
#[derive(Debug, Clone, Serialize)]
pub struct ControllerPendingWakeSlot {
    pub orchestration_job_id: ResourceId,
    pub request_digest: Sha256Digest,
    pub node_event_id: ResourceId,
    pub node_outbox_id: ResourceId,
    pub job_event_id: ResourceId,
    pub job_outbox_id: ResourceId,
}
impl ControllerPendingWakeSlot {
    pub fn validate(&self) -> Result<(), ControllerStoreError> {
        if self.orchestration_job_id.kind() != ResourceKind::Job
            || [&self.node_event_id, &self.job_event_id]
                .into_iter()
                .any(|id| id.kind() != ResourceKind::Event)
            || [&self.node_outbox_id, &self.job_outbox_id]
                .into_iter()
                .any(|id| id.kind() != ResourceKind::OutboxEvent)
        {
            return Err(ControllerStoreError::InvalidInput(
                "controller pending-wake identities are invalid".to_owned(),
            ));
        }
        Ok(())
    }
}
#[derive(Debug, Clone, Serialize)]
pub struct ControllerRemainderCancellationSlot {
    pub expected_scope_id: ResourceId,
    pub quota_entry_ids: Vec<ResourceId>,
    pub node_cancelling_event_id: ResourceId,
    pub node_cancelling_outbox_id: ResourceId,
    pub node_terminal_event_id: ResourceId,
    pub node_terminal_outbox_id: ResourceId,
    pub scope_closing_event_id: ResourceId,
    pub scope_closing_outbox_id: ResourceId,
    pub scope_terminal_event_id: ResourceId,
    pub scope_terminal_outbox_id: ResourceId,
    pub job_terminal_event_id: ResourceId,
    pub job_terminal_outbox_id: ResourceId,
}
impl ControllerRemainderCancellationSlot {
    pub fn validate(&self) -> Result<(), ControllerStoreError> {
        if self.expected_scope_id.kind() != ResourceKind::ScopeInstance
            || self.quota_entry_ids.len() != MAX_ORCHESTRATION_QUOTA_LINES
            || self
                .quota_entry_ids
                .iter()
                .any(|id| id.kind() != ResourceKind::QuotaLedgerEntry)
            || [
                &self.node_cancelling_event_id,
                &self.node_terminal_event_id,
                &self.scope_closing_event_id,
                &self.scope_terminal_event_id,
                &self.job_terminal_event_id,
            ]
            .into_iter()
            .any(|id| id.kind() != ResourceKind::Event)
            || [
                &self.node_cancelling_outbox_id,
                &self.node_terminal_outbox_id,
                &self.scope_closing_outbox_id,
                &self.scope_terminal_outbox_id,
                &self.job_terminal_outbox_id,
            ]
            .into_iter()
            .any(|id| id.kind() != ResourceKind::OutboxEvent)
        {
            return Err(ControllerStoreError::InvalidInput(
                "controller remainder-cancellation identities are invalid".to_owned(),
            ));
        }
        Ok(())
    }
}
#[derive(Debug, Clone, Serialize)]
pub struct ControllerStepMutationIds {
    pub receipt_id: ResourceId,
    pub quota_entry_ids: Vec<ResourceId>,
    pub run_event_id: ResourceId,
    pub run_outbox_id: ResourceId,
    pub node_event_id: ResourceId,
    pub node_outbox_id: ResourceId,
    pub job_event_id: ResourceId,
    pub job_outbox_id: ResourceId,
    pub activations: Vec<ControllerActivationSlot>,
    pub pending_nodes: Vec<ControllerPendingNodeSlot>,
    pub structural_exit: Option<ControllerStructuralExitSlot>,
    pub pending_wake: Option<ControllerPendingWakeSlot>,
    pub remainder_cancellations: Vec<ControllerRemainderCancellationSlot>,
}
impl ControllerStepMutationIds {
    pub fn validate(&self, plan_limits: PlanLimits) -> Result<(), ControllerStoreError> {
        if self.receipt_id.kind() != ResourceKind::Receipt
            || self.quota_entry_ids.len() != MAX_ORCHESTRATION_QUOTA_LINES
            || self
                .quota_entry_ids
                .iter()
                .any(|id| id.kind() != ResourceKind::QuotaLedgerEntry)
            || self.activations.len() > plan_limits.maximum_fan_out
            || self.pending_nodes.len() > plan_limits.maximum_fan_out
            || self.remainder_cancellations.len() > plan_limits.maximum_fan_out
            || [&self.run_event_id, &self.node_event_id, &self.job_event_id]
                .into_iter()
                .any(|id| id.kind() != ResourceKind::Event)
            || [
                &self.run_outbox_id,
                &self.node_outbox_id,
                &self.job_outbox_id,
            ]
            .into_iter()
            .any(|id| id.kind() != ResourceKind::OutboxEvent)
        {
            return Err(ControllerStoreError::InvalidInput(
                "controller step mutation identities are invalid".to_owned(),
            ));
        }
        let mut unique = BTreeSet::new();
        for id in self.quota_entry_ids.iter().chain([
            &self.receipt_id,
            &self.run_event_id,
            &self.run_outbox_id,
            &self.node_event_id,
            &self.node_outbox_id,
            &self.job_event_id,
            &self.job_outbox_id,
        ]) {
            if !unique.insert(id.to_string()) {
                return Err(ControllerStoreError::InvalidInput(
                    "controller step mutation identities must be globally unique".to_owned(),
                ));
            }
        }
        for activation in &self.activations {
            activation.validate()?;
            for id in [
                &activation.node_execution_id,
                &activation.orchestration_job_id,
                &activation.node_event_id,
                &activation.node_outbox_id,
                &activation.job_event_id,
                &activation.job_outbox_id,
            ] {
                if !unique.insert(id.to_string()) {
                    return Err(ControllerStoreError::InvalidInput(
                        "controller step mutation identities must be globally unique".to_owned(),
                    ));
                }
            }
            if let Some(scope) = &activation.scope {
                for id in [
                    &scope.scope_instance_id,
                    &scope.scope_event_id,
                    &scope.scope_outbox_id,
                ] {
                    if !unique.insert(id.to_string()) {
                        return Err(ControllerStoreError::InvalidInput(
                            "controller step mutation identities must be globally unique"
                                .to_owned(),
                        ));
                    }
                }
            }
        }
        for pending in &self.pending_nodes {
            pending.validate()?;
            for id in [
                &pending.node_execution_id,
                &pending.node_event_id,
                &pending.node_outbox_id,
            ] {
                if !unique.insert(id.to_string()) {
                    return Err(ControllerStoreError::InvalidInput(
                        "controller step mutation identities must be globally unique".to_owned(),
                    ));
                }
            }
        }
        if let Some(exit) = &self.structural_exit {
            exit.validate()?;
            for id in [
                &exit.scope_closing_event_id,
                &exit.scope_closing_outbox_id,
                &exit.scope_terminal_event_id,
                &exit.scope_terminal_outbox_id,
            ] {
                if !unique.insert(id.to_string()) {
                    return Err(ControllerStoreError::InvalidInput(
                        "controller step mutation identities must be globally unique".to_owned(),
                    ));
                }
            }
            if let Some(rollover) = &exit.loop_rollover {
                for id in [
                    &rollover.scope.scope_instance_id,
                    &rollover.scope.scope_event_id,
                    &rollover.scope.scope_outbox_id,
                ]
                .into_iter()
                .chain(rollover.carried_value_ids.iter())
                {
                    if !unique.insert(id.to_string()) {
                        return Err(ControllerStoreError::InvalidInput(
                            "controller step mutation identities must be globally unique"
                                .to_owned(),
                        ));
                    }
                }
            }
        }
        if let Some(wake) = &self.pending_wake {
            wake.validate()?;
            for id in [
                &wake.orchestration_job_id,
                &wake.node_event_id,
                &wake.node_outbox_id,
                &wake.job_event_id,
                &wake.job_outbox_id,
            ] {
                if !unique.insert(id.to_string()) {
                    return Err(ControllerStoreError::InvalidInput(
                        "controller step mutation identities must be globally unique".to_owned(),
                    ));
                }
            }
        }
        let mut cancellation_scopes = BTreeSet::new();
        for cancellation in &self.remainder_cancellations {
            cancellation.validate()?;
            if !cancellation_scopes.insert(cancellation.expected_scope_id.to_string()) {
                return Err(ControllerStoreError::InvalidInput(
                    "controller remainder-cancellation Scopes must be unique".to_owned(),
                ));
            }
            for id in cancellation.quota_entry_ids.iter().chain([
                &cancellation.node_cancelling_event_id,
                &cancellation.node_cancelling_outbox_id,
                &cancellation.node_terminal_event_id,
                &cancellation.node_terminal_outbox_id,
                &cancellation.scope_closing_event_id,
                &cancellation.scope_closing_outbox_id,
                &cancellation.scope_terminal_event_id,
                &cancellation.scope_terminal_outbox_id,
                &cancellation.job_terminal_event_id,
                &cancellation.job_terminal_outbox_id,
            ]) {
                if !unique.insert(id.to_string()) {
                    return Err(ControllerStoreError::InvalidInput(
                        "controller step mutation identities must be globally unique".to_owned(),
                    ));
                }
            }
        }
        Ok(())
    }
}
#[derive(Debug, Clone)]
pub struct ApplyOrchestrationControllerStep {
    pub fence: JobFence,
    pub plan: RuntimePlan,
    pub observation: ControllerObservation,
    pub idempotency_key_digest: Sha256Digest,
    pub request_digest: Sha256Digest,
    pub receipt_expires_at: DateTime<Utc>,
    pub mutations: ControllerStepMutationIds,
}
impl ApplyOrchestrationControllerStep {
    pub fn validate_at(
        &self,
        now: DateTime<Utc>,
        plan_limits: PlanLimits,
    ) -> Result<(), ControllerStoreError> {
        self.fence.validate()?;
        self.mutations.validate(plan_limits)?;
        self.plan
            .validate(plan_limits)
            .map_err(|failure| ControllerStoreError::InvalidInput(failure.to_string()))?;
        if self.receipt_expires_at <= now {
            return Err(ControllerStoreError::InvalidInput(
                "controller step receipt deadline is invalid".to_owned(),
            ));
        }
        Ok(())
    }
}
#[derive(Debug, Clone)]
pub struct ApplyDerivedExpressionControllerStep {
    pub step: ApplyOrchestrationControllerStep,
    pub materialized_inputs: Vec<CommittedExpressionInput>,
    pub evaluation: ControllerEvaluation,
    pub output_value_ids: Vec<ResourceId>,
}
impl ApplyDerivedExpressionControllerStep {
    pub fn validate_at(
        &self,
        now: DateTime<Utc>,
        plan_limits: PlanLimits,
    ) -> Result<(), ControllerStoreError> {
        self.step.validate_at(now, plan_limits)?;
        self.evaluation
            .validate()
            .map_err(|failure| ControllerStoreError::InvalidInput(failure.to_string()))?;
        if self.step.observation != self.evaluation.observation
            || self.materialized_inputs.len() > plan_limits.expression.maximum_input_ports
            || self.output_value_ids.len() > plan_limits.maximum_map_items as usize
            || self
                .output_value_ids
                .iter()
                .any(|id| id.kind() != ResourceKind::RunValue)
            || self.output_value_ids.iter().collect::<BTreeSet<_>>().len()
                != self.output_value_ids.len()
        {
            return Err(ControllerStoreError::InvalidInput(
                "derived expression command is inconsistent".to_owned(),
            ));
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ControllerStructuralRequirement {
    None,
    Close,
    LoopRollover { carried_value_count: usize },
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControllerMutationRequirements {
    /// One entry per activation; true means the activation must allocate a new Scope.
    pub activation_scopes: Vec<bool>,
    pub pending_node_count: usize,
    pub structural_exit: ControllerStructuralRequirement,
    pub pending_wake: bool,
    pub remainder_cancellation_scope_ids: Vec<ResourceId>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum OrchestrationFailureCause {
    Committed { failure: Failure },
    Admission { failure: Failure },
    Controller { observation: ControllerObservation },
}
impl OrchestrationFailureCause {
    pub fn validate(&self) -> Result<(), ControllerStoreError> {
        if let Self::Committed { failure } | Self::Admission { failure } = self {
            failure
                .validate(1_024)
                .map_err(|failure| ControllerStoreError::InvalidInput(failure.to_string()))?;
        }
        Ok(())
    }
}
#[derive(Debug, Clone)]
pub struct FailOrchestrationJob {
    pub fence: JobFence,
    pub plan: RuntimePlan,
    pub cause: OrchestrationFailureCause,
    pub derived_expression: Option<DerivedExpressionFailureEvidence>,
    pub idempotency_key_digest: Sha256Digest,
    pub request_digest: Sha256Digest,
    pub receipt_expires_at: DateTime<Utc>,
    pub mutations: ControllerStepMutationIds,
}
impl FailOrchestrationJob {
    pub fn validate_at(
        &self,
        now: DateTime<Utc>,
        plan_limits: PlanLimits,
    ) -> Result<(), ControllerStoreError> {
        self.fence.validate()?;
        self.cause.validate()?;
        self.mutations.validate(plan_limits)?;
        self.plan
            .validate(plan_limits)
            .map_err(|failure| ControllerStoreError::InvalidInput(failure.to_string()))?;
        if let Some(derived) = &self.derived_expression {
            derived
                .evaluation
                .validate()
                .map_err(|failure| ControllerStoreError::InvalidInput(failure.to_string()))?;
            if !matches!(
                &self.cause,
                OrchestrationFailureCause::Controller { observation }
                    if observation == &derived.evaluation.observation
            ) || derived.materialized_inputs.len() > plan_limits.expression.maximum_input_ports
            {
                return Err(ControllerStoreError::InvalidInput(
                    "derived expression failure evidence is inconsistent".to_owned(),
                ));
            }
        }
        if self.receipt_expires_at <= now {
            return Err(ControllerStoreError::InvalidInput(
                "orchestration failure receipt deadline is invalid".to_owned(),
            ));
        }
        Ok(())
    }
}
#[derive(Debug, Clone)]
pub struct DerivedExpressionFailureEvidence {
    pub materialized_inputs: Vec<CommittedExpressionInput>,
    pub evaluation: ControllerEvaluation,
}
#[derive(Debug, Clone, PartialEq)]
pub struct ControllerActivationRecord {
    pub node_id: String,
    pub plan_node_key: PlanNodeKey,
    pub node_kind: PlanNodeKind,
    pub node_version: i64,
    pub job: JobRecord,
}
#[derive(Debug, Clone, PartialEq)]
pub struct ControllerScopeRecord {
    pub scope_id: String,
    pub scope_kind: String,
    pub state: String,
    pub version: i64,
}
#[derive(Debug, Clone, PartialEq)]
pub struct ControllerPendingNodeRecord {
    pub node_id: String,
    pub plan_node_key: PlanNodeKey,
    pub node_kind: PlanNodeKind,
    pub node_version: i64,
}
#[derive(Debug, Clone, PartialEq)]
pub struct ControllerCancelledRemainderRecord {
    pub scope: ControllerScopeRecord,
    pub reason_code: String,
    pub node_id: String,
    pub node_version: i64,
    pub node_cancelling_version: Option<i64>,
    pub job: JobRecord,
    pub settled_quota_account_ids: Vec<String>,
}
#[derive(Debug, Clone, PartialEq)]
pub struct AppliedOrchestrationControllerStep {
    pub run: RunRecord,
    pub source_node_id: String,
    pub source_node_version: i64,
    pub source_job: JobRecord,
    pub activations: Vec<ControllerActivationRecord>,
    pub created_scopes: Vec<ControllerScopeRecord>,
    pub pending_nodes: Vec<ControllerPendingNodeRecord>,
    pub settled_scopes: Vec<ControllerScopeRecord>,
    pub woken_nodes: Vec<ControllerActivationRecord>,
    pub cancelled_remainders: Vec<ControllerCancelledRemainderRecord>,
    pub settled_quota_account_ids: Vec<String>,
}
#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedExpressionInput {
    pub run_value_id: ResourceId,
    pub producing_node_id: Option<ResourceId>,
    pub value_kind: String,
    pub port: ExactDataPortRef,
    pub classification: DataClassification,
    pub schema_digest: Sha256Digest,
    pub content_digest: Sha256Digest,
    pub value: ValueRef,
}
#[derive(Debug, Clone, PartialEq)]
pub struct ExpressionControllerFacts {
    pub node_execution_id: ResourceId,
    pub node_execution_version: i64,
    pub plan_node_key: PlanNodeKey,
    pub loop_iteration: u32,
    pub inputs: Vec<ResolvedExpressionInput>,
}
#[derive(Debug, Clone, PartialEq)]
pub struct ControllerFactIdentity {
    pub node_execution_id: ResourceId,
    pub node_execution_version: i64,
    pub plan_node_key: PlanNodeKey,
}
#[derive(Debug, Clone, PartialEq)]
pub struct ChildAgentDispatchFacts {
    pub identity: ControllerFactIdentity,
    pub input: ResolvedExpressionInput,
    pub route: Option<ResolvedExpressionInput>,
    pub selection_policy: insight_platform_contracts::ExactPolicyBinding,
    pub selection_document: CandidateSelectionPolicyDocument,
    pub candidates: Vec<ExactDeploymentRef>,
}
#[derive(Debug, Clone, PartialEq)]
pub struct CapabilityDispatchFacts {
    pub identity: ControllerFactIdentity,
    pub input: ResolvedExpressionInput,
    pub route: Option<ResolvedExpressionInput>,
    pub selection_policy: insight_platform_contracts::ExactPolicyBinding,
    pub selection_document: CandidateSelectionPolicyDocument,
    pub candidates: Vec<ExactDeploymentRef>,
}
#[derive(Debug, Clone, PartialEq)]
pub struct ContextDispatchFacts {
    pub identity: ControllerFactIdentity,
    pub input: ResolvedExpressionInput,
}
#[derive(Debug, Clone, PartialEq)]
pub struct ModelDispatchFacts {
    pub identity: ControllerFactIdentity,
    pub input: ResolvedExpressionInput,
    pub route: Option<ResolvedExpressionInput>,
    pub selection_policy: insight_platform_contracts::ExactPolicyBinding,
    pub selection_document: CandidateSelectionPolicyDocument,
    pub candidates: Vec<ExactDeploymentRef>,
    pub tool_slots: Vec<insight_platform_contracts::FrozenSlotBinding>,
}
#[derive(Debug, Clone, PartialEq)]
pub struct ModelToolIntentDispatchFact {
    pub call_id: String,
    pub projected_tool_name: String,
    pub arguments: ClosedJsonValue,
    pub slot_id: String,
    pub selected_candidate_ordinal: u16,
    pub selected_deployment: ExactDeploymentRef,
}
#[derive(Debug, Clone, PartialEq)]
pub struct ModelToolContinuationFacts {
    pub tenant_id: ResourceId,
    pub run_id: ResourceId,
    pub node_execution_id: ResourceId,
    pub node_execution_version: u64,
    pub model_turn_id: ResourceId,
    pub round_ordinal: u16,
    pub calls: Vec<ModelToolIntentDispatchFact>,
}
#[derive(Debug, Clone, PartialEq)]
pub struct ModelToolResultContinuationFacts {
    pub tenant_id: ResourceId,
    pub run_id: ResourceId,
    pub node_execution_id: ResourceId,
    pub node_execution_version: u64,
    pub previous_request: insight_platform_models::ModelRequestValue,
    pub results: Vec<insight_platform_models::ModelToolResult>,
    pub requested_attempt_limit: u32,
    pub cost_ceiling_microunits: u64,
}
#[derive(Debug, Clone, PartialEq)]
pub enum ControllerFacts {
    Expression(ExpressionControllerFacts),
    ChildAgentDispatch(Box<ChildAgentDispatchFacts>),
    CapabilityDispatch(Box<CapabilityDispatchFacts>),
    ContextDispatch(Box<ContextDispatchFacts>),
    ModelDispatch(Box<ModelDispatchFacts>),
    Committed {
        identity: ControllerFactIdentity,
        observation: ControllerObservation,
    },
}
#[derive(Debug, Clone, PartialEq)]
pub struct FailedOrchestrationJob {
    pub run: RunRecord,
    pub source_node_id: String,
    pub source_node_version: i64,
    pub source_job: JobRecord,
    pub failure: Failure,
    pub controller_code: Option<String>,
    pub handler_activations: Vec<ControllerActivationRecord>,
    pub settled_scopes: Vec<ControllerScopeRecord>,
    pub woken_nodes: Vec<ControllerActivationRecord>,
    pub cancelled_remainders: Vec<ControllerCancelledRemainderRecord>,
    pub settled_quota_account_ids: Vec<String>,
}
#[derive(Debug, Clone)]
pub struct DeferOrchestrationTaskMutationIds {
    pub receipt_id: ResourceId,
    pub quota_entry_ids: Vec<ResourceId>,
    pub run_event_id: ResourceId,
    pub run_outbox_id: ResourceId,
    pub node_event_id: ResourceId,
    pub node_outbox_id: ResourceId,
    pub task_event_id: ResourceId,
    pub task_outbox_id: ResourceId,
    pub job_event_id: ResourceId,
    pub job_outbox_id: ResourceId,
}
impl DeferOrchestrationTaskMutationIds {
    pub fn validate(&self) -> Result<(), ControllerStoreError> {
        if self.receipt_id.kind() != ResourceKind::Receipt
            || self.quota_entry_ids.len() != MAX_ORCHESTRATION_QUOTA_LINES
            || self
                .quota_entry_ids
                .iter()
                .any(|id| id.kind() != ResourceKind::QuotaLedgerEntry)
            || [
                &self.run_event_id,
                &self.node_event_id,
                &self.task_event_id,
                &self.job_event_id,
            ]
            .into_iter()
            .any(|id| id.kind() != ResourceKind::Event)
            || [
                &self.run_outbox_id,
                &self.node_outbox_id,
                &self.task_outbox_id,
                &self.job_outbox_id,
            ]
            .into_iter()
            .any(|id| id.kind() != ResourceKind::OutboxEvent)
        {
            return Err(ControllerStoreError::InvalidInput(
                "orchestration Task deferral identities are invalid".to_owned(),
            ));
        }
        let mut unique = BTreeSet::new();
        for id in self.quota_entry_ids.iter().chain([
            &self.receipt_id,
            &self.run_event_id,
            &self.run_outbox_id,
            &self.node_event_id,
            &self.node_outbox_id,
            &self.task_event_id,
            &self.task_outbox_id,
            &self.job_event_id,
            &self.job_outbox_id,
        ]) {
            if !unique.insert(id.to_string()) {
                return Err(ControllerStoreError::InvalidInput(
                    "orchestration Task deferral identities must be unique".to_owned(),
                ));
            }
        }
        Ok(())
    }
}
#[derive(Debug, Clone)]
pub struct DeferOrchestrationToTask {
    pub fence: JobFence,
    pub plan: RuntimePlan,
    pub task_id: ResourceId,
    pub definition: TaskDefinition,
    pub response_schema_digest: Option<Sha256Digest>,
    pub task_deadline: DateTime<Utc>,
    pub idempotency_key_digest: Sha256Digest,
    pub request_digest: Sha256Digest,
    pub receipt_expires_at: DateTime<Utc>,
    pub mutations: DeferOrchestrationTaskMutationIds,
}
impl DeferOrchestrationToTask {
    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), ControllerStoreError> {
        self.fence.validate()?;
        self.mutations.validate()?;
        self.definition
            .validate()
            .map_err(|failure| ControllerStoreError::InvalidInput(failure.to_string()))?;
        let kind = self.definition.task_kind();
        if kind == TaskKind::Approval
            || self.task_id.kind() != kind.task_id_kind()
            || self.receipt_expires_at <= now
            || (matches!(kind, TaskKind::InteractionForm | TaskKind::HumanWork)
                && self.response_schema_digest.is_none())
        {
            return Err(ControllerStoreError::InvalidInput(
                "orchestration Task deferral is invalid".to_owned(),
            ));
        }
        Ok(())
    }
}
#[derive(Debug, Clone)]
pub struct DeferOrchestrationContextMutationIds {
    pub source: OrchestrationYieldMutationIds,
    pub context_create_receipt_id: ResourceId,
    pub context_create_event_id: ResourceId,
    pub context_create_outbox_id: ResourceId,
    pub context_prepare_receipt_id: ResourceId,
    pub context_prepare_event_id: ResourceId,
    pub context_prepare_outbox_id: ResourceId,
}
impl DeferOrchestrationContextMutationIds {
    pub fn validate(&self) -> Result<(), ControllerStoreError> {
        self.source.validate()?;
        let expected = [
            (&self.context_create_receipt_id, ResourceKind::Receipt),
            (&self.context_create_event_id, ResourceKind::Event),
            (&self.context_create_outbox_id, ResourceKind::OutboxEvent),
            (&self.context_prepare_receipt_id, ResourceKind::Receipt),
            (&self.context_prepare_event_id, ResourceKind::Event),
            (&self.context_prepare_outbox_id, ResourceKind::OutboxEvent),
        ];
        if expected.iter().any(|(id, kind)| id.kind() != *kind) {
            return Err(ControllerStoreError::InvalidInput(
                "orchestration Context mutation identity is invalid".to_owned(),
            ));
        }
        let mut unique = BTreeSet::new();
        for id in self.source.quota_entry_ids.iter().chain([
            &self.source.receipt_id,
            &self.source.run_event_id,
            &self.source.run_outbox_id,
            &self.source.node_event_id,
            &self.source.node_outbox_id,
            &self.source.job_event_id,
            &self.source.job_outbox_id,
            &self.context_create_receipt_id,
            &self.context_create_event_id,
            &self.context_create_outbox_id,
            &self.context_prepare_receipt_id,
            &self.context_prepare_event_id,
            &self.context_prepare_outbox_id,
        ]) {
            if !unique.insert(id.to_string()) {
                return Err(ControllerStoreError::InvalidInput(
                    "orchestration Context mutation identities must be unique".to_owned(),
                ));
            }
        }
        Ok(())
    }
}
#[derive(Debug, Clone)]
pub struct DeferOrchestrationModelMutationIds {
    pub source: OrchestrationYieldMutationIds,
    pub model_create_receipt_id: ResourceId,
    pub model_create_event_id: ResourceId,
    pub model_create_outbox_id: ResourceId,
    pub model_prepare_receipt_id: ResourceId,
    pub model_prepare_event_id: ResourceId,
    pub model_prepare_outbox_id: ResourceId,
}
impl DeferOrchestrationModelMutationIds {
    pub fn validate(&self) -> Result<(), ControllerStoreError> {
        self.source.validate()?;
        let expected = [
            (&self.model_create_receipt_id, ResourceKind::Receipt),
            (&self.model_create_event_id, ResourceKind::Event),
            (&self.model_create_outbox_id, ResourceKind::OutboxEvent),
            (&self.model_prepare_receipt_id, ResourceKind::Receipt),
            (&self.model_prepare_event_id, ResourceKind::Event),
            (&self.model_prepare_outbox_id, ResourceKind::OutboxEvent),
        ];
        if expected.iter().any(|(id, kind)| id.kind() != *kind) {
            return Err(ControllerStoreError::InvalidInput(
                "orchestration Model mutation identity is invalid".to_owned(),
            ));
        }
        let mut unique = BTreeSet::new();
        for id in self.source.quota_entry_ids.iter().chain([
            &self.source.receipt_id,
            &self.source.run_event_id,
            &self.source.run_outbox_id,
            &self.source.node_event_id,
            &self.source.node_outbox_id,
            &self.source.job_event_id,
            &self.source.job_outbox_id,
            &self.model_create_receipt_id,
            &self.model_create_event_id,
            &self.model_create_outbox_id,
            &self.model_prepare_receipt_id,
            &self.model_prepare_event_id,
            &self.model_prepare_outbox_id,
        ]) {
            if !unique.insert(id.to_string()) {
                return Err(ControllerStoreError::InvalidInput(
                    "orchestration Model mutation identities must be unique".to_owned(),
                ));
            }
        }
        Ok(())
    }
}
#[derive(Debug, Clone)]
pub struct DeferOrchestrationToModelTurn {
    pub fence: JobFence,
    pub plan: RuntimePlan,
    pub model_turn_id: ResourceId,
    pub model_job_id: ResourceId,
    pub input: ResolvedExpressionInput,
    pub request: ModelRequestValue,
    pub route: Option<(ResolvedExpressionInput, ClosedJsonValue)>,
    pub selection_evidence: CandidateSelectionEvidence,
    pub tool_slots: Vec<insight_platform_contracts::FrozenSlotBinding>,
    pub requested_attempt_limit: u32,
    pub cost_ceiling_microunits: u64,
    pub idempotency_key_digest: Sha256Digest,
    pub request_digest: Sha256Digest,
    pub receipt_expires_at: DateTime<Utc>,
    pub mutations: DeferOrchestrationModelMutationIds,
}
impl DeferOrchestrationToModelTurn {
    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), ControllerStoreError> {
        self.fence.validate()?;
        self.mutations.validate()?;
        if self.model_turn_id.kind() != ResourceKind::ModelTurn
            || self.model_job_id.kind() != ResourceKind::Job
            || self.input.run_value_id.kind() != ResourceKind::RunValue
            || self.request.value_id.kind() != ResourceKind::RunValue
            || self.requested_attempt_limit == 0
            || self.cost_ceiling_microunits == 0
            || self.receipt_expires_at <= now
        {
            return Err(ControllerStoreError::InvalidInput(
                "orchestration Model deferral is invalid".to_owned(),
            ));
        }
        Ok(())
    }
}
#[derive(Debug, Clone)]
pub struct ContinueModelToolResultsToModelTurn {
    pub fence: JobFence,
    pub plan: RuntimePlan,
    pub continuation: crate::ModelToolContinuation,
    pub model_turn_id: ResourceId,
    pub model_job_id: ResourceId,
    pub request: ModelRequestValue,
    pub requested_attempt_limit: u32,
    pub cost_ceiling_microunits: u64,
    pub idempotency_key_digest: Sha256Digest,
    pub request_digest: Sha256Digest,
    pub receipt_expires_at: DateTime<Utc>,
    pub mutations: DeferOrchestrationModelMutationIds,
}
impl ContinueModelToolResultsToModelTurn {
    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), ControllerStoreError> {
        self.fence.validate()?;
        self.continuation
            .validate()
            .map_err(|failure| ControllerStoreError::InvalidInput(failure.to_string()))?;
        self.mutations.validate()?;
        if self.continuation.results.is_empty()
            || self.model_turn_id.kind() != ResourceKind::ModelTurn
            || self.model_job_id.kind() != ResourceKind::Job
            || self.request.value_id.kind() != ResourceKind::RunValue
            || self.request.request.model_turn_id != self.model_turn_id
            || self.requested_attempt_limit == 0
            || self.cost_ceiling_microunits == 0
            || self.receipt_expires_at <= now
        {
            return Err(ControllerStoreError::InvalidInput(
                "Model tool result continuation is invalid".to_owned(),
            ));
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct DeferredOrchestrationModelTurn {
    pub run: RunRecord,
    pub node_id: String,
    pub node_version: i64,
    pub source_job: JobRecord,
    pub turn: insight_platform_models::ModelTurnRecord,
    pub model_job: JobRecord,
    pub settled_quota_account_ids: Vec<String>,
}
#[derive(Debug, Clone)]
pub struct DeferOrchestrationCapabilityMutationIds {
    pub source: OrchestrationYieldMutationIds,
    pub invocation_admit_receipt_id: ResourceId,
    pub invocation_admit_event_id: ResourceId,
    pub invocation_admit_outbox_id: ResourceId,
    pub invocation_prepare_receipt_id: ResourceId,
    pub invocation_prepare_event_id: ResourceId,
    pub invocation_prepare_outbox_id: ResourceId,
}
impl DeferOrchestrationCapabilityMutationIds {
    pub fn validate(&self) -> Result<(), ControllerStoreError> {
        self.source.validate()?;
        let expected = [
            (&self.invocation_admit_receipt_id, ResourceKind::Receipt),
            (&self.invocation_admit_event_id, ResourceKind::Event),
            (&self.invocation_admit_outbox_id, ResourceKind::OutboxEvent),
            (&self.invocation_prepare_receipt_id, ResourceKind::Receipt),
            (&self.invocation_prepare_event_id, ResourceKind::Event),
            (
                &self.invocation_prepare_outbox_id,
                ResourceKind::OutboxEvent,
            ),
        ];
        if expected.iter().any(|(id, kind)| id.kind() != *kind) {
            return Err(ControllerStoreError::InvalidInput(
                "orchestration Capability mutation identity is invalid".to_owned(),
            ));
        }
        let mut unique = BTreeSet::new();
        for id in self.source.quota_entry_ids.iter().chain([
            &self.source.receipt_id,
            &self.source.run_event_id,
            &self.source.run_outbox_id,
            &self.source.node_event_id,
            &self.source.node_outbox_id,
            &self.source.job_event_id,
            &self.source.job_outbox_id,
            &self.invocation_admit_receipt_id,
            &self.invocation_admit_event_id,
            &self.invocation_admit_outbox_id,
            &self.invocation_prepare_receipt_id,
            &self.invocation_prepare_event_id,
            &self.invocation_prepare_outbox_id,
        ]) {
            if !unique.insert(id.to_string()) {
                return Err(ControllerStoreError::InvalidInput(
                    "orchestration Capability mutation identities must be unique".to_owned(),
                ));
            }
        }
        Ok(())
    }
}
#[derive(Debug, Clone)]
pub struct DeferOrchestrationToCapabilityInvocation {
    pub fence: JobFence,
    pub plan: RuntimePlan,
    pub invocation_id: ResourceId,
    pub capability_job_id: ResourceId,
    pub input: ResolvedExpressionInput,
    pub route: Option<(ResolvedExpressionInput, ClosedJsonValue)>,
    pub selection_evidence: CandidateSelectionEvidence,
    pub policy_decisions: InvocationPolicyDecisionBundle,
    pub approval_task_id: Option<ResourceId>,
    pub input_artifact_link_id: Option<ResourceId>,
    pub mcp_runtime: Option<McpCapabilityRuntimeRequest>,
    pub sandbox_submission:
        Option<insight_platform_sandbox::contracts::SandboxCapabilitySubmission>,
    pub idempotency_key_digest: Sha256Digest,
    pub request_digest: Sha256Digest,
    pub receipt_expires_at: DateTime<Utc>,
    pub mutations: DeferOrchestrationCapabilityMutationIds,
}
impl DeferOrchestrationToCapabilityInvocation {
    pub fn validate_at(
        &self,
        now: DateTime<Utc>,
        inline_limits: JsonLimits,
    ) -> Result<(), ControllerStoreError> {
        self.fence.validate()?;
        self.mutations.validate()?;
        if let Some(submission) = &self.sandbox_submission {
            submission.validate()?;
        }
        self.input
            .value
            .validate(inline_limits)
            .map_err(|failure| ControllerStoreError::InvalidInput(failure.to_string()))?;
        if let Some((resolved, value)) = &self.route {
            resolved
                .value
                .validate(inline_limits)
                .map_err(|failure| ControllerStoreError::InvalidInput(failure.to_string()))?;
            value
                .validate()
                .map_err(|failure| ControllerStoreError::InvalidInput(failure.to_string()))?;
        }
        if self.invocation_id.kind() != ResourceKind::CapabilityInvocation
            || self.capability_job_id.kind() != ResourceKind::Job
            || self.input.run_value_id.kind() != ResourceKind::RunValue
            || self.approval_task_id.is_some() != self.policy_decisions.approval.is_some()
            || self
                .approval_task_id
                .as_ref()
                .is_some_and(|id| id.kind() != ResourceKind::ApprovalTask)
            || self
                .input_artifact_link_id
                .as_ref()
                .is_some_and(|id| id.kind() != ResourceKind::ArtifactLink)
            || self.input_artifact_link_id.is_some()
                != matches!(self.input.value, ValueRef::Artifact { .. })
            || (self.sandbox_submission.is_some()
                && (self.mcp_runtime.is_some() || self.approval_task_id.is_some()))
            || self.receipt_expires_at <= now
        {
            return Err(ControllerStoreError::InvalidInput(
                "orchestration Capability deferral is invalid".to_owned(),
            ));
        }
        Ok(())
    }
}
#[derive(Debug, Clone)]
pub struct ModelToolCapabilityMutationIds {
    pub admit_receipt_id: ResourceId,
    pub admit_event_id: ResourceId,
    pub admit_outbox_id: ResourceId,
    pub prepare_receipt_id: ResourceId,
    pub prepare_event_id: ResourceId,
    pub prepare_outbox_id: ResourceId,
    pub sibling_cancel_event_id: ResourceId,
    pub sibling_cancel_outbox_id: ResourceId,
}
impl ModelToolCapabilityMutationIds {
    pub fn validate(&self) -> Result<(), ControllerStoreError> {
        let expected = [
            (&self.admit_receipt_id, ResourceKind::Receipt),
            (&self.admit_event_id, ResourceKind::Event),
            (&self.admit_outbox_id, ResourceKind::OutboxEvent),
            (&self.prepare_receipt_id, ResourceKind::Receipt),
            (&self.prepare_event_id, ResourceKind::Event),
            (&self.prepare_outbox_id, ResourceKind::OutboxEvent),
            (&self.sibling_cancel_event_id, ResourceKind::Event),
            (&self.sibling_cancel_outbox_id, ResourceKind::OutboxEvent),
        ];
        if expected.iter().any(|(id, kind)| id.kind() != *kind)
            || expected
                .iter()
                .map(|(id, _)| id.to_string())
                .collect::<BTreeSet<_>>()
                .len()
                != expected.len()
        {
            return Err(ControllerStoreError::InvalidInput(
                "Model tool mutation identities are invalid".to_owned(),
            ));
        }
        Ok(())
    }
}
#[derive(Debug, Clone)]
pub struct ModelToolCapabilityAdmission {
    pub call_id: String,
    pub call_id_digest: Sha256Digest,
    pub projected_tool_name: String,
    pub slot_id: String,
    pub selected_candidate_ordinal: u16,
    pub selected_deployment: ExactDeploymentRef,
    pub input_value_id: ResourceId,
    pub arguments: ClosedJsonValue,
    pub invocation_id: ResourceId,
    pub capability_job_id: ResourceId,
    pub policy_decisions: InvocationPolicyDecisionBundle,
    pub approval_task_id: Option<ResourceId>,
    pub mcp_runtime: Option<McpCapabilityRuntimeRequest>,
    pub requested_attempt_limit: u32,
    pub requested_retry_backoff_milliseconds: u64,
    pub idempotency_key_digest: Sha256Digest,
    pub mutations: ModelToolCapabilityMutationIds,
}
#[derive(Debug, Clone)]
pub struct DispatchModelToolCapabilities {
    pub fence: JobFence,
    pub plan: RuntimePlan,
    pub continuation: crate::ModelToolContinuation,
    pub calls: Vec<ModelToolCapabilityAdmission>,
    pub request_digest: Sha256Digest,
    pub receipt_expires_at: DateTime<Utc>,
    pub source_mutations: OrchestrationYieldMutationIds,
}
impl DispatchModelToolCapabilities {
    pub fn validate_at(
        &self,
        now: DateTime<Utc>,
        plan_limits: PlanLimits,
    ) -> Result<(), ControllerStoreError> {
        self.fence.validate()?;
        self.continuation
            .validate()
            .map_err(|failure| ControllerStoreError::InvalidInput(failure.to_string()))?;
        self.plan.validate(plan_limits)?;
        self.source_mutations.validate()?;
        if self.calls.len() != usize::from(self.continuation.tool_intent_count)
            || self.calls.is_empty()
            || self.receipt_expires_at <= now
        {
            return Err(ControllerStoreError::InvalidInput(
                "Model tool dispatch batch is invalid".to_owned(),
            ));
        }
        let mut call_ids = BTreeSet::new();
        let mut invocation_ids = BTreeSet::new();
        for call in &self.calls {
            call.mutations.validate()?;
            call.arguments
                .validate()
                .map_err(|failure| ControllerStoreError::InvalidInput(failure.to_string()))?;
            if call.invocation_id.kind() != ResourceKind::CapabilityInvocation
                || call.capability_job_id.kind() != ResourceKind::Job
                || call.input_value_id.kind() != ResourceKind::RunValue
                || call.selected_deployment.resource_kind != ResourceKind::CapabilityDeployment
                || call.approval_task_id.is_some() != call.policy_decisions.approval.is_some()
                || call.requested_attempt_limit == 0
                || call.requested_retry_backoff_milliseconds == 0
                || call.requested_retry_backoff_milliseconds > 60_000
                || !call_ids.insert(call.call_id.as_str())
                || !invocation_ids.insert(call.invocation_id.to_string())
            {
                return Err(ControllerStoreError::InvalidInput(
                    "Model tool call admission is invalid".to_owned(),
                ));
            }
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct DispatchedModelToolCapabilities {
    pub run: RunRecord,
    pub node_id: String,
    pub node_version: i64,
    pub source_job: JobRecord,
    pub invocations: Vec<CapabilityInvocationRecord>,
    pub capability_jobs: Vec<JobRecord>,
}
#[derive(Debug, Clone, PartialEq)]
pub struct DeferredOrchestrationCapabilityInvocation {
    pub run: RunRecord,
    pub node_id: String,
    pub node_version: i64,
    pub source_job: JobRecord,
    pub invocation: CapabilityInvocationRecord,
    pub capability_job: Option<JobRecord>,
    pub settled_quota_account_ids: Vec<String>,
}
#[derive(Debug, Clone)]
pub struct DeferOrchestrationToContextQuery {
    pub fence: JobFence,
    pub plan: RuntimePlan,
    pub context_query_id: ResourceId,
    pub context_job_id: ResourceId,
    pub input: ResolvedExpressionInput,
    pub materialized_input: ClosedJsonValue,
    pub input_artifact_link_id: Option<ResourceId>,
    pub idempotency_key_digest: Sha256Digest,
    pub request_digest: Sha256Digest,
    pub receipt_expires_at: DateTime<Utc>,
    pub mutations: DeferOrchestrationContextMutationIds,
}
impl DeferOrchestrationToContextQuery {
    pub fn validate_at(
        &self,
        now: DateTime<Utc>,
        inline_limits: JsonLimits,
    ) -> Result<(), ControllerStoreError> {
        self.fence.validate()?;
        self.mutations.validate()?;
        self.materialized_input
            .validate()
            .map_err(|failure| ControllerStoreError::InvalidInput(failure.to_string()))?;
        self.input
            .value
            .validate(inline_limits)
            .map_err(|failure| ControllerStoreError::InvalidInput(failure.to_string()))?;
        if self.context_query_id.kind() != ResourceKind::ContextQuery
            || self.context_job_id.kind() != ResourceKind::Job
            || self.input.run_value_id.kind() != ResourceKind::RunValue
            || self
                .input_artifact_link_id
                .as_ref()
                .is_some_and(|id| id.kind() != ResourceKind::ArtifactLink)
            || self.input_artifact_link_id.is_some()
                != matches!(self.input.value, ValueRef::Artifact { .. })
            || self.receipt_expires_at <= now
        {
            return Err(ControllerStoreError::InvalidInput(
                "orchestration Context deferral is invalid".to_owned(),
            ));
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct DeferredOrchestrationContextQuery {
    pub run: RunRecord,
    pub node_id: String,
    pub node_version: i64,
    pub source_job: JobRecord,
    pub query: insight_platform_context::ContextQueryRecord,
    pub context_job: JobRecord,
    pub settled_quota_account_ids: Vec<String>,
}
#[derive(Debug, Clone, PartialEq)]
pub struct DeferredOrchestrationTask {
    pub run: RunRecord,
    pub node_id: String,
    pub node_version: i64,
    pub job: JobRecord,
    pub task: TaskRecord,
    pub settled_quota_account_ids: Vec<String>,
}
#[derive(Debug, Clone)]
pub struct DeferOrchestrationChildMutationIds {
    pub receipt_id: ResourceId,
    pub quota_entry_ids: Vec<ResourceId>,
    pub root_run_event_id: ResourceId,
    pub root_run_outbox_id: ResourceId,
    pub parent_run_event_id: ResourceId,
    pub parent_run_outbox_id: ResourceId,
    pub parent_node_event_id: ResourceId,
    pub parent_node_outbox_id: ResourceId,
    pub parent_job_event_id: ResourceId,
    pub parent_job_outbox_id: ResourceId,
    pub child_link_event_id: ResourceId,
    pub child_link_outbox_id: ResourceId,
    pub child_run_event_id: ResourceId,
    pub child_run_outbox_id: ResourceId,
    pub child_job_event_id: ResourceId,
    pub child_job_outbox_id: ResourceId,
}
impl DeferOrchestrationChildMutationIds {
    pub fn validate(&self) -> Result<(), ControllerStoreError> {
        let event_ids = [
            &self.root_run_event_id,
            &self.parent_run_event_id,
            &self.parent_node_event_id,
            &self.parent_job_event_id,
            &self.child_link_event_id,
            &self.child_run_event_id,
            &self.child_job_event_id,
        ];
        let outbox_ids = [
            &self.root_run_outbox_id,
            &self.parent_run_outbox_id,
            &self.parent_node_outbox_id,
            &self.parent_job_outbox_id,
            &self.child_link_outbox_id,
            &self.child_run_outbox_id,
            &self.child_job_outbox_id,
        ];
        if self.receipt_id.kind() != ResourceKind::Receipt
            || self.quota_entry_ids.len() != MAX_ORCHESTRATION_QUOTA_LINES
            || self
                .quota_entry_ids
                .iter()
                .any(|id| id.kind() != ResourceKind::QuotaLedgerEntry)
            || event_ids.iter().any(|id| id.kind() != ResourceKind::Event)
            || outbox_ids
                .iter()
                .any(|id| id.kind() != ResourceKind::OutboxEvent)
        {
            return Err(ControllerStoreError::InvalidInput(
                "orchestration child deferral identities are invalid".to_owned(),
            ));
        }
        let mut unique = BTreeSet::new();
        for id in self
            .quota_entry_ids
            .iter()
            .chain([&self.receipt_id])
            .chain(event_ids)
            .chain(outbox_ids)
        {
            if !unique.insert(id.to_string()) {
                return Err(ControllerStoreError::InvalidInput(
                    "orchestration child deferral identities must be unique".to_owned(),
                ));
            }
        }
        Ok(())
    }
}
#[derive(Debug, Clone)]
pub struct DeferOrchestrationToChildRun {
    pub fence: JobFence,
    pub plan: RuntimePlan,
    pub slot_id: String,
    pub selected_child_deployment: ExactDeploymentRef,
    pub selection_evidence: CandidateSelectionEvidence,
    pub materialized_route: Option<ClosedJsonValue>,
    pub child_link_id: ResourceId,
    pub child_run_id: ResourceId,
    pub child_root_scope_id: ResourceId,
    pub child_entry_node_execution_id: ResourceId,
    pub child_orchestration_job_id: ResourceId,
    pub input: RunInputValue,
    pub source_value_ids: Vec<ResourceId>,
    pub budget: insight_platform_plan::ChildBudgetLimit,
    pub cancellation_policy: ChildCancellationPolicy,
    pub logical_key: String,
    pub child_attempt_limit: u16,
    pub child_retry_backoff_milliseconds: u64,
    pub idempotency_key_digest: Sha256Digest,
    pub request_digest: Sha256Digest,
    pub receipt_expires_at: DateTime<Utc>,
    pub mutations: DeferOrchestrationChildMutationIds,
}
impl DeferOrchestrationToChildRun {
    pub fn validate_at(
        &self,
        now: DateTime<Utc>,
        inline_limits: JsonLimits,
    ) -> Result<(), ControllerStoreError> {
        self.fence.validate()?;
        self.mutations.validate()?;
        self.selected_child_deployment
            .validate()
            .map_err(|failure| ControllerStoreError::InvalidInput(failure.to_string()))?;
        self.input
            .validate(inline_limits)
            .map_err(|failure| ControllerStoreError::InvalidInput(failure.to_string()))?;
        validate_child_budget_limit(&self.budget)
            .map_err(|failure| ControllerStoreError::InvalidInput(failure.to_string()))?;
        if let Some(route) = &self.materialized_route {
            route
                .validate()
                .map_err(|failure| ControllerStoreError::InvalidInput(failure.to_string()))?;
        }
        let mut source_ids = BTreeSet::new();
        if self.selected_child_deployment.resource_kind != ResourceKind::AgentDeployment
            || self.child_link_id.kind() != ResourceKind::ChildRunLink
            || self.child_run_id.kind() != ResourceKind::Run
            || self.child_root_scope_id.kind() != ResourceKind::ScopeInstance
            || self.child_entry_node_execution_id.kind() != ResourceKind::NodeExecution
            || self.child_orchestration_job_id.kind() != ResourceKind::Job
            || self.source_value_ids.len() > MAX_CHILD_INPUT_SOURCES
            || self
                .source_value_ids
                .iter()
                .any(|id| id.kind() != ResourceKind::RunValue || !source_ids.insert(id.to_string()))
            || self.slot_id.is_empty()
            || self.slot_id.len() > 128
            || self.selection_evidence.slot_id != self.slot_id
            || self.selection_evidence.selected_deployment != self.selected_child_deployment
            || self.selection_evidence.route_value.is_some() != self.materialized_route.is_some()
            || self.logical_key.is_empty()
            || self.logical_key.len() > 255
            || self.child_attempt_limit == 0
            || self.child_attempt_limit > 32
            || self.child_retry_backoff_milliseconds == 0
            || self.child_retry_backoff_milliseconds > 60_000
            || self.receipt_expires_at <= now
        {
            return Err(ControllerStoreError::InvalidInput(
                "orchestration child deferral is invalid".to_owned(),
            ));
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct ChildRunLinkRecord {
    pub tenant_id: String,
    pub child_link_id: String,
    pub parent_run_id: String,
    pub parent_node_execution_id: String,
    pub child_run_id: String,
    pub state: ChildLinkState,
    pub generation: i64,
    pub version: i64,
    pub slot_id: String,
    pub source_value_ids: Vec<ResourceId>,
    pub child_root_scope_id: ResourceId,
    pub child_entry_node_execution_id: ResourceId,
    pub child_orchestration_job_id: ResourceId,
    pub payload: ChildRunLinkPayload,
    pub deadline: DateTime<Utc>,
    pub terminal_at: Option<DateTime<Utc>>,
}
#[derive(Debug, Clone, PartialEq)]
pub struct DeferredOrchestrationChildRun {
    pub root_run: RunRecord,
    pub parent_run: RunRecord,
    pub parent_node_id: String,
    pub parent_node_version: i64,
    pub parent_job: JobRecord,
    pub child_link: ChildRunLinkRecord,
    pub child_run: RunRecord,
    pub child_job: JobRecord,
    pub settled_quota_account_ids: Vec<String>,
}
#[derive(Debug, Clone)]
pub struct TerminalChildRunSlot {
    pub parent_output_value_id: ResourceId,
    pub resume_job_id: ResourceId,
    pub resume_request_digest: Sha256Digest,
    pub child_link_event_id: ResourceId,
    pub child_link_outbox_id: ResourceId,
    pub parent_run_event_id: ResourceId,
    pub parent_run_outbox_id: ResourceId,
    pub parent_node_event_id: ResourceId,
    pub parent_node_outbox_id: ResourceId,
    pub resume_job_event_id: ResourceId,
    pub resume_job_outbox_id: ResourceId,
}
impl TerminalChildRunSlot {
    pub fn validate(&self) -> Result<(), ControllerStoreError> {
        let event_ids = [
            &self.child_link_event_id,
            &self.parent_run_event_id,
            &self.parent_node_event_id,
            &self.resume_job_event_id,
        ];
        let outbox_ids = [
            &self.child_link_outbox_id,
            &self.parent_run_outbox_id,
            &self.parent_node_outbox_id,
            &self.resume_job_outbox_id,
        ];
        if self.parent_output_value_id.kind() != ResourceKind::RunValue
            || self.resume_job_id.kind() != ResourceKind::Job
            || event_ids.iter().any(|id| id.kind() != ResourceKind::Event)
            || outbox_ids
                .iter()
                .any(|id| id.kind() != ResourceKind::OutboxEvent)
        {
            return Err(ControllerStoreError::InvalidInput(
                "terminal child Run slot identities are invalid".to_owned(),
            ));
        }
        let mut unique = BTreeSet::new();
        for id in [&self.parent_output_value_id, &self.resume_job_id]
            .into_iter()
            .chain(event_ids)
            .chain(outbox_ids)
        {
            if !unique.insert(id.to_string()) {
                return Err(ControllerStoreError::InvalidInput(
                    "terminal child Run slot identities must be unique".to_owned(),
                ));
            }
        }
        Ok(())
    }
}
impl TerminalChildRunSlot {
    pub fn resume_mutations(&self) -> insight_platform_contracts::ExternalLeafResumeMutationIds {
        insight_platform_contracts::ExternalLeafResumeMutationIds {
            continuation_job_id: self.resume_job_id.clone(),
            run_event_id: self.parent_run_event_id.clone(),
            run_outbox_id: self.parent_run_outbox_id.clone(),
            leaf_node_event_id: self.parent_node_event_id.clone(),
            leaf_node_outbox_id: self.parent_node_outbox_id.clone(),
            continuation_job_event_id: self.resume_job_event_id.clone(),
            continuation_job_outbox_id: self.resume_job_outbox_id.clone(),
        }
    }

    pub fn failure_mutations(&self) -> insight_platform_contracts::ExternalLeafFailureMutationIds {
        insight_platform_contracts::ExternalLeafFailureMutationIds {
            convergence_job_id: self.resume_job_id.clone(),
            run_event_id: self.parent_run_event_id.clone(),
            run_outbox_id: self.parent_run_outbox_id.clone(),
            leaf_node_event_id: self.parent_node_event_id.clone(),
            leaf_node_outbox_id: self.parent_node_outbox_id.clone(),
            convergence_job_event_id: self.resume_job_event_id.clone(),
            convergence_job_outbox_id: self.resume_job_outbox_id.clone(),
        }
    }
}
#[derive(Debug, Clone)]
pub struct DriveTerminalChildRuns {
    pub after: Option<SafetyScanCursor>,
    pub limit: u16,
    pub slots: Vec<TerminalChildRunSlot>,
}
impl DriveTerminalChildRuns {
    pub fn validate(&self, maximum_batch: u16) -> Result<(), ControllerStoreError> {
        if self.limit == 0
            || self.limit > maximum_batch
            || self.slots.len() != usize::from(self.limit)
        {
            return Err(ControllerStoreError::InvalidInput(
                "terminal child Run scan is outside the scheduler bound".to_owned(),
            ));
        }
        if let Some(cursor) = &self.after {
            cursor.validate(ResourceKind::ChildRunLink).map_err(|_| {
                ControllerStoreError::InvalidInput("invalid child recovery cursor".into())
            })?;
        }
        let mut unique = BTreeSet::new();
        for slot in &self.slots {
            slot.validate()?;
            for id in [
                &slot.parent_output_value_id,
                &slot.resume_job_id,
                &slot.child_link_event_id,
                &slot.child_link_outbox_id,
                &slot.parent_run_event_id,
                &slot.parent_run_outbox_id,
                &slot.parent_node_event_id,
                &slot.parent_node_outbox_id,
                &slot.resume_job_event_id,
                &slot.resume_job_outbox_id,
            ] {
                if !unique.insert(id.to_string()) {
                    return Err(ControllerStoreError::InvalidInput(
                        "terminal child Run scan identities must be globally unique".to_owned(),
                    ));
                }
            }
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedOrchestrationChildRun {
    pub parent_run: RunRecord,
    pub parent_node_id: String,
    pub parent_node_version: i64,
    pub child_link: ChildRunLinkRecord,
    pub child_run: RunRecord,
    pub parent_output_value_id: Option<ResourceId>,
    pub resume_job: JobRecord,
}
#[derive(Debug, Clone)]
pub struct ChildRunCancellationSlot {
    pub child_link_event_id: ResourceId,
    pub child_link_outbox_id: ResourceId,
    pub child_run_event_id: ResourceId,
    pub child_run_outbox_id: ResourceId,
}
impl ChildRunCancellationSlot {
    pub fn validate(&self) -> Result<(), ControllerStoreError> {
        if self.child_link_event_id.kind() != ResourceKind::Event
            || self.child_link_outbox_id.kind() != ResourceKind::OutboxEvent
            || self.child_run_event_id.kind() != ResourceKind::Event
            || self.child_run_outbox_id.kind() != ResourceKind::OutboxEvent
        {
            return Err(ControllerStoreError::InvalidInput(
                "child Run cancellation identities are invalid".to_owned(),
            ));
        }
        let mut unique = BTreeSet::new();
        for id in [
            &self.child_link_event_id,
            &self.child_link_outbox_id,
            &self.child_run_event_id,
            &self.child_run_outbox_id,
        ] {
            if !unique.insert(id.to_string()) {
                return Err(ControllerStoreError::InvalidInput(
                    "child Run cancellation identities must be unique".to_owned(),
                ));
            }
        }
        Ok(())
    }
}
#[derive(Debug, Clone)]
pub struct DriveChildRunCancellations {
    pub after: Option<SafetyScanCursor>,
    pub limit: u16,
    pub slots: Vec<ChildRunCancellationSlot>,
}
impl DriveChildRunCancellations {
    pub fn validate(&self, maximum_batch: u16) -> Result<(), ControllerStoreError> {
        if self.limit == 0
            || self.limit > maximum_batch
            || self.slots.len() != usize::from(self.limit)
        {
            return Err(ControllerStoreError::InvalidInput(
                "child Run cancellation scan is outside the recovery bound".to_owned(),
            ));
        }
        if let Some(cursor) = &self.after {
            cursor.validate(ResourceKind::ChildRunLink).map_err(|_| {
                ControllerStoreError::InvalidInput("invalid child recovery cursor".into())
            })?;
        }
        let mut unique = BTreeSet::new();
        for slot in &self.slots {
            slot.validate()?;
            for id in [
                &slot.child_link_event_id,
                &slot.child_link_outbox_id,
                &slot.child_run_event_id,
                &slot.child_run_outbox_id,
            ] {
                if !unique.insert(id.to_string()) {
                    return Err(ControllerStoreError::InvalidInput(
                        "child Run cancellation identities must be globally unique".to_owned(),
                    ));
                }
            }
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct CancellingOrchestrationChildRun {
    pub parent_run: RunRecord,
    pub child_link: ChildRunLinkRecord,
    pub child_run: RunRecord,
}
#[derive(Debug, Clone)]
pub struct ResolveOrchestrationTaskMutationIds {
    pub run_event_id: ResourceId,
    pub run_outbox_id: ResourceId,
    pub node_event_id: ResourceId,
    pub node_outbox_id: ResourceId,
    pub job_event_id: ResourceId,
    pub job_outbox_id: ResourceId,
}
impl ResolveOrchestrationTaskMutationIds {
    pub fn validate(&self) -> Result<(), ControllerStoreError> {
        if [&self.run_event_id, &self.node_event_id, &self.job_event_id]
            .into_iter()
            .any(|id| id.kind() != ResourceKind::Event)
            || [
                &self.run_outbox_id,
                &self.node_outbox_id,
                &self.job_outbox_id,
            ]
            .into_iter()
            .any(|id| id.kind() != ResourceKind::OutboxEvent)
        {
            return Err(ControllerStoreError::InvalidInput(
                "orchestration Task response identities are invalid".to_owned(),
            ));
        }
        let mut unique = BTreeSet::new();
        for id in [
            &self.run_event_id,
            &self.run_outbox_id,
            &self.node_event_id,
            &self.node_outbox_id,
            &self.job_event_id,
            &self.job_outbox_id,
        ] {
            if !unique.insert(id.to_string()) {
                return Err(ControllerStoreError::InvalidInput(
                    "orchestration Task response identities must be unique".to_owned(),
                ));
            }
        }
        Ok(())
    }
}
#[derive(Debug, Clone)]
pub struct ResolveOrchestrationTask {
    pub validated_input: Option<insight_platform_tasks::ValidatedTaskInput>,
    pub audit: CommandAudit,
    pub task_id: ResourceId,
    pub expected_generation: u64,
    pub expected_task_version: u64,
    pub target: TaskState,
    pub response: Option<RunInputValue>,
    pub resume_job_id: ResourceId,
    pub resume_request_digest: Sha256Digest,
    pub mutations: ResolveOrchestrationTaskMutationIds,
}
impl ResolveOrchestrationTask {
    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), ControllerStoreError> {
        self.audit
            .validate_at(now)
            .map_err(|failure| ControllerStoreError::InvalidInput(failure.to_string()))?;
        self.mutations.validate()?;
        if !matches!(
            self.target,
            TaskState::Responded | TaskState::Declined | TaskState::Cancelled
        ) || self.task_id.kind() != ResourceKind::Interaction
            || self.expected_generation == 0
            || self.expected_task_version == 0
            || self.resume_job_id.kind() != ResourceKind::Job
            || (self.target == TaskState::Responded) != self.response.is_some()
        {
            return Err(ControllerStoreError::InvalidInput(
                "orchestration Task response is invalid".to_owned(),
            ));
        }
        if let Some(response) = &self.response {
            response
                .validate(JsonLimits {
                    max_bytes: 65_536,
                    max_depth: 32,
                    max_properties_per_object: 1_024,
                    max_items_per_array: 4_096,
                    max_string_bytes: 65_536,
                })
                .map_err(|failure| ControllerStoreError::InvalidInput(failure.to_string()))?;
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedOrchestrationTask {
    pub run: RunRecord,
    pub node_id: String,
    pub node_version: i64,
    pub task: TaskRecord,
    pub job: JobRecord,
}
#[derive(Debug, Clone)]
pub struct ExpiredOrchestrationTaskSlot {
    pub resume_job_id: ResourceId,
    pub resume_request_digest: Sha256Digest,
    pub task_event_id: ResourceId,
    pub task_outbox_id: ResourceId,
    pub run_event_id: ResourceId,
    pub run_outbox_id: ResourceId,
    pub node_event_id: ResourceId,
    pub node_outbox_id: ResourceId,
    pub job_event_id: ResourceId,
    pub job_outbox_id: ResourceId,
}
impl ExpiredOrchestrationTaskSlot {
    pub fn validate(&self) -> Result<(), ControllerStoreError> {
        if self.resume_job_id.kind() != ResourceKind::Job
            || [
                &self.task_event_id,
                &self.run_event_id,
                &self.node_event_id,
                &self.job_event_id,
            ]
            .into_iter()
            .any(|id| id.kind() != ResourceKind::Event)
            || [
                &self.task_outbox_id,
                &self.run_outbox_id,
                &self.node_outbox_id,
                &self.job_outbox_id,
            ]
            .into_iter()
            .any(|id| id.kind() != ResourceKind::OutboxEvent)
        {
            return Err(ControllerStoreError::InvalidInput(
                "expired orchestration Task slot identity is invalid".to_owned(),
            ));
        }
        let mut unique = BTreeSet::new();
        for id in [
            &self.resume_job_id,
            &self.task_event_id,
            &self.task_outbox_id,
            &self.run_event_id,
            &self.run_outbox_id,
            &self.node_event_id,
            &self.node_outbox_id,
            &self.job_event_id,
            &self.job_outbox_id,
        ] {
            if !unique.insert(id.to_string()) {
                return Err(ControllerStoreError::InvalidInput(
                    "expired orchestration Task slot identities must be unique".to_owned(),
                ));
            }
        }
        Ok(())
    }
}
#[derive(Debug, Clone)]
pub struct DriveExpiredOrchestrationTasks {
    pub shard: SafetyScanShard,
    pub after: Option<SafetyScanCursor>,
    pub limit: u16,
    pub slots: Vec<ExpiredOrchestrationTaskSlot>,
}
impl DriveExpiredOrchestrationTasks {
    pub fn validate(
        &self,
        maximum_batch: u16,
        maximum_shards: u16,
    ) -> Result<(), ControllerStoreError> {
        validate_safety_scan_request(
            self.shard,
            self.after.as_ref(),
            ResourceKind::Interaction,
            self.limit,
            self.slots.len(),
            maximum_batch,
            maximum_shards,
        )?;
        let mut identities = BTreeSet::new();
        for slot in &self.slots {
            slot.validate()?;
            for id in [
                &slot.resume_job_id,
                &slot.task_event_id,
                &slot.task_outbox_id,
                &slot.run_event_id,
                &slot.run_outbox_id,
                &slot.node_event_id,
                &slot.node_outbox_id,
                &slot.job_event_id,
                &slot.job_outbox_id,
            ] {
                if !identities.insert(id.to_string()) {
                    return Err(ControllerStoreError::InvalidInput(
                        "expired orchestration Task scan identities must be globally unique"
                            .to_owned(),
                    ));
                }
            }
        }
        Ok(())
    }
}
#[derive(Debug, Clone)]
pub struct OrchestrationWakeMutationIds {
    pub receipt_id: ResourceId,
    pub run_event_id: ResourceId,
    pub run_outbox_id: ResourceId,
    pub node_event_id: ResourceId,
    pub node_outbox_id: ResourceId,
    pub job_event_id: ResourceId,
    pub job_outbox_id: ResourceId,
}
impl OrchestrationWakeMutationIds {
    pub fn validate(&self) -> Result<(), ControllerStoreError> {
        if self.receipt_id.kind() != ResourceKind::Receipt
            || [&self.run_event_id, &self.node_event_id, &self.job_event_id]
                .into_iter()
                .any(|identity| identity.kind() != ResourceKind::Event)
            || [
                &self.run_outbox_id,
                &self.node_outbox_id,
                &self.job_outbox_id,
            ]
            .into_iter()
            .any(|identity| identity.kind() != ResourceKind::OutboxEvent)
        {
            return Err(ControllerStoreError::InvalidInput(
                "orchestration wake mutation identity is invalid".to_owned(),
            ));
        }
        let mut unique = BTreeSet::new();
        for identity in [
            &self.receipt_id,
            &self.run_event_id,
            &self.run_outbox_id,
            &self.node_event_id,
            &self.node_outbox_id,
            &self.job_event_id,
            &self.job_outbox_id,
        ] {
            if !unique.insert(identity.to_string()) {
                return Err(ControllerStoreError::InvalidInput(
                    "orchestration wake mutation identities must be unique".to_owned(),
                ));
            }
        }
        Ok(())
    }
}
#[derive(Debug, Clone)]
pub struct OrchestrationSignalAuthority {
    pub tenant_id: ResourceId,
    pub principal_id: ResourceId,
    pub principal_kind: PrincipalKind,
    pub run_id: ResourceId,
    pub idempotency_key_digest: Sha256Digest,
    pub request_digest: Sha256Digest,
}
#[derive(Debug, Clone)]
pub struct ResolveOrchestrationSignalTarget {
    pub tenant_id: ResourceId,
    pub principal_id: ResourceId,
    pub principal_kind: PrincipalKind,
    pub run_id: ResourceId,
    pub signal_key: String,
    pub idempotency_key_digest: Sha256Digest,
    pub request_digest: Sha256Digest,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrchestrationSignalWakeTarget {
    pub job_id: ResourceId,
    pub job_version: i64,
    pub wake_generation: u64,
}
#[derive(Debug, Clone)]
pub struct WakeOrchestrationJob {
    pub tenant_id: ResourceId,
    pub job_id: ResourceId,
    pub expected_job_version: i64,
    pub expected_wake_generation: u64,
    pub source: WakeSource,
    pub signal_key: Option<String>,
    pub signal_payload: Option<RunInputValue>,
    pub idempotency_key_digest: Sha256Digest,
    pub request_digest: Sha256Digest,
    pub receipt_expires_at: DateTime<Utc>,
    pub signal_authority: Option<OrchestrationSignalAuthority>,
    pub mutations: OrchestrationWakeMutationIds,
}
impl WakeOrchestrationJob {
    pub fn validate_at(
        &self,
        now: DateTime<Utc>,
        inline_limits: JsonLimits,
    ) -> Result<(), ControllerStoreError> {
        self.mutations.validate()?;
        if self.tenant_id.kind() != ResourceKind::Tenant
            || self.job_id.kind() != ResourceKind::Job
            || self.expected_job_version <= 0
            || self.expected_wake_generation == 0
            || self.receipt_expires_at <= now
            || (self.source == WakeSource::Signal) != self.signal_key.is_some()
            || (self.source != WakeSource::Signal && self.signal_payload.is_some())
            || self
                .signal_key
                .as_deref()
                .is_some_and(|key| !valid_orchestration_signal_key(key))
            || self.signal_authority.as_ref().is_some_and(|authority| {
                self.source != WakeSource::Signal
                    || authority.tenant_id.kind() != ResourceKind::Tenant
                    || authority.run_id.kind() != ResourceKind::Run
                    || authority.tenant_id != self.tenant_id
                    || authority.idempotency_key_digest != self.idempotency_key_digest
                    || authority.request_digest != self.request_digest
            })
        {
            return Err(ControllerStoreError::InvalidInput(
                "orchestration wake command is invalid".to_owned(),
            ));
        }
        if let Some(payload) = &self.signal_payload {
            payload.validate(inline_limits)?;
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct WokenOrchestrationJob {
    pub run: RunRecord,
    pub job: JobRecord,
    pub node_id: String,
    pub node_version: i64,
}
#[derive(Debug, Clone)]
pub struct DueOrchestrationWaitSlot {
    pub idempotency_key_digest: Sha256Digest,
    pub request_digest: Sha256Digest,
    pub mutations: OrchestrationWakeMutationIds,
}
impl DueOrchestrationWaitSlot {
    pub fn validate(&self) -> Result<(), ControllerStoreError> {
        self.mutations.validate()
    }
}
#[derive(Debug, Clone)]
pub struct DriveDueOrchestrationWaits {
    pub shard: SafetyScanShard,
    pub after: Option<SafetyScanCursor>,
    pub limit: u16,
    pub slots: Vec<DueOrchestrationWaitSlot>,
}
impl DriveDueOrchestrationWaits {
    pub fn validate(
        &self,
        maximum_batch: u16,
        maximum_shards: u16,
    ) -> Result<(), ControllerStoreError> {
        validate_safety_scan_request(
            self.shard,
            self.after.as_ref(),
            ResourceKind::Job,
            self.limit,
            self.slots.len(),
            maximum_batch,
            maximum_shards,
        )?;
        let mut unique = BTreeSet::new();
        for slot in &self.slots {
            slot.validate()?;
            for identity in [
                &slot.mutations.receipt_id,
                &slot.mutations.node_event_id,
                &slot.mutations.node_outbox_id,
                &slot.mutations.job_event_id,
                &slot.mutations.job_outbox_id,
            ] {
                if !unique.insert(identity.to_string()) {
                    return Err(ControllerStoreError::InvalidInput(
                        "due wait mutation identities must be globally unique".to_owned(),
                    ));
                }
            }
        }
        Ok(())
    }
}
#[derive(Debug, Clone)]
pub struct DueOrchestrationRetrySlot {
    pub node_event_id: ResourceId,
    pub node_outbox_id: ResourceId,
    pub job_event_id: ResourceId,
    pub job_outbox_id: ResourceId,
}
impl DueOrchestrationRetrySlot {
    pub fn validate(&self) -> Result<(), ControllerStoreError> {
        if [&self.node_event_id, &self.job_event_id]
            .into_iter()
            .any(|identity| identity.kind() != ResourceKind::Event)
            || [&self.node_outbox_id, &self.job_outbox_id]
                .into_iter()
                .any(|identity| identity.kind() != ResourceKind::OutboxEvent)
        {
            return Err(ControllerStoreError::InvalidInput(
                "due retry mutation identity is invalid".to_owned(),
            ));
        }
        Ok(())
    }
}
#[derive(Debug, Clone)]
pub struct DriveDueOrchestrationRetries {
    pub shard: SafetyScanShard,
    pub after: Option<SafetyScanCursor>,
    pub limit: u16,
    pub slots: Vec<DueOrchestrationRetrySlot>,
}
impl DriveDueOrchestrationRetries {
    pub fn validate(
        &self,
        maximum_batch: u16,
        maximum_shards: u16,
    ) -> Result<(), ControllerStoreError> {
        validate_safety_scan_request(
            self.shard,
            self.after.as_ref(),
            ResourceKind::Job,
            self.limit,
            self.slots.len(),
            maximum_batch,
            maximum_shards,
        )?;
        let mut unique = BTreeSet::new();
        for slot in &self.slots {
            slot.validate()?;
            for identity in [
                &slot.node_event_id,
                &slot.node_outbox_id,
                &slot.job_event_id,
                &slot.job_outbox_id,
            ] {
                if !unique.insert(identity.to_string()) {
                    return Err(ControllerStoreError::InvalidInput(
                        "due retry mutation identities must be unique".to_owned(),
                    ));
                }
            }
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct PromotedOrchestrationRetry {
    pub job: JobRecord,
    pub node_id: String,
    pub node_version: i64,
}
#[derive(Debug, Clone)]
pub struct ExpiredOrchestrationRecoverySlot {
    pub quota_entry_ids: Vec<ResourceId>,
    pub run_event_id: ResourceId,
    pub run_outbox_id: ResourceId,
    pub node_event_id: ResourceId,
    pub node_outbox_id: ResourceId,
    pub job_event_id: ResourceId,
    pub job_outbox_id: ResourceId,
}
impl ExpiredOrchestrationRecoverySlot {
    pub fn validate(&self) -> Result<(), ControllerStoreError> {
        if self.quota_entry_ids.len() != MAX_ORCHESTRATION_QUOTA_LINES
            || self
                .quota_entry_ids
                .iter()
                .any(|identity| identity.kind() != ResourceKind::QuotaLedgerEntry)
            || [&self.run_event_id, &self.node_event_id, &self.job_event_id]
                .into_iter()
                .any(|identity| identity.kind() != ResourceKind::Event)
            || [
                &self.run_outbox_id,
                &self.node_outbox_id,
                &self.job_outbox_id,
            ]
            .into_iter()
            .any(|identity| identity.kind() != ResourceKind::OutboxEvent)
        {
            return Err(ControllerStoreError::InvalidInput(
                "expired orchestration recovery identity is invalid".to_owned(),
            ));
        }
        let mut unique = BTreeSet::new();
        for identity in self.quota_entry_ids.iter().chain([
            &self.run_event_id,
            &self.run_outbox_id,
            &self.node_event_id,
            &self.node_outbox_id,
            &self.job_event_id,
            &self.job_outbox_id,
        ]) {
            if !unique.insert(identity.to_string()) {
                return Err(ControllerStoreError::InvalidInput(
                    "expired recovery mutation identities must be unique".to_owned(),
                ));
            }
        }
        Ok(())
    }
}
#[derive(Debug, Clone)]
pub struct DriveExpiredOrchestrationJobs {
    pub shard: SafetyScanShard,
    pub after: Option<SafetyScanCursor>,
    pub limit: u16,
    pub slots: Vec<ExpiredOrchestrationRecoverySlot>,
}
impl DriveExpiredOrchestrationJobs {
    pub fn validate(
        &self,
        maximum_batch: u16,
        maximum_shards: u16,
    ) -> Result<(), ControllerStoreError> {
        validate_safety_scan_request(
            self.shard,
            self.after.as_ref(),
            ResourceKind::Job,
            self.limit,
            self.slots.len(),
            maximum_batch,
            maximum_shards,
        )?;
        let mut unique = BTreeSet::new();
        for slot in &self.slots {
            slot.validate()?;
            for identity in slot.quota_entry_ids.iter().chain([
                &slot.run_event_id,
                &slot.run_outbox_id,
                &slot.node_event_id,
                &slot.node_outbox_id,
                &slot.job_event_id,
                &slot.job_outbox_id,
            ]) {
                if !unique.insert(identity.to_string()) {
                    return Err(ControllerStoreError::InvalidInput(
                        "expired recovery identities must be globally unique".to_owned(),
                    ));
                }
            }
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct RecoveredOrchestrationJob {
    pub run: RunRecord,
    pub job: JobRecord,
    pub node_id: String,
    pub node_version: i64,
    pub settled_quota_account_ids: Vec<String>,
}
#[derive(Debug, Clone)]
pub struct OrchestrationConvergenceSlot {
    pub quota_entry_ids: Vec<ResourceId>,
    pub run_event_id: ResourceId,
    pub run_outbox_id: ResourceId,
    pub node_event_id: ResourceId,
    pub node_outbox_id: ResourceId,
    pub node_cancelling_event_id: ResourceId,
    pub node_cancelling_outbox_id: ResourceId,
    pub scope_closing_event_id: ResourceId,
    pub scope_closing_outbox_id: ResourceId,
    pub scope_terminal_event_id: ResourceId,
    pub scope_terminal_outbox_id: ResourceId,
    pub job_event_id: ResourceId,
    pub job_outbox_id: ResourceId,
}
impl OrchestrationConvergenceSlot {
    pub fn validate(&self) -> Result<(), ControllerStoreError> {
        if self.quota_entry_ids.len() != MAX_ORCHESTRATION_QUOTA_LINES
            || self
                .quota_entry_ids
                .iter()
                .any(|identity| identity.kind() != ResourceKind::QuotaLedgerEntry)
            || [
                &self.run_event_id,
                &self.node_event_id,
                &self.node_cancelling_event_id,
                &self.scope_closing_event_id,
                &self.scope_terminal_event_id,
                &self.job_event_id,
            ]
            .into_iter()
            .any(|identity| identity.kind() != ResourceKind::Event)
            || [
                &self.run_outbox_id,
                &self.node_outbox_id,
                &self.node_cancelling_outbox_id,
                &self.scope_closing_outbox_id,
                &self.scope_terminal_outbox_id,
                &self.job_outbox_id,
            ]
            .into_iter()
            .any(|identity| identity.kind() != ResourceKind::OutboxEvent)
        {
            return Err(ControllerStoreError::InvalidInput(
                "orchestration convergence identity is invalid".to_owned(),
            ));
        }
        let mut unique = BTreeSet::new();
        for identity in self.quota_entry_ids.iter().chain([
            &self.run_event_id,
            &self.run_outbox_id,
            &self.node_event_id,
            &self.node_outbox_id,
            &self.node_cancelling_event_id,
            &self.node_cancelling_outbox_id,
            &self.scope_closing_event_id,
            &self.scope_closing_outbox_id,
            &self.scope_terminal_event_id,
            &self.scope_terminal_outbox_id,
            &self.job_event_id,
            &self.job_outbox_id,
        ]) {
            if !unique.insert(identity.to_string()) {
                return Err(ControllerStoreError::InvalidInput(
                    "orchestration convergence identities must be unique".to_owned(),
                ));
            }
        }
        Ok(())
    }
}
#[derive(Debug, Clone)]
pub struct DriveOrchestrationConvergence {
    pub shard: SafetyScanShard,
    pub after: Option<SafetyScanCursor>,
    pub limit: u16,
    pub slots: Vec<OrchestrationConvergenceSlot>,
}
impl DriveOrchestrationConvergence {
    pub fn validate(
        &self,
        maximum_batch: u16,
        maximum_shards: u16,
    ) -> Result<(), ControllerStoreError> {
        validate_safety_scan_request(
            self.shard,
            self.after.as_ref(),
            ResourceKind::Run,
            self.limit,
            self.slots.len(),
            maximum_batch,
            maximum_shards,
        )?;
        let mut unique = BTreeSet::new();
        for slot in &self.slots {
            slot.validate()?;
            for identity in slot.quota_entry_ids.iter().chain([
                &slot.run_event_id,
                &slot.run_outbox_id,
                &slot.node_event_id,
                &slot.node_outbox_id,
                &slot.node_cancelling_event_id,
                &slot.node_cancelling_outbox_id,
                &slot.scope_closing_event_id,
                &slot.scope_closing_outbox_id,
                &slot.scope_terminal_event_id,
                &slot.scope_terminal_outbox_id,
                &slot.job_event_id,
                &slot.job_outbox_id,
            ]) {
                if !unique.insert(identity.to_string()) {
                    return Err(ControllerStoreError::InvalidInput(
                        "orchestration convergence identities must be globally unique".to_owned(),
                    ));
                }
            }
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct ConvergedOrchestrationRun {
    pub reason: OrchestrationConvergenceReason,
    pub run: RunRecord,
    pub step: OrchestrationConvergenceStep,
}
/// One bounded, committed convergence step; a non-ready Run never poisons its scan page.
#[derive(Debug, Clone, PartialEq)]
pub enum OrchestrationConvergenceStep {
    ControlObserved,
    Domain {
        owner_id: String,
        state: String,
    },
    JobNode {
        job: Box<JobRecord>,
        node_id: String,
        node_version: i64,
        settled_quota_account_ids: Vec<String>,
    },
    PendingNode {
        node_id: String,
        node_version: i64,
    },
    Task {
        task_id: String,
        task_version: i64,
    },
    Scope {
        scope_id: String,
        scope_version: i64,
    },
    RunTerminal,
    NotReady,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrchestrationRunTerminalState {
    Succeeded,
    Failed,
    TimedOut,
}
impl OrchestrationRunTerminalState {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::TimedOut => "timed_out",
        }
    }

    pub const fn job_state(self) -> JobState {
        match self {
            Self::Succeeded => JobState::Succeeded,
            Self::Failed => JobState::Failed,
            Self::TimedOut => JobState::TimedOut,
        }
    }

    pub const fn scope_state(self) -> &'static str {
        match self {
            Self::Succeeded => "succeeded",
            Self::Failed | Self::TimedOut => "failed",
        }
    }
}
#[derive(Debug, Clone)]
pub struct OrchestrationTerminalMutationIds {
    pub receipt_id: ResourceId,
    pub quota_entry_ids: Vec<ResourceId>,
    pub run_event_id: ResourceId,
    pub run_outbox_id: ResourceId,
    pub node_event_id: ResourceId,
    pub node_outbox_id: ResourceId,
    pub scope_closing_event_id: ResourceId,
    pub scope_closing_outbox_id: ResourceId,
    pub scope_terminal_event_id: ResourceId,
    pub scope_terminal_outbox_id: ResourceId,
    pub job_event_id: ResourceId,
    pub job_outbox_id: ResourceId,
}
impl OrchestrationTerminalMutationIds {
    pub fn validate(&self) -> Result<(), ControllerStoreError> {
        if self.receipt_id.kind() != ResourceKind::Receipt
            || self.quota_entry_ids.len() != MAX_ORCHESTRATION_QUOTA_LINES
            || self
                .quota_entry_ids
                .iter()
                .any(|identity| identity.kind() != ResourceKind::QuotaLedgerEntry)
            || [
                &self.run_event_id,
                &self.node_event_id,
                &self.scope_closing_event_id,
                &self.scope_terminal_event_id,
                &self.job_event_id,
            ]
            .into_iter()
            .any(|identity| identity.kind() != ResourceKind::Event)
            || [
                &self.run_outbox_id,
                &self.node_outbox_id,
                &self.scope_closing_outbox_id,
                &self.scope_terminal_outbox_id,
                &self.job_outbox_id,
            ]
            .into_iter()
            .any(|identity| identity.kind() != ResourceKind::OutboxEvent)
        {
            return Err(ControllerStoreError::InvalidInput(
                "orchestration terminal mutation identity is invalid".to_owned(),
            ));
        }
        let mut unique = BTreeSet::new();
        for identity in self.quota_entry_ids.iter().chain([
            &self.receipt_id,
            &self.run_event_id,
            &self.run_outbox_id,
            &self.node_event_id,
            &self.node_outbox_id,
            &self.scope_closing_event_id,
            &self.scope_closing_outbox_id,
            &self.scope_terminal_event_id,
            &self.scope_terminal_outbox_id,
            &self.job_event_id,
            &self.job_outbox_id,
        ]) {
            if !unique.insert(identity.to_string()) {
                return Err(ControllerStoreError::InvalidInput(
                    "orchestration terminal mutation identities must be unique".to_owned(),
                ));
            }
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct MaterializedTerminalValue {
    pub value_id: ResourceId,
    pub classification: DataClassification,
    pub schema_digest: Sha256Digest,
    pub content_digest: Sha256Digest,
    pub body: Value,
}
impl MaterializedTerminalValue {
    pub fn validate(&self) -> Result<(), ControllerStoreError> {
        if self.value_id.kind() != ResourceKind::RunValue {
            return Err(ControllerStoreError::InvalidInput(
                "terminal value ID has the wrong kind".to_owned(),
            ));
        }
        let value = ClosedJsonValue::build(self.schema_digest.clone(), self.body.clone())
            .map_err(|failure| ControllerStoreError::InvalidInput(failure.to_string()))?;
        if value.canonical_digest != self.content_digest {
            return Err(ControllerStoreError::InvalidInput(
                "terminal value content digest differs".to_owned(),
            ));
        }
        Ok(())
    }
}
#[derive(Debug, Clone)]
pub struct CommitPlanTerminal {
    pub fence: JobFence,
    pub plan: RuntimePlan,
    pub value: MaterializedTerminalValue,
    pub idempotency_key_digest: Sha256Digest,
    pub request_digest: Sha256Digest,
    pub receipt_expires_at: DateTime<Utc>,
    pub mutations: OrchestrationTerminalMutationIds,
}
impl CommitPlanTerminal {
    pub fn validate_at(
        &self,
        now: DateTime<Utc>,
        limits: PlanLimits,
    ) -> Result<(), ControllerStoreError> {
        self.fence.validate()?;
        self.plan
            .validate(limits)
            .map_err(|failure| ControllerStoreError::InvalidInput(failure.to_string()))?;
        self.value.validate()?;
        self.mutations.validate()?;
        if self.receipt_expires_at <= now {
            return Err(ControllerStoreError::InvalidInput(
                "Plan terminal receipt has expired".to_owned(),
            ));
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct CompletedOrchestrationRun {
    pub run: RunRecord,
    pub job: JobRecord,
    pub node_id: String,
    pub scope_id: String,
    pub node_version: i64,
    pub scope_version: i64,
    pub settled_quota_account_ids: Vec<String>,
}
pub fn runtime_human_task_definition(definition: &RuntimeHumanTaskDefinition) -> TaskDefinition {
    match definition {
        RuntimeHumanTaskDefinition::Interaction {
            eligibility_rule: _,
            interaction_kind,
            eligible_principal_rule_digest,
            safe_prompt_key,
        } => TaskDefinition::Interaction {
            interaction_kind: *interaction_kind,
            eligible_principal_rule_digest: eligible_principal_rule_digest.clone(),
            safe_prompt_key: safe_prompt_key.clone(),
        },
        RuntimeHumanTaskDefinition::HumanWork {
            eligibility_rule: _,
            eligible_principal_rule_digest,
            safe_prompt_key,
        } => TaskDefinition::HumanWork {
            eligible_principal_rule_digest: eligible_principal_rule_digest.clone(),
            safe_prompt_key: safe_prompt_key.clone(),
        },
    }
}
pub fn valid_orchestration_signal_key(value: &str) -> bool {
    let bytes = value.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= 64
        && bytes[0].is_ascii_lowercase()
        && bytes
            .iter()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || *byte == b'_')
}

impl From<insight_platform_plan::PlanError> for ControllerStoreError {
    fn from(error: insight_platform_plan::PlanError) -> Self {
        Self::InvalidInput(error.to_string())
    }
}
impl From<crate::OrchestratorError> for ControllerStoreError {
    fn from(error: crate::OrchestratorError) -> Self {
        Self::InvalidInput(error.to_string())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct RunResultRecord {
    pub run_id: ResourceId,
    pub value_id: ResourceId,
    pub classification: DataClassification,
    pub schema_digest: Sha256Digest,
    pub content_digest: Sha256Digest,
    pub value: ValueRef,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RunValueMetadataRecord {
    pub node_id: Option<ResourceId>,
    pub run_id: ResourceId,
    pub value_id: ResourceId,
    pub classification: DataClassification,
    pub schema_digest: Sha256Digest,
    pub content_digest: Sha256Digest,
    pub storage_kind: RunValueStorageKind,
}

#[derive(Debug, Clone)]
pub struct RunValuesQuery {
    pub tenant_id: ResourceId,
    pub principal_id: ResourceId,
    pub principal_kind: insight_platform_contracts::PrincipalKind,
    pub run_id: ResourceId,
    pub node_id: Option<ResourceId>,
    pub page_size: u16,
    pub snapshot_at: Option<DateTime<Utc>>,
    pub boundary: Option<(DateTime<Utc>, ResourceId)>,
}
#[derive(Debug, Clone)]
pub struct RunValuesPage {
    pub items: Vec<RunValueMetadataRecord>,
    pub snapshot_at: DateTime<Utc>,
    pub next_boundary: Option<(DateTime<Utc>, ResourceId)>,
}

/// Read-only ancestry projection of existing Run and NodeExecution authority.
/// It is not an evaluation record and never grants access to Run value bodies.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublicChildRunLinkRecord {
    pub schema_version: u32,
    pub parent_run_id: ResourceId,
    pub parent_node_id: ResourceId,
    pub parent_plan_node_key: insight_platform_plan::PlanNodeKey,
    pub child_run_id: ResourceId,
    pub child_agent_deployment: insight_platform_contracts::ExactDeploymentRef,
    pub child_state: RunState,
    pub child_version: u64,
    pub input_value_id: ResourceId,
    pub output_value_id: Option<ResourceId>,
    pub created_at: DateTime<Utc>,
}
impl PublicChildRunLinkRecord {
    pub fn validate_for(
        &self,
        parent_run_id: &ResourceId,
        parent_node_id: Option<&ResourceId>,
    ) -> Result<(), ControllerStoreError> {
        if self.schema_version != 1
            || self.parent_run_id != *parent_run_id
            || parent_run_id.kind() != ResourceKind::Run
            || self.parent_node_id.kind() != ResourceKind::NodeExecution
            || parent_node_id.is_some_and(|id| id != &self.parent_node_id)
            || self.child_run_id.kind() != ResourceKind::Run
            || self.child_run_id == self.parent_run_id
            || self.child_agent_deployment.resource_kind != ResourceKind::AgentDeployment
            || self.child_agent_deployment.validate().is_err()
            || self.child_version == 0
            || self.child_version > insight_platform_contracts::MAX_SAFE_JSON_INTEGER
            || self.input_value_id.kind() != ResourceKind::RunValue
            || self
                .output_value_id
                .as_ref()
                .is_some_and(|id| id.kind() != ResourceKind::RunValue)
        {
            return Err(ControllerStoreError::InvalidInput(
                "invalid child Run link projection".into(),
            ));
        }
        Ok(())
    }
}
#[derive(Debug, Clone)]
pub struct ChildRunLinksQuery {
    pub tenant_id: ResourceId,
    pub principal_id: ResourceId,
    pub principal_kind: PrincipalKind,
    pub parent_run_id: ResourceId,
    pub parent_node_id: Option<ResourceId>,
    pub page_size: u16,
    pub snapshot_at: Option<DateTime<Utc>>,
    pub boundary: Option<(DateTime<Utc>, ResourceId)>,
}
#[derive(Debug, Clone)]
pub struct ChildRunLinksPage {
    pub items: Vec<PublicChildRunLinkRecord>,
    pub snapshot_at: DateTime<Utc>,
    pub next_boundary: Option<(DateTime<Utc>, ResourceId)>,
}
