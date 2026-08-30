use crate::artifact_repository::{
    load_internal_artifact_admission_authority, reserve_internal_artifact_staging_quota,
};
use crate::repository::{
    append_command_event, append_scheduler_event, append_scheduler_event_with_trace,
    claim_command_receipt, database_timestamp, decode_deployment_closure, decode_typed_payload,
    decode_versioned_payload, job_from_row, job_projection, load_deployment,
    load_job_for_update_by_text, load_resource, load_resource_for_update, load_task_for_update,
    payload_from_row, require_ready_run_artifact, require_tenant_permission, task_projection,
    terminalize_command_receipt, validate_deployment_closure_exists,
    validate_exact_secret_bindings_at_creation, PgRegistryTransaction, PgRepository,
    RepositoryError, ResourceRecord, TaskRecord, TypedPayload,
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use insight_platform_artifacts::{
    ArtifactAwaitingStageSnapshot, ArtifactJobPayload, ArtifactMetadataSnapshot,
    ArtifactReferenceSnapshot, ArtifactScanDisposition,
};
use insight_platform_context::{
    AcceptedContextSubscriptionRefresh, AdmitContextSubscriptionRefresh,
    ContextSubscriptionAdmissionAuthority, ContextSubscriptionAdmissionError,
    ContextSubscriptionExecutionError, ContextSubscriptionRefreshAttempt,
    ContextSubscriptionRefreshCause, ContextSubscriptionRefreshJobPayload,
    ContextSubscriptionRefreshResponse,
};
use insight_platform_contracts::{
    checked_in_hard_limit_profile, ArtifactPurpose, ArtifactReferenceKind, CommandOutcome,
    ContextBackendBinding, ContextBackendContract, DeploymentClosure, EntityLifecycle,
    ExactDeploymentRef, JobState, McpAuthorizationPrincipalKind, McpAuthorizationState,
    McpDeploymentClosure, McpProtocolPolicyDocument, McpServerExecutionContract, McpSessionState,
    Permission, PolicyKind, PrincipalIdentityState, PrincipalKind, QuotaDimension,
    RegistryResourceKind, ResourceDocument, ResourceId, ResourceKind, Sha256Digest, WorkClass,
};
use insight_platform_jobs::{
    decide_claim as decide_job_claim, decide_expired_lease as decide_expired_job_lease,
    decide_owner_terminal as decide_job_owner_terminal, decide_retry as decide_job_retry,
    decide_start as decide_job_start, decide_terminal as decide_job_terminal,
    decide_wait as decide_job_wait, decide_wake as decide_job_wake, JobFence, JobLease,
    JobOwnerRef, JobProjection, LeasePolicy, WakeContract, WakeKind, WakeSource,
};
use insight_platform_mcp_host::{
    AuthenticatedMcpOAuthState, AuthorizedMcpOAuthPkceCleanup, BeginMcpOAuthAuthorization,
    CancelMcpDiscoveryOperation, CompleteMcpOAuthCallback, CompleteMcpSubscriptionReconcile,
    CompleteMcpSubscriptionRefresh, ContextSubscriptionRefreshResolver,
    CreateMcpAuthorizationBinding, CreateMcpDiscoveryOperation, DriveExpiredMcpOAuthTasks,
    DueMcpSubscriptionReconcile, DueMcpSubscriptionRecovery, ExpiredMcpDiscoveryJobObservation,
    FinalizeMcpDiscovery, McpAuthorizationBindingRecord, McpAuthorizationContext,
    McpAuthorizationReplacement, McpDiscoveryAdmission, McpDiscoveryAttemptResolution,
    McpDiscoveryContractQuery, McpDiscoveryExecutionContract,
    McpDiscoveryExecutionContractResolver, McpDiscoveryJobPayload, McpDiscoveryOperationPayload,
    McpDiscoveryOperationRecord, McpDiscoveryOperationState, McpDiscoveryPersistenceError,
    McpDiscoveryRequest, McpDiscoveryResultBinding, McpDiscoveryResultStore,
    McpDiscoverySnapshotRecord, McpExecutionContractQuery, McpExecutionContractResolutionError,
    McpExecutionContractResolver, McpHostExecutionContract, McpJobPayload,
    McpNotificationApplyDisposition, McpNotificationCommit, McpNotificationCommitAuthority,
    McpNotificationCommitOutcome, McpNotificationPersistenceError, McpNotificationReceipt,
    McpOAuthAuthorizationStartAuthority, McpOAuthAuthorizationStartAuthorityError,
    McpOAuthAuthorizationStartCommitDisposition, McpOAuthAuthorizationStartCommitOutcome,
    McpOAuthAuthorizationStartIntent, McpOAuthCallbackAuthority, McpOAuthCallbackAuthorityError,
    McpOAuthCallbackCommitDisposition, McpOAuthCallbackCommitOutcome, McpOAuthCallbackResolution,
    McpOAuthExchangeContract, McpOAuthPkceCleanupAuthority, McpOAuthPkceCleanupAuthorityError,
    McpOAuthPkceCleanupCause, McpOAuthPkceCleanupHint, McpOAuthPkceCleanupRequest,
    McpResourceSubscriptionBinding, McpSessionBindingKey, McpSessionRecord,
    McpSubscriptionAuthority, McpSubscriptionContractQuery, McpSubscriptionExecutionResolver,
    McpSubscriptionJobPayload, McpSubscriptionPayload, McpSubscriptionPersistenceError,
    McpSubscriptionReconcileAuthority, McpSubscriptionReconcileScan, McpSubscriptionRecord,
    McpSubscriptionRecoveryAuthority, McpSubscriptionRecoveryCause, McpSubscriptionRecoveryScan,
    McpSubscriptionState, McpSubscriptionTransportTerminationAuthority, McpSubscriptionWorkerAudit,
    NewMcpAuthorizationBinding, NewMcpDiscoveryAdmission, NewMcpDiscoveryArtifactPolicyClosure,
    NewMcpDiscoveryExecutionContract, NewMcpDiscoverySnapshotRecord, NewMcpHostExecutionContract,
    NewMcpResourceSubscriptionBinding, ParkMcpDiscoveryVerification,
    ReactivateMcpAuthorizationBinding, RecoverDueMcpSubscription, RecoverExpiredMcpDiscoveryJob,
    ReportMcpSubscriptionSessionLoss, ReportMcpSubscriptionTransportTermination,
    ResolveMcpDiscoveryAttempt, ResolvedContextSubscriptionRefresh, ResolvedMcpDiscoveryExecution,
    ResolvedMcpOAuthAuthorizationStart, ResolvedMcpSubscriptionExecution,
    SaveMcpSubscriptionSession, TransitionMcpAuthorizationBinding, WakeMcpSubscriptionReconcile,
    MAX_MCP_SUBSCRIPTION_RECONCILE_SCAN, MCP_OAUTH_PKCE_SECRET_PURPOSE,
    MIN_MCP_SUBSCRIPTION_RECONCILE_IDLE_MILLISECONDS,
};
use insight_platform_tasks::{
    decide_resolution as decide_task_resolution, ResolveTask, TaskDefinition, TaskPayload,
    TaskState,
};
use sqlx::{Acquire, Postgres, Row, Transaction};
use std::collections::BTreeSet;
use uuid::Uuid;

const MCP_AUTHORIZATION_RESOURCE_KIND: &str = "mcp_authorization_binding";
const MCP_DISCOVERY_RESOURCE_KIND: &str = "mcp_discovery_snapshot";
pub const CONTEXT_SUBSCRIPTION_REFRESH_MAX_BATCH: u16 = 64;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimableContextSubscriptionRefreshJob {
    pub tenant_id: ResourceId,
    pub job_id: ResourceId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContextSubscriptionRefreshClaimSlot {
    pub tenant_id: ResourceId,
    pub job_id: ResourceId,
    pub lease_token_digest: Sha256Digest,
    pub quota_reservation_id: ResourceId,
    pub quota_reserve_entry_id: ResourceId,
    pub event_id: ResourceId,
    pub outbox_id: ResourceId,
}

#[derive(Debug, Clone)]
pub struct ClaimContextSubscriptionRefreshJobs {
    pub worker_process_generation_id: ResourceId,
    pub worker_manifest_digest: Sha256Digest,
    pub slots: Vec<ContextSubscriptionRefreshClaimSlot>,
    pub lease_policy: LeasePolicy,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ClaimedContextSubscriptionRefresh {
    pub job: crate::repository::JobRecord,
    pub attempt: ContextSubscriptionRefreshAttempt,
    pub quota_account_id: ResourceId,
}

#[derive(Debug, Clone)]
pub struct CommitContextSubscriptionRefresh {
    pub attempt: ContextSubscriptionRefreshAttempt,
    pub response: ContextSubscriptionRefreshResponse,
    pub quota_settlement_entry_id: ResourceId,
    pub event_id: ResourceId,
    pub outbox_id: ResourceId,
}

#[derive(Debug, Clone)]
pub struct ContextSubscriptionRefreshRecoverySlot {
    pub quota_settlement_entry_id: ResourceId,
    pub event_id: ResourceId,
    pub outbox_id: ResourceId,
}

#[derive(Debug, Clone)]
pub struct DriveExpiredContextSubscriptionRefreshJobs {
    pub shard_index: u16,
    pub shard_count: u16,
    pub limit: u16,
    pub retry_backoff_milliseconds: u64,
    pub slots: Vec<ContextSubscriptionRefreshRecoverySlot>,
}

impl ClaimContextSubscriptionRefreshJobs {
    fn validate(&self) -> Result<(), RepositoryError> {
        if self.worker_process_generation_id.kind() != ResourceKind::WorkerProcessGeneration
            || self.slots.is_empty()
            || self.slots.len() > usize::from(CONTEXT_SUBSCRIPTION_REFRESH_MAX_BATCH)
            || self.lease_policy.requested_milliseconds == 0
            || self.lease_policy.requested_milliseconds
                > self.lease_policy.hard_maximum_milliseconds
        {
            return Err(RepositoryError::InvalidInput(
                "Context subscription refresh claim is invalid".to_owned(),
            ));
        }
        let mut ids = BTreeSet::new();
        for slot in &self.slots {
            if slot.tenant_id.kind() != ResourceKind::Tenant
                || slot.job_id.kind() != ResourceKind::Job
                || slot.quota_reservation_id.kind() != ResourceKind::UsageReservation
                || slot.quota_reserve_entry_id.kind() != ResourceKind::QuotaLedgerEntry
                || slot.event_id.kind() != ResourceKind::Event
                || slot.outbox_id.kind() != ResourceKind::OutboxEvent
            {
                return Err(RepositoryError::InvalidInput(
                    "Context subscription refresh claim identity is invalid".to_owned(),
                ));
            }
            for id in [
                &slot.quota_reservation_id,
                &slot.quota_reserve_entry_id,
                &slot.event_id,
                &slot.outbox_id,
            ] {
                if !ids.insert(id.to_string()) {
                    return Err(RepositoryError::InvalidInput(
                        "Context subscription refresh claim identities must be unique".to_owned(),
                    ));
                }
            }
        }
        Ok(())
    }
}

impl CommitContextSubscriptionRefresh {
    fn validate_at(&self, now: DateTime<Utc>) -> Result<(), RepositoryError> {
        self.attempt
            .validate_at(now)
            .map_err(invalid_context_subscription_execution)?;
        self.response
            .validate_for(&self.attempt, now)
            .map_err(invalid_context_subscription_execution)?;
        if self.quota_settlement_entry_id.kind() != ResourceKind::QuotaLedgerEntry
            || self.event_id.kind() != ResourceKind::Event
            || self.outbox_id.kind() != ResourceKind::OutboxEvent
        {
            return Err(RepositoryError::InvalidInput(
                "Context subscription refresh outcome identity is invalid".to_owned(),
            ));
        }
        Ok(())
    }
}

impl DriveExpiredContextSubscriptionRefreshJobs {
    fn validate(&self) -> Result<(), RepositoryError> {
        if self.shard_count == 0
            || self.shard_count > 256
            || self.shard_index >= self.shard_count
            || self.limit == 0
            || self.limit > CONTEXT_SUBSCRIPTION_REFRESH_MAX_BATCH
            || self.slots.len() != usize::from(self.limit)
            || self.retry_backoff_milliseconds == 0
            || self.retry_backoff_milliseconds > 3_600_000
        {
            return Err(RepositoryError::InvalidInput(
                "Context subscription refresh recovery request is invalid".to_owned(),
            ));
        }
        let mut ids = BTreeSet::new();
        for slot in &self.slots {
            if slot.quota_settlement_entry_id.kind() != ResourceKind::QuotaLedgerEntry
                || slot.event_id.kind() != ResourceKind::Event
                || slot.outbox_id.kind() != ResourceKind::OutboxEvent
                || !ids.insert(slot.quota_settlement_entry_id.to_string())
                || !ids.insert(slot.event_id.to_string())
                || !ids.insert(slot.outbox_id.to_string())
            {
                return Err(RepositoryError::InvalidInput(
                    "Context subscription refresh recovery identity is invalid".to_owned(),
                ));
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct McpOAuthCallbackRecord {
    pub task: TaskRecord,
    pub authorization: Option<McpAuthorizationBindingRecord>,
}

impl PgRegistryTransaction {
    pub async fn create_mcp_authorization_binding(
        &mut self,
        command: CreateMcpAuthorizationBinding,
    ) -> Result<CommandOutcome<McpAuthorizationBindingRecord>, RepositoryError> {
        command
            .validate_at(Utc::now())
            .map_err(invalid_authorization)?;
        let mut transaction = self.transaction.begin().await?;
        let database_now = database_now(&mut transaction).await?;
        command
            .validate_at(database_now)
            .map_err(invalid_authorization)?;
        let binding_id = command.input.authorization_binding_id.clone();
        if claim_command_receipt(
            &mut transaction,
            &command.audit,
            MCP_AUTHORIZATION_RESOURCE_KIND,
            &binding_id.to_string(),
            "mcp.authorization.create",
        )
        .await?
        {
            let record = load_mcp_authorization_binding(
                &mut transaction,
                &command.audit.tenant_id,
                &binding_id,
                false,
            )
            .await?;
            transaction.commit().await?;
            return Ok(CommandOutcome::Replayed(record));
        }
        require_tenant_permission(&mut transaction, &command.audit, Permission::McpWrite).await?;
        validate_principal_binding(
            &mut transaction,
            &command.input.tenant_id,
            &command.input.principal_id,
            command.input.principal_identity_kind,
            command.input.principal_binding_generation,
        )
        .await?;
        validate_mcp_authorization_dependencies(
            &mut transaction,
            &command.input.tenant_id,
            &command.input.mcp_deployment,
            &command.input.audience_identity_digest,
            &command.input.token_secret_binding,
        )
        .await?;
        lock_authorization_identity(
            &mut transaction,
            &command.input.tenant_id,
            &command.input.mcp_deployment.deployment_id,
            &command.input.principal_id,
        )
        .await?;
        reject_duplicate_live_authorization(
            &mut transaction,
            &command.input.tenant_id,
            &command.input.mcp_deployment.deployment_id,
            &command.input.principal_id,
        )
        .await?;
        let record = McpAuthorizationBindingRecord::create(command.input, database_now)
            .map_err(invalid_authorization)?;
        let payload = TypedPayload::from_versioned(1, &record, 262_144)?;
        sqlx::query(
            r#"
            INSERT INTO insight_platform.resources (
                tenant_id, resource_id, resource_kind, lifecycle_state, gate_state,
                payload_schema_version, payload, payload_digest
            ) VALUES ($1, $2, $3, 'active', 'enabled', $4, $5, $6)
            "#,
        )
        .bind(record.tenant_id.to_string())
        .bind(record.authorization_binding_id.to_string())
        .bind(MCP_AUTHORIZATION_RESOURCE_KIND)
        .bind(payload.schema_version)
        .bind(&payload.value)
        .bind(&payload.digest)
        .execute(&mut *transaction)
        .await?;
        append_authorization_event(
            &mut transaction,
            &command.audit,
            &record,
            "mcp.authorization_created",
        )
        .await?;
        terminalize_command_receipt(
            &mut transaction,
            &command.audit,
            &binding_id.to_string(),
            "created",
        )
        .await?;
        transaction.commit().await?;
        Ok(CommandOutcome::Applied(record))
    }

    pub async fn transition_mcp_authorization_binding(
        &mut self,
        command: TransitionMcpAuthorizationBinding,
    ) -> Result<CommandOutcome<McpAuthorizationBindingRecord>, RepositoryError> {
        command
            .validate_at(Utc::now())
            .map_err(invalid_authorization)?;
        let mut transaction = self.transaction.begin().await?;
        let database_now = database_now(&mut transaction).await?;
        command
            .validate_at(database_now)
            .map_err(invalid_authorization)?;
        let operation = format!("mcp.authorization.{}", command.target.as_str());
        if claim_command_receipt(
            &mut transaction,
            &command.audit,
            MCP_AUTHORIZATION_RESOURCE_KIND,
            &command.authorization_binding_id.to_string(),
            &operation,
        )
        .await?
        {
            let record = load_mcp_authorization_binding(
                &mut transaction,
                &command.audit.tenant_id,
                &command.authorization_binding_id,
                false,
            )
            .await?;
            transaction.commit().await?;
            return Ok(CommandOutcome::Replayed(record));
        }
        require_tenant_permission(&mut transaction, &command.audit, Permission::McpWrite).await?;
        let current = load_mcp_authorization_binding(
            &mut transaction,
            &command.audit.tenant_id,
            &command.authorization_binding_id,
            true,
        )
        .await?;
        let next = current
            .transition(command.expected_version, command.target, database_now)
            .map_err(invalid_authorization)?;
        update_mcp_authorization(&mut transaction, &current, &next, database_now).await?;
        let event_type = match command.target {
            McpAuthorizationState::ReauthRequired => "mcp.authorization_reauth_required",
            McpAuthorizationState::Revoked => "mcp.authorization_revoked",
            McpAuthorizationState::Expired => "mcp.authorization_expired",
            McpAuthorizationState::Active => {
                return Err(RepositoryError::InvalidInput(
                    "MCP authorization activation requires reauthorization".to_owned(),
                ));
            }
        };
        append_authorization_event(&mut transaction, &command.audit, &next, event_type).await?;
        terminalize_command_receipt(
            &mut transaction,
            &command.audit,
            &next.authorization_binding_id.to_string(),
            next.state.as_str(),
        )
        .await?;
        transaction.commit().await?;
        Ok(CommandOutcome::Applied(next))
    }

    pub async fn reactivate_mcp_authorization_binding(
        &mut self,
        command: ReactivateMcpAuthorizationBinding,
    ) -> Result<CommandOutcome<McpAuthorizationBindingRecord>, RepositoryError> {
        command
            .validate_at(Utc::now())
            .map_err(invalid_authorization)?;
        let mut transaction = self.transaction.begin().await?;
        let database_now = database_now(&mut transaction).await?;
        command
            .validate_at(database_now)
            .map_err(invalid_authorization)?;
        if claim_command_receipt(
            &mut transaction,
            &command.audit,
            MCP_AUTHORIZATION_RESOURCE_KIND,
            &command.authorization_binding_id.to_string(),
            "mcp.authorization.reactivate",
        )
        .await?
        {
            let record = load_mcp_authorization_binding(
                &mut transaction,
                &command.audit.tenant_id,
                &command.authorization_binding_id,
                false,
            )
            .await?;
            transaction.commit().await?;
            return Ok(CommandOutcome::Replayed(record));
        }
        require_tenant_permission(&mut transaction, &command.audit, Permission::McpWrite).await?;
        let current = load_mcp_authorization_binding(
            &mut transaction,
            &command.audit.tenant_id,
            &command.authorization_binding_id,
            true,
        )
        .await?;
        validate_principal_binding(
            &mut transaction,
            &current.tenant_id,
            &current.principal_id,
            current.principal_identity_kind,
            command.replacement.principal_binding_generation,
        )
        .await?;
        validate_mcp_authorization_dependencies(
            &mut transaction,
            &current.tenant_id,
            &current.mcp_deployment,
            &current.audience_identity_digest,
            &command.replacement.token_secret_binding,
        )
        .await?;
        let next = current
            .reactivate(command.expected_version, command.replacement, database_now)
            .map_err(invalid_authorization)?;
        update_mcp_authorization(&mut transaction, &current, &next, database_now).await?;
        append_authorization_event(
            &mut transaction,
            &command.audit,
            &next,
            "mcp.authorization_reactivated",
        )
        .await?;
        terminalize_command_receipt(
            &mut transaction,
            &command.audit,
            &next.authorization_binding_id.to_string(),
            "active",
        )
        .await?;
        transaction.commit().await?;
        Ok(CommandOutcome::Applied(next))
    }

    pub async fn begin_mcp_oauth_authorization(
        &mut self,
        mut command: BeginMcpOAuthAuthorization,
    ) -> Result<CommandOutcome<TaskRecord>, RepositoryError> {
        command.deadline = database_timestamp(command.deadline);
        command
            .validate_at(Utc::now())
            .map_err(invalid_authorization)?;
        let mut transaction = self.transaction.begin().await?;
        let database_now = database_now(&mut transaction).await?;
        command
            .validate_at(database_now)
            .map_err(invalid_authorization)?;
        if claim_command_receipt(
            &mut transaction,
            &command.audit,
            "mcp_oauth_task",
            &command.task_id.to_string(),
            "mcp.oauth.begin",
        )
        .await?
        {
            let task =
                load_task_for_update(&mut transaction, &command.audit.tenant_id, &command.task_id)
                    .await?;
            require_mcp_oauth_task_matches_begin(&task, &command)?;
            transaction.commit().await?;
            return Ok(CommandOutcome::Replayed(task));
        }
        let principal =
            require_tenant_permission(&mut transaction, &command.audit, Permission::McpWrite)
                .await?;
        let resolved = load_mcp_oauth_start_dependencies(
            &mut transaction,
            &command.audit.tenant_id,
            &command.mcp_deployment,
            &command.callback_binding_digest,
            &command.requested_scopes,
        )
        .await?;
        if resolved.token_credential_purpose == command.pkce_secret_binding.purpose
            || resolved.auth_profile.pkce_secret_provider_id
                != command.pkce_secret_binding.provider_id
        {
            return Err(RepositoryError::Conflict("MCP OAuth credential purpose"));
        }
        validate_exact_secret_bindings_at_creation(
            &mut transaction,
            &command.audit.tenant_id,
            std::slice::from_ref(&command.pkce_secret_binding),
        )
        .await?;
        lock_authorization_identity(
            &mut transaction,
            &command.audit.tenant_id,
            &command.mcp_deployment.deployment_id,
            &command.audit.principal_id,
        )
        .await?;
        match command.reauthorization {
            Some(fence) => {
                let current = load_mcp_authorization_binding(
                    &mut transaction,
                    &command.audit.tenant_id,
                    &command.authorization_binding_id,
                    true,
                )
                .await?;
                if current.state != McpAuthorizationState::ReauthRequired
                    || current.version != fence.authorization_version
                    || current.generation != fence.authorization_generation
                    || current.mcp_deployment != command.mcp_deployment
                    || current.principal_id != command.audit.principal_id
                    || current.principal_identity_kind != command.audit.principal_kind
                    || current.audience_identity_digest != resolved.audience_identity_digest
                {
                    return Err(RepositoryError::Conflict("MCP OAuth reauthorization fence"));
                }
            }
            None => {
                reject_duplicate_live_authorization(
                    &mut transaction,
                    &command.audit.tenant_id,
                    &command.mcp_deployment.deployment_id,
                    &command.audit.principal_id,
                )
                .await?;
            }
        }
        reject_duplicate_pending_oauth_task(
            &mut transaction,
            &command.audit.tenant_id,
            &command.mcp_deployment.deployment_id,
            &command.audit.principal_id,
        )
        .await?;
        let oauth_binding = command
            .task_binding(
                &principal,
                resolved.audience_identity_digest,
                resolved.token_credential_purpose,
                resolved.auth_policy,
            )
            .map_err(invalid_authorization)?;
        let task_payload = TaskPayload {
            definition: TaskDefinition::McpOAuthAuthorization {
                binding: Box::new(oauth_binding),
                safe_prompt_key: command.safe_prompt_key.clone(),
            },
            created_by: principal,
            resolution: None,
        };
        task_payload
            .validate()
            .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;
        insert_mcp_oauth_task(&mut transaction, &command, &task_payload, database_now).await?;
        append_command_event(
            &mut transaction,
            &command.audit,
            "mcp_oauth_task",
            &command.task_id.to_string(),
            1,
            "mcp.oauth_authorization_started",
            &TypedPayload::new(
                1,
                &serde_json::json!({
                    "authorization_binding_id": command.authorization_binding_id,
                    "mcp_deployment": command.mcp_deployment,
                    "requested_scope_digest": insight_platform_contracts::canonical_digest(
                        &serde_json::json!({"scopes": command.requested_scopes})
                    ).map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?,
                    "task_id": command.task_id,
                }),
            )?,
        )
        .await?;
        terminalize_command_receipt(
            &mut transaction,
            &command.audit,
            &command.task_id.to_string(),
            "pending",
        )
        .await?;
        let task =
            load_task_for_update(&mut transaction, &command.audit.tenant_id, &command.task_id)
                .await?;
        transaction.commit().await?;
        Ok(CommandOutcome::Applied(task))
    }

    pub async fn complete_mcp_oauth_callback(
        &mut self,
        command: CompleteMcpOAuthCallback,
    ) -> Result<CommandOutcome<McpOAuthCallbackRecord>, RepositoryError> {
        command
            .audit
            .validate_at(Utc::now())
            .map_err(invalid_authorization)?;
        let mut transaction = self.transaction.begin().await?;
        let database_now = database_now(&mut transaction).await?;
        command
            .audit
            .validate_at(database_now)
            .map_err(invalid_authorization)?;
        let receipt_payload = mcp_oauth_callback_receipt_payload(&command)?;
        if claim_mcp_oauth_callback_receipt(&mut transaction, &command, &receipt_payload).await? {
            let record = load_mcp_oauth_callback_record(&mut transaction, &command).await?;
            transaction.commit().await?;
            return Ok(CommandOutcome::Replayed(record));
        }
        let current_task =
            load_task_for_update(&mut transaction, &command.audit.tenant_id, &command.task_id)
                .await?;
        let current_projection = task_projection(&current_task)?;
        let TaskDefinition::McpOAuthAuthorization { binding, .. } =
            &current_projection.payload.definition
        else {
            return Err(RepositoryError::CorruptRow(
                "MCP OAuth Task has the wrong definition".to_owned(),
            ));
        };
        command
            .validate_for_binding(
                &current_projection.tenant_id,
                &current_projection.task_id,
                binding,
                database_now,
            )
            .map_err(invalid_authorization)?;
        if current_projection.state != TaskState::Pending
            || current_projection.generation != command.expected_task_generation
            || current_projection.version != command.expected_task_version
            || current_projection.deadline <= database_now
        {
            terminalize_mcp_oauth_callback_receipt(
                &mut transaction,
                &command,
                "rejected_stale",
                &command.task_id,
            )
            .await?;
            let record = load_mcp_oauth_callback_record(&mut transaction, &command).await?;
            transaction.commit().await?;
            return Ok(CommandOutcome::Applied(record));
        }
        let mcp_deployment = &binding.mcp_deployment;
        let principal_binding_generation = binding.principal_binding_generation;
        let audience_identity_digest = &binding.audience_identity_digest;
        let expected_authorization_generation = binding.expected_authorization_generation;
        let expected_authorization_version = binding.expected_authorization_version;
        let authorization = match &command.resolution {
            McpOAuthCallbackResolution::Authorized(grant) => {
                validate_principal_binding(
                    &mut transaction,
                    &command.audit.tenant_id,
                    &current_projection.payload.created_by.principal_id,
                    current_projection.payload.created_by.principal_kind,
                    principal_binding_generation,
                )
                .await?;
                validate_mcp_authorization_dependencies(
                    &mut transaction,
                    &command.audit.tenant_id,
                    mcp_deployment,
                    audience_identity_digest,
                    &grant.token_secret_binding,
                )
                .await?;
                lock_authorization_identity(
                    &mut transaction,
                    &command.audit.tenant_id,
                    &mcp_deployment.deployment_id,
                    &current_projection.payload.created_by.principal_id,
                )
                .await?;
                let next = match (
                    expected_authorization_generation,
                    expected_authorization_version,
                ) {
                    (None, None) => {
                        reject_duplicate_live_authorization(
                            &mut transaction,
                            &command.audit.tenant_id,
                            &mcp_deployment.deployment_id,
                            &current_projection.payload.created_by.principal_id,
                        )
                        .await?;
                        let record = McpAuthorizationBindingRecord::create(
                            NewMcpAuthorizationBinding {
                                tenant_id: command.audit.tenant_id.clone(),
                                authorization_binding_id: command.authorization_binding_id.clone(),
                                mcp_deployment: mcp_deployment.clone(),
                                principal_kind: McpAuthorizationPrincipalKind::PerUser,
                                principal_id: current_projection
                                    .payload
                                    .created_by
                                    .principal_id
                                    .clone(),
                                principal_identity_kind: current_projection
                                    .payload
                                    .created_by
                                    .principal_kind,
                                principal_binding_generation,
                                audience_identity_digest: audience_identity_digest.clone(),
                                granted_scopes: grant.granted_scopes.clone(),
                                token_secret_binding: grant.token_secret_binding.clone(),
                                expires_at: grant.expires_at,
                            },
                            database_now,
                        )
                        .map_err(invalid_authorization)?;
                        insert_mcp_authorization_record(&mut transaction, &record, database_now)
                            .await?;
                        record
                    }
                    (Some(expected_generation), Some(expected_version)) => {
                        let current = load_mcp_authorization_binding(
                            &mut transaction,
                            &command.audit.tenant_id,
                            &command.authorization_binding_id,
                            true,
                        )
                        .await?;
                        if current.generation != expected_generation
                            || current.version != expected_version
                            || current.mcp_deployment != *mcp_deployment
                            || current.principal_id
                                != current_projection.payload.created_by.principal_id
                            || current.audience_identity_digest != *audience_identity_digest
                        {
                            return Err(RepositoryError::Conflict(
                                "MCP OAuth reauthorization first-winner",
                            ));
                        }
                        let next = current
                            .reactivate(
                                expected_version,
                                McpAuthorizationReplacement {
                                    principal_binding_generation,
                                    granted_scopes: grant.granted_scopes.clone(),
                                    token_secret_binding: grant.token_secret_binding.clone(),
                                    expires_at: grant.expires_at,
                                },
                                database_now,
                            )
                            .map_err(invalid_authorization)?;
                        update_mcp_authorization(&mut transaction, &current, &next, database_now)
                            .await?;
                        next
                    }
                    _ => {
                        return Err(RepositoryError::CorruptRow(
                            "MCP OAuth Task has an open reauthorization fence".to_owned(),
                        ));
                    }
                };
                Some(next)
            }
            McpOAuthCallbackResolution::Declined { .. } => None,
        };
        let next_task = decide_task_resolution(
            &current_projection,
            ResolveTask {
                expected_generation: command.expected_task_generation,
                expected_version: command.expected_task_version,
                target: mcp_oauth_resolution_task_state(&command.resolution),
                principal: Some(current_projection.payload.created_by.clone()),
                response_value_id: None,
                response_schema_digest: None,
            },
            database_now,
        )?;
        update_mcp_oauth_task(&mut transaction, &current_task, &next_task, database_now).await?;
        let pkce_cleanup = McpOAuthPkceCleanupHint {
            schema_version: 1,
            secret_binding_id: binding.pkce_secret_binding.secret_binding_id.clone(),
            binding_generation: binding.pkce_secret_binding.binding_generation,
        };
        pkce_cleanup
            .validate()
            .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;
        let (aggregate_kind, aggregate_id, aggregate_version, event_type, response_reference) =
            if let Some(record) = &authorization {
                (
                    MCP_AUTHORIZATION_RESOURCE_KIND,
                    record.authorization_binding_id.clone(),
                    record.version,
                    "mcp.oauth_authorization_completed",
                    record.authorization_binding_id.clone(),
                )
            } else {
                (
                    "mcp_oauth_task",
                    command.task_id.clone(),
                    next_task.version,
                    "mcp.oauth_authorization_declined",
                    command.task_id.clone(),
                )
            };
        append_scheduler_event_with_trace(
            &mut transaction,
            command.audit.trace,
            &command.audit.tenant_id.to_string(),
            &command.audit.event_id,
            &command.audit.outbox_id,
            aggregate_kind,
            &aggregate_id.to_string(),
            as_i64(aggregate_version, "MCP OAuth aggregate version")?,
            None,
            event_type,
            &TypedPayload::new(
                1,
                &serde_json::json!({
                    "authorization_binding_id": command.authorization_binding_id,
                    "callback_ingress_generation_id": command.audit.callback_ingress_generation_id,
                    "pkce_cleanup": pkce_cleanup,
                    "state": mcp_oauth_resolution_task_state(&command.resolution),
                    "task_id": command.task_id,
                }),
            )?,
        )
        .await?;
        terminalize_mcp_oauth_callback_receipt(
            &mut transaction,
            &command,
            mcp_oauth_resolution_task_state(&command.resolution).as_str(),
            &response_reference,
        )
        .await?;
        let task =
            load_task_for_update(&mut transaction, &command.audit.tenant_id, &command.task_id)
                .await?;
        transaction.commit().await?;
        Ok(CommandOutcome::Applied(McpOAuthCallbackRecord {
            task,
            authorization,
        }))
    }
}

impl PgRepository {
    async fn resolve_mcp_oauth_exchange_contract(
        &self,
        identity: &AuthenticatedMcpOAuthState,
    ) -> Result<McpOAuthExchangeContract, RepositoryError> {
        identity
            .validate()
            .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;
        let mut transaction = self.pool().begin().await?;
        let database_now = database_now(&mut transaction).await?;
        let task =
            load_task_for_update(&mut transaction, &identity.tenant_id, &identity.task_id).await?;
        let projection = task_projection(&task)?;
        let TaskDefinition::McpOAuthAuthorization { binding, .. } = &projection.payload.definition
        else {
            return Err(RepositoryError::NotFound("pending MCP OAuth Task"));
        };
        if projection.state != TaskState::Pending
            || projection.tenant_id != identity.tenant_id
            || projection.task_id != identity.task_id
            || projection.deadline <= database_now
        {
            return Err(RepositoryError::NotFound("pending MCP OAuth Task"));
        }
        let deployment = load_deployment(
            &mut transaction,
            &identity.tenant_id,
            &binding.mcp_deployment.deployment_id,
        )
        .await?;
        if deployment.bindings.digest != binding.mcp_deployment.deployment_digest.to_string() {
            return Err(RepositoryError::Conflict("MCP OAuth exact Deployment"));
        }
        let DeploymentClosure::McpServer(deployment_closure) =
            decode_deployment_closure(&deployment.bindings)?
        else {
            return Err(RepositoryError::CorruptRow(
                "MCP OAuth Deployment contains the wrong closure".to_owned(),
            ));
        };
        let server_payload = crate::invocation_repository::load_enabled_exact_published_version(
            &mut transaction,
            &identity.tenant_id,
            &deployment_closure.server_revision,
            RegistryResourceKind::McpServer,
        )
        .await?;
        let ResourceDocument::McpServer(server_resource) = server_payload.document else {
            return Err(RepositoryError::CorruptRow(
                "MCP OAuth Server revision contains the wrong document".to_owned(),
            ));
        };
        let server = McpServerExecutionContract::build(
            deployment_closure.server_revision.clone(),
            server_resource.transport,
            server_resource.protocol_policy,
            server_resource.deployment_credential_requirements,
            server_resource.authorization_credential_purpose,
            server_resource.limits,
        )
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
        let auth_payload = crate::invocation_repository::load_enabled_exact_published_version(
            &mut transaction,
            &identity.tenant_id,
            &binding.auth_policy,
            RegistryResourceKind::Policy,
        )
        .await?;
        let ResourceDocument::Policy(auth_resource) = auth_payload.document else {
            return Err(RepositoryError::CorruptRow(
                "MCP OAuth Auth Profile revision contains the wrong document".to_owned(),
            ));
        };
        if auth_resource.policy_kind != PolicyKind::McpAuth {
            return Err(RepositoryError::Conflict("MCP OAuth Auth Profile kind"));
        }
        let auth_profile = auth_resource.mcp_auth.ok_or_else(|| {
            RepositoryError::CorruptRow("MCP OAuth Auth Profile document is missing".to_owned())
        })?;
        let contract = McpOAuthExchangeContract {
            tenant_id: projection.tenant_id,
            task_id: projection.task_id,
            task_generation: projection.generation,
            task_version: projection.version,
            task_deadline: projection.deadline,
            binding: binding.as_ref().clone(),
            deployment_closure,
            server,
            auth_profile: *auth_profile,
        };
        contract
            .validate_at(database_now)
            .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
        transaction.commit().await?;
        Ok(contract)
    }

    pub async fn drive_expired_mcp_oauth_tasks(
        &self,
        command: DriveExpiredMcpOAuthTasks,
    ) -> Result<Vec<TaskRecord>, RepositoryError> {
        command.validate().map_err(invalid_authorization)?;
        let mut transaction = self.pool().begin().await?;
        let database_now = database_now(&mut transaction).await?;
        let rows = sqlx::query(
            r#"
            SELECT task_id
            FROM insight_platform.tasks
            WHERE tenant_id = $1 AND task_kind = 'external_authorization'
              AND state = 'pending' AND responded_at IS NULL AND deadline <= $2
              AND payload->'definition'->>'kind' = 'mcp_oauth_authorization'
            ORDER BY deadline, task_id
            FOR UPDATE SKIP LOCKED
            LIMIT $3
            "#,
        )
        .bind(command.tenant_id.to_string())
        .bind(database_now)
        .bind(i64::from(command.limit))
        .fetch_all(&mut *transaction)
        .await?;
        let mut expired = Vec::with_capacity(rows.len());
        for (row, slot) in rows.into_iter().zip(&command.slots) {
            let task_id = row
                .try_get::<String, _>("task_id")?
                .parse::<ResourceId>()
                .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
            let current =
                load_task_for_update(&mut transaction, &command.tenant_id, &task_id).await?;
            let projection = task_projection(&current)?;
            let TaskDefinition::McpOAuthAuthorization { binding, .. } =
                &projection.payload.definition
            else {
                return Err(RepositoryError::CorruptRow(
                    "MCP OAuth expiry candidate has the wrong definition".to_owned(),
                ));
            };
            let next = decide_task_resolution(
                &projection,
                ResolveTask {
                    expected_generation: projection.generation,
                    expected_version: projection.version,
                    target: TaskState::Expired,
                    principal: None,
                    response_value_id: None,
                    response_schema_digest: None,
                },
                database_now,
            )?;
            update_mcp_oauth_task(&mut transaction, &current, &next, database_now).await?;
            let pkce_cleanup = McpOAuthPkceCleanupHint {
                schema_version: 1,
                secret_binding_id: binding.pkce_secret_binding.secret_binding_id.clone(),
                binding_generation: binding.pkce_secret_binding.binding_generation,
            };
            pkce_cleanup
                .validate()
                .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;
            append_scheduler_event(
                &mut transaction,
                &command.tenant_id.to_string(),
                &slot.event_id,
                &slot.outbox_id,
                "mcp_oauth_task",
                &task_id.to_string(),
                as_i64(next.version, "MCP OAuth Task version")?,
                None,
                "mcp.oauth_authorization_expired",
                &TypedPayload::new(
                    1,
                    &serde_json::json!({
                        "authorization_binding_id": binding.authorization_binding_id,
                        "pkce_cleanup": pkce_cleanup,
                        "scheduler_generation_id": command.scheduler_generation_id,
                        "task_id": task_id,
                    }),
                )?,
            )
            .await?;
            expired
                .push(load_task_for_update(&mut transaction, &command.tenant_id, &task_id).await?);
        }
        transaction.commit().await?;
        Ok(expired)
    }

    pub async fn create_mcp_discovery_operation(
        &self,
        mut command: CreateMcpDiscoveryOperation,
    ) -> Result<CommandOutcome<McpDiscoveryOperationRecord>, RepositoryError> {
        command.deadline = database_timestamp(command.deadline);
        command
            .validate_at(Utc::now())
            .map_err(invalid_mcp_discovery)?;
        let mut transaction = self.pool().begin().await?;
        let database_now = database_now(&mut transaction).await?;
        command
            .validate_at(database_now)
            .map_err(invalid_mcp_discovery)?;
        if let Some(original_operation_id) =
            claim_mcp_discovery_create_receipt(&mut transaction, &command).await?
        {
            let record = load_mcp_discovery_operation(
                &mut transaction,
                &command.audit.tenant_id,
                &original_operation_id,
                false,
            )
            .await?;
            transaction.commit().await?;
            return Ok(CommandOutcome::Replayed(record));
        }
        require_tenant_permission(&mut transaction, &command.audit, Permission::McpWrite).await?;
        let authorization_record = load_mcp_authorization_binding(
            &mut transaction,
            &command.audit.tenant_id,
            &command.authorization_binding_id,
            false,
        )
        .await?;
        let authorization = authorization_record
            .execution_context(database_now)
            .map_err(invalid_authorization)?;
        if authorization.mcp_deployment != command.mcp_deployment
            || authorization.principal_id != command.audit.principal_id
            || authorization.principal_identity_kind != command.audit.principal_kind
            || command.deadline > authorization.expires_at
        {
            return Err(RepositoryError::Conflict(
                "MCP discovery authorization binding",
            ));
        }
        validate_principal_binding(
            &mut transaction,
            &authorization.tenant_id,
            &authorization.principal_id,
            authorization.principal_identity_kind,
            authorization.principal_binding_generation,
        )
        .await?;
        validate_mcp_authorization_dependencies(
            &mut transaction,
            &command.audit.tenant_id,
            &authorization.mcp_deployment,
            &authorization.audience_identity_digest,
            &authorization.token_secret_binding,
        )
        .await?;
        let deployment = load_deployment(
            &mut transaction,
            &command.audit.tenant_id,
            &command.mcp_deployment.deployment_id,
        )
        .await?;
        if deployment.bindings.digest != command.mcp_deployment.deployment_digest.to_string() {
            return Err(RepositoryError::Conflict("exact MCP discovery Deployment"));
        }
        let closure = match decode_deployment_closure(&deployment.bindings)? {
            DeploymentClosure::McpServer(closure) => closure,
            _ => {
                return Err(RepositoryError::CorruptRow(
                    "MCP discovery Deployment contains the wrong closure".to_owned(),
                ));
            }
        };
        validate_deployment_closure_exists(
            &mut transaction,
            &command.audit.tenant_id,
            &DeploymentClosure::McpServer(closure.clone()),
        )
        .await?;
        if closure.server_identity_digest != authorization.audience_identity_digest {
            return Err(RepositoryError::Conflict("MCP discovery server audience"));
        }
        let discovery_execution = resolve_mcp_base_execution_contract(
            &mut transaction,
            &command.audit.tenant_id,
            &command.mcp_deployment,
            &authorization.authorization_binding_id,
            authorization.generation,
            &authorization.canonical_digest,
            &authorization.principal_id,
            database_now,
        )
        .await?;
        let artifact_authority =
            load_internal_artifact_admission_authority(&mut transaction, &command.audit.tenant_id)
                .await?;
        let minimum_retention_seconds = i64::try_from(
            artifact_authority
                .retention_policy
                .minimum_retention_seconds,
        )
        .map_err(|_| {
            RepositoryError::CorruptRow(
                "Artifact retention duration exceeds clock representation".to_owned(),
            )
        })?;
        let minimum_retain_until = database_now
            .checked_add_signed(chrono::Duration::seconds(minimum_retention_seconds))
            .ok_or_else(|| {
                RepositoryError::InvalidInput("Artifact retention deadline overflows".to_owned())
            })?;
        let artifact_policy = insight_platform_mcp_host::McpDiscoveryArtifactPolicyClosure::build(
            NewMcpDiscoveryArtifactPolicyClosure {
                quota_account_id: artifact_authority.quota_account_id,
                maximum_bytes: u64::from(discovery_execution.server.limits.maximum_response_bytes),
                retention_policy_revision: artifact_authority.retention_policy_revision,
                artifact_io_policy_revision: artifact_authority.artifact_io_policy_revision,
                scanner_contract_digest: artifact_authority
                    .artifact_io_policy
                    .scanner_contract_digest,
                ruleset_digest: artifact_authority.artifact_io_rules_digest,
                evidence_ttl_milliseconds: artifact_authority
                    .artifact_io_policy
                    .verification_evidence_ttl_milliseconds,
                retry_backoff_milliseconds: artifact_authority
                    .artifact_io_policy
                    .verification_retry_backoff_milliseconds,
                write_storage_binding_digest: artifact_authority
                    .artifact_io_policy
                    .write_storage_binding_digest,
                encryption_domain_id: artifact_authority.artifact_io_policy.encryption_domain_id,
                retain_until: command.deadline.max(minimum_retain_until),
            },
        )
        .map_err(invalid_mcp_discovery)?;
        reserve_internal_artifact_staging_quota(
            &mut transaction,
            &command.audit.tenant_id,
            &artifact_policy.quota_account_id,
            &command.artifact_preallocation.quota_entry_id,
            &command.artifact_preallocation.artifact_id,
            artifact_policy.maximum_bytes,
            &command.audit.request_digest,
        )
        .await?;
        let admission = McpDiscoveryAdmission::build(NewMcpDiscoveryAdmission {
            operation_id: command.operation_id.clone(),
            job_id: command.job_id.clone(),
            tenant_id: command.audit.tenant_id.clone(),
            mcp_deployment: command.mcp_deployment.clone(),
            server_revision: closure.server_revision,
            protocol_profile: closure.protocol_policy,
            authorization_binding_id: authorization.authorization_binding_id,
            authorization_generation: authorization.generation,
            authorization_context_digest: authorization.canonical_digest,
            principal_id: authorization.principal_id,
            artifact_preallocation: command.artifact_preallocation.clone(),
            artifact_policy,
            requested_at: database_now,
            deadline: command.deadline,
        })
        .map_err(invalid_mcp_discovery)?;
        let operation_payload = McpDiscoveryOperationPayload::pending(admission.clone())
            .map_err(invalid_mcp_discovery)?;
        let operation_typed = TypedPayload::from_versioned(2, &operation_payload, 1_048_576)?;
        let job_payload =
            McpDiscoveryJobPayload::build(&admission).map_err(invalid_mcp_discovery)?;
        let job_typed =
            TypedPayload::with_limit(1, &McpJobPayload::Discovery(job_payload), 1_048_576)?;
        let artifact_job_payload = ArtifactJobPayload::AwaitingStage {
            stage: ArtifactAwaitingStageSnapshot {
                schema_version: 1,
                producer_job_id: command.job_id.clone(),
                artifact_id: command.artifact_preallocation.artifact_id.clone(),
                blob_id: command.artifact_preallocation.blob_id.clone(),
                quota_account_id: admission.artifact_policy.quota_account_id.clone(),
                quota_entry_id: command.artifact_preallocation.quota_entry_id.clone(),
                purpose: ArtifactPurpose::McpResource,
                classification: admission.artifact_policy.classification,
                maximum_bytes: admission.artifact_policy.maximum_bytes,
                declared_media_type: admission.artifact_policy.declared_media_type.clone(),
                retention_policy_revision: admission
                    .artifact_policy
                    .retention_policy_revision
                    .clone(),
                artifact_io_policy_revision: admission
                    .artifact_policy
                    .artifact_io_policy_revision
                    .clone(),
                scanner_contract_digest: admission.artifact_policy.scanner_contract_digest.clone(),
                ruleset_digest: admission.artifact_policy.ruleset_digest.clone(),
                evidence_ttl_milliseconds: admission.artifact_policy.evidence_ttl_milliseconds,
                retry_backoff_milliseconds: admission.artifact_policy.retry_backoff_milliseconds,
                write_storage_binding_digest: admission
                    .artifact_policy
                    .write_storage_binding_digest
                    .clone(),
                encryption_domain_id: admission.artifact_policy.encryption_domain_id.clone(),
                retain_until: admission.artifact_policy.retain_until,
                deadline: command.deadline,
            },
        };
        artifact_job_payload
            .validate_for_owner(&command.artifact_preallocation.artifact_id)
            .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;
        let artifact_job_typed = TypedPayload::new(1, &artifact_job_payload)?;
        sqlx::query(
            r#"
            INSERT INTO insight_platform.invocations (
                tenant_id, invocation_id, invocation_kind, owner_kind, owner_id,
                logical_key, deployment_id, state, version, payload_schema_version,
                payload, payload_digest, deadline, created_at, updated_at, trace_id
            ) VALUES ($1, $2, 'mcp_discovery', 'mcp_operation', $2,
                      $3, $4, 'pending', 1, $5, $6, $7, $8, $9, $9, $10)
            "#,
        )
        .bind(command.audit.tenant_id.to_string())
        .bind(command.operation_id.to_string())
        .bind(&command.logical_key)
        .bind(command.mcp_deployment.deployment_id.to_string())
        .bind(operation_typed.schema_version)
        .bind(&operation_typed.value)
        .bind(&operation_typed.digest)
        .bind(command.deadline)
        .bind(database_now)
        .bind(command.audit.trace.trace_id.to_string())
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            r#"
            INSERT INTO insight_platform.jobs (
                tenant_id, job_id, job_kind, work_class, owner_kind, owner_id, invocation_id,
                state, attempt_limit, scheduled_at, deadline, priority, request_digest,
                payload_schema_version, payload, payload_digest, created_at, updated_at,
                trace_id
            ) VALUES ($1, $2, 'mcp_discovery', 'mcp', 'mcp_operation', $3, $3,
                      'ready', $4, $5, $6, 0, $7, $8, $9, $10, $5, $5, $11)
            "#,
        )
        .bind(command.audit.tenant_id.to_string())
        .bind(command.job_id.to_string())
        .bind(command.operation_id.to_string())
        .bind(i32::from(command.attempt_limit))
        .bind(database_now)
        .bind(command.deadline)
        .bind(command.audit.request_digest.to_string())
        .bind(job_typed.schema_version)
        .bind(&job_typed.value)
        .bind(&job_typed.digest)
        .bind(command.audit.trace.trace_id.to_string())
        .execute(&mut *transaction)
        .await?;
        let artifact_attempt_limit = i32::try_from(
            checked_in_hard_limit_profile()
                .run_scheduler
                .attempts_per_work
                .q1_default,
        )
        .map_err(|_| {
            RepositoryError::InvalidInput(
                "Artifact verification attempt limit exceeds integer".to_owned(),
            )
        })?;
        sqlx::query(
            r#"
            INSERT INTO insight_platform.jobs (
                tenant_id, job_id, job_kind, work_class, owner_kind, owner_id,
                state, attempt_limit, scheduled_at, deadline, priority, request_digest,
                payload_schema_version, payload, payload_digest, created_at, updated_at,
                trace_id
            ) VALUES ($1, $2, 'artifact_scan', 'artifact', 'artifact', $3,
                      'waiting', $4, $5, $6, 0, $7, $8, $9, $10, $5, $5, $11)
            "#,
        )
        .bind(command.audit.tenant_id.to_string())
        .bind(
            command
                .artifact_preallocation
                .verification_job_id
                .to_string(),
        )
        .bind(command.artifact_preallocation.artifact_id.to_string())
        .bind(artifact_attempt_limit)
        .bind(database_now)
        .bind(command.deadline)
        .bind(command.audit.request_digest.to_string())
        .bind(artifact_job_typed.schema_version)
        .bind(&artifact_job_typed.value)
        .bind(&artifact_job_typed.digest)
        .bind(command.audit.trace.trace_id.to_string())
        .execute(&mut *transaction)
        .await?;
        append_command_event(
            &mut transaction,
            &command.audit,
            "mcp_operation",
            &command.operation_id.to_string(),
            1,
            "mcp.discovery_scheduled",
            &TypedPayload::new(
                1,
                &serde_json::json!({
                    "authorization_binding_id": command.authorization_binding_id,
                    "artifact_id": command.artifact_preallocation.artifact_id,
                    "artifact_verification_job_id": command.artifact_preallocation.verification_job_id,
                    "job_id": command.job_id,
                    "mcp_deployment": command.mcp_deployment,
                    "state": "pending",
                }),
            )?,
        )
        .await?;
        terminalize_command_receipt(
            &mut transaction,
            &command.audit,
            &command.operation_id.to_string(),
            "scheduled",
        )
        .await?;
        let record = load_mcp_discovery_operation(
            &mut transaction,
            &command.audit.tenant_id,
            &command.operation_id,
            false,
        )
        .await?;
        transaction.commit().await?;
        Ok(CommandOutcome::Applied(record))
    }

    pub async fn park_mcp_discovery_for_verification(
        &self,
        command: ParkMcpDiscoveryVerification,
    ) -> Result<CommandOutcome<McpDiscoveryOperationRecord>, RepositoryError> {
        let mut transaction = self.pool().begin().await?;
        let database_now = database_now(&mut transaction).await?;
        let receipt_payload = mcp_discovery_park_receipt_payload(&command)?;
        if claim_mcp_discovery_park_receipt(&mut transaction, &command, &receipt_payload).await? {
            let record = load_mcp_discovery_operation(
                &mut transaction,
                &command.audit.tenant_id,
                &command.operation_id,
                false,
            )
            .await?;
            if record
                .payload
                .pending_verification
                .as_ref()
                .map(|evidence| (&evidence.canonical_digest, &evidence.artifact_id))
                != Some((
                    &command.evidence.canonical_digest,
                    &command.staged.artifact_id,
                ))
            {
                return Err(RepositoryError::Conflict(
                    "MCP discovery verification park replay",
                ));
            }
            transaction.commit().await?;
            return Ok(CommandOutcome::Replayed(record));
        }
        let current = load_mcp_discovery_operation(
            &mut transaction,
            &command.audit.tenant_id,
            &command.operation_id,
            true,
        )
        .await?;
        command
            .validate_at(
                &current.payload.admission.artifact_preallocation,
                database_now,
            )
            .map_err(invalid_mcp_discovery)?;
        if current.job_id != command.job_id
            || current.version != command.expected_operation_version
            || !matches!(
                current.state,
                McpDiscoveryOperationState::Pending | McpDiscoveryOperationState::Running
            )
            || current.payload.pending_verification.is_some()
            || current.payload.result.is_some()
            || command.evidence.expires_at > current.deadline
        {
            return Err(RepositoryError::Conflict(
                "MCP discovery verification park CAS",
            ));
        }
        let job = load_mcp_discovery_job(
            &mut transaction,
            &command.audit.tenant_id,
            &command.job_id,
            true,
        )
        .await?;
        require_exact_mcp_discovery_job_fence(&job, &current, &command.fence, database_now)?;
        let current_job = mcp_discovery_job_projection(
            &job,
            &command.audit.tenant_id,
            &command.job_id,
            &command.operation_id,
        )?;
        let wake = WakeContract {
            kind: WakeKind::RemoteInvocation,
            generation: command.fence.lease_generation,
            accepted_sources: vec![WakeSource::Signal, WakeSource::Timeout],
            expected_response_schema_digest: None,
            opaque_state_digest: Some(command.evidence.canonical_digest.clone()),
            next_poll_at: None,
            poll_count: 0,
            poll_limit: 0,
            callback_binding_digest: None,
            deadline: current.deadline.min(command.evidence.expires_at),
        };
        let next_job = decide_job_wait(&current_job, &command.fence, database_now, wake.clone())?;
        let next_payload = current
            .payload
            .park_for_verification(command.evidence.clone())
            .map_err(invalid_mcp_discovery)?;
        let next_operation_version = current.version.checked_add(1).ok_or_else(|| {
            RepositoryError::InvalidInput("MCP operation version overflow".to_owned())
        })?;
        let next_typed = TypedPayload::from_versioned(2, &next_payload, 1_048_576)?;
        let operation_affected = sqlx::query(
            r#"
            UPDATE insight_platform.invocations
            SET state = 'running', version = $4, payload_schema_version = $5, payload = $6,
                payload_digest = $7, updated_at = $8
            WHERE tenant_id = $1 AND invocation_id = $2 AND version = $3
              AND invocation_kind = 'mcp_discovery' AND state IN ('pending', 'running')
            "#,
        )
        .bind(command.audit.tenant_id.to_string())
        .bind(command.operation_id.to_string())
        .bind(as_i64(current.version, "MCP discovery operation version")?)
        .bind(as_i64(
            next_operation_version,
            "MCP discovery operation version",
        )?)
        .bind(next_typed.schema_version)
        .bind(&next_typed.value)
        .bind(&next_typed.digest)
        .bind(database_now)
        .execute(&mut *transaction)
        .await?
        .rows_affected();
        if operation_affected != 1 {
            return Err(RepositoryError::Conflict(
                "MCP discovery verification park CAS",
            ));
        }
        let job_affected = sqlx::query(
            r#"
            UPDATE insight_platform.jobs
            SET state = 'waiting', version = $4, worker_id = NULL,
                lease_token_digest = NULL, lease_expires_at = NULL, heartbeat_at = NULL,
                retry_at = NULL, wake_kind = $5, wake_state = 'pending',
                wake_generation = $6, started_at = NULL, updated_at = $7
            WHERE tenant_id = $1 AND job_id = $2 AND version = $3
              AND work_class = 'mcp' AND job_kind = 'mcp_discovery'
              AND owner_kind = 'mcp_operation' AND state = 'running'
            "#,
        )
        .bind(command.audit.tenant_id.to_string())
        .bind(command.job_id.to_string())
        .bind(as_i64(job.version, "MCP discovery Job version")?)
        .bind(as_i64(next_job.version, "MCP discovery Job version")?)
        .bind(wake.kind.as_str())
        .bind(as_i64(wake.generation, "MCP discovery wake generation")?)
        .bind(database_now)
        .execute(&mut *transaction)
        .await?
        .rows_affected();
        if job_affected != 1 {
            return Err(RepositoryError::Conflict(
                "MCP discovery verification park Job CAS",
            ));
        }
        let tenant_id = command.audit.tenant_id.to_string();
        let operation_id = command.operation_id.to_string();
        append_scheduler_event_with_trace(
            &mut transaction,
            job.trace,
            &tenant_id,
            &command.audit.event_id,
            &command.audit.outbox_id,
            "mcp_operation",
            &operation_id,
            as_i64(next_operation_version, "MCP discovery operation version")?,
            None,
            "mcp.discovery_verification_pending",
            &TypedPayload::new(
                1,
                &serde_json::json!({
                    "artifact_id": command.staged.artifact_id,
                    "descriptor_count": command.evidence.descriptor_count,
                    "descriptor_digest": command.evidence.descriptor_digest,
                    "job_id": command.job_id,
                    "verification_job_id": command.staged.verification_job_id,
                }),
            )?,
        )
        .await?;
        terminalize_mcp_discovery_park_receipt(&mut transaction, &command).await?;
        let record = load_mcp_discovery_operation(
            &mut transaction,
            &command.audit.tenant_id,
            &command.operation_id,
            false,
        )
        .await?;
        transaction.commit().await?;
        Ok(CommandOutcome::Applied(record))
    }

    pub async fn resolve_mcp_discovery_attempt(
        &self,
        command: ResolveMcpDiscoveryAttempt,
    ) -> Result<CommandOutcome<McpDiscoveryOperationRecord>, RepositoryError> {
        command
            .validate_at(Utc::now())
            .map_err(invalid_mcp_discovery)?;
        let mut transaction = self.pool().begin().await?;
        let database_now = database_now(&mut transaction).await?;
        command
            .validate_at(database_now)
            .map_err(invalid_mcp_discovery)?;
        let receipt_payload = mcp_discovery_resolution_receipt_payload(&command)?;
        if claim_mcp_resolution_receipt(
            &mut transaction,
            &command,
            "mcp.discovery.resolve_attempt",
            &receipt_payload,
        )
        .await?
        {
            let record = load_mcp_discovery_operation(
                &mut transaction,
                &command.audit.tenant_id,
                &command.operation_id,
                false,
            )
            .await?;
            transaction.commit().await?;
            return Ok(CommandOutcome::Replayed(record));
        }

        let current = load_mcp_discovery_operation(
            &mut transaction,
            &command.audit.tenant_id,
            &command.operation_id,
            true,
        )
        .await?;
        if current.job_id != command.job_id
            || current.version != command.expected_operation_version
            || !matches!(
                current.state,
                McpDiscoveryOperationState::Pending | McpDiscoveryOperationState::Running
            )
            || current.payload.result.is_some()
            || command
                .resolution
                .retry_at()
                .is_some_and(|retry_at| retry_at >= current.deadline)
        {
            return Err(RepositoryError::Conflict("MCP discovery operation CAS"));
        }
        let job = load_mcp_discovery_job(
            &mut transaction,
            &command.audit.tenant_id,
            &command.job_id,
            true,
        )
        .await?;
        require_exact_mcp_discovery_job_fence(&job, &current, &command.fence, database_now)?;
        let current_job = mcp_discovery_job_projection(
            &job,
            &command.audit.tenant_id,
            &command.job_id,
            &command.operation_id,
        )?;
        let next_job = match &command.resolution {
            McpDiscoveryAttemptResolution::Retry { retry_at, .. } => {
                decide_job_retry(&current_job, &command.fence, database_now, *retry_at)?
            }
            McpDiscoveryAttemptResolution::Failed { .. }
            | McpDiscoveryAttemptResolution::ReauthorizationRequired { .. }
            | McpDiscoveryAttemptResolution::Cancelled { .. } => decide_job_terminal(
                &current_job,
                &command.fence,
                database_now,
                command.resolution.job_state(),
            )?,
        };
        let resolution_payload = TypedPayload::new(1, &command.resolution)?;
        update_resolved_mcp_discovery_job(
            &mut transaction,
            &command,
            &next_job,
            &resolution_payload.digest,
            database_now,
        )
        .await?;

        let next_operation_version = current.version.checked_add(1).ok_or_else(|| {
            RepositoryError::InvalidInput("MCP operation version overflow".to_owned())
        })?;
        let terminal_at = command
            .resolution
            .operation_state()
            .is_terminal()
            .then_some(database_now);
        let affected = sqlx::query(
            r#"
            UPDATE insight_platform.invocations
            SET state = $4, version = $5, terminal_at = $6, updated_at = $7
            WHERE tenant_id = $1 AND invocation_id = $2 AND version = $3
              AND invocation_kind = 'mcp_discovery' AND state IN ('pending', 'running')
            "#,
        )
        .bind(command.audit.tenant_id.to_string())
        .bind(command.operation_id.to_string())
        .bind(as_i64(current.version, "MCP discovery operation version")?)
        .bind(command.resolution.operation_state().as_str())
        .bind(as_i64(
            next_operation_version,
            "MCP discovery operation version",
        )?)
        .bind(terminal_at)
        .bind(database_now)
        .execute(&mut *transaction)
        .await?
        .rows_affected();
        if affected != 1 {
            return Err(RepositoryError::Conflict("MCP discovery operation CAS"));
        }
        append_scheduler_event(
            &mut transaction,
            &command.audit.tenant_id.to_string(),
            &command.audit.event_id,
            &command.audit.outbox_id,
            "mcp_operation",
            &command.operation_id.to_string(),
            as_i64(next_operation_version, "MCP discovery operation version")?,
            None,
            command.resolution.event_type(),
            &TypedPayload::new(
                1,
                &serde_json::json!({
                    "job_id": command.job_id,
                    "resolution": command.resolution,
                }),
            )?,
        )
        .await?;
        terminalize_mcp_resolution_receipt(
            &mut transaction,
            &command,
            command.resolution.operation_state().as_str(),
        )
        .await?;
        let record = load_mcp_discovery_operation(
            &mut transaction,
            &command.audit.tenant_id,
            &command.operation_id,
            false,
        )
        .await?;
        transaction.commit().await?;
        Ok(CommandOutcome::Applied(record))
    }

    pub async fn finalize_mcp_discovery_after_verification(
        &self,
        command: FinalizeMcpDiscovery,
    ) -> Result<CommandOutcome<McpDiscoveryOperationRecord>, RepositoryError> {
        command
            .validate_at(Utc::now())
            .map_err(invalid_mcp_discovery)?;
        let mut transaction = self.pool().begin().await?;
        let database_now = database_now(&mut transaction).await?;
        command
            .validate_at(database_now)
            .map_err(invalid_mcp_discovery)?;
        let receipt_payload = mcp_discovery_finalize_receipt_payload(&command)?;
        if claim_mcp_discovery_finalize_receipt(&mut transaction, &command, &receipt_payload)
            .await?
        {
            let record = load_mcp_discovery_operation(
                &mut transaction,
                &command.audit.tenant_id,
                &command.operation_id,
                false,
            )
            .await?;
            if !matches!(
                record.state,
                McpDiscoveryOperationState::Succeeded | McpDiscoveryOperationState::Failed
            ) {
                return Err(RepositoryError::Conflict(
                    "MCP discovery finalize replay state",
                ));
            }
            transaction.commit().await?;
            return Ok(CommandOutcome::Replayed(record));
        }
        let current = load_mcp_discovery_operation(
            &mut transaction,
            &command.audit.tenant_id,
            &command.operation_id,
            true,
        )
        .await?;
        let pending = current
            .payload
            .pending_verification
            .as_ref()
            .ok_or(RepositoryError::Conflict("MCP discovery finalize evidence"))?;
        if current.job_id != command.job_id
            || current.version != command.expected_operation_version
            || current.state != McpDiscoveryOperationState::Running
            || current.payload.result.is_some()
        {
            return Err(RepositoryError::Conflict("MCP discovery finalize CAS"));
        }
        let job = load_mcp_discovery_job(
            &mut transaction,
            &command.audit.tenant_id,
            &command.job_id,
            true,
        )
        .await?;
        require_exact_mcp_discovery_job_fence(&job, &current, &command.fence, database_now)?;
        let verified = sqlx::query(
            r#"
            SELECT artifact.blob_id, artifact.purpose, artifact.classification,
                   artifact.expected_size_bytes, artifact.expected_digest,
                   artifact.declared_media_type, artifact.verified_media_type,
                   artifact.state AS artifact_state, artifact.version AS artifact_version,
                   artifact.metadata_schema_version, artifact.metadata,
                   artifact.metadata_digest, artifact.retain_until,
                   blob.content_digest, blob.size_bytes, blob.state AS blob_state,
                   blob.version AS blob_version,
                   verification.state AS verification_state,
                   verification.version AS verification_version,
                   verification.result_digest AS verification_result_digest
            FROM insight_platform.artifacts AS artifact
            JOIN insight_platform.artifact_blobs AS blob
              ON blob.tenant_id = artifact.tenant_id AND blob.blob_id = artifact.blob_id
            JOIN insight_platform.jobs AS verification
              ON verification.tenant_id = artifact.tenant_id
             AND verification.job_id = $3
             AND verification.owner_id = artifact.artifact_id
            WHERE artifact.tenant_id = $1 AND artifact.artifact_id = $2
              AND verification.work_class = 'artifact'
              AND verification.job_kind = 'artifact_scan'
              AND verification.owner_kind = 'artifact'
            FOR UPDATE OF artifact, blob, verification
            "#,
        )
        .bind(command.audit.tenant_id.to_string())
        .bind(pending.artifact_id.to_string())
        .bind(pending.verification_job_id.to_string())
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(RepositoryError::NotFound("verified MCP discovery Artifact"))?;
        let metadata_payload = TypedPayload {
            schema_version: verified.try_get("metadata_schema_version")?,
            value: verified.try_get("metadata")?,
            digest: verified.try_get("metadata_digest")?,
        };
        let metadata: ArtifactMetadataSnapshot =
            decode_versioned_payload(&metadata_payload, "MCP discovery Artifact metadata")?;
        metadata
            .validate()
            .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
        let verification =
            metadata
                .current_verification
                .as_ref()
                .ok_or(RepositoryError::Conflict(
                    "MCP discovery verification evidence",
                ))?;
        let content_digest = verified
            .try_get::<Option<String>, _>("content_digest")?
            .ok_or(RepositoryError::Conflict("MCP discovery content digest"))?
            .parse::<Sha256Digest>()
            .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
        let size_bytes = u64::try_from(
            verified
                .try_get::<Option<i64>, _>("size_bytes")?
                .ok_or(RepositoryError::Conflict("MCP discovery Artifact size"))?,
        )
        .map_err(|_| RepositoryError::CorruptRow("negative Artifact size".to_owned()))?;
        let verified_media_type = verified
            .try_get::<Option<String>, _>("verified_media_type")?
            .ok_or(RepositoryError::Conflict(
                "MCP discovery verified media type",
            ))?;
        let artifact_state = verified.try_get::<String, _>("artifact_state")?;
        let blob_state = verified.try_get::<String, _>("blob_state")?;
        let verification_state = verified.try_get::<String, _>("verification_state")?;
        let verification_result_digest =
            verified.try_get::<Option<String>, _>("verification_result_digest")?;
        if verified.try_get::<String, _>("purpose")? != ArtifactPurpose::McpResource.as_str()
            || verified.try_get::<String, _>("classification")?
                != current
                    .payload
                    .admission
                    .artifact_policy
                    .classification
                    .as_str()
            || verified.try_get::<i64, _>("expected_size_bytes")?
                != i64::try_from(pending.descriptor_size_bytes).unwrap_or(i64::MAX)
            || verified.try_get::<Option<String>, _>("expected_digest")?
                != Some(pending.descriptor_digest.to_string())
            || verified.try_get::<Option<String>, _>("declared_media_type")?
                != Some(
                    current
                        .payload
                        .admission
                        .artifact_policy
                        .declared_media_type
                        .clone(),
                )
            || content_digest != pending.descriptor_digest
            || size_bytes != pending.descriptor_size_bytes
            || verified_media_type
                != current
                    .payload
                    .admission
                    .artifact_policy
                    .declared_media_type
            || verification.scan_job_id != pending.verification_job_id
            || verification.content_digest != pending.descriptor_digest
            || verification.size_bytes != pending.descriptor_size_bytes
            || verification.verified_media_type != verified_media_type
            || verified.try_get::<DateTime<Utc>, _>("retain_until")? < current.deadline
        {
            return Err(RepositoryError::Conflict(
                "MCP discovery verification evidence drift",
            ));
        }
        let expected_artifact_state = match verification.disposition {
            ArtifactScanDisposition::Verified => "verified",
            ArtifactScanDisposition::Quarantined | ArtifactScanDisposition::Corrupt => {
                "quarantined"
            }
            ArtifactScanDisposition::Rejected => "rejected",
        };
        let expected_verification_terminal = match verification.disposition {
            ArtifactScanDisposition::Rejected => Some("failed"),
            ArtifactScanDisposition::Verified
            | ArtifactScanDisposition::Quarantined
            | ArtifactScanDisposition::Corrupt => None,
        };
        if artifact_state != expected_artifact_state
            || blob_state != "verified"
            || expected_verification_terminal.is_some_and(|state| verification_state != state)
            || expected_verification_terminal.is_none() && verification_state != "waiting"
            || expected_verification_terminal.is_some()
                != verification_result_digest
                    .as_deref()
                    .is_some_and(|digest| digest == verification.evidence_digest.to_string())
        {
            return Err(RepositoryError::Conflict(
                "MCP discovery verification terminal drift",
            ));
        }
        if verification.disposition != ArtifactScanDisposition::Verified
            || verification.expires_at <= database_now
            || pending.expires_at <= database_now
        {
            let failure_code = if verification.expires_at <= database_now
                || pending.expires_at <= database_now
            {
                "mcp_discovery_verification_expired"
            } else {
                match verification.disposition {
                    ArtifactScanDisposition::Quarantined => "mcp_discovery_artifact_quarantined",
                    ArtifactScanDisposition::Rejected => "mcp_discovery_artifact_rejected",
                    ArtifactScanDisposition::Corrupt => "mcp_discovery_artifact_corrupt",
                    ArtifactScanDisposition::Verified => unreachable!("handled by expiry"),
                }
            };
            settle_mcp_discovery_artifact_quota(
                &mut transaction,
                &current,
                &command,
                pending.descriptor_size_bytes,
                database_now,
            )
            .await?;
            fail_mcp_discovery_after_verification(
                &mut transaction,
                &current,
                &command,
                &job,
                &verified,
                verification_state.as_str(),
                &verification.evidence_digest,
                failure_code,
                database_now,
            )
            .await?;
            let record = load_mcp_discovery_operation(
                &mut transaction,
                &command.audit.tenant_id,
                &command.operation_id,
                false,
            )
            .await?;
            transaction.commit().await?;
            return Ok(CommandOutcome::Applied(record));
        }
        validate_mcp_discovery_frozen_dependencies(&mut transaction, &current, database_now)
            .await?;
        let artifact = insight_platform_contracts::ArtifactRef::new(
            pending.artifact_id.clone(),
            content_digest,
            size_bytes,
            verified_media_type,
            current.payload.admission.artifact_policy.classification,
            metadata.display_name.clone(),
        )
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
        let snapshot = insight_platform_contracts::McpDiscoverySnapshot::build(
            current
                .payload
                .admission
                .artifact_preallocation
                .snapshot_id
                .clone(),
            current.payload.admission.mcp_deployment.clone(),
            current.payload.admission.server_revision.clone(),
            current.payload.admission.protocol_profile.clone(),
            current
                .payload
                .admission
                .authorization_context_digest
                .clone(),
            pending.negotiated_version.clone(),
            pending.negotiated_capabilities.clone(),
            artifact,
            pending.observed_at,
            pending.expires_at,
        )
        .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;
        settle_mcp_discovery_artifact_quota(
            &mut transaction,
            &current,
            &command,
            pending.descriptor_size_bytes,
            database_now,
        )
        .await?;
        let artifact_version = u64::try_from(verified.try_get::<i64, _>("artifact_version")?)
            .map_err(|_| RepositoryError::CorruptRow("negative Artifact version".to_owned()))?;
        let artifact_affected = sqlx::query(
            r#"
            UPDATE insight_platform.artifacts
            SET state = 'ready', version = $4, updated_at = $5
            WHERE tenant_id = $1 AND artifact_id = $2 AND version = $3
              AND state = 'verified'
            "#,
        )
        .bind(command.audit.tenant_id.to_string())
        .bind(pending.artifact_id.to_string())
        .bind(as_i64(artifact_version, "MCP discovery Artifact version")?)
        .bind(as_i64(
            artifact_version
                .checked_add(1)
                .ok_or(RepositoryError::Conflict("MCP discovery Artifact version"))?,
            "MCP discovery Artifact version",
        )?)
        .bind(database_now)
        .execute(&mut *transaction)
        .await?
        .rows_affected();
        if artifact_affected != 1 {
            return Err(RepositoryError::Conflict(
                "MCP discovery Artifact ready CAS",
            ));
        }
        let verification_version =
            u64::try_from(verified.try_get::<i64, _>("verification_version")?).map_err(|_| {
                RepositoryError::CorruptRow("negative verification Job version".to_owned())
            })?;
        let verification_affected = sqlx::query(
            r#"
            UPDATE insight_platform.jobs
            SET state = 'succeeded', version = $4, result_digest = $5,
                terminal_at = $6, updated_at = $6
            WHERE tenant_id = $1 AND job_id = $2 AND version = $3
              AND work_class = 'artifact' AND job_kind = 'artifact_scan'
              AND owner_kind = 'artifact' AND state = 'waiting'
            "#,
        )
        .bind(command.audit.tenant_id.to_string())
        .bind(pending.verification_job_id.to_string())
        .bind(as_i64(
            verification_version,
            "Artifact verification Job version",
        )?)
        .bind(as_i64(
            verification_version
                .checked_add(1)
                .ok_or(RepositoryError::Conflict(
                    "Artifact verification Job version",
                ))?,
            "Artifact verification Job version",
        )?)
        .bind(verification.evidence_digest.to_string())
        .bind(database_now)
        .execute(&mut *transaction)
        .await?
        .rows_affected();
        if verification_affected != 1 {
            return Err(RepositoryError::Conflict(
                "Artifact verification Job finalize CAS",
            ));
        }
        let snapshot_record = McpDiscoverySnapshotRecord::build(NewMcpDiscoverySnapshotRecord {
            tenant_id: command.audit.tenant_id.clone(),
            source_operation_id: command.operation_id.clone(),
            artifact_link_id: current
                .payload
                .admission
                .artifact_preallocation
                .artifact_link_id
                .clone(),
            snapshot: snapshot.clone(),
            completed_at: database_now,
        })
        .map_err(invalid_mcp_discovery)?;
        insert_mcp_discovery_snapshot(
            &mut transaction,
            &snapshot_record,
            &current.payload.admission.principal_id,
            database_now,
        )
        .await?;
        let result = McpDiscoveryResultBinding {
            snapshot_id: snapshot.snapshot_id.clone(),
            snapshot_digest: snapshot.canonical_digest.clone(),
            objects_artifact: snapshot.objects_artifact.clone(),
            artifact_link_id: current
                .payload
                .admission
                .artifact_preallocation
                .artifact_link_id
                .clone(),
        };
        let next_payload = current
            .payload
            .complete(result)
            .map_err(invalid_mcp_discovery)?;
        let next_operation_version = current.version.checked_add(1).ok_or_else(|| {
            RepositoryError::InvalidInput("MCP operation version overflow".to_owned())
        })?;
        let next_typed = TypedPayload::from_versioned(2, &next_payload, 1_048_576)?;
        let operation_affected = sqlx::query(
            r#"
            UPDATE insight_platform.invocations
            SET state = 'succeeded', version = $4, payload_schema_version = $5,
                payload = $6, payload_digest = $7, terminal_at = $8, updated_at = $8
            WHERE tenant_id = $1 AND invocation_id = $2 AND version = $3
              AND invocation_kind = 'mcp_discovery' AND state = 'running'
            "#,
        )
        .bind(command.audit.tenant_id.to_string())
        .bind(command.operation_id.to_string())
        .bind(as_i64(current.version, "MCP discovery operation version")?)
        .bind(as_i64(
            next_operation_version,
            "MCP discovery operation version",
        )?)
        .bind(next_typed.schema_version)
        .bind(&next_typed.value)
        .bind(&next_typed.digest)
        .bind(database_now)
        .execute(&mut *transaction)
        .await?
        .rows_affected();
        if operation_affected != 1 {
            return Err(RepositoryError::Conflict("MCP discovery finalize CAS"));
        }
        finalize_mcp_discovery_owner_job(
            &mut transaction,
            &command,
            &snapshot.canonical_digest,
            database_now,
        )
        .await?;
        append_scheduler_event_with_trace(
            &mut transaction,
            job.trace,
            &command.audit.tenant_id.to_string(),
            &command.audit.event_id,
            &command.audit.outbox_id,
            "mcp_operation",
            &command.operation_id.to_string(),
            as_i64(next_operation_version, "MCP discovery operation version")?,
            None,
            "mcp.discovery_succeeded",
            &TypedPayload::new(
                1,
                &serde_json::json!({
                    "artifact_id": pending.artifact_id,
                    "job_id": command.job_id,
                    "snapshot_digest": snapshot.canonical_digest,
                    "snapshot_id": snapshot.snapshot_id,
                    "verification_evidence_digest": verification.evidence_digest,
                }),
            )?,
        )
        .await?;
        terminalize_mcp_discovery_finalize_receipt(
            &mut transaction,
            &command,
            "succeeded",
            &snapshot.snapshot_id,
        )
        .await?;
        let record = load_mcp_discovery_operation(
            &mut transaction,
            &command.audit.tenant_id,
            &command.operation_id,
            false,
        )
        .await?;
        transaction.commit().await?;
        Ok(CommandOutcome::Applied(record))
    }

    pub async fn cancel_mcp_discovery_operation(
        &self,
        command: CancelMcpDiscoveryOperation,
    ) -> Result<CommandOutcome<McpDiscoveryOperationRecord>, RepositoryError> {
        command
            .validate_at(Utc::now())
            .map_err(invalid_mcp_discovery)?;
        let mut transaction = self.pool().begin().await?;
        let database_now = database_now(&mut transaction).await?;
        command
            .validate_at(database_now)
            .map_err(invalid_mcp_discovery)?;
        if claim_command_receipt(
            &mut transaction,
            &command.audit,
            "mcp_operation",
            &command.operation_id.to_string(),
            "mcp.discovery.cancel",
        )
        .await?
        {
            let record = load_mcp_discovery_operation(
                &mut transaction,
                &command.audit.tenant_id,
                &command.operation_id,
                false,
            )
            .await?;
            if record.state != McpDiscoveryOperationState::Cancelled {
                return Err(RepositoryError::Conflict("MCP discovery cancel replay"));
            }
            transaction.commit().await?;
            return Ok(CommandOutcome::Replayed(record));
        }
        require_tenant_permission(&mut transaction, &command.audit, Permission::McpWrite).await?;
        let current = load_mcp_discovery_operation(
            &mut transaction,
            &command.audit.tenant_id,
            &command.operation_id,
            true,
        )
        .await?;
        if current.version != command.expected_operation_version
            || !matches!(
                current.state,
                McpDiscoveryOperationState::Pending | McpDiscoveryOperationState::Running
            )
            || current.payload.result.is_some()
        {
            return Err(RepositoryError::Conflict(
                "MCP discovery cancellation first-winner",
            ));
        }
        let job = load_mcp_discovery_job(
            &mut transaction,
            &command.audit.tenant_id,
            &current.job_id,
            true,
        )
        .await?;
        let current_job = mcp_discovery_job_projection(
            &job,
            &command.audit.tenant_id,
            &current.job_id,
            &command.operation_id,
        )?;
        let next_job = decide_job_owner_terminal(&current_job, JobState::Cancelled)?;
        update_owner_terminal_mcp_discovery_job(
            &mut transaction,
            &current,
            &next_job,
            database_now,
        )
        .await?;
        let next_version = current.version.checked_add(1).ok_or_else(|| {
            RepositoryError::InvalidInput("MCP operation version overflow".to_owned())
        })?;
        let affected = sqlx::query(
            r#"
            UPDATE insight_platform.invocations
            SET state = 'cancelled', version = $4, terminal_at = $5, updated_at = $5
            WHERE tenant_id = $1 AND invocation_id = $2 AND version = $3
              AND invocation_kind = 'mcp_discovery' AND state IN ('pending', 'running')
            "#,
        )
        .bind(command.audit.tenant_id.to_string())
        .bind(command.operation_id.to_string())
        .bind(as_i64(current.version, "MCP discovery operation version")?)
        .bind(as_i64(next_version, "MCP discovery operation version")?)
        .bind(database_now)
        .execute(&mut *transaction)
        .await?
        .rows_affected();
        if affected != 1 {
            return Err(RepositoryError::Conflict(
                "MCP discovery cancellation first-winner",
            ));
        }
        append_command_event(
            &mut transaction,
            &command.audit,
            "mcp_operation",
            &command.operation_id.to_string(),
            as_i64(next_version, "MCP discovery operation version")?,
            "mcp.discovery_cancelled",
            &TypedPayload::new(
                1,
                &serde_json::json!({
                    "job_id": current.job_id,
                    "reason_code": command.reason_code,
                }),
            )?,
        )
        .await?;
        terminalize_command_receipt(
            &mut transaction,
            &command.audit,
            &command.operation_id.to_string(),
            "cancelled",
        )
        .await?;
        let record = load_mcp_discovery_operation(
            &mut transaction,
            &command.audit.tenant_id,
            &command.operation_id,
            false,
        )
        .await?;
        transaction.commit().await?;
        Ok(CommandOutcome::Applied(record))
    }

    pub async fn recover_expired_mcp_discovery_job(
        &self,
        command: RecoverExpiredMcpDiscoveryJob,
    ) -> Result<McpDiscoveryOperationRecord, RepositoryError> {
        command.validate().map_err(invalid_mcp_discovery)?;
        let mut transaction = self.pool().begin().await?;
        let database_now = database_now(&mut transaction).await?;
        let current = load_mcp_discovery_operation(
            &mut transaction,
            &command.tenant_id,
            &command.operation_id,
            true,
        )
        .await?;
        if current.job_id != command.job_id
            || current.version != command.observed_operation_version
            || !matches!(
                current.state,
                McpDiscoveryOperationState::Pending | McpDiscoveryOperationState::Running
            )
            || current.payload.result.is_some()
        {
            return Err(RepositoryError::Conflict(
                "MCP discovery recovery observation",
            ));
        }
        let job =
            load_mcp_discovery_job(&mut transaction, &command.tenant_id, &command.job_id, true)
                .await?;
        if job.version != command.observed_job_version
            || job.lease_generation != command.observed_lease_generation
        {
            return Err(RepositoryError::StaleFence);
        }
        let current_job = mcp_discovery_job_projection(
            &job,
            &command.tenant_id,
            &command.job_id,
            &command.operation_id,
        )?;
        let (target_job_state, target_operation_state, retry_at, event_type) = if database_now
            >= current.deadline
        {
            if command.retry_at.is_some() {
                return Err(RepositoryError::InvalidInput(
                    "timed-out MCP discovery recovery cannot retry".to_owned(),
                ));
            }
            (
                JobState::TimedOut,
                McpDiscoveryOperationState::TimedOut,
                None,
                "mcp.discovery_timed_out",
            )
        } else {
            match job.state {
                JobState::Leased => {
                    if command.retry_at.is_some() {
                        return Err(RepositoryError::InvalidInput(
                            "unstarted MCP discovery recovery cannot schedule a retry".to_owned(),
                        ));
                    }
                    (
                        JobState::Ready,
                        McpDiscoveryOperationState::Pending,
                        None,
                        "mcp.discovery_lease_recovered",
                    )
                }
                JobState::Running if job.physical_attempt < job.attempt_limit => {
                    let retry_at = command.retry_at.ok_or_else(|| {
                        RepositoryError::InvalidInput(
                            "MCP discovery retry recovery requires retry_at".to_owned(),
                        )
                    })?;
                    (
                        JobState::RetryScheduled,
                        McpDiscoveryOperationState::Pending,
                        Some(retry_at),
                        "mcp.discovery_retry_recovered",
                    )
                }
                JobState::Running => {
                    if command.retry_at.is_some() {
                        return Err(RepositoryError::InvalidInput(
                            "exhausted MCP discovery recovery cannot retry".to_owned(),
                        ));
                    }
                    (
                        JobState::Failed,
                        McpDiscoveryOperationState::Failed,
                        None,
                        "mcp.discovery_attempts_exhausted",
                    )
                }
                _ => {
                    return Err(RepositoryError::Conflict(
                        "MCP discovery recovery Job state",
                    ));
                }
            }
        };
        let next_job = decide_expired_job_lease(
            &current_job,
            command.observed_job_version,
            command.observed_lease_generation,
            database_now,
            target_job_state,
            retry_at,
        )?;
        update_recovered_mcp_discovery_job(&mut transaction, &command, &next_job, database_now)
            .await?;
        let next_operation_version = current.version.checked_add(1).ok_or_else(|| {
            RepositoryError::InvalidInput("MCP operation version overflow".to_owned())
        })?;
        let terminal_at = target_operation_state.is_terminal().then_some(database_now);
        let affected = sqlx::query(
            r#"
            UPDATE insight_platform.invocations
            SET state = $4, version = $5, terminal_at = $6, updated_at = $7
            WHERE tenant_id = $1 AND invocation_id = $2 AND version = $3
              AND invocation_kind = 'mcp_discovery' AND state IN ('pending', 'running')
            "#,
        )
        .bind(command.tenant_id.to_string())
        .bind(command.operation_id.to_string())
        .bind(as_i64(current.version, "MCP discovery operation version")?)
        .bind(target_operation_state.as_str())
        .bind(as_i64(
            next_operation_version,
            "MCP discovery operation version",
        )?)
        .bind(terminal_at)
        .bind(database_now)
        .execute(&mut *transaction)
        .await?
        .rows_affected();
        if affected != 1 {
            return Err(RepositoryError::Conflict("MCP discovery recovery CAS"));
        }
        append_scheduler_event(
            &mut transaction,
            &command.tenant_id.to_string(),
            &command.event_id,
            &command.outbox_id,
            "mcp_operation",
            &command.operation_id.to_string(),
            as_i64(next_operation_version, "MCP discovery operation version")?,
            None,
            event_type,
            &TypedPayload::new(
                1,
                &serde_json::json!({
                    "job_id": command.job_id,
                    "observed_job_version": command.observed_job_version,
                    "observed_lease_generation": command.observed_lease_generation,
                    "recovered_job_state": next_job.state,
                    "retry_at": next_job.retry_at,
                }),
            )?,
        )
        .await?;
        let record = load_mcp_discovery_operation(
            &mut transaction,
            &command.tenant_id,
            &command.operation_id,
            false,
        )
        .await?;
        transaction.commit().await?;
        Ok(record)
    }

    pub async fn list_expired_mcp_discovery_jobs(
        &self,
        limit: u16,
    ) -> Result<Vec<ExpiredMcpDiscoveryJobObservation>, RepositoryError> {
        if limit == 0 || limit > 256 {
            return Err(RepositoryError::InvalidInput(
                "MCP discovery recovery scan limit is outside the platform bound".to_owned(),
            ));
        }
        let mut transaction = self.pool().begin().await?;
        let observed_at = database_now(&mut transaction).await?;
        let rows = sqlx::query(
            r#"
            SELECT operation.tenant_id, operation.invocation_id, operation.version AS operation_version,
                   job.job_id, job.version AS job_version, job.lease_epoch, job.state,
                   job.attempt_no, job.attempt_limit, job.lease_expires_at, job.deadline
            FROM insight_platform.jobs AS job
            JOIN insight_platform.invocations AS operation
              ON operation.tenant_id = job.tenant_id
             AND operation.invocation_id = job.invocation_id
            WHERE job.work_class = 'mcp' AND job.job_kind = 'mcp_discovery'
              AND job.owner_kind = 'mcp_operation'
              AND job.owner_id = operation.invocation_id
              AND operation.invocation_kind = 'mcp_discovery'
              AND operation.state IN ('pending', 'running')
              AND job.state IN ('leased', 'running')
              AND job.lease_expires_at <= $1
            ORDER BY job.lease_expires_at, job.tenant_id, job.job_id
            LIMIT $2
            "#,
        )
        .bind(observed_at)
        .bind(i64::from(limit))
        .fetch_all(&mut *transaction)
        .await?;
        let mut observations = Vec::with_capacity(rows.len());
        for row in rows {
            let observation = ExpiredMcpDiscoveryJobObservation {
                tenant_id: parse_resource_id_column(&row, "tenant_id")?,
                operation_id: parse_resource_id_column(&row, "invocation_id")?,
                job_id: parse_resource_id_column(&row, "job_id")?,
                operation_version: parse_positive_u64_column(&row, "operation_version")?,
                job_version: parse_positive_u64_column(&row, "job_version")?,
                lease_generation: parse_positive_u64_column(&row, "lease_epoch")?,
                physical_attempt: u32::try_from(row.try_get::<i32, _>("attempt_no")?).map_err(
                    |_| {
                        RepositoryError::CorruptRow(
                            "negative MCP discovery physical attempt".to_owned(),
                        )
                    },
                )?,
                attempt_limit: u32::try_from(row.try_get::<i32, _>("attempt_limit")?).map_err(
                    |_| {
                        RepositoryError::CorruptRow(
                            "negative MCP discovery attempt limit".to_owned(),
                        )
                    },
                )?,
                job_state: row
                    .try_get::<String, _>("state")?
                    .parse::<JobState>()
                    .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?,
                lease_expires_at: row.try_get("lease_expires_at")?,
                deadline: row.try_get("deadline")?,
                observed_at,
            };
            observation
                .validate()
                .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
            observations.push(observation);
        }
        transaction.commit().await?;
        Ok(observations)
    }
}

async fn claim_mcp_discovery_create_receipt(
    transaction: &mut Transaction<'_, Postgres>,
    command: &CreateMcpDiscoveryOperation,
) -> Result<Option<ResourceId>, RepositoryError> {
    let scope_id = command.mcp_deployment.deployment_id.to_string();
    let payload = TypedPayload::with_limit(
        1,
        &serde_json::json!({
            "operation": "mcp.discovery.create",
            "principal_id": command.audit.principal_id,
            "scope_id": scope_id,
            "scope_kind": "mcp_deployment",
        }),
        65_536,
    )?;
    let inserted = sqlx::query(
        r#"
        INSERT INTO insight_platform.receipts (
            tenant_id, receipt_id, receipt_kind, scope_kind, scope_id,
            dedupe_owner_id, operation, idempotency_key_digest, request_digest,
            state, payload_schema_version, payload, payload_digest, expires_at
        ) VALUES ($1, $2, 'command', 'mcp_deployment', $3, $4,
                  'mcp.discovery.create', $5, $6, 'processing', $7, $8, $9, $10)
        ON CONFLICT (
            tenant_id, receipt_kind, scope_kind, scope_id, dedupe_owner_id,
            operation, idempotency_key_digest
        ) DO NOTHING
        RETURNING receipt_id
        "#,
    )
    .bind(command.audit.tenant_id.to_string())
    .bind(command.audit.receipt_id.to_string())
    .bind(&scope_id)
    .bind(command.audit.principal_id.to_string())
    .bind(command.audit.idempotency_key_digest.to_string())
    .bind(command.audit.request_digest.to_string())
    .bind(payload.schema_version)
    .bind(&payload.value)
    .bind(&payload.digest)
    .bind(command.audit.receipt_expires_at)
    .fetch_optional(&mut **transaction)
    .await?;
    if inserted.is_some() {
        return Ok(None);
    }
    let row = sqlx::query(
        r#"
        SELECT request_digest, state, response_reference_id
        FROM insight_platform.receipts
        WHERE tenant_id = $1 AND receipt_kind = 'command'
          AND scope_kind = 'mcp_deployment' AND scope_id = $2
          AND dedupe_owner_id = $3 AND operation = 'mcp.discovery.create'
          AND idempotency_key_digest = $4
        FOR UPDATE
        "#,
    )
    .bind(command.audit.tenant_id.to_string())
    .bind(&scope_id)
    .bind(command.audit.principal_id.to_string())
    .bind(command.audit.idempotency_key_digest.to_string())
    .fetch_one(&mut **transaction)
    .await?;
    if row.try_get::<String, _>("request_digest")? != command.audit.request_digest.to_string() {
        return Err(RepositoryError::IdempotencyConflict);
    }
    if row.try_get::<String, _>("state")? != "succeeded" {
        return Err(RepositoryError::Conflict("MCP discovery Receipt"));
    }
    let operation_id = row
        .try_get::<Option<String>, _>("response_reference_id")?
        .ok_or_else(|| {
            RepositoryError::CorruptRow("MCP discovery Receipt has no result".to_owned())
        })?
        .parse::<ResourceId>()
        .map_err(|_| {
            RepositoryError::CorruptRow("MCP discovery Receipt result is invalid".to_owned())
        })?;
    if operation_id.kind() != insight_platform_contracts::ResourceKind::McpOperation {
        return Err(RepositoryError::CorruptRow(
            "MCP discovery Receipt result has the wrong kind".to_owned(),
        ));
    }
    Ok(Some(operation_id))
}

impl PgRepository {
    pub async fn admit_context_subscription_refresh(
        &self,
        command: AdmitContextSubscriptionRefresh,
    ) -> Result<AcceptedContextSubscriptionRefresh, RepositoryError> {
        command
            .validate_at(Utc::now())
            .map_err(invalid_context_subscription_admission)?;
        let mut transaction = self.pool().begin().await?;
        let database_now = database_now(&mut transaction).await?;
        command
            .validate_at(database_now)
            .map_err(invalid_context_subscription_admission)?;

        if let Some(accepted) =
            claim_context_subscription_admission_receipt(&mut transaction, &command, database_now)
                .await?
        {
            accepted
                .validate_for(&command.request, database_now)
                .map_err(invalid_context_subscription_admission)?;
            transaction.commit().await?;
            return Ok(accepted);
        }

        let record = load_mcp_subscription(
            &mut transaction,
            &command.request.tenant_id,
            &command.request.subscription_id,
            true,
            database_now,
        )
        .await?;
        validate_context_subscription_admission_source(
            &mut transaction,
            &record,
            &command,
            database_now,
        )
        .await?;

        let job_id = new_internal_id(ResourceKind::Job)?;
        let event_id = new_internal_id(ResourceKind::Event)?;
        let outbox_id = new_internal_id(ResourceKind::OutboxEvent)?;
        let job_payload =
            ContextSubscriptionRefreshJobPayload::from_request(command.request.clone());
        job_payload
            .validate_at(database_now)
            .map_err(invalid_context_subscription_admission)?;
        let durable_work_digest = job_payload
            .canonical_digest()
            .map_err(invalid_context_subscription_admission)?;
        let typed_job = TypedPayload::from_versioned(1, &job_payload, 1_048_576)?;
        let attempt_limit = i32::try_from(self.context_query_limits().maximum_attempts())
            .map_err(|_| RepositoryError::InvalidInput("Context attempt limit".to_owned()))?;
        sqlx::query(
            r#"
            INSERT INTO insight_platform.jobs (
                tenant_id, job_id, job_kind, work_class, owner_kind, owner_id, invocation_id,
                state, version, attempt_no, attempt_limit, lease_epoch, scheduled_at,
                deadline, priority, request_digest, payload_schema_version, payload,
                payload_digest, created_at, updated_at, trace_id
            ) VALUES ($1, $2, 'context_subscription_refresh', 'context', 'mcp_operation', $3, $3,
                      'ready', 1, 0, $4, 0, $5, $6, 0, $7, $8, $9, $10, $5, $5, $11)
            "#,
        )
        .bind(command.request.tenant_id.to_string())
        .bind(job_id.to_string())
        .bind(command.request.subscription_id.to_string())
        .bind(attempt_limit)
        .bind(database_now)
        .bind(command.request.deadline)
        .bind(command.request.request_digest.to_string())
        .bind(typed_job.schema_version)
        .bind(&typed_job.value)
        .bind(&typed_job.digest)
        .bind(command.audit.trace.trace_id.to_string())
        .execute(&mut *transaction)
        .await?;

        let accepted = AcceptedContextSubscriptionRefresh {
            schema_version: insight_platform_context::CONTEXT_SUBSCRIPTION_ADMISSION_SCHEMA_VERSION,
            request_digest: command.request.request_digest.clone(),
            durable_work_digest,
            job_id,
            accepted_at: database_now,
        };
        accepted
            .validate_for(&command.request, database_now)
            .map_err(invalid_context_subscription_admission)?;
        append_scheduler_event(
            &mut transaction,
            &command.request.tenant_id.to_string(),
            &event_id,
            &outbox_id,
            "mcp_operation",
            &command.request.subscription_id.to_string(),
            as_i64(record.version, "MCP subscription version")?,
            None,
            "context.subscription_refresh_admitted",
            &TypedPayload::new(
                1,
                &serde_json::json!({
                    "context_deployment": command.request.context_deployment,
                    "durable_work_digest": accepted.durable_work_digest,
                    "job_id": accepted.job_id,
                    "request_digest": accepted.request_digest,
                    "subscription_id": command.request.subscription_id,
                }),
            )?,
        )
        .await?;
        complete_context_subscription_admission_receipt(&mut transaction, &command, &accepted)
            .await?;
        transaction.commit().await?;
        Ok(accepted)
    }

    pub async fn scan_claimable_context_subscription_refresh_jobs(
        &self,
        worker_manifest_digest: &Sha256Digest,
        limit: u16,
    ) -> Result<Vec<ClaimableContextSubscriptionRefreshJob>, RepositoryError> {
        if limit == 0 || limit > CONTEXT_SUBSCRIPTION_REFRESH_MAX_BATCH {
            return Err(RepositoryError::InvalidInput(
                "Context subscription refresh scan limit is invalid".to_owned(),
            ));
        }
        let rows = sqlx::query(
            r#"
            SELECT job.tenant_id, job.job_id
            FROM insight_platform.jobs AS job
            JOIN insight_platform.deployments AS deployment
              ON deployment.tenant_id = job.tenant_id
             AND deployment.deployment_id = job.payload #>> '{request,context_deployment,deployment_id}'
            WHERE job.work_class = 'context'
              AND job.job_kind = 'context_subscription_refresh'
              AND job.owner_kind = 'mcp_operation'
              AND job.state IN ('ready', 'retry_scheduled')
              AND job.scheduled_at <= clock_timestamp()
              AND COALESCE(job.retry_at, job.scheduled_at) <= clock_timestamp()
              AND job.deadline > clock_timestamp()
              AND job.attempt_no < job.attempt_limit
              AND deployment.bindings #>> '{bindings,required_worker_manifest_digest}' = $2
            ORDER BY job.priority DESC, COALESCE(job.retry_at, job.scheduled_at), job.job_id
            LIMIT $1
            "#,
        )
        .bind(i64::from(limit))
        .bind(worker_manifest_digest.to_string())
        .fetch_all(self.pool())
        .await?;
        rows.into_iter()
            .map(|row| {
                Ok(ClaimableContextSubscriptionRefreshJob {
                    tenant_id: row.try_get::<String, _>("tenant_id")?.parse().map_err(
                        |failure: insight_platform_contracts::ResourceIdError| {
                            RepositoryError::CorruptRow(failure.to_string())
                        },
                    )?,
                    job_id: row.try_get::<String, _>("job_id")?.parse().map_err(
                        |failure: insight_platform_contracts::ResourceIdError| {
                            RepositoryError::CorruptRow(failure.to_string())
                        },
                    )?,
                })
            })
            .collect()
    }

    pub async fn claim_context_subscription_refresh_jobs(
        &self,
        command: ClaimContextSubscriptionRefreshJobs,
    ) -> Result<Vec<ClaimedContextSubscriptionRefresh>, RepositoryError> {
        command.validate()?;
        let mut transaction = self.pool().begin().await?;
        let database_now = database_now(&mut transaction).await?;
        let mut slots = command.slots.iter().collect::<Vec<_>>();
        slots.sort_by(|left, right| {
            (&left.tenant_id, &left.job_id).cmp(&(&right.tenant_id, &right.job_id))
        });
        let mut claimed = Vec::with_capacity(slots.len());
        for slot in slots {
            let observed = load_context_subscription_refresh_job(
                &mut transaction,
                &slot.tenant_id,
                &slot.job_id,
                false,
            )
            .await?;
            let payload: ContextSubscriptionRefreshJobPayload =
                decode_versioned_payload(&observed.payload, "Context subscription refresh Job")?;
            payload
                .validate_at(database_now)
                .map_err(invalid_context_subscription_admission)?;
            if observed.work_class != WorkClass::Context.as_str()
                || observed.owner_kind != ResourceKind::McpOperation.descriptor().name
                || observed.owner_id != payload.request.subscription_id.to_string()
                || observed.request_digest != payload.request.request_digest.to_string()
            {
                return Err(RepositoryError::CorruptRow(
                    "Context subscription refresh Job binding drifted".to_owned(),
                ));
            }
            let subscription = load_mcp_subscription(
                &mut transaction,
                &slot.tenant_id,
                &payload.request.subscription_id,
                true,
                database_now,
            )
            .await?;
            let required_manifest = validate_context_subscription_refresh_source(
                &mut transaction,
                &subscription,
                &payload.request,
                ContextSubscriptionRefreshSourceEvidence::AcceptedJob(&slot.job_id),
                database_now,
            )
            .await?;
            if required_manifest != command.worker_manifest_digest {
                return Err(RepositoryError::Conflict(
                    "Context subscription refresh Worker manifest",
                ));
            }
            let current = load_context_subscription_refresh_job(
                &mut transaction,
                &slot.tenant_id,
                &slot.job_id,
                true,
            )
            .await?;
            require_same_context_subscription_refresh_job(&observed, &current)?;
            let projection = job_projection(&current)?;
            let leased = decide_job_claim(
                &projection,
                database_now,
                command.worker_process_generation_id.clone(),
                slot.lease_token_digest.clone(),
                command.lease_policy,
            )
            .map_err(map_job_decision_error)?;
            let lease_fence = JobFence {
                expected_version: leased.version,
                worker_process_generation_id: command.worker_process_generation_id.clone(),
                lease_generation: leased.lease_generation,
                token_digest: slot.lease_token_digest.clone(),
            };
            let running = decide_job_start(&leased, &lease_fence, database_now)
                .map_err(map_job_decision_error)?;
            let quota_account_id = reserve_context_subscription_refresh_quota(
                &mut transaction,
                &current,
                slot,
                &running,
            )
            .await?;
            let job = update_context_subscription_refresh_job(
                &mut transaction,
                &current,
                &running,
                Some(&slot.quota_reservation_id),
                None,
                database_now,
            )
            .await?;
            let attempt = ContextSubscriptionRefreshAttempt {
                schema_version:
                    insight_platform_context::CONTEXT_SUBSCRIPTION_REFRESH_EXECUTION_SCHEMA_VERSION,
                job_id: slot.job_id.clone(),
                worker_process_generation_id: command.worker_process_generation_id.clone(),
                job_fence: JobFence {
                    expected_version: running.version,
                    worker_process_generation_id: command.worker_process_generation_id.clone(),
                    lease_generation: running.lease_generation,
                    token_digest: slot.lease_token_digest.clone(),
                },
                attempt_number: running.attempt_count,
                request: payload.request,
            };
            attempt
                .validate_at(database_now)
                .map_err(invalid_context_subscription_execution)?;
            append_scheduler_event(
                &mut transaction,
                &current.tenant_id,
                &slot.event_id,
                &slot.outbox_id,
                "job",
                &current.job_id,
                as_i64(running.version, "Context refresh Job version")?,
                None,
                "context.subscription_refresh.started",
                &TypedPayload::new(
                    1,
                    &serde_json::json!({
                        "attempt_count": running.attempt_count,
                        "job_id": running.job_id,
                        "lease_generation": running.lease_generation,
                        "quota_account_id": quota_account_id,
                        "worker_process_generation_id": command.worker_process_generation_id,
                    }),
                )?,
            )
            .await?;
            claimed.push(ClaimedContextSubscriptionRefresh {
                job,
                attempt,
                quota_account_id,
            });
        }
        transaction.commit().await?;
        Ok(claimed)
    }

    pub async fn commit_context_subscription_refresh(
        &self,
        command: CommitContextSubscriptionRefresh,
    ) -> Result<crate::repository::JobRecord, RepositoryError> {
        command.validate_at(Utc::now())?;
        let mut transaction = self.pool().begin().await?;
        let database_now = database_now(&mut transaction).await?;
        command.validate_at(database_now)?;
        let response_digest = command
            .response
            .canonical_digest()
            .map_err(invalid_context_subscription_execution)?;
        if claim_context_subscription_refresh_commit_receipt(
            &mut transaction,
            &command,
            &response_digest,
            database_now,
        )
        .await?
        {
            let job = load_job_for_update_by_text(
                &mut transaction,
                &command.attempt.request.tenant_id.to_string(),
                &command.attempt.job_id.to_string(),
            )
            .await?;
            transaction.commit().await?;
            return Ok(job);
        }
        let observed = load_context_subscription_refresh_job(
            &mut transaction,
            &command.attempt.request.tenant_id,
            &command.attempt.job_id,
            false,
        )
        .await?;
        let payload: ContextSubscriptionRefreshJobPayload =
            decode_versioned_payload(&observed.payload, "Context subscription refresh Job")?;
        if payload.request != command.attempt.request {
            return Err(RepositoryError::StaleFence);
        }
        let subscription = load_mcp_subscription(
            &mut transaction,
            &command.attempt.request.tenant_id,
            &command.attempt.request.subscription_id,
            true,
            database_now,
        )
        .await?;
        validate_context_subscription_refresh_source(
            &mut transaction,
            &subscription,
            &command.attempt.request,
            ContextSubscriptionRefreshSourceEvidence::AcceptedJob(&command.attempt.job_id),
            database_now,
        )
        .await?;
        let current = load_context_subscription_refresh_job(
            &mut transaction,
            &command.attempt.request.tenant_id,
            &command.attempt.job_id,
            true,
        )
        .await?;
        require_same_context_subscription_refresh_job(&observed, &current)?;
        require_context_subscription_refresh_fence(&current, &command.attempt)?;
        let projection = job_projection(&current)?;
        let (next, disposition, event_type, result_digest) =
            decide_context_subscription_refresh_outcome(
                &projection,
                &command,
                &response_digest,
                database_now,
            )?;
        settle_context_subscription_refresh_quota(
            &mut transaction,
            &current,
            &command.quota_settlement_entry_id,
            &response_digest,
        )
        .await?;
        let job = update_context_subscription_refresh_job(
            &mut transaction,
            &current,
            &next,
            current
                .quota_reservation_id
                .as_deref()
                .map(|value| value.parse::<ResourceId>())
                .transpose()
                .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?
                .as_ref(),
            Some(&result_digest),
            database_now,
        )
        .await?;
        append_scheduler_event(
            &mut transaction,
            &current.tenant_id,
            &command.event_id,
            &command.outbox_id,
            "job",
            &current.job_id,
            as_i64(next.version, "Context refresh Job version")?,
            None,
            event_type,
            &TypedPayload::new(
                1,
                &serde_json::json!({
                    "attempt_count": next.attempt_count,
                    "job_id": next.job_id,
                    "lease_generation": next.lease_generation,
                    "result_digest": result_digest,
                    "state": next.state,
                }),
            )?,
        )
        .await?;
        complete_context_subscription_refresh_commit_receipt(
            &mut transaction,
            &command,
            &response_digest,
            disposition,
            database_now,
        )
        .await?;
        transaction.commit().await?;
        Ok(job)
    }

    pub async fn drive_expired_context_subscription_refresh_jobs(
        &self,
        command: DriveExpiredContextSubscriptionRefreshJobs,
    ) -> Result<Vec<crate::repository::JobRecord>, RepositoryError> {
        command.validate()?;
        let mut transaction = self.pool().begin().await?;
        let database_now = database_now(&mut transaction).await?;
        let rows = sqlx::query(
            r#"
            SELECT job.*
            FROM insight_platform.jobs AS job
            WHERE job.work_class = 'context'
              AND job.job_kind = 'context_subscription_refresh'
              AND job.owner_kind = 'mcp_operation'
              AND job.state IN ('leased', 'running')
              AND job.lease_expires_at <= $1 AND job.terminal_at IS NULL
              AND mod(('x' || right(job.job_id, 8))::bit(32)::bigint, $4) = $3
            ORDER BY job.lease_expires_at, job.tenant_id, job.job_id
            LIMIT $2
            "#,
        )
        .bind(database_now)
        .bind(i64::from(command.limit))
        .bind(i64::from(command.shard_index))
        .bind(i64::from(command.shard_count))
        .fetch_all(&mut *transaction)
        .await?;
        let mut recovered = Vec::with_capacity(rows.len());
        for (row, slot) in rows.into_iter().zip(&command.slots) {
            let observed = job_from_row(row)?;
            let observed_job_id = observed.job_id.parse::<ResourceId>().map_err(
                |failure: insight_platform_contracts::ResourceIdError| {
                    RepositoryError::CorruptRow(failure.to_string())
                },
            )?;
            let payload: ContextSubscriptionRefreshJobPayload =
                decode_versioned_payload(&observed.payload, "Context subscription refresh Job")?;
            let subscription = load_mcp_subscription(
                &mut transaction,
                &payload.request.tenant_id,
                &payload.request.subscription_id,
                true,
                database_now,
            )
            .await?;
            validate_context_subscription_refresh_source(
                &mut transaction,
                &subscription,
                &payload.request,
                ContextSubscriptionRefreshSourceEvidence::AcceptedJob(&observed_job_id),
                database_now,
            )
            .await?;
            let current = load_context_subscription_refresh_job(
                &mut transaction,
                &payload.request.tenant_id,
                &observed_job_id,
                true,
            )
            .await?;
            require_same_context_subscription_refresh_job(&observed, &current)?;
            let projection = job_projection(&current)?;
            let retry_at = database_now
                .checked_add_signed(chrono::Duration::milliseconds(
                    i64::try_from(command.retry_backoff_milliseconds).map_err(|_| {
                        RepositoryError::InvalidInput(
                            "Context refresh recovery backoff overflow".to_owned(),
                        )
                    })?,
                ))
                .ok_or_else(|| {
                    RepositoryError::InvalidInput(
                        "Context refresh recovery backoff overflow".to_owned(),
                    )
                })?;
            let target = if database_now >= projection.deadline {
                JobState::TimedOut
            } else if projection.state == JobState::Leased {
                JobState::Ready
            } else if projection.attempt_count < projection.attempt_limit
                && retry_at < projection.deadline
            {
                JobState::RetryScheduled
            } else {
                JobState::Failed
            };
            let next = decide_expired_job_lease(
                &projection,
                projection.version,
                projection.lease_generation,
                database_now,
                target,
                (target == JobState::RetryScheduled).then_some(retry_at),
            )
            .map_err(map_job_decision_error)?;
            settle_context_subscription_refresh_quota(
                &mut transaction,
                &current,
                &slot.quota_settlement_entry_id,
                &payload.request.request_digest,
            )
            .await?;
            let result_digest = matches!(target, JobState::Failed | JobState::TimedOut)
                .then_some(&payload.request.request_digest);
            let job = update_context_subscription_refresh_job(
                &mut transaction,
                &current,
                &next,
                None,
                result_digest,
                database_now,
            )
            .await?;
            append_scheduler_event(
                &mut transaction,
                &current.tenant_id,
                &slot.event_id,
                &slot.outbox_id,
                "job",
                &current.job_id,
                as_i64(next.version, "Context refresh Job version")?,
                None,
                "context.subscription_refresh.lease_recovered",
                &TypedPayload::new(
                    1,
                    &serde_json::json!({
                        "attempt_count": next.attempt_count,
                        "job_id": next.job_id,
                        "lease_generation": next.lease_generation,
                        "state": next.state,
                    }),
                )?,
            )
            .await?;
            recovered.push(job);
        }
        transaction.commit().await?;
        Ok(recovered)
    }
}

async fn claim_context_subscription_admission_receipt(
    transaction: &mut Transaction<'_, Postgres>,
    command: &AdmitContextSubscriptionRefresh,
    database_now: DateTime<Utc>,
) -> Result<Option<AcceptedContextSubscriptionRefresh>, RepositoryError> {
    let receipt_id = new_internal_id(ResourceKind::Receipt)?;
    let payload = TypedPayload::new(
        1,
        &serde_json::json!({
            "correlation_digest": command.audit.correlation_digest,
            "request_id": command.audit.request_id,
        }),
    )?;
    let inserted = sqlx::query(
        r#"
        INSERT INTO insight_platform.receipts (
            tenant_id, receipt_id, receipt_kind, scope_kind, scope_id,
            dedupe_owner_id, operation, idempotency_key_digest, request_digest,
            state, payload_schema_version, payload, payload_digest, expires_at,
            created_at
        ) VALUES ($1, $2, 'command', 'mcp_operation', $3, $3,
                  'context.subscription_refresh.admit', $4, $4, 'processing',
                  $5, $6, $7, $8, $9)
        ON CONFLICT (
            tenant_id, receipt_kind, scope_kind, scope_id, dedupe_owner_id,
            operation, idempotency_key_digest
        ) DO NOTHING
        RETURNING receipt_id
        "#,
    )
    .bind(command.request.tenant_id.to_string())
    .bind(receipt_id.to_string())
    .bind(command.request.subscription_id.to_string())
    .bind(command.request.request_digest.to_string())
    .bind(payload.schema_version)
    .bind(&payload.value)
    .bind(&payload.digest)
    .bind(command.request.deadline)
    .bind(database_now)
    .fetch_optional(&mut **transaction)
    .await?;
    if inserted.is_some() {
        return Ok(None);
    }
    let row = sqlx::query(
        r#"
        SELECT request_digest, state, response_reference_id,
               payload_schema_version, payload, payload_digest
        FROM insight_platform.receipts
        WHERE tenant_id = $1 AND receipt_kind = 'command'
          AND scope_kind = 'mcp_operation' AND scope_id = $2
          AND dedupe_owner_id = $2
          AND operation = 'context.subscription_refresh.admit'
          AND idempotency_key_digest = $3
        FOR UPDATE
        "#,
    )
    .bind(command.request.tenant_id.to_string())
    .bind(command.request.subscription_id.to_string())
    .bind(command.request.request_digest.to_string())
    .fetch_one(&mut **transaction)
    .await?;
    if row.try_get::<String, _>("request_digest")? != command.request.request_digest.to_string()
        || row.try_get::<String, _>("state")? != "succeeded"
    {
        return Err(RepositoryError::IdempotencyConflict);
    }
    let stored = payload_from_row(&row, "payload_schema_version", "payload", "payload_digest")?;
    let accepted: AcceptedContextSubscriptionRefresh =
        decode_versioned_payload(&stored, "Context subscription admission Receipt")?;
    if row.try_get::<Option<String>, _>("response_reference_id")?
        != Some(accepted.job_id.to_string())
    {
        return Err(RepositoryError::CorruptRow(
            "Context subscription admission Receipt result drifted".to_owned(),
        ));
    }
    Ok(Some(accepted))
}

async fn validate_context_subscription_admission_source(
    transaction: &mut Transaction<'_, Postgres>,
    record: &McpSubscriptionRecord,
    command: &AdmitContextSubscriptionRefresh,
    database_now: DateTime<Utc>,
) -> Result<(), RepositoryError> {
    validate_context_subscription_refresh_source(
        transaction,
        record,
        &command.request,
        ContextSubscriptionRefreshSourceEvidence::Pending,
        database_now,
    )
    .await
    .map(|_| ())
}

enum ContextSubscriptionRefreshSourceEvidence<'a> {
    Pending,
    AcceptedJob(&'a ResourceId),
}

async fn validate_context_subscription_refresh_source(
    transaction: &mut Transaction<'_, Postgres>,
    record: &McpSubscriptionRecord,
    request: &insight_platform_context::ContextSubscriptionRefreshRequest,
    source_evidence: ContextSubscriptionRefreshSourceEvidence<'_>,
    database_now: DateTime<Utc>,
) -> Result<Sha256Digest, RepositoryError> {
    let binding = &record.payload.binding;
    if record.tenant_id != request.tenant_id
        || record.subscription_id != request.subscription_id
        || record.state != McpSubscriptionState::Active
        || !matches!(
            record.payload.session.state,
            McpSessionState::Ready | McpSessionState::Degraded
        )
        || record.deadline != request.deadline
        || binding.context_deployment != request.context_deployment
        || binding.mcp_deployment != request.mcp_deployment
        || binding.discovery_snapshot_id != request.discovery_snapshot_id
        || binding.discovery_snapshot_digest != request.discovery_snapshot_digest
        || binding.resource_uri != request.resource_uri
        || binding.resource_uri_digest != request.resource_uri_digest
        || binding.authorization_generation != request.authorization_generation
        || record.payload.session.generation != request.session_generation
    {
        return Err(RepositoryError::Conflict(
            "Context subscription admission source",
        ));
    }
    match source_evidence {
        ContextSubscriptionRefreshSourceEvidence::Pending => match &request.cause {
            ContextSubscriptionRefreshCause::FullReconcile {
                observed_subscription_version,
            } => {
                if record.payload.pending_invalidation.is_some()
                    || *observed_subscription_version != record.version
                    || request.event_generation != record.version
                    || request.event_key_digest != binding.resource_uri_digest
                    || request.body_digest != binding.discovery_snapshot_digest
                {
                    return Err(RepositoryError::Conflict(
                        "Context subscription reconcile evidence",
                    ));
                }
            }
            cause => {
                let pending = record.payload.pending_invalidation.as_ref().ok_or(
                    RepositoryError::Conflict("Context subscription notification evidence"),
                )?;
                let cause_matches = matches!(
                    (cause, pending.class),
                    (
                        ContextSubscriptionRefreshCause::ResourceUpdated,
                        insight_platform_mcp_host::McpNotificationClass::ResourceUpdated
                    ) | (
                        ContextSubscriptionRefreshCause::ResourceListChanged,
                        insight_platform_mcp_host::McpNotificationClass::ResourceListChanged
                    ) | (
                        ContextSubscriptionRefreshCause::ToolListChanged,
                        insight_platform_mcp_host::McpNotificationClass::ToolListChanged
                    ) | (
                        ContextSubscriptionRefreshCause::PromptListChanged,
                        insight_platform_mcp_host::McpNotificationClass::PromptListChanged
                    )
                );
                if !cause_matches
                    || pending.session_generation != request.session_generation
                    || pending.event_generation != request.event_generation
                    || pending.event_key_digest != request.event_key_digest
                    || pending.body_digest != request.body_digest
                    || (pending.class
                        == insight_platform_mcp_host::McpNotificationClass::ResourceUpdated
                        && pending.resource_uri_digest.as_ref()
                            != Some(&request.resource_uri_digest))
                {
                    return Err(RepositoryError::Conflict(
                        "Context subscription notification evidence",
                    ));
                }
            }
        },
        ContextSubscriptionRefreshSourceEvidence::AcceptedJob(job_id) => {
            require_context_subscription_refresh_admission_receipt(transaction, request, job_id)
                .await?;
        }
    }

    let contract = resolve_mcp_execution_contract(
        transaction,
        &McpExecutionContractQuery {
            schema_version: 1,
            tenant_id: binding.tenant_id.clone(),
            mcp_deployment: binding.mcp_deployment.clone(),
            discovery_snapshot_id: binding.discovery_snapshot_id.clone(),
            discovery_snapshot_digest: binding.discovery_snapshot_digest.clone(),
            authorization_binding_id: binding.authorization_binding_id.clone(),
            authorization_generation: binding.authorization_generation,
            authorization_context_digest: binding.authorization_context_digest.clone(),
            principal_id: binding.principal_id.clone(),
        },
        database_now,
    )
    .await?;
    validate_mcp_subscription_context_binding(
        transaction,
        &request.tenant_id,
        &request.context_deployment,
        &contract,
    )
    .await
}

async fn require_context_subscription_refresh_admission_receipt(
    transaction: &mut Transaction<'_, Postgres>,
    request: &insight_platform_context::ContextSubscriptionRefreshRequest,
    job_id: &ResourceId,
) -> Result<(), RepositoryError> {
    let row = sqlx::query(
        r#"
        SELECT request_digest, state, response_reference_id,
               payload_schema_version, payload, payload_digest
        FROM insight_platform.receipts
        WHERE tenant_id = $1 AND receipt_kind = 'command'
          AND scope_kind = 'mcp_operation' AND scope_id = $2
          AND dedupe_owner_id = $2
          AND operation = 'context.subscription_refresh.admit'
          AND idempotency_key_digest = $3
        FOR SHARE
        "#,
    )
    .bind(request.tenant_id.to_string())
    .bind(request.subscription_id.to_string())
    .bind(request.request_digest.to_string())
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::Conflict(
        "Context subscription refresh admission Receipt",
    ))?;
    if row.try_get::<String, _>("request_digest")? != request.request_digest.to_string()
        || row.try_get::<String, _>("state")? != "succeeded"
        || row.try_get::<Option<String>, _>("response_reference_id")? != Some(job_id.to_string())
    {
        return Err(RepositoryError::Conflict(
            "Context subscription refresh admission Receipt",
        ));
    }
    let stored = payload_from_row(&row, "payload_schema_version", "payload", "payload_digest")?;
    let accepted: AcceptedContextSubscriptionRefresh =
        decode_versioned_payload(&stored, "Context subscription refresh admission Receipt")?;
    if accepted.job_id != *job_id || accepted.request_digest != request.request_digest {
        return Err(RepositoryError::Conflict(
            "Context subscription refresh admission Receipt",
        ));
    }
    Ok(())
}

async fn complete_context_subscription_admission_receipt(
    transaction: &mut Transaction<'_, Postgres>,
    command: &AdmitContextSubscriptionRefresh,
    accepted: &AcceptedContextSubscriptionRefresh,
) -> Result<(), RepositoryError> {
    let payload = TypedPayload::from_versioned(1, accepted, 262_144)?;
    let affected = sqlx::query(
        r#"
        UPDATE insight_platform.receipts
        SET state = 'succeeded', disposition = 'scheduled', response_reference_id = $4,
            payload_schema_version = $5, payload = $6, payload_digest = $7,
            completed_at = $8
        WHERE tenant_id = $1 AND receipt_kind = 'command'
          AND scope_kind = 'mcp_operation' AND scope_id = $2
          AND dedupe_owner_id = $2
          AND operation = 'context.subscription_refresh.admit'
          AND idempotency_key_digest = $3 AND request_digest = $3
          AND state = 'processing'
        "#,
    )
    .bind(command.request.tenant_id.to_string())
    .bind(command.request.subscription_id.to_string())
    .bind(command.request.request_digest.to_string())
    .bind(accepted.job_id.to_string())
    .bind(payload.schema_version)
    .bind(&payload.value)
    .bind(&payload.digest)
    .bind(accepted.accepted_at)
    .execute(&mut **transaction)
    .await?
    .rows_affected();
    if affected != 1 {
        return Err(RepositoryError::Conflict(
            "Context subscription admission Receipt",
        ));
    }
    Ok(())
}

fn new_internal_id(kind: ResourceKind) -> Result<ResourceId, RepositoryError> {
    ResourceId::from_uuid_v7(kind, Uuid::now_v7())
        .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))
}

fn invalid_context_subscription_admission(
    failure: ContextSubscriptionAdmissionError,
) -> RepositoryError {
    RepositoryError::InvalidInput(failure.to_string())
}

fn invalid_context_subscription_execution(
    failure: ContextSubscriptionExecutionError,
) -> RepositoryError {
    RepositoryError::InvalidInput(failure.to_string())
}

fn map_job_decision_error(failure: insight_platform_jobs::JobError) -> RepositoryError {
    match failure {
        insight_platform_jobs::JobError::LeaseExpired => RepositoryError::LeaseExpired,
        insight_platform_jobs::JobError::StaleFence => RepositoryError::StaleFence,
        _ => RepositoryError::Conflict("Context subscription refresh Job transition"),
    }
}

async fn load_context_subscription_refresh_job(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    job_id: &ResourceId,
    for_update: bool,
) -> Result<crate::repository::JobRecord, RepositoryError> {
    let row = if for_update {
        sqlx::query(
            r#"
            SELECT * FROM insight_platform.jobs
            WHERE tenant_id = $1 AND job_id = $2
              AND work_class = 'context'
              AND job_kind = 'context_subscription_refresh'
              AND owner_kind = 'mcp_operation'
            FOR UPDATE
            "#,
        )
        .bind(tenant_id.to_string())
        .bind(job_id.to_string())
        .fetch_optional(&mut **transaction)
        .await?
    } else {
        sqlx::query(
            r#"
            SELECT * FROM insight_platform.jobs
            WHERE tenant_id = $1 AND job_id = $2
              AND work_class = 'context'
              AND job_kind = 'context_subscription_refresh'
              AND owner_kind = 'mcp_operation'
            "#,
        )
        .bind(tenant_id.to_string())
        .bind(job_id.to_string())
        .fetch_optional(&mut **transaction)
        .await?
    }
    .ok_or(RepositoryError::NotFound(
        "Context subscription refresh Job",
    ))?;
    job_from_row(row)
}

fn require_same_context_subscription_refresh_job(
    observed: &crate::repository::JobRecord,
    current: &crate::repository::JobRecord,
) -> Result<(), RepositoryError> {
    if observed.version != current.version
        || observed.state != current.state
        || observed.lease_epoch != current.lease_epoch
        || observed.worker_id != current.worker_id
        || observed.lease_token_digest != current.lease_token_digest
        || observed.quota_reservation_id != current.quota_reservation_id
        || observed.payload.digest != current.payload.digest
    {
        return Err(RepositoryError::Conflict(
            "Context subscription refresh Job",
        ));
    }
    Ok(())
}

async fn reserve_context_subscription_refresh_quota(
    transaction: &mut Transaction<'_, Postgres>,
    job: &crate::repository::JobRecord,
    slot: &ContextSubscriptionRefreshClaimSlot,
    running: &JobProjection,
) -> Result<ResourceId, RepositoryError> {
    let row = sqlx::query(
        r#"
        SELECT quota_account_id, version, reserved_value, used_value, limit_value
        FROM insight_platform.quota_accounts
        WHERE tenant_id = $1 AND scope_kind = 'tenant' AND scope_id = $1
          AND work_class = 'context' AND metric = $2
        ORDER BY quota_account_id
        FOR UPDATE
        "#,
    )
    .bind(&job.tenant_id)
    .bind(QuotaDimension::WorkClassConcurrentOperations.as_str())
    .fetch_all(&mut **transaction)
    .await?;
    if row.len() != 1 {
        return Err(RepositoryError::QuotaExceeded);
    }
    let row = &row[0];
    let quota_account_id = row
        .try_get::<String, _>("quota_account_id")?
        .parse::<ResourceId>()
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    let version: i64 = row.try_get("version")?;
    let reserved: i64 = row.try_get("reserved_value")?;
    let used: i64 = row.try_get("used_value")?;
    let limit: i64 = row.try_get("limit_value")?;
    if reserved
        .checked_add(used)
        .and_then(|value| value.checked_add(1))
        .is_none_or(|value| value > limit)
    {
        return Err(RepositoryError::QuotaExceeded);
    }
    let next_version: i64 = sqlx::query_scalar(
        r#"
        UPDATE insight_platform.quota_accounts
        SET reserved_value = reserved_value + 1, version = version + 1,
            updated_at = clock_timestamp()
        WHERE tenant_id = $1 AND quota_account_id = $2 AND version = $3
          AND reserved_value + used_value + 1 <= limit_value
        RETURNING version
        "#,
    )
    .bind(&job.tenant_id)
    .bind(quota_account_id.to_string())
    .bind(version)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::QuotaExceeded)?;
    let request = TypedPayload::with_limit(
        1,
        &serde_json::json!({
            "job_id": running.job_id,
            "lease_generation": running.lease_generation,
            "quota_account_id": quota_account_id,
            "quota_reservation_id": slot.quota_reservation_id,
        }),
        65_536,
    )?;
    sqlx::query(
        r#"
        INSERT INTO insight_platform.quota_ledger (
            tenant_id, quota_entry_id, quota_account_id, correlation_id,
            entry_kind, reserved_amount, used_amount, account_version, request_digest
        ) VALUES ($1, $2, $3, $4, 'reserve', 1, 0, $5, $6)
        "#,
    )
    .bind(&job.tenant_id)
    .bind(slot.quota_reserve_entry_id.to_string())
    .bind(quota_account_id.to_string())
    .bind(slot.quota_reservation_id.to_string())
    .bind(next_version)
    .bind(request.digest)
    .execute(&mut **transaction)
    .await?;
    Ok(quota_account_id)
}

async fn settle_context_subscription_refresh_quota(
    transaction: &mut Transaction<'_, Postgres>,
    job: &crate::repository::JobRecord,
    settlement_entry_id: &ResourceId,
    request_digest: &Sha256Digest,
) -> Result<(), RepositoryError> {
    let reservation_id = job.quota_reservation_id.as_deref().ok_or_else(|| {
        RepositoryError::CorruptRow(
            "Context subscription refresh Job has no quota reservation".to_owned(),
        )
    })?;
    let row = sqlx::query(
        r#"
        SELECT account.quota_account_id, account.version, account.reserved_value,
               reserve.reserved_amount, reserve.used_amount
        FROM insight_platform.quota_ledger AS reserve
        JOIN insight_platform.quota_accounts AS account
          ON account.tenant_id = reserve.tenant_id
         AND account.quota_account_id = reserve.quota_account_id
        WHERE reserve.tenant_id = $1 AND reserve.correlation_id = $2
          AND reserve.entry_kind = 'reserve'
          AND account.scope_kind = 'tenant' AND account.scope_id = $1
          AND account.work_class = 'context' AND account.metric = $3
        FOR UPDATE OF account
        "#,
    )
    .bind(&job.tenant_id)
    .bind(reservation_id)
    .bind(QuotaDimension::WorkClassConcurrentOperations.as_str())
    .fetch_all(&mut **transaction)
    .await?;
    if row.len() != 1
        || row[0].try_get::<i64, _>("reserved_amount")? != 1
        || row[0].try_get::<i64, _>("used_amount")? != 0
        || row[0].try_get::<i64, _>("reserved_value")? < 1
    {
        return Err(RepositoryError::CorruptRow(
            "Context subscription refresh quota bundle drifted".to_owned(),
        ));
    }
    let quota_account_id: String = row[0].try_get("quota_account_id")?;
    let version: i64 = row[0].try_get("version")?;
    let next_version: i64 = sqlx::query_scalar(
        r#"
        UPDATE insight_platform.quota_accounts
        SET reserved_value = reserved_value - 1, version = version + 1,
            updated_at = clock_timestamp()
        WHERE tenant_id = $1 AND quota_account_id = $2 AND version = $3
          AND reserved_value >= 1
        RETURNING version
        "#,
    )
    .bind(&job.tenant_id)
    .bind(&quota_account_id)
    .bind(version)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::Conflict(
        "Context subscription refresh quota settlement",
    ))?;
    sqlx::query(
        r#"
        INSERT INTO insight_platform.quota_ledger (
            tenant_id, quota_entry_id, quota_account_id, correlation_id,
            entry_kind, reserved_amount, used_amount, account_version, request_digest
        ) VALUES ($1, $2, $3, $4, 'settle', 1, 0, $5, $6)
        "#,
    )
    .bind(&job.tenant_id)
    .bind(settlement_entry_id.to_string())
    .bind(&quota_account_id)
    .bind(reservation_id)
    .bind(next_version)
    .bind(request_digest.to_string())
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

async fn update_context_subscription_refresh_job(
    transaction: &mut Transaction<'_, Postgres>,
    current: &crate::repository::JobRecord,
    next: &JobProjection,
    quota_reservation_id: Option<&ResourceId>,
    result_digest: Option<&Sha256Digest>,
    database_now: DateTime<Utc>,
) -> Result<crate::repository::JobRecord, RepositoryError> {
    let terminal = matches!(
        next.state,
        JobState::Succeeded | JobState::Failed | JobState::Cancelled | JobState::TimedOut
    );
    let lease = next.lease.as_ref();
    let affected = sqlx::query(
        r#"
        UPDATE insight_platform.jobs
        SET state = $4, version = $5, attempt_no = $6, lease_epoch = $7,
            worker_id = $8, lease_token_digest = $9, heartbeat_at = $10,
            lease_expires_at = $11, scheduled_at = $12, retry_at = $13,
            quota_reservation_id = COALESCE($14, quota_reservation_id),
            result_digest = COALESCE($15, result_digest),
            started_at = CASE WHEN $4 = 'running' THEN COALESCE(started_at, $16) ELSE started_at END,
            terminal_at = CASE WHEN $17 THEN $16 ELSE NULL END,
            updated_at = $16
        WHERE tenant_id = $1 AND job_id = $2 AND version = $3
          AND work_class = 'context'
          AND job_kind = 'context_subscription_refresh'
          AND owner_kind = 'mcp_operation'
        "#,
    )
    .bind(&current.tenant_id)
    .bind(&current.job_id)
    .bind(current.version)
    .bind(next.state.as_str())
    .bind(as_i64(next.version, "Context subscription refresh Job version")?)
    .bind(i32::try_from(next.attempt_count).map_err(|_| {
        RepositoryError::CorruptRow("Context refresh attempt overflow".to_owned())
    })?)
    .bind(as_i64(
        next.lease_generation,
        "Context subscription refresh lease generation",
    )?)
    .bind(
        lease.map(|lease| lease.worker_process_generation_id.to_string()),
    )
    .bind(lease.map(|lease| lease.token_digest.to_string()))
    .bind(lease.map(|lease| lease.heartbeat_at))
    .bind(lease.map(|lease| lease.expires_at))
    .bind(next.scheduled_at)
    .bind(next.retry_at)
    .bind(quota_reservation_id.map(ToString::to_string))
    .bind(result_digest.map(ToString::to_string))
    .bind(database_now)
    .bind(terminal)
    .execute(&mut **transaction)
    .await?
    .rows_affected();
    if affected != 1 {
        return Err(RepositoryError::StaleFence);
    }
    load_job_for_update_by_text(transaction, &current.tenant_id, &current.job_id).await
}

fn require_context_subscription_refresh_fence(
    current: &crate::repository::JobRecord,
    attempt: &ContextSubscriptionRefreshAttempt,
) -> Result<(), RepositoryError> {
    if current.tenant_id != attempt.request.tenant_id.to_string()
        || current.job_id != attempt.job_id.to_string()
        || current.work_class != WorkClass::Context.as_str()
        || current.owner_kind != ResourceKind::McpOperation.descriptor().name
        || current.owner_id != attempt.request.subscription_id.to_string()
        || current.state != JobState::Running.as_str()
        || current.version
            != as_i64(
                attempt.job_fence.expected_version,
                "Context subscription refresh fence version",
            )?
        || current.worker_id.as_deref()
            != Some(attempt.worker_process_generation_id.to_string().as_str())
        || current.lease_epoch
            != as_i64(
                attempt.job_fence.lease_generation,
                "Context subscription refresh lease generation",
            )?
        || current.lease_token_digest.as_deref()
            != Some(attempt.job_fence.token_digest.to_string().as_str())
        || current.attempt_no
            != i32::try_from(attempt.attempt_number).map_err(|_| {
                RepositoryError::InvalidInput("Context refresh attempt overflow".to_owned())
            })?
    {
        return Err(RepositoryError::StaleFence);
    }
    Ok(())
}

fn decide_context_subscription_refresh_outcome(
    current: &JobProjection,
    command: &CommitContextSubscriptionRefresh,
    response_digest: &Sha256Digest,
    database_now: DateTime<Utc>,
) -> Result<(JobProjection, &'static str, &'static str, Sha256Digest), RepositoryError> {
    match &command.response {
        ContextSubscriptionRefreshResponse::Completed { evidence } => Ok((
            decide_job_terminal(
                current,
                &command.attempt.job_fence,
                database_now,
                JobState::Succeeded,
            )
            .map_err(map_job_decision_error)?,
            "completed",
            "context.subscription_refresh.completed",
            evidence
                .canonical_digest()
                .map_err(invalid_context_subscription_execution)?,
        )),
        ContextSubscriptionRefreshResponse::RetryableFailure {
            retry_after_milliseconds,
            ..
        } => {
            let retry_at = database_now
                .checked_add_signed(chrono::Duration::milliseconds(
                    i64::try_from(*retry_after_milliseconds).map_err(|_| {
                        RepositoryError::InvalidInput("Context refresh retry overflow".to_owned())
                    })?,
                ))
                .ok_or_else(|| {
                    RepositoryError::InvalidInput("Context refresh retry overflow".to_owned())
                })?;
            if current.attempt_count < current.attempt_limit && retry_at < current.deadline {
                Ok((
                    decide_job_retry(current, &command.attempt.job_fence, database_now, retry_at)
                        .map_err(map_job_decision_error)?,
                    "retry_scheduled",
                    "context.subscription_refresh.retry_scheduled",
                    response_digest.clone(),
                ))
            } else {
                Ok((
                    decide_job_terminal(
                        current,
                        &command.attempt.job_fence,
                        database_now,
                        JobState::Failed,
                    )
                    .map_err(map_job_decision_error)?,
                    "failed",
                    "context.subscription_refresh.failed",
                    response_digest.clone(),
                ))
            }
        }
        ContextSubscriptionRefreshResponse::PermanentFailure { class, .. } => {
            let (terminal, disposition) = match class {
                insight_platform_context::ContextSubscriptionRefreshFailureClass::Deadline => {
                    (JobState::TimedOut, "timed_out")
                }
                insight_platform_context::ContextSubscriptionRefreshFailureClass::Cancelled => {
                    (JobState::Cancelled, "cancelled")
                }
                _ => (JobState::Failed, "failed"),
            };
            Ok((
                decide_job_terminal(current, &command.attempt.job_fence, database_now, terminal)
                    .map_err(map_job_decision_error)?,
                disposition,
                "context.subscription_refresh.failed",
                response_digest.clone(),
            ))
        }
    }
}

async fn claim_context_subscription_refresh_commit_receipt(
    transaction: &mut Transaction<'_, Postgres>,
    command: &CommitContextSubscriptionRefresh,
    response_digest: &Sha256Digest,
    database_now: DateTime<Utc>,
) -> Result<bool, RepositoryError> {
    let attempt_digest = command
        .attempt
        .canonical_digest()
        .map_err(invalid_context_subscription_execution)?;
    let receipt_id = new_internal_id(ResourceKind::Receipt)?;
    let payload = TypedPayload::with_limit(1, &command.response, 262_144)?;
    let inserted = sqlx::query(
        r#"
        INSERT INTO insight_platform.receipts (
            tenant_id, receipt_id, receipt_kind, scope_kind, scope_id,
            dedupe_owner_id, operation, idempotency_key_digest, request_digest,
            state, payload_schema_version, payload, payload_digest, expires_at, created_at
        ) VALUES ($1, $2, 'job_commit', 'job', $3, $4,
                  'context.subscription_refresh.commit', $5, $6, 'processing',
                  $7, $8, $9, $10, $11)
        ON CONFLICT (
            tenant_id, receipt_kind, scope_kind, scope_id, dedupe_owner_id,
            operation, idempotency_key_digest
        ) DO NOTHING
        RETURNING receipt_id
        "#,
    )
    .bind(command.attempt.request.tenant_id.to_string())
    .bind(receipt_id.to_string())
    .bind(command.attempt.job_id.to_string())
    .bind(command.attempt.worker_process_generation_id.to_string())
    .bind(attempt_digest.to_string())
    .bind(response_digest.to_string())
    .bind(payload.schema_version)
    .bind(&payload.value)
    .bind(&payload.digest)
    .bind(command.attempt.request.deadline)
    .bind(database_now)
    .fetch_optional(&mut **transaction)
    .await?;
    if inserted.is_some() {
        return Ok(false);
    }
    let row = sqlx::query(
        r#"
        SELECT request_digest, state, response_reference_id,
               payload_schema_version, payload, payload_digest
        FROM insight_platform.receipts
        WHERE tenant_id = $1 AND receipt_kind = 'job_commit'
          AND scope_kind = 'job' AND scope_id = $2 AND dedupe_owner_id = $3
          AND operation = 'context.subscription_refresh.commit'
          AND idempotency_key_digest = $4
        FOR UPDATE
        "#,
    )
    .bind(command.attempt.request.tenant_id.to_string())
    .bind(command.attempt.job_id.to_string())
    .bind(command.attempt.worker_process_generation_id.to_string())
    .bind(attempt_digest.to_string())
    .fetch_one(&mut **transaction)
    .await?;
    if row.try_get::<String, _>("request_digest")? != response_digest.to_string()
        || row.try_get::<String, _>("state")? != "succeeded"
        || row.try_get::<Option<String>, _>("response_reference_id")?
            != Some(command.attempt.job_id.to_string())
    {
        return Err(RepositoryError::IdempotencyConflict);
    }
    let stored = payload_from_row(&row, "payload_schema_version", "payload", "payload_digest")?;
    let response: ContextSubscriptionRefreshResponse =
        decode_typed_payload(&stored, "Context subscription refresh commit Receipt")?;
    if response != command.response {
        return Err(RepositoryError::IdempotencyConflict);
    }
    Ok(true)
}

async fn complete_context_subscription_refresh_commit_receipt(
    transaction: &mut Transaction<'_, Postgres>,
    command: &CommitContextSubscriptionRefresh,
    response_digest: &Sha256Digest,
    disposition: &str,
    database_now: DateTime<Utc>,
) -> Result<(), RepositoryError> {
    let attempt_digest = command
        .attempt
        .canonical_digest()
        .map_err(invalid_context_subscription_execution)?;
    let affected = sqlx::query(
        r#"
        UPDATE insight_platform.receipts
        SET state = 'succeeded', disposition = $6, response_reference_id = $2,
            completed_at = $7
        WHERE tenant_id = $1 AND receipt_kind = 'job_commit'
          AND scope_kind = 'job' AND scope_id = $2 AND dedupe_owner_id = $3
          AND operation = 'context.subscription_refresh.commit'
          AND idempotency_key_digest = $4 AND request_digest = $5
          AND state = 'processing'
        "#,
    )
    .bind(command.attempt.request.tenant_id.to_string())
    .bind(command.attempt.job_id.to_string())
    .bind(command.attempt.worker_process_generation_id.to_string())
    .bind(attempt_digest.to_string())
    .bind(response_digest.to_string())
    .bind(disposition)
    .bind(database_now)
    .execute(&mut **transaction)
    .await?
    .rows_affected();
    if affected != 1 {
        return Err(RepositoryError::IdempotencyConflict);
    }
    Ok(())
}

fn map_context_subscription_admission_error(
    failure: RepositoryError,
) -> ContextSubscriptionAdmissionError {
    match failure {
        RepositoryError::Database(_) => ContextSubscriptionAdmissionError::CommitUncertain,
        RepositoryError::CorruptRow(_) => ContextSubscriptionAdmissionError::Unavailable,
        RepositoryError::InvalidInput(_)
        | RepositoryError::NotFound(_)
        | RepositoryError::Conflict(_)
        | RepositoryError::StaleFence
        | RepositoryError::LeaseExpired
        | RepositoryError::QuotaExceeded
        | RepositoryError::PermissionDenied
        | RepositoryError::IdempotencyConflict => ContextSubscriptionAdmissionError::Rejected,
    }
}

#[async_trait]
impl ContextSubscriptionAdmissionAuthority for PgRepository {
    async fn admit_context_subscription_refresh(
        &self,
        command: AdmitContextSubscriptionRefresh,
    ) -> Result<AcceptedContextSubscriptionRefresh, ContextSubscriptionAdmissionError> {
        PgRepository::admit_context_subscription_refresh(self, command)
            .await
            .map_err(map_context_subscription_admission_error)
    }
}

#[async_trait]
impl McpNotificationCommitAuthority for PgRepository {
    async fn commit(
        &self,
        command: McpNotificationCommit,
    ) -> Result<McpNotificationReceipt, McpNotificationPersistenceError> {
        PgRepository::commit_mcp_notification(self, command)
            .await
            .map(|outcome| McpNotificationReceipt {
                disposition: outcome.disposition,
                replayed: outcome.replayed,
            })
            .map_err(|failure| match failure {
                RepositoryError::Database(_) | RepositoryError::CorruptRow(_) => {
                    McpNotificationPersistenceError::Unavailable
                }
                _ => McpNotificationPersistenceError::Conflict,
            })
    }
}

#[async_trait]
impl McpSubscriptionExecutionResolver for PgRepository {
    async fn resolve_mcp_subscription_execution(
        &self,
        query: &McpSubscriptionContractQuery,
    ) -> Result<ResolvedMcpSubscriptionExecution, McpExecutionContractResolutionError> {
        query.validate()?;
        let mut transaction = self
            .pool()
            .begin()
            .await
            .map_err(|_| McpExecutionContractResolutionError::AuthorityUnavailable)?;
        let database_now = database_now(&mut transaction)
            .await
            .map_err(map_execution_resolution_error)?;
        let record = load_mcp_subscription(
            &mut transaction,
            &query.tenant_id,
            &query.subscription_id,
            false,
            database_now,
        )
        .await
        .map_err(map_execution_resolution_error)?;
        let job =
            load_mcp_subscription_job(&mut transaction, &query.tenant_id, &query.job_id, false)
                .await
                .map_err(map_execution_resolution_error)?;
        require_mcp_subscription_job_fence(&job, &record, &query.fence, database_now)
            .map_err(map_execution_resolution_error)?;
        let binding = &record.payload.binding;
        let contract = resolve_mcp_execution_contract(
            &mut transaction,
            &McpExecutionContractQuery {
                schema_version: 1,
                tenant_id: binding.tenant_id.clone(),
                mcp_deployment: binding.mcp_deployment.clone(),
                discovery_snapshot_id: binding.discovery_snapshot_id.clone(),
                discovery_snapshot_digest: binding.discovery_snapshot_digest.clone(),
                authorization_binding_id: binding.authorization_binding_id.clone(),
                authorization_generation: binding.authorization_generation,
                authorization_context_digest: binding.authorization_context_digest.clone(),
                principal_id: binding.principal_id.clone(),
            },
            database_now,
        )
        .await
        .map_err(map_execution_resolution_error)?;
        let resolved = ResolvedMcpSubscriptionExecution { record, contract };
        resolved.validate_for(query, database_now)?;
        transaction
            .commit()
            .await
            .map_err(|_| McpExecutionContractResolutionError::AuthorityUnavailable)?;
        Ok(resolved)
    }
}

#[async_trait]
impl ContextSubscriptionRefreshResolver for PgRepository {
    async fn resolve_context_subscription_refresh(
        &self,
        attempt: &ContextSubscriptionRefreshAttempt,
    ) -> Result<ResolvedContextSubscriptionRefresh, ContextSubscriptionExecutionError> {
        attempt.validate_at(Utc::now())?;
        let mut transaction = self
            .pool()
            .begin()
            .await
            .map_err(|_| ContextSubscriptionExecutionError::Unavailable)?;
        let database_now = database_now(&mut transaction)
            .await
            .map_err(map_context_subscription_resolution_error)?;
        attempt.validate_at(database_now)?;
        let job = load_context_subscription_refresh_job(
            &mut transaction,
            &attempt.request.tenant_id,
            &attempt.job_id,
            false,
        )
        .await
        .map_err(map_context_subscription_resolution_error)?;
        let payload: ContextSubscriptionRefreshJobPayload =
            decode_versioned_payload(&job.payload, "Context subscription refresh Job")
                .map_err(map_context_subscription_resolution_error)?;
        if payload.request != attempt.request
            || job
                .lease_expires_at
                .is_none_or(|expires_at| expires_at <= database_now)
        {
            return Err(ContextSubscriptionExecutionError::Rejected);
        }
        require_context_subscription_refresh_fence(&job, attempt)
            .map_err(map_context_subscription_resolution_error)?;
        let subscription = load_mcp_subscription(
            &mut transaction,
            &attempt.request.tenant_id,
            &attempt.request.subscription_id,
            false,
            database_now,
        )
        .await
        .map_err(map_context_subscription_resolution_error)?;
        validate_context_subscription_refresh_source(
            &mut transaction,
            &subscription,
            &attempt.request,
            ContextSubscriptionRefreshSourceEvidence::AcceptedJob(&attempt.job_id),
            database_now,
        )
        .await
        .map_err(map_context_subscription_resolution_error)?;
        let binding = &subscription.payload.binding;
        let contract = resolve_mcp_execution_contract(
            &mut transaction,
            &McpExecutionContractQuery {
                schema_version: 1,
                tenant_id: binding.tenant_id.clone(),
                mcp_deployment: binding.mcp_deployment.clone(),
                discovery_snapshot_id: binding.discovery_snapshot_id.clone(),
                discovery_snapshot_digest: binding.discovery_snapshot_digest.clone(),
                authorization_binding_id: binding.authorization_binding_id.clone(),
                authorization_generation: binding.authorization_generation,
                authorization_context_digest: binding.authorization_context_digest.clone(),
                principal_id: binding.principal_id.clone(),
            },
            database_now,
        )
        .await
        .map_err(map_context_subscription_resolution_error)?;
        let resolved = ResolvedContextSubscriptionRefresh {
            subscription,
            contract,
        };
        resolved.validate_for(attempt)?;
        transaction
            .commit()
            .await
            .map_err(|_| ContextSubscriptionExecutionError::Unavailable)?;
        Ok(resolved)
    }
}

#[async_trait]
impl McpSubscriptionAuthority for PgRepository {
    async fn save_subscription_session(
        &self,
        command: SaveMcpSubscriptionSession,
    ) -> Result<CommandOutcome<McpSubscriptionRecord>, McpSubscriptionPersistenceError> {
        self.save_mcp_subscription_session(command)
            .await
            .map_err(map_mcp_subscription_persistence_error)
    }

    async fn complete_subscription_refresh(
        &self,
        command: CompleteMcpSubscriptionRefresh,
    ) -> Result<CommandOutcome<McpSubscriptionRecord>, McpSubscriptionPersistenceError> {
        self.complete_mcp_subscription_refresh(command)
            .await
            .map_err(map_mcp_subscription_persistence_error)
    }

    async fn complete_subscription_reconcile(
        &self,
        command: CompleteMcpSubscriptionReconcile,
    ) -> Result<CommandOutcome<McpSubscriptionRecord>, McpSubscriptionPersistenceError> {
        self.complete_mcp_subscription_reconcile(command)
            .await
            .map_err(map_mcp_subscription_persistence_error)
    }
}

#[async_trait]
impl McpSubscriptionReconcileAuthority for PgRepository {
    async fn list_due_reconciliations(
        &self,
        scan: McpSubscriptionReconcileScan,
    ) -> Result<Vec<DueMcpSubscriptionReconcile>, McpSubscriptionPersistenceError> {
        self.list_due_mcp_subscription_reconciliations(scan)
            .await
            .map_err(|failure| match failure {
                RepositoryError::InvalidInput(_) => McpSubscriptionPersistenceError::InvalidCommand,
                RepositoryError::Database(_) | RepositoryError::CorruptRow(_) => {
                    McpSubscriptionPersistenceError::AuthorityUnavailable
                }
                _ => McpSubscriptionPersistenceError::Conflict,
            })
    }

    async fn wake_reconciliation(
        &self,
        command: WakeMcpSubscriptionReconcile,
    ) -> Result<CommandOutcome<McpSubscriptionRecord>, McpSubscriptionPersistenceError> {
        self.wake_mcp_subscription_reconcile(command)
            .await
            .map_err(map_mcp_subscription_persistence_error)
    }
}

#[async_trait]
impl McpSubscriptionRecoveryAuthority for PgRepository {
    async fn list_due_recoveries(
        &self,
        scan: McpSubscriptionRecoveryScan,
    ) -> Result<Vec<DueMcpSubscriptionRecovery>, McpSubscriptionPersistenceError> {
        self.list_due_mcp_subscription_recoveries(scan)
            .await
            .map_err(map_mcp_subscription_persistence_error)
    }

    async fn recover_due_subscription(
        &self,
        command: RecoverDueMcpSubscription,
    ) -> Result<CommandOutcome<McpSubscriptionRecord>, McpSubscriptionPersistenceError> {
        self.recover_due_mcp_subscription(command)
            .await
            .map_err(map_mcp_subscription_persistence_error)
    }

    async fn report_session_loss(
        &self,
        command: ReportMcpSubscriptionSessionLoss,
    ) -> Result<CommandOutcome<McpSubscriptionRecord>, McpSubscriptionPersistenceError> {
        self.report_mcp_subscription_session_loss(command)
            .await
            .map_err(map_mcp_subscription_persistence_error)
    }
}

#[async_trait]
impl McpSubscriptionTransportTerminationAuthority for PgRepository {
    async fn report_transport_termination(
        &self,
        command: ReportMcpSubscriptionTransportTermination,
    ) -> Result<CommandOutcome<McpSubscriptionRecord>, McpSubscriptionPersistenceError> {
        self.report_mcp_subscription_transport_termination(command)
            .await
            .map_err(map_mcp_subscription_persistence_error)
    }
}

#[async_trait]
#[cfg(any())]
impl ManagedMcpSandboxSessionGatewayAuthority for PgRepository {
    type Error = RepositoryError;

    async fn accept_managed_mcp_sandbox_session(
        &self,
        command: AcceptManagedMcpSandboxSession,
    ) -> Result<CommandOutcome<AcceptedManagedMcpSandboxSession>, Self::Error> {
        PgRepository::accept_managed_mcp_sandbox_session(self, command).await
    }
}

#[async_trait]
#[cfg(any())]
impl ManagedMcpSandboxSessionClaimAuthority for PgRepository {
    async fn claim_managed_mcp_sandbox_sessions(
        &self,
        command: ClaimSandboxJobs,
    ) -> Result<Vec<ClaimedManagedMcpSandboxSession>, SandboxClaimFailure> {
        PgRepository::claim_managed_mcp_sandbox_sessions(self, command)
            .await
            .map_err(map_managed_mcp_sandbox_claim_failure)
    }
}

#[async_trait]
#[cfg(any())]
impl ManagedMcpSandboxSessionExecutionAuthority for PgRepository {
    type Error = RepositoryError;

    async fn commit_managed_mcp_sandbox_session_phase(
        &self,
        command: CommitManagedMcpSandboxSessionPhase,
    ) -> Result<CommandOutcome<ManagedMcpSandboxSessionPhaseDecision>, Self::Error> {
        PgRepository::commit_managed_mcp_sandbox_session_phase(self, command).await
    }

    async fn commit_managed_mcp_sandbox_session_ready(
        &self,
        command: CommitManagedMcpSandboxSessionReady,
    ) -> Result<CommandOutcome<ManagedMcpSandboxSessionPhaseDecision>, Self::Error> {
        PgRepository::commit_managed_mcp_sandbox_session_ready(self, command).await
    }

    async fn heartbeat_managed_mcp_sandbox_session(
        &self,
        command: HeartbeatSandboxExecution,
    ) -> Result<ManagedMcpSandboxSessionPhaseDecision, Self::Error> {
        PgRepository::heartbeat_managed_mcp_sandbox_session(self, command).await
    }

    async fn commit_managed_mcp_sandbox_session_lost(
        &self,
        command: CommitManagedMcpSandboxSessionLost,
    ) -> Result<CommandOutcome<ManagedMcpSandboxSessionPhaseDecision>, Self::Error> {
        PgRepository::commit_managed_mcp_sandbox_session_lost(self, command).await
    }
}

#[async_trait]
#[cfg(any())]
impl ManagedMcpSandboxSessionRecoveryAuthority for PgRepository {
    async fn scan_expired_managed_mcp_sandbox_session_leases(
        &self,
        command: insight_platform_sandbox::ScanExpiredManagedMcpSandboxSessionLeases,
    ) -> Result<ExpiredManagedMcpSandboxSessionLeasePage, ManagedMcpSandboxSessionRecoveryFailure>
    {
        PgRepository::scan_expired_managed_mcp_sandbox_session_leases(self, command)
            .await
            .map_err(map_managed_mcp_sandbox_recovery_failure)
    }

    async fn recover_expired_managed_mcp_sandbox_session_lease(
        &self,
        command: RecoverExpiredManagedMcpSandboxSessionLease,
    ) -> Result<
        CommandOutcome<ManagedMcpSandboxSessionLeaseRecoveryResult>,
        ManagedMcpSandboxSessionRecoveryFailure,
    > {
        PgRepository::recover_expired_managed_mcp_sandbox_session_lease(self, command)
            .await
            .map_err(map_managed_mcp_sandbox_recovery_failure)
    }
}

#[cfg(any())]
fn map_managed_mcp_sandbox_claim_failure(failure: RepositoryError) -> SandboxClaimFailure {
    match failure {
        RepositoryError::Database(_) => SandboxClaimFailure::Unavailable,
        RepositoryError::NotFound(_)
        | RepositoryError::Conflict(_)
        | RepositoryError::StaleFence
        | RepositoryError::LeaseExpired => SandboxClaimFailure::FirstWinnerLost,
        RepositoryError::InvalidInput(_)
        | RepositoryError::QuotaExceeded
        | RepositoryError::PermissionDenied
        | RepositoryError::IdempotencyConflict
        | RepositoryError::CorruptRow(_) => SandboxClaimFailure::InvariantViolation,
    }
}

#[cfg(any())]
fn map_managed_mcp_sandbox_recovery_failure(
    failure: RepositoryError,
) -> ManagedMcpSandboxSessionRecoveryFailure {
    match failure {
        RepositoryError::Database(_) => ManagedMcpSandboxSessionRecoveryFailure::Unavailable,
        RepositoryError::NotFound(_)
        | RepositoryError::Conflict(_)
        | RepositoryError::StaleFence
        | RepositoryError::LeaseExpired => ManagedMcpSandboxSessionRecoveryFailure::FirstWinnerLost,
        RepositoryError::InvalidInput(_)
        | RepositoryError::QuotaExceeded
        | RepositoryError::PermissionDenied
        | RepositoryError::IdempotencyConflict
        | RepositoryError::CorruptRow(_) => {
            ManagedMcpSandboxSessionRecoveryFailure::InvariantViolation
        }
    }
}

fn map_mcp_subscription_persistence_error(
    failure: RepositoryError,
) -> McpSubscriptionPersistenceError {
    match failure {
        RepositoryError::Database(_) => McpSubscriptionPersistenceError::CommitUncertain,
        RepositoryError::CorruptRow(_) => McpSubscriptionPersistenceError::AuthorityUnavailable,
        RepositoryError::InvalidInput(_) => McpSubscriptionPersistenceError::InvalidCommand,
        RepositoryError::NotFound(_)
        | RepositoryError::Conflict(_)
        | RepositoryError::StaleFence
        | RepositoryError::LeaseExpired
        | RepositoryError::QuotaExceeded
        | RepositoryError::PermissionDenied
        | RepositoryError::IdempotencyConflict => McpSubscriptionPersistenceError::Conflict,
    }
}

#[async_trait]
impl McpOAuthAuthorizationStartAuthority for PgRepository {
    async fn resolve_authorization_start(
        &self,
        intent: &McpOAuthAuthorizationStartIntent,
        callback_binding_digest: &Sha256Digest,
    ) -> Result<ResolvedMcpOAuthAuthorizationStart, McpOAuthAuthorizationStartAuthorityError> {
        intent
            .validate_at(Utc::now())
            .map_err(|_| McpOAuthAuthorizationStartAuthorityError::NotFoundOrChanged)?;
        let mut transaction = self
            .pool()
            .begin()
            .await
            .map_err(|_| McpOAuthAuthorizationStartAuthorityError::Unavailable)?;
        let database_now = database_now(&mut transaction)
            .await
            .map_err(map_mcp_oauth_start_resolve_error)?;
        intent
            .validate_at(database_now)
            .map_err(|_| McpOAuthAuthorizationStartAuthorityError::NotFoundOrChanged)?;
        let principal =
            require_tenant_permission(&mut transaction, &intent.audit, Permission::McpWrite)
                .await
                .map_err(map_mcp_oauth_start_resolve_error)?;
        if principal.binding_generation != intent.expected_principal_binding_generation {
            return Err(McpOAuthAuthorizationStartAuthorityError::NotFoundOrChanged);
        }
        let resolved = load_mcp_oauth_start_dependencies(
            &mut transaction,
            &intent.audit.tenant_id,
            &intent.mcp_deployment,
            callback_binding_digest,
            &intent.requested_scopes,
        )
        .await
        .map_err(map_mcp_oauth_start_resolve_error)?;
        // Current AuthorizationBinding and pending-Task races are intentionally revalidated only
        // in the commit transaction. A preflight rejection here would break exact Receipt replay;
        // any preparation orphan from a losing race is bounded by the Secret Manager expiry.
        transaction
            .commit()
            .await
            .map_err(|_| McpOAuthAuthorizationStartAuthorityError::Unavailable)?;
        Ok(resolved)
    }

    async fn commit_authorization_start(
        &self,
        command: BeginMcpOAuthAuthorization,
    ) -> Result<McpOAuthAuthorizationStartCommitOutcome, McpOAuthAuthorizationStartAuthorityError>
    {
        let mut transaction = self
            .begin_registry_transaction()
            .await
            .map_err(map_mcp_oauth_start_commit_error)?;
        let outcome = transaction
            .begin_mcp_oauth_authorization(command)
            .await
            .map_err(map_mcp_oauth_start_commit_error)?;
        let disposition = match outcome {
            CommandOutcome::Applied(_) => McpOAuthAuthorizationStartCommitDisposition::Applied,
            CommandOutcome::Replayed(_) => McpOAuthAuthorizationStartCommitDisposition::Replayed,
        };
        transaction
            .commit()
            .await
            .map_err(map_mcp_oauth_start_commit_error)?;
        Ok(McpOAuthAuthorizationStartCommitOutcome { disposition })
    }
}

#[async_trait]
impl McpOAuthCallbackAuthority for PgRepository {
    async fn resolve_exchange_contract(
        &self,
        identity: &AuthenticatedMcpOAuthState,
    ) -> Result<McpOAuthExchangeContract, McpOAuthCallbackAuthorityError> {
        self.resolve_mcp_oauth_exchange_contract(identity)
            .await
            .map_err(map_mcp_oauth_resolve_error)
    }

    async fn commit_callback(
        &self,
        command: CompleteMcpOAuthCallback,
    ) -> Result<McpOAuthCallbackCommitOutcome, McpOAuthCallbackAuthorityError> {
        let mut transaction = self
            .begin_registry_transaction()
            .await
            .map_err(map_mcp_oauth_commit_error)?;
        let outcome = transaction
            .complete_mcp_oauth_callback(command)
            .await
            .map_err(map_mcp_oauth_commit_error)?;
        let disposition = match outcome {
            CommandOutcome::Replayed(_) => McpOAuthCallbackCommitDisposition::Replayed,
            CommandOutcome::Applied(record) if record.authorization.is_some() => {
                McpOAuthCallbackCommitDisposition::Authorized
            }
            CommandOutcome::Applied(record) if record.task.state == TaskState::Declined => {
                McpOAuthCallbackCommitDisposition::Declined
            }
            CommandOutcome::Applied(_) => McpOAuthCallbackCommitDisposition::RejectedStale,
        };
        transaction
            .commit()
            .await
            .map_err(map_mcp_oauth_commit_error)?;
        Ok(McpOAuthCallbackCommitOutcome { disposition })
    }
}

#[async_trait]
impl McpOAuthPkceCleanupAuthority for PgRepository {
    async fn authorize_cleanup(
        &self,
        request: &McpOAuthPkceCleanupRequest,
    ) -> Result<AuthorizedMcpOAuthPkceCleanup, McpOAuthPkceCleanupAuthorityError> {
        request
            .validate()
            .map_err(|_| McpOAuthPkceCleanupAuthorityError::StaleOrNotFound)?;
        let mut transaction = self
            .pool()
            .begin()
            .await
            .map_err(|_| McpOAuthPkceCleanupAuthorityError::Unavailable)?;
        let task = load_task_for_update(&mut transaction, &request.tenant_id, &request.task_id)
            .await
            .map_err(map_mcp_oauth_cleanup_error)?;
        let projection = task_projection(&task).map_err(map_mcp_oauth_cleanup_error)?;
        let TaskDefinition::McpOAuthAuthorization { binding, .. } = &projection.payload.definition
        else {
            return Err(McpOAuthPkceCleanupAuthorityError::StaleOrNotFound);
        };
        let expected_state = match request.cause {
            McpOAuthPkceCleanupCause::Authorized => TaskState::Responded,
            McpOAuthPkceCleanupCause::Declined => TaskState::Declined,
            McpOAuthPkceCleanupCause::Expired => TaskState::Expired,
        };
        if projection.state != expected_state
            || binding.pkce_secret_binding.secret_binding_id != request.hint.secret_binding_id
            || binding.pkce_secret_binding.binding_generation != request.hint.binding_generation
        {
            return Err(McpOAuthPkceCleanupAuthorityError::StaleOrNotFound);
        }
        validate_exact_secret_bindings_at_creation(
            &mut transaction,
            &request.tenant_id,
            std::slice::from_ref(binding.pkce_secret_binding.as_ref()),
        )
        .await
        .map_err(map_mcp_oauth_cleanup_error)?;
        let authorization = AuthorizedMcpOAuthPkceCleanup {
            tenant_id: request.tenant_id.clone(),
            task_id: request.task_id.clone(),
            secret_binding: binding.pkce_secret_binding.as_ref().clone(),
        };
        authorization
            .validate_for(request)
            .map_err(|_| McpOAuthPkceCleanupAuthorityError::StaleOrNotFound)?;
        transaction
            .commit()
            .await
            .map_err(|_| McpOAuthPkceCleanupAuthorityError::Unavailable)?;
        Ok(authorization)
    }
}

fn map_mcp_oauth_cleanup_error(error: RepositoryError) -> McpOAuthPkceCleanupAuthorityError {
    match error {
        RepositoryError::Database(_) | RepositoryError::CorruptRow(_) => {
            McpOAuthPkceCleanupAuthorityError::Unavailable
        }
        _ => McpOAuthPkceCleanupAuthorityError::StaleOrNotFound,
    }
}

fn map_mcp_oauth_start_resolve_error(
    error: RepositoryError,
) -> McpOAuthAuthorizationStartAuthorityError {
    match error {
        RepositoryError::Database(_) | RepositoryError::CorruptRow(_) => {
            McpOAuthAuthorizationStartAuthorityError::Unavailable
        }
        _ => McpOAuthAuthorizationStartAuthorityError::NotFoundOrChanged,
    }
}

fn map_mcp_oauth_start_commit_error(
    error: RepositoryError,
) -> McpOAuthAuthorizationStartAuthorityError {
    match error {
        RepositoryError::Database(_) => McpOAuthAuthorizationStartAuthorityError::CommitUncertain,
        RepositoryError::CorruptRow(_) => McpOAuthAuthorizationStartAuthorityError::Unavailable,
        _ => McpOAuthAuthorizationStartAuthorityError::NotFoundOrChanged,
    }
}

fn map_mcp_oauth_resolve_error(error: RepositoryError) -> McpOAuthCallbackAuthorityError {
    match error {
        RepositoryError::Database(_) | RepositoryError::CorruptRow(_) => {
            McpOAuthCallbackAuthorityError::Unavailable
        }
        _ => McpOAuthCallbackAuthorityError::NotFoundOrChanged,
    }
}

fn map_mcp_oauth_commit_error(error: RepositoryError) -> McpOAuthCallbackAuthorityError {
    match error {
        RepositoryError::Database(_) => McpOAuthCallbackAuthorityError::CommitUncertain,
        RepositoryError::CorruptRow(_) => McpOAuthCallbackAuthorityError::Unavailable,
        _ => McpOAuthCallbackAuthorityError::NotFoundOrChanged,
    }
}

#[async_trait]
impl McpDiscoveryResultStore for PgRepository {
    async fn park_mcp_discovery_for_verification(
        &self,
        command: ParkMcpDiscoveryVerification,
    ) -> Result<CommandOutcome<McpDiscoveryOperationRecord>, McpDiscoveryPersistenceError> {
        PgRepository::park_mcp_discovery_for_verification(self, command)
            .await
            .map_err(map_mcp_discovery_persistence_error)
    }

    async fn finalize_mcp_discovery_after_verification(
        &self,
        command: FinalizeMcpDiscovery,
    ) -> Result<CommandOutcome<McpDiscoveryOperationRecord>, McpDiscoveryPersistenceError> {
        PgRepository::finalize_mcp_discovery_after_verification(self, command)
            .await
            .map_err(map_mcp_discovery_persistence_error)
    }

    async fn resolve_mcp_discovery_attempt_result(
        &self,
        command: ResolveMcpDiscoveryAttempt,
    ) -> Result<CommandOutcome<McpDiscoveryOperationRecord>, McpDiscoveryPersistenceError> {
        self.resolve_mcp_discovery_attempt(command)
            .await
            .map_err(map_mcp_discovery_persistence_error)
    }
}

#[async_trait]
impl McpExecutionContractResolver for PgRepository {
    async fn resolve_mcp_execution_contract(
        &self,
        query: &McpExecutionContractQuery,
    ) -> Result<McpHostExecutionContract, McpExecutionContractResolutionError> {
        query.validate()?;
        let mut transaction = self
            .pool()
            .begin()
            .await
            .map_err(|_| McpExecutionContractResolutionError::AuthorityUnavailable)?;
        let database_now = database_now(&mut transaction)
            .await
            .map_err(map_execution_resolution_error)?;
        let contract = resolve_mcp_execution_contract(&mut transaction, query, database_now)
            .await
            .map_err(map_execution_resolution_error)?;
        transaction
            .commit()
            .await
            .map_err(|_| McpExecutionContractResolutionError::AuthorityUnavailable)?;
        Ok(contract)
    }
}

#[async_trait]
impl McpDiscoveryExecutionContractResolver for PgRepository {
    async fn resolve_mcp_discovery_execution(
        &self,
        query: &McpDiscoveryContractQuery,
    ) -> Result<ResolvedMcpDiscoveryExecution, McpExecutionContractResolutionError> {
        query.validate()?;
        let mut transaction = self
            .pool()
            .begin()
            .await
            .map_err(|_| McpExecutionContractResolutionError::AuthorityUnavailable)?;
        let database_now = database_now(&mut transaction)
            .await
            .map_err(map_execution_resolution_error)?;
        let operation = load_mcp_discovery_operation(
            &mut transaction,
            &query.tenant_id,
            &query.operation_id,
            false,
        )
        .await
        .map_err(map_execution_resolution_error)?;
        if operation.job_id != query.job_id
            || !matches!(
                operation.state,
                McpDiscoveryOperationState::Pending | McpDiscoveryOperationState::Running
            )
            || operation.payload.result.is_some()
            || operation.deadline <= database_now
        {
            return Err(McpExecutionContractResolutionError::NotFoundOrChanged);
        }
        let job = load_mcp_discovery_job(&mut transaction, &query.tenant_id, &query.job_id, false)
            .await
            .map_err(map_execution_resolution_error)?;
        require_exact_mcp_discovery_job_fence(&job, &operation, &query.fence, database_now)
            .map_err(map_execution_resolution_error)?;

        let admission = &operation.payload.admission;
        let base = resolve_mcp_base_execution_contract(
            &mut transaction,
            &query.tenant_id,
            &admission.mcp_deployment,
            &admission.authorization_binding_id,
            admission.authorization_generation,
            &admission.authorization_context_digest,
            &admission.principal_id,
            database_now,
        )
        .await
        .map_err(map_execution_resolution_error)?;
        if base.deployment_closure.server_revision != admission.server_revision
            || base.deployment_closure.protocol_policy != admission.protocol_profile
        {
            return Err(McpExecutionContractResolutionError::NotFoundOrChanged);
        }
        let contract = McpDiscoveryExecutionContract::build(NewMcpDiscoveryExecutionContract {
            deployment: admission.mcp_deployment.clone(),
            deployment_closure: base.deployment_closure,
            server: base.server,
            protocol_profile: base.protocol_profile,
            authorization: base.authorization,
        })
        .map_err(|_| McpExecutionContractResolutionError::NotFoundOrChanged)?;
        let resolved = ResolvedMcpDiscoveryExecution {
            operation_version: operation.version,
            admission_digest: admission.canonical_digest.clone(),
            artifact_preallocation: admission.artifact_preallocation.clone(),
            artifact_policy: admission.artifact_policy.clone(),
            pending_verification: operation.payload.pending_verification.clone(),
            attempt_limit: job.attempt_limit,
            contract,
            request: McpDiscoveryRequest {
                schema_version: 1,
                operation_id: operation.operation_id,
                tenant_id: operation.tenant_id,
                job_id: operation.job_id,
                worker_process_generation_id: query.fence.worker_process_generation_id.clone(),
                lease_generation: query.fence.lease_generation,
                physical_attempt: job.physical_attempt,
                authorization_binding_id: admission.authorization_binding_id.clone(),
                deadline: operation.deadline,
            },
        };
        resolved.validate_for(query, database_now)?;
        transaction
            .commit()
            .await
            .map_err(|_| McpExecutionContractResolutionError::AuthorityUnavailable)?;
        Ok(resolved)
    }
}

struct ResolvedMcpBaseExecutionContract {
    deployment_closure: McpDeploymentClosure,
    server: McpServerExecutionContract,
    protocol_profile: McpProtocolPolicyDocument,
    authorization: McpAuthorizationContext,
}

#[allow(clippy::too_many_arguments)]
async fn resolve_mcp_base_execution_contract(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    mcp_deployment: &ExactDeploymentRef,
    authorization_binding_id: &ResourceId,
    authorization_generation: u64,
    authorization_context_digest: &Sha256Digest,
    principal_id: &ResourceId,
    database_now: DateTime<Utc>,
) -> Result<ResolvedMcpBaseExecutionContract, RepositoryError> {
    let deployment = load_deployment(transaction, tenant_id, &mcp_deployment.deployment_id).await?;
    if deployment.bindings.digest != mcp_deployment.deployment_digest.to_string() {
        return Err(RepositoryError::Conflict("exact MCP Deployment"));
    }
    let resource_id = deployment
        .resource_id
        .parse::<ResourceId>()
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    let resource = load_resource(transaction, tenant_id, &resource_id).await?;
    if resource.resource_kind != RegistryResourceKind::McpServer.as_str()
        || resource.lifecycle_state != EntityLifecycle::Active.as_str()
        || resource.gate_state != "enabled"
    {
        return Err(RepositoryError::Conflict("MCP Deployment gate"));
    }
    let deployment_closure = match decode_deployment_closure(&deployment.bindings)? {
        DeploymentClosure::McpServer(closure) => closure,
        _ => {
            return Err(RepositoryError::CorruptRow(
                "MCP Deployment contains the wrong closure".to_owned(),
            ));
        }
    };
    validate_deployment_closure_exists(
        transaction,
        tenant_id,
        &DeploymentClosure::McpServer(deployment_closure.clone()),
    )
    .await?;

    let server_payload = crate::invocation_repository::load_enabled_exact_published_version(
        transaction,
        tenant_id,
        &deployment_closure.server_revision,
        RegistryResourceKind::McpServer,
    )
    .await?;
    let ResourceDocument::McpServer(server_spec) = server_payload.document else {
        return Err(RepositoryError::CorruptRow(
            "MCP Server revision contains the wrong document".to_owned(),
        ));
    };
    if server_spec.transport != deployment_closure.transport.kind()
        || server_spec.protocol_policy != deployment_closure.protocol_policy
    {
        return Err(RepositoryError::Conflict("MCP Server execution closure"));
    }
    let server = McpServerExecutionContract::build(
        deployment_closure.server_revision.clone(),
        server_spec.transport,
        server_spec.protocol_policy,
        server_spec.deployment_credential_requirements,
        server_spec.authorization_credential_purpose,
        server_spec.limits,
    )
    .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;

    let protocol_payload = crate::invocation_repository::load_enabled_exact_published_version(
        transaction,
        tenant_id,
        &deployment_closure.protocol_policy,
        RegistryResourceKind::Policy,
    )
    .await?;
    let ResourceDocument::Policy(protocol_spec) = protocol_payload.document else {
        return Err(RepositoryError::CorruptRow(
            "MCP Protocol Policy revision contains the wrong document".to_owned(),
        ));
    };
    if protocol_spec.policy_kind != PolicyKind::Protocol {
        return Err(RepositoryError::Conflict("MCP Protocol Policy kind"));
    }
    let protocol_profile = protocol_spec.mcp_protocol.ok_or_else(|| {
        RepositoryError::CorruptRow("MCP Protocol Policy has no closed document".to_owned())
    })?;

    let authorization_record =
        load_mcp_authorization_binding(transaction, tenant_id, authorization_binding_id, false)
            .await?;
    let authorization = authorization_record
        .execution_context(database_now)
        .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;
    if authorization.mcp_deployment != *mcp_deployment
        || authorization.generation != authorization_generation
        || authorization.canonical_digest != *authorization_context_digest
        || authorization.principal_id != *principal_id
        || authorization.audience_identity_digest != deployment_closure.server_identity_digest
    {
        return Err(RepositoryError::Conflict(
            "MCP execution authorization generation",
        ));
    }
    validate_principal_binding(
        transaction,
        &authorization.tenant_id,
        &authorization.principal_id,
        authorization.principal_identity_kind,
        authorization.principal_binding_generation,
    )
    .await?;
    validate_mcp_authorization_dependencies(
        transaction,
        tenant_id,
        &authorization.mcp_deployment,
        &authorization.audience_identity_digest,
        &authorization.token_secret_binding,
    )
    .await?;

    Ok(ResolvedMcpBaseExecutionContract {
        deployment_closure,
        server,
        protocol_profile,
        authorization,
    })
}

pub(crate) async fn resolve_mcp_execution_contract(
    transaction: &mut Transaction<'_, Postgres>,
    query: &McpExecutionContractQuery,
    database_now: DateTime<Utc>,
) -> Result<McpHostExecutionContract, RepositoryError> {
    let base = resolve_mcp_base_execution_contract(
        transaction,
        &query.tenant_id,
        &query.mcp_deployment,
        &query.authorization_binding_id,
        query.authorization_generation,
        &query.authorization_context_digest,
        &query.principal_id,
        database_now,
    )
    .await?;

    let discovery = load_mcp_discovery_snapshot_record(
        transaction,
        &query.tenant_id,
        &query.discovery_snapshot_id,
    )
    .await?;
    if discovery.snapshot.canonical_digest != query.discovery_snapshot_digest
        || discovery.snapshot.mcp_deployment != query.mcp_deployment
        || discovery.snapshot.server_revision != base.deployment_closure.server_revision
        || discovery.snapshot.protocol_profile != base.deployment_closure.protocol_policy
        || discovery.snapshot.authorization_context_digest != query.authorization_context_digest
        || discovery.snapshot.observed_at > database_now
        || discovery.snapshot.expires_at <= database_now
    {
        return Err(RepositoryError::Conflict("MCP Discovery Snapshot binding"));
    }
    validate_mcp_discovery_source(transaction, &discovery, query).await?;

    McpHostExecutionContract::build(NewMcpHostExecutionContract {
        deployment: query.mcp_deployment.clone(),
        deployment_closure: base.deployment_closure,
        server: base.server,
        protocol_profile: base.protocol_profile,
        authorization: base.authorization,
        discovery: discovery.snapshot,
    })
    .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))
}

pub(crate) async fn load_mcp_discovery_snapshot_record(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    snapshot_id: &ResourceId,
) -> Result<McpDiscoverySnapshotRecord, RepositoryError> {
    let resource = load_resource(transaction, tenant_id, snapshot_id).await?;
    if resource.resource_kind != MCP_DISCOVERY_RESOURCE_KIND
        || resource.lifecycle_state != EntityLifecycle::Active.as_str()
        || resource.gate_state != "enabled"
        || resource.active_version_id.is_some()
        || resource.active_deployment_id.is_some()
        || resource.version != 1
    {
        return Err(RepositoryError::Conflict("MCP Discovery Snapshot resource"));
    }
    let record: McpDiscoverySnapshotRecord =
        decode_versioned_payload(&resource.payload, "MCP Discovery Snapshot")?;
    if record.tenant_id != *tenant_id || record.snapshot.snapshot_id != *snapshot_id {
        return Err(RepositoryError::CorruptRow(
            "MCP Discovery Snapshot aggregate disagrees with its payload".to_owned(),
        ));
    }
    record
        .validate()
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    require_ready_run_artifact(transaction, tenant_id, &record.snapshot.objects_artifact).await?;
    Ok(record)
}

async fn validate_mcp_discovery_source(
    transaction: &mut Transaction<'_, Postgres>,
    record: &McpDiscoverySnapshotRecord,
    query: &McpExecutionContractQuery,
) -> Result<(), RepositoryError> {
    let operation = sqlx::query(
        r#"
        SELECT invocation_kind, owner_kind, owner_id, deployment_id, state,
               payload_schema_version, payload, payload_digest, terminal_at
        FROM insight_platform.invocations
        WHERE tenant_id = $1 AND invocation_id = $2
        "#,
    )
    .bind(query.tenant_id.to_string())
    .bind(record.source_operation_id.to_string())
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::NotFound("MCP discovery operation"))?;
    if operation.try_get::<String, _>("invocation_kind")? != "mcp_discovery"
        || operation.try_get::<String, _>("owner_kind")? != "mcp_operation"
        || operation.try_get::<String, _>("owner_id")? != record.source_operation_id.to_string()
        || operation.try_get::<Option<String>, _>("deployment_id")?
            != Some(query.mcp_deployment.deployment_id.to_string())
        || operation.try_get::<String, _>("state")? != "succeeded"
        || operation
            .try_get::<Option<DateTime<Utc>>, _>("terminal_at")?
            .is_none()
    {
        return Err(RepositoryError::Conflict("MCP discovery operation source"));
    }
    let operation_payload = payload_from_row(
        &operation,
        "payload_schema_version",
        "payload",
        "payload_digest",
    )?;
    let operation_payload: McpDiscoveryOperationPayload =
        decode_versioned_payload(&operation_payload, "MCP discovery operation")?;
    operation_payload
        .validate()
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    let result = operation_payload.result.as_ref().ok_or_else(|| {
        RepositoryError::CorruptRow("succeeded MCP discovery has no result".to_owned())
    })?;
    if operation_payload.admission.operation_id != record.source_operation_id
        || operation_payload.admission.tenant_id != query.tenant_id
        || operation_payload.admission.mcp_deployment != query.mcp_deployment
        || operation_payload.admission.server_revision != record.snapshot.server_revision
        || operation_payload.admission.protocol_profile != record.snapshot.protocol_profile
        || operation_payload.admission.authorization_binding_id != query.authorization_binding_id
        || operation_payload.admission.authorization_generation != query.authorization_generation
        || operation_payload.admission.authorization_context_digest
            != query.authorization_context_digest
        || operation_payload.admission.principal_id != query.principal_id
        || result.snapshot_id != record.snapshot.snapshot_id
        || result.snapshot_digest != record.snapshot.canonical_digest
        || result.objects_artifact != record.snapshot.objects_artifact
        || result.artifact_link_id != record.artifact_link_id
    {
        return Err(RepositoryError::Conflict("MCP discovery operation closure"));
    }

    let link = sqlx::query(
        r#"
        SELECT link_kind, owner_kind, owner_id, target_artifact_id, link_key_digest,
               state, payload_schema_version, payload, payload_digest, released_at
        FROM insight_platform.artifact_links
        WHERE tenant_id = $1 AND artifact_link_id = $2
        "#,
    )
    .bind(query.tenant_id.to_string())
    .bind(record.artifact_link_id.to_string())
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::NotFound("MCP discovery ArtifactLink"))?;
    if link.try_get::<String, _>("link_kind")? != "reference"
        || link.try_get::<String, _>("owner_kind")? != "mcp_operation"
        || link.try_get::<String, _>("owner_id")? != record.source_operation_id.to_string()
        || link.try_get::<Option<String>, _>("target_artifact_id")?
            != Some(record.snapshot.objects_artifact.artifact_id().to_string())
        || link.try_get::<String, _>("state")? != "active"
        || link
            .try_get::<Option<DateTime<Utc>>, _>("released_at")?
            .is_some()
    {
        return Err(RepositoryError::Conflict("MCP discovery ArtifactLink"));
    }
    let link_payload =
        payload_from_row(&link, "payload_schema_version", "payload", "payload_digest")?;
    let reference: ArtifactReferenceSnapshot =
        decode_versioned_payload(&link_payload, "MCP discovery ArtifactLink")?;
    if reference.artifact_id != *record.snapshot.objects_artifact.artifact_id()
        || reference.owner_id != record.source_operation_id
        || reference.reference_kind != ArtifactReferenceKind::Evidence
        || reference.purpose != ArtifactPurpose::McpResource
        || reference
            .link_key_digest()
            .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?
            .to_string()
            != link.try_get::<String, _>("link_key_digest")?
    {
        return Err(RepositoryError::Conflict(
            "MCP discovery ArtifactLink payload",
        ));
    }
    Ok(())
}

async fn load_mcp_discovery_operation(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    operation_id: &ResourceId,
    for_update: bool,
) -> Result<McpDiscoveryOperationRecord, RepositoryError> {
    let query = if for_update {
        "SELECT * FROM insight_platform.invocations WHERE tenant_id = $1 AND invocation_id = $2 FOR UPDATE"
    } else {
        "SELECT * FROM insight_platform.invocations WHERE tenant_id = $1 AND invocation_id = $2"
    };
    let row = sqlx::query(query)
        .bind(tenant_id.to_string())
        .bind(operation_id.to_string())
        .fetch_optional(&mut **transaction)
        .await?
        .ok_or(RepositoryError::NotFound("MCP discovery operation"))?;
    if row.try_get::<String, _>("invocation_kind")? != "mcp_discovery"
        || row.try_get::<String, _>("owner_kind")? != "mcp_operation"
        || row.try_get::<String, _>("owner_id")? != operation_id.to_string()
    {
        return Err(RepositoryError::CorruptRow(
            "MCP discovery operation row has the wrong owner".to_owned(),
        ));
    }
    let payload = payload_from_row(&row, "payload_schema_version", "payload", "payload_digest")?;
    let payload: McpDiscoveryOperationPayload =
        decode_versioned_payload(&payload, "MCP discovery operation")?;
    let record = McpDiscoveryOperationRecord {
        tenant_id: row
            .try_get::<String, _>("tenant_id")?
            .parse::<ResourceId>()
            .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?,
        operation_id: row
            .try_get::<String, _>("invocation_id")?
            .parse::<ResourceId>()
            .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?,
        job_id: payload.admission.job_id.clone(),
        logical_key: row.try_get("logical_key")?,
        state: row
            .try_get::<String, _>("state")?
            .parse::<McpDiscoveryOperationState>()
            .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?,
        version: u64::try_from(row.try_get::<i64, _>("version")?).map_err(|_| {
            RepositoryError::CorruptRow("negative MCP discovery operation version".to_owned())
        })?,
        payload,
        deadline: row.try_get("deadline")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
        terminal_at: row.try_get("terminal_at")?,
    };
    record
        .validate()
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    Ok(record)
}

struct LockedMcpDiscoveryJob {
    trace: insight_platform_contracts::TraceIdentityV1,
    state: JobState,
    version: u64,
    physical_attempt: u32,
    attempt_limit: u32,
    lease_generation: u64,
    worker_id: Option<String>,
    lease_token_digest: Option<String>,
    heartbeat_at: Option<DateTime<Utc>>,
    lease_expires_at: Option<DateTime<Utc>>,
    scheduled_at: DateTime<Utc>,
    retry_at: Option<DateTime<Utc>>,
    wake_kind: Option<String>,
    wake_state: Option<String>,
    wake_generation: u64,
    deadline: DateTime<Utc>,
    owner_id: String,
    invocation_id: Option<String>,
    payload: McpDiscoveryJobPayload,
}

async fn load_mcp_discovery_job(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    job_id: &ResourceId,
    for_update: bool,
) -> Result<LockedMcpDiscoveryJob, RepositoryError> {
    let query = if for_update {
        "SELECT * FROM insight_platform.jobs WHERE tenant_id = $1 AND job_id = $2 FOR UPDATE"
    } else {
        "SELECT * FROM insight_platform.jobs WHERE tenant_id = $1 AND job_id = $2"
    };
    let row = sqlx::query(query)
        .bind(tenant_id.to_string())
        .bind(job_id.to_string())
        .fetch_optional(&mut **transaction)
        .await?
        .ok_or(RepositoryError::NotFound("MCP discovery Job"))?;
    if row.try_get::<String, _>("work_class")? != "mcp"
        || row.try_get::<String, _>("job_kind")? != "mcp_discovery"
        || row.try_get::<String, _>("owner_kind")? != "mcp_operation"
    {
        return Err(RepositoryError::CorruptRow(
            "MCP discovery Job has the wrong typed owner".to_owned(),
        ));
    }
    let payload = payload_from_row(&row, "payload_schema_version", "payload", "payload_digest")?;
    let payload: McpJobPayload = decode_typed_payload(&payload, "MCP Job")?;
    let McpJobPayload::Discovery(payload) = payload else {
        return Err(RepositoryError::CorruptRow(
            "MCP discovery Job contains a subscription payload".to_owned(),
        ));
    };
    payload
        .validate()
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    Ok(LockedMcpDiscoveryJob {
        trace: insight_platform_contracts::TraceIdentityV1::new(
            row.try_get::<String, _>("trace_id")?
                .parse::<insight_platform_contracts::TraceId>()
                .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?,
        ),
        state: row
            .try_get::<String, _>("state")?
            .parse::<JobState>()
            .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?,
        version: u64::try_from(row.try_get::<i64, _>("version")?)
            .map_err(|_| RepositoryError::CorruptRow("negative MCP Job version".to_owned()))?,
        physical_attempt: u32::try_from(row.try_get::<i32, _>("attempt_no")?).map_err(|_| {
            RepositoryError::CorruptRow("negative MCP Job physical attempt".to_owned())
        })?,
        attempt_limit: u32::try_from(row.try_get::<i32, _>("attempt_limit")?).map_err(|_| {
            RepositoryError::CorruptRow("negative MCP Job attempt limit".to_owned())
        })?,
        lease_generation: u64::try_from(row.try_get::<i64, _>("lease_epoch")?).map_err(|_| {
            RepositoryError::CorruptRow("negative MCP Job lease generation".to_owned())
        })?,
        worker_id: row.try_get("worker_id")?,
        lease_token_digest: row.try_get("lease_token_digest")?,
        heartbeat_at: row.try_get("heartbeat_at")?,
        lease_expires_at: row.try_get("lease_expires_at")?,
        scheduled_at: row.try_get("scheduled_at")?,
        retry_at: row.try_get("retry_at")?,
        wake_kind: row.try_get("wake_kind")?,
        wake_state: row.try_get("wake_state")?,
        wake_generation: u64::try_from(row.try_get::<i64, _>("wake_generation")?).map_err(
            |_| RepositoryError::CorruptRow("negative MCP Job wake generation".to_owned()),
        )?,
        deadline: row.try_get("deadline")?,
        owner_id: row.try_get("owner_id")?,
        invocation_id: row.try_get("invocation_id")?,
        payload,
    })
}

pub(crate) struct McpDiscoveryArtifactStageAuthority {
    pub trace: insight_platform_contracts::TraceIdentityV1,
    pub principal_id: ResourceId,
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn wake_mcp_discovery_after_artifact_verification(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    producer_job_id: &ResourceId,
    verification_job_id: &ResourceId,
    artifact_id: &ResourceId,
    blob_id: &ResourceId,
    content_digest: &Sha256Digest,
    verification_evidence_digest: &Sha256Digest,
    disposition: ArtifactScanDisposition,
    database_now: DateTime<Utc>,
) -> Result<(), RepositoryError> {
    let job = load_mcp_discovery_job(transaction, tenant_id, producer_job_id, true).await?;
    let operation_id = job
        .owner_id
        .parse::<ResourceId>()
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    let operation =
        load_mcp_discovery_operation(transaction, tenant_id, &operation_id, true).await?;
    let evidence =
        operation
            .payload
            .pending_verification
            .as_ref()
            .ok_or(RepositoryError::Conflict(
                "MCP discovery verification wake evidence",
            ))?;
    if operation.state != McpDiscoveryOperationState::Running
        || operation.job_id != *producer_job_id
        || evidence.verification_job_id != *verification_job_id
        || evidence.artifact_id != *artifact_id
        || evidence.blob_id != *blob_id
        || evidence.descriptor_digest != *content_digest
        || job.state != JobState::Waiting
        || job.worker_id.is_some()
        || job.lease_token_digest.is_some()
        || job.lease_expires_at.is_some()
        || job.heartbeat_at.is_some()
        || job.wake_kind.as_deref() != Some(WakeKind::RemoteInvocation.as_str())
        || job.wake_state.as_deref() != Some("pending")
        || job.wake_generation == 0
    {
        return Err(RepositoryError::Conflict(
            "MCP discovery verification wake authority",
        ));
    }
    let wake = WakeContract {
        kind: WakeKind::RemoteInvocation,
        generation: job.wake_generation,
        accepted_sources: vec![WakeSource::Signal, WakeSource::Timeout],
        expected_response_schema_digest: None,
        opaque_state_digest: Some(evidence.canonical_digest.clone()),
        next_poll_at: None,
        poll_count: 0,
        poll_limit: 0,
        callback_binding_digest: None,
        deadline: operation.deadline.min(evidence.expires_at),
    };
    let current = JobProjection {
        trace: job.trace,
        tenant_id: tenant_id.clone(),
        job_id: producer_job_id.clone(),
        work_class: WorkClass::Mcp,
        owner: JobOwnerRef {
            owner_id: operation_id.clone(),
            owner_kind: ResourceKind::McpOperation,
        },
        state: job.state,
        version: job.version,
        attempt_count: job.physical_attempt,
        attempt_limit: job.attempt_limit,
        lease_generation: job.lease_generation,
        lease: None,
        scheduled_at: job.scheduled_at,
        retry_at: job.retry_at,
        wake: Some(wake),
        deadline: job.deadline,
    };
    let next = decide_job_wake(
        &current,
        job.wake_generation,
        WakeSource::Signal,
        database_now,
    )?;
    let next_operation_version = operation.version.checked_add(1).ok_or_else(|| {
        RepositoryError::InvalidInput("MCP discovery operation version overflow".to_owned())
    })?;
    let operation_affected = sqlx::query(
        r#"
        UPDATE insight_platform.invocations
        SET version = $4, updated_at = $5
        WHERE tenant_id = $1 AND invocation_id = $2 AND version = $3
          AND invocation_kind = 'mcp_discovery' AND state = 'running'
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(operation_id.to_string())
    .bind(as_i64(
        operation.version,
        "MCP discovery operation version",
    )?)
    .bind(as_i64(
        next_operation_version,
        "MCP discovery operation version",
    )?)
    .bind(database_now)
    .execute(&mut **transaction)
    .await?
    .rows_affected();
    if operation_affected != 1 {
        return Err(RepositoryError::Conflict(
            "MCP discovery verification wake operation CAS",
        ));
    }
    let job_affected = sqlx::query(
        r#"
        UPDATE insight_platform.jobs
        SET state = 'ready', version = $4, scheduled_at = $5,
            wake_kind = NULL, wake_state = NULL, wake_generation = 0,
            updated_at = $5
        WHERE tenant_id = $1 AND job_id = $2 AND version = $3
          AND work_class = 'mcp' AND job_kind = 'mcp_discovery'
          AND owner_kind = 'mcp_operation' AND state = 'waiting'
          AND wake_kind = 'remote_invocation' AND wake_state = 'pending'
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(producer_job_id.to_string())
    .bind(as_i64(job.version, "MCP discovery Job version")?)
    .bind(as_i64(next.version, "MCP discovery Job version")?)
    .bind(database_now)
    .execute(&mut **transaction)
    .await?
    .rows_affected();
    if job_affected != 1 {
        return Err(RepositoryError::Conflict(
            "MCP discovery verification wake Job CAS",
        ));
    }
    let event_id = ResourceId::from_uuid_v7(ResourceKind::Event, Uuid::now_v7())
        .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;
    let outbox_id = ResourceId::from_uuid_v7(ResourceKind::OutboxEvent, Uuid::now_v7())
        .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;
    append_scheduler_event_with_trace(
        transaction,
        job.trace,
        &tenant_id.to_string(),
        &event_id,
        &outbox_id,
        "mcp_operation",
        &operation_id.to_string(),
        as_i64(next_operation_version, "MCP discovery operation version")?,
        None,
        "mcp.discovery_verification_woken",
        &TypedPayload::new(
            1,
            &serde_json::json!({
                "artifact_id": artifact_id,
                "disposition": disposition,
                "job_id": producer_job_id,
                "verification_evidence_digest": verification_evidence_digest,
                "verification_job_id": verification_job_id,
            }),
        )?,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn require_mcp_discovery_artifact_stage_authority_for(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    producer_job_id: &ResourceId,
    producer_fence: &JobFence,
    verification_job_id: &ResourceId,
    artifact_id: &ResourceId,
    blob_id: &ResourceId,
    database_now: DateTime<Utc>,
) -> Result<McpDiscoveryArtifactStageAuthority, RepositoryError> {
    let job = load_mcp_discovery_job(transaction, tenant_id, producer_job_id, true).await?;
    let operation_id = job
        .owner_id
        .parse::<ResourceId>()
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    if operation_id.kind() != ResourceKind::McpOperation {
        return Err(RepositoryError::CorruptRow(
            "MCP discovery Job owner is invalid".to_owned(),
        ));
    }
    let operation =
        load_mcp_discovery_operation(transaction, tenant_id, &operation_id, true).await?;
    require_exact_mcp_discovery_job_fence(&job, &operation, producer_fence, database_now)?;
    let preallocation = &operation.payload.admission.artifact_preallocation;
    if !matches!(
        operation.state,
        McpDiscoveryOperationState::Pending | McpDiscoveryOperationState::Running
    ) || operation.job_id != *producer_job_id
        || preallocation.verification_job_id != *verification_job_id
        || preallocation.artifact_id != *artifact_id
        || preallocation.blob_id != *blob_id
    {
        return Err(RepositoryError::StaleFence);
    }
    Ok(McpDiscoveryArtifactStageAuthority {
        trace: job.trace,
        principal_id: operation.payload.admission.principal_id.clone(),
    })
}

fn mcp_discovery_job_projection(
    job: &LockedMcpDiscoveryJob,
    tenant_id: &ResourceId,
    job_id: &ResourceId,
    operation_id: &ResourceId,
) -> Result<JobProjection, RepositoryError> {
    let lease = match (
        &job.worker_id,
        &job.lease_token_digest,
        job.heartbeat_at,
        job.lease_expires_at,
    ) {
        (Some(worker_id), Some(token_digest), Some(heartbeat_at), Some(expires_at)) => {
            Some(JobLease {
                worker_process_generation_id: worker_id
                    .parse::<ResourceId>()
                    .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?,
                lease_generation: job.lease_generation,
                token_digest: token_digest
                    .parse::<Sha256Digest>()
                    .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?,
                heartbeat_at,
                expires_at,
            })
        }
        (None, None, None, None) => None,
        _ => {
            return Err(RepositoryError::CorruptRow(
                "MCP discovery Job lease columns are incomplete".to_owned(),
            ));
        }
    };
    let projection = JobProjection {
        trace: job.trace,
        tenant_id: tenant_id.clone(),
        job_id: job_id.clone(),
        work_class: insight_platform_contracts::WorkClass::Mcp,
        owner: JobOwnerRef {
            owner_id: operation_id.clone(),
            owner_kind: insight_platform_contracts::ResourceKind::McpOperation,
        },
        state: job.state,
        version: job.version,
        attempt_count: job.physical_attempt,
        attempt_limit: job.attempt_limit,
        lease_generation: job.lease_generation,
        lease,
        scheduled_at: job.scheduled_at,
        retry_at: job.retry_at,
        wake: None,
        deadline: job.deadline,
    };
    projection
        .validate()
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    Ok(projection)
}

fn require_exact_mcp_discovery_job_fence(
    job: &LockedMcpDiscoveryJob,
    operation: &McpDiscoveryOperationRecord,
    fence: &insight_platform_jobs::JobFence,
    database_now: DateTime<Utc>,
) -> Result<(), RepositoryError> {
    let worker_id = fence.worker_process_generation_id.to_string();
    if job.state != JobState::Running
        || job.version != fence.expected_version
        || job.physical_attempt == 0
        || job.lease_generation != fence.lease_generation
        || job.worker_id.as_deref() != Some(worker_id.as_str())
        || job.lease_token_digest.as_deref() != Some(fence.token_digest.as_str())
        || job
            .lease_expires_at
            .is_none_or(|expiry| expiry <= database_now)
        || job.wake_kind.is_some()
        || job.wake_state.is_some()
        || job.wake_generation != 0
        || job.owner_id != operation.operation_id.to_string()
        || job.invocation_id != Some(operation.operation_id.to_string())
        || job.payload.operation_id != operation.operation_id
        || job.payload.admission_digest != operation.payload.admission.canonical_digest
    {
        return Err(RepositoryError::StaleFence);
    }
    Ok(())
}

async fn validate_mcp_discovery_frozen_dependencies(
    transaction: &mut Transaction<'_, Postgres>,
    operation: &McpDiscoveryOperationRecord,
    database_now: DateTime<Utc>,
) -> Result<(), RepositoryError> {
    let admission = &operation.payload.admission;
    let authorization_record = load_mcp_authorization_binding(
        transaction,
        &operation.tenant_id,
        &admission.authorization_binding_id,
        false,
    )
    .await?;
    let authorization = authorization_record
        .execution_context(database_now)
        .map_err(invalid_authorization)?;
    if authorization.generation != admission.authorization_generation
        || authorization.canonical_digest != admission.authorization_context_digest
        || authorization.principal_id != admission.principal_id
        || authorization.mcp_deployment != admission.mcp_deployment
    {
        return Err(RepositoryError::Conflict(
            "MCP discovery authorization changed",
        ));
    }
    validate_principal_binding(
        transaction,
        &authorization.tenant_id,
        &authorization.principal_id,
        authorization.principal_identity_kind,
        authorization.principal_binding_generation,
    )
    .await?;
    validate_mcp_authorization_dependencies(
        transaction,
        &operation.tenant_id,
        &authorization.mcp_deployment,
        &authorization.audience_identity_digest,
        &authorization.token_secret_binding,
    )
    .await
}

async fn settle_mcp_discovery_artifact_quota(
    transaction: &mut Transaction<'_, Postgres>,
    operation: &McpDiscoveryOperationRecord,
    command: &FinalizeMcpDiscovery,
    actual_amount: u64,
    database_now: DateTime<Utc>,
) -> Result<(), RepositoryError> {
    let admission = &operation.payload.admission;
    let quota_account_id = &admission.artifact_policy.quota_account_id;
    let actual_amount = i64::try_from(actual_amount).map_err(|_| {
        RepositoryError::InvalidInput("MCP discovery quota amount exceeds bigint".to_owned())
    })?;
    let reservation_amount =
        i64::try_from(admission.artifact_policy.maximum_bytes).map_err(|_| {
            RepositoryError::CorruptRow("MCP discovery quota reservation exceeds bigint".to_owned())
        })?;
    if actual_amount > reservation_amount {
        return Err(RepositoryError::Conflict(
            "MCP discovery Artifact quota amount",
        ));
    }
    let account = sqlx::query(
        r#"
        SELECT scope_kind, scope_id, work_class, metric, reserved_value, version
        FROM insight_platform.quota_accounts
        WHERE tenant_id = $1 AND quota_account_id = $2
        FOR UPDATE
        "#,
    )
    .bind(command.audit.tenant_id.to_string())
    .bind(quota_account_id.to_string())
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::NotFound(
        "MCP discovery Artifact quota account",
    ))?;
    if account.try_get::<String, _>("scope_kind")? != "tenant"
        || account.try_get::<String, _>("scope_id")? != command.audit.tenant_id.to_string()
        || account.try_get::<String, _>("work_class")? != "artifact"
        || account.try_get::<String, _>("metric")? != "artifact.staging_bytes"
        || account.try_get::<i64, _>("reserved_value")? < reservation_amount
    {
        return Err(RepositoryError::Conflict(
            "MCP discovery Artifact quota authority",
        ));
    }
    let reserved: Option<i64> = sqlx::query_scalar(
        r#"
        SELECT reserved_amount
        FROM insight_platform.quota_ledger
        WHERE tenant_id = $1 AND quota_entry_id = $2 AND quota_account_id = $3
          AND correlation_id = $4 AND entry_kind = 'reserve'
        FOR SHARE
        "#,
    )
    .bind(command.audit.tenant_id.to_string())
    .bind(admission.artifact_preallocation.quota_entry_id.to_string())
    .bind(quota_account_id.to_string())
    .bind(admission.artifact_preallocation.artifact_id.to_string())
    .fetch_optional(&mut **transaction)
    .await?;
    if reserved != Some(reservation_amount) {
        return Err(RepositoryError::Conflict(
            "MCP discovery Artifact quota reservation",
        ));
    }
    let account_version: i64 = account.try_get("version")?;
    let next_version: i64 = sqlx::query_scalar(
        r#"
        UPDATE insight_platform.quota_accounts
        SET reserved_value = reserved_value - $4, version = version + 1,
            updated_at = $5
        WHERE tenant_id = $1 AND quota_account_id = $2 AND version = $3
          AND reserved_value >= $4
        RETURNING version
        "#,
    )
    .bind(command.audit.tenant_id.to_string())
    .bind(quota_account_id.to_string())
    .bind(account_version)
    .bind(reservation_amount)
    .bind(database_now)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::Conflict(
        "MCP discovery Artifact quota account",
    ))?;
    let settlement_id = ResourceId::from_uuid_v7(ResourceKind::QuotaLedgerEntry, Uuid::now_v7())
        .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;
    sqlx::query(
        r#"
        INSERT INTO insight_platform.quota_ledger (
            tenant_id, quota_entry_id, quota_account_id, correlation_id,
            entry_kind, reserved_amount, used_amount, account_version, request_digest
        ) VALUES ($1, $2, $3, $4, 'settle', $5, 0, $6, $7)
        "#,
    )
    .bind(command.audit.tenant_id.to_string())
    .bind(settlement_id.to_string())
    .bind(quota_account_id.to_string())
    .bind(admission.artifact_preallocation.artifact_id.to_string())
    .bind(reservation_amount)
    .bind(next_version)
    .bind(command.audit.request_digest.to_string())
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

async fn finalize_mcp_discovery_owner_job(
    transaction: &mut Transaction<'_, Postgres>,
    command: &FinalizeMcpDiscovery,
    result_digest: &Sha256Digest,
    database_now: DateTime<Utc>,
) -> Result<(), RepositoryError> {
    let affected = sqlx::query(
        r#"
        UPDATE insight_platform.jobs
        SET state = 'succeeded', version = version + 1, result_digest = $7,
            worker_id = NULL, lease_token_digest = NULL, lease_expires_at = NULL,
            heartbeat_at = NULL, terminal_at = $8, updated_at = $8
        WHERE tenant_id = $1 AND job_id = $2 AND version = $3
          AND work_class = 'mcp' AND job_kind = 'mcp_discovery'
          AND owner_kind = 'mcp_operation'
          AND state = 'running' AND worker_id = $4 AND lease_epoch = $5
          AND lease_token_digest = $6
        "#,
    )
    .bind(command.audit.tenant_id.to_string())
    .bind(command.job_id.to_string())
    .bind(as_i64(
        command.fence.expected_version,
        "MCP discovery Job version",
    )?)
    .bind(command.fence.worker_process_generation_id.to_string())
    .bind(as_i64(
        command.fence.lease_generation,
        "MCP discovery Job lease generation",
    )?)
    .bind(command.fence.token_digest.to_string())
    .bind(result_digest.to_string())
    .bind(database_now)
    .execute(&mut **transaction)
    .await?
    .rows_affected();
    if affected != 1 {
        return Err(RepositoryError::Conflict("MCP discovery Job finalize CAS"));
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn fail_mcp_discovery_after_verification(
    transaction: &mut Transaction<'_, Postgres>,
    operation: &McpDiscoveryOperationRecord,
    command: &FinalizeMcpDiscovery,
    owner_job: &LockedMcpDiscoveryJob,
    verification_row: &sqlx::postgres::PgRow,
    verification_state: &str,
    evidence_digest: &Sha256Digest,
    failure_code: &str,
    database_now: DateTime<Utc>,
) -> Result<(), RepositoryError> {
    let pending = operation
        .payload
        .pending_verification
        .as_ref()
        .ok_or(RepositoryError::Conflict("MCP discovery failure evidence"))?;
    if verification_state == "waiting" {
        let verification_version = u64::try_from(
            verification_row.try_get::<i64, _>("verification_version")?,
        )
        .map_err(|_| RepositoryError::CorruptRow("negative verification Job version".to_owned()))?;
        let affected = sqlx::query(
            r#"
            UPDATE insight_platform.jobs
            SET state = 'failed', version = $4, result_digest = $5,
                terminal_at = $6, updated_at = $6
            WHERE tenant_id = $1 AND job_id = $2 AND version = $3
              AND work_class = 'artifact' AND job_kind = 'artifact_scan'
              AND owner_kind = 'artifact' AND state = 'waiting'
            "#,
        )
        .bind(command.audit.tenant_id.to_string())
        .bind(pending.verification_job_id.to_string())
        .bind(as_i64(
            verification_version,
            "Artifact verification Job version",
        )?)
        .bind(as_i64(
            verification_version
                .checked_add(1)
                .ok_or(RepositoryError::Conflict(
                    "Artifact verification Job version",
                ))?,
            "Artifact verification Job version",
        )?)
        .bind(evidence_digest.to_string())
        .bind(database_now)
        .execute(&mut **transaction)
        .await?
        .rows_affected();
        if affected != 1 {
            return Err(RepositoryError::Conflict(
                "Artifact verification Job failure CAS",
            ));
        }
    } else if verification_state != "failed" {
        return Err(RepositoryError::Conflict(
            "Artifact verification Job failure state",
        ));
    }

    if verification_row.try_get::<String, _>("artifact_state")? == "verified" {
        let artifact_version =
            u64::try_from(verification_row.try_get::<i64, _>("artifact_version")?)
                .map_err(|_| RepositoryError::CorruptRow("negative Artifact version".to_owned()))?;
        let affected = sqlx::query(
            r#"
            UPDATE insight_platform.artifacts
            SET state = 'quarantined', version = $4, updated_at = $5
            WHERE tenant_id = $1 AND artifact_id = $2 AND version = $3
              AND state = 'verified'
            "#,
        )
        .bind(command.audit.tenant_id.to_string())
        .bind(pending.artifact_id.to_string())
        .bind(as_i64(artifact_version, "MCP discovery Artifact version")?)
        .bind(as_i64(
            artifact_version
                .checked_add(1)
                .ok_or(RepositoryError::Conflict("MCP discovery Artifact version"))?,
            "MCP discovery Artifact version",
        )?)
        .bind(database_now)
        .execute(&mut **transaction)
        .await?
        .rows_affected();
        if affected != 1 {
            return Err(RepositoryError::Conflict(
                "MCP discovery expired Artifact quarantine CAS",
            ));
        }
    }

    let next_operation_version = operation.version.checked_add(1).ok_or_else(|| {
        RepositoryError::InvalidInput("MCP operation version overflow".to_owned())
    })?;
    let operation_affected = sqlx::query(
        r#"
        UPDATE insight_platform.invocations
        SET state = 'failed', version = $4, terminal_at = $5, updated_at = $5
        WHERE tenant_id = $1 AND invocation_id = $2 AND version = $3
          AND invocation_kind = 'mcp_discovery' AND state = 'running'
        "#,
    )
    .bind(command.audit.tenant_id.to_string())
    .bind(command.operation_id.to_string())
    .bind(as_i64(
        operation.version,
        "MCP discovery operation version",
    )?)
    .bind(as_i64(
        next_operation_version,
        "MCP discovery operation version",
    )?)
    .bind(database_now)
    .execute(&mut **transaction)
    .await?
    .rows_affected();
    if operation_affected != 1 {
        return Err(RepositoryError::Conflict(
            "MCP discovery verification failure CAS",
        ));
    }

    let owner_affected = sqlx::query(
        r#"
        UPDATE insight_platform.jobs
        SET state = 'failed', version = version + 1, result_digest = $7,
            worker_id = NULL, lease_token_digest = NULL, lease_expires_at = NULL,
            heartbeat_at = NULL, terminal_at = $8, updated_at = $8
        WHERE tenant_id = $1 AND job_id = $2 AND version = $3
          AND work_class = 'mcp' AND job_kind = 'mcp_discovery'
          AND owner_kind = 'mcp_operation'
          AND state = 'running' AND worker_id = $4 AND lease_epoch = $5
          AND lease_token_digest = $6
        "#,
    )
    .bind(command.audit.tenant_id.to_string())
    .bind(command.job_id.to_string())
    .bind(as_i64(
        command.fence.expected_version,
        "MCP discovery Job version",
    )?)
    .bind(command.fence.worker_process_generation_id.to_string())
    .bind(as_i64(
        command.fence.lease_generation,
        "MCP discovery Job lease generation",
    )?)
    .bind(command.fence.token_digest.to_string())
    .bind(evidence_digest.to_string())
    .bind(database_now)
    .execute(&mut **transaction)
    .await?
    .rows_affected();
    if owner_affected != 1 {
        return Err(RepositoryError::Conflict(
            "MCP discovery Job verification failure CAS",
        ));
    }

    append_scheduler_event_with_trace(
        transaction,
        owner_job.trace,
        &command.audit.tenant_id.to_string(),
        &command.audit.event_id,
        &command.audit.outbox_id,
        "mcp_operation",
        &command.operation_id.to_string(),
        as_i64(next_operation_version, "MCP discovery operation version")?,
        None,
        "mcp.discovery_failed",
        &TypedPayload::new(
            1,
            &serde_json::json!({
                "artifact_id": pending.artifact_id,
                "failure_code": failure_code,
                "job_id": command.job_id,
                "verification_evidence_digest": evidence_digest,
                "verification_job_id": pending.verification_job_id,
            }),
        )?,
    )
    .await?;
    terminalize_mcp_discovery_finalize_receipt(
        transaction,
        command,
        failure_code,
        &pending.artifact_id,
    )
    .await
}

async fn insert_mcp_discovery_snapshot(
    transaction: &mut Transaction<'_, Postgres>,
    record: &McpDiscoverySnapshotRecord,
    created_by: &ResourceId,
    database_now: DateTime<Utc>,
) -> Result<(), RepositoryError> {
    let payload = TypedPayload::from_versioned(1, record, 1_048_576)?;
    sqlx::query(
        r#"
        INSERT INTO insight_platform.resources (
            tenant_id, resource_id, resource_kind, lifecycle_state, gate_state,
            payload_schema_version, payload, payload_digest, created_at, updated_at
        ) VALUES ($1, $2, 'mcp_discovery_snapshot', 'active', 'enabled',
                  $3, $4, $5, $6, $6)
        "#,
    )
    .bind(record.tenant_id.to_string())
    .bind(record.snapshot.snapshot_id.to_string())
    .bind(payload.schema_version)
    .bind(&payload.value)
    .bind(&payload.digest)
    .bind(database_now)
    .execute(&mut **transaction)
    .await?;

    let reference = ArtifactReferenceSnapshot {
        schema_version: 1,
        artifact_id: record.snapshot.objects_artifact.artifact_id().clone(),
        owner_id: record.source_operation_id.clone(),
        reference_kind: ArtifactReferenceKind::Evidence,
        purpose: ArtifactPurpose::McpResource,
        created_by: created_by.clone(),
    };
    let reference_payload = TypedPayload::from_versioned(1, &reference, 262_144)?;
    sqlx::query(
        r#"
        INSERT INTO insight_platform.artifact_links (
            tenant_id, artifact_link_id, link_kind, owner_kind, owner_id,
            target_artifact_id, link_key_digest, state, payload_schema_version,
            payload, payload_digest, created_at, updated_at
        ) VALUES ($1, $2, 'reference', 'mcp_operation', $3,
                  $4, $5, 'active', $6, $7, $8, $9, $9)
        "#,
    )
    .bind(record.tenant_id.to_string())
    .bind(record.artifact_link_id.to_string())
    .bind(record.source_operation_id.to_string())
    .bind(record.snapshot.objects_artifact.artifact_id().to_string())
    .bind(
        reference
            .link_key_digest()
            .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?
            .to_string(),
    )
    .bind(reference_payload.schema_version)
    .bind(&reference_payload.value)
    .bind(&reference_payload.digest)
    .bind(database_now)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

async fn update_resolved_mcp_discovery_job(
    transaction: &mut Transaction<'_, Postgres>,
    command: &ResolveMcpDiscoveryAttempt,
    next: &JobProjection,
    resolution_digest: &str,
    database_now: DateTime<Utc>,
) -> Result<(), RepositoryError> {
    let terminal_at = matches!(
        next.state,
        JobState::Succeeded | JobState::Failed | JobState::Cancelled | JobState::TimedOut
    )
    .then_some(database_now);
    let result_digest = terminal_at.map(|_| resolution_digest);
    let affected = sqlx::query(
        r#"
        UPDATE insight_platform.jobs
        SET state = $7, version = $8, retry_at = $9, result_digest = $10,
            worker_id = NULL, lease_token_digest = NULL, lease_expires_at = NULL,
            heartbeat_at = NULL, terminal_at = $11, updated_at = $12
        WHERE tenant_id = $1 AND job_id = $2 AND version = $3
          AND work_class = 'mcp' AND job_kind = 'mcp_discovery'
          AND owner_kind = 'mcp_operation'
          AND state = 'running' AND worker_id = $4 AND lease_epoch = $5
          AND lease_token_digest = $6
        "#,
    )
    .bind(command.audit.tenant_id.to_string())
    .bind(command.job_id.to_string())
    .bind(as_i64(
        command.fence.expected_version,
        "MCP discovery Job version",
    )?)
    .bind(command.fence.worker_process_generation_id.to_string())
    .bind(as_i64(
        command.fence.lease_generation,
        "MCP discovery Job lease generation",
    )?)
    .bind(command.fence.token_digest.to_string())
    .bind(next.state.as_str())
    .bind(as_i64(next.version, "MCP discovery Job version")?)
    .bind(next.retry_at)
    .bind(result_digest)
    .bind(terminal_at)
    .bind(database_now)
    .execute(&mut **transaction)
    .await?
    .rows_affected();
    if affected != 1 {
        return Err(RepositoryError::StaleFence);
    }
    Ok(())
}

async fn update_owner_terminal_mcp_discovery_job(
    transaction: &mut Transaction<'_, Postgres>,
    operation: &McpDiscoveryOperationRecord,
    next: &JobProjection,
    database_now: DateTime<Utc>,
) -> Result<(), RepositoryError> {
    let expected_version = next.version.checked_sub(1).ok_or_else(|| {
        RepositoryError::CorruptRow("MCP discovery Job version cannot be zero".to_owned())
    })?;
    let affected = sqlx::query(
        r#"
        UPDATE insight_platform.jobs
        SET state = $5, version = $6, retry_at = NULL,
            worker_id = NULL, lease_token_digest = NULL, lease_expires_at = NULL,
            heartbeat_at = NULL, terminal_at = $7, updated_at = $7
        WHERE tenant_id = $1 AND job_id = $2 AND version = $3
          AND work_class = 'mcp' AND job_kind = 'mcp_discovery'
          AND owner_kind = 'mcp_operation' AND owner_id = $4
          AND state IN ('ready', 'leased', 'running', 'retry_scheduled', 'cancelling')
        "#,
    )
    .bind(operation.tenant_id.to_string())
    .bind(operation.job_id.to_string())
    .bind(as_i64(expected_version, "MCP discovery Job version")?)
    .bind(operation.operation_id.to_string())
    .bind(next.state.as_str())
    .bind(as_i64(next.version, "MCP discovery Job version")?)
    .bind(database_now)
    .execute(&mut **transaction)
    .await?
    .rows_affected();
    if affected != 1 {
        return Err(RepositoryError::Conflict(
            "MCP discovery Job cancellation CAS",
        ));
    }
    Ok(())
}

async fn update_recovered_mcp_discovery_job(
    transaction: &mut Transaction<'_, Postgres>,
    command: &RecoverExpiredMcpDiscoveryJob,
    next: &JobProjection,
    database_now: DateTime<Utc>,
) -> Result<(), RepositoryError> {
    let terminal_at = matches!(
        next.state,
        JobState::Succeeded | JobState::Failed | JobState::Cancelled | JobState::TimedOut
    )
    .then_some(database_now);
    let affected = sqlx::query(
        r#"
        UPDATE insight_platform.jobs
        SET state = $7, version = $8, scheduled_at = $9, retry_at = $10,
            worker_id = NULL, lease_token_digest = NULL, lease_expires_at = NULL,
            heartbeat_at = NULL, terminal_at = $11, updated_at = $12
        WHERE tenant_id = $1 AND job_id = $2 AND version = $3
          AND lease_epoch = $4
          AND work_class = 'mcp' AND job_kind = 'mcp_discovery'
          AND owner_kind = 'mcp_operation' AND owner_id = $5
          AND state IN ('leased', 'running') AND lease_expires_at <= $6
        "#,
    )
    .bind(command.tenant_id.to_string())
    .bind(command.job_id.to_string())
    .bind(as_i64(
        command.observed_job_version,
        "MCP discovery Job version",
    )?)
    .bind(as_i64(
        command.observed_lease_generation,
        "MCP discovery Job lease generation",
    )?)
    .bind(command.operation_id.to_string())
    .bind(database_now)
    .bind(next.state.as_str())
    .bind(as_i64(next.version, "MCP discovery Job version")?)
    .bind(next.scheduled_at)
    .bind(next.retry_at)
    .bind(terminal_at)
    .bind(database_now)
    .execute(&mut **transaction)
    .await?
    .rows_affected();
    if affected != 1 {
        return Err(RepositoryError::StaleFence);
    }
    Ok(())
}

fn mcp_discovery_resolution_receipt_payload(
    command: &ResolveMcpDiscoveryAttempt,
) -> Result<TypedPayload, RepositoryError> {
    TypedPayload::with_limit(
        1,
        &serde_json::json!({
            "fence": {
                "expected_version": command.fence.expected_version,
                "lease_generation": command.fence.lease_generation,
                "lease_token_digest": command.fence.token_digest,
                "worker_process_generation_id": command.fence.worker_process_generation_id,
            },
            "job_id": command.job_id,
            "operation_id": command.operation_id,
            "resolution": command.resolution,
        }),
        65_536,
    )
}

async fn claim_mcp_resolution_receipt(
    transaction: &mut Transaction<'_, Postgres>,
    command: &ResolveMcpDiscoveryAttempt,
    operation: &str,
    payload: &TypedPayload,
) -> Result<bool, RepositoryError> {
    let inserted = sqlx::query(
        r#"
        INSERT INTO insight_platform.receipts (
            tenant_id, receipt_id, receipt_kind, scope_kind, scope_id,
            dedupe_owner_id, operation, idempotency_key_digest, request_digest,
            state, payload_schema_version, payload, payload_digest, expires_at
        ) VALUES ($1, $2, 'job_commit', 'job', $3, $4, $5, $6, $7,
                  'processing', $8, $9, $10, $11)
        ON CONFLICT (
            tenant_id, receipt_kind, scope_kind, scope_id, dedupe_owner_id,
            operation, idempotency_key_digest
        ) DO NOTHING
        RETURNING receipt_id
        "#,
    )
    .bind(command.audit.tenant_id.to_string())
    .bind(command.audit.receipt_id.to_string())
    .bind(command.job_id.to_string())
    .bind(command.audit.worker_process_generation_id.to_string())
    .bind(operation)
    .bind(command.audit.idempotency_key_digest.to_string())
    .bind(command.audit.request_digest.to_string())
    .bind(payload.schema_version)
    .bind(&payload.value)
    .bind(&payload.digest)
    .bind(command.audit.receipt_expires_at)
    .fetch_optional(&mut **transaction)
    .await?;
    if inserted.is_some() {
        return Ok(false);
    }
    let existing = sqlx::query(
        r#"
        SELECT request_digest, payload_digest, state
        FROM insight_platform.receipts
        WHERE tenant_id = $1 AND receipt_kind = 'job_commit'
          AND scope_kind = 'job' AND scope_id = $2 AND dedupe_owner_id = $3
          AND operation = $4 AND idempotency_key_digest = $5
        FOR UPDATE
        "#,
    )
    .bind(command.audit.tenant_id.to_string())
    .bind(command.job_id.to_string())
    .bind(command.audit.worker_process_generation_id.to_string())
    .bind(operation)
    .bind(command.audit.idempotency_key_digest.to_string())
    .fetch_one(&mut **transaction)
    .await?;
    if existing.try_get::<String, _>("request_digest")? != command.audit.request_digest.to_string()
        || existing.try_get::<String, _>("payload_digest")? != payload.digest
    {
        return Err(RepositoryError::IdempotencyConflict);
    }
    if existing.try_get::<String, _>("state")? != "succeeded" {
        return Err(RepositoryError::Conflict("MCP worker receipt"));
    }
    Ok(true)
}

async fn terminalize_mcp_resolution_receipt(
    transaction: &mut Transaction<'_, Postgres>,
    command: &ResolveMcpDiscoveryAttempt,
    disposition: &str,
) -> Result<(), RepositoryError> {
    let affected = sqlx::query(
        r#"
        UPDATE insight_platform.receipts
        SET state = 'succeeded', disposition = $4, response_reference_id = $5,
            completed_at = clock_timestamp()
        WHERE tenant_id = $1 AND receipt_id = $2 AND request_digest = $3
          AND scope_id = $6 AND state = 'processing'
        "#,
    )
    .bind(command.audit.tenant_id.to_string())
    .bind(command.audit.receipt_id.to_string())
    .bind(command.audit.request_digest.to_string())
    .bind(disposition)
    .bind(command.operation_id.to_string())
    .bind(command.job_id.to_string())
    .execute(&mut **transaction)
    .await?
    .rows_affected();
    if affected != 1 {
        return Err(RepositoryError::Conflict("MCP worker receipt"));
    }
    Ok(())
}

fn mcp_discovery_park_receipt_payload(
    command: &ParkMcpDiscoveryVerification,
) -> Result<TypedPayload, RepositoryError> {
    TypedPayload::with_limit(
        1,
        &serde_json::json!({
            "artifact_id": command.staged.artifact_id,
            "descriptor_digest": command.evidence.descriptor_digest,
            "evidence_digest": command.evidence.canonical_digest,
            "fence": {
                "expected_version": command.fence.expected_version,
                "lease_generation": command.fence.lease_generation,
                "lease_token_digest": command.fence.token_digest,
                "worker_process_generation_id": command.fence.worker_process_generation_id,
            },
            "job_id": command.job_id,
            "operation_id": command.operation_id,
            "verification_job_id": command.staged.verification_job_id,
        }),
        65_536,
    )
}

async fn claim_mcp_discovery_park_receipt(
    transaction: &mut Transaction<'_, Postgres>,
    command: &ParkMcpDiscoveryVerification,
    payload: &TypedPayload,
) -> Result<bool, RepositoryError> {
    let operation = "mcp.discovery.park_verification";
    let inserted = sqlx::query(
        r#"
        INSERT INTO insight_platform.receipts (
            tenant_id, receipt_id, receipt_kind, scope_kind, scope_id,
            dedupe_owner_id, operation, idempotency_key_digest, request_digest,
            state, payload_schema_version, payload, payload_digest, expires_at
        ) VALUES ($1, $2, 'job_commit', 'job', $3, $4, $5, $6, $7,
                  'processing', $8, $9, $10, $11)
        ON CONFLICT (
            tenant_id, receipt_kind, scope_kind, scope_id, dedupe_owner_id,
            operation, idempotency_key_digest
        ) DO NOTHING
        RETURNING receipt_id
        "#,
    )
    .bind(command.audit.tenant_id.to_string())
    .bind(command.audit.receipt_id.to_string())
    .bind(command.job_id.to_string())
    .bind(command.audit.worker_process_generation_id.to_string())
    .bind(operation)
    .bind(command.audit.idempotency_key_digest.to_string())
    .bind(command.audit.request_digest.to_string())
    .bind(payload.schema_version)
    .bind(&payload.value)
    .bind(&payload.digest)
    .bind(command.audit.receipt_expires_at)
    .fetch_optional(&mut **transaction)
    .await?;
    if inserted.is_some() {
        return Ok(false);
    }
    let existing = sqlx::query(
        r#"
        SELECT request_digest, payload_digest, state
        FROM insight_platform.receipts
        WHERE tenant_id = $1 AND receipt_kind = 'job_commit'
          AND scope_kind = 'job' AND scope_id = $2 AND dedupe_owner_id = $3
          AND operation = $4 AND idempotency_key_digest = $5
        FOR UPDATE
        "#,
    )
    .bind(command.audit.tenant_id.to_string())
    .bind(command.job_id.to_string())
    .bind(command.audit.worker_process_generation_id.to_string())
    .bind(operation)
    .bind(command.audit.idempotency_key_digest.to_string())
    .fetch_one(&mut **transaction)
    .await?;
    if existing.try_get::<String, _>("request_digest")? != command.audit.request_digest.to_string()
        || existing.try_get::<String, _>("payload_digest")? != payload.digest
    {
        return Err(RepositoryError::IdempotencyConflict);
    }
    if existing.try_get::<String, _>("state")? != "succeeded" {
        return Err(RepositoryError::Conflict("MCP discovery park receipt"));
    }
    Ok(true)
}

async fn terminalize_mcp_discovery_park_receipt(
    transaction: &mut Transaction<'_, Postgres>,
    command: &ParkMcpDiscoveryVerification,
) -> Result<(), RepositoryError> {
    let affected = sqlx::query(
        r#"
        UPDATE insight_platform.receipts
        SET state = 'succeeded', disposition = 'verification_pending',
            response_reference_id = $4, completed_at = clock_timestamp()
        WHERE tenant_id = $1 AND receipt_id = $2 AND request_digest = $3
          AND scope_id = $5 AND state = 'processing'
        "#,
    )
    .bind(command.audit.tenant_id.to_string())
    .bind(command.audit.receipt_id.to_string())
    .bind(command.audit.request_digest.to_string())
    .bind(command.staged.artifact_id.to_string())
    .bind(command.job_id.to_string())
    .execute(&mut **transaction)
    .await?
    .rows_affected();
    if affected != 1 {
        return Err(RepositoryError::Conflict("MCP discovery park receipt"));
    }
    Ok(())
}

fn mcp_discovery_finalize_receipt_payload(
    command: &FinalizeMcpDiscovery,
) -> Result<TypedPayload, RepositoryError> {
    TypedPayload::with_limit(
        1,
        &serde_json::json!({
            "fence": {
                "expected_version": command.fence.expected_version,
                "lease_generation": command.fence.lease_generation,
                "lease_token_digest": command.fence.token_digest,
                "worker_process_generation_id": command.fence.worker_process_generation_id,
            },
            "job_id": command.job_id,
            "operation_id": command.operation_id,
            "operation_version": command.expected_operation_version,
        }),
        65_536,
    )
}

async fn claim_mcp_discovery_finalize_receipt(
    transaction: &mut Transaction<'_, Postgres>,
    command: &FinalizeMcpDiscovery,
    payload: &TypedPayload,
) -> Result<bool, RepositoryError> {
    let operation = "mcp.discovery.finalize_verified";
    let inserted = sqlx::query(
        r#"
        INSERT INTO insight_platform.receipts (
            tenant_id, receipt_id, receipt_kind, scope_kind, scope_id,
            dedupe_owner_id, operation, idempotency_key_digest, request_digest,
            state, payload_schema_version, payload, payload_digest, expires_at
        ) VALUES ($1, $2, 'job_commit', 'job', $3, $4, $5, $6, $7,
                  'processing', $8, $9, $10, $11)
        ON CONFLICT (
            tenant_id, receipt_kind, scope_kind, scope_id, dedupe_owner_id,
            operation, idempotency_key_digest
        ) DO NOTHING
        RETURNING receipt_id
        "#,
    )
    .bind(command.audit.tenant_id.to_string())
    .bind(command.audit.receipt_id.to_string())
    .bind(command.job_id.to_string())
    .bind(command.audit.worker_process_generation_id.to_string())
    .bind(operation)
    .bind(command.audit.idempotency_key_digest.to_string())
    .bind(command.audit.request_digest.to_string())
    .bind(payload.schema_version)
    .bind(&payload.value)
    .bind(&payload.digest)
    .bind(command.audit.receipt_expires_at)
    .fetch_optional(&mut **transaction)
    .await?;
    if inserted.is_some() {
        return Ok(false);
    }
    let existing = sqlx::query(
        r#"
        SELECT request_digest, payload_digest, state
        FROM insight_platform.receipts
        WHERE tenant_id = $1 AND receipt_kind = 'job_commit'
          AND scope_kind = 'job' AND scope_id = $2 AND dedupe_owner_id = $3
          AND operation = $4 AND idempotency_key_digest = $5
        FOR UPDATE
        "#,
    )
    .bind(command.audit.tenant_id.to_string())
    .bind(command.job_id.to_string())
    .bind(command.audit.worker_process_generation_id.to_string())
    .bind(operation)
    .bind(command.audit.idempotency_key_digest.to_string())
    .fetch_one(&mut **transaction)
    .await?;
    if existing.try_get::<String, _>("request_digest")? != command.audit.request_digest.to_string()
        || existing.try_get::<String, _>("payload_digest")? != payload.digest
    {
        return Err(RepositoryError::IdempotencyConflict);
    }
    if existing.try_get::<String, _>("state")? != "succeeded" {
        return Err(RepositoryError::Conflict("MCP discovery finalize receipt"));
    }
    Ok(true)
}

async fn terminalize_mcp_discovery_finalize_receipt(
    transaction: &mut Transaction<'_, Postgres>,
    command: &FinalizeMcpDiscovery,
    disposition: &str,
    response_reference_id: &ResourceId,
) -> Result<(), RepositoryError> {
    let affected = sqlx::query(
        r#"
        UPDATE insight_platform.receipts
        SET state = 'succeeded', disposition = $4,
            response_reference_id = $5, completed_at = clock_timestamp()
        WHERE tenant_id = $1 AND receipt_id = $2 AND request_digest = $3
          AND scope_id = $6 AND state = 'processing'
        "#,
    )
    .bind(command.audit.tenant_id.to_string())
    .bind(command.audit.receipt_id.to_string())
    .bind(command.audit.request_digest.to_string())
    .bind(disposition)
    .bind(response_reference_id.to_string())
    .bind(command.job_id.to_string())
    .execute(&mut **transaction)
    .await?
    .rows_affected();
    if affected != 1 {
        return Err(RepositoryError::Conflict("MCP discovery finalize receipt"));
    }
    Ok(())
}

fn map_execution_resolution_error(failure: RepositoryError) -> McpExecutionContractResolutionError {
    match failure {
        RepositoryError::Database(_) => McpExecutionContractResolutionError::AuthorityUnavailable,
        RepositoryError::InvalidInput(_)
        | RepositoryError::NotFound(_)
        | RepositoryError::Conflict(_)
        | RepositoryError::StaleFence
        | RepositoryError::LeaseExpired
        | RepositoryError::QuotaExceeded
        | RepositoryError::PermissionDenied
        | RepositoryError::IdempotencyConflict
        | RepositoryError::CorruptRow(_) => McpExecutionContractResolutionError::NotFoundOrChanged,
    }
}

fn map_context_subscription_resolution_error(
    failure: RepositoryError,
) -> ContextSubscriptionExecutionError {
    match failure {
        RepositoryError::Database(_) => ContextSubscriptionExecutionError::Unavailable,
        RepositoryError::InvalidInput(_) => ContextSubscriptionExecutionError::InvalidAttempt,
        RepositoryError::NotFound(_)
        | RepositoryError::Conflict(_)
        | RepositoryError::StaleFence
        | RepositoryError::LeaseExpired
        | RepositoryError::QuotaExceeded
        | RepositoryError::PermissionDenied
        | RepositoryError::IdempotencyConflict
        | RepositoryError::CorruptRow(_) => ContextSubscriptionExecutionError::Rejected,
    }
}

fn map_mcp_discovery_persistence_error(failure: RepositoryError) -> McpDiscoveryPersistenceError {
    match failure {
        RepositoryError::Database(_) => McpDiscoveryPersistenceError::AuthorityUnavailable,
        RepositoryError::InvalidInput(_) => McpDiscoveryPersistenceError::InvalidCommand,
        RepositoryError::NotFound(_)
        | RepositoryError::Conflict(_)
        | RepositoryError::StaleFence
        | RepositoryError::LeaseExpired
        | RepositoryError::QuotaExceeded
        | RepositoryError::PermissionDenied
        | RepositoryError::IdempotencyConflict
        | RepositoryError::CorruptRow(_) => McpDiscoveryPersistenceError::Conflict,
    }
}

pub(crate) async fn load_mcp_authorization_binding(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    authorization_binding_id: &ResourceId,
    for_update: bool,
) -> Result<McpAuthorizationBindingRecord, RepositoryError> {
    let resource = if for_update {
        load_resource_for_update(transaction, tenant_id, authorization_binding_id).await?
    } else {
        load_resource(transaction, tenant_id, authorization_binding_id).await?
    };
    validate_authorization_resource(&resource)?;
    let record: McpAuthorizationBindingRecord =
        decode_versioned_payload(&resource.payload, "MCP AuthorizationBinding")?;
    if record.tenant_id != *tenant_id
        || record.authorization_binding_id != *authorization_binding_id
        || i64::try_from(record.version).ok() != Some(resource.version)
    {
        return Err(RepositoryError::CorruptRow(
            "MCP AuthorizationBinding aggregate disagrees with its payload".to_owned(),
        ));
    }
    record
        .validate_canonical()
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    Ok(record)
}

fn validate_authorization_resource(resource: &ResourceRecord) -> Result<(), RepositoryError> {
    if resource.resource_kind != MCP_AUTHORIZATION_RESOURCE_KIND
        || resource.lifecycle_state != EntityLifecycle::Active.as_str()
        || resource.gate_state != "enabled"
        || resource.active_version_id.is_some()
        || resource.active_deployment_id.is_some()
    {
        return Err(RepositoryError::Conflict(
            "MCP AuthorizationBinding resource gate",
        ));
    }
    Ok(())
}

pub(crate) async fn validate_mcp_authorization_dependencies(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    deployment_ref: &insight_platform_contracts::ExactDeploymentRef,
    audience_identity_digest: &insight_platform_contracts::Sha256Digest,
    token_secret_binding: &insight_platform_contracts::ExactSecretBindingRef,
) -> Result<(), RepositoryError> {
    let deployment = load_deployment(transaction, tenant_id, &deployment_ref.deployment_id).await?;
    if deployment.bindings.digest != deployment_ref.deployment_digest.to_string() {
        return Err(RepositoryError::Conflict(
            "MCP AuthorizationBinding exact Deployment",
        ));
    }
    let resource_id = deployment
        .resource_id
        .parse::<ResourceId>()
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    let resource = load_resource(transaction, tenant_id, &resource_id).await?;
    if resource.resource_kind != RegistryResourceKind::McpServer.as_str()
        || resource.lifecycle_state != EntityLifecycle::Active.as_str()
        || resource.gate_state != "enabled"
    {
        return Err(RepositoryError::Conflict(
            "MCP AuthorizationBinding Deployment gate",
        ));
    }
    let closure = decode_deployment_closure(&deployment.bindings)?;
    validate_deployment_closure_exists(transaction, tenant_id, &closure).await?;
    let DeploymentClosure::McpServer(closure) = closure else {
        return Err(RepositoryError::CorruptRow(
            "MCP Deployment contains the wrong closure".to_owned(),
        ));
    };
    let server_payload = crate::invocation_repository::load_enabled_exact_published_version(
        transaction,
        tenant_id,
        &closure.server_revision,
        RegistryResourceKind::McpServer,
    )
    .await?;
    let ResourceDocument::McpServer(server) = server_payload.document else {
        return Err(RepositoryError::CorruptRow(
            "MCP Server revision contains the wrong document".to_owned(),
        ));
    };
    let auth_policy = closure
        .auth_policy
        .as_ref()
        .ok_or(RepositoryError::Conflict(
            "MCP AuthorizationBinding Auth Profile closure",
        ))?;
    let auth_payload = crate::invocation_repository::load_enabled_exact_published_version(
        transaction,
        tenant_id,
        auth_policy,
        RegistryResourceKind::Policy,
    )
    .await?;
    let ResourceDocument::Policy(auth_resource) = auth_payload.document else {
        return Err(RepositoryError::CorruptRow(
            "MCP AuthorizationBinding Auth Profile has the wrong document".to_owned(),
        ));
    };
    let auth_profile = auth_resource.mcp_auth.ok_or(RepositoryError::Conflict(
        "MCP AuthorizationBinding Auth Profile document",
    ))?;
    if closure.server_identity_digest != *audience_identity_digest
        || auth_resource.policy_kind != PolicyKind::McpAuth
        || auth_profile
            .canonical_digest()
            .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?
            != auth_resource.rules_digest
        || token_secret_binding.provider_id != auth_profile.token_secret_provider_id
        || server.authorization_credential_purpose.as_ref() != Some(&token_secret_binding.purpose)
    {
        return Err(RepositoryError::Conflict(
            "MCP AuthorizationBinding Deployment closure",
        ));
    }
    validate_exact_secret_bindings_at_creation(
        transaction,
        tenant_id,
        std::slice::from_ref(token_secret_binding),
    )
    .await?;
    Ok(())
}

async fn load_mcp_oauth_start_dependencies(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    deployment_ref: &ExactDeploymentRef,
    callback_binding_digest: &Sha256Digest,
    requested_scopes: &[String],
) -> Result<ResolvedMcpOAuthAuthorizationStart, RepositoryError> {
    let deployment = load_deployment(transaction, tenant_id, &deployment_ref.deployment_id).await?;
    if deployment.bindings.digest != deployment_ref.deployment_digest.to_string() {
        return Err(RepositoryError::Conflict("MCP OAuth exact Deployment"));
    }
    let resource_id = deployment
        .resource_id
        .parse::<ResourceId>()
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    let resource = load_resource(transaction, tenant_id, &resource_id).await?;
    if resource.resource_kind != RegistryResourceKind::McpServer.as_str()
        || resource.lifecycle_state != EntityLifecycle::Active.as_str()
        || resource.gate_state != "enabled"
    {
        return Err(RepositoryError::Conflict("MCP OAuth Deployment gate"));
    }
    let closure = decode_deployment_closure(&deployment.bindings)?;
    validate_deployment_closure_exists(transaction, tenant_id, &closure).await?;
    let DeploymentClosure::McpServer(closure) = closure else {
        return Err(RepositoryError::CorruptRow(
            "MCP OAuth Deployment contains the wrong closure".to_owned(),
        ));
    };
    let server_payload = crate::invocation_repository::load_enabled_exact_published_version(
        transaction,
        tenant_id,
        &closure.server_revision,
        RegistryResourceKind::McpServer,
    )
    .await?;
    let ResourceDocument::McpServer(server) = server_payload.document else {
        return Err(RepositoryError::CorruptRow(
            "MCP OAuth Server revision contains the wrong document".to_owned(),
        ));
    };
    let token_purpose = server
        .authorization_credential_purpose
        .ok_or(RepositoryError::Conflict("MCP OAuth credential purpose"))?;
    let auth_policy = closure
        .auth_policy
        .clone()
        .ok_or(RepositoryError::Conflict("MCP OAuth policy closure"))?;
    let auth_payload = crate::invocation_repository::load_enabled_exact_published_version(
        transaction,
        tenant_id,
        &auth_policy,
        RegistryResourceKind::Policy,
    )
    .await?;
    let ResourceDocument::Policy(auth_resource) = auth_payload.document else {
        return Err(RepositoryError::CorruptRow(
            "MCP OAuth Auth Profile revision contains the wrong document".to_owned(),
        ));
    };
    let auth_profile = auth_resource
        .mcp_auth
        .ok_or(RepositoryError::Conflict("MCP OAuth Auth Profile document"))?;
    if auth_resource.policy_kind != PolicyKind::McpAuth
        || auth_profile
            .canonical_digest()
            .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?
            != auth_resource.rules_digest
        || auth_profile.resource_indicator.endpoint_identity_digest
            != closure.server_identity_digest
        || auth_profile.redirect_uri.endpoint_identity_digest != *callback_binding_digest
        || !auth_profile.permits_scopes(requested_scopes)
        || auth_profile
            .client_credential_purpose
            .as_ref()
            .is_some_and(|purpose| {
                !server.deployment_credential_requirements.contains(purpose)
                    || !closure
                        .secret_bindings
                        .iter()
                        .any(|binding| &binding.purpose == purpose)
            })
        || token_purpose.as_str() == MCP_OAUTH_PKCE_SECRET_PURPOSE
        || auth_profile.client_credential_purpose.as_ref() == Some(&token_purpose)
    {
        return Err(RepositoryError::Conflict("MCP OAuth policy closure"));
    }
    Ok(ResolvedMcpOAuthAuthorizationStart {
        tenant_id: tenant_id.clone(),
        mcp_deployment: deployment_ref.clone(),
        audience_identity_digest: closure.server_identity_digest,
        token_credential_purpose: token_purpose,
        auth_policy,
        auth_profile: *auth_profile,
    })
}

fn require_mcp_oauth_task_matches_begin(
    task: &TaskRecord,
    command: &BeginMcpOAuthAuthorization,
) -> Result<(), RepositoryError> {
    let projection = task_projection(task)?;
    let TaskDefinition::McpOAuthAuthorization {
        binding,
        safe_prompt_key,
    } = &projection.payload.definition
    else {
        return Err(RepositoryError::Conflict("MCP OAuth Task definition"));
    };
    let (expected_generation, expected_version) =
        command.reauthorization.map_or((None, None), |fence| {
            (
                Some(fence.authorization_generation),
                Some(fence.authorization_version),
            )
        });
    if projection.tenant_id != command.audit.tenant_id
        || projection.task_id != command.task_id
        || task.owner_kind != MCP_AUTHORIZATION_RESOURCE_KIND
        || task.owner_id != command.authorization_binding_id.to_string()
        || binding.authorization_binding_id != command.authorization_binding_id
        || binding.mcp_deployment != command.mcp_deployment
        || binding.principal_binding_generation != command.expected_principal_binding_generation
        || binding.requested_scopes != command.requested_scopes
        || binding.state_digest != command.state_digest
        || binding.nonce_digest != command.nonce_digest
        || binding.callback_binding_digest != command.callback_binding_digest
        || binding.pkce_secret_binding.as_ref() != &command.pkce_secret_binding
        || binding.expected_authorization_generation != expected_generation
        || binding.expected_authorization_version != expected_version
        || safe_prompt_key != &command.safe_prompt_key
        || projection.payload.created_by.principal_id != command.audit.principal_id
        || projection.payload.created_by.principal_kind != command.audit.principal_kind
        || projection.deadline != command.deadline
    {
        return Err(RepositoryError::IdempotencyConflict);
    }
    Ok(())
}

async fn reject_duplicate_pending_oauth_task(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    deployment_id: &ResourceId,
    principal_id: &ResourceId,
) -> Result<(), RepositoryError> {
    let exists: bool = sqlx::query_scalar(
        r#"
        SELECT EXISTS (
            SELECT 1 FROM insight_platform.tasks
            WHERE tenant_id = $1 AND task_kind = 'external_authorization'
              AND state = 'pending' AND responded_at IS NULL
              AND payload->'definition'->>'kind' = 'mcp_oauth_authorization'
              AND payload->'definition'->'binding'->'mcp_deployment'->>'deployment_id' = $2
              AND payload->'created_by'->>'principal_id' = $3
        )
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(deployment_id.to_string())
    .bind(principal_id.to_string())
    .fetch_one(&mut **transaction)
    .await?;
    if exists {
        return Err(RepositoryError::Conflict("pending MCP OAuth Task"));
    }
    Ok(())
}

async fn insert_mcp_oauth_task(
    transaction: &mut Transaction<'_, Postgres>,
    command: &BeginMcpOAuthAuthorization,
    task_payload: &TaskPayload,
    database_now: DateTime<Utc>,
) -> Result<(), RepositoryError> {
    let payload = TypedPayload::with_limit(1, task_payload, 262_144)?;
    sqlx::query(
        r#"
        INSERT INTO insight_platform.tasks (
            tenant_id, task_id, task_kind, owner_kind, owner_id, run_id, node_id,
            invocation_id, state, generation, version, response_schema_digest,
            principal_snapshot_schema_version, payload_schema_version, payload,
            payload_digest, response_value_id, deadline, responded_at, created_at, updated_at,
            trace_id
        ) VALUES ($1, $2, 'external_authorization', 'mcp_authorization_binding', $3,
                  NULL, NULL, NULL, 'pending', 1, 1, NULL, 1, $4, $5, $6,
                  NULL, $7, NULL, $8, $8, $9)
        "#,
    )
    .bind(command.audit.tenant_id.to_string())
    .bind(command.task_id.to_string())
    .bind(command.authorization_binding_id.to_string())
    .bind(payload.schema_version)
    .bind(&payload.value)
    .bind(&payload.digest)
    .bind(command.deadline)
    .bind(database_now)
    .bind(command.audit.trace.trace_id.to_string())
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

async fn insert_mcp_authorization_record(
    transaction: &mut Transaction<'_, Postgres>,
    record: &McpAuthorizationBindingRecord,
    database_now: DateTime<Utc>,
) -> Result<(), RepositoryError> {
    let payload = TypedPayload::from_versioned(1, record, 262_144)?;
    sqlx::query(
        r#"
        INSERT INTO insight_platform.resources (
            tenant_id, resource_id, resource_kind, lifecycle_state, gate_state,
            version, payload_schema_version, payload, payload_digest, created_at, updated_at
        ) VALUES ($1, $2, 'mcp_authorization_binding', 'active', 'enabled',
                  $3, $4, $5, $6, $7, $7)
        "#,
    )
    .bind(record.tenant_id.to_string())
    .bind(record.authorization_binding_id.to_string())
    .bind(as_i64(record.version, "MCP AuthorizationBinding version")?)
    .bind(payload.schema_version)
    .bind(&payload.value)
    .bind(&payload.digest)
    .bind(database_now)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

async fn update_mcp_oauth_task(
    transaction: &mut Transaction<'_, Postgres>,
    current: &TaskRecord,
    next: &insight_platform_tasks::TaskProjection,
    database_now: DateTime<Utc>,
) -> Result<(), RepositoryError> {
    let payload = TypedPayload::with_limit(1, &next.payload, 262_144)?;
    let affected = sqlx::query(
        r#"
        UPDATE insight_platform.tasks
        SET state = $4, version = $5, payload_schema_version = $6,
            payload = $7, payload_digest = $8, responded_at = $9, updated_at = $9
        WHERE tenant_id = $1 AND task_id = $2 AND version = $3
          AND generation = $10 AND task_kind = 'external_authorization'
          AND state = 'pending' AND responded_at IS NULL
        "#,
    )
    .bind(&current.tenant_id)
    .bind(&current.task_id)
    .bind(current.version)
    .bind(next.state.as_str())
    .bind(as_i64(next.version, "MCP OAuth Task version")?)
    .bind(payload.schema_version)
    .bind(&payload.value)
    .bind(&payload.digest)
    .bind(database_now)
    .bind(as_i64(next.generation, "MCP OAuth Task generation")?)
    .execute(&mut **transaction)
    .await?
    .rows_affected();
    if affected != 1 {
        return Err(RepositoryError::Conflict("MCP OAuth Task first-winner"));
    }
    Ok(())
}

fn mcp_oauth_callback_receipt_payload(
    command: &CompleteMcpOAuthCallback,
) -> Result<TypedPayload, RepositoryError> {
    TypedPayload::with_limit(
        1,
        &serde_json::json!({
            "authorization_binding_id": command.authorization_binding_id,
            "callback_binding_digest": command.audit.callback_binding_digest,
            "callback_ingress_generation_id": command.audit.callback_ingress_generation_id,
            "expected_task_generation": command.expected_task_generation,
            "expected_task_version": command.expected_task_version,
            "resolution": command.resolution,
            "state_digest": command.state_digest,
            "task_id": command.task_id,
        }),
        65_536,
    )
}

async fn claim_mcp_oauth_callback_receipt(
    transaction: &mut Transaction<'_, Postgres>,
    command: &CompleteMcpOAuthCallback,
    payload: &TypedPayload,
) -> Result<bool, RepositoryError> {
    let inserted = sqlx::query(
        r#"
        INSERT INTO insight_platform.receipts (
            tenant_id, receipt_id, receipt_kind, scope_kind, scope_id,
            dedupe_owner_id, operation, idempotency_key_digest, request_digest, state,
            payload_schema_version, payload, payload_digest, expires_at
        ) VALUES ($1, $2, 'callback', 'mcp_oauth_task', $3, $3,
                  'mcp.oauth.callback', $4, $5, 'processing', $6, $7, $8, $9)
        ON CONFLICT (
            tenant_id, receipt_kind, scope_kind, scope_id, dedupe_owner_id,
            operation, idempotency_key_digest
        ) DO NOTHING
        RETURNING receipt_id
        "#,
    )
    .bind(command.audit.tenant_id.to_string())
    .bind(command.audit.receipt_id.to_string())
    .bind(command.task_id.to_string())
    .bind(command.audit.idempotency_key_digest.to_string())
    .bind(command.audit.request_digest.to_string())
    .bind(payload.schema_version)
    .bind(&payload.value)
    .bind(&payload.digest)
    .bind(command.audit.receipt_expires_at)
    .fetch_optional(&mut **transaction)
    .await?;
    if inserted.is_some() {
        return Ok(false);
    }
    let existing = sqlx::query(
        r#"
        SELECT request_digest, state, payload_digest
        FROM insight_platform.receipts
        WHERE tenant_id = $1 AND receipt_kind = 'callback'
          AND scope_kind = 'mcp_oauth_task' AND scope_id = $2 AND dedupe_owner_id = $2
          AND operation = 'mcp.oauth.callback' AND idempotency_key_digest = $3
        FOR UPDATE
        "#,
    )
    .bind(command.audit.tenant_id.to_string())
    .bind(command.task_id.to_string())
    .bind(command.audit.idempotency_key_digest.to_string())
    .fetch_one(&mut **transaction)
    .await?;
    if existing.try_get::<String, _>("request_digest")? != command.audit.request_digest.to_string()
        || existing.try_get::<String, _>("payload_digest")? != payload.digest
    {
        return Err(RepositoryError::IdempotencyConflict);
    }
    if existing.try_get::<String, _>("state")? != "succeeded" {
        return Err(RepositoryError::Conflict("MCP OAuth callback receipt"));
    }
    Ok(true)
}

async fn terminalize_mcp_oauth_callback_receipt(
    transaction: &mut Transaction<'_, Postgres>,
    command: &CompleteMcpOAuthCallback,
    disposition: &str,
    response_reference_id: &ResourceId,
) -> Result<(), RepositoryError> {
    let affected = sqlx::query(
        r#"
        UPDATE insight_platform.receipts
        SET state = 'succeeded', disposition = $4, response_reference_id = $5,
            completed_at = clock_timestamp()
        WHERE tenant_id = $1 AND receipt_id = $2 AND request_digest = $3
          AND scope_kind = 'mcp_oauth_task' AND scope_id = $6 AND state = 'processing'
        "#,
    )
    .bind(command.audit.tenant_id.to_string())
    .bind(command.audit.receipt_id.to_string())
    .bind(command.audit.request_digest.to_string())
    .bind(disposition)
    .bind(response_reference_id.to_string())
    .bind(command.task_id.to_string())
    .execute(&mut **transaction)
    .await?
    .rows_affected();
    if affected != 1 {
        return Err(RepositoryError::Conflict("MCP OAuth callback receipt"));
    }
    Ok(())
}

async fn load_mcp_oauth_callback_record(
    transaction: &mut Transaction<'_, Postgres>,
    command: &CompleteMcpOAuthCallback,
) -> Result<McpOAuthCallbackRecord, RepositoryError> {
    let task =
        load_task_for_update(transaction, &command.audit.tenant_id, &command.task_id).await?;
    let authorization = if task.state == TaskState::Responded {
        Some(
            load_mcp_authorization_binding(
                transaction,
                &command.audit.tenant_id,
                &command.authorization_binding_id,
                false,
            )
            .await?,
        )
    } else {
        None
    };
    Ok(McpOAuthCallbackRecord {
        task,
        authorization,
    })
}

fn mcp_oauth_resolution_task_state(resolution: &McpOAuthCallbackResolution) -> TaskState {
    match resolution {
        McpOAuthCallbackResolution::Authorized(_) => TaskState::Responded,
        McpOAuthCallbackResolution::Declined { .. } => TaskState::Declined,
    }
}

async fn validate_principal_binding(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    principal_id: &ResourceId,
    principal_kind: PrincipalKind,
    generation: u64,
) -> Result<(), RepositoryError> {
    let matched: bool = sqlx::query_scalar(
        r#"
        SELECT EXISTS (
            SELECT 1
            FROM insight_platform.tenant_principals AS binding
            JOIN insight_platform.principals AS principal
              ON principal.principal_id = binding.principal_id
            WHERE binding.tenant_id = $1 AND binding.principal_id = $2
              AND binding.principal_kind = $3 AND binding.state = 'active'
              AND binding.generation = $4 AND principal.state = $5
        )
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(principal_id.to_string())
    .bind(principal_kind.as_str())
    .bind(i64::try_from(generation).map_err(|_| {
        RepositoryError::InvalidInput("MCP principal generation overflow".to_owned())
    })?)
    .bind(PrincipalIdentityState::Active.as_str())
    .fetch_one(&mut **transaction)
    .await?;
    if !matched {
        return Err(RepositoryError::Conflict(
            "MCP AuthorizationBinding principal generation",
        ));
    }
    Ok(())
}

async fn lock_authorization_identity(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    deployment_id: &ResourceId,
    principal_id: &ResourceId,
) -> Result<(), RepositoryError> {
    let identity = format!("{tenant_id}:{deployment_id}:{principal_id}");
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
        .bind(identity)
        .execute(&mut **transaction)
        .await?;
    Ok(())
}

async fn reject_duplicate_live_authorization(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    deployment_id: &ResourceId,
    principal_id: &ResourceId,
) -> Result<(), RepositoryError> {
    let exists: bool = sqlx::query_scalar(
        r#"
        SELECT EXISTS (
            SELECT 1 FROM insight_platform.resources
            WHERE tenant_id = $1 AND resource_kind = 'mcp_authorization_binding'
              AND payload->'mcp_deployment'->>'deployment_id' = $2
              AND payload->>'principal_id' = $3
              AND payload->>'state' IN ('active', 'reauth_required')
        )
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(deployment_id.to_string())
    .bind(principal_id.to_string())
    .fetch_one(&mut **transaction)
    .await?;
    if exists {
        return Err(RepositoryError::Conflict(
            "live MCP AuthorizationBinding already exists",
        ));
    }
    Ok(())
}

async fn update_mcp_authorization(
    transaction: &mut Transaction<'_, Postgres>,
    current: &McpAuthorizationBindingRecord,
    next: &McpAuthorizationBindingRecord,
    now: DateTime<Utc>,
) -> Result<(), RepositoryError> {
    let payload = TypedPayload::from_versioned(1, next, 262_144)?;
    let affected = sqlx::query(
        r#"
        UPDATE insight_platform.resources
        SET version = $4, payload_schema_version = $5, payload = $6,
            payload_digest = $7, updated_at = $8
        WHERE tenant_id = $1 AND resource_id = $2
          AND resource_kind = 'mcp_authorization_binding' AND version = $3
        "#,
    )
    .bind(current.tenant_id.to_string())
    .bind(current.authorization_binding_id.to_string())
    .bind(as_i64(current.version, "MCP AuthorizationBinding version")?)
    .bind(as_i64(next.version, "MCP AuthorizationBinding version")?)
    .bind(payload.schema_version)
    .bind(&payload.value)
    .bind(&payload.digest)
    .bind(now)
    .execute(&mut **transaction)
    .await?
    .rows_affected();
    if affected != 1 {
        return Err(RepositoryError::Conflict("MCP AuthorizationBinding CAS"));
    }
    Ok(())
}

async fn append_authorization_event(
    transaction: &mut Transaction<'_, Postgres>,
    audit: &insight_platform_contracts::CommandAudit,
    record: &McpAuthorizationBindingRecord,
    event_type: &str,
) -> Result<(), RepositoryError> {
    append_command_event(
        transaction,
        audit,
        MCP_AUTHORIZATION_RESOURCE_KIND,
        &record.authorization_binding_id.to_string(),
        as_i64(record.version, "MCP AuthorizationBinding version")?,
        event_type,
        &TypedPayload::new(
            1,
            &serde_json::json!({
                "authorization_binding_id": record.authorization_binding_id,
                "authorization_generation": record.generation,
                "deployment": record.mcp_deployment,
                "principal_kind": record.principal_kind,
                "scope_digest": record.scope_digest,
                "state": record.state,
            }),
        )?,
    )
    .await
}

async fn database_now(
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<DateTime<Utc>, RepositoryError> {
    Ok(sqlx::query_scalar("SELECT clock_timestamp()")
        .fetch_one(&mut **transaction)
        .await?)
}

fn invalid_authorization(failure: insight_platform_mcp_host::McpHostError) -> RepositoryError {
    RepositoryError::InvalidInput(failure.to_string())
}

fn invalid_mcp_discovery(failure: insight_platform_mcp_host::McpHostError) -> RepositoryError {
    RepositoryError::InvalidInput(failure.to_string())
}

fn as_i64(value: u64, label: &str) -> Result<i64, RepositoryError> {
    i64::try_from(value)
        .map_err(|_| RepositoryError::InvalidInput(format!("{label} exceeds PostgreSQL bigint")))
}

fn parse_resource_id_column(
    row: &sqlx::postgres::PgRow,
    column: &str,
) -> Result<ResourceId, RepositoryError> {
    row.try_get::<String, _>(column)?
        .parse::<ResourceId>()
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))
}

fn parse_positive_u64_column(
    row: &sqlx::postgres::PgRow,
    column: &str,
) -> Result<u64, RepositoryError> {
    let value = u64::try_from(row.try_get::<i64, _>(column)?)
        .map_err(|_| RepositoryError::CorruptRow(format!("negative {column}")))?;
    if value == 0 {
        return Err(RepositoryError::CorruptRow(format!("zero {column}")));
    }
    Ok(value)
}

impl PgRegistryTransaction {
    /// Creates one durable MCP Resource subscription using the shared Invocation/Job authority.
    pub async fn create_mcp_resource_subscription(
        &mut self,
        mut command: insight_platform_mcp_host::CreateMcpResourceSubscription,
    ) -> Result<CommandOutcome<McpSubscriptionRecord>, RepositoryError> {
        command
            .validate_at(Utc::now())
            .map_err(invalid_mcp_subscription)?;
        let mut transaction = self.transaction.begin().await?;
        let database_now = database_now(&mut transaction).await?;
        command
            .validate_at(database_now)
            .map_err(invalid_mcp_subscription)?;
        command.deadline = database_timestamp(command.deadline);
        if claim_command_receipt(
            &mut transaction,
            &command.audit,
            "mcp_operation",
            &command.subscription_id.to_string(),
            "mcp.subscription.create",
        )
        .await?
        {
            let record = load_mcp_subscription(
                &mut transaction,
                &command.audit.tenant_id,
                &command.subscription_id,
                false,
                database_now,
            )
            .await?;
            require_same_mcp_subscription_create(&record, &command)?;
            transaction.commit().await?;
            return Ok(CommandOutcome::Replayed(record));
        }
        let principal =
            require_tenant_permission(&mut transaction, &command.audit, Permission::McpRead)
                .await?;
        let context_principal =
            require_tenant_permission(&mut transaction, &command.audit, Permission::ContextRead)
                .await?;
        if principal != context_principal
            || principal.principal_id != command.execution.principal_id
            || principal.principal_id != command.audit.principal_id
        {
            return Err(RepositoryError::PermissionDenied);
        }
        let contract =
            resolve_mcp_execution_contract(&mut transaction, &command.execution, database_now)
                .await?;
        if contract.authorization.principal_identity_kind != command.audit.principal_kind {
            return Err(RepositoryError::PermissionDenied);
        }
        validate_mcp_subscription_context_binding(
            &mut transaction,
            &command.audit.tenant_id,
            &command.context_deployment,
            &contract,
        )
        .await?;
        lock_mcp_subscription_capacity(
            &mut transaction,
            &command.audit.tenant_id,
            &contract.authorization.authorization_binding_id,
        )
        .await?;
        let maximum = checked_in_hard_limit_profile()
            .model_context_mcp
            .mcp_subscriptions_per_session
            .hard_max;
        let active_count: i64 = sqlx::query_scalar(
            r#"
            SELECT count(*)
            FROM insight_platform.invocations
            WHERE tenant_id = $1 AND invocation_kind = 'mcp_subscription'
              AND terminal_at IS NULL
              AND payload -> 'binding' ->> 'authorization_binding_id' = $2
              AND payload -> 'binding' ->> 'mcp_deployment' IS NOT NULL
            "#,
        )
        .bind(command.audit.tenant_id.to_string())
        .bind(contract.authorization.authorization_binding_id.to_string())
        .fetch_one(&mut *transaction)
        .await?;
        if u64::try_from(active_count).unwrap_or(u64::MAX) >= maximum {
            return Err(RepositoryError::QuotaExceeded);
        }

        let session_key =
            McpSessionBindingKey::build(&contract).map_err(invalid_mcp_subscription)?;
        let session =
            McpSessionRecord::disconnected(session_key).map_err(invalid_mcp_subscription)?;
        let binding = McpResourceSubscriptionBinding::build(
            NewMcpResourceSubscriptionBinding {
                subscription_id: command.subscription_id.clone(),
                job_id: command.job_id.clone(),
                context_deployment: command.context_deployment.clone(),
                resource_uri: command.resource_uri.clone(),
            },
            &contract,
            database_now,
        )
        .map_err(invalid_mcp_subscription)?;
        let payload = McpSubscriptionPayload::pending(binding.clone(), session)
            .map_err(invalid_mcp_subscription)?;
        let job_payload =
            McpSubscriptionJobPayload::build(&binding).map_err(invalid_mcp_subscription)?;
        let operation_typed = TypedPayload::from_versioned(1, &payload, 1_048_576)?;
        let job_typed =
            TypedPayload::with_limit(1, &McpJobPayload::Subscription(job_payload), 1_048_576)?;
        sqlx::query(
            r#"
            INSERT INTO insight_platform.invocations (
                tenant_id, invocation_id, invocation_kind, owner_kind, owner_id,
                logical_key, deployment_id, state, payload_schema_version, payload,
                payload_digest, deadline, created_at, updated_at, trace_id
            ) VALUES ($1, $2, 'mcp_subscription', 'mcp_operation', $2,
                      $3, $4, 'pending', $5, $6, $7, $8, $9, $9, $10)
            "#,
        )
        .bind(command.audit.tenant_id.to_string())
        .bind(command.subscription_id.to_string())
        .bind(&command.logical_key)
        .bind(command.execution.mcp_deployment.deployment_id.to_string())
        .bind(operation_typed.schema_version)
        .bind(&operation_typed.value)
        .bind(&operation_typed.digest)
        .bind(command.deadline)
        .bind(database_now)
        .bind(command.audit.trace.trace_id.to_string())
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            r#"
            INSERT INTO insight_platform.jobs (
                tenant_id, job_id, job_kind, work_class, owner_kind, owner_id, invocation_id,
                state, attempt_limit, scheduled_at, deadline, priority, request_digest,
                payload_schema_version, payload, payload_digest, created_at, updated_at,
                trace_id
            ) VALUES ($1, $2, 'mcp_subscription', 'mcp', 'mcp_operation', $3, $3,
                      'ready', $4, $5, $6, 0, $7, $8, $9, $10, $5, $5, $11)
            "#,
        )
        .bind(command.audit.tenant_id.to_string())
        .bind(command.job_id.to_string())
        .bind(command.subscription_id.to_string())
        .bind(i32::from(command.attempt_limit))
        .bind(database_now)
        .bind(command.deadline)
        .bind(command.audit.request_digest.to_string())
        .bind(job_typed.schema_version)
        .bind(&job_typed.value)
        .bind(&job_typed.digest)
        .bind(command.audit.trace.trace_id.to_string())
        .execute(&mut *transaction)
        .await?;
        append_command_event(
            &mut transaction,
            &command.audit,
            "mcp_operation",
            &command.subscription_id.to_string(),
            1,
            "mcp.subscription_scheduled",
            &TypedPayload::new(
                1,
                &serde_json::json!({
                    "authorization_binding_id": binding.authorization_binding_id,
                    "authorization_generation": binding.authorization_generation,
                    "context_deployment": binding.context_deployment,
                    "job_id": binding.job_id,
                    "mcp_deployment": binding.mcp_deployment,
                    "resource_uri_digest": binding.resource_uri_digest,
                    "state": "pending",
                }),
            )?,
        )
        .await?;
        terminalize_command_receipt(
            &mut transaction,
            &command.audit,
            &command.subscription_id.to_string(),
            "scheduled",
        )
        .await?;
        let record = load_mcp_subscription(
            &mut transaction,
            &command.audit.tenant_id,
            &command.subscription_id,
            false,
            database_now,
        )
        .await?;
        transaction.commit().await?;
        Ok(CommandOutcome::Applied(record))
    }
}

impl PgRepository {
    /// Atomically parks one logical MCP subscription Job and admits its exact-generation
    /// physical Sandbox Job. No executor request is emitted before this transaction commits.
    #[cfg(any())]
    pub async fn accept_managed_mcp_sandbox_session(
        &self,
        command: AcceptManagedMcpSandboxSession,
    ) -> Result<CommandOutcome<AcceptedManagedMcpSandboxSession>, RepositoryError> {
        command
            .validate_at(Utc::now(), self.sandbox_limits())
            .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;
        let mut transaction = self.pool().begin().await?;
        let database_now = database_now(&mut transaction).await?;
        command
            .validate_at(database_now, self.sandbox_limits())
            .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;
        let receipt_payload = managed_mcp_sandbox_session_receipt_payload(&command)?;
        let identity = &command.request.identity;
        if claim_mcp_subscription_worker_receipt(
            &mut transaction,
            &command.audit,
            &identity.logical_job_id,
            "mcp.subscription.managed_sandbox.schedule",
            &receipt_payload,
        )
        .await?
        {
            let current = load_mcp_subscription(
                &mut transaction,
                &identity.tenant_id,
                &identity.subscription_id,
                false,
                database_now,
            )
            .await?;
            let (physical_job, physical_payload, usage_reservation_id) =
                load_managed_mcp_sandbox_session_job(
                    &mut transaction,
                    &identity.tenant_id,
                    &identity.physical_job_id,
                    self.sandbox_limits(),
                    false,
                )
                .await?;
            require_managed_mcp_sandbox_session_replay(
                &current,
                &physical_job,
                &physical_payload,
                &usage_reservation_id,
                &command,
            )?;
            let accepted = AcceptedManagedMcpSandboxSession {
                logical_payload: current.payload,
                logical_state: current.state,
                physical_job,
                physical_payload,
            };
            transaction.commit().await?;
            return Ok(CommandOutcome::Replayed(accepted));
        }

        let current = load_mcp_subscription(
            &mut transaction,
            &identity.tenant_id,
            &identity.subscription_id,
            true,
            database_now,
        )
        .await?;
        let logical_job = load_mcp_subscription_job(
            &mut transaction,
            &identity.tenant_id,
            &identity.logical_job_id,
            true,
        )
        .await?;
        require_mcp_subscription_job_fence(
            &logical_job,
            &current,
            &command.logical_fence,
            database_now,
        )?;
        if current.version != identity.admitted_subscription_version
            || logical_job.version != identity.admitted_logical_job_version
            || current.payload.binding != command.request.subscription_binding
        {
            return Err(RepositoryError::StaleFence);
        }

        let binding = &current.payload.binding;
        let resolved_contract = resolve_mcp_execution_contract(
            &mut transaction,
            &McpExecutionContractQuery {
                schema_version: 1,
                tenant_id: binding.tenant_id.clone(),
                mcp_deployment: binding.mcp_deployment.clone(),
                discovery_snapshot_id: binding.discovery_snapshot_id.clone(),
                discovery_snapshot_digest: binding.discovery_snapshot_digest.clone(),
                authorization_binding_id: binding.authorization_binding_id.clone(),
                authorization_generation: binding.authorization_generation,
                authorization_context_digest: binding.authorization_context_digest.clone(),
                principal_id: binding.principal_id.clone(),
            },
            database_now,
        )
        .await?;
        if resolved_contract != *command.request.mcp_contract {
            return Err(RepositoryError::Conflict(
                "Managed MCP Session execution contract",
            ));
        }
        validate_mcp_subscription_context_binding(
            &mut transaction,
            &identity.tenant_id,
            &binding.context_deployment,
            &resolved_contract,
        )
        .await?;
        verify_managed_mcp_session_sandbox_bindings(&mut transaction, &command.request).await?;
        lock_and_persist_managed_mcp_session_artifact_grants(
            &mut transaction,
            &command.request,
            database_now,
        )
        .await?;
        lock_managed_mcp_session_secret_grants(&mut transaction, &command.request).await?;
        reserve_managed_mcp_session_quota(
            &mut transaction,
            &command.request,
            &command.usage_reservation_id,
            &command.quota_entry_ids,
            database_now,
        )
        .await?;

        let accepted = decide_accept_managed_mcp_sandbox_session(
            &current,
            &command,
            database_now,
            self.sandbox_limits(),
        )
        .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;
        let next = update_mcp_subscription(
            &mut transaction,
            &current,
            accepted.logical_state,
            &accepted.logical_payload,
            database_now,
        )
        .await?;
        let stored_payload =
            SandboxJobPayload::managed_mcp_subscription_session(accepted.physical_payload.clone());
        let physical_typed = TypedPayload::from_versioned(1, &stored_payload, 1_048_576)?;
        sqlx::query(
            r#"
            INSERT INTO insight_platform.jobs (
                tenant_id, job_id, job_kind, work_class, owner_kind, owner_id, invocation_id,
                state, version, attempt_no, attempt_limit, lease_epoch, scheduled_at,
                deadline, priority, request_digest, quota_reservation_id,
                payload_schema_version, payload, payload_digest, created_at, updated_at,
                trace_id
            ) VALUES ($1, $2, 'sandbox_managed_mcp_session', 'sandbox', 'job', $3, $4,
                      'ready', 1, 0, 1, 0, $5, $6, 0, $7, $8,
                      $9, $10, $11, $5, $5, $12)
            "#,
        )
        .bind(identity.tenant_id.to_string())
        .bind(identity.physical_job_id.to_string())
        .bind(identity.sandbox_job_id.to_string())
        .bind(identity.subscription_id.to_string())
        .bind(database_now)
        .bind(command.request.deadline)
        .bind(command.request.request_digest.to_string())
        .bind(command.usage_reservation_id.to_string())
        .bind(physical_typed.schema_version)
        .bind(&physical_typed.value)
        .bind(&physical_typed.digest)
        .bind(command.audit.trace.trace_id.to_string())
        .execute(&mut *transaction)
        .await?;
        park_mcp_subscription_job(&mut transaction, &logical_job, &next, database_now).await?;
        append_scheduler_event(
            &mut transaction,
            &identity.tenant_id.to_string(),
            &command.audit.event_id,
            &command.audit.outbox_id,
            "mcp_operation",
            &identity.subscription_id.to_string(),
            as_i64(next.version, "MCP subscription version")?,
            None,
            "mcp.managed_sandbox_session_scheduled",
            &TypedPayload::new(
                1,
                &serde_json::json!({
                    "logical_job_id": identity.logical_job_id,
                    "physical_job_id": identity.physical_job_id,
                    "request_digest": command.request.request_digest,
                    "sandbox_job_id": identity.sandbox_job_id,
                    "session_generation": identity.session_generation,
                    "subscription_id": identity.subscription_id,
                }),
            )?,
        )
        .await?;
        terminalize_mcp_subscription_worker_receipt(
            &mut transaction,
            &command.audit,
            &identity.logical_job_id,
            "managed_sandbox_session_scheduled",
            &identity.sandbox_job_id,
        )
        .await?;
        transaction.commit().await?;
        Ok(CommandOutcome::Applied(accepted))
    }

    /// Claims only the physical Jobs created by Managed MCP subscription admission. The logical
    /// subscription and its parked MCP Job are locked first, so a worker can never lease an
    /// orphaned or superseded session generation.
    #[cfg(any())]
    pub async fn scan_expired_managed_mcp_sandbox_session_leases(
        &self,
        command: insight_platform_sandbox::ScanExpiredManagedMcpSandboxSessionLeases,
    ) -> Result<ExpiredManagedMcpSandboxSessionLeasePage, RepositoryError> {
        command.validate(self.recovery_batch_limit(), self.recovery_shard_limit())?;
        let mut transaction = self.pool().begin().await?;
        let database_now = database_now(&mut transaction).await?;
        let rows = sqlx::query(
            r#"
            SELECT job.*
            FROM insight_platform.jobs AS job
            WHERE job.work_class = 'sandbox'
              AND job.job_kind = 'sandbox_managed_mcp_session'
              AND job.owner_kind = 'job'
              AND job.state IN ('leased', 'running')
              AND job.lease_expires_at <= $1 AND job.terminal_at IS NULL
              AND job.worker_id IS NOT NULL AND job.lease_epoch > 0
              AND job.payload ->> 'workload_kind' = 'managed_mcp_subscription_session'
              AND job.payload #>> '{workload,request,executor_worker_manifest_digest}' = $5
              AND job.payload #>> '{workload,request,isolation_backend_contract_digest}' = $6
              AND (
                    job.payload #>> '{workload,physical_state}' = 'accepted'
                    OR (
                        job.payload #>> '{workload,physical_state}'
                            IN ('preparing', 'starting', 'running')
                        AND job.payload #>> '{workload,attestor_route}' = $7
                    )
              )
              AND mod(('x' || right(job.job_id, 8))::bit(32)::bigint, $4) = $3
              AND (
                    $8::timestamptz IS NULL
                    OR (job.lease_expires_at, job.tenant_id, job.job_id) >
                       ($8::timestamptz, $9::text, $10::text)
              )
            ORDER BY job.lease_expires_at, job.tenant_id, job.job_id
            LIMIT $2
            "#,
        )
        .bind(database_now)
        .bind(i64::from(command.limit))
        .bind(i64::from(command.shard.index))
        .bind(i64::from(command.shard.count))
        .bind(command.executor.worker_manifest_digest.to_string())
        .bind(
            command
                .executor
                .isolation_backend_contract_digest
                .to_string(),
        )
        .bind(command.executor.attestor_route.as_str())
        .bind(command.after.as_ref().map(|cursor| cursor.lease_expires_at))
        .bind(
            command
                .after
                .as_ref()
                .map(|cursor| cursor.tenant_id.to_string()),
        )
        .bind(
            command
                .after
                .as_ref()
                .map(|cursor| cursor.physical_job_id.to_string()),
        )
        .fetch_all(&mut *transaction)
        .await?;
        let scanned_count = rows.len();
        let next_cursor = rows
            .last()
            .map(
                |row| -> Result<ManagedMcpSandboxSessionRecoveryCursor, RepositoryError> {
                    Ok(ManagedMcpSandboxSessionRecoveryCursor {
                        lease_expires_at: row.try_get("lease_expires_at")?,
                        tenant_id: parse_resource_id_column(row, "tenant_id")?,
                        physical_job_id: parse_resource_id_column(row, "job_id")?,
                    })
                },
            )
            .transpose()?;
        let mut records = Vec::with_capacity(rows.len());
        for row in rows {
            let record = job_from_row(row)?;
            let job = job_projection(&record)?;
            let stored: SandboxJobPayload =
                decode_versioned_payload(&record.payload, "Sandbox Job")?;
            let insight_platform_sandbox::SandboxJobWorkload::ManagedMcpSubscriptionSession(
                payload,
            ) = stored.workload
            else {
                return Err(RepositoryError::CorruptRow(
                    "Managed MCP recovery scan decoded a different workload".to_owned(),
                ));
            };
            payload
                .validate_for(&job, self.sandbox_limits())
                .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
            let expected_shared_state = match payload.physical_state {
                SandboxJobState::Accepted => JobState::Leased,
                SandboxJobState::Preparing
                | SandboxJobState::Starting
                | SandboxJobState::Running => JobState::Running,
                _ => {
                    return Err(RepositoryError::CorruptRow(
                        "terminal Managed MCP payload retained an active lease".to_owned(),
                    ));
                }
            };
            let lease = job.lease.as_ref().ok_or_else(|| {
                RepositoryError::CorruptRow(
                    "expired Managed MCP physical Job has no lease".to_owned(),
                )
            })?;
            let usage_reservation_id = record
                .quota_reservation_id
                .as_deref()
                .ok_or_else(|| {
                    RepositoryError::CorruptRow(
                        "Managed MCP physical Job has no quota reservation".to_owned(),
                    )
                })?
                .parse::<ResourceId>()
                .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
            if job.state != expected_shared_state
                || payload.request.executor_worker_manifest_digest
                    != command.executor.worker_manifest_digest
                || payload.request.isolation_backend_contract_digest
                    != command.executor.isolation_backend_contract_digest
                || (payload.physical_state != SandboxJobState::Accepted
                    && payload.attestor_route.as_ref() != Some(&command.executor.attestor_route))
            {
                return Err(RepositoryError::CorruptRow(
                    "Managed MCP recovery candidate binding drifted".to_owned(),
                ));
            }
            let expired = ExpiredManagedMcpSandboxSessionLease {
                request: payload.request.as_ref().clone(),
                usage_reservation_id,
                observed_job_version: job.version,
                observed_lease_generation: job.lease_generation,
                previous_worker_process_generation_id: lease.worker_process_generation_id.clone(),
                lease_expires_at: lease.expires_at,
                database_observed_at: database_now,
                physical_state: payload.physical_state,
                executor_identity_digest: payload.executor_identity_digest.clone(),
                attestor_route: payload.attestor_route.clone(),
                phase_evidence_digest: payload.phase_evidence_digest.clone(),
                prepared_binding: payload.prepared_binding.clone(),
                ready_binding: payload.ready_binding.clone(),
            };
            expired
                .validate(self.sandbox_limits())
                .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
            records.push(expired);
        }
        transaction.commit().await?;
        let exhausted = scanned_count < usize::from(command.limit);
        Ok(ExpiredManagedMcpSandboxSessionLeasePage {
            records,
            next_cursor: (!exhausted).then_some(next_cursor).flatten(),
            exhausted,
        })
    }

    #[cfg(any())]
    pub async fn claim_managed_mcp_sandbox_sessions(
        &self,
        command: ClaimSandboxJobs,
    ) -> Result<Vec<ClaimedManagedMcpSandboxSession>, RepositoryError> {
        command.validate(
            self.recovery_batch_limit(),
            u64::try_from(MAX_JOB_LEASE_MILLISECONDS).expect("positive Sandbox lease hard maximum"),
        )?;
        let mut transaction = self.pool().begin().await?;
        let database_now = database_now(&mut transaction).await?;
        let candidates = sqlx::query(
            r#"
            SELECT job.tenant_id, job.job_id, job.invocation_id
            FROM insight_platform.jobs AS job
            JOIN insight_platform.invocations AS subscription
              ON subscription.tenant_id = job.tenant_id
             AND subscription.invocation_id = job.invocation_id
             AND subscription.invocation_kind = 'mcp_subscription'
             AND subscription.state = 'pending'
            WHERE job.work_class = 'sandbox'
              AND job.job_kind = 'sandbox_managed_mcp_session'
              AND job.owner_kind = 'job'
              AND job.state = 'ready' AND job.terminal_at IS NULL
              AND job.worker_id IS NULL AND job.scheduled_at <= $1
              AND job.deadline > $1
              AND job.payload ->> 'workload_kind' = 'managed_mcp_subscription_session'
              AND job.payload #>> '{workload,request,executor_worker_manifest_digest}' = $3
              AND job.payload #>> '{workload,request,isolation_backend_contract_digest}' = $4
            ORDER BY job.priority DESC, job.scheduled_at, job.job_id
            LIMIT $2
            "#,
        )
        .bind(database_now)
        .bind(i64::from(command.limit))
        .bind(command.worker_manifest_digest.to_string())
        .bind(command.isolation_backend_contract_digest.to_string())
        .fetch_all(&mut *transaction)
        .await?;
        let mut claimed = Vec::with_capacity(candidates.len());
        for (candidate, lease_token_digest) in
            candidates.into_iter().zip(&command.lease_token_digests)
        {
            let tenant_id = candidate
                .try_get::<String, _>("tenant_id")?
                .parse::<ResourceId>()
                .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
            let physical_job_id = candidate
                .try_get::<String, _>("job_id")?
                .parse::<ResourceId>()
                .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
            let subscription_id = candidate
                .try_get::<Option<String>, _>("invocation_id")?
                .ok_or_else(|| {
                    RepositoryError::CorruptRow(
                        "Managed MCP Sandbox claim candidate has no subscription".to_owned(),
                    )
                })?
                .parse::<ResourceId>()
                .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;

            let subscription = load_mcp_subscription(
                &mut transaction,
                &tenant_id,
                &subscription_id,
                true,
                database_now,
            )
            .await?;
            if subscription.state != McpSubscriptionState::Pending
                || subscription.payload.session.state
                    != insight_platform_contracts::McpSessionState::Connecting
            {
                continue;
            }
            let logical_job =
                load_mcp_subscription_job(&mut transaction, &tenant_id, &subscription.job_id, true)
                    .await?;
            let (physical_job, physical_payload, usage_reservation_id) =
                load_managed_mcp_sandbox_session_job(
                    &mut transaction,
                    &tenant_id,
                    &physical_job_id,
                    self.sandbox_limits(),
                    true,
                )
                .await?;
            let identity = &physical_payload.request.identity;
            let link = subscription
                .payload
                .managed_sandbox_session
                .as_ref()
                .ok_or_else(|| {
                    RepositoryError::CorruptRow(
                        "Managed MCP subscription has no physical session link".to_owned(),
                    )
                })?;
            if physical_job.state != JobState::Ready
                || physical_payload.physical_state != SandboxJobState::Accepted
                || identity.tenant_id != tenant_id
                || identity.subscription_id != subscription_id
                || identity.physical_job_id != physical_job_id
                || identity.logical_job_id != subscription.job_id
                || identity.admitted_subscription_version.checked_add(1)
                    != Some(subscription.version)
                || identity.admitted_logical_job_version.checked_add(1) != Some(logical_job.version)
                || logical_job.state != JobState::Waiting
                || logical_job.worker_id.is_some()
                || logical_job.lease_token_digest.is_some()
                || logical_job.lease_expires_at.is_some()
                || link.identity != *identity
                || link.sandbox_request_digest != physical_payload.request.request_digest
                || physical_payload.request.executor_worker_manifest_digest
                    != command.worker_manifest_digest
                || physical_payload.request.isolation_backend_contract_digest
                    != command.isolation_backend_contract_digest
            {
                continue;
            }
            verify_managed_mcp_session_sandbox_bindings(
                &mut transaction,
                &physical_payload.request,
            )
            .await?;
            let next = decide_claim(
                &physical_job,
                database_now,
                command.worker_process_generation_id.clone(),
                lease_token_digest.clone(),
                LeasePolicy {
                    requested_milliseconds: command.lease_milliseconds,
                    hard_maximum_milliseconds: u64::try_from(MAX_JOB_LEASE_MILLISECONDS)
                        .expect("positive Sandbox lease hard maximum"),
                },
            )?;
            persist_managed_mcp_sandbox_claim(&mut transaction, &physical_job, &next, database_now)
                .await?;
            let lease = next.lease.as_ref().ok_or_else(|| {
                RepositoryError::CorruptRow(
                    "Managed MCP Sandbox claim produced no lease".to_owned(),
                )
            })?;
            let result = ClaimedManagedMcpSandboxSession {
                request: physical_payload.request.as_ref().clone(),
                fence: insight_platform_jobs::JobFence {
                    expected_version: next.version,
                    worker_process_generation_id: lease.worker_process_generation_id.clone(),
                    lease_generation: next.lease_generation,
                    token_digest: lease.token_digest.clone(),
                },
                usage_reservation_id,
            };
            result
                .validate_at(database_now, self.sandbox_limits())
                .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
            claimed.push(result);
        }
        transaction.commit().await?;
        Ok(claimed)
    }

    #[cfg(any())]
    pub async fn commit_managed_mcp_sandbox_session_phase(
        &self,
        command: CommitManagedMcpSandboxSessionPhase,
    ) -> Result<CommandOutcome<ManagedMcpSandboxSessionPhaseDecision>, RepositoryError> {
        let mut transaction = self.pool().begin().await?;
        let database_now = database_now(&mut transaction).await?;
        command.validate_at(database_now)?;
        let operation = format!(
            "mcp.managed_sandbox_session.phase.{}",
            command.target.as_str()
        );
        if claim_sandbox_worker_receipt(
            &mut transaction,
            &command.audit,
            &command.identity.physical_job_id,
            &operation,
        )
        .await?
        {
            let decision = load_managed_mcp_sandbox_session_decision(
                &mut transaction,
                &command.identity,
                self.sandbox_limits(),
                false,
                database_now,
            )
            .await?;
            require_managed_mcp_sandbox_session_phase_replay(&decision, &command)?;
            transaction.commit().await?;
            return Ok(CommandOutcome::Replayed(decision));
        }

        let current = load_mcp_subscription(
            &mut transaction,
            &command.identity.tenant_id,
            &command.identity.subscription_id,
            true,
            database_now,
        )
        .await?;
        let (physical_job, physical_payload, _) = load_managed_mcp_sandbox_session_job(
            &mut transaction,
            &command.identity.tenant_id,
            &command.identity.physical_job_id,
            self.sandbox_limits(),
            true,
        )
        .await?;
        let decision = decide_managed_mcp_sandbox_session_phase(
            &current,
            &physical_job,
            &physical_payload,
            &command,
            database_now,
            self.sandbox_limits(),
        )?;
        let logical = if decision.logical_state != current.state
            || decision.logical_payload != current.payload
        {
            update_mcp_subscription(
                &mut transaction,
                &current,
                decision.logical_state,
                &decision.logical_payload,
                database_now,
            )
            .await?
        } else {
            current
        };
        update_managed_mcp_sandbox_session_job(
            &mut transaction,
            &physical_job,
            &decision,
            database_now,
        )
        .await?;
        append_managed_mcp_sandbox_session_event(
            &mut transaction,
            &command.audit,
            &logical,
            &decision,
            "mcp.managed_sandbox_session_phase_changed",
        )
        .await?;
        terminalize_sandbox_worker_receipt(
            &mut transaction,
            &command.audit,
            &command.identity.physical_job_id,
            command.target.as_str(),
        )
        .await?;
        transaction.commit().await?;
        Ok(CommandOutcome::Applied(decision))
    }

    /// Commits the credential-free prepared session handle together with the logical Ready state
    /// and physical Running state. The caller may activate notification delivery only after this
    /// transaction returns successfully.
    #[cfg(any())]
    pub async fn commit_managed_mcp_sandbox_session_ready(
        &self,
        command: CommitManagedMcpSandboxSessionReady,
    ) -> Result<CommandOutcome<ManagedMcpSandboxSessionPhaseDecision>, RepositoryError> {
        let mut transaction = self.pool().begin().await?;
        let database_now = database_now(&mut transaction).await?;
        let operation = "mcp.managed_sandbox_session.ready";
        if claim_sandbox_worker_receipt(
            &mut transaction,
            &command.audit,
            &command.identity.physical_job_id,
            operation,
        )
        .await?
        {
            let decision = load_managed_mcp_sandbox_session_decision(
                &mut transaction,
                &command.identity,
                self.sandbox_limits(),
                false,
                database_now,
            )
            .await?;
            command.validate_at(&decision.physical_payload.request, database_now)?;
            require_managed_mcp_sandbox_session_ready_replay(&decision, &command)?;
            transaction.commit().await?;
            return Ok(CommandOutcome::Replayed(decision));
        }
        let current = load_mcp_subscription(
            &mut transaction,
            &command.identity.tenant_id,
            &command.identity.subscription_id,
            true,
            database_now,
        )
        .await?;
        let (physical_job, physical_payload, _) = load_managed_mcp_sandbox_session_job(
            &mut transaction,
            &command.identity.tenant_id,
            &command.identity.physical_job_id,
            self.sandbox_limits(),
            true,
        )
        .await?;
        command.validate_at(&physical_payload.request, database_now)?;
        let decision = decide_managed_mcp_sandbox_session_ready(
            &current,
            &physical_job,
            &physical_payload,
            &command,
            database_now,
            self.sandbox_limits(),
        )?;
        let logical = update_mcp_subscription(
            &mut transaction,
            &current,
            decision.logical_state,
            &decision.logical_payload,
            database_now,
        )
        .await?;
        update_managed_mcp_sandbox_session_job(
            &mut transaction,
            &physical_job,
            &decision,
            database_now,
        )
        .await?;
        append_managed_mcp_sandbox_session_event(
            &mut transaction,
            &command.audit,
            &logical,
            &decision,
            "mcp.managed_sandbox_session_ready",
        )
        .await?;
        terminalize_sandbox_worker_receipt(
            &mut transaction,
            &command.audit,
            &command.identity.physical_job_id,
            "ready",
        )
        .await?;
        transaction.commit().await?;
        Ok(CommandOutcome::Applied(decision))
    }

    /// Renews the exact physical Managed-session Job without creating an audit event. The logical
    /// subscription is locked first so a concurrent terminal/session-loss transition remains the
    /// single first winner across both lifecycles.
    #[cfg(any())]
    pub async fn heartbeat_managed_mcp_sandbox_session(
        &self,
        command: HeartbeatSandboxExecution,
    ) -> Result<ManagedMcpSandboxSessionPhaseDecision, RepositoryError> {
        let maximum_lease_milliseconds =
            u64::try_from(MAX_JOB_LEASE_MILLISECONDS).expect("positive Sandbox lease hard maximum");
        command.validate(maximum_lease_milliseconds)?;
        let mut transaction = self.pool().begin().await?;
        let database_now = database_now(&mut transaction).await?;
        let subscription_id = sqlx::query_scalar::<_, Option<String>>(
            r#"
            SELECT invocation_id
            FROM insight_platform.jobs
            WHERE tenant_id = $1 AND job_id = $2
              AND work_class = 'sandbox'
              AND job_kind = 'sandbox_managed_mcp_session'
              AND owner_kind = 'job'
            "#,
        )
        .bind(command.tenant_id.to_string())
        .bind(command.job_id.to_string())
        .fetch_optional(&mut *transaction)
        .await?
        .flatten()
        .ok_or(RepositoryError::NotFound(
            "Managed MCP physical Sandbox Job subscription",
        ))?
        .parse::<ResourceId>()
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
        let current = load_mcp_subscription(
            &mut transaction,
            &command.tenant_id,
            &subscription_id,
            true,
            database_now,
        )
        .await?;
        let (physical_job, physical_payload, _) = load_managed_mcp_sandbox_session_job(
            &mut transaction,
            &command.tenant_id,
            &command.job_id,
            self.sandbox_limits(),
            true,
        )
        .await?;
        let decision = decide_managed_mcp_sandbox_session_heartbeat(
            &current,
            &physical_job,
            &physical_payload,
            &command,
            database_now,
            self.sandbox_limits(),
            maximum_lease_milliseconds,
        )?;
        update_managed_mcp_sandbox_session_job(
            &mut transaction,
            &physical_job,
            &decision,
            database_now,
        )
        .await?;
        transaction.commit().await?;
        Ok(decision)
    }

    #[cfg(any())]
    pub async fn commit_managed_mcp_sandbox_session_lost(
        &self,
        command: CommitManagedMcpSandboxSessionLost,
    ) -> Result<CommandOutcome<ManagedMcpSandboxSessionPhaseDecision>, RepositoryError> {
        let mut transaction = self.pool().begin().await?;
        let database_now = database_now(&mut transaction).await?;
        command.validate_at(database_now)?;
        let operation = "mcp.managed_sandbox_session.lost";
        if claim_sandbox_worker_receipt(
            &mut transaction,
            &command.audit,
            &command.identity.physical_job_id,
            operation,
        )
        .await?
        {
            let logical = load_mcp_subscription(
                &mut transaction,
                &command.identity.tenant_id,
                &command.identity.subscription_id,
                false,
                database_now,
            )
            .await?;
            let (physical_job, physical_payload, usage_reservation_id) =
                load_managed_mcp_sandbox_session_job(
                    &mut transaction,
                    &command.identity.tenant_id,
                    &command.identity.physical_job_id,
                    self.sandbox_limits(),
                    false,
                )
                .await?;
            let decision = ManagedMcpSandboxSessionPhaseDecision {
                logical_payload: logical.payload,
                logical_state: logical.state,
                physical_job,
                physical_payload,
            };
            require_managed_mcp_sandbox_session_lost_replay(
                &decision,
                &usage_reservation_id,
                &command,
            )?;
            transaction.commit().await?;
            return Ok(CommandOutcome::Replayed(decision));
        }

        let current = load_mcp_subscription(
            &mut transaction,
            &command.identity.tenant_id,
            &command.identity.subscription_id,
            true,
            database_now,
        )
        .await?;
        let logical_job = load_mcp_subscription_job(
            &mut transaction,
            &command.identity.tenant_id,
            &command.identity.logical_job_id,
            true,
        )
        .await?;
        let (physical_job, physical_payload, usage_reservation_id) =
            load_managed_mcp_sandbox_session_job(
                &mut transaction,
                &command.identity.tenant_id,
                &command.identity.physical_job_id,
                self.sandbox_limits(),
                true,
            )
            .await?;
        if usage_reservation_id != command.usage_reservation_id {
            return Err(RepositoryError::Conflict(
                "Managed MCP Sandbox usage reservation",
            ));
        }
        let decision = decide_managed_mcp_sandbox_session_lost(
            &current,
            &physical_job,
            &physical_payload,
            &command,
            database_now,
            self.sandbox_limits(),
        )?;
        settle_managed_mcp_session_quota(
            &mut transaction,
            &command.identity.tenant_id,
            &command.usage_reservation_id,
            &physical_payload.request.resources,
            &command.quota_entry_ids,
            &command.audit.request_digest,
        )
        .await?;
        let expected_grants = u64::try_from(physical_payload.request.artifact_grants.len())
            .map_err(|_| {
                RepositoryError::InvalidInput(
                    "Managed MCP Sandbox Artifact grant count exceeds bigint".to_owned(),
                )
            })?;
        release_and_confirm_sandbox_artifact_grants(
            &mut transaction,
            &command.identity.tenant_id.to_string(),
            &command.identity.sandbox_job_id.to_string(),
            expected_grants,
            database_now,
        )
        .await?;
        let logical = update_mcp_subscription(
            &mut transaction,
            &current,
            decision.logical_state,
            &decision.logical_payload,
            database_now,
        )
        .await?;
        requeue_recovered_mcp_subscription_job(
            &mut transaction,
            &command.identity.tenant_id,
            &logical_job,
            database_now,
        )
        .await?;
        update_terminal_managed_mcp_sandbox_session_job(
            &mut transaction,
            &physical_job,
            &decision,
            database_now,
        )
        .await?;
        append_managed_mcp_sandbox_session_event(
            &mut transaction,
            &command.audit,
            &logical,
            &decision,
            "mcp.managed_sandbox_session_lost",
        )
        .await?;
        terminalize_sandbox_worker_receipt(
            &mut transaction,
            &command.audit,
            &command.identity.physical_job_id,
            "lost",
        )
        .await?;
        transaction.commit().await?;
        Ok(CommandOutcome::Applied(decision))
    }

    #[cfg(any())]
    pub async fn recover_expired_managed_mcp_sandbox_session_lease(
        &self,
        command: RecoverExpiredManagedMcpSandboxSessionLease,
    ) -> Result<CommandOutcome<ManagedMcpSandboxSessionLeaseRecoveryResult>, RepositoryError> {
        let mut transaction = self.pool().begin().await?;
        let database_now = database_now(&mut transaction).await?;
        command.validate_at(database_now, self.sandbox_limits())?;
        if claim_managed_mcp_sandbox_recovery_receipt(&mut transaction, &command).await? {
            let result = managed_mcp_sandbox_recovery_result(&command);
            transaction.commit().await?;
            return Ok(CommandOutcome::Replayed(result));
        }

        let identity = &command.expired.request.identity;
        let current = load_mcp_subscription(
            &mut transaction,
            &identity.tenant_id,
            &identity.subscription_id,
            true,
            database_now,
        )
        .await?;
        let logical_job = load_mcp_subscription_job(
            &mut transaction,
            &identity.tenant_id,
            &identity.logical_job_id,
            true,
        )
        .await?;
        let (physical_job, physical_payload, usage_reservation_id) =
            load_managed_mcp_sandbox_session_job(
                &mut transaction,
                &identity.tenant_id,
                &identity.physical_job_id,
                self.sandbox_limits(),
                true,
            )
            .await?;
        if usage_reservation_id != command.expired.usage_reservation_id {
            return Err(RepositoryError::Conflict(
                "Managed MCP recovery usage reservation",
            ));
        }
        let decision = decide_expired_managed_mcp_sandbox_session_lease(
            &current,
            &physical_job,
            &physical_payload,
            &command,
            database_now,
            self.sandbox_limits(),
        )?;
        let terminal = !matches!(
            &command.action,
            ManagedMcpSandboxSessionLeaseRecoveryAction::RequeueUnstarted { .. }
        );
        if terminal {
            match &command.action {
                ManagedMcpSandboxSessionLeaseRecoveryAction::TimeoutUnstarted { .. } => {
                    settle_unstarted_managed_mcp_session_quota(
                        &mut transaction,
                        &identity.tenant_id,
                        &usage_reservation_id,
                        &physical_payload.request.resources,
                        &command.quota_entry_ids,
                        &command.audit.request_digest,
                    )
                    .await?;
                }
                ManagedMcpSandboxSessionLeaseRecoveryAction::MarkLost { .. } => {
                    settle_managed_mcp_session_quota(
                        &mut transaction,
                        &identity.tenant_id,
                        &usage_reservation_id,
                        &physical_payload.request.resources,
                        &command.quota_entry_ids,
                        &command.audit.request_digest,
                    )
                    .await?;
                }
                ManagedMcpSandboxSessionLeaseRecoveryAction::RequeueUnstarted { .. } => {
                    unreachable!()
                }
            }
            let expected_grants = u64::try_from(physical_payload.request.artifact_grants.len())
                .map_err(|_| {
                    RepositoryError::InvalidInput(
                        "Managed MCP Sandbox Artifact grant count exceeds bigint".to_owned(),
                    )
                })?;
            release_and_confirm_sandbox_artifact_grants(
                &mut transaction,
                &identity.tenant_id.to_string(),
                &identity.sandbox_job_id.to_string(),
                expected_grants,
                database_now,
            )
            .await?;
        }

        let logical = if terminal {
            update_mcp_subscription(
                &mut transaction,
                &current,
                decision.logical_state,
                &decision.logical_payload,
                database_now,
            )
            .await?
        } else {
            current
        };
        match &command.action {
            ManagedMcpSandboxSessionLeaseRecoveryAction::RequeueUnstarted { .. } => {
                update_requeued_managed_mcp_sandbox_session_job(
                    &mut transaction,
                    &physical_job,
                    &decision,
                    database_now,
                )
                .await?;
            }
            ManagedMcpSandboxSessionLeaseRecoveryAction::TimeoutUnstarted { .. } => {
                terminalize_recovered_mcp_subscription_job(
                    &mut transaction,
                    &identity.tenant_id,
                    &logical_job,
                    database_now,
                )
                .await?;
                update_terminal_managed_mcp_sandbox_session_job(
                    &mut transaction,
                    &physical_job,
                    &decision,
                    database_now,
                )
                .await?;
            }
            ManagedMcpSandboxSessionLeaseRecoveryAction::MarkLost { .. } => {
                requeue_recovered_mcp_subscription_job(
                    &mut transaction,
                    &identity.tenant_id,
                    &logical_job,
                    database_now,
                )
                .await?;
                update_terminal_managed_mcp_sandbox_session_job(
                    &mut transaction,
                    &physical_job,
                    &decision,
                    database_now,
                )
                .await?;
            }
        }
        append_managed_mcp_sandbox_recovery_event(&mut transaction, &command, &logical, &decision)
            .await?;
        terminalize_managed_mcp_sandbox_recovery_receipt(&mut transaction, &command).await?;
        let result = managed_mcp_sandbox_recovery_result(&command);
        transaction.commit().await?;
        Ok(CommandOutcome::Applied(result))
    }

    pub async fn report_mcp_subscription_transport_termination(
        &self,
        command: ReportMcpSubscriptionTransportTermination,
    ) -> Result<CommandOutcome<McpSubscriptionRecord>, RepositoryError> {
        command
            .validate_at(Utc::now())
            .map_err(invalid_mcp_subscription)?;
        let mut transaction = self.pool().begin().await?;
        let database_now = database_now(&mut transaction).await?;
        command
            .validate_at(database_now)
            .map_err(invalid_mcp_subscription)?;
        let current = load_mcp_subscription(
            &mut transaction,
            &command.audit.tenant_id,
            &command.subscription_id,
            true,
            database_now,
        )
        .await?;
        let job = load_mcp_subscription_job(
            &mut transaction,
            &command.audit.tenant_id,
            &current.job_id,
            true,
        )
        .await?;
        let receipt_payload =
            mcp_subscription_transport_termination_receipt_payload(&command, &current.job_id)?;
        if claim_mcp_subscription_worker_receipt(
            &mut transaction,
            &command.audit,
            &current.job_id,
            "mcp.subscription.transport_termination",
            &receipt_payload,
        )
        .await?
        {
            transaction.commit().await?;
            return Ok(CommandOutcome::Replayed(current));
        }
        if current.payload.binding.authorization_generation
            != command.expected_authorization_generation
            || current.payload.session.generation != command.expected_session_generation
            || current.state != McpSubscriptionState::Active
            || !matches!(
                current.payload.session.state,
                insight_platform_contracts::McpSessionState::Ready
                    | insight_platform_contracts::McpSessionState::Degraded
            )
            || job.state != JobState::Waiting
        {
            return Err(RepositoryError::StaleFence);
        }
        let base = resolve_mcp_base_execution_contract(
            &mut transaction,
            &current.tenant_id,
            &current.payload.binding.mcp_deployment,
            &current.payload.binding.authorization_binding_id,
            current.payload.binding.authorization_generation,
            &current.payload.binding.authorization_context_digest,
            &current.payload.binding.principal_id,
            database_now,
        )
        .await?;
        require_same_subscription_base(&current.payload.binding, &base)?;
        let (next_payload, next_state) = current
            .payload
            .rebuild_after_session_loss(current.payload.session.version, database_now)
            .map_err(invalid_mcp_subscription)?;
        let next = update_mcp_subscription(
            &mut transaction,
            &current,
            next_state,
            &next_payload,
            database_now,
        )
        .await?;
        requeue_recovered_mcp_subscription_job(
            &mut transaction,
            &next.tenant_id,
            &job,
            database_now,
        )
        .await?;
        append_scheduler_event(
            &mut transaction,
            &next.tenant_id.to_string(),
            &command.audit.event_id,
            &command.audit.outbox_id,
            "mcp_operation",
            &next.subscription_id.to_string(),
            as_i64(next.version, "MCP subscription version")?,
            None,
            "mcp.subscription_transport_terminated",
            &TypedPayload::new(
                1,
                &serde_json::json!({
                    "job_id": next.job_id,
                    "prior_session_generation": command.expected_session_generation,
                    "session_loss_evidence_digest": command.session_loss_evidence_digest,
                }),
            )?,
        )
        .await?;
        terminalize_mcp_subscription_worker_receipt(
            &mut transaction,
            &command.audit,
            &next.job_id,
            "session_rebuild_scheduled",
            &command.subscription_id,
        )
        .await?;
        transaction.commit().await?;
        Ok(CommandOutcome::Applied(next))
    }

    pub async fn save_mcp_subscription_session(
        &self,
        command: SaveMcpSubscriptionSession,
    ) -> Result<CommandOutcome<McpSubscriptionRecord>, RepositoryError> {
        command
            .validate_at(Utc::now())
            .map_err(invalid_mcp_subscription)?;
        let mut transaction = self.pool().begin().await?;
        let database_now = database_now(&mut transaction).await?;
        command
            .validate_at(database_now)
            .map_err(invalid_mcp_subscription)?;
        let receipt_payload = mcp_subscription_session_receipt_payload(&command)?;
        if claim_mcp_subscription_worker_receipt(
            &mut transaction,
            &command.audit,
            &command.job_id,
            "mcp.subscription.session",
            &receipt_payload,
        )
        .await?
        {
            let record = load_mcp_subscription(
                &mut transaction,
                &command.audit.tenant_id,
                &command.subscription_id,
                false,
                database_now,
            )
            .await?;
            transaction.commit().await?;
            return Ok(CommandOutcome::Replayed(record));
        }
        let current = load_mcp_subscription(
            &mut transaction,
            &command.audit.tenant_id,
            &command.subscription_id,
            true,
            database_now,
        )
        .await?;
        let job = load_mcp_subscription_job(
            &mut transaction,
            &command.audit.tenant_id,
            &command.job_id,
            true,
        )
        .await?;
        require_mcp_subscription_job_fence(&job, &current, &command.fence, database_now)?;
        if current.version != command.expected_subscription_version {
            return Err(RepositoryError::StaleFence);
        }
        let base = resolve_mcp_base_execution_contract(
            &mut transaction,
            &current.tenant_id,
            &current.payload.binding.mcp_deployment,
            &current.payload.binding.authorization_binding_id,
            current.payload.binding.authorization_generation,
            &current.payload.binding.authorization_context_digest,
            &current.payload.binding.principal_id,
            database_now,
        )
        .await?;
        require_same_subscription_base(&current.payload.binding, &base)?;
        let (next_payload, next_state) = current
            .payload
            .transition_session(
                command.expected_session_version,
                command.target,
                command.encrypted_opaque_session.clone(),
                command.expires_at,
                base.server.limits.maximum_session_milliseconds,
                database_now,
            )
            .map_err(invalid_mcp_subscription)?;
        let next = update_mcp_subscription(
            &mut transaction,
            &current,
            next_state,
            &next_payload,
            database_now,
        )
        .await?;
        update_mcp_subscription_job_for_session(
            &mut transaction,
            &job,
            &next,
            command.target,
            database_now,
        )
        .await?;
        append_scheduler_event(
            &mut transaction,
            &command.audit.tenant_id.to_string(),
            &command.audit.event_id,
            &command.audit.outbox_id,
            "mcp_operation",
            &command.subscription_id.to_string(),
            as_i64(next.version, "MCP subscription version")?,
            None,
            "mcp.subscription_session_changed",
            &TypedPayload::new(
                1,
                &serde_json::json!({
                    "authorization_generation": next.payload.binding.authorization_generation,
                    "job_id": next.job_id,
                    "phase_evidence_digest": command.phase_evidence_digest,
                    "session_generation": next.payload.session.generation,
                    "session_state": next.payload.session.state,
                    "subscription_state": next.state,
                }),
            )?,
        )
        .await?;
        terminalize_mcp_subscription_worker_receipt(
            &mut transaction,
            &command.audit,
            &command.job_id,
            "session_saved",
            &command.subscription_id,
        )
        .await?;
        transaction.commit().await?;
        Ok(CommandOutcome::Applied(next))
    }

    pub async fn commit_mcp_notification(
        &self,
        command: McpNotificationCommit,
    ) -> Result<McpNotificationCommitOutcome, RepositoryError> {
        command
            .validate_at(Utc::now())
            .map_err(invalid_mcp_subscription)?;
        let mut transaction = self.pool().begin().await?;
        let database_now = database_now(&mut transaction).await?;
        command
            .validate_at(database_now)
            .map_err(invalid_mcp_subscription)?;
        let request_digest = command.request_digest().map_err(invalid_mcp_subscription)?;
        let receipt_payload = mcp_notification_receipt_payload(&command)?;
        if let Some(disposition) = claim_mcp_notification_receipt(
            &mut transaction,
            &command,
            &request_digest,
            &receipt_payload,
        )
        .await?
        {
            let record = load_mcp_subscription(
                &mut transaction,
                &command.tenant_id,
                &command.subscription_id,
                false,
                database_now,
            )
            .await?;
            transaction.commit().await?;
            return Ok(McpNotificationCommitOutcome {
                record,
                disposition,
                replayed: true,
            });
        }
        let current = load_mcp_subscription(
            &mut transaction,
            &command.tenant_id,
            &command.subscription_id,
            true,
            database_now,
        )
        .await?;
        let job =
            load_mcp_subscription_job(&mut transaction, &command.tenant_id, &current.job_id, true)
                .await?;
        let base = resolve_mcp_base_execution_contract(
            &mut transaction,
            &current.tenant_id,
            &current.payload.binding.mcp_deployment,
            &current.payload.binding.authorization_binding_id,
            current.payload.binding.authorization_generation,
            &current.payload.binding.authorization_context_digest,
            &current.payload.binding.principal_id,
            database_now,
        )
        .await?;
        require_same_subscription_base(&current.payload.binding, &base)?;
        if command.wire_bytes > base.server.limits.maximum_sse_event_bytes {
            return Err(RepositoryError::InvalidInput(
                "MCP notification exceeds the exact Server limit".to_owned(),
            ));
        }
        let (next_payload, disposition) = current
            .payload
            .apply_notification(&command, database_now)
            .map_err(invalid_mcp_subscription)?;
        let record = match disposition {
            McpNotificationApplyDisposition::Stale => current,
            McpNotificationApplyDisposition::Wake => {
                if current.state != McpSubscriptionState::Active || job.state != JobState::Waiting {
                    return Err(RepositoryError::Conflict(
                        "MCP notification wake requires an active waiting subscription",
                    ));
                }
                let next = update_mcp_subscription(
                    &mut transaction,
                    &current,
                    current.state,
                    &next_payload,
                    database_now,
                )
                .await?;
                wake_mcp_subscription_job(&mut transaction, &current.tenant_id, &job, database_now)
                    .await?;
                append_scheduler_event(
                    &mut transaction,
                    &command.tenant_id.to_string(),
                    &command.audit.event_id,
                    &command.audit.outbox_id,
                    "mcp_operation",
                    &command.subscription_id.to_string(),
                    as_i64(next.version, "MCP subscription version")?,
                    None,
                    "mcp.subscription_invalidated",
                    &TypedPayload::new(
                        1,
                        &serde_json::json!({
                            "body_digest": command.body_digest,
                            "class": command.class,
                            "event_generation": command.event_generation,
                            "event_key_digest": command.event_key_digest,
                            "resource_uri_digest": command.resource_uri_digest,
                            "session_generation": command.session_generation,
                        }),
                    )?,
                )
                .await?;
                next
            }
            McpNotificationApplyDisposition::Coalesced => {
                if !matches!(
                    job.state,
                    JobState::Ready | JobState::Leased | JobState::Running
                ) {
                    return Err(RepositoryError::Conflict(
                        "MCP notification coalescing requires scheduled refresh work",
                    ));
                }
                update_mcp_subscription(
                    &mut transaction,
                    &current,
                    current.state,
                    &next_payload,
                    database_now,
                )
                .await?
            }
        };
        terminalize_mcp_notification_receipt(
            &mut transaction,
            &command,
            &request_digest,
            disposition,
        )
        .await?;
        transaction.commit().await?;
        Ok(McpNotificationCommitOutcome {
            record,
            disposition,
            replayed: false,
        })
    }

    pub async fn complete_mcp_subscription_refresh(
        &self,
        command: CompleteMcpSubscriptionRefresh,
    ) -> Result<CommandOutcome<McpSubscriptionRecord>, RepositoryError> {
        command
            .validate_at(Utc::now())
            .map_err(invalid_mcp_subscription)?;
        let mut transaction = self.pool().begin().await?;
        let database_now = database_now(&mut transaction).await?;
        command
            .validate_at(database_now)
            .map_err(invalid_mcp_subscription)?;
        let receipt_payload = mcp_subscription_refresh_receipt_payload(&command)?;
        if claim_mcp_subscription_worker_receipt(
            &mut transaction,
            &command.audit,
            &command.job_id,
            "mcp.subscription.refresh",
            &receipt_payload,
        )
        .await?
        {
            let record = load_mcp_subscription(
                &mut transaction,
                &command.audit.tenant_id,
                &command.subscription_id,
                false,
                database_now,
            )
            .await?;
            transaction.commit().await?;
            return Ok(CommandOutcome::Replayed(record));
        }
        let current = load_mcp_subscription(
            &mut transaction,
            &command.audit.tenant_id,
            &command.subscription_id,
            true,
            database_now,
        )
        .await?;
        let job = load_mcp_subscription_job(
            &mut transaction,
            &command.audit.tenant_id,
            &command.job_id,
            true,
        )
        .await?;
        require_mcp_subscription_job_fence(&job, &current, &command.fence, database_now)?;
        if current.version != command.expected_subscription_version
            || current.state != McpSubscriptionState::Active
        {
            return Err(RepositoryError::StaleFence);
        }
        let base = resolve_mcp_base_execution_contract(
            &mut transaction,
            &current.tenant_id,
            &current.payload.binding.mcp_deployment,
            &current.payload.binding.authorization_binding_id,
            current.payload.binding.authorization_generation,
            &current.payload.binding.authorization_context_digest,
            &current.payload.binding.principal_id,
            database_now,
        )
        .await?;
        require_same_subscription_base(&current.payload.binding, &base)?;
        let payload = current
            .payload
            .acknowledge_invalidation(
                command.expected_session_generation,
                command.expected_event_generation,
                database_now,
            )
            .map_err(invalid_mcp_subscription)?;
        let next = update_mcp_subscription(
            &mut transaction,
            &current,
            current.state,
            &payload,
            database_now,
        )
        .await?;
        park_mcp_subscription_job(&mut transaction, &job, &next, database_now).await?;
        append_scheduler_event(
            &mut transaction,
            &command.audit.tenant_id.to_string(),
            &command.audit.event_id,
            &command.audit.outbox_id,
            "mcp_operation",
            &command.subscription_id.to_string(),
            as_i64(next.version, "MCP subscription version")?,
            None,
            "mcp.subscription_refresh_committed",
            &TypedPayload::new(
                1,
                &serde_json::json!({
                    "event_generation": command.expected_event_generation,
                    "job_id": command.job_id,
                    "refresh_evidence_digest": command.refresh_evidence_digest,
                    "session_generation": command.expected_session_generation,
                }),
            )?,
        )
        .await?;
        terminalize_mcp_subscription_worker_receipt(
            &mut transaction,
            &command.audit,
            &command.job_id,
            "refreshed",
            &command.subscription_id,
        )
        .await?;
        transaction.commit().await?;
        Ok(CommandOutcome::Applied(next))
    }

    pub async fn complete_mcp_subscription_reconcile(
        &self,
        command: CompleteMcpSubscriptionReconcile,
    ) -> Result<CommandOutcome<McpSubscriptionRecord>, RepositoryError> {
        command
            .validate_at(Utc::now())
            .map_err(invalid_mcp_subscription)?;
        let mut transaction = self.pool().begin().await?;
        let database_now = database_now(&mut transaction).await?;
        command
            .validate_at(database_now)
            .map_err(invalid_mcp_subscription)?;
        let receipt_payload = mcp_subscription_reconcile_receipt_payload(&command)?;
        if claim_mcp_subscription_worker_receipt(
            &mut transaction,
            &command.audit,
            &command.job_id,
            "mcp.subscription.reconcile",
            &receipt_payload,
        )
        .await?
        {
            let record = load_mcp_subscription(
                &mut transaction,
                &command.audit.tenant_id,
                &command.subscription_id,
                false,
                database_now,
            )
            .await?;
            transaction.commit().await?;
            return Ok(CommandOutcome::Replayed(record));
        }
        let current = load_mcp_subscription(
            &mut transaction,
            &command.audit.tenant_id,
            &command.subscription_id,
            true,
            database_now,
        )
        .await?;
        let job = load_mcp_subscription_job(
            &mut transaction,
            &command.audit.tenant_id,
            &command.job_id,
            true,
        )
        .await?;
        require_mcp_subscription_job_fence(&job, &current, &command.fence, database_now)?;
        if current.version != command.expected_subscription_version
            || current.state != McpSubscriptionState::Active
            || current.payload.pending_invalidation.is_some()
            || current.payload.session.generation != command.expected_session_generation
            || !matches!(
                current.payload.session.state,
                insight_platform_contracts::McpSessionState::Ready
                    | insight_platform_contracts::McpSessionState::Degraded
            )
            || current
                .payload
                .session
                .expires_at
                .is_none_or(|expiry| expiry <= database_now)
        {
            return Err(RepositoryError::StaleFence);
        }
        let base = resolve_mcp_base_execution_contract(
            &mut transaction,
            &current.tenant_id,
            &current.payload.binding.mcp_deployment,
            &current.payload.binding.authorization_binding_id,
            current.payload.binding.authorization_generation,
            &current.payload.binding.authorization_context_digest,
            &current.payload.binding.principal_id,
            database_now,
        )
        .await?;
        require_same_subscription_base(&current.payload.binding, &base)?;
        let next_payload = current
            .payload
            .acknowledge_full_reconcile(command.expected_session_generation, database_now)
            .map_err(invalid_mcp_subscription)?;
        let next = update_mcp_subscription(
            &mut transaction,
            &current,
            current.state,
            &next_payload,
            database_now,
        )
        .await?;
        park_mcp_subscription_job(&mut transaction, &job, &next, database_now).await?;
        append_scheduler_event(
            &mut transaction,
            &command.audit.tenant_id.to_string(),
            &command.audit.event_id,
            &command.audit.outbox_id,
            "mcp_operation",
            &command.subscription_id.to_string(),
            as_i64(next.version, "MCP subscription version")?,
            None,
            "mcp.subscription_reconciled",
            &TypedPayload::new(
                1,
                &serde_json::json!({
                    "job_id": command.job_id,
                    "reconcile_evidence_digest": command.reconcile_evidence_digest,
                    "session_generation": command.expected_session_generation,
                }),
            )?,
        )
        .await?;
        terminalize_mcp_subscription_worker_receipt(
            &mut transaction,
            &command.audit,
            &command.job_id,
            "reconciled",
            &command.subscription_id,
        )
        .await?;
        transaction.commit().await?;
        Ok(CommandOutcome::Applied(next))
    }

    /// Bounded production scan across tenant partitions. The first query selects only tenants
    /// that currently own exact due logical-subscription Jobs; tenant-scoped authority methods
    /// then reconstruct and validate each candidate under the existing closed contract.
    pub async fn list_due_mcp_subscription_reconciliations_global(
        &self,
        limit: u16,
        minimum_idle_milliseconds: u64,
    ) -> Result<Vec<DueMcpSubscriptionReconcile>, RepositoryError> {
        if limit == 0
            || limit > MAX_MCP_SUBSCRIPTION_RECONCILE_SCAN
            || minimum_idle_milliseconds < MIN_MCP_SUBSCRIPTION_RECONCILE_IDLE_MILLISECONDS
        {
            return Err(RepositoryError::InvalidInput(
                "global MCP subscription reconcile scan is invalid".to_owned(),
            ));
        }
        let mut transaction = self.pool().begin().await?;
        let observed_at = database_now(&mut transaction).await?;
        let idle_milliseconds = i64::try_from(minimum_idle_milliseconds).map_err(|_| {
            RepositoryError::InvalidInput(
                "MCP subscription reconcile interval is invalid".to_owned(),
            )
        })?;
        let not_updated_after = observed_at - chrono::Duration::milliseconds(idle_milliseconds);
        let rows = sqlx::query(
            r#"
            SELECT DISTINCT subscription.tenant_id
            FROM insight_platform.invocations AS subscription
            JOIN insight_platform.jobs AS job
              ON job.tenant_id = subscription.tenant_id
             AND job.invocation_id = subscription.invocation_id
            WHERE subscription.invocation_kind = 'mcp_subscription'
              AND subscription.owner_kind = 'mcp_operation'
              AND subscription.owner_id = subscription.invocation_id
              AND subscription.state = 'active'
              AND subscription.updated_at <= $1
              AND subscription.deadline > $2
              AND subscription.payload -> 'pending_invalidation' = 'null'::jsonb
              AND subscription.payload #>> '{session,state}' IN ('ready', 'degraded')
              AND (subscription.payload #>> '{session,expires_at}')::timestamptz > $2
              AND job.work_class = 'mcp'
              AND job.job_kind = 'mcp_subscription'
              AND job.owner_kind = 'mcp_operation'
              AND job.owner_id = subscription.invocation_id
              AND job.state = 'waiting'
              AND job.wake_kind = 'remote_invocation'
              AND job.wake_state = 'pending'
            ORDER BY subscription.tenant_id
            LIMIT $3
            "#,
        )
        .bind(not_updated_after)
        .bind(observed_at)
        .bind(i64::from(limit))
        .fetch_all(&mut *transaction)
        .await?;
        let tenants = rows
            .into_iter()
            .map(|row| parse_resource_id_column(&row, "tenant_id"))
            .collect::<Result<Vec<_>, _>>()?;
        transaction.commit().await?;
        let mut candidates = Vec::new();
        for tenant_id in tenants {
            let remaining = usize::from(limit).saturating_sub(candidates.len());
            if remaining == 0 {
                break;
            }
            let mut tenant_candidates = self
                .list_due_mcp_subscription_reconciliations(McpSubscriptionReconcileScan {
                    tenant_id,
                    limit: u16::try_from(remaining).map_err(|_| {
                        RepositoryError::InvalidInput(
                            "global MCP subscription reconcile limit overflow".to_owned(),
                        )
                    })?,
                    minimum_idle_milliseconds,
                })
                .await?;
            candidates.append(&mut tenant_candidates);
        }
        Ok(candidates)
    }

    pub async fn list_due_mcp_subscription_reconciliations(
        &self,
        scan: McpSubscriptionReconcileScan,
    ) -> Result<Vec<DueMcpSubscriptionReconcile>, RepositoryError> {
        scan.validate().map_err(invalid_mcp_subscription)?;
        let mut transaction = self.pool().begin().await?;
        let observed_at = database_now(&mut transaction).await?;
        let idle_milliseconds = i64::try_from(scan.minimum_idle_milliseconds).map_err(|_| {
            RepositoryError::InvalidInput(
                "MCP subscription reconcile interval is invalid".to_owned(),
            )
        })?;
        let not_updated_after = observed_at - chrono::Duration::milliseconds(idle_milliseconds);
        let rows = sqlx::query(
            r#"
            SELECT subscription.invocation_id, subscription.version AS subscription_version,
                   subscription.trace_id,
                   subscription.payload_schema_version, subscription.payload,
                   subscription.payload_digest, job.job_id, job.version AS job_version
            FROM insight_platform.invocations AS subscription
            JOIN insight_platform.jobs AS job
              ON job.tenant_id = subscription.tenant_id
             AND job.invocation_id = subscription.invocation_id
            WHERE subscription.tenant_id = $1
              AND subscription.invocation_kind = 'mcp_subscription'
              AND subscription.owner_kind = 'mcp_operation'
              AND subscription.owner_id = subscription.invocation_id
              AND subscription.state = 'active'
              AND subscription.updated_at <= $2
              AND subscription.deadline > $3
              AND subscription.payload -> 'pending_invalidation' = 'null'::jsonb
              AND subscription.payload #>> '{session,state}' IN ('ready', 'degraded')
              AND (subscription.payload #>> '{session,expires_at}')::timestamptz > $3
              AND job.work_class = 'mcp'
              AND job.job_kind = 'mcp_subscription'
              AND job.owner_kind = 'mcp_operation'
              AND job.owner_id = subscription.invocation_id
              AND job.state = 'waiting'
              AND job.wake_kind = 'remote_invocation'
              AND job.wake_state = 'pending'
            ORDER BY subscription.updated_at, subscription.invocation_id
            LIMIT $4
            "#,
        )
        .bind(scan.tenant_id.to_string())
        .bind(not_updated_after)
        .bind(observed_at)
        .bind(i64::from(scan.limit))
        .fetch_all(&mut *transaction)
        .await?;
        let mut candidates = Vec::with_capacity(rows.len());
        for row in rows {
            let typed =
                payload_from_row(&row, "payload_schema_version", "payload", "payload_digest")?;
            let payload: McpSubscriptionPayload =
                decode_versioned_payload(&typed, "MCP subscription")?;
            let candidate = DueMcpSubscriptionReconcile {
                trace: insight_platform_contracts::TraceIdentityV1::new(
                    row.try_get::<String, _>("trace_id")?
                        .parse::<insight_platform_contracts::TraceId>()
                        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?,
                ),
                tenant_id: scan.tenant_id.clone(),
                subscription_id: parse_resource_id_column(&row, "invocation_id")?,
                job_id: parse_resource_id_column(&row, "job_id")?,
                subscription_version: parse_positive_u64_column(&row, "subscription_version")?,
                job_version: parse_positive_u64_column(&row, "job_version")?,
                session_generation: payload.session.generation,
                not_updated_after,
                observed_at,
            };
            candidate
                .validate()
                .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
            candidates.push(candidate);
        }
        transaction.commit().await?;
        Ok(candidates)
    }

    pub async fn wake_mcp_subscription_reconcile(
        &self,
        command: WakeMcpSubscriptionReconcile,
    ) -> Result<CommandOutcome<McpSubscriptionRecord>, RepositoryError> {
        command
            .validate_at(Utc::now())
            .map_err(invalid_mcp_subscription)?;
        let mut transaction = self.pool().begin().await?;
        let database_now = database_now(&mut transaction).await?;
        command
            .validate_at(database_now)
            .map_err(invalid_mcp_subscription)?;
        let receipt_payload = mcp_subscription_reconcile_wake_receipt_payload(&command)?;
        if claim_mcp_subscription_worker_receipt(
            &mut transaction,
            &command.audit,
            &command.candidate.job_id,
            "mcp.subscription.reconcile_scan",
            &receipt_payload,
        )
        .await?
        {
            let record = load_mcp_subscription(
                &mut transaction,
                &command.candidate.tenant_id,
                &command.candidate.subscription_id,
                false,
                database_now,
            )
            .await?;
            transaction.commit().await?;
            return Ok(CommandOutcome::Replayed(record));
        }
        let current = load_mcp_subscription(
            &mut transaction,
            &command.candidate.tenant_id,
            &command.candidate.subscription_id,
            true,
            database_now,
        )
        .await?;
        let job = load_mcp_subscription_job(
            &mut transaction,
            &command.candidate.tenant_id,
            &command.candidate.job_id,
            true,
        )
        .await?;
        if current.version != command.candidate.subscription_version
            || current.state != McpSubscriptionState::Active
            || current.updated_at > command.candidate.not_updated_after
            || current.payload.pending_invalidation.is_some()
            || current.payload.session.generation != command.candidate.session_generation
            || !matches!(
                current.payload.session.state,
                insight_platform_contracts::McpSessionState::Ready
                    | insight_platform_contracts::McpSessionState::Degraded
            )
            || current
                .payload
                .session
                .expires_at
                .is_none_or(|expiry| expiry <= database_now)
            || job.version != command.candidate.job_version
            || job.state != JobState::Waiting
        {
            return Err(RepositoryError::StaleFence);
        }
        let base = resolve_mcp_base_execution_contract(
            &mut transaction,
            &current.tenant_id,
            &current.payload.binding.mcp_deployment,
            &current.payload.binding.authorization_binding_id,
            current.payload.binding.authorization_generation,
            &current.payload.binding.authorization_context_digest,
            &current.payload.binding.principal_id,
            database_now,
        )
        .await?;
        require_same_subscription_base(&current.payload.binding, &base)?;
        wake_mcp_subscription_job(&mut transaction, &current.tenant_id, &job, database_now).await?;
        let next_job_version = job.version.checked_add(1).ok_or_else(|| {
            RepositoryError::InvalidInput("MCP subscription Job version overflow".to_owned())
        })?;
        append_scheduler_event(
            &mut transaction,
            &command.audit.tenant_id.to_string(),
            &command.audit.event_id,
            &command.audit.outbox_id,
            "job",
            &command.candidate.job_id.to_string(),
            as_i64(next_job_version, "MCP subscription Job version")?,
            None,
            "mcp.subscription_reconcile_due",
            &TypedPayload::new(
                1,
                &serde_json::json!({
                    "session_generation": command.candidate.session_generation,
                    "subscription_id": command.candidate.subscription_id,
                    "subscription_version": command.candidate.subscription_version,
                }),
            )?,
        )
        .await?;
        terminalize_mcp_subscription_worker_receipt(
            &mut transaction,
            &command.audit,
            &command.candidate.job_id,
            "scheduled",
            &command.candidate.subscription_id,
        )
        .await?;
        transaction.commit().await?;
        Ok(CommandOutcome::Applied(current))
    }

    pub async fn list_due_mcp_subscription_recoveries_global(
        &self,
        limit: u16,
    ) -> Result<Vec<DueMcpSubscriptionRecovery>, RepositoryError> {
        if limit == 0 || limit > MAX_MCP_SUBSCRIPTION_RECONCILE_SCAN {
            return Err(RepositoryError::InvalidInput(
                "global MCP subscription recovery scan is invalid".to_owned(),
            ));
        }
        let mut transaction = self.pool().begin().await?;
        let observed_at = database_now(&mut transaction).await?;
        let rows = sqlx::query(
            r#"
            SELECT DISTINCT subscription.tenant_id
            FROM insight_platform.invocations AS subscription
            JOIN insight_platform.jobs AS job
              ON job.tenant_id = subscription.tenant_id
             AND job.job_id = subscription.payload #>> '{binding,job_id}'
            WHERE subscription.invocation_kind = 'mcp_subscription'
              AND subscription.owner_kind = 'mcp_operation'
              AND subscription.owner_id = subscription.invocation_id
              AND subscription.state IN ('pending', 'active')
              AND subscription.terminal_at IS NULL
              AND subscription.deadline > $1
              AND job.work_class = 'mcp'
              AND job.job_kind = 'mcp_subscription'
              AND job.owner_kind = 'mcp_operation'
              AND job.owner_id = subscription.invocation_id
              AND job.terminal_at IS NULL
              AND (
                    (job.state IN ('leased', 'running') AND job.lease_expires_at <= $1)
                 OR (job.state = 'waiting'
                     AND subscription.state = 'active'
                     AND subscription.payload #>> '{session,state}' IN ('ready', 'degraded')
                     AND (subscription.payload #>> '{session,expires_at}')::timestamptz <= $1)
              )
            ORDER BY subscription.tenant_id
            LIMIT $2
            "#,
        )
        .bind(observed_at)
        .bind(i64::from(limit))
        .fetch_all(&mut *transaction)
        .await?;
        let tenants = rows
            .into_iter()
            .map(|row| parse_resource_id_column(&row, "tenant_id"))
            .collect::<Result<Vec<_>, _>>()?;
        transaction.commit().await?;
        let mut candidates = Vec::new();
        for tenant_id in tenants {
            let remaining = usize::from(limit).saturating_sub(candidates.len());
            if remaining == 0 {
                break;
            }
            let mut tenant_candidates = self
                .list_due_mcp_subscription_recoveries(McpSubscriptionRecoveryScan {
                    tenant_id,
                    limit: u16::try_from(remaining).map_err(|_| {
                        RepositoryError::InvalidInput(
                            "global MCP subscription recovery limit overflow".to_owned(),
                        )
                    })?,
                })
                .await?;
            candidates.append(&mut tenant_candidates);
        }
        Ok(candidates)
    }

    pub async fn list_due_mcp_subscription_recoveries(
        &self,
        scan: McpSubscriptionRecoveryScan,
    ) -> Result<Vec<DueMcpSubscriptionRecovery>, RepositoryError> {
        scan.validate().map_err(invalid_mcp_subscription)?;
        let mut transaction = self.pool().begin().await?;
        let observed_at = database_now(&mut transaction).await?;
        let rows = sqlx::query(
            r#"
            SELECT subscription.*,
                   job.job_id AS recovery_job_id,
                   job.state AS recovery_job_state,
                   job.version AS recovery_job_version,
                   job.lease_epoch AS recovery_lease_generation,
                   job.lease_expires_at AS recovery_lease_expires_at
            FROM insight_platform.invocations AS subscription
            JOIN insight_platform.jobs AS job
              ON job.tenant_id = subscription.tenant_id
             AND job.job_id = subscription.payload #>> '{binding,job_id}'
            WHERE subscription.tenant_id = $1
              AND subscription.invocation_kind = 'mcp_subscription'
              AND subscription.owner_kind = 'mcp_operation'
              AND subscription.owner_id = subscription.invocation_id
              AND subscription.state IN ('pending', 'active')
              AND subscription.terminal_at IS NULL
              AND subscription.deadline > $2
              AND job.work_class = 'mcp'
              AND job.job_kind = 'mcp_subscription'
              AND job.owner_kind = 'mcp_operation'
              AND job.owner_id = subscription.invocation_id
              AND job.terminal_at IS NULL
              AND (
                    (job.state IN ('leased', 'running') AND job.lease_expires_at <= $2)
                 OR (job.state = 'waiting'
                     AND subscription.state = 'active'
                     AND subscription.payload #>> '{session,state}' IN ('ready', 'degraded')
                     AND (subscription.payload #>> '{session,expires_at}')::timestamptz <= $2)
              )
            ORDER BY LEAST(
                         COALESCE(job.lease_expires_at, 'infinity'::timestamptz),
                         COALESCE(
                             (subscription.payload #>> '{session,expires_at}')::timestamptz,
                             'infinity'::timestamptz
                         )
                     ), subscription.invocation_id
            LIMIT $3
            "#,
        )
        .bind(scan.tenant_id.to_string())
        .bind(observed_at)
        .bind(i64::from(scan.limit))
        .fetch_all(&mut *transaction)
        .await?;
        let mut candidates = Vec::with_capacity(rows.len());
        for row in rows {
            let typed =
                payload_from_row(&row, "payload_schema_version", "payload", "payload_digest")?;
            let payload: McpSubscriptionPayload =
                decode_versioned_payload(&typed, "MCP subscription")?;
            let observed_job_state = row
                .try_get::<String, _>("recovery_job_state")?
                .parse::<JobState>()
                .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
            let cause = if matches!(observed_job_state, JobState::Leased | JobState::Running) {
                McpSubscriptionRecoveryCause::ExpiredLease
            } else {
                McpSubscriptionRecoveryCause::ExpiredSession
            };
            let lease_generation = u64::try_from(
                row.try_get::<i64, _>("recovery_lease_generation")?,
            )
            .map_err(|_| RepositoryError::CorruptRow("negative MCP lease generation".to_owned()))?;
            let candidate = DueMcpSubscriptionRecovery {
                trace: insight_platform_contracts::TraceIdentityV1::new(
                    row.try_get::<String, _>("trace_id")?
                        .parse::<insight_platform_contracts::TraceId>()
                        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?,
                ),
                tenant_id: parse_resource_id_column(&row, "tenant_id")?,
                subscription_id: parse_resource_id_column(&row, "invocation_id")?,
                job_id: parse_resource_id_column(&row, "recovery_job_id")?,
                subscription_version: parse_positive_u64_column(&row, "version")?,
                session_version: payload.session.version,
                session_generation: payload.session.generation,
                job_version: u64::try_from(row.try_get::<i64, _>("recovery_job_version")?)
                    .map_err(|_| {
                        RepositoryError::CorruptRow("negative MCP Job version".to_owned())
                    })?,
                observed_job_state,
                observed_lease_generation: (cause == McpSubscriptionRecoveryCause::ExpiredLease)
                    .then_some(lease_generation),
                observed_lease_expires_at: row.try_get("recovery_lease_expires_at")?,
                observed_session_expires_at: payload.session.expires_at,
                cause,
                observed_at,
            };
            candidate.validate().map_err(invalid_mcp_subscription)?;
            candidates.push(candidate);
        }
        transaction.commit().await?;
        Ok(candidates)
    }

    pub async fn recover_due_mcp_subscription(
        &self,
        command: RecoverDueMcpSubscription,
    ) -> Result<CommandOutcome<McpSubscriptionRecord>, RepositoryError> {
        command
            .validate_at(Utc::now())
            .map_err(invalid_mcp_subscription)?;
        let mut transaction = self.pool().begin().await?;
        let database_now = database_now(&mut transaction).await?;
        command
            .validate_at(database_now)
            .map_err(invalid_mcp_subscription)?;
        let receipt_payload = mcp_subscription_recovery_receipt_payload(&command)?;
        if claim_mcp_subscription_worker_receipt(
            &mut transaction,
            &command.audit,
            &command.candidate.job_id,
            "mcp.subscription.recovery",
            &receipt_payload,
        )
        .await?
        {
            let record = load_mcp_subscription(
                &mut transaction,
                &command.audit.tenant_id,
                &command.candidate.subscription_id,
                false,
                database_now,
            )
            .await?;
            transaction.commit().await?;
            return Ok(CommandOutcome::Replayed(record));
        }
        let current = load_mcp_subscription(
            &mut transaction,
            &command.audit.tenant_id,
            &command.candidate.subscription_id,
            true,
            database_now,
        )
        .await?;
        let job = load_mcp_subscription_job(
            &mut transaction,
            &command.audit.tenant_id,
            &command.candidate.job_id,
            true,
        )
        .await?;
        require_same_mcp_recovery_observation(&current, &job, &command.candidate, database_now)?;
        let base = resolve_mcp_base_execution_contract(
            &mut transaction,
            &current.tenant_id,
            &current.payload.binding.mcp_deployment,
            &current.payload.binding.authorization_binding_id,
            current.payload.binding.authorization_generation,
            &current.payload.binding.authorization_context_digest,
            &current.payload.binding.principal_id,
            database_now,
        )
        .await?;
        require_same_subscription_base(&current.payload.binding, &base)?;
        let (next_payload, next_state) = match command.candidate.cause {
            McpSubscriptionRecoveryCause::ExpiredLease
                if (job.state == JobState::Leased
                    && current
                        .payload
                        .session
                        .expires_at
                        .is_none_or(|expiry| expiry > database_now))
                    || current.payload.session.state
                        == insight_platform_contracts::McpSessionState::Disconnected =>
            {
                (current.payload.clone(), current.state)
            }
            McpSubscriptionRecoveryCause::ExpiredLease
            | McpSubscriptionRecoveryCause::ExpiredSession => current
                .payload
                .rebuild_after_session_loss(current.payload.session.version, database_now)
                .map_err(invalid_mcp_subscription)?,
        };
        let next = update_mcp_subscription(
            &mut transaction,
            &current,
            next_state,
            &next_payload,
            database_now,
        )
        .await?;
        requeue_recovered_mcp_subscription_job(
            &mut transaction,
            &next.tenant_id,
            &job,
            database_now,
        )
        .await?;
        append_scheduler_event(
            &mut transaction,
            &next.tenant_id.to_string(),
            &command.audit.event_id,
            &command.audit.outbox_id,
            "mcp_operation",
            &next.subscription_id.to_string(),
            as_i64(next.version, "MCP subscription version")?,
            None,
            "mcp.subscription_recovery_scheduled",
            &TypedPayload::new(
                1,
                &serde_json::json!({
                    "cause": command.candidate.cause,
                    "full_reconcile_required": next.payload.full_reconcile_required,
                    "job_id": next.job_id,
                    "observed_job_state": command.candidate.observed_job_state,
                    "observed_lease_generation": command.candidate.observed_lease_generation,
                    "prior_session_generation": command.candidate.session_generation,
                }),
            )?,
        )
        .await?;
        terminalize_mcp_subscription_worker_receipt(
            &mut transaction,
            &command.audit,
            &command.candidate.job_id,
            "recovery_scheduled",
            &command.candidate.subscription_id,
        )
        .await?;
        transaction.commit().await?;
        Ok(CommandOutcome::Applied(next))
    }

    pub async fn report_mcp_subscription_session_loss(
        &self,
        command: ReportMcpSubscriptionSessionLoss,
    ) -> Result<CommandOutcome<McpSubscriptionRecord>, RepositoryError> {
        command
            .validate_at(Utc::now())
            .map_err(invalid_mcp_subscription)?;
        let mut transaction = self.pool().begin().await?;
        let database_now = database_now(&mut transaction).await?;
        command
            .validate_at(database_now)
            .map_err(invalid_mcp_subscription)?;
        let receipt_payload = mcp_subscription_session_loss_receipt_payload(&command)?;
        if claim_mcp_subscription_worker_receipt(
            &mut transaction,
            &command.audit,
            &command.job_id,
            "mcp.subscription.session_loss",
            &receipt_payload,
        )
        .await?
        {
            let record = load_mcp_subscription(
                &mut transaction,
                &command.audit.tenant_id,
                &command.subscription_id,
                false,
                database_now,
            )
            .await?;
            transaction.commit().await?;
            return Ok(CommandOutcome::Replayed(record));
        }
        let current = load_mcp_subscription(
            &mut transaction,
            &command.audit.tenant_id,
            &command.subscription_id,
            true,
            database_now,
        )
        .await?;
        let job = load_mcp_subscription_job(
            &mut transaction,
            &command.audit.tenant_id,
            &command.job_id,
            true,
        )
        .await?;
        if current.version != command.expected_subscription_version
            || current.payload.session.version != command.expected_session_version
            || current.payload.session.generation != command.expected_session_generation
            || current.state != McpSubscriptionState::Active
            || !matches!(
                current.payload.session.state,
                insight_platform_contracts::McpSessionState::Ready
                    | insight_platform_contracts::McpSessionState::Degraded
            )
            || job.state != JobState::Waiting
        {
            return Err(RepositoryError::StaleFence);
        }
        let base = resolve_mcp_base_execution_contract(
            &mut transaction,
            &current.tenant_id,
            &current.payload.binding.mcp_deployment,
            &current.payload.binding.authorization_binding_id,
            current.payload.binding.authorization_generation,
            &current.payload.binding.authorization_context_digest,
            &current.payload.binding.principal_id,
            database_now,
        )
        .await?;
        require_same_subscription_base(&current.payload.binding, &base)?;
        let (next_payload, next_state) = current
            .payload
            .rebuild_after_session_loss(command.expected_session_version, database_now)
            .map_err(invalid_mcp_subscription)?;
        let next = update_mcp_subscription(
            &mut transaction,
            &current,
            next_state,
            &next_payload,
            database_now,
        )
        .await?;
        requeue_recovered_mcp_subscription_job(
            &mut transaction,
            &next.tenant_id,
            &job,
            database_now,
        )
        .await?;
        append_scheduler_event(
            &mut transaction,
            &next.tenant_id.to_string(),
            &command.audit.event_id,
            &command.audit.outbox_id,
            "mcp_operation",
            &next.subscription_id.to_string(),
            as_i64(next.version, "MCP subscription version")?,
            None,
            "mcp.subscription_session_lost",
            &TypedPayload::new(
                1,
                &serde_json::json!({
                    "job_id": next.job_id,
                    "prior_session_generation": command.expected_session_generation,
                    "session_loss_evidence_digest": command.session_loss_evidence_digest,
                }),
            )?,
        )
        .await?;
        terminalize_mcp_subscription_worker_receipt(
            &mut transaction,
            &command.audit,
            &command.job_id,
            "session_rebuild_scheduled",
            &command.subscription_id,
        )
        .await?;
        transaction.commit().await?;
        Ok(CommandOutcome::Applied(next))
    }
}

#[derive(Debug)]
struct LockedMcpSubscriptionJob {
    job_id: ResourceId,
    owner_id: ResourceId,
    state: JobState,
    version: u64,
    lease_generation: u64,
    worker_id: Option<String>,
    lease_token_digest: Option<String>,
    lease_expires_at: Option<DateTime<Utc>>,
    payload: McpSubscriptionJobPayload,
}

async fn load_mcp_subscription(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    subscription_id: &ResourceId,
    for_update: bool,
    database_now: DateTime<Utc>,
) -> Result<McpSubscriptionRecord, RepositoryError> {
    let query = if for_update {
        "SELECT * FROM insight_platform.invocations WHERE tenant_id = $1 AND invocation_id = $2 FOR UPDATE"
    } else {
        "SELECT * FROM insight_platform.invocations WHERE tenant_id = $1 AND invocation_id = $2"
    };
    let row = sqlx::query(query)
        .bind(tenant_id.to_string())
        .bind(subscription_id.to_string())
        .fetch_optional(&mut **transaction)
        .await?
        .ok_or(RepositoryError::NotFound("MCP subscription"))?;
    if row.try_get::<String, _>("invocation_kind")? != "mcp_subscription"
        || row.try_get::<String, _>("owner_kind")? != "mcp_operation"
        || row.try_get::<String, _>("owner_id")? != subscription_id.to_string()
    {
        return Err(RepositoryError::CorruptRow(
            "MCP subscription row has the wrong typed owner".to_owned(),
        ));
    }
    let typed = payload_from_row(&row, "payload_schema_version", "payload", "payload_digest")?;
    let payload: McpSubscriptionPayload = decode_versioned_payload(&typed, "MCP subscription")?;
    let record = McpSubscriptionRecord {
        tenant_id: parse_resource_id_column(&row, "tenant_id")?,
        subscription_id: parse_resource_id_column(&row, "invocation_id")?,
        job_id: payload.binding.job_id.clone(),
        logical_key: row.try_get("logical_key")?,
        state: row
            .try_get::<String, _>("state")?
            .parse::<McpSubscriptionState>()
            .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?,
        version: parse_positive_u64_column(&row, "version")?,
        payload,
        deadline: row.try_get("deadline")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
        terminal_at: row.try_get("terminal_at")?,
    };
    record
        .validate_at(database_now)
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    Ok(record)
}

async fn load_mcp_subscription_job(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    job_id: &ResourceId,
    for_update: bool,
) -> Result<LockedMcpSubscriptionJob, RepositoryError> {
    let query = if for_update {
        "SELECT * FROM insight_platform.jobs WHERE tenant_id = $1 AND job_id = $2 FOR UPDATE"
    } else {
        "SELECT * FROM insight_platform.jobs WHERE tenant_id = $1 AND job_id = $2"
    };
    let row = sqlx::query(query)
        .bind(tenant_id.to_string())
        .bind(job_id.to_string())
        .fetch_optional(&mut **transaction)
        .await?
        .ok_or(RepositoryError::NotFound("MCP subscription Job"))?;
    if row.try_get::<String, _>("work_class")? != "mcp"
        || row.try_get::<String, _>("job_kind")? != "mcp_subscription"
        || row.try_get::<String, _>("owner_kind")? != "mcp_operation"
    {
        return Err(RepositoryError::CorruptRow(
            "MCP subscription Job has the wrong typed owner".to_owned(),
        ));
    }
    let typed = payload_from_row(&row, "payload_schema_version", "payload", "payload_digest")?;
    let payload: McpJobPayload = decode_typed_payload(&typed, "MCP Job")?;
    let payload = match payload {
        McpJobPayload::Subscription(payload) => payload,
        McpJobPayload::Discovery(_) => {
            return Err(RepositoryError::CorruptRow(
                "MCP subscription Job contains a discovery payload".to_owned(),
            ));
        }
    };
    let owner_id = parse_resource_id_column(&row, "owner_id")?;
    payload
        .validate_for(&owner_id)
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    Ok(LockedMcpSubscriptionJob {
        job_id: parse_resource_id_column(&row, "job_id")?,
        owner_id,
        state: row
            .try_get::<String, _>("state")?
            .parse::<JobState>()
            .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?,
        version: parse_positive_u64_column(&row, "version")?,
        lease_generation: u64::try_from(row.try_get::<i64, _>("lease_epoch")?)
            .map_err(|_| RepositoryError::CorruptRow("negative MCP lease generation".to_owned()))?,
        worker_id: row.try_get("worker_id")?,
        lease_token_digest: row.try_get("lease_token_digest")?,
        lease_expires_at: row.try_get("lease_expires_at")?,
        payload,
    })
}

#[cfg(any())]
pub(crate) async fn load_managed_mcp_sandbox_session_job(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    job_id: &ResourceId,
    limits: SandboxCommandLimits,
    for_update: bool,
) -> Result<
    (
        JobProjection,
        ManagedMcpSandboxSessionJobPayload,
        ResourceId,
    ),
    RepositoryError,
> {
    let query = if for_update {
        "SELECT * FROM insight_platform.jobs WHERE tenant_id = $1 AND job_id = $2 FOR UPDATE"
    } else {
        "SELECT * FROM insight_platform.jobs WHERE tenant_id = $1 AND job_id = $2"
    };
    let row = sqlx::query(query)
        .bind(tenant_id.to_string())
        .bind(job_id.to_string())
        .fetch_optional(&mut **transaction)
        .await?
        .ok_or(RepositoryError::NotFound(
            "Managed MCP physical Sandbox Job",
        ))?;
    let record = job_from_row(row)?;
    let job = job_projection(&record)?;
    let stored: SandboxJobPayload = decode_versioned_payload(&record.payload, "Sandbox Job")?;
    let insight_platform_sandbox::SandboxJobWorkload::ManagedMcpSubscriptionSession(payload) =
        stored.workload
    else {
        return Err(RepositoryError::CorruptRow(
            "Managed MCP physical Job contains a different Sandbox workload".to_owned(),
        ));
    };
    payload
        .validate_for(&job, limits)
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    let usage_reservation_id = record
        .quota_reservation_id
        .as_deref()
        .ok_or_else(|| {
            RepositoryError::CorruptRow(
                "Managed MCP physical Job has no quota reservation".to_owned(),
            )
        })?
        .parse::<ResourceId>()
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    if usage_reservation_id.kind() != ResourceKind::UsageReservation
        || record.invocation_id.as_deref()
            != Some(
                payload
                    .request
                    .identity
                    .subscription_id
                    .to_string()
                    .as_str(),
            )
        || record.request_digest != payload.request.request_digest.to_string()
    {
        return Err(RepositoryError::CorruptRow(
            "Managed MCP physical Job binding is invalid".to_owned(),
        ));
    }
    Ok((job, *payload, usage_reservation_id))
}

#[cfg(any())]
async fn load_managed_mcp_sandbox_session_decision(
    transaction: &mut Transaction<'_, Postgres>,
    identity: &insight_platform_mcp_host::ManagedMcpSandboxSessionIdentity,
    limits: SandboxCommandLimits,
    for_update: bool,
    database_now: DateTime<Utc>,
) -> Result<ManagedMcpSandboxSessionPhaseDecision, RepositoryError> {
    let logical = load_mcp_subscription(
        transaction,
        &identity.tenant_id,
        &identity.subscription_id,
        for_update,
        database_now,
    )
    .await?;
    let (physical_job, physical_payload, _) = load_managed_mcp_sandbox_session_job(
        transaction,
        &identity.tenant_id,
        &identity.physical_job_id,
        limits,
        for_update,
    )
    .await?;
    let link = logical
        .payload
        .managed_sandbox_session
        .as_ref()
        .ok_or_else(|| {
            RepositoryError::CorruptRow(
                "Managed MCP subscription has no physical session link".to_owned(),
            )
        })?;
    if logical.tenant_id != identity.tenant_id
        || logical.subscription_id != identity.subscription_id
        || logical.job_id != identity.logical_job_id
        || link.identity != *identity
        || link.sandbox_request_digest != physical_payload.request.request_digest
        || physical_job.job_id != identity.physical_job_id
        || physical_payload.request.identity != *identity
    {
        return Err(RepositoryError::CorruptRow(
            "Managed MCP logical and physical session bindings disagree".to_owned(),
        ));
    }
    Ok(ManagedMcpSandboxSessionPhaseDecision {
        logical_payload: logical.payload,
        logical_state: logical.state,
        physical_job,
        physical_payload,
    })
}

#[cfg(any())]
async fn persist_managed_mcp_sandbox_claim(
    transaction: &mut Transaction<'_, Postgres>,
    current: &JobProjection,
    next: &JobProjection,
    database_now: DateTime<Utc>,
) -> Result<(), RepositoryError> {
    let lease = next.lease.as_ref().ok_or_else(|| {
        RepositoryError::CorruptRow("Managed MCP Sandbox claim produced no lease".to_owned())
    })?;
    let affected = sqlx::query(
        r#"
        UPDATE insight_platform.jobs
        SET state = $4, version = $5, lease_epoch = $6,
            worker_id = $7, lease_token_digest = $8,
            lease_expires_at = $9, heartbeat_at = $10,
            retry_at = NULL, updated_at = $11
        WHERE tenant_id = $1 AND job_id = $2 AND version = $3
          AND work_class = 'sandbox'
          AND job_kind = 'sandbox_managed_mcp_session'
          AND owner_kind = 'job'
          AND state = 'ready' AND worker_id IS NULL AND terminal_at IS NULL
        "#,
    )
    .bind(current.tenant_id.to_string())
    .bind(current.job_id.to_string())
    .bind(as_i64(current.version, "Managed MCP Sandbox Job version")?)
    .bind(next.state.as_str())
    .bind(as_i64(next.version, "Managed MCP Sandbox Job version")?)
    .bind(as_i64(
        next.lease_generation,
        "Managed MCP Sandbox lease generation",
    )?)
    .bind(lease.worker_process_generation_id.to_string())
    .bind(lease.token_digest.to_string())
    .bind(lease.expires_at)
    .bind(lease.heartbeat_at)
    .bind(database_now)
    .execute(&mut **transaction)
    .await?
    .rows_affected();
    if affected != 1 {
        return Err(RepositoryError::Conflict(
            "Managed MCP physical Sandbox Job claim",
        ));
    }
    Ok(())
}

#[cfg(any())]
async fn update_managed_mcp_sandbox_session_job(
    transaction: &mut Transaction<'_, Postgres>,
    current: &JobProjection,
    decision: &ManagedMcpSandboxSessionPhaseDecision,
    database_now: DateTime<Utc>,
) -> Result<(), RepositoryError> {
    let stored =
        SandboxJobPayload::managed_mcp_subscription_session(decision.physical_payload.clone());
    let payload = TypedPayload::from_versioned(1, &stored, 1_048_576)?;
    let lease = decision.physical_job.lease.as_ref().ok_or_else(|| {
        RepositoryError::CorruptRow(
            "active Managed MCP physical Sandbox Job has no lease".to_owned(),
        )
    })?;
    let affected = sqlx::query(
        r#"
        UPDATE insight_platform.jobs
        SET state = $4, version = $5, attempt_no = $6, lease_epoch = $7,
            worker_id = $8, lease_token_digest = $9, lease_expires_at = $10,
            heartbeat_at = $11, scheduled_at = $12, retry_at = $13,
            payload_schema_version = $14, payload = $15, payload_digest = $16,
            started_at = COALESCE(started_at, $17), updated_at = $17
        WHERE tenant_id = $1 AND job_id = $2 AND version = $3
          AND work_class = 'sandbox'
          AND job_kind = 'sandbox_managed_mcp_session'
          AND owner_kind = 'job'
          AND terminal_at IS NULL
        "#,
    )
    .bind(current.tenant_id.to_string())
    .bind(current.job_id.to_string())
    .bind(as_i64(current.version, "Managed MCP Sandbox Job version")?)
    .bind(decision.physical_job.state.as_str())
    .bind(as_i64(
        decision.physical_job.version,
        "Managed MCP Sandbox Job version",
    )?)
    .bind(
        i32::try_from(decision.physical_job.attempt_count).map_err(|_| {
            RepositoryError::InvalidInput(
                "Managed MCP Sandbox attempt count exceeds integer".to_owned(),
            )
        })?,
    )
    .bind(as_i64(
        decision.physical_job.lease_generation,
        "Managed MCP Sandbox lease generation",
    )?)
    .bind(lease.worker_process_generation_id.to_string())
    .bind(lease.token_digest.to_string())
    .bind(lease.expires_at)
    .bind(lease.heartbeat_at)
    .bind(decision.physical_job.scheduled_at)
    .bind(decision.physical_job.retry_at)
    .bind(payload.schema_version)
    .bind(&payload.value)
    .bind(&payload.digest)
    .bind(database_now)
    .execute(&mut **transaction)
    .await?
    .rows_affected();
    if affected != 1 {
        return Err(RepositoryError::StaleFence);
    }
    Ok(())
}

#[cfg(any())]
async fn update_requeued_managed_mcp_sandbox_session_job(
    transaction: &mut Transaction<'_, Postgres>,
    current: &JobProjection,
    decision: &ManagedMcpSandboxSessionPhaseDecision,
    database_now: DateTime<Utc>,
) -> Result<(), RepositoryError> {
    if decision.physical_job.lease.is_some()
        || decision.physical_job.state != JobState::Ready
        || decision.physical_payload.physical_state != SandboxJobState::Accepted
    {
        return Err(RepositoryError::CorruptRow(
            "Managed MCP Sandbox requeue decision is invalid".to_owned(),
        ));
    }
    let stored =
        SandboxJobPayload::managed_mcp_subscription_session(decision.physical_payload.clone());
    let payload = TypedPayload::from_versioned(1, &stored, 1_048_576)?;
    let affected = sqlx::query(
        r#"
        UPDATE insight_platform.jobs
        SET state = 'ready', version = $4,
            worker_id = NULL, lease_token_digest = NULL,
            lease_expires_at = NULL, heartbeat_at = NULL,
            retry_at = NULL, wake_kind = NULL, wake_state = NULL, wake_generation = 0,
            payload_schema_version = $5, payload = $6, payload_digest = $7,
            updated_at = $8
        WHERE tenant_id = $1 AND job_id = $2 AND version = $3
          AND work_class = 'sandbox'
          AND job_kind = 'sandbox_managed_mcp_session'
          AND owner_kind = 'job'
          AND state = 'leased' AND terminal_at IS NULL
        "#,
    )
    .bind(current.tenant_id.to_string())
    .bind(current.job_id.to_string())
    .bind(as_i64(current.version, "Managed MCP Sandbox Job version")?)
    .bind(as_i64(
        decision.physical_job.version,
        "Managed MCP Sandbox Job version",
    )?)
    .bind(payload.schema_version)
    .bind(&payload.value)
    .bind(&payload.digest)
    .bind(database_now)
    .execute(&mut **transaction)
    .await?
    .rows_affected();
    if affected != 1 {
        return Err(RepositoryError::StaleFence);
    }
    Ok(())
}

#[cfg(any())]
async fn update_terminal_managed_mcp_sandbox_session_job(
    transaction: &mut Transaction<'_, Postgres>,
    current: &JobProjection,
    decision: &ManagedMcpSandboxSessionPhaseDecision,
    database_now: DateTime<Utc>,
) -> Result<(), RepositoryError> {
    let valid_terminal = matches!(
        (
            decision.physical_job.state,
            decision.physical_payload.physical_state
        ),
        (JobState::ReconciliationRequired, SandboxJobState::Lost)
            | (JobState::TimedOut, SandboxJobState::TimedOut)
    );
    if decision.physical_job.lease.is_some() || !valid_terminal {
        return Err(RepositoryError::CorruptRow(
            "Managed MCP Sandbox terminal decision is invalid".to_owned(),
        ));
    }
    let stored =
        SandboxJobPayload::managed_mcp_subscription_session(decision.physical_payload.clone());
    let payload = TypedPayload::from_versioned(1, &stored, 1_048_576)?;
    let affected = sqlx::query(
        r#"
        UPDATE insight_platform.jobs
        SET state = $4, version = $5,
            worker_id = NULL, lease_token_digest = NULL,
            lease_expires_at = NULL, heartbeat_at = NULL,
            retry_at = NULL, wake_kind = NULL, wake_state = NULL, wake_generation = 0,
            payload_schema_version = $6, payload = $7, payload_digest = $8,
            terminal_at = $9, updated_at = $9
        WHERE tenant_id = $1 AND job_id = $2 AND version = $3
          AND work_class = 'sandbox'
          AND job_kind = 'sandbox_managed_mcp_session'
          AND owner_kind = 'job'
          AND terminal_at IS NULL
        "#,
    )
    .bind(current.tenant_id.to_string())
    .bind(current.job_id.to_string())
    .bind(as_i64(current.version, "Managed MCP Sandbox Job version")?)
    .bind(decision.physical_job.state.as_str())
    .bind(as_i64(
        decision.physical_job.version,
        "Managed MCP Sandbox Job version",
    )?)
    .bind(payload.schema_version)
    .bind(&payload.value)
    .bind(&payload.digest)
    .bind(database_now)
    .execute(&mut **transaction)
    .await?
    .rows_affected();
    if affected != 1 {
        return Err(RepositoryError::StaleFence);
    }
    Ok(())
}

#[cfg(any())]
async fn append_managed_mcp_sandbox_session_event(
    transaction: &mut Transaction<'_, Postgres>,
    audit: &insight_platform_sandbox::SandboxWorkerAudit,
    logical: &McpSubscriptionRecord,
    decision: &ManagedMcpSandboxSessionPhaseDecision,
    event_type: &str,
) -> Result<(), RepositoryError> {
    append_scheduler_event(
        transaction,
        &audit.tenant_id.to_string(),
        &audit.event_id,
        &audit.outbox_id,
        "job",
        &decision.physical_job.job_id.to_string(),
        as_i64(
            decision.physical_job.version,
            "Managed MCP Sandbox Job version",
        )?,
        None,
        event_type,
        &TypedPayload::new(
            1,
            &serde_json::json!({
                "lease_generation": decision.physical_job.lease_generation,
                "logical_job_id": logical.job_id,
                "physical_job_id": decision.physical_job.job_id,
                "physical_state": decision.physical_payload.physical_state,
                "phase_evidence_digest": decision.physical_payload.phase_evidence_digest,
                "phase_sequence": decision.physical_payload.phase_sequence,
                "session_generation": logical.payload.session.generation,
                "session_state": logical.payload.session.state,
                "subscription_id": logical.subscription_id,
                "subscription_version": logical.version,
            }),
        )?,
    )
    .await
}

#[cfg(any())]
fn managed_mcp_sandbox_recovery_result(
    command: &RecoverExpiredManagedMcpSandboxSessionLease,
) -> ManagedMcpSandboxSessionLeaseRecoveryResult {
    let disposition = match &command.action {
        ManagedMcpSandboxSessionLeaseRecoveryAction::RequeueUnstarted { .. } => {
            ManagedMcpSandboxSessionLeaseRecoveryDisposition::Requeued
        }
        ManagedMcpSandboxSessionLeaseRecoveryAction::TimeoutUnstarted { .. } => {
            ManagedMcpSandboxSessionLeaseRecoveryDisposition::TimedOut
        }
        ManagedMcpSandboxSessionLeaseRecoveryAction::MarkLost { .. } => {
            ManagedMcpSandboxSessionLeaseRecoveryDisposition::Lost
        }
    };
    ManagedMcpSandboxSessionLeaseRecoveryResult {
        tenant_id: command.expired.request.identity.tenant_id.clone(),
        subscription_id: command.expired.request.identity.subscription_id.clone(),
        physical_job_id: command.expired.request.identity.physical_job_id.clone(),
        recovered_lease_generation: command.expired.observed_lease_generation,
        disposition,
    }
}

#[cfg(any())]
async fn claim_managed_mcp_sandbox_recovery_receipt(
    transaction: &mut Transaction<'_, Postgres>,
    command: &RecoverExpiredManagedMcpSandboxSessionLease,
) -> Result<bool, RepositoryError> {
    let operation = "mcp.managed_sandbox_session.lease_recover";
    let identity = &command.expired.request.identity;
    let payload = TypedPayload::new(
        1,
        &serde_json::json!({
            "observed_job_version": command.expired.observed_job_version,
            "observed_lease_generation": command.expired.observed_lease_generation,
            "physical_job_id": identity.physical_job_id,
            "previous_worker_process_generation_id": command.expired.previous_worker_process_generation_id,
            "subscription_id": identity.subscription_id,
        }),
    )?;
    let inserted = sqlx::query(
        r#"
        INSERT INTO insight_platform.receipts (
            tenant_id, receipt_id, receipt_kind, scope_kind, scope_id,
            dedupe_owner_id, operation, idempotency_key_digest, request_digest, state,
            payload_schema_version, payload, payload_digest, expires_at
        ) VALUES ($1, $2, 'job_commit', 'job', $3, $4, $5, $6, $7,
                  'processing', $8, $9, $10, $11)
        ON CONFLICT (
            tenant_id, receipt_kind, scope_kind, scope_id, dedupe_owner_id,
            operation, idempotency_key_digest
        ) DO NOTHING
        RETURNING receipt_id
        "#,
    )
    .bind(command.audit.tenant_id.to_string())
    .bind(command.audit.receipt_id.to_string())
    .bind(identity.physical_job_id.to_string())
    .bind(
        command
            .expired
            .previous_worker_process_generation_id
            .to_string(),
    )
    .bind(operation)
    .bind(command.audit.idempotency_key_digest.to_string())
    .bind(command.audit.request_digest.to_string())
    .bind(payload.schema_version)
    .bind(&payload.value)
    .bind(&payload.digest)
    .bind(command.audit.receipt_expires_at)
    .fetch_optional(&mut **transaction)
    .await?;
    if inserted.is_some() {
        return Ok(false);
    }
    let row = sqlx::query(
        r#"
        SELECT request_digest, state
        FROM insight_platform.receipts
        WHERE tenant_id = $1 AND receipt_kind = 'job_commit'
          AND scope_kind = 'job' AND scope_id = $2 AND dedupe_owner_id = $3
          AND operation = $4 AND idempotency_key_digest = $5
        FOR UPDATE
        "#,
    )
    .bind(command.audit.tenant_id.to_string())
    .bind(identity.physical_job_id.to_string())
    .bind(
        command
            .expired
            .previous_worker_process_generation_id
            .to_string(),
    )
    .bind(operation)
    .bind(command.audit.idempotency_key_digest.to_string())
    .fetch_one(&mut **transaction)
    .await?;
    if row.try_get::<String, _>("request_digest")? != command.audit.request_digest.to_string() {
        return Err(RepositoryError::IdempotencyConflict);
    }
    if row.try_get::<String, _>("state")? != "succeeded" {
        return Err(RepositoryError::Conflict(
            "Managed MCP lease recovery receipt",
        ));
    }
    Ok(true)
}

#[cfg(any())]
async fn terminalize_managed_mcp_sandbox_recovery_receipt(
    transaction: &mut Transaction<'_, Postgres>,
    command: &RecoverExpiredManagedMcpSandboxSessionLease,
) -> Result<(), RepositoryError> {
    let result = managed_mcp_sandbox_recovery_result(command);
    let affected = sqlx::query(
        r#"
        UPDATE insight_platform.receipts
        SET state = 'succeeded', disposition = $4, response_reference_id = $5,
            completed_at = clock_timestamp()
        WHERE tenant_id = $1 AND receipt_id = $2 AND request_digest = $3
          AND state = 'processing'
        "#,
    )
    .bind(command.audit.tenant_id.to_string())
    .bind(command.audit.receipt_id.to_string())
    .bind(command.audit.request_digest.to_string())
    .bind(match result.disposition {
        ManagedMcpSandboxSessionLeaseRecoveryDisposition::Requeued => "requeued",
        ManagedMcpSandboxSessionLeaseRecoveryDisposition::TimedOut => "timed_out",
        ManagedMcpSandboxSessionLeaseRecoveryDisposition::Lost => "lost",
    })
    .bind(command.expired.request.identity.physical_job_id.to_string())
    .execute(&mut **transaction)
    .await?
    .rows_affected();
    if affected != 1 {
        return Err(RepositoryError::Conflict(
            "Managed MCP lease recovery receipt",
        ));
    }
    Ok(())
}

#[cfg(any())]
async fn append_managed_mcp_sandbox_recovery_event(
    transaction: &mut Transaction<'_, Postgres>,
    command: &RecoverExpiredManagedMcpSandboxSessionLease,
    logical: &McpSubscriptionRecord,
    decision: &ManagedMcpSandboxSessionPhaseDecision,
) -> Result<(), RepositoryError> {
    let event_type = match &command.action {
        ManagedMcpSandboxSessionLeaseRecoveryAction::RequeueUnstarted { .. } => {
            "mcp.managed_sandbox_session_lease_recovered"
        }
        ManagedMcpSandboxSessionLeaseRecoveryAction::TimeoutUnstarted { .. }
        | ManagedMcpSandboxSessionLeaseRecoveryAction::MarkLost { .. } => {
            "mcp.managed_sandbox_session_failed"
        }
    };
    append_scheduler_event(
        transaction,
        &command.audit.tenant_id.to_string(),
        &command.audit.event_id,
        &command.audit.outbox_id,
        "job",
        &decision.physical_job.job_id.to_string(),
        as_i64(
            decision.physical_job.version,
            "Managed MCP Sandbox Job version",
        )?,
        None,
        event_type,
        &TypedPayload::new(
            1,
            &serde_json::json!({
                "lease_generation": command.expired.observed_lease_generation,
                "logical_job_id": logical.job_id,
                "physical_job_id": decision.physical_job.job_id,
                "physical_state": decision.physical_payload.physical_state,
                "previous_worker_process_generation_id": command.expired.previous_worker_process_generation_id,
                "recovery_action": command.action,
                "recovery_process_generation_id": command.audit.recovery_process_generation_id,
                "session_generation": logical.payload.session.generation,
                "session_state": logical.payload.session.state,
                "subscription_id": logical.subscription_id,
                "subscription_version": logical.version,
            }),
        )?,
    )
    .await
}

#[cfg(any())]
fn require_managed_mcp_sandbox_session_replay(
    current: &McpSubscriptionRecord,
    physical_job: &JobProjection,
    physical_payload: &ManagedMcpSandboxSessionJobPayload,
    usage_reservation_id: &ResourceId,
    command: &AcceptManagedMcpSandboxSession,
) -> Result<(), RepositoryError> {
    let identity = &command.request.identity;
    let link = current
        .payload
        .managed_sandbox_session
        .as_ref()
        .ok_or(RepositoryError::IdempotencyConflict)?;
    if current.tenant_id != identity.tenant_id
        || current.subscription_id != identity.subscription_id
        || current.job_id != identity.logical_job_id
        || link.identity != *identity
        || link.sandbox_request_digest != command.request.request_digest
        || physical_job.tenant_id != identity.tenant_id
        || physical_job.job_id != identity.physical_job_id
        || physical_job.owner.owner_id != identity.sandbox_job_id
        || physical_job.owner.owner_kind != ResourceKind::Job
        || physical_payload.request.as_ref() != &command.request
        || usage_reservation_id != &command.usage_reservation_id
    {
        return Err(RepositoryError::IdempotencyConflict);
    }
    Ok(())
}

#[cfg(any())]
fn require_managed_mcp_sandbox_session_phase_replay(
    decision: &ManagedMcpSandboxSessionPhaseDecision,
    command: &CommitManagedMcpSandboxSessionPhase,
) -> Result<(), RepositoryError> {
    let same_binding = decision.physical_payload.request.identity == command.identity
        && decision.physical_job.job_id == command.identity.physical_job_id
        && decision
            .logical_payload
            .managed_sandbox_session
            .as_ref()
            .is_some_and(|link| link.identity == command.identity)
        && decision.physical_payload.executor_identity_digest.as_ref()
            == Some(&command.executor_identity_digest)
        && decision.physical_payload.attestor_route.as_ref() == Some(&command.attestor_route)
        && decision.physical_job.version > command.fence.expected_version;
    let reached_target = match command.target {
        SandboxJobState::Preparing => matches!(
            decision.physical_payload.physical_state,
            SandboxJobState::Preparing | SandboxJobState::Starting | SandboxJobState::Running
        ),
        SandboxJobState::Starting => matches!(
            decision.physical_payload.physical_state,
            SandboxJobState::Starting | SandboxJobState::Running
        ),
        _ => false,
    };
    let current_target_has_exact_evidence = decision.physical_payload.physical_state
        != command.target
        || (decision.physical_payload.phase_evidence_digest.as_ref()
            == Some(&command.phase_evidence_digest)
            && decision.physical_payload.prepared_binding == command.prepared_binding);
    if !same_binding || !reached_target || !current_target_has_exact_evidence {
        return Err(RepositoryError::Conflict(
            "Managed MCP Sandbox session phase replay",
        ));
    }
    Ok(())
}

#[cfg(any())]
fn require_managed_mcp_sandbox_session_ready_replay(
    decision: &ManagedMcpSandboxSessionPhaseDecision,
    command: &CommitManagedMcpSandboxSessionReady,
) -> Result<(), RepositoryError> {
    let expected_ready_binding = command
        .ready
        .durable_binding(&decision.physical_payload.request)?;
    if decision.logical_state != McpSubscriptionState::Active
        || decision.logical_payload.session.state
            != insight_platform_contracts::McpSessionState::Ready
        || decision
            .logical_payload
            .session
            .encrypted_opaque_session
            .as_ref()
            != Some(&command.ready.encrypted_opaque_session)
        || decision.logical_payload.session.expires_at != Some(command.ready.expires_at)
        || decision.physical_job.job_id != command.identity.physical_job_id
        || decision.physical_job.version <= command.fence.expected_version
        || decision.physical_payload.request.identity != command.identity
        || decision.physical_payload.physical_state != SandboxJobState::Running
        || decision.physical_payload.phase_evidence_digest.as_ref()
            != Some(&command.phase_evidence_digest)
        || decision.physical_payload.ready_binding.as_ref() != Some(&expected_ready_binding)
    {
        return Err(RepositoryError::Conflict(
            "Managed MCP Sandbox session Ready replay",
        ));
    }
    Ok(())
}

#[cfg(any())]
fn require_managed_mcp_sandbox_session_lost_replay(
    decision: &ManagedMcpSandboxSessionPhaseDecision,
    usage_reservation_id: &ResourceId,
    command: &CommitManagedMcpSandboxSessionLost,
) -> Result<(), RepositoryError> {
    if decision.logical_state != McpSubscriptionState::Pending
        || decision.logical_payload.session.state
            != insight_platform_contracts::McpSessionState::Disconnected
        || decision.logical_payload.managed_sandbox_session.is_some()
        || !decision.logical_payload.full_reconcile_required
        || decision.physical_job.job_id != command.identity.physical_job_id
        || decision.physical_job.version <= command.fence.expected_version
        || decision.physical_job.state != JobState::ReconciliationRequired
        || decision.physical_payload.request.identity != command.identity
        || decision.physical_payload.physical_state != SandboxJobState::Lost
        || decision.physical_payload.phase_evidence_digest.as_ref()
            != Some(&command.session_loss_evidence_digest)
        || decision.physical_payload.cleanup.as_ref() != Some(&command.cleanup)
        || usage_reservation_id != &command.usage_reservation_id
    {
        return Err(RepositoryError::Conflict(
            "Managed MCP Sandbox session lost replay",
        ));
    }
    Ok(())
}

fn require_mcp_subscription_job_fence(
    job: &LockedMcpSubscriptionJob,
    subscription: &McpSubscriptionRecord,
    fence: &insight_platform_jobs::JobFence,
    database_now: DateTime<Utc>,
) -> Result<(), RepositoryError> {
    let worker_process_generation_id = fence.worker_process_generation_id.to_string();
    if job.job_id != subscription.job_id
        || job.owner_id != subscription.subscription_id
        || job.payload.subscription_id != subscription.subscription_id
        || job.payload.binding_digest != subscription.payload.binding.canonical_digest
        || job.state != JobState::Running
        || job.version != fence.expected_version
        || job.lease_generation != fence.lease_generation
        || job.worker_id.as_deref() != Some(worker_process_generation_id.as_str())
        || job.lease_token_digest.as_deref() != Some(fence.token_digest.as_str())
        || job
            .lease_expires_at
            .is_none_or(|expiry| expiry <= database_now)
    {
        return Err(RepositoryError::StaleFence);
    }
    Ok(())
}

fn require_same_mcp_recovery_observation(
    subscription: &McpSubscriptionRecord,
    job: &LockedMcpSubscriptionJob,
    candidate: &DueMcpSubscriptionRecovery,
    database_now: DateTime<Utc>,
) -> Result<(), RepositoryError> {
    if subscription.tenant_id != candidate.tenant_id
        || subscription.subscription_id != candidate.subscription_id
        || subscription.job_id != candidate.job_id
        || subscription.version != candidate.subscription_version
        || subscription.payload.session.version != candidate.session_version
        || subscription.payload.session.generation != candidate.session_generation
        || subscription.payload.session.expires_at != candidate.observed_session_expires_at
        || job.job_id != candidate.job_id
        || job.owner_id != candidate.subscription_id
        || job.version != candidate.job_version
        || job.state != candidate.observed_job_state
    {
        return Err(RepositoryError::StaleFence);
    }
    match candidate.cause {
        McpSubscriptionRecoveryCause::ExpiredLease => {
            if job.lease_generation != candidate.observed_lease_generation.unwrap_or_default()
                || job.lease_expires_at != candidate.observed_lease_expires_at
                || job
                    .lease_expires_at
                    .is_none_or(|expiry| expiry > database_now)
            {
                return Err(RepositoryError::StaleFence);
            }
        }
        McpSubscriptionRecoveryCause::ExpiredSession => {
            if subscription.state != McpSubscriptionState::Active
                || !matches!(
                    subscription.payload.session.state,
                    insight_platform_contracts::McpSessionState::Ready
                        | insight_platform_contracts::McpSessionState::Degraded
                )
                || subscription
                    .payload
                    .session
                    .expires_at
                    .is_none_or(|expiry| expiry > database_now)
                || job.worker_id.is_some()
                || job.lease_token_digest.is_some()
                || job.lease_expires_at.is_some()
            {
                return Err(RepositoryError::StaleFence);
            }
        }
    }
    Ok(())
}

async fn update_mcp_subscription(
    transaction: &mut Transaction<'_, Postgres>,
    current: &McpSubscriptionRecord,
    state: McpSubscriptionState,
    payload: &McpSubscriptionPayload,
    database_now: DateTime<Utc>,
) -> Result<McpSubscriptionRecord, RepositoryError> {
    let typed = TypedPayload::from_versioned(1, payload, 1_048_576)?;
    let terminal_at = state.is_terminal().then_some(database_now);
    let next_version = current.version.checked_add(1).ok_or_else(|| {
        RepositoryError::InvalidInput("MCP subscription version overflow".to_owned())
    })?;
    let affected = sqlx::query(
        r#"
        UPDATE insight_platform.invocations
        SET state = $4, version = $5, payload_schema_version = $6,
            payload = $7, payload_digest = $8, terminal_at = $9, updated_at = $10
        WHERE tenant_id = $1 AND invocation_id = $2 AND version = $3
          AND invocation_kind = 'mcp_subscription'
        "#,
    )
    .bind(current.tenant_id.to_string())
    .bind(current.subscription_id.to_string())
    .bind(as_i64(current.version, "MCP subscription version")?)
    .bind(state.as_str())
    .bind(as_i64(next_version, "MCP subscription version")?)
    .bind(typed.schema_version)
    .bind(&typed.value)
    .bind(&typed.digest)
    .bind(terminal_at)
    .bind(database_now)
    .execute(&mut **transaction)
    .await?
    .rows_affected();
    if affected != 1 {
        return Err(RepositoryError::StaleFence);
    }
    load_mcp_subscription(
        transaction,
        &current.tenant_id,
        &current.subscription_id,
        false,
        database_now,
    )
    .await
}

async fn update_mcp_subscription_job_for_session(
    transaction: &mut Transaction<'_, Postgres>,
    job: &LockedMcpSubscriptionJob,
    subscription: &McpSubscriptionRecord,
    target: insight_platform_contracts::McpSessionState,
    database_now: DateTime<Utc>,
) -> Result<(), RepositoryError> {
    let (state, wake_kind, wake_state, wake_generation, terminal_at, release_lease) = match target {
        insight_platform_contracts::McpSessionState::Ready
        | insight_platform_contracts::McpSessionState::Degraded
            if subscription.payload.full_reconcile_required =>
        {
            (JobState::Running, None, None, 0, None, false)
        }
        insight_platform_contracts::McpSessionState::Ready
        | insight_platform_contracts::McpSessionState::Degraded
        | insight_platform_contracts::McpSessionState::ReauthRequired => (
            JobState::Waiting,
            Some("remote_invocation"),
            Some("pending"),
            as_i64(
                subscription.payload.session.generation,
                "MCP session generation",
            )?,
            None,
            true,
        ),
        insight_platform_contracts::McpSessionState::Closed => {
            (JobState::Succeeded, None, None, 0, Some(database_now), true)
        }
        insight_platform_contracts::McpSessionState::Failed => {
            (JobState::Failed, None, None, 0, Some(database_now), true)
        }
        insight_platform_contracts::McpSessionState::Disconnected
        | insight_platform_contracts::McpSessionState::Connecting
        | insight_platform_contracts::McpSessionState::Initializing
        | insight_platform_contracts::McpSessionState::Draining => {
            (JobState::Running, None, None, 0, None, false)
        }
    };
    let affected = sqlx::query(
        r#"
        UPDATE insight_platform.jobs
        SET state = $4, version = version + 1,
            wake_kind = $5, wake_state = $6, wake_generation = $7,
            worker_id = CASE WHEN $8 THEN NULL ELSE worker_id END,
            lease_token_digest = CASE WHEN $8 THEN NULL ELSE lease_token_digest END,
            lease_expires_at = CASE WHEN $8 THEN NULL ELSE lease_expires_at END,
            heartbeat_at = CASE WHEN $8 THEN NULL ELSE heartbeat_at END,
            terminal_at = $9, updated_at = $10
        WHERE tenant_id = $1 AND job_id = $2 AND version = $3
          AND work_class = 'mcp' AND job_kind = 'mcp_subscription'
          AND owner_kind = 'mcp_operation' AND state = 'running'
        "#,
    )
    .bind(subscription.tenant_id.to_string())
    .bind(job.job_id.to_string())
    .bind(as_i64(job.version, "MCP subscription Job version")?)
    .bind(state.as_str())
    .bind(wake_kind)
    .bind(wake_state)
    .bind(wake_generation)
    .bind(release_lease)
    .bind(terminal_at)
    .bind(database_now)
    .execute(&mut **transaction)
    .await?
    .rows_affected();
    if affected != 1 {
        return Err(RepositoryError::StaleFence);
    }
    Ok(())
}

async fn wake_mcp_subscription_job(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    job: &LockedMcpSubscriptionJob,
    database_now: DateTime<Utc>,
) -> Result<(), RepositoryError> {
    let affected = sqlx::query(
        r#"
        UPDATE insight_platform.jobs
        SET state = 'ready', version = version + 1,
            wake_kind = NULL, wake_state = NULL, wake_generation = 0,
            scheduled_at = $4, updated_at = $4
        WHERE tenant_id = $1 AND job_id = $2 AND version = $3
          AND work_class = 'mcp' AND job_kind = 'mcp_subscription'
          AND owner_kind = 'mcp_operation' AND state = 'waiting'
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(job.job_id.to_string())
    .bind(as_i64(job.version, "MCP subscription Job version")?)
    .bind(database_now)
    .execute(&mut **transaction)
    .await?
    .rows_affected();
    if affected != 1 {
        return Err(RepositoryError::StaleFence);
    }
    Ok(())
}

async fn requeue_recovered_mcp_subscription_job(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    job: &LockedMcpSubscriptionJob,
    database_now: DateTime<Utc>,
) -> Result<(), RepositoryError> {
    let affected = sqlx::query(
        r#"
        UPDATE insight_platform.jobs
        SET state = 'ready', version = version + 1,
            wake_kind = NULL, wake_state = NULL, wake_generation = 0,
            worker_id = NULL, lease_token_digest = NULL,
            lease_expires_at = NULL, heartbeat_at = NULL,
            scheduled_at = $4, retry_at = NULL, updated_at = $4
        WHERE tenant_id = $1 AND job_id = $2 AND version = $3
          AND work_class = 'mcp' AND job_kind = 'mcp_subscription'
          AND owner_kind = 'mcp_operation'
          AND state IN ('leased', 'running', 'waiting')
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(job.job_id.to_string())
    .bind(as_i64(job.version, "MCP subscription Job version")?)
    .bind(database_now)
    .execute(&mut **transaction)
    .await?
    .rows_affected();
    if affected != 1 {
        return Err(RepositoryError::StaleFence);
    }
    Ok(())
}

#[cfg(any())]
async fn terminalize_recovered_mcp_subscription_job(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    job: &LockedMcpSubscriptionJob,
    database_now: DateTime<Utc>,
) -> Result<(), RepositoryError> {
    let affected = sqlx::query(
        r#"
        UPDATE insight_platform.jobs
        SET state = 'failed', version = version + 1,
            wake_kind = NULL, wake_state = NULL, wake_generation = 0,
            worker_id = NULL, lease_token_digest = NULL,
            lease_expires_at = NULL, heartbeat_at = NULL,
            retry_at = NULL, terminal_at = $4, updated_at = $4
        WHERE tenant_id = $1 AND job_id = $2 AND version = $3
          AND work_class = 'mcp' AND job_kind = 'mcp_subscription'
          AND owner_kind = 'mcp_operation'
          AND state IN ('leased', 'running', 'waiting') AND terminal_at IS NULL
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(job.job_id.to_string())
    .bind(as_i64(job.version, "MCP subscription Job version")?)
    .bind(database_now)
    .execute(&mut **transaction)
    .await?
    .rows_affected();
    if affected != 1 {
        return Err(RepositoryError::StaleFence);
    }
    Ok(())
}

async fn park_mcp_subscription_job(
    transaction: &mut Transaction<'_, Postgres>,
    job: &LockedMcpSubscriptionJob,
    subscription: &McpSubscriptionRecord,
    database_now: DateTime<Utc>,
) -> Result<(), RepositoryError> {
    let affected = sqlx::query(
        r#"
        UPDATE insight_platform.jobs
        SET state = 'waiting', version = version + 1,
            wake_kind = 'remote_invocation', wake_state = 'pending', wake_generation = $4,
            worker_id = NULL, lease_token_digest = NULL,
            lease_expires_at = NULL, heartbeat_at = NULL, updated_at = $5
        WHERE tenant_id = $1 AND job_id = $2 AND version = $3
          AND work_class = 'mcp' AND job_kind = 'mcp_subscription'
          AND owner_kind = 'mcp_operation' AND state = 'running'
        "#,
    )
    .bind(subscription.tenant_id.to_string())
    .bind(job.job_id.to_string())
    .bind(as_i64(job.version, "MCP subscription Job version")?)
    .bind(as_i64(
        subscription.payload.session.generation,
        "MCP session generation",
    )?)
    .bind(database_now)
    .execute(&mut **transaction)
    .await?
    .rows_affected();
    if affected != 1 {
        return Err(RepositoryError::StaleFence);
    }
    Ok(())
}

fn require_same_mcp_subscription_create(
    record: &McpSubscriptionRecord,
    command: &insight_platform_mcp_host::CreateMcpResourceSubscription,
) -> Result<(), RepositoryError> {
    if record.subscription_id != command.subscription_id
        || record.job_id != command.job_id
        || record.logical_key != command.logical_key
        || record.payload.binding.mcp_deployment != command.execution.mcp_deployment
        || record.payload.binding.discovery_snapshot_id != command.execution.discovery_snapshot_id
        || record.payload.binding.discovery_snapshot_digest
            != command.execution.discovery_snapshot_digest
        || record.payload.binding.authorization_binding_id
            != command.execution.authorization_binding_id
        || record.payload.binding.authorization_generation
            != command.execution.authorization_generation
        || record.payload.binding.context_deployment != command.context_deployment
        || record.payload.binding.resource_uri != command.resource_uri
        || record.deadline != command.deadline
    {
        return Err(RepositoryError::IdempotencyConflict);
    }
    Ok(())
}

async fn validate_mcp_subscription_context_binding(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    context_deployment: &ExactDeploymentRef,
    contract: &McpHostExecutionContract,
) -> Result<Sha256Digest, RepositoryError> {
    let deployment =
        load_deployment(transaction, tenant_id, &context_deployment.deployment_id).await?;
    if deployment.bindings.digest != context_deployment.deployment_digest.to_string() {
        return Err(RepositoryError::Conflict("exact Context Deployment"));
    }
    let resource_id = deployment
        .resource_id
        .parse::<ResourceId>()
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    let resource = load_resource(transaction, tenant_id, &resource_id).await?;
    if resource.resource_kind != RegistryResourceKind::ContextSourceInterface.as_str()
        || resource.lifecycle_state != EntityLifecycle::Active.as_str()
        || resource.gate_state != "enabled"
    {
        return Err(RepositoryError::Conflict("Context Deployment gate"));
    }
    let closure = match decode_deployment_closure(&deployment.bindings)? {
        DeploymentClosure::ContextSourceInterface(closure) => closure,
        _ => {
            return Err(RepositoryError::CorruptRow(
                "Context Deployment contains the wrong closure".to_owned(),
            ));
        }
    };
    validate_deployment_closure_exists(
        transaction,
        tenant_id,
        &DeploymentClosure::ContextSourceInterface(closure.clone()),
    )
    .await?;
    let ContextBackendBinding::McpResources {
        mcp_deployment,
        discovery_snapshot_id,
        discovery_snapshot_digest,
    } = &closure.backend
    else {
        return Err(RepositoryError::Conflict(
            "Context Deployment is not MCP-backed",
        ));
    };
    if mcp_deployment != &contract.deployment
        || discovery_snapshot_id != &contract.discovery.snapshot_id
        || discovery_snapshot_digest != &contract.discovery.canonical_digest
    {
        return Err(RepositoryError::Conflict("Context Deployment MCP binding"));
    }
    let implementation = crate::invocation_repository::load_enabled_exact_published_version(
        transaction,
        tenant_id,
        &closure.implementation,
        RegistryResourceKind::ContextSourceImplementation,
    )
    .await?;
    let ResourceDocument::ContextSourceImplementation(implementation) = implementation.document
    else {
        return Err(RepositoryError::CorruptRow(
            "Context Implementation revision contains the wrong document".to_owned(),
        ));
    };
    let ContextBackendContract::McpResources { uri_policy, .. } = &implementation.contract.backend
    else {
        return Err(RepositoryError::Conflict(
            "Context Implementation MCP contract",
        ));
    };
    if implementation.interface_revision != closure.interface
        || implementation.backend_kind != implementation.contract.backend.kind()
    {
        return Err(RepositoryError::Conflict(
            "Context Implementation MCP binding",
        ));
    }
    let uri_policy = crate::invocation_repository::load_enabled_exact_published_version(
        transaction,
        tenant_id,
        uri_policy,
        RegistryResourceKind::Policy,
    )
    .await?;
    if !matches!(uri_policy.document, ResourceDocument::Policy(_)) {
        return Err(RepositoryError::CorruptRow(
            "Context URI Policy revision contains the wrong document".to_owned(),
        ));
    }
    Ok(closure.required_worker_manifest_digest)
}

fn require_same_subscription_base(
    binding: &McpResourceSubscriptionBinding,
    base: &ResolvedMcpBaseExecutionContract,
) -> Result<(), RepositoryError> {
    if base.deployment_closure.protocol_policy != binding.protocol_profile
        || base.authorization.authorization_binding_id != binding.authorization_binding_id
        || base.authorization.generation != binding.authorization_generation
        || base.authorization.canonical_digest != binding.authorization_context_digest
        || base.authorization.scope_digest != binding.scope_digest
        || base.authorization.principal_kind != binding.principal_kind
        || base.authorization.principal_id != binding.principal_id
        || base.authorization.principal_identity_kind != binding.principal_identity_kind
        || base.authorization.principal_binding_generation != binding.principal_binding_generation
        || base.deployment_closure.server_identity_digest != binding.server_identity_digest
        || base.deployment_closure.transport.kind() != binding.transport_kind
        || insight_platform_contracts::canonical_digest(
            &serde_json::to_value(&base.deployment_closure.transport)
                .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?,
        )
        .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?
            != binding.transport_binding_digest.to_string()
        || !base.protocol_profile.allowed_server_capabilities.resources
        || !base
            .protocol_profile
            .allowed_server_capabilities
            .subscriptions
    {
        return Err(RepositoryError::Conflict(
            "MCP subscription execution closure",
        ));
    }
    Ok(())
}

async fn lock_mcp_subscription_capacity(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    authorization_binding_id: &ResourceId,
) -> Result<(), RepositoryError> {
    let key = format!("mcp-subscription:{tenant_id}:{authorization_binding_id}");
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
        .bind(key)
        .execute(&mut **transaction)
        .await?;
    Ok(())
}

fn mcp_subscription_session_receipt_payload(
    command: &SaveMcpSubscriptionSession,
) -> Result<TypedPayload, RepositoryError> {
    let request_digest = command.request_digest().map_err(invalid_mcp_subscription)?;
    if request_digest != command.audit.request_digest {
        return Err(RepositoryError::InvalidInput(
            "MCP subscription session request digest mismatch".to_owned(),
        ));
    }
    TypedPayload::with_limit(
        1,
        &serde_json::json!({
            "expected_session_version": command.expected_session_version,
            "expected_subscription_version": command.expected_subscription_version,
            "expires_at": command.expires_at,
            "fence": {
                "expected_version": command.fence.expected_version,
                "lease_generation": command.fence.lease_generation,
                "lease_token_digest": command.fence.token_digest,
                "worker_process_generation_id": command.fence.worker_process_generation_id,
            },
            "job_id": command.job_id,
            "opaque_state_digest": command.encrypted_opaque_session.as_ref().map(|value| &value.plaintext_digest),
            "phase_evidence_digest": command.phase_evidence_digest,
            "subscription_id": command.subscription_id,
            "target": command.target,
        }),
        65_536,
    )
}

#[cfg(any())]
fn managed_mcp_sandbox_session_receipt_payload(
    command: &AcceptManagedMcpSandboxSession,
) -> Result<TypedPayload, RepositoryError> {
    if command.audit.request_digest != command.request.request_digest {
        return Err(RepositoryError::InvalidInput(
            "Managed MCP Sandbox Session request digest mismatch".to_owned(),
        ));
    }
    TypedPayload::with_limit(
        1,
        &serde_json::json!({
            "logical_fence": {
                "expected_version": command.logical_fence.expected_version,
                "lease_generation": command.logical_fence.lease_generation,
                "token_digest": command.logical_fence.token_digest,
                "worker_process_generation_id": command.logical_fence.worker_process_generation_id,
            },
            "quota_entry_ids": command.quota_entry_ids,
            "request_digest": command.request.request_digest,
            "session_identity": command.request.identity,
            "usage_reservation_id": command.usage_reservation_id,
        }),
        131_072,
    )
}

fn mcp_subscription_refresh_receipt_payload(
    command: &CompleteMcpSubscriptionRefresh,
) -> Result<TypedPayload, RepositoryError> {
    let request_digest = command.request_digest().map_err(invalid_mcp_subscription)?;
    if request_digest != command.audit.request_digest {
        return Err(RepositoryError::InvalidInput(
            "MCP subscription refresh request digest mismatch".to_owned(),
        ));
    }
    TypedPayload::with_limit(
        1,
        &serde_json::json!({
            "event_generation": command.expected_event_generation,
            "expected_subscription_version": command.expected_subscription_version,
            "fence": {
                "expected_version": command.fence.expected_version,
                "lease_generation": command.fence.lease_generation,
                "lease_token_digest": command.fence.token_digest,
                "worker_process_generation_id": command.fence.worker_process_generation_id,
            },
            "job_id": command.job_id,
            "refresh_evidence_digest": command.refresh_evidence_digest,
            "session_generation": command.expected_session_generation,
            "subscription_id": command.subscription_id,
        }),
        65_536,
    )
}

fn mcp_subscription_reconcile_receipt_payload(
    command: &CompleteMcpSubscriptionReconcile,
) -> Result<TypedPayload, RepositoryError> {
    let request_digest = command.request_digest().map_err(invalid_mcp_subscription)?;
    if request_digest != command.audit.request_digest {
        return Err(RepositoryError::InvalidInput(
            "MCP subscription reconcile request digest mismatch".to_owned(),
        ));
    }
    TypedPayload::with_limit(
        1,
        &serde_json::json!({
            "expected_subscription_version": command.expected_subscription_version,
            "fence": {
                "expected_version": command.fence.expected_version,
                "lease_generation": command.fence.lease_generation,
                "lease_token_digest": command.fence.token_digest,
                "worker_process_generation_id": command.fence.worker_process_generation_id,
            },
            "job_id": command.job_id,
            "reconcile_evidence_digest": command.reconcile_evidence_digest,
            "session_generation": command.expected_session_generation,
            "subscription_id": command.subscription_id,
        }),
        65_536,
    )
}

fn mcp_subscription_reconcile_wake_receipt_payload(
    command: &WakeMcpSubscriptionReconcile,
) -> Result<TypedPayload, RepositoryError> {
    let request_digest = command.request_digest().map_err(invalid_mcp_subscription)?;
    if request_digest != command.audit.request_digest {
        return Err(RepositoryError::InvalidInput(
            "MCP subscription reconcile wake request digest mismatch".to_owned(),
        ));
    }
    TypedPayload::with_limit(
        1,
        &serde_json::json!({
            "job_id": command.candidate.job_id,
            "job_version": command.candidate.job_version,
            "not_updated_after": command.candidate.not_updated_after,
            "observed_at": command.candidate.observed_at,
            "session_generation": command.candidate.session_generation,
            "subscription_id": command.candidate.subscription_id,
            "subscription_version": command.candidate.subscription_version,
        }),
        65_536,
    )
}

fn mcp_subscription_recovery_receipt_payload(
    command: &RecoverDueMcpSubscription,
) -> Result<TypedPayload, RepositoryError> {
    let request_digest = command.request_digest().map_err(invalid_mcp_subscription)?;
    if request_digest != command.audit.request_digest {
        return Err(RepositoryError::InvalidInput(
            "MCP subscription recovery request digest mismatch".to_owned(),
        ));
    }
    TypedPayload::with_limit(
        1,
        &serde_json::json!({
            "candidate": command.candidate,
            "worker_process_generation_id": command.audit.worker_process_generation_id,
        }),
        65_536,
    )
}

fn mcp_subscription_session_loss_receipt_payload(
    command: &ReportMcpSubscriptionSessionLoss,
) -> Result<TypedPayload, RepositoryError> {
    let request_digest = command.request_digest().map_err(invalid_mcp_subscription)?;
    if request_digest != command.audit.request_digest {
        return Err(RepositoryError::InvalidInput(
            "MCP subscription session-loss request digest mismatch".to_owned(),
        ));
    }
    TypedPayload::with_limit(
        1,
        &serde_json::json!({
            "expected_session_generation": command.expected_session_generation,
            "expected_session_version": command.expected_session_version,
            "expected_subscription_version": command.expected_subscription_version,
            "job_id": command.job_id,
            "reported_at": command.reported_at,
            "session_loss_evidence_digest": command.session_loss_evidence_digest,
            "subscription_id": command.subscription_id,
            "worker_process_generation_id": command.audit.worker_process_generation_id,
        }),
        65_536,
    )
}

fn mcp_subscription_transport_termination_receipt_payload(
    command: &ReportMcpSubscriptionTransportTermination,
    job_id: &ResourceId,
) -> Result<TypedPayload, RepositoryError> {
    let request_digest = command.request_digest().map_err(invalid_mcp_subscription)?;
    if request_digest != command.audit.request_digest {
        return Err(RepositoryError::InvalidInput(
            "MCP subscription transport-termination request digest mismatch".to_owned(),
        ));
    }
    TypedPayload::with_limit(
        1,
        &serde_json::json!({
            "expected_authorization_generation": command.expected_authorization_generation,
            "expected_session_generation": command.expected_session_generation,
            "job_id": job_id,
            "reported_at": command.reported_at,
            "session_loss_evidence_digest": command.session_loss_evidence_digest,
            "subscription_id": command.subscription_id,
            "worker_process_generation_id": command.audit.worker_process_generation_id,
        }),
        65_536,
    )
}

async fn claim_mcp_subscription_worker_receipt(
    transaction: &mut Transaction<'_, Postgres>,
    audit: &McpSubscriptionWorkerAudit,
    job_id: &ResourceId,
    operation: &str,
    payload: &TypedPayload,
) -> Result<bool, RepositoryError> {
    let inserted = sqlx::query(
        r#"
        INSERT INTO insight_platform.receipts (
            tenant_id, receipt_id, receipt_kind, scope_kind, scope_id,
            dedupe_owner_id, operation, idempotency_key_digest, request_digest,
            state, payload_schema_version, payload, payload_digest, expires_at
        ) VALUES ($1, $2, 'job_commit', 'job', $3, $4, $5, $6, $7,
                  'processing', $8, $9, $10, $11)
        ON CONFLICT (
            tenant_id, receipt_kind, scope_kind, scope_id, dedupe_owner_id,
            operation, idempotency_key_digest
        ) DO NOTHING
        RETURNING receipt_id
        "#,
    )
    .bind(audit.tenant_id.to_string())
    .bind(audit.receipt_id.to_string())
    .bind(job_id.to_string())
    .bind(audit.worker_process_generation_id.to_string())
    .bind(operation)
    .bind(audit.idempotency_key_digest.to_string())
    .bind(audit.request_digest.to_string())
    .bind(payload.schema_version)
    .bind(&payload.value)
    .bind(&payload.digest)
    .bind(audit.receipt_expires_at)
    .fetch_optional(&mut **transaction)
    .await?;
    if inserted.is_some() {
        return Ok(false);
    }
    let existing = sqlx::query(
        r#"
        SELECT request_digest, payload_digest, state
        FROM insight_platform.receipts
        WHERE tenant_id = $1 AND receipt_kind = 'job_commit'
          AND scope_kind = 'job' AND scope_id = $2 AND dedupe_owner_id = $3
          AND operation = $4 AND idempotency_key_digest = $5
        FOR UPDATE
        "#,
    )
    .bind(audit.tenant_id.to_string())
    .bind(job_id.to_string())
    .bind(audit.worker_process_generation_id.to_string())
    .bind(operation)
    .bind(audit.idempotency_key_digest.to_string())
    .fetch_one(&mut **transaction)
    .await?;
    if existing.try_get::<String, _>("request_digest")? != audit.request_digest.to_string()
        || existing.try_get::<String, _>("payload_digest")? != payload.digest
    {
        return Err(RepositoryError::IdempotencyConflict);
    }
    if existing.try_get::<String, _>("state")? != "succeeded" {
        return Err(RepositoryError::Conflict("MCP subscription worker receipt"));
    }
    Ok(true)
}

async fn terminalize_mcp_subscription_worker_receipt(
    transaction: &mut Transaction<'_, Postgres>,
    audit: &McpSubscriptionWorkerAudit,
    job_id: &ResourceId,
    disposition: &str,
    response_reference_id: &ResourceId,
) -> Result<(), RepositoryError> {
    let affected = sqlx::query(
        r#"
        UPDATE insight_platform.receipts
        SET state = 'succeeded', disposition = $4, response_reference_id = $5,
            completed_at = clock_timestamp()
        WHERE tenant_id = $1 AND receipt_id = $2 AND request_digest = $3
          AND scope_id = $6 AND state = 'processing'
        "#,
    )
    .bind(audit.tenant_id.to_string())
    .bind(audit.receipt_id.to_string())
    .bind(audit.request_digest.to_string())
    .bind(disposition)
    .bind(response_reference_id.to_string())
    .bind(job_id.to_string())
    .execute(&mut **transaction)
    .await?
    .rows_affected();
    if affected != 1 {
        return Err(RepositoryError::Conflict("MCP subscription worker receipt"));
    }
    Ok(())
}

fn mcp_notification_receipt_payload(
    command: &McpNotificationCommit,
) -> Result<TypedPayload, RepositoryError> {
    TypedPayload::with_limit(
        1,
        &serde_json::json!({
            "authorization_generation": command.authorization_generation,
            "body_digest": command.body_digest,
            "class": command.class,
            "event_generation": command.event_generation,
            "event_key_digest": command.event_key_digest,
            "resource_uri_digest": command.resource_uri_digest,
            "session_generation": command.session_generation,
            "subscription_id": command.subscription_id,
            "wire_bytes": command.wire_bytes,
        }),
        65_536,
    )
}

async fn claim_mcp_notification_receipt(
    transaction: &mut Transaction<'_, Postgres>,
    command: &McpNotificationCommit,
    request_digest: &Sha256Digest,
    payload: &TypedPayload,
) -> Result<Option<McpNotificationApplyDisposition>, RepositoryError> {
    let inserted = sqlx::query(
        r#"
        INSERT INTO insight_platform.receipts (
            tenant_id, receipt_id, receipt_kind, scope_kind, scope_id,
            dedupe_owner_id, operation, idempotency_key_digest, request_digest,
            state, payload_schema_version, payload, payload_digest, expires_at
        ) VALUES ($1, $2, 'callback', 'mcp_subscription', $3, $3,
                  'mcp.notification', $4, $5, 'processing', $6, $7, $8, $9)
        ON CONFLICT (
            tenant_id, receipt_kind, scope_kind, scope_id, dedupe_owner_id,
            operation, idempotency_key_digest
        ) DO NOTHING
        RETURNING receipt_id
        "#,
    )
    .bind(command.tenant_id.to_string())
    .bind(command.audit.receipt_id.to_string())
    .bind(command.subscription_id.to_string())
    .bind(command.event_key_digest.to_string())
    .bind(request_digest.to_string())
    .bind(payload.schema_version)
    .bind(&payload.value)
    .bind(&payload.digest)
    .bind(command.audit.receipt_expires_at)
    .fetch_optional(&mut **transaction)
    .await?;
    if inserted.is_some() {
        return Ok(None);
    }
    let existing = sqlx::query(
        r#"
        SELECT request_digest, payload_digest, state, disposition
        FROM insight_platform.receipts
        WHERE tenant_id = $1 AND receipt_kind = 'callback'
          AND scope_kind = 'mcp_subscription' AND scope_id = $2 AND dedupe_owner_id = $2
          AND operation = 'mcp.notification' AND idempotency_key_digest = $3
        FOR UPDATE
        "#,
    )
    .bind(command.tenant_id.to_string())
    .bind(command.subscription_id.to_string())
    .bind(command.event_key_digest.to_string())
    .fetch_one(&mut **transaction)
    .await?;
    if existing.try_get::<String, _>("request_digest")? != request_digest.to_string()
        || existing.try_get::<String, _>("payload_digest")? != payload.digest
    {
        return Err(RepositoryError::IdempotencyConflict);
    }
    if existing.try_get::<String, _>("state")? != "succeeded" {
        return Err(RepositoryError::Conflict("MCP notification receipt"));
    }
    let disposition = match existing.try_get::<String, _>("disposition")?.as_str() {
        "wake" => McpNotificationApplyDisposition::Wake,
        "coalesced" => McpNotificationApplyDisposition::Coalesced,
        "stale" => McpNotificationApplyDisposition::Stale,
        _ => {
            return Err(RepositoryError::CorruptRow(
                "MCP notification receipt has an unknown disposition".to_owned(),
            ));
        }
    };
    Ok(Some(disposition))
}

async fn terminalize_mcp_notification_receipt(
    transaction: &mut Transaction<'_, Postgres>,
    command: &McpNotificationCommit,
    request_digest: &Sha256Digest,
    disposition: McpNotificationApplyDisposition,
) -> Result<(), RepositoryError> {
    let affected = sqlx::query(
        r#"
        UPDATE insight_platform.receipts
        SET state = 'succeeded', disposition = $4, response_reference_id = $5,
            completed_at = clock_timestamp()
        WHERE tenant_id = $1 AND receipt_id = $2 AND request_digest = $3
          AND scope_kind = 'mcp_subscription' AND scope_id = $5 AND state = 'processing'
        "#,
    )
    .bind(command.tenant_id.to_string())
    .bind(command.audit.receipt_id.to_string())
    .bind(request_digest.to_string())
    .bind(disposition.as_str())
    .bind(command.subscription_id.to_string())
    .execute(&mut **transaction)
    .await?
    .rows_affected();
    if affected != 1 {
        return Err(RepositoryError::Conflict("MCP notification receipt"));
    }
    Ok(())
}

fn invalid_mcp_subscription(failure: insight_platform_mcp_host::McpHostError) -> RepositoryError {
    RepositoryError::InvalidInput(failure.to_string())
}
