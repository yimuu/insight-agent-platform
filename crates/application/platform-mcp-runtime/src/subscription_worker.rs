use async_trait::async_trait;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use futures::FutureExt;
use insight_platform_context::{
    AdmitContextSubscriptionRefresh, ContextSubscriptionAdmissionAudit,
    ContextSubscriptionAdmissionAuthority, ContextSubscriptionAdmissionError,
    ContextSubscriptionRefreshCause, ContextSubscriptionRefreshRequest,
    CONTEXT_SUBSCRIPTION_ADMISSION_SCHEMA_VERSION,
};
use insight_platform_contracts::{
    CommandOutcome, McpSessionState, McpTransportKind, ResourceId, ResourceKind, Sha256Digest,
};
use insight_platform_jobs::JobFence;
use std::{collections::BTreeSet, panic::AssertUnwindSafe, sync::Arc, time::Duration};
use tokio::sync::Semaphore;
use uuid::Uuid;

use insight_platform_mcp_host::*;

/// Production adapter from the MCP worker contract to the durable Context
/// application owner. The MCP process supplies no Job identity or work digest.
pub struct ContextSubscriptionInvalidationTarget<A: ?Sized> {
    authority: Arc<A>,
}

#[async_trait]
impl<A> McpSubscriptionInvalidationTarget for ContextSubscriptionInvalidationTarget<A>
where
    A: ContextSubscriptionAdmissionAuthority + ?Sized,
{
    async fn accept_invalidation(
        &self,
        request: McpSubscriptionInvalidationRequest,
    ) -> Result<AcceptedMcpSubscriptionInvalidation, McpSubscriptionInvalidationError> {
        let mcp_request_digest = request.request_digest.clone();
        let command = context_invalidation_command(request)?;
        let accepted = self
            .authority
            .admit_context_subscription_refresh(command)
            .await
            .map_err(map_context_subscription_error)?;
        Ok(AcceptedMcpSubscriptionInvalidation {
            request_digest: mcp_request_digest,
            durable_work_digest: accepted.durable_work_digest,
            accepted_at: accepted.accepted_at,
        })
    }

    async fn accept_reconcile(
        &self,
        request: McpSubscriptionReconcileRequest,
    ) -> Result<AcceptedMcpSubscriptionInvalidation, McpSubscriptionInvalidationError> {
        let mcp_request_digest = request.request_digest.clone();
        let command = context_reconcile_command(request)?;
        let accepted = self
            .authority
            .admit_context_subscription_refresh(command)
            .await
            .map_err(map_context_subscription_error)?;
        Ok(AcceptedMcpSubscriptionInvalidation {
            request_digest: mcp_request_digest,
            durable_work_digest: accepted.durable_work_digest,
            accepted_at: accepted.accepted_at,
        })
    }
}

fn context_invalidation_command(
    request: McpSubscriptionInvalidationRequest,
) -> Result<AdmitContextSubscriptionRefresh, McpSubscriptionInvalidationError> {
    let cause = match request.reason {
        McpSubscriptionRefreshReason::ResourceUpdated {
            resource_uri,
            resource_uri_digest,
        } if resource_uri == request.resource_uri
            && resource_uri_digest == request.resource_uri_digest =>
        {
            ContextSubscriptionRefreshCause::ResourceUpdated
        }
        McpSubscriptionRefreshReason::ResourceUpdated { .. } => {
            return Err(McpSubscriptionInvalidationError::Rejected);
        }
        McpSubscriptionRefreshReason::ResourceListChanged => {
            ContextSubscriptionRefreshCause::ResourceListChanged
        }
        McpSubscriptionRefreshReason::ToolListChanged => {
            ContextSubscriptionRefreshCause::ToolListChanged
        }
        McpSubscriptionRefreshReason::PromptListChanged => {
            ContextSubscriptionRefreshCause::PromptListChanged
        }
    };
    context_admission_command(
        request.tenant_id,
        request.subscription_id,
        request.context_deployment,
        request.mcp_deployment,
        request.discovery_snapshot_id,
        request.discovery_snapshot_digest,
        request.resource_uri,
        request.resource_uri_digest,
        request.authorization_generation,
        request.session_generation,
        request.event_generation,
        request.event_key_digest,
        request.body_digest,
        cause,
        request.deadline,
        request.request_digest,
    )
}

fn context_reconcile_command(
    request: McpSubscriptionReconcileRequest,
) -> Result<AdmitContextSubscriptionRefresh, McpSubscriptionInvalidationError> {
    context_admission_command(
        request.tenant_id,
        request.subscription_id,
        request.context_deployment,
        request.mcp_deployment,
        request.discovery_snapshot_id,
        request.discovery_snapshot_digest.clone(),
        request.resource_uri,
        request.resource_uri_digest.clone(),
        request.authorization_generation,
        request.session_generation,
        request.observed_subscription_version,
        request.resource_uri_digest,
        request.discovery_snapshot_digest,
        ContextSubscriptionRefreshCause::FullReconcile {
            observed_subscription_version: request.observed_subscription_version,
        },
        request.deadline,
        request.request_digest,
    )
}

#[allow(clippy::too_many_arguments)]
fn context_admission_command(
    tenant_id: ResourceId,
    subscription_id: ResourceId,
    context_deployment: insight_platform_contracts::ExactDeploymentRef,
    mcp_deployment: insight_platform_contracts::ExactDeploymentRef,
    discovery_snapshot_id: ResourceId,
    discovery_snapshot_digest: Sha256Digest,
    resource_uri: String,
    resource_uri_digest: Sha256Digest,
    authorization_generation: u64,
    session_generation: u64,
    event_generation: u64,
    event_key_digest: Sha256Digest,
    body_digest: Sha256Digest,
    cause: ContextSubscriptionRefreshCause,
    deadline: DateTime<Utc>,
    correlation_digest: Sha256Digest,
) -> Result<AdmitContextSubscriptionRefresh, McpSubscriptionInvalidationError> {
    let mut request = ContextSubscriptionRefreshRequest {
        schema_version: CONTEXT_SUBSCRIPTION_ADMISSION_SCHEMA_VERSION,
        tenant_id,
        subscription_id,
        context_deployment,
        mcp_deployment,
        discovery_snapshot_id,
        discovery_snapshot_digest,
        resource_uri,
        resource_uri_digest,
        authorization_generation,
        session_generation,
        event_generation,
        event_key_digest,
        body_digest,
        cause,
        deadline,
        request_digest: static_digest("context_subscription_admission_placeholder"),
    };
    request.request_digest = request
        .canonical_request_digest()
        .map_err(|_| McpSubscriptionInvalidationError::Rejected)?;
    let request_id = ResourceId::from_uuid_v7(ResourceKind::ServerRequest, Uuid::now_v7())
        .map_err(|_| McpSubscriptionInvalidationError::Unavailable)?;
    let command = AdmitContextSubscriptionRefresh {
        request,
        audit: ContextSubscriptionAdmissionAudit {
            schema_version: CONTEXT_SUBSCRIPTION_ADMISSION_SCHEMA_VERSION,
            request_id,
            correlation_digest,
            trace: insight_platform_contracts::TraceIdentityV1::generate(),
        },
    };
    command
        .validate_at(Utc::now())
        .map_err(|_| McpSubscriptionInvalidationError::Rejected)?;
    Ok(command)
}

fn map_context_subscription_error(
    failure: ContextSubscriptionAdmissionError,
) -> McpSubscriptionInvalidationError {
    match failure {
        ContextSubscriptionAdmissionError::Unavailable => {
            McpSubscriptionInvalidationError::Unavailable
        }
        ContextSubscriptionAdmissionError::CommitUncertain => {
            McpSubscriptionInvalidationError::CommitUncertain
        }
        ContextSubscriptionAdmissionError::InvalidRequest
        | ContextSubscriptionAdmissionError::InvalidAudit
        | ContextSubscriptionAdmissionError::InvalidJobPayload
        | ContextSubscriptionAdmissionError::InvalidAcceptance
        | ContextSubscriptionAdmissionError::Rejected
        | ContextSubscriptionAdmissionError::Canonicalization => {
            McpSubscriptionInvalidationError::Rejected
        }
    }
}

pub struct McpSubscriptionRecoveryDriver {
    authority: Arc<dyn McpSubscriptionRecoveryAuthority>,
    permits: Arc<Semaphore>,
}

impl McpSubscriptionRecoveryDriver {
    pub fn new(
        authority: Arc<dyn McpSubscriptionRecoveryAuthority>,
        maximum_concurrent_scans: usize,
    ) -> Result<Self, McpHostError> {
        if maximum_concurrent_scans == 0 || maximum_concurrent_scans > 64 {
            return Err(McpHostError::InvalidSubscription);
        }
        Ok(Self {
            authority,
            permits: Arc::new(Semaphore::new(maximum_concurrent_scans)),
        })
    }

    pub async fn drive(
        &self,
        command: DriveMcpSubscriptionRecoveries,
    ) -> Result<McpSubscriptionRecoveryDriveOutcome, McpSubscriptionReconcileDriverError> {
        command
            .validate_at(Utc::now())
            .map_err(|_| McpSubscriptionReconcileDriverError::InvalidCommand)?;
        let _permit = Arc::clone(&self.permits)
            .try_acquire_owned()
            .map_err(|_| McpSubscriptionReconcileDriverError::Saturated)?;
        let tenant_id = command.scan.tenant_id.clone();
        let candidates = self
            .authority
            .list_due_recoveries(command.scan)
            .await
            .map_err(McpSubscriptionReconcileDriverError::Persistence)?;
        let page = candidates;
        let candidates = page.records;
        if candidates.len() > command.audits.len() {
            return Err(McpSubscriptionReconcileDriverError::InvalidCommand);
        }
        let mut observed_subscriptions = BTreeSet::new();
        for candidate in &candidates {
            candidate
                .validate()
                .map_err(|_| McpSubscriptionReconcileDriverError::InvalidCommand)?;
            if candidate.tenant_id != tenant_id
                || !observed_subscriptions
                    .insert((candidate.subscription_id.clone(), candidate.job_id.clone()))
            {
                return Err(McpSubscriptionReconcileDriverError::InvalidCommand);
            }
        }
        let observed = u16::try_from(candidates.len())
            .map_err(|_| McpSubscriptionReconcileDriverError::InvalidCommand)?;
        let mut recovered = 0_u16;
        let mut stale = 0_u16;
        for (candidate, audit) in candidates.into_iter().zip(command.audits) {
            let mut recovery = RecoverDueMcpSubscription { audit, candidate };
            recovery.audit.request_digest = recovery
                .request_digest()
                .map_err(|_| McpSubscriptionReconcileDriverError::InvalidCommand)?;
            match self.authority.recover_due_subscription(recovery).await {
                Ok(_) => recovered = recovered.saturating_add(1),
                Err(McpSubscriptionPersistenceError::Conflict) => {
                    stale = stale.saturating_add(1);
                }
                Err(failure) => {
                    return Err(McpSubscriptionReconcileDriverError::Persistence(failure));
                }
            }
        }
        Ok(McpSubscriptionRecoveryDriveOutcome {
            diagnostics: page.diagnostics,
            next_cursor: page.next_cursor,
            exhausted: page.exhausted,
            observed,
            recovered,
            stale,
        })
    }
}

/// Bounded critical-control driver. It owns a dedicated local permit and issues one durable wake
/// command per observed candidate; notification and request saturation cannot expand the scan.
pub struct McpSubscriptionReconcileDriver {
    authority: Arc<dyn McpSubscriptionReconcileAuthority>,
    permits: Arc<Semaphore>,
}

impl McpSubscriptionReconcileDriver {
    pub fn new(
        authority: Arc<dyn McpSubscriptionReconcileAuthority>,
        maximum_concurrent_scans: usize,
    ) -> Result<Self, McpHostError> {
        if maximum_concurrent_scans == 0 || maximum_concurrent_scans > 64 {
            return Err(McpHostError::InvalidSubscription);
        }
        Ok(Self {
            authority,
            permits: Arc::new(Semaphore::new(maximum_concurrent_scans)),
        })
    }

    pub async fn drive(
        &self,
        command: DriveMcpSubscriptionReconciliations,
    ) -> Result<McpSubscriptionReconcileDriveOutcome, McpSubscriptionReconcileDriverError> {
        command
            .validate_at(Utc::now())
            .map_err(|_| McpSubscriptionReconcileDriverError::InvalidCommand)?;
        let _permit = Arc::clone(&self.permits)
            .try_acquire_owned()
            .map_err(|_| McpSubscriptionReconcileDriverError::Saturated)?;
        let tenant_id = command.scan.tenant_id.clone();
        let candidates = self
            .authority
            .list_due_reconciliations(command.scan)
            .await
            .map_err(McpSubscriptionReconcileDriverError::Persistence)?;
        let page = candidates;
        let candidates = page.records;
        if candidates.len() > command.audits.len() {
            return Err(McpSubscriptionReconcileDriverError::InvalidCommand);
        }
        let mut observed_subscriptions = BTreeSet::new();
        for candidate in &candidates {
            candidate
                .validate()
                .map_err(|_| McpSubscriptionReconcileDriverError::InvalidCommand)?;
            if candidate.tenant_id != tenant_id
                || !observed_subscriptions
                    .insert((candidate.subscription_id.clone(), candidate.job_id.clone()))
            {
                return Err(McpSubscriptionReconcileDriverError::InvalidCommand);
            }
        }
        let observed = u16::try_from(candidates.len())
            .map_err(|_| McpSubscriptionReconcileDriverError::InvalidCommand)?;
        let mut scheduled = 0_u16;
        let mut stale = 0_u16;
        for (candidate, audit) in candidates.into_iter().zip(command.audits) {
            let mut wake = WakeMcpSubscriptionReconcile { audit, candidate };
            wake.audit.request_digest = wake
                .request_digest()
                .map_err(|_| McpSubscriptionReconcileDriverError::InvalidCommand)?;
            match self.authority.wake_reconciliation(wake).await {
                Ok(_) => scheduled = scheduled.saturating_add(1),
                Err(McpSubscriptionPersistenceError::Conflict) => {
                    stale = stale.saturating_add(1);
                }
                Err(failure) => {
                    return Err(McpSubscriptionReconcileDriverError::Persistence(failure));
                }
            }
        }
        Ok(McpSubscriptionReconcileDriveOutcome {
            diagnostics: page.diagnostics,
            next_cursor: page.next_cursor,
            exhausted: page.exhausted,
            observed,
            scheduled,
            stale,
        })
    }
}

struct FixedMcpSubscriptionRemoteIoLease;

#[async_trait]
impl McpSubscriptionRemoteIoLease for FixedMcpSubscriptionRemoteIoLease {
    async fn enter_remote_io(&self, fence: &JobFence) -> Result<JobFence, ()> {
        Ok(fence.clone())
    }

    async fn exit_remote_io(&self, fence: &JobFence) -> Result<JobFence, ()> {
        Ok(fence.clone())
    }
}

pub struct McpSubscriptionWorker {
    resolver: Arc<dyn McpSubscriptionExecutionResolver>,
    transport: Arc<dyn McpSubscriptionTransport>,
    invalidation_target: Arc<dyn McpSubscriptionInvalidationTarget>,
    authority: Arc<dyn McpSubscriptionAuthority>,
}

impl McpSubscriptionWorker {
    pub fn new(
        resolver: Arc<dyn McpSubscriptionExecutionResolver>,
        transport: Arc<dyn McpSubscriptionTransport>,
        invalidation_target: Arc<dyn McpSubscriptionInvalidationTarget>,
        authority: Arc<dyn McpSubscriptionAuthority>,
    ) -> Self {
        Self {
            resolver,
            transport,
            invalidation_target,
            authority,
        }
    }

    pub async fn execute(
        &self,
        command: ExecuteMcpSubscriptionJob,
    ) -> Result<McpSubscriptionWorkerResult, McpSubscriptionWorkerError> {
        self.execute_with_remote_io_lease(command, Arc::new(FixedMcpSubscriptionRemoteIoLease))
            .await
    }

    pub async fn execute_with_remote_io_lease(
        &self,
        command: ExecuteMcpSubscriptionJob,
        remote_io_lease: Arc<dyn McpSubscriptionRemoteIoLease>,
    ) -> Result<McpSubscriptionWorkerResult, McpSubscriptionWorkerError> {
        let started_at = Utc::now();
        command
            .validate_at(started_at)
            .map_err(|_| McpSubscriptionWorkerError::InvalidCommand)?;
        let resolved = self
            .resolver
            .resolve_mcp_subscription_execution(&command.query)
            .await
            .map_err(McpSubscriptionWorkerError::Contract)?;
        resolved
            .validate_for(&command.query, Utc::now())
            .map_err(McpSubscriptionWorkerError::Contract)?;
        if resolved.contract.transport_kind() != McpTransportKind::StreamableHttp
            || self.transport.kind() != McpTransportKind::StreamableHttp
        {
            return Err(McpSubscriptionWorkerError::InvalidCommand);
        }

        if resolved.record.payload.pending_invalidation.is_some() {
            return self.refresh(command, resolved).await;
        }
        if resolved.record.state == McpSubscriptionState::Active
            && matches!(
                resolved.record.payload.session.state,
                McpSessionState::Ready | McpSessionState::Degraded
            )
        {
            return self.reconcile(command, resolved).await;
        }
        self.establish(command, resolved, remote_io_lease).await
    }

    async fn establish(
        &self,
        command: ExecuteMcpSubscriptionJob,
        resolved: ResolvedMcpSubscriptionExecution,
        remote_io_lease: Arc<dyn McpSubscriptionRemoteIoLease>,
    ) -> Result<McpSubscriptionWorkerResult, McpSubscriptionWorkerError> {
        let mut record = resolved.record;
        let mut fence = command.query.fence;
        match record.payload.session.state {
            McpSessionState::Disconnected => {
                let outcome = self
                    .save_session_phase(
                        &record,
                        &fence,
                        command.audits.connecting.clone(),
                        McpSessionState::Connecting,
                        None,
                        None,
                    )
                    .await?;
                record = outcome_record(&outcome).clone();
                fence = next_fence(&fence)?;
                let outcome = self
                    .save_session_phase(
                        &record,
                        &fence,
                        command.audits.initializing.clone(),
                        McpSessionState::Initializing,
                        None,
                        None,
                    )
                    .await?;
                record = outcome_record(&outcome).clone();
                fence = next_fence(&fence)?;
            }
            McpSessionState::Connecting => {
                let outcome = self
                    .save_session_phase(
                        &record,
                        &fence,
                        command.audits.initializing.clone(),
                        McpSessionState::Initializing,
                        None,
                        None,
                    )
                    .await?;
                record = outcome_record(&outcome).clone();
                fence = next_fence(&fence)?;
            }
            McpSessionState::Initializing => {}
            _ => return Err(McpSubscriptionWorkerError::InvalidCommand),
        }

        fence = refreshed_subscription_fence(
            &fence,
            remote_io_lease
                .enter_remote_io(&fence)
                .await
                .map_err(|()| McpSubscriptionWorkerError::LeaseCoordination)?,
        )?;
        let now = Utc::now();
        let remaining = u64::try_from((record.deadline - now).num_milliseconds())
            .map_err(|_| McpSubscriptionWorkerError::InvalidCommand)?;
        let timeout_milliseconds =
            remaining.min(resolved.contract.server.limits.total_timeout_milliseconds);
        let future = AssertUnwindSafe(self.transport.establish(
            &resolved.contract,
            &record.payload.binding,
            record.payload.session.generation,
            &fence.worker_process_generation_id,
            record.deadline,
        ))
        .catch_unwind();
        let transport_outcome =
            tokio::time::timeout(Duration::from_millis(timeout_milliseconds), future).await;
        fence = refreshed_subscription_fence(
            &fence,
            remote_io_lease
                .exit_remote_io(&fence)
                .await
                .map_err(|()| McpSubscriptionWorkerError::LeaseCoordination)?,
        )?;
        let prepared = match transport_outcome {
            Ok(Ok(Ok(prepared))) => prepared,
            Ok(Ok(Err(failure))) => {
                return self
                    .handle_establish_failure(record, fence, command.audits.terminal, failure)
                    .await;
            }
            Ok(Err(_)) => {
                let failure = retryable_failure("mcp_subscription_transport_panic");
                return Err(McpSubscriptionWorkerError::Transport(failure));
            }
            Err(_) => {
                let failure = retryable_failure("mcp_subscription_transport_timeout");
                return Err(McpSubscriptionWorkerError::Transport(failure));
            }
        };
        prepared
            .established
            .validate_for(&record.payload.binding, &resolved.contract, Utc::now())
            .map_err(|_| McpSubscriptionWorkerError::InvalidCommand)?;
        let reconcile_audit = command.audits.refresh.clone();
        let ready = self
            .save_session_phase(
                &record,
                &fence,
                command.audits.ready,
                McpSessionState::Ready,
                Some((
                    prepared.established.encrypted_opaque_session.clone(),
                    prepared.established.expires_at,
                )),
                Some(prepared.established.evidence_digest.clone()),
            )
            .await?;
        prepared.activate().await;
        let ready_record = outcome_record(&ready);
        if ready_record.payload.full_reconcile_required {
            return self
                .reconcile_record(ready_record.clone(), next_fence(&fence)?, reconcile_audit)
                .await;
        }
        Ok(McpSubscriptionWorkerResult::Established(ready))
    }

    async fn handle_establish_failure(
        &self,
        record: McpSubscriptionRecord,
        fence: JobFence,
        audit: McpSubscriptionWorkerAudit,
        failure: McpTransportFailure,
    ) -> Result<McpSubscriptionWorkerResult, McpSubscriptionWorkerError> {
        failure
            .validate_wire_shape()
            .map_err(|_| McpSubscriptionWorkerError::InvalidCommand)?;
        let terminal = match &failure {
            McpTransportFailure::ReauthorizationRequired { challenge_digest } => {
                Some((McpSessionState::ReauthRequired, challenge_digest.clone()))
            }
            McpTransportFailure::RejectedBeforeDispatch(failure)
            | McpTransportFailure::Permanent(failure) => {
                Some((McpSessionState::Failed, failure.evidence_digest.clone()))
            }
            McpTransportFailure::RetryableBeforeDispatch(_)
            | McpTransportFailure::PostDispatchUncertain { .. } => None,
        };
        let Some((target, phase_evidence_digest)) = terminal else {
            return Err(McpSubscriptionWorkerError::Transport(failure));
        };
        let terminal = self
            .save_session_phase(
                &record,
                &fence,
                audit,
                target,
                None,
                Some(phase_evidence_digest),
            )
            .await?;
        Ok(McpSubscriptionWorkerResult::Terminalized(terminal))
    }

    async fn refresh(
        &self,
        command: ExecuteMcpSubscriptionJob,
        resolved: ResolvedMcpSubscriptionExecution,
    ) -> Result<McpSubscriptionWorkerResult, McpSubscriptionWorkerError> {
        if !matches!(resolved.record.state, McpSubscriptionState::Active)
            || !matches!(
                resolved.record.payload.session.state,
                McpSessionState::Ready | McpSessionState::Degraded
            )
        {
            return Err(McpSubscriptionWorkerError::InvalidCommand);
        }
        let request = McpSubscriptionInvalidationRequest::build(&resolved.record)
            .map_err(|_| McpSubscriptionWorkerError::InvalidCommand)?;
        let accepted = self
            .invalidation_target
            .accept_invalidation(request.clone())
            .await
            .map_err(McpSubscriptionWorkerError::Invalidation)?;
        accepted
            .validate_for(&request, Utc::now())
            .map_err(|_| McpSubscriptionWorkerError::InvalidCommand)?;
        let pending = resolved
            .record
            .payload
            .pending_invalidation
            .as_ref()
            .ok_or(McpSubscriptionWorkerError::InvalidCommand)?;
        let mut completion = CompleteMcpSubscriptionRefresh {
            audit: command.audits.refresh,
            subscription_id: resolved.record.subscription_id.clone(),
            job_id: resolved.record.job_id.clone(),
            fence: command.query.fence,
            expected_subscription_version: resolved.record.version,
            expected_session_generation: pending.session_generation,
            expected_event_generation: pending.event_generation,
            refresh_evidence_digest: accepted.durable_work_digest,
        };
        completion.audit.request_digest = completion
            .request_digest()
            .map_err(|_| McpSubscriptionWorkerError::InvalidCommand)?;
        let outcome = self
            .authority
            .complete_subscription_refresh(completion)
            .await
            .map_err(McpSubscriptionWorkerError::Persistence)?;
        Ok(McpSubscriptionWorkerResult::RefreshAccepted(outcome))
    }

    async fn reconcile(
        &self,
        command: ExecuteMcpSubscriptionJob,
        resolved: ResolvedMcpSubscriptionExecution,
    ) -> Result<McpSubscriptionWorkerResult, McpSubscriptionWorkerError> {
        self.reconcile_record(resolved.record, command.query.fence, command.audits.refresh)
            .await
    }

    async fn reconcile_record(
        &self,
        record: McpSubscriptionRecord,
        fence: JobFence,
        audit: McpSubscriptionWorkerAudit,
    ) -> Result<McpSubscriptionWorkerResult, McpSubscriptionWorkerError> {
        let request = McpSubscriptionReconcileRequest::build(&record)
            .map_err(|_| McpSubscriptionWorkerError::InvalidCommand)?;
        request
            .validate_for(&record)
            .map_err(|_| McpSubscriptionWorkerError::InvalidCommand)?;
        let accepted = self
            .invalidation_target
            .accept_reconcile(request.clone())
            .await
            .map_err(McpSubscriptionWorkerError::Invalidation)?;
        if accepted.request_digest != request.request_digest
            || accepted.accepted_at
                > Utc::now() + ChronoDuration::seconds(MAX_SUBSCRIPTION_CLOCK_SKEW_SECONDS)
        {
            return Err(McpSubscriptionWorkerError::InvalidCommand);
        }
        let mut completion = CompleteMcpSubscriptionReconcile {
            audit,
            subscription_id: record.subscription_id.clone(),
            job_id: record.job_id.clone(),
            fence,
            expected_subscription_version: record.version,
            expected_session_generation: record.payload.session.generation,
            reconcile_evidence_digest: accepted.durable_work_digest,
        };
        completion.audit.request_digest = completion
            .request_digest()
            .map_err(|_| McpSubscriptionWorkerError::InvalidCommand)?;
        let outcome = self
            .authority
            .complete_subscription_reconcile(completion)
            .await
            .map_err(McpSubscriptionWorkerError::Persistence)?;
        Ok(McpSubscriptionWorkerResult::Reconciled(outcome))
    }

    async fn save_session_phase(
        &self,
        record: &McpSubscriptionRecord,
        fence: &JobFence,
        audit: McpSubscriptionWorkerAudit,
        target: McpSessionState,
        ready: Option<(EncryptedMcpState, DateTime<Utc>)>,
        phase_evidence_digest: Option<Sha256Digest>,
    ) -> Result<CommandOutcome<McpSubscriptionRecord>, McpSubscriptionWorkerError> {
        let expected_record_version = record
            .version
            .checked_add(1)
            .ok_or(McpSubscriptionWorkerError::InvalidCommand)?;
        let expected_session_version = record
            .payload
            .session
            .version
            .checked_add(1)
            .ok_or(McpSubscriptionWorkerError::InvalidCommand)?;
        let (encrypted_opaque_session, expires_at) = ready.unzip();
        let mut command = SaveMcpSubscriptionSession {
            audit,
            subscription_id: record.subscription_id.clone(),
            job_id: record.job_id.clone(),
            fence: fence.clone(),
            expected_subscription_version: record.version,
            expected_session_version: record.payload.session.version,
            target,
            encrypted_opaque_session,
            expires_at,
            phase_evidence_digest,
        };
        command.audit.request_digest = command
            .request_digest()
            .map_err(|_| McpSubscriptionWorkerError::InvalidCommand)?;
        let outcome = self
            .authority
            .save_subscription_session(command)
            .await
            .map_err(McpSubscriptionWorkerError::Persistence)?;
        let next = outcome_record(&outcome);
        if next.version != expected_record_version
            || next.payload.session.version != expected_session_version
            || next.payload.session.state != target
        {
            return Err(McpSubscriptionWorkerError::Persistence(
                McpSubscriptionPersistenceError::Conflict,
            ));
        }
        Ok(outcome)
    }
}

fn refreshed_subscription_fence(
    current: &JobFence,
    candidate: JobFence,
) -> Result<JobFence, McpSubscriptionWorkerError> {
    if candidate.worker_process_generation_id != current.worker_process_generation_id
        || candidate.lease_generation != current.lease_generation
        || candidate.token_digest != current.token_digest
        || candidate.expected_version < current.expected_version
    {
        return Err(McpSubscriptionWorkerError::LeaseCoordination);
    }
    Ok(candidate)
}

fn next_fence(current: &JobFence) -> Result<JobFence, McpSubscriptionWorkerError> {
    Ok(JobFence {
        expected_version: current
            .expected_version
            .checked_add(1)
            .ok_or(McpSubscriptionWorkerError::InvalidCommand)?,
        worker_process_generation_id: current.worker_process_generation_id.clone(),
        lease_generation: current.lease_generation,
        token_digest: current.token_digest.clone(),
    })
}

fn outcome_record(outcome: &CommandOutcome<McpSubscriptionRecord>) -> &McpSubscriptionRecord {
    match outcome {
        CommandOutcome::Applied(record) | CommandOutcome::Replayed(record) => record,
    }
}

fn retryable_failure(code: &str) -> McpTransportFailure {
    McpTransportFailure::RetryableBeforeDispatch(SafeMcpFailure {
        safe_code: code.to_owned(),
        safe_message: "MCP subscription completion could not be observed".to_owned(),
        evidence_digest: static_digest(code),
    })
}

impl<A: ?Sized> ContextSubscriptionInvalidationTarget<A> {
    pub fn new(authority: Arc<A>) -> Self {
        Self { authority }
    }
}

#[cfg(test)]
mod lease_tests {
    use super::*;

    #[test]
    fn transport_failure_display_exposes_only_the_safe_code() {
        let error = McpSubscriptionWorkerError::Transport(
            McpTransportFailure::RejectedBeforeDispatch(SafeMcpFailure {
                safe_code: "mcp_fixture_rejected".to_owned(),
                safe_message: "transport-message-canary".to_owned(),
                evidence_digest: static_digest("transport-evidence-canary"),
            }),
        );
        assert_eq!(
            error.to_string(),
            "MCP subscription transport failed: mcp_fixture_rejected"
        );
    }

    fn fence(version: u64, token: char) -> JobFence {
        JobFence {
            expected_version: version,
            worker_process_generation_id: ResourceId::from_uuid_v7(
                ResourceKind::WorkerProcessGeneration,
                Uuid::now_v7(),
            )
            .unwrap(),
            lease_generation: 3,
            token_digest: format!("sha256:{}", token.to_string().repeat(64))
                .parse()
                .unwrap(),
        }
    }

    #[test]
    fn remote_io_lease_accepts_only_a_monotonic_exact_fence() {
        let current = fence(5, 'a');
        let newer = JobFence {
            expected_version: 8,
            ..current.clone()
        };
        assert_eq!(
            refreshed_subscription_fence(&current, newer.clone()).unwrap(),
            newer
        );
        assert!(matches!(
            refreshed_subscription_fence(
                &current,
                JobFence {
                    expected_version: 4,
                    ..current.clone()
                }
            ),
            Err(McpSubscriptionWorkerError::LeaseCoordination)
        ));
        assert!(matches!(
            refreshed_subscription_fence(
                &current,
                JobFence {
                    token_digest: fence(5, 'b').token_digest,
                    ..current.clone()
                }
            ),
            Err(McpSubscriptionWorkerError::LeaseCoordination)
        ));
    }
}
