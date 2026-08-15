use crate::{
    capability_execution_repository::{
        insert_capability_input_task, insert_capability_value_and_reference,
        update_capability_invocation,
    },
    invocation_repository::{
        load_capability_continuation_input, load_capability_execution_input,
        load_capability_invocation, load_enabled_exact_published_version,
        load_exact_capability_interface_spec, validate_capability_value_against_schema,
    },
    mcp_repository::{load_managed_mcp_sandbox_session_job, resolve_mcp_execution_contract},
    repository::{
        append_command_event, append_scheduler_event, begin_read_only_repeatable,
        claim_command_receipt, decode_deployment_closure, decode_versioned_payload, job_from_row,
        job_projection, load_deployment, load_job_for_update_by_text, load_resource,
        payload_from_row, require_tenant_permission, safety_scan_cursor_from_row, safety_scan_page,
        terminalize_command_receipt, validate_safety_scan_request, JobRecord, PgRepository,
        RepositoryError, SafetyScanCursor, SafetyScanPage, SafetyScanShard, TypedPayload,
        MAX_JOB_LEASE_MILLISECONDS,
    },
};
use base64::Engine as _;
use chrono::{DateTime, Duration, Utc};
use insight_platform_artifacts::{
    ArtifactObjectReadAuthority, ArtifactObjectReadAuthorityError, ArtifactReferenceSnapshot,
    AuthorizedArtifactObjectRead, EncryptedArtifactObjectReference,
};
use insight_platform_contracts::{
    canonical_digest, ArtifactGrantOperation, ArtifactPurpose, ArtifactReferenceKind,
    CommandOutcome, DataClassification, DeploymentClosure, EntityLifecycle, ExactVersionRef,
    Failure, FailureClass, FailureCode, FailureSource, InvocationState, JobState, Permission,
    PlatformFailureCode, PolicyKind, PolicyResourceSpec, QuotaDimension, RegistryResourceKind,
    ResourceDocument, ResourceId, ResourceKind, Retryability, SandboxJobState, Sha256Digest,
    ValueRef, WorkClass,
};
use insight_platform_invocations::{
    decide_defer_to_sandbox, decide_detached_job_outcome, CapabilityControlKind,
    CapabilityDetachedPending, CapabilityExecutionInputMaterial, CapabilityInputAction,
    CapabilityInvocationRecord, CapabilityOutputValue, CapabilityUncertainty,
    DetachedCapabilityJobOutcome, DetachedSandboxSourceKind, DispatchOutcome,
    InvocationValueStorage, PreviousDetachedSandboxJob,
};
use insight_platform_jobs::{decide_claim, decide_heartbeat, JobProjection, LeasePolicy};
use insight_platform_mcp_host::{
    McpElicitationAction, McpElicitationResponse, McpExecutionContractQuery,
    McpLogicalOperationRequest,
};
use insight_platform_sandbox::{
    decide_accept, decide_advance_phase, decide_begin_execution, decide_execution_outcome,
    decide_expired_lease_recovery, decide_prestart_control, AcceptSandboxExecution,
    AuthorizedManagedMcpSandboxSecretDelivery, ClaimSandboxJobs, ClaimedSandboxJob,
    CommitSandboxOutcome, CommitSandboxPhase, ExpiredSandboxLease, HeartbeatSandboxExecution,
    ManagedMcpSandboxSecretCommitOutcome, ManagedMcpSandboxSecretDeliveryAuthority,
    ManagedMcpSandboxSecretDeliveryError, ManagedMcpSandboxSecretDeliveryEvidence,
    ManagedMcpSandboxSecretDeliveryRequest, ManagedMcpSandboxSecretReservationOutcome,
    ManagedMcpSandboxSessionRequest, MergeSandboxCapabilityOutcome, MicroVmArtifactReadPurpose,
    MicroVmArtifactReadRequest, MicroVmGrantRevocationError, MicroVmGrantRevocationEvidence,
    MicroVmGrantRevoker, MicroVmSandboxWorkloadKind, PendingSandboxCapabilityOutcome,
    RecoverExpiredSandboxLease, RecoverSandboxControlSignals, ResolveSandboxControlEvent,
    RevokeMicroVmSandboxGrants, RevokeWasiSandboxGrants, SandboxClaimAuthority,
    SandboxClaimFailure, SandboxCommandLimits, SandboxControlAuthority, SandboxControlScanCursor,
    SandboxControlSignalPage, SandboxControlSignalSource, SandboxExecutionAuthority,
    SandboxExecutionJobPayload, SandboxExecutionOutcome, SandboxExecutionPolicyClosure,
    SandboxExecutionRequest, SandboxExecutionSource, SandboxGatewayAuthority, SandboxJobPayload,
    SandboxLeaseRecoveryAction, SandboxLeaseRecoveryAuthority, SandboxLeaseRecoveryDisposition,
    SandboxLeaseRecoveryResult, SandboxPhaseDecision, SandboxPrestartControlOutcome,
    SandboxRecoveryAudit, SandboxResourceEnvelope, SandboxResourceUsage, SandboxStopReason,
    SandboxStopSignal, SandboxWorkerAudit, ScopedArtifactGrant, ScopedSecretGrant,
    StopUnclaimedSandboxJob, WasiArtifactReadPurpose, WasiArtifactReadRequest,
    WasiGrantRevocationError, WasiGrantRevocationEvidence, WasiGrantRevoker, WasiValueDirection,
    WasiValueValidationError, WasiValueValidationRequest, WasiValueValidator, SANDBOX_QUOTA_LINES,
};
use serde::Deserialize;
use sha2::{Digest as _, Sha256};
use sqlx::{postgres::PgRow, Postgres, Row, Transaction};
use std::collections::BTreeSet;

fn detached_sandbox_source_kind(source: &SandboxExecutionSource) -> DetachedSandboxSourceKind {
    match source {
        SandboxExecutionSource::SandboxCapability { .. } => {
            DetachedSandboxSourceKind::SandboxCapability
        }
        SandboxExecutionSource::ManagedMcp { .. } => DetachedSandboxSourceKind::ManagedMcp,
    }
}

async fn verify_managed_mcp_detached_continuation(
    transaction: &mut Transaction<'_, Postgres>,
    request: &SandboxExecutionRequest,
    invocation: &CapabilityInvocationRecord,
    previous: Option<&LockedSandboxJob>,
) -> Result<(), RepositoryError> {
    let SandboxExecutionSource::ManagedMcp { operation, .. } = &request.execution_source else {
        if invocation.payload.detached_pending.is_some() {
            return Err(RepositoryError::Conflict(
                "non-MCP Sandbox request cannot consume MCP continuation",
            ));
        }
        return Ok(());
    };
    let previous_operation = previous
        .map(
            |previous| match &previous.payload.request.execution_source {
                SandboxExecutionSource::ManagedMcp { operation, .. } => Ok(operation.as_ref()),
                SandboxExecutionSource::SandboxCapability { .. } => Err(RepositoryError::Conflict(
                    "Managed MCP continuation has a non-MCP predecessor",
                )),
            },
        )
        .transpose()?;
    if previous_operation
        .is_some_and(|previous| !same_managed_mcp_operation_lineage(previous, operation))
    {
        return Err(RepositoryError::Conflict(
            "Managed MCP continuation changed its logical operation",
        ));
    }
    match &invocation.payload.detached_pending {
        None => {
            if invocation.state == InvocationState::Ready {
                if previous.is_some() || operation.continuation.is_some() {
                    return Err(RepositoryError::Conflict(
                        "initial Managed MCP operation has unexpected continuation",
                    ));
                }
            } else if invocation.state == InvocationState::RetryScheduled {
                let previous = previous_operation.ok_or(RepositoryError::Conflict(
                    "Managed MCP retry has no prior physical operation",
                ))?;
                if operation.task_requested != previous.task_requested
                    || operation.continuation != previous.continuation
                {
                    return Err(RepositoryError::Conflict(
                        "Managed MCP retry changed its protocol continuation",
                    ));
                }
            }
        }
        Some(CapabilityDetachedPending::RemoteTask { wait, poll_count }) => {
            let previous = previous.ok_or(RepositoryError::Conflict(
                "Managed MCP remote Task has no terminal predecessor",
            ))?;
            if !matches!(
                previous.payload.outcome.as_ref(),
                Some(SandboxExecutionOutcome::ManagedMcp(output))
                    if output.logical_poll_count == *poll_count
                        && matches!(&output.outcome, DispatchOutcome::Deferred(prior) if prior == wait)
            ) {
                return Err(RepositoryError::Conflict(
                    "Managed MCP remote Task does not match terminal evidence",
                ));
            }
            let continuation = operation
                .continuation
                .as_ref()
                .ok_or(RepositoryError::Conflict(
                    "Managed MCP poll has no continuation",
                ))?;
            if operation.task_requested
                || continuation.poll_count != poll_count.saturating_add(1)
                || continuation.external_identity_digest != wait.external_identity_digest
                || continuation.elicitation_response.is_some()
                || !same_encrypted_mcp_state(&wait.encrypted_state, &continuation.encrypted_state)
            {
                return Err(RepositoryError::Conflict(
                    "Managed MCP poll continuation is not exact",
                ));
            }
        }
        Some(CapabilityDetachedPending::InputRequired {
            request: input,
            poll_count,
            resolution: Some(resolution),
        }) => {
            let previous = previous.ok_or(RepositoryError::Conflict(
                "Managed MCP elicitation has no terminal predecessor",
            ))?;
            if !matches!(
                previous.payload.outcome.as_ref(),
                Some(SandboxExecutionOutcome::ManagedMcp(output))
                    if output.logical_poll_count == *poll_count
                        && matches!(&output.outcome, DispatchOutcome::InputRequired(prior) if prior == input.as_ref())
            ) {
                return Err(RepositoryError::Conflict(
                    "Managed MCP elicitation does not match terminal evidence",
                ));
            }
            let continuation = operation
                .continuation
                .as_ref()
                .ok_or(RepositoryError::Conflict(
                    "Managed MCP elicitation has no continuation",
                ))?;
            let expected_response =
                managed_mcp_elicitation_response(transaction, invocation, input, resolution)
                    .await?;
            if operation.task_requested
                || continuation.poll_count != *poll_count
                || continuation.external_identity_digest != input.external_identity_digest
                || continuation.elicitation_response.as_ref() != Some(&expected_response)
                || !same_encrypted_mcp_state(&input.encrypted_state, &continuation.encrypted_state)
            {
                return Err(RepositoryError::Conflict(
                    "Managed MCP elicitation continuation is not exact",
                ));
            }
        }
        Some(CapabilityDetachedPending::InputRequired {
            resolution: None, ..
        }) => {
            return Err(RepositoryError::Conflict(
                "Managed MCP elicitation has not been resolved",
            ));
        }
    }
    Ok(())
}

fn same_managed_mcp_operation_lineage(
    previous: &McpLogicalOperationRequest,
    next: &McpLogicalOperationRequest,
) -> bool {
    let mut normalized = previous.clone();
    normalized.job_id = next.job_id.clone();
    normalized.physical_attempt = next.physical_attempt;
    normalized.task_requested = next.task_requested;
    normalized.continuation = next.continuation.clone();
    normalized == *next
}

fn same_encrypted_mcp_state(
    stored: &insight_platform_invocations::EncryptedRemoteState,
    protocol: &insight_platform_mcp_host::EncryptedMcpState,
) -> bool {
    stored.scheme == protocol.scheme
        && stored.key_id == protocol.key_id
        && stored.key_reference_digest == protocol.key_reference_digest
        && stored.plaintext_digest == protocol.plaintext_digest
        && base64::engine::general_purpose::STANDARD
            .decode(&stored.ciphertext)
            .is_ok_and(|ciphertext| ciphertext == protocol.ciphertext)
}

async fn managed_mcp_elicitation_response(
    transaction: &mut Transaction<'_, Postgres>,
    invocation: &CapabilityInvocationRecord,
    input: &insight_platform_invocations::BackendInputRequest,
    resolution: &insight_platform_invocations::CapabilityDetachedInputResolution,
) -> Result<McpElicitationResponse, RepositoryError> {
    let (action, content) = match (resolution.action, resolution.response.as_ref()) {
        (CapabilityInputAction::Accept, Some(response)) => {
            let material = load_capability_continuation_input(
                transaction,
                invocation,
                response,
                resolution.response_artifact_link_id.as_ref(),
            )
            .await?;
            let CapabilityExecutionInputMaterial::Inline { value } = material.material else {
                return Err(RepositoryError::Conflict(
                    "Managed MCP form response must be inline",
                ));
            };
            let content = insight_platform_contracts::ClosedJsonValue::build(
                input.response_schema_digest.clone(),
                value,
            )
            .map_err(|_| RepositoryError::Conflict("Managed MCP form response is invalid"))?;
            (McpElicitationAction::Accept, Some(content))
        }
        (CapabilityInputAction::Decline, None) => (McpElicitationAction::Decline, None),
        (CapabilityInputAction::Cancel, None) => (McpElicitationAction::Cancel, None),
        _ => {
            return Err(RepositoryError::Conflict(
                "Managed MCP elicitation resolution is invalid",
            ));
        }
    };
    Ok(McpElicitationResponse { action, content })
}

#[async_trait::async_trait]
impl SandboxClaimAuthority for PgRepository {
    async fn claim_sandbox_jobs(
        &self,
        command: ClaimSandboxJobs,
    ) -> Result<Vec<ClaimedSandboxJob>, SandboxClaimFailure> {
        PgRepository::claim_sandbox_jobs(self, command)
            .await
            .map_err(|error| match error {
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
            })
    }
}

#[async_trait::async_trait]
impl ArtifactObjectReadAuthority<WasiArtifactReadRequest> for PgRepository {
    async fn authorize_object_read(
        &self,
        request: &WasiArtifactReadRequest,
    ) -> Result<AuthorizedArtifactObjectRead, ArtifactObjectReadAuthorityError> {
        if request.tenant_id.kind() != ResourceKind::Tenant
            || request.sandbox_job_id.kind() != ResourceKind::SandboxJob
            || request.worker_process_generation_id.kind() != ResourceKind::WorkerProcessGeneration
            || request.lease_generation == 0
            || request.maximum_bytes == 0
            || request.artifact.validate().is_err()
        {
            return Err(ArtifactObjectReadAuthorityError::Denied);
        }
        let job_id = ResourceId::from_uuid_v7(ResourceKind::Job, request.sandbox_job_id.uuid())
            .map_err(|_| ArtifactObjectReadAuthorityError::Denied)?;
        let mut transaction = begin_read_only_repeatable(self.pool())
            .await
            .map_err(|_| ArtifactObjectReadAuthorityError::Unavailable)?;
        let database_now = database_now(&mut transaction)
            .await
            .map_err(|_| ArtifactObjectReadAuthorityError::Unavailable)?;
        let current = load_sandbox_job_read_only(
            &mut transaction,
            &request.tenant_id,
            &job_id,
            self.sandbox_limits(),
        )
        .await
        .map_err(classify_artifact_read_repository_error)?;
        require_sandbox_command_owner(&current, &request.sandbox_job_id, &request.tenant_id)
            .map_err(classify_artifact_read_repository_error)?;
        let lease = current
            .job
            .lease
            .as_ref()
            .ok_or(ArtifactObjectReadAuthorityError::Denied)?;
        let leased_request = current
            .payload
            .request
            .as_ref()
            .clone()
            .bind_lease_generation(current.job.lease_generation)
            .map_err(|_| ArtifactObjectReadAuthorityError::InvalidEvidence)?;
        if leased_request.request_digest != request.request_digest
            || lease.worker_process_generation_id != request.worker_process_generation_id
            || lease.lease_generation != request.lease_generation
            || lease.expires_at <= database_now
            || request.deadline != leased_request.deadline
            || request.deadline <= database_now
            || request.maximum_bytes == 0
            || u64::try_from(request.maximum_bytes)
                .map_or(true, |maximum| maximum < request.artifact.byte_length())
            || !matches!(
                current.payload.physical_state,
                SandboxJobState::Preparing
                    | SandboxJobState::Starting
                    | SandboxJobState::Running
                    | SandboxJobState::Collecting
                    | SandboxJobState::Cancelling
            )
        {
            return Err(ArtifactObjectReadAuthorityError::Denied);
        }
        authorize_wasi_artifact_purpose(&mut transaction, request, &leased_request, database_now)
            .await
            .map_err(classify_artifact_read_repository_error)?;
        let projection = ArtifactObjectReadProjection {
            tenant_id: &request.tenant_id,
            owner_kind: "sandbox_job",
            owner_id: &request.sandbox_job_id,
            request_digest: &request.request_digest,
            worker_process_generation_id: &request.worker_process_generation_id,
            provider_process_generation_id: None,
            sandbox_identity_digest: None,
            lease_generation: request.lease_generation,
            artifact: &request.artifact,
            purpose_domain: match request.purpose {
                WasiArtifactReadPurpose::RuntimeBundle => "runtime_bundle",
                WasiArtifactReadPurpose::InputValue => "input_value",
            },
            purpose_class: match request.purpose {
                WasiArtifactReadPurpose::RuntimeBundle => ArtifactObjectReadPurpose::Package,
                WasiArtifactReadPurpose::InputValue => ArtifactObjectReadPurpose::SandboxInput,
            },
        };
        let authorized = load_authorized_artifact_object(&mut transaction, projection)
            .await
            .map_err(classify_artifact_read_repository_error)?;
        transaction
            .commit()
            .await
            .map_err(|_| ArtifactObjectReadAuthorityError::Unavailable)?;
        Ok(authorized)
    }
}

#[async_trait::async_trait]
impl ArtifactObjectReadAuthority<MicroVmArtifactReadRequest> for PgRepository {
    async fn authorize_object_read(
        &self,
        request: &MicroVmArtifactReadRequest,
    ) -> Result<AuthorizedArtifactObjectRead, ArtifactObjectReadAuthorityError> {
        request
            .validate()
            .map_err(|_| ArtifactObjectReadAuthorityError::Denied)?;
        if request.workload_kind == MicroVmSandboxWorkloadKind::CapabilityExecution {
            let wasi_request = WasiArtifactReadRequest {
                tenant_id: request.tenant_id.clone(),
                sandbox_job_id: request.sandbox_job_id.clone(),
                request_digest: request.request_digest.clone(),
                worker_process_generation_id: request.executor_worker_process_generation_id.clone(),
                lease_generation: request.lease_generation,
                artifact: request.artifact.clone(),
                purpose: match request.purpose {
                    MicroVmArtifactReadPurpose::RuntimeBundle => {
                        WasiArtifactReadPurpose::RuntimeBundle
                    }
                    MicroVmArtifactReadPurpose::InputValue => WasiArtifactReadPurpose::InputValue,
                },
                read_grant: request.read_grant.clone(),
                maximum_bytes: request.maximum_bytes,
                deadline: request.deadline,
            };
            let job_id = ResourceId::from_uuid_v7(ResourceKind::Job, request.sandbox_job_id.uuid())
                .map_err(|_| ArtifactObjectReadAuthorityError::Denied)?;
            let mut transaction = begin_read_only_repeatable(self.pool())
                .await
                .map_err(|_| ArtifactObjectReadAuthorityError::Unavailable)?;
            let database_now = database_now(&mut transaction)
                .await
                .map_err(|_| ArtifactObjectReadAuthorityError::Unavailable)?;
            let current = load_sandbox_job_read_only(
                &mut transaction,
                &request.tenant_id,
                &job_id,
                self.sandbox_limits(),
            )
            .await
            .map_err(classify_artifact_read_repository_error)?;
            require_sandbox_command_owner(&current, &request.sandbox_job_id, &request.tenant_id)
                .map_err(classify_artifact_read_repository_error)?;
            let lease = current
                .job
                .lease
                .as_ref()
                .ok_or(ArtifactObjectReadAuthorityError::Denied)?;
            let leased_request = current
                .payload
                .request
                .as_ref()
                .clone()
                .bind_lease_generation(current.job.lease_generation)
                .map_err(|_| ArtifactObjectReadAuthorityError::InvalidEvidence)?;
            let prepared = current
                .payload
                .prepared
                .as_ref()
                .ok_or(ArtifactObjectReadAuthorityError::Denied)?;
            if leased_request.isolation_class
                != insight_platform_contracts::SandboxIsolationClass::MicroVm
                || leased_request.request_digest != request.request_digest
                || lease.worker_process_generation_id
                    != request.executor_worker_process_generation_id
                || lease.lease_generation != request.lease_generation
                || lease.expires_at <= database_now
                || prepared.provider_process_generation_id.as_ref()
                    != Some(&request.provider_process_generation_id)
                || prepared.sandbox_identity_digest != request.sandbox_identity_digest
                || request.deadline != leased_request.deadline
                || request.deadline <= database_now
                || u64::try_from(request.maximum_bytes)
                    .map_or(true, |maximum| maximum < request.artifact.byte_length())
                || !matches!(
                    current.payload.physical_state,
                    SandboxJobState::Starting
                        | SandboxJobState::Running
                        | SandboxJobState::Collecting
                        | SandboxJobState::Cancelling
                )
            {
                return Err(ArtifactObjectReadAuthorityError::Denied);
            }
            authorize_wasi_artifact_purpose(
                &mut transaction,
                &wasi_request,
                &leased_request,
                database_now,
            )
            .await
            .map_err(classify_artifact_read_repository_error)?;
            let authorized = load_authorized_artifact_object(
                &mut transaction,
                ArtifactObjectReadProjection {
                    tenant_id: &request.tenant_id,
                    owner_kind: "sandbox_job",
                    owner_id: &request.sandbox_job_id,
                    request_digest: &request.request_digest,
                    worker_process_generation_id: &request.executor_worker_process_generation_id,
                    provider_process_generation_id: Some(&request.provider_process_generation_id),
                    sandbox_identity_digest: Some(&request.sandbox_identity_digest),
                    lease_generation: request.lease_generation,
                    artifact: &request.artifact,
                    purpose_domain: match request.purpose {
                        MicroVmArtifactReadPurpose::RuntimeBundle => "runtime_bundle",
                        MicroVmArtifactReadPurpose::InputValue => "input_value",
                    },
                    purpose_class: match request.purpose {
                        MicroVmArtifactReadPurpose::RuntimeBundle => {
                            ArtifactObjectReadPurpose::Package
                        }
                        MicroVmArtifactReadPurpose::InputValue => {
                            ArtifactObjectReadPurpose::SandboxInput
                        }
                    },
                },
            )
            .await
            .map_err(classify_artifact_read_repository_error)?;
            transaction
                .commit()
                .await
                .map_err(|_| ArtifactObjectReadAuthorityError::Unavailable)?;
            return Ok(authorized);
        }
        if request.purpose != MicroVmArtifactReadPurpose::RuntimeBundle {
            return Err(ArtifactObjectReadAuthorityError::Denied);
        }
        let grant = request
            .read_grant
            .as_ref()
            .ok_or(ArtifactObjectReadAuthorityError::Denied)?;
        let job_id = ResourceId::from_uuid_v7(ResourceKind::Job, request.sandbox_job_id.uuid())
            .map_err(|_| ArtifactObjectReadAuthorityError::Denied)?;
        let mut transaction = begin_read_only_repeatable(self.pool())
            .await
            .map_err(|_| ArtifactObjectReadAuthorityError::Unavailable)?;
        let database_now = database_now(&mut transaction)
            .await
            .map_err(|_| ArtifactObjectReadAuthorityError::Unavailable)?;
        let (job, payload, _) = load_managed_mcp_sandbox_session_job(
            &mut transaction,
            &request.tenant_id,
            &job_id,
            self.sandbox_limits(),
            false,
        )
        .await
        .map_err(classify_artifact_read_repository_error)?;
        let lease = job
            .lease
            .as_ref()
            .ok_or(ArtifactObjectReadAuthorityError::Denied)?;
        let prepared = payload
            .prepared_binding
            .as_ref()
            .ok_or(ArtifactObjectReadAuthorityError::Denied)?;
        if payload.physical_state != SandboxJobState::Starting
            || payload.request.identity.sandbox_job_id != request.sandbox_job_id
            || payload.request.request_digest != request.request_digest
            || lease.worker_process_generation_id != request.executor_worker_process_generation_id
            || lease.lease_generation != request.lease_generation
            || lease.expires_at <= database_now
            || prepared.provider_process_generation_id != request.provider_process_generation_id
            || prepared.sandbox_identity_digest != request.sandbox_identity_digest
            || payload.request.deadline != request.deadline
            || request.deadline <= database_now
            || request.artifact != payload.request.package.runtime_bundle_artifact
            || !payload
                .request
                .artifact_grants
                .iter()
                .any(|frozen| frozen == grant)
        {
            return Err(ArtifactObjectReadAuthorityError::Denied);
        }
        require_active_sandbox_artifact_grant(
            &mut transaction,
            &request.tenant_id,
            &request.sandbox_job_id,
            &request.artifact,
            grant,
            database_now,
        )
        .await
        .map_err(classify_artifact_read_repository_error)?;
        let authorized = load_authorized_artifact_object(
            &mut transaction,
            ArtifactObjectReadProjection {
                tenant_id: &request.tenant_id,
                owner_kind: "sandbox_job",
                owner_id: &request.sandbox_job_id,
                request_digest: &request.request_digest,
                worker_process_generation_id: &request.executor_worker_process_generation_id,
                provider_process_generation_id: Some(&request.provider_process_generation_id),
                sandbox_identity_digest: Some(&request.sandbox_identity_digest),
                lease_generation: request.lease_generation,
                artifact: &request.artifact,
                purpose_domain: "managed_mcp_session_runtime_bundle",
                purpose_class: ArtifactObjectReadPurpose::Package,
            },
        )
        .await
        .map_err(classify_artifact_read_repository_error)?;
        transaction
            .commit()
            .await
            .map_err(|_| ArtifactObjectReadAuthorityError::Unavailable)?;
        Ok(authorized)
    }
}

#[async_trait::async_trait]
impl ManagedMcpSandboxSecretDeliveryAuthority for PgRepository {
    async fn reserve_managed_mcp_sandbox_secret_delivery(
        &self,
        request: &ManagedMcpSandboxSecretDeliveryRequest,
    ) -> Result<ManagedMcpSandboxSecretReservationOutcome, ManagedMcpSandboxSecretDeliveryError>
    {
        request
            .validate_shape()
            .map_err(|_| ManagedMcpSandboxSecretDeliveryError::Denied)?;
        let mut transaction = self
            .pool()
            .begin()
            .await
            .map_err(|_| ManagedMcpSandboxSecretDeliveryError::Unavailable)?;
        let database_now = database_now(&mut transaction)
            .await
            .map_err(|_| ManagedMcpSandboxSecretDeliveryError::Unavailable)?;
        authorize_managed_mcp_secret_delivery(
            &mut transaction,
            request,
            database_now,
            self.sandbox_limits(),
        )
        .await
        .map_err(classify_managed_mcp_secret_delivery_repository_error)?;

        if let Some((receipt_id, request_digest, state, payload)) =
            load_managed_mcp_secret_delivery_receipt(&mut transaction, request)
                .await
                .map_err(classify_managed_mcp_secret_delivery_repository_error)?
        {
            if receipt_id != request.receipt_id.to_string()
                || request_digest != request.canonical_digest.to_string()
            {
                return Err(ManagedMcpSandboxSecretDeliveryError::Denied);
            }
            if state == "processing" {
                let authorization: AuthorizedManagedMcpSandboxSecretDelivery =
                    decode_versioned_payload(&payload, "Managed MCP Secret delivery authorization")
                        .map_err(classify_managed_mcp_secret_delivery_repository_error)?;
                authorization
                    .validate_for(request)
                    .map_err(|_| ManagedMcpSandboxSecretDeliveryError::Denied)?;
            } else if state != "succeeded" {
                return Err(ManagedMcpSandboxSecretDeliveryError::OutcomeUncertain);
            }
            transaction
                .commit()
                .await
                .map_err(|_| ManagedMcpSandboxSecretDeliveryError::Unavailable)?;
            return match state.as_str() {
                "processing" => Ok(ManagedMcpSandboxSecretReservationOutcome::AlreadyReserved),
                "succeeded" => Ok(ManagedMcpSandboxSecretReservationOutcome::AlreadyDelivered),
                _ => unreachable!("closed receipt state was checked before commit"),
            };
        }

        let consumed: i64 = sqlx::query_scalar(
            r#"
            SELECT count(*)
            FROM insight_platform.receipts
            WHERE tenant_id = $1
              AND receipt_kind = 'sandbox_secret_delivery'
              AND scope_kind = 'sandbox_job' AND scope_id = $2
              AND dedupe_owner_id = $3 AND operation = 'sandbox.secret.deliver'
              AND state IN ('processing', 'succeeded')
            "#,
        )
        .bind(request.identity.tenant_id.to_string())
        .bind(request.identity.sandbox_job_id.to_string())
        .bind(
            request
                .secret_grant
                .secret_binding
                .secret_binding_id
                .to_string(),
        )
        .fetch_one(&mut *transaction)
        .await
        .map_err(|_| ManagedMcpSandboxSecretDeliveryError::Unavailable)?;
        if consumed < 0
            || u64::try_from(consumed).ok() >= Some(u64::from(request.secret_grant.maximum_reads))
        {
            return Err(ManagedMcpSandboxSecretDeliveryError::Denied);
        }

        let authorization = AuthorizedManagedMcpSandboxSecretDelivery {
            schema_version: 1,
            receipt_id: request.receipt_id.clone(),
            tenant_id: request.identity.tenant_id.clone(),
            sandbox_job_id: request.identity.sandbox_job_id.clone(),
            secret_binding: request.secret_grant.secret_binding.clone(),
            resolved_binding_generation: request.secret_grant.resolved_binding_generation,
            delivery_request_digest: request.canonical_digest.clone(),
            expires_at: request.secret_grant.expires_at,
            authorization_digest: request.canonical_digest.clone(),
        }
        .seal()
        .map_err(|_| ManagedMcpSandboxSecretDeliveryError::Denied)?;
        let payload = TypedPayload::from_versioned(1, &authorization, 262_144)
            .map_err(classify_managed_mcp_secret_delivery_repository_error)?;
        let inserted = sqlx::query(
            r#"
            INSERT INTO insight_platform.receipts (
                tenant_id, receipt_id, receipt_kind, scope_kind, scope_id,
                dedupe_owner_id, operation, idempotency_key_digest, request_digest, state,
                payload_schema_version, payload, payload_digest, expires_at
            ) VALUES ($1, $2, 'sandbox_secret_delivery', 'sandbox_job', $3,
                      $4, 'sandbox.secret.deliver', $5, $6, 'processing', $7, $8, $9, $10)
            ON CONFLICT DO NOTHING
            "#,
        )
        .bind(request.identity.tenant_id.to_string())
        .bind(request.receipt_id.to_string())
        .bind(request.identity.sandbox_job_id.to_string())
        .bind(
            request
                .secret_grant
                .secret_binding
                .secret_binding_id
                .to_string(),
        )
        .bind(request.idempotency_key_digest.to_string())
        .bind(request.canonical_digest.to_string())
        .bind(payload.schema_version)
        .bind(&payload.value)
        .bind(&payload.digest)
        .bind(request.secret_grant.expires_at)
        .execute(&mut *transaction)
        .await
        .map_err(|_| ManagedMcpSandboxSecretDeliveryError::Unavailable)?
        .rows_affected();
        if inserted != 1 {
            return Err(ManagedMcpSandboxSecretDeliveryError::OutcomeUncertain);
        }
        transaction
            .commit()
            .await
            .map_err(|_| ManagedMcpSandboxSecretDeliveryError::Unavailable)?;
        Ok(ManagedMcpSandboxSecretReservationOutcome::Authorized(
            Box::new(authorization),
        ))
    }

    async fn commit_managed_mcp_sandbox_secret_delivery(
        &self,
        request: &ManagedMcpSandboxSecretDeliveryRequest,
        authorization: &AuthorizedManagedMcpSandboxSecretDelivery,
        resolution_evidence_digest: &Sha256Digest,
    ) -> Result<ManagedMcpSandboxSecretCommitOutcome, ManagedMcpSandboxSecretDeliveryError> {
        request
            .validate_shape()
            .map_err(|_| ManagedMcpSandboxSecretDeliveryError::Denied)?;
        authorization
            .validate_for(request)
            .map_err(|_| ManagedMcpSandboxSecretDeliveryError::Denied)?;
        let mut transaction = self
            .pool()
            .begin()
            .await
            .map_err(|_| ManagedMcpSandboxSecretDeliveryError::Unavailable)?;
        let database_now = database_now(&mut transaction)
            .await
            .map_err(|_| ManagedMcpSandboxSecretDeliveryError::Unavailable)?;
        authorize_managed_mcp_secret_delivery(
            &mut transaction,
            request,
            database_now,
            self.sandbox_limits(),
        )
        .await
        .map_err(classify_managed_mcp_secret_delivery_repository_error)?;
        let row = sqlx::query(
            r#"
            SELECT request_digest, state, payload_schema_version, payload, payload_digest
            FROM insight_platform.receipts
            WHERE tenant_id = $1 AND receipt_id = $2
              AND receipt_kind = 'sandbox_secret_delivery'
              AND scope_kind = 'sandbox_job' AND scope_id = $3
              AND dedupe_owner_id = $4 AND operation = 'sandbox.secret.deliver'
              AND idempotency_key_digest = $5
            FOR UPDATE
            "#,
        )
        .bind(request.identity.tenant_id.to_string())
        .bind(request.receipt_id.to_string())
        .bind(request.identity.sandbox_job_id.to_string())
        .bind(
            request
                .secret_grant
                .secret_binding
                .secret_binding_id
                .to_string(),
        )
        .bind(request.idempotency_key_digest.to_string())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(|_| ManagedMcpSandboxSecretDeliveryError::Unavailable)?
        .ok_or(ManagedMcpSandboxSecretDeliveryError::Denied)?;
        if row
            .try_get::<String, _>("request_digest")
            .map_err(|_| ManagedMcpSandboxSecretDeliveryError::OutcomeUncertain)?
            != request.canonical_digest.to_string()
        {
            return Err(ManagedMcpSandboxSecretDeliveryError::Denied);
        }
        let state: String = row
            .try_get("state")
            .map_err(|_| ManagedMcpSandboxSecretDeliveryError::OutcomeUncertain)?;
        let payload = payload_from_row(&row, "payload_schema_version", "payload", "payload_digest")
            .map_err(classify_managed_mcp_secret_delivery_repository_error)?;
        if state == "succeeded" {
            let evidence: ManagedMcpSandboxSecretDeliveryEvidence =
                decode_versioned_payload(&payload, "Managed MCP Secret delivery evidence")
                    .map_err(classify_managed_mcp_secret_delivery_repository_error)?;
            evidence
                .validate_for(request, authorization)
                .map_err(|_| ManagedMcpSandboxSecretDeliveryError::OutcomeUncertain)?;
            if evidence.resolution_evidence_digest != *resolution_evidence_digest {
                return Err(ManagedMcpSandboxSecretDeliveryError::Denied);
            }
            transaction
                .commit()
                .await
                .map_err(|_| ManagedMcpSandboxSecretDeliveryError::Unavailable)?;
            return Ok(ManagedMcpSandboxSecretCommitOutcome::Replayed(evidence));
        }
        if state != "processing" {
            return Err(ManagedMcpSandboxSecretDeliveryError::OutcomeUncertain);
        }
        let stored: AuthorizedManagedMcpSandboxSecretDelivery =
            decode_versioned_payload(&payload, "Managed MCP Secret delivery authorization")
                .map_err(classify_managed_mcp_secret_delivery_repository_error)?;
        if stored != *authorization {
            return Err(ManagedMcpSandboxSecretDeliveryError::Denied);
        }
        let evidence = ManagedMcpSandboxSecretDeliveryEvidence {
            schema_version: 1,
            receipt_id: request.receipt_id.clone(),
            tenant_id: request.identity.tenant_id.clone(),
            sandbox_job_id: request.identity.sandbox_job_id.clone(),
            secret_binding_id: request
                .secret_grant
                .secret_binding
                .secret_binding_id
                .clone(),
            resolved_binding_generation: request.secret_grant.resolved_binding_generation,
            authorization_digest: authorization.authorization_digest.clone(),
            resolution_evidence_digest: resolution_evidence_digest.clone(),
            delivered_at: database_now,
            evidence_digest: request.canonical_digest.clone(),
        }
        .seal()
        .map_err(|_| ManagedMcpSandboxSecretDeliveryError::Denied)?;
        evidence
            .validate_for(request, authorization)
            .map_err(|_| ManagedMcpSandboxSecretDeliveryError::Denied)?;
        let receipt_payload = TypedPayload::from_versioned(1, &evidence, 262_144)
            .map_err(classify_managed_mcp_secret_delivery_repository_error)?;
        let updated = sqlx::query(
            r#"
            UPDATE insight_platform.receipts
            SET state = 'succeeded', disposition = 'delivered',
                payload_schema_version = $3, payload = $4, payload_digest = $5,
                completed_at = $6
            WHERE tenant_id = $1 AND receipt_id = $2 AND state = 'processing'
            "#,
        )
        .bind(request.identity.tenant_id.to_string())
        .bind(request.receipt_id.to_string())
        .bind(receipt_payload.schema_version)
        .bind(&receipt_payload.value)
        .bind(&receipt_payload.digest)
        .bind(database_now)
        .execute(&mut *transaction)
        .await
        .map_err(|_| ManagedMcpSandboxSecretDeliveryError::Unavailable)?
        .rows_affected();
        if updated != 1 {
            return Err(ManagedMcpSandboxSecretDeliveryError::OutcomeUncertain);
        }
        append_scheduler_event(
            &mut transaction,
            &request.identity.tenant_id.to_string(),
            &request.event_id,
            &request.outbox_id,
            "sandbox_secret_delivery",
            &request.receipt_id.to_string(),
            1,
            None,
            "sandbox.secret_delivered",
            &TypedPayload::new(
                1,
                &serde_json::json!({
                    "authorization_digest": authorization.authorization_digest,
                    "provider_process_generation_id": request.prepared.provider_process_generation_id,
                    "resolved_binding_generation": request.secret_grant.resolved_binding_generation,
                    "sandbox_identity_digest": request.prepared.sandbox_identity_digest,
                    "sandbox_job_id": request.identity.sandbox_job_id,
                    "secret_binding_id": request.secret_grant.secret_binding.secret_binding_id,
                }),
            )
            .map_err(classify_managed_mcp_secret_delivery_repository_error)?,
        )
        .await
        .map_err(classify_managed_mcp_secret_delivery_repository_error)?;
        transaction
            .commit()
            .await
            .map_err(|_| ManagedMcpSandboxSecretDeliveryError::Unavailable)?;
        Ok(ManagedMcpSandboxSecretCommitOutcome::Delivered(evidence))
    }
}

async fn authorize_managed_mcp_secret_delivery(
    transaction: &mut Transaction<'_, Postgres>,
    request: &ManagedMcpSandboxSecretDeliveryRequest,
    database_now: DateTime<Utc>,
    limits: SandboxCommandLimits,
) -> Result<(), RepositoryError> {
    request.validate_shape()?;
    let (job, payload, _) = load_managed_mcp_sandbox_session_job(
        transaction,
        &request.identity.tenant_id,
        &request.identity.physical_job_id,
        limits,
        true,
    )
    .await?;
    let lease = job.lease.as_ref().ok_or(RepositoryError::StaleFence)?;
    let executor_identity_digest = payload
        .executor_identity_digest
        .as_ref()
        .ok_or(RepositoryError::StaleFence)?;
    request
        .prepared
        .validate_for(&payload.request, &request.fence, executor_identity_digest)?;
    if payload.physical_state != SandboxJobState::Starting
        || payload.request.identity != request.identity
        || payload.request.request_digest != request.request_digest
        || payload.phase_evidence_digest.as_ref() != Some(&request.prepared.canonical_digest)
        || job.version != request.fence.expected_version
        || lease.worker_process_generation_id != request.fence.worker_process_generation_id
        || lease.lease_generation != request.fence.lease_generation
        || lease.token_digest != request.fence.token_digest
        || lease.expires_at <= database_now
        || request.secret_grant.expires_at <= database_now
        || !payload
            .request
            .secret_grants
            .iter()
            .any(|grant| grant == &request.secret_grant)
    {
        return Err(RepositoryError::StaleFence);
    }
    lock_sandbox_secret_grants(
        transaction,
        &request.identity.tenant_id,
        std::slice::from_ref(&request.secret_grant),
    )
    .await
}

async fn load_managed_mcp_secret_delivery_receipt(
    transaction: &mut Transaction<'_, Postgres>,
    request: &ManagedMcpSandboxSecretDeliveryRequest,
) -> Result<Option<(String, String, String, TypedPayload)>, RepositoryError> {
    let row = sqlx::query(
        r#"
        SELECT receipt_id, request_digest, state,
               payload_schema_version, payload, payload_digest
        FROM insight_platform.receipts
        WHERE tenant_id = $1
          AND receipt_kind = 'sandbox_secret_delivery'
          AND scope_kind = 'sandbox_job' AND scope_id = $2
          AND dedupe_owner_id = $3 AND operation = 'sandbox.secret.deliver'
          AND idempotency_key_digest = $4
        FOR UPDATE
        "#,
    )
    .bind(request.identity.tenant_id.to_string())
    .bind(request.identity.sandbox_job_id.to_string())
    .bind(
        request
            .secret_grant
            .secret_binding
            .secret_binding_id
            .to_string(),
    )
    .bind(request.idempotency_key_digest.to_string())
    .fetch_optional(&mut **transaction)
    .await?;
    row.map(|row| {
        Ok((
            row.try_get("receipt_id")?,
            row.try_get("request_digest")?,
            row.try_get("state")?,
            payload_from_row(&row, "payload_schema_version", "payload", "payload_digest")?,
        ))
    })
    .transpose()
}

fn classify_managed_mcp_secret_delivery_repository_error(
    error: RepositoryError,
) -> ManagedMcpSandboxSecretDeliveryError {
    match error {
        RepositoryError::Database(_) => ManagedMcpSandboxSecretDeliveryError::Unavailable,
        RepositoryError::Conflict(_) | RepositoryError::IdempotencyConflict => {
            ManagedMcpSandboxSecretDeliveryError::OutcomeUncertain
        }
        _ => ManagedMcpSandboxSecretDeliveryError::Denied,
    }
}

async fn authorize_wasi_artifact_purpose(
    transaction: &mut Transaction<'_, Postgres>,
    request: &WasiArtifactReadRequest,
    leased_request: &SandboxExecutionRequest,
    database_now: DateTime<Utc>,
) -> Result<(), RepositoryError> {
    match request.purpose {
        WasiArtifactReadPurpose::RuntimeBundle => {
            if request.read_grant.is_some()
                || request.artifact != leased_request.package.runtime_bundle_artifact
            {
                return Err(RepositoryError::PermissionDenied);
            }
        }
        WasiArtifactReadPurpose::InputValue => {
            let grant = request
                .read_grant
                .as_ref()
                .ok_or(RepositoryError::PermissionDenied)?;
            if grant.operation != ArtifactGrantOperation::ReadWhole
                || grant.artifact.as_ref() != Some(&request.artifact)
                || grant.expires_at <= database_now
                || !leased_request
                    .artifact_grants
                    .iter()
                    .any(|frozen| frozen == grant)
            {
                return Err(RepositoryError::PermissionDenied);
            }
            require_active_sandbox_artifact_grant(
                transaction,
                &request.tenant_id,
                &request.sandbox_job_id,
                &request.artifact,
                grant,
                database_now,
            )
            .await?;
        }
    }
    Ok(())
}

async fn require_active_sandbox_artifact_grant(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    sandbox_job_id: &ResourceId,
    artifact: &insight_platform_contracts::ArtifactRef,
    grant: &ScopedArtifactGrant,
    database_now: DateTime<Utc>,
) -> Result<(), RepositoryError> {
    let row = sqlx::query(
        r#"
        SELECT source_artifact_id, link_key_digest, state, version,
               payload_schema_version, payload, payload_digest, expires_at
        FROM insight_platform.artifact_links
        WHERE tenant_id = $1 AND artifact_link_id = $2
          AND link_kind = 'grant' AND owner_kind = 'sandbox_job'
          AND owner_id = $3
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(grant.grant_id.to_string())
    .bind(sandbox_job_id.to_string())
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::PermissionDenied)?;
    let payload = payload_from_row(&row, "payload_schema_version", "payload", "payload_digest")?;
    let durable_grant: ScopedArtifactGrant =
        decode_versioned_payload(&payload, "Sandbox Artifact grant")?;
    if grant.operation != ArtifactGrantOperation::ReadWhole
        || grant.artifact.as_ref() != Some(artifact)
        || grant.expires_at <= database_now
        || row.try_get::<String, _>("state")? != "active"
        || row.try_get::<i64, _>("version")? <= 0
        || row
            .try_get::<Option<String>, _>("source_artifact_id")?
            .as_deref()
            != Some(artifact.artifact_id().to_string().as_str())
        || row.try_get::<Option<DateTime<Utc>>, _>("expires_at")? != Some(grant.expires_at)
        || durable_grant != *grant
        || row.try_get::<String, _>("link_key_digest")? != grant.grant_digest.to_string()
    {
        return Err(RepositoryError::PermissionDenied);
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ArtifactObjectReadPurpose {
    Package,
    SandboxInput,
    ModelInput,
}

pub(crate) struct ArtifactObjectReadProjection<'a> {
    pub tenant_id: &'a ResourceId,
    pub owner_kind: &'static str,
    pub owner_id: &'a ResourceId,
    pub request_digest: &'a Sha256Digest,
    pub worker_process_generation_id: &'a ResourceId,
    pub provider_process_generation_id: Option<&'a ResourceId>,
    pub sandbox_identity_digest: Option<&'a Sha256Digest>,
    pub lease_generation: u64,
    pub artifact: &'a insight_platform_contracts::ArtifactRef,
    pub purpose_domain: &'static str,
    pub purpose_class: ArtifactObjectReadPurpose,
}

pub(crate) async fn load_authorized_artifact_object(
    transaction: &mut Transaction<'_, Postgres>,
    request: ArtifactObjectReadProjection<'_>,
) -> Result<AuthorizedArtifactObjectRead, RepositoryError> {
    let row = sqlx::query(
        r#"
        SELECT artifact.blob_id, artifact.purpose, artifact.classification,
               artifact.verified_media_type, artifact.metadata_schema_version,
               artifact.metadata, artifact.metadata_digest,
               blob.backend, blob.storage_binding_digest,
               blob.object_reference_ciphertext, blob.object_generation,
               blob.key_id, blob.encryption_domain_id, blob.content_digest,
               blob.size_bytes, blob.version AS blob_version
        FROM insight_platform.artifacts AS artifact
        JOIN insight_platform.artifact_blobs AS blob
          ON blob.tenant_id = artifact.tenant_id AND blob.blob_id = artifact.blob_id
        WHERE artifact.tenant_id = $1 AND artifact.artifact_id = $2
          AND artifact.state = 'ready' AND artifact.terminal_at IS NULL
          AND blob.state = 'verified' AND blob.deleted_at IS NULL
        "#,
    )
    .bind(request.tenant_id.to_string())
    .bind(request.artifact.artifact_id().to_string())
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::NotFound("Ready Artifact object"))?;
    let metadata = payload_from_row(
        &row,
        "metadata_schema_version",
        "metadata",
        "metadata_digest",
    )?;
    let current_display_name = metadata
        .value
        .get("display_name")
        .and_then(|value| value.as_str());
    let expected_purpose = match request.purpose_class {
        ArtifactObjectReadPurpose::Package => ArtifactPurpose::Package,
        ArtifactObjectReadPurpose::SandboxInput => {
            let purpose = row
                .try_get::<String, _>("purpose")?
                .parse::<ArtifactPurpose>()
                .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
            if !matches!(
                purpose,
                ArtifactPurpose::CapabilityInput | ArtifactPurpose::SandboxInput
            ) {
                return Err(RepositoryError::PermissionDenied);
            }
            purpose
        }
        ArtifactObjectReadPurpose::ModelInput => ArtifactPurpose::RunInput,
    };
    if row.try_get::<String, _>("purpose")? != expected_purpose.as_str()
        || row.try_get::<String, _>("classification")? != request.artifact.classification().as_str()
        || row
            .try_get::<Option<String>, _>("verified_media_type")?
            .as_deref()
            != Some(request.artifact.media_type())
        || row
            .try_get::<Option<String>, _>("content_digest")?
            .as_deref()
            != Some(request.artifact.content_digest().to_string().as_str())
        || row.try_get::<Option<i64>, _>("size_bytes")?
            != Some(i64::try_from(request.artifact.byte_length()).map_err(|_| {
                RepositoryError::CorruptRow("Artifact size exceeds bigint".to_owned())
            })?)
        || current_display_name != request.artifact.display_name()
    {
        return Err(RepositoryError::Conflict(
            "exact Artifact object projection",
        ));
    }
    let blob_id = row
        .try_get::<String, _>("blob_id")?
        .parse::<ResourceId>()
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    let storage_binding_digest = row
        .try_get::<String, _>("storage_binding_digest")?
        .parse::<Sha256Digest>()
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    let encryption_domain_id = row
        .try_get::<String, _>("encryption_domain_id")?
        .parse::<ResourceId>()
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    let backend: String = row.try_get("backend")?;
    let key_id: String = row.try_get("key_id")?;
    let object_generation: String = row
        .try_get::<Option<String>, _>("object_generation")?
        .ok_or_else(|| RepositoryError::CorruptRow("Blob generation is absent".to_owned()))?;
    let blob_version: i64 = row.try_get("blob_version")?;
    let object_reference_ciphertext: Vec<u8> = row.try_get("object_reference_ciphertext")?;
    let object_reference_ciphertext_digest = prefixed_sha256(&object_reference_ciphertext);
    if request.provider_process_generation_id.is_some() != request.sandbox_identity_digest.is_some()
    {
        return Err(RepositoryError::CorruptRow(
            "Artifact read physical binding is incomplete".to_owned(),
        ));
    }
    let mut authorization = serde_json::json!({
        "artifact": request.artifact,
        "backend": backend,
        "blob_id": blob_id,
        "blob_version": blob_version,
        "encryption_domain_id": encryption_domain_id,
        "authority_generation": request.lease_generation,
        "key_id": key_id,
        "object_generation": object_generation,
        "object_reference_ciphertext_digest": object_reference_ciphertext_digest,
        "owner_id": request.owner_id,
        "owner_kind": request.owner_kind,
        "purpose": request.purpose_domain,
        "request_digest": request.request_digest,
        "schema_version": 1,
        "storage_binding_digest": storage_binding_digest,
        "tenant_id": request.tenant_id,
        "worker_process_generation_id": request.worker_process_generation_id,
    });
    if let (Some(provider), Some(identity)) = (
        request.provider_process_generation_id,
        request.sandbox_identity_digest,
    ) {
        let object = authorization
            .as_object_mut()
            .ok_or_else(|| RepositoryError::CorruptRow("Artifact authorization".to_owned()))?;
        object.insert(
            "provider_process_generation_id".to_owned(),
            serde_json::to_value(provider)
                .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?,
        );
        object.insert(
            "sandbox_identity_digest".to_owned(),
            serde_json::to_value(identity)
                .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?,
        );
    }
    let authorization_digest: Sha256Digest = canonical_digest(&authorization)
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?
        .parse::<Sha256Digest>()
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    let authorized = AuthorizedArtifactObjectRead {
        tenant_id: request.tenant_id.clone(),
        blob_id,
        artifact: request.artifact.clone(),
        backend,
        storage_binding_digest,
        encryption_domain_id,
        key_id,
        object_reference_ciphertext: EncryptedArtifactObjectReference::new(
            object_reference_ciphertext,
        )
        .map_err(|_| RepositoryError::CorruptRow("Blob ciphertext is invalid".to_owned()))?,
        object_generation,
        authorization_digest,
    };
    authorized.validate().map_err(|_| {
        RepositoryError::CorruptRow("Artifact read projection is invalid".to_owned())
    })?;
    Ok(authorized)
}

fn prefixed_sha256(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut encoded = String::with_capacity("sha256:".len() + digest.len() * 2);
    encoded.push_str("sha256:");
    for byte in digest {
        use std::fmt::Write as _;
        write!(&mut encoded, "{byte:02x}").expect("writing to String cannot fail");
    }
    encoded
}

#[async_trait::async_trait]
impl WasiValueValidator for PgRepository {
    async fn validate(
        &self,
        request: WasiValueValidationRequest,
    ) -> Result<Sha256Digest, WasiValueValidationError> {
        let job_id = ResourceId::from_uuid_v7(ResourceKind::Job, request.sandbox_job_id.uuid())
            .map_err(|_| WasiValueValidationError::Invalid)?;
        let mut transaction = self
            .pool()
            .begin()
            .await
            .map_err(|_| WasiValueValidationError::Unavailable)?;
        let current = load_sandbox_job(
            &mut transaction,
            &request.tenant_id,
            &job_id,
            self.sandbox_limits(),
        )
        .await
        .map_err(classify_wasi_value_repository_error)?;
        let invocation = load_capability_invocation(
            &mut transaction,
            &request.tenant_id,
            &current.payload.request.invocation_id,
            false,
        )
        .await
        .map_err(classify_wasi_value_repository_error)?;
        let spec = load_exact_capability_interface_spec(
            &mut transaction,
            &request.tenant_id,
            &invocation.payload.admission,
        )
        .await
        .map_err(classify_wasi_value_repository_error)?;
        let lease = current
            .job
            .lease
            .as_ref()
            .ok_or(WasiValueValidationError::Invalid)?;
        let leased_request = current
            .payload
            .request
            .as_ref()
            .clone()
            .bind_lease_generation(current.job.lease_generation)
            .map_err(|_| WasiValueValidationError::Invalid)?;
        let (schema, maximum_bytes, direction_valid) = match request.direction {
            WasiValueDirection::Input => (
                &spec.input_schema,
                spec.execution_limits.maximum_input_bytes,
                current.payload.physical_state == SandboxJobState::Preparing
                    && leased_request.input_schema_digest == request.schema_digest
                    && spec.data_policy.permits_input(request.classification),
            ),
            WasiValueDirection::Output => (
                &spec.output_schema,
                spec.execution_limits.maximum_output_bytes,
                current.payload.physical_state == SandboxJobState::Collecting
                    && leased_request.output_schema_digest == request.schema_digest
                    && spec.data_policy.permits_output(
                        invocation.payload.admission.input.classification,
                        request.classification,
                    ),
            ),
        };
        if leased_request.request_digest != request.request_digest
            || leased_request.classification != request.classification
            || invocation.payload.current_job_id.as_ref() != Some(&job_id)
            || lease.worker_process_generation_id != request.worker_process_generation_id
            || lease.lease_generation != request.lease_generation
            || !direction_valid
        {
            return Err(WasiValueValidationError::Invalid);
        }
        let value = ValueRef::Inline {
            value: request.value.clone(),
        };
        validate_capability_value_against_schema(schema, &value, maximum_bytes)
            .map_err(|_| WasiValueValidationError::Invalid)?;
        canonical_digest(&serde_json::json!({
            "classification": request.classification,
            "direction": request.direction,
            "interface_revision": invocation.payload.admission.interface,
            "invocation_id": invocation.invocation_id,
            "job_id": job_id,
            "schema_digest": request.schema_digest,
            "request_digest": request.request_digest,
            "schema_version": 1,
            "tenant_id": request.tenant_id,
            "value": request.value,
        }))
        .map_err(|_| WasiValueValidationError::Invalid)?
        .parse()
        .map_err(|_| WasiValueValidationError::Invalid)
    }
}

#[async_trait::async_trait]
impl WasiGrantRevoker for PgRepository {
    async fn revoke_exact(
        &self,
        request: RevokeWasiSandboxGrants,
    ) -> Result<WasiGrantRevocationEvidence, WasiGrantRevocationError> {
        let job_id = ResourceId::from_uuid_v7(ResourceKind::Job, request.sandbox_job_id.uuid())
            .map_err(|_| WasiGrantRevocationError::Rejected)?;
        let mut transaction = self
            .pool()
            .begin()
            .await
            .map_err(|_| WasiGrantRevocationError::Unavailable)?;
        let database_now = database_now(&mut transaction)
            .await
            .map_err(|_| WasiGrantRevocationError::Unavailable)?;
        let current = load_sandbox_job(
            &mut transaction,
            &request.tenant_id,
            &job_id,
            self.sandbox_limits(),
        )
        .await
        .map_err(classify_wasi_grant_repository_error)?;
        require_sandbox_command_owner(&current, &request.sandbox_job_id, &request.tenant_id)
            .map_err(classify_wasi_grant_repository_error)?;
        let lease = current
            .job
            .lease
            .as_ref()
            .ok_or(WasiGrantRevocationError::Rejected)?;
        let leased_request = current
            .payload
            .request
            .as_ref()
            .clone()
            .bind_lease_generation(current.job.lease_generation)
            .map_err(|_| WasiGrantRevocationError::Rejected)?;
        if leased_request.request_digest != request.request_digest
            || leased_request.attempt_no != request.attempt_no
            || lease.worker_process_generation_id != request.worker_process_generation_id
            || lease.lease_generation != request.lease_generation
            || !matches!(
                current.payload.physical_state,
                SandboxJobState::Preparing
                    | SandboxJobState::Starting
                    | SandboxJobState::Running
                    | SandboxJobState::Collecting
                    | SandboxJobState::Cancelling
            )
        {
            return Err(WasiGrantRevocationError::Rejected);
        }
        let expected = u64::try_from(current.payload.request.artifact_grants.len())
            .map_err(|_| WasiGrantRevocationError::Rejected)?;
        let tenant_id = request.tenant_id.to_string();
        let sandbox_job_id = request.sandbox_job_id.to_string();
        release_and_confirm_sandbox_artifact_grants(
            &mut transaction,
            &tenant_id,
            &sandbox_job_id,
            expected,
            database_now,
        )
        .await
        .map_err(classify_wasi_grant_repository_error)?;
        let evidence_digest: Sha256Digest = canonical_digest(&serde_json::json!({
            "attempt_no": request.attempt_no,
            "lease_generation": request.lease_generation,
            "request_digest": request.request_digest,
            "sandbox_job_id": request.sandbox_job_id,
            "schema_version": 1,
            "tenant_id": request.tenant_id,
            "worker_process_generation_id": request.worker_process_generation_id,
        }))
        .map_err(|_| WasiGrantRevocationError::Rejected)?
        .parse()
        .map_err(|_| WasiGrantRevocationError::Rejected)?;
        transaction
            .commit()
            .await
            .map_err(|_| WasiGrantRevocationError::Unavailable)?;
        Ok(WasiGrantRevocationEvidence { evidence_digest })
    }
}

#[async_trait::async_trait]
impl MicroVmGrantRevoker for PgRepository {
    async fn revoke_exact(
        &self,
        request: RevokeMicroVmSandboxGrants,
    ) -> Result<MicroVmGrantRevocationEvidence, MicroVmGrantRevocationError> {
        request.validate()?;
        if request.workload_kind == MicroVmSandboxWorkloadKind::ManagedMcpSubscriptionSession {
            return revoke_managed_mcp_session_grants(self, request).await;
        }
        let job_id = ResourceId::from_uuid_v7(ResourceKind::Job, request.sandbox_job_id.uuid())
            .map_err(|_| MicroVmGrantRevocationError::Rejected)?;
        let mut transaction = self
            .pool()
            .begin()
            .await
            .map_err(|_| MicroVmGrantRevocationError::Unavailable)?;
        let database_now = database_now(&mut transaction)
            .await
            .map_err(|_| MicroVmGrantRevocationError::Unavailable)?;
        let current = load_sandbox_job(
            &mut transaction,
            &request.tenant_id,
            &job_id,
            self.sandbox_limits(),
        )
        .await
        .map_err(classify_micro_vm_grant_repository_error)?;
        require_sandbox_command_owner(&current, &request.sandbox_job_id, &request.tenant_id)
            .map_err(classify_micro_vm_grant_repository_error)?;
        let lease = current
            .job
            .lease
            .as_ref()
            .ok_or(MicroVmGrantRevocationError::Rejected)?;
        let leased_request = current
            .payload
            .request
            .as_ref()
            .clone()
            .bind_lease_generation(current.job.lease_generation)
            .map_err(|_| MicroVmGrantRevocationError::Rejected)?;
        if leased_request.request_digest != request.request_digest
            || leased_request.attempt_no != request.attempt_no
            || lease.worker_process_generation_id != request.executor_worker_process_generation_id
            || lease.lease_generation != request.lease_generation
            || !matches!(
                current.payload.physical_state,
                SandboxJobState::Preparing
                    | SandboxJobState::Starting
                    | SandboxJobState::Running
                    | SandboxJobState::Collecting
                    | SandboxJobState::Cancelling
            )
        {
            return Err(MicroVmGrantRevocationError::Rejected);
        }
        let expected = u64::try_from(current.payload.request.artifact_grants.len())
            .map_err(|_| MicroVmGrantRevocationError::Rejected)?;
        release_and_confirm_sandbox_artifact_grants(
            &mut transaction,
            &request.tenant_id.to_string(),
            &request.sandbox_job_id.to_string(),
            expected,
            database_now,
        )
        .await
        .map_err(classify_micro_vm_grant_repository_error)?;
        let evidence_digest: Sha256Digest = canonical_digest(&serde_json::json!({
            "attempt_no": request.attempt_no,
            "executor_worker_process_generation_id": request.executor_worker_process_generation_id,
            "lease_generation": request.lease_generation,
            "provider_process_generation_id": request.provider_process_generation_id,
            "request_digest": request.request_digest,
            "sandbox_identity_digest": request.sandbox_identity_digest,
            "sandbox_job_id": request.sandbox_job_id,
            "schema_version": 1,
            "tenant_id": request.tenant_id,
            "workload_kind": request.workload_kind,
        }))
        .map_err(|_| MicroVmGrantRevocationError::Rejected)?
        .parse()
        .map_err(|_| MicroVmGrantRevocationError::Rejected)?;
        transaction
            .commit()
            .await
            .map_err(|_| MicroVmGrantRevocationError::Unavailable)?;
        Ok(MicroVmGrantRevocationEvidence { evidence_digest })
    }
}

async fn revoke_managed_mcp_session_grants(
    repository: &PgRepository,
    request: RevokeMicroVmSandboxGrants,
) -> Result<MicroVmGrantRevocationEvidence, MicroVmGrantRevocationError> {
    let job_id = ResourceId::from_uuid_v7(ResourceKind::Job, request.sandbox_job_id.uuid())
        .map_err(|_| MicroVmGrantRevocationError::Rejected)?;
    let mut transaction = repository
        .pool()
        .begin()
        .await
        .map_err(|_| MicroVmGrantRevocationError::Unavailable)?;
    let database_now = database_now(&mut transaction)
        .await
        .map_err(|_| MicroVmGrantRevocationError::Unavailable)?;
    let (job, payload, _) = load_managed_mcp_sandbox_session_job(
        &mut transaction,
        &request.tenant_id,
        &job_id,
        repository.sandbox_limits(),
        true,
    )
    .await
    .map_err(classify_micro_vm_grant_repository_error)?;
    let lease = job
        .lease
        .as_ref()
        .ok_or(MicroVmGrantRevocationError::Rejected)?;
    let ready_identity_matches = payload
        .ready_binding
        .as_ref()
        .is_none_or(|ready| ready.sandbox_identity_digest == request.sandbox_identity_digest);
    if payload.request.identity.sandbox_job_id != request.sandbox_job_id
        || payload.request.request_digest != request.request_digest
        || job.attempt_count != request.attempt_no
        || lease.worker_process_generation_id != request.executor_worker_process_generation_id
        || lease.lease_generation != request.lease_generation
        || !matches!(
            payload.physical_state,
            SandboxJobState::Preparing | SandboxJobState::Starting | SandboxJobState::Running
        )
        || !ready_identity_matches
    {
        return Err(MicroVmGrantRevocationError::Rejected);
    }
    let expected = u64::try_from(payload.request.artifact_grants.len())
        .map_err(|_| MicroVmGrantRevocationError::Rejected)?;
    release_and_confirm_sandbox_artifact_grants(
        &mut transaction,
        &request.tenant_id.to_string(),
        &request.sandbox_job_id.to_string(),
        expected,
        database_now,
    )
    .await
    .map_err(classify_micro_vm_grant_repository_error)?;
    let evidence_digest: Sha256Digest = canonical_digest(&serde_json::json!({
        "attempt_no": request.attempt_no,
        "executor_worker_process_generation_id": request.executor_worker_process_generation_id,
        "lease_generation": request.lease_generation,
        "provider_process_generation_id": request.provider_process_generation_id,
        "request_digest": request.request_digest,
        "sandbox_identity_digest": request.sandbox_identity_digest,
        "sandbox_job_id": request.sandbox_job_id,
        "schema_version": 1,
        "tenant_id": request.tenant_id,
        "workload_kind": request.workload_kind,
    }))
    .map_err(|_| MicroVmGrantRevocationError::Rejected)?
    .parse()
    .map_err(|_| MicroVmGrantRevocationError::Rejected)?;
    transaction
        .commit()
        .await
        .map_err(|_| MicroVmGrantRevocationError::Unavailable)?;
    Ok(MicroVmGrantRevocationEvidence { evidence_digest })
}

fn classify_wasi_value_repository_error(error: RepositoryError) -> WasiValueValidationError {
    match error {
        RepositoryError::Database(_) => WasiValueValidationError::Unavailable,
        _ => WasiValueValidationError::Invalid,
    }
}

fn classify_artifact_read_repository_error(
    error: RepositoryError,
) -> ArtifactObjectReadAuthorityError {
    match error {
        RepositoryError::Database(_) => ArtifactObjectReadAuthorityError::Unavailable,
        RepositoryError::NotFound(_) => ArtifactObjectReadAuthorityError::NotFound,
        RepositoryError::PermissionDenied => ArtifactObjectReadAuthorityError::Denied,
        _ => ArtifactObjectReadAuthorityError::InvalidEvidence,
    }
}

fn classify_wasi_grant_repository_error(error: RepositoryError) -> WasiGrantRevocationError {
    match error {
        RepositoryError::Database(_) => WasiGrantRevocationError::Unavailable,
        _ => WasiGrantRevocationError::Rejected,
    }
}

fn classify_micro_vm_grant_repository_error(error: RepositoryError) -> MicroVmGrantRevocationError {
    match error {
        RepositoryError::Database(_) => MicroVmGrantRevocationError::Unavailable,
        _ => MicroVmGrantRevocationError::Rejected,
    }
}

#[derive(Debug, Clone)]
pub struct ResolveSandboxStopSignals {
    pub tenant_id: ResourceId,
    pub source_event_id: ResourceId,
    pub executor_worker_process_generation_id: ResourceId,
    pub limit: u16,
}

impl ResolveSandboxStopSignals {
    fn validate(&self, maximum_batch: u16) -> Result<(), RepositoryError> {
        if self.tenant_id.kind() != ResourceKind::Tenant
            || self.source_event_id.kind() != ResourceKind::Event
            || self.executor_worker_process_generation_id.kind()
                != ResourceKind::WorkerProcessGeneration
            || self.limit == 0
            || self.limit > maximum_batch
        {
            return Err(RepositoryError::InvalidInput(
                "Sandbox stop-signal resolution is outside the platform bound".to_owned(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct ScanSandboxStopSignals {
    pub shard: SafetyScanShard,
    pub after: Option<SafetyScanCursor>,
    pub executor_worker_process_generation_id: ResourceId,
    pub limit: u16,
}

#[derive(Debug, Clone)]
pub struct ScanExpiredSandboxLeases {
    pub shard: SafetyScanShard,
    pub after: Option<SafetyScanCursor>,
    pub limit: u16,
}

#[derive(Debug, Clone)]
pub struct ScanPendingSandboxCapabilityOutcomes {
    pub shard: SafetyScanShard,
    pub after: Option<SafetyScanCursor>,
    pub limit: u16,
}

impl ScanPendingSandboxCapabilityOutcomes {
    fn validate(&self, maximum_batch: u16, maximum_shards: u16) -> Result<(), RepositoryError> {
        validate_safety_scan_request(
            self.shard,
            self.after.as_ref(),
            ResourceKind::Job,
            self.limit,
            usize::from(self.limit),
            maximum_batch,
            maximum_shards,
        )
    }
}

impl ScanExpiredSandboxLeases {
    fn validate(&self, maximum_batch: u16, maximum_shards: u16) -> Result<(), RepositoryError> {
        validate_safety_scan_request(
            self.shard,
            self.after.as_ref(),
            ResourceKind::Job,
            self.limit,
            usize::from(self.limit),
            maximum_batch,
            maximum_shards,
        )
    }
}

impl ScanSandboxStopSignals {
    fn validate(&self, maximum_batch: u16, maximum_shards: u16) -> Result<(), RepositoryError> {
        if self.executor_worker_process_generation_id.kind()
            != ResourceKind::WorkerProcessGeneration
        {
            return Err(RepositoryError::InvalidInput(
                "Sandbox control scan Worker identity is invalid".to_owned(),
            ));
        }
        validate_safety_scan_request(
            self.shard,
            self.after.as_ref(),
            ResourceKind::Job,
            self.limit,
            usize::from(self.limit),
            maximum_batch,
            maximum_shards,
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
struct CapabilityControlEventPayload {
    schema_version: u32,
    control_kind: CapabilityControlKind,
    job_id: Option<ResourceId>,
    state: InvocationState,
    task_id: Option<ResourceId>,
}

impl PgRepository {
    /// Finds terminal Sandbox Jobs whose current logical Capability owner has not yet consumed the
    /// terminal Event. Discovery is read-only; merge remains the sole optimistic mutation.
    pub async fn scan_pending_sandbox_capability_outcomes(
        &self,
        command: ScanPendingSandboxCapabilityOutcomes,
    ) -> Result<SafetyScanPage<PendingSandboxCapabilityOutcome>, RepositoryError> {
        command.validate(self.recovery_batch_limit(), self.recovery_shard_limit())?;
        let mut transaction = self.pool().begin().await?;
        let rows = sqlx::query(
            r#"
            SELECT job.*, event.event_id AS source_event_id,
                   event.aggregate_version AS source_job_version,
                   event.payload_digest AS source_event_payload_digest,
                   event.occurred_at AS source_event_occurred_at,
                   event.occurred_at AS scan_sort_at,
                   invocation.version AS invocation_version,
                   invocation.deadline AS invocation_deadline
            FROM insight_platform.jobs AS job
            JOIN insight_platform.invocations AS invocation
              ON invocation.tenant_id = job.tenant_id
             AND invocation.invocation_id = job.invocation_id
            JOIN insight_platform.events AS event
              ON event.tenant_id = job.tenant_id
             AND event.aggregate_kind = 'job'
             AND event.aggregate_id = job.job_id
             AND event.aggregate_version = job.version
             AND event.event_type IN ('sandbox.job.completed', 'sandbox.job.failed')
            WHERE job.work_class = 'sandbox' AND job.owner_kind = 'sandbox_job'
              AND (
                  (job.state IN ('succeeded', 'failed', 'cancelled', 'timed_out')
                   AND job.terminal_at IS NOT NULL)
                  OR (job.state = 'reconciliation_required' AND job.terminal_at IS NULL)
              )
              AND job.worker_id IS NULL
              AND invocation.invocation_kind = 'capability'
              AND invocation.state IN ('deferred', 'cancelling')
              AND invocation.payload ->> 'current_job_id' = job.job_id
              AND mod(('x' || right(job.job_id, 8))::bit(32)::bigint, $2) = $1
              AND (
                  $3::timestamptz IS NULL OR
                  (event.occurred_at, job.tenant_id, job.job_id) >
                      ($3::timestamptz, $4::text, $5::text)
              )
            ORDER BY event.occurred_at, job.tenant_id, job.job_id
            LIMIT $6
            "#,
        )
        .bind(i64::from(command.shard.index))
        .bind(i64::from(command.shard.count))
        .bind(command.after.as_ref().map(|cursor| cursor.sort_at))
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
                .map(|cursor| cursor.item_id.to_string()),
        )
        .bind(i64::from(command.limit))
        .fetch_all(&mut *transaction)
        .await?;
        let scanned_count = rows.len();
        let last_cursor = rows
            .last()
            .map(|row| safety_scan_cursor_from_row(row, "job_id", ResourceKind::Job))
            .transpose()?;
        let records = rows
            .into_iter()
            .map(|row| sandbox_capability_outcome_candidate(row, self.sandbox_limits()))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(safety_scan_page(
            records,
            scanned_count,
            command.limit,
            last_cursor,
        ))
    }

    /// Returns exact expired Sandbox lease generations for a bounded recovery worker. This scan is
    /// read-only; the later recovery command is the single optimistic first-winner mutation.
    pub async fn scan_expired_sandbox_leases(
        &self,
        command: ScanExpiredSandboxLeases,
    ) -> Result<SafetyScanPage<ExpiredSandboxLease>, RepositoryError> {
        command.validate(self.recovery_batch_limit(), self.recovery_shard_limit())?;
        let mut transaction = self.pool().begin().await?;
        let database_now = database_now(&mut transaction).await?;
        let rows = sqlx::query(
            r#"
            SELECT job.*, job.lease_expires_at AS scan_sort_at
            FROM insight_platform.jobs AS job
            WHERE job.work_class = 'sandbox' AND job.owner_kind = 'sandbox_job'
              AND job.state IN ('leased', 'running', 'cancelling')
              AND job.lease_expires_at <= $1 AND job.terminal_at IS NULL
              AND job.worker_id IS NOT NULL AND job.lease_epoch > 0
              AND job.payload ->> 'workload_kind' = 'capability_execution'
              AND mod(('x' || right(job.job_id, 8))::bit(32)::bigint, $4) = $3
              AND (
                  $5::timestamptz IS NULL OR
                  (job.lease_expires_at, job.tenant_id, job.job_id) >
                      ($5::timestamptz, $6::text, $7::text)
              )
            ORDER BY job.lease_expires_at, job.tenant_id, job.job_id
            LIMIT $2
            "#,
        )
        .bind(database_now)
        .bind(i64::from(command.limit))
        .bind(i64::from(command.shard.index))
        .bind(i64::from(command.shard.count))
        .bind(command.after.as_ref().map(|cursor| cursor.sort_at))
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
                .map(|cursor| cursor.item_id.to_string()),
        )
        .fetch_all(&mut *transaction)
        .await?;
        let scanned_count = rows.len();
        let last_cursor = rows
            .last()
            .map(|row| safety_scan_cursor_from_row(row, "job_id", ResourceKind::Job))
            .transpose()?;
        let mut leases = Vec::with_capacity(rows.len());
        for row in rows {
            let record = job_from_row(row)?;
            let job = job_projection(&record)?;
            let payload: SandboxExecutionJobPayload =
                decode_sandbox_capability_payload(&record.payload)?;
            payload
                .validate_for(&job, self.sandbox_limits())
                .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
            let expected_shared_state = match payload.physical_state {
                SandboxJobState::Accepted => "leased",
                SandboxJobState::Preparing
                | SandboxJobState::Starting
                | SandboxJobState::Running
                | SandboxJobState::Collecting => "running",
                SandboxJobState::Cancelling => "cancelling",
                _ => {
                    return Err(RepositoryError::CorruptRow(
                        "terminal Sandbox payload retained an active lease".to_owned(),
                    ));
                }
            };
            if record.state != expected_shared_state {
                return Err(RepositoryError::CorruptRow(
                    "Sandbox physical/shared lease state diverged".to_owned(),
                ));
            }
            let lease = job.lease.as_ref().ok_or_else(|| {
                RepositoryError::CorruptRow("expired Sandbox Job has no lease".to_owned())
            })?;
            let request = payload
                .request
                .as_ref()
                .clone()
                .bind_lease_generation(job.lease_generation)
                .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
            let expired = ExpiredSandboxLease {
                tenant_id: job.tenant_id.clone(),
                sandbox_job_id: payload.request.sandbox_job_id.clone(),
                invocation_id: payload.request.invocation_id.clone(),
                job_id: job.job_id.clone(),
                request,
                observed_job_version: job.version,
                observed_lease_generation: job.lease_generation,
                previous_worker_process_generation_id: lease.worker_process_generation_id.clone(),
                lease_expires_at: lease.expires_at,
                database_observed_at: database_now,
                physical_state: payload.physical_state,
                executor_identity_digest: payload.executor_identity_digest.clone(),
                attestor_route: payload.attestor_route.clone(),
            };
            expired.validate(self.sandbox_limits())?;
            leases.push(expired);
        }
        transaction.commit().await?;
        Ok(safety_scan_page(
            leases,
            scanned_count,
            command.limit,
            last_cursor,
        ))
    }

    /// Claims only Sandbox Jobs whose owning Capability Invocation is still executable and whose
    /// exact Capability/Runtime/Package/Profile gates remain enabled. Generic Job claim is not a
    /// supported Sandbox execution path.
    pub async fn claim_sandbox_jobs(
        &self,
        command: ClaimSandboxJobs,
    ) -> Result<Vec<ClaimedSandboxJob>, RepositoryError> {
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
            JOIN insight_platform.invocations AS invocation
              ON invocation.tenant_id = job.tenant_id
             AND invocation.invocation_id = job.invocation_id
             AND invocation.invocation_kind = 'capability'
            AND invocation.state = 'deferred'
            WHERE job.work_class = 'sandbox' AND job.owner_kind = 'sandbox_job'
              AND job.state = 'ready' AND job.terminal_at IS NULL
              AND job.worker_id IS NULL AND job.scheduled_at <= $1
              AND job.deadline > $1
              AND job.payload ->> 'workload_kind' = 'capability_execution'
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
            let tenant_id: String = candidate.try_get("tenant_id")?;
            let job_id: String = candidate.try_get("job_id")?;
            let invocation_id: String = candidate
                .try_get::<Option<String>, _>("invocation_id")?
                .ok_or_else(|| {
                    RepositoryError::CorruptRow(
                        "Sandbox claim candidate has no Invocation".to_owned(),
                    )
                })?;
            let invocation_state = sqlx::query_scalar::<_, String>(
                r#"
                SELECT state
                FROM insight_platform.invocations
                WHERE tenant_id = $1 AND invocation_id = $2
                  AND invocation_kind = 'capability'
                FOR SHARE
                "#,
            )
            .bind(&tenant_id)
            .bind(&invocation_id)
            .fetch_optional(&mut *transaction)
            .await?;
            if invocation_state.as_deref() != Some("deferred") {
                continue;
            }
            let row = sqlx::query(
                r#"
                SELECT *
                FROM insight_platform.jobs
                WHERE tenant_id = $1 AND job_id = $2
                  AND work_class = 'sandbox' AND owner_kind = 'sandbox_job'
                  AND invocation_id = $3 AND state = 'ready'
                  AND terminal_at IS NULL AND worker_id IS NULL
                  AND scheduled_at <= $4 AND deadline > $4
                FOR UPDATE SKIP LOCKED
                "#,
            )
            .bind(&tenant_id)
            .bind(&job_id)
            .bind(&invocation_id)
            .bind(database_now)
            .fetch_optional(&mut *transaction)
            .await?;
            let Some(row) = row else {
                continue;
            };
            let current = job_from_row(row)?;
            let current_job = job_projection(&current)?;
            let payload: SandboxExecutionJobPayload =
                decode_sandbox_capability_payload(&current.payload)?;
            payload
                .validate_for(&current_job, self.sandbox_limits())
                .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
            if payload.physical_state != SandboxJobState::Accepted
                || payload.request.invocation_id.to_string() != invocation_id
                || payload.request.request_digest.to_string() != current.request_digest
                || payload.request.executor_worker_manifest_digest != command.worker_manifest_digest
                || payload.request.isolation_backend_contract_digest
                    != command.isolation_backend_contract_digest
            {
                return Err(RepositoryError::CorruptRow(
                    "Sandbox claim candidate binding is invalid".to_owned(),
                ));
            }
            let tenant_id = tenant_id
                .parse::<ResourceId>()
                .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
            let invocation_id = invocation_id
                .parse::<ResourceId>()
                .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
            let invocation =
                load_capability_invocation(&mut transaction, &tenant_id, &invocation_id, false)
                    .await?;
            if invocation.state != InvocationState::Deferred
                || invocation.version != payload.request.expected_invocation_version + 1
                || invocation.payload.current_job_id.as_ref() != Some(&payload.request.job_id)
            {
                continue;
            }
            verify_sandbox_exact_bindings(&mut transaction, &payload.request, &invocation).await?;
            let next = decide_claim(
                &current_job,
                database_now,
                command.worker_process_generation_id.clone(),
                lease_token_digest.clone(),
                LeasePolicy {
                    requested_milliseconds: command.lease_milliseconds,
                    hard_maximum_milliseconds: u64::try_from(MAX_JOB_LEASE_MILLISECONDS)
                        .expect("positive Sandbox lease hard maximum"),
                },
            )?;
            let lease = next.lease.as_ref().ok_or_else(|| {
                RepositoryError::CorruptRow("Sandbox claim produced no lease".to_owned())
            })?;
            let row = sqlx::query(
                r#"
                UPDATE insight_platform.jobs
                SET state = $4, version = $5, lease_epoch = $6,
                    worker_id = $7, lease_token_digest = $8,
                    lease_expires_at = $9, heartbeat_at = $10,
                    updated_at = $10
                WHERE tenant_id = $1 AND job_id = $2 AND version = $3
                  AND state = 'ready' AND worker_id IS NULL
                RETURNING *
                "#,
            )
            .bind(&current.tenant_id)
            .bind(&current.job_id)
            .bind(current.version)
            .bind(next.state.as_str())
            .bind(as_i64(next.version, "Sandbox Job version")?)
            .bind(as_i64(next.lease_generation, "Sandbox lease generation")?)
            .bind(lease.worker_process_generation_id.to_string())
            .bind(lease.token_digest.to_string())
            .bind(lease.expires_at)
            .bind(lease.heartbeat_at)
            .fetch_one(&mut *transaction)
            .await?;
            let leased = job_from_row(row)?;
            let usage_reservation_id = leased
                .quota_reservation_id
                .as_deref()
                .ok_or_else(|| {
                    RepositoryError::CorruptRow("Sandbox Job has no usage reservation".to_owned())
                })?
                .parse::<ResourceId>()
                .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
            let request = payload
                .request
                .as_ref()
                .clone()
                .bind_lease_generation(next.lease_generation)
                .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
            let claimed_job = ClaimedSandboxJob {
                request,
                fence: insight_platform_jobs::JobFence {
                    expected_version: next.version,
                    worker_process_generation_id: lease.worker_process_generation_id.clone(),
                    lease_generation: next.lease_generation,
                    token_digest: lease.token_digest.clone(),
                },
                usage_reservation_id,
            };
            claimed_job
                .validate_at(database_now, self.sandbox_limits())
                .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
            claimed.push(claimed_job);
        }
        transaction.commit().await?;
        Ok(claimed)
    }

    /// Resolves a committed Capability control event into exact signals for locally leased
    /// Sandbox Jobs. The event and Invocation remain the only durable control authority.
    pub async fn resolve_sandbox_stop_signals(
        &self,
        command: ResolveSandboxStopSignals,
    ) -> Result<Vec<SandboxStopSignal>, RepositoryError> {
        command.validate(self.recovery_batch_limit())?;
        let rows = sqlx::query(
            r#"
            SELECT job.*,
                   source.event_id AS source_event_id,
                   source.aggregate_version AS source_invocation_version,
                   source.event_type AS source_event_type,
                   source.payload_schema_version AS source_payload_schema_version,
                   source.payload AS source_payload,
                   source.payload_digest AS source_payload_digest,
                   source.occurred_at AS scan_sort_at
            FROM insight_platform.events AS source
            JOIN insight_platform.invocations AS invocation
              ON invocation.tenant_id = source.tenant_id
             AND invocation.invocation_id = source.aggregate_id
             AND invocation.invocation_kind = 'capability'
             AND invocation.version >= source.aggregate_version
             AND invocation.state IN (
                 'cancelling', 'cancelled', 'timed_out', 'reconciliation_required'
             )
            JOIN insight_platform.jobs AS job
              ON job.tenant_id = invocation.tenant_id
             AND job.invocation_id = invocation.invocation_id
             AND job.work_class = 'sandbox'
             AND job.owner_kind = 'sandbox_job'
             AND job.state IN ('leased', 'running', 'cancelling')
             AND job.worker_id = $3
             AND job.lease_epoch > 0
             AND job.terminal_at IS NULL
            WHERE source.tenant_id = $1 AND source.event_id = $2
              AND source.aggregate_kind = 'capability_invocation'
              AND source.aggregate_version IS NOT NULL
              AND source.event_type IN (
                  'capability.cancelling', 'capability.cancelled',
                  'capability.timed_out', 'capability.reconciliation_required'
              )
            ORDER BY job.job_id
            LIMIT $4
            "#,
        )
        .bind(command.tenant_id.to_string())
        .bind(command.source_event_id.to_string())
        .bind(command.executor_worker_process_generation_id.to_string())
        .bind(i64::from(command.limit))
        .fetch_all(self.pool())
        .await?;
        rows.into_iter()
            .map(|row| sandbox_stop_signal_from_row(row, self.sandbox_limits()))
            .collect()
    }

    /// Bounded, sharded read-only recovery scan for committed control events whose wake hint was
    /// lost. Re-delivery is idempotent and does not create a second Sandbox control state.
    pub async fn scan_sandbox_stop_signals(
        &self,
        command: ScanSandboxStopSignals,
    ) -> Result<SafetyScanPage<SandboxStopSignal>, RepositoryError> {
        command.validate(self.recovery_batch_limit(), self.recovery_shard_limit())?;
        let rows = sqlx::query(
            r#"
            SELECT job.*,
                   source.event_id AS source_event_id,
                   source.aggregate_version AS source_invocation_version,
                   source.event_type AS source_event_type,
                   source.payload_schema_version AS source_payload_schema_version,
                   source.payload AS source_payload,
                   source.payload_digest AS source_payload_digest,
                   source.occurred_at AS scan_sort_at
            FROM insight_platform.jobs AS job
            JOIN insight_platform.invocations AS invocation
              ON invocation.tenant_id = job.tenant_id
             AND invocation.invocation_id = job.invocation_id
             AND invocation.invocation_kind = 'capability'
             AND invocation.state IN (
                 'cancelling', 'cancelled', 'timed_out', 'reconciliation_required'
             )
            JOIN LATERAL (
                SELECT event.event_id, event.aggregate_version, event.event_type,
                       event.payload_schema_version, event.payload, event.payload_digest,
                       event.occurred_at
                FROM insight_platform.events AS event
                WHERE event.tenant_id = invocation.tenant_id
                  AND event.aggregate_kind = 'capability_invocation'
                  AND event.aggregate_id = invocation.invocation_id
                  AND event.aggregate_version IS NOT NULL
                  AND event.aggregate_version <= invocation.version
                  AND event.event_type IN (
                      'capability.cancelling', 'capability.cancelled',
                      'capability.timed_out', 'capability.reconciliation_required'
                  )
                ORDER BY event.aggregate_version DESC, event.occurred_at DESC, event.event_id DESC
                LIMIT 1
            ) AS source ON TRUE
            WHERE job.work_class = 'sandbox' AND job.owner_kind = 'sandbox_job'
              AND job.state IN ('leased', 'running', 'cancelling')
              AND job.worker_id = $1 AND job.lease_epoch > 0
              AND job.terminal_at IS NULL
              AND mod(('x' || right(job.job_id, 8))::bit(32)::bigint, $3) = $2
              AND (
                  $4::timestamptz IS NULL OR
                  (source.occurred_at, job.tenant_id, job.job_id) >
                      ($4::timestamptz, $5::text, $6::text)
              )
            ORDER BY source.occurred_at, job.tenant_id, job.job_id
            LIMIT $7
            "#,
        )
        .bind(command.executor_worker_process_generation_id.to_string())
        .bind(i64::from(command.shard.index))
        .bind(i64::from(command.shard.count))
        .bind(command.after.as_ref().map(|cursor| cursor.sort_at))
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
                .map(|cursor| cursor.item_id.to_string()),
        )
        .bind(i64::from(command.limit))
        .fetch_all(self.pool())
        .await?;
        let scanned_count = rows.len();
        let last_cursor = rows
            .last()
            .map(|row| safety_scan_cursor_from_row(row, "job_id", ResourceKind::Job))
            .transpose()?;
        let signals = rows
            .into_iter()
            .map(|row| sandbox_stop_signal_from_row(row, self.sandbox_limits()))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(safety_scan_page(
            signals,
            scanned_count,
            command.limit,
            last_cursor,
        ))
    }

    /// Merges one already-terminal physical Sandbox Job into its logical Capability Invocation.
    /// This controller command never updates the Job and therefore cannot create a second
    /// attempt/lease/terminal authority.
    pub async fn merge_sandbox_capability_outcome(
        &self,
        command: MergeSandboxCapabilityOutcome,
    ) -> Result<CommandOutcome<CapabilityInvocationRecord>, RepositoryError> {
        let mut transaction = self.pool().begin().await?;
        let database_now = database_now(&mut transaction).await?;
        command.validate_at(database_now)?;
        let source =
            lock_sandbox_terminal_source(&mut transaction, &command, self.sandbox_limits()).await?;
        let receipt_payload = TypedPayload::new(
            1,
            &serde_json::json!({
                "controller_process_generation_id": command.audit.controller_process_generation_id,
                "expected_invocation_version": command.expected_invocation_version,
                "job_id": command.job_id,
                "sandbox_job_id": command.sandbox_job_id,
                "source_event_id": command.audit.source_event_id,
                "source_job_version": command.audit.source_job_version,
            }),
        )?;
        if claim_sandbox_outcome_merge_receipt(&mut transaction, &command, &receipt_payload).await?
        {
            let invocation = load_capability_invocation(
                &mut transaction,
                &command.audit.tenant_id,
                &command.invocation_id,
                false,
            )
            .await?;
            transaction.commit().await?;
            return Ok(CommandOutcome::Replayed(invocation));
        }

        let current = load_capability_invocation(
            &mut transaction,
            &command.audit.tenant_id,
            &command.invocation_id,
            true,
        )
        .await?;
        if current.version != command.expected_invocation_version
            || current.payload.current_job_id.as_ref() != Some(&command.job_id)
            || source.payload.request.invocation_id != current.invocation_id
            || source.payload.request.request_digest != command.sandbox_request_digest
        {
            return Err(RepositoryError::Conflict(
                "Sandbox Capability outcome first-winner",
            ));
        }
        let mut normalized =
            normalize_sandbox_capability_outcome(&source.payload, database_now, &current)?;
        if let DetachedCapabilityJobOutcome::Completed(output) = &mut normalized {
            let interface = load_exact_capability_interface_spec(
                &mut transaction,
                &command.audit.tenant_id,
                &current.payload.admission,
            )
            .await?;
            if !interface.data_policy.permits_output(
                current.payload.admission.input.classification,
                output.classification,
            ) {
                return Err(RepositoryError::InvalidInput(
                    "Sandbox output violates the frozen data-flow policy".to_owned(),
                ));
            }
            validate_capability_value_against_schema(
                &interface.output_schema,
                &output.value,
                interface.execution_limits.maximum_output_bytes,
            )?;
            output.validation_evidence_digest = canonical_digest(&serde_json::json!({
                "classification": output.classification,
                "interface_revision": current.payload.admission.interface,
                "invocation_id": current.invocation_id,
                "job_id": source.job.job_id,
                "output_content_digest": output.content_digest,
                "output_schema_digest": output.schema_digest,
                "sandbox_request_digest": source.payload.request.request_digest,
                "schema_version": 1,
                "tenant_id": current.tenant_id,
            }))
            .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?
            .parse()
            .map_err(|failure| {
                RepositoryError::InvalidInput(format!(
                    "Sandbox validation evidence digest is invalid: {failure}"
                ))
            })?;
        }
        let decision = decide_detached_job_outcome(
            &current,
            detached_sandbox_source_kind(&source.payload.request.execution_source),
            &source.job,
            source.payload.request.attempt_no,
            &normalized,
            database_now,
            self.invocation_limits(),
        )?;
        if let Some(output) = decision.output.as_ref() {
            if matches!(
                source.payload.outcome,
                Some(SandboxExecutionOutcome::ManagedMcp(_))
            ) {
                insert_capability_value_and_reference(
                    &mut transaction,
                    &decision.invocation,
                    output,
                    database_now,
                )
                .await?;
            } else {
                insert_sandbox_capability_value(
                    &mut transaction,
                    &decision.invocation,
                    output,
                    &source.payload,
                    database_now,
                )
                .await?;
            }
        }
        if let Some(input) = decision.input_request.as_ref() {
            insert_capability_input_task(
                &mut transaction,
                &decision.invocation,
                &source.job,
                input,
                database_now,
            )
            .await?;
        }
        update_capability_invocation(&mut transaction, &current, &decision.invocation).await?;
        let (event_type, disposition) = match decision.invocation.state {
            InvocationState::Succeeded => ("capability.completed", "completed"),
            InvocationState::RetryScheduled => ("capability.waiting", "retry_scheduled"),
            InvocationState::Deferred => ("capability.waiting", "deferred"),
            InvocationState::AwaitingInput => ("capability.input_required", "input_required"),
            InvocationState::Failed => ("capability.failed", "failed"),
            InvocationState::Cancelled => ("capability.cancelled", "cancelled"),
            InvocationState::TimedOut => ("capability.timed_out", "timed_out"),
            InvocationState::ReconciliationRequired => (
                "capability.reconciliation_required",
                "reconciliation_required",
            ),
            _ => {
                return Err(RepositoryError::CorruptRow(
                    "Sandbox Capability merge produced an unsupported state".to_owned(),
                ));
            }
        };
        append_scheduler_event(
            &mut transaction,
            &command.audit.tenant_id.to_string(),
            &command.audit.event_id,
            &command.audit.outbox_id,
            "capability_invocation",
            &command.invocation_id.to_string(),
            as_i64(decision.invocation.version, "Invocation version")?,
            Some(&current.run_id.to_string()),
            event_type,
            &TypedPayload::new(
                1,
                &serde_json::json!({
                    "job_id": command.job_id,
                    "job_state": source.job.state,
                    "sandbox_job_id": command.sandbox_job_id,
                    "source_event_id": command.audit.source_event_id,
                    "state": decision.invocation.state,
                }),
            )?,
        )
        .await?;
        terminalize_sandbox_outcome_merge_receipt(&mut transaction, &command, disposition).await?;
        transaction.commit().await?;
        Ok(CommandOutcome::Applied(decision.invocation))
    }
}

#[async_trait::async_trait]
impl SandboxControlSignalSource for PgRepository {
    type Error = RepositoryError;

    async fn resolve_committed_control_event(
        &self,
        query: ResolveSandboxControlEvent,
    ) -> Result<Vec<SandboxStopSignal>, Self::Error> {
        PgRepository::resolve_sandbox_stop_signals(
            self,
            ResolveSandboxStopSignals {
                tenant_id: query.tenant_id,
                source_event_id: query.source_event_id,
                executor_worker_process_generation_id: query.executor_worker_process_generation_id,
                limit: query.limit,
            },
        )
        .await
    }

    async fn recover_committed_control_signals(
        &self,
        query: RecoverSandboxControlSignals,
    ) -> Result<SandboxControlSignalPage, Self::Error> {
        let page = PgRepository::scan_sandbox_stop_signals(
            self,
            ScanSandboxStopSignals {
                shard: SafetyScanShard {
                    index: query.shard.index,
                    count: query.shard.count,
                },
                after: query.after.map(|cursor| SafetyScanCursor {
                    sort_at: cursor.sort_at,
                    tenant_id: cursor.tenant_id,
                    item_id: cursor.job_id,
                }),
                executor_worker_process_generation_id: query.executor_worker_process_generation_id,
                limit: query.limit,
            },
        )
        .await?;
        Ok(SandboxControlSignalPage {
            signals: page.records,
            next_cursor: page.next_cursor.map(|cursor| SandboxControlScanCursor {
                sort_at: cursor.sort_at,
                tenant_id: cursor.tenant_id,
                job_id: cursor.item_id,
            }),
            exhausted: page.exhausted,
        })
    }
}

#[async_trait::async_trait]
impl SandboxControlAuthority for PgRepository {
    type Error = RepositoryError;

    async fn stop_unclaimed_sandbox_job(
        &self,
        command: StopUnclaimedSandboxJob,
    ) -> Result<SandboxPrestartControlOutcome, Self::Error> {
        let mut transaction = self.pool().begin().await?;
        let database_now = database_now(&mut transaction).await?;
        command.validate_at(database_now)?;
        lock_sandbox_control_source(&mut transaction, &command).await?;
        let current = load_sandbox_job(
            &mut transaction,
            &command.audit.tenant_id,
            &command.job_id,
            self.sandbox_limits(),
        )
        .await?;
        require_sandbox_command_owner(&current, &command.sandbox_job_id, &command.audit.tenant_id)?;
        if current.record.invocation_id.as_deref()
            != Some(command.invocation_id.to_string().as_str())
            || current.payload.request.request_digest != command.sandbox_request_digest
        {
            return Err(RepositoryError::Conflict(
                "Sandbox pre-start control binding",
            ));
        }

        let existing_receipt = load_sandbox_controller_receipt(&mut transaction, &command).await?;
        if is_sandbox_physical_terminal(current.payload.physical_state) {
            let decision = SandboxPhaseDecision {
                job: current.job,
                payload: current.payload,
            };
            transaction.commit().await?;
            if existing_receipt {
                require_prestart_control_replay(&decision, &command)?;
                return Ok(SandboxPrestartControlOutcome::Replayed(decision));
            }
            return Ok(SandboxPrestartControlOutcome::AlreadyTerminal(
                decision.payload.physical_state,
            ));
        }
        if current.job.state != JobState::Ready
            || current.payload.physical_state != SandboxJobState::Accepted
        {
            if existing_receipt {
                return Err(RepositoryError::CorruptRow(
                    "Sandbox controller Receipt exists before terminal state".to_owned(),
                ));
            }
            transaction.commit().await?;
            return Ok(SandboxPrestartControlOutcome::RequiresExecutor);
        }
        if existing_receipt {
            return Err(RepositoryError::CorruptRow(
                "Sandbox controller Receipt exists for a Ready Job".to_owned(),
            ));
        }

        insert_sandbox_controller_receipt(&mut transaction, &command).await?;
        let quota = lock_sandbox_quota_bundle(&mut transaction, &current).await?;
        let decision = decide_prestart_control(
            &current.job,
            &current.payload,
            command.reason,
            command.audit.source_event_payload_digest.clone(),
            self.sandbox_limits(),
        )?;
        settle_sandbox_quota(
            &mut transaction,
            &current,
            &quota,
            &command.quota_entry_ids,
            None,
            &command.audit.request_digest,
        )
        .await?;
        release_sandbox_artifact_grants(&mut transaction, &current, database_now).await?;
        let updated = update_sandbox_job(
            &mut transaction,
            &current.record,
            &decision,
            database_now,
            true,
        )
        .await?;
        append_sandbox_controller_event(&mut transaction, &command, &updated, &decision).await?;
        terminalize_sandbox_controller_receipt(&mut transaction, &command).await?;
        transaction.commit().await?;
        Ok(SandboxPrestartControlOutcome::Applied(decision))
    }
}

fn is_sandbox_physical_terminal(state: SandboxJobState) -> bool {
    matches!(
        state,
        SandboxJobState::Succeeded
            | SandboxJobState::Failed
            | SandboxJobState::Cancelled
            | SandboxJobState::TimedOut
            | SandboxJobState::Lost
    )
}

fn require_prestart_control_replay(
    decision: &SandboxPhaseDecision,
    command: &StopUnclaimedSandboxJob,
) -> Result<(), RepositoryError> {
    let expected = match command.reason {
        SandboxStopReason::Cancelled => SandboxJobState::Cancelled,
        SandboxStopReason::TimedOut => SandboxJobState::TimedOut,
    };
    if decision.payload.physical_state != expected
        || decision.payload.executor_identity_digest.is_some()
        || decision.payload.phase_evidence_digest.as_ref()
            != Some(&command.audit.source_event_payload_digest)
        || decision.payload.outcome.is_some()
        || decision.payload.cleanup.is_some()
    {
        return Err(RepositoryError::IdempotencyConflict);
    }
    Ok(())
}

async fn lock_sandbox_control_source(
    transaction: &mut Transaction<'_, Postgres>,
    command: &StopUnclaimedSandboxJob,
) -> Result<(), RepositoryError> {
    let row = sqlx::query(
        r#"
        SELECT source.aggregate_version AS source_invocation_version,
               source.event_type AS source_event_type,
               source.payload_schema_version AS source_payload_schema_version,
               source.payload AS source_payload,
               source.payload_digest AS source_payload_digest,
               invocation.version AS current_invocation_version
        FROM insight_platform.events AS source
        JOIN insight_platform.invocations AS invocation
          ON invocation.tenant_id = source.tenant_id
         AND invocation.invocation_id = source.aggregate_id
         AND invocation.invocation_kind = 'capability'
         AND invocation.version >= source.aggregate_version
         AND invocation.state IN (
             'cancelling', 'cancelled', 'timed_out', 'reconciliation_required'
         )
        WHERE source.tenant_id = $1 AND source.event_id = $2
          AND source.aggregate_kind = 'capability_invocation'
          AND source.aggregate_id = $3
          AND source.aggregate_version IS NOT NULL
          AND source.event_type IN (
              'capability.cancelling', 'capability.cancelled',
              'capability.timed_out', 'capability.reconciliation_required'
          )
        FOR SHARE OF invocation
        "#,
    )
    .bind(command.audit.tenant_id.to_string())
    .bind(command.audit.source_event_id.to_string())
    .bind(command.invocation_id.to_string())
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::NotFound("committed Sandbox control Event"))?;
    let source_invocation_version = positive_sandbox_control_u64(
        row.try_get("source_invocation_version")?,
        "source Invocation version",
    )?;
    let source_payload = payload_from_row(
        &row,
        "source_payload_schema_version",
        "source_payload",
        "source_payload_digest",
    )?;
    let source: CapabilityControlEventPayload =
        decode_versioned_payload(&source_payload, "Capability control Event")?;
    let event_type: String = row.try_get("source_event_type")?;
    let reason = match source.control_kind {
        CapabilityControlKind::Cancel => SandboxStopReason::Cancelled,
        CapabilityControlKind::Timeout => SandboxStopReason::TimedOut,
    };
    if source.schema_version != 1
        || source_invocation_version != command.audit.source_invocation_version
        || source_payload.digest != command.audit.source_event_payload_digest.to_string()
        || reason != command.reason
        || !capability_control_event_matches(&event_type, source.control_kind, source.state)
    {
        return Err(RepositoryError::Conflict("committed Sandbox control Event"));
    }
    Ok(())
}

fn sandbox_stop_signal_from_row(
    row: PgRow,
    limits: SandboxCommandLimits,
) -> Result<SandboxStopSignal, RepositoryError> {
    let source_event_id = parse_sandbox_control_id(&row, "source_event_id", ResourceKind::Event)?;
    let source_invocation_version = positive_sandbox_control_u64(
        row.try_get("source_invocation_version")?,
        "source Invocation version",
    )?;
    let source_event_type: String = row.try_get("source_event_type")?;
    let source_payload = payload_from_row(
        &row,
        "source_payload_schema_version",
        "source_payload",
        "source_payload_digest",
    )?;
    if source_payload.schema_version != 1 {
        return Err(RepositoryError::CorruptRow(
            "Capability control Event schema is unsupported".to_owned(),
        ));
    }
    let source: CapabilityControlEventPayload =
        decode_versioned_payload(&source_payload, "Capability control Event")?;
    if source.schema_version != 1
        || source
            .job_id
            .as_ref()
            .is_some_and(|id| id.kind() != ResourceKind::Job)
        || source
            .task_id
            .as_ref()
            .is_some_and(|id| id.kind() != ResourceKind::Interaction)
        || !capability_control_event_matches(&source_event_type, source.control_kind, source.state)
    {
        return Err(RepositoryError::CorruptRow(
            "Capability control Event payload is invalid".to_owned(),
        ));
    }
    let source_event_payload_digest: Sha256Digest = source_payload.digest.parse().map_err(
        |failure: insight_platform_contracts::NominalTypeError| {
            RepositoryError::CorruptRow(failure.to_string())
        },
    )?;

    let record = job_from_row(row)?;
    if record.work_class != "sandbox"
        || record.owner_kind != "sandbox_job"
        || record.terminal_at.is_some()
        || record.worker_id.is_none()
        || record.lease_epoch <= 0
        || !matches!(record.state.as_str(), "leased" | "running" | "cancelling")
    {
        return Err(RepositoryError::CorruptRow(
            "Sandbox control candidate is not an active leased Job".to_owned(),
        ));
    }
    let job = job_projection(&record)?;
    let payload = decode_sandbox_capability_payload(&record.payload)?;
    payload
        .validate_for(&job, limits)
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    let invocation_id = record
        .invocation_id
        .as_deref()
        .ok_or_else(|| {
            RepositoryError::CorruptRow("Sandbox Job has no Capability Invocation".to_owned())
        })?
        .parse::<ResourceId>()
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    let worker_process_generation_id = record
        .worker_id
        .as_deref()
        .ok_or_else(|| {
            RepositoryError::CorruptRow("Sandbox Job has no Executor Worker".to_owned())
        })?
        .parse::<ResourceId>()
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    if invocation_id.kind() != ResourceKind::CapabilityInvocation
        || worker_process_generation_id.kind() != ResourceKind::WorkerProcessGeneration
        || invocation_id != payload.request.invocation_id
        || record.owner_id != payload.request.sandbox_job_id.to_string()
    {
        return Err(RepositoryError::CorruptRow(
            "Sandbox Job control binding is invalid".to_owned(),
        ));
    }
    let lease_generation =
        positive_sandbox_control_u64(record.lease_epoch, "Sandbox Job lease generation")?;
    let leased_request = payload
        .request
        .as_ref()
        .clone()
        .bind_lease_generation(lease_generation)
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    let reason = match source.control_kind {
        CapabilityControlKind::Cancel => SandboxStopReason::Cancelled,
        CapabilityControlKind::Timeout => SandboxStopReason::TimedOut,
    };
    let signal = SandboxStopSignal {
        schema_version: 1,
        tenant_id: leased_request.tenant_id.clone(),
        sandbox_job_id: leased_request.sandbox_job_id.clone(),
        invocation_id,
        job_id: leased_request.job_id.clone(),
        request_digest: leased_request.request_digest.clone(),
        attempt_no: leased_request.attempt_no,
        lease_generation,
        worker_process_generation_id: worker_process_generation_id.clone(),
        reason,
        source_event_id,
        source_invocation_version,
        source_event_payload_digest,
        signal_digest: leased_request.request_digest.clone(),
    }
    .seal()
    .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    signal
        .validate_for_execution(&leased_request, &worker_process_generation_id)
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    Ok(signal)
}

fn capability_control_event_matches(
    event_type: &str,
    kind: CapabilityControlKind,
    state: InvocationState,
) -> bool {
    matches!(
        (event_type, kind, state),
        (
            "capability.cancelling",
            CapabilityControlKind::Cancel,
            InvocationState::Cancelling
        ) | (
            "capability.cancelling",
            CapabilityControlKind::Timeout,
            InvocationState::Cancelling
        ) | (
            "capability.cancelled",
            CapabilityControlKind::Cancel,
            InvocationState::Cancelled
        ) | (
            "capability.timed_out",
            CapabilityControlKind::Timeout,
            InvocationState::TimedOut
        ) | (
            "capability.reconciliation_required",
            CapabilityControlKind::Cancel | CapabilityControlKind::Timeout,
            InvocationState::ReconciliationRequired
        )
    )
}

fn parse_sandbox_control_id(
    row: &PgRow,
    column: &str,
    expected_kind: ResourceKind,
) -> Result<ResourceId, RepositoryError> {
    let id = row
        .try_get::<String, _>(column)?
        .parse::<ResourceId>()
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    if id.kind() != expected_kind {
        return Err(RepositoryError::CorruptRow(format!(
            "{column} has the wrong identity kind"
        )));
    }
    Ok(id)
}

fn positive_sandbox_control_u64(value: i64, field: &str) -> Result<u64, RepositoryError> {
    u64::try_from(value)
        .ok()
        .filter(|value| *value > 0)
        .ok_or_else(|| RepositoryError::CorruptRow(format!("{field} is not positive")))
}

#[async_trait::async_trait]
impl SandboxGatewayAuthority for PgRepository {
    type Error = RepositoryError;

    async fn accept_sandbox_execution(
        &self,
        command: AcceptSandboxExecution,
    ) -> Result<CommandOutcome<SandboxPhaseDecision>, Self::Error> {
        let mut transaction = self.pool().begin().await?;
        let database_now = database_now(&mut transaction).await?;
        command.validate_at(database_now, self.sandbox_limits())?;
        if claim_command_receipt(
            &mut transaction,
            &command.audit,
            "sandbox_job",
            &command.request.sandbox_job_id.to_string(),
            "sandbox.execute",
        )
        .await?
        {
            let current = load_sandbox_job(
                &mut transaction,
                &command.audit.tenant_id,
                &command.request.job_id,
                self.sandbox_limits(),
            )
            .await?;
            require_accept_replay(&current, &command)?;
            transaction.commit().await?;
            return Ok(CommandOutcome::Replayed(SandboxPhaseDecision {
                job: current.job,
                payload: current.payload,
            }));
        }

        require_tenant_permission(&mut transaction, &command.audit, Permission::SandboxExecute)
            .await?;
        let invocation = lock_sandbox_invocation(&mut transaction, &command).await?;
        verify_sandbox_exact_bindings(&mut transaction, &command.request, &invocation).await?;
        let previous_sandbox = match invocation.payload.current_job_id.as_ref() {
            Some(previous_job_id) => Some(
                load_sandbox_job(
                    &mut transaction,
                    &command.request.tenant_id,
                    previous_job_id,
                    self.sandbox_limits(),
                )
                .await?,
            ),
            None => None,
        };
        verify_managed_mcp_detached_continuation(
            &mut transaction,
            &command.request,
            &invocation,
            previous_sandbox.as_ref(),
        )
        .await?;
        lock_and_persist_artifact_grants(&mut transaction, &command, database_now).await?;
        lock_secret_grants(&mut transaction, &command).await?;
        reserve_sandbox_quota(&mut transaction, &command, database_now).await?;

        let accepted = decide_accept(&command, database_now, self.sandbox_limits())?;
        let deferred = decide_defer_to_sandbox(
            &invocation,
            detached_sandbox_source_kind(&command.request.execution_source),
            command.request.expected_invocation_version,
            &command.request.job_id,
            command.request.attempt_no,
            previous_sandbox
                .as_ref()
                .map(|previous| PreviousDetachedSandboxJob {
                    job: &previous.job,
                    attempt_no: previous.payload.request.attempt_no,
                }),
            database_now,
        )?;
        update_capability_invocation(&mut transaction, &invocation, &deferred).await?;
        let stored_payload = SandboxJobPayload::capability_execution(accepted.payload.clone());
        let payload = TypedPayload::from_versioned(1, &stored_payload, 1_048_576)?;
        sqlx::query(
            r#"
            INSERT INTO insight_platform.jobs (
                tenant_id, job_id, work_class, owner_kind, owner_id, invocation_id,
                run_id, node_id,
                state, version, attempt_no, attempt_limit, lease_epoch, scheduled_at,
                deadline, priority, request_digest, quota_reservation_id,
                payload_schema_version, payload, payload_digest, created_at, updated_at
            ) VALUES ($1, $2, 'sandbox', 'sandbox_job', $3, $4, $5, $6,
                      'ready', 1, 0, 1, 0, $7, $8, 0, $9, $10,
                      $11, $12, $13, $7, $7)
            "#,
        )
        .bind(command.request.tenant_id.to_string())
        .bind(command.request.job_id.to_string())
        .bind(command.request.sandbox_job_id.to_string())
        .bind(command.request.invocation_id.to_string())
        .bind(invocation.run_id.to_string())
        .bind(invocation.node_execution_id.to_string())
        .bind(database_now)
        .bind(command.request.deadline)
        .bind(command.request.request_digest.to_string())
        .bind(command.usage_reservation_id.to_string())
        .bind(payload.schema_version)
        .bind(&payload.value)
        .bind(&payload.digest)
        .execute(&mut *transaction)
        .await?;
        append_command_event(
            &mut transaction,
            &command.audit,
            "capability_invocation",
            &command.request.invocation_id.to_string(),
            as_i64(deferred.version, "Invocation version")?,
            "capability.waiting",
            &TypedPayload::new(
                1,
                &serde_json::json!({
                    "invocation_state": deferred.state,
                    "invocation_id": command.request.invocation_id,
                    "isolation_class": command.request.isolation_class,
                    "job_id": command.request.job_id,
                    "job_state": accepted.job.state,
                    "sandbox_job_id": command.request.sandbox_job_id,
                }),
            )?,
        )
        .await?;
        terminalize_command_receipt(
            &mut transaction,
            &command.audit,
            &command.request.job_id.to_string(),
            "accepted",
        )
        .await?;
        transaction.commit().await?;
        Ok(CommandOutcome::Applied(SandboxPhaseDecision {
            job: accepted.job,
            payload: accepted.payload,
        }))
    }
}

#[async_trait::async_trait]
impl SandboxExecutionAuthority for PgRepository {
    type Error = RepositoryError;

    async fn commit_sandbox_phase(
        &self,
        command: CommitSandboxPhase,
    ) -> Result<CommandOutcome<SandboxPhaseDecision>, Self::Error> {
        let mut transaction = self.pool().begin().await?;
        let database_now = database_now(&mut transaction).await?;
        command.validate_at(database_now)?;
        let operation = format!("sandbox.phase.{}", command.target.as_str());
        if claim_sandbox_worker_receipt(
            &mut transaction,
            &command.audit,
            &command.job_id,
            &operation,
        )
        .await?
        {
            let decision = load_sandbox_decision(
                &mut transaction,
                &command.audit.tenant_id,
                &command.job_id,
                self.sandbox_limits(),
            )
            .await?;
            require_phase_replay(&decision, &command)?;
            transaction.commit().await?;
            return Ok(CommandOutcome::Replayed(decision));
        }

        let current = load_sandbox_job(
            &mut transaction,
            &command.audit.tenant_id,
            &command.job_id,
            self.sandbox_limits(),
        )
        .await?;
        require_sandbox_command_owner(&current, &command.sandbox_job_id, &command.audit.tenant_id)?;
        let decision = if command.target == SandboxJobState::Preparing {
            decide_begin_execution(
                &current.job,
                &current.payload,
                &command.fence,
                insight_platform_sandbox::WasiExecutorProcessBinding {
                    executor_identity_digest: command.executor_identity_digest.clone(),
                    attestor_route: command.attestor_route.clone(),
                },
                command.phase_evidence_digest.clone(),
                database_now,
                self.sandbox_limits(),
            )?
        } else {
            decide_advance_phase(
                &current.job,
                &current.payload,
                &command.fence,
                insight_platform_sandbox::AdvanceSandboxPhase {
                    target: command.target,
                    phase_evidence_digest: command.phase_evidence_digest.clone(),
                    prepared: command.prepared.clone(),
                    database_now,
                },
                self.sandbox_limits(),
            )?
        };
        let updated = update_sandbox_job(
            &mut transaction,
            &current.record,
            &decision,
            database_now,
            false,
        )
        .await?;
        append_sandbox_event(
            &mut transaction,
            &command.audit,
            &updated,
            "sandbox.job.phase_changed",
            &decision,
        )
        .await?;
        terminalize_sandbox_worker_receipt(
            &mut transaction,
            &command.audit,
            &command.job_id,
            command.target.as_str(),
        )
        .await?;
        transaction.commit().await?;
        Ok(CommandOutcome::Applied(decision))
    }

    async fn commit_sandbox_outcome(
        &self,
        command: CommitSandboxOutcome,
    ) -> Result<CommandOutcome<SandboxPhaseDecision>, Self::Error> {
        let mut transaction = self.pool().begin().await?;
        let database_now = database_now(&mut transaction).await?;
        command.validate_at(database_now)?;
        let operation = "sandbox.outcome.commit";
        if claim_sandbox_worker_receipt(
            &mut transaction,
            &command.audit,
            &command.job_id,
            operation,
        )
        .await?
        {
            let decision = load_sandbox_decision(
                &mut transaction,
                &command.audit.tenant_id,
                &command.job_id,
                self.sandbox_limits(),
            )
            .await?;
            require_outcome_replay(&decision, &command)?;
            transaction.commit().await?;
            return Ok(CommandOutcome::Replayed(decision));
        }

        let current = load_sandbox_job(
            &mut transaction,
            &command.audit.tenant_id,
            &command.job_id,
            self.sandbox_limits(),
        )
        .await?;
        require_sandbox_command_owner(&current, &command.sandbox_job_id, &command.audit.tenant_id)?;
        if current.record.quota_reservation_id.as_deref()
            != Some(command.usage_reservation_id.to_string().as_str())
        {
            return Err(RepositoryError::Conflict("Sandbox usage reservation"));
        }
        let quota = lock_sandbox_quota_bundle(&mut transaction, &current).await?;
        let decision = decide_execution_outcome(
            &current.job,
            &current.payload,
            &command.fence,
            command.outcome.clone(),
            command.cleanup.clone(),
            command.phase_evidence_digest.clone(),
            database_now,
            self.sandbox_limits(),
        )?;
        settle_sandbox_quota(
            &mut transaction,
            &current,
            &quota,
            &command.quota_entry_ids,
            Some(&command.outcome),
            &command.audit.request_digest,
        )
        .await?;
        release_sandbox_artifact_grants(&mut transaction, &current, database_now).await?;
        let updated = update_sandbox_job(
            &mut transaction,
            &current.record,
            &decision,
            database_now,
            true,
        )
        .await?;
        let event_type = match decision.payload.physical_state {
            SandboxJobState::Succeeded => "sandbox.job.completed",
            SandboxJobState::Failed
            | SandboxJobState::Cancelled
            | SandboxJobState::TimedOut
            | SandboxJobState::Lost => "sandbox.job.failed",
            _ => {
                return Err(RepositoryError::CorruptRow(
                    "Sandbox outcome did not produce a terminal physical state".to_owned(),
                ));
            }
        };
        append_sandbox_event(
            &mut transaction,
            &command.audit,
            &updated,
            event_type,
            &decision,
        )
        .await?;
        terminalize_sandbox_worker_receipt(
            &mut transaction,
            &command.audit,
            &command.job_id,
            decision.payload.physical_state.as_str(),
        )
        .await?;
        transaction.commit().await?;
        Ok(CommandOutcome::Applied(decision))
    }

    async fn heartbeat_sandbox_execution(
        &self,
        command: HeartbeatSandboxExecution,
    ) -> Result<SandboxPhaseDecision, Self::Error> {
        command.validate(
            u64::try_from(MAX_JOB_LEASE_MILLISECONDS).expect("positive Sandbox lease hard maximum"),
        )?;
        let mut transaction = self.pool().begin().await?;
        let database_now = database_now(&mut transaction).await?;
        let current = load_sandbox_job(
            &mut transaction,
            &command.tenant_id,
            &command.job_id,
            self.sandbox_limits(),
        )
        .await?;
        require_sandbox_command_owner(&current, &command.sandbox_job_id, &command.tenant_id)?;
        let next_job = decide_heartbeat(
            &current.job,
            &command.fence,
            database_now,
            LeasePolicy {
                requested_milliseconds: command.lease_milliseconds,
                hard_maximum_milliseconds: u64::try_from(MAX_JOB_LEASE_MILLISECONDS)
                    .expect("positive Sandbox lease hard maximum"),
            },
        )?;
        let decision = SandboxPhaseDecision {
            job: next_job,
            payload: current.payload.clone(),
        };
        decision
            .payload
            .validate_for(&decision.job, self.sandbox_limits())?;
        let updated = update_sandbox_job(
            &mut transaction,
            &current.record,
            &decision,
            database_now,
            false,
        )
        .await?;
        if updated.version
            != i64::try_from(decision.job.version)
                .map_err(|_| RepositoryError::InvalidInput("Sandbox Job version".to_owned()))?
        {
            return Err(RepositoryError::CorruptRow(
                "Sandbox heartbeat version drifted".to_owned(),
            ));
        }
        transaction.commit().await?;
        Ok(decision)
    }
}

#[async_trait::async_trait]
impl SandboxLeaseRecoveryAuthority for PgRepository {
    type Error = RepositoryError;

    async fn recover_expired_sandbox_lease(
        &self,
        command: RecoverExpiredSandboxLease,
    ) -> Result<CommandOutcome<SandboxLeaseRecoveryResult>, Self::Error> {
        let mut transaction = self.pool().begin().await?;
        let database_now = database_now(&mut transaction).await?;
        command.validate_at(database_now)?;
        if claim_sandbox_recovery_receipt(&mut transaction, &command).await? {
            let result = sandbox_recovery_result(&command);
            transaction.commit().await?;
            return Ok(CommandOutcome::Replayed(result));
        }

        let current = load_sandbox_job(
            &mut transaction,
            &command.audit.tenant_id,
            &command.job_id,
            self.sandbox_limits(),
        )
        .await?;
        require_sandbox_command_owner(&current, &command.sandbox_job_id, &command.audit.tenant_id)?;
        let physical_request_digest = current
            .payload
            .request
            .as_ref()
            .clone()
            .bind_lease_generation(current.job.lease_generation)
            .map_err(|failure| {
                RepositoryError::CorruptRow(format!(
                    "Sandbox recovery request binding is invalid: {failure}"
                ))
            })?
            .request_digest;
        if current.record.invocation_id.as_deref()
            != Some(command.invocation_id.to_string().as_str())
            || physical_request_digest != command.sandbox_request_digest
            || current.job.version != command.observed_job_version
            || current.job.lease_generation != command.observed_lease_generation
            || current
                .job
                .lease
                .as_ref()
                .map(|lease| &lease.worker_process_generation_id)
                != Some(&command.previous_worker_process_generation_id)
        {
            return Err(RepositoryError::Conflict(
                "expired Sandbox lease first-winner",
            ));
        }
        let decision = decide_expired_lease_recovery(
            &current.job,
            &current.payload,
            command.observed_job_version,
            command.observed_lease_generation,
            &command.action,
            database_now,
            self.sandbox_limits(),
        )?;
        let terminal = !matches!(
            command.action,
            SandboxLeaseRecoveryAction::RequeueUnstarted { .. }
        );
        if terminal {
            let quota = lock_sandbox_quota_bundle(&mut transaction, &current).await?;
            let recovery_outcome = match &command.action {
                SandboxLeaseRecoveryAction::MarkLost { uncertainty, .. } => {
                    Some(SandboxExecutionOutcome::Uncertain(uncertainty.clone()))
                }
                SandboxLeaseRecoveryAction::TimeoutUnstarted { .. } => None,
                SandboxLeaseRecoveryAction::RequeueUnstarted { .. } => unreachable!(),
            };
            settle_sandbox_quota(
                &mut transaction,
                &current,
                &quota,
                &command.quota_entry_ids,
                recovery_outcome.as_ref(),
                &command.audit.request_digest,
            )
            .await?;
            release_sandbox_artifact_grants(&mut transaction, &current, database_now).await?;
        }
        let updated = update_sandbox_job(
            &mut transaction,
            &current.record,
            &decision,
            database_now,
            terminal,
        )
        .await?;
        append_sandbox_recovery_event(
            &mut transaction,
            &command.audit,
            &updated,
            &command,
            &decision,
        )
        .await?;
        terminalize_sandbox_recovery_receipt(&mut transaction, &command).await?;
        let result = sandbox_recovery_result(&command);
        transaction.commit().await?;
        Ok(CommandOutcome::Applied(result))
    }
}

struct LockedSandboxJob {
    record: JobRecord,
    job: JobProjection,
    payload: SandboxExecutionJobPayload,
}

fn sandbox_capability_outcome_candidate(
    row: PgRow,
    limits: SandboxCommandLimits,
) -> Result<PendingSandboxCapabilityOutcome, RepositoryError> {
    let source_event_id = parse_sandbox_control_id(&row, "source_event_id", ResourceKind::Event)?;
    let source_job_version = positive_sandbox_control_u64(
        row.try_get::<Option<i64>, _>("source_job_version")?
            .ok_or_else(|| {
                RepositoryError::CorruptRow(
                    "Sandbox terminal Event has no aggregate version".to_owned(),
                )
            })?,
        "Sandbox source Job version",
    )?;
    let source_event_payload_digest = row
        .try_get::<String, _>("source_event_payload_digest")?
        .parse::<Sha256Digest>()
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    let source_event_occurred_at = row.try_get("source_event_occurred_at")?;
    let expected_invocation_version = positive_sandbox_control_u64(
        row.try_get("invocation_version")?,
        "Sandbox owner Invocation version",
    )?;
    let deadline = row.try_get("invocation_deadline")?;

    let record = job_from_row(row)?;
    if record.work_class != WorkClass::Sandbox.as_str()
        || record.owner_kind != ResourceKind::SandboxJob.descriptor().name
        || record.worker_id.is_some()
        || record.version
            != i64::try_from(source_job_version).map_err(|_| {
                RepositoryError::CorruptRow(
                    "Sandbox source Job version is outside PostgreSQL range".to_owned(),
                )
            })?
    {
        return Err(RepositoryError::CorruptRow(
            "Sandbox outcome candidate is not the exact terminal Job generation".to_owned(),
        ));
    }
    let job = job_projection(&record)?;
    let payload = decode_sandbox_capability_payload(&record.payload)?;
    payload
        .validate_for(&job, limits)
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    let invocation_id = record
        .invocation_id
        .as_deref()
        .ok_or_else(|| {
            RepositoryError::CorruptRow(
                "Sandbox outcome Job has no Capability Invocation".to_owned(),
            )
        })?
        .parse::<ResourceId>()
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    if !is_sandbox_physical_terminal(payload.physical_state)
        || invocation_id.kind() != ResourceKind::CapabilityInvocation
        || invocation_id != payload.request.invocation_id
        || record.owner_id != payload.request.sandbox_job_id.to_string()
        || record.request_digest != payload.request.request_digest.to_string()
        || payload.request.deadline != deadline
    {
        return Err(RepositoryError::CorruptRow(
            "Sandbox terminal Job and Capability owner binding disagree".to_owned(),
        ));
    }
    let candidate = PendingSandboxCapabilityOutcome {
        tenant_id: job.tenant_id,
        source_event_id,
        source_job_version,
        source_event_payload_digest,
        source_event_occurred_at,
        sandbox_job_id: payload.request.sandbox_job_id.clone(),
        invocation_id,
        job_id: job.job_id,
        sandbox_request_digest: payload.request.request_digest.clone(),
        expected_invocation_version,
        deadline,
    };
    candidate
        .validate()
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    Ok(candidate)
}

fn require_accept_replay(
    current: &LockedSandboxJob,
    command: &AcceptSandboxExecution,
) -> Result<(), RepositoryError> {
    if current.payload.request.as_ref() != &command.request
        || current.record.invocation_id.as_deref()
            != Some(command.request.invocation_id.to_string().as_str())
    {
        return Err(RepositoryError::Conflict("Sandbox admission replay"));
    }
    Ok(())
}

async fn lock_sandbox_invocation(
    transaction: &mut Transaction<'_, Postgres>,
    command: &AcceptSandboxExecution,
) -> Result<CapabilityInvocationRecord, RepositoryError> {
    let invocation = load_capability_invocation(
        transaction,
        &command.request.tenant_id,
        &command.request.invocation_id,
        true,
    )
    .await?;
    let initial = invocation.state == InvocationState::Ready
        && invocation.payload.current_job_id.is_none()
        && command.request.attempt_no == 1;
    let retry = invocation.state == InvocationState::RetryScheduled
        && invocation.payload.current_job_id.is_some()
        && command.request.attempt_no > 1;
    if (!initial && !retry)
        || invocation.version != command.request.expected_invocation_version
        || invocation.payload.admission.deployment != command.request.capability_deployment
        || invocation.payload.admission.backend_kind
            != command.request.execution_source.capability_binding().kind()
    {
        return Err(RepositoryError::Conflict("Sandbox owner Invocation"));
    }
    Ok(invocation)
}

async fn lock_sandbox_terminal_source(
    transaction: &mut Transaction<'_, Postgres>,
    command: &MergeSandboxCapabilityOutcome,
    limits: SandboxCommandLimits,
) -> Result<LockedSandboxJob, RepositoryError> {
    let event = sqlx::query(
        r#"
        SELECT aggregate_version, event_type, payload_digest
        FROM insight_platform.events
        WHERE tenant_id = $1 AND event_id = $2
          AND aggregate_kind = 'job' AND aggregate_id = $3
        FOR SHARE
        "#,
    )
    .bind(command.audit.tenant_id.to_string())
    .bind(command.audit.source_event_id.to_string())
    .bind(command.job_id.to_string())
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::NotFound("Sandbox terminal Event"))?;
    let event_type: String = event.try_get("event_type")?;
    if event.try_get::<Option<i64>, _>("aggregate_version")?
        != Some(as_i64(
            command.audit.source_job_version,
            "Sandbox source Job version",
        )?)
        || event.try_get::<String, _>("payload_digest")?
            != command.audit.source_event_payload_digest.to_string()
        || !matches!(
            event_type.as_str(),
            "sandbox.job.completed" | "sandbox.job.failed"
        )
    {
        return Err(RepositoryError::Conflict("Sandbox terminal Event binding"));
    }
    let current = load_sandbox_job(
        transaction,
        &command.audit.tenant_id,
        &command.job_id,
        limits,
    )
    .await?;
    if current.job.version != command.audit.source_job_version
        || current.payload.request.sandbox_job_id != command.sandbox_job_id
        || current.payload.request.invocation_id != command.invocation_id
        || current.payload.request.request_digest != command.sandbox_request_digest
        || !is_sandbox_physical_terminal(current.payload.physical_state)
    {
        return Err(RepositoryError::Conflict("Sandbox terminal source"));
    }
    Ok(current)
}

async fn claim_sandbox_outcome_merge_receipt(
    transaction: &mut Transaction<'_, Postgres>,
    command: &MergeSandboxCapabilityOutcome,
    payload: &TypedPayload,
) -> Result<bool, RepositoryError> {
    let inserted = sqlx::query(
        r#"
        INSERT INTO insight_platform.receipts (
            tenant_id, receipt_id, receipt_kind, scope_kind, scope_id,
            dedupe_owner_id, operation, idempotency_key_digest, request_digest, state,
            payload_schema_version, payload, payload_digest, expires_at
        ) VALUES ($1, $2, 'job_commit', 'capability_invocation', $3, $4,
                  'sandbox.capability.merge', $5, $6, 'processing', $7, $8, $9, $10)
        ON CONFLICT (
            tenant_id, receipt_kind, scope_kind, scope_id, dedupe_owner_id,
            operation, idempotency_key_digest
        ) DO NOTHING
        RETURNING receipt_id
        "#,
    )
    .bind(command.audit.tenant_id.to_string())
    .bind(command.audit.receipt_id.to_string())
    .bind(command.invocation_id.to_string())
    .bind(command.audit.source_event_id.to_string())
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
        SELECT request_digest, state, payload_digest
        FROM insight_platform.receipts
        WHERE tenant_id = $1 AND receipt_kind = 'job_commit'
          AND scope_kind = 'capability_invocation' AND scope_id = $2
          AND dedupe_owner_id = $3 AND operation = 'sandbox.capability.merge'
          AND idempotency_key_digest = $4
        FOR UPDATE
        "#,
    )
    .bind(command.audit.tenant_id.to_string())
    .bind(command.invocation_id.to_string())
    .bind(command.audit.source_event_id.to_string())
    .bind(command.audit.idempotency_key_digest.to_string())
    .fetch_one(&mut **transaction)
    .await?;
    if row.try_get::<String, _>("request_digest")? != command.audit.request_digest.to_string()
        || row.try_get::<String, _>("payload_digest")? != payload.digest
    {
        return Err(RepositoryError::IdempotencyConflict);
    }
    if row.try_get::<String, _>("state")? != "succeeded" {
        return Err(RepositoryError::Conflict(
            "Sandbox Capability merge receipt",
        ));
    }
    Ok(true)
}

async fn terminalize_sandbox_outcome_merge_receipt(
    transaction: &mut Transaction<'_, Postgres>,
    command: &MergeSandboxCapabilityOutcome,
    disposition: &str,
) -> Result<(), RepositoryError> {
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
    .bind(disposition)
    .bind(command.invocation_id.to_string())
    .execute(&mut **transaction)
    .await?
    .rows_affected();
    if affected != 1 {
        return Err(RepositoryError::Conflict(
            "Sandbox Capability merge receipt",
        ));
    }
    Ok(())
}

fn normalize_sandbox_capability_outcome(
    payload: &SandboxExecutionJobPayload,
    database_now: DateTime<Utc>,
    invocation: &CapabilityInvocationRecord,
) -> Result<DetachedCapabilityJobOutcome, RepositoryError> {
    let failure = |class, retryability, safe_message: Option<String>| {
        insight_platform_invocations::SafeBackendFailure {
            failure: Failure {
                code: FailureCode::Platform {
                    code: PlatformFailureCode::CapabilityFailed,
                },
                class,
                retryability,
                safe_message,
                details_ref: None,
                source: FailureSource::Capability,
            },
            evidence_digest: payload
                .phase_evidence_digest
                .clone()
                .unwrap_or_else(|| payload.payload_digest.clone()),
        }
    };
    match payload.outcome.as_ref() {
        Some(SandboxExecutionOutcome::Completed(output)) => {
            let artifact_link_id = match &output.value {
                ValueRef::Inline { .. } => None,
                ValueRef::Artifact { artifact } => {
                    let index = output
                        .artifact_outputs
                        .iter()
                        .position(|candidate| candidate == artifact)
                        .ok_or_else(|| {
                            RepositoryError::CorruptRow(
                                "Sandbox primary output Artifact has no reference link".to_owned(),
                            )
                        })?;
                    Some(output.artifact_link_ids[index].clone())
                }
            };
            Ok(DetachedCapabilityJobOutcome::Completed(
                CapabilityOutputValue {
                    value_id: output.value_id.clone(),
                    classification: output.classification,
                    schema_digest: output.schema_digest.clone(),
                    content_digest: output.content_digest.clone(),
                    value: output.value.clone(),
                    artifact_link_id,
                    validation_evidence_digest: output.validation_evidence_digest.clone(),
                },
            ))
        }
        Some(SandboxExecutionOutcome::ManagedMcp(output)) => match &output.outcome {
            DispatchOutcome::Completed(completed) => {
                Ok(DetachedCapabilityJobOutcome::Completed(completed.clone()))
            }
            DispatchOutcome::Deferred(wait) => Ok(DetachedCapabilityJobOutcome::Deferred {
                wait: wait.clone(),
                poll_count: output.logical_poll_count,
            }),
            DispatchOutcome::InputRequired(input) => {
                Ok(DetachedCapabilityJobOutcome::InputRequired {
                    request: input.clone(),
                    poll_count: output.logical_poll_count,
                })
            }
            DispatchOutcome::RetryableFailure { failure, .. } => {
                let retry_delay = i64::try_from(payload.request.retry_backoff_milliseconds)
                    .ok()
                    .filter(|value| (1..=60_000).contains(value))
                    .and_then(Duration::try_milliseconds)
                    .ok_or_else(|| {
                        RepositoryError::InvalidInput(
                            "Sandbox retry backoff is outside the platform bound".to_owned(),
                        )
                    })?;
                let retry_at = database_now
                    .checked_add_signed(retry_delay)
                    .ok_or_else(|| {
                        RepositoryError::InvalidInput("Sandbox retry_at overflow".to_owned())
                    })?;
                if retry_at >= invocation.deadline {
                    return Ok(DetachedCapabilityJobOutcome::TimedOut);
                }
                Ok(DetachedCapabilityJobOutcome::RetryableFailure {
                    failure: failure.clone(),
                    retry_at,
                })
            }
            DispatchOutcome::PermanentFailure(failure) => Ok(
                DetachedCapabilityJobOutcome::PermanentFailure(failure.clone()),
            ),
            DispatchOutcome::Uncertain(uncertainty) => {
                Ok(DetachedCapabilityJobOutcome::Uncertain(uncertainty.clone()))
            }
        },
        Some(SandboxExecutionOutcome::Failed(safe)) => {
            let can_retry = safe.retryability == Retryability::SafeWithinPolicy
                && !safe.external_effect_possible
                && payload.request.attempt_no < invocation.payload.admission.attempt_limit;
            if can_retry {
                let retry_delay = i64::try_from(payload.request.retry_backoff_milliseconds)
                    .ok()
                    .filter(|value| (1..=60_000).contains(value))
                    .and_then(Duration::try_milliseconds)
                    .ok_or_else(|| {
                        RepositoryError::InvalidInput(
                            "Sandbox retry backoff is outside the platform bound".to_owned(),
                        )
                    })?;
                let retry_at = database_now
                    .checked_add_signed(retry_delay)
                    .ok_or_else(|| {
                        RepositoryError::InvalidInput("Sandbox retry_at overflow".to_owned())
                    })?;
                if retry_at < invocation.deadline {
                    return Ok(DetachedCapabilityJobOutcome::RetryableFailure {
                        failure: failure(
                            safe.resource_violation
                                .as_ref()
                                .map_or(FailureClass::External, |_| FailureClass::Resource),
                            safe.retryability,
                            Some(safe.safe_message.clone()),
                        ),
                        retry_at,
                    });
                }
            }
            if safe.external_effect_possible {
                Ok(DetachedCapabilityJobOutcome::Uncertain(
                    CapabilityUncertainty {
                        observation_digest: safe.evidence_digest.clone(),
                        policy_path_digest: invocation
                            .payload
                            .admission
                            .policies
                            .canonical_digest
                            .clone(),
                        external_identity_digest: payload
                            .executor_identity_digest
                            .clone()
                            .unwrap_or_else(|| safe.evidence_digest.clone()),
                        manual: true,
                    },
                ))
            } else {
                Ok(DetachedCapabilityJobOutcome::PermanentFailure(failure(
                    safe.resource_violation
                        .as_ref()
                        .map_or(FailureClass::External, |_| FailureClass::Resource),
                    Retryability::Never,
                    Some(safe.safe_message.clone()),
                )))
            }
        }
        Some(SandboxExecutionOutcome::Cancelled(evidence)) => {
            if evidence.external_effect_possible {
                Ok(DetachedCapabilityJobOutcome::Uncertain(
                    CapabilityUncertainty {
                        observation_digest: evidence.evidence_digest.clone(),
                        policy_path_digest: invocation
                            .payload
                            .admission
                            .policies
                            .canonical_digest
                            .clone(),
                        external_identity_digest: payload
                            .executor_identity_digest
                            .clone()
                            .unwrap_or_else(|| evidence.evidence_digest.clone()),
                        manual: true,
                    },
                ))
            } else {
                Ok(DetachedCapabilityJobOutcome::Cancelled)
            }
        }
        Some(SandboxExecutionOutcome::TimedOut(evidence)) => {
            if evidence.external_effect_possible {
                Ok(DetachedCapabilityJobOutcome::Uncertain(
                    CapabilityUncertainty {
                        observation_digest: evidence.evidence_digest.clone(),
                        policy_path_digest: invocation
                            .payload
                            .admission
                            .policies
                            .canonical_digest
                            .clone(),
                        external_identity_digest: payload
                            .executor_identity_digest
                            .clone()
                            .unwrap_or_else(|| evidence.evidence_digest.clone()),
                        manual: true,
                    },
                ))
            } else {
                Ok(DetachedCapabilityJobOutcome::TimedOut)
            }
        }
        Some(SandboxExecutionOutcome::Uncertain(uncertain)) => Ok(
            DetachedCapabilityJobOutcome::Uncertain(CapabilityUncertainty {
                observation_digest: uncertain.evidence_digest.clone(),
                policy_path_digest: invocation
                    .payload
                    .admission
                    .policies
                    .canonical_digest
                    .clone(),
                external_identity_digest: uncertain.sandbox_identity_digest.clone(),
                manual: uncertain.manual_reconciliation_required,
            }),
        ),
        None if payload.physical_state == SandboxJobState::Cancelled => {
            Ok(DetachedCapabilityJobOutcome::Cancelled)
        }
        None if payload.physical_state == SandboxJobState::TimedOut => {
            Ok(DetachedCapabilityJobOutcome::TimedOut)
        }
        None => Err(RepositoryError::CorruptRow(
            "Sandbox terminal Job has no logical outcome evidence".to_owned(),
        )),
    }
}

async fn insert_sandbox_capability_value(
    transaction: &mut Transaction<'_, Postgres>,
    invocation: &CapabilityInvocationRecord,
    output: &CapabilityOutputValue,
    sandbox_payload: &SandboxExecutionJobPayload,
    database_now: DateTime<Utc>,
) -> Result<(), RepositoryError> {
    let completed = match sandbox_payload.outcome.as_ref() {
        Some(SandboxExecutionOutcome::Completed(completed)) => completed,
        _ => {
            return Err(RepositoryError::CorruptRow(
                "Sandbox successful merge has no completed output".to_owned(),
            ));
        }
    };
    let (inline_value, artifact_id) = match &output.value {
        ValueRef::Inline { value } => (Some(value), None),
        ValueRef::Artifact { artifact } => {
            crate::repository::require_ready_run_artifact(
                transaction,
                &invocation.tenant_id,
                artifact,
            )
            .await?;
            (None, Some(artifact.artifact_id().to_string()))
        }
    };
    sqlx::query(
        r#"
        INSERT INTO insight_platform.run_values (
            tenant_id, value_id, run_id, node_id, value_kind, classification,
            schema_digest, content_digest, inline_value, artifact_id, created_at
        ) VALUES ($1, $2, $3, $4, 'capability_output', $5, $6, $7, $8, $9, $10)
        "#,
    )
    .bind(invocation.tenant_id.to_string())
    .bind(output.value_id.to_string())
    .bind(invocation.run_id.to_string())
    .bind(invocation.node_execution_id.to_string())
    .bind(output.classification.as_str())
    .bind(output.schema_digest.to_string())
    .bind(output.content_digest.to_string())
    .bind(inline_value)
    .bind(artifact_id)
    .bind(database_now)
    .execute(&mut **transaction)
    .await?;
    for (link_id, artifact) in completed
        .artifact_link_ids
        .iter()
        .zip(&completed.artifact_outputs)
    {
        crate::repository::require_ready_run_artifact(transaction, &invocation.tenant_id, artifact)
            .await?;
        let snapshot = ArtifactReferenceSnapshot {
            schema_version: 1,
            artifact_id: artifact.artifact_id().clone(),
            owner_id: invocation.invocation_id.clone(),
            reference_kind: ArtifactReferenceKind::Output,
            purpose: ArtifactPurpose::CapabilityOutput,
            created_by: invocation.payload.admission.principal.principal_id.clone(),
        };
        let payload = TypedPayload::from_versioned(1, &snapshot, 262_144)?;
        sqlx::query(
            r#"
            INSERT INTO insight_platform.artifact_links (
                tenant_id, artifact_link_id, link_kind, owner_kind, owner_id,
                target_artifact_id, link_key_digest, state, payload_schema_version,
                payload, payload_digest, created_at, updated_at
            ) VALUES ($1, $2, 'reference', 'capability_invocation', $3, $4, $5,
                      'active', $6, $7, $8, $9, $9)
            "#,
        )
        .bind(invocation.tenant_id.to_string())
        .bind(link_id.to_string())
        .bind(invocation.invocation_id.to_string())
        .bind(artifact.artifact_id().to_string())
        .bind(
            snapshot
                .link_key_digest()
                .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?
                .to_string(),
        )
        .bind(payload.schema_version)
        .bind(&payload.value)
        .bind(&payload.digest)
        .bind(database_now)
        .execute(&mut **transaction)
        .await?;
    }
    Ok(())
}

async fn verify_sandbox_exact_bindings(
    transaction: &mut Transaction<'_, Postgres>,
    request: &SandboxExecutionRequest,
    invocation: &CapabilityInvocationRecord,
) -> Result<(), RepositoryError> {
    let input = load_capability_execution_input(transaction, invocation).await?;
    let input_material_matches = match (&input.exact.storage, &input.material, &request.input_ref) {
        (
            InvocationValueStorage::Inline,
            CapabilityExecutionInputMaterial::Inline { value },
            insight_platform_contracts::ValueRef::Inline { value: supplied },
        ) => value == supplied,
        (
            InvocationValueStorage::Artifact { artifact },
            CapabilityExecutionInputMaterial::LinkedArtifact { .. },
            insight_platform_contracts::ValueRef::Artifact { artifact: supplied },
        ) => artifact == supplied,
        _ => false,
    };
    if request.input_value_id != invocation.input_value_id
        || request.input_value_id != input.exact.value_id
        || request.input_schema_digest != invocation.payload.admission.input_schema_digest
        || request.input_schema_digest != input.exact.schema_digest
        || request.output_schema_digest != invocation.payload.admission.output_schema_digest
        || request.retry_backoff_milliseconds
            != invocation.payload.admission.retry_backoff_milliseconds
        || request.effect != invocation.payload.admission.effect
        || request.classification != input.exact.classification
        || !input_material_matches
    {
        return Err(RepositoryError::Conflict(
            "Sandbox Capability Invocation value binding",
        ));
    }
    let output_exists: bool = sqlx::query_scalar(
        r#"
        SELECT EXISTS (
            SELECT 1 FROM insight_platform.run_values
            WHERE tenant_id = $1 AND value_id = $2
        )
        "#,
    )
    .bind(request.tenant_id.to_string())
    .bind(request.output_value_id.to_string())
    .fetch_one(&mut **transaction)
    .await?;
    if output_exists {
        return Err(RepositoryError::Conflict(
            "Sandbox reserved output RunValue identity",
        ));
    }
    let deployment = load_deployment(
        transaction,
        &request.tenant_id,
        &request.capability_deployment.deployment_id,
    )
    .await?;
    if deployment.bindings.digest != request.capability_deployment.deployment_digest.to_string() {
        return Err(RepositoryError::Conflict(
            "exact Sandbox Capability Deployment",
        ));
    }
    let resource_id = deployment
        .resource_id
        .parse::<ResourceId>()
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    let resource = load_resource(transaction, &request.tenant_id, &resource_id).await?;
    if resource.resource_kind != RegistryResourceKind::CapabilityInterface.as_str()
        || resource.lifecycle_state != EntityLifecycle::Active.as_str()
        || resource.gate_state != "enabled"
    {
        return Err(RepositoryError::Conflict(
            "Sandbox Capability Deployment gate",
        ));
    }
    let closure = match decode_deployment_closure(&deployment.bindings)? {
        DeploymentClosure::CapabilityInterface(closure) => closure,
        _ => {
            return Err(RepositoryError::CorruptRow(
                "Sandbox Capability Deployment has the wrong closure".to_owned(),
            ));
        }
    };
    if &closure != request.execution_source.capability_deployment_closure() {
        return Err(RepositoryError::Conflict(
            "Sandbox exact Capability Deployment closure",
        ));
    }
    let mut expected_secret_bindings = closure.secret_bindings.iter().collect::<Vec<_>>();
    if let SandboxExecutionSource::ManagedMcp {
        capability_interface_revision,
        capability_interface,
        capability_implementation_revision,
        capability_implementation,
        mcp_contract,
        operation,
        ..
    } = &request.execution_source
    {
        let runtime = invocation
            .payload
            .admission
            .mcp_runtime
            .as_ref()
            .ok_or(RepositoryError::Conflict("Managed MCP admission binding"))?;
        if runtime.mcp_operation_id != operation.mcp_operation_id
            || runtime.mcp_deployment != mcp_contract.deployment
            || runtime.discovery_snapshot_id != mcp_contract.discovery.snapshot_id
            || runtime.discovery_snapshot_digest != mcp_contract.discovery.canonical_digest
            || runtime.authorization_binding_id
                != mcp_contract.authorization.authorization_binding_id
            || runtime.authorization_generation != mcp_contract.authorization.generation
            || runtime.authorization_context_digest != mcp_contract.authorization.canonical_digest
            || runtime.principal_id != mcp_contract.authorization.principal_id
        {
            return Err(RepositoryError::Conflict("Managed MCP admission binding"));
        }
        if closure.interface != *capability_interface_revision
            || closure.implementation != *capability_implementation_revision
            || invocation.payload.admission.interface != *capability_interface_revision
            || invocation.payload.admission.implementation != *capability_implementation_revision
            || invocation.payload.admission.backend_contract_digest
                != capability_implementation.backend_contract_digest
            || invocation.payload.admission.implementation_features
                != capability_implementation.features
        {
            return Err(RepositoryError::Conflict(
                "Managed MCP Capability execution closure",
            ));
        }
        let exact_interface = load_enabled_exact_published_version(
            transaction,
            &request.tenant_id,
            capability_interface_revision,
            RegistryResourceKind::CapabilityInterface,
        )
        .await?;
        let exact_implementation = load_enabled_exact_published_version(
            transaction,
            &request.tenant_id,
            capability_implementation_revision,
            RegistryResourceKind::CapabilityImplementation,
        )
        .await?;
        if exact_interface.document
            != ResourceDocument::CapabilityInterface((**capability_interface).clone())
            || exact_implementation.document
                != ResourceDocument::CapabilityImplementation((**capability_implementation).clone())
        {
            return Err(RepositoryError::Conflict(
                "Managed MCP Capability published closure",
            ));
        }
        let mcp_now = database_now(transaction).await?;
        let resolved = resolve_mcp_execution_contract(
            transaction,
            &McpExecutionContractQuery {
                schema_version: 1,
                tenant_id: request.tenant_id.clone(),
                mcp_deployment: runtime.mcp_deployment.clone(),
                discovery_snapshot_id: runtime.discovery_snapshot_id.clone(),
                discovery_snapshot_digest: runtime.discovery_snapshot_digest.clone(),
                authorization_binding_id: runtime.authorization_binding_id.clone(),
                authorization_generation: runtime.authorization_generation,
                authorization_context_digest: runtime.authorization_context_digest.clone(),
                principal_id: runtime.principal_id.clone(),
            },
            mcp_now,
        )
        .await?;
        if &resolved != mcp_contract.as_ref() {
            return Err(RepositoryError::Conflict("Managed MCP execution closure"));
        }
        expected_secret_bindings.extend(mcp_contract.deployment_closure.secret_bindings.iter());
        expected_secret_bindings.push(&mcp_contract.authorization.token_secret_binding);
    }
    if expected_secret_bindings
        .iter()
        .map(|binding| &binding.secret_binding_id)
        .collect::<BTreeSet<_>>()
        .len()
        != expected_secret_bindings.len()
        || deployment.resource_version_id != closure.interface.revision_id.to_string()
        || request.secret_grants.len() != expected_secret_bindings.len()
        || request.secret_grants.iter().any(|grant| {
            !expected_secret_bindings.iter().any(|binding| {
                *binding == &grant.secret_binding
                    && binding.permits_resolved_generation(
                        &grant.secret_binding.secret_binding_id,
                        &grant.secret_binding.purpose,
                        grant.resolved_binding_generation,
                    )
            })
        })
    {
        return Err(RepositoryError::Conflict(
            "Sandbox Capability binding closure",
        ));
    }

    verify_sandbox_resource_closure(
        transaction,
        &request.tenant_id,
        &request.runtime_revision,
        &request.runtime,
        &request.package_revision,
        &request.package,
        &request.profile_revision,
        &request.profile,
        &request.policies,
    )
    .await
}

pub(crate) async fn verify_managed_mcp_session_sandbox_bindings(
    transaction: &mut Transaction<'_, Postgres>,
    request: &ManagedMcpSandboxSessionRequest,
) -> Result<(), RepositoryError> {
    verify_sandbox_resource_closure(
        transaction,
        &request.identity.tenant_id,
        &request.runtime_revision,
        &request.runtime,
        &request.package_revision,
        &request.package,
        &request.profile_revision,
        &request.profile,
        &request.policies,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn verify_sandbox_resource_closure(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    runtime_revision: &ExactVersionRef,
    runtime_document: &insight_platform_contracts::SandboxRuntimeResourceSpec,
    package_revision: &ExactVersionRef,
    package_document: &insight_platform_contracts::SandboxPackageResourceSpec,
    profile_revision: &ExactVersionRef,
    profile_document: &insight_platform_contracts::SandboxProfileResourceSpec,
    policies: &SandboxExecutionPolicyClosure,
) -> Result<(), RepositoryError> {
    let runtime = load_enabled_exact_published_version(
        transaction,
        tenant_id,
        runtime_revision,
        RegistryResourceKind::SandboxRuntime,
    )
    .await?;
    let package = load_enabled_exact_published_version(
        transaction,
        tenant_id,
        package_revision,
        RegistryResourceKind::SandboxPackage,
    )
    .await?;
    let profile = load_enabled_exact_published_version(
        transaction,
        tenant_id,
        profile_revision,
        RegistryResourceKind::SandboxProfile,
    )
    .await?;
    if runtime.document != ResourceDocument::SandboxRuntime(runtime_document.clone())
        || package.document != ResourceDocument::SandboxPackage(package_document.clone())
        || profile.document != ResourceDocument::SandboxProfile(profile_document.clone())
    {
        return Err(RepositoryError::Conflict(
            "Sandbox frozen ResourceVersion snapshot",
        ));
    }
    let isolation_policy = load_exact_sandbox_policy(
        transaction,
        tenant_id,
        &profile_document.isolation_policy,
        PolicyKind::Isolation,
    )
    .await?;
    let resource_policy = load_exact_sandbox_policy(
        transaction,
        tenant_id,
        &profile_document.resource_policy,
        PolicyKind::Resource,
    )
    .await?;
    let network_policy = load_exact_sandbox_policy(
        transaction,
        tenant_id,
        &profile_document.network_policy,
        PolicyKind::Network,
    )
    .await?;
    let artifact_io_policy = load_exact_sandbox_policy(
        transaction,
        tenant_id,
        &profile_document.artifact_io_policy,
        PolicyKind::ArtifactIo,
    )
    .await?;
    let secret_policy = match &profile_document.secret_policy {
        Some(exact) => Some(
            load_exact_sandbox_policy(transaction, tenant_id, exact, PolicyKind::SecretResolution)
                .await?,
        ),
        None => None,
    };
    if isolation_policy.sandbox_isolation.as_ref() != Some(&policies.isolation)
        || resource_policy.sandbox_resource.as_ref() != Some(&policies.resource)
        || network_policy.sandbox_network.as_ref() != Some(&policies.network)
        || artifact_io_policy.sandbox_artifact_io.as_ref() != Some(&policies.artifact_io)
        || secret_policy
            .as_ref()
            .and_then(|policy| policy.sandbox_secret_resolution.as_ref())
            != policies.secret_resolution.as_ref()
    {
        return Err(RepositoryError::Conflict(
            "Sandbox frozen Policy document closure",
        ));
    }
    Ok(())
}

async fn load_exact_sandbox_policy(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    exact: &ExactVersionRef,
    expected_kind: PolicyKind,
) -> Result<PolicyResourceSpec, RepositoryError> {
    let published = load_enabled_exact_published_version(
        transaction,
        tenant_id,
        exact,
        RegistryResourceKind::Policy,
    )
    .await?;
    published.document.validate().map_err(|failure| {
        RepositoryError::CorruptRow(format!("published Sandbox Policy is invalid: {failure}"))
    })?;
    match published.document {
        ResourceDocument::Policy(policy) if policy.policy_kind == expected_kind => Ok(policy),
        ResourceDocument::Policy(_) => Err(RepositoryError::Conflict("Sandbox exact Policy kind")),
        _ => Err(RepositoryError::CorruptRow(
            "Sandbox Policy revision contains the wrong Resource document".to_owned(),
        )),
    }
}

async fn lock_and_persist_artifact_grants(
    transaction: &mut Transaction<'_, Postgres>,
    command: &AcceptSandboxExecution,
    database_now: DateTime<Utc>,
) -> Result<(), RepositoryError> {
    lock_and_persist_sandbox_artifact_grants(
        transaction,
        &command.request.tenant_id,
        &command.request.sandbox_job_id,
        command.request.classification,
        &command.request.artifact_grants,
        database_now,
    )
    .await
}

pub(crate) async fn lock_and_persist_managed_mcp_session_artifact_grants(
    transaction: &mut Transaction<'_, Postgres>,
    request: &ManagedMcpSandboxSessionRequest,
    database_now: DateTime<Utc>,
) -> Result<(), RepositoryError> {
    lock_and_persist_sandbox_artifact_grants(
        transaction,
        &request.identity.tenant_id,
        &request.identity.sandbox_job_id,
        request.classification,
        &request.artifact_grants,
        database_now,
    )
    .await
}

async fn lock_and_persist_sandbox_artifact_grants(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    sandbox_job_id: &ResourceId,
    request_classification: DataClassification,
    grants: &[ScopedArtifactGrant],
    database_now: DateTime<Utc>,
) -> Result<(), RepositoryError> {
    for grant in grants {
        let (source_artifact_id, target_artifact_id) = match grant.operation {
            ArtifactGrantOperation::ReadWhole | ArtifactGrantOperation::ReadRange => {
                let artifact = grant.artifact.as_ref().ok_or_else(|| {
                    RepositoryError::InvalidInput(
                        "Sandbox read grant has no Artifact reference".to_owned(),
                    )
                })?;
                let row = sqlx::query(
                    r#"
                    SELECT artifact.classification, artifact.verified_media_type,
                           blob.content_digest, blob.size_bytes
                    FROM insight_platform.artifacts AS artifact
                    JOIN insight_platform.artifact_blobs AS blob
                      ON blob.tenant_id = artifact.tenant_id AND blob.blob_id = artifact.blob_id
                    WHERE artifact.tenant_id = $1 AND artifact.artifact_id = $2
                      AND artifact.state = 'ready' AND artifact.terminal_at IS NULL
                      AND blob.state = 'verified' AND blob.deleted_at IS NULL
                    FOR KEY SHARE OF artifact, blob
                    "#,
                )
                .bind(tenant_id.to_string())
                .bind(artifact.artifact_id().to_string())
                .fetch_optional(&mut **transaction)
                .await?
                .ok_or(RepositoryError::NotFound("Ready Sandbox input Artifact"))?;
                if row.try_get::<String, _>("classification")? != artifact.classification().as_str()
                    || row
                        .try_get::<Option<String>, _>("verified_media_type")?
                        .as_deref()
                        != Some(artifact.media_type())
                    || row
                        .try_get::<Option<String>, _>("content_digest")?
                        .as_deref()
                        != Some(artifact.content_digest().to_string().as_str())
                    || row.try_get::<Option<i64>, _>("size_bytes")?
                        != Some(i64::try_from(artifact.byte_length()).map_err(|_| {
                            RepositoryError::InvalidInput(
                                "Sandbox Artifact size exceeds bigint".to_owned(),
                            )
                        })?)
                {
                    return Err(RepositoryError::Conflict(
                        "Sandbox input Artifact reference",
                    ));
                }
                (Some(artifact.artifact_id().to_string()), None)
            }
            ArtifactGrantOperation::WriteStaging | ArtifactGrantOperation::CommitStaging => {
                let artifact_id = grant.staging_artifact_id.as_ref().ok_or_else(|| {
                    RepositoryError::InvalidInput(
                        "Sandbox output grant has no staging Artifact".to_owned(),
                    )
                })?;
                let row = sqlx::query(
                    r#"
                    SELECT state, expected_size_bytes, classification
                    FROM insight_platform.artifacts
                    WHERE tenant_id = $1 AND artifact_id = $2
                    FOR KEY SHARE
                    "#,
                )
                .bind(tenant_id.to_string())
                .bind(artifact_id.to_string())
                .fetch_optional(&mut **transaction)
                .await?
                .ok_or(RepositoryError::NotFound("Sandbox staging Artifact"))?;
                let classification = row
                    .try_get::<String, _>("classification")?
                    .parse::<insight_platform_contracts::DataClassification>()
                    .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
                if row.try_get::<String, _>("state")? != "staging"
                    || row.try_get::<i64, _>("expected_size_bytes")?
                        < i64::try_from(grant.maximum_bytes).map_err(|_| {
                            RepositoryError::InvalidInput(
                                "Sandbox grant size exceeds bigint".to_owned(),
                            )
                        })?
                    || classification.rank() < request_classification.rank()
                {
                    return Err(RepositoryError::Conflict("Sandbox staging Artifact grant"));
                }
                (None, Some(artifact_id.to_string()))
            }
        };
        if grant.expires_at <= database_now {
            return Err(RepositoryError::Conflict("expired Sandbox Artifact grant"));
        }
        let payload = TypedPayload::from_versioned(1, grant, 262_144)?;
        sqlx::query(
            r#"
            INSERT INTO insight_platform.artifact_links (
                tenant_id, artifact_link_id, link_kind, owner_kind, owner_id,
                source_artifact_id, target_artifact_id, link_key_digest, state,
                version, payload_schema_version, payload, payload_digest,
                expires_at, created_at, updated_at
            ) VALUES ($1, $2, 'grant', 'sandbox_job', $3, $4, $5, $6, 'active',
                      1, $7, $8, $9, $10, $11, $11)
            "#,
        )
        .bind(tenant_id.to_string())
        .bind(grant.grant_id.to_string())
        .bind(sandbox_job_id.to_string())
        .bind(source_artifact_id)
        .bind(target_artifact_id)
        .bind(grant.grant_digest.to_string())
        .bind(payload.schema_version)
        .bind(&payload.value)
        .bind(&payload.digest)
        .bind(grant.expires_at)
        .bind(database_now)
        .execute(&mut **transaction)
        .await?;
    }
    Ok(())
}

async fn lock_secret_grants(
    transaction: &mut Transaction<'_, Postgres>,
    command: &AcceptSandboxExecution,
) -> Result<(), RepositoryError> {
    lock_sandbox_secret_grants(
        transaction,
        &command.request.tenant_id,
        &command.request.secret_grants,
    )
    .await
}

pub(crate) async fn lock_managed_mcp_session_secret_grants(
    transaction: &mut Transaction<'_, Postgres>,
    request: &ManagedMcpSandboxSessionRequest,
) -> Result<(), RepositoryError> {
    lock_sandbox_secret_grants(
        transaction,
        &request.identity.tenant_id,
        &request.secret_grants,
    )
    .await
}

async fn lock_sandbox_secret_grants(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    grants: &[ScopedSecretGrant],
) -> Result<(), RepositoryError> {
    for grant in grants {
        let row = sqlx::query(
            r#"
            SELECT purpose, state, generation
            FROM insight_platform.secret_bindings
            WHERE tenant_id = $1 AND secret_binding_id = $2
            FOR KEY SHARE
            "#,
        )
        .bind(tenant_id.to_string())
        .bind(grant.secret_binding.secret_binding_id.to_string())
        .fetch_optional(&mut **transaction)
        .await?
        .ok_or(RepositoryError::NotFound("Sandbox SecretBinding"))?;
        if row.try_get::<String, _>("purpose")? != grant.secret_binding.purpose.as_str()
            || row.try_get::<String, _>("state")? != "active"
            || row.try_get::<i64, _>("generation")?
                != i64::try_from(grant.resolved_binding_generation).map_err(|_| {
                    RepositoryError::InvalidInput(
                        "Sandbox Secret generation exceeds bigint".to_owned(),
                    )
                })?
        {
            return Err(RepositoryError::Conflict("Sandbox Secret grant"));
        }
    }
    Ok(())
}

async fn reserve_sandbox_quota(
    transaction: &mut Transaction<'_, Postgres>,
    command: &AcceptSandboxExecution,
    database_now: DateTime<Utc>,
) -> Result<(), RepositoryError> {
    reserve_sandbox_quota_bundle(
        transaction,
        &command.request.tenant_id,
        &command.usage_reservation_id,
        &command.quota_entry_ids,
        &command.request.request_digest,
        &command.request.resources,
        database_now,
    )
    .await
}

pub(crate) async fn reserve_managed_mcp_session_quota(
    transaction: &mut Transaction<'_, Postgres>,
    request: &ManagedMcpSandboxSessionRequest,
    usage_reservation_id: &ResourceId,
    quota_entry_ids: &[ResourceId],
    database_now: DateTime<Utc>,
) -> Result<(), RepositoryError> {
    reserve_sandbox_quota_bundle(
        transaction,
        &request.identity.tenant_id,
        usage_reservation_id,
        quota_entry_ids,
        &request.request_digest,
        &request.resources,
        database_now,
    )
    .await
}

async fn reserve_sandbox_quota_bundle(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    usage_reservation_id: &ResourceId,
    quota_entry_ids: &[ResourceId],
    request_digest: &Sha256Digest,
    resources: &SandboxResourceEnvelope,
    database_now: DateTime<Utc>,
) -> Result<(), RepositoryError> {
    if quota_entry_ids.len() != SANDBOX_QUOTA_LINES
        || quota_entry_ids
            .iter()
            .any(|entry_id| entry_id.kind() != ResourceKind::QuotaLedgerEntry)
        || quota_entry_ids.iter().collect::<BTreeSet<_>>().len() != quota_entry_ids.len()
    {
        return Err(RepositoryError::InvalidInput(
            "Sandbox quota ledger identity bundle is invalid".to_owned(),
        ));
    }
    let expected_metrics = vec![
        QuotaDimension::SandboxConcurrentExecutions
            .as_str()
            .to_owned(),
        QuotaDimension::SandboxCpuSeconds.as_str().to_owned(),
        QuotaDimension::SandboxMemoryMebibytes.as_str().to_owned(),
        QuotaDimension::SandboxOutputBytes.as_str().to_owned(),
    ];
    let rows = sqlx::query(
        r#"
        SELECT tenant_id, quota_account_id, metric, limit_value, reserved_value,
               used_value, version
        FROM insight_platform.quota_accounts
        WHERE tenant_id = $1 AND scope_kind = 'tenant' AND scope_id = $1
          AND work_class = 'sandbox' AND metric = ANY($2)
        ORDER BY tenant_id, quota_account_id
        FOR UPDATE
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(&expected_metrics)
    .fetch_all(&mut **transaction)
    .await?;
    if rows.len() != SANDBOX_QUOTA_LINES {
        return Err(RepositoryError::QuotaExceeded);
    }
    let mut metrics = BTreeSet::new();
    for (row, entry_id) in rows.iter().zip(quota_entry_ids) {
        let metric: String = row.try_get("metric")?;
        if !metrics.insert(metric.clone()) {
            return Err(RepositoryError::CorruptRow(
                "duplicate Sandbox quota metric".to_owned(),
            ));
        }
        let amount = sandbox_reservation_amount(&metric, resources)?;
        let version: i64 = row.try_get("version")?;
        let next_version: i64 = sqlx::query_scalar(
            r#"
            UPDATE insight_platform.quota_accounts
            SET reserved_value = reserved_value + $4,
                version = version + 1, updated_at = $5
            WHERE tenant_id = $1 AND quota_account_id = $2 AND version = $3
              AND reserved_value + used_value + $4 <= limit_value
            RETURNING version
            "#,
        )
        .bind(row.try_get::<String, _>("tenant_id")?)
        .bind(row.try_get::<String, _>("quota_account_id")?)
        .bind(version)
        .bind(amount)
        .bind(database_now)
        .fetch_optional(&mut **transaction)
        .await?
        .ok_or(RepositoryError::QuotaExceeded)?;
        sqlx::query(
            r#"
            INSERT INTO insight_platform.quota_ledger (
                tenant_id, quota_entry_id, quota_account_id, correlation_id,
                entry_kind, reserved_amount, used_amount, account_version,
                request_digest, created_at
            ) VALUES ($1, $2, $3, $4, 'reserve', $5, 0, $6, $7, $8)
            "#,
        )
        .bind(tenant_id.to_string())
        .bind(entry_id.to_string())
        .bind(row.try_get::<String, _>("quota_account_id")?)
        .bind(usage_reservation_id.to_string())
        .bind(amount)
        .bind(next_version)
        .bind(request_digest.to_string())
        .bind(database_now)
        .execute(&mut **transaction)
        .await?;
    }
    if metrics != expected_metrics.into_iter().collect::<BTreeSet<_>>() {
        return Err(RepositoryError::CorruptRow(
            "Sandbox quota bundle is incomplete".to_owned(),
        ));
    }
    Ok(())
}

async fn load_sandbox_job(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    job_id: &ResourceId,
    limits: SandboxCommandLimits,
) -> Result<LockedSandboxJob, RepositoryError> {
    let record =
        load_job_for_update_by_text(transaction, &tenant_id.to_string(), &job_id.to_string())
            .await?;
    let job = job_projection(&record)?;
    let payload = decode_sandbox_capability_payload(&record.payload)?;
    payload
        .validate_for(&job, limits)
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    Ok(LockedSandboxJob {
        record,
        job,
        payload,
    })
}

async fn load_sandbox_job_read_only(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    job_id: &ResourceId,
    limits: SandboxCommandLimits,
) -> Result<LockedSandboxJob, RepositoryError> {
    let row = sqlx::query(
        r#"
        SELECT * FROM insight_platform.jobs
        WHERE tenant_id = $1 AND job_id = $2
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(job_id.to_string())
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::NotFound("Sandbox Job"))?;
    let record = job_from_row(row)?;
    let job = job_projection(&record)?;
    let payload = decode_sandbox_capability_payload(&record.payload)?;
    payload
        .validate_for(&job, limits)
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    Ok(LockedSandboxJob {
        record,
        job,
        payload,
    })
}

fn decode_sandbox_capability_payload(
    payload: &TypedPayload,
) -> Result<SandboxExecutionJobPayload, RepositoryError> {
    decode_versioned_payload::<SandboxJobPayload>(payload, "Sandbox Job")?
        .into_capability_execution()
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))
}

async fn load_sandbox_decision(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    job_id: &ResourceId,
    limits: SandboxCommandLimits,
) -> Result<SandboxPhaseDecision, RepositoryError> {
    let current = load_sandbox_job(transaction, tenant_id, job_id, limits).await?;
    Ok(SandboxPhaseDecision {
        job: current.job,
        payload: current.payload,
    })
}

fn require_sandbox_command_owner(
    current: &LockedSandboxJob,
    sandbox_job_id: &ResourceId,
    tenant_id: &ResourceId,
) -> Result<(), RepositoryError> {
    if &current.job.tenant_id != tenant_id
        || &current.payload.request.sandbox_job_id != sandbox_job_id
        || current.record.owner_id != sandbox_job_id.to_string()
        || current.record.work_class != "sandbox"
        || current.record.invocation_id.as_deref()
            != Some(current.payload.request.invocation_id.to_string().as_str())
        || current.record.request_digest != current.payload.submission_digest.to_string()
    {
        return Err(RepositoryError::Conflict("Sandbox Job owner"));
    }
    Ok(())
}

fn require_phase_replay(
    decision: &SandboxPhaseDecision,
    command: &CommitSandboxPhase,
) -> Result<(), RepositoryError> {
    if decision.payload.physical_state != command.target
        || decision.payload.executor_identity_digest.as_ref()
            != Some(&command.executor_identity_digest)
        || decision.payload.attestor_route.as_ref() != Some(&command.attestor_route)
        || decision.payload.phase_evidence_digest.as_ref() != Some(&command.phase_evidence_digest)
        || decision.job.version != command.fence.expected_version.saturating_add(1)
    {
        return Err(RepositoryError::Conflict("Sandbox phase replay"));
    }
    Ok(())
}

fn require_outcome_replay(
    decision: &SandboxPhaseDecision,
    command: &CommitSandboxOutcome,
) -> Result<(), RepositoryError> {
    if decision.payload.outcome.as_ref() != Some(&command.outcome)
        || decision.payload.cleanup.as_ref() != Some(&command.cleanup)
        || decision.payload.phase_evidence_digest.as_ref() != Some(&command.phase_evidence_digest)
        || decision.job.version != command.fence.expected_version.saturating_add(1)
    {
        return Err(RepositoryError::Conflict("Sandbox outcome replay"));
    }
    Ok(())
}

async fn update_sandbox_job(
    transaction: &mut Transaction<'_, Postgres>,
    current: &JobRecord,
    decision: &SandboxPhaseDecision,
    database_now: DateTime<Utc>,
    release_quota: bool,
) -> Result<JobRecord, RepositoryError> {
    let stored_payload = SandboxJobPayload::capability_execution(decision.payload.clone());
    let payload = TypedPayload::from_versioned(1, &stored_payload, 1_048_576)?;
    let (worker_id, lease_token_digest, lease_expires_at, heartbeat_at) = decision
        .job
        .lease
        .as_ref()
        .map(|lease| {
            (
                Some(lease.worker_process_generation_id.to_string()),
                Some(lease.token_digest.to_string()),
                Some(lease.expires_at),
                Some(lease.heartbeat_at),
            )
        })
        .unwrap_or((None, None, None, None));
    let shared_terminal = matches!(
        decision.job.state,
        JobState::Succeeded | JobState::Failed | JobState::Cancelled | JobState::TimedOut
    );
    let physical_terminal = matches!(
        decision.payload.physical_state,
        SandboxJobState::Succeeded
            | SandboxJobState::Failed
            | SandboxJobState::Cancelled
            | SandboxJobState::TimedOut
            | SandboxJobState::Lost
    );
    if release_quota != physical_terminal {
        return Err(RepositoryError::CorruptRow(
            "Sandbox quota release differs from physical outcome".to_owned(),
        ));
    }
    let result_digest = physical_terminal.then_some(payload.digest.clone());
    let started_at = if current.started_at.is_none() && decision.job.state == JobState::Running {
        Some(database_now)
    } else {
        current.started_at
    };
    let row = sqlx::query(
        r#"
        UPDATE insight_platform.jobs
        SET state = $4, version = $5, attempt_no = $6, lease_epoch = $7,
            worker_id = $8, lease_token_digest = $9, lease_expires_at = $10,
            heartbeat_at = $11, scheduled_at = $12, retry_at = $13,
            result_digest = $14, payload_schema_version = $15, payload = $16,
            payload_digest = $17, started_at = $18, terminal_at = $19,
            updated_at = $20, quota_reservation_id = $21
        WHERE tenant_id = $1 AND job_id = $2 AND version = $3
        RETURNING *
        "#,
    )
    .bind(&current.tenant_id)
    .bind(&current.job_id)
    .bind(current.version)
    .bind(decision.job.state.as_str())
    .bind(as_i64(decision.job.version, "Sandbox Job version")?)
    .bind(i32::try_from(decision.job.attempt_count).map_err(|_| {
        RepositoryError::InvalidInput("Sandbox attempt count exceeds integer".to_owned())
    })?)
    .bind(as_i64(
        decision.job.lease_generation,
        "Sandbox lease generation",
    )?)
    .bind(worker_id)
    .bind(lease_token_digest)
    .bind(lease_expires_at)
    .bind(heartbeat_at)
    .bind(decision.job.scheduled_at)
    .bind(decision.job.retry_at)
    .bind(result_digest)
    .bind(payload.schema_version)
    .bind(&payload.value)
    .bind(&payload.digest)
    .bind(started_at)
    .bind(shared_terminal.then_some(database_now))
    .bind(database_now)
    .bind(
        (!release_quota)
            .then(|| current.quota_reservation_id.clone())
            .flatten(),
    )
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::Conflict("Sandbox Job"))?;
    job_from_row(row)
}

async fn append_sandbox_event(
    transaction: &mut Transaction<'_, Postgres>,
    audit: &SandboxWorkerAudit,
    job: &JobRecord,
    event_type: &str,
    decision: &SandboxPhaseDecision,
) -> Result<(), RepositoryError> {
    append_scheduler_event(
        transaction,
        &job.tenant_id,
        &audit.event_id,
        &audit.outbox_id,
        "job",
        &job.job_id,
        job.version,
        job.run_id.as_deref(),
        event_type,
        &TypedPayload::new(
            1,
            &serde_json::json!({
                "job_id": job.job_id,
                "lease_generation": decision.job.lease_generation,
                "phase_sequence": decision.payload.phase_sequence,
                "physical_state": decision.payload.physical_state,
                "sandbox_job_id": decision.payload.request.sandbox_job_id,
            }),
        )?,
    )
    .await
}

fn sandbox_recovery_result(command: &RecoverExpiredSandboxLease) -> SandboxLeaseRecoveryResult {
    let disposition = match &command.action {
        SandboxLeaseRecoveryAction::RequeueUnstarted { .. } => {
            SandboxLeaseRecoveryDisposition::Requeued
        }
        SandboxLeaseRecoveryAction::TimeoutUnstarted { .. } => {
            SandboxLeaseRecoveryDisposition::TimedOut
        }
        SandboxLeaseRecoveryAction::MarkLost { .. } => SandboxLeaseRecoveryDisposition::Lost,
    };
    SandboxLeaseRecoveryResult {
        tenant_id: command.audit.tenant_id.clone(),
        sandbox_job_id: command.sandbox_job_id.clone(),
        job_id: command.job_id.clone(),
        recovered_lease_generation: command.observed_lease_generation,
        disposition,
    }
}

async fn append_sandbox_recovery_event(
    transaction: &mut Transaction<'_, Postgres>,
    audit: &SandboxRecoveryAudit,
    job: &JobRecord,
    command: &RecoverExpiredSandboxLease,
    decision: &SandboxPhaseDecision,
) -> Result<(), RepositoryError> {
    let event_type = match &command.action {
        SandboxLeaseRecoveryAction::RequeueUnstarted { .. } => "sandbox.job.lease_recovered",
        SandboxLeaseRecoveryAction::TimeoutUnstarted { .. }
        | SandboxLeaseRecoveryAction::MarkLost { .. } => "sandbox.job.failed",
    };
    append_scheduler_event(
        transaction,
        &job.tenant_id,
        &audit.event_id,
        &audit.outbox_id,
        "job",
        &job.job_id,
        job.version,
        job.run_id.as_deref(),
        event_type,
        &TypedPayload::new(
            1,
            &serde_json::json!({
                "job_id": job.job_id,
                "observed_job_version": command.observed_job_version,
                "observed_lease_generation": command.observed_lease_generation,
                "previous_worker_process_generation_id": command.previous_worker_process_generation_id,
                "recovery_process_generation_id": audit.recovery_process_generation_id,
                "recovery_action": command.action,
                "physical_state": decision.payload.physical_state,
                "phase_sequence": decision.payload.phase_sequence,
                "sandbox_job_id": command.sandbox_job_id,
            }),
        )?,
    )
    .await
}

fn sandbox_controller_operation(reason: SandboxStopReason) -> &'static str {
    match reason {
        SandboxStopReason::Cancelled => "sandbox.control.prestart.cancel",
        SandboxStopReason::TimedOut => "sandbox.control.prestart.timeout",
    }
}

async fn load_sandbox_controller_receipt(
    transaction: &mut Transaction<'_, Postgres>,
    command: &StopUnclaimedSandboxJob,
) -> Result<bool, RepositoryError> {
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
    .bind(command.job_id.to_string())
    .bind(command.audit.source_event_id.to_string())
    .bind(sandbox_controller_operation(command.reason))
    .bind(command.audit.idempotency_key_digest.to_string())
    .fetch_optional(&mut **transaction)
    .await?;
    let Some(row) = row else {
        return Ok(false);
    };
    if row.try_get::<String, _>("request_digest")? != command.audit.request_digest.to_string() {
        return Err(RepositoryError::IdempotencyConflict);
    }
    if row.try_get::<String, _>("state")? != "succeeded" {
        return Err(RepositoryError::Conflict(
            "Sandbox controller JobCommit receipt",
        ));
    }
    Ok(true)
}

async fn insert_sandbox_controller_receipt(
    transaction: &mut Transaction<'_, Postgres>,
    command: &StopUnclaimedSandboxJob,
) -> Result<(), RepositoryError> {
    let payload = TypedPayload::new(
        1,
        &serde_json::json!({
            "controller_process_generation_id": command.audit.controller_process_generation_id,
            "job_id": command.job_id,
            "operation": sandbox_controller_operation(command.reason),
            "source_event_id": command.audit.source_event_id,
            "source_event_payload_digest": command.audit.source_event_payload_digest,
            "source_invocation_version": command.audit.source_invocation_version,
        }),
    )?;
    sqlx::query(
        r#"
        INSERT INTO insight_platform.receipts (
            tenant_id, receipt_id, receipt_kind, scope_kind, scope_id,
            dedupe_owner_id, operation, idempotency_key_digest, request_digest, state,
            payload_schema_version, payload, payload_digest, expires_at
        ) VALUES ($1, $2, 'job_commit', 'job', $3, $4, $5, $6, $7,
                  'processing', $8, $9, $10, $11)
        "#,
    )
    .bind(command.audit.tenant_id.to_string())
    .bind(command.audit.receipt_id.to_string())
    .bind(command.job_id.to_string())
    .bind(command.audit.source_event_id.to_string())
    .bind(sandbox_controller_operation(command.reason))
    .bind(command.audit.idempotency_key_digest.to_string())
    .bind(command.audit.request_digest.to_string())
    .bind(payload.schema_version)
    .bind(&payload.value)
    .bind(&payload.digest)
    .bind(command.audit.receipt_expires_at)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

async fn terminalize_sandbox_controller_receipt(
    transaction: &mut Transaction<'_, Postgres>,
    command: &StopUnclaimedSandboxJob,
) -> Result<(), RepositoryError> {
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
    .bind(match command.reason {
        SandboxStopReason::Cancelled => "cancelled",
        SandboxStopReason::TimedOut => "timed_out",
    })
    .bind(command.job_id.to_string())
    .execute(&mut **transaction)
    .await?
    .rows_affected();
    if affected != 1 {
        return Err(RepositoryError::Conflict(
            "Sandbox controller JobCommit receipt",
        ));
    }
    Ok(())
}

async fn append_sandbox_controller_event(
    transaction: &mut Transaction<'_, Postgres>,
    command: &StopUnclaimedSandboxJob,
    job: &JobRecord,
    decision: &SandboxPhaseDecision,
) -> Result<(), RepositoryError> {
    let transitions = match command.reason {
        SandboxStopReason::Cancelled => vec!["cancelling", "cancelled"],
        SandboxStopReason::TimedOut => vec!["timed_out"],
    };
    append_scheduler_event(
        transaction,
        &job.tenant_id,
        &command.audit.event_id,
        &command.audit.outbox_id,
        "job",
        &job.job_id,
        job.version,
        job.run_id.as_deref(),
        "sandbox.job.failed",
        &TypedPayload::new(
            1,
            &serde_json::json!({
                "controller_process_generation_id": command.audit.controller_process_generation_id,
                "job_id": job.job_id,
                "phase_sequence": decision.payload.phase_sequence,
                "physical_state": decision.payload.physical_state,
                "sandbox_job_id": command.sandbox_job_id,
                "source_event_id": command.audit.source_event_id,
                "source_event_payload_digest": command.audit.source_event_payload_digest,
                "source_invocation_version": command.audit.source_invocation_version,
                "transitions": transitions,
            }),
        )?,
    )
    .await
}

pub(crate) async fn claim_sandbox_worker_receipt(
    transaction: &mut Transaction<'_, Postgres>,
    audit: &SandboxWorkerAudit,
    job_id: &ResourceId,
    operation: &str,
) -> Result<bool, RepositoryError> {
    let payload = TypedPayload::new(
        1,
        &serde_json::json!({
            "job_id": job_id,
            "operation": operation,
            "worker_process_generation_id": audit.worker_process_generation_id,
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
    .bind(audit.tenant_id.to_string())
    .bind(job_id.to_string())
    .bind(audit.worker_process_generation_id.to_string())
    .bind(operation)
    .bind(audit.idempotency_key_digest.to_string())
    .fetch_one(&mut **transaction)
    .await?;
    if row.try_get::<String, _>("request_digest")? != audit.request_digest.to_string() {
        return Err(RepositoryError::IdempotencyConflict);
    }
    if row.try_get::<String, _>("state")? != "succeeded" {
        return Err(RepositoryError::Conflict("Sandbox JobCommit receipt"));
    }
    Ok(true)
}

async fn claim_sandbox_recovery_receipt(
    transaction: &mut Transaction<'_, Postgres>,
    command: &RecoverExpiredSandboxLease,
) -> Result<bool, RepositoryError> {
    let operation = "sandbox.lease.recover";
    let payload = TypedPayload::new(
        1,
        &serde_json::json!({
            "job_id": command.job_id,
            "observed_lease_generation": command.observed_lease_generation,
            "previous_worker_process_generation_id": command.previous_worker_process_generation_id,
            "recovery_process_generation_id": command.audit.recovery_process_generation_id,
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
    .bind(command.job_id.to_string())
    .bind(command.previous_worker_process_generation_id.to_string())
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
    .bind(command.job_id.to_string())
    .bind(command.previous_worker_process_generation_id.to_string())
    .bind(operation)
    .bind(command.audit.idempotency_key_digest.to_string())
    .fetch_one(&mut **transaction)
    .await?;
    if row.try_get::<String, _>("request_digest")? != command.audit.request_digest.to_string() {
        return Err(RepositoryError::IdempotencyConflict);
    }
    if row.try_get::<String, _>("state")? != "succeeded" {
        return Err(RepositoryError::Conflict("Sandbox lease recovery receipt"));
    }
    Ok(true)
}

async fn terminalize_sandbox_recovery_receipt(
    transaction: &mut Transaction<'_, Postgres>,
    command: &RecoverExpiredSandboxLease,
) -> Result<(), RepositoryError> {
    let result = sandbox_recovery_result(command);
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
        SandboxLeaseRecoveryDisposition::Requeued => "requeued",
        SandboxLeaseRecoveryDisposition::TimedOut => "timed_out",
        SandboxLeaseRecoveryDisposition::Lost => "lost",
    })
    .bind(command.job_id.to_string())
    .execute(&mut **transaction)
    .await?
    .rows_affected();
    if affected != 1 {
        return Err(RepositoryError::Conflict("Sandbox lease recovery receipt"));
    }
    Ok(())
}

pub(crate) async fn terminalize_sandbox_worker_receipt(
    transaction: &mut Transaction<'_, Postgres>,
    audit: &SandboxWorkerAudit,
    job_id: &ResourceId,
    disposition: &str,
) -> Result<(), RepositoryError> {
    let affected = sqlx::query(
        r#"
        UPDATE insight_platform.receipts
        SET state = 'succeeded', disposition = $4, response_reference_id = $5,
            completed_at = clock_timestamp()
        WHERE tenant_id = $1 AND receipt_id = $2 AND request_digest = $3
          AND state = 'processing'
        "#,
    )
    .bind(audit.tenant_id.to_string())
    .bind(audit.receipt_id.to_string())
    .bind(audit.request_digest.to_string())
    .bind(disposition)
    .bind(job_id.to_string())
    .execute(&mut **transaction)
    .await?
    .rows_affected();
    if affected != 1 {
        return Err(RepositoryError::Conflict("Sandbox JobCommit receipt"));
    }
    Ok(())
}

#[derive(Debug)]
pub(crate) struct SandboxQuotaLine {
    tenant_id: String,
    quota_account_id: String,
    metric: String,
    reserved_amount: i64,
    account_version: i64,
}

async fn lock_sandbox_quota_bundle(
    transaction: &mut Transaction<'_, Postgres>,
    current: &LockedSandboxJob,
) -> Result<Vec<SandboxQuotaLine>, RepositoryError> {
    let reservation_id = current
        .record
        .quota_reservation_id
        .as_deref()
        .ok_or_else(|| {
            RepositoryError::CorruptRow("Sandbox Job has no quota reservation".to_owned())
        })?;
    lock_sandbox_quota_bundle_for(
        transaction,
        &current.record.tenant_id,
        reservation_id,
        &current.payload.request.resources,
    )
    .await
}

pub(crate) async fn lock_sandbox_quota_bundle_for(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &str,
    reservation_id: &str,
    resources: &SandboxResourceEnvelope,
) -> Result<Vec<SandboxQuotaLine>, RepositoryError> {
    let rows = sqlx::query(
        r#"
        SELECT account.tenant_id, account.quota_account_id, account.scope_kind,
               account.scope_id, account.work_class, account.metric,
               account.reserved_value, account.version,
               reserve.reserved_amount, reserve.used_amount AS reservation_used_amount
        FROM insight_platform.quota_ledger AS reserve
        JOIN insight_platform.quota_accounts AS account
          ON account.tenant_id = reserve.tenant_id
         AND account.quota_account_id = reserve.quota_account_id
        WHERE reserve.tenant_id = $1 AND reserve.correlation_id = $2
          AND reserve.entry_kind = 'reserve'
        ORDER BY account.tenant_id, account.quota_account_id
        FOR UPDATE OF account
        "#,
    )
    .bind(tenant_id)
    .bind(reservation_id)
    .fetch_all(&mut **transaction)
    .await?;
    if rows.len() != SANDBOX_QUOTA_LINES {
        return Err(RepositoryError::CorruptRow(
            "Sandbox quota bundle must contain exactly four lines".to_owned(),
        ));
    }
    let already_settled: bool = sqlx::query_scalar(
        r#"
        SELECT EXISTS (
            SELECT 1 FROM insight_platform.quota_ledger
            WHERE tenant_id = $1 AND correlation_id = $2 AND entry_kind = 'settle'
        )
        "#,
    )
    .bind(tenant_id)
    .bind(reservation_id)
    .fetch_one(&mut **transaction)
    .await?;
    if already_settled {
        return Err(RepositoryError::Conflict("Sandbox quota settlement"));
    }
    let mut metrics = BTreeSet::new();
    let mut lines = Vec::with_capacity(rows.len());
    for row in rows {
        let metric: String = row.try_get("metric")?;
        let reserved_amount: i64 = row.try_get("reserved_amount")?;
        let row_tenant_id: String = row.try_get("tenant_id")?;
        let scope_kind: String = row.try_get("scope_kind")?;
        let scope_id: String = row.try_get("scope_id")?;
        let work_class: String = row.try_get("work_class")?;
        let account_reserved: i64 = row.try_get("reserved_value")?;
        if row_tenant_id != tenant_id
            || scope_kind != "tenant"
            || scope_id != tenant_id
            || work_class != "sandbox"
            || !metrics.insert(metric.clone())
            || reserved_amount != sandbox_reservation_amount(&metric, resources)?
            || row.try_get::<i64, _>("reservation_used_amount")? != 0
            || account_reserved < reserved_amount
        {
            return Err(RepositoryError::CorruptRow(
                "Sandbox quota reservation is invalid".to_owned(),
            ));
        }
        lines.push(SandboxQuotaLine {
            tenant_id: row_tenant_id,
            quota_account_id: row.try_get("quota_account_id")?,
            metric,
            reserved_amount,
            account_version: row.try_get("version")?,
        });
    }
    let expected = BTreeSet::from([
        QuotaDimension::SandboxConcurrentExecutions
            .as_str()
            .to_owned(),
        QuotaDimension::SandboxCpuSeconds.as_str().to_owned(),
        QuotaDimension::SandboxMemoryMebibytes.as_str().to_owned(),
        QuotaDimension::SandboxOutputBytes.as_str().to_owned(),
    ]);
    if metrics != expected {
        return Err(RepositoryError::CorruptRow(
            "Sandbox quota bundle is incomplete".to_owned(),
        ));
    }
    Ok(lines)
}

async fn settle_sandbox_quota(
    transaction: &mut Transaction<'_, Postgres>,
    current: &LockedSandboxJob,
    lines: &[SandboxQuotaLine],
    entry_ids: &[ResourceId],
    outcome: Option<&SandboxExecutionOutcome>,
    request_digest: &Sha256Digest,
) -> Result<(), RepositoryError> {
    let reservation_id = current
        .record
        .quota_reservation_id
        .as_deref()
        .ok_or_else(|| {
            RepositoryError::CorruptRow("Sandbox Job has no quota reservation".to_owned())
        })?;
    let usage = match outcome {
        Some(SandboxExecutionOutcome::Completed(output)) => Some(&output.usage),
        Some(SandboxExecutionOutcome::ManagedMcp(output)) => Some(&output.usage),
        Some(
            SandboxExecutionOutcome::Failed(_)
            | SandboxExecutionOutcome::Cancelled(_)
            | SandboxExecutionOutcome::TimedOut(_)
            | SandboxExecutionOutcome::Uncertain(_),
        )
        | None => None,
    };
    settle_sandbox_quota_lines(
        transaction,
        reservation_id,
        lines,
        entry_ids,
        usage,
        outcome.is_some(),
        request_digest,
    )
    .await
}

pub(crate) async fn settle_managed_mcp_session_quota(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    usage_reservation_id: &ResourceId,
    resources: &SandboxResourceEnvelope,
    entry_ids: &[ResourceId],
    request_digest: &Sha256Digest,
) -> Result<(), RepositoryError> {
    let tenant = tenant_id.to_string();
    let reservation = usage_reservation_id.to_string();
    let lines =
        lock_sandbox_quota_bundle_for(transaction, &tenant, &reservation, resources).await?;
    settle_sandbox_quota_lines(
        transaction,
        &reservation,
        &lines,
        entry_ids,
        None,
        true,
        request_digest,
    )
    .await
}

pub(crate) async fn settle_unstarted_managed_mcp_session_quota(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    usage_reservation_id: &ResourceId,
    resources: &SandboxResourceEnvelope,
    entry_ids: &[ResourceId],
    request_digest: &Sha256Digest,
) -> Result<(), RepositoryError> {
    let tenant = tenant_id.to_string();
    let reservation = usage_reservation_id.to_string();
    let lines =
        lock_sandbox_quota_bundle_for(transaction, &tenant, &reservation, resources).await?;
    settle_sandbox_quota_lines(
        transaction,
        &reservation,
        &lines,
        entry_ids,
        None,
        false,
        request_digest,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn settle_sandbox_quota_lines(
    transaction: &mut Transaction<'_, Postgres>,
    reservation_id: &str,
    lines: &[SandboxQuotaLine],
    entry_ids: &[ResourceId],
    usage: Option<&SandboxResourceUsage>,
    charge_unknown_usage: bool,
    request_digest: &Sha256Digest,
) -> Result<(), RepositoryError> {
    if lines.len() != SANDBOX_QUOTA_LINES || entry_ids.len() != SANDBOX_QUOTA_LINES {
        return Err(RepositoryError::InvalidInput(
            "Sandbox quota settlement line count is invalid".to_owned(),
        ));
    }
    for (line, entry_id) in lines.iter().zip(entry_ids) {
        let used_amount = sandbox_used_amount(line, usage, charge_unknown_usage)?;
        if used_amount < 0 || used_amount > line.reserved_amount {
            return Err(RepositoryError::QuotaExceeded);
        }
        let version: i64 = sqlx::query_scalar(
            r#"
            UPDATE insight_platform.quota_accounts
            SET reserved_value = reserved_value - $4,
                used_value = used_value + $5,
                version = version + 1, updated_at = clock_timestamp()
            WHERE tenant_id = $1 AND quota_account_id = $2 AND version = $3
              AND reserved_value >= $4
              AND reserved_value + used_value - $4 + $5 <= limit_value
            RETURNING version
            "#,
        )
        .bind(&line.tenant_id)
        .bind(&line.quota_account_id)
        .bind(line.account_version)
        .bind(line.reserved_amount)
        .bind(used_amount)
        .fetch_optional(&mut **transaction)
        .await?
        .ok_or(RepositoryError::Conflict("Sandbox quota account"))?;
        sqlx::query(
            r#"
            INSERT INTO insight_platform.quota_ledger (
                tenant_id, quota_entry_id, quota_account_id, correlation_id,
                entry_kind, reserved_amount, used_amount, account_version, request_digest
            ) VALUES ($1, $2, $3, $4, 'settle', $5, $6, $7, $8)
            "#,
        )
        .bind(&line.tenant_id)
        .bind(entry_id.to_string())
        .bind(&line.quota_account_id)
        .bind(reservation_id)
        .bind(line.reserved_amount)
        .bind(used_amount)
        .bind(version)
        .bind(request_digest.to_string())
        .execute(&mut **transaction)
        .await?;
    }
    Ok(())
}

async fn release_sandbox_artifact_grants(
    transaction: &mut Transaction<'_, Postgres>,
    current: &LockedSandboxJob,
    database_now: DateTime<Utc>,
) -> Result<(), RepositoryError> {
    let expected = u64::try_from(current.payload.request.artifact_grants.len()).map_err(|_| {
        RepositoryError::InvalidInput("Sandbox Artifact grant count exceeds bigint".to_owned())
    })?;
    release_and_confirm_sandbox_artifact_grants(
        transaction,
        &current.record.tenant_id,
        &current.payload.request.sandbox_job_id.to_string(),
        expected,
        database_now,
    )
    .await
}

pub(crate) async fn release_and_confirm_sandbox_artifact_grants(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &str,
    sandbox_job_id: &str,
    expected: u64,
    database_now: DateTime<Utc>,
) -> Result<(), RepositoryError> {
    let affected = sqlx::query(
        r#"
        UPDATE insight_platform.artifact_links
        SET state = 'released', version = version + 1,
            released_at = $3, updated_at = $3
        WHERE tenant_id = $1 AND owner_kind = 'sandbox_job' AND owner_id = $2
          AND link_kind = 'grant' AND state = 'active' AND released_at IS NULL
        "#,
    )
    .bind(tenant_id)
    .bind(sandbox_job_id)
    .bind(database_now)
    .execute(&mut **transaction)
    .await?
    .rows_affected();
    let released: i64 = sqlx::query_scalar(
        r#"
        SELECT count(*)
        FROM insight_platform.artifact_links
        WHERE tenant_id = $1 AND owner_kind = 'sandbox_job' AND owner_id = $2
          AND link_kind = 'grant' AND state = 'released' AND released_at IS NOT NULL
        "#,
    )
    .bind(tenant_id)
    .bind(sandbox_job_id)
    .fetch_one(&mut **transaction)
    .await?;
    if affected > expected || u64::try_from(released).ok() != Some(expected) {
        return Err(RepositoryError::Conflict("Sandbox Artifact grant release"));
    }
    Ok(())
}

fn sandbox_reservation_amount(
    metric: &str,
    resources: &SandboxResourceEnvelope,
) -> Result<i64, RepositoryError> {
    let amount = if metric == QuotaDimension::SandboxConcurrentExecutions.as_str() {
        1
    } else if metric == QuotaDimension::SandboxCpuSeconds.as_str() {
        ceil_div(
            u64::from(resources.cpu_millicores)
                .checked_mul(resources.wall_milliseconds)
                .ok_or_else(|| {
                    RepositoryError::InvalidInput("Sandbox CPU ceiling overflow".to_owned())
                })?,
            1_000_000,
        )
    } else if metric == QuotaDimension::SandboxMemoryMebibytes.as_str() {
        u64::from(resources.memory_mebibytes)
    } else if metric == QuotaDimension::SandboxOutputBytes.as_str() {
        resources
            .stdout_bytes
            .checked_add(resources.stderr_bytes)
            .and_then(|value| value.checked_add(resources.result_bytes))
            .and_then(|value| value.checked_add(resources.artifact_output_bytes))
            .ok_or_else(|| {
                RepositoryError::InvalidInput("Sandbox output ceiling overflow".to_owned())
            })?
    } else {
        return Err(RepositoryError::CorruptRow(
            "Sandbox quota metric is not registered".to_owned(),
        ));
    };
    i64::try_from(amount)
        .map_err(|_| RepositoryError::InvalidInput("Sandbox quota exceeds bigint".to_owned()))
}

fn sandbox_used_amount(
    line: &SandboxQuotaLine,
    usage: Option<&SandboxResourceUsage>,
    charge_unknown_usage: bool,
) -> Result<i64, RepositoryError> {
    let amount = if line.metric == QuotaDimension::SandboxConcurrentExecutions.as_str()
        || line.metric == QuotaDimension::SandboxMemoryMebibytes.as_str()
    {
        0
    } else if line.metric == QuotaDimension::SandboxCpuSeconds.as_str() {
        usage
            .map(|usage| ceil_div(usage.cpu_milliseconds, 1_000))
            .unwrap_or_else(|| {
                if charge_unknown_usage {
                    u64::try_from(line.reserved_amount).unwrap_or(u64::MAX)
                } else {
                    0
                }
            })
    } else if line.metric == QuotaDimension::SandboxOutputBytes.as_str() {
        usage
            .map(sandbox_output_bytes)
            .transpose()?
            .unwrap_or_else(|| {
                if charge_unknown_usage {
                    u64::try_from(line.reserved_amount).unwrap_or(u64::MAX)
                } else {
                    0
                }
            })
    } else {
        return Err(RepositoryError::CorruptRow(
            "Sandbox quota metric is not registered".to_owned(),
        ));
    };
    i64::try_from(amount)
        .map_err(|_| RepositoryError::InvalidInput("Sandbox quota usage exceeds bigint".to_owned()))
}

fn sandbox_output_bytes(usage: &SandboxResourceUsage) -> Result<u64, RepositoryError> {
    usage
        .stdout_bytes
        .checked_add(usage.stderr_bytes)
        .and_then(|value| value.checked_add(usage.result_bytes))
        .and_then(|value| value.checked_add(usage.artifact_output_bytes))
        .ok_or_else(|| RepositoryError::InvalidInput("Sandbox output usage overflow".to_owned()))
}

fn ceil_div(value: u64, divisor: u64) -> u64 {
    value.div_ceil(divisor)
}

async fn database_now(
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<DateTime<Utc>, RepositoryError> {
    Ok(sqlx::query_scalar("SELECT clock_timestamp()")
        .fetch_one(&mut **transaction)
        .await?)
}

fn as_i64(value: u64, label: &str) -> Result<i64, RepositoryError> {
    i64::try_from(value)
        .map_err(|_| RepositoryError::InvalidInput(format!("{label} exceeds bigint")))
}
