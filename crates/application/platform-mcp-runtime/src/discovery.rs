use async_trait::async_trait;
use chrono::Utc;
use futures::FutureExt;
use insight_platform_artifacts::StageWorkloadArtifactRequest;
use insight_platform_contracts::ResourceId;
use insight_platform_jobs::JobFence;
use std::{panic::AssertUnwindSafe, sync::Arc, time::Duration};

use insight_platform_mcp_host::*;

pub struct McpDiscoveryWorker {
    resolver: Arc<dyn McpDiscoveryExecutionContractResolver>,
    client: Arc<dyn McpDiscoveryClient>,
    artifact_stager: Arc<dyn McpDiscoveryArtifactStager>,
    store: Arc<dyn McpDiscoveryResultStore>,
}

#[derive(Debug, Clone)]
enum PreparedMcpDiscoveryCommand {
    Stage(Box<PreparedMcpDiscoveryStage>),
    Finalize(Box<FinalizeMcpDiscovery>),
    Resolve(Box<ResolveMcpDiscoveryAttempt>),
}

#[derive(Debug, Clone)]
pub struct PreparedMcpDiscovery {
    command: PreparedMcpDiscoveryCommand,
}

#[derive(Debug, Clone)]
struct PreparedMcpDiscoveryStage {
    audit: McpWorkerAudit,
    operation_id: ResourceId,
    job_id: ResourceId,
    fence: JobFence,
    expected_operation_version: u64,
    preallocation: McpDiscoveryArtifactPreallocation,
    artifact_policy: McpDiscoveryArtifactPolicyClosure,
    candidate: McpDiscoveryCandidate,
}

impl PreparedMcpDiscovery {
    pub fn refresh_fence(&mut self, fence: JobFence) -> Result<(), McpDiscoveryWorkerError> {
        let current = match &self.command {
            PreparedMcpDiscoveryCommand::Stage(command) => &command.fence,
            PreparedMcpDiscoveryCommand::Finalize(command) => &command.fence,
            PreparedMcpDiscoveryCommand::Resolve(command) => &command.fence,
        };
        if fence.expected_version <= current.expected_version
            || fence.worker_process_generation_id != current.worker_process_generation_id
            || fence.lease_generation != current.lease_generation
            || fence.token_digest != current.token_digest
        {
            return Err(McpDiscoveryWorkerError::InvalidCommand);
        }
        match &mut self.command {
            PreparedMcpDiscoveryCommand::Stage(command) => command.fence = fence,
            PreparedMcpDiscoveryCommand::Finalize(command) => command.fence = fence,
            PreparedMcpDiscoveryCommand::Resolve(command) => command.fence = fence,
        }
        Ok(())
    }
}

impl McpDiscoveryWorker {
    pub fn new(
        resolver: Arc<dyn McpDiscoveryExecutionContractResolver>,
        client: Arc<dyn McpDiscoveryClient>,
        artifact_stager: Arc<dyn McpDiscoveryArtifactStager>,
        store: Arc<dyn McpDiscoveryResultStore>,
    ) -> Self {
        Self {
            resolver,
            client,
            artifact_stager,
            store,
        }
    }

    pub async fn execute(
        &self,
        command: ExecuteMcpDiscoveryJob,
    ) -> Result<McpDiscoveryWorkerResult, McpDiscoveryWorkerError> {
        let prepared = self.prepare(command).await?;
        self.commit(prepared).await
    }

    pub async fn prepare(
        &self,
        command: ExecuteMcpDiscoveryJob,
    ) -> Result<PreparedMcpDiscovery, McpDiscoveryWorkerError> {
        let started_at = Utc::now();
        command
            .validate_at(started_at)
            .map_err(|_| McpDiscoveryWorkerError::InvalidCommand)?;
        let resolved = self
            .resolver
            .resolve_mcp_discovery_execution(&command.query)
            .await
            .map_err(McpDiscoveryWorkerError::Contract)?;
        resolved
            .validate_for(&command.query, Utc::now())
            .map_err(McpDiscoveryWorkerError::Contract)?;
        if resolved.pending_verification.is_some() {
            return Ok(PreparedMcpDiscovery {
                command: PreparedMcpDiscoveryCommand::Finalize(Box::new(FinalizeMcpDiscovery {
                    audit: command.audit,
                    operation_id: command.query.operation_id,
                    job_id: command.query.job_id,
                    fence: command.query.fence,
                    expected_operation_version: resolved.operation_version,
                })),
            });
        }
        let outcome = self
            .client
            .discover(&resolved.contract, &resolved.request)
            .await;
        if let Ok(McpDiscoveryOutcome::Candidate(candidate)) = outcome {
            if u64::try_from(candidate.descriptor_bytes.len())
                .ok()
                .is_none_or(|length| length > resolved.artifact_policy.maximum_bytes)
            {
                return Err(McpDiscoveryWorkerError::InvalidCommand);
            }
            return Ok(PreparedMcpDiscovery {
                command: PreparedMcpDiscoveryCommand::Stage(Box::new(PreparedMcpDiscoveryStage {
                    audit: command.audit,
                    operation_id: command.query.operation_id,
                    job_id: command.query.job_id,
                    fence: command.query.fence,
                    expected_operation_version: resolved.operation_version,
                    preallocation: resolved.artifact_preallocation,
                    artifact_policy: resolved.artifact_policy,
                    candidate,
                })),
            });
        }

        let resolution = match outcome {
            Ok(McpDiscoveryOutcome::RetryableFailure(failure))
                if resolved.request.physical_attempt < resolved.attempt_limit
                    && command.retry_at < resolved.request.deadline
                    && command.retry_at > Utc::now() =>
            {
                McpDiscoveryAttemptResolution::Retry {
                    retry_at: command.retry_at,
                    failure,
                }
            }
            Ok(
                McpDiscoveryOutcome::RetryableFailure(failure)
                | McpDiscoveryOutcome::PermanentFailure(failure),
            ) => McpDiscoveryAttemptResolution::Failed { failure },
            Ok(McpDiscoveryOutcome::ReauthorizationRequired { challenge_digest }) => {
                McpDiscoveryAttemptResolution::ReauthorizationRequired { challenge_digest }
            }
            Ok(McpDiscoveryOutcome::Candidate(_)) => unreachable!("candidate handled above"),
            Err(_) => McpDiscoveryAttemptResolution::Failed {
                failure: SafeMcpFailure {
                    safe_code: "mcp_discovery_host_rejected".to_owned(),
                    safe_message: "MCP discovery response failed host validation".to_owned(),
                    evidence_digest: static_digest("mcp_discovery_host_rejected"),
                },
            },
        };
        Ok(PreparedMcpDiscovery {
            command: PreparedMcpDiscoveryCommand::Resolve(Box::new(ResolveMcpDiscoveryAttempt {
                audit: command.audit,
                operation_id: command.query.operation_id,
                job_id: command.query.job_id,
                fence: command.query.fence,
                expected_operation_version: resolved.operation_version,
                resolution,
            })),
        })
    }

    pub async fn commit(
        &self,
        prepared: PreparedMcpDiscovery,
    ) -> Result<McpDiscoveryWorkerResult, McpDiscoveryWorkerError> {
        match prepared.command {
            PreparedMcpDiscoveryCommand::Stage(command) => {
                let command = *command;
                let stage_request = StageWorkloadArtifactRequest {
                    schema_version: 1,
                    tenant_id: command.audit.tenant_id.clone(),
                    producer_job_id: command.job_id.clone(),
                    producer_fence: command.fence.clone(),
                    verification_job_id: command.preallocation.verification_job_id.clone(),
                    artifact_id: command.preallocation.artifact_id.clone(),
                    blob_id: command.preallocation.blob_id.clone(),
                    descriptor_bytes: command.candidate.descriptor_bytes.clone(),
                    descriptor_digest: command.candidate.descriptor_digest.clone(),
                    media_type: command.artifact_policy.declared_media_type.clone(),
                };
                stage_request
                    .validate()
                    .map_err(|_| McpDiscoveryWorkerError::InvalidCommand)?;
                let staged = self
                    .artifact_stager
                    .stage_mcp_discovery_artifact(stage_request)
                    .await
                    .map_err(McpDiscoveryWorkerError::ArtifactStage)?;
                let evidence = McpDiscoveryTransportEvidence::build(
                    &command.candidate,
                    &staged,
                    &command.preallocation,
                )
                .map_err(|_| McpDiscoveryWorkerError::InvalidCommand)?;
                self.store
                    .park_mcp_discovery_for_verification(ParkMcpDiscoveryVerification {
                        audit: command.audit,
                        operation_id: command.operation_id,
                        job_id: command.job_id,
                        fence: command.fence,
                        expected_operation_version: command.expected_operation_version,
                        staged,
                        evidence,
                    })
                    .await
                    .map(McpDiscoveryWorkerResult::VerificationPending)
                    .map_err(McpDiscoveryWorkerError::Persistence)
            }
            PreparedMcpDiscoveryCommand::Finalize(command) => self
                .store
                .finalize_mcp_discovery_after_verification(*command)
                .await
                .map(McpDiscoveryWorkerResult::SnapshotCommitted)
                .map_err(McpDiscoveryWorkerError::Persistence),
            PreparedMcpDiscoveryCommand::Resolve(command) => self
                .store
                .resolve_mcp_discovery_attempt_result(*command)
                .await
                .map(McpDiscoveryWorkerResult::AttemptResolved)
                .map_err(McpDiscoveryWorkerError::Persistence),
        }
    }
}

pub struct McpDiscoveryService {
    transport: Arc<dyn McpDiscoveryTransport>,
}

impl McpDiscoveryService {
    pub fn new(transport: Arc<dyn McpDiscoveryTransport>) -> Self {
        Self { transport }
    }
}

#[async_trait]
impl McpDiscoveryClient for McpDiscoveryService {
    async fn discover(
        &self,
        contract: &McpDiscoveryExecutionContract,
        request: &McpDiscoveryRequest,
    ) -> Result<McpDiscoveryOutcome, McpHostError> {
        let now = Utc::now();
        contract.validate_canonical_at(now)?;
        request.validate_for(contract, now)?;
        if self.transport.kind() != contract.transport_kind() {
            return Err(McpHostError::WrongTransport);
        }
        let remaining = u64::try_from((request.deadline - now).num_milliseconds())
            .map_err(|_| McpHostError::InvalidDiscovery)?;
        let timeout = remaining.min(contract.server.limits.total_timeout_milliseconds);
        let future = AssertUnwindSafe(self.transport.discover(contract, request)).catch_unwind();
        let outcome = match tokio::time::timeout(Duration::from_millis(timeout), future).await {
            Ok(Ok(Ok(candidate))) => McpDiscoveryOutcome::Candidate(candidate),
            Ok(Ok(Err(failure))) => map_discovery_transport_failure(failure)?,
            Ok(Err(_)) => retryable_discovery_failure("mcp_discovery_transport_panic"),
            Err(_) => retryable_discovery_failure("mcp_discovery_transport_timeout"),
        };
        outcome.validate_for(contract)?;
        Ok(outcome)
    }
}

fn map_discovery_transport_failure(
    failure: McpTransportFailure,
) -> Result<McpDiscoveryOutcome, McpHostError> {
    failure.validate_wire_shape()?;
    Ok(match failure {
        McpTransportFailure::RejectedBeforeDispatch(failure)
        | McpTransportFailure::Permanent(failure) => McpDiscoveryOutcome::PermanentFailure(failure),
        McpTransportFailure::RetryableBeforeDispatch(failure)
        | McpTransportFailure::PostDispatchUncertain { failure, .. } => {
            McpDiscoveryOutcome::RetryableFailure(failure)
        }
        McpTransportFailure::ReauthorizationRequired { challenge_digest } => {
            McpDiscoveryOutcome::ReauthorizationRequired { challenge_digest }
        }
    })
}

fn retryable_discovery_failure(domain: &str) -> McpDiscoveryOutcome {
    McpDiscoveryOutcome::RetryableFailure(SafeMcpFailure {
        safe_code: domain.to_owned(),
        safe_message: "MCP discovery completion could not be observed".to_owned(),
        evidence_digest: static_digest(domain),
    })
}
