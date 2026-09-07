use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::FutureExt;
use insight_platform_contracts::*;
use insight_platform_mcp_host::*;
use std::{panic::AssertUnwindSafe, sync::Arc, time::Duration};

pub struct McpHostService {
    transport: Arc<dyn McpHostTransport>,
}

#[async_trait]
impl McpHostClient for McpHostService {
    async fn execute(
        &self,
        contract: &McpHostExecutionContract,
        request: &McpOperationRequest,
    ) -> Result<McpOperationOutcome, McpHostError> {
        Self::execute(self, contract, request).await
    }

    async fn cancel_remote_task(
        &self,
        contract: &McpHostExecutionContract,
        request: &McpOperationRequest,
        deadline: DateTime<Utc>,
    ) -> Result<McpRemoteTaskCancelOutcome, McpHostError> {
        Self::cancel_remote_task(self, contract, request, deadline).await
    }
}

impl McpHostService {
    pub fn new(transport: Arc<dyn McpHostTransport>) -> Self {
        Self { transport }
    }

    pub async fn execute(
        &self,
        contract: &McpHostExecutionContract,
        request: &McpOperationRequest,
    ) -> Result<McpOperationOutcome, McpHostError> {
        let now = Utc::now();
        contract.validate_canonical_at(now)?;
        request.validate_for(contract, now)?;
        if self.transport.kind() != contract.transport_kind() {
            return Err(McpHostError::WrongTransport);
        }
        let remaining = u64::try_from((request.deadline - now).num_milliseconds())
            .map_err(|_| McpHostError::InvalidOperation)?;
        let timeout = remaining.min(contract.server.limits.total_timeout_milliseconds);
        let future = AssertUnwindSafe(self.transport.execute(contract, request)).catch_unwind();
        let mut outcome = match tokio::time::timeout(Duration::from_millis(timeout), future).await {
            Ok(Ok(Ok(outcome))) => outcome,
            Ok(Ok(Err(failure))) => map_transport_failure(request, contract, failure)?,
            Ok(Err(_)) => unknown_transport_outcome(request, contract, "mcp_transport_panic"),
            Err(_) => unknown_transport_outcome(request, contract, "mcp_transport_timeout"),
        };
        let validation_now = Utc::now();
        normalize_remote_task_poll(&mut outcome, request, contract, validation_now)?;
        outcome.validate_for(request, contract, validation_now)?;
        Ok(outcome)
    }

    pub async fn cancel_remote_task(
        &self,
        contract: &McpHostExecutionContract,
        request: &McpOperationRequest,
        deadline: DateTime<Utc>,
    ) -> Result<McpRemoteTaskCancelOutcome, McpHostError> {
        let now = Utc::now();
        contract.validate_canonical_at(now)?;
        let mut validation_request = request.clone();
        validation_request.deadline = deadline;
        validation_request.validate_for(contract, now)?;
        if request.continuation.is_none()
            || request.task_requested
            || !contract.discovery.negotiated_capabilities.tasks_cancel
            || self.transport.kind() != contract.transport_kind()
        {
            return Err(McpHostError::InvalidOperation);
        }
        let remaining = u64::try_from((deadline - now).num_milliseconds())
            .map_err(|_| McpHostError::InvalidOperation)?;
        let timeout = remaining.min(contract.server.limits.total_timeout_milliseconds);
        let future = AssertUnwindSafe(
            self.transport
                .cancel_remote_task(contract, request, deadline),
        )
        .catch_unwind();
        match tokio::time::timeout(Duration::from_millis(timeout), future).await {
            Ok(Ok(Ok(outcome))) => Ok(outcome),
            Ok(Ok(Err(failure))) => {
                failure.validate_wire_shape()?;
                Err(McpHostError::InvalidOutcome)
            }
            Ok(Err(_)) | Err(_) => Err(McpHostError::InvalidOutcome),
        }
    }
}

fn normalize_remote_task_poll(
    outcome: &mut McpOperationOutcome,
    request: &McpOperationRequest,
    contract: &McpHostExecutionContract,
    now: DateTime<Utc>,
) -> Result<(), McpHostError> {
    let McpOperationOutcome::RemoteTask { next_poll_at, .. } = outcome else {
        return Ok(());
    };
    let limits = contract
        .protocol_profile
        .method_limits
        .get(&PublishedMcpMethod::TasksGet)
        .ok_or(McpHostError::InvalidOutcome)?;
    let current_wait = (*next_poll_at - now).num_milliseconds();
    let maximum_wait = i64::try_from(limits.maximum_poll_milliseconds)
        .map_err(|_| McpHostError::InvalidOutcome)?;
    if current_wait <= 0 || current_wait > maximum_wait {
        return Err(McpHostError::InvalidOutcome);
    }
    let minimum_poll = chrono::Duration::milliseconds(
        i64::try_from(limits.minimum_poll_milliseconds)
            .map_err(|_| McpHostError::InvalidOutcome)?,
    );
    let minimum_next_poll = now
        .checked_add_signed(minimum_poll)
        .ok_or(McpHostError::InvalidOutcome)?;
    if *next_poll_at < minimum_next_poll {
        *next_poll_at = minimum_next_poll;
    }
    if *next_poll_at >= request.deadline {
        return Err(McpHostError::InvalidOutcome);
    }
    Ok(())
}

fn map_transport_failure(
    request: &McpOperationRequest,
    contract: &McpHostExecutionContract,
    failure: McpTransportFailure,
) -> Result<McpOperationOutcome, McpHostError> {
    failure.validate_wire_shape()?;
    Ok(match failure {
        McpTransportFailure::RetryableBeforeDispatch(_)
        | McpTransportFailure::PostDispatchUncertain { .. }
            if request.continuation.is_some() =>
        {
            defer_existing_task(request, contract)?
        }
        McpTransportFailure::RejectedBeforeDispatch(failure)
        | McpTransportFailure::Permanent(failure) => McpOperationOutcome::PermanentFailure(failure),
        McpTransportFailure::RetryableBeforeDispatch(failure) => {
            McpOperationOutcome::RetryableFailure(failure)
        }
        McpTransportFailure::ReauthorizationRequired { challenge_digest } => {
            McpOperationOutcome::ReauthorizationRequired { challenge_digest }
        }
        McpTransportFailure::PostDispatchUncertain {
            failure,
            external_identity_digest: _,
        } if request.safe_to_retry_after_unknown() => {
            McpOperationOutcome::RetryableFailure(failure)
        }
        McpTransportFailure::PostDispatchUncertain {
            failure,
            external_identity_digest,
        } => McpOperationOutcome::Uncertain {
            observation_digest: failure.evidence_digest,
            external_identity_digest,
        },
    })
}

fn unknown_transport_outcome(
    request: &McpOperationRequest,
    contract: &McpHostExecutionContract,
    domain: &str,
) -> McpOperationOutcome {
    if request.continuation.is_some() {
        return defer_existing_task(request, contract).unwrap_or_else(|_| {
            McpOperationOutcome::Uncertain {
                observation_digest: static_digest(domain),
                external_identity_digest: request
                    .continuation
                    .as_ref()
                    .expect("continuation checked")
                    .external_identity_digest
                    .clone(),
            }
        });
    }
    let failure = SafeMcpFailure {
        safe_code: domain.to_owned(),
        safe_message: "MCP transport completion could not be observed".to_owned(),
        evidence_digest: static_digest(domain),
    };
    if request.safe_to_retry_after_unknown() {
        McpOperationOutcome::RetryableFailure(failure)
    } else {
        McpOperationOutcome::Uncertain {
            observation_digest: failure.evidence_digest,
            external_identity_digest: request.idempotency_key_digest.clone(),
        }
    }
}

fn defer_existing_task(
    request: &McpOperationRequest,
    contract: &McpHostExecutionContract,
) -> Result<McpOperationOutcome, McpHostError> {
    let continuation = request
        .continuation
        .as_ref()
        .ok_or(McpHostError::InvalidOperation)?;
    let limits = contract
        .protocol_profile
        .method_limits
        .get(&PublishedMcpMethod::TasksGet)
        .ok_or(McpHostError::InvalidOperation)?;
    let delay = limits.minimum_poll_milliseconds.max(1);
    let next_poll_at = Utc::now()
        .checked_add_signed(chrono::Duration::milliseconds(
            i64::try_from(delay).map_err(|_| McpHostError::InvalidOperation)?,
        ))
        .ok_or(McpHostError::InvalidOperation)?;
    if next_poll_at >= request.deadline {
        return Err(McpHostError::InvalidOutcome);
    }
    Ok(McpOperationOutcome::RemoteTask {
        encrypted_state: continuation.encrypted_state.clone(),
        external_identity_digest: continuation.external_identity_digest.clone(),
        next_poll_at,
    })
}
