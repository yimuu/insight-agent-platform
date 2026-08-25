use async_trait::async_trait;
use chrono::Utc;
use insight_platform_context::{
    ContextSubscriptionExecutionError, ContextSubscriptionRefreshAttempt,
    ContextSubscriptionRefreshBackend, ContextSubscriptionRefreshResponse,
};
use std::sync::Arc;

use crate::{McpHostExecutionContract, McpSubscriptionRecord, McpSubscriptionState};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedContextSubscriptionRefresh {
    pub subscription: McpSubscriptionRecord,
    pub contract: McpHostExecutionContract,
}

impl ResolvedContextSubscriptionRefresh {
    pub fn validate_for(
        &self,
        attempt: &ContextSubscriptionRefreshAttempt,
    ) -> Result<(), ContextSubscriptionExecutionError> {
        let now = Utc::now();
        attempt.validate_at(now)?;
        self.subscription
            .validate_at(now)
            .map_err(|_| ContextSubscriptionExecutionError::Rejected)?;
        self.contract
            .validate_canonical_at(now)
            .map_err(|_| ContextSubscriptionExecutionError::Rejected)?;
        let binding = &self.subscription.payload.binding;
        let request = &attempt.request;
        if self.subscription.tenant_id != request.tenant_id
            || self.subscription.subscription_id != request.subscription_id
            || self.subscription.state != McpSubscriptionState::Active
            || binding.context_deployment != request.context_deployment
            || binding.mcp_deployment != request.mcp_deployment
            || binding.discovery_snapshot_id != request.discovery_snapshot_id
            || binding.discovery_snapshot_digest != request.discovery_snapshot_digest
            || binding.authorization_generation != request.authorization_generation
            || self.subscription.payload.session.generation != request.session_generation
            || binding.resource_uri != request.resource_uri
            || binding.resource_uri_digest != request.resource_uri_digest
            || binding
                .validate_for_execution_contract_at(&self.contract, now)
                .is_err()
        {
            return Err(ContextSubscriptionExecutionError::Rejected);
        }
        Ok(())
    }
}

#[async_trait]
pub trait ContextSubscriptionRefreshResolver: Send + Sync {
    async fn resolve_context_subscription_refresh(
        &self,
        attempt: &ContextSubscriptionRefreshAttempt,
    ) -> Result<ResolvedContextSubscriptionRefresh, ContextSubscriptionExecutionError>;
}

#[async_trait]
pub trait McpResourceRefreshProtocol: Send + Sync {
    async fn refresh_resources(
        &self,
        attempt: &ContextSubscriptionRefreshAttempt,
        resolved: &ResolvedContextSubscriptionRefresh,
    ) -> Result<ContextSubscriptionRefreshResponse, ContextSubscriptionExecutionError>;
}

pub struct McpResourceRefreshHost<R, P> {
    resolver: Arc<R>,
    protocol: Arc<P>,
}

impl<R, P> McpResourceRefreshHost<R, P> {
    pub fn new(resolver: Arc<R>, protocol: Arc<P>) -> Self {
        Self { resolver, protocol }
    }
}

#[async_trait]
impl<R, P> ContextSubscriptionRefreshBackend for McpResourceRefreshHost<R, P>
where
    R: ContextSubscriptionRefreshResolver + 'static,
    P: McpResourceRefreshProtocol + 'static,
{
    async fn refresh_subscription_resources(
        &self,
        attempt: ContextSubscriptionRefreshAttempt,
    ) -> Result<ContextSubscriptionRefreshResponse, ContextSubscriptionExecutionError> {
        attempt.validate_at(Utc::now())?;
        let resolved = self
            .resolver
            .resolve_context_subscription_refresh(&attempt)
            .await?;
        resolved.validate_for(&attempt)?;
        let response = self.protocol.refresh_resources(&attempt, &resolved).await?;
        response.validate_for(&attempt, Utc::now())?;
        Ok(response)
    }
}
