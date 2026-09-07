use async_trait::async_trait;
use chrono::Utc;
use insight_platform_context::{
    ContextSubscriptionExecutionError, ContextSubscriptionRefreshAttempt,
    ContextSubscriptionRefreshBackend, ContextSubscriptionRefreshResponse,
};
use std::sync::Arc;

use insight_platform_mcp_host::*;

pub struct McpResourceRefreshHost<R, P> {
    resolver: Arc<R>,
    protocol: Arc<P>,
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
        resolved.validate_for(Utc::now(), &attempt)?;
        let response = self.protocol.refresh_resources(&attempt, &resolved).await?;
        response.validate_for(&attempt, Utc::now())?;
        Ok(response)
    }
}

impl<R, P> McpResourceRefreshHost<R, P> {
    pub fn new(resolver: Arc<R>, protocol: Arc<P>) -> Self {
        Self { resolver, protocol }
    }
}
