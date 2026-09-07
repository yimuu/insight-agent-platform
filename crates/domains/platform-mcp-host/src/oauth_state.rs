use super::{AuthenticatedMcpOAuthState, McpOAuthCallbackError, SensitiveOAuthValue};
use chrono::{DateTime, Utc};
pub const MAX_MCP_OAUTH_STATE_LIFETIME_SECONDS: i64 = 3_600;

pub trait McpOAuthStateIssuer: Send + Sync {
    fn issue_state(
        &self,
        identity: &AuthenticatedMcpOAuthState,
        issued_at: DateTime<Utc>,
        expires_at: DateTime<Utc>,
    ) -> Result<SensitiveOAuthValue, McpOAuthCallbackError>;
}
