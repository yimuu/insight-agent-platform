//! Durable ownership authority for MCP server-exported Agent tasks.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::{McpInteractionPrincipal, RepositoryError, RepositoryErrorExt as _};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpServerTask {
    task_id: String,
    principal: McpInteractionPrincipal,
    run_id: String,
    agent_id: String,
    created_at: DateTime<Utc>,
    expires_at: DateTime<Utc>,
}

impl McpServerTask {
    pub fn new(
        task_id: impl Into<String>,
        principal: McpInteractionPrincipal,
        run_id: impl Into<String>,
        agent_id: impl Into<String>,
        created_at: DateTime<Utc>,
        expires_at: DateTime<Utc>,
    ) -> Result<Self, RepositoryError> {
        let task_id = task_id.into();
        let run_id = run_id.into();
        let agent_id = agent_id.into();
        if [&task_id, &run_id, &agent_id].iter().any(|value| {
            value.is_empty() || value.len() > 256 || value.chars().any(char::is_control)
        }) || expires_at <= created_at
        {
            return Err(RepositoryError::invalid_data());
        }
        Ok(Self {
            task_id,
            principal,
            run_id,
            agent_id,
            created_at,
            expires_at,
        })
    }

    pub fn task_id(&self) -> &str {
        &self.task_id
    }
    pub fn principal(&self) -> &McpInteractionPrincipal {
        &self.principal
    }
    pub fn run_id(&self) -> &str {
        &self.run_id
    }
    pub fn agent_id(&self) -> &str {
        &self.agent_id
    }
    pub fn created_at(&self) -> DateTime<Utc> {
        self.created_at
    }
    pub fn expires_at(&self) -> DateTime<Utc> {
        self.expires_at
    }
}

#[async_trait]
pub trait McpServerTaskDurableRepository: Send + Sync {
    async fn create_mcp_server_task(&self, task: McpServerTask) -> Result<bool, RepositoryError>;

    async fn load_mcp_server_task(
        &self,
        principal: &McpInteractionPrincipal,
        task_id: &str,
    ) -> Result<Option<McpServerTask>, RepositoryError>;

    /// Returns a bounded, stable page of expired task authorities.
    ///
    /// The caller must first cancel the referenced Run and only then call
    /// [`Self::delete_expired_mcp_server_task`]. Keeping those two steps
    /// separate makes a process crash retryable instead of orphaning a Run
    /// after prematurely deleting its lookup authority.
    async fn list_expired_mcp_server_tasks(
        &self,
        now: DateTime<Utc>,
        limit: u32,
    ) -> Result<Vec<McpServerTask>, RepositoryError>;

    /// Deletes the exact expired authority after its Run cancellation has
    /// reached a durable terminal winner.
    async fn delete_expired_mcp_server_task(
        &self,
        task_id: &str,
        expected_expires_at: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> Result<bool, RepositoryError>;
}
