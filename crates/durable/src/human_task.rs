//! Backend-neutral durable Human Task contracts.

use super::RepositoryErrorExt as _;

use async_trait::async_trait;
use serde::Serialize;
use serde_json::Value;

pub use insight_engine::human::{
    HumanTaskPrincipal, HumanWorkItem, HumanWorkItemId, HumanWorkItemState,
};
use insight_engine::{SignalId, TransitionOutcome};

use super::RepositoryError;

const MAX_COMPLETION_VALUE_BYTES: usize = 1024 * 1024;

fn validate_label(value: &str) -> Result<(), RepositoryError> {
    if value.is_empty()
        || value.len() > 256
        || value
            .chars()
            .any(|character| character.is_control() || character.is_whitespace())
    {
        return Err(RepositoryError::invalid_data());
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ClaimHumanWorkItemCommand {
    work_item_id: HumanWorkItemId,
    principal: HumanTaskPrincipal,
    request_id: String,
}

impl ClaimHumanWorkItemCommand {
    pub fn new(
        work_item_id: HumanWorkItemId,
        principal: HumanTaskPrincipal,
        request_id: impl Into<String>,
    ) -> Result<Self, RepositoryError> {
        let request_id = request_id.into();
        validate_label(&request_id)?;
        Ok(Self {
            work_item_id,
            principal,
            request_id,
        })
    }
    pub fn work_item_id(&self) -> &HumanWorkItemId {
        &self.work_item_id
    }
    pub fn principal(&self) -> &HumanTaskPrincipal {
        &self.principal
    }
    pub fn request_id(&self) -> &str {
        &self.request_id
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct HumanWorkItemClaim {
    work_item: HumanWorkItem,
}

impl HumanWorkItemClaim {
    pub fn work_item(&self) -> &HumanWorkItem {
        &self.work_item
    }
    pub fn claim_fence(&self) -> u64 {
        self.work_item.claim_fence()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct CompleteHumanWorkItemCommand {
    work_item_id: HumanWorkItemId,
    principal: HumanTaskPrincipal,
    request_id: String,
    claim_fence: u64,
    value: Value,
}

impl CompleteHumanWorkItemCommand {
    pub fn new(
        work_item_id: HumanWorkItemId,
        principal: HumanTaskPrincipal,
        request_id: impl Into<String>,
        claim_fence: u64,
        value: Value,
    ) -> Result<Self, RepositoryError> {
        let request_id = request_id.into();
        validate_label(&request_id)?;
        if claim_fence == 0 {
            return Err(RepositoryError::invalid_data());
        }
        if serde_jcs::to_vec(&value)
            .map_err(|_| RepositoryError::canonicalization())?
            .len()
            > MAX_COMPLETION_VALUE_BYTES
        {
            return Err(RepositoryError::invalid_data());
        }
        Ok(Self {
            work_item_id,
            principal,
            request_id,
            claim_fence,
            value,
        })
    }
    pub fn work_item_id(&self) -> &HumanWorkItemId {
        &self.work_item_id
    }
    pub fn principal(&self) -> &HumanTaskPrincipal {
        &self.principal
    }
    pub fn request_id(&self) -> &str {
        &self.request_id
    }
    pub fn claim_fence(&self) -> u64 {
        self.claim_fence
    }
    pub fn value(&self) -> &Value {
        &self.value
    }
}

/// Durable authority returned before publishing the scheduler completion
/// signal. Replaying this exact authority closes the process-crash gap.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct HumanWorkItemCompletionAuthority {
    work_item: HumanWorkItem,
    signal_id: SignalId,
    value: Value,
    message_id: String,
}

impl HumanWorkItemCompletionAuthority {
    pub fn work_item(&self) -> &HumanWorkItem {
        &self.work_item
    }
    pub fn signal_id(&self) -> &SignalId {
        &self.signal_id
    }
    pub fn value(&self) -> &Value {
        &self.value
    }
    pub fn message_id(&self) -> &str {
        &self.message_id
    }
}

#[async_trait]
pub trait HumanTaskDurableRepository: Send + Sync {
    async fn list_human_work_items(
        &self,
        principal: &HumanTaskPrincipal,
        limit: u32,
    ) -> Result<Vec<HumanWorkItem>, RepositoryError>;

    async fn load_human_work_item(
        &self,
        work_item_id: &HumanWorkItemId,
    ) -> Result<Option<HumanWorkItem>, RepositoryError>;

    async fn claim_human_work_item(
        &self,
        command: ClaimHumanWorkItemCommand,
    ) -> Result<TransitionOutcome<HumanWorkItemClaim>, RepositoryError>;

    /// Reserves an idempotent typed completion. The caller must publish and
    /// resolve the returned signal, then call `finalize_human_work_item`.
    async fn complete_human_work_item(
        &self,
        command: CompleteHumanWorkItemCommand,
    ) -> Result<TransitionOutcome<HumanWorkItemCompletionAuthority>, RepositoryError>;

    async fn finalize_human_work_item(
        &self,
        work_item_id: &HumanWorkItemId,
        request_id: &str,
    ) -> Result<bool, RepositoryError>;

    /// Durable completion reservations whose signal publication may have
    /// been interrupted by process failure.
    async fn list_pending_human_work_item_completions(
        &self,
        limit: u32,
    ) -> Result<Vec<HumanWorkItemCompletionAuthority>, RepositoryError>;

    async fn reconcile_human_work_items(&self, limit: u32) -> Result<u64, RepositoryError>;
}

/// Workspace-internal construction surface for storage adapters.
#[doc(hidden)]
pub mod adapter {
    use serde_json::Value;

    use insight_engine::{human::HumanWorkItem, SignalId};

    use super::{HumanWorkItemClaim, HumanWorkItemCompletionAuthority};

    pub fn human_work_item_claim(work_item: HumanWorkItem) -> HumanWorkItemClaim {
        HumanWorkItemClaim { work_item }
    }

    pub fn human_work_item_completion_authority(
        work_item: HumanWorkItem,
        signal_id: SignalId,
        value: Value,
        message_id: String,
    ) -> HumanWorkItemCompletionAuthority {
        HumanWorkItemCompletionAuthority {
            work_item,
            signal_id,
            value,
            message_id,
        }
    }
}
