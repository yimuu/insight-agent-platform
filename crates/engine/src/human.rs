use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{repository::RepositoryError, ActivationId, PlanType, RunId};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct HumanWorkItemId(String);

impl HumanWorkItemId {
    pub fn new(value: impl Into<String>) -> Result<Self, RepositoryError> {
        let value = value.into();
        validate_label(&value)?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HumanWorkItemState {
    Open,
    Claimed,
    Completed,
    Cancelled,
    Expired,
}

impl HumanWorkItemState {
    fn parse(value: &str) -> Result<Self, RepositoryError> {
        match value {
            "open" => Ok(Self::Open),
            "claimed" => Ok(Self::Claimed),
            "completed" => Ok(Self::Completed),
            "cancelled" => Ok(Self::Cancelled),
            "expired" => Ok(Self::Expired),
            _ => Err(RepositoryError::invalid_data()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct HumanTaskPrincipal {
    identity: String,
    groups: Vec<String>,
}

impl HumanTaskPrincipal {
    pub fn new(
        identity: impl Into<String>,
        groups: impl IntoIterator<Item = String>,
    ) -> Result<Self, RepositoryError> {
        let identity = identity.into();
        validate_label(&identity)?;
        let mut groups = groups.into_iter().collect::<Vec<_>>();
        for group in &groups {
            validate_label(group)?;
        }
        groups.sort();
        groups.dedup();
        Ok(Self { identity, groups })
    }

    pub fn identity(&self) -> &str {
        &self.identity
    }

    pub fn groups(&self) -> &[String] {
        &self.groups
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct HumanWorkItem {
    work_item_id: HumanWorkItemId,
    run_id: RunId,
    activation_id: ActivationId,
    signal_name: String,
    request: Value,
    response_type: PlanType,
    assignees: Vec<String>,
    candidate_groups: Vec<String>,
    state: HumanWorkItemState,
    claim_fence: u64,
    claimed_by: Option<String>,
    claim_expires_at: Option<String>,
    projection_version: u64,
}

impl HumanWorkItem {
    pub fn work_item_id(&self) -> &HumanWorkItemId {
        &self.work_item_id
    }

    pub fn run_id(&self) -> &RunId {
        &self.run_id
    }

    pub fn activation_id(&self) -> &ActivationId {
        &self.activation_id
    }

    pub fn signal_name(&self) -> &str {
        &self.signal_name
    }

    pub fn request(&self) -> &Value {
        &self.request
    }

    pub fn response_type(&self) -> &PlanType {
        &self.response_type
    }

    pub fn assignees(&self) -> &[String] {
        &self.assignees
    }

    pub fn candidate_groups(&self) -> &[String] {
        &self.candidate_groups
    }

    pub fn state(&self) -> HumanWorkItemState {
        self.state
    }

    pub fn claim_fence(&self) -> u64 {
        self.claim_fence
    }

    pub fn claimed_by(&self) -> Option<&str> {
        self.claimed_by.as_deref()
    }

    pub fn claim_expires_at(&self) -> Option<&str> {
        self.claim_expires_at.as_deref()
    }

    pub fn projection_version(&self) -> u64 {
        self.projection_version
    }

    fn assigned_to(&self, principal: &HumanTaskPrincipal) -> bool {
        (self.assignees.is_empty() && self.candidate_groups.is_empty())
            || self
                .assignees
                .iter()
                .any(|value| value == principal.identity())
            || self
                .candidate_groups
                .iter()
                .any(|candidate| principal.groups().iter().any(|group| group == candidate))
    }
}

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

/// Workspace-internal construction surface for durable storage adapters.
#[doc(hidden)]
pub mod adapter {
    use serde_json::Value;

    use crate::{ActivationId, PlanType, RunId};

    use super::{
        HumanTaskPrincipal, HumanWorkItem, HumanWorkItemId, HumanWorkItemState, RepositoryError,
    };

    pub struct HumanWorkItemParts {
        pub work_item_id: HumanWorkItemId,
        pub run_id: RunId,
        pub activation_id: ActivationId,
        pub signal_name: String,
        pub request: Value,
        pub response_type: PlanType,
        pub assignees: Vec<String>,
        pub candidate_groups: Vec<String>,
        pub state: HumanWorkItemState,
        pub claim_fence: u64,
        pub claimed_by: Option<String>,
        pub claim_expires_at: Option<String>,
        pub projection_version: u64,
    }

    pub fn from_validated_storage_parts(parts: HumanWorkItemParts) -> HumanWorkItem {
        HumanWorkItem {
            work_item_id: parts.work_item_id,
            run_id: parts.run_id,
            activation_id: parts.activation_id,
            signal_name: parts.signal_name,
            request: parts.request,
            response_type: parts.response_type,
            assignees: parts.assignees,
            candidate_groups: parts.candidate_groups,
            state: parts.state,
            claim_fence: parts.claim_fence,
            claimed_by: parts.claimed_by,
            claim_expires_at: parts.claim_expires_at,
            projection_version: parts.projection_version,
        }
    }

    pub fn parse_state(value: &str) -> Result<HumanWorkItemState, RepositoryError> {
        HumanWorkItemState::parse(value)
    }

    pub fn assigned_to(work_item: &HumanWorkItem, principal: &HumanTaskPrincipal) -> bool {
        work_item.assigned_to(principal)
    }
}
