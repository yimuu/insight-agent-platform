use crate::{PrincipalKind, ResourceId, ResourceKind, Sha256Digest};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::{error::Error, fmt};

/// Immutable audit and idempotency identity shared by tenant-scoped commands.
#[derive(Debug, Clone)]
pub struct CommandAudit {
    pub tenant_id: ResourceId,
    pub principal_id: ResourceId,
    /// Exact tenant binding selected by authentication; tenant commands never search across roles.
    pub principal_kind: PrincipalKind,
    pub receipt_id: ResourceId,
    pub event_id: ResourceId,
    pub outbox_id: ResourceId,
    pub idempotency_key_digest: Sha256Digest,
    pub request_digest: Sha256Digest,
    pub receipt_expires_at: DateTime<Utc>,
}

impl CommandAudit {
    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), CommandContractError> {
        let kinds = [
            (&self.tenant_id, ResourceKind::Tenant),
            (&self.principal_id, ResourceKind::Principal),
            (&self.receipt_id, ResourceKind::Receipt),
            (&self.event_id, ResourceKind::Event),
            (&self.outbox_id, ResourceKind::OutboxEvent),
        ];
        if kinds.iter().any(|(id, expected)| id.kind() != *expected)
            || self.principal_kind == PrincipalKind::InstallationOperator
            || self.receipt_expires_at <= now
        {
            return Err(CommandContractError::InvalidAudit);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "disposition", content = "value", rename_all = "snake_case")]
pub enum CommandOutcome<T> {
    Applied(T),
    Replayed(T),
}

/// Opaque identities preallocated for one external-leaf success continuation.
/// The owner repository derives every semantic field from the frozen Run/Plan closure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExternalLeafResumeMutationIds {
    pub continuation_node_execution_id: ResourceId,
    pub continuation_job_id: ResourceId,
    pub run_event_id: ResourceId,
    pub run_outbox_id: ResourceId,
    pub leaf_node_event_id: ResourceId,
    pub leaf_node_outbox_id: ResourceId,
    pub continuation_node_event_id: ResourceId,
    pub continuation_node_outbox_id: ResourceId,
    pub continuation_job_event_id: ResourceId,
    pub continuation_job_outbox_id: ResourceId,
}

impl ExternalLeafResumeMutationIds {
    pub fn validate(&self) -> Result<(), CommandContractError> {
        let expected = [
            (
                &self.continuation_node_execution_id,
                ResourceKind::NodeExecution,
            ),
            (&self.continuation_job_id, ResourceKind::Job),
            (&self.run_event_id, ResourceKind::Event),
            (&self.run_outbox_id, ResourceKind::OutboxEvent),
            (&self.leaf_node_event_id, ResourceKind::Event),
            (&self.leaf_node_outbox_id, ResourceKind::OutboxEvent),
            (&self.continuation_node_event_id, ResourceKind::Event),
            (&self.continuation_node_outbox_id, ResourceKind::OutboxEvent),
            (&self.continuation_job_event_id, ResourceKind::Event),
            (&self.continuation_job_outbox_id, ResourceKind::OutboxEvent),
        ];
        if expected.iter().any(|(id, kind)| id.kind() != *kind) {
            return Err(CommandContractError::InvalidMutationIds);
        }
        let mut unique = std::collections::BTreeSet::new();
        if expected
            .iter()
            .any(|(id, _)| !unique.insert(id.to_string()))
        {
            return Err(CommandContractError::InvalidMutationIds);
        }
        Ok(())
    }
}

/// Opaque identities preallocated for one external-leaf terminal failure handoff. The leaf owner
/// derives the typed failure and all convergence semantics; callers provide identities only.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExternalLeafFailureMutationIds {
    pub convergence_job_id: ResourceId,
    pub run_event_id: ResourceId,
    pub run_outbox_id: ResourceId,
    pub leaf_node_event_id: ResourceId,
    pub leaf_node_outbox_id: ResourceId,
    pub convergence_job_event_id: ResourceId,
    pub convergence_job_outbox_id: ResourceId,
}

impl ExternalLeafFailureMutationIds {
    pub fn validate(&self) -> Result<(), CommandContractError> {
        let expected = [
            (&self.convergence_job_id, ResourceKind::Job),
            (&self.run_event_id, ResourceKind::Event),
            (&self.run_outbox_id, ResourceKind::OutboxEvent),
            (&self.leaf_node_event_id, ResourceKind::Event),
            (&self.leaf_node_outbox_id, ResourceKind::OutboxEvent),
            (&self.convergence_job_event_id, ResourceKind::Event),
            (&self.convergence_job_outbox_id, ResourceKind::OutboxEvent),
        ];
        if expected.iter().any(|(id, kind)| id.kind() != *kind) {
            return Err(CommandContractError::InvalidMutationIds);
        }
        let mut unique = std::collections::BTreeSet::new();
        if expected
            .iter()
            .any(|(id, _)| !unique.insert(id.to_string()))
        {
            return Err(CommandContractError::InvalidMutationIds);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommandContractError {
    InvalidAudit,
    InvalidMutationIds,
}

impl fmt::Display for CommandContractError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidAudit => {
                formatter.write_str("command audit identity or expiry is invalid")
            }
            Self::InvalidMutationIds => formatter.write_str("command mutation IDs are invalid"),
        }
    }
}

impl Error for CommandContractError {}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(value: &str) -> ResourceId {
        value.parse().unwrap()
    }

    fn digest(character: char) -> Sha256Digest {
        format!("sha256:{}", character.to_string().repeat(64))
            .parse()
            .unwrap()
    }

    #[test]
    fn audit_ids_are_not_interchangeable() {
        let audit = CommandAudit {
            tenant_id: id("ten_0198f1c3-8f49-7c3e-b1f3-773c28367b90"),
            principal_id: id("prn_0198f1c3-8f49-7c3e-b1f3-773c28367b91"),
            principal_kind: PrincipalKind::AgentRunner,
            receipt_id: id("evt_0198f1c3-8f49-7c3e-b1f3-773c28367b92"),
            event_id: id("evt_0198f1c3-8f49-7c3e-b1f3-773c28367b93"),
            outbox_id: id("obx_0198f1c3-8f49-7c3e-b1f3-773c28367b94"),
            idempotency_key_digest: digest('a'),
            request_digest: digest('b'),
            receipt_expires_at: Utc::now() + chrono::Duration::minutes(1),
        };
        assert_eq!(
            audit.validate_at(Utc::now()),
            Err(CommandContractError::InvalidAudit)
        );
    }
}
