//! Exact controller mutation-slot allocation from a repository-derived durable shape.

use crate::{CoordinatorIdentityFactory, IdentityFactoryError};
use insight_platform_contracts::{ResourceId, ResourceKind, Sha256Digest};
use insight_platform_postgres::repository::{
    ControllerActivationSlot, ControllerLoopRolloverSlot, ControllerMutationRequirements,
    ControllerPendingNodeSlot, ControllerPendingWakeSlot, ControllerRemainderCancellationSlot,
    ControllerScopeSlot, ControllerStepMutationIds, ControllerStructuralExitSlot,
    ControllerStructuralRequirement, OrchestrationTerminalMutationIds,
    MAX_ORCHESTRATION_QUOTA_LINES,
};

pub fn allocate_orchestration_terminal_mutations(
    identities: &impl CoordinatorIdentityFactory,
) -> Result<OrchestrationTerminalMutationIds, IdentityFactoryError> {
    Ok(OrchestrationTerminalMutationIds {
        receipt_id: new_id(identities, ResourceKind::Receipt)?,
        quota_entry_ids: allocate_ids(
            identities,
            ResourceKind::QuotaLedgerEntry,
            MAX_ORCHESTRATION_QUOTA_LINES,
        )?,
        run_event_id: new_id(identities, ResourceKind::Event)?,
        run_outbox_id: new_id(identities, ResourceKind::OutboxEvent)?,
        node_event_id: new_id(identities, ResourceKind::Event)?,
        node_outbox_id: new_id(identities, ResourceKind::OutboxEvent)?,
        scope_closing_event_id: new_id(identities, ResourceKind::Event)?,
        scope_closing_outbox_id: new_id(identities, ResourceKind::OutboxEvent)?,
        scope_terminal_event_id: new_id(identities, ResourceKind::Event)?,
        scope_terminal_outbox_id: new_id(identities, ResourceKind::OutboxEvent)?,
        job_event_id: new_id(identities, ResourceKind::Event)?,
        job_outbox_id: new_id(identities, ResourceKind::OutboxEvent)?,
    })
}

pub fn allocate_controller_step_mutations(
    identities: &impl CoordinatorIdentityFactory,
    requirements: &ControllerMutationRequirements,
    pending_wake_request_digest: Sha256Digest,
) -> Result<ControllerStepMutationIds, IdentityFactoryError> {
    let activations = requirements
        .activation_scopes
        .iter()
        .map(|create_scope| {
            Ok(ControllerActivationSlot {
                node_execution_id: new_id(identities, ResourceKind::NodeExecution)?,
                orchestration_job_id: new_id(identities, ResourceKind::Job)?,
                scope: create_scope
                    .then(|| allocate_scope(identities))
                    .transpose()?,
                node_event_id: new_id(identities, ResourceKind::Event)?,
                node_outbox_id: new_id(identities, ResourceKind::OutboxEvent)?,
                job_event_id: new_id(identities, ResourceKind::Event)?,
                job_outbox_id: new_id(identities, ResourceKind::OutboxEvent)?,
            })
        })
        .collect::<Result<Vec<_>, IdentityFactoryError>>()?;
    let pending_nodes = (0..requirements.pending_node_count)
        .map(|_| {
            Ok(ControllerPendingNodeSlot {
                node_execution_id: new_id(identities, ResourceKind::NodeExecution)?,
                node_event_id: new_id(identities, ResourceKind::Event)?,
                node_outbox_id: new_id(identities, ResourceKind::OutboxEvent)?,
            })
        })
        .collect::<Result<Vec<_>, IdentityFactoryError>>()?;
    let structural_exit = match requirements.structural_exit {
        ControllerStructuralRequirement::None => None,
        ControllerStructuralRequirement::Close => Some(allocate_structural_exit(identities, None)?),
        ControllerStructuralRequirement::LoopRollover {
            carried_value_count,
        } => Some(allocate_structural_exit(
            identities,
            Some(ControllerLoopRolloverSlot {
                scope: allocate_scope(identities)?,
                carried_value_ids: (0..carried_value_count)
                    .map(|_| new_id(identities, ResourceKind::RunValue))
                    .collect::<Result<Vec<_>, _>>()?,
            }),
        )?),
    };
    let pending_wake = requirements
        .pending_wake
        .then(|| {
            Ok(ControllerPendingWakeSlot {
                orchestration_job_id: new_id(identities, ResourceKind::Job)?,
                request_digest: pending_wake_request_digest,
                node_event_id: new_id(identities, ResourceKind::Event)?,
                node_outbox_id: new_id(identities, ResourceKind::OutboxEvent)?,
                job_event_id: new_id(identities, ResourceKind::Event)?,
                job_outbox_id: new_id(identities, ResourceKind::OutboxEvent)?,
            })
        })
        .transpose()?;
    let remainder_cancellations = requirements
        .remainder_cancellation_scope_ids
        .iter()
        .map(|scope_id| allocate_remainder_cancellation(identities, scope_id.clone()))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(ControllerStepMutationIds {
        receipt_id: new_id(identities, ResourceKind::Receipt)?,
        quota_entry_ids: allocate_ids(
            identities,
            ResourceKind::QuotaLedgerEntry,
            MAX_ORCHESTRATION_QUOTA_LINES,
        )?,
        run_event_id: new_id(identities, ResourceKind::Event)?,
        run_outbox_id: new_id(identities, ResourceKind::OutboxEvent)?,
        node_event_id: new_id(identities, ResourceKind::Event)?,
        node_outbox_id: new_id(identities, ResourceKind::OutboxEvent)?,
        job_event_id: new_id(identities, ResourceKind::Event)?,
        job_outbox_id: new_id(identities, ResourceKind::OutboxEvent)?,
        activations,
        pending_nodes,
        structural_exit,
        pending_wake,
        remainder_cancellations,
    })
}

fn allocate_scope(
    identities: &impl CoordinatorIdentityFactory,
) -> Result<ControllerScopeSlot, IdentityFactoryError> {
    Ok(ControllerScopeSlot {
        scope_instance_id: new_id(identities, ResourceKind::ScopeInstance)?,
        scope_event_id: new_id(identities, ResourceKind::Event)?,
        scope_outbox_id: new_id(identities, ResourceKind::OutboxEvent)?,
    })
}

fn allocate_structural_exit(
    identities: &impl CoordinatorIdentityFactory,
    loop_rollover: Option<ControllerLoopRolloverSlot>,
) -> Result<ControllerStructuralExitSlot, IdentityFactoryError> {
    Ok(ControllerStructuralExitSlot {
        scope_closing_event_id: new_id(identities, ResourceKind::Event)?,
        scope_closing_outbox_id: new_id(identities, ResourceKind::OutboxEvent)?,
        scope_terminal_event_id: new_id(identities, ResourceKind::Event)?,
        scope_terminal_outbox_id: new_id(identities, ResourceKind::OutboxEvent)?,
        loop_rollover,
    })
}

fn allocate_remainder_cancellation(
    identities: &impl CoordinatorIdentityFactory,
    expected_scope_id: ResourceId,
) -> Result<ControllerRemainderCancellationSlot, IdentityFactoryError> {
    Ok(ControllerRemainderCancellationSlot {
        expected_scope_id,
        quota_entry_ids: allocate_ids(
            identities,
            ResourceKind::QuotaLedgerEntry,
            MAX_ORCHESTRATION_QUOTA_LINES,
        )?,
        node_cancelling_event_id: new_id(identities, ResourceKind::Event)?,
        node_cancelling_outbox_id: new_id(identities, ResourceKind::OutboxEvent)?,
        node_terminal_event_id: new_id(identities, ResourceKind::Event)?,
        node_terminal_outbox_id: new_id(identities, ResourceKind::OutboxEvent)?,
        scope_closing_event_id: new_id(identities, ResourceKind::Event)?,
        scope_closing_outbox_id: new_id(identities, ResourceKind::OutboxEvent)?,
        scope_terminal_event_id: new_id(identities, ResourceKind::Event)?,
        scope_terminal_outbox_id: new_id(identities, ResourceKind::OutboxEvent)?,
        job_terminal_event_id: new_id(identities, ResourceKind::Event)?,
        job_terminal_outbox_id: new_id(identities, ResourceKind::OutboxEvent)?,
    })
}

fn allocate_ids(
    identities: &impl CoordinatorIdentityFactory,
    kind: ResourceKind,
    count: usize,
) -> Result<Vec<ResourceId>, IdentityFactoryError> {
    (0..count).map(|_| new_id(identities, kind)).collect()
}

fn new_id(
    identities: &impl CoordinatorIdentityFactory,
    kind: ResourceKind,
) -> Result<ResourceId, IdentityFactoryError> {
    identities.new_resource_id(kind)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::UuidCoordinatorIdentityFactory;

    fn digest() -> Sha256Digest {
        format!("sha256:{}", "a".repeat(64)).parse().unwrap()
    }

    #[test]
    fn allocator_matches_map_and_loop_rollover_shapes() {
        let identities = UuidCoordinatorIdentityFactory;
        let map = allocate_controller_step_mutations(
            &identities,
            &ControllerMutationRequirements {
                activation_scopes: vec![true, true],
                pending_node_count: 1,
                structural_exit: ControllerStructuralRequirement::None,
                pending_wake: true,
                remainder_cancellation_scope_ids: vec![],
            },
            digest(),
        )
        .unwrap();
        assert_eq!(map.activations.len(), 2);
        assert!(map.activations.iter().all(|slot| slot.scope.is_some()));
        assert_eq!(map.pending_nodes.len(), 1);
        assert!(map.pending_wake.is_some());

        let rollover = allocate_controller_step_mutations(
            &identities,
            &ControllerMutationRequirements {
                activation_scopes: vec![],
                pending_node_count: 0,
                structural_exit: ControllerStructuralRequirement::LoopRollover {
                    carried_value_count: 2,
                },
                pending_wake: true,
                remainder_cancellation_scope_ids: vec![],
            },
            digest(),
        )
        .unwrap();
        let rollover = rollover.structural_exit.unwrap().loop_rollover.unwrap();
        assert_eq!(rollover.carried_value_ids.len(), 2);
    }
}
