//! PostgreSQL-backed controller admission facts for Capability leaves.
//!
//! The provider derives the exact policy set from the immutable Run binding plus the selected
//! Capability Deployment. The owner transaction remains authoritative and repeats every binding,
//! gate and policy check before it creates an Invocation.

use crate::{
    ControllerCapabilityAdmissionDecision, ControllerCapabilityAdmissionProvider,
    ControllerCapabilityAdmissionRequest, DurablePlanDriverError,
};
use async_trait::async_trait;
use insight_platform_contracts::{
    canonical_digest, CapabilityBackendBinding, DeploymentClosure, ExactVersionRef,
    FrozenSlotTarget, RunBindingsSnapshot, Sha256Digest,
};
use insight_platform_invocations::{
    InvocationPolicyDecision, InvocationPolicyDecisionBundle, InvocationPolicyDisposition,
};
use insight_platform_postgres::repository::PgRepository;
use serde_json::Value;
use sqlx::Row;
use std::collections::BTreeMap;

#[derive(Clone)]
pub struct PostgresControllerCapabilityAdmissionProvider {
    repository: PgRepository,
}

impl PostgresControllerCapabilityAdmissionProvider {
    pub fn new(repository: PgRepository) -> Self {
        Self { repository }
    }

    async fn load_facts(
        &self,
        request: &ControllerCapabilityAdmissionRequest,
    ) -> Result<
        (
            RunBindingsSnapshot,
            insight_platform_contracts::CapabilityDeploymentClosure,
        ),
        DurablePlanDriverError,
    > {
        let row = sqlx::query(
            r#"
            SELECT run.bindings_schema_version, run.bindings, run.bindings_digest,
                   deployment.payload_schema_version, deployment.bindings AS deployment_bindings,
                   deployment.bindings_digest AS deployment_bindings_digest
            FROM insight_platform.runs AS run
            JOIN insight_platform.deployments AS deployment
              ON deployment.tenant_id = run.tenant_id
             AND deployment.deployment_id = $3
            WHERE run.tenant_id = $1 AND run.run_id = $2
            "#,
        )
        .bind(request.tenant_id.to_string())
        .bind(request.run_id.to_string())
        .bind(request.selected_deployment.deployment_id.to_string())
        .fetch_optional(self.repository.pool())
        .await
        .map_err(|_| DurablePlanDriverError::Unavailable)?
        .ok_or(DurablePlanDriverError::FenceLost)?;
        if row
            .try_get::<i32, _>("bindings_schema_version")
            .map_err(|_| DurablePlanDriverError::InvariantViolation)?
            != 1
            || row
                .try_get::<i32, _>("payload_schema_version")
                .map_err(|_| DurablePlanDriverError::InvariantViolation)?
                != 1
        {
            return Err(DurablePlanDriverError::InvariantViolation);
        }
        let bindings_value: Value = row
            .try_get("bindings")
            .map_err(|_| DurablePlanDriverError::InvariantViolation)?;
        let bindings: RunBindingsSnapshot = serde_json::from_value(bindings_value.clone())
            .map_err(|_| DurablePlanDriverError::InvariantViolation)?;
        bindings
            .validate()
            .map_err(|_| DurablePlanDriverError::InvariantViolation)?;
        let stored_bindings_digest: String = row
            .try_get("bindings_digest")
            .map_err(|_| DurablePlanDriverError::InvariantViolation)?;
        if bindings.canonical_digest.to_string() != stored_bindings_digest {
            return Err(DurablePlanDriverError::InvariantViolation);
        }
        let mut deployment_value: Value = row
            .try_get("deployment_bindings")
            .map_err(|_| DurablePlanDriverError::InvariantViolation)?;
        let stored_deployment_digest: String = row
            .try_get("deployment_bindings_digest")
            .map_err(|_| DurablePlanDriverError::InvariantViolation)?;
        let actual_deployment_digest: Sha256Digest = canonical_digest(&deployment_value)
            .map_err(|_| DurablePlanDriverError::InvariantViolation)?
            .parse()
            .map_err(|_| DurablePlanDriverError::InvariantViolation)?;
        if actual_deployment_digest.to_string() != stored_deployment_digest
            || request.selected_deployment.deployment_digest != actual_deployment_digest
        {
            return Err(DurablePlanDriverError::FenceLost);
        }
        deployment_value
            .as_object_mut()
            .ok_or(DurablePlanDriverError::InvariantViolation)?
            .remove("schema_version");
        let closure: DeploymentClosure = serde_json::from_value(deployment_value)
            .map_err(|_| DurablePlanDriverError::InvariantViolation)?;
        closure
            .validate()
            .map_err(|_| DurablePlanDriverError::InvariantViolation)?;
        let DeploymentClosure::CapabilityInterface(capability) = closure else {
            return Err(DurablePlanDriverError::InvariantViolation);
        };
        Ok((bindings, capability))
    }
}

#[async_trait]
impl ControllerCapabilityAdmissionProvider for PostgresControllerCapabilityAdmissionProvider {
    async fn decide(
        &self,
        request: ControllerCapabilityAdmissionRequest,
    ) -> Result<ControllerCapabilityAdmissionDecision, DurablePlanDriverError> {
        let (bindings, deployment) = self.load_facts(&request).await?;
        let slot = bindings
            .slots
            .iter()
            .find(|slot| slot.slot_id == request.slot_id)
            .ok_or(DurablePlanDriverError::FenceLost)?;
        let FrozenSlotTarget::Capability { candidates, .. } = &slot.target else {
            return Err(DurablePlanDriverError::FenceLost);
        };
        if !candidates.contains(&request.selected_deployment)
            || matches!(deployment.backend, CapabilityBackendBinding::Mcp { .. })
        {
            return Err(DurablePlanDriverError::FenceLost);
        }
        let policies = build_allowed_policy_bundle(
            &request,
            bindings
                .policies
                .iter()
                .map(|binding| binding.revision.clone())
                .chain(deployment.policies.iter().cloned()),
        )?;
        Ok(ControllerCapabilityAdmissionDecision {
            policies,
            mcp_runtime: None,
        })
    }
}

fn build_allowed_policy_bundle(
    request: &ControllerCapabilityAdmissionRequest,
    policies: impl IntoIterator<Item = ExactVersionRef>,
) -> Result<InvocationPolicyDecisionBundle, DurablePlanDriverError> {
    let mut exact = BTreeMap::<_, ExactVersionRef>::new();
    for policy in policies {
        exact.insert(policy.revision_id.clone(), policy);
    }
    let decisions = exact
        .into_values()
        .map(|policy| {
            let evidence_digest: Sha256Digest = canonical_digest(&serde_json::json!({
                "input_content_digest": request.input_content_digest,
                "input_value_id": request.input_value_id,
                "node_execution_id": request.node_execution_id,
                "policy": policy,
                "run_id": request.run_id,
                "schema_version": 1,
                "selected_deployment": request.selected_deployment,
                "selection_evidence_digest": request.selection_evidence_digest,
                "slot_id": request.slot_id,
                "tenant_id": request.tenant_id,
            }))
            .map_err(|_| DurablePlanDriverError::InvariantViolation)?
            .parse()
            .map_err(|_| DurablePlanDriverError::InvariantViolation)?;
            Ok(InvocationPolicyDecision {
                policy,
                disposition: InvocationPolicyDisposition::Allowed,
                evidence_digest,
            })
        })
        .collect::<Result<Vec<_>, DurablePlanDriverError>>()?;
    InvocationPolicyDecisionBundle::build(decisions, None)
        .map_err(|_| DurablePlanDriverError::InvariantViolation)
}

#[cfg(test)]
mod tests {
    use super::*;
    use insight_platform_contracts::{ExactDeploymentRef, ResourceId, ResourceKind};

    fn id(kind: ResourceKind, suffix: u16) -> ResourceId {
        format!(
            "{}_0198f1c5-0787-75e1-a9e8-d95ca0f3{suffix:04x}",
            kind.descriptor().prefix
        )
        .parse()
        .unwrap()
    }

    fn digest(character: char) -> Sha256Digest {
        format!("sha256:{}", character.to_string().repeat(64))
            .parse()
            .unwrap()
    }

    fn request() -> ControllerCapabilityAdmissionRequest {
        ControllerCapabilityAdmissionRequest {
            tenant_id: id(ResourceKind::Tenant, 1),
            run_id: id(ResourceKind::Run, 2),
            node_execution_id: id(ResourceKind::NodeExecution, 3),
            slot_id: "search".to_owned(),
            selected_deployment: ExactDeploymentRef::new(
                id(ResourceKind::CapabilityDeployment, 4),
                digest('a'),
            )
            .unwrap(),
            input_value_id: id(ResourceKind::RunValue, 5),
            input_content_digest: digest('b'),
            selection_evidence_digest: digest('c'),
        }
    }

    #[test]
    fn policy_bundle_is_exact_deduplicated_and_request_bound() {
        let first = ExactVersionRef::new(id(ResourceKind::PolicyRevision, 6), digest('d')).unwrap();
        let second =
            ExactVersionRef::new(id(ResourceKind::PolicyRevision, 7), digest('e')).unwrap();
        let bundle =
            build_allowed_policy_bundle(&request(), vec![second.clone(), first.clone(), second])
                .unwrap();
        bundle
            .validate_for(&[
                first.clone(),
                first.clone(),
                bundle.decisions[1].policy.clone(),
            ])
            .unwrap();
        assert_eq!(bundle.decisions.len(), 2);
        assert!(bundle.approval.is_none());

        let mut changed = request();
        changed.input_content_digest = digest('f');
        let changed = build_allowed_policy_bundle(
            &changed,
            bundle
                .decisions
                .iter()
                .map(|decision| decision.policy.clone()),
        )
        .unwrap();
        assert_ne!(bundle.canonical_digest, changed.canonical_digest);
    }
}
