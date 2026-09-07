//! Exact dependency feature projections. No additional deployment or installation authority.
use crate::repository::{
    decode_deployment_closure, payload_from_row, validate_deployment_closure_exists,
    RepositoryError,
};
use insight_platform_contracts::*;
use sqlx::{Postgres, Row, Transaction};
use std::collections::BTreeSet;

pub(crate) async fn derive_agent_deployment_features(
    tx: &mut Transaction<'_, Postgres>,
    tenant: &ResourceId,
    exact: &ExactDeploymentRef,
) -> Result<AgentDeploymentFeaturesV1, RepositoryError> {
    exact
        .validate()
        .map_err(|error| RepositoryError::InvalidInput(error.to_string()))?;
    let row = sqlx::query("SELECT d.payload_schema_version,d.bindings,d.bindings_digest FROM insight_platform.deployments d JOIN insight_platform.resources r ON r.tenant_id=d.tenant_id AND r.resource_id=d.resource_id WHERE d.tenant_id=$1 AND d.deployment_id=$2 AND r.gate_state='enabled' AND r.lifecycle_state<>'retired'")
        .bind(tenant.to_string()).bind(exact.deployment_id.to_string()).fetch_optional(&mut **tx).await?
        .ok_or(RepositoryError::NotFound("exact feature deployment"))?;
    if row.try_get::<String, _>("bindings_digest")? != exact.deployment_digest.as_str() {
        return Err(RepositoryError::Conflict("exact feature deployment"));
    }
    let closure = decode_deployment_closure(&payload_from_row(
        &row,
        "payload_schema_version",
        "bindings",
        "bindings_digest",
    )?)?;
    // Reuse actual owning checks of implementation contracts, installed codec references,
    // conformance Artifacts, exact policies and backend bindings. A backend enum is insufficient.
    validate_deployment_closure_exists(tx, tenant, &closure).await?;
    let mut features = BTreeSet::new();
    let interface_contract_digest = match closure {
        DeploymentClosure::CapabilityInterface(capability)
            if exact.resource_kind == ResourceKind::CapabilityDeployment =>
        {
            match capability.backend {
                CapabilityBackendBinding::Native { .. } => {}
                CapabilityBackendBinding::Http { .. } | CapabilityBackendBinding::Grpc { .. } => {
                    features.insert(AgentRequiredFeature::RemoteCapability);
                }
                CapabilityBackendBinding::Mcp { .. } => {
                    features.insert(AgentRequiredFeature::RemoteCapability);
                    features.insert(AgentRequiredFeature::Mcp);
                }
                CapabilityBackendBinding::Sandbox { .. } => {
                    features.insert(AgentRequiredFeature::Sandbox);
                }
            }
            capability.interface.semantic_digest
        }
        DeploymentClosure::ContextSourceInterface(context)
            if exact.resource_kind == ResourceKind::ContextDeployment =>
        {
            features.insert(AgentRequiredFeature::Context);
            if matches!(context.backend, ContextBackendBinding::McpResources { .. }) {
                features.insert(AgentRequiredFeature::Mcp);
            }
            if context.embedding_model_deployment.is_some() {
                features.insert(AgentRequiredFeature::Model);
            }
            context.interface.semantic_digest
        }
        DeploymentClosure::Agent(agent) if exact.resource_kind == ResourceKind::AgentDeployment => {
            let version = crate::invocation_repository::load_enabled_exact_published_version(
                tx,
                tenant,
                &agent.plan,
                RegistryResourceKind::Agent,
            )
            .await?;
            let ResourceDocument::Agent(document) = version.document else {
                return Err(RepositoryError::CorruptRow(
                    "Child Agent feature source is invalid".into(),
                ));
            };
            if document.typed_plan_digest != agent.plan.semantic_digest || version.validation.program_requirement.as_ref().is_none_or(|requirement| !matches!(requirement,ExecutionRequirement::Program { definition_digest, .. } if definition_digest==&document.typed_plan_digest)) {
                return Err(RepositoryError::Conflict("validated Child Agent feature source"));
            }
            features.extend(document.required_features);
            agent.interface.semantic_digest
        }
        _ => {
            return Err(RepositoryError::InvalidInput(
                "deployment needs no derived feature evidence".into(),
            ))
        }
    };
    let result = AgentDeploymentFeaturesV1 {
        schema_version: 1,
        deployment: exact.clone(),
        interface_contract_digest,
        required_features: features.into_iter().collect(),
    };
    result
        .validate()
        .map_err(|error| RepositoryError::CorruptRow(error.to_string()))?;
    Ok(result)
}
