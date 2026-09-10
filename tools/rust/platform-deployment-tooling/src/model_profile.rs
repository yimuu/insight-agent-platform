//! One physical Model catalog producer shared with the actual worker configuration.
use insight_platform_contracts::{
    InstalledModelAdapter, InstalledModelDestinationGrant, ModelBootstrapPolicyRole as Policy,
    ModelInstallationCatalogV1, ModelInstallationDestinationV1, MODEL_API_KEY_PURPOSE,
};
use insight_platform_deployment_contracts::installation::{
    InstallationError, InstallationIdentityV1, InstallationInputV1,
};

pub fn installation_catalog(
    input: &InstallationInputV1,
    identity: &InstallationIdentityV1,
    policies: &insight_platform_registry::model_policy_bootstrap::ModelPolicyBootstrapMaterial,
    builds: &crate::worker_profile::WorkerBuilds,
) -> Result<Option<ModelInstallationCatalogV1>, InstallationError> {
    input.validate()?;
    identity.validate()?;
    if identity.input_digest != input.digest()? {
        return Err(InstallationError::IdentityDrift);
    }
    if input.model_destinations.is_empty() {
        return Ok(None);
    }
    let manifest = crate::worker_profile::model_manifest(builds)
        .ok_or(InstallationError::InvalidRoleClosure)?;
    let manifest_digest: insight_platform_contracts::Sha256Digest =
        insight_platform_contracts::canonical_digest(&manifest)
            .map_err(|_| InstallationError::InvalidInput)?
            .parse()
            .map_err(|_| InstallationError::InvalidInput)?;
    let destinations = input
        .model_destinations
        .iter()
        .map(|destination| {
            Ok(ModelInstallationDestinationV1 {
                adapter: InstalledModelAdapter {
                    qualified_name: destination.protocol.qualified_name().to_owned(),
                    worker_manifest_digest: manifest_digest.clone(),
                    adapter_contract_digest: destination.protocol.adapter_contract_digest(),
                },
                grant: InstalledModelDestinationGrant {
                    schema_version: 1,
                    protocol: destination.protocol,
                    endpoint: destination.endpoint.clone(),
                    endpoint_identity_digest: destination
                        .endpoint
                        .canonical_digest()
                        .map_err(|_| InstallationError::InvalidEndpoint)?,
                    credential_purpose: MODEL_API_KEY_PURPOSE
                        .parse()
                        .map_err(|_| InstallationError::InvalidInput)?,
                    network_policy: policies.policy(Policy::Network).exact.revision.clone(),
                    tls_policy: policies.policy(Policy::Tls).exact.revision.clone(),
                    trust_policy: policies.policy(Policy::Trust).exact.revision.clone(),
                    data_policy: policies.policy(Policy::Data).exact.revision.clone(),
                    region: destination.region.clone(),
                    development_loopback: false,
                    development_anonymous: false,
                    trusted_root_pem: None,
                },
            })
        })
        .collect::<Result<Vec<_>, InstallationError>>()?;
    let catalog = ModelInstallationCatalogV1 {
        schema_version: 1,
        environment: input.environment_class.clone(),
        secret_provider_id: identity.secret_provider_id.clone(),
        policies: policies.configuration_policies(),
        destinations,
    };
    if !catalog.validate() {
        return Err(InstallationError::InvalidInput);
    }
    Ok(Some(catalog))
}
