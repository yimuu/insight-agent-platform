//! One physical Model catalog producer shared with the actual worker configuration.
use insight_platform_contracts::{InstalledModelAdapter, ModelInstallationCatalogV2};
use insight_platform_deployment_contracts::installation::{
    InstallationError, InstallationIdentityV1, InstallationInputV1,
};

pub fn installation_catalog(
    input: &InstallationInputV1,
    identity: &InstallationIdentityV1,
    policies: &insight_platform_registry::model_policy_bootstrap::ModelPolicyBootstrapMaterial,
    builds: &crate::worker_profile::WorkerBuilds,
) -> Result<Option<ModelInstallationCatalogV2>, InstallationError> {
    input.validate()?;
    identity.validate()?;
    if identity.input_digest != input.digest()? {
        return Err(InstallationError::IdentityDrift);
    }
    let manifest = crate::worker_profile::model_manifest(builds)
        .ok_or(InstallationError::InvalidRoleClosure)?;
    let manifest_digest: insight_platform_contracts::Sha256Digest =
        insight_platform_contracts::canonical_digest(&manifest)
            .map_err(|_| InstallationError::InvalidInput)?
            .parse()
            .map_err(|_| InstallationError::InvalidInput)?;
    let adapters = [
        insight_platform_contracts::ModelProviderWireProtocol::OpenAiResponses,
        insight_platform_contracts::ModelProviderWireProtocol::AnthropicMessages,
    ]
    .into_iter()
    .map(|protocol| InstalledModelAdapter {
        qualified_name: protocol.qualified_name().to_owned(),
        worker_manifest_digest: manifest_digest.clone(),
        adapter_contract_digest: protocol.adapter_contract_digest(),
    })
    .collect();
    let catalog = ModelInstallationCatalogV2 {
        schema_version: 2,
        environment: input.environment_class.clone(),
        secret_provider_id: identity.secret_provider_id.clone(),
        policies: policies.configuration_policies(),
        adapters,
    };
    if !catalog.validate() {
        return Err(InstallationError::InvalidInput);
    }
    Ok(Some(catalog))
}
