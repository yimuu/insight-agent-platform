use super::*;
use crate::openbao::OpenBaoProvider;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SecretProviderReadinessError {
    Unavailable,
    InvalidEvidence,
}

pub struct SecretProviderCatalog {
    providers: InstalledSecretProviderCatalog,
    references: Arc<ReferenceCatalog>,
    readiness: Vec<Readiness>,
}

enum Readiness {
    Aws(AwsSecretProviderCatalog),
    OpenBao(Arc<OpenBaoProvider>),
}

impl fmt::Debug for SecretProviderCatalog {
    fn fmt(&self, output: &mut fmt::Formatter<'_>) -> fmt::Result {
        output
            .debug_struct("SecretProviderCatalog")
            .field("provider_count", &self.readiness.len())
            .finish_non_exhaustive()
    }
}

impl SecretProviderCatalog {
    pub async fn install(
        config: SecretProviderCatalogConfigV2,
    ) -> Result<Self, SecretProviderConfigError> {
        Self::install_with_observer(config, Arc::new(NoopSecretExternalDependencyObserver)).await
    }

    pub async fn install_with_observer(
        config: SecretProviderCatalogConfigV2,
        observer: Arc<dyn SecretExternalDependencyObserver>,
    ) -> Result<Self, SecretProviderConfigError> {
        config.validate()?;
        let mut installed = Vec::new();
        let mut references = ReferenceCatalog {
            sealers: BTreeMap::new(),
            unsealers: BTreeMap::new(),
        };
        let mut readiness = Vec::new();
        for config in config.providers {
            let id = config.provider_id().clone();
            match config {
                SecretProviderConfig::AwsSecretsManager(config) => {
                    let aws = AwsSecretProviderCatalog::install_with_observer(
                        AwsSecretProviderCatalogConfig {
                            schema_version: 1,
                            providers: vec![*config],
                        },
                        Arc::clone(&observer),
                    )
                    .await
                    .map_err(|_| SecretProviderConfigError::InvalidProvider)?;
                    let (sealer, unsealer, providers) = aws.cloned_components();
                    let provider = providers
                        .get(&id)
                        .ok_or(SecretProviderConfigError::InvalidProvider)?;
                    installed.push(provider);
                    references.sealers.insert(id.clone(), sealer);
                    references.unsealers.insert(id, unsealer);
                    readiness.push(Readiness::Aws(aws));
                }
                SecretProviderConfig::OpenBaoKvV2(config) => {
                    let bao = Arc::new(OpenBaoProvider::install(*config, Arc::clone(&observer))?);
                    installed.push(Arc::clone(&bao) as Arc<dyn InstalledSecretProvider>);
                    references.sealers.insert(
                        id.clone(),
                        Arc::clone(&bao) as Arc<dyn SecretReferenceSealer>,
                    );
                    references
                        .unsealers
                        .insert(id, Arc::clone(&bao) as Arc<dyn SecretReferenceUnsealer>);
                    readiness.push(Readiness::OpenBao(bao));
                }
            }
        }
        Ok(Self {
            providers: InstalledSecretProviderCatalog::new(installed)
                .map_err(|_| SecretProviderConfigError::InvalidCatalog)?,
            references: Arc::new(references),
            readiness,
        })
    }

    pub async fn check_readiness(&self) -> Result<(), SecretProviderReadinessError> {
        for provider in &self.readiness {
            match provider {
                Readiness::Aws(aws) => {
                    aws.check_readiness().await.map_err(|error| match error {
                        AwsSecretProviderReadinessError::SecretsManagerUnavailable
                        | AwsSecretProviderReadinessError::KmsUnavailable => {
                            SecretProviderReadinessError::Unavailable
                        }
                        _ => SecretProviderReadinessError::InvalidEvidence,
                    })?
                }
                Readiness::OpenBao(bao) => {
                    bao.check_readiness().await.map_err(|error| match error {
                        insight_platform_openbao::BaoError::Unavailable => {
                            SecretProviderReadinessError::Unavailable
                        }
                        _ => SecretProviderReadinessError::InvalidEvidence,
                    })?
                }
            }
        }
        Ok(())
    }

    pub fn into_components(
        self,
    ) -> (
        Arc<dyn SecretReferenceSealer>,
        Arc<dyn SecretReferenceUnsealer>,
        InstalledSecretProviderCatalog,
    ) {
        (
            Arc::clone(&self.references) as Arc<dyn SecretReferenceSealer>,
            self.references as Arc<dyn SecretReferenceUnsealer>,
            self.providers,
        )
    }
}

struct ReferenceCatalog {
    sealers: BTreeMap<ResourceId, Arc<dyn SecretReferenceSealer>>,
    unsealers: BTreeMap<ResourceId, Arc<dyn SecretReferenceUnsealer>>,
}

#[async_trait]
impl SecretReferenceSealer for ReferenceCatalog {
    async fn seal(
        &self,
        tenant: &ResourceId,
        binding: &ResourceId,
        provider: &ResourceId,
        generation: u64,
        reference: &OpaqueSecretReference,
    ) -> Result<SealedSecretReference, SecretReferenceSealError> {
        self.sealers
            .get(provider)
            .ok_or(SecretReferenceSealError::Rejected)?
            .seal(tenant, binding, provider, generation, reference)
            .await
    }
}

#[async_trait]
impl SecretReferenceUnsealer for ReferenceCatalog {
    async fn unseal(
        &self,
        record: &SecretBindingResolutionRecord,
    ) -> Result<OpaqueSecretReference, SecretReferenceUnsealError> {
        self.unsealers
            .get(&record.provider_id)
            .ok_or(SecretReferenceUnsealError::Rejected)?
            .unseal(record)
            .await
    }
}
