use super::*;
use insight_platform_api::model_configuration::*;
use insight_platform_contracts::{ArtifactRef, ModelInstallationCatalogV2};
use insight_platform_registry::model_configuration::*;

pub(super) struct PgModelConfiguration {
    pub repository: Arc<PgRepository>,
    pub catalog: Option<ModelInstallationCatalogV2>,
}
impl PgModelConfiguration {
    fn installed(
        &self,
        intent: &ModelConfigurationIntent,
        digest: Option<&Sha256Digest>,
    ) -> Result<&ModelInstallationCatalogV2, ModelConfigurationApplicationError> {
        if intent.deadline <= chrono::Utc::now() {
            return Err(ModelConfigurationApplicationError::Unavailable);
        }
        let catalog = self
            .catalog
            .as_ref()
            .ok_or(ModelConfigurationApplicationError::Unavailable)?;
        if digest.is_some_and(|expected| catalog.canonical_digest().as_ref() != Ok(expected)) {
            return Err(ModelConfigurationApplicationError::Conflict);
        }
        Ok(catalog)
    }
    async fn facts(
        &self,
        intent: &ModelConfigurationIntent,
        catalog: &ModelInstallationCatalogV2,
        input: Option<&ModelConfigurationInputV1>,
        artifact: Option<&ArtifactRef>,
    ) -> Result<Option<ModelConfigurationSourceFacts>, ModelConfigurationApplicationError> {
        let remaining = (intent.deadline - chrono::Utc::now())
            .to_std()
            .map_err(|_| ModelConfigurationApplicationError::Unavailable)?;
        tokio::time::timeout(
            remaining,
            self.repository.model_configuration_facts(
                &intent.principal.tenant_id,
                &intent.principal.principal_id,
                intent.principal.principal_kind,
                catalog,
                input,
                artifact,
            ),
        )
        .await
        .map_err(|_| ModelConfigurationApplicationError::Unavailable)?
        .map_err(repository_error)
    }
}
#[async_trait]
impl ModelConfigurationApplication for PgModelConfiguration {
    async fn resources(
        &self,
        intent: ModelConfigurationIntent,
        query: ModelConfigurationResourceQueryV1,
    ) -> Result<ModelConfigurationResourcePageV1, ModelConfigurationApplicationError> {
        let remaining = (intent.deadline - chrono::Utc::now())
            .to_std()
            .map_err(|_| ModelConfigurationApplicationError::Unavailable)?;
        let mut rows = tokio::time::timeout(
            remaining,
            self.repository.model_configuration_resources(
                &intent.principal.tenant_id,
                &intent.principal.principal_id,
                intent.principal.principal_kind,
                query.kind,
                query.after.as_ref(),
            ),
        )
        .await
        .map_err(|_| ModelConfigurationApplicationError::Unavailable)?
        .map_err(repository_error)?;
        let more = rows.len() > 25;
        rows.truncate(25);
        let mut items = Vec::with_capacity(rows.len());
        for (record, active_deployment) in rows {
            let id = record
                .resource_id
                .parse()
                .map_err(|_| ModelConfigurationApplicationError::Unavailable)?;
            let view = resource_view_from_record(record, query.kind, &id)
                .map_err(|_| ModelConfigurationApplicationError::Unavailable)?;
            view.validate()
                .map_err(|_| ModelConfigurationApplicationError::Unavailable)?;
            items.push(ModelConfigurationResourceSummaryV1 {
                resource_id: id,
                resource_kind: query.kind,
                alias: view.draft.alias,
                display_name: view.draft.display_name,
                version: view.version,
                etag: view.etag,
                lifecycle_state: view.lifecycle_state,
                gate_state: view.gate_state,
                active_deployment,
            });
        }
        let next_after = if more {
            items.last().map(|item| item.resource_id.clone())
        } else {
            None
        };
        Ok(ModelConfigurationResourcePageV1 {
            schema_version: 1,
            items,
            next_after,
        })
    }
    async fn catalog(
        &self,
        intent: ModelConfigurationIntent,
    ) -> Result<ModelConfigurationCatalogViewV2, ModelConfigurationApplicationError> {
        let catalog = self.installed(&intent, None)?;
        self.facts(&intent, catalog, None, None).await?;
        ModelConfigurationCatalogViewV2::from_catalog(catalog)
    }
    async fn declare(
        &self,
        intent: ModelConfigurationIntent,
        request: DeclareModelConfigurationRequestV1,
    ) -> Result<ModelConfigurationDeclarationV1, ModelConfigurationApplicationError> {
        let catalog = self.installed(&intent, Some(&request.installation_digest))?;
        let facts = self
            .facts(&intent, catalog, Some(&request.input), None)
            .await?;
        match &request.input {
            ModelConfigurationInputV1::Source(source) => declare_model_source(source, catalog),
            ModelConfigurationInputV1::Model(model) => declare_basic_model(
                model,
                catalog,
                &facts.ok_or(ModelConfigurationApplicationError::Conflict)?,
                chrono::Utc::now(),
            ),
        }
        .map_err(compiler_error)
    }
    async fn compile(
        &self,
        intent: ModelConfigurationIntent,
        request: CompileModelConfigurationRequestV1,
    ) -> Result<CompiledModelConfigurationV1, ModelConfigurationApplicationError> {
        let catalog = self.installed(&intent, Some(&request.installation_digest))?;
        let facts = self
            .facts(
                &intent,
                catalog,
                Some(&request.input),
                Some(&request.artifact),
            )
            .await?;
        match &request.input {
            ModelConfigurationInputV1::Source(source) => {
                compile_model_source(source, catalog, &request.artifact)
            }
            ModelConfigurationInputV1::Model(model) => compile_basic_model(
                model,
                catalog,
                &facts.ok_or(ModelConfigurationApplicationError::Conflict)?,
                &request.artifact,
                chrono::Utc::now(),
            ),
        }
        .map_err(compiler_error)
    }
}
fn compiler_error(error: ModelConfigurationError) -> ModelConfigurationApplicationError {
    match error {
        ModelConfigurationError::Invalid => ModelConfigurationApplicationError::Invalid,
        ModelConfigurationError::DestinationRejected => {
            ModelConfigurationApplicationError::NotFound
        }
        ModelConfigurationError::SourceMismatch | ModelConfigurationError::DeclarationMismatch => {
            ModelConfigurationApplicationError::Conflict
        }
    }
}
fn repository_error(error: RepositoryError) -> ModelConfigurationApplicationError {
    match map_resource_repository_error(error) {
        ResourceApplicationError::Invalid => ModelConfigurationApplicationError::Invalid,
        ResourceApplicationError::Unauthenticated => {
            ModelConfigurationApplicationError::Unauthenticated
        }
        ResourceApplicationError::Denied => ModelConfigurationApplicationError::Forbidden,
        ResourceApplicationError::NotFound => ModelConfigurationApplicationError::NotFound,
        ResourceApplicationError::Unavailable | ResourceApplicationError::Internal => {
            ModelConfigurationApplicationError::Unavailable
        }
        _ => ModelConfigurationApplicationError::Conflict,
    }
}
