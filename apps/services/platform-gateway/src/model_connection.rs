use super::*;
use insight_platform_api::model_connection::{ModelConnectionApplication, ModelConnectionIntent};
use insight_platform_api::model_credential_management::*;
use insight_platform_contracts::{
    ModelConnectionError, ModelConnectionObservationV1, ModelConnectionProbeAuthorizationV1,
};

pub(super) struct PgModelConnections {
    pub repository: Arc<PgRepository>,
    pub catalog: Option<insight_platform_contracts::ModelInstallationCatalogV1>,
    pub egress: Arc<dyn super::model_credentials::ModelManagementEgress>,
}
#[async_trait]
impl ModelConnectionApplication for PgModelConnections {
    async fn probe(
        &self,
        intent: ModelConnectionIntent,
    ) -> Result<ModelConnectionObservationV1, ResourceApplicationError> {
        let catalog = self
            .catalog
            .as_ref()
            .ok_or(ResourceApplicationError::Unavailable)?;
        if !catalog.validate()
            || catalog.canonical_digest().ok().as_ref() != Some(&intent.request.installation_digest)
        {
            return Err(ResourceApplicationError::Conflict);
        }
        // Revalidate installed policy authority through the same current configuration read path.
        self.repository
            .model_configuration_facts(
                &intent.principal.tenant_id,
                &intent.principal.principal_id,
                intent.principal.principal_kind,
                catalog,
                None,
                None,
            )
            .await
            .map_err(map_resource_repository_error)?;
        let request = ModelConnectionProbeAuthorizationV1 {
            schema_version: 1,
            request_id: new_id(ResourceKind::ServerRequest)?,
            tenant_id: intent.principal.tenant_id,
            principal_id: intent.principal.principal_id,
            principal_kind: intent.principal.principal_kind,
            installation_digest: intent.request.installation_digest,
            model_deployment: intent.request.model_deployment,
            environment: catalog.environment.clone(),
            deadline: insight_platform_contracts::UtcTimestamp::from_datetime(intent.deadline),
        };
        self.egress
            .probe_model_connection(request)
            .await
            .map_err(|e| match e {
                ModelConnectionError::Rejected => ResourceApplicationError::Denied,
                ModelConnectionError::NotFound => ResourceApplicationError::NotFound,
                ModelConnectionError::Conflict => ResourceApplicationError::Conflict,
                ModelConnectionError::Unavailable => ResourceApplicationError::Unavailable,
            })
    }
}
pub(super) struct PgModelCredentials(pub Arc<PgRepository>);
#[async_trait]
impl ModelCredentialManagementApplication for PgModelCredentials {
    async fn read(
        &self,
        intent: ModelCredentialReadIntent,
    ) -> Result<ModelCredentialMetadataViewV1, ResourceApplicationError> {
        if intent.deadline <= chrono::Utc::now() {
            return Err(ResourceApplicationError::Unavailable);
        }
        view(
            self.0
                .read_model_credential_for_principal(
                    &intent.principal.tenant_id,
                    &intent.principal.principal_id,
                    intent.principal.principal_kind,
                    &intent.secret_binding_id,
                )
                .await
                .map_err(map_resource_repository_error)?,
        )
    }
    async fn revoke(
        &self,
        intent: ModelCredentialRevokeIntent,
    ) -> Result<ModelCredentialMetadataViewV1, ResourceApplicationError> {
        let now = chrono::Utc::now();
        if intent.read.deadline <= now {
            return Err(ResourceApplicationError::Unavailable);
        }
        let principal = intent.read.principal;
        let command = insight_platform_security::RevokeSecretBinding {
            audit: CommandAudit {
                trace: principal.trace,
                tenant_id: principal.tenant_id,
                principal_id: principal.principal_id,
                principal_kind: principal.principal_kind,
                receipt_id: new_id(ResourceKind::Receipt)?,
                event_id: new_id(ResourceKind::Event)?,
                outbox_id: new_id(ResourceKind::OutboxEvent)?,
                idempotency_key_digest: intent.idempotency_key_digest,
                request_digest: intent.request_digest,
                receipt_expires_at: now + chrono::Duration::hours(24),
            },
            secret_binding_id: intent.read.secret_binding_id,
            expected_generation: i64::try_from(intent.expected_generation)
                .map_err(|_| ResourceApplicationError::Invalid)?,
            expected_version: i64::try_from(intent.expected_version)
                .map_err(|_| ResourceApplicationError::Invalid)?,
        };
        let mut tx = self
            .0
            .begin_security_transaction()
            .await
            .map_err(map_resource_repository_error)?;
        let outcome = tx
            .revoke_model_credential(command)
            .await
            .map_err(map_resource_repository_error)?;
        tx.commit().await.map_err(map_resource_repository_error)?;
        view(match outcome {
            insight_platform_contracts::CommandOutcome::Applied(v)
            | insight_platform_contracts::CommandOutcome::Replayed(v) => v,
        })
    }
}
fn view(
    record: insight_platform_postgres::repository::SecretBindingMetadataRecord,
) -> Result<ModelCredentialMetadataViewV1, ResourceApplicationError> {
    let invalid = || ResourceApplicationError::Internal;
    let binding: ResourceId = record.secret_binding_id.parse().map_err(|_| invalid())?;
    let version = u64::try_from(record.version).map_err(|_| invalid())?;
    let view = ModelCredentialMetadataViewV1 {
        schema_version: 1,
        tenant_id: record.tenant_id.parse().map_err(|_| invalid())?,
        etag: resource_etag(&binding, version),
        secret_binding_id: binding,
        provider_id: record.provider_id.parse().map_err(|_| invalid())?,
        purpose: record.purpose.parse().map_err(|_| invalid())?,
        state: record.state.parse().map_err(|_| invalid())?,
        generation: u64::try_from(record.generation).map_err(|_| invalid())?,
        version,
    };
    if !view.validate() {
        return Err(invalid());
    }
    Ok(view)
}
