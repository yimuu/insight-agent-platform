use super::*;
use insight_platform_api::resource::{
    ModelDefaultViewV1, ReadModelDefaultIntent, SetModelDefaultIntent,
};

impl PgResources {
    pub(super) async fn read_model_default_inner(
        &self,
        intent: ReadModelDefaultIntent,
    ) -> Result<ModelDefaultViewV1, ResourceApplicationError> {
        if intent.deadline <= chrono::Utc::now() {
            return Err(ResourceApplicationError::Unavailable);
        }
        let record = self
            .repository
            .read_model_default_for_principal(
                &intent.principal.tenant_id,
                &intent.principal.principal_id,
                intent.principal.principal_kind,
            )
            .await
            .map_err(map_resource_repository_error)?;
        view(record)
    }

    pub(super) async fn set_model_default_inner(
        &self,
        intent: SetModelDefaultIntent,
    ) -> Result<ModelDefaultViewV1, ResourceApplicationError> {
        let now = chrono::Utc::now();
        if intent.deadline <= now {
            return Err(ResourceApplicationError::Unavailable);
        }
        let expected_tenant_version = i64::try_from(intent.expected_tenant_version)
            .map_err(|_| ResourceApplicationError::Invalid)?;
        let audit = CommandAudit {
            trace: intent.principal.trace,
            tenant_id: intent.principal.tenant_id,
            principal_id: intent.principal.principal_id,
            principal_kind: intent.principal.principal_kind,
            receipt_id: new_id(ResourceKind::Receipt)?,
            event_id: new_id(ResourceKind::Event)?,
            outbox_id: new_id(ResourceKind::OutboxEvent)?,
            idempotency_key_digest: intent.idempotency_key_digest,
            request_digest: intent.request_digest,
            receipt_expires_at: now + chrono::Duration::hours(24),
        };
        let mut transaction = self
            .repository
            .begin_registry_transaction()
            .await
            .map_err(map_resource_repository_error)?;
        let outcome = transaction
            .bind_tenant_model_default(
                insight_platform_registry::authoring::BindTenantModelDefault {
                    audit,
                    expected_tenant_version,
                    model: intent.default_model,
                },
            )
            .await
            .map_err(map_resource_repository_error)?;
        transaction
            .commit()
            .await
            .map_err(map_resource_repository_error)?;
        let record = match outcome {
            insight_platform_contracts::CommandOutcome::Applied(record)
            | insight_platform_contracts::CommandOutcome::Replayed(record) => record,
        };
        view(record)
    }
}

fn view(
    record: insight_platform_postgres::repository::TenantRecord,
) -> Result<ModelDefaultViewV1, ResourceApplicationError> {
    let tenant_id: ResourceId = record
        .tenant_id
        .parse()
        .map_err(|_| ResourceApplicationError::Internal)?;
    let version = u64::try_from(record.version).map_err(|_| ResourceApplicationError::Internal)?;
    let view = ModelDefaultViewV1 {
        schema_version: 1,
        etag: resource_etag(&tenant_id, version),
        tenant_id,
        default_model: record.config.default_model,
        version,
    };
    view.validate()?;
    Ok(view)
}
