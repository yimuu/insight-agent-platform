use super::*;
use insight_platform_api::resource::{ModelQuotaViewV1, ReadModelQuotaIntent, SetModelQuotaIntent};
impl PgResources {
    pub(super) async fn read_model_quota_inner(
        &self,
        intent: ReadModelQuotaIntent,
    ) -> Result<ModelQuotaViewV1, ResourceApplicationError> {
        let remaining = (intent.deadline - chrono::Utc::now())
            .to_std()
            .map_err(|_| ResourceApplicationError::Unavailable)?;
        tokio::time::timeout(
            remaining,
            self.repository.read_model_quota_for_principal(
                &intent.principal.tenant_id,
                &intent.principal.principal_id,
                intent.principal.principal_kind,
                &intent.model_deployment_id,
            ),
        )
        .await
        .map_err(|_| ResourceApplicationError::Unavailable)?
        .map_err(map_resource_repository_error)
    }
    pub(super) async fn set_model_quota_inner(
        &self,
        intent: SetModelQuotaIntent,
    ) -> Result<ModelQuotaViewV1, ResourceApplicationError> {
        let now = chrono::Utc::now();
        let remaining = (intent.deadline - now)
            .to_std()
            .map_err(|_| ResourceApplicationError::Unavailable)?;
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
        let command = insight_platform_registry::model_quota::SetModelQuota {
            audit,
            request: intent.request,
            expected_etag: intent.expected_etag,
            account_ids: ids(ResourceKind::QuotaAccount)?,
            event_ids: ids(ResourceKind::Event)?,
            outbox_ids: ids(ResourceKind::OutboxEvent)?,
        };
        tokio::time::timeout(remaining, async {
            let mut tx = self
                .repository
                .begin_registry_transaction()
                .await
                .map_err(map_resource_repository_error)?;
            let outcome = tx
                .set_model_quota(command)
                .await
                .map_err(map_resource_repository_error)?;
            tx.commit().await.map_err(map_resource_repository_error)?;
            Ok(match outcome {
                insight_platform_contracts::CommandOutcome::Applied(view)
                | insight_platform_contracts::CommandOutcome::Replayed(view) => view,
            })
        })
        .await
        .map_err(|_| ResourceApplicationError::Unavailable)?
    }
}
fn ids(kind: ResourceKind) -> Result<[ResourceId; 3], ResourceApplicationError> {
    Ok([new_id(kind)?, new_id(kind)?, new_id(kind)?])
}
