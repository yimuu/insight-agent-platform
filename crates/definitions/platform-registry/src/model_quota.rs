//! Allocation changes existing quota accounts; Registry supplies the command/audit boundary.
use chrono::{DateTime, Utc};
use insight_platform_contracts::{
    canonical_digest, validate_model_quota_etag, CommandAudit, ModelQuotaError, ResourceId,
    ResourceKind, SetModelQuotaRequestV1, Sha256Digest,
};
use std::collections::BTreeSet;
#[derive(Debug, Clone)]
pub struct SetModelQuota {
    pub audit: CommandAudit,
    pub request: SetModelQuotaRequestV1,
    pub expected_etag: String,
    pub account_ids: [ResourceId; 3],
    pub event_ids: [ResourceId; 3],
    pub outbox_ids: [ResourceId; 3],
}
impl SetModelQuota {
    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), ModelQuotaError> {
        self.audit.validate_at(now).map_err(|_| ModelQuotaError)?;
        self.request.validate()?;
        validate_model_quota_etag(&self.expected_etag)?;
        let mut ids = BTreeSet::new();
        for (values, kind) in [
            (&self.account_ids, ResourceKind::QuotaAccount),
            (&self.event_ids, ResourceKind::Event),
            (&self.outbox_ids, ResourceKind::OutboxEvent),
        ] {
            for id in values {
                if id.kind() != kind || !ids.insert(id) {
                    return Err(ModelQuotaError);
                }
            }
        }
        if self.audit.request_digest
            != model_quota_request_digest(
                &self.audit.tenant_id,
                &self.audit.principal_id,
                &self.request,
                &self.expected_etag,
                &self.audit.idempotency_key_digest,
            )?
        {
            return Err(ModelQuotaError);
        }
        Ok(())
    }
}
pub fn model_quota_request_digest(
    tenant: &ResourceId,
    principal: &ResourceId,
    request: &SetModelQuotaRequestV1,
    etag: &str,
    receipt: &Sha256Digest,
) -> Result<Sha256Digest, ModelQuotaError> {
    canonical_digest(&serde_json::json!({"schema_version":1,"operation":"model.quota.set","tenant_id":tenant,"principal_id":principal,"request":request,"expected_etag":etag,"idempotency_key_digest":receipt})).map_err(|_|ModelQuotaError)?.parse().map_err(|_|ModelQuotaError)
}
