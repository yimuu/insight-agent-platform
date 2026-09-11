use super::*;
use insight_platform_contracts::{
    ModelCredentialImportAuthorizationV1, ModelCredentialImportError,
    ModelCredentialImportIdentityV1, ModelCredentialImportPermitV1,
};
use insight_platform_security::ModelCredentialImportAuthority;

pub(super) async fn require_import_principal(
    transaction: &mut Transaction<'_, Postgres>,
    identity: &ModelCredentialImportIdentityV1,
) -> Result<(), RepositoryError> {
    if !identity.validate() {
        return Err(RepositoryError::PermissionDenied);
    }
    let tenant = load_tenant(transaction, &identity.tenant_id).await?;
    if tenant.state != "active" {
        return Err(RepositoryError::PermissionDenied);
    }
    let current = load_current_principal_snapshot(
        transaction,
        &identity.tenant_id,
        &identity.principal_id,
        identity.principal_kind,
    )
    .await?;
    if !current.permissions.contains(Permission::SecretBind) {
        return Err(RepositoryError::PermissionDenied);
    }
    Ok(())
}

#[async_trait::async_trait]
impl ModelCredentialImportAuthority for PgRepository {
    async fn authorize_model_credential_import(
        &self,
        request: &ModelCredentialImportAuthorizationV1,
    ) -> Result<ModelCredentialImportPermitV1, ModelCredentialImportError> {
        self.authorize_model_credential_import_inner(request)
            .await
            .map_err(|error| match error {
                RepositoryError::Database(_) => ModelCredentialImportError::TemporarilyUnavailable,
                _ => ModelCredentialImportError::Rejected,
            })
    }
}
impl PgRepository {
    async fn authorize_model_credential_import_inner(
        &self,
        request: &ModelCredentialImportAuthorizationV1,
    ) -> Result<ModelCredentialImportPermitV1, RepositoryError> {
        let mut transaction = self.pool.begin().await?;
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .execute(&mut *transaction)
            .await?;
        let now: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(&mut *transaction)
            .await?;
        if !request.validate_at(now) {
            return Err(RepositoryError::PermissionDenied);
        }
        require_import_principal(&mut transaction, &request.identity).await?;
        let permit = ModelCredentialImportPermitV1 {
            schema_version: 1,
            request_digest: request
                .canonical_digest()
                .map_err(|_| RepositoryError::PermissionDenied)?,
            valid_until: request.deadline,
        };
        transaction.commit().await?;
        Ok(permit)
    }
}
