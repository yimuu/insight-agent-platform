use crate::repository::{payload_from_row, PgRepository, RepositoryError};
use insight_platform_contracts::{
    PrincipalKind, PrincipalSnapshot, ResourceId, ResourceKind, Sha256Digest,
    TenantPrincipalPayload,
};
use sqlx::Row;

impl PgRepository {
    /// Rebind verified external identity evidence to the current active tenant membership.
    ///
    /// The caller cannot select a principal ID; the database resolves it from the unique
    /// authentication-authority and subject digest pair.
    pub async fn resolve_external_principal(
        &self,
        tenant_id: ResourceId,
        authentication_authority_digest: Sha256Digest,
        subject_digest: Sha256Digest,
        asserted_principal_kind: PrincipalKind,
    ) -> Result<PrincipalSnapshot, RepositoryError> {
        if tenant_id.kind() != ResourceKind::Tenant
            || asserted_principal_kind == PrincipalKind::InstallationOperator
        {
            return Err(RepositoryError::InvalidInput(
                "external principal evidence is invalid".to_owned(),
            ));
        }
        let row = sqlx::query(
            r#"
            SELECT principal.principal_id,
                   principal.version AS principal_version,
                   binding.generation AS binding_generation,
                   binding.version AS binding_version,
                   binding.permissions_schema_version,
                   binding.permissions,
                   binding.permissions_digest
            FROM insight_platform.principals AS principal
            JOIN insight_platform.tenant_principals AS binding
              ON binding.principal_id = principal.principal_id
            WHERE principal.authentication_authority_digest = $1
              AND principal.subject_digest = $2
              AND principal.state = 'active'
              AND binding.tenant_id = $3
              AND binding.principal_kind = $4
              AND binding.state = 'active'
            "#,
        )
        .bind(authentication_authority_digest.as_str())
        .bind(subject_digest.as_str())
        .bind(tenant_id.to_string())
        .bind(asserted_principal_kind.as_str())
        .fetch_optional(self.pool())
        .await?;
        let Some(row) = row else {
            return Err(RepositoryError::PermissionDenied);
        };
        let permissions_payload = payload_from_row(
            &row,
            "permissions_schema_version",
            "permissions",
            "permissions_digest",
        )?;
        let permissions: TenantPrincipalPayload = crate::repository::decode_typed_payload(
            &permissions_payload,
            "tenant principal permissions",
        )?;
        let principal_id: ResourceId = row
            .try_get::<String, _>("principal_id")?
            .parse()
            .map_err(|_| RepositoryError::CorruptRow("invalid principal identity".to_owned()))?;
        let snapshot = PrincipalSnapshot::build(
            tenant_id,
            principal_id,
            asserted_principal_kind,
            permissions.permissions,
            u64::try_from(row.try_get::<i64, _>("principal_version")?).map_err(|_| {
                RepositoryError::CorruptRow("negative principal version".to_owned())
            })?,
            u64::try_from(row.try_get::<i64, _>("binding_generation")?).map_err(|_| {
                RepositoryError::CorruptRow("negative binding generation".to_owned())
            })?,
            u64::try_from(row.try_get::<i64, _>("binding_version")?)
                .map_err(|_| RepositoryError::CorruptRow("negative binding version".to_owned()))?,
        )
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
        if snapshot.permissions_digest.as_str() != permissions_payload.digest {
            return Err(RepositoryError::CorruptRow(
                "tenant principal permissions digest is inconsistent".to_owned(),
            ));
        }
        Ok(snapshot)
    }
}
