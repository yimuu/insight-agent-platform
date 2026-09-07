//! Security lifecycle transactions. All mutations use the caller-owned PostgreSQL transaction.
//! Current authorization precedes Receipt access; idempotency is never permission to read a projection.
use super::*;

/// Shared application of the scheduling binding authority. Callers retain their own audit,
/// Receipt and Tenant transaction; these helpers never grant caller permissions or commit.
pub(super) async fn lock_tenant_scheduler_fairness(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
) -> Result<(), RepositoryError> {
    let rows = sqlx::query("SELECT work_class FROM insight_platform.scheduler_tenant_state WHERE tenant_id=$1 ORDER BY work_class FOR UPDATE")
        .bind(tenant_id.to_string()).fetch_all(&mut **transaction).await?;
    if rows.len() != WorkClass::ALL.len() {
        return Err(RepositoryError::CorruptRow(
            "tenant fairness rows are not provisioned".into(),
        ));
    }
    Ok(())
}

pub(super) async fn load_tenant_scheduler_policy_identity(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    exact: &ExactDeploymentRef,
) -> Result<(ExactVersionRef, Sha256Digest), RepositoryError> {
    let (revision, policy) =
        load_exact_active_policy_deployment(transaction, tenant_id, exact, PolicyKind::Scheduling)
            .await?;
    if policy.policy_kind != PolicyKind::Scheduling || policy.scheduling.is_none() {
        return Err(RepositoryError::InvalidInput(
            "tenant scheduling binding requires a Scheduling policy document".into(),
        ));
    }
    Ok((revision, policy.rules_digest))
}

pub(super) async fn bind_locked_tenant_scheduler_policy(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    exact: &ExactDeploymentRef,
) -> Result<u64, RepositoryError> {
    lock_active_policy_deployment_for_binding(transaction, tenant_id, exact).await?;
    let (revision, rules_digest) =
        load_tenant_scheduler_policy_identity(transaction, tenant_id, exact).await?;
    Ok(sqlx::query("UPDATE insight_platform.scheduler_tenant_state SET policy_version_id=$2,policy_version_digest=$3,rules_digest=$4,deficit=0,credited_round=NULL,version=version+1,updated_at=clock_timestamp() WHERE tenant_id=$1 AND (policy_version_id,policy_version_digest,rules_digest) IS DISTINCT FROM ($2::text,$3::text,$4::text)")
        .bind(tenant_id.to_string()).bind(revision.revision_id.to_string()).bind(revision.semantic_digest.to_string()).bind(rules_digest.to_string())
        .execute(&mut **transaction).await?.rows_affected())
}

impl PgSecurityTransaction {
    pub async fn bind_tenant_principal(
        &mut self,
        command: BindTenantPrincipal,
    ) -> Result<CommandOutcome<TenantPrincipalRecord>, RepositoryError> {
        command.validate_at(Utc::now())?;
        let mut transaction = self.transaction.begin().await?;
        require_tenant_permission(&mut transaction, &command.audit, Permission::TenantManage)
            .await?;
        let scope_kind = tenant_principal_scope_kind(command.principal_kind);
        if claim_command_receipt(
            &mut transaction,
            &command.audit,
            &scope_kind,
            &command.principal_id.to_string(),
            "security.tenant_principal.bind",
        )
        .await?
        {
            let record = load_tenant_principal(
                &mut transaction,
                &command.audit.tenant_id,
                &command.principal_id,
                command.principal_kind,
            )
            .await?;
            transaction.commit().await?;
            return Ok(CommandOutcome::Replayed(record));
        }
        let payload = TypedPayload::with_limit(
            1,
            &TenantPrincipalPayload {
                permissions: command.permissions,
            },
            65_536,
        )?;
        let row = sqlx::query(
            r#"
            INSERT INTO insight_platform.tenant_principals (
                tenant_id, principal_id, principal_kind, state, permissions_schema_version,
                permissions, permissions_digest
            )
            SELECT $1, $2, $3, 'active', $4, $5, $6
            FROM insight_platform.principals AS principal
            JOIN insight_platform.tenants AS tenant
              ON tenant.tenant_id = $1 AND tenant.state = 'active'
            WHERE principal.principal_id = $2 AND principal.state = 'active'
            ON CONFLICT (tenant_id, principal_id, principal_kind) DO NOTHING
            RETURNING tenant_id, principal_id, principal_kind, state, generation, version,
                      permissions_schema_version, permissions, permissions_digest,
                      created_at, updated_at
            "#,
        )
        .bind(command.audit.tenant_id.to_string())
        .bind(command.principal_id.to_string())
        .bind(command.principal_kind.as_str())
        .bind(payload.schema_version)
        .bind(&payload.value)
        .bind(&payload.digest)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(RepositoryError::Conflict("tenant principal"))?;
        let record = tenant_principal_from_row(row)?;
        append_command_event(
            &mut transaction,
            &command.audit,
            &scope_kind,
            &record.principal_id,
            record.version,
            "security.tenant_principal_bound",
            &TypedPayload::new(
                1,
                &serde_json::json!({
                    "generation": record.generation,
                    "principal_kind": record.principal_kind,
                    "permissions_digest": record.permissions.digest,
                }),
            )?,
        )
        .await?;
        terminalize_command_receipt(
            &mut transaction,
            &command.audit,
            &record.principal_id,
            "bound",
        )
        .await?;
        transaction.commit().await?;
        Ok(CommandOutcome::Applied(record))
    }

    pub async fn update_tenant_principal_permissions(
        &mut self,
        command: UpdateTenantPrincipalPermissions,
    ) -> Result<CommandOutcome<TenantPrincipalRecord>, RepositoryError> {
        command.validate_at(Utc::now())?;
        let mut transaction = self.transaction.begin().await?;
        require_tenant_permission(&mut transaction, &command.audit, Permission::TenantManage)
            .await?;
        let scope_kind = tenant_principal_scope_kind(command.principal_kind);
        if claim_command_receipt(
            &mut transaction,
            &command.audit,
            &scope_kind,
            &command.principal_id.to_string(),
            "security.tenant_principal.update_permissions",
        )
        .await?
        {
            let record = load_tenant_principal(
                &mut transaction,
                &command.audit.tenant_id,
                &command.principal_id,
                command.principal_kind,
            )
            .await?;
            transaction.commit().await?;
            return Ok(CommandOutcome::Replayed(record));
        }
        let payload = TypedPayload::with_limit(
            1,
            &TenantPrincipalPayload {
                permissions: command.permissions,
            },
            65_536,
        )?;
        let row = sqlx::query(
            r#"
            UPDATE insight_platform.tenant_principals
            SET permissions_schema_version = $6, permissions = $7, permissions_digest = $8,
                generation = generation + 1, version = version + 1,
                updated_at = clock_timestamp()
            WHERE tenant_id = $1 AND principal_id = $2 AND principal_kind = $3
              AND generation = $4 AND version = $5 AND state = 'active'
            RETURNING tenant_id, principal_id, principal_kind, state, generation, version,
                      permissions_schema_version, permissions, permissions_digest,
                      created_at, updated_at
            "#,
        )
        .bind(command.audit.tenant_id.to_string())
        .bind(command.principal_id.to_string())
        .bind(command.principal_kind.as_str())
        .bind(command.expected_generation)
        .bind(command.expected_version)
        .bind(payload.schema_version)
        .bind(&payload.value)
        .bind(&payload.digest)
        .fetch_optional(&mut *transaction)
        .await?;
        let Some(row) = row else {
            return Err(classify_tenant_principal_cas(
                &mut transaction,
                &command.audit.tenant_id,
                &command.principal_id,
                command.principal_kind,
            )
            .await?);
        };
        let record = tenant_principal_from_row(row)?;
        append_command_event(
            &mut transaction,
            &command.audit,
            &scope_kind,
            &record.principal_id,
            record.version,
            "security.tenant_principal_permissions_updated",
            &TypedPayload::new(
                1,
                &serde_json::json!({
                    "generation": record.generation,
                    "principal_kind": record.principal_kind,
                    "permissions_digest": record.permissions.digest,
                }),
            )?,
        )
        .await?;
        terminalize_command_receipt(
            &mut transaction,
            &command.audit,
            &record.principal_id,
            "updated",
        )
        .await?;
        transaction.commit().await?;
        Ok(CommandOutcome::Applied(record))
    }

    pub async fn revoke_tenant_principal(
        &mut self,
        command: RevokeTenantPrincipal,
    ) -> Result<CommandOutcome<TenantPrincipalRecord>, RepositoryError> {
        command.validate_at(Utc::now())?;
        let mut transaction = self.transaction.begin().await?;
        require_tenant_permission(&mut transaction, &command.audit, Permission::TenantManage)
            .await?;
        let scope_kind = tenant_principal_scope_kind(command.principal_kind);
        if claim_command_receipt(
            &mut transaction,
            &command.audit,
            &scope_kind,
            &command.principal_id.to_string(),
            "security.tenant_principal.revoke",
        )
        .await?
        {
            let record = load_tenant_principal(
                &mut transaction,
                &command.audit.tenant_id,
                &command.principal_id,
                command.principal_kind,
            )
            .await?;
            transaction.commit().await?;
            return Ok(CommandOutcome::Replayed(record));
        }
        let row = sqlx::query(
            r#"
            UPDATE insight_platform.tenant_principals
            SET state = 'revoked', generation = generation + 1, version = version + 1,
                updated_at = clock_timestamp()
            WHERE tenant_id = $1 AND principal_id = $2 AND principal_kind = $3
              AND generation = $4 AND version = $5 AND state = 'active'
            RETURNING tenant_id, principal_id, principal_kind, state, generation, version,
                      permissions_schema_version, permissions, permissions_digest,
                      created_at, updated_at
            "#,
        )
        .bind(command.audit.tenant_id.to_string())
        .bind(command.principal_id.to_string())
        .bind(command.principal_kind.as_str())
        .bind(command.expected_generation)
        .bind(command.expected_version)
        .fetch_optional(&mut *transaction)
        .await?;
        let Some(row) = row else {
            return Err(classify_tenant_principal_cas(
                &mut transaction,
                &command.audit.tenant_id,
                &command.principal_id,
                command.principal_kind,
            )
            .await?);
        };
        let record = tenant_principal_from_row(row)?;
        append_command_event(
            &mut transaction,
            &command.audit,
            &scope_kind,
            &record.principal_id,
            record.version,
            "security.tenant_principal_revoked",
            &TypedPayload::new(
                1,
                &serde_json::json!({
                    "generation": record.generation,
                    "principal_kind": record.principal_kind,
                }),
            )?,
        )
        .await?;
        terminalize_command_receipt(
            &mut transaction,
            &command.audit,
            &record.principal_id,
            "revoked",
        )
        .await?;
        transaction.commit().await?;
        Ok(CommandOutcome::Applied(record))
    }

    pub async fn bind_tenant_scheduling_policy(
        &mut self,
        command: BindTenantSchedulingPolicy,
    ) -> Result<CommandOutcome<TenantRecord>, RepositoryError> {
        command.validate_at(Utc::now())?;
        let mut transaction = self.transaction.begin().await?;
        require_tenant_permission(&mut transaction, &command.audit, Permission::TenantManage)
            .await?;
        lock_tenant_scheduler_fairness(&mut transaction, &command.audit.tenant_id).await?;
        if claim_command_receipt(
            &mut transaction,
            &command.audit,
            "tenant",
            &command.audit.tenant_id.to_string(),
            "security.tenant.bind_scheduling_policy",
        )
        .await?
        {
            let record = load_tenant(&mut transaction, &command.audit.tenant_id).await?;
            transaction.commit().await?;
            return Ok(CommandOutcome::Replayed(record));
        }
        let current = load_tenant_for_update(&mut transaction, &command.audit.tenant_id).await?;
        if current.state != "active" || current.version != command.expected_tenant_version {
            return Err(RepositoryError::Conflict("tenant"));
        }

        let reset_rows = bind_locked_tenant_scheduler_policy(
            &mut transaction,
            &command.audit.tenant_id,
            &command.policy,
        )
        .await?;
        let mut config = current.config.clone();
        config.scheduling_policy = Some(command.policy.clone());
        config
            .validate()
            .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;
        let config = TypedPayload::with_limit(1, &config, 65_536)?;
        let row = sqlx::query(
            r#"
            UPDATE insight_platform.tenants
            SET config_schema_version = $3, config = $4, config_digest = $5,
                version = version + 1, updated_at = clock_timestamp()
            WHERE tenant_id = $1 AND version = $2 AND state = 'active'
            RETURNING tenant_id, state, version, config_schema_version, config,
                      config_digest, created_at, updated_at
            "#,
        )
        .bind(command.audit.tenant_id.to_string())
        .bind(command.expected_tenant_version)
        .bind(config.schema_version)
        .bind(&config.value)
        .bind(&config.digest)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(RepositoryError::Conflict("tenant"))?;
        let record = tenant_from_row(row)?;
        append_command_event(
            &mut transaction,
            &command.audit,
            "tenant",
            &record.tenant_id,
            record.version,
            "security.tenant_scheduling_policy_bound",
            &TypedPayload::new(
                1,
                &serde_json::json!({
                    "scheduler_accounting_reset_rows": reset_rows,
                    "policy_deployment_id": command.policy.deployment_id,
                    "policy_deployment_digest": command.policy.deployment_digest,
                }),
            )?,
        )
        .await?;
        terminalize_command_receipt(&mut transaction, &command.audit, &record.tenant_id, "bound")
            .await?;
        transaction.commit().await?;
        Ok(CommandOutcome::Applied(record))
    }

    pub async fn bind_tenant_artifact_policies(
        &mut self,
        command: BindTenantArtifactPolicies,
    ) -> Result<CommandOutcome<TenantRecord>, RepositoryError> {
        command.validate_at(Utc::now())?;
        let mut transaction = self.transaction.begin().await?;
        require_tenant_permission(&mut transaction, &command.audit, Permission::TenantManage)
            .await?;
        if claim_command_receipt(
            &mut transaction,
            &command.audit,
            "tenant",
            &command.audit.tenant_id.to_string(),
            "security.tenant.bind_artifact_policies",
        )
        .await?
        {
            let record = load_tenant(&mut transaction, &command.audit.tenant_id).await?;
            transaction.commit().await?;
            return Ok(CommandOutcome::Replayed(record));
        }
        let current = load_tenant_for_update(&mut transaction, &command.audit.tenant_id).await?;
        if current.state != "active" || current.version != command.expected_tenant_version {
            return Err(RepositoryError::Conflict("tenant"));
        }
        for (exact, expected_kind) in [
            (&command.retention_policy, PolicyKind::Retention),
            (&command.artifact_io_policy, PolicyKind::ArtifactIo),
        ] {
            lock_active_policy_deployment_for_binding(
                &mut transaction,
                &command.audit.tenant_id,
                exact,
            )
            .await?;
            let (_, policy) = load_exact_active_policy_deployment(
                &mut transaction,
                &command.audit.tenant_id,
                exact,
                expected_kind,
            )
            .await?;
            if policy.policy_kind != expected_kind {
                return Err(RepositoryError::InvalidInput(
                    "tenant Artifact policy binding has the wrong PolicyKind".to_owned(),
                ));
            }
        }
        let mut config = current.config.clone();
        config.artifact_retention_policy = Some(command.retention_policy.clone());
        config.artifact_io_policy = Some(command.artifact_io_policy.clone());
        config
            .validate()
            .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;
        let config = TypedPayload::with_limit(1, &config, 65_536)?;
        let row = sqlx::query(
            r#"
            UPDATE insight_platform.tenants
            SET config_schema_version = $3, config = $4, config_digest = $5,
                version = version + 1, updated_at = clock_timestamp()
            WHERE tenant_id = $1 AND version = $2 AND state = 'active'
            RETURNING tenant_id, state, version, config_schema_version, config,
                      config_digest, created_at, updated_at
            "#,
        )
        .bind(command.audit.tenant_id.to_string())
        .bind(command.expected_tenant_version)
        .bind(config.schema_version)
        .bind(&config.value)
        .bind(&config.digest)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(RepositoryError::Conflict("tenant"))?;
        let record = tenant_from_row(row)?;
        append_command_event(
            &mut transaction,
            &command.audit,
            "tenant",
            &record.tenant_id,
            record.version,
            "security.tenant_artifact_policies_bound",
            &TypedPayload::new(
                1,
                &serde_json::json!({
                    "artifact_io_policy_deployment_id": command.artifact_io_policy.deployment_id,
                    "artifact_io_policy_deployment_digest": command.artifact_io_policy.deployment_digest,
                    "retention_policy_deployment_id": command.retention_policy.deployment_id,
                    "retention_policy_deployment_digest": command.retention_policy.deployment_digest,
                }),
            )?,
        )
        .await?;
        terminalize_command_receipt(&mut transaction, &command.audit, &record.tenant_id, "bound")
            .await?;
        transaction.commit().await?;
        Ok(CommandOutcome::Applied(record))
    }

    pub async fn create_secret_binding(
        &mut self,
        command: CreateSecretBindingCommand,
    ) -> Result<CommandOutcome<SecretBindingMetadataRecord>, RepositoryError> {
        command.validate_at(Utc::now())?;
        let mut transaction = self.transaction.begin().await?;
        require_tenant_permission(&mut transaction, &command.audit, Permission::SecretBind).await?;
        if claim_command_receipt(
            &mut transaction,
            &command.audit,
            "secret_binding",
            &command.secret_binding_id.to_string(),
            "security.secret_binding.create",
        )
        .await?
        {
            let record = load_secret_binding_metadata(
                &mut transaction,
                &command.audit.tenant_id,
                &command.secret_binding_id,
            )
            .await?;
            transaction.commit().await?;
            return Ok(CommandOutcome::Replayed(record));
        }
        let payload = TypedPayload::with_limit(1, &command.payload, 65_536)?;
        let row = sqlx::query(
            r#"
            INSERT INTO insight_platform.secret_bindings (
                tenant_id, secret_binding_id, purpose, provider, state,
                opaque_reference_ciphertext, key_id, reference_digest,
                payload_schema_version, payload, payload_digest
            ) VALUES ($1, $2, $3, $4, 'active', $5, $6, $7, $8, $9, $10)
            ON CONFLICT (tenant_id, secret_binding_id) DO NOTHING
            RETURNING tenant_id, secret_binding_id, purpose, provider, state, generation,
                      version, payload_schema_version, payload, payload_digest,
                      created_at, updated_at, revoked_at
            "#,
        )
        .bind(command.audit.tenant_id.to_string())
        .bind(command.secret_binding_id.to_string())
        .bind(command.purpose.as_str())
        .bind(command.payload.provider_id.to_string())
        .bind(command.encrypted_reference.as_bytes())
        .bind(&command.key_id)
        .bind(command.reference_digest.to_string())
        .bind(payload.schema_version)
        .bind(&payload.value)
        .bind(&payload.digest)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(RepositoryError::Conflict("secret binding"))?;
        let record = secret_binding_metadata_from_row(row)?;
        append_secret_binding_event(
            &mut transaction,
            &command.audit,
            &record,
            "security.secret_binding_created",
            None,
        )
        .await?;
        terminalize_command_receipt(
            &mut transaction,
            &command.audit,
            &record.secret_binding_id,
            "created",
        )
        .await?;
        transaction.commit().await?;
        Ok(CommandOutcome::Applied(record))
    }

    pub async fn rotate_secret_binding(
        &mut self,
        command: RotateSecretBinding,
    ) -> Result<CommandOutcome<SecretBindingMetadataRecord>, RepositoryError> {
        command.validate_at(Utc::now())?;
        let mut transaction = self.transaction.begin().await?;
        require_tenant_permission(&mut transaction, &command.audit, Permission::SecretRotate)
            .await?;
        if claim_command_receipt(
            &mut transaction,
            &command.audit,
            "secret_binding",
            &command.secret_binding_id.to_string(),
            "security.secret_binding.rotate",
        )
        .await?
        {
            let record = load_secret_binding_metadata(
                &mut transaction,
                &command.audit.tenant_id,
                &command.secret_binding_id,
            )
            .await?;
            transaction.commit().await?;
            return Ok(CommandOutcome::Replayed(record));
        }
        let current = load_secret_binding_metadata_for_update(
            &mut transaction,
            &command.audit.tenant_id,
            &command.secret_binding_id,
        )
        .await?;
        if current.state != "active"
            || current.generation != command.expected_generation
            || current.version != command.expected_version
            || current.provider_id != command.payload.provider_id.to_string()
        {
            return Err(RepositoryError::Conflict("secret binding"));
        }
        let payload = TypedPayload::with_limit(1, &command.payload, 65_536)?;
        let row = sqlx::query(
            r#"
            UPDATE insight_platform.secret_bindings
            SET opaque_reference_ciphertext = $5, key_id = $6, reference_digest = $7,
                payload_schema_version = $8, payload = $9, payload_digest = $10,
                generation = generation + 1, version = version + 1,
                updated_at = clock_timestamp()
            WHERE tenant_id = $1 AND secret_binding_id = $2
              AND generation = $3 AND version = $4 AND state = 'active'
            RETURNING tenant_id, secret_binding_id, purpose, provider, state, generation,
                      version, payload_schema_version, payload, payload_digest,
                      created_at, updated_at, revoked_at
            "#,
        )
        .bind(command.audit.tenant_id.to_string())
        .bind(command.secret_binding_id.to_string())
        .bind(command.expected_generation)
        .bind(command.expected_version)
        .bind(command.encrypted_reference.as_bytes())
        .bind(&command.key_id)
        .bind(command.reference_digest.to_string())
        .bind(payload.schema_version)
        .bind(&payload.value)
        .bind(&payload.digest)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(RepositoryError::Conflict("secret binding"))?;
        let record = secret_binding_metadata_from_row(row)?;
        append_secret_binding_event(
            &mut transaction,
            &command.audit,
            &record,
            "security.secret_binding_rotated",
            Some(&command.provider_evidence_digest),
        )
        .await?;
        terminalize_command_receipt(
            &mut transaction,
            &command.audit,
            &record.secret_binding_id,
            "rotated",
        )
        .await?;
        transaction.commit().await?;
        Ok(CommandOutcome::Applied(record))
    }

    pub async fn revoke_secret_binding(
        &mut self,
        command: RevokeSecretBinding,
    ) -> Result<CommandOutcome<SecretBindingMetadataRecord>, RepositoryError> {
        command.validate_at(Utc::now())?;
        let mut transaction = self.transaction.begin().await?;
        require_tenant_permission(&mut transaction, &command.audit, Permission::SecretRevoke)
            .await?;
        if claim_command_receipt(
            &mut transaction,
            &command.audit,
            "secret_binding",
            &command.secret_binding_id.to_string(),
            "security.secret_binding.revoke",
        )
        .await?
        {
            let record = load_secret_binding_metadata(
                &mut transaction,
                &command.audit.tenant_id,
                &command.secret_binding_id,
            )
            .await?;
            transaction.commit().await?;
            return Ok(CommandOutcome::Replayed(record));
        }
        let row = sqlx::query(
            r#"
            UPDATE insight_platform.secret_bindings
            SET state = 'revoked', generation = generation + 1, version = version + 1,
                updated_at = clock_timestamp(), revoked_at = clock_timestamp()
            WHERE tenant_id = $1 AND secret_binding_id = $2
              AND generation = $3 AND version = $4 AND state = 'active'
            RETURNING tenant_id, secret_binding_id, purpose, provider, state, generation,
                      version, payload_schema_version, payload, payload_digest,
                      created_at, updated_at, revoked_at
            "#,
        )
        .bind(command.audit.tenant_id.to_string())
        .bind(command.secret_binding_id.to_string())
        .bind(command.expected_generation)
        .bind(command.expected_version)
        .fetch_optional(&mut *transaction)
        .await?;
        let Some(row) = row else {
            return Err(classify_secret_binding_cas(
                &mut transaction,
                &command.audit.tenant_id,
                &command.secret_binding_id,
            )
            .await?);
        };
        let record = secret_binding_metadata_from_row(row)?;
        append_secret_binding_event(
            &mut transaction,
            &command.audit,
            &record,
            "security.secret_binding_revoked",
            None,
        )
        .await?;
        terminalize_command_receipt(
            &mut transaction,
            &command.audit,
            &record.secret_binding_id,
            "revoked",
        )
        .await?;
        transaction.commit().await?;
        Ok(CommandOutcome::Applied(record))
    }

    pub async fn commit(self) -> Result<(), RepositoryError> {
        self.transaction.commit().await?;
        Ok(())
    }

    pub async fn rollback(self) -> Result<(), RepositoryError> {
        self.transaction.rollback().await?;
        Ok(())
    }
}

impl SecurityTransaction for PgSecurityTransaction {
    type Error = RepositoryError;
    type TenantRecord = TenantRecord;
    type TenantPrincipalRecord = TenantPrincipalRecord;
    type SecretBindingRecord = SecretBindingMetadataRecord;

    async fn bind_tenant_principal(
        &mut self,
        command: BindTenantPrincipal,
    ) -> Result<CommandOutcome<Self::TenantPrincipalRecord>, Self::Error> {
        PgSecurityTransaction::bind_tenant_principal(self, command).await
    }

    async fn update_tenant_principal_permissions(
        &mut self,
        command: UpdateTenantPrincipalPermissions,
    ) -> Result<CommandOutcome<Self::TenantPrincipalRecord>, Self::Error> {
        PgSecurityTransaction::update_tenant_principal_permissions(self, command).await
    }

    async fn revoke_tenant_principal(
        &mut self,
        command: RevokeTenantPrincipal,
    ) -> Result<CommandOutcome<Self::TenantPrincipalRecord>, Self::Error> {
        PgSecurityTransaction::revoke_tenant_principal(self, command).await
    }

    async fn bind_tenant_scheduling_policy(
        &mut self,
        command: BindTenantSchedulingPolicy,
    ) -> Result<CommandOutcome<Self::TenantRecord>, Self::Error> {
        PgSecurityTransaction::bind_tenant_scheduling_policy(self, command).await
    }

    async fn bind_tenant_artifact_policies(
        &mut self,
        command: BindTenantArtifactPolicies,
    ) -> Result<CommandOutcome<Self::TenantRecord>, Self::Error> {
        PgSecurityTransaction::bind_tenant_artifact_policies(self, command).await
    }

    async fn create_secret_binding(
        &mut self,
        command: CreateSecretBindingCommand,
    ) -> Result<CommandOutcome<Self::SecretBindingRecord>, Self::Error> {
        PgSecurityTransaction::create_secret_binding(self, command).await
    }

    async fn rotate_secret_binding(
        &mut self,
        command: RotateSecretBinding,
    ) -> Result<CommandOutcome<Self::SecretBindingRecord>, Self::Error> {
        PgSecurityTransaction::rotate_secret_binding(self, command).await
    }

    async fn revoke_secret_binding(
        &mut self,
        command: RevokeSecretBinding,
    ) -> Result<CommandOutcome<Self::SecretBindingRecord>, Self::Error> {
        PgSecurityTransaction::revoke_secret_binding(self, command).await
    }

    async fn commit(self) -> Result<(), Self::Error> {
        PgSecurityTransaction::commit(self).await
    }

    async fn rollback(self) -> Result<(), Self::Error> {
        PgSecurityTransaction::rollback(self).await
    }
}
