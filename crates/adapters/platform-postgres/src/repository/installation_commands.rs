//! PostgreSQL installation commands. Shared locks and atomicity remain in this adapter.
use super::*;

impl PgRepository {
    pub async fn bootstrap_installation_operator(
        &self,
        command: BootstrapInstallationOperator,
    ) -> Result<BootstrapOutcome, RepositoryError> {
        if command.principal_id.kind() != ResourceKind::Principal
            || command.request_id.kind() != ResourceKind::ServerRequest
        {
            return Err(RepositoryError::InvalidInput(
                "bootstrap principal or request ID has the wrong kind".to_owned(),
            ));
        }
        let bindings = PrincipalBindingsPayload {
            installation_bindings: vec![InstallationPrincipalBinding {
                principal_kind: PrincipalKind::InstallationOperator,
                permissions: PermissionSet::new(vec![
                    Permission::InstallationManage,
                    Permission::InstallationSupport,
                ])
                .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?,
                state: PrincipalBindingState::Active,
                generation: 1,
            }],
        };
        bindings
            .validate()
            .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;
        let principal_payload = TypedPayload::with_limit(1, &bindings, 65_536)?;
        let event_payload = TypedPayload::with_limit(
            1,
            &serde_json::json!({
                "authentication_authority_digest": command.authentication_authority_digest.as_str(),
                "evidence_digest": command.evidence_digest.as_str(),
                "principal_id": command.principal_id.to_string(),
                "request_id": command.request_id.to_string(),
                "subject_digest": command.subject_digest.as_str(),
            }),
            65_536,
        )?;
        let event_id = format!("evt_{}", command.request_id.uuid().hyphenated());
        let trace = TraceIdentityV1::generate();
        let mut transaction = self.pool.begin().await?;
        sqlx::query("SELECT pg_advisory_xact_lock($1)")
            .bind(0x4950_424f_4f54_5354_i64)
            .execute(&mut *transaction)
            .await?;
        sqlx::query(
            "LOCK TABLE insight_platform.principals, insight_platform.tenant_principals IN SHARE ROW EXCLUSIVE MODE",
        )
        .execute(&mut *transaction)
        .await?;

        let principal_count: i64 =
            sqlx::query_scalar("SELECT count(*) FROM insight_platform.principals")
                .fetch_one(&mut *transaction)
                .await?;
        let tenant_binding_count: i64 =
            sqlx::query_scalar("SELECT count(*) FROM insight_platform.tenant_principals")
                .fetch_one(&mut *transaction)
                .await?;
        if principal_count == 0 && tenant_binding_count == 0 {
            sqlx::query(
                r#"
                INSERT INTO insight_platform.principals (
                    principal_id, state, authentication_authority_digest, subject_digest,
                    payload_schema_version, payload, payload_digest
                ) VALUES ($1, 'active', $2, $3, $4, $5, $6)
                "#,
            )
            .bind(command.principal_id.to_string())
            .bind(command.authentication_authority_digest.to_string())
            .bind(command.subject_digest.to_string())
            .bind(principal_payload.schema_version)
            .bind(&principal_payload.value)
            .bind(&principal_payload.digest)
            .execute(&mut *transaction)
            .await?;
            sqlx::query(
                r#"
                INSERT INTO insight_platform.events (
                    tenant_id, event_id, aggregate_kind, aggregate_id, aggregate_version,
                    trace_id, event_type, visibility, payload_schema_version, payload,
                    payload_digest
                ) VALUES (NULL, $1, 'principal', $2, 1, $3, 'installation.bootstrap',
                          'internal', $4, $5, $6)
                "#,
            )
            .bind(&event_id)
            .bind(command.principal_id.to_string())
            .bind(trace.trace_id.to_string())
            .bind(event_payload.schema_version)
            .bind(&event_payload.value)
            .bind(&event_payload.digest)
            .execute(&mut *transaction)
            .await?;
            transaction.commit().await?;
            return Ok(BootstrapOutcome::Created);
        }

        if principal_count != 1 || tenant_binding_count != 0 {
            return Err(RepositoryError::Conflict("installation bootstrap"));
        }
        let row = sqlx::query(
            r#"
            SELECT principal_id, state, authentication_authority_digest, subject_digest,
                   version, payload_schema_version, payload, payload_digest,
                   created_at, updated_at
            FROM insight_platform.principals
            FOR UPDATE
            "#,
        )
        .fetch_one(&mut *transaction)
        .await?;
        let stored = principal_from_row(row)?;
        let stored_event_digest: Option<String> = sqlx::query_scalar(
            r#"
            SELECT payload_digest FROM insight_platform.events
            WHERE tenant_id IS NULL AND event_id = $1 AND aggregate_kind = 'principal'
              AND aggregate_id = $2 AND aggregate_version = 1
              AND event_type = 'installation.bootstrap'
            "#,
        )
        .bind(&event_id)
        .bind(command.principal_id.to_string())
        .fetch_optional(&mut *transaction)
        .await?;
        if stored.principal_id != command.principal_id.to_string()
            || stored.state != "active"
            || stored.version != 1
            || stored.authentication_authority_digest
                != command.authentication_authority_digest.to_string()
            || stored.subject_digest != command.subject_digest.to_string()
            || stored.payload.digest != principal_payload.digest
            || stored_event_digest.as_deref() != Some(event_payload.digest.as_str())
        {
            return Err(RepositoryError::Conflict("installation bootstrap"));
        }
        transaction.commit().await?;
        Ok(BootstrapOutcome::Replayed)
    }

    /// Bootstraps every development identity row as one fresh-database transaction.
    ///
    /// This is intentionally separate from the idempotent installation-operator bootstrap: a
    /// development profile needs its tenant and developer bindings to appear atomically with the
    /// operator, rather than leave a partially initialized authority after a process failure.
    pub async fn bootstrap_development_profile(
        &self,
        command: BootstrapDevelopmentProfile,
    ) -> Result<BootstrapOutcome, RepositoryError> {
        validate_development_bootstrap(&command)?;
        let installation_bindings = PrincipalBindingsPayload {
            installation_bindings: vec![InstallationPrincipalBinding {
                principal_kind: PrincipalKind::InstallationOperator,
                permissions: PermissionSet::new(vec![
                    Permission::InstallationManage,
                    Permission::InstallationSupport,
                ])
                .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?,
                state: PrincipalBindingState::Active,
                generation: 1,
            }],
        };
        installation_bindings
            .validate()
            .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;
        command
            .developer
            .installation_bindings
            .validate()
            .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;
        for principal in &command.service_principals {
            principal
                .installation_bindings
                .validate()
                .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;
        }
        command
            .tenant
            .config
            .validate()
            .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;
        let tenant_id = ResourceId::parse_expected(&command.tenant.tenant_id, ResourceKind::Tenant)
            .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;
        let artifact_authority = command
            .artifact_authority
            .as_ref()
            .map(|seed| development_artifact_authority_material(&tenant_id, seed))
            .transpose()?;
        let effective_tenant_config = artifact_authority
            .as_ref()
            .map_or(&command.tenant.config, |material| &material.tenant_config);
        let installation_payload = TypedPayload::with_limit(1, &installation_bindings, 65_536)?;
        let installation_event = TypedPayload::with_limit(
            1,
            &serde_json::json!({
                "authentication_authority_digest": command.installation.authentication_authority_digest.as_str(),
                "evidence_digest": command.installation.evidence_digest.as_str(),
                "principal_id": command.installation.principal_id.to_string(),
                "request_id": command.installation.request_id.to_string(),
                "subject_digest": command.installation.subject_digest.as_str(),
            }),
            65_536,
        )?;
        let tenant_config = TypedPayload::with_limit(1, effective_tenant_config, 65_536)?;
        let developer_payload =
            TypedPayload::with_limit(1, &command.developer.installation_bindings, 65_536)?;
        let service_principal_payloads = command
            .service_principals
            .iter()
            .map(|principal| TypedPayload::with_limit(1, &principal.installation_bindings, 65_536))
            .collect::<Result<Vec<_>, _>>()?;
        let tenant_bindings = command
            .tenant_principal_bindings
            .iter()
            .map(|binding| TypedPayload::with_limit(1, &binding.payload, 65_536))
            .collect::<Result<Vec<_>, _>>()?;
        let event_id = format!(
            "evt_{}",
            command.installation.request_id.uuid().hyphenated()
        );
        let trace = TraceIdentityV1::generate();
        let mut transaction = self.pool.begin().await?;
        sqlx::query("SELECT pg_advisory_xact_lock($1)")
            .bind(0x4950_4445_5642_4f4f_i64)
            .execute(&mut *transaction)
            .await?;
        sqlx::query(
            "LOCK TABLE insight_platform.tenants, insight_platform.principals, \
             insight_platform.tenant_principals, insight_platform.events IN SHARE ROW EXCLUSIVE MODE",
        )
        .execute(&mut *transaction)
        .await?;
        let state_count: i64 = sqlx::query_scalar(
            "SELECT (SELECT count(*) FROM insight_platform.tenants) + \
                    (SELECT count(*) FROM insight_platform.principals) + \
                    (SELECT count(*) FROM insight_platform.tenant_principals) + \
                    (SELECT count(*) FROM insight_platform.events)",
        )
        .fetch_one(&mut *transaction)
        .await?;
        if state_count != 0 {
            verify_development_profile_replay(
                &mut transaction,
                &command,
                DevelopmentReplayEvidence {
                    installation_payload: &installation_payload,
                    installation_event: &installation_event,
                    tenant_config: &tenant_config,
                    developer_payload: &developer_payload,
                    service_principal_payloads: &service_principal_payloads,
                    tenant_bindings: &tenant_bindings,
                    artifact_authority: artifact_authority.as_ref(),
                },
            )
            .await?;
            transaction.commit().await?;
            return Ok(BootstrapOutcome::Replayed);
        }
        sqlx::query(
            r#"
            INSERT INTO insight_platform.principals (
                principal_id, state, authentication_authority_digest, subject_digest,
                payload_schema_version, payload, payload_digest
            ) VALUES ($1, 'active', $2, $3, $4, $5, $6)
            "#,
        )
        .bind(command.installation.principal_id.to_string())
        .bind(
            command
                .installation
                .authentication_authority_digest
                .to_string(),
        )
        .bind(command.installation.subject_digest.to_string())
        .bind(installation_payload.schema_version)
        .bind(&installation_payload.value)
        .bind(&installation_payload.digest)
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            r#"
            INSERT INTO insight_platform.events (
                tenant_id, event_id, aggregate_kind, aggregate_id, aggregate_version,
                trace_id, event_type, visibility, payload_schema_version, payload,
                payload_digest
            ) VALUES (NULL, $1, 'principal', $2, 1, $3, 'installation.bootstrap',
                      'internal', $4, $5, $6)
            "#,
        )
        .bind(&event_id)
        .bind(command.installation.principal_id.to_string())
        .bind(trace.trace_id.to_string())
        .bind(installation_event.schema_version)
        .bind(&installation_event.value)
        .bind(&installation_event.digest)
        .execute(&mut *transaction)
        .await?;
        let bootstrap_tenant_id = tenant_id.clone();
        let enrollment = crate::partition_scheduler::lock_tenant_enrollment(
            &mut transaction,
            &bootstrap_tenant_id,
        )
        .await?;
        sqlx::query(
            r#"
            INSERT INTO insight_platform.tenants (
                tenant_id, state, config_schema_version, config, config_digest, scheduler_partition_id
            ) VALUES ($1, $2, $3, $4, $5, $6)
            "#,
        )
        .bind(&command.tenant.tenant_id)
        .bind(&command.tenant.state)
        .bind(tenant_config.schema_version)
        .bind(&tenant_config.value)
        .bind(&tenant_config.digest)
        .bind(i16::from(insight_platform_contracts::SchedulerPartitionId::for_tenant(&bootstrap_tenant_id).map_err(|error|RepositoryError::InvalidInput(error.to_string()))?.0))
        .execute(&mut *transaction)
        .await?;
        crate::partition_scheduler::provision_tenant_fairness(
            &mut transaction,
            &bootstrap_tenant_id,
            &enrollment,
        )
        .await?;
        sqlx::query(
            r#"
            INSERT INTO insight_platform.principals (
                principal_id, state, authentication_authority_digest, subject_digest,
                payload_schema_version, payload, payload_digest
            ) VALUES ($1, 'active', $2, $3, $4, $5, $6)
            "#,
        )
        .bind(command.developer.principal_id.to_string())
        .bind(
            command
                .developer
                .authentication_authority_digest
                .to_string(),
        )
        .bind(command.developer.subject_digest.to_string())
        .bind(developer_payload.schema_version)
        .bind(&developer_payload.value)
        .bind(&developer_payload.digest)
        .execute(&mut *transaction)
        .await?;
        for (principal, payload) in command
            .service_principals
            .iter()
            .zip(service_principal_payloads.iter())
        {
            sqlx::query(
                r#"
                INSERT INTO insight_platform.principals (
                    principal_id, state, authentication_authority_digest, subject_digest,
                    payload_schema_version, payload, payload_digest
                ) VALUES ($1, 'active', $2, $3, $4, $5, $6)
                "#,
            )
            .bind(principal.principal_id.to_string())
            .bind(principal.authentication_authority_digest.to_string())
            .bind(principal.subject_digest.to_string())
            .bind(payload.schema_version)
            .bind(&payload.value)
            .bind(&payload.digest)
            .execute(&mut *transaction)
            .await?;
        }
        for (binding, payload) in command
            .tenant_principal_bindings
            .iter()
            .zip(tenant_bindings.iter())
        {
            sqlx::query(
                r#"
                INSERT INTO insight_platform.tenant_principals (
                    tenant_id, principal_id, principal_kind, state, permissions_schema_version,
                    permissions, permissions_digest
                ) VALUES ($1, $2, $3, 'active', $4, $5, $6)
                "#,
            )
            .bind(binding.tenant_id.to_string())
            .bind(binding.principal_id.to_string())
            .bind(binding.principal_kind.as_str())
            .bind(payload.schema_version)
            .bind(&payload.value)
            .bind(&payload.digest)
            .execute(&mut *transaction)
            .await?;
        }
        if let (Some(seed), Some(material)) = (&command.artifact_authority, &artifact_authority) {
            insert_development_artifact_authority(
                &mut transaction,
                &tenant_id,
                &command.developer.principal_id,
                seed,
                material,
            )
            .await?;
            // Enrollment and the real published-policy binding become visible together.
            let policy = material
                .tenant_config
                .scheduling_policy
                .as_ref()
                .ok_or_else(|| {
                    RepositoryError::CorruptRow("development Scheduling policy is absent".into())
                })?;
            security_commands::lock_tenant_scheduler_fairness(&mut transaction, &tenant_id).await?;
            security_commands::bind_locked_tenant_scheduler_policy(
                &mut transaction,
                &tenant_id,
                policy,
            )
            .await?;
        }
        transaction.commit().await?;
        Ok(BootstrapOutcome::Created)
    }

    pub async fn create_tenant(&self, command: NewTenant) -> Result<TenantRecord, RepositoryError> {
        validate_id(&command.tenant_id)?;
        validate_code("tenant state", &command.state)?;
        command
            .config
            .validate()
            .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;
        if command.config.scheduling_policy.is_some() {
            return Err(RepositoryError::InvalidInput(
                "tenant scheduling policy must be bound after its ResourceVersion is published"
                    .to_owned(),
            ));
        }
        let config = TypedPayload::with_limit(1, &command.config, 65_536)?;
        let tenant_id: ResourceId = command
            .tenant_id
            .parse::<ResourceId>()
            .map_err(|error| RepositoryError::InvalidInput(error.to_string()))?;
        let partition = insight_platform_contracts::SchedulerPartitionId::for_tenant(&tenant_id)
            .map_err(|error| RepositoryError::InvalidInput(error.to_string()))?;
        let mut transaction = self.pool.begin().await?;
        let enrollment =
            crate::partition_scheduler::lock_tenant_enrollment(&mut transaction, &tenant_id)
                .await?;
        let row = sqlx::query(
            r#"
            INSERT INTO insight_platform.tenants (
                tenant_id, state, config_schema_version, config, config_digest, scheduler_partition_id
            ) VALUES ($1, $2, $3, $4, $5, $6)
            RETURNING tenant_id, state, version, config_schema_version, config,
                      config_digest, created_at, updated_at
            "#,
        )
        .bind(command.tenant_id)
        .bind(command.state)
        .bind(config.schema_version)
        .bind(config.value)
        .bind(config.digest)
        .bind(i16::from(partition.0))
        .fetch_one(&mut *transaction)
        .await?;
        let record = tenant_from_row(row)?;
        crate::partition_scheduler::provision_tenant_fairness(
            &mut transaction,
            &tenant_id,
            &enrollment,
        )
        .await?;
        transaction.commit().await?;
        Ok(record)
    }

    pub async fn create_principal(
        &self,
        command: NewPrincipal,
    ) -> Result<PrincipalRecord, RepositoryError> {
        if command.principal_id.kind() != ResourceKind::Principal {
            return Err(RepositoryError::InvalidInput(
                "principal ID has the wrong kind".to_owned(),
            ));
        }
        command
            .installation_bindings
            .validate()
            .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;
        let payload = TypedPayload::with_limit(1, &command.installation_bindings, 65_536)?;
        let row = sqlx::query(
            r#"
            INSERT INTO insight_platform.principals (
                principal_id, state, authentication_authority_digest, subject_digest,
                payload_schema_version, payload, payload_digest
            ) VALUES ($1, 'active', $2, $3, $4, $5, $6)
            RETURNING principal_id, state, authentication_authority_digest, subject_digest,
                      version, payload_schema_version, payload, payload_digest,
                      created_at, updated_at
            "#,
        )
        .bind(command.principal_id.to_string())
        .bind(command.authentication_authority_digest.to_string())
        .bind(command.subject_digest.to_string())
        .bind(payload.schema_version)
        .bind(payload.value)
        .bind(payload.digest)
        .fetch_one(&self.pool)
        .await?;
        principal_from_row(row)
    }

    pub async fn bind_tenant_principal(
        &self,
        command: NewTenantPrincipal,
    ) -> Result<TenantPrincipalRecord, RepositoryError> {
        if command.tenant_id.kind() != ResourceKind::Tenant
            || command.principal_id.kind() != ResourceKind::Principal
            || command.principal_kind == PrincipalKind::InstallationOperator
        {
            return Err(RepositoryError::InvalidInput(
                "tenant principal identity or kind is invalid".to_owned(),
            ));
        }
        let payload = TypedPayload::with_limit(1, &command.payload, 65_536)?;
        let row = sqlx::query(
            r#"
            INSERT INTO insight_platform.tenant_principals (
                tenant_id, principal_id, principal_kind, state, permissions_schema_version,
                permissions, permissions_digest
            )
            SELECT $1, $2, $3, 'active', $4, $5, $6
            FROM insight_platform.principals
            WHERE principal_id = $2 AND state = 'active'
            RETURNING tenant_id, principal_id, principal_kind, state, generation, version,
                      permissions_schema_version, permissions, permissions_digest,
                      created_at, updated_at
            "#,
        )
        .bind(command.tenant_id.to_string())
        .bind(command.principal_id.to_string())
        .bind(command.principal_kind.as_str())
        .bind(payload.schema_version)
        .bind(payload.value)
        .bind(payload.digest)
        .fetch_optional(&self.pool)
        .await?;
        row.map(tenant_principal_from_row)
            .transpose()?
            .ok_or(RepositoryError::NotFound("active principal"))
    }

    pub async fn create_secret_binding(
        &self,
        command: NewSecretBinding,
    ) -> Result<SecretBindingRecord, RepositoryError> {
        if command.tenant_id.kind() != ResourceKind::Tenant
            || command.secret_binding_id.kind() != ResourceKind::SecretBinding
        {
            return Err(RepositoryError::InvalidInput(
                "secret binding identity is invalid".to_owned(),
            ));
        }
        command
            .payload
            .validate()
            .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;
        if command.payload.provider_id != command.provider_id
            || command.opaque_reference_ciphertext.is_empty()
            || command.opaque_reference_ciphertext.len() > 16_384
            || command.key_id.is_empty()
            || command.key_id.len() > 255
        {
            return Err(RepositoryError::InvalidInput(
                "secret binding metadata is inconsistent or unbounded".to_owned(),
            ));
        }
        let payload = TypedPayload::with_limit(1, &command.payload, 65_536)?;
        let row = sqlx::query(
            r#"
            INSERT INTO insight_platform.secret_bindings (
                tenant_id, secret_binding_id, purpose, provider, state,
                opaque_reference_ciphertext, key_id, reference_digest,
                payload_schema_version, payload, payload_digest
            ) VALUES ($1, $2, $3, $4, 'active', $5, $6, $7, $8, $9, $10)
            RETURNING tenant_id, secret_binding_id, purpose, provider, state, generation,
                      version, opaque_reference_ciphertext, key_id, reference_digest,
                      payload_schema_version, payload, payload_digest,
                      created_at, updated_at, revoked_at
            "#,
        )
        .bind(command.tenant_id.to_string())
        .bind(command.secret_binding_id.to_string())
        .bind(command.purpose.as_str())
        .bind(command.provider_id.to_string())
        .bind(command.opaque_reference_ciphertext)
        .bind(command.key_id)
        .bind(command.reference_digest.to_string())
        .bind(payload.schema_version)
        .bind(payload.value)
        .bind(payload.digest)
        .fetch_one(&self.pool)
        .await?;
        secret_binding_from_row(row)
    }
}
