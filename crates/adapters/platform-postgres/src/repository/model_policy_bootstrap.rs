//! One-shot Policy bootstrap, followed by read-only verification of immutable installation roots.
use super::*;
use insight_platform_artifacts::{ArtifactMetadataSnapshot, ArtifactReferenceSnapshot};
use insight_platform_contracts::{
    ArtifactPurpose, ArtifactReferenceKind, ModelBootstrapPolicyIdentityV1,
    ModelPolicyArtifactMaterialV1, ModelPolicyBootstrapSeedV1, QuotaDimension,
    INITIAL_MODEL_CONCURRENCY_LIMIT,
};
use insight_platform_registry::model_policy_bootstrap::{
    build_model_policy_bootstrap, ModelPolicyBootstrapMaterial,
};

fn invalid() -> RepositoryError {
    RepositoryError::InvalidInput("model Policy bootstrap is invalid".to_owned())
}
fn conflict() -> RepositoryError {
    RepositoryError::Conflict("model Policy bootstrap immutable material")
}
fn installation_event(
    command: &BootstrapInstallationOperator,
) -> Result<TypedPayload, RepositoryError> {
    Ok(TypedPayload::with_limit(
        1,
        &serde_json::json!({
        "authentication_authority_digest":command.authentication_authority_digest,
        "evidence_digest":command.evidence_digest,"principal_id":command.principal_id,
        "request_id":command.request_id,"subject_digest":command.subject_digest}),
        65_536,
    )?)
}
fn model_event(
    seed: &ModelPolicyBootstrapSeedV1,
    material: &ModelPolicyArtifactMaterialV1,
) -> Result<TypedPayload, RepositoryError> {
    Ok(TypedPayload::new(
        1,
        &serde_json::json!({"seed_digest":seed.canonical_digest().map_err(|_|invalid())?,
        "declaration_digest":material.content_digest,"artifact_id":seed.authoring_artifact_id,
        "blob_id":seed.authoring_blob_id,"backend_evidence_digest":material.backend_evidence_digest,
        "model_quota_account_id":seed.model_quota_account_id,"initial_model_concurrency":INITIAL_MODEL_CONCURRENCY_LIMIT}),
    )?)
}
fn artifact_metadata(
    seed: &ModelPolicyBootstrapSeedV1,
    built: &ModelPolicyBootstrapMaterial,
) -> Result<TypedPayload, RepositoryError> {
    let metadata = ArtifactMetadataSnapshot::new_installation(
        built.authoring_artifact.display_name().map(str::to_owned),
        seed.request_id.clone(),
    )
    .map_err(|_| invalid())?;
    Ok(TypedPayload::from_versioned(
        metadata.schema_version as i32,
        &metadata,
        65_536,
    )?)
}
fn policy_reference(
    seed: &ModelPolicyBootstrapSeedV1,
    policy: &ModelBootstrapPolicyIdentityV1,
) -> ArtifactReferenceSnapshot {
    ArtifactReferenceSnapshot {
        schema_version: 1,
        artifact_id: seed.authoring_artifact_id.clone(),
        owner_id: policy.revision_id.clone(),
        reference_kind: ArtifactReferenceKind::Definition,
        purpose: ArtifactPurpose::AuthoringDocument,
        created_by: seed.created_by.clone(),
    }
}
async fn require_policy_reference(
    tx: &mut Transaction<'_, Postgres>,
    seed: &ModelPolicyBootstrapSeedV1,
    policy: &ModelBootstrapPolicyIdentityV1,
) -> Result<(), RepositoryError> {
    let reference = policy_reference(seed, policy);
    let payload = TypedPayload::from_versioned(1, &reference, 65_536)?;
    // A legitimate release remains released. Verify immutable ownership without repairing lifecycle.
    let exact:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM insight_platform.artifact_links WHERE tenant_id=$1 AND artifact_link_id=$2 AND link_kind='reference' AND owner_kind='resource_version' AND owner_id=$3 AND source_artifact_id IS NULL AND target_artifact_id=$4 AND link_key_digest=$5 AND payload_schema_version=$6 AND payload=$7 AND payload_digest=$8 AND expires_at IS NULL AND ((state='active' AND released_at IS NULL) OR (state='released' AND released_at IS NOT NULL)))")
        .bind(seed.tenant_id.to_string()).bind(policy.artifact_reference_id.to_string()).bind(policy.revision_id.to_string())
        .bind(seed.authoring_artifact_id.to_string()).bind(reference.link_key_digest().map_err(|_|invalid())?.to_string())
        .bind(payload.schema_version).bind(&payload.value).bind(&payload.digest).fetch_one(&mut **tx).await?;
    if !exact {
        return Err(conflict());
    }
    Ok(())
}
fn validate_input(
    base: &BootstrapDevelopmentProfile,
    seed: &ModelPolicyBootstrapSeedV1,
    physical: &ModelPolicyArtifactMaterialV1,
) -> Result<ModelPolicyBootstrapMaterial, RepositoryError> {
    validate_development_bootstrap(base)?;
    let built = build_model_policy_bootstrap(seed).map_err(|_| invalid())?;
    physical
        .validate_for(
            seed,
            &built.content_digest,
            built.declaration_bytes.len() as u64,
        )
        .map_err(|_| invalid())?;
    let Some(artifact) = &base.artifact_authority else {
        return Err(invalid());
    };
    let tenant: ResourceId = base.tenant.tenant_id.parse().map_err(|_| invalid())?;
    let roots = development_artifact_authority_material(&tenant, artifact)?;
    if seed.tenant_id != tenant
        || seed.installation_principal_id != base.installation.principal_id
        || seed.created_by != base.developer.principal_id
        || seed.request_id == base.installation.request_id
        || seed.encryption_domain_id != artifact.artifact_io_policy.encryption_domain_id
        || physical.storage_binding_digest
            != artifact.artifact_io_policy.write_storage_binding_digest
        || seed.retention_policy.revision_id != artifact.retention_policy_revision_id
        || seed.retention_policy.semantic_digest.as_str() != roots.retention_version.digest
    {
        return Err(invalid());
    }
    Ok(built)
}

impl PgRepository {
    /// Reads the actual exact Policy revisions used by a fresh installation before the model seed
    /// is frozen. Later recovery consumes that frozen seed instead of selecting new policy heads.
    pub async fn read_model_policy_bootstrap_artifact_inputs(
        &self,
        base: &BootstrapDevelopmentProfile,
    ) -> Result<(ExactVersionRef, ResourceId, Sha256Digest), RepositoryError> {
        validate_development_bootstrap(base)?;
        let mut tx = begin_read_only_repeatable(self.pool()).await?;
        verify_base(&mut tx, base).await?;
        let tenant_id: ResourceId = base.tenant.tenant_id.parse().map_err(|_| invalid())?;
        let tenant = load_tenant(&mut tx, &tenant_id).await?;
        let Some(seed) = base.artifact_authority.as_ref() else {
            return Err(invalid());
        };
        let expected = development_artifact_authority_material(&tenant_id, seed)?;
        if tenant.config.artifact_retention_policy
            != expected.tenant_config.artifact_retention_policy
            || tenant.config.artifact_io_policy != expected.tenant_config.artifact_io_policy
        {
            return Err(conflict());
        }
        let mut retention = None;
        let mut io = None;
        for (exact, kind) in [
            (
                tenant
                    .config
                    .artifact_retention_policy
                    .as_ref()
                    .ok_or_else(conflict)?,
                PolicyKind::Retention,
            ),
            (
                tenant
                    .config
                    .artifact_io_policy
                    .as_ref()
                    .ok_or_else(conflict)?,
                PolicyKind::ArtifactIo,
            ),
        ] {
            let deployment = default_binding_for_seed(&mut tx, &tenant_id, exact).await?;
            let DeploymentClosure::Policy(closure) =
                decode_deployment_closure(&deployment.bindings)?
            else {
                return Err(conflict());
            };
            let version=resource_version_from_row(sqlx::query("SELECT * FROM insight_platform.resource_versions WHERE tenant_id=$1 AND resource_version_id=$2")
                .bind(tenant_id.to_string()).bind(closure.policy_revision.revision_id.to_string())
                .fetch_optional(&mut *tx).await?.ok_or_else(conflict)?)?;
            if version.content_digest != closure.policy_revision.semantic_digest.as_str() {
                return Err(conflict());
            }
            let published = decode_published_version_payload(&version.payload)?;
            let ResourceDocument::Policy(policy) = published.document else {
                return Err(conflict());
            };
            if policy.policy_kind != kind {
                return Err(conflict());
            }
            if kind == PolicyKind::Retention {
                retention = Some(closure.policy_revision)
            } else {
                io = policy.sandbox_artifact_io
            }
        }
        let io = io.ok_or_else(conflict)?;
        tx.commit().await?;
        Ok((
            retention.ok_or_else(conflict)?,
            io.encryption_domain_id,
            io.write_storage_binding_digest,
        ))
    }
    /// No DDL, grants, writes or mutable configuration repairs. This is installation identity and
    /// immutable material verification, not current permission to execute business operations.
    pub async fn verify_installation_profile(
        &self,
        base: &BootstrapDevelopmentProfile,
    ) -> Result<(), RepositoryError> {
        validate_development_bootstrap(base)?;
        let mut tx = begin_read_only_repeatable(self.pool()).await?;
        verify_base(&mut tx, base).await?;
        tx.commit().await?;
        Ok(())
    }
    pub async fn verify_model_policy_authority(
        &self,
        base: &BootstrapDevelopmentProfile,
        seed: &ModelPolicyBootstrapSeedV1,
        physical: &ModelPolicyArtifactMaterialV1,
    ) -> Result<(), RepositoryError> {
        let built = validate_input(base, seed, physical)?;
        let mut tx = begin_read_only_repeatable(self.pool()).await?;
        verify_base(&mut tx, base).await?;
        verify_model(&mut tx, seed, physical, &built).await?;
        tx.commit().await?;
        Ok(())
    }
    pub async fn bootstrap_model_policy_authority(
        &self,
        base: &BootstrapDevelopmentProfile,
        seed: &ModelPolicyBootstrapSeedV1,
        physical: &ModelPolicyArtifactMaterialV1,
    ) -> Result<BootstrapOutcome, RepositoryError> {
        let built = validate_input(base, seed, physical)?;
        let mut tx = self.pool.begin().await?;
        // Serializes only bootstrap attempts for this Tenant. Normal Registry CAS remains intact.
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 5282259186414055289))")
            .bind(seed.tenant_id.to_string())
            .execute(&mut *tx)
            .await?;
        verify_base(&mut tx, base).await?;
        let ids: Vec<String> = seed
            .policies
            .iter()
            .map(|p| p.resource_id.to_string())
            .collect();
        let event_id = format!("evt_{}", seed.request_id.uuid().hyphenated());
        let count:i64=sqlx::query_scalar("SELECT (SELECT count(*) FROM insight_platform.resources WHERE tenant_id=$1 AND resource_id=ANY($2::text[])) + (SELECT count(*) FROM insight_platform.artifacts WHERE tenant_id=$1 AND artifact_id=$3) + (SELECT count(*) FROM insight_platform.artifact_blobs WHERE tenant_id=$1 AND blob_id=$4) + (SELECT count(*) FROM insight_platform.events WHERE tenant_id=$1 AND event_id=$5) + (SELECT count(*) FROM insight_platform.quota_accounts WHERE tenant_id=$1 AND quota_account_id=$6)")
            .bind(seed.tenant_id.to_string()).bind(ids).bind(seed.authoring_artifact_id.to_string())
            .bind(seed.authoring_blob_id.to_string()).bind(&event_id).bind(seed.model_quota_account_id.to_string()).fetch_one(&mut *tx).await?;
        if count != 0 {
            verify_model(&mut tx, seed, physical, &built).await?;
            tx.commit().await?;
            return Ok(BootstrapOutcome::Replayed);
        }
        let principal = principal_from_row(
            sqlx::query(
                "SELECT * FROM insight_platform.principals WHERE principal_id=$1 FOR SHARE",
            )
            .bind(seed.installation_principal_id.to_string())
            .fetch_one(&mut *tx)
            .await?,
        )?;
        let permissions: PrincipalBindingsPayload =
            decode_typed_payload(&principal.payload, "installation principal")?;
        if principal.state != "active"
            || !permissions.installation_bindings.iter().any(|binding| {
                binding.principal_kind == PrincipalKind::InstallationOperator
                    && binding.state == PrincipalBindingState::Active
                    && binding.permissions.contains(Permission::InstallationManage)
            })
        {
            return Err(RepositoryError::PermissionDenied);
        }
        let tenant = load_tenant_for_update(&mut tx, &seed.tenant_id).await?;
        if tenant.state != "active" {
            return Err(RepositoryError::PermissionDenied);
        }
        let now: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(&mut *tx)
            .await?;
        let retain_until = DateTime::parse_from_rfc3339(seed.retain_until.as_str())
            .map_err(|_| invalid())?
            .with_timezone(&Utc);
        if retain_until <= now {
            return Err(invalid());
        }
        insert_model(&mut tx, seed, physical, &built, now, retain_until).await?;
        tx.commit().await?;
        Ok(BootstrapOutcome::Created)
    }
}

async fn default_binding_for_seed(
    tx: &mut Transaction<'_, Postgres>,
    tenant: &ResourceId,
    exact: &ExactDeploymentRef,
) -> Result<DeploymentRecord, RepositoryError> {
    let row=sqlx::query("SELECT * FROM insight_platform.deployments WHERE tenant_id=$1 AND deployment_id=$2 AND bindings_digest=$3")
        .bind(tenant.to_string()).bind(exact.deployment_id.to_string()).bind(exact.deployment_digest.to_string())
        .fetch_optional(&mut **tx).await?.ok_or_else(conflict)?;
    deployment_from_row(row)
}

async fn verify_base(
    tx: &mut Transaction<'_, Postgres>,
    base: &BootstrapDevelopmentProfile,
) -> Result<(), RepositoryError> {
    let tenant: ResourceId = base.tenant.tenant_id.parse().map_err(|_| invalid())?;
    // Owning decoders check the current shape; exact comparisons deliberately exclude mutable
    // state, permissions, generations, TenantConfig and current policy heads.
    load_tenant(tx, &tenant).await?;
    let mut principals = vec![(
        &base.installation.principal_id,
        &base.installation.authentication_authority_digest,
        &base.installation.subject_digest,
    )];
    principals.extend(
        std::iter::once(&base.developer)
            .chain(&base.service_principals)
            .map(|p| {
                (
                    &p.principal_id,
                    &p.authentication_authority_digest,
                    &p.subject_digest,
                )
            }),
    );
    for (id, authority, subject) in principals {
        let row = sqlx::query("SELECT * FROM insight_platform.principals WHERE principal_id=$1")
            .bind(id.to_string())
            .fetch_optional(&mut **tx)
            .await?
            .ok_or_else(conflict)?;
        let current = principal_from_row(row)?;
        let _: PrincipalBindingsPayload =
            decode_typed_payload(&current.payload, "installation principal")?;
        if current.authentication_authority_digest != authority.as_str()
            || current.subject_digest != subject.as_str()
        {
            return Err(conflict());
        }
    }
    for binding in &base.tenant_principal_bindings {
        load_tenant_principal(tx, &tenant, &binding.principal_id, binding.principal_kind).await?;
    }
    let evidence = installation_event(&base.installation)?;
    require_event(
        tx,
        None,
        &format!("evt_{}", base.installation.request_id.uuid().hyphenated()),
        "principal",
        &base.installation.principal_id,
        "installation.bootstrap",
        &evidence,
    )
    .await?;
    let Some(seed) = &base.artifact_authority else {
        return Ok(());
    };
    let material = development_artifact_authority_material(&tenant, seed)?;
    for (resource, revision, deployment, published, bindings) in [
        (
            &seed.retention_policy_id,
            &seed.retention_policy_revision_id,
            &seed.retention_policy_deployment_id,
            &material.retention_version,
            &material.retention_deployment,
        ),
        (
            &seed.artifact_io_policy_id,
            &seed.artifact_io_policy_revision_id,
            &seed.artifact_io_policy_deployment_id,
            &material.artifact_io_version,
            &material.artifact_io_deployment,
        ),
        (
            &seed.scheduling_policy_id,
            &seed.scheduling_policy_revision_id,
            &seed.scheduling_policy_deployment_id,
            &material.scheduling_version,
            &material.scheduling_deployment,
        ),
    ] {
        require_policy_history(
            tx,
            &tenant,
            resource,
            revision,
            deployment,
            "local",
            &seed.authoring_artifact_id,
            &base.developer.principal_id,
            published,
            bindings,
        )
        .await?;
    }
    let locator = canonical_json(
        &serde_json::json!({"kind":"builtin-development-authority","tenant_id":tenant}),
    )
    .map_err(|_| invalid())?;
    require_blob(
        tx,
        &tenant,
        &seed.authoring_blob_id,
        "builtin",
        &seed.artifact_io_policy.write_storage_binding_digest,
        &material.security_domain_digest,
        &locator,
        "builtin-v1",
        "builtin-development",
        &seed.artifact_io_policy.encryption_domain_id,
        &material.authoring_content_digest,
        material.authoring_size_bytes,
    )
    .await?;
    require_artifact(
        tx,
        &tenant,
        &seed.authoring_artifact_id,
        &seed.authoring_blob_id,
        &material.authoring_content_digest,
        material.authoring_size_bytes,
        &seed.retention_policy_revision_id,
        &base.developer.principal_id,
        &material.authoring_metadata,
        None,
    )
    .await?;
    for (id, class, metric, limit) in [
        (
            &seed.staging_quota_account_id,
            "artifact",
            "artifact.staging_bytes",
            seed.staging_quota_bytes,
        ),
        (
            &seed.orchestration_quota_account_id,
            "orchestration",
            "concurrent_jobs",
            seed.orchestration_concurrent_jobs,
        ),
    ] {
        let exact:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM insight_platform.quota_accounts WHERE tenant_id=$1 AND quota_account_id=$2 AND scope_kind='tenant' AND scope_id=$1 AND work_class=$3 AND metric=$4 AND limit_value=$5 AND payload_schema_version=$6 AND payload_digest=$7 AND payload=$8)")
            .bind(tenant.to_string()).bind(id.to_string()).bind(class).bind(metric).bind(limit)
            .bind(material.quota_payload.schema_version).bind(&material.quota_payload.digest).bind(&material.quota_payload.value)
            .fetch_one(&mut **tx).await?;
        if !exact {
            return Err(conflict());
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn require_policy_history(
    tx: &mut Transaction<'_, Postgres>,
    tenant: &ResourceId,
    resource: &ResourceId,
    revision: &ResourceId,
    deployment: &ResourceId,
    environment: &str,
    artifact: &ResourceId,
    created_by: &ResourceId,
    published: &TypedPayload,
    bindings: &TypedPayload,
) -> Result<(), RepositoryError> {
    let exact:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM insight_platform.resources r JOIN insight_platform.resource_versions v ON v.tenant_id=r.tenant_id AND v.resource_id=r.resource_id JOIN insight_platform.deployments d ON d.tenant_id=r.tenant_id AND d.resource_id=r.resource_id WHERE r.tenant_id=$1 AND r.resource_id=$2 AND r.resource_kind='policy' AND v.resource_version_id=$3 AND v.resource_version_kind='policy_revision' AND v.revision_no=1 AND v.content_digest=$4 AND v.payload_digest=$4 AND v.payload_schema_version=$5 AND v.payload=$6 AND v.artifact_id=$7 AND v.created_by=$8 AND d.deployment_id=$9 AND d.resource_version_id=$3 AND d.environment=$10 AND d.bindings_digest=$11 AND d.payload_schema_version=$12 AND d.bindings=$13 AND d.created_by=$8)")
        .bind(tenant.to_string()).bind(resource.to_string()).bind(revision.to_string()).bind(&published.digest)
        .bind(published.schema_version).bind(&published.value).bind(artifact.to_string()).bind(created_by.to_string())
        .bind(deployment.to_string()).bind(environment).bind(&bindings.digest).bind(bindings.schema_version).bind(&bindings.value)
        .fetch_one(&mut **tx).await?;
    if !exact {
        return Err(conflict());
    }
    Ok(())
}
async fn require_event(
    tx: &mut Transaction<'_, Postgres>,
    tenant: Option<&ResourceId>,
    event_id: &str,
    aggregate_kind: &str,
    aggregate_id: &ResourceId,
    event_type: &str,
    payload: &TypedPayload,
) -> Result<(), RepositoryError> {
    let exact:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM insight_platform.events WHERE tenant_id IS NOT DISTINCT FROM $1 AND event_id=$2 AND aggregate_kind=$3 AND aggregate_id=$4 AND aggregate_version=1 AND event_type=$5 AND visibility='internal' AND payload_schema_version=$6 AND payload_digest=$7 AND payload=$8)")
        .bind(tenant.map(ToString::to_string)).bind(event_id).bind(aggregate_kind).bind(aggregate_id.to_string()).bind(event_type)
        .bind(payload.schema_version).bind(&payload.digest).bind(&payload.value).fetch_one(&mut **tx).await?;
    if !exact {
        return Err(conflict());
    }
    Ok(())
}
#[allow(clippy::too_many_arguments)]
async fn require_blob(
    tx: &mut Transaction<'_, Postgres>,
    tenant: &ResourceId,
    blob: &ResourceId,
    backend: &str,
    storage: &Sha256Digest,
    security: &Sha256Digest,
    locator: &[u8],
    generation: &str,
    key: &str,
    encryption: &ResourceId,
    content: &Sha256Digest,
    size: i64,
) -> Result<(), RepositoryError> {
    let exact:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM insight_platform.artifact_blobs WHERE tenant_id=$1 AND blob_id=$2 AND backend=$3 AND storage_binding_digest=$4 AND security_domain_digest=$5 AND object_reference_ciphertext=$6 AND object_generation=$7 AND key_id=$8 AND encryption_domain_id=$9 AND content_digest=$10 AND size_bytes=$11 AND state='verified' AND verified_at IS NOT NULL)")
        .bind(tenant.to_string()).bind(blob.to_string()).bind(backend).bind(storage.to_string()).bind(security.to_string())
        .bind(locator).bind(generation).bind(key).bind(encryption.to_string()).bind(content.to_string()).bind(size)
        .fetch_one(&mut **tx).await?;
    if !exact {
        return Err(conflict());
    }
    Ok(())
}
#[allow(clippy::too_many_arguments)]
async fn require_artifact(
    tx: &mut Transaction<'_, Postgres>,
    tenant: &ResourceId,
    artifact: &ResourceId,
    blob: &ResourceId,
    content: &Sha256Digest,
    size: i64,
    retention: &ResourceId,
    created_by: &ResourceId,
    metadata: &TypedPayload,
    retain_until: Option<DateTime<Utc>>,
) -> Result<(), RepositoryError> {
    let exact:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM insight_platform.artifacts WHERE tenant_id=$1 AND artifact_id=$2 AND blob_id=$3 AND purpose='authoring_document' AND classification='internal' AND expected_digest=$4 AND expected_size_bytes=$5 AND declared_media_type='application/json' AND verified_media_type='application/json' AND state='ready' AND retention_policy_revision_id=$6 AND created_by=$7 AND metadata_schema_version=$8 AND metadata_digest=$9 AND metadata=$10 AND ($11::timestamptz IS NULL OR retain_until=$11))")
        .bind(tenant.to_string()).bind(artifact.to_string()).bind(blob.to_string()).bind(content.to_string()).bind(size)
        .bind(retention.to_string()).bind(created_by.to_string()).bind(metadata.schema_version).bind(&metadata.digest).bind(&metadata.value)
        .bind(retain_until).fetch_one(&mut **tx).await?;
    if !exact {
        return Err(conflict());
    }
    Ok(())
}
fn security_domain(seed: &ModelPolicyBootstrapSeedV1) -> Result<Sha256Digest, RepositoryError> {
    insight_platform_artifacts::ArtifactBlobSecurityDomain {
        schema_version: 1,
        classification: DataClassification::Internal,
        retention_policy_revision_id: seed.retention_policy.revision_id.clone(),
        encryption_domain_id: seed.encryption_domain_id.clone(),
    }
    .canonical_digest()
    .map_err(|_| invalid())
}
fn model_concurrency_payload(
    seed: &ModelPolicyBootstrapSeedV1,
) -> Result<TypedPayload, RepositoryError> {
    Ok(TypedPayload::new(
        1,
        &serde_json::json!({"installation_request_id":seed.request_id,"accounting_mode":"leased"}),
    )?)
}
async fn verify_model_concurrency(
    tx: &mut Transaction<'_, Postgres>,
    seed: &ModelPolicyBootstrapSeedV1,
) -> Result<(), RepositoryError> {
    let payload = model_concurrency_payload(seed)?;
    let exact:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM insight_platform.quota_accounts WHERE tenant_id=$1 AND quota_account_id=$2 AND scope_kind='tenant' AND scope_id=$1 AND work_class='model' AND metric=$3 AND payload_schema_version=$4 AND payload=$5 AND payload_digest=$6)")
        .bind(seed.tenant_id.to_string()).bind(seed.model_quota_account_id.to_string()).bind(QuotaDimension::WorkClassConcurrentOperations.as_str())
        .bind(payload.schema_version).bind(&payload.value).bind(&payload.digest).fetch_one(&mut **tx).await?;
    if !exact {
        return Err(conflict());
    }
    Ok(())
}
async fn verify_model(
    tx: &mut Transaction<'_, Postgres>,
    seed: &ModelPolicyBootstrapSeedV1,
    physical: &ModelPolicyArtifactMaterialV1,
    built: &ModelPolicyBootstrapMaterial,
) -> Result<(), RepositoryError> {
    verify_model_concurrency(tx, seed).await?;
    for policy in &built.policies {
        require_policy_history(
            tx,
            &seed.tenant_id,
            &policy.identity.resource_id,
            &policy.identity.revision_id,
            &policy.identity.deployment_id,
            &seed.environment,
            &seed.authoring_artifact_id,
            &seed.created_by,
            &policy.published,
            &policy.deployment,
        )
        .await?;
        require_policy_reference(tx, seed, &policy.identity).await?;
    }
    require_blob(
        tx,
        &seed.tenant_id,
        &seed.authoring_blob_id,
        &physical.storage_backend,
        &physical.storage_binding_digest,
        &security_domain(seed)?,
        &physical.object_reference_ciphertext,
        &physical.object_generation,
        &physical.key_id,
        &seed.encryption_domain_id,
        &physical.content_digest,
        physical.size_bytes as i64,
    )
    .await?;
    let retain_until = DateTime::parse_from_rfc3339(seed.retain_until.as_str())
        .map_err(|_| invalid())?
        .with_timezone(&Utc);
    require_artifact(
        tx,
        &seed.tenant_id,
        &seed.authoring_artifact_id,
        &seed.authoring_blob_id,
        &physical.content_digest,
        physical.size_bytes as i64,
        &seed.retention_policy.revision_id,
        &seed.created_by,
        &artifact_metadata(seed, built)?,
        Some(retain_until),
    )
    .await?;
    require_event(
        tx,
        Some(&seed.tenant_id),
        &format!("evt_{}", seed.request_id.uuid().hyphenated()),
        "tenant",
        &seed.tenant_id,
        "installation.model_policies_bootstrapped",
        &model_event(seed, physical)?,
    )
    .await
}
async fn insert_model(
    tx: &mut Transaction<'_, Postgres>,
    seed: &ModelPolicyBootstrapSeedV1,
    physical: &ModelPolicyArtifactMaterialV1,
    built: &ModelPolicyBootstrapMaterial,
    now: DateTime<Utc>,
    retain_until: DateTime<Utc>,
) -> Result<(), RepositoryError> {
    let quota = model_concurrency_payload(seed)?;
    sqlx::query("INSERT INTO insight_platform.quota_accounts(tenant_id,quota_account_id,scope_kind,scope_id,work_class,metric,limit_value,payload_schema_version,payload,payload_digest,created_at,updated_at) VALUES($1,$2,'tenant',$1,'model',$3,$4,$5,$6,$7,$8,$8)")
        .bind(seed.tenant_id.to_string()).bind(seed.model_quota_account_id.to_string()).bind(QuotaDimension::WorkClassConcurrentOperations.as_str()).bind(INITIAL_MODEL_CONCURRENCY_LIMIT as i64)
        .bind(quota.schema_version).bind(&quota.value).bind(&quota.digest).bind(now).execute(&mut **tx).await?;
    sqlx::query("INSERT INTO insight_platform.artifact_blobs (tenant_id,blob_id,backend,storage_binding_digest,security_domain_digest,object_reference_ciphertext,object_generation,key_id,encryption_domain_id,content_digest,size_bytes,state,verified_at,created_at,updated_at) VALUES($1,$2,'s3',$3,$4,$5,$6,$7,$8,$9,$10,'verified',$11,$11,$11)")
        .bind(seed.tenant_id.to_string()).bind(seed.authoring_blob_id.to_string()).bind(physical.storage_binding_digest.to_string())
        .bind(security_domain(seed)?.to_string()).bind(&physical.object_reference_ciphertext).bind(&physical.object_generation)
        .bind(&physical.key_id).bind(seed.encryption_domain_id.to_string()).bind(physical.content_digest.to_string())
        .bind(physical.size_bytes as i64).bind(now).execute(&mut **tx).await?;
    let metadata = artifact_metadata(seed, built)?;
    sqlx::query("INSERT INTO insight_platform.artifacts (tenant_id,artifact_id,blob_id,purpose,classification,expected_size_bytes,expected_digest,declared_media_type,verified_media_type,state,metadata_schema_version,metadata,metadata_digest,retention_policy_revision_id,retain_until,created_by,created_at,updated_at) VALUES($1,$2,$3,'authoring_document','internal',$4,$5,'application/json','application/json','ready',$6,$7,$8,$9,$10,$11,$12,$12)")
        .bind(seed.tenant_id.to_string()).bind(seed.authoring_artifact_id.to_string()).bind(seed.authoring_blob_id.to_string())
        .bind(physical.size_bytes as i64).bind(physical.content_digest.to_string()).bind(metadata.schema_version).bind(&metadata.value)
        .bind(&metadata.digest).bind(seed.retention_policy.revision_id.to_string()).bind(retain_until).bind(seed.created_by.to_string())
        .bind(now).execute(&mut **tx).await?;
    for policy in &built.policies {
        sqlx::query("INSERT INTO insight_platform.resources (tenant_id,resource_id,resource_kind,lifecycle_state,gate_state,version,payload_schema_version,payload,payload_digest,created_at,updated_at) VALUES($1,$2,'policy','active','enabled',1,$3,$4,$5,$6,$6)")
            .bind(seed.tenant_id.to_string()).bind(policy.identity.resource_id.to_string()).bind(policy.resource.schema_version)
            .bind(&policy.resource.value).bind(&policy.resource.digest).bind(now).execute(&mut **tx).await?;
        sqlx::query("INSERT INTO insight_platform.resource_versions (tenant_id,resource_version_id,resource_id,resource_version_kind,revision_no,content_digest,artifact_id,payload_schema_version,payload,payload_digest,created_by,created_at) VALUES($1,$2,$3,'policy_revision',1,$4,$5,$6,$7,$4,$8,$9)")
            .bind(seed.tenant_id.to_string()).bind(policy.identity.revision_id.to_string()).bind(policy.identity.resource_id.to_string())
            .bind(&policy.published.digest).bind(seed.authoring_artifact_id.to_string()).bind(policy.published.schema_version)
            .bind(&policy.published.value).bind(seed.created_by.to_string()).bind(now).execute(&mut **tx).await?;
        let reference = policy_reference(seed, &policy.identity);
        let payload = TypedPayload::from_versioned(1, &reference, 65_536)?;
        sqlx::query("INSERT INTO insight_platform.artifact_links (tenant_id,artifact_link_id,link_kind,owner_kind,owner_id,target_artifact_id,link_key_digest,state,payload_schema_version,payload,payload_digest,created_at,updated_at) VALUES($1,$2,'reference','resource_version',$3,$4,$5,'active',$6,$7,$8,$9,$9)")
            .bind(seed.tenant_id.to_string()).bind(policy.identity.artifact_reference_id.to_string()).bind(policy.identity.revision_id.to_string())
            .bind(seed.authoring_artifact_id.to_string()).bind(reference.link_key_digest().map_err(|_|invalid())?.to_string())
            .bind(payload.schema_version).bind(&payload.value).bind(&payload.digest).bind(now).execute(&mut **tx).await?;
        sqlx::query("INSERT INTO insight_platform.deployments (tenant_id,deployment_id,resource_id,resource_version_id,environment,bindings_digest,payload_schema_version,bindings,created_by,created_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)")
            .bind(seed.tenant_id.to_string()).bind(policy.identity.deployment_id.to_string()).bind(policy.identity.resource_id.to_string())
            .bind(policy.identity.revision_id.to_string()).bind(&seed.environment).bind(&policy.deployment.digest)
            .bind(policy.deployment.schema_version).bind(&policy.deployment.value).bind(seed.created_by.to_string()).bind(now)
            .execute(&mut **tx).await?;
        sqlx::query("UPDATE insight_platform.resources SET active_deployment_id=$3,version=3 WHERE tenant_id=$1 AND resource_id=$2")
            .bind(seed.tenant_id.to_string()).bind(policy.identity.resource_id.to_string()).bind(policy.identity.deployment_id.to_string())
            .execute(&mut **tx).await?;
    }
    let event = model_event(seed, physical)?;
    sqlx::query("INSERT INTO insight_platform.events (tenant_id,event_id,aggregate_kind,aggregate_id,aggregate_version,trace_id,event_type,visibility,payload_schema_version,payload,payload_digest) VALUES($1,$2,'tenant',$1,1,$3,'installation.model_policies_bootstrapped','internal',$4,$5,$6)")
        .bind(seed.tenant_id.to_string()).bind(format!("evt_{}",seed.request_id.uuid().hyphenated()))
        .bind(TraceIdentityV1::generate().trace_id.to_string()).bind(event.schema_version).bind(&event.value).bind(&event.digest)
        .execute(&mut **tx).await?;
    Ok(())
}
