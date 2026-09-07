//! Read-only Registry compilation input authority. Artifact bytes are read by the
//! broker outside these transactions; every broker authorization repeats these checks.
use crate::repository::{
    begin_read_only_repeatable, decode_typed_payload, decode_versioned_payload, job_from_row,
    load_current_principal_snapshot, load_resource, payload_from_row, PgRepository,
    RepositoryError,
};
use chrono::Utc;
use insight_platform_artifacts::{
    GatewayArtifactReadAuthority, GatewayArtifactReadRequest, RegistryArtifactReadAuthorityV1,
    RegistryArtifactReadRequestV1, RegistryArtifactSelectorV1, MAX_REGISTRY_ARTIFACT_BYTES,
};
use insight_platform_contracts::{
    canonical_digest, ArtifactPurpose, ArtifactRef, Permission, PrincipalKind,
    RegistryResourceKind, ResourceDocument, ResourceDraftPayload, ResourceId, Sha256Digest,
};
use insight_platform_registry::RegistryValidationJobPayload;
use sqlx::{Postgres, Row, Transaction};

pub(crate) async fn registry_validation_document(
    tx: &mut Transaction<'_, Postgres>,
    tenant: &ResourceId,
    principal: &ResourceId,
    principal_kind: PrincipalKind,
    authority: &RegistryArtifactReadAuthorityV1,
) -> Result<(ResourceDraftPayload, Sha256Digest), RepositoryError> {
    authority
        .validate()
        .map_err(|_| RepositoryError::PermissionDenied)?;
    let current = load_current_principal_snapshot(tx, tenant, principal, principal_kind).await?;
    if !current.permissions.contains(Permission::AgentWrite) {
        return Err(RepositoryError::PermissionDenied);
    }
    let (resource_id, expected_version, expected_draft_digest) = match authority {
        RegistryArtifactReadAuthorityV1::RegistryDraft {
            resource_id,
            expected_resource_version,
        } => (resource_id.clone(), *expected_resource_version, None),
        RegistryArtifactReadAuthorityV1::RegistryJob {
            job_id,
            worker_process_generation_id,
            lease_epoch,
            expected_job_version,
            lease_token_digest,
        } => {
            if principal_kind != PrincipalKind::ServiceIdentity {
                return Err(RepositoryError::PermissionDenied);
            }
            let row = sqlx::query("SELECT * FROM insight_platform.jobs WHERE tenant_id=$1 AND job_id=$2 AND state='running' AND terminal_at IS NULL AND lease_expires_at>clock_timestamp() AND deadline>clock_timestamp()")
                .bind(tenant.to_string()).bind(job_id.to_string()).fetch_optional(&mut **tx).await?
                .ok_or(RepositoryError::PermissionDenied)?;
            let job = job_from_row(row)?;
            if job.job_kind != "registry_validation"
                || job.work_class != "registry_validation"
                || job.owner_kind != "job"
                || job.owner_id != job_id.to_string()
                || job.worker_id.as_deref()
                    != Some(worker_process_generation_id.to_string().as_str())
                || u64::try_from(job.lease_epoch).ok() != Some(*lease_epoch)
                || u64::try_from(job.version).ok() != Some(*expected_job_version)
                || job.lease_token_digest.as_deref() != Some(lease_token_digest.as_str())
            {
                return Err(RepositoryError::PermissionDenied);
            }
            let payload: RegistryValidationJobPayload =
                decode_versioned_payload(&job.payload, "Registry compilation Job")?;
            payload
                .validate_for_owner(job_id)
                .map_err(|error| RepositoryError::CorruptRow(error.to_string()))?;
            if payload.resource_kind != RegistryResourceKind::Agent
                || job.execution_requirement != payload.execution_requirement
            {
                return Err(RepositoryError::PermissionDenied);
            }
            (
                payload.resource_id,
                payload.expected_resource_version,
                Some(payload.draft_digest),
            )
        }
    };
    let record = load_resource(tx, tenant, &resource_id).await?;
    if record.resource_kind != "agent"
        || record.lifecycle_state == "retired"
        || u64::try_from(record.version).ok() != Some(expected_version)
    {
        return Err(RepositoryError::PermissionDenied);
    }
    let draft: ResourceDraftPayload =
        decode_typed_payload(&record.payload, "Registry compilation Draft")?;
    draft
        .validate()
        .map_err(|error| RepositoryError::CorruptRow(error.to_string()))?;
    let digest = draft
        .document_digest()
        .map_err(|error| RepositoryError::CorruptRow(error.to_string()))?;
    if expected_draft_digest.is_some_and(|expected| expected != digest) {
        return Err(RepositoryError::PermissionDenied);
    }
    Ok((draft, digest))
}

pub(crate) async fn registry_selected_artifact(
    tx: &mut Transaction<'_, Postgres>,
    tenant: &ResourceId,
    document: &ResourceDocument,
    selector: RegistryArtifactSelectorV1,
) -> Result<(ArtifactRef, ArtifactPurpose), RepositoryError> {
    let ResourceDocument::Agent(agent) = document else {
        return Err(RepositoryError::PermissionDenied);
    };
    let (artifact_id, digest, purpose) = match selector {
        RegistryArtifactSelectorV1::Authoring => (
            agent.authoring_package.artifact.artifact_id(),
            agent.authoring_package.artifact.content_digest(),
            ArtifactPurpose::AuthoringDocument,
        ),
        RegistryArtifactSelectorV1::TypedPlan => (
            &agent.typed_plan_artifact_id,
            &agent.typed_plan_digest,
            ArtifactPurpose::TypedPlan,
        ),
    };
    let row = sqlx::query("SELECT a.classification,a.verified_media_type,a.metadata_schema_version,a.metadata,a.metadata_digest,b.size_bytes FROM insight_platform.artifacts a JOIN insight_platform.artifact_blobs b ON b.tenant_id=a.tenant_id AND b.blob_id=a.blob_id WHERE a.tenant_id=$1 AND a.artifact_id=$2 AND a.state='ready' AND a.terminal_at IS NULL AND a.purpose=$3 AND a.expected_digest=$4 AND b.state='verified' AND b.deleted_at IS NULL AND b.content_digest=$4 AND b.size_bytes=a.expected_size_bytes")
        .bind(tenant.to_string()).bind(artifact_id.to_string()).bind(purpose.as_str()).bind(digest.to_string())
        .fetch_optional(&mut **tx).await?.ok_or(RepositoryError::NotFound("Registry input Artifact"))?;
    let metadata = payload_from_row(
        &row,
        "metadata_schema_version",
        "metadata",
        "metadata_digest",
    )?;
    let length = u64::try_from(row.try_get::<i64, _>("size_bytes")?)
        .map_err(|_| RepositoryError::CorruptRow("invalid Registry Artifact size".into()))?;
    if length > MAX_REGISTRY_ARTIFACT_BYTES as u64 {
        return Err(RepositoryError::InvalidInput(
            "Registry Artifact exceeds compiler budget".into(),
        ));
    }
    let artifact = ArtifactRef::new(
        artifact_id.clone(),
        digest.clone(),
        length,
        row.try_get::<String, _>("verified_media_type")?,
        row.try_get::<String, _>("classification")?
            .parse()
            .map_err(|_| RepositoryError::CorruptRow("invalid Artifact classification".into()))?,
        metadata
            .value
            .get("display_name")
            .and_then(serde_json::Value::as_str)
            .map(ToOwned::to_owned),
    )
    .map_err(|error| RepositoryError::CorruptRow(error.to_string()))?;
    if selector == RegistryArtifactSelectorV1::Authoring
        && artifact != agent.authoring_package.artifact
    {
        return Err(RepositoryError::PermissionDenied);
    }
    Ok((artifact, purpose))
}

impl PgRepository {
    pub async fn resolve_registry_validation_artifact(
        &self,
        tenant: &ResourceId,
        principal: &ResourceId,
        principal_kind: PrincipalKind,
        request: &RegistryArtifactReadRequestV1,
    ) -> Result<(GatewayArtifactReadRequest, ArtifactPurpose), RepositoryError> {
        request
            .validate_at(Utc::now())
            .map_err(|_| RepositoryError::PermissionDenied)?;
        let mut tx = begin_read_only_repeatable(self.pool()).await?;
        let (draft, draft_digest) = registry_validation_document(
            &mut tx,
            tenant,
            principal,
            principal_kind,
            &request.authority,
        )
        .await?;
        let (artifact, purpose) =
            registry_selected_artifact(&mut tx, tenant, &draft.document, request.selector).await?;
        let request_digest = canonical_digest(&serde_json::json!({"schema_version":1,"request":request,"draft_digest":draft_digest,"artifact":artifact}))
            .map_err(|_| RepositoryError::InvalidInput("invalid Registry read request".into()))?.parse().map_err(|_| RepositoryError::InvalidInput("invalid Registry read digest".into()))?;
        tx.commit().await?;
        Ok((
            GatewayArtifactReadRequest {
                authority: GatewayArtifactReadAuthority::RegistryValidation {
                    authority: request.authority.clone(),
                    selector: request.selector,
                },
                tenant_id: tenant.clone(),
                principal_id: principal.clone(),
                principal_kind,
                artifact,
                request_digest,
                maximum_bytes: MAX_REGISTRY_ARTIFACT_BYTES,
                deadline: request.deadline,
            },
            purpose,
        ))
    }

    pub async fn read_registry_validation_document(
        &self,
        tenant: &ResourceId,
        principal: &ResourceId,
        authority: &RegistryArtifactReadAuthorityV1,
    ) -> Result<ResourceDocument, RepositoryError> {
        let mut tx = begin_read_only_repeatable(self.pool()).await?;
        let (draft, _) = registry_validation_document(
            &mut tx,
            tenant,
            principal,
            PrincipalKind::ServiceIdentity,
            authority,
        )
        .await?;
        tx.commit().await?;
        Ok(draft.document)
    }
}

impl PgRepository {
    pub async fn lookup_registry_validation_receipt(
        &self,
        query: &insight_platform_registry::LookupRegistryValidationReceipt,
    ) -> Result<Option<ResourceId>, RepositoryError> {
        query.validate_at(Utc::now()).map_err(|_| {
            RepositoryError::InvalidInput("invalid validation Receipt lookup".to_owned())
        })?;
        let mut tx = begin_read_only_repeatable(self.pool()).await?;
        let principal = load_current_principal_snapshot(
            &mut tx,
            &query.tenant_id,
            &query.principal_id,
            query.principal_kind,
        )
        .await?;
        let row = sqlx::query("SELECT request_digest,state,response_reference_id,expires_at>clock_timestamp() AS live FROM insight_platform.receipts WHERE tenant_id=$1 AND receipt_kind='command' AND scope_kind='resource' AND scope_id=$2 AND dedupe_owner_id=$3 AND operation='resource.validate' AND idempotency_key_digest=$4")
            .bind(query.tenant_id.to_string()).bind(query.resource_id.to_string()).bind(query.principal_id.to_string()).bind(query.idempotency_key_digest.to_string()).fetch_optional(&mut *tx).await?;
        let Some(row) = row else {
            tx.commit().await?;
            return Ok(None);
        };
        if !principal.permissions.contains(Permission::OperationRead) {
            return Err(RepositoryError::PermissionDenied);
        }
        if row.try_get::<String, _>("request_digest")? != query.request_digest.as_str() {
            return Err(RepositoryError::IdempotencyConflict);
        }
        if !row.try_get::<bool, _>("live")? || row.try_get::<String, _>("state")? != "succeeded" {
            return Err(RepositoryError::Conflict("validation Receipt unavailable"));
        }
        let id = row
            .try_get::<Option<String>, _>("response_reference_id")?
            .ok_or_else(|| {
                RepositoryError::CorruptRow("validation Receipt result is missing".to_owned())
            })?;
        let id = ResourceId::parse_expected(&id, insight_platform_contracts::ResourceKind::Job)
            .map_err(|_| {
                RepositoryError::CorruptRow(
                    "validation Receipt result identity is invalid".to_owned(),
                )
            })?;
        tx.commit().await?;
        Ok(Some(id))
    }
}
