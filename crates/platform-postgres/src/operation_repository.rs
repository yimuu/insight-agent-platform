use crate::repository::{
    decode_typed_payload, job_from_row, load_current_principal_snapshot, JobRecord, RepositoryError,
};
use chrono::Utc;
use insight_platform_artifacts::{ArtifactJobPayload, ArtifactUploadOperationSnapshot};
use insight_platform_context::ContextDatasetBuildJobPayload;
use insight_platform_contracts::{
    operation_etag, JobState, OperationViewV1, Permission, PublicJobKind, PublicJobState,
    PublicJobTarget, ReadOperation, ResourceId, SafeJobFailure, SafeJobResult, Sha256Digest,
    UtcTimestamp, WorkClass,
};
use insight_platform_mcp_host::McpJobPayload;
use insight_platform_registry::RegistryValidationJobPayload;
use std::{error::Error, fmt, str::FromStr};

#[derive(Debug)]
pub enum OperationReadError {
    InvalidRequest,
    Denied,
    NotFound,
    NotPublic,
    AuthorityUnavailable,
    CorruptAuthority,
}

impl fmt::Display for OperationReadError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidRequest => "Operation read request is invalid",
            Self::Denied => "Operation read is denied",
            Self::NotFound => "Operation was not found",
            Self::NotPublic => "Job is not a public Operation",
            Self::AuthorityUnavailable => "Operation authority is unavailable",
            Self::CorruptAuthority => "Operation authority is corrupt",
        })
    }
}

impl Error for OperationReadError {}

impl crate::repository::PgRepository {
    pub async fn read_public_operation(
        &self,
        request: &ReadOperation,
    ) -> Result<OperationViewV1, OperationReadError> {
        request
            .validate_at(Utc::now())
            .map_err(|_| OperationReadError::InvalidRequest)?;
        let mut transaction = self
            .pool()
            .begin()
            .await
            .map_err(|_| OperationReadError::AuthorityUnavailable)?;
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            .execute(&mut *transaction)
            .await
            .map_err(|_| OperationReadError::AuthorityUnavailable)?;
        let principal = load_current_principal_snapshot(
            &mut transaction,
            &request.tenant_id,
            &request.principal_id,
            request.principal_kind,
        )
        .await
        .map_err(classify_authority_error)?;
        if !principal.permissions.contains(Permission::OperationRead) {
            return Err(OperationReadError::Denied);
        }
        let row = sqlx::query(
            r#"
            SELECT *
            FROM insight_platform.jobs
            WHERE tenant_id = $1 AND job_id = $2
            "#,
        )
        .bind(request.tenant_id.to_string())
        .bind(request.operation_id.to_string())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(|_| OperationReadError::AuthorityUnavailable)?
        .ok_or(OperationReadError::NotFound)?;
        let job = job_from_row(row).map_err(classify_authority_error)?;
        let (kind, target) = public_kind_and_target(&mut transaction, &job).await?;
        let view = project_operation(job, kind, target)?;
        transaction
            .commit()
            .await
            .map_err(|_| OperationReadError::AuthorityUnavailable)?;
        Ok(view)
    }
}

pub fn project_registry_validation_operation(
    job: JobRecord,
) -> Result<OperationViewV1, OperationReadError> {
    if WorkClass::from_str(&job.work_class).map_err(|_| OperationReadError::CorruptAuthority)?
        != WorkClass::RegistryValidation
    {
        return Err(OperationReadError::NotPublic);
    }
    let payload: RegistryValidationJobPayload =
        serde_json::from_value(job.payload.value.clone())
            .map_err(|_| OperationReadError::CorruptAuthority)?;
    let owner: ResourceId = job
        .owner_id
        .parse()
        .map_err(|_| OperationReadError::CorruptAuthority)?;
    payload
        .validate_for_owner(&owner)
        .map_err(|_| OperationReadError::CorruptAuthority)?;
    let target = PublicJobTarget::ResourceVersion {
        resource_id: payload.resource_id,
        resource_version: payload.expected_resource_version,
    };
    project_operation(job, PublicJobKind::ResourceValidation, target)
}

pub fn project_context_dataset_build_operation(
    job: JobRecord,
) -> Result<OperationViewV1, OperationReadError> {
    if WorkClass::from_str(&job.work_class).map_err(|_| OperationReadError::CorruptAuthority)?
        != WorkClass::Context
        || job.owner_kind != "context_dataset"
    {
        return Err(OperationReadError::NotPublic);
    }
    let dataset_id: ResourceId = job
        .owner_id
        .parse()
        .map_err(|_| OperationReadError::CorruptAuthority)?;
    let payload: ContextDatasetBuildJobPayload = serde_json::from_value(job.payload.value.clone())
        .map_err(|_| OperationReadError::CorruptAuthority)?;
    payload
        .validate_for_owner(&dataset_id)
        .map_err(|_| OperationReadError::CorruptAuthority)?;
    project_operation(
        job,
        PublicJobKind::ContextDatasetBuild,
        PublicJobTarget::ContextDataset {
            context_dataset_id: dataset_id,
        },
    )
}

async fn public_kind_and_target(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    job: &JobRecord,
) -> Result<(PublicJobKind, PublicJobTarget), OperationReadError> {
    match WorkClass::from_str(&job.work_class).map_err(|_| OperationReadError::CorruptAuthority)? {
        WorkClass::RegistryValidation => {
            let payload: RegistryValidationJobPayload =
                serde_json::from_value(job.payload.value.clone())
                    .map_err(|_| OperationReadError::CorruptAuthority)?;
            let owner: ResourceId = job
                .owner_id
                .parse()
                .map_err(|_| OperationReadError::CorruptAuthority)?;
            payload
                .validate_for_owner(&owner)
                .map_err(|_| OperationReadError::CorruptAuthority)?;
            Ok((
                PublicJobKind::ResourceValidation,
                PublicJobTarget::ResourceVersion {
                    resource_id: payload.resource_id,
                    resource_version: payload.expected_resource_version,
                },
            ))
        }
        WorkClass::Mcp if job.owner_kind == "mcp_operation" => {
            let mut value = job.payload.value.clone();
            value
                .as_object_mut()
                .ok_or(OperationReadError::CorruptAuthority)?
                .remove("schema_version");
            let payload: McpJobPayload =
                serde_json::from_value(value).map_err(|_| OperationReadError::CorruptAuthority)?;
            let owner: ResourceId = job
                .owner_id
                .parse()
                .map_err(|_| OperationReadError::CorruptAuthority)?;
            payload
                .validate_for_owner(&owner)
                .map_err(|_| OperationReadError::CorruptAuthority)?;
            if !matches!(payload, McpJobPayload::Discovery(_)) {
                return Err(OperationReadError::NotPublic);
            }
            let deployment_id: String = sqlx::query_scalar(
                r#"
                SELECT deployment_id
                FROM insight_platform.invocations
                WHERE tenant_id = $1 AND invocation_id = $2
                  AND invocation_kind = 'mcp_discovery'
                  AND owner_kind = 'mcp_operation' AND owner_id = $2
                "#,
            )
            .bind(&job.tenant_id)
            .bind(&job.owner_id)
            .fetch_optional(&mut **transaction)
            .await
            .map_err(|_| OperationReadError::AuthorityUnavailable)?
            .ok_or(OperationReadError::CorruptAuthority)?;
            Ok((
                PublicJobKind::McpDiscovery,
                PublicJobTarget::Deployment {
                    deployment_id: deployment_id
                        .parse()
                        .map_err(|_| OperationReadError::CorruptAuthority)?,
                },
            ))
        }
        WorkClass::Context if job.owner_kind == "context_dataset" => {
            let dataset_id: ResourceId = job
                .owner_id
                .parse()
                .map_err(|_| OperationReadError::CorruptAuthority)?;
            let payload: ContextDatasetBuildJobPayload =
                serde_json::from_value(job.payload.value.clone())
                    .map_err(|_| OperationReadError::CorruptAuthority)?;
            payload
                .validate_for_owner(&dataset_id)
                .map_err(|_| OperationReadError::CorruptAuthority)?;
            Ok((
                PublicJobKind::ContextDatasetBuild,
                PublicJobTarget::ContextDataset {
                    context_dataset_id: dataset_id,
                },
            ))
        }
        WorkClass::Artifact if job.owner_kind == "artifact" => {
            let artifact_id: ResourceId = job
                .owner_id
                .parse()
                .map_err(|_| OperationReadError::CorruptAuthority)?;
            let kind = match decode_typed_payload::<ArtifactJobPayload>(
                &job.payload,
                "public Artifact Operation Job",
            ) {
                Ok(payload) => {
                    payload
                        .validate_for_owner(&artifact_id)
                        .map_err(|_| OperationReadError::CorruptAuthority)?;
                    match payload {
                        ArtifactJobPayload::AwaitingStage { .. } => {
                            return Err(OperationReadError::NotPublic)
                        }
                        ArtifactJobPayload::Scan { .. } | ArtifactJobPayload::Rescan { .. } => {
                            PublicJobKind::ArtifactVerify
                        }
                        ArtifactJobPayload::Delete { .. } => PublicJobKind::ArtifactDelete,
                        ArtifactJobPayload::BlobCleanup { .. } => {
                            return Err(OperationReadError::NotPublic)
                        }
                    }
                }
                Err(_) => {
                    serde_json::from_value::<ArtifactUploadOperationSnapshot>(
                        job.payload.value.clone(),
                    )
                    .map_err(|_| OperationReadError::NotPublic)?;
                    PublicJobKind::ArtifactVerify
                }
            };
            Ok((kind, PublicJobTarget::Artifact { artifact_id }))
        }
        _ => Err(OperationReadError::NotPublic),
    }
}

fn project_operation(
    job: JobRecord,
    kind: PublicJobKind,
    target: PublicJobTarget,
) -> Result<OperationViewV1, OperationReadError> {
    let operation_id: ResourceId = job
        .job_id
        .parse()
        .map_err(|_| OperationReadError::CorruptAuthority)?;
    let tenant_id: ResourceId = job
        .tenant_id
        .parse()
        .map_err(|_| OperationReadError::CorruptAuthority)?;
    let state = job
        .state
        .parse::<JobState>()
        .map(PublicJobState::from)
        .map_err(|_| OperationReadError::CorruptAuthority)?;
    let version = u64::try_from(job.version).map_err(|_| OperationReadError::CorruptAuthority)?;
    if version == 0 {
        return Err(OperationReadError::CorruptAuthority);
    }
    let result = match (state, job.result_digest.as_deref()) {
        (PublicJobState::Succeeded, Some(digest)) => Some(SafeJobResult {
            result_digest: digest
                .parse::<Sha256Digest>()
                .map_err(|_| OperationReadError::CorruptAuthority)?,
        }),
        (PublicJobState::Failed, Some(digest)) => {
            digest
                .parse::<Sha256Digest>()
                .map_err(|_| OperationReadError::CorruptAuthority)?;
            None
        }
        (_, Some(_)) => return Err(OperationReadError::CorruptAuthority),
        (_, None) => None,
    };
    let error = (state == PublicJobState::Failed).then(|| SafeJobFailure {
        code: "operation_failed".to_owned(),
        message: "The operation failed. Inspect authorized audit events for details.".to_owned(),
    });
    let view = OperationViewV1 {
        operation_id: operation_id.clone(),
        tenant_id,
        kind,
        target,
        state,
        progress: None,
        result,
        error,
        created_at: UtcTimestamp::from_datetime(job.created_at),
        updated_at: UtcTimestamp::from_datetime(job.updated_at),
        etag: operation_etag(&operation_id.to_string(), version),
    };
    view.validate()
        .map_err(|_| OperationReadError::CorruptAuthority)?;
    Ok(view)
}

fn classify_authority_error(failure: RepositoryError) -> OperationReadError {
    match failure {
        RepositoryError::Database(_) => OperationReadError::AuthorityUnavailable,
        RepositoryError::PermissionDenied => OperationReadError::Denied,
        RepositoryError::NotFound(_) => OperationReadError::NotFound,
        _ => OperationReadError::CorruptAuthority,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use insight_platform_contracts::{ResourceKind, SchedulerPriority};
    use serde_json::json;

    fn id(kind: ResourceKind, suffix: u16) -> String {
        format!(
            "{}_0198f1cb-32e4-75e1-a9e8-d95ca0f7{suffix:04x}",
            kind.descriptor().prefix
        )
    }

    fn job(state: &str, result_digest: Option<String>) -> JobRecord {
        let now = Utc.with_ymd_and_hms(2026, 8, 21, 0, 0, 0).unwrap();
        JobRecord {
            tenant_id: id(ResourceKind::Tenant, 1),
            job_id: id(ResourceKind::Job, 2),
            job_kind: "artifact_scan".to_owned(),
            work_class: "artifact".to_owned(),
            owner_kind: "artifact".to_owned(),
            owner_id: id(ResourceKind::Artifact, 3),
            trace: insight_platform_contracts::TraceIdentityV1::generate(),
            invocation_id: None,
            run_id: None,
            node_id: None,
            state: state.to_owned(),
            version: 3,
            attempt_no: 0,
            attempt_limit: 1,
            lease_epoch: 0,
            worker_id: None,
            lease_token_digest: None,
            lease_expires_at: None,
            heartbeat_at: None,
            scheduled_at: now,
            retry_at: None,
            deadline: now + chrono::Duration::minutes(1),
            priority: SchedulerPriority::Normal,
            wake_kind: None,
            wake_state: None,
            wake_generation: 0,
            request_digest: format!("sha256:{}", "a".repeat(64)),
            result_digest,
            effect_key_digest: None,
            quota_reservation_id: None,
            payload: crate::repository::TypedPayload {
                schema_version: 1,
                value: json!({"schema_version": 1}),
                digest: format!("sha256:{}", "b".repeat(64)),
            },
            started_at: None,
            terminal_at: None,
            created_at: now,
            updated_at: now,
        }
    }

    #[test]
    fn projection_never_exposes_job_payload_or_lease_evidence() {
        let target = PublicJobTarget::Artifact {
            artifact_id: id(ResourceKind::Artifact, 3).parse().unwrap(),
        };
        let view =
            project_operation(job("running", None), PublicJobKind::ArtifactVerify, target).unwrap();
        let json = serde_json::to_string(&view).unwrap();
        assert!(!json.contains("payload"));
        assert!(!json.contains("lease"));
        assert_eq!(view.state, PublicJobState::Running);
        assert_eq!(view.etag, format!("\"{}-3\"", view.operation_id));
    }

    #[test]
    fn failed_projection_uses_only_a_stable_safe_failure() {
        let target = PublicJobTarget::Artifact {
            artifact_id: id(ResourceKind::Artifact, 3).parse().unwrap(),
        };
        let view =
            project_operation(job("failed", None), PublicJobKind::ArtifactDelete, target).unwrap();
        assert_eq!(view.error.unwrap().code, "operation_failed");
    }
}
