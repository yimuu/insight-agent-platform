//! Frozen execution identities. Run is the authority for Program work; a Job's
//! copy is a verified routing projection, never a new definition selection.

use crate::repository::{decode_published_version_payload, payload_from_row, RepositoryError};
use insight_platform_contracts::{
    ExecutionRequirement, ResourceDocument, ResourceId, RunBindingsSnapshot, Sha256Digest,
    EXECUTION_REQUIREMENT_VERSION,
};
use sqlx::{postgres::PgRow, Postgres, Row, Transaction};

pub(crate) struct StoredExecutionRequirement {
    pub version: i32,
    pub value: serde_json::Value,
    pub digest: String,
}

impl StoredExecutionRequirement {
    pub fn new(requirement: &ExecutionRequirement) -> Result<Self, RepositoryError> {
        requirement
            .validate()
            .map_err(|error| RepositoryError::InvalidInput(error.to_string()))?;
        Ok(Self {
            version: EXECUTION_REQUIREMENT_VERSION as i32,
            value: serde_json::to_value(requirement)
                .map_err(|error| RepositoryError::InvalidInput(error.to_string()))?,
            digest: requirement
                .canonical_digest()
                .map_err(|error| RepositoryError::InvalidInput(error.to_string()))?
                .to_string(),
        })
    }
}

/// A newly admitted Run requires publication evidence produced by the compiler
/// validation owner. Existing immutable revisions without it must be republished.
pub(crate) async fn published_program_requirement(
    tx: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    bindings: &RunBindingsSnapshot,
) -> Result<StoredExecutionRequirement, RepositoryError> {
    let row=sqlx::query("SELECT payload_schema_version,payload,payload_digest FROM insight_platform.resource_versions WHERE tenant_id=$1 AND resource_version_id=$2 AND resource_version_kind='agent_plan_revision' AND content_digest=$3")
        .bind(tenant_id.to_string()).bind(bindings.plan.revision_id.to_string()).bind(bindings.plan.semantic_digest.to_string())
        .fetch_optional(&mut **tx).await?.ok_or(RepositoryError::NotFound("exact Program revision"))?;
    let payload = payload_from_row(&row, "payload_schema_version", "payload", "payload_digest")?;
    let published = decode_published_version_payload(&payload)?;
    let requirement=published.validation.program_requirement.as_ref().ok_or_else(||RepositoryError::InvalidInput(
        "Agent publication has no frozen Program execution evidence; validate and republish it before new Run admission".into()))?;
    match (&published.document, requirement) {
        (
            ResourceDocument::Agent(agent),
            ExecutionRequirement::Program {
                definition_digest,
                program_semantic_identity,
                ir_abi_version,
            },
        ) => {
            let known =
                insight_platform_plan::execution::program_semantic_identity(*ir_abi_version)
                    .map_err(|_| {
                        RepositoryError::InvalidInput(
                            "published Program ABI is not supported by this release".into(),
                        )
                    })?;
            if definition_digest != &agent.typed_plan_digest || &known != program_semantic_identity
            {
                return Err(RepositoryError::CorruptRow(
                    "published Program execution evidence disagrees with its exact definition"
                        .into(),
                ));
            }
        }
        _ => {
            return Err(RepositoryError::CorruptRow(
                "Agent publication contains a non-Program requirement".into(),
            ))
        }
    }
    StoredExecutionRequirement::new(requirement)
}

pub(crate) fn requirement_from_row(row: &PgRow) -> Result<ExecutionRequirement, RepositoryError> {
    let version: i32 = row.try_get("execution_requirement_version")?;
    let requirement: ExecutionRequirement =
        serde_json::from_value(row.try_get("execution_requirement")?)
            .map_err(|error| RepositoryError::CorruptRow(error.to_string()))?;
    let digest: String = row.try_get("execution_requirement_digest")?;
    if version != EXECUTION_REQUIREMENT_VERSION as i32
        || requirement.validate().is_err()
        || requirement
            .canonical_digest()
            .map_err(|error| RepositoryError::CorruptRow(error.to_string()))?
            .as_str()
            != digest
    {
        return Err(RepositoryError::CorruptRow(
            "frozen execution requirement checksum/version mismatch".into(),
        ));
    }
    Ok(requirement)
}

/// Called inside the start transaction after the Job row is fenced and before
/// commit. Build provenance is part of that start version, not a new transition.
pub(crate) async fn record_attempt_build(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    job: &insight_platform_jobs::store::JobRecord,
    build: &Sha256Digest,
) -> Result<insight_platform_jobs::store::JobRecord, RepositoryError> {
    let row=sqlx::query("UPDATE insight_platform.jobs SET attempt_build_digest=$6 WHERE tenant_id=$1 AND job_id=$2 AND version=$3 AND worker_id=$4 AND lease_epoch=$5 AND state IN ('leased','running') RETURNING *")
        .bind(&job.tenant_id).bind(&job.job_id).bind(job.version).bind(&job.worker_id).bind(job.lease_epoch).bind(build.to_string())
        .fetch_optional(&mut **tx).await?.ok_or(RepositoryError::Conflict("worker build start fence"))?;
    crate::repository::job_from_row(row)
}
