use async_trait::async_trait;
use insight_engine::{
    repository::{RepositoryError, REPOSITORY_CONSTRAINT_CONFLICT},
    ArtifactRef, ContentHash,
};
use sqlx::{PgPool, Postgres, Row, Transaction};
use uuid::Uuid;

use crate::repository::{database_time, PostgresDurableRepository, RepositoryErrorExt as _};

use super::*;

fn constraint_conflict() -> RepositoryError {
    RepositoryError::new(
        REPOSITORY_CONSTRAINT_CONFLICT,
        "terminal artifact staging intent conflicts with durable authority",
    )
}

fn validate_text(value: &str, max_bytes: usize) -> Result<(), RepositoryError> {
    if value.is_empty() || value.len() > max_bytes || value.chars().any(char::is_control) {
        return Err(invalid_data());
    }
    Ok(())
}

fn validate_stage(command: &NewTerminalArtifactStage) -> Result<ArtifactRef, RepositoryError> {
    validate_text(&command.tenant_id, 256)?;
    validate_text(&command.content_ref, 16 * 1024)?;
    validate_text(&command.source_id, 512)?;
    if command.available_at < command.created_at {
        return Err(invalid_data());
    }
    let artifact: ArtifactRef =
        serde_json::from_str(&command.content_ref).map_err(|_| invalid_data())?;
    if artifact.content_hash() != &command.content_hash
        || artifact.media_type() != Some(TERMINAL_SCOPED_ARTIFACT_MEDIA_TYPE)
    {
        return Err(invalid_data());
    }
    Ok(artifact)
}

fn decode_stage(row: &sqlx::postgres::PgRow) -> Result<TerminalArtifactStage, RepositoryError> {
    Ok(TerminalArtifactStage {
        staging_id: row.try_get("staging_id").map_err(|_| invalid_data())?,
        tenant_id: row.try_get("tenant_id").map_err(|_| invalid_data())?,
        content_ref: row.try_get("content_ref").map_err(|_| invalid_data())?,
        content_hash: ContentHash::parse(
            row.try_get::<String, _>("content_hash")
                .map_err(|_| invalid_data())?,
        )
        .map_err(|_| invalid_data())?,
        source_kind: parse_terminal_artifact_source_kind(
            &row.try_get::<String, _>("source_kind")
                .map_err(|_| invalid_data())?,
        )?,
        source_id: row.try_get("source_id").map_err(|_| invalid_data())?,
        attempts: u64::try_from(
            row.try_get::<i64, _>("attempts")
                .map_err(|_| invalid_data())?,
        )
        .map_err(|_| invalid_data())?,
        available_at: row.try_get("available_at").map_err(|_| invalid_data())?,
        created_at: row.try_get("created_at").map_err(|_| invalid_data())?,
    })
}

fn stage_matches(stored: &TerminalArtifactStage, command: &NewTerminalArtifactStage) -> bool {
    stored.staging_id
        == terminal_artifact_staging_id(&command.tenant_id, command.source_kind, &command.source_id)
        && stored.tenant_id == command.tenant_id
        && stored.content_ref == command.content_ref
        && stored.content_hash == command.content_hash
        && stored.source_kind == command.source_kind
        && stored.source_id == command.source_id
}

async fn lock_content_deletion_fence(
    transaction: &mut Transaction<'_, Postgres>,
    content_hash: &ContentHash,
) -> Result<(), RepositoryError> {
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 812493764366122121::bigint))")
        .bind(content_hash.as_str())
        .execute(&mut **transaction)
        .await
        .map_err(RepositoryError::storage)?;
    Ok(())
}

async fn deletion_fence_exists(
    transaction: &mut Transaction<'_, Postgres>,
    content_hash: &ContentHash,
) -> Result<bool, RepositoryError> {
    sqlx::query_scalar::<_, bool>(
        "SELECT
           EXISTS(
                SELECT 1 FROM terminal_content_deletion_jobs
                WHERE content_hash=$1 AND job_state='claimed'
           )
           OR EXISTS(
                SELECT 1 FROM terminal_artifact_staging
                WHERE content_hash=$1 AND staging_state='claimed'
           )",
    )
    .bind(content_hash.as_str())
    .fetch_one(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)
}

pub(crate) async fn consume_terminal_artifact_stage(
    executor: &mut Transaction<'_, Postgres>,
    tenant_id: &str,
    content_ref: &str,
    source_kind: TerminalArtifactSourceKind,
    source_id: &str,
) -> Result<(), RepositoryError> {
    let Ok(artifact) = serde_json::from_str::<ArtifactRef>(content_ref) else {
        // Pre-staging terminal metadata used opaque adapter-specific refs.
        // Only the closed scoped ArtifactRef contract requires staging.
        return Ok(());
    };
    if artifact.media_type() != Some(TERMINAL_SCOPED_ARTIFACT_MEDIA_TYPE) {
        return Ok(());
    }
    let staging_id = terminal_artifact_staging_id(tenant_id, source_kind, source_id);
    let consumed = sqlx::query(
        "DELETE FROM terminal_artifact_staging
         WHERE staging_id=$1 AND tenant_id=$2 AND content_ref=$3
           AND source_kind=$4 AND source_id=$5 AND staging_state='pending'",
    )
    .bind(staging_id)
    .bind(tenant_id)
    .bind(content_ref)
    .bind(terminal_artifact_source_kind_as_str(source_kind))
    .bind(source_id)
    .execute(&mut **executor)
    .await
    .map_err(RepositoryError::storage)?
    .rows_affected();
    if consumed != 1 {
        return Err(constraint_conflict());
    }
    Ok(())
}

async fn referenced_authority_exists(
    executor: &mut Transaction<'_, Postgres>,
    stage: &TerminalArtifactStage,
) -> Result<bool, RepositoryError> {
    sqlx::query_scalar::<_, bool>(
        "SELECT
           EXISTS(SELECT 1 FROM terminal_run_admissions WHERE input_ref=$1)
           OR EXISTS(SELECT 1 FROM terminal_run_results WHERE output_ref=$1)
           OR EXISTS(SELECT 1 FROM conversation_messages WHERE content_ref=$1)
           OR EXISTS(SELECT 1 FROM conversation_summaries WHERE summary_ref=$1)
           OR EXISTS(
                SELECT 1 FROM artifacts
                WHERE content_hash=$2 AND artifact_state<>'deleted'
           )",
    )
    .bind(&stage.content_ref)
    .bind(stage.content_hash.as_str())
    .fetch_one(&mut **executor)
    .await
    .map_err(RepositoryError::storage)
}

#[async_trait]
impl TerminalArtifactStagingStore for PostgresDurableRepository {
    async fn stage_terminal_artifact(
        &self,
        command: NewTerminalArtifactStage,
    ) -> Result<StageTerminalArtifactOutcome, RepositoryError> {
        let artifact = validate_stage(&command)?;
        let staging_id = terminal_artifact_staging_id(
            &command.tenant_id,
            command.source_kind,
            &command.source_id,
        );
        let mut transaction = self.pool.begin().await.map_err(RepositoryError::storage)?;
        lock_content_deletion_fence(&mut transaction, artifact.content_hash()).await?;
        if deletion_fence_exists(&mut transaction, artifact.content_hash()).await? {
            return Err(constraint_conflict());
        }
        let inserted = sqlx::query(
            "INSERT INTO terminal_artifact_staging (
                 staging_id,tenant_id,content_ref,content_hash,source_kind,source_id,
                 staging_state,available_at,attempts,created_at
             ) VALUES ($1,$2,$3,$4,$5,$6,'pending',$7,0,$8)
             ON CONFLICT DO NOTHING",
        )
        .bind(&staging_id)
        .bind(&command.tenant_id)
        .bind(&command.content_ref)
        .bind(command.content_hash.as_str())
        .bind(terminal_artifact_source_kind_as_str(command.source_kind))
        .bind(&command.source_id)
        .bind(database_time(command.available_at))
        .bind(database_time(command.created_at))
        .execute(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?
        .rows_affected()
            == 1;
        let rows = sqlx::query(
            "SELECT staging_id,tenant_id,content_ref,content_hash,source_kind,source_id,
                    staging_state,attempts,available_at,created_at
             FROM terminal_artifact_staging
             WHERE staging_id=$1 OR content_ref=$2
                OR (tenant_id=$3 AND source_kind=$4 AND source_id=$5)
             FOR UPDATE",
        )
        .bind(&staging_id)
        .bind(&command.content_ref)
        .bind(&command.tenant_id)
        .bind(terminal_artifact_source_kind_as_str(command.source_kind))
        .bind(&command.source_id)
        .fetch_all(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?;
        if rows.len() != 1 {
            return Err(constraint_conflict());
        }
        if rows[0]
            .try_get::<String, _>("staging_state")
            .map_err(|_| invalid_data())?
            != "pending"
        {
            return Err(constraint_conflict());
        }
        let stage = decode_stage(&rows[0])?;
        if !stage_matches(&stage, &command) {
            return Err(constraint_conflict());
        }
        transaction
            .commit()
            .await
            .map_err(RepositoryError::storage)?;
        Ok(StageTerminalArtifactOutcome {
            stage,
            replayed: !inserted,
        })
    }

    async fn claim_terminal_artifact_stages(
        &self,
        command: ClaimTerminalArtifactStages,
    ) -> Result<Vec<TerminalArtifactStageClaim>, RepositoryError> {
        validate_text(&command.claimed_by, 256)?;
        if command.limit == 0
            || command.limit > 1_000
            || command.claim_expires_at <= command.observed_at
        {
            return Err(invalid_data());
        }
        let observed_at = database_time(command.observed_at);
        let mut transaction = self.pool.begin().await.map_err(RepositoryError::storage)?;
        let mut ids = sqlx::query_scalar::<_, String>(
            "SELECT staging_id
             FROM terminal_artifact_staging
             WHERE staging_state='pending' AND available_at<=$1
             ORDER BY available_at,created_at,staging_id
             FOR UPDATE SKIP LOCKED
             LIMIT $2",
        )
        .bind(observed_at)
        .bind(i64::from(command.limit))
        .fetch_all(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?;
        let remaining = usize::try_from(command.limit)
            .map_err(|_| invalid_data())?
            .saturating_sub(ids.len());
        if remaining > 0 {
            ids.extend(
                sqlx::query_scalar::<_, String>(
                    "SELECT staging_id
                     FROM terminal_artifact_staging
                     WHERE staging_state='claimed' AND claim_expires_at<=$1
                     ORDER BY claim_expires_at,staging_id
                     FOR UPDATE SKIP LOCKED
                     LIMIT $2",
                )
                .bind(observed_at)
                .bind(i64::try_from(remaining).map_err(|_| invalid_data())?)
                .fetch_all(&mut *transaction)
                .await
                .map_err(RepositoryError::storage)?,
            );
        }
        let mut claims = Vec::with_capacity(ids.len());
        for staging_id in ids {
            let claim_token = format!("terminal_stage_claim_{}", Uuid::new_v4().simple());
            let row = sqlx::query(
                "UPDATE terminal_artifact_staging
                 SET staging_state='claimed',claim_token=$2,claimed_by=$3,
                     claim_expires_at=$4,attempts=attempts+1
                 WHERE staging_id=$1
                 RETURNING staging_id,tenant_id,content_ref,content_hash,source_kind,source_id,
                           attempts,available_at,created_at",
            )
            .bind(&staging_id)
            .bind(&claim_token)
            .bind(&command.claimed_by)
            .bind(database_time(command.claim_expires_at))
            .fetch_one(&mut *transaction)
            .await
            .map_err(RepositoryError::storage)?;
            claims.push(TerminalArtifactStageClaim {
                stage: decode_stage(&row)?,
                claim_token,
            });
        }
        transaction
            .commit()
            .await
            .map_err(RepositoryError::storage)?;
        Ok(claims)
    }

    async fn resolve_terminal_artifact_stage(
        &self,
        command: ResolveTerminalArtifactStage,
    ) -> Result<TerminalArtifactStageDisposition, RepositoryError> {
        validate_text(&command.staging_id, 256)?;
        validate_text(&command.claim_token, 256)?;
        let mut transaction = self.pool.begin().await.map_err(RepositoryError::storage)?;
        let row = sqlx::query(
            "SELECT staging_id,tenant_id,content_ref,content_hash,source_kind,source_id,
                    attempts,available_at,created_at
             FROM terminal_artifact_staging
             WHERE staging_id=$1 AND staging_state='claimed' AND claim_token=$2
             FOR UPDATE",
        )
        .bind(&command.staging_id)
        .bind(&command.claim_token)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?;
        let Some(row) = row else {
            transaction
                .commit()
                .await
                .map_err(RepositoryError::storage)?;
            return Ok(TerminalArtifactStageDisposition::Lost);
        };
        let stage = decode_stage(&row)?;
        lock_content_deletion_fence(&mut transaction, &stage.content_hash).await?;
        if referenced_authority_exists(&mut transaction, &stage).await? {
            sqlx::query(
                "DELETE FROM terminal_artifact_staging
                 WHERE staging_id=$1 AND staging_state='claimed' AND claim_token=$2",
            )
            .bind(&command.staging_id)
            .bind(&command.claim_token)
            .execute(&mut *transaction)
            .await
            .map_err(RepositoryError::storage)?;
            transaction
                .commit()
                .await
                .map_err(RepositoryError::storage)?;
            return Ok(TerminalArtifactStageDisposition::Authoritative);
        }
        transaction
            .commit()
            .await
            .map_err(RepositoryError::storage)?;
        Ok(TerminalArtifactStageDisposition::DeleteOrphan)
    }

    async fn ack_terminal_artifact_stage(
        &self,
        command: AckTerminalArtifactStage,
    ) -> Result<bool, RepositoryError> {
        validate_text(&command.staging_id, 256)?;
        validate_text(&command.claim_token, 256)?;
        Ok(sqlx::query(
            "DELETE FROM terminal_artifact_staging
             WHERE staging_id=$1 AND staging_state='claimed' AND claim_token=$2",
        )
        .bind(command.staging_id)
        .bind(command.claim_token)
        .execute(&self.pool)
        .await
        .map_err(RepositoryError::storage)?
        .rows_affected()
            == 1)
    }
}

#[allow(dead_code)]
fn _pool_is_send_sync(_: &PgPool) {}
