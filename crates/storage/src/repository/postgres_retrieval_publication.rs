use super::RepositoryErrorExt as _;

use serde_json::Value;
use sqlx::{AssertSqlSafe, Postgres, Row, Transaction};

use insight_durable::retrieval_publication::adapter as retrieval_adapter;

use insight_engine::RunId;

use super::RepositoryError;

const SELECT_COLUMNS: &str = "run_id,retrieval_id,task_id,activation_id,node_id,attempt_no,\
     retrieval_resource_id,retrieval_resource_version,retrieval_descriptor_hash,query_field,\
     effective_public_policy,effective_public_policy_hash,public_projection,\
     public_projection_hash,completion_transition_key,completion_intent_hash,\
     completion_event_id,completion_event_seq,publication_hash";

pub(crate) async fn insert_retrieval_publication_postgres(
    transaction: &mut Transaction<'_, Postgres>,
    publication: &Value,
) -> Result<(), RepositoryError> {
    validate_publication_artifacts_postgres(transaction, publication).await?;
    let (run_id, retrieval_id, task_id, activation_id, node_id, attempt_no) =
        retrieval_adapter::prepared_retrieval_identity(publication)?;
    let (resource_id, resource_version, descriptor_hash, query_field, policy, policy_hash) =
        retrieval_adapter::prepared_retrieval_resource(publication)?;
    let (public_projection, public_projection_hash) =
        retrieval_adapter::prepared_retrieval_public_projection(publication)?;
    let (transition_key, intent_hash, event_id, event_seq, publication_hash) =
        retrieval_adapter::prepared_retrieval_completion(publication)?;
    let rows = sqlx::query(
        "INSERT INTO workflow_retrieval_publications (
            run_id,retrieval_id,task_id,activation_id,node_id,attempt_no,
            retrieval_resource_id,retrieval_resource_version,retrieval_descriptor_hash,
            query_field,effective_public_policy,effective_public_policy_hash,
            public_projection,public_projection_hash,completion_transition_key,
            completion_intent_hash,completion_event_id,completion_event_seq,publication_hash
         ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19)",
    )
    .bind(run_id)
    .bind(retrieval_id)
    .bind(task_id)
    .bind(activation_id)
    .bind(node_id)
    .bind(i32::try_from(attempt_no).map_err(|_| RepositoryError::invalid_data())?)
    .bind(resource_id)
    .bind(resource_version)
    .bind(descriptor_hash)
    .bind(query_field)
    .bind(policy)
    .bind(policy_hash)
    .bind(public_projection)
    .bind(public_projection_hash)
    .bind(transition_key)
    .bind(intent_hash)
    .bind(event_id)
    .bind(i64::try_from(event_seq).map_err(|_| RepositoryError::invalid_data())?)
    .bind(publication_hash)
    .execute(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?
    .rows_affected();
    if rows != 1 {
        return Err(RepositoryError::invalid_data());
    }
    Ok(())
}

async fn validate_publication_artifacts_postgres(
    transaction: &mut Transaction<'_, Postgres>,
    publication: &Value,
) -> Result<(), RepositoryError> {
    let (run_id, ..) = retrieval_adapter::prepared_retrieval_identity(publication)?;
    let public = retrieval_adapter::prepared_retrieval_public(publication)?;
    for artifact in public
        .iter()
        .flat_map(|retrieval| retrieval.results())
        .filter_map(insight_engine::run_stream::RunRetrievalResult::artifact)
    {
        let row = sqlx::query(
            "SELECT content_hash,size_bytes,media_type,artifact_state
             FROM artifacts WHERE run_id=$1 AND artifact_id=$2 FOR KEY SHARE",
        )
        .bind(run_id)
        .bind(artifact.artifact_id().as_str())
        .fetch_optional(&mut **transaction)
        .await
        .map_err(RepositoryError::storage)?
        .ok_or_else(RepositoryError::invalid_data)?;
        let stored_hash: String = row
            .try_get("content_hash")
            .map_err(|_| RepositoryError::invalid_data())?;
        let stored_size = u64::try_from(
            row.try_get::<i64, _>("size_bytes")
                .map_err(|_| RepositoryError::invalid_data())?,
        )
        .map_err(|_| RepositoryError::invalid_data())?;
        let stored_media_type: Option<String> = row
            .try_get("media_type")
            .map_err(|_| RepositoryError::invalid_data())?;
        let stored_state: String = row
            .try_get("artifact_state")
            .map_err(|_| RepositoryError::invalid_data())?;
        if stored_state != "referenced"
            || stored_hash != artifact.content_hash().as_str()
            || stored_size != artifact.size_bytes()
            || stored_media_type.as_deref() != artifact.media_type()
        {
            return Err(RepositoryError::invalid_data());
        }
    }
    Ok(())
}

pub(crate) async fn validate_exact_retrieval_publication_postgres(
    transaction: &mut Transaction<'_, Postgres>,
    run_id: &RunId,
    task_id: &str,
    expected: Option<&Value>,
) -> Result<(), RepositoryError> {
    let query = format!(
        "SELECT {SELECT_COLUMNS} FROM workflow_retrieval_publications
         WHERE run_id=$1 AND task_id=$2"
    );
    let row = sqlx::query(AssertSqlSafe(query))
        .bind(run_id.as_str())
        .bind(task_id)
        .fetch_optional(&mut **transaction)
        .await
        .map_err(RepositoryError::storage)?;
    match (row, expected) {
        (None, None) => Ok(()),
        (Some(row), Some(expected)) => {
            let stored = decode_row(&row)?;
            retrieval_adapter::validate_exact_retrieval_publication(&stored, expected)
        }
        (None, Some(_)) | (Some(_), None) => Err(RepositoryError::invalid_data()),
    }
}

pub(crate) async fn load_terminal_retrievals_postgres(
    transaction: &mut Transaction<'_, Postgres>,
    run_id: &RunId,
) -> Result<Vec<insight_engine::run_stream::RunRetrieval>, RepositoryError> {
    let query = format!(
        "SELECT {SELECT_COLUMNS} FROM workflow_retrieval_publications
         WHERE run_id=$1
         ORDER BY activation_id COLLATE \"C\",attempt_no,retrieval_id COLLATE \"C\""
    );
    let rows = sqlx::query(AssertSqlSafe(query))
        .bind(run_id.as_str())
        .fetch_all(&mut **transaction)
        .await
        .map_err(RepositoryError::storage)?;
    let mut retrievals = Vec::new();
    for row in rows {
        if let Some(public) =
            retrieval_adapter::stored_retrieval_validate_and_project(&decode_row(&row)?)?
        {
            retrievals.push(public);
        }
    }
    Ok(retrievals)
}

fn decode_row(row: &sqlx::postgres::PgRow) -> Result<Value, RepositoryError> {
    let attempt_no = u32::try_from(
        row.try_get::<i32, _>("attempt_no")
            .map_err(|_| RepositoryError::invalid_data())?,
    )
    .map_err(|_| RepositoryError::invalid_data())?;
    let completion_event_seq = u64::try_from(
        row.try_get::<i64, _>("completion_event_seq")
            .map_err(|_| RepositoryError::invalid_data())?,
    )
    .map_err(|_| RepositoryError::invalid_data())?;
    retrieval_adapter::stored_retrieval_publication(
        text(row, "run_id")?,
        text(row, "retrieval_id")?,
        text(row, "task_id")?,
        text(row, "activation_id")?,
        text(row, "node_id")?,
        attempt_no,
        text(row, "retrieval_resource_id")?,
        text(row, "retrieval_resource_version")?,
        text(row, "retrieval_descriptor_hash")?,
        text(row, "query_field")?,
        row.try_get("effective_public_policy")
            .map_err(|_| RepositoryError::invalid_data())?,
        text(row, "effective_public_policy_hash")?,
        row.try_get("public_projection")
            .map_err(|_| RepositoryError::invalid_data())?,
        row.try_get("public_projection_hash")
            .map_err(|_| RepositoryError::invalid_data())?,
        text(row, "completion_transition_key")?,
        text(row, "completion_intent_hash")?,
        text(row, "completion_event_id")?,
        completion_event_seq,
        text(row, "publication_hash")?,
    )
}

fn text(row: &sqlx::postgres::PgRow, field: &str) -> Result<String, RepositoryError> {
    row.try_get(field)
        .map_err(|_| RepositoryError::invalid_data())
}
