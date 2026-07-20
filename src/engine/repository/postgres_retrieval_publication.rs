use sqlx::{AssertSqlSafe, Postgres, Row, Transaction};

use crate::engine::{ActivationId, AttemptNo, RunId};

use super::{
    retrieval_publication::{
        validate_exact_retrieval_publication, PreparedRetrievalPublication,
        StoredRetrievalPublication,
    },
    RepositoryError,
};

const SELECT_COLUMNS: &str = "run_id,retrieval_id,task_id,activation_id,node_id,attempt_no,\
     retrieval_resource_id,retrieval_resource_version,retrieval_descriptor_hash,query_field,\
     effective_public_policy,effective_public_policy_hash,public_projection,\
     public_projection_hash,completion_transition_key,completion_intent_hash,\
     completion_event_id,completion_event_seq,publication_hash";

pub(crate) async fn insert_retrieval_publication_postgres(
    transaction: &mut Transaction<'_, Postgres>,
    publication: &PreparedRetrievalPublication,
) -> Result<(), RepositoryError> {
    validate_publication_artifacts_postgres(transaction, publication).await?;
    let rows = sqlx::query(
        "INSERT INTO workflow_retrieval_publications (
            run_id,retrieval_id,task_id,activation_id,node_id,attempt_no,
            retrieval_resource_id,retrieval_resource_version,retrieval_descriptor_hash,
            query_field,effective_public_policy,effective_public_policy_hash,
            public_projection,public_projection_hash,completion_transition_key,
            completion_intent_hash,completion_event_id,completion_event_seq,publication_hash
         ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19)",
    )
    .bind(publication.run_id().as_str())
    .bind(publication.retrieval_id())
    .bind(publication.task_id())
    .bind(publication.activation_id().as_str())
    .bind(publication.node_id())
    .bind(
        i32::try_from(publication.attempt_no().get())
            .map_err(|_| RepositoryError::invalid_data())?,
    )
    .bind(publication.resource_id())
    .bind(publication.resource_version())
    .bind(publication.descriptor_hash())
    .bind(publication.query_field())
    .bind(publication.effective_public_policy())
    .bind(publication.effective_public_policy_hash())
    .bind(publication.public_projection())
    .bind(publication.public_projection_hash())
    .bind(publication.completion_transition_key())
    .bind(publication.completion_intent_hash())
    .bind(publication.completion_event_id())
    .bind(
        i64::try_from(publication.completion_event_seq())
            .map_err(|_| RepositoryError::invalid_data())?,
    )
    .bind(publication.publication_hash())
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
    publication: &PreparedRetrievalPublication,
) -> Result<(), RepositoryError> {
    let public = publication.public_retrieval()?;
    for artifact in public
        .iter()
        .flat_map(|retrieval| retrieval.results())
        .filter_map(crate::runtime::response_stream::WorkflowRetrievalResult::artifact)
    {
        let row = sqlx::query(
            "SELECT content_hash,size_bytes,media_type,artifact_state
             FROM artifacts WHERE run_id=$1 AND artifact_id=$2 FOR KEY SHARE",
        )
        .bind(publication.run_id().as_str())
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
    expected: Option<&PreparedRetrievalPublication>,
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
            validate_exact_retrieval_publication(&stored, expected)
        }
        (None, Some(_)) | (Some(_), None) => Err(RepositoryError::invalid_data()),
    }
}

pub(crate) async fn load_terminal_retrievals_postgres(
    transaction: &mut Transaction<'_, Postgres>,
    run_id: &RunId,
) -> Result<Vec<crate::runtime::response_stream::WorkflowRetrieval>, RepositoryError> {
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
        if let Some(public) = decode_row(&row)?.validate_and_project()? {
            retrievals.push(public);
        }
    }
    Ok(retrievals)
}

fn decode_row(row: &sqlx::postgres::PgRow) -> Result<StoredRetrievalPublication, RepositoryError> {
    let attempt_no = u32::try_from(
        row.try_get::<i32, _>("attempt_no")
            .map_err(|_| RepositoryError::invalid_data())?,
    )
    .ok()
    .and_then(|value| AttemptNo::new(value).ok())
    .ok_or_else(RepositoryError::invalid_data)?;
    let completion_event_seq = u64::try_from(
        row.try_get::<i64, _>("completion_event_seq")
            .map_err(|_| RepositoryError::invalid_data())?,
    )
    .map_err(|_| RepositoryError::invalid_data())?;
    Ok(StoredRetrievalPublication {
        run_id: RunId::new(text(row, "run_id")?).map_err(|_| RepositoryError::invalid_data())?,
        retrieval_id: text(row, "retrieval_id")?,
        task_id: text(row, "task_id")?,
        activation_id: ActivationId::new(text(row, "activation_id")?)
            .map_err(|_| RepositoryError::invalid_data())?,
        node_id: text(row, "node_id")?,
        attempt_no,
        resource_id: text(row, "retrieval_resource_id")?,
        resource_version: text(row, "retrieval_resource_version")?,
        descriptor_hash: text(row, "retrieval_descriptor_hash")?,
        query_field: text(row, "query_field")?,
        effective_public_policy: row
            .try_get("effective_public_policy")
            .map_err(|_| RepositoryError::invalid_data())?,
        effective_public_policy_hash: text(row, "effective_public_policy_hash")?,
        public_projection: row
            .try_get("public_projection")
            .map_err(|_| RepositoryError::invalid_data())?,
        public_projection_hash: row
            .try_get("public_projection_hash")
            .map_err(|_| RepositoryError::invalid_data())?,
        completion_transition_key: text(row, "completion_transition_key")?,
        completion_intent_hash: text(row, "completion_intent_hash")?,
        completion_event_id: text(row, "completion_event_id")?,
        completion_event_seq,
        publication_hash: text(row, "publication_hash")?,
    })
}

fn text(row: &sqlx::postgres::PgRow, field: &str) -> Result<String, RepositoryError> {
    row.try_get(field)
        .map_err(|_| RepositoryError::invalid_data())
}
