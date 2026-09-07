//! Typed, explicitly scoped persisted-object failures. Database/shared authority errors stay fatal.
use crate::repository::RepositoryError;
use insight_platform_contracts::{ResourceId, ResourceKind};
use insight_platform_jobs::store::{SafeScanDiagnostic, SafetyScanDiagnosticCode, SafetyScanPhase};
use sqlx::{postgres::PgRow, Row};

pub(crate) fn identity(
    row: &PgRow,
    item_column: &str,
    item_kind: ResourceKind,
    phase: SafetyScanPhase,
) -> Result<SafeScanDiagnostic, RepositoryError> {
    let tenant_id: ResourceId = row
        .try_get::<String, _>("tenant_id")?
        .parse()
        .map_err(|_| RepositoryError::CorruptRow("invalid scan tenant identity".into()))?;
    let item_id: ResourceId = row
        .try_get::<String, _>(item_column)?
        .parse()
        .map_err(|_| RepositoryError::CorruptRow("invalid scan object identity".into()))?;
    if tenant_id.kind() != ResourceKind::Tenant || item_id.kind() != item_kind {
        return Err(RepositoryError::CorruptRow(
            "invalid scan identity kind".into(),
        ));
    }
    Ok(SafeScanDiagnostic {
        schema_version: 1,
        tenant_id,
        item_id,
        phase,
        code: SafetyScanDiagnosticCode::InvalidPersistedObject,
    })
}

/// Use only around a persisted owning decoder/validator, never a transaction or quota operation.
pub(crate) fn persisted<T>(
    result: Result<T, RepositoryError>,
    diagnostic: &SafeScanDiagnostic,
) -> Result<T, RepositoryError> {
    result.map_err(|error| match error {
        RepositoryError::CorruptRow(_) | RepositoryError::InvalidInput(_) => {
            RepositoryError::InvalidPersistedObject(diagnostic.clone())
        }
        other => other,
    })
}

pub(crate) fn job<T>(
    result: Result<T, RepositoryError>,
    job: &insight_platform_jobs::store::JobRecord,
    phase: SafetyScanPhase,
) -> Result<T, RepositoryError> {
    let tenant_id = job
        .tenant_id
        .parse()
        .map_err(|_| RepositoryError::CorruptRow("invalid Job tenant".into()))?;
    let item_id = job
        .job_id
        .parse()
        .map_err(|_| RepositoryError::CorruptRow("invalid Job identity".into()))?;
    persisted(
        result,
        &SafeScanDiagnostic {
            schema_version: 1,
            tenant_id,
            item_id,
            phase,
            code: SafetyScanDiagnosticCode::InvalidPersistedObject,
        },
    )
}

pub(crate) fn addressed<T>(
    result: Result<T, RepositoryError>,
    tenant_id: &ResourceId,
    item_id: &ResourceId,
    phase: SafetyScanPhase,
) -> Result<T, RepositoryError> {
    persisted(
        result,
        &SafeScanDiagnostic {
            schema_version: 1,
            tenant_id: tenant_id.clone(),
            item_id: item_id.clone(),
            phase,
            code: SafetyScanDiagnosticCode::InvalidPersistedObject,
        },
    )
}

/// A value produced by this transaction violating its owner is an implementation invariant,
/// not pre-existing bad data. It must abort the complete command and cannot be isolated.
pub(crate) fn produced<T>(result: Result<T, RepositoryError>) -> Result<T, RepositoryError> {
    result.map_err(|error| match error {
        RepositoryError::InvalidPersistedObject(_) => {
            RepositoryError::CorruptRow("mutation produced invalid owning state".into())
        }
        other => other,
    })
}

pub(crate) fn observe(diagnostic: &SafeScanDiagnostic) {
    tracing::warn!(
        phase=?diagnostic.phase,code=?diagnostic.code,"invalid persisted scan object retained");
}

pub(crate) fn collect<T>(
    result: Result<T, RepositoryError>,
    diagnostics: &mut Vec<SafeScanDiagnostic>,
) -> Result<Option<T>, RepositoryError> {
    match result {
        Ok(value) => Ok(Some(value)),
        Err(RepositoryError::InvalidPersistedObject(diagnostic)) => {
            diagnostics.push(diagnostic);
            Ok(None)
        }
        Err(error) => Err(error),
    }
}
