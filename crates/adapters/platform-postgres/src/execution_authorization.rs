//! Purpose-specific authority for closing already admitted work. Callers are
//! authenticated worker/system entry points using their current workload identity;
//! this helper does not turn a public principal into a worker or issue a new effect.

use crate::repository::{require_exact_running_job_fence, RepositoryError};
use chrono::{DateTime, Utc};
use insight_platform_jobs::store::{JobCommandFence, JobRecord};

/// Check a row loaded inside the caller's owner/Job transaction. Revoking the
/// initiating principal's business permission cannot prevent recording a legal
/// result, releasing its quota or performing required cleanup. A revoked worker
/// identity is rejected by workload authentication; another authorized process
/// must acquire its own current fence before calling this completion path.
///
/// This authority never permits reading Artifact/RunValue content or initiating a
/// business action. Those entry points retain current-purpose principal checks.
pub(crate) fn authorize_restricted_job_completion(
    current: &JobRecord,
    fence: &JobCommandFence,
    database_now: DateTime<Utc>,
) -> Result<(), RepositoryError> {
    fence.validate()?;
    require_exact_running_job_fence(current, fence, database_now).map_err(|error| match error {
        RepositoryError::Conflict("running Job fence") => RepositoryError::StaleFence,
        other => other,
    })
}
