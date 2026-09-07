//! PostgreSQL task queries. Shared locks and atomicity remain in this adapter.
use super::*;
use insight_platform_tasks::{store::TaskAccessRecord, TaskQueryPurpose};

impl PgRepository {
    pub async fn read_task_for_principal(
        &self,
        tenant_id: &ResourceId,
        principal_id: &ResourceId,
        principal_kind: PrincipalKind,
        task_id: &ResourceId,
        purpose: TaskQueryPurpose,
    ) -> Result<TaskAccessRecord, RepositoryError> {
        self.read_task_authority(
            tenant_id,
            principal_id,
            principal_kind,
            task_id,
            purpose,
            false,
        )
        .await
    }

    pub async fn read_task_form_for_principal(
        &self,
        tenant_id: &ResourceId,
        principal_id: &ResourceId,
        principal_kind: PrincipalKind,
        task_id: &ResourceId,
    ) -> Result<TaskAccessRecord, RepositoryError> {
        self.read_task_authority(
            tenant_id,
            principal_id,
            principal_kind,
            task_id,
            TaskQueryPurpose::Respondable,
            true,
        )
        .await
    }

    async fn read_task_authority(
        &self,
        tenant_id: &ResourceId,
        principal_id: &ResourceId,
        principal_kind: PrincipalKind,
        task_id: &ResourceId,
        purpose: TaskQueryPurpose,
        content: bool,
    ) -> Result<TaskAccessRecord, RepositoryError> {
        if !matches!(
            task_id.kind(),
            ResourceKind::Interaction | ResourceKind::ApprovalTask
        ) {
            return Err(RepositoryError::NotFound("task"));
        }
        let mut transaction = begin_read_only_repeatable(&self.pool).await?;
        let principal = load_current_principal_snapshot(
            &mut transaction,
            tenant_id,
            principal_id,
            principal_kind,
        )
        .await?;
        if content
            && !insight_platform_contracts::permits_content_disclosure(
                &principal,
                insight_platform_contracts::ExecutionAuthorizationPurpose::ContentDisclosure,
            )
        {
            return Err(RepositoryError::PermissionDenied);
        }
        let task = load_task_by_text(
            &mut transaction,
            &tenant_id.to_string(),
            &task_id.to_string(),
        )
        .await?;
        let projection = task_projection(&task)?;
        if !insight_platform_tasks::can_query(&projection, &principal, purpose)? {
            return Err(RepositoryError::PermissionDenied);
        }
        let now =
            sqlx::query_scalar::<_, chrono::DateTime<chrono::Utc>>("SELECT clock_timestamp()")
                .fetch_one(&mut *transaction)
                .await?;
        let allowed_actions =
            insight_platform_tasks::allowed_actions(&projection, &principal, now)?;
        transaction.commit().await?;
        Ok(TaskAccessRecord {
            task,
            allowed_actions,
        })
    }
}
