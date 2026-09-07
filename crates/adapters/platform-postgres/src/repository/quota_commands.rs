//! PostgreSQL quota commands. Shared locks and atomicity remain in this adapter.
use super::*;

impl PgRepository {
    pub async fn create_quota_account(
        &self,
        command: NewQuotaAccount,
    ) -> Result<QuotaAccountRecord, RepositoryError> {
        command.validate()?;
        let row = sqlx::query(
            r#"
            INSERT INTO insight_platform.quota_accounts (
                tenant_id, quota_account_id, scope_kind, scope_id, work_class, metric,
                limit_value, payload_schema_version, payload, payload_digest
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            RETURNING *
            "#,
        )
        .bind(command.tenant_id)
        .bind(command.quota_account_id)
        .bind(command.scope_kind)
        .bind(command.scope_id)
        .bind(command.work_class)
        .bind(command.metric)
        .bind(command.limit_value)
        .bind(command.payload.schema_version)
        .bind(command.payload.value)
        .bind(command.payload.digest)
        .fetch_one(&self.pool)
        .await?;
        quota_account_from_row(row)
    }

    pub async fn reserve_quota(
        &self,
        command: ReserveQuota,
    ) -> Result<QuotaMutationOutcome, RepositoryError> {
        command.validate()?;
        let mut transaction = self.pool.begin().await?;
        if let Some(replay) = existing_quota_entry(
            &mut transaction,
            &command.tenant_id,
            &command.quota_account_id,
            &command.correlation_id,
            "reserve",
            &command.request_digest,
        )
        .await?
        {
            transaction.commit().await?;
            return Ok(QuotaMutationOutcome::Replayed(replay));
        }
        let account = sqlx::query(
            r#"
            UPDATE insight_platform.quota_accounts
            SET reserved_value = reserved_value + $3, version = version + 1,
                updated_at = clock_timestamp()
            WHERE tenant_id = $1 AND quota_account_id = $2
              AND reserved_value + used_value + $3 <= limit_value
            RETURNING *
            "#,
        )
        .bind(&command.tenant_id)
        .bind(&command.quota_account_id)
        .bind(command.amount)
        .fetch_optional(&mut *transaction)
        .await?;
        let Some(account) = account else {
            return Err(RepositoryError::QuotaExceeded);
        };
        let account = quota_account_from_row(account)?;
        insert_quota_entry(
            &mut transaction,
            QuotaEntryInsert {
                tenant_id: &command.tenant_id,
                quota_entry_id: &command.quota_entry_id,
                quota_account_id: &command.quota_account_id,
                correlation_id: &command.correlation_id,
                entry_kind: "reserve",
                reserved_amount: command.amount,
                used_amount: 0,
                account_version: account.version,
                request_digest: &command.request_digest,
            },
        )
        .await?;
        transaction.commit().await?;
        Ok(QuotaMutationOutcome::Applied(account))
    }

    pub async fn settle_quota(
        &self,
        command: SettleQuota,
    ) -> Result<QuotaMutationOutcome, RepositoryError> {
        command.validate()?;
        let mut transaction = self.pool.begin().await?;
        if let Some(replay) = existing_quota_entry(
            &mut transaction,
            &command.tenant_id,
            &command.quota_account_id,
            &command.correlation_id,
            "settle",
            &command.request_digest,
        )
        .await?
        {
            transaction.commit().await?;
            return Ok(QuotaMutationOutcome::Replayed(replay));
        }
        let reserve = sqlx::query(
            r#"
            SELECT reserved_amount FROM insight_platform.quota_ledger
            WHERE tenant_id = $1 AND quota_account_id = $2
              AND correlation_id = $3 AND entry_kind = 'reserve'
            FOR SHARE
            "#,
        )
        .bind(&command.tenant_id)
        .bind(&command.quota_account_id)
        .bind(&command.correlation_id)
        .fetch_optional(&mut *transaction)
        .await?;
        let Some(reserve) = reserve else {
            return Err(RepositoryError::NotFound("quota reservation"));
        };
        let reserved_amount: i64 = reserve.try_get("reserved_amount")?;
        if command.used_amount > reserved_amount {
            return Err(RepositoryError::QuotaExceeded);
        }
        let account = sqlx::query(
            r#"
            UPDATE insight_platform.quota_accounts
            SET reserved_value = reserved_value - $3,
                used_value = used_value + $4,
                version = version + 1, updated_at = clock_timestamp()
            WHERE tenant_id = $1 AND quota_account_id = $2
              AND reserved_value >= $3 AND used_value + $4 <= limit_value
            RETURNING *
            "#,
        )
        .bind(&command.tenant_id)
        .bind(&command.quota_account_id)
        .bind(reserved_amount)
        .bind(command.used_amount)
        .fetch_optional(&mut *transaction)
        .await?;
        let Some(account) = account else {
            return Err(RepositoryError::Conflict("quota account"));
        };
        let account = quota_account_from_row(account)?;
        insert_quota_entry(
            &mut transaction,
            QuotaEntryInsert {
                tenant_id: &command.tenant_id,
                quota_entry_id: &command.quota_entry_id,
                quota_account_id: &command.quota_account_id,
                correlation_id: &command.correlation_id,
                entry_kind: "settle",
                reserved_amount,
                used_amount: command.used_amount,
                account_version: account.version,
                request_digest: &command.request_digest,
            },
        )
        .await?;
        transaction.commit().await?;
        Ok(QuotaMutationOutcome::Applied(account))
    }
}
