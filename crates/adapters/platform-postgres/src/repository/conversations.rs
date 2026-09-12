//! Conversation roots serialize submission, while Run remains execution/body authority.
use super::*;
use insight_platform_contracts::conversation::*;
use insight_platform_contracts::{
    permits_content_disclosure, ExecutionAuthorizationPurpose, PrincipalKind, UtcTimestamp,
};

#[derive(Debug, Clone)]
pub struct ConversationReadScope {
    pub tenant_id: ResourceId,
    pub principal_id: ResourceId,
    pub principal_kind: PrincipalKind,
}
fn bad() -> RepositoryError {
    RepositoryError::InvalidInput("invalid conversation request".into())
}
fn corrupt() -> RepositoryError {
    RepositoryError::CorruptRow("invalid conversation authority".into())
}
fn stamp(value: DateTime<Utc>) -> Result<UtcTimestamp, RepositoryError> {
    Ok(UtcTimestamp::from_datetime(value))
}
fn conversation_row(row: &sqlx::postgres::PgRow) -> Result<ConversationViewV1, RepositoryError> {
    Ok(ConversationViewV1 {
        schema_version: 1,
        conversation_id: row
            .try_get::<String, _>("conversation_id")?
            .parse()
            .map_err(|_| corrupt())?,
        agent_id: row
            .try_get::<String, _>("agent_id")?
            .parse()
            .map_err(|_| corrupt())?,
        agent_deployment: ExactDeploymentRef::new(
            row.try_get::<String, _>("agent_deployment_id")?
                .parse()
                .map_err(|_| corrupt())?,
            row.try_get::<String, _>("deployment_digest")?
                .parse()
                .map_err(|_| corrupt())?,
        )
        .map_err(|_| corrupt())?,
        input_field: row.try_get("input_field")?,
        input_schema_digest: row
            .try_get::<String, _>("input_schema_digest")?
            .parse()
            .map_err(|_| corrupt())?,
        title: row.try_get("title")?,
        created_by: row
            .try_get::<String, _>("created_by")?
            .parse()
            .map_err(|_| corrupt())?,
        version: u64::try_from(row.try_get::<i64, _>("version")?).map_err(|_| corrupt())?,
        turn_count: u32::try_from(row.try_get::<i32, _>("turn_count")?).map_err(|_| corrupt())?,
        created_at: stamp(row.try_get("created_at")?)?,
        updated_at: stamp(row.try_get("updated_at")?)?,
    })
}
fn turn_row(row: &sqlx::postgres::PgRow) -> Result<ConversationTurnViewV1, RepositoryError> {
    Ok(ConversationTurnViewV1 {
        schema_version: 1,
        conversation_id: row
            .try_get::<String, _>("conversation_id")?
            .parse()
            .map_err(|_| corrupt())?,
        ordinal: u32::try_from(row.try_get::<i32, _>("ordinal")?).map_err(|_| corrupt())?,
        run_id: row
            .try_get::<String, _>("run_id")?
            .parse()
            .map_err(|_| corrupt())?,
        history_through: u32::try_from(row.try_get::<i32, _>("history_through")?)
            .map_err(|_| corrupt())?,
        conversation_version: u64::try_from(row.try_get::<i64, _>("conversation_version")?)
            .map_err(|_| corrupt())?,
        created_at: stamp(row.try_get("created_at")?)?,
    })
}
async fn authorize(
    tx: &mut Transaction<'_, Postgres>,
    scope: &ConversationReadScope,
    body: bool,
) -> Result<PrincipalSnapshot, RepositoryError> {
    let principal = load_current_principal_snapshot(
        tx,
        &scope.tenant_id,
        &scope.principal_id,
        scope.principal_kind,
    )
    .await?;
    if !principal.permissions.contains(Permission::RuntimeRead)
        || (body
            && !permits_content_disclosure(
                &principal,
                ExecutionAuthorizationPurpose::ContentDisclosure,
            ))
    {
        return Err(RepositoryError::PermissionDenied);
    }
    Ok(principal)
}
async fn read_one(
    tx: &mut Transaction<'_, Postgres>,
    tenant: &ResourceId,
    id: &ResourceId,
    lock: bool,
) -> Result<ConversationViewV1, RepositoryError> {
    if id.kind() != ResourceKind::Conversation {
        return Err(bad());
    }
    let sql = if lock {
        "SELECT * FROM insight_platform.conversations WHERE tenant_id=$1 AND conversation_id=$2 FOR UPDATE"
    } else {
        "SELECT * FROM insight_platform.conversations WHERE tenant_id=$1 AND conversation_id=$2"
    };
    conversation_row(
        &sqlx::query(sql)
            .bind(tenant.to_string())
            .bind(id.to_string())
            .fetch_optional(&mut **tx)
            .await?
            .ok_or(RepositoryError::NotFound("conversation"))?,
    )
}
impl PgRepository {
    pub async fn conversation_snapshot_at(&self) -> Result<DateTime<Utc>, RepositoryError> {
        Ok(sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(&self.pool)
            .await?)
    }
    pub async fn read_conversation(
        &self,
        scope: &ConversationReadScope,
        id: &ResourceId,
    ) -> Result<ConversationViewV1, RepositoryError> {
        let mut tx = begin_read_only_repeatable(&self.pool).await?;
        authorize(&mut tx, scope, false).await?;
        let result = read_one(&mut tx, &scope.tenant_id, id, false).await?;
        tx.commit().await?;
        Ok(result)
    }
    pub async fn list_conversations(
        &self,
        scope: &ConversationReadScope,
        agent_id: Option<&ResourceId>,
        snapshot_at: DateTime<Utc>,
        after: Option<(DateTime<Utc>, ResourceId)>,
        limit: u16,
    ) -> Result<Vec<ConversationViewV1>, RepositoryError> {
        if limit == 0 || limit > MAX_CONVERSATION_PAGE_SIZE + 1 {
            return Err(bad());
        }
        let mut tx = begin_read_only_repeatable(&self.pool).await?;
        authorize(&mut tx, scope, false).await?;
        let rows=sqlx::query("SELECT * FROM insight_platform.conversations WHERE tenant_id=$1 AND ($2::text IS NULL OR agent_id=$2) AND created_at<=$3 AND ($4::timestamptz IS NULL OR (created_at,conversation_id)>($4,$5)) ORDER BY created_at,conversation_id LIMIT $6")
            .bind(scope.tenant_id.to_string()).bind(agent_id.map(ToString::to_string)).bind(snapshot_at).bind(after.as_ref().map(|x|x.0)).bind(after.as_ref().map(|x|x.1.to_string())).bind(i32::from(limit)).fetch_all(&mut *tx).await?;
        let result = rows.iter().map(conversation_row).collect();
        tx.commit().await?;
        result
    }
    pub async fn list_conversation_turns(
        &self,
        scope: &ConversationReadScope,
        id: &ResourceId,
        snapshot_at: DateTime<Utc>,
        after_ordinal: u32,
        limit: u16,
    ) -> Result<Vec<ConversationTurnViewV1>, RepositoryError> {
        if limit == 0
            || limit > MAX_CONVERSATION_PAGE_SIZE + 1
            || after_ordinal > MAX_CONVERSATION_TURNS
        {
            return Err(bad());
        }
        let mut tx = begin_read_only_repeatable(&self.pool).await?;
        authorize(&mut tx, scope, false).await?;
        read_one(&mut tx, &scope.tenant_id, id, false).await?;
        let rows=sqlx::query("SELECT * FROM insight_platform.conversation_turns WHERE tenant_id=$1 AND conversation_id=$2 AND created_at<=$3 AND ordinal>$4 ORDER BY ordinal LIMIT $5")
            .bind(scope.tenant_id.to_string()).bind(id.to_string()).bind(snapshot_at).bind(after_ordinal as i32).bind(i32::from(limit)).fetch_all(&mut *tx).await?;
        let result = rows.iter().map(turn_row).collect();
        tx.commit().await?;
        result
    }
    pub async fn create_conversation(
        &self,
        audit: CommandAudit,
        conversation_id: ResourceId,
        agent_id: ResourceId,
        title: String,
    ) -> Result<CommandOutcome<ConversationViewV1>, RepositoryError> {
        if conversation_id.kind() != ResourceKind::Conversation
            || agent_id.kind() != ResourceKind::Agent
            || title.trim().is_empty()
            || title.len() > MAX_CONVERSATION_TITLE_BYTES
            || title.chars().any(char::is_control)
        {
            return Err(bad());
        }
        let mut tx = self.pool.begin().await?;
        sqlx::query("SELECT set_config('statement_timeout','10s',true),set_config('lock_timeout','5s',true)").execute(&mut *tx).await?;
        audit.validate_at(Utc::now()).map_err(|_| bad())?;
        require_tenant_permission(&mut tx, &audit, Permission::AgentRun).await?;
        authorize(
            &mut tx,
            &ConversationReadScope {
                tenant_id: audit.tenant_id.clone(),
                principal_id: audit.principal_id.clone(),
                principal_kind: audit.principal_kind,
            },
            true,
        )
        .await?;
        let scope = audit.tenant_id.to_string();
        if claim_command_receipt(
            &mut tx,
            &audit,
            "conversation_collection",
            &scope,
            "conversation.create",
        )
        .await?
        {
            let id = load_command_receipt_response_reference(
                &mut tx,
                &audit,
                "conversation_collection",
                &scope,
                "conversation.create",
            )
            .await?
            .parse()
            .map_err(|_| corrupt())?;
            let result = read_one(&mut tx, &audit.tenant_id, &id, false).await?;
            tx.commit().await?;
            return Ok(CommandOutcome::Replayed(result));
        }
        // Collection creation takes tenant before the Agent resource; no Run/Job is held here.
        sqlx::query("SELECT tenant_id FROM insight_platform.tenants WHERE tenant_id=$1 FOR UPDATE")
            .bind(&scope)
            .fetch_one(&mut *tx)
            .await?;
        let count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM insight_platform.conversations WHERE tenant_id=$1",
        )
        .bind(&scope)
        .fetch_one(&mut *tx)
        .await?;
        if count >= MAX_CONVERSATIONS_PER_TENANT {
            return Err(RepositoryError::Conflict("conversation capacity"));
        }
        let row=sqlx::query("SELECT d.* FROM insight_platform.resources r JOIN insight_platform.deployments d ON d.tenant_id=r.tenant_id AND d.deployment_id=r.active_deployment_id WHERE r.tenant_id=$1 AND r.resource_id=$2 AND r.resource_kind='agent' AND r.lifecycle_state='active' AND r.gate_state='enabled' FOR SHARE OF r")
            .bind(&scope).bind(agent_id.to_string()).fetch_optional(&mut *tx).await?.ok_or(RepositoryError::NotFound("active agent deployment"))?;
        let deployment = deployment_from_row(row)?;
        let DeploymentClosure::Agent(closure) = decode_deployment_closure(&deployment.bindings)?
        else {
            return Err(corrupt());
        };
        let payload = crate::invocation_repository::load_enabled_exact_published_version(
            &mut tx,
            &audit.tenant_id,
            &closure.interface,
            RegistryResourceKind::Agent,
        )
        .await?;
        let ResourceDocument::Agent(agent) = payload.document else {
            return Err(corrupt());
        };
        let field = conversation_input_field(&agent.input_schema, &agent.output_schema)
            .ok_or(RepositoryError::Conflict("Agent is not chat compatible"))?;
        sqlx::query("INSERT INTO insight_platform.conversations(tenant_id,conversation_id,agent_id,agent_deployment_id,deployment_digest,input_field,input_schema_digest,title,created_by) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)")
            .bind(&scope).bind(conversation_id.to_string()).bind(agent_id.to_string()).bind(deployment.deployment_id).bind(deployment.bindings.digest).bind(field).bind(agent.input_schema.canonical_digest.to_string()).bind(title).bind(audit.principal_id.to_string()).execute(&mut *tx).await?;
        terminalize_command_receipt(&mut tx, &audit, &conversation_id.to_string(), "applied")
            .await?;
        let result = read_one(&mut tx, &audit.tenant_id, &conversation_id, false).await?;
        tx.commit().await?;
        Ok(CommandOutcome::Applied(result))
    }
}
impl PgRunTransaction {
    pub async fn admit_conversation_turn(
        &mut self,
        conversation_id: &ResourceId,
        expected_version: u64,
        command: AdmitRun,
    ) -> Result<CommandOutcome<ConversationTurnViewV1>, RepositoryError> {
        sqlx::query("SAVEPOINT conversation_submission")
            .execute(&mut *self.transaction)
            .await?;
        sqlx::query("SELECT set_config('statement_timeout','10s',true),set_config('lock_timeout','5s',true)").execute(&mut *self.transaction).await?;
        let result = self
            .admit_conversation_turn_inner(conversation_id, expected_version, command)
            .await;
        if result.is_err() {
            sqlx::query("ROLLBACK TO SAVEPOINT conversation_submission")
                .execute(&mut *self.transaction)
                .await?;
        }
        sqlx::query("RELEASE SAVEPOINT conversation_submission")
            .execute(&mut *self.transaction)
            .await?;
        result
    }
    async fn admit_conversation_turn_inner(
        &mut self,
        conversation_id: &ResourceId,
        expected_version: u64,
        command: AdmitRun,
    ) -> Result<CommandOutcome<ConversationTurnViewV1>, RepositoryError> {
        // The enclosing transaction owns this savepoint and the Conversation lock until commit.
        let scope = ConversationReadScope {
            tenant_id: command.audit.tenant_id.clone(),
            principal_id: command.audit.principal_id.clone(),
            principal_kind: command.audit.principal_kind,
        };
        authorize(&mut self.transaction, &scope, true).await?;
        require_tenant_permission(&mut self.transaction, &command.audit, Permission::AgentRun)
            .await?;
        command.validate_shape()?;
        // Claim command identity before the parent root lock (global Receipt -> root order).
        if let Some(run) = claim_run_admission_receipt(
            &mut self.transaction,
            &command.audit,
            &command.admission_scope_id,
        )
        .await?
        {
            let row=sqlx::query("SELECT * FROM insight_platform.conversation_turns WHERE tenant_id=$1 AND conversation_id=$2 AND run_id=$3").bind(scope.tenant_id.to_string()).bind(conversation_id.to_string()).bind(run.run_id).fetch_optional(&mut *self.transaction).await?.ok_or(RepositoryError::IdempotencyConflict)?;
            return Ok(CommandOutcome::Replayed(turn_row(&row)?));
        }
        let conversation = read_one(
            &mut self.transaction,
            &scope.tenant_id,
            conversation_id,
            true,
        )
        .await?;
        if expected_version != conversation.version {
            return Err(RepositoryError::Conflict("conversation version"));
        }
        if conversation.turn_count >= MAX_CONVERSATION_TURNS {
            return Err(RepositoryError::Conflict("conversation capacity"));
        }
        if command.admission_scope_id != conversation.agent_id
            || command.bindings.agent != conversation.agent_deployment
            || command.expected_agent_deployment.as_ref() != Some(&conversation.agent_deployment)
            || command.input.schema_digest != conversation.input_schema_digest
        {
            return Err(RepositoryError::Conflict("conversation deployment changed"));
        }
        let ValueRef::Inline { value } = &command.input.value else {
            return Err(bad());
        };
        let payload = crate::invocation_repository::load_enabled_exact_published_version(
            &mut self.transaction,
            &scope.tenant_id,
            &command.bindings.agent_interface,
            RegistryResourceKind::Agent,
        )
        .await?;
        let ResourceDocument::Agent(agent) = payload.document else {
            return Err(corrupt());
        };
        if command.input.classification != agent.input_classification {
            return Err(bad());
        }
        agent
            .input_schema
            .validate_instance(value)
            .map_err(|_| bad())?;
        let object = value.as_object().ok_or_else(bad)?;
        let message = object
            .get(&conversation.input_field)
            .and_then(Value::as_str)
            .ok_or_else(bad)?;
        if object.len() != 1 || !validate_conversation_message(message) {
            return Err(bad());
        }
        if conversation.turn_count > 0 {
            let state:String=sqlx::query_scalar("SELECT r.state FROM insight_platform.conversation_turns t JOIN insight_platform.runs r ON r.tenant_id=t.tenant_id AND r.run_id=t.run_id WHERE t.tenant_id=$1 AND t.conversation_id=$2 AND t.ordinal=$3").bind(scope.tenant_id.to_string()).bind(conversation_id.to_string()).bind(conversation.turn_count as i32).fetch_one(&mut *self.transaction).await?;
            if !matches!(
                state.as_str(),
                "succeeded" | "failed" | "cancelled" | "timed_out"
            ) {
                return Err(RepositoryError::Conflict("conversation busy"));
            }
        }
        // Preflight the same immutable successful values the controller will read again.
        let history = load_history(
            &mut self.transaction,
            &scope,
            conversation_id,
            conversation.turn_count,
            &conversation.input_field,
        )
        .await?;
        if history.iter().map(|x| x.budget_bytes).sum::<usize>() + message.len()
            > MAX_CONVERSATION_HISTORY_BYTES
        {
            return Err(RepositoryError::Conflict("conversation history capacity"));
        }
        let run_id = command.run_id.clone();
        self.admit_run_inner(command, true).await?;
        let row=sqlx::query("INSERT INTO insight_platform.conversation_turns(tenant_id,conversation_id,ordinal,run_id,history_through,conversation_version) VALUES($1,$2,$3,$4,$5,$6) RETURNING *")
            .bind(scope.tenant_id.to_string()).bind(conversation_id.to_string()).bind((conversation.turn_count+1) as i32).bind(run_id.to_string()).bind(conversation.turn_count as i32).bind((conversation.version+1) as i64).fetch_one(&mut *self.transaction).await?;
        sqlx::query("UPDATE insight_platform.conversations SET version=version+1,turn_count=turn_count+1,updated_at=clock_timestamp() WHERE tenant_id=$1 AND conversation_id=$2").bind(scope.tenant_id.to_string()).bind(conversation_id.to_string()).execute(&mut *self.transaction).await?;
        Ok(CommandOutcome::Applied(turn_row(&row)?))
    }
}

/// Exact immutable references; Artifact bytes are materialized only through the fenced broker.
#[derive(Debug, Clone)]
pub struct ConversationHistoryValue {
    pub value_id: ResourceId,
    pub content_digest: Sha256Digest,
    pub classification: DataClassification,
    pub inline: Option<Value>,
    pub budget_bytes: usize,
    pub field: String,
    pub user: bool,
    pub ordinal: u32,
}
impl ConversationHistoryValue {
    pub fn into_block(
        self,
        value: Value,
    ) -> Result<insight_platform_models::PromptAssemblyBlock, RepositoryError> {
        if canonical_digest(&value).map_err(|_| corrupt())? != self.content_digest.to_string() {
            return Err(corrupt());
        }
        let text = value
            .get(&self.field)
            .and_then(Value::as_str)
            .ok_or(RepositoryError::Conflict("conversation answer is not text"))?
            .to_owned();
        if text.is_empty() || text.len() > MAX_CONVERSATION_HISTORY_BYTES {
            return Err(RepositoryError::Conflict("conversation history capacity"));
        }
        Ok(insight_platform_models::PromptAssemblyBlock {
            phase: insight_platform_models::PromptAssemblyPhase::ConversationHistory,
            history_role: Some(if self.user {
                insight_platform_models::ConversationHistoryRole::User
            } else {
                insight_platform_models::ConversationHistoryRole::Assistant
            }),
            ordinal: self.ordinal,
            source_kind: "conversation_run_value".into(),
            source_id: self.value_id.to_string(),
            source_digest: self.content_digest,
            classification: self.classification,
            byte_budget: text.len() as u32,
            token_budget: insight_platform_contracts::estimate_model_text_tokens(text.len() as u32)
                .ok_or_else(bad)?,
            text,
        })
    }
}
async fn load_history(
    tx: &mut Transaction<'_, Postgres>,
    scope: &ConversationReadScope,
    id: &ResourceId,
    through: u32,
    input_field: &str,
) -> Result<Vec<ConversationHistoryValue>, RepositoryError> {
    authorize(tx, scope, true).await?;
    // Reject the bounded serialized corpus before fetching any historical JSON bodies.
    let stored_bytes:i64=sqlx::query_scalar("SELECT COALESCE(sum(CASE WHEN v.inline_value IS NOT NULL THEN octet_length(v.inline_value::text) ELSE COALESCE(b.size_bytes,$4::bigint) END),0)::bigint FROM insight_platform.conversation_turns t JOIN insight_platform.runs r ON r.tenant_id=t.tenant_id AND r.run_id=t.run_id JOIN insight_platform.run_values v ON v.tenant_id=r.tenant_id AND v.run_id=r.run_id AND v.value_id IN (r.input_value_id,r.output_value_id) LEFT JOIN insight_platform.artifacts a ON a.tenant_id=v.tenant_id AND a.artifact_id=v.artifact_id LEFT JOIN insight_platform.artifact_blobs b ON b.tenant_id=a.tenant_id AND b.blob_id=a.blob_id WHERE t.tenant_id=$1 AND t.conversation_id=$2 AND t.ordinal<=$3 AND r.state='succeeded'")
        .bind(scope.tenant_id.to_string()).bind(id.to_string()).bind(through as i32).bind((MAX_CONVERSATION_HISTORY_BYTES+1) as i64).fetch_one(&mut **tx).await?;
    if stored_bytes < 0 || stored_bytes as usize > MAX_CONVERSATION_HISTORY_BYTES {
        return Err(RepositoryError::Conflict("conversation history capacity"));
    }
    let rows=sqlx::query("SELECT t.ordinal,v.value_id,v.value_kind,v.classification,v.content_digest,v.inline_value,b.size_bytes FROM insight_platform.conversation_turns t JOIN insight_platform.runs r ON r.tenant_id=t.tenant_id AND r.run_id=t.run_id JOIN insight_platform.run_values v ON v.tenant_id=r.tenant_id AND v.run_id=r.run_id AND v.value_id IN (r.input_value_id,r.output_value_id) LEFT JOIN insight_platform.artifacts a ON a.tenant_id=v.tenant_id AND a.artifact_id=v.artifact_id AND a.state='ready' LEFT JOIN insight_platform.artifact_blobs b ON b.tenant_id=a.tenant_id AND b.blob_id=a.blob_id AND b.state='verified' AND b.deleted_at IS NULL WHERE t.tenant_id=$1 AND t.conversation_id=$2 AND t.ordinal<=$3 AND r.state='succeeded' ORDER BY t.ordinal, CASE WHEN v.value_id=r.input_value_id THEN 0 ELSE 1 END")
        .bind(scope.tenant_id.to_string()).bind(id.to_string()).bind(through as i32).fetch_all(&mut **tx).await?;
    let mut values = Vec::new();
    let mut bytes = 0usize;
    for row in rows {
        let inline: Option<Value> = row.try_get("inline_value")?;
        let user = row.try_get::<String, _>("value_kind")? == "run_input";
        let field = if user { input_field } else { "answer" }.to_owned();
        let budget_bytes = match &inline {
            Some(v) => v
                .get(&field)
                .and_then(Value::as_str)
                .ok_or(RepositoryError::Conflict("conversation answer is not text"))?
                .len(),
            None => usize::try_from(row.try_get::<Option<i64>, _>("size_bytes")?.ok_or(
                RepositoryError::Conflict("conversation historical artifact unavailable"),
            )?)
            .map_err(|_| corrupt())?,
        };
        bytes = bytes.checked_add(budget_bytes).ok_or_else(bad)?;
        if bytes > MAX_CONVERSATION_HISTORY_BYTES {
            return Err(RepositoryError::Conflict("conversation history capacity"));
        }
        let item = ConversationHistoryValue {
            value_id: row
                .try_get::<String, _>("value_id")?
                .parse()
                .map_err(|_| corrupt())?,
            content_digest: row
                .try_get::<String, _>("content_digest")?
                .parse()
                .map_err(|_| corrupt())?,
            classification: row
                .try_get::<String, _>("classification")?
                .parse()
                .map_err(|_| corrupt())?,
            inline,
            budget_bytes,
            field,
            user,
            ordinal: values.len() as u32,
        };
        if let Some(value) = item.inline.clone() {
            item.clone().into_block(value)?;
        }
        values.push(item);
    }
    if values.len() % 2 != 0 {
        return Err(corrupt());
    }
    Ok(values)
}
impl PgRepository {
    pub async fn load_conversation_history_for_run(
        &self,
        tenant: &ResourceId,
        run_id: &ResourceId,
    ) -> Result<Vec<ConversationHistoryValue>, RepositoryError> {
        let mut tx = begin_read_only_repeatable(&self.pool).await?;
        let row=sqlx::query("SELECT c.conversation_id,c.input_field,t.history_through FROM insight_platform.conversation_turns t JOIN insight_platform.conversations c ON c.tenant_id=t.tenant_id AND c.conversation_id=t.conversation_id WHERE t.tenant_id=$1 AND t.run_id=$2")
            .bind(tenant.to_string()).bind(run_id.to_string()).fetch_optional(&mut *tx).await?;
        let Some(row) = row else {
            tx.commit().await?;
            return Ok(Vec::new());
        };
        let run = load_run(&mut tx, tenant, run_id).await?;
        let scope = ConversationReadScope {
            tenant_id: tenant.clone(),
            principal_id: run.bindings.principal.principal_id.clone(),
            principal_kind: run.bindings.principal.principal_kind,
        };
        let id = row
            .try_get::<String, _>("conversation_id")?
            .parse()
            .map_err(|_| corrupt())?;
        let values = load_history(
            &mut tx,
            &scope,
            &id,
            row.try_get::<i32, _>("history_through")? as u32,
            &row.try_get::<String, _>("input_field")?,
        )
        .await?;
        tx.commit().await?;
        Ok(values)
    }
}
impl PgRepository {
    pub async fn read_conversation_turn_replay(
        &self,
        scope: &ConversationReadScope,
        id: &ResourceId,
        key: &Sha256Digest,
        digest: &Sha256Digest,
    ) -> Result<Option<ConversationTurnViewV1>, RepositoryError> {
        let mut tx = self.pool.begin().await?;
        sqlx::query("SELECT set_config('statement_timeout','10s',true),set_config('lock_timeout','5s',true)").execute(&mut *tx).await?;
        let principal = authorize(&mut tx, scope, true).await?;
        if !principal.permissions.contains(Permission::AgentRun) {
            return Err(RepositoryError::PermissionDenied);
        }
        let conversation = read_one(&mut tx, &scope.tenant_id, id, false).await?;
        let row=sqlx::query("SELECT request_digest,state,response_reference_id,payload_schema_version,payload,payload_digest FROM insight_platform.receipts WHERE tenant_id=$1 AND receipt_kind='command' AND scope_kind='run_admission' AND scope_id=$2 AND dedupe_owner_id=$3 AND operation='run.admit' AND idempotency_key_digest=$4 FOR UPDATE")
            .bind(scope.tenant_id.to_string()).bind(conversation.agent_id.to_string()).bind(scope.principal_id.to_string()).bind(key.to_string()).fetch_optional(&mut *tx).await?;
        let Some(row) = row else {
            tx.commit().await?;
            return Ok(None);
        };
        if row.try_get::<String, _>("request_digest")? != digest.to_string() {
            return Err(RepositoryError::IdempotencyConflict);
        }
        if row.try_get::<String, _>("state")? != "succeeded" {
            return Err(RepositoryError::Conflict("conversation receipt"));
        }
        let payload =
            payload_from_row(&row, "payload_schema_version", "payload", "payload_digest")?;
        let receipt: RunAdmissionReceiptResult =
            decode_versioned_payload(&payload, "run admission Receipt result")?;
        validate_run_admission_receipt_result(&receipt, &scope.tenant_id)?;
        let run: String = row.try_get("response_reference_id")?;
        if receipt.run.run_id != run {
            return Err(corrupt());
        }
        let turn=sqlx::query("SELECT * FROM insight_platform.conversation_turns WHERE tenant_id=$1 AND conversation_id=$2 AND run_id=$3").bind(scope.tenant_id.to_string()).bind(id.to_string()).bind(run).fetch_optional(&mut *tx).await?.ok_or(RepositoryError::IdempotencyConflict)?;
        let result = turn_row(&turn)?;
        tx.commit().await?;
        Ok(Some(result))
    }
    pub async fn read_conversation_input_classification(
        &self,
        scope: &ConversationReadScope,
        id: &ResourceId,
    ) -> Result<DataClassification, RepositoryError> {
        let mut tx = begin_read_only_repeatable(&self.pool).await?;
        authorize(&mut tx, scope, true).await?;
        let conversation = read_one(&mut tx, &scope.tenant_id, id, false).await?;
        let deployment = load_deployment(
            &mut tx,
            &scope.tenant_id,
            &conversation.agent_deployment.deployment_id,
        )
        .await?;
        if deployment.bindings.digest != conversation.agent_deployment.deployment_digest.to_string()
        {
            return Err(corrupt());
        }
        let DeploymentClosure::Agent(closure) = decode_deployment_closure(&deployment.bindings)?
        else {
            return Err(corrupt());
        };
        let payload = crate::invocation_repository::load_enabled_exact_published_version(
            &mut tx,
            &scope.tenant_id,
            &closure.interface,
            RegistryResourceKind::Agent,
        )
        .await?;
        let ResourceDocument::Agent(agent) = payload.document else {
            return Err(corrupt());
        };
        tx.commit().await?;
        Ok(agent.input_classification)
    }
}
/// Both artifact resolution and final broker authorization call this current-principal guard.
pub(crate) async fn authorize_conversation_run_read(
    tx: &mut Transaction<'_, Postgres>,
    tenant: &ResourceId,
    run_id: &ResourceId,
) -> Result<(), RepositoryError> {
    let conversation:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM insight_platform.conversation_turns WHERE tenant_id=$1 AND run_id=$2)").bind(tenant.to_string()).bind(run_id.to_string()).fetch_one(&mut **tx).await?;
    if conversation {
        let row=sqlx::query("SELECT bindings_schema_version,bindings,bindings_digest FROM insight_platform.runs WHERE tenant_id=$1 AND run_id=$2").bind(tenant.to_string()).bind(run_id.to_string()).fetch_one(&mut **tx).await?;
        let bindings = run_bindings_from_row(
            &row,
            "bindings_schema_version",
            "bindings",
            "bindings_digest",
        )?;
        authorize(
            tx,
            &ConversationReadScope {
                tenant_id: tenant.clone(),
                principal_id: bindings.principal.principal_id,
                principal_kind: bindings.principal.principal_kind,
            },
            true,
        )
        .await?;
    }
    Ok(())
}
