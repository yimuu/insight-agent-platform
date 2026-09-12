use super::*;
use insight_platform_api::authentication::AuthenticatedPrincipal;
use insight_platform_api::conversation::*;
use insight_platform_api::product::ListKeysetBoundary;
use insight_platform_contracts::{ConversationTurnViewV1, ConversationViewV1, UtcTimestamp};
use insight_platform_postgres::repository::ConversationReadScope;
pub struct PgConversations(pub Arc<PgRepository>);
fn scope(p: &AuthenticatedPrincipal) -> ConversationReadScope {
    ConversationReadScope {
        tenant_id: p.tenant_id.clone(),
        principal_id: p.principal_id.clone(),
        principal_kind: p.principal_kind,
    }
}
fn audit(c: &ConversationCommand) -> Result<CommandAudit, RunApplicationError> {
    let id = |kind| {
        ResourceId::from_uuid_v7(kind, uuid::Uuid::now_v7())
            .map_err(|_| RunApplicationError::Internal)
    };
    Ok(CommandAudit {
        trace: c.principal.trace.clone(),
        tenant_id: c.principal.tenant_id.clone(),
        principal_id: c.principal.principal_id.clone(),
        principal_kind: c.principal.principal_kind,
        receipt_id: id(ResourceKind::Receipt)?,
        event_id: id(ResourceKind::Event)?,
        outbox_id: id(ResourceKind::OutboxEvent)?,
        idempotency_key_digest: c.idempotency_key_digest.clone(),
        request_digest: c.request_digest.clone(),
        receipt_expires_at: chrono::Utc::now() + chrono::Duration::hours(24),
    })
}
fn timestamp(v: &UtcTimestamp) -> Result<chrono::DateTime<chrono::Utc>, RunApplicationError> {
    chrono::DateTime::parse_from_rfc3339(v.as_str())
        .map(|x| x.with_timezone(&chrono::Utc))
        .map_err(|_| RunApplicationError::Invalid)
}
#[async_trait]
impl ConversationApplication for PgConversations {
    async fn create(
        &self,
        c: ConversationCommand,
        r: CreateConversationRequestV1,
    ) -> Result<ConversationViewV1, RunApplicationError> {
        let id = ResourceId::from_uuid_v7(ResourceKind::Conversation, uuid::Uuid::now_v7())
            .map_err(|_| RunApplicationError::Internal)?;
        match self
            .0
            .create_conversation(audit(&c)?, id, r.agent_id, r.title)
            .await
            .map_err(map_run_repository_error)?
        {
            insight_platform_contracts::CommandOutcome::Applied(v)
            | insight_platform_contracts::CommandOutcome::Replayed(v) => Ok(v),
        }
    }
    async fn read(
        &self,
        p: AuthenticatedPrincipal,
        id: ResourceId,
    ) -> Result<ConversationViewV1, RunApplicationError> {
        self.0
            .read_conversation(&scope(&p), &id)
            .await
            .map_err(map_run_repository_error)
    }
    async fn list(
        &self,
        i: ConversationListIntent,
    ) -> Result<AuthorityListPage<ConversationViewV1>, RunApplicationError> {
        let at = match i.snapshot_at {
            Some(v) => v,
            None => self
                .0
                .conversation_snapshot_at()
                .await
                .map_err(map_run_repository_error)?,
        };
        let after = match i.boundary {
            None => None,
            Some(ListKeysetBoundary::Conversation {
                created_at,
                conversation_id,
            }) => Some((timestamp(&created_at)?, conversation_id)),
            _ => return Err(RunApplicationError::Invalid),
        };
        let mut items = self
            .0
            .list_conversations(
                &scope(&i.principal),
                i.agent_id.as_ref(),
                at,
                after,
                i.limit + 1,
            )
            .await
            .map_err(map_run_repository_error)?;
        let more = items.len() > usize::from(i.limit);
        items.truncate(usize::from(i.limit));
        let next_boundary = if more {
            items.last().map(|v| ListKeysetBoundary::Conversation {
                created_at: v.created_at.clone(),
                conversation_id: v.conversation_id.clone(),
            })
        } else {
            None
        };
        Ok(AuthorityListPage {
            items,
            snapshot_at: at,
            next_boundary,
        })
    }
    async fn turns(
        &self,
        i: ConversationListIntent,
    ) -> Result<AuthorityListPage<ConversationTurnViewV1>, RunApplicationError> {
        let id = i.conversation_id.ok_or(RunApplicationError::Invalid)?;
        let at = match i.snapshot_at {
            Some(v) => v,
            None => self
                .0
                .conversation_snapshot_at()
                .await
                .map_err(map_run_repository_error)?,
        };
        let after = match i.boundary {
            None => 0,
            Some(ListKeysetBoundary::ConversationTurn { ordinal, .. }) => ordinal,
            _ => return Err(RunApplicationError::Invalid),
        };
        let mut items = self
            .0
            .list_conversation_turns(&scope(&i.principal), &id, at, after, i.limit + 1)
            .await
            .map_err(map_run_repository_error)?;
        let more = items.len() > usize::from(i.limit);
        items.truncate(usize::from(i.limit));
        let next_boundary = if more {
            items.last().map(|v| ListKeysetBoundary::ConversationTurn {
                created_at: v.created_at.clone(),
                ordinal: v.ordinal,
            })
        } else {
            None
        };
        Ok(AuthorityListPage {
            items,
            snapshot_at: at,
            next_boundary,
        })
    }
    async fn send(
        &self,
        c: ConversationCommand,
        id: ResourceId,
        version: u64,
        r: SendConversationRequestV1,
    ) -> Result<ConversationTurnViewV1, RunApplicationError> {
        let scope = scope(&c.principal);
        if let Some(turn) = self
            .0
            .read_conversation_turn_replay(
                &scope,
                &id,
                &c.idempotency_key_digest,
                &c.request_digest,
            )
            .await
            .map_err(map_run_repository_error)?
        {
            return Ok(turn);
        }
        let conversation = self
            .0
            .read_conversation(&scope, &id)
            .await
            .map_err(map_run_repository_error)?;
        let classification = self
            .0
            .read_conversation_input_classification(&scope, &id)
            .await
            .map_err(map_run_repository_error)?;
        let request = insight_platform_api::run::CreateRunRequestV1 {
            agent_id: conversation.agent_id,
            expected_agent_deployment: Some(conversation.agent_deployment),
            deadline: r.deadline,
            input: insight_platform_api::run::CreateRunInputV1 {
                classification,
                schema_digest: conversation.input_schema_digest,
                value: ValueRef::Inline {
                    value: serde_json::json!({conversation.input_field:r.message}),
                },
            },
        };
        let command = prepare_root_run_command(
            &self.0,
            CreateRunIntent {
                principal: c.principal,
                idempotency_key_digest: c.idempotency_key_digest,
                request_digest: c.request_digest,
                request,
                deadline: chrono::Utc::now() + chrono::Duration::seconds(10),
            },
        )
        .await?;
        let mut transaction = self
            .0
            .begin_run_transaction()
            .await
            .map_err(map_run_repository_error)?;
        let outcome = transaction
            .admit_conversation_turn(&id, version, command)
            .await
            .map_err(map_run_repository_error)?;
        transaction
            .commit()
            .await
            .map_err(map_run_repository_error)?;
        match outcome {
            insight_platform_contracts::CommandOutcome::Applied(v)
            | insight_platform_contracts::CommandOutcome::Replayed(v) => Ok(v),
        }
    }
}
