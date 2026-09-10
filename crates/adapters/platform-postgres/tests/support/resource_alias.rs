//! Real Registry commands plus the physical tenant/kind uniqueness boundary.
use super::*;
use insight_platform_registry::UpdateResourceDraft;

fn unique_audit(tenant: &str) -> CommandAudit {
    let request = uuid::Uuid::now_v7();
    let make = |kind| ResourceId::from_uuid_v7(kind, uuid::Uuid::now_v7()).unwrap();
    let digest =
        insight_platform_contracts::canonical_digest(&json!({"resource_alias_test":request}))
            .unwrap()
            .parse()
            .unwrap();
    CommandAudit {
        trace: insight_platform_contracts::TraceIdentityV1::generate(),
        tenant_id: id(tenant),
        principal_id: id(PRINCIPAL_ID),
        principal_kind: PrincipalKind::TenantAdmin,
        receipt_id: make(ResourceKind::Receipt),
        event_id: make(ResourceKind::Event),
        outbox_id: make(ResourceKind::OutboxEvent),
        idempotency_key_digest: digest,
        request_digest: insight_platform_contracts::canonical_digest(&json!({"request":request}))
            .unwrap()
            .parse()
            .unwrap(),
        receipt_expires_at: Utc::now() + Duration::hours(1),
    }
}
async fn create(
    repository: &PgRepository,
    command: CreateResourceDraft,
) -> Result<insight_platform_postgres::repository::ResourceRecord, RepositoryError> {
    let mut tx = repository.begin_registry_transaction().await.unwrap();
    match tx.create_resource_draft(command).await {
        Ok(result) => {
            tx.commit().await.unwrap();
            Ok(applied(result))
        }
        Err(error) => {
            tx.rollback().await.unwrap();
            Err(error)
        }
    }
}
async fn update(
    repository: &PgRepository,
    command: UpdateResourceDraft,
) -> Result<insight_platform_postgres::repository::ResourceRecord, RepositoryError> {
    let mut tx = repository.begin_registry_transaction().await.unwrap();
    match tx.update_resource_draft(command).await {
        Ok(result) => {
            tx.commit().await.unwrap();
            Ok(applied(result))
        }
        Err(error) => {
            tx.rollback().await.unwrap();
            Err(error)
        }
    }
}
async fn command_facts(pool: &sqlx::PgPool, audit: &CommandAudit) -> (i64, i64, i64) {
    sqlx::query_as("SELECT (SELECT count(*) FROM insight_platform.receipts WHERE tenant_id=$1 AND receipt_id=$2), (SELECT count(*) FROM insight_platform.events WHERE tenant_id=$1 AND event_id=$3), (SELECT count(*) FROM insight_platform.outbox_events WHERE tenant_id=$1 AND outbox_id=$4)")
        .bind(audit.tenant_id.to_string()).bind(audit.receipt_id.to_string()).bind(audit.event_id.to_string()).bind(audit.outbox_id.to_string()).fetch_one(pool).await.unwrap()
}

pub(super) async fn verify(
    pool: &sqlx::PgPool,
    repository: &PgRepository,
    policy: &ResourceDraftPayload,
    skill: &ResourceDraftPayload,
) {
    let alias: insight_platform_contracts::ResourceAlias =
        format!("scope-{}", uuid::Uuid::now_v7().simple())
            .parse()
            .unwrap();
    let mut draft = policy.clone();
    draft.validation = None;
    draft.alias = Some(alias.clone());
    let make = || CreateResourceDraft {
        audit: unique_audit(TENANT_ID),
        resource_id: ResourceId::from_uuid_v7(ResourceKind::Policy, uuid::Uuid::now_v7()).unwrap(),
        draft: draft.clone(),
    };
    let first = make();
    let second = make();
    let (left, right) = tokio::join!(
        create(repository, first.clone()),
        create(repository, second.clone())
    );
    let (winner, loser) = match (left, right) {
        (Ok(winner), Err(RepositoryError::Conflict("resource alias"))) => (winner, &second.audit),
        (Err(RepositoryError::Conflict("resource alias")), Ok(winner)) => (winner, &first.audit),
        other => panic!("one atomic alias winner was required: {other:?}"),
    };
    assert_eq!(command_facts(pool, loser).await, (0, 0, 0));
    let winner_id: ResourceId = winner.resource_id.parse().unwrap();
    let mut changed = draft.clone();
    changed.display_name = "Display name remains mutable".into();
    let updated = update(
        repository,
        UpdateResourceDraft {
            audit: unique_audit(TENANT_ID),
            resource_id: winner_id.clone(),
            expected_resource_version: winner.version,
            draft: changed.clone(),
        },
    )
    .await
    .unwrap();
    assert_eq!(updated.payload.value["alias"], alias.as_str());
    for alias in [None, Some("changed-alias".parse().unwrap())] {
        let audit = unique_audit(TENANT_ID);
        let mut changed = changed.clone();
        changed.alias = alias;
        assert!(update(
            repository,
            UpdateResourceDraft {
                audit: audit.clone(),
                resource_id: winner_id.clone(),
                expected_resource_version: updated.version,
                draft: changed,
            }
        )
        .await
        .is_err());
        assert_eq!(command_facts(pool, &audit).await, (0, 0, 0));
    }
    let mut skill = skill.clone();
    skill.alias = Some(alias.clone());
    skill.validation = None;
    create(
        repository,
        CreateResourceDraft {
            audit: unique_audit(TENANT_ID),
            resource_id: ResourceId::from_uuid_v7(ResourceKind::Skill, uuid::Uuid::now_v7())
                .unwrap(),
            draft: skill,
        },
    )
    .await
    .unwrap();
    // This insertion isolates only the database uniqueness scope; it does not claim a cross-tenant
    // authoring Artifact can pass the Registry's separate admission checks.
    sqlx::query("INSERT INTO insight_platform.resources (tenant_id,resource_id,resource_kind,lifecycle_state,gate_state,payload_schema_version,payload,payload_digest) SELECT $1,$2,resource_kind,lifecycle_state,gate_state,payload_schema_version,payload,payload_digest FROM insight_platform.resources WHERE tenant_id=$3 AND resource_id=$4")
        .bind(TENANT_B_ID).bind(ResourceId::from_uuid_v7(ResourceKind::Policy,uuid::Uuid::now_v7()).unwrap().to_string()).bind(TENANT_ID).bind(winner_id.to_string()).execute(pool).await.unwrap();
}
