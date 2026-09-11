//! The Model fixture allocates deployment accounts through the management command, not direct SQL.
use super::*;
use insight_platform_contracts::{ModelQuotaLimitsV1, ModelQuotaViewV1, SetModelQuotaRequestV1};
use insight_platform_registry::model_quota::{model_quota_request_digest, SetModelQuota};
fn fresh(kind: ResourceKind) -> ResourceId {
    ResourceId::from_uuid_v7(kind, uuid::Uuid::now_v7()).unwrap()
}
fn ids(kind: ResourceKind) -> [ResourceId; 3] {
    [fresh(kind), fresh(kind), fresh(kind)]
}
const LIMITS: ModelQuotaLimitsV1 = ModelQuotaLimitsV1 {
    // Two additional owner-driven public-event attempts (failure and deadline timeout).
    requests: 18,
    tokens: 16_384,
    cost_microunits: 2_000_000,
};
async fn reader(repository: &PgRepository, tenant: &ResourceId) -> ResourceId {
    let principal = fresh(ResourceKind::Principal);
    repository
        .create_principal(NewPrincipal {
            principal_id: principal.clone(),
            authentication_authority_digest: named_digest("model quota authority"),
            subject_digest: canonical_digest(&json!({"subject":principal}))
                .unwrap()
                .parse()
                .unwrap(),
            installation_bindings: PrincipalBindingsPayload {
                installation_bindings: vec![],
            },
        })
        .await
        .unwrap();
    repository
        .bind_tenant_principal(NewTenantPrincipal {
            tenant_id: tenant.clone(),
            principal_id: principal.clone(),
            principal_kind: PrincipalKind::AgentRunner,
            payload: TenantPrincipalPayload {
                permissions: PermissionSet::new(vec![
                    Permission::ModelRead,
                    Permission::TenantManage,
                ])
                .unwrap(),
            },
        })
        .await
        .unwrap();
    principal
}
fn command(
    view: &ModelQuotaViewV1,
    principal: &ResourceId,
    limits: ModelQuotaLimitsV1,
) -> SetModelQuota {
    let receipt = fresh(ResourceKind::Receipt);
    let request = SetModelQuotaRequestV1 {
        schema_version: 1,
        model_deployment: view.model_deployment.clone(),
        limits,
    };
    let key: Sha256Digest = canonical_digest(&json!({"receipt":receipt}))
        .unwrap()
        .parse()
        .unwrap();
    SetModelQuota {
        audit: CommandAudit {
            trace: insight_platform_contracts::TraceIdentityV1::generate(),
            tenant_id: view.tenant_id.clone(),
            principal_id: principal.clone(),
            principal_kind: PrincipalKind::AgentRunner,
            receipt_id: receipt,
            event_id: fresh(ResourceKind::Event),
            outbox_id: fresh(ResourceKind::OutboxEvent),
            request_digest: model_quota_request_digest(
                &view.tenant_id,
                principal,
                &request,
                &view.etag,
                &key,
            )
            .unwrap(),
            idempotency_key_digest: key,
            receipt_expires_at: Utc::now() + Duration::hours(1),
        },
        request,
        expected_etag: view.etag.clone(),
        account_ids: ids(ResourceKind::QuotaAccount),
        event_ids: ids(ResourceKind::Event),
        outbox_ids: ids(ResourceKind::OutboxEvent),
    }
}
async fn execute(
    repository: &PgRepository,
    command: SetModelQuota,
) -> Result<CommandOutcome<ModelQuotaViewV1>, RepositoryError> {
    let mut tx = repository.begin_registry_transaction().await.unwrap();
    let result = tx.set_model_quota(command).await;
    if result.is_ok() {
        tx.commit().await.unwrap();
    } else {
        tx.rollback().await.unwrap();
    }
    result
}
fn view(result: CommandOutcome<ModelQuotaViewV1>) -> ModelQuotaViewV1 {
    match result {
        CommandOutcome::Applied(v) | CommandOutcome::Replayed(v) => v,
    }
}
async fn read(
    repository: &PgRepository,
    tenant: &ResourceId,
    principal: &ResourceId,
    target: &ExactDeploymentRef,
) -> ModelQuotaViewV1 {
    repository
        .read_model_quota_for_principal(
            tenant,
            principal,
            PrincipalKind::AgentRunner,
            &target.deployment_id,
        )
        .await
        .unwrap()
}
async fn facts(repository: &PgRepository, tenant: &ResourceId) -> serde_json::Value {
    sqlx::query_scalar("SELECT jsonb_build_object('accounts',(SELECT jsonb_agg(to_jsonb(q) ORDER BY quota_account_id) FROM insight_platform.quota_accounts q WHERE tenant_id=$1),'ledger',(SELECT jsonb_agg(to_jsonb(q) ORDER BY quota_entry_id) FROM insight_platform.quota_ledger q WHERE tenant_id=$1),'events',(SELECT jsonb_agg(to_jsonb(e) ORDER BY event_id) FROM insight_platform.events e WHERE tenant_id=$1),'receipts',(SELECT jsonb_agg(to_jsonb(r) ORDER BY receipt_id) FROM insight_platform.receipts r WHERE tenant_id=$1))")
        .bind(tenant.to_string()).fetch_one(repository.pool()).await.unwrap()
}
pub(super) async fn provision_additional(
    repository: &PgRepository,
    tenant: &ResourceId,
    target: &ExactDeploymentRef,
) {
    let principal = reader(repository, tenant).await;
    let absent = read(repository, tenant, &principal, target).await;
    assert!(absent.allocation.is_none());
    assert!(matches!(
        execute(repository, command(&absent, &principal, LIMITS))
            .await
            .unwrap(),
        CommandOutcome::Applied(_)
    ));
}

pub(super) async fn provision(
    repository: &PgRepository,
    tenant: &ResourceId,
    target: &ExactDeploymentRef,
) {
    let principal = reader(repository, tenant).await;
    let absent = read(repository, tenant, &principal, target).await;
    assert!(absent.allocation.is_none());
    let partial = fresh(ResourceKind::QuotaAccount);
    repository
        .create_quota_account(NewQuotaAccount {
            tenant_id: tenant.to_string(),
            quota_account_id: partial.to_string(),
            scope_kind: "model_deployment".into(),
            scope_id: target.deployment_id.to_string(),
            work_class: "model".into(),
            metric: QuotaDimension::ModelRequests.as_str().into(),
            limit_value: 0,
            payload: TypedPayload::new(1, &json!({"fixture":"partial quota"})).unwrap(),
        })
        .await
        .unwrap();
    let partial_facts = facts(repository, tenant).await;
    assert!(matches!(
        repository
            .read_model_quota_for_principal(
                tenant,
                &principal,
                PrincipalKind::AgentRunner,
                &target.deployment_id
            )
            .await,
        Err(RepositoryError::CorruptRow(_))
    ));
    assert!(matches!(
        execute(repository, command(&absent, &principal, LIMITS)).await,
        Err(RepositoryError::CorruptRow(_))
    ));
    assert_eq!(facts(repository, tenant).await, partial_facts);
    sqlx::query(
        "DELETE FROM insight_platform.quota_accounts WHERE tenant_id=$1 AND quota_account_id=$2",
    )
    .bind(tenant.to_string())
    .bind(partial.to_string())
    .execute(repository.pool())
    .await
    .unwrap();
    let foreign = fresh(ResourceKind::Tenant);
    assert!(matches!(
        repository
            .read_model_quota_for_principal(
                &foreign,
                &principal,
                PrincipalKind::AgentRunner,
                &target.deployment_id
            )
            .await,
        Err(RepositoryError::PermissionDenied)
    ));
    let mut wrong_tenant = command(&absent, &principal, LIMITS);
    wrong_tenant.audit.tenant_id = foreign;
    wrong_tenant.audit.request_digest = model_quota_request_digest(
        &wrong_tenant.audit.tenant_id,
        &principal,
        &wrong_tenant.request,
        &wrong_tenant.expected_etag,
        &wrong_tenant.audit.idempotency_key_digest,
    )
    .unwrap();
    assert!(matches!(
        execute(repository, wrong_tenant).await,
        Err(RepositoryError::PermissionDenied)
    ));
    let mut allocate = command(&absent, &principal, LIMITS);
    // Preserve existing fixture account identity expectations; only the owning command creates them.
    allocate.account_ids = [
        id(ResourceKind::QuotaAccount, 0x41),
        id(ResourceKind::QuotaAccount, 0x42),
        id(ResourceKind::QuotaAccount, 0x43),
    ];
    let (left, right) = tokio::join!(
        execute(repository, allocate.clone()),
        execute(repository, allocate.clone())
    );
    assert!(
        matches!(
            (&left, &right),
            (
                Ok(CommandOutcome::Applied(_)),
                Ok(CommandOutcome::Replayed(_))
            ) | (
                Ok(CommandOutcome::Replayed(_)),
                Ok(CommandOutcome::Applied(_))
            )
        ),
        "left={left:?}; right={right:?}"
    );
    let allocated = view(left.unwrap());
    assert_eq!(allocated.allocation.as_ref().unwrap().limits, LIMITS);
    let before = facts(repository, tenant).await;
    assert!(matches!(
        execute(repository, command(&absent, &principal, LIMITS)).await,
        Err(RepositoryError::Conflict(_))
    ));
    assert_eq!(facts(repository, tenant).await, before);
    let mut wrong = command(&allocated, &principal, LIMITS);
    wrong.request.model_deployment.deployment_digest = digest('0');
    wrong.audit.request_digest = model_quota_request_digest(
        tenant,
        &principal,
        &wrong.request,
        &wrong.expected_etag,
        &wrong.audit.idempotency_key_digest,
    )
    .unwrap();
    assert!(matches!(
        execute(repository, wrong).await,
        Err(RepositoryError::Conflict(_))
    ));
    assert_eq!(facts(repository, tenant).await, before);
    let permissions = TypedPayload::new(
        1,
        &TenantPrincipalPayload {
            permissions: PermissionSet::new(vec![Permission::ModelRead]).unwrap(),
        },
    )
    .unwrap();
    let old:(i32,serde_json::Value,String)=sqlx::query_as("SELECT permissions_schema_version,permissions,permissions_digest FROM insight_platform.tenant_principals WHERE tenant_id=$1 AND principal_id=$2").bind(tenant.to_string()).bind(principal.to_string()).fetch_one(repository.pool()).await.unwrap();
    sqlx::query("UPDATE insight_platform.tenant_principals SET permissions_schema_version=$3,permissions=$4,permissions_digest=$5 WHERE tenant_id=$1 AND principal_id=$2").bind(tenant.to_string()).bind(principal.to_string()).bind(permissions.schema_version).bind(&permissions.value).bind(&permissions.digest).execute(repository.pool()).await.unwrap();
    assert!(matches!(
        execute(repository, allocate).await,
        Err(RepositoryError::PermissionDenied)
    ));
    sqlx::query("UPDATE insight_platform.tenant_principals SET permissions_schema_version=$3,permissions=$4,permissions_digest=$5 WHERE tenant_id=$1 AND principal_id=$2").bind(tenant.to_string()).bind(principal.to_string()).bind(old.0).bind(old.1).bind(old.2).execute(repository.pool()).await.unwrap();
    assert_eq!(facts(repository, tenant).await, before);
    let current = read(repository, tenant, &principal, target).await;
    let (left, right) = tokio::join!(
        execute(repository, command(&current, &principal, LIMITS)),
        execute(repository, command(&current, &principal, LIMITS))
    );
    assert!(matches!(
        (&left, &right),
        (
            Ok(CommandOutcome::Applied(_)),
            Err(RepositoryError::Conflict(_))
        ) | (
            Err(RepositoryError::Conflict(_)),
            Ok(CommandOutcome::Applied(_))
        )
    ));
    let allocated = read(repository, tenant, &principal, target).await;
    let zero = ModelQuotaLimitsV1 {
        requests: 0,
        tokens: 0,
        cost_microunits: 0,
    };
    let zeroed = view(
        execute(repository, command(&allocated, &principal, zero))
            .await
            .unwrap(),
    );
    assert_eq!(zeroed.allocation.as_ref().unwrap().limits, zero);
    view(
        execute(repository, command(&zeroed, &principal, LIMITS))
            .await
            .unwrap(),
    );
}
pub(super) async fn verify_usage(repository: &PgRepository, fixture: &Fixture) {
    let principal = reader(repository, &fixture.tenant_id).await;
    let old = read(
        repository,
        &fixture.tenant_id,
        &principal,
        &fixture.model_deployment,
    )
    .await;
    let usage = old.allocation.as_ref().unwrap();
    assert_eq!(usage.used.requests, 2);
    assert_eq!(usage.used.tokens, 80);
    let before = facts(repository, &fixture.tenant_id).await;
    assert!(matches!(
        execute(
            repository,
            command(
                &old,
                &principal,
                ModelQuotaLimitsV1 {
                    requests: 1,
                    ..LIMITS
                }
            )
        )
        .await,
        Err(RepositoryError::Conflict(_))
    ));
    assert_eq!(facts(repository, &fixture.tenant_id).await, before);
    let changed = view(
        execute(
            repository,
            command(
                &old,
                &principal,
                ModelQuotaLimitsV1 {
                    requests: 20,
                    ..LIMITS
                },
            ),
        )
        .await
        .unwrap(),
    );
    assert_eq!(changed.allocation.as_ref().unwrap().used, usage.used);
    assert_eq!(
        changed.allocation.as_ref().unwrap().reserved,
        usage.reserved
    );
    let after = facts(repository, &fixture.tenant_id).await;
    assert_eq!(before["ledger"], after["ledger"]);
    execute(repository, command(&changed, &principal, LIMITS))
        .await
        .unwrap();
}

pub(super) async fn verify_reserved(repository: &PgRepository, fixture: &Fixture) {
    let principal = reader(repository, &fixture.tenant_id).await;
    let old = read(
        repository,
        &fixture.tenant_id,
        &principal,
        &fixture.model_deployment,
    )
    .await;
    assert!(old.allocation.as_ref().unwrap().reserved.requests > 0);
    let before = facts(repository, &fixture.tenant_id).await;
    assert!(matches!(
        execute(
            repository,
            command(
                &old,
                &principal,
                ModelQuotaLimitsV1 {
                    requests: 0,
                    ..LIMITS
                }
            )
        )
        .await,
        Err(RepositoryError::Conflict(_))
    ));
    assert_eq!(facts(repository, &fixture.tenant_id).await, before);
}
