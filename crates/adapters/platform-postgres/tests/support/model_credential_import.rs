use super::*;
use insight_platform_contracts::{
    ModelCredentialImportAuthorizationV1, ModelCredentialImportError,
    ModelCredentialImportIdentityV1, ResourceKind, MODEL_API_KEY_PURPOSE,
};
use insight_platform_security::ModelCredentialImportAuthority;
fn fresh(kind: ResourceKind) -> ResourceId {
    ResourceId::from_uuid_v7(kind, uuid::Uuid::now_v7()).unwrap()
}
async fn snapshot(pool: &sqlx::PgPool) -> serde_json::Value {
    sqlx::query_scalar("SELECT jsonb_build_object('bindings',(SELECT jsonb_agg(to_jsonb(s) ORDER BY secret_binding_id) FROM insight_platform.secret_bindings s WHERE tenant_id=$1),'receipts',(SELECT jsonb_agg(to_jsonb(r) ORDER BY receipt_id) FROM insight_platform.receipts r WHERE tenant_id=$1),'events',(SELECT jsonb_agg(to_jsonb(e) ORDER BY event_id) FROM insight_platform.events e WHERE tenant_id=$1),'outbox',(SELECT jsonb_agg(to_jsonb(o) ORDER BY outbox_id) FROM insight_platform.outbox_events o WHERE tenant_id=$1))")
        .bind(TENANT_ID).fetch_one(pool).await.unwrap()
}
pub(super) async fn verify(
    owner: &PgRepository,
    restricted: &PgRepository,
    template: &RegisterPreparedSecretBinding,
) {
    let pool = owner.pool();
    // An independent service binding preserves the earlier revoked-broker regression unchanged.
    let broker = fresh(ResourceKind::Principal);
    let marker: Sha256Digest =
        insight_platform_contracts::canonical_digest(&serde_json::json!({"broker":broker}))
            .unwrap()
            .parse()
            .unwrap();
    owner
        .create_principal(NewPrincipal {
            principal_id: broker.clone(),
            authentication_authority_digest: marker.clone(),
            subject_digest: marker,
            installation_bindings: PrincipalBindingsPayload {
                installation_bindings: vec![],
            },
        })
        .await
        .unwrap();
    seed_service_binding(
        owner,
        TENANT_ID,
        &broker.to_string(),
        vec![Permission::SecretBind],
    )
    .await;
    let identity = ModelCredentialImportIdentityV1 {
        schema_version: 1,
        operation_id: uuid::Uuid::new_v4().to_string().parse().unwrap(),
        tenant_id: id(TENANT_ID),
        principal_id: id(ADMIN_ID),
        principal_kind: PrincipalKind::TenantAdmin,
        provider_id: id(PROVIDER_ID),
        purpose: MODEL_API_KEY_PURPOSE.parse().unwrap(),
    };
    let request = ModelCredentialImportAuthorizationV1 {
        schema_version: 1,
        identity: identity.clone(),
        deadline: Utc::now() + Duration::seconds(25),
    };
    let permit = restricted
        .authorize_model_credential_import(&request)
        .await
        .unwrap();
    assert!(permit.validate_for(&request, Utc::now()));
    let before = snapshot(pool).await;
    let mut denied = request.clone();
    denied.identity.principal_id = id(DENIED_ID);
    assert_eq!(
        restricted
            .authorize_model_credential_import(&denied)
            .await
            .unwrap_err(),
        ModelCredentialImportError::Rejected
    );
    let mut expired = request.clone();
    expired.deadline = Utc::now();
    assert_eq!(
        restricted
            .authorize_model_credential_import(&expired)
            .await
            .unwrap_err(),
        ModelCredentialImportError::Rejected
    );
    assert_eq!(snapshot(pool).await, before);
    let mut command = template.clone();
    command.audit.principal_id = broker.clone();
    command.audit.receipt_id = fresh(ResourceKind::Receipt);
    command.audit.event_id = fresh(ResourceKind::Event);
    command.audit.outbox_id = fresh(ResourceKind::OutboxEvent);
    command.audit.receipt_expires_at = Utc::now() + Duration::hours(1);
    command.preparation_digest = identity.preparation_digest().unwrap();
    command.audit.idempotency_key_digest = command.preparation_digest.clone();
    command.secret_binding_id = identity.secret_binding_id().unwrap();
    command.purpose = identity.purpose.clone();
    command.delegated_import = Some(identity.clone());
    command.audit.request_digest = command.semantic_request_digest().unwrap();
    let mut resealed = command.clone();
    resealed.encrypted_reference =
        EncryptedOpaqueReference::new(b"different-sealed-representation".to_vec()).unwrap();
    resealed.key_id = "other-key".to_owned();
    assert_eq!(
        resealed.semantic_request_digest().unwrap(),
        command.audit.request_digest
    );
    let (first, second) = tokio::join!(
        restricted.register_prepared(command.clone()),
        restricted.register_prepared(resealed)
    );
    let first = first.unwrap();
    let second = second.unwrap();
    assert_eq!(first.exact_binding, second.exact_binding);
    assert_ne!(first.disposition, second.disposition);
    let committed = snapshot(pool).await;
    // Both original and service authorization precede Receipt replay. Restore only test metadata.
    for principal in [id(ADMIN_ID), broker.clone()] {
        sqlx::query("UPDATE insight_platform.tenant_principals SET state='revoked' WHERE tenant_id=$1 AND principal_id=$2")
            .bind(TENANT_ID).bind(principal.to_string()).execute(pool).await.unwrap();
        assert_eq!(
            restricted
                .register_prepared(command.clone())
                .await
                .unwrap_err(),
            PreparedSecretBindingRegistrationError::Rejected
        );
        if principal == id(ADMIN_ID) {
            assert_eq!(
                restricted
                    .authorize_model_credential_import(&request)
                    .await
                    .unwrap_err(),
                ModelCredentialImportError::Rejected
            );
        }
        assert_eq!(snapshot(pool).await, committed);
        sqlx::query("UPDATE insight_platform.tenant_principals SET state='active' WHERE tenant_id=$1 AND principal_id=$2")
            .bind(TENANT_ID).bind(principal.to_string()).execute(pool).await.unwrap();
    }
    sqlx::query("UPDATE insight_platform.tenants SET state='suspended' WHERE tenant_id=$1")
        .bind(TENANT_ID)
        .execute(pool)
        .await
        .unwrap();
    assert_eq!(
        restricted
            .register_prepared(command.clone())
            .await
            .unwrap_err(),
        PreparedSecretBindingRegistrationError::Rejected
    );
    sqlx::query("UPDATE insight_platform.tenants SET state='active' WHERE tenant_id=$1")
        .bind(TENANT_ID)
        .execute(pool)
        .await
        .unwrap();
    assert_eq!(snapshot(pool).await, committed);
    // Emulate exact retention of this expired receipt, not a second registration authority.
    sqlx::query("UPDATE insight_platform.receipts SET created_at=clock_timestamp()-interval '2 hours',expires_at=clock_timestamp()-interval '1 hour' WHERE tenant_id=$1 AND receipt_id=$2")
        .bind(TENANT_ID).bind(command.audit.receipt_id.to_string()).execute(pool).await.unwrap();
    assert_eq!(sqlx::query("DELETE FROM insight_platform.receipts WHERE tenant_id=$1 AND receipt_id=$2 AND expires_at<clock_timestamp()")
        .bind(TENANT_ID).bind(command.audit.receipt_id.to_string()).execute(pool).await.unwrap().rows_affected(),1);
    let event_count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM insight_platform.events WHERE tenant_id=$1")
            .bind(TENANT_ID)
            .fetch_one(pool)
            .await
            .unwrap();
    command.audit.receipt_id = fresh(ResourceKind::Receipt);
    assert_eq!(
        restricted
            .register_prepared(command.clone())
            .await
            .unwrap()
            .disposition,
        PreparedSecretBindingRegistrationDisposition::Replayed
    );
    let after_count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM insight_platform.events WHERE tenant_id=$1")
            .bind(TENANT_ID)
            .fetch_one(pool)
            .await
            .unwrap();
    assert_eq!(
        after_count, event_count,
        "current Binding replay creates no duplicate event"
    );
    let before = snapshot(pool).await;
    let mut swapped = command.clone();
    swapped.reference_digest = digest('1');
    swapped.audit.request_digest = swapped.semantic_request_digest().unwrap();
    assert_eq!(
        restricted.register_prepared(swapped).await.unwrap_err(),
        PreparedSecretBindingRegistrationError::Rejected
    );
    assert_eq!(snapshot(pool).await, before);
    sqlx::query("UPDATE insight_platform.secret_bindings SET state='revoked',revoked_at=clock_timestamp() WHERE tenant_id=$1 AND secret_binding_id=$2")
        .bind(TENANT_ID).bind(command.secret_binding_id.to_string()).execute(pool).await.unwrap();
    let before = snapshot(pool).await;
    assert_eq!(
        restricted.register_prepared(command).await.unwrap_err(),
        PreparedSecretBindingRegistrationError::Rejected
    );
    assert_eq!(snapshot(pool).await, before);
}
