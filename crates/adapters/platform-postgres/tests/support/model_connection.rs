//! Current-authority probes and ordinary SecretBinding CAS, without any network/model call.
use super::*;
use futures::FutureExt;
use insight_platform_contracts::{
    ModelConnectionError, ModelConnectionProbeAuthorizationV1, UtcTimestamp, MODEL_API_KEY_PURPOSE,
};
use insight_platform_security::{ModelConnectionProbeAuthority, RevokeSecretBinding};
use sqlx::Row;
use std::panic::AssertUnwindSafe;

pub(super) async fn verify(repository: &PgRepository, fixture: &Fixture) {
    let role = model_security_role::ModelSecurityRole::create(repository.pool()).await;
    let authority = PgRepository::new(role.pool.clone());
    let result = AssertUnwindSafe(async {
        role.assert_read_boundary().await;
        verify_inner(repository, &authority, fixture).await;
    })
    .catch_unwind()
    .await;
    role.close().await;
    result.unwrap();
}

async fn verify_inner(repository: &PgRepository, authority: &PgRepository, fixture: &Fixture) {
    let pool = repository.pool();
    let tenant = &fixture.tenant_id;
    let actor = id(ResourceKind::Principal, 0x7600);
    repository
        .create_principal(NewPrincipal {
            principal_id: actor.clone(),
            authentication_authority_digest: named_digest("probe authority"),
            subject_digest: named_digest("probe subject"),
            installation_bindings: PrincipalBindingsPayload {
                installation_bindings: vec![],
            },
        })
        .await
        .unwrap();
    repository
        .bind_tenant_principal(NewTenantPrincipal {
            tenant_id: tenant.clone(),
            principal_id: actor.clone(),
            principal_kind: PrincipalKind::AgentRunner,
            payload: TenantPrincipalPayload {
                permissions: PermissionSet::new(vec![
                    Permission::ModelRead,
                    Permission::SecretBind,
                    Permission::SecretRevoke,
                ])
                .unwrap(),
            },
        })
        .await
        .unwrap();
    let previous:Option<String>=sqlx::query_scalar("SELECT active_deployment_id FROM insight_platform.resources WHERE tenant_id=$1 AND resource_id=$2").bind(tenant.to_string()).bind(fixture.profile_resource_id.to_string()).fetch_one(pool).await.unwrap();
    sqlx::query("UPDATE insight_platform.resources SET active_deployment_id=$3 WHERE tenant_id=$1 AND resource_id=$2").bind(tenant.to_string()).bind(fixture.profile_resource_id.to_string()).bind(fixture.model_deployment.deployment_id.to_string()).execute(pool).await.unwrap();
    let request = ModelConnectionProbeAuthorizationV1 {
        schema_version: 1,
        request_id: id(ResourceKind::ServerRequest, 0x7601),
        tenant_id: tenant.clone(),
        principal_id: actor.clone(),
        principal_kind: PrincipalKind::AgentRunner,
        installation_digest: named_digest("fixture installation"),
        model_deployment: fixture.model_deployment.clone(),
        environment: "test".to_owned(),
        deadline: UtcTimestamp::from_datetime(Utc::now() + Duration::seconds(29)),
    };
    let permit = authority
        .authorize_model_connection_probe(&request)
        .await
        .unwrap();
    assert!(permit.validate_for(&request, Utc::now()));
    assert_eq!(permit.target.provider, fixture.provider_closure);
    assert_eq!(permit.target.maximum_output_tokens, 32);
    // The restricted authority must read exact current metadata, not merely find any Artifact.
    // Keep owner writes separate and restore each fact before asserting the observed rejection.
    let artifact_id = fixture
        .provider_closure
        .admission_evidence
        .artifact
        .artifact_id()
        .to_string();
    let artifact = sqlx::query(
        "SELECT state,blob_id FROM insight_platform.artifacts WHERE tenant_id=$1 AND artifact_id=$2",
    )
    .bind(tenant.to_string())
    .bind(&artifact_id)
    .fetch_one(pool)
    .await
    .unwrap();
    let blob_id: String = artifact.get("blob_id");
    let blob = sqlx::query(
        "SELECT state,content_digest FROM insight_platform.artifact_blobs WHERE tenant_id=$1 AND blob_id=$2",
    )
    .bind(tenant.to_string())
    .bind(&blob_id)
    .fetch_one(pool)
    .await
    .unwrap();
    for (sql, row_id, rejected, original) in [
        (
            "UPDATE insight_platform.artifacts SET state=$3 WHERE tenant_id=$1 AND artifact_id=$2",
            artifact_id.clone(),
            "quarantined".to_owned(),
            artifact.get::<String, _>("state"),
        ),
        (
            "UPDATE insight_platform.artifact_blobs SET state=$3 WHERE tenant_id=$1 AND blob_id=$2",
            blob_id.clone(),
            "quarantined".to_owned(),
            blob.get::<String, _>("state"),
        ),
        (
            "UPDATE insight_platform.artifact_blobs SET content_digest=$3 WHERE tenant_id=$1 AND blob_id=$2",
            blob_id,
            named_digest("different provider declaration bytes").to_string(),
            blob.get::<String, _>("content_digest"),
        ),
    ] {
        assert_eq!(sqlx::query(sql).bind(tenant.to_string()).bind(&row_id).bind(rejected).execute(pool).await.unwrap().rows_affected(), 1);
        let result = authority.authorize_model_connection_probe(&request).await;
        assert_eq!(sqlx::query(sql).bind(tenant.to_string()).bind(&row_id).bind(original).execute(pool).await.unwrap().rows_affected(), 1);
        assert_eq!(result, Err(ModelConnectionError::Rejected));
        assert!(authority.authorize_model_connection_probe(&request).await.is_ok());
    }
    let binding = &fixture.provider_closure.secret_bindings[0].secret_binding_id;
    let metadata = repository
        .read_model_credential_for_principal(tenant, &actor, PrincipalKind::AgentRunner, binding)
        .await
        .unwrap();
    assert_eq!(metadata.purpose, MODEL_API_KEY_PURPOSE);
    let mut invalid = request.clone();
    invalid.environment = "wrong".to_owned();
    assert_eq!(
        authority
            .authorize_model_connection_probe(&invalid)
            .await
            .unwrap_err(),
        ModelConnectionError::Rejected
    );
    let mut invalid = request.clone();
    invalid.model_deployment.deployment_digest = named_digest("wrong exact");
    assert_eq!(
        authority
            .authorize_model_connection_probe(&invalid)
            .await
            .unwrap_err(),
        ModelConnectionError::Rejected
    );
    let mut invalid = request.clone();
    invalid.deadline = UtcTimestamp::from_datetime(Utc::now() - Duration::seconds(1));
    assert_eq!(
        authority
            .authorize_model_connection_probe(&invalid)
            .await
            .unwrap_err(),
        ModelConnectionError::Rejected
    );
    let membership=sqlx::query("SELECT permissions_schema_version,permissions,permissions_digest FROM insight_platform.tenant_principals WHERE tenant_id=$1 AND principal_id=$2").bind(tenant.to_string()).bind(actor.to_string()).fetch_one(pool).await.unwrap();
    for permissions in [
        vec![Permission::ModelRead],
        vec![Permission::SecretBind],
        vec![Permission::SecretRevoke],
    ] {
        let payload = TypedPayload::new(
            1,
            &TenantPrincipalPayload {
                permissions: PermissionSet::new(permissions.clone()).unwrap(),
            },
        )
        .unwrap();
        sqlx::query("UPDATE insight_platform.tenant_principals SET permissions=$3,permissions_digest=$4 WHERE tenant_id=$1 AND principal_id=$2").bind(tenant.to_string()).bind(actor.to_string()).bind(payload.value).bind(payload.digest).execute(pool).await.unwrap();
        assert_eq!(
            authority
                .authorize_model_connection_probe(&request)
                .await
                .unwrap_err(),
            ModelConnectionError::Rejected
        );
        assert_eq!(
            repository
                .read_model_credential_for_principal(
                    tenant,
                    &actor,
                    PrincipalKind::AgentRunner,
                    binding
                )
                .await
                .is_ok(),
            permissions != vec![Permission::ModelRead]
        );
    }
    sqlx::query("UPDATE insight_platform.tenant_principals SET permissions_schema_version=$3,permissions=$4,permissions_digest=$5 WHERE tenant_id=$1 AND principal_id=$2").bind(tenant.to_string()).bind(actor.to_string()).bind(membership.get::<i32,_>("permissions_schema_version")).bind(membership.get::<serde_json::Value,_>("permissions")).bind(membership.get::<String,_>("permissions_digest")).execute(pool).await.unwrap();
    for resource in [&fixture.profile_resource_id, &fixture.provider_resource_id] {
        sqlx::query("UPDATE insight_platform.resources SET gate_state='disabled' WHERE tenant_id=$1 AND resource_id=$2").bind(tenant.to_string()).bind(resource.to_string()).execute(pool).await.unwrap();
        assert_eq!(
            authority
                .authorize_model_connection_probe(&request)
                .await
                .unwrap_err(),
            ModelConnectionError::Rejected
        );
        sqlx::query("UPDATE insight_platform.resources SET gate_state='enabled' WHERE tenant_id=$1 AND resource_id=$2").bind(tenant.to_string()).bind(resource.to_string()).execute(pool).await.unwrap();
    }
    let mut foreign = request.clone();
    foreign.tenant_id = id(ResourceKind::Tenant, 0x76ff);
    assert!(authority
        .authorize_model_connection_probe(&foreign)
        .await
        .is_err());
    assert!(repository
        .read_model_credential_for_principal(
            &foreign.tenant_id,
            &actor,
            PrincipalKind::AgentRunner,
            binding
        )
        .await
        .is_err());
    sqlx::query("UPDATE insight_platform.resources SET active_deployment_id=NULL WHERE tenant_id=$1 AND resource_id=$2").bind(tenant.to_string()).bind(fixture.profile_resource_id.to_string()).execute(pool).await.unwrap();
    assert_eq!(
        authority
            .authorize_model_connection_probe(&request)
            .await
            .unwrap_err(),
        ModelConnectionError::Rejected
    );
    sqlx::query("UPDATE insight_platform.resources SET active_deployment_id=$3 WHERE tenant_id=$1 AND resource_id=$2").bind(tenant.to_string()).bind(fixture.profile_resource_id.to_string()).bind(fixture.model_deployment.deployment_id.to_string()).execute(pool).await.unwrap();
    sqlx::query("UPDATE insight_platform.secret_bindings SET purpose='other_api_key' WHERE tenant_id=$1 AND secret_binding_id=$2").bind(tenant.to_string()).bind(binding.to_string()).execute(pool).await.unwrap();
    assert!(repository
        .read_model_credential_for_principal(tenant, &actor, PrincipalKind::AgentRunner, binding)
        .await
        .is_err());
    sqlx::query("UPDATE insight_platform.secret_bindings SET purpose=$3 WHERE tenant_id=$1 AND secret_binding_id=$2").bind(tenant.to_string()).bind(binding.to_string()).bind(MODEL_API_KEY_PURPOSE).execute(pool).await.unwrap();
    let policy_id = &fixture.provider_closure.network_policy.revision_id;
    let policy=sqlx::query("SELECT payload,payload_digest FROM insight_platform.resource_versions WHERE tenant_id=$1 AND resource_version_id=$2").bind(tenant.to_string()).bind(policy_id.to_string()).fetch_one(pool).await.unwrap();
    let original: serde_json::Value = policy.get("payload");
    let mut wrong = original.clone();
    assert_eq!(wrong["document"]["spec"]["policy_kind"], json!("network"));
    wrong["document"]["spec"]["policy_kind"] = json!("tls");
    let wrong = TypedPayload::from_versioned(1, &wrong, 1048576).unwrap();
    sqlx::query("UPDATE insight_platform.resource_versions SET payload=$3,payload_digest=$4 WHERE tenant_id=$1 AND resource_version_id=$2").bind(tenant.to_string()).bind(policy_id.to_string()).bind(wrong.value).bind(wrong.digest).execute(pool).await.unwrap();
    assert_eq!(
        authority
            .authorize_model_connection_probe(&request)
            .await
            .unwrap_err(),
        ModelConnectionError::Rejected
    );
    sqlx::query("UPDATE insight_platform.resource_versions SET payload=$3,payload_digest=$4 WHERE tenant_id=$1 AND resource_version_id=$2").bind(tenant.to_string()).bind(policy_id.to_string()).bind(original).bind(policy.get::<String,_>("payload_digest")).execute(pool).await.unwrap();
    let command = RevokeSecretBinding {
        audit: audit(tenant, &actor, 0x7610, '2', '3'),
        secret_binding_id: binding.clone(),
        expected_generation: metadata.generation,
        expected_version: metadata.version,
    };
    let mut invalid = command.clone();
    invalid.expected_generation += 1;
    let mut tx = repository.begin_security_transaction().await.unwrap();
    assert!(matches!(
        tx.revoke_model_credential(invalid).await,
        Err(RepositoryError::Conflict(_))
    ));
    tx.rollback().await.unwrap();
    let apply = |command: RevokeSecretBinding| async {
        let mut tx = repository.begin_security_transaction().await.unwrap();
        match tx.revoke_model_credential(command).await {
            Ok(outcome) => {
                tx.commit().await.unwrap();
                Ok(outcome)
            }
            Err(error) => {
                tx.rollback().await.unwrap();
                Err(error)
            }
        }
    };
    let (left, right) = tokio::join!(apply(command.clone()), apply(command.clone()));
    assert_eq!(
        [&left, &right]
            .iter()
            .filter(|result| matches!(result, Ok(CommandOutcome::Applied(_))))
            .count(),
        1,
        "one original transaction must commit the revocation"
    );
    let mut outcomes = Vec::new();
    for result in [left, right] {
        match result {
            Ok(outcome) => outcomes.push(outcome),
            Err(error) => {
                // Concurrent identical candidate Receipt IDs can hit both unique indexes.
                // PostgreSQL may abort one transaction; the API reports Unavailable. Only
                // this explicit, fully rolled-back deadlock is recovered by the same command.
                assert!(matches!(
                    error,
                    RepositoryError::Database(sqlx::Error::Database(ref database))
                        if database.code().as_deref() == Some("40P01")
                ));
                eprintln!("model credential concurrent revoke: original transaction aborted 40P01; explicit same-command recovery");
                outcomes.push(apply(command.clone()).await.unwrap());
            }
        }
    }
    assert!(matches!(
        (&outcomes[0], &outcomes[1]),
        (CommandOutcome::Applied(_), CommandOutcome::Replayed(_))
            | (CommandOutcome::Replayed(_), CommandOutcome::Applied(_))
    ));
    let denied = TypedPayload::new(
        1,
        &TenantPrincipalPayload {
            permissions: PermissionSet::new(vec![Permission::SecretBind]).unwrap(),
        },
    )
    .unwrap();
    sqlx::query("UPDATE insight_platform.tenant_principals SET permissions=$3,permissions_digest=$4 WHERE tenant_id=$1 AND principal_id=$2").bind(tenant.to_string()).bind(actor.to_string()).bind(denied.value).bind(denied.digest).execute(pool).await.unwrap();
    let mut tx = repository.begin_security_transaction().await.unwrap();
    assert!(matches!(
        tx.revoke_model_credential(command.clone()).await,
        Err(RepositoryError::PermissionDenied)
    ));
    tx.rollback().await.unwrap();
    sqlx::query("UPDATE insight_platform.tenant_principals SET permissions=$3,permissions_digest=$4 WHERE tenant_id=$1 AND principal_id=$2").bind(tenant.to_string()).bind(actor.to_string()).bind(membership.get::<serde_json::Value,_>("permissions")).bind(membership.get::<String,_>("permissions_digest")).execute(pool).await.unwrap();
    assert!(matches!(
        apply(command.clone()).await.unwrap(),
        CommandOutcome::Replayed(_)
    ));
    let events: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM insight_platform.events WHERE tenant_id=$1 AND event_id=$2",
    )
    .bind(tenant.to_string())
    .bind(command.audit.event_id.to_string())
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(events, 1);
    let (receipts, succeeded): (i64, i64) = sqlx::query_as(
        "SELECT count(*), count(*) FILTER (WHERE state='succeeded') FROM insight_platform.receipts WHERE tenant_id=$1 AND receipt_kind='command' AND scope_kind='secret_binding' AND scope_id=$2 AND dedupe_owner_id=$3 AND operation='security.secret_binding.revoke' AND idempotency_key_digest=$4 AND request_digest=$5",
    )
    .bind(tenant.to_string())
    .bind(binding.to_string())
    .bind(actor.to_string())
    .bind(command.audit.idempotency_key_digest.to_string())
    .bind(command.audit.request_digest.to_string())
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!((receipts, succeeded), (1, 1));
    assert_eq!(
        authority
            .authorize_model_connection_probe(&request)
            .await
            .unwrap_err(),
        ModelConnectionError::Rejected
    );
    let revoked = repository
        .read_model_credential_for_principal(tenant, &actor, PrincipalKind::AgentRunner, binding)
        .await
        .unwrap();
    assert_eq!(revoked.state, "revoked");
    assert_eq!(revoked.generation, metadata.generation + 1);
    assert_eq!(revoked.version, metadata.version + 1);
    sqlx::query("UPDATE insight_platform.secret_bindings SET state='active',generation=$3,version=$4,updated_at=$5,revoked_at=NULL WHERE tenant_id=$1 AND secret_binding_id=$2").bind(tenant.to_string()).bind(binding.to_string()).bind(metadata.generation).bind(metadata.version).bind(metadata.updated_at).execute(pool).await.unwrap();
    assert!(authority
        .authorize_model_connection_probe(&request)
        .await
        .is_ok());
    sqlx::query("UPDATE insight_platform.resources SET active_deployment_id=$3 WHERE tenant_id=$1 AND resource_id=$2").bind(tenant.to_string()).bind(fixture.profile_resource_id.to_string()).bind(previous).execute(pool).await.unwrap();
}
