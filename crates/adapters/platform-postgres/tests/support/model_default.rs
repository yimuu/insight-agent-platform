//! Default-model commands and authoring selectors against the existing Model fixture.
use super::*;
use futures::FutureExt;
use insight_platform_contracts::AgentSlotTargetInputV1;
use insight_platform_postgres::repository::TenantRecord;
use insight_platform_registry::authoring::{
    AuthoringDeploymentSelectorV1, AuthoringQueryError, AuthoringResolutionV1,
    AuthoringSlotSelectionV1, AuthoringSlotTargetV1, BindTenantModelDefault,
    ResolveAgentBindingsRequestV1,
};
use std::panic::AssertUnwindSafe;

fn fresh(kind: ResourceKind) -> ResourceId {
    ResourceId::from_uuid_v7(kind, uuid::Uuid::now_v7()).unwrap()
}

fn command(
    tenant: &ResourceId,
    principal: &ResourceId,
    expected_tenant_version: i64,
    model: Option<ExactDeploymentRef>,
) -> BindTenantModelDefault {
    let receipt_id = fresh(ResourceKind::Receipt);
    BindTenantModelDefault {
        audit: CommandAudit {
            trace: insight_platform_contracts::TraceIdentityV1::generate(),
            tenant_id: tenant.clone(),
            principal_id: principal.clone(),
            principal_kind: PrincipalKind::AgentRunner,
            idempotency_key_digest: canonical_digest(&json!({"model_default": receipt_id}))
                .unwrap()
                .parse()
                .unwrap(),
            request_digest: request_digest(expected_tenant_version, &model),
            receipt_id,
            event_id: fresh(ResourceKind::Event),
            outbox_id: fresh(ResourceKind::OutboxEvent),
            receipt_expires_at: Utc::now() + Duration::hours(1),
        },
        expected_tenant_version,
        model,
    }
}

fn request_digest(version: i64, model: &Option<ExactDeploymentRef>) -> Sha256Digest {
    canonical_digest(&json!({"expected_tenant_version": version, "model": model}))
        .unwrap()
        .parse()
        .unwrap()
}

async fn execute(
    repository: &PgRepository,
    command: BindTenantModelDefault,
) -> Result<CommandOutcome<TenantRecord>, RepositoryError> {
    let mut tx = repository.begin_registry_transaction().await.unwrap();
    let result = tx.bind_tenant_model_default(command).await;
    if result.is_ok() {
        tx.commit().await.unwrap();
    } else {
        tx.rollback().await.unwrap();
    }
    result
}

async fn principal(
    repository: &PgRepository,
    tenant: &ResourceId,
    permissions: Vec<Permission>,
) -> ResourceId {
    let principal_id = fresh(ResourceKind::Principal);
    repository
        .create_principal(NewPrincipal {
            principal_id: principal_id.clone(),
            authentication_authority_digest: named_digest("model default test authority"),
            subject_digest: canonical_digest(&json!({"principal": principal_id}))
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
            principal_id: principal_id.clone(),
            principal_kind: PrincipalKind::AgentRunner,
            payload: TenantPrincipalPayload {
                permissions: PermissionSet::new(permissions).unwrap(),
            },
        })
        .await
        .unwrap();
    principal_id
}

async fn read(repository: &PgRepository, tenant: &ResourceId, reader: &ResourceId) -> TenantRecord {
    repository
        .read_model_default_for_principal(tenant, reader, PrincipalKind::AgentRunner)
        .await
        .unwrap()
}

// Include the current config and all durable command effects, so a rejected command cannot pass
// merely by leaving the default pointer unchanged while committing a Receipt/Event/Outbox.
async fn facts(pool: &PgPool, tenant: &ResourceId) -> serde_json::Value {
    sqlx::query_scalar(
        "SELECT jsonb_build_object(\
            'tenant', (SELECT jsonb_build_object('version',version,'config',config,'digest',config_digest) FROM insight_platform.tenants WHERE tenant_id=$1),\
            'receipts', (SELECT count(*) FROM insight_platform.receipts WHERE tenant_id=$1 AND operation='model.default.set'),\
            'events', (SELECT count(*) FROM insight_platform.events WHERE tenant_id=$1 AND event_type='model.default_changed'),\
            'outbox', (SELECT count(*) FROM insight_platform.outbox_events o JOIN insight_platform.events e USING(tenant_id,event_id) WHERE e.tenant_id=$1 AND e.event_type='model.default_changed'))",
    ).bind(tenant.to_string()).fetch_one(pool).await.unwrap()
}

fn selectors(fixture: &Fixture, alias: &str, environment: &str) -> ResolveAgentBindingsRequestV1 {
    ResolveAgentBindingsRequestV1 {
        schema_version: 1,
        slots: [
            (
                "default",
                AuthoringDeploymentSelectorV1::DefaultModel {
                    environment: environment.to_owned(),
                },
            ),
            (
                "alias",
                AuthoringDeploymentSelectorV1::Alias {
                    alias: alias.parse().unwrap(),
                    environment: environment.to_owned(),
                },
            ),
        ]
        .into_iter()
        .map(|(slot_id, selector)| AuthoringSlotSelectionV1 {
            slot_id: slot_id.to_owned(),
            requirement_digest: named_digest("model default authoring requirement"),
            interface_contract_digest: Some(fixture.profile_revision.semantic_digest.clone()),
            target: AuthoringSlotTargetV1::Model {
                candidates: vec![selector],
                selection_policy: fixture.selection_policy_binding.clone(),
            },
        })
        .collect(),
    }
}

async fn resolve(
    repository: &PgRepository,
    tenant: &ResourceId,
    reader: &ResourceId,
    request: &ResolveAgentBindingsRequestV1,
) -> Vec<AuthoringResolutionV1> {
    repository
        .resolve_agent_authoring_bindings(tenant, reader, PrincipalKind::AgentRunner, request)
        .await
        .unwrap()
        .slots
        .into_iter()
        .map(|slot| slot.resolution)
        .collect()
}

fn assert_rejected(resolutions: &[AuthoringResolutionV1], expected: AuthoringQueryError) {
    assert!(!resolutions.is_empty());
    for resolution in resolutions {
        assert_eq!(
            resolution,
            &AuthoringResolutionV1::Rejected { code: expected }
        );
    }
}

fn assert_resolved(
    resolutions: &[AuthoringResolutionV1],
    fixture: &Fixture,
    request: &ResolveAgentBindingsRequestV1,
    call_authorized: bool,
) {
    assert_eq!(resolutions.len(), request.slots.len());
    for (resolution, slot) in resolutions.iter().zip(&request.slots) {
        let AuthoringResolutionV1::Resolved {
            binding,
            observed_contract_digests,
            contract_match,
            call_authorized: observed_authorized,
            ..
        } = resolution
        else {
            panic!("expected an exact Model binding, got {resolution:?}")
        };
        assert_eq!(binding.slot_id, slot.slot_id);
        assert_eq!(binding.requirement_digest, slot.requirement_digest);
        assert_eq!(
            binding.target,
            AgentSlotTargetInputV1::Model {
                candidates: vec![fixture.model_deployment.clone()],
                selection_policy: fixture.selection_policy_binding.clone(),
            }
        );
        assert_eq!(
            observed_contract_digests,
            std::slice::from_ref(&fixture.profile_revision.semantic_digest)
        );
        assert_eq!(*contract_match, Some(true));
        assert_eq!(*observed_authorized, call_authorized);
    }
}

pub(super) async fn assert_model_defaults(repository: &PgRepository, fixture: &Fixture) {
    let pool = repository.pool();
    let manager = principal(
        repository,
        &fixture.tenant_id,
        vec![
            Permission::ModelRead,
            Permission::ModelWrite,
            Permission::ModelInvoke,
            Permission::PolicyRead,
        ],
    )
    .await;
    let reader = principal(
        repository,
        &fixture.tenant_id,
        vec![Permission::ModelRead, Permission::PolicyRead],
    )
    .await;
    let writer = principal(repository, &fixture.tenant_id, vec![Permission::ModelWrite]).await;
    let original = read(repository, &fixture.tenant_id, &manager).await;
    assert!(
        original.config.default_model.is_none(),
        "the parent fixture must begin without a default"
    );
    let model_state: (Option<String>, serde_json::Value, String, String, String) = sqlx::query_as(
        "SELECT active_deployment_id,payload,payload_digest,gate_state,lifecycle_state FROM insight_platform.resources WHERE tenant_id=$1 AND resource_id=$2",
    ).bind(fixture.tenant_id.to_string()).bind(fixture.profile_resource_id.to_string()).fetch_one(pool).await.unwrap();
    let provider_gate: String = sqlx::query_scalar(
        "SELECT gate_state FROM insight_platform.resources WHERE tenant_id=$1 AND resource_id=$2",
    )
    .bind(fixture.tenant_id.to_string())
    .bind(fixture.provider_resource_id.to_string())
    .fetch_one(pool)
    .await
    .unwrap();
    let secret_id = &fixture.provider_closure.secret_bindings[0].secret_binding_id;
    let secret_state: (String, i64) = sqlx::query_as(
        "SELECT state,generation FROM insight_platform.secret_bindings WHERE tenant_id=$1 AND secret_binding_id=$2",
    ).bind(fixture.tenant_id.to_string()).bind(secret_id.to_string()).fetch_one(pool).await.unwrap();
    assert!(matches!(
        fixture.provider_closure.secret_bindings[0].resolution_policy,
        SecretResolutionPolicy::Pinned { .. }
    ));

    // The execution fixture freezes exact Deployments without selecting an authoring head.
    // Select that same already-seeded Model, and add only its temporary authoring alias.
    let alias = format!("model-default-{}", uuid::Uuid::now_v7());
    let foreign = fresh(ResourceKind::Tenant);
    let result = AssertUnwindSafe(async {
        let mut payload = model_state.1.clone();
        payload["alias"] = json!(alias);
        let payload_digest = canonical_digest(&payload).unwrap();
        sqlx::query("UPDATE insight_platform.resources SET active_deployment_id=$3,payload=$4,payload_digest=$5 WHERE tenant_id=$1 AND resource_id=$2")
            .bind(fixture.tenant_id.to_string()).bind(fixture.profile_resource_id.to_string())
            .bind(fixture.model_deployment.deployment_id.to_string()).bind(payload).bind(payload_digest)
            .execute(pool).await.unwrap();
        assert_commands(repository, fixture, [&manager, &reader, &writer], &alias, &original, &foreign).await;
    }).catch_unwind().await;

    // Remove only the empty Tenant created by the negative cross-tenant assertion. Leaving its
    // enrolled partition changes the pre-existing execution fixture's frozen claim-hint set.
    // A surprising durable effect prevents this cleanup instead of erasing failure evidence.
    cleanup_foreign_tenant(pool, &foreign, &manager).await;
    // Restore only the exact fixture metadata touched here, even when an assertion panics.
    // No credential ciphertext/reference is read, copied, or modified by these assertions.
    sqlx::query("UPDATE insight_platform.resources SET active_deployment_id=$3,payload=$4,payload_digest=$5,gate_state=$6,lifecycle_state=$7 WHERE tenant_id=$1 AND resource_id=$2")
        .bind(fixture.tenant_id.to_string()).bind(fixture.profile_resource_id.to_string())
        .bind(&model_state.0).bind(&model_state.1).bind(&model_state.2).bind(&model_state.3).bind(&model_state.4)
        .execute(pool).await.unwrap();
    sqlx::query(
        "UPDATE insight_platform.resources SET gate_state=$3 WHERE tenant_id=$1 AND resource_id=$2",
    )
    .bind(fixture.tenant_id.to_string())
    .bind(fixture.provider_resource_id.to_string())
    .bind(provider_gate)
    .execute(pool)
    .await
    .unwrap();
    sqlx::query("UPDATE insight_platform.secret_bindings SET state=$3,generation=$4 WHERE tenant_id=$1 AND secret_binding_id=$2")
        .bind(fixture.tenant_id.to_string()).bind(secret_id.to_string()).bind(secret_state.0).bind(secret_state.1)
        .execute(pool).await.unwrap();
    sqlx::query("UPDATE insight_platform.tenants SET state=$2 WHERE tenant_id=$1")
        .bind(fixture.tenant_id.to_string())
        .bind(&original.state)
        .execute(pool)
        .await
        .unwrap();
    sqlx::query("UPDATE insight_platform.tenant_principals SET state='active' WHERE tenant_id=$1 AND principal_id=$2")
        .bind(fixture.tenant_id.to_string()).bind(writer.to_string()).execute(pool).await.unwrap();
    let current = read(repository, &fixture.tenant_id, &manager).await;
    if current.config.default_model.is_some() {
        execute(
            repository,
            command(&fixture.tenant_id, &manager, current.version, None),
        )
        .await
        .unwrap();
    }
    assert_eq!(
        read(repository, &fixture.tenant_id, &manager).await.config,
        original.config
    );
    if let Err(panic) = result {
        std::panic::resume_unwind(panic);
    }
}

async fn assert_commands(
    repository: &PgRepository,
    fixture: &Fixture,
    actors: [&ResourceId; 3],
    alias: &str,
    original: &TenantRecord,
    foreign: &ResourceId,
) {
    let [manager, reader, writer] = actors;
    let tenant = &fixture.tenant_id;
    let pool = repository.pool();
    let mut request = selectors(fixture, alias, "test");
    let before = facts(pool, tenant).await;
    let unconfigured = resolve(repository, tenant, reader, &request).await;
    assert_eq!(
        unconfigured[0],
        AuthoringResolutionV1::Rejected {
            code: AuthoringQueryError::DefaultNotConfigured
        }
    );
    assert_resolved(
        &unconfigured[1..],
        fixture,
        &ResolveAgentBindingsRequestV1 {
            schema_version: 1,
            slots: request.slots[1..].to_vec(),
        },
        false,
    );

    for actor in [writer, &fixture.principal_id] {
        assert!(matches!(
            repository
                .read_model_default_for_principal(tenant, actor, PrincipalKind::AgentRunner)
                .await,
            Err(RepositoryError::PermissionDenied)
        ));
    }
    for actor in [reader, &fixture.principal_id] {
        assert!(matches!(
            execute(
                repository,
                command(
                    tenant,
                    actor,
                    original.version,
                    Some(fixture.model_deployment.clone())
                )
            )
            .await,
            Err(RepositoryError::PermissionDenied)
        ));
    }
    assert_rejected(
        &resolve(repository, tenant, writer, &request).await,
        AuthoringQueryError::Denied,
    );
    assert_eq!(facts(pool, tenant).await, before);

    let mut rolled_back = repository.begin_registry_transaction().await.unwrap();
    assert!(matches!(
        rolled_back
            .bind_tenant_model_default(command(
                tenant,
                writer,
                original.version,
                Some(fixture.model_deployment.clone()),
            ))
            .await
            .unwrap(),
        CommandOutcome::Applied(_)
    ));
    rolled_back.rollback().await.unwrap();
    assert_eq!(
        facts(pool, tenant).await,
        before,
        "outer rollback must include the default and all effects"
    );

    // ModelWrite suffices to bind; it does not implicitly grant ModelRead or invocation.
    let set = command(
        tenant,
        writer,
        original.version,
        Some(fixture.model_deployment.clone()),
    );
    let CommandOutcome::Applied(bound) = execute(repository, set.clone()).await.unwrap() else {
        panic!("fresh default command replayed")
    };
    assert_eq!(bound.version, original.version + 1);
    let mut expected_config = original.config.clone();
    expected_config.default_model = Some(fixture.model_deployment.clone());
    assert_eq!(bound.config, expected_config);
    assert_eq!(read(repository, tenant, reader).await, bound);
    let effects: (String, Option<String>, Option<String>, String, Option<i64>, serde_json::Value, String) = sqlx::query_as(
        "SELECT r.state,r.disposition,r.response_reference_id,e.aggregate_id,e.aggregate_version,e.payload,o.event_id FROM insight_platform.receipts r JOIN insight_platform.events e ON e.tenant_id=r.tenant_id AND e.event_id=$3 JOIN insight_platform.outbox_events o ON o.tenant_id=e.tenant_id AND o.event_id=e.event_id AND o.outbox_id=$4 WHERE r.tenant_id=$1 AND r.receipt_id=$2",
    ).bind(tenant.to_string()).bind(set.audit.receipt_id.to_string()).bind(set.audit.event_id.to_string()).bind(set.audit.outbox_id.to_string()).fetch_one(pool).await.unwrap();
    assert_eq!(
        effects,
        (
            "succeeded".to_owned(),
            Some("bound".to_owned()),
            Some(tenant.to_string()),
            tenant.to_string(),
            Some(bound.version),
            json!({"schema_version": 1, "default_model": fixture.model_deployment}),
            set.audit.event_id.to_string()
        )
    );
    let bound_facts = facts(pool, tenant).await;
    assert_eq!(
        bound_facts["receipts"].as_i64(),
        Some(before["receipts"].as_i64().unwrap() + 1)
    );
    assert_eq!(
        bound_facts["events"].as_i64(),
        Some(before["events"].as_i64().unwrap() + 1)
    );
    assert_eq!(
        bound_facts["outbox"].as_i64(),
        Some(before["outbox"].as_i64().unwrap() + 1)
    );

    // The original expected version is now stale, but the identical request replays first.
    let mut replay = set.clone();
    replay.audit.receipt_id = fresh(ResourceKind::Receipt);
    replay.audit.event_id = fresh(ResourceKind::Event);
    replay.audit.outbox_id = fresh(ResourceKind::OutboxEvent);
    assert!(
        matches!(execute(repository, replay).await.unwrap(), CommandOutcome::Replayed(record) if record == bound)
    );
    assert_eq!(facts(pool, tenant).await, bound_facts);
    let mut changed = set.clone();
    changed.model = None;
    changed.audit.request_digest = request_digest(changed.expected_tenant_version, &changed.model);
    assert!(matches!(
        execute(repository, changed).await,
        Err(RepositoryError::IdempotencyConflict)
    ));
    assert!(matches!(
        execute(repository, command(tenant, manager, original.version, None)).await,
        Err(RepositoryError::Conflict("tenant"))
    ));
    assert_eq!(facts(pool, tenant).await, bound_facts);

    let wrong_kind = command(
        tenant,
        manager,
        bound.version,
        Some(fixture.capability_deployment.clone()),
    );
    assert!(matches!(
        execute(repository, wrong_kind).await,
        Err(RepositoryError::InvalidInput(_))
    ));
    let mut wrong_digest = fixture.model_deployment.clone();
    wrong_digest.deployment_digest = named_digest("wrong default deployment digest");
    assert!(matches!(
        execute(
            repository,
            command(tenant, manager, bound.version, Some(wrong_digest))
        )
        .await,
        Err(RepositoryError::Conflict(_))
    ));
    assert_eq!(facts(pool, tenant).await, bound_facts);

    repository
        .create_tenant(NewTenant {
            tenant_id: foreign.to_string(),
            state: "active".to_owned(),
            config: TenantConfig::default(),
        })
        .await
        .unwrap();
    repository
        .bind_tenant_principal(NewTenantPrincipal {
            tenant_id: foreign.clone(),
            principal_id: manager.clone(),
            principal_kind: PrincipalKind::AgentRunner,
            payload: TenantPrincipalPayload {
                permissions: PermissionSet::new(vec![
                    Permission::ModelRead,
                    Permission::ModelWrite,
                    Permission::PolicyRead,
                ])
                .unwrap(),
            },
        })
        .await
        .unwrap();
    let foreign_before = facts(pool, foreign).await;
    assert!(matches!(
        execute(
            repository,
            command(foreign, manager, 1, Some(fixture.model_deployment.clone()))
        )
        .await,
        Err(RepositoryError::Conflict(_))
    ));
    let foreign_resolution = resolve(repository, foreign, manager, &request).await;
    assert_eq!(
        foreign_resolution[0],
        AuthoringResolutionV1::Rejected {
            code: AuthoringQueryError::DefaultNotConfigured
        }
    );
    assert_eq!(
        foreign_resolution[1],
        AuthoringResolutionV1::Rejected {
            code: AuthoringQueryError::NotFound
        }
    );
    assert_eq!(facts(pool, foreign).await, foreign_before);

    assert_resolved(
        &resolve(repository, tenant, manager, &request).await,
        fixture,
        &request,
        true,
    );
    assert_resolved(
        &resolve(repository, tenant, reader, &request).await,
        fixture,
        &request,
        false,
    );
    assert_rejected(
        &resolve(
            repository,
            tenant,
            reader,
            &selectors(fixture, alias, "another-environment"),
        )
        .await,
        AuthoringQueryError::NotFound,
    );
    // A caller-supplied, well-typed but nonexistent Selection Policy must not be replaced.
    for slot in &mut request.slots {
        let AuthoringSlotTargetV1::Model {
            selection_policy, ..
        } = &mut slot.target
        else {
            unreachable!()
        };
        selection_policy.deployment.deployment_id = fresh(ResourceKind::PolicyDeployment);
    }
    assert_rejected(
        &resolve(repository, tenant, reader, &request).await,
        AuthoringQueryError::NotFound,
    );
    request = selectors(fixture, alias, "test");

    sqlx::query("UPDATE insight_platform.resources SET active_deployment_id=NULL WHERE tenant_id=$1 AND resource_id=$2")
        .bind(tenant.to_string()).bind(fixture.profile_resource_id.to_string()).execute(pool).await.unwrap();
    let inactive_head = execute(
        repository,
        command(
            tenant,
            manager,
            bound.version,
            Some(fixture.model_deployment.clone()),
        ),
    )
    .await;
    let inactive_head_resolution = repository
        .resolve_agent_authoring_bindings(tenant, reader, PrincipalKind::AgentRunner, &request)
        .await;
    sqlx::query("UPDATE insight_platform.resources SET active_deployment_id=$3 WHERE tenant_id=$1 AND resource_id=$2")
        .bind(tenant.to_string()).bind(fixture.profile_resource_id.to_string()).bind(fixture.model_deployment.deployment_id.to_string()).execute(pool).await.unwrap();
    assert!(matches!(inactive_head, Err(RepositoryError::Conflict(_))));
    assert_rejected(
        &inactive_head_resolution
            .unwrap()
            .slots
            .into_iter()
            .map(|slot| slot.resolution)
            .collect::<Vec<_>>(),
        AuthoringQueryError::NotFound,
    );
    assert_eq!(facts(pool, tenant).await, bound_facts);

    for (label, sql, row_id, blocked, restored) in [
        ("provider gate", "UPDATE insight_platform.resources SET gate_state=$3 WHERE tenant_id=$1 AND resource_id=$2", fixture.provider_resource_id.to_string(), "disabled", "enabled"),
        ("model gate", "UPDATE insight_platform.resources SET gate_state=$3 WHERE tenant_id=$1 AND resource_id=$2", fixture.profile_resource_id.to_string(), "disabled", "enabled"),
        ("credential state", "UPDATE insight_platform.secret_bindings SET state=$3 WHERE tenant_id=$1 AND secret_binding_id=$2", fixture.provider_closure.secret_bindings[0].secret_binding_id.to_string(), "revoked", "active"),
    ] {
        sqlx::query(sql).bind(tenant.to_string()).bind(&row_id).bind(blocked).execute(pool).await.unwrap();
        let set_result = execute(repository, command(tenant, manager, bound.version, Some(fixture.model_deployment.clone()))).await;
        let resolved = repository.resolve_agent_authoring_bindings(tenant, reader, PrincipalKind::AgentRunner, &request).await;
        sqlx::query(sql).bind(tenant.to_string()).bind(&row_id).bind(restored).execute(pool).await.unwrap();
        assert!(matches!(set_result, Err(RepositoryError::Conflict(_))), "{label}: {set_result:?}");
        let resolutions: Vec<_> = resolved.unwrap().slots.into_iter().map(|slot| slot.resolution).collect();
        let code = if label == "model gate" { AuthoringQueryError::Disabled } else { AuthoringQueryError::ContractMismatch };
        assert_rejected(&resolutions, code);
        assert_eq!(facts(pool, tenant).await, bound_facts, "{label}");
    }
    let exact_secret = &fixture.provider_closure.secret_bindings[0];
    sqlx::query("UPDATE insight_platform.secret_bindings SET generation=generation+1 WHERE tenant_id=$1 AND secret_binding_id=$2")
        .bind(tenant.to_string()).bind(exact_secret.secret_binding_id.to_string()).execute(pool).await.unwrap();
    let generation_result = execute(
        repository,
        command(
            tenant,
            manager,
            bound.version,
            Some(fixture.model_deployment.clone()),
        ),
    )
    .await;
    let generation_resolution = repository
        .resolve_agent_authoring_bindings(tenant, reader, PrincipalKind::AgentRunner, &request)
        .await;
    sqlx::query("UPDATE insight_platform.secret_bindings SET generation=$3 WHERE tenant_id=$1 AND secret_binding_id=$2")
        .bind(tenant.to_string()).bind(exact_secret.secret_binding_id.to_string()).bind(i64::try_from(exact_secret.binding_generation).unwrap()).execute(pool).await.unwrap();
    assert!(matches!(
        generation_result,
        Err(RepositoryError::Conflict(_))
    ));
    assert_rejected(
        &generation_resolution
            .unwrap()
            .slots
            .into_iter()
            .map(|slot| slot.resolution)
            .collect::<Vec<_>>(),
        AuthoringQueryError::ContractMismatch,
    );
    assert_eq!(facts(pool, tenant).await, bound_facts);

    // Replays must reauthorize the current membership before looking up the Receipt.
    sqlx::query("UPDATE insight_platform.tenant_principals SET state='revoked' WHERE tenant_id=$1 AND principal_id=$2")
        .bind(tenant.to_string()).bind(writer.to_string()).execute(pool).await.unwrap();
    let revoked_replay = execute(repository, set.clone()).await;
    sqlx::query("UPDATE insight_platform.tenant_principals SET state='active' WHERE tenant_id=$1 AND principal_id=$2")
        .bind(tenant.to_string()).bind(writer.to_string()).execute(pool).await.unwrap();
    assert!(matches!(
        revoked_replay,
        Err(RepositoryError::PermissionDenied)
    ));
    sqlx::query("UPDATE insight_platform.tenants SET state='suspended' WHERE tenant_id=$1")
        .bind(tenant.to_string())
        .execute(pool)
        .await
        .unwrap();
    let inactive_replay = execute(repository, set.clone()).await;
    let inactive_read = repository
        .read_model_default_for_principal(tenant, reader, PrincipalKind::AgentRunner)
        .await;
    let inactive_resolution = repository
        .resolve_agent_authoring_bindings(tenant, reader, PrincipalKind::AgentRunner, &request)
        .await;
    sqlx::query("UPDATE insight_platform.tenants SET state='active' WHERE tenant_id=$1")
        .bind(tenant.to_string())
        .execute(pool)
        .await
        .unwrap();
    assert!(matches!(
        inactive_replay,
        Err(RepositoryError::PermissionDenied)
    ));
    assert!(matches!(
        inactive_read,
        Err(RepositoryError::PermissionDenied)
    ));
    assert_rejected(
        &inactive_resolution
            .unwrap()
            .slots
            .into_iter()
            .map(|slot| slot.resolution)
            .collect::<Vec<_>>(),
        AuthoringQueryError::Denied,
    );
    assert_eq!(facts(pool, tenant).await, bound_facts);

    let clear = command(tenant, manager, bound.version, None);
    let CommandOutcome::Applied(cleared) = execute(repository, clear.clone()).await.unwrap() else {
        panic!("fresh clear replayed")
    };
    assert_eq!(cleared.version, bound.version + 1);
    assert_eq!(cleared.config, original.config);
    let cleared_facts = facts(pool, tenant).await;
    assert!(
        matches!(execute(repository, clear).await.unwrap(), CommandOutcome::Replayed(record) if record == cleared)
    );
    // A much later replay reads current Tenant state; it cannot resurrect the earlier default.
    assert!(
        matches!(execute(repository, set).await.unwrap(), CommandOutcome::Replayed(record) if record == cleared)
    );
    assert_eq!(facts(pool, tenant).await, cleared_facts);
    let cleared_resolution = resolve(repository, tenant, reader, &request).await;
    assert_eq!(
        cleared_resolution[0],
        AuthoringResolutionV1::Rejected {
            code: AuthoringQueryError::DefaultNotConfigured
        }
    );
    assert_resolved(
        &cleared_resolution[1..],
        fixture,
        &ResolveAgentBindingsRequestV1 {
            schema_version: 1,
            slots: request.slots[1..].to_vec(),
        },
        false,
    );
}

async fn cleanup_foreign_tenant(pool: &PgPool, tenant: &ResourceId, principal: &ResourceId) {
    let mut transaction = pool.begin().await.unwrap();
    let effects: i64 = sqlx::query_scalar("SELECT (SELECT count(*) FROM insight_platform.receipts WHERE tenant_id=$1)+(SELECT count(*) FROM insight_platform.events WHERE tenant_id=$1)+(SELECT count(*) FROM insight_platform.outbox_events WHERE tenant_id=$1)+(SELECT count(*) FROM insight_platform.resources WHERE tenant_id=$1)+(SELECT count(*) FROM insight_platform.jobs WHERE tenant_id=$1)+(SELECT count(*) FROM insight_platform.runs WHERE tenant_id=$1)+(SELECT count(*) FROM insight_platform.quota_accounts WHERE tenant_id=$1)")
        .bind(tenant.to_string()).fetch_one(&mut *transaction).await.unwrap();
    assert_eq!(
        effects, 0,
        "foreign negative fixture must remain free of durable effects"
    );
    sqlx::query(
        "DELETE FROM insight_platform.tenant_principals WHERE tenant_id=$1 AND principal_id=$2",
    )
    .bind(tenant.to_string())
    .bind(principal.to_string())
    .execute(&mut *transaction)
    .await
    .unwrap();
    sqlx::query("DELETE FROM insight_platform.scheduler_tenant_state WHERE tenant_id=$1")
        .bind(tenant.to_string())
        .execute(&mut *transaction)
        .await
        .unwrap();
    sqlx::query("DELETE FROM insight_platform.tenants WHERE tenant_id=$1 AND state='active' AND version=1 AND config->>'default_model' IS NULL")
        .bind(tenant.to_string()).execute(&mut *transaction).await.unwrap();
    let remaining: i64 =
        sqlx::query_scalar("SELECT count(*) FROM insight_platform.tenants WHERE tenant_id=$1")
            .bind(tenant.to_string())
            .fetch_one(&mut *transaction)
            .await
            .unwrap();
    assert_eq!(remaining, 0);
    transaction.commit().await.unwrap();
}
