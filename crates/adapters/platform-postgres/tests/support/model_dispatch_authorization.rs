//! Real PostgreSQL assertions inside the claimed ModelTurn fixture. These do not contact a model.
use super::*;
use futures::FutureExt;
use insight_platform_contracts::{ModelDispatchAuthorizationError, ModelDispatchAuthorizationV1};
use insight_platform_models::execution::ModelAdapterExecutionRequest;
use insight_platform_security::ModelDispatchAuthority;
use std::panic::AssertUnwindSafe;

pub(super) async fn assert_current_dispatch_authorization(
    pool: &PgPool,
    fixture: &Fixture,
    execution: &ModelAdapterExecutionRequest,
) {
    let role = model_security_role::ModelSecurityRole::create(pool).await;
    let repository = PgRepository::new(role.pool.clone());
    let result = AssertUnwindSafe(async {
        role.assert_read_boundary().await;
        assert_current_dispatch_inner(pool, &repository, fixture, execution).await;
    })
    .catch_unwind()
    .await;
    role.close().await;
    result.unwrap();
}

async fn assert_current_dispatch_inner(
    pool: &PgPool,
    repository: &PgRepository,
    fixture: &Fixture,
    execution: &ModelAdapterExecutionRequest,
) {
    let closure = &execution.provider_closure;
    let limits = &execution.provider.request_limits;
    let request = ModelDispatchAuthorizationV1 {
        schema_version: 1,
        tenant_id: execution.tenant_id.clone(),
        model_turn_id: execution.model_turn_id.clone(),
        job_id: execution.job_id.clone(),
        worker_process_generation_id: execution.worker_process_generation_id.clone(),
        attempt_no: execution.attempt_no,
        lease_generation: execution.lease_generation,
        admission_digest: execution.admission_digest.clone(),
        model_request_digest: execution.request_digest.clone(),
        provider_deployment: execution.provider_deployment.clone(),
        provider_revision: execution.provider_revision.clone(),
        endpoint_identity_digest: closure.endpoint_identity_digest.clone(),
        secret_bindings: closure.secret_bindings.clone(),
        network_policy: closure.network_policy.clone(),
        tls_policy: closure.tls_policy.clone(),
        trust_policy: closure.trust_policy.clone(),
        data_policy: closure.data_policy.clone(),
        region: closure.region.clone(),
        adapter_qualified_name: execution.provider.installed_adapter.qualified_name.clone(),
        maximum_request_bytes: limits.maximum_request_bytes,
        maximum_response_bytes: limits.maximum_response_bytes,
        connect_timeout_milliseconds: limits.connect_timeout_milliseconds,
        total_timeout_milliseconds: limits.total_timeout_milliseconds,
        deadline: execution.request.deadline,
    };
    assert!(request.validate_at(Utc::now()));
    let permit = repository.authorize_model_dispatch(&request).await.unwrap();
    assert!(permit.validate_for(&request, Utc::now()));
    let lease_expiry: DateTime<Utc> = sqlx::query_scalar(
        "SELECT lease_expires_at FROM insight_platform.jobs WHERE tenant_id=$1 AND job_id=$2",
    )
    .bind(request.tenant_id.to_string())
    .bind(request.job_id.to_string())
    .fetch_one(pool)
    .await
    .unwrap();
    assert!(permit.valid_until <= lease_expiry);
    // Swapping any of these identities must fail even though every value remains well-typed.
    let mutations: Vec<(&str, ModelDispatchAuthorizationV1)> = [
        ("tenant", 0),
        ("turn", 1),
        ("job", 2),
        ("worker", 3),
        ("attempt", 4),
        ("lease", 5),
        ("admission", 6),
        ("request", 7),
        ("provider", 8),
        ("revision", 9),
        ("endpoint", 10),
        ("credential", 11),
        ("policy", 12),
        ("adapter", 13),
        ("request limit", 14),
        ("deadline", 15),
    ]
    .into_iter()
    .map(|(name, mutation)| {
        let mut changed = request.clone();
        match mutation {
            0 => changed.tenant_id = id(ResourceKind::Tenant, 0x7e00),
            1 => changed.model_turn_id = id(ResourceKind::ModelTurn, 0x7e01),
            2 => changed.job_id = id(ResourceKind::Job, 0x7e02),
            3 => {
                changed.worker_process_generation_id =
                    id(ResourceKind::WorkerProcessGeneration, 0x7e03)
            }
            4 => changed.attempt_no += 1,
            5 => changed.lease_generation += 1,
            6 => changed.admission_digest = named_digest("wrong admission"),
            7 => changed.model_request_digest = named_digest("wrong request"),
            8 => changed.provider_deployment.deployment_digest = named_digest("wrong provider"),
            9 => {
                changed.provider_revision.semantic_digest = named_digest("wrong provider revision")
            }
            10 => changed.endpoint_identity_digest = named_digest("wrong endpoint"),
            11 => {
                changed.secret_bindings[0].secret_binding_id =
                    id(ResourceKind::SecretBinding, 0x7e04)
            }
            12 => changed.network_policy.semantic_digest = named_digest("wrong policy"),
            13 => changed.adapter_qualified_name = "other.protocol/v1".to_owned(),
            14 => changed.maximum_request_bytes += 1,
            15 => changed.deadline += Duration::seconds(1),
            _ => unreachable!(),
        }
        (name, changed)
    })
    .collect();
    for (name, changed) in mutations {
        assert_eq!(
            repository.authorize_model_dispatch(&changed).await,
            Err(ModelDispatchAuthorizationError::Rejected),
            "{name}"
        );
        assert!(
            !permit.validate_for(&changed, Utc::now()),
            "permit must bind {name}"
        );
    }

    // Commit revocations so the independent read transaction must observe the current authority.
    // Each update is scoped to this fixture's exact row and restored before its main journey resumes.
    for (label, sql, row_id, denied, restored) in [
        ("membership", "UPDATE insight_platform.tenant_principals SET state=$3 WHERE tenant_id=$1 AND principal_id=$2", fixture.principal_id.to_string(), "revoked", "active"),
        ("provider gate", "UPDATE insight_platform.resources SET gate_state=$3 WHERE tenant_id=$1 AND resource_id=$2", fixture.provider_resource_id.to_string(), "disabled", "enabled"),
        ("model gate", "UPDATE insight_platform.resources SET gate_state=$3 WHERE tenant_id=$1 AND resource_id=$2", fixture.profile_resource_id.to_string(), "disabled", "enabled"),
        ("credential", "UPDATE insight_platform.secret_bindings SET state=$3 WHERE tenant_id=$1 AND secret_binding_id=$2", request.secret_bindings[0].secret_binding_id.to_string(), "revoked", "active"),
    ] {
        assert_eq!(sqlx::query(sql).bind(request.tenant_id.to_string()).bind(&row_id).bind(denied)
            .execute(pool).await.unwrap().rows_affected(), 1);
        let result = repository.authorize_model_dispatch(&request).await;
        assert_eq!(sqlx::query(sql).bind(request.tenant_id.to_string()).bind(&row_id).bind(restored)
            .execute(pool).await.unwrap().rows_affected(), 1);
        assert_eq!(result, Err(ModelDispatchAuthorizationError::Rejected), "{label}");
        assert!(repository.authorize_model_dispatch(&request).await.is_ok(), "restored {label}");
    }
    sqlx::query("UPDATE insight_platform.jobs SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE tenant_id=$1 AND job_id=$2")
        .bind(request.tenant_id.to_string()).bind(request.job_id.to_string()).execute(pool).await.unwrap();
    let expired = repository.authorize_model_dispatch(&request).await;
    sqlx::query(
        "UPDATE insight_platform.jobs SET lease_expires_at=$3 WHERE tenant_id=$1 AND job_id=$2",
    )
    .bind(request.tenant_id.to_string())
    .bind(request.job_id.to_string())
    .bind(lease_expiry)
    .execute(pool)
    .await
    .unwrap();
    assert_eq!(expired, Err(ModelDispatchAuthorizationError::Rejected));
    assert!(repository.authorize_model_dispatch(&request).await.is_ok());
}
