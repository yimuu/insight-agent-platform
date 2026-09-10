//! Actual claimed Remote Context authorization under the installed Security database role.
use super::*;
use futures::FutureExt;
use insight_platform_contracts::{
    ContextDispatchAuthorizationError, ContextDispatchAuthorizationV1, ExactSecretBindingRef,
    SecretBindingPayload, SecretResolutionPolicy,
};
use insight_platform_postgres::repository::NewSecretBinding;
use insight_platform_security::ContextDispatchAuthority;
use std::panic::AssertUnwindSafe;
#[path = "model_security_role.rs"]
mod security_role;

#[test]
fn current_remote_dispatch_is_exact_and_read_only_under_security_role() {
    let _namespace = select_fixture_namespace(0xd0f9);
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .thread_stack_size(16 * 1024 * 1024)
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(runtime.spawn(async {
        let url = std::env::var("PLATFORM_TEST_DATABASE_URL").expect("actual PostgreSQL fixture required");
        let pool = PgPoolOptions::new().max_connections(8).connect(&url).await.unwrap();
        verify_schema(&pool).await.unwrap();
        let owner = PgRepository::new(pool.clone());
        let mut fixture = seed_fixture_with_backend(&pool, &owner, ContextFixtureBackend::RemoteSearch {
            endpoint: CanonicalHttpEndpoint { scheme: CapabilityEndpointScheme::Https, host: "search.example.test".to_owned(), port: 443, base_path: "/v1/query".to_owned() },
            protocol_contract_digest: insight_platform_context::remote_context_protocol_contract_digest(),
            result_mapping_digest: insight_platform_context::remote_context_result_mapping_digest(),
            installed_adapter_digest: named_digest("context-dispatch-fixture-adapter"),
        }).await;
        let secret = seed_authenticated_context(&pool, &owner, &mut fixture).await;
        // A legitimate new membership snapshot between Run and Query is admitted by the existing
        // fresh Context lifecycle. The grant's policy generation still belongs to the frozen Run.
        sqlx::query("UPDATE insight_platform.tenant_principals SET generation=generation+1,version=version+1 WHERE tenant_id=$1 AND principal_id=$2")
            .bind(fixture.tenant_id.to_string()).bind(fixture.principal_id.to_string()).execute(&pool).await.unwrap();
        let created = match execute_create(&owner, create_command(&fixture, 0xe00)).await.unwrap() {
            CommandOutcome::Applied(query) => query,
            _ => panic!("fresh Context fixture unexpectedly replayed"),
        };
        assert_eq!(created.payload.admission.principal.binding_generation, 2);
        assert_eq!(created.payload.admission.grant.policy_generation, 1);
        assert_ne!(created.payload.admission.request.normalized_query_digest, created.payload.admission.request.input.content_digest);
        let job_id = id(ResourceKind::Job, 0xe10);
        execute_prepare(&owner, PrepareContextDispatch {
            audit: audit(&fixture.tenant_id, &fixture.principal_id, 0xe11, "prepare-authorized-context"),
            context_query_id: created.context_query_id, expected_query_version: created.version,
            job_id: job_id.clone(), scheduled_at: created.created_at,
        }).await.unwrap();
        let claimed = claim(&owner, &fixture, job_id, 0xe20).await;
        let wire = RemoteContextSearchRequest::from_admission(fixture.tenant_id.clone(), claimed.claimed.job.job_id.parse().unwrap(),
            u32::try_from(claimed.claimed.job.attempt_no).unwrap(), &claimed.fence(), &claimed.claimed.query.payload.admission, claimed.claimed.query_input.clone()).unwrap();
        wire.validate_at(Utc::now()).unwrap();
        let request = wire.dispatch_authorization().unwrap();
        let role = security_role::ModelSecurityRole::create(&pool).await;
        let security = PgRepository::new(role.pool.clone());
        let result = AssertUnwindSafe(async {
            role.assert_read_boundary().await;
            for sql in [
                "SELECT inline_value FROM insight_platform.run_values LIMIT 0",
                "UPDATE insight_platform.invocations SET version=version WHERE false",
                "UPDATE insight_platform.run_nodes SET version=version WHERE false",
                "SELECT state FROM insight_platform.invocations WHERE false FOR SHARE",
                "SELECT state FROM insight_platform.jobs WHERE false FOR UPDATE",
            ] {
                let error = sqlx::query(sql).execute(&role.pool).await.unwrap_err();
                assert_eq!(error.as_database_error().unwrap().code().as_deref(), Some("42501"));
            }
            assert_authorization(&pool, &security, &fixture, &secret, &wire, &request).await;
        }).catch_unwind().await;
        role.close().await;
        result.unwrap();
        pool.close().await;
    })).unwrap();
}

// Transport/formatter behavior is exercised by Worker/RPC tests. These helpers add the real
// existing orchestration-owned leaf wait and transactional settlement/replay evidence below.
pub(super) fn staged_failure(claim: &ClaimEvidence) -> Failure {
    let evidence:Sha256Digest=canonical_digest(&json!({
        "stage":"fixture_rpc_attempt", "tenant_id":claim.claimed.query.tenant_id,
        "context_query_id":claim.claimed.query.context_query_id, "job_id":claim.claimed.job.job_id,
        "attempt":claim.claimed.job.attempt_no, "admission_digest":claim.claimed.query.payload.admission.canonical_digest,
    })).unwrap().parse().unwrap();
    Failure {
        code: FailureCode::Platform {
            code: PlatformFailureCode::ContextQueryFailed,
        },
        class: FailureClass::External,
        retryability: Retryability::Never,
        safe_message: Some(format!(
            "Remote Context uncertain_dispatch; failure; evidence {evidence}"
        )),
        details_ref: None,
        source: FailureSource::Context,
    }
}

pub(super) async fn quota_snapshot(
    pool: &PgPool,
    claim: &ClaimEvidence,
) -> Vec<(String, i64, i64, i64)> {
    sqlx::query_as("SELECT account.metric,account.reserved_value,account.used_value,reserve.reserved_amount FROM insight_platform.quota_accounts account JOIN insight_platform.quota_ledger reserve ON reserve.tenant_id=account.tenant_id AND reserve.quota_account_id=account.quota_account_id WHERE account.tenant_id=$1 AND reserve.correlation_id=$2 AND reserve.entry_kind='reserve' ORDER BY account.metric")
        .bind(&claim.claimed.job.tenant_id).bind(claim.claimed.job.quota_reservation_id.as_ref().unwrap()).fetch_all(pool).await.unwrap()
}

pub(super) fn assert_failed_evidence(
    failed: &insight_platform_postgres::context_query_repository::PreparedContextExecution,
    expected: &Failure,
    before: &[(String, i64, i64, i64)],
    after: &[(String, i64, i64, i64)],
) {
    assert_eq!(failed.query.state, ContextQueryState::Failed);
    assert_eq!(failed.query.payload.failure.as_ref(), Some(expected));
    assert_eq!(failed.job.state, JobState::Failed.as_str());
    assert!(failed.job.quota_reservation_id.is_none());
    assert_eq!(failed.job.attempt_no, 1);
    let payload: insight_platform_context::ContextJobPayload =
        serde_json::from_value(failed.job.payload.value.clone()).unwrap();
    let digest: Sha256Digest = canonical_digest(&serde_json::to_value(expected).unwrap())
        .unwrap()
        .parse()
        .unwrap();
    assert!(
        matches!(payload.physical_outcome,Some(insight_platform_context::ContextPhysicalOutcomeEvidence::Failed {failure_digest}) if failure_digest==digest)
    );
    assert!(payload.query_consumed);
    assert_eq!(before.len(), 3);
    assert_eq!(after.len(), 3);
    for (old, new) in before.iter().zip(after) {
        assert_eq!(old.0, new.0);
        assert_eq!(old.3, new.3);
        assert_eq!(new.1, old.1 - old.3);
        assert_eq!(
            new.2,
            old.2 + i64::from(old.0 == QuotaDimension::ContextQueries.as_str())
        );
    }
}

// Complete the synthetic seed before any Query admission: new immutable Context/Agent deployments
// bind a real SecretBinding row, and the not-yet-dispatched Run is built from that exact closure.
// No current admission is forged or rewritten, and no provider/network port is substituted.
async fn seed_authenticated_context(
    pool: &PgPool,
    owner: &PgRepository,
    fixture: &mut Fixture,
) -> ExactSecretBindingRef {
    let provider_id = id(ResourceKind::SecretProvider, 0xf00);
    let binding_id = id(ResourceKind::SecretBinding, 0xf00);
    let purpose: insight_platform_contracts::SecretPurpose = "context_api_key".parse().unwrap();
    let resolution = SecretResolutionPolicy::Pinned {
        opaque_version_identity_digest: named_digest("context-fixture-version"),
    };
    owner
        .create_secret_binding(NewSecretBinding {
            tenant_id: fixture.tenant_id.clone(),
            secret_binding_id: binding_id.clone(),
            purpose: purpose.clone(),
            provider_id: provider_id.clone(),
            opaque_reference_ciphertext: vec![1, 2, 3],
            key_id: "fixture-sealer".to_owned(),
            reference_digest: named_digest("fixture-reference"),
            payload: SecretBindingPayload {
                provider_id: provider_id.clone(),
                resolution_policy: resolution.clone(),
            },
        })
        .await
        .unwrap();
    let secret =
        ExactSecretBindingRef::build(binding_id, 1, provider_id, purpose, resolution).unwrap();
    let mut context: serde_json::Value = sqlx::query_scalar(
        "SELECT bindings FROM insight_platform.deployments WHERE tenant_id=$1 AND deployment_id=$2",
    )
    .bind(fixture.tenant_id.to_string())
    .bind(fixture.context_deployment.deployment_id.to_string())
    .fetch_one(pool)
    .await
    .unwrap();
    context.as_object_mut().unwrap().remove("schema_version");
    let DeploymentClosure::ContextSourceInterface(mut context) =
        serde_json::from_value(context).unwrap()
    else {
        panic!("Context fixture kind")
    };
    context.secret_bindings = vec![secret.clone()];
    let mut implementation: serde_json::Value = sqlx::query_scalar("SELECT payload FROM insight_platform.resource_versions WHERE tenant_id=$1 AND resource_version_id=$2")
        .bind(fixture.tenant_id.to_string()).bind(context.implementation.revision_id.to_string()).fetch_one(pool).await.unwrap();
    implementation
        .as_object_mut()
        .unwrap()
        .remove("schema_version");
    let mut published: PublishedVersionPayload = serde_json::from_value(implementation).unwrap();
    let ResourceDocument::ContextSourceImplementation(implementation) = &mut published.document
    else {
        panic!("remote implementation seed")
    };
    implementation.contract.credential_requirements = vec![secret.purpose.clone()];
    implementation.contract_digest =
        canonical_digest(&serde_json::to_value(&implementation.contract).unwrap())
            .unwrap()
            .parse()
            .unwrap();
    let revision = ExactVersionRef::new(
        id(ResourceKind::ContextSourceImplementationRevision, 0xf04),
        canonical_digest(&serde_json::to_value(&published.document).unwrap())
            .unwrap()
            .parse()
            .unwrap(),
    )
    .unwrap();
    insert_version(
        pool,
        &fixture.tenant_id,
        &id(ResourceKind::ContextSourceImplementation, 0x13),
        RegistryResourceKind::ContextSourceImplementation,
        &revision,
        2,
        &fixture.principal_id,
        published,
    )
    .await;
    context.implementation = revision;
    let context =
        TypedPayload::new(1, &DeploymentClosure::ContextSourceInterface(context)).unwrap();
    let context_id = id(ResourceKind::ContextDeployment, 0xf01);
    insert_deployment(
        pool,
        &fixture.tenant_id,
        &context_id,
        &id(ResourceKind::ContextSourceInterface, 0x12),
        &fixture.interface_revision.revision_id,
        &fixture.principal_id,
        &context,
    )
    .await;
    let old_context = fixture.context_deployment.deployment_id.clone();
    fixture.context_deployment =
        ExactDeploymentRef::new(context_id.clone(), context.digest.parse().unwrap()).unwrap();
    // Reuse the seed's unused finite accounts; only the newly seeded target changes, never usage.
    assert_eq!(sqlx::query("UPDATE insight_platform.quota_accounts SET scope_id=$3 WHERE tenant_id=$1 AND scope_kind='context_deployment' AND scope_id=$2 AND used_value=0 AND reserved_value=0")
        .bind(fixture.tenant_id.to_string()).bind(old_context.to_string()).bind(context_id.to_string()).execute(pool).await.unwrap().rows_affected(), 2);
    let bindings: serde_json::Value = sqlx::query_scalar(
        "SELECT bindings FROM insight_platform.runs WHERE tenant_id=$1 AND run_id=$2",
    )
    .bind(fixture.tenant_id.to_string())
    .bind(fixture.run_id.to_string())
    .fetch_one(pool)
    .await
    .unwrap();
    let old: RunBindingsSnapshot = serde_json::from_value(bindings).unwrap();
    let mut agent: serde_json::Value = sqlx::query_scalar(
        "SELECT bindings FROM insight_platform.deployments WHERE tenant_id=$1 AND deployment_id=$2",
    )
    .bind(fixture.tenant_id.to_string())
    .bind(old.agent.deployment_id.to_string())
    .fetch_one(pool)
    .await
    .unwrap();
    agent.as_object_mut().unwrap().remove("schema_version");
    let DeploymentClosure::Agent(mut agent) = serde_json::from_value(agent).unwrap() else {
        panic!("Agent fixture kind")
    };
    let agent_id = id(ResourceKind::AgentDeployment, 0xf02);
    for slot in &mut agent.slots {
        if let FrozenSlotTarget::Context { binding } = &mut slot.target {
            **binding = ContextBindingSnapshot::build(
                id(ResourceKind::ContextBinding, 0xf03),
                agent_id.clone(),
                fixture.context_deployment.clone(),
                binding.consistency.clone(),
                binding.allowed_projection.clone(),
                binding.authorization_policy.clone(),
                binding.ranking_policy.clone(),
            )
            .unwrap();
        }
    }
    let payload = TypedPayload::new(1, &DeploymentClosure::Agent(agent.clone())).unwrap();
    insert_deployment(
        pool,
        &fixture.tenant_id,
        &agent_id,
        &id(ResourceKind::Agent, 0x11),
        &agent.plan.revision_id,
        &fixture.principal_id,
        &payload,
    )
    .await;
    let bindings = RunBindingsSnapshot::build(
        ExactDeploymentRef::new(agent_id.clone(), payload.digest.parse().unwrap()).unwrap(),
        old.principal,
        &agent,
    )
    .unwrap();
    let current = TypedPayload::from_versioned(
        1,
        &RunCurrentSnapshot::initial(
            fixture.run_id.clone(),
            agent_id.clone(),
            id(ResourceKind::RunValue, 0x63),
        ),
        1_048_576,
    )
    .unwrap();
    sqlx::query("UPDATE insight_platform.runs SET agent_deployment_id=$3,bindings=$4,bindings_digest=$5,current_payload=$6,current_payload_digest=$7 WHERE tenant_id=$1 AND run_id=$2")
        .bind(fixture.tenant_id.to_string()).bind(fixture.run_id.to_string()).bind(agent_id.to_string()).bind(serde_json::to_value(&bindings).unwrap()).bind(bindings.canonical_digest.to_string()).bind(current.value).bind(current.digest).execute(pool).await.unwrap();
    secret
}

async fn assert_rejected(
    security: &PgRepository,
    request: &ContextDispatchAuthorizationV1,
    label: &str,
) {
    assert_eq!(
        security.authorize_context_dispatch(request).await,
        Err(ContextDispatchAuthorizationError::Rejected),
        "{label}"
    );
}

async fn assert_authorization(
    pool: &PgPool,
    security: &PgRepository,
    fixture: &Fixture,
    secret: &ExactSecretBindingRef,
    wire: &RemoteContextSearchRequest,
    request: &ContextDispatchAuthorizationV1,
) {
    let before = durable_counts(pool, &fixture.tenant_id).await;
    let permit = security.authorize_context_dispatch(request).await.unwrap();
    assert!(permit.validate_for(request, Utc::now()));
    let lease: DateTime<Utc> = sqlx::query_scalar(
        "SELECT lease_expires_at FROM insight_platform.jobs WHERE tenant_id=$1 AND job_id=$2",
    )
    .bind(fixture.tenant_id.to_string())
    .bind(request.job_id.to_string())
    .fetch_one(pool)
    .await
    .unwrap();
    assert!(permit.valid_until <= lease);
    for case in 0..11 {
        let mut changed = request.clone();
        match case {
            0 => changed.tenant_id = fixture.other_tenant_id.clone(),
            1 => changed.context_query_id = id(ResourceKind::ContextQuery, 0xf80),
            2 => changed.job_id = id(ResourceKind::Job, 0xf80),
            3 => {
                changed.worker_process_generation_id =
                    id(ResourceKind::WorkerProcessGeneration, 0xf80)
            }
            4 => changed.physical_attempt += 1,
            5 => changed.lease_generation += 1,
            6 => changed.lease_token_digest = named_digest("wrong-token"),
            7 => changed.admission_digest = named_digest("wrong-admission"),
            8 => changed.request_metadata_digest = named_digest("wrong-metadata"),
            9 => changed.input_content_digest = request.request_metadata_digest.clone(),
            10 => changed.deadline = Utc::now() - Duration::seconds(1),
            _ => unreachable!(),
        }
        assert_rejected(
            security,
            &changed,
            &format!("authorization identity {case}"),
        )
        .await;
        assert!(!permit.validate_for(&changed, Utc::now()));
    }
    for case in 0..20 {
        let mut changed = wire.clone();
        match case {
            0 => changed.context_deployment.deployment_digest = named_digest("wrong-deployment"),
            1 => {
                changed.implementation_revision.semantic_digest =
                    named_digest("wrong-implementation")
            }
            2 => changed.protocol_contract_digest = named_digest("wrong-protocol"),
            3 => changed.result_mapping_digest = named_digest("wrong-mapping"),
            4 => {
                changed.endpoint.host = "other.example.test".to_owned();
                changed.endpoint_identity_digest = changed.endpoint.canonical_digest().unwrap();
            }
            5 => changed.region = "cn-west-1".parse().unwrap(),
            6 => changed.secret_bindings.clear(),
            7 => changed.network_policy.semantic_digest = named_digest("wrong-network"),
            8 => changed.tls_policy.semantic_digest = named_digest("wrong-tls"),
            9 => changed.trust_policy.semantic_digest = named_digest("wrong-trust"),
            10 => changed.normalized_query_digest = named_digest("wrong-query"),
            11 => changed.normalized_filter_digest = named_digest("wrong-filter"),
            12 => changed.requested_projection = vec!["other".to_owned()],
            13 => changed.maximum_classification = DataClassification::Public,
            14 => changed.page_size += 1,
            15 => changed.cursor_digest = Some(named_digest("other-cursor")),
            16 => changed.maximum_request_bytes -= 1,
            17 => changed.maximum_response_bytes -= 1,
            18 => {
                changed.query_input = ValueRef::Inline {
                    value: json!({"question":"body-canary"}),
                }
            }
            19 => changed.deadline -= Duration::seconds(1),
            _ => unreachable!(),
        }
        assert_ne!(
            &changed, wire,
            "mutation must actually change wire case {case}"
        );
        let auth = changed.dispatch_authorization().unwrap();
        assert_rejected(security, &auth, &format!("complete wire binding {case}")).await;
    }
    assert_eq!(durable_counts(pool, &fixture.tenant_id).await, before);

    for (label,sql,target,bad,good) in [
        ("tenant suspended","UPDATE insight_platform.tenants SET state=$3 WHERE tenant_id=$1 AND tenant_id=$2",fixture.tenant_id.to_string(),"suspended","active"),
        ("principal suspended","UPDATE insight_platform.principals SET state=$3 WHERE principal_id=$2 AND EXISTS(SELECT 1 FROM insight_platform.tenant_principals WHERE tenant_id=$1 AND principal_id=$2)",fixture.principal_id.to_string(),"suspended","active"),
        ("membership revoked","UPDATE insight_platform.tenant_principals SET state=$3 WHERE tenant_id=$1 AND principal_id=$2",fixture.principal_id.to_string(),"revoked","active"),
        ("Context gate","UPDATE insight_platform.resources SET gate_state=$3 WHERE tenant_id=$1 AND resource_id=$2",id(ResourceKind::ContextSourceInterface,0x12).to_string(),"disabled","enabled"),
        ("implementation gate","UPDATE insight_platform.resources SET gate_state=$3 WHERE tenant_id=$1 AND resource_id=$2",id(ResourceKind::ContextSourceImplementation,0x13).to_string(),"disabled","enabled"),
        ("policy gate","UPDATE insight_platform.resources SET gate_state=$3 WHERE tenant_id=$1 AND resource_id=$2",id(ResourceKind::Policy,0x10).to_string(),"disabled","enabled"),
        ("secret revoked","UPDATE insight_platform.secret_bindings SET state=$3 WHERE tenant_id=$1 AND secret_binding_id=$2",secret.secret_binding_id.to_string(),"revoked","active"),
        ("secret purpose","UPDATE insight_platform.secret_bindings SET purpose=$3 WHERE tenant_id=$1 AND secret_binding_id=$2",secret.secret_binding_id.to_string(),"other_key","context_api_key"),
        ("node kind","UPDATE insight_platform.run_nodes SET node_kind=$3 WHERE tenant_id=$1 AND node_id=$2",fixture.node_id.to_string(),"capability_call","context_query"),
    ] {
        assert_eq!(sqlx::query(sql).bind(fixture.tenant_id.to_string()).bind(&target).bind(bad).execute(pool).await.unwrap().rows_affected(),1);
        let result=security.authorize_context_dispatch(request).await;
        sqlx::query(sql).bind(fixture.tenant_id.to_string()).bind(&target).bind(good).execute(pool).await.unwrap();
        assert_eq!(result,Err(ContextDispatchAuthorizationError::Rejected),"{label}");
        assert!(security.authorize_context_dispatch(request).await.is_ok(),"restored {label}");
    }
    // Current membership changes after Query admission cannot reuse its old entitlement snapshot.
    sqlx::query("UPDATE insight_platform.tenant_principals SET version=version+1 WHERE tenant_id=$1 AND principal_id=$2")
        .bind(fixture.tenant_id.to_string()).bind(fixture.principal_id.to_string()).execute(pool).await.unwrap();
    let revoked = security.authorize_context_dispatch(request).await;
    sqlx::query("UPDATE insight_platform.tenant_principals SET version=version-1 WHERE tenant_id=$1 AND principal_id=$2")
        .bind(fixture.tenant_id.to_string()).bind(fixture.principal_id.to_string()).execute(pool).await.unwrap();
    assert_eq!(revoked, Err(ContextDispatchAuthorizationError::Rejected));
    assert_current_permission(pool, security, fixture, request).await;
    // Heartbeats advance Job version without changing the stable lease identity.
    sqlx::query(
        "UPDATE insight_platform.jobs SET version=version+1 WHERE tenant_id=$1 AND job_id=$2",
    )
    .bind(fixture.tenant_id.to_string())
    .bind(request.job_id.to_string())
    .execute(pool)
    .await
    .unwrap();
    assert!(security.authorize_context_dispatch(request).await.is_ok());
    sqlx::query("UPDATE insight_platform.jobs SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE tenant_id=$1 AND job_id=$2")
        .bind(fixture.tenant_id.to_string()).bind(request.job_id.to_string()).execute(pool).await.unwrap();
    let expired = security.authorize_context_dispatch(request).await;
    sqlx::query(
        "UPDATE insight_platform.jobs SET lease_expires_at=$3 WHERE tenant_id=$1 AND job_id=$2",
    )
    .bind(fixture.tenant_id.to_string())
    .bind(request.job_id.to_string())
    .bind(lease)
    .execute(pool)
    .await
    .unwrap();
    assert_eq!(expired, Err(ContextDispatchAuthorizationError::Rejected));
    assert_run_controls(pool, security, fixture, request).await;
    assert_run_bindings(pool, security, fixture, request).await;
    assert!(security.authorize_context_dispatch(request).await.is_ok());
}

async fn assert_current_permission(
    pool: &PgPool,
    security: &PgRepository,
    fixture: &Fixture,
    request: &ContextDispatchAuthorizationV1,
) {
    let mut permissions:serde_json::Value=sqlx::query_scalar("SELECT permissions FROM insight_platform.tenant_principals WHERE tenant_id=$1 AND principal_id=$2")
        .bind(fixture.tenant_id.to_string()).bind(fixture.principal_id.to_string()).fetch_one(pool).await.unwrap();
    permissions
        .as_object_mut()
        .unwrap()
        .remove("schema_version");
    let original: TenantPrincipalPayload = serde_json::from_value(permissions).unwrap();
    let denied = TenantPrincipalPayload {
        permissions: PermissionSet::new(vec![Permission::OperationRead]).unwrap(),
    };
    for (payload, expected) in [(&denied, false), (&original, true)] {
        let typed = TypedPayload::new(1, payload).unwrap();
        sqlx::query("UPDATE insight_platform.tenant_principals SET permissions=$3,permissions_digest=$4 WHERE tenant_id=$1 AND principal_id=$2")
            .bind(fixture.tenant_id.to_string()).bind(fixture.principal_id.to_string()).bind(typed.value).bind(typed.digest).execute(pool).await.unwrap();
        let result = security.authorize_context_dispatch(request).await;
        if expected {
            assert!(result.is_ok());
        } else {
            assert_eq!(result, Err(ContextDispatchAuthorizationError::Rejected));
        }
    }
}

async fn assert_run_bindings(
    pool: &PgPool,
    security: &PgRepository,
    fixture: &Fixture,
    request: &ContextDispatchAuthorizationV1,
) {
    let original: serde_json::Value = sqlx::query_scalar(
        "SELECT bindings FROM insight_platform.runs WHERE tenant_id=$1 AND run_id=$2",
    )
    .bind(fixture.tenant_id.to_string())
    .bind(fixture.run_id.to_string())
    .fetch_one(pool)
    .await
    .unwrap();
    let original: RunBindingsSnapshot = serde_json::from_value(original).unwrap();
    let mut closure: serde_json::Value = sqlx::query_scalar(
        "SELECT bindings FROM insight_platform.deployments WHERE tenant_id=$1 AND deployment_id=$2",
    )
    .bind(fixture.tenant_id.to_string())
    .bind(original.agent.deployment_id.to_string())
    .fetch_one(pool)
    .await
    .unwrap();
    closure.as_object_mut().unwrap().remove("schema_version");
    let DeploymentClosure::Agent(closure) = serde_json::from_value(closure).unwrap() else {
        panic!("Agent seed")
    };
    let principal = &original.principal;
    let changed = PrincipalSnapshot::build(
        principal.tenant_id.clone(),
        principal.principal_id.clone(),
        principal.principal_kind,
        principal.permissions.clone(),
        principal.principal_version,
        principal.binding_generation + 1,
        principal.binding_version,
    )
    .unwrap();
    let changed = RunBindingsSnapshot::build(original.agent.clone(), changed, &closure).unwrap();
    sqlx::query("UPDATE insight_platform.runs SET bindings=$3,bindings_digest=$4 WHERE tenant_id=$1 AND run_id=$2")
        .bind(fixture.tenant_id.to_string()).bind(fixture.run_id.to_string()).bind(serde_json::to_value(&changed).unwrap()).bind(changed.canonical_digest.to_string()).execute(pool).await.unwrap();
    let result = security.authorize_context_dispatch(request).await;
    sqlx::query("UPDATE insight_platform.runs SET bindings=$3,bindings_digest=$4 WHERE tenant_id=$1 AND run_id=$2")
        .bind(fixture.tenant_id.to_string()).bind(fixture.run_id.to_string()).bind(serde_json::to_value(&original).unwrap()).bind(original.canonical_digest.to_string()).execute(pool).await.unwrap();
    assert_eq!(result, Err(ContextDispatchAuthorizationError::Rejected));
}

async fn assert_run_controls(
    pool: &PgPool,
    security: &PgRepository,
    fixture: &Fixture,
    request: &ContextDispatchAuthorizationV1,
) {
    let original: serde_json::Value = sqlx::query_scalar(
        "SELECT current_payload FROM insight_platform.runs WHERE tenant_id=$1 AND run_id=$2",
    )
    .bind(fixture.tenant_id.to_string())
    .bind(fixture.run_id.to_string())
    .fetch_one(pool)
    .await
    .unwrap();
    let original: RunCurrentSnapshot = serde_json::from_value(original).unwrap();
    let bindings: serde_json::Value = sqlx::query_scalar(
        "SELECT bindings FROM insight_platform.runs WHERE tenant_id=$1 AND run_id=$2",
    )
    .bind(fixture.tenant_id.to_string())
    .bind(fixture.run_id.to_string())
    .fetch_one(pool)
    .await
    .unwrap();
    let bindings: RunBindingsSnapshot = serde_json::from_value(bindings).unwrap();
    for case in 0..3 {
        let mut current = original.clone();
        let decision = match case {
            0 => insight_platform_orchestrator::decide_pause(
                &current.control,
                current.control.pause_generation,
                true,
            ),
            1 => insight_platform_orchestrator::decide_cancel(
                &current.control,
                current.control.cancel_generation,
                Utc::now(),
                "fixture_cancel".to_owned(),
                bindings.principal.clone(),
            ),
            2 => insight_platform_orchestrator::decide_timeout(
                &current.control,
                current.control.timeout_generation,
                Utc::now(),
                Utc::now() - Duration::seconds(1),
                "running".to_owned(),
                1,
            ),
            _ => unreachable!(),
        }
        .unwrap();
        let insight_platform_orchestrator::ControlDecision::Updated(control) = decision else {
            panic!("fresh control")
        };
        current.control = control;
        current.control.validate().unwrap();
        write_current(pool, fixture, &current).await;
        let result = security.authorize_context_dispatch(request).await;
        write_current(pool, fixture, &original).await;
        assert_eq!(
            result,
            Err(ContextDispatchAuthorizationError::Rejected),
            "Run control {case}"
        );
    }
}
async fn write_current(pool: &PgPool, fixture: &Fixture, current: &RunCurrentSnapshot) {
    let payload = TypedPayload::from_versioned(1, current, 1_048_576).unwrap();
    sqlx::query("UPDATE insight_platform.runs SET current_payload=$3,current_payload_digest=$4,pause_generation=$5,cancel_generation=$6,timeout_generation=$7 WHERE tenant_id=$1 AND run_id=$2")
        .bind(fixture.tenant_id.to_string()).bind(fixture.run_id.to_string()).bind(payload.value).bind(payload.digest)
        .bind(i64::try_from(current.control.pause_generation).unwrap()).bind(i64::try_from(current.control.cancel_generation).unwrap()).bind(i64::try_from(current.control.timeout_generation).unwrap()).execute(pool).await.unwrap();
}
async fn durable_counts(pool: &PgPool, tenant: &ResourceId) -> serde_json::Value {
    sqlx::query_scalar("SELECT jsonb_build_array((SELECT count(*) FROM insight_platform.receipts WHERE tenant_id=$1),(SELECT count(*) FROM insight_platform.events WHERE tenant_id=$1),(SELECT count(*) FROM insight_platform.outbox_events WHERE tenant_id=$1),(SELECT sum(version) FROM insight_platform.jobs WHERE tenant_id=$1),(SELECT sum(version) FROM insight_platform.invocations WHERE tenant_id=$1),(SELECT sum(used_value) FROM insight_platform.quota_accounts WHERE tenant_id=$1),(SELECT sum(reserved_value) FROM insight_platform.quota_accounts WHERE tenant_id=$1))")
        .bind(tenant.to_string()).fetch_one(pool).await.unwrap()
}
