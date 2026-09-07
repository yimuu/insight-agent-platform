//! Called by the resource lifecycle fixture after its real Agent publication and activation.
//! Its typed Selection Policy rows exercise reads, not Policy publication admission.
use insight_platform_contracts::{
    canonical_digest, CandidateSelectionMode, CandidateSelectionPolicyDocument, DependencySlotKind,
    DeploymentClosure, ExactDeploymentRef, ExactPolicyBinding, ExactVersionRef, Permission,
    PermissionSet, PolicyKind, PrincipalBindingsPayload, PrincipalKind, PublishedVersionPayload,
    RegistryResourceKind, ResourceDocument, ResourceId, ResourceKind, Sha256Digest,
    TenantPrincipalPayload, TypedPayload,
};
use insight_platform_postgres::repository::{NewPrincipal, NewTenantPrincipal, PgRepository};
use insight_platform_registry::authoring::{
    AuthoringDependencyFiltersV1, AuthoringDeploymentSelectorV1, AuthoringQueryError,
    AuthoringResolutionV1, AuthoringSlotSelectionV1, AuthoringSlotTargetV1,
    DiscoverAuthoringDependencies, ResolveAgentBindingsRequestV1,
};
use serde::de::DeserializeOwned;
use serde_json::{json, Value};
use sqlx::{PgPool, Row};

fn fresh(kind: ResourceKind) -> ResourceId {
    ResourceId::from_uuid_v7(kind, uuid::Uuid::now_v7()).unwrap()
}
fn digest(value: &str) -> Sha256Digest {
    canonical_digest(&json!({"authoring_query_fixture":value}))
        .unwrap()
        .parse()
        .unwrap()
}
async fn reader(
    repository: &PgRepository,
    tenant: &ResourceId,
    permissions: Vec<Permission>,
) -> ResourceId {
    let principal = fresh(ResourceKind::Principal);
    repository
        .create_principal(NewPrincipal {
            principal_id: principal.clone(),
            authentication_authority_digest: digest(&uuid::Uuid::now_v7().to_string()),
            subject_digest: digest(&principal.to_string()),
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
                permissions: PermissionSet::new(permissions).unwrap(),
            },
        })
        .await
        .unwrap();
    principal
}
async fn counts(pool: &PgPool, tenant: &ResourceId) -> (i64, i64, i64, i64) {
    sqlx::query_as("SELECT (SELECT count(*) FROM insight_platform.receipts WHERE tenant_id=$1), (SELECT count(*) FROM insight_platform.jobs WHERE tenant_id=$1), (SELECT count(*) FROM insight_platform.events WHERE tenant_id=$1), (SELECT count(*) FROM insight_platform.outbox_events WHERE tenant_id=$1)")
        .bind(tenant.to_string()).fetch_one(pool).await.unwrap()
}
fn query(
    tenant: &ResourceId,
    principal: &ResourceId,
    environment: &str,
    contract: Option<Sha256Digest>,
) -> DiscoverAuthoringDependencies {
    DiscoverAuthoringDependencies {
        tenant_id: tenant.clone(),
        principal_id: principal.clone(),
        principal_kind: PrincipalKind::AgentRunner,
        filters: AuthoringDependencyFiltersV1 {
            kind: DependencySlotKind::ChildAgent,
            environment: Some(environment.into()),
            interface_contract_digest: contract,
        },
        page_size: 50,
        snapshot_at: None,
        boundary: None,
    }
}
fn resolve(
    candidates: Vec<AuthoringDeploymentSelectorV1>,
    contract: &Sha256Digest,
    policy: &ExactPolicyBinding,
) -> ResolveAgentBindingsRequestV1 {
    ResolveAgentBindingsRequestV1 {
        schema_version: 1,
        slots: vec![AuthoringSlotSelectionV1 {
            slot_id: "published-child".into(),
            requirement_digest: digest("child slot requirement"),
            interface_contract_digest: Some(contract.clone()),
            target: AuthoringSlotTargetV1::ChildAgent {
                candidates,
                selection_policy: policy.clone(),
            },
        }],
    }
}

fn decode_payload<T: DeserializeOwned>(mut value: Value, expected_digest: &str) -> T {
    assert_eq!(canonical_digest(&value).unwrap(), expected_digest);
    assert_eq!(
        value.as_object_mut().unwrap().remove("schema_version"),
        Some(json!(1))
    );
    serde_json::from_value(value).unwrap()
}

async fn seed_selection_policy(
    pool: &PgPool,
    tenant: &ResourceId,
    original: &ExactPolicyBinding,
) -> ExactPolicyBinding {
    let mut transaction = pool.begin().await.unwrap();
    let row = sqlx::query("SELECT r.resource_id,v.payload,v.payload_digest,d.bindings,d.bindings_digest,d.created_by,d.environment FROM insight_platform.resources r JOIN insight_platform.resource_versions v ON v.tenant_id=r.tenant_id AND v.resource_id=r.resource_id JOIN insight_platform.deployments d ON d.tenant_id=v.tenant_id AND d.resource_version_id=v.resource_version_id WHERE r.tenant_id=$1 AND v.resource_version_id=$2 AND v.content_digest=$3 AND d.deployment_id=$4 AND d.bindings_digest=$5 FOR UPDATE OF r")
        .bind(tenant.to_string()).bind(original.revision.revision_id.to_string()).bind(original.revision.semantic_digest.to_string())
        .bind(original.deployment.deployment_id.to_string()).bind(original.deployment.deployment_digest.to_string())
        .fetch_one(&mut *transaction).await.unwrap();
    let resource: String = row.try_get("resource_id").unwrap();
    let creator: String = row.try_get("created_by").unwrap();
    let environment: String = row.try_get("environment").unwrap();
    let mut published: PublishedVersionPayload = decode_payload(
        row.try_get("payload").unwrap(),
        &row.try_get::<String, _>("payload_digest").unwrap(),
    );
    published
        .validate_for(RegistryResourceKind::Policy, &original.revision.revision_id)
        .unwrap();
    let ResourceDocument::Policy(spec) = &mut published.document else {
        panic!("fixture must provide actual Policy")
    };
    assert_eq!(spec.policy_kind, PolicyKind::Authorization);
    let selection = CandidateSelectionPolicyDocument {
        schema_version: 1,
        mode: CandidateSelectionMode::OnlyCandidate,
        route_schema_digest: None,
    };
    spec.policy_kind = PolicyKind::Selection;
    spec.rules_digest = selection.canonical_digest().unwrap();
    spec.selection = Some(selection);
    let version = fresh(ResourceKind::PolicyRevision);
    published
        .validate_for(RegistryResourceKind::Policy, &version)
        .unwrap();
    let semantic_digest: Sha256Digest =
        canonical_digest(&serde_json::to_value(&published.document).unwrap())
            .unwrap()
            .parse()
            .unwrap();
    let revision = ExactVersionRef::new(version, semantic_digest).unwrap();
    let payload = TypedPayload::new(1, &published).unwrap();
    let mut closure: DeploymentClosure = decode_payload(
        row.try_get("bindings").unwrap(),
        &row.try_get::<String, _>("bindings_digest").unwrap(),
    );
    let DeploymentClosure::Policy(policy) = &mut closure else {
        panic!("fixture must provide Policy closure")
    };
    assert_eq!(policy.policy_revision, original.revision);
    policy.policy_revision = revision.clone();
    closure.validate().unwrap();
    let bindings = TypedPayload::new(1, &closure).unwrap();
    let deployment = ExactDeploymentRef::new(
        fresh(ResourceKind::PolicyDeployment),
        bindings.digest.parse().unwrap(),
    )
    .unwrap();
    let revision_no: i64 = sqlx::query_scalar("SELECT COALESCE(max(revision_no),0)+1 FROM insight_platform.resource_versions WHERE tenant_id=$1 AND resource_id=$2 AND resource_version_kind='policy_revision'")
        .bind(tenant.to_string()).bind(&resource).fetch_one(&mut *transaction).await.unwrap();
    sqlx::query("INSERT INTO insight_platform.resource_versions (tenant_id,resource_version_id,resource_id,resource_version_kind,revision_no,content_digest,payload_schema_version,payload,payload_digest,created_by) VALUES ($1,$2,$3,'policy_revision',$4,$5,$6,$7,$8,$9)")
        .bind(tenant.to_string()).bind(revision.revision_id.to_string()).bind(&resource).bind(revision_no).bind(revision.semantic_digest.to_string())
        .bind(payload.schema_version).bind(payload.value).bind(payload.digest).bind(&creator).execute(&mut *transaction).await.unwrap();
    sqlx::query("INSERT INTO insight_platform.deployments (tenant_id,deployment_id,resource_id,resource_version_id,environment,bindings_digest,payload_schema_version,bindings,created_by) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)")
        .bind(tenant.to_string()).bind(deployment.deployment_id.to_string()).bind(&resource).bind(revision.revision_id.to_string()).bind(environment)
        .bind(bindings.digest).bind(bindings.schema_version).bind(bindings.value).bind(creator).execute(&mut *transaction).await.unwrap();
    transaction.commit().await.unwrap();
    ExactPolicyBinding {
        deployment,
        revision,
    }
}

/// `authorization_policy` must be the lifecycle fixture's actual published Authorization Policy.
/// The helper adds new typed Selection revision/deployment rows without changing existing versions.
pub async fn assert_product_authoring_queries(
    repository: &PgRepository,
    tenant: &ResourceId,
    agent: &ResourceId,
    deployment: &ExactDeploymentRef,
    contract: &Sha256Digest,
    authorization_policy: &ExactPolicyBinding,
) {
    let pool = repository.pool();
    // Reuse the real published deployment and its environment, without changing its owner rows.
    let environment: String = sqlx::query_scalar("SELECT environment FROM insight_platform.deployments WHERE tenant_id=$1 AND resource_id=$2 AND deployment_id=$3 AND bindings_digest=$4")
        .bind(tenant.to_string()).bind(agent.to_string()).bind(deployment.deployment_id.to_string()).bind(deployment.deployment_digest.to_string())
        .fetch_one(pool).await.unwrap();
    let allowed = reader(
        repository,
        tenant,
        vec![Permission::AgentRead, Permission::PolicyRead],
    )
    .await;
    let denied_read = reader(repository, tenant, vec![Permission::PolicyRead]).await;
    let denied_policy = reader(repository, tenant, vec![Permission::AgentRead]).await;
    let can_run = reader(
        repository,
        tenant,
        vec![
            Permission::AgentRead,
            Permission::PolicyRead,
            Permission::AgentRun,
        ],
    )
    .await;
    let selection_policy = seed_selection_policy(pool, tenant, authorization_policy).await;
    let before = counts(pool, tenant).await;

    for requested in [
        None,
        Some(contract.clone()),
        Some(digest("different interface")),
    ] {
        let filters = query(tenant, &allowed, &environment, requested.clone());
        let page = repository
            .discover_agent_authoring_dependencies(filters.clone())
            .await
            .unwrap();
        let item = page
            .items
            .iter()
            .find(|item| &item.resource_id == agent)
            .expect("actual active Agent is discoverable");
        item.validate_for(&filters.filters).unwrap();
        assert_eq!(&item.deployment, deployment);
        assert_eq!(&item.interface_contract_digest, contract);
        assert_eq!(
            item.contract_match,
            requested.as_ref().map(|value| value == contract)
        );
        assert!(
            !item.call_authorized,
            "discovery read does not grant AgentRun"
        );
    }
    let active = AuthoringDeploymentSelectorV1::Active {
        resource_id: agent.clone(),
        environment: environment.clone(),
    };
    let exact = AuthoringDeploymentSelectorV1::Exact {
        deployment: deployment.clone(),
    };
    let mut exact_binding = None;
    for (principal, expected_call_authorized) in [(&allowed, false), (&can_run, true)] {
        let page = repository
            .discover_agent_authoring_dependencies(query(
                tenant,
                principal,
                &environment,
                Some(contract.clone()),
            ))
            .await
            .unwrap();
        assert_eq!(
            page.items
                .iter()
                .find(|item| &item.resource_id == agent)
                .unwrap()
                .call_authorized,
            expected_call_authorized
        );
        for selector in [active.clone(), exact.clone()] {
            let request = resolve(vec![selector], contract, &selection_policy);
            let response = repository
                .resolve_agent_authoring_bindings(
                    tenant,
                    principal,
                    PrincipalKind::AgentRunner,
                    &request,
                )
                .await
                .unwrap();
            response.validate_for(&request).unwrap();
            let AuthoringResolutionV1::Resolved {
                binding,
                observed_contract_digests,
                contract_match,
                call_authorized,
                deployment_features,
            } = response.slots.into_iter().next().unwrap().resolution
            else {
                panic!("actual Selection Policy must resolve")
            };
            assert_eq!(
                deployment_features.len(),
                1,
                "ChildAgent resolves its actual frozen Plan features"
            );
            assert_eq!(&deployment_features[0].deployment, deployment);
            assert_eq!(&deployment_features[0].interface_contract_digest, contract);
            deployment_features[0].validate().unwrap();
            assert_eq!(observed_contract_digests, vec![contract.clone()]);
            assert_eq!(contract_match, Some(true));
            assert_eq!(call_authorized, expected_call_authorized);
            if let Some(expected) = &exact_binding {
                assert_eq!(expected, &binding);
            } else {
                exact_binding = Some(binding);
            }
        }
    }
    let mismatch = resolve(
        vec![exact.clone()],
        &digest("mismatched resolved contract"),
        &selection_policy,
    );
    let response = repository
        .resolve_agent_authoring_bindings(tenant, &allowed, PrincipalKind::AgentRunner, &mismatch)
        .await
        .unwrap();
    assert!(matches!(
        response.slots[0].resolution,
        AuthoringResolutionV1::Rejected {
            code: AuthoringQueryError::ContractMismatch
        }
    ));
    let absent = AuthoringDeploymentSelectorV1::Exact {
        deployment: ExactDeploymentRef::new(
            fresh(ResourceKind::AgentDeployment),
            digest("absent deployment"),
        )
        .unwrap(),
    };
    for denied in [&denied_read, &denied_policy] {
        for filter_environment in [environment.as_str(), "missing-environment"] {
            assert!(matches!(
                repository
                    .discover_agent_authoring_dependencies(query(
                        tenant,
                        denied,
                        filter_environment,
                        None
                    ))
                    .await,
                Err(AuthoringQueryError::Denied)
            ));
        }
        // Both an existing target and a nominal absent target return the same denial.
        for selector in [active.clone(), exact.clone(), absent.clone()] {
            let request = resolve(vec![selector], contract, authorization_policy);
            let response = repository
                .resolve_agent_authoring_bindings(
                    tenant,
                    denied,
                    PrincipalKind::AgentRunner,
                    &request,
                )
                .await
                .unwrap();
            response.validate_for(&request).unwrap();
            assert!(matches!(
                response.slots[0].resolution,
                AuthoringResolutionV1::Rejected {
                    code: AuthoringQueryError::Denied
                }
            ));
        }
    }

    // Active and Exact are different source selectors but the same actual candidate.
    // Deduplication happens before reading the (deliberately wrong-kind) Selection Policy.
    let duplicate = resolve(vec![active, exact.clone()], contract, authorization_policy);
    duplicate.validate().unwrap();
    let response = repository
        .resolve_agent_authoring_bindings(tenant, &allowed, PrincipalKind::AgentRunner, &duplicate)
        .await
        .unwrap();
    response.validate_for(&duplicate).unwrap();
    assert!(matches!(
        response.slots[0].resolution,
        AuthoringResolutionV1::Rejected {
            code: AuthoringQueryError::Invalid
        }
    ));
    let wrong_policy = resolve(vec![exact], contract, authorization_policy);
    let response = repository
        .resolve_agent_authoring_bindings(
            tenant,
            &allowed,
            PrincipalKind::AgentRunner,
            &wrong_policy,
        )
        .await
        .unwrap();
    assert!(
        matches!(
            response.slots[0].resolution,
            AuthoringResolutionV1::Rejected {
                code: AuthoringQueryError::ContractMismatch
            }
        ),
        "actual Authorization Policy cannot masquerade as Selection Policy"
    );
    assert_eq!(
        counts(pool, tenant).await,
        before,
        "authoring queries create no Receipt, Job, Event or outbox record"
    );
}
