use chrono::{DateTime, Duration, Utc};
use insight_platform_contracts::{
    canonical_digest, CapabilityArtifactContract, CapabilityBackendFeatures, CapabilityBackendKind,
    CapabilityCancellationKind, CapabilityDataFlowPolicy, CapabilityIdempotencyKind,
    CapabilityInterfaceLimits, CapabilityProgressContract, CapabilityProgressDurability,
    CapabilityProgressMode, DataClassification, Effect, ExactDeploymentRef, ExactPolicyBinding,
    ExactVersionRef, InvocationState, JobState, Permission, PermissionSet,
    PrincipalBindingsPayload, PrincipalKind, PrincipalSnapshot, QuotaDimension, ResourceId,
    ResourceKind, Sha256Digest, TenantConfig, TraceIdentityV1,
};
use insight_platform_invocations::{
    decide_defer_to_sandbox, CapabilityAdmissionSnapshot, CapabilityInvocationPayload,
    CapabilityInvocationRecord, DetachedSandboxSourceKind, ExactInvocationValueRef,
    InvocationOrigin, InvocationPolicyDecision, InvocationPolicyDecisionBundle,
    InvocationPolicyDisposition, InvocationSelectionEvidence, InvocationValueStorage,
};
use insight_platform_jobs::{JobOwnerRef, JobProjection};
use insight_platform_postgres::{
    repository::{NewPrincipal, NewTenant, PgRepository, RepositoryError, TypedPayload},
    verify_schema,
};
use insight_platform_sandbox::opensandbox::{
    AuthorizeSandboxActivationV1, CommitSandboxTerminalV1, OpaqueActivationToken, OpenSandboxId,
    PhysicalDecision, RecordProvisioningIntentV1, RecordSandboxCleanupObservationV1,
    RecordSandboxObservationV1, RunnerBootId, SandboxCandidateMetadataV1, SandboxCandidateV1,
    SandboxClaimV1, SandboxCleanupClaimV1, SandboxCleanupObservationV1,
    SandboxDispatcherJobPayloadV1, SandboxDurableObservationV1, SandboxExecutionPlanV1,
    SandboxExecutionRequestV1, SandboxFailureClassV1, SandboxFencedIdentityV1,
    SandboxJobRepository, SandboxNetworkMode, SandboxProvisioningLimitsV1,
    SandboxProvisioningTokenV1, SandboxResourceLimitsV1, SandboxRunnerOutcomeV1,
    SandboxRunnerResultFrameV1, SandboxTerminalOutcomeV1, SelectSandboxCandidateV1,
};
use serde::Serialize;
use serde_json::{json, Value};
use sqlx::{postgres::PgPoolOptions, PgPool, Row};
use uuid::Uuid;

struct Fixture {
    repository: PgRepository,
    pool: PgPool,
    invocation: CapabilityInvocationRecord,
    request: SandboxExecutionRequestV1,
    usage_reservation_id: ResourceId,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn opensandbox_shared_job_is_fenced_atomic_and_recoverable() {
    let Ok(database_url) = std::env::var("PLATFORM_TEST_DATABASE_URL") else {
        eprintln!("PLATFORM_TEST_DATABASE_URL is unset; real OpenSandbox PostgreSQL L2 skipped");
        return;
    };
    let pool = PgPoolOptions::new()
        .max_connections(16)
        .connect(&database_url)
        .await
        .unwrap();
    verify_schema(&pool).await.unwrap();
    let fixture = seed_fixture(pool).await;

    let worker_one = id(ResourceKind::WorkerProcessGeneration);
    let worker_two = id(ResourceKind::WorkerProcessGeneration);
    let claim_one = claim(&worker_one, digest('1'));
    let claim_two = claim(&worker_two, digest('2'));
    let (first, second) = tokio::join!(
        SandboxJobRepository::claim(&fixture.repository, claim_one),
        SandboxJobRepository::claim(&fixture.repository, claim_two),
    );
    let mut batches = [first.unwrap(), second.unwrap()];
    assert_eq!(batches.iter().map(Vec::len).sum::<usize>(), 1);
    let leased = batches
        .iter_mut()
        .find_map(Vec::pop)
        .expect("one concurrent claim must win");
    assert_eq!(leased.job.attempt_count, 0);
    assert_eq!(leased.request.input, json!({"question":"answer"}));
    assert_eq!(leased.usage_reservation_id, fixture.usage_reservation_id);

    let activation_token = OpaqueActivationToken::parse("1".repeat(64)).unwrap();
    let started = SandboxJobRepository::record_provisioning_intent(
        &fixture.repository,
        RecordProvisioningIntentV1 {
            identity: identity(&leased),
            activation_token: activation_token.clone(),
        },
    )
    .await
    .unwrap();
    assert_eq!(started.job.state, JobState::Running);
    assert_eq!(started.job.attempt_count, 1);

    // A lost PostgreSQL response may replay the exact intent with an older optimistic version,
    // but only while the same lease generation/process/token remains current.
    let replayed_start = SandboxJobRepository::record_provisioning_intent(
        &fixture.repository,
        RecordProvisioningIntentV1 {
            identity: identity(&leased),
            activation_token,
        },
    )
    .await
    .unwrap();
    assert_eq!(replayed_start.job.version, started.job.version);

    let candidate = candidate(&leased.request, "candidate-one");
    let mut wrong_runtime = candidate.clone();
    wrong_runtime.metadata.runtime_contract_digest = digest('f');
    assert!(matches!(
        SandboxJobRepository::record_physical_observation(
            &fixture.repository,
            RecordSandboxObservationV1 {
                identity: decision_identity(&started),
                observation: SandboxDurableObservationV1::Candidate {
                    candidate: wrong_runtime,
                    limits: provisioning_limits(),
                },
            },
        )
        .await,
        Err(RepositoryError::InvalidInput(_))
    ));
    let after_wrong = SandboxJobRepository::recover(
        &fixture.repository,
        &fixture.request.tenant_id,
        &fixture.request.job_id,
    )
    .await
    .unwrap();
    assert_eq!(after_wrong.job.version, started.job.version);
    assert_eq!(
        after_wrong
            .payload
            .physical
            .as_deref()
            .unwrap()
            .candidate_count,
        0
    );

    let observed_command = RecordSandboxObservationV1 {
        identity: decision_identity(&started),
        observation: SandboxDurableObservationV1::Candidate {
            candidate: candidate.clone(),
            limits: provisioning_limits(),
        },
    };
    let observed = SandboxJobRepository::record_physical_observation(
        &fixture.repository,
        observed_command.clone(),
    )
    .await
    .unwrap();
    let observed_replay =
        SandboxJobRepository::record_physical_observation(&fixture.repository, observed_command)
            .await
            .unwrap();
    assert_eq!(observed_replay.job.version, observed.job.version);

    let select_command = SelectSandboxCandidateV1 {
        identity: decision_identity(&observed),
        candidate: candidate.clone(),
    };
    let selected =
        SandboxJobRepository::select_candidate(&fixture.repository, select_command.clone())
            .await
            .unwrap()
            .into_inner();
    assert!(matches!(
        SandboxJobRepository::select_candidate(&fixture.repository, select_command)
            .await
            .unwrap(),
        PhysicalDecision::Replayed(_)
    ));

    let boot_id = RunnerBootId::parse("boot-one").unwrap();
    let authorized = SandboxJobRepository::authorize_activation(
        &fixture.repository,
        AuthorizeSandboxActivationV1 {
            identity: decision_identity(&selected),
            sandbox_id: candidate.sandbox_id.clone(),
            boot_id: boot_id.clone(),
        },
    )
    .await
    .unwrap()
    .into_inner();
    let result = SandboxRunnerResultFrameV1 {
        magic: String::new(),
        schema_version: 1,
        execution_request_digest: leased.request.request_digest.clone(),
        boot_id,
        result: SandboxRunnerOutcomeV1::Failed {
            failure_class: SandboxFailureClassV1::PackageFailed,
            diagnostic_digest: digest('a'),
            diagnostic_bytes: 128,
        },
        frame_digest: digest('0'),
    }
    .seal()
    .unwrap();
    let with_result = SandboxJobRepository::record_physical_observation(
        &fixture.repository,
        RecordSandboxObservationV1 {
            identity: decision_identity(&authorized),
            observation: SandboxDurableObservationV1::Result {
                frame: result.clone(),
            },
        },
    )
    .await
    .unwrap();
    let terminal_command = CommitSandboxTerminalV1 {
        identity: decision_identity(&with_result),
        outcome: SandboxTerminalOutcomeV1::Failed {
            failure_class: SandboxFailureClassV1::PackageFailed,
            result: Some(Box::new(result)),
            evidence_digest: digest('b'),
        },
    };
    let applied =
        SandboxJobRepository::commit_terminal(&fixture.repository, terminal_command.clone())
            .await
            .unwrap();
    assert!(matches!(applied, PhysicalDecision::Applied(_)));
    let replayed =
        SandboxJobRepository::commit_terminal(&fixture.repository, terminal_command.clone())
            .await
            .unwrap();
    assert!(matches!(replayed, PhysicalDecision::Replayed(_)));
    let mut losing = terminal_command;
    let SandboxTerminalOutcomeV1::Failed {
        evidence_digest, ..
    } = &mut losing.outcome
    else {
        unreachable!("fixture terminal outcome is failed")
    };
    *evidence_digest = digest('c');
    assert!(matches!(
        SandboxJobRepository::commit_terminal(&fixture.repository, losing).await,
        Err(RepositoryError::Conflict(_))
    ));

    let row = sqlx::query(
        "SELECT state, worker_id, quota_reservation_id, payload::text AS payload FROM insight_platform.jobs WHERE tenant_id = $1 AND job_id = $2",
    )
    .bind(fixture.request.tenant_id.to_string())
    .bind(fixture.request.job_id.to_string())
    .fetch_one(&fixture.pool)
    .await
    .unwrap();
    assert_eq!(row.get::<String, _>("state"), "failed");
    assert!(row.get::<Option<String>, _>("worker_id").is_none());
    assert!(row
        .get::<Option<String>, _>("quota_reservation_id")
        .is_none());
    let payload_text = row.get::<String, _>("payload");
    assert!(!payload_text.contains("question"));
    assert!(!payload_text.contains("answer"));
    let invocation_state: String = sqlx::query_scalar(
        "SELECT state FROM insight_platform.invocations WHERE tenant_id = $1 AND invocation_id = $2",
    )
    .bind(fixture.request.tenant_id.to_string())
    .bind(fixture.invocation.invocation_id.to_string())
    .fetch_one(&fixture.pool)
    .await
    .unwrap();
    assert_eq!(invocation_state, "failed");
    let settlements: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM insight_platform.quota_ledger WHERE tenant_id = $1 AND correlation_id = $2 AND entry_kind = 'settle'",
    )
    .bind(fixture.request.tenant_id.to_string())
    .bind(fixture.usage_reservation_id.to_string())
    .fetch_one(&fixture.pool)
    .await
    .unwrap();
    assert_eq!(settlements, 4);

    let cleanup_process = id(ResourceKind::WorkerProcessGeneration);
    let cleanup = SandboxJobRepository::claim_cleanup(
        &fixture.repository,
        SandboxCleanupClaimV1 {
            process_generation_id: cleanup_process.clone(),
            limit: 1,
            lease_milliseconds: 30_000,
        },
    )
    .await
    .unwrap()
    .pop()
    .unwrap();
    let cleanup_replay = SandboxJobRepository::claim_cleanup(
        &fixture.repository,
        SandboxCleanupClaimV1 {
            process_generation_id: cleanup_process,
            limit: 1,
            lease_milliseconds: 30_000,
        },
    )
    .await
    .unwrap()
    .pop()
    .unwrap();
    assert_eq!(cleanup_replay.fence, cleanup.fence);
    let absence = RecordSandboxCleanupObservationV1 {
        fence: cleanup.fence,
        observation: SandboxCleanupObservationV1::Absent {
            sandbox_id: candidate.sandbox_id,
            evidence_digest: digest('d'),
        },
    };
    let absent =
        SandboxJobRepository::record_cleanup_observation(&fixture.repository, absence.clone())
            .await
            .unwrap();
    assert!(!absent.payload.cleanup.required);
    let absence_replay =
        SandboxJobRepository::record_cleanup_observation(&fixture.repository, absence)
            .await
            .unwrap();
    assert_eq!(absence_replay.job.version, absent.job.version);
}

async fn seed_fixture(pool: PgPool) -> Fixture {
    let repository = PgRepository::new(pool.clone());
    let tenant_id = id(ResourceKind::Tenant);
    let principal_id = id(ResourceKind::Principal);
    let authentication_authority_digest = value_digest(&json!({
        "kind": "fixture_authority",
        "principal_id": principal_id,
    }));
    let subject_digest = value_digest(&json!({
        "kind": "fixture_subject",
        "principal_id": principal_id,
    }));
    repository
        .create_tenant(NewTenant {
            tenant_id: tenant_id.to_string(),
            state: "active".to_owned(),
            config: TenantConfig::default(),
        })
        .await
        .unwrap();
    repository
        .create_principal(NewPrincipal {
            principal_id: principal_id.clone(),
            authentication_authority_digest,
            subject_digest,
            installation_bindings: PrincipalBindingsPayload {
                installation_bindings: vec![],
            },
        })
        .await
        .unwrap();
    let now: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
        .fetch_one(&pool)
        .await
        .unwrap();
    let deadline = now + Duration::minutes(5);
    let deployment_id = id(ResourceKind::CapabilityDeployment);
    seed_deployment(&pool, &tenant_id, &principal_id, &deployment_id).await;
    let run_id = id(ResourceKind::Run);
    let scope_id = id(ResourceKind::ScopeInstance);
    let node_id = id(ResourceKind::NodeExecution);
    let trace = TraceIdentityV1::generate();
    let empty = TypedPayload::new(1, &json!({})).unwrap();
    sqlx::query(
        r#"
        INSERT INTO insight_platform.runs (
            tenant_id, run_id, root_run_id, agent_deployment_id, principal_id, trace_id,
            state, bindings_schema_version, bindings, bindings_digest,
            current_schema_version, current_payload, current_payload_digest, deadline
        ) VALUES ($1, $2, $2, $3, $4, $5, 'running', $6, $7, $8, $6, $7, $8, $9)
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(run_id.to_string())
    .bind(deployment_id.to_string())
    .bind(principal_id.to_string())
    .bind(trace.trace_id.to_string())
    .bind(empty.schema_version)
    .bind(&empty.value)
    .bind(&empty.digest)
    .bind(deadline)
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        r#"
        INSERT INTO insight_platform.run_nodes (
            tenant_id, node_id, run_id, record_kind, scope_id, logical_key,
            node_kind, state, payload_schema_version, payload, payload_digest, deadline
        ) VALUES ($1, $2, $3, 'scope_instance', $2, 'root-scope', 'scope', 'open', $4, $5, $6, $7)
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(scope_id.to_string())
    .bind(run_id.to_string())
    .bind(empty.schema_version)
    .bind(&empty.value)
    .bind(&empty.digest)
    .bind(deadline)
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        r#"
        INSERT INTO insight_platform.run_nodes (
            tenant_id, node_id, run_id, record_kind, scope_id, plan_node_key,
            activation_ordinal, logical_key, node_kind, state,
            payload_schema_version, payload, payload_digest, deadline
        ) VALUES ($1, $2, $3, 'node_execution', $4, 'sandbox', 1,
                  'sandbox-node', 'capability_call', 'waiting', $5, $6, $7, $8)
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(node_id.to_string())
    .bind(run_id.to_string())
    .bind(scope_id.to_string())
    .bind(empty.schema_version)
    .bind(&empty.value)
    .bind(&empty.digest)
    .bind(deadline)
    .execute(&pool)
    .await
    .unwrap();

    let input_value = json!({"question":"answer"});
    let input_digest = value_digest(&input_value);
    let input_schema_digest = digest('3');
    let output_schema_digest = digest('4');
    let input_value_id = id(ResourceKind::RunValue);
    sqlx::query(
        r#"
        INSERT INTO insight_platform.run_values (
            tenant_id, value_id, run_id, value_kind, classification,
            schema_digest, content_digest, inline_value
        ) VALUES ($1, $2, $3, 'run_input', 'internal', $4, $5, $6)
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(input_value_id.to_string())
    .bind(run_id.to_string())
    .bind(input_schema_digest.to_string())
    .bind(input_digest.to_string())
    .bind(&input_value)
    .execute(&pool)
    .await
    .unwrap();

    let job_id = id(ResourceKind::Job);
    let invocation = deferred_invocation(
        tenant_id.clone(),
        principal_id,
        run_id.clone(),
        node_id.clone(),
        deployment_id,
        input_value_id.clone(),
        input_schema_digest.clone(),
        input_digest.clone(),
        output_schema_digest.clone(),
        trace,
        deadline,
        job_id.clone(),
        now,
    );
    let invocation_payload =
        TypedPayload::from_versioned(1, &invocation.payload, 1_048_576).unwrap();
    sqlx::query(
        r#"
        INSERT INTO insight_platform.invocations (
            tenant_id, invocation_id, trace_id, invocation_kind, owner_kind, owner_id,
            logical_key, run_id, node_id, deployment_id, state, version,
            input_value_id, effect_key_digest, payload_schema_version, payload,
            payload_digest, deadline, started_at, created_at, updated_at
        ) VALUES ($1, $2, $3, 'capability', 'node_execution', $4, $5, $6, $4, $7,
                  $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $17)
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(invocation.invocation_id.to_string())
    .bind(trace.trace_id.to_string())
    .bind(node_id.to_string())
    .bind(&invocation.logical_key)
    .bind(run_id.to_string())
    .bind(invocation.deployment_id.to_string())
    .bind(invocation.state.as_str())
    .bind(i64::try_from(invocation.version).unwrap())
    .bind(input_value_id.to_string())
    .bind(invocation.effect_key_digest.to_string())
    .bind(invocation_payload.schema_version)
    .bind(&invocation_payload.value)
    .bind(&invocation_payload.digest)
    .bind(deadline)
    .bind(invocation.started_at)
    .bind(now)
    .execute(&pool)
    .await
    .unwrap();

    let request = SandboxExecutionRequestV1 {
        schema_version: 1,
        tenant_id: tenant_id.clone(),
        invocation_id: invocation.invocation_id.clone(),
        job_id: job_id.clone(),
        lease_generation: 1,
        physical_attempt: 1,
        worker_process_generation_id: id(ResourceKind::WorkerProcessGeneration),
        package_version_id: id(ResourceKind::SandboxPackageRevision),
        image_uri: format!("registry.invalid/package@sha256:{}", "a".repeat(64)),
        runtime_version_id: id(ResourceKind::SandboxRuntimeRevision),
        runtime_contract_digest: digest('5'),
        sandbox_profile_deployment_id: id(ResourceKind::SandboxProfileDeployment),
        profile_deployment_digest: digest('6'),
        runner_argv: vec!["/usr/local/bin/platform-sandbox-runner".to_owned()],
        package_argv: vec!["/opt/insight/package".to_owned()],
        input_value_id,
        output_value_id: id(ResourceKind::RunValue),
        classification: DataClassification::Internal,
        input: input_value,
        input_schema_digest,
        input_digest: digest('0'),
        output_schema_digest,
        network_mode: SandboxNetworkMode::Direct,
        limits: resource_limits(),
        deadline_at: deadline,
        trace,
        request_digest: digest('0'),
    }
    .seal()
    .unwrap();
    let payload = SandboxDispatcherJobPayloadV1::accepted(
        SandboxExecutionPlanV1::from_request(&request).unwrap(),
    )
    .unwrap();
    let job = JobProjection {
        trace,
        tenant_id: tenant_id.clone(),
        job_id: job_id.clone(),
        work_class: insight_platform_contracts::WorkClass::Sandbox,
        owner: JobOwnerRef {
            owner_kind: ResourceKind::Job,
            owner_id: job_id.clone(),
        },
        state: JobState::Ready,
        version: 1,
        attempt_count: 0,
        attempt_limit: 1,
        lease_generation: 0,
        lease: None,
        scheduled_at: now,
        retry_at: None,
        wake: None,
        deadline,
    };
    payload.validate_for(&job).unwrap();
    let usage_reservation_id = id(ResourceKind::UsageReservation);
    seed_quota(&pool, &payload, &usage_reservation_id).await;
    let job_payload = TypedPayload::from_versioned(1, &payload, 1_048_576).unwrap();
    sqlx::query(
        r#"
        INSERT INTO insight_platform.jobs (
            tenant_id, job_id, job_kind, work_class, owner_kind, owner_id, trace_id,
            invocation_id, run_id, node_id, state, version, attempt_no, attempt_limit,
            lease_epoch, scheduled_at, deadline, priority, request_digest,
            quota_reservation_id, payload_schema_version, payload, payload_digest
        ) VALUES ($1, $2, 'sandbox_capability_execution', 'sandbox', 'job', $2, $3,
                  $4, $5, $6, 'ready', 1, 0, 1, 0, $7, $8, 0, $9, $10, $11, $12, $13)
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(job_id.to_string())
    .bind(request.trace.trace_id.to_string())
    .bind(invocation.invocation_id.to_string())
    .bind(run_id.to_string())
    .bind(node_id.to_string())
    .bind(now)
    .bind(deadline)
    .bind(request.request_digest.to_string())
    .bind(usage_reservation_id.to_string())
    .bind(job_payload.schema_version)
    .bind(&job_payload.value)
    .bind(&job_payload.digest)
    .execute(&pool)
    .await
    .unwrap();

    Fixture {
        repository,
        pool,
        invocation,
        request,
        usage_reservation_id,
    }
}

#[allow(clippy::too_many_arguments)]
fn deferred_invocation(
    tenant_id: ResourceId,
    principal_id: ResourceId,
    run_id: ResourceId,
    node_id: ResourceId,
    deployment_id: ResourceId,
    input_value_id: ResourceId,
    input_schema_digest: Sha256Digest,
    input_digest: Sha256Digest,
    output_schema_digest: Sha256Digest,
    trace: TraceIdentityV1,
    deadline: DateTime<Utc>,
    job_id: ResourceId,
    now: DateTime<Utc>,
) -> CapabilityInvocationRecord {
    let deployment = ExactDeploymentRef::new(deployment_id.clone(), digest('7')).unwrap();
    let policy = ExactVersionRef::new(id(ResourceKind::PolicyRevision), digest('8')).unwrap();
    let policies = InvocationPolicyDecisionBundle::build(
        vec![InvocationPolicyDecision {
            policy: policy.clone(),
            disposition: InvocationPolicyDisposition::Allowed,
            evidence_digest: digest('9'),
        }],
        None,
    )
    .unwrap();
    let input = ExactInvocationValueRef {
        schema_version: 1,
        value_id: input_value_id.clone(),
        run_id: run_id.clone(),
        producing_node_id: None,
        value_kind: "run_input".to_owned(),
        classification: DataClassification::Internal,
        schema_digest: input_schema_digest.clone(),
        content_digest: input_digest,
        storage: InvocationValueStorage::Inline,
    };
    let principal = PrincipalSnapshot::build(
        tenant_id.clone(),
        principal_id,
        PrincipalKind::AgentRunner,
        PermissionSet::new(vec![Permission::CapabilityInvoke]).unwrap(),
        1,
        1,
        1,
    )
    .unwrap();
    let mut admission = CapabilityAdmissionSnapshot {
        schema_version: 1,
        origin_key: InvocationOrigin::PlanNode {
            node_execution_id: node_id.clone(),
        },
        slot_id: "sandbox".to_owned(),
        slot_binding_digest: digest('a'),
        run_bindings_digest: digest('b'),
        selection_policy: ExactPolicyBinding {
            deployment: ExactDeploymentRef::new(id(ResourceKind::PolicyDeployment), digest('c'))
                .unwrap(),
            revision: policy,
        },
        selection_evidence: InvocationSelectionEvidence::build(
            std::slice::from_ref(&deployment),
            0,
            digest('d'),
        )
        .unwrap(),
        deployment: deployment.clone(),
        interface: ExactVersionRef::new(id(ResourceKind::CapabilityInterfaceRevision), digest('e'))
            .unwrap(),
        capability_name: "fixture.sandbox".parse().unwrap(),
        implementation: ExactVersionRef::new(
            id(ResourceKind::CapabilityImplementationRevision),
            digest('f'),
        )
        .unwrap(),
        backend_kind: CapabilityBackendKind::Sandbox,
        backend_contract_digest: digest('1'),
        mcp_runtime: None,
        input: input.clone(),
        input_artifact_link_id: None,
        effect: Effect::Pure,
        idempotency: CapabilityIdempotencyKind::Intrinsic,
        cancellation: CapabilityCancellationKind::BestEffort,
        progress: CapabilityProgressContract {
            mode: CapabilityProgressMode::Events,
            schema_digest: Some(digest('2')),
            max_events: 8,
            max_bytes_per_event: 4_096,
            minimum_interval_milliseconds: 1,
            durability: CapabilityProgressDurability::CoarseDurable,
        },
        implementation_features: CapabilityBackendFeatures {
            deferred: true,
            input_required: false,
            callback: false,
            poll: false,
            progress: true,
            cancellation: true,
            max_remote_state_bytes: 4_096,
            max_poll_count: 0,
        },
        input_schema_digest,
        output_schema_digest,
        error_schema_digest: digest('3'),
        artifact_contract: CapabilityArtifactContract { ports: vec![] },
        data_flow_policy: CapabilityDataFlowPolicy {
            maximum_input_classification: DataClassification::Restricted,
            maximum_output_classification: DataClassification::Restricted,
            allowed_regions: vec!["global".parse().unwrap()],
            declassification_policy: None,
        },
        interface_limits: CapabilityInterfaceLimits {
            maximum_input_bytes: 1_048_576,
            maximum_output_bytes: 1_048_576,
            maximum_artifacts: 0,
            maximum_execution_milliseconds: 60_000,
        },
        policies,
        principal,
        effect_key_digest: digest('4'),
        idempotency_key_digest: digest('5'),
        attempt_limit: 1,
        retry_backoff_milliseconds: 100,
        deadline,
        canonical_digest: digest('0'),
    };
    admission.canonical_digest = digest_without_field(&admission, "canonical_digest");
    let ready = CapabilityInvocationRecord {
        tenant_id,
        invocation_id: id(ResourceKind::CapabilityInvocation),
        trace,
        run_id,
        node_execution_id: node_id.clone(),
        owner_kind: ResourceKind::NodeExecution,
        owner_id: node_id,
        logical_key: admission.origin_key.logical_key(),
        deployment_id: deployment.deployment_id,
        input_value_id,
        output_value_id: None,
        effect_key_digest: admission.effect_key_digest.clone(),
        state: InvocationState::Ready,
        version: 1,
        payload: CapabilityInvocationPayload {
            schema_version: 1,
            admission,
            current_job_id: None,
            approval_task_id: None,
            input_task_id: None,
            detached_pending: None,
            result: None,
            failure: None,
            reconciliation: None,
        },
        deadline,
        retry_at: None,
        started_at: None,
        terminal_at: None,
        created_at: now,
        updated_at: now,
    };
    ready.validate().unwrap();
    decide_defer_to_sandbox(
        &ready,
        DetachedSandboxSourceKind::SandboxCapability,
        1,
        &job_id,
        1,
        None,
        now,
    )
    .unwrap()
}

async fn seed_deployment(
    pool: &PgPool,
    tenant_id: &ResourceId,
    principal_id: &ResourceId,
    deployment_id: &ResourceId,
) {
    let resource_id = id(ResourceKind::CapabilityInterface);
    let version_id = id(ResourceKind::CapabilityInterfaceRevision);
    let empty = TypedPayload::new(1, &json!({})).unwrap();
    sqlx::query(
        "INSERT INTO insight_platform.resources (tenant_id, resource_id, resource_kind, lifecycle_state, gate_state, payload_schema_version, payload, payload_digest) VALUES ($1, $2, 'capability_interface', 'active', 'enabled', $3, $4, $5)",
    )
    .bind(tenant_id.to_string())
    .bind(resource_id.to_string())
    .bind(empty.schema_version)
    .bind(&empty.value)
    .bind(&empty.digest)
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO insight_platform.resource_versions (tenant_id, resource_version_id, resource_id, resource_version_kind, revision_no, content_digest, payload_schema_version, payload, payload_digest, created_by) VALUES ($1, $2, $3, 'capability_interface', 1, $4, $5, $6, $7, $8)",
    )
    .bind(tenant_id.to_string())
    .bind(version_id.to_string())
    .bind(resource_id.to_string())
    .bind(digest('6').to_string())
    .bind(empty.schema_version)
    .bind(&empty.value)
    .bind(&empty.digest)
    .bind(principal_id.to_string())
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO insight_platform.deployments (tenant_id, deployment_id, resource_id, resource_version_id, environment, bindings_digest, payload_schema_version, bindings, created_by) VALUES ($1, $2, $3, $4, 'test', $5, $6, $7, $8)",
    )
    .bind(tenant_id.to_string())
    .bind(deployment_id.to_string())
    .bind(resource_id.to_string())
    .bind(version_id.to_string())
    .bind(digest('7').to_string())
    .bind(empty.schema_version)
    .bind(&empty.value)
    .bind(principal_id.to_string())
    .execute(pool)
    .await
    .unwrap();
}

async fn seed_quota(
    pool: &PgPool,
    payload: &SandboxDispatcherJobPayloadV1,
    reservation_id: &ResourceId,
) {
    let empty = TypedPayload::new(1, &json!({})).unwrap();
    let limits = &payload.plan.limits;
    let lines = [
        (QuotaDimension::SandboxConcurrentExecutions.as_str(), 1_i64),
        (
            QuotaDimension::SandboxCpuSeconds.as_str(),
            i64::try_from(
                (u64::from(limits.cpu_millicores) * limits.wall_milliseconds).div_ceil(1_000_000),
            )
            .unwrap(),
        ),
        (
            QuotaDimension::SandboxMemoryMebibytes.as_str(),
            i64::from(limits.memory_mebibytes),
        ),
        (
            QuotaDimension::SandboxOutputBytes.as_str(),
            i64::try_from(limits.maximum_output_bytes).unwrap(),
        ),
    ];
    for (metric, amount) in lines {
        let account_id = id(ResourceKind::QuotaAccount);
        sqlx::query(
            r#"
            INSERT INTO insight_platform.quota_accounts (
                tenant_id, quota_account_id, scope_kind, scope_id, work_class, metric,
                limit_value, reserved_value, version, payload_schema_version, payload, payload_digest
            ) VALUES ($1, $2, 'tenant', $1, 'sandbox', $3, 1000000000, $4, 2, $5, $6, $7)
            "#,
        )
        .bind(payload.plan.tenant_id.to_string())
        .bind(account_id.to_string())
        .bind(metric)
        .bind(amount)
        .bind(empty.schema_version)
        .bind(&empty.value)
        .bind(&empty.digest)
        .execute(pool)
        .await
        .unwrap();
        sqlx::query(
            r#"
            INSERT INTO insight_platform.quota_ledger (
                tenant_id, quota_entry_id, quota_account_id, correlation_id,
                entry_kind, reserved_amount, used_amount, account_version, request_digest
            ) VALUES ($1, $2, $3, $4, 'reserve', $5, 0, 2, $6)
            "#,
        )
        .bind(payload.plan.tenant_id.to_string())
        .bind(id(ResourceKind::QuotaLedgerEntry).to_string())
        .bind(account_id.to_string())
        .bind(reservation_id.to_string())
        .bind(amount)
        .bind(payload.plan.request_digest.to_string())
        .execute(pool)
        .await
        .unwrap();
    }
}

fn candidate(request: &SandboxExecutionRequestV1, sandbox_id: &str) -> SandboxCandidateV1 {
    let token = SandboxProvisioningTokenV1::from_request(request);
    SandboxCandidateV1 {
        schema_version: 1,
        sandbox_id: OpenSandboxId::parse(sandbox_id).unwrap(),
        metadata: SandboxCandidateMetadataV1 {
            schema_version: 1,
            provisioning_token_digest: token.digest().unwrap(),
            execution_request_digest: request.request_digest.clone(),
            runtime_contract_digest: request.runtime_contract_digest.clone(),
            profile_deployment_digest: request.profile_deployment_digest.clone(),
            network_mode: request.network_mode,
        },
        observed_at: request.deadline_at - Duration::seconds(1),
    }
}

fn resource_limits() -> SandboxResourceLimitsV1 {
    SandboxResourceLimitsV1 {
        maximum_input_bytes: 65_536,
        maximum_output_bytes: 65_536,
        cpu_millicores: 1_000,
        memory_mebibytes: 128,
        pids: 64,
        ephemeral_storage_bytes: 67_108_864,
        wall_milliseconds: 60_000,
        cleanup_milliseconds: 10_000,
    }
}

fn provisioning_limits() -> SandboxProvisioningLimitsV1 {
    SandboxProvisioningLimitsV1 {
        maximum_candidates: 2,
        candidate_page_items: 4,
        candidate_quiescence_milliseconds: 500,
        provisioning_timeout_milliseconds: 10_000,
        orphan_page_items: 20,
        runner_header_bytes: 8_192,
        diagnostic_bytes: 8_192,
    }
}

fn claim(worker: &ResourceId, token: Sha256Digest) -> SandboxClaimV1 {
    SandboxClaimV1 {
        worker_process_generation_id: worker.clone(),
        lease_token_digests: vec![token],
        limit: 1,
        lease_milliseconds: 60_000,
    }
}

fn identity(
    leased: &insight_platform_sandbox::opensandbox::LeasedSandboxJobV1,
) -> SandboxFencedIdentityV1 {
    SandboxFencedIdentityV1 {
        tenant_id: leased.job.tenant_id.clone(),
        job_id: leased.job.job_id.clone(),
        fence: leased.fence.clone(),
    }
}

fn decision_identity(
    decision: &insight_platform_sandbox::opensandbox::SandboxRepositoryDecisionV1,
) -> SandboxFencedIdentityV1 {
    SandboxFencedIdentityV1 {
        tenant_id: decision.job.tenant_id.clone(),
        job_id: decision.job.job_id.clone(),
        fence: decision.fence.clone().unwrap(),
    }
}

fn id(kind: ResourceKind) -> ResourceId {
    ResourceId::from_uuid_v7(kind, Uuid::now_v7()).unwrap()
}

fn digest(character: char) -> Sha256Digest {
    format!("sha256:{}", character.to_string().repeat(64))
        .parse()
        .unwrap()
}

fn value_digest(value: &Value) -> Sha256Digest {
    canonical_digest(value).unwrap().parse().unwrap()
}

fn digest_without_field<T: Serialize>(value: &T, field: &str) -> Sha256Digest {
    let mut value = serde_json::to_value(value).unwrap();
    value.as_object_mut().unwrap().remove(field);
    canonical_digest(&value).unwrap().parse().unwrap()
}
