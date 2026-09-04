use chrono::{DateTime, Duration, Utc};
use insight_platform_contracts::{
    canonical_digest, ArtifactRef, AuthoringPackage, CapabilityArtifactContract,
    CapabilityBackendFeatures, CapabilityBackendKind, CapabilityCancellationKind,
    CapabilityDataFlowPolicy, CapabilityIdempotencyKind, CapabilityInterfaceLimits,
    CapabilityInterfaceResourceSpec, CapabilityProgressContract, CapabilityProgressDurability,
    CapabilityProgressMode, ClosedJsonSchema, CommandAudit, CommandOutcome, DataClassification,
    Effect, ExactDeploymentRef, ExactPolicyBinding, ExactVersionRef, InvocationState, JobState,
    Permission, PermissionSet, PrincipalBindingsPayload, PrincipalKind, PrincipalSnapshot,
    PublishedVersionPayload, QuotaDimension, RegistryResourceKind, ResourceDocument, ResourceId,
    ResourceKind, Sha256Digest, TenantConfig, TenantPrincipalPayload, TraceIdentityV1,
    ValidationSummary,
};
use insight_platform_invocations::{
    decide_defer_to_sandbox, CapabilityAdmissionSnapshot, CapabilityControlKind,
    CapabilityInvocationPayload, CapabilityInvocationRecord, ControlCapabilityInvocation,
    DetachedSandboxSourceKind, ExactInvocationValueRef, InvocationOrigin, InvocationPolicyDecision,
    InvocationPolicyDecisionBundle, InvocationPolicyDisposition, InvocationSelectionEvidence,
    InvocationTransaction, InvocationValueStorage,
};
use insight_platform_jobs::{JobOwnerRef, JobProjection};
use insight_platform_opensandbox_client::{
    OpenSandboxApiKey, OpenSandboxHttpClient, OpenSandboxHttpClientConfig,
};
use insight_platform_postgres::{
    repository::{
        NewPrincipal, NewTenant, NewTenantPrincipal, PgRepository, RepositoryError, TypedPayload,
    },
    verify_schema,
};
use insight_platform_sandbox::dispatcher::{
    OpenSandboxDispatcher, SandboxCleanupProgressV1, SandboxDispatchProgressV1,
};
use insight_platform_sandbox::opensandbox::{
    AuthorizeCandidateCreateV1, AuthorizeSandboxActivationV1, CommitSandboxTerminalV1,
    OpaqueActivationToken, OpenSandboxId, PhysicalDecision, ReconcileSandboxControlsV1,
    RecordProvisioningIntentV1, RecordSandboxCleanupObservationV1, RecordSandboxObservationV1,
    RunnerBootId, SandboxCandidateMetadataV1, SandboxCandidatePurposeV1, SandboxCandidateV1,
    SandboxClaimV1, SandboxCleanupClaimV1, SandboxCleanupObservationV1, SandboxControlKindV1,
    SandboxDispatcherJobPayloadV1, SandboxDurableObservationV1, SandboxExecutionPlanV1,
    SandboxExecutionRequestV1, SandboxFailureClassV1, SandboxFencedIdentityV1,
    SandboxJobRepository, SandboxNetworkMode, SandboxOrphanDispositionV1, SandboxPhysicalPhaseV1,
    SandboxProvisioningLimitsV1, SandboxProvisioningTokenV1, SandboxResourceLimitsV1,
    SandboxRunnerOutcomeV1, SandboxRunnerPhaseV1, SandboxRunnerResultFrameV1,
    SandboxRunnerStateFrameV1, SandboxTerminalOutcomeV1, SelectSandboxCandidateV1,
};
use serde::Serialize;
use serde_json::{json, Value};
use sqlx::{postgres::PgPoolOptions, PgPool, Row};
use std::sync::Arc;
use uuid::Uuid;

struct Fixture {
    repository: PgRepository,
    pool: PgPool,
    invocation: CapabilityInvocationRecord,
    request: SandboxExecutionRequestV1,
    usage_reservation_id: ResourceId,
}

struct FixtureOptions {
    input: Value,
    image_uri: String,
    runtime_contract_digest: Sha256Digest,
    package_argv: Vec<String>,
    network_mode: SandboxNetworkMode,
    deadline_after: Duration,
}

impl Default for FixtureOptions {
    fn default() -> Self {
        Self {
            input: json!({"question":"answer"}),
            image_uri: format!("registry.invalid/package@sha256:{}", "a".repeat(64)),
            runtime_contract_digest: digest('5'),
            package_argv: vec!["/opt/insight/package".to_owned()],
            network_mode: SandboxNetworkMode::Direct,
            deadline_after: Duration::minutes(5),
        }
    }
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

    let create_authorization = AuthorizeCandidateCreateV1 {
        identity: decision_identity(&started),
        create_ordinal: 1,
        limits: provisioning_limits(),
    };
    let (authorization_one, authorization_two) = tokio::join!(
        SandboxJobRepository::authorize_candidate_create(
            &fixture.repository,
            create_authorization.clone(),
        ),
        SandboxJobRepository::authorize_candidate_create(
            &fixture.repository,
            create_authorization.clone(),
        ),
    );
    let authorizations = [authorization_one.unwrap(), authorization_two.unwrap()];
    assert_eq!(
        authorizations
            .iter()
            .filter(|decision| matches!(decision, PhysicalDecision::Applied(_)))
            .count(),
        1
    );
    assert_eq!(
        authorizations
            .iter()
            .filter(|decision| matches!(decision, PhysicalDecision::Replayed(_)))
            .count(),
        1
    );
    let authorized = authorizations[0].clone().into_inner().decision;
    assert_eq!(
        authorized
            .payload
            .physical
            .as_deref()
            .unwrap()
            .create_authorization_count,
        1
    );
    assert!(matches!(
        SandboxJobRepository::authorize_candidate_create(
            &fixture.repository,
            AuthorizeCandidateCreateV1 {
                identity: decision_identity(&authorized),
                create_ordinal: 3,
                limits: provisioning_limits(),
            },
        )
        .await,
        Err(RepositoryError::Conflict(_))
    ));

    let candidate = candidate(&leased.request, "candidate-one");
    let retain_provisioning =
        SandboxJobRepository::decide_orphan(&fixture.repository, candidate.clone())
            .await
            .unwrap();
    assert_eq!(
        retain_provisioning.disposition,
        SandboxOrphanDispositionV1::RetainProvisioning
    );
    assert!(!retain_provisioning.disposition.may_delete());
    let mut wrong_runtime = candidate.clone();
    wrong_runtime.metadata.runtime_contract_digest = digest('f');
    assert!(matches!(
        SandboxJobRepository::record_physical_observation(
            &fixture.repository,
            RecordSandboxObservationV1 {
                identity: decision_identity(&authorized),
                observation: SandboxDurableObservationV1::Candidate {
                    candidate: wrong_runtime.clone(),
                    limits: provisioning_limits(),
                },
            },
        )
        .await,
        Err(RepositoryError::InvalidInput(_))
    ));
    assert!(matches!(
        SandboxJobRepository::decide_orphan(&fixture.repository, wrong_runtime).await,
        Err(RepositoryError::InvalidInput(_))
    ));
    let mut unauthorized_ordinal = candidate.clone();
    unauthorized_ordinal.metadata.create_ordinal = 2;
    assert!(matches!(
        SandboxJobRepository::record_physical_observation(
            &fixture.repository,
            RecordSandboxObservationV1 {
                identity: decision_identity(&authorized),
                observation: SandboxDurableObservationV1::Candidate {
                    candidate: unauthorized_ordinal.clone(),
                    limits: provisioning_limits(),
                },
            },
        )
        .await,
        Err(RepositoryError::InvalidInput(_))
    ));
    assert!(matches!(
        SandboxJobRepository::decide_orphan(&fixture.repository, unauthorized_ordinal).await,
        Err(RepositoryError::InvalidInput(_))
    ));
    let after_wrong = SandboxJobRepository::recover(
        &fixture.repository,
        &fixture.request.tenant_id,
        &fixture.request.job_id,
    )
    .await
    .unwrap();
    assert_eq!(after_wrong.job.version, authorized.job.version);
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
        identity: decision_identity(&authorized),
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

    let retained_selected =
        SandboxJobRepository::decide_orphan(&fixture.repository, candidate.clone())
            .await
            .unwrap();
    assert_eq!(
        retained_selected.disposition,
        SandboxOrphanDispositionV1::RetainSelected
    );
    let mut late_candidate = candidate.clone();
    late_candidate.sandbox_id = OpenSandboxId::parse("candidate-late").unwrap();
    let delete_late = SandboxJobRepository::decide_orphan(&fixture.repository, late_candidate)
        .await
        .unwrap();
    assert_eq!(
        delete_late.disposition,
        SandboxOrphanDispositionV1::DeleteLateCandidate
    );
    assert!(delete_late.disposition.may_delete());
    let mut stale_attempt = candidate.clone();
    stale_attempt.metadata.physical_attempt += 1;
    let delete_stale = SandboxJobRepository::decide_orphan(&fixture.repository, stale_attempt)
        .await
        .unwrap();
    assert_eq!(
        delete_stale.disposition,
        SandboxOrphanDispositionV1::DeleteStaleAttempt
    );
    let mut missing_owner = candidate.clone();
    missing_owner.metadata.tenant_id = id(ResourceKind::Tenant);
    missing_owner.metadata.job_id = id(ResourceKind::Job);
    let delete_missing = SandboxJobRepository::decide_orphan(&fixture.repository, missing_owner)
        .await
        .unwrap();
    assert_eq!(
        delete_missing.disposition,
        SandboxOrphanDispositionV1::DeleteMissingOwner
    );
    let after_orphan_decisions = SandboxJobRepository::recover(
        &fixture.repository,
        &fixture.request.tenant_id,
        &fixture.request.job_id,
    )
    .await
    .unwrap();
    assert_eq!(after_orphan_decisions.job.version, selected.job.version);

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

    exercise_lease_rollover_and_stale_result(fixture.pool.clone()).await;
    exercise_cancel_timeout_and_quota(fixture.pool.clone()).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn opensandbox_boot_rollover_is_durable_unknown_outcome() {
    let Ok(database_url) = std::env::var("PLATFORM_TEST_DATABASE_URL") else {
        eprintln!("PLATFORM_TEST_DATABASE_URL is unset; OpenSandbox boot rollover L2 skipped");
        return;
    };
    let pool = PgPoolOptions::new()
        .max_connections(8)
        .connect(&database_url)
        .await
        .unwrap();
    verify_schema(&pool).await.unwrap();
    exercise_boot_rollover_unknown_outcome(pool).await;
}

async fn exercise_lease_rollover_and_stale_result(pool: PgPool) {
    let fixture = seed_fixture(pool).await;
    let first_worker = id(ResourceKind::WorkerProcessGeneration);
    let mut first_claim = claim(&first_worker, digest('6'));
    first_claim.lease_milliseconds = 500;
    let leased = SandboxJobRepository::claim(&fixture.repository, first_claim)
        .await
        .unwrap()
        .pop()
        .unwrap();
    let started = SandboxJobRepository::record_provisioning_intent(
        &fixture.repository,
        RecordProvisioningIntentV1 {
            identity: identity(&leased),
            activation_token: OpaqueActivationToken::parse("2".repeat(64)).unwrap(),
        },
    )
    .await
    .unwrap();
    let create_authorized = SandboxJobRepository::authorize_candidate_create(
        &fixture.repository,
        AuthorizeCandidateCreateV1 {
            identity: decision_identity(&started),
            create_ordinal: 1,
            limits: provisioning_limits(),
        },
    )
    .await
    .unwrap()
    .into_inner()
    .decision;
    let candidate = candidate(&leased.request, "rollover-candidate");
    let observed = SandboxJobRepository::record_physical_observation(
        &fixture.repository,
        RecordSandboxObservationV1 {
            identity: decision_identity(&create_authorized),
            observation: SandboxDurableObservationV1::Candidate {
                candidate: candidate.clone(),
                limits: provisioning_limits(),
            },
        },
    )
    .await
    .unwrap();
    let selected = SandboxJobRepository::select_candidate(
        &fixture.repository,
        SelectSandboxCandidateV1 {
            identity: decision_identity(&observed),
            candidate: candidate.clone(),
        },
    )
    .await
    .unwrap()
    .into_inner();
    let boot_id = RunnerBootId::parse("rollover-boot").unwrap();
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
    tokio::time::sleep(std::time::Duration::from_millis(600)).await;

    let second_worker = id(ResourceKind::WorkerProcessGeneration);
    let continued =
        SandboxJobRepository::claim(&fixture.repository, claim(&second_worker, digest('7')))
            .await
            .unwrap()
            .pop()
            .unwrap();
    assert_eq!(continued.job.attempt_count, 1);
    assert_eq!(continued.job.lease_generation, 2);
    assert_eq!(continued.request.physical_attempt, 1);
    assert_eq!(continued.payload.physical, authorized.payload.physical);

    let result = SandboxRunnerResultFrameV1 {
        magic: String::new(),
        schema_version: 1,
        execution_request_digest: continued.request.request_digest.clone(),
        boot_id,
        result: SandboxRunnerOutcomeV1::Failed {
            failure_class: SandboxFailureClassV1::PackageFailed,
            diagnostic_digest: digest('8'),
            diagnostic_bytes: 64,
        },
        frame_digest: digest('0'),
    }
    .seal()
    .unwrap();
    let stale_version = continued.job.version;
    assert!(matches!(
        SandboxJobRepository::record_physical_observation(
            &fixture.repository,
            RecordSandboxObservationV1 {
                identity: decision_identity(&authorized),
                observation: SandboxDurableObservationV1::Result {
                    frame: result.clone(),
                },
            },
        )
        .await,
        Err(RepositoryError::StaleFence)
    ));
    let after_stale = SandboxJobRepository::recover(
        &fixture.repository,
        &fixture.request.tenant_id,
        &fixture.request.job_id,
    )
    .await
    .unwrap();
    assert_eq!(after_stale.job.version, stale_version);

    let observed_result = SandboxJobRepository::record_physical_observation(
        &fixture.repository,
        RecordSandboxObservationV1 {
            identity: identity(&continued),
            observation: SandboxDurableObservationV1::Result {
                frame: result.clone(),
            },
        },
    )
    .await
    .unwrap();
    let terminal = SandboxJobRepository::commit_terminal(
        &fixture.repository,
        CommitSandboxTerminalV1 {
            identity: decision_identity(&observed_result),
            outcome: SandboxTerminalOutcomeV1::Failed {
                failure_class: SandboxFailureClassV1::PackageFailed,
                result: Some(Box::new(result)),
                evidence_digest: digest('9'),
            },
        },
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(terminal.job.state, JobState::Failed);
    assert_eq!(terminal.job.attempt_count, 1);
    assert_eq!(terminal.job.lease_generation, 2);

    let cleanup = SandboxJobRepository::claim_cleanup(
        &fixture.repository,
        SandboxCleanupClaimV1 {
            process_generation_id: id(ResourceKind::WorkerProcessGeneration),
            limit: 1,
            lease_milliseconds: 30_000,
        },
    )
    .await
    .unwrap()
    .pop()
    .unwrap();
    assert_eq!(cleanup.job.job_id, fixture.request.job_id);
    let absent = SandboxJobRepository::record_cleanup_observation(
        &fixture.repository,
        RecordSandboxCleanupObservationV1 {
            fence: cleanup.fence,
            observation: SandboxCleanupObservationV1::Absent {
                sandbox_id: candidate.sandbox_id,
                evidence_digest: digest('a'),
            },
        },
    )
    .await
    .unwrap();
    assert!(!absent.payload.cleanup.required);
}

async fn exercise_cancel_timeout_and_quota(pool: PgPool) {
    let cancelled_fixture = seed_fixture(pool.clone()).await;
    let leased = SandboxJobRepository::claim(
        &cancelled_fixture.repository,
        claim(&id(ResourceKind::WorkerProcessGeneration), digest('a')),
    )
    .await
    .unwrap()
    .pop()
    .unwrap();
    let old_identity = identity(&leased);
    SandboxJobRepository::record_provisioning_intent(
        &cancelled_fixture.repository,
        RecordProvisioningIntentV1 {
            identity: old_identity.clone(),
            activation_token: OpaqueActivationToken::parse("3".repeat(64)).unwrap(),
        },
    )
    .await
    .unwrap();
    let controlled = execute_control(
        &cancelled_fixture.repository,
        ControlCapabilityInvocation {
            audit: control_audit(&cancelled_fixture),
            invocation_id: cancelled_fixture.invocation.invocation_id.clone(),
            expected_invocation_version: cancelled_fixture.invocation.version,
            quota_entry_ids: vec![],
            kind: CapabilityControlKind::Cancel,
        },
    )
    .await;
    assert_eq!(controlled.invocation.state, InvocationState::Cancelling);
    let controlled_job = controlled.job.unwrap();
    assert_eq!(controlled_job.state, "cancelling");
    let controlled_payload: SandboxDispatcherJobPayloadV1 =
        serde_json::from_value(controlled_job.payload.value).unwrap();
    assert_eq!(
        controlled_payload
            .control
            .as_deref()
            .map(|control| control.kind),
        Some(SandboxControlKindV1::Cancel)
    );
    assert!(matches!(
        SandboxJobRepository::record_provisioning_intent(
            &cancelled_fixture.repository,
            RecordProvisioningIntentV1 {
                identity: old_identity,
                activation_token: OpaqueActivationToken::parse("3".repeat(64)).unwrap(),
            },
        )
        .await,
        Err(RepositoryError::StaleFence)
    ));
    let cancelled = SandboxJobRepository::reconcile_controls(
        &cancelled_fixture.repository,
        ReconcileSandboxControlsV1 { limit: 1 },
    )
    .await
    .unwrap()
    .pop()
    .unwrap();
    assert_eq!(cancelled.job.state, JobState::Cancelled);
    assert_eq!(cancelled.job.attempt_count, 1);
    assert_eq!(
        cancelled
            .payload
            .control
            .as_deref()
            .map(|control| control.kind),
        Some(SandboxControlKindV1::Cancel)
    );
    assert_sandbox_control_settled(&cancelled_fixture, "cancelled").await;

    let timeout_fixture = seed_fixture_with(
        pool,
        FixtureOptions {
            deadline_after: Duration::milliseconds(100),
            ..FixtureOptions::default()
        },
    )
    .await;
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;
    let timed_out = SandboxJobRepository::reconcile_controls(
        &timeout_fixture.repository,
        ReconcileSandboxControlsV1 { limit: 1 },
    )
    .await
    .unwrap()
    .pop()
    .unwrap();
    assert_eq!(timed_out.job.state, JobState::TimedOut);
    assert_eq!(timed_out.job.attempt_count, 0);
    assert!(timed_out.payload.physical.is_none());
    assert!(!timed_out.payload.cleanup.required);
    assert_eq!(
        timed_out
            .payload
            .control
            .as_deref()
            .map(|control| control.kind),
        Some(SandboxControlKindV1::Timeout)
    );
    assert_sandbox_control_settled(&timeout_fixture, "timed_out").await;
}

async fn execute_control(
    repository: &PgRepository,
    command: ControlCapabilityInvocation,
) -> insight_platform_postgres::capability_execution_repository::ControlledCapabilityExecution {
    let mut transaction = repository.begin_invocation_transaction().await.unwrap();
    let controlled = match transaction
        .control_capability_invocation(command)
        .await
        .unwrap()
    {
        CommandOutcome::Applied(controlled) => controlled,
        CommandOutcome::Replayed(_) => panic!("fresh Sandbox control unexpectedly replayed"),
    };
    transaction.commit().await.unwrap();
    controlled
}

fn control_audit(fixture: &Fixture) -> CommandAudit {
    control_audit_parts(
        &fixture.request.tenant_id,
        &fixture.invocation.payload.admission.principal.principal_id,
    )
}

fn control_audit_parts(tenant_id: &ResourceId, principal_id: &ResourceId) -> CommandAudit {
    CommandAudit {
        trace: TraceIdentityV1::generate(),
        tenant_id: tenant_id.clone(),
        principal_id: principal_id.clone(),
        principal_kind: PrincipalKind::AgentRunner,
        receipt_id: id(ResourceKind::Receipt),
        event_id: id(ResourceKind::Event),
        outbox_id: id(ResourceKind::OutboxEvent),
        idempotency_key_digest: digest('b'),
        request_digest: digest('c'),
        receipt_expires_at: Utc::now() + Duration::hours(1),
    }
}

async fn assert_sandbox_control_settled(fixture: &Fixture, expected_state: &str) {
    let invocation_state: String = sqlx::query_scalar(
        "SELECT state FROM insight_platform.invocations WHERE tenant_id = $1 AND invocation_id = $2",
    )
    .bind(fixture.request.tenant_id.to_string())
    .bind(fixture.invocation.invocation_id.to_string())
    .fetch_one(&fixture.pool)
    .await
    .unwrap();
    assert_eq!(invocation_state, expected_state);
    let settlement_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM insight_platform.quota_ledger WHERE tenant_id = $1 AND correlation_id = $2 AND entry_kind = 'settle'",
    )
    .bind(fixture.request.tenant_id.to_string())
    .bind(fixture.usage_reservation_id.to_string())
    .fetch_one(&fixture.pool)
    .await
    .unwrap();
    assert_eq!(settlement_count, 4);
    let unreleased_reservations: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM insight_platform.quota_accounts WHERE tenant_id = $1 AND reserved_value <> 0",
    )
    .bind(fixture.request.tenant_id.to_string())
    .fetch_one(&fixture.pool)
    .await
    .unwrap();
    assert_eq!(unreleased_reservations, 0);
    let quota_reservation_id: Option<String> = sqlx::query_scalar(
        "SELECT quota_reservation_id FROM insight_platform.jobs WHERE tenant_id = $1 AND job_id = $2",
    )
    .bind(fixture.request.tenant_id.to_string())
    .bind(fixture.request.job_id.to_string())
    .fetch_one(&fixture.pool)
    .await
    .unwrap();
    assert!(quota_reservation_id.is_none());
}

async fn exercise_boot_rollover_unknown_outcome(pool: PgPool) {
    let fixture = seed_fixture(pool).await;
    let leased = SandboxJobRepository::claim(
        &fixture.repository,
        claim(&id(ResourceKind::WorkerProcessGeneration), digest('1')),
    )
    .await
    .unwrap()
    .pop()
    .unwrap();
    let started = SandboxJobRepository::record_provisioning_intent(
        &fixture.repository,
        RecordProvisioningIntentV1 {
            identity: identity(&leased),
            activation_token: OpaqueActivationToken::parse("7".repeat(64)).unwrap(),
        },
    )
    .await
    .unwrap();
    let authorized_create = SandboxJobRepository::authorize_candidate_create(
        &fixture.repository,
        AuthorizeCandidateCreateV1 {
            identity: decision_identity(&started),
            create_ordinal: 1,
            limits: provisioning_limits(),
        },
    )
    .await
    .unwrap()
    .into_inner()
    .decision;
    let candidate = candidate(&fixture.request, "sandbox-boot-rollover");
    let observed = SandboxJobRepository::record_physical_observation(
        &fixture.repository,
        RecordSandboxObservationV1 {
            identity: decision_identity(&authorized_create),
            observation: SandboxDurableObservationV1::Candidate {
                candidate: candidate.clone(),
                limits: provisioning_limits(),
            },
        },
    )
    .await
    .unwrap();
    let selected = SandboxJobRepository::select_candidate(
        &fixture.repository,
        SelectSandboxCandidateV1 {
            identity: decision_identity(&observed),
            candidate: candidate.clone(),
        },
    )
    .await
    .unwrap()
    .into_inner();
    let activated = SandboxJobRepository::authorize_activation(
        &fixture.repository,
        AuthorizeSandboxActivationV1 {
            identity: decision_identity(&selected),
            sandbox_id: candidate.sandbox_id.clone(),
            boot_id: RunnerBootId::parse("boot-original").unwrap(),
        },
    )
    .await
    .unwrap()
    .into_inner();
    let rollover = SandboxRunnerStateFrameV1 {
        magic: String::new(),
        schema_version: 1,
        sandbox_id: candidate.sandbox_id,
        boot_id: RunnerBootId::parse("boot-restarted").unwrap(),
        execution_request_digest: fixture.request.request_digest.clone(),
        phase: SandboxRunnerPhaseV1::Armed,
        frame_digest: digest('0'),
    }
    .seal()
    .unwrap();
    let observation_command = RecordSandboxObservationV1 {
        identity: decision_identity(&activated),
        observation: SandboxDurableObservationV1::RunnerState {
            frame: rollover.clone(),
        },
    };
    let unknown = SandboxJobRepository::record_physical_observation(
        &fixture.repository,
        observation_command.clone(),
    )
    .await
    .unwrap();
    let physical = unknown.payload.physical.as_deref().unwrap();
    assert_eq!(physical.phase, SandboxPhysicalPhaseV1::UnknownOutcome);
    let rollover_digest = physical
        .runner_boot_rollover
        .as_deref()
        .unwrap()
        .evidence_digest
        .clone();
    let replayed =
        SandboxJobRepository::record_physical_observation(&fixture.repository, observation_command)
            .await
            .unwrap();
    assert_eq!(
        replayed
            .payload
            .physical
            .as_deref()
            .unwrap()
            .runner_boot_rollover
            .as_deref()
            .map(|evidence| &evidence.evidence_digest),
        Some(&rollover_digest)
    );

    let terminal = SandboxJobRepository::commit_terminal(
        &fixture.repository,
        CommitSandboxTerminalV1 {
            identity: decision_identity(&unknown),
            outcome: SandboxTerminalOutcomeV1::UnknownOutcome {
                evidence_digest: rollover_digest,
            },
        },
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(terminal.job.state, JobState::ReconciliationRequired);
    let invocation_state: String = sqlx::query_scalar(
        "SELECT state FROM insight_platform.invocations WHERE tenant_id = $1 AND invocation_id = $2",
    )
    .bind(fixture.request.tenant_id.to_string())
    .bind(fixture.invocation.invocation_id.to_string())
    .fetch_one(&fixture.pool)
    .await
    .unwrap();
    assert_eq!(invocation_state, "reconciliation_required");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn opensandbox_kubernetes_l3_dispatcher_kill_reclaims_same_started_runner() {
    if std::env::var("PLATFORM_OPENSANDBOX_L3_DISPATCHER").as_deref() != Ok("1") {
        eprintln!(
            "PLATFORM_OPENSANDBOX_L3_DISPATCHER is unset; real Dispatcher kill/reclaim L3 skipped"
        );
        return;
    }
    let database_url = std::env::var("PLATFORM_TEST_DATABASE_URL").unwrap();
    let image_uri = std::env::var("PLATFORM_OPENSANDBOX_L3_IMAGE").unwrap();
    let runtime_contract_digest = std::env::var("PLATFORM_OPENSANDBOX_L3_RUNTIME_DIGEST")
        .unwrap()
        .parse()
        .unwrap();
    let pool = PgPoolOptions::new()
        .max_connections(16)
        .connect(&database_url)
        .await
        .unwrap();
    verify_schema(&pool).await.unwrap();
    let expected_output = json!({"l3":"dispatcher-kill-reclaim"});
    let fixture = seed_fixture_with(
        pool,
        FixtureOptions {
            input: expected_output.clone(),
            image_uri,
            runtime_contract_digest,
            package_argv: vec![
                "/opt/insight/package".to_owned(),
                "sleep-echo".to_owned(),
                "15000".to_owned(),
            ],
            network_mode: SandboxNetworkMode::Disabled,
            deadline_after: Duration::minutes(5),
        },
    )
    .await;

    let before = wait_for_job(
        &fixture.pool,
        &fixture.request.tenant_id,
        &fixture.request.job_id,
        |snapshot| snapshot.physical_phase() == Some("started"),
        std::time::Duration::from_secs(45),
    )
    .await;
    assert_eq!(before.state, "running");
    assert_eq!(before.attempt_no, 1);
    assert_eq!(before.create_authorization_count(), Some(1));
    assert_eq!(before.candidate_ids().map(Vec::len), Some(1));
    let old_dispatcher = ready_dispatcher_pod().await;
    kubectl(&[
        "delete",
        "pod",
        "-n",
        "platform-sandbox",
        &old_dispatcher,
        "--wait=false",
    ])
    .await;
    let replacement_deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(60);
    loop {
        if let Some(replacement) = optional_ready_dispatcher_pod().await {
            if replacement != old_dispatcher {
                break;
            }
        }
        assert!(
            tokio::time::Instant::now() < replacement_deadline,
            "replacement Dispatcher did not become ready"
        );
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }

    let terminal = wait_for_job(
        &fixture.pool,
        &fixture.request.tenant_id,
        &fixture.request.job_id,
        |snapshot| snapshot.state == "succeeded",
        std::time::Duration::from_secs(100),
    )
    .await;
    assert_eq!(terminal.attempt_no, 1);
    assert!(terminal.lease_epoch > before.lease_epoch);
    assert_eq!(terminal.create_authorization_count(), Some(1));
    assert_eq!(terminal.candidate_ids(), before.candidate_ids());
    assert_eq!(terminal.selected_sandbox_id(), before.selected_sandbox_id());
    assert_eq!(terminal.runner_boot_id(), before.runner_boot_id());
    assert!(
        terminal.activation_token() == before.activation_token(),
        "activation token changed across Dispatcher reclaim"
    );

    let output: Value = sqlx::query_scalar(
        "SELECT inline_value FROM insight_platform.run_values WHERE tenant_id = $1 AND value_id = $2",
    )
    .bind(fixture.request.tenant_id.to_string())
    .bind(fixture.request.output_value_id.to_string())
    .fetch_one(&fixture.pool)
    .await
    .unwrap();
    assert_eq!(output, expected_output);

    let absent = wait_for_job(
        &fixture.pool,
        &fixture.request.tenant_id,
        &fixture.request.job_id,
        |snapshot| snapshot.physical_phase() == Some("absent"),
        std::time::Duration::from_secs(45),
    )
    .await;
    assert_eq!(absent.state, "succeeded");
    assert_eq!(absent.attempt_no, 1);
    let selector = format!("platform.insight.dev/job={}", fixture.request.job_id);
    let remaining = kubectl_json(&[
        "get",
        "batchsandboxes",
        "-n",
        "platform-sandbox-workloads",
        "-l",
        &selector,
        "-o",
        "json",
    ])
    .await;
    assert_eq!(
        remaining
            .get("items")
            .and_then(Value::as_array)
            .map(Vec::len),
        Some(0)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn opensandbox_kubernetes_l3_running_cancel_intent_survives_dispatcher_exit() {
    if std::env::var("PLATFORM_OPENSANDBOX_L3_CONTROL_PHASE").as_deref() != Ok("cancel-intent") {
        eprintln!(
            "PLATFORM_OPENSANDBOX_L3_CONTROL_PHASE=cancel-intent is unset; real control L3 skipped"
        );
        return;
    }
    let pool = l3_pool().await;
    verify_schema(&pool).await.unwrap();
    let fixture = seed_fixture_with(
        pool,
        FixtureOptions {
            input: json!({"l3":"control-server-outage"}),
            image_uri: std::env::var("PLATFORM_OPENSANDBOX_L3_IMAGE").unwrap(),
            runtime_contract_digest: std::env::var("PLATFORM_OPENSANDBOX_L3_RUNTIME_DIGEST")
                .unwrap()
                .parse()
                .unwrap(),
            package_argv: vec![
                "/opt/insight/package".to_owned(),
                "sleep-echo".to_owned(),
                "30000".to_owned(),
            ],
            network_mode: SandboxNetworkMode::Disabled,
            deadline_after: Duration::minutes(5),
        },
    )
    .await;
    let repository = Arc::new(fixture.repository);
    let dispatcher = OpenSandboxDispatcher::new(Arc::clone(&repository), Arc::new(l3_client()));
    let leased = SandboxJobRepository::claim(
        repository.as_ref(),
        claim(&id(ResourceKind::WorkerProcessGeneration), digest('d')),
    )
    .await
    .unwrap()
    .pop()
    .unwrap();
    let running = match dispatcher.drive_job(leased).await.unwrap() {
        SandboxDispatchProgressV1::AwaitingRunner(running) => running,
        progress => panic!("real Sandbox did not remain running: {progress:?}"),
    };
    assert_eq!(
        running
            .payload
            .physical
            .as_deref()
            .map(|physical| physical.phase),
        Some(SandboxPhysicalPhaseV1::Started)
    );
    let sandbox_id = running
        .payload
        .physical
        .as_deref()
        .and_then(|physical| physical.selected_sandbox_id.as_ref())
        .unwrap()
        .clone();

    let controlled = execute_control(
        repository.as_ref(),
        ControlCapabilityInvocation {
            audit: control_audit_parts(
                &fixture.request.tenant_id,
                &fixture.invocation.payload.admission.principal.principal_id,
            ),
            invocation_id: fixture.invocation.invocation_id.clone(),
            expected_invocation_version: fixture.invocation.version,
            quota_entry_ids: vec![],
            kind: CapabilityControlKind::Cancel,
        },
    )
    .await;
    assert_eq!(controlled.invocation.state, InvocationState::Cancelling);
    assert_eq!(controlled.job.unwrap().state, "cancelling");
    assert!(batchsandbox_exists(&fixture.request.job_id, &sandbox_id).await);

    // The qualification script requests an abrupt process boundary here. The next phase runs in
    // a new process while OpenSandbox Server is deliberately unavailable.
    if std::env::var("PLATFORM_OPENSANDBOX_L3_ABORT_AFTER_INTENT").as_deref() == Ok("1") {
        std::process::abort();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn opensandbox_kubernetes_l3_cancel_terminal_commits_while_server_is_unavailable() {
    if std::env::var("PLATFORM_OPENSANDBOX_L3_CONTROL_PHASE").as_deref() != Ok("cancel-terminal") {
        eprintln!(
            "PLATFORM_OPENSANDBOX_L3_CONTROL_PHASE=cancel-terminal is unset; real control L3 skipped"
        );
        return;
    }
    let pool = l3_pool().await;
    verify_schema(&pool).await.unwrap();
    let repository = PgRepository::new(pool.clone());
    let before = load_l3_control_job(&pool, "control-server-outage").await;
    assert_eq!(before.state, "cancelling");
    assert_eq!(before.physical_phase(), Some("started"));

    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
    let terminal = 'reconcile: loop {
        for decision in SandboxJobRepository::reconcile_controls(
            &repository,
            ReconcileSandboxControlsV1 { limit: 1 },
        )
        .await
        .unwrap()
        {
            if decision.job.tenant_id == before.tenant_id && decision.job.job_id == before.job_id {
                break 'reconcile decision;
            }
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "durable cancel intent was not reconciled"
        );
    };
    assert_eq!(terminal.job.state, JobState::Cancelled);
    assert_eq!(terminal.job.attempt_count, 1);
    assert!(terminal.fence.is_none());
    assert!(terminal.payload.cleanup.required);
    assert_l3_control_terminal(&pool, &before.tenant_id, &before.job_id, "cancelled").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn opensandbox_kubernetes_l3_cancel_cleanup_resumes_after_server_recovery() {
    if std::env::var("PLATFORM_OPENSANDBOX_L3_CONTROL_PHASE").as_deref() != Ok("cancel-cleanup") {
        eprintln!(
            "PLATFORM_OPENSANDBOX_L3_CONTROL_PHASE=cancel-cleanup is unset; real control L3 skipped"
        );
        return;
    }
    let pool = l3_pool().await;
    verify_schema(&pool).await.unwrap();
    let before = load_l3_control_job(&pool, "control-server-outage").await;
    assert_eq!(before.state, "cancelled");
    assert_eq!(before.physical_phase(), Some("started"));
    let repository = Arc::new(PgRepository::new(pool.clone()));
    cleanup_l3_control_job(Arc::clone(&repository), &before.tenant_id, &before.job_id).await;
    let after = load_l3_control_job(&pool, "control-server-outage").await;
    assert_eq!(after.state, "cancelled");
    assert_eq!(after.physical_phase(), Some("absent"));
    assert!(!after.cleanup_required());
    assert_no_batchsandbox_for_job(&after.job_id).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn opensandbox_kubernetes_l3_running_deadline_terminal_and_cleanup() {
    if std::env::var("PLATFORM_OPENSANDBOX_L3_CONTROL_PHASE").as_deref() != Ok("timeout") {
        eprintln!(
            "PLATFORM_OPENSANDBOX_L3_CONTROL_PHASE=timeout is unset; real control L3 skipped"
        );
        return;
    }
    let pool = l3_pool().await;
    verify_schema(&pool).await.unwrap();
    let fixture = seed_fixture_with(
        pool.clone(),
        FixtureOptions {
            input: json!({"l3":"running-timeout"}),
            image_uri: std::env::var("PLATFORM_OPENSANDBOX_L3_IMAGE").unwrap(),
            runtime_contract_digest: std::env::var("PLATFORM_OPENSANDBOX_L3_RUNTIME_DIGEST")
                .unwrap()
                .parse()
                .unwrap(),
            package_argv: vec![
                "/opt/insight/package".to_owned(),
                "sleep-echo".to_owned(),
                "30000".to_owned(),
            ],
            network_mode: SandboxNetworkMode::Disabled,
            deadline_after: Duration::seconds(8),
        },
    )
    .await;
    let repository = Arc::new(fixture.repository);
    let dispatcher = OpenSandboxDispatcher::new(Arc::clone(&repository), Arc::new(l3_client()));
    let leased = SandboxJobRepository::claim(
        repository.as_ref(),
        claim(&id(ResourceKind::WorkerProcessGeneration), digest('e')),
    )
    .await
    .unwrap()
    .pop()
    .unwrap();
    let running = match dispatcher.drive_job(leased).await.unwrap() {
        SandboxDispatchProgressV1::AwaitingRunner(running) => running,
        progress => panic!("real Sandbox did not remain running: {progress:?}"),
    };
    assert_eq!(
        running
            .payload
            .physical
            .as_deref()
            .map(|physical| physical.phase),
        Some(SandboxPhysicalPhaseV1::Started)
    );
    let until = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
    let terminal = loop {
        let reconciled = SandboxJobRepository::reconcile_controls(
            repository.as_ref(),
            ReconcileSandboxControlsV1 { limit: 1 },
        )
        .await
        .unwrap();
        if let Some(decision) = reconciled.into_iter().find(|decision| {
            decision.job.tenant_id == fixture.request.tenant_id
                && decision.job.job_id == fixture.request.job_id
        }) {
            break decision;
        }
        assert!(
            tokio::time::Instant::now() < until,
            "running Sandbox deadline was not materialized"
        );
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    };
    assert_eq!(terminal.job.state, JobState::TimedOut);
    assert_eq!(terminal.job.attempt_count, 1);
    assert_l3_control_terminal(
        &pool,
        &fixture.request.tenant_id,
        &fixture.request.job_id,
        "timed_out",
    )
    .await;
    cleanup_l3_control_job(
        Arc::clone(&repository),
        &fixture.request.tenant_id,
        &fixture.request.job_id,
    )
    .await;
    assert_no_batchsandbox_for_job(&fixture.request.job_id).await;
}

#[derive(Debug)]
struct LiveSandboxJob {
    state: String,
    attempt_no: i32,
    lease_epoch: i64,
    payload: Value,
}

impl LiveSandboxJob {
    fn physical(&self) -> Option<&serde_json::Map<String, Value>> {
        self.payload.get("physical")?.as_object()
    }

    fn physical_phase(&self) -> Option<&str> {
        self.physical()?.get("phase")?.as_str()
    }

    fn create_authorization_count(&self) -> Option<u64> {
        self.physical()?.get("create_authorization_count")?.as_u64()
    }

    fn candidate_ids(&self) -> Option<&Vec<Value>> {
        self.physical()?.get("candidate_ids")?.as_array()
    }

    fn selected_sandbox_id(&self) -> Option<&str> {
        self.physical()?.get("selected_sandbox_id")?.as_str()
    }

    fn runner_boot_id(&self) -> Option<&str> {
        self.physical()?.get("runner_boot_id")?.as_str()
    }

    fn activation_token(&self) -> Option<&str> {
        self.physical()?.get("activation_token")?.as_str()
    }
}

#[derive(Debug)]
struct L3ControlJob {
    tenant_id: ResourceId,
    job_id: ResourceId,
    state: String,
    payload: Value,
}

impl L3ControlJob {
    fn physical_phase(&self) -> Option<&str> {
        self.payload.pointer("/physical/phase")?.as_str()
    }

    fn cleanup_required(&self) -> bool {
        self.payload
            .pointer("/cleanup/required")
            .and_then(Value::as_bool)
            .unwrap_or(false)
    }
}

async fn l3_pool() -> PgPool {
    PgPoolOptions::new()
        .max_connections(16)
        .connect(&std::env::var("PLATFORM_TEST_DATABASE_URL").unwrap())
        .await
        .unwrap()
}

fn l3_client() -> OpenSandboxHttpClient {
    OpenSandboxHttpClient::new(OpenSandboxHttpClientConfig {
        lifecycle_base_url: std::env::var("PLATFORM_OPENSANDBOX_L3_URL")
            .unwrap()
            .parse()
            .unwrap(),
        api_key: OpenSandboxApiKey::parse(
            std::env::var("PLATFORM_OPENSANDBOX_L3_API_KEY").unwrap(),
        )
        .unwrap(),
        request_timeout_milliseconds: 10_000,
        connect_timeout_milliseconds: 1_000,
        candidate_page_items: 4,
        orphan_page_items: 20,
    })
    .unwrap()
}

async fn load_l3_control_job(pool: &PgPool, marker: &str) -> L3ControlJob {
    let row = sqlx::query(
        r#"
        SELECT job.tenant_id, job.job_id, job.state, job.payload
        FROM insight_platform.jobs AS job
        JOIN insight_platform.invocations AS invocation
          ON invocation.tenant_id = job.tenant_id AND invocation.invocation_id = job.invocation_id
        JOIN insight_platform.run_values AS input
          ON input.tenant_id = invocation.tenant_id AND input.value_id = invocation.input_value_id
        WHERE input.inline_value ->> 'l3' = $1
        ORDER BY job.updated_at DESC
        LIMIT 1
        "#,
    )
    .bind(marker)
    .fetch_one(pool)
    .await
    .unwrap();
    L3ControlJob {
        tenant_id: row.get::<String, _>("tenant_id").parse().unwrap(),
        job_id: row.get::<String, _>("job_id").parse().unwrap(),
        state: row.get("state"),
        payload: row.get("payload"),
    }
}

async fn assert_l3_control_terminal(
    pool: &PgPool,
    tenant_id: &ResourceId,
    job_id: &ResourceId,
    expected_state: &str,
) {
    let row = sqlx::query(
        r#"
        SELECT job.state,
               invocation.state AS invocation_state,
               job.quota_reservation_id,
               (SELECT count(*) FROM insight_platform.quota_ledger AS ledger
                WHERE ledger.tenant_id = job.tenant_id AND ledger.entry_kind = 'settle') AS settlements,
               (SELECT count(*) FROM insight_platform.quota_accounts AS account
                WHERE account.tenant_id = job.tenant_id AND account.reserved_value <> 0) AS reserved_accounts,
               (SELECT count(*)
                FROM insight_platform.events AS event
                JOIN insight_platform.outbox_events AS outbox
                  ON outbox.tenant_id = event.tenant_id AND outbox.event_id = event.event_id
                WHERE event.tenant_id = job.tenant_id AND event.aggregate_id = job.job_id
                  AND event.event_type = 'sandbox.job.controlled') AS controlled_events
        FROM insight_platform.jobs AS job
        JOIN insight_platform.invocations AS invocation
          ON invocation.tenant_id = job.tenant_id AND invocation.invocation_id = job.invocation_id
        WHERE job.tenant_id = $1 AND job.job_id = $2
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(job_id.to_string())
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(row.get::<String, _>("state"), expected_state);
    assert_eq!(row.get::<String, _>("invocation_state"), expected_state);
    assert!(row
        .get::<Option<String>, _>("quota_reservation_id")
        .is_none());
    assert_eq!(row.get::<i64, _>("settlements"), 4);
    assert_eq!(row.get::<i64, _>("reserved_accounts"), 0);
    assert_eq!(row.get::<i64, _>("controlled_events"), 1);
}

async fn cleanup_l3_control_job(
    repository: Arc<PgRepository>,
    tenant_id: &ResourceId,
    job_id: &ResourceId,
) {
    let dispatcher = OpenSandboxDispatcher::new(Arc::clone(&repository), Arc::new(l3_client()));
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(45);
    loop {
        let current = sqlx::query_scalar::<_, Value>(
            "SELECT payload FROM insight_platform.jobs WHERE tenant_id = $1 AND job_id = $2",
        )
        .bind(tenant_id.to_string())
        .bind(job_id.to_string())
        .fetch_one(repository.pool())
        .await
        .unwrap();
        if current
            .pointer("/cleanup/required")
            .and_then(Value::as_bool)
            == Some(false)
        {
            return;
        }
        let claims = SandboxJobRepository::claim_cleanup(
            repository.as_ref(),
            SandboxCleanupClaimV1 {
                process_generation_id: id(ResourceKind::WorkerProcessGeneration),
                limit: 1,
                lease_milliseconds: 1_000,
            },
        )
        .await
        .unwrap();
        for mut claim in claims {
            loop {
                match dispatcher.cleanup_once(claim).await {
                    Ok(SandboxCleanupProgressV1::Complete) => break,
                    Ok(SandboxCleanupProgressV1::CandidateAbsent(next)) => claim = *next,
                    Err(_) => {
                        // DELETE may have committed while its response was lost. Leave the
                        // durable cleanup requirement set, let the short fence expire, and prove
                        // absence from a fresh claim.
                        tokio::time::sleep(std::time::Duration::from_millis(1_100)).await;
                        break;
                    }
                }
            }
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "controlled Sandbox cleanup did not complete"
        );
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}

async fn batchsandbox_exists(job_id: &ResourceId, sandbox_id: &OpenSandboxId) -> bool {
    let selector = format!("platform.insight.dev/job={job_id}");
    let value = kubectl_json(&[
        "get",
        "batchsandboxes",
        "-n",
        "platform-sandbox-workloads",
        "-l",
        &selector,
        "-o",
        "json",
    ])
    .await;
    value
        .get("items")
        .and_then(Value::as_array)
        .is_some_and(|items| {
            items.iter().any(|item| {
                item.pointer("/metadata/name").and_then(Value::as_str) == Some(sandbox_id.as_str())
            })
        })
}

async fn assert_no_batchsandbox_for_job(job_id: &ResourceId) {
    let selector = format!("platform.insight.dev/job={job_id}");
    let remaining = kubectl_json(&[
        "get",
        "batchsandboxes",
        "-n",
        "platform-sandbox-workloads",
        "-l",
        &selector,
        "-o",
        "json",
    ])
    .await;
    assert_eq!(
        remaining
            .get("items")
            .and_then(Value::as_array)
            .map(Vec::len),
        Some(0)
    );
}

async fn wait_for_job(
    pool: &PgPool,
    tenant_id: &ResourceId,
    job_id: &ResourceId,
    predicate: impl Fn(&LiveSandboxJob) -> bool,
    timeout: std::time::Duration,
) -> LiveSandboxJob {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let row = sqlx::query(
            "SELECT state, attempt_no, lease_epoch, payload FROM insight_platform.jobs WHERE tenant_id = $1 AND job_id = $2",
        )
        .bind(tenant_id.to_string())
        .bind(job_id.to_string())
        .fetch_one(pool)
        .await
        .unwrap();
        let snapshot = LiveSandboxJob {
            state: row.get("state"),
            attempt_no: row.get("attempt_no"),
            lease_epoch: row.get("lease_epoch"),
            payload: row.get("payload"),
        };
        if predicate(&snapshot) {
            return snapshot;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "shared Sandbox Job did not reach the expected state"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}

async fn ready_dispatcher_pod() -> String {
    optional_ready_dispatcher_pod()
        .await
        .expect("a ready Dispatcher pod")
}

async fn optional_ready_dispatcher_pod() -> Option<String> {
    let value = kubectl_json(&[
        "get",
        "pods",
        "-n",
        "platform-sandbox",
        "-l",
        "app.kubernetes.io/component=dispatcher",
        "-o",
        "json",
    ])
    .await;
    value
        .get("items")?
        .as_array()?
        .iter()
        .find(|pod| {
            pod.pointer("/status/containerStatuses/0/ready")
                .and_then(Value::as_bool)
                == Some(true)
        })
        .and_then(|pod| pod.pointer("/metadata/name"))
        .and_then(Value::as_str)
        .map(str::to_owned)
}

async fn kubectl_json(arguments: &[&str]) -> Value {
    serde_json::from_slice(&kubectl(arguments).await).unwrap()
}

async fn kubectl(arguments: &[&str]) -> Vec<u8> {
    let output = tokio::process::Command::new("kubectl")
        .args(arguments)
        .output()
        .await
        .unwrap();
    assert!(
        output.status.success(),
        "kubectl failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    output.stdout
}

async fn seed_fixture(pool: PgPool) -> Fixture {
    seed_fixture_with(pool, FixtureOptions::default()).await
}

async fn seed_fixture_with(pool: PgPool, options: FixtureOptions) -> Fixture {
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
    repository
        .bind_tenant_principal(NewTenantPrincipal {
            tenant_id: tenant_id.clone(),
            principal_id: principal_id.clone(),
            principal_kind: PrincipalKind::AgentRunner,
            payload: TenantPrincipalPayload {
                permissions: PermissionSet::new(vec![
                    Permission::CapabilityInvoke,
                    Permission::RuntimeControl,
                ])
                .unwrap(),
            },
        })
        .await
        .unwrap();
    let now: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
        .fetch_one(&pool)
        .await
        .unwrap();
    let deadline = now + options.deadline_after;
    let deployment_id = id(ResourceKind::CapabilityDeployment);
    let interface_exact =
        ExactVersionRef::new(id(ResourceKind::CapabilityInterfaceRevision), digest('e')).unwrap();
    let interface_spec = fixture_interface_spec();
    seed_deployment(
        &pool,
        &tenant_id,
        &principal_id,
        &deployment_id,
        &interface_exact,
        &interface_spec,
    )
    .await;
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

    let input_value = options.input;
    let input_digest = value_digest(&input_value);
    let input_schema_digest = interface_spec.input_schema.canonical_digest.clone();
    let output_schema_digest = interface_spec.output_schema.canonical_digest.clone();
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
        interface_exact,
        &interface_spec,
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
        image_uri: options.image_uri,
        runtime_version_id: id(ResourceKind::SandboxRuntimeRevision),
        runtime_contract_digest: options.runtime_contract_digest,
        sandbox_profile_deployment_id: id(ResourceKind::SandboxProfileDeployment),
        profile_deployment_digest: digest('6'),
        runner_argv: vec!["/usr/local/bin/platform-sandbox-runner".to_owned()],
        package_argv: options.package_argv,
        input_value_id,
        output_value_id: id(ResourceKind::RunValue),
        classification: DataClassification::Internal,
        input: input_value,
        input_schema_digest,
        input_digest: digest('0'),
        output_schema_digest,
        network_mode: options.network_mode,
        limits: resource_limits(),
        provisioning_limits: provisioning_limits(),
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
    interface_exact: ExactVersionRef,
    interface_spec: &CapabilityInterfaceResourceSpec,
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
        interface: interface_exact,
        capability_name: interface_spec.qualified_name.clone(),
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
        effect: interface_spec.effect,
        idempotency: interface_spec.idempotency,
        cancellation: interface_spec.cancellation,
        progress: interface_spec.progress.clone(),
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
        error_schema_digest: interface_spec.error_schema.canonical_digest.clone(),
        artifact_contract: interface_spec.artifacts.clone(),
        data_flow_policy: interface_spec.data_policy.clone(),
        interface_limits: interface_spec.execution_limits,
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

fn fixture_interface_spec() -> CapabilityInterfaceResourceSpec {
    let value_schema = ClosedJsonSchema::build(json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "properties": {
            "l3": {
                "description": "Bounded L3 fixture marker.",
                "x-platform-classification": "internal",
                "type": "string",
                "minLength": 1,
                "maxLength": 128,
                "x-platform-max-bytes": 512
            },
            "question": {
                "description": "Bounded L2 fixture input.",
                "x-platform-classification": "internal",
                "type": "string",
                "minLength": 1,
                "maxLength": 128,
                "x-platform-max-bytes": 512
            }
        },
        "required": [],
        "additionalProperties": false
    }))
    .unwrap();
    let error_schema = ClosedJsonSchema::build(json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "properties": {
            "error": {
                "description": "Bounded fixture error.",
                "x-platform-classification": "internal",
                "type": "string",
                "minLength": 1,
                "maxLength": 128,
                "x-platform-max-bytes": 512
            }
        },
        "required": ["error"],
        "additionalProperties": false
    }))
    .unwrap();
    CapabilityInterfaceResourceSpec {
        authoring_package: AuthoringPackage {
            artifact: ArtifactRef::new(
                id(ResourceKind::Artifact),
                digest('7'),
                1,
                "application/json",
                DataClassification::Internal,
                Some("opensandbox-l3-fixture.json".to_owned()),
            )
            .unwrap(),
            manifest_digest: digest('8'),
        },
        contract_digest: digest('9'),
        dependency_versions: vec![],
        policy_versions: vec![],
        qualified_name: "fixture.sandbox".parse().unwrap(),
        input_schema: value_schema.clone(),
        output_schema: value_schema,
        error_schema,
        artifacts: CapabilityArtifactContract { ports: vec![] },
        data_policy: CapabilityDataFlowPolicy {
            maximum_input_classification: DataClassification::Restricted,
            maximum_output_classification: DataClassification::Restricted,
            allowed_regions: vec!["global".parse().unwrap()],
            declassification_policy: None,
        },
        execution_limits: CapabilityInterfaceLimits {
            maximum_input_bytes: 1_048_576,
            maximum_output_bytes: 1_048_576,
            maximum_artifacts: 0,
            maximum_execution_milliseconds: 60_000,
        },
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
    }
}

async fn seed_deployment(
    pool: &PgPool,
    tenant_id: &ResourceId,
    principal_id: &ResourceId,
    deployment_id: &ResourceId,
    interface_exact: &ExactVersionRef,
    interface_spec: &CapabilityInterfaceResourceSpec,
) {
    let resource_id = id(ResourceKind::CapabilityInterface);
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
    let published = PublishedVersionPayload {
        document: ResourceDocument::CapabilityInterface(interface_spec.clone()),
        validation: ValidationSummary {
            validator_digest: digest('a'),
            validated_draft_digest: digest('b'),
            dependency_closure_digest: digest('c'),
            security_evidence_digest: digest('d'),
            warnings: vec![],
        },
    };
    published
        .validate_for(
            RegistryResourceKind::CapabilityInterface,
            &interface_exact.revision_id,
        )
        .unwrap();
    let version_payload = TypedPayload::new(1, &published).unwrap();
    sqlx::query(
        "INSERT INTO insight_platform.resource_versions (tenant_id, resource_version_id, resource_id, resource_version_kind, revision_no, content_digest, payload_schema_version, payload, payload_digest, created_by) VALUES ($1, $2, $3, $4, 1, $5, $6, $7, $8, $9)",
    )
    .bind(tenant_id.to_string())
    .bind(interface_exact.revision_id.to_string())
    .bind(resource_id.to_string())
    .bind(interface_exact.resource_kind.descriptor().name)
    .bind(interface_exact.semantic_digest.to_string())
    .bind(version_payload.schema_version)
    .bind(&version_payload.value)
    .bind(&version_payload.digest)
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
    .bind(interface_exact.revision_id.to_string())
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
            purpose: SandboxCandidatePurposeV1::Job,
            tenant_id: request.tenant_id.clone(),
            job_id: request.job_id.clone(),
            physical_attempt: request.physical_attempt,
            create_ordinal: 1,
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
