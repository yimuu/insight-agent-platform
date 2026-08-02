mod support;

use std::collections::{BTreeMap, BTreeSet};

use chrono::{Duration, Utc};
use insight_dsl::{compile_source, CompileOptions};
use insight_durable::{
    ActivateProviderRevisionCommand, ClaimProviderOperationsCommand,
    CompleteProviderConnectionTestCommand, CompleteProviderConnectionTestResult,
    CompleteProviderDiscoveryCommand, CompleteProviderDiscoveryResult, CreateProviderCommand,
    CreateProviderConnectionTestCommand, CreateProviderDiscoveryCommand,
    CreateProviderValidationCommand, CreateRunCommand, DurableRepository,
    ProviderConnectionTestMode, ProviderManagementConflict, ProviderManagementDurableRepository,
    ProviderManagementWriteError, ProviderMutationMetadata, ProviderValidationReport,
    PublishProviderRevisionCommand, RecordProviderManagementRejectionCommand,
    ReplaceProviderDraftCommand, ResumeProviderCommand, RetireProviderCommand,
    SuspendProviderCommand, VersionedPlan,
};
use insight_engine::{
    DefinitionRevisionId, DeploymentRevisionId, RunId, TransitionKey, TransitionOutcome,
};
use insight_storage::{PostgresDurableRepository, SqliteDurableRepository};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use sqlx::{postgres::PgPoolOptions, AssertSqlSafe, PgPool};
use uuid::Uuid;

fn hash_bytes(bytes: &[u8]) -> String {
    let mut value = String::from("sha256:");
    for byte in Sha256::digest(bytes) {
        use std::fmt::Write as _;
        write!(&mut value, "{byte:02x}").unwrap();
    }
    value
}

fn hash(label: &str) -> String {
    hash_bytes(label.as_bytes())
}

fn json_hash(value: &Value) -> String {
    hash_bytes(&serde_jcs::to_vec(value).unwrap())
}

fn now() -> chrono::DateTime<Utc> {
    "2026-08-01T08:00:00Z".parse().unwrap()
}

fn metadata(id: &str, method: &str, path: &str) -> ProviderMutationMetadata {
    ProviderMutationMetadata {
        operator_id: "operator-a".to_owned(),
        capability: "provider.write".to_owned(),
        method: method.to_owned(),
        canonical_path: path.to_owned(),
        request_id: id.to_owned(),
        request_hash: hash(id),
        now: now(),
    }
}

fn admission_plan() -> VersionedPlan {
    let source = r#"api_version: insight.agent/v1
kind: agent
inputs: {}
output: string
workflow:
  steps:
    - return: admitted
"#;
    let plan = compile_source(
        source,
        CompileOptions::new(
            DefinitionRevisionId::new("provider_admission_definition_v1").unwrap(),
            "provider-admission.yaml",
            source,
        ),
    )
    .unwrap();
    VersionedPlan::from_verified_plan(
        "provider_admission_definition",
        "provider_admission_agent",
        "Provider admission fence fixture",
        DeploymentRevisionId::new("provider_admission_deployment_v1").unwrap(),
        "expression-3.0.0",
        json!({"author":"structured"}),
        &plan,
        json!({}),
        json!([]),
        json!([]),
    )
    .unwrap()
}

async fn exercise_provider_lifecycle<R>(repository: &R)
where
    R: ProviderManagementDurableRepository + DurableRepository + ?Sized,
{
    repository
        .record_provider_management_rejection(RecordProviderManagementRejectionCommand {
            actor_id: "operator-a".to_owned(),
            capability: "provider.write".to_owned(),
            request_id: "provider-rejected-1".to_owned(),
            provider_id: None,
            subject_id: "provider.write".to_owned(),
            result_code: "http_400".to_owned(),
            now: now(),
        })
        .await
        .unwrap();
    let draft = json!({
        "adapter_type":"open_ai_compatible",
        "base_url":"https://models.example.test/v1",
        "credential_ref":"secret://providers/example",
        "models":[]
    });
    let input_hash = json_hash(&draft);
    let create = CreateProviderCommand {
        metadata: metadata("create-1", "POST", "/v1/admin/providers"),
        provider_id: "example".to_owned(),
        display_name: "Example".to_owned(),
        adapter_type: "open_ai_compatible".to_owned(),
        draft_document: draft,
        provider_input_hash: input_hash.clone(),
    };
    let created = repository.create_provider(create.clone()).await.unwrap();
    assert_eq!(created.status, 201);
    assert_eq!(created.etag.as_deref(), Some("\"provider-1\""));
    assert!(repository.create_provider(create).await.unwrap().replayed);
    repository
        .create_provider(CreateProviderCommand {
            metadata: metadata("create-2", "POST", "/v1/admin/providers"),
            provider_id: "example-backup".to_owned(),
            display_name: "Example Backup".to_owned(),
            adapter_type: "open_ai_compatible".to_owned(),
            draft_document: json!({"models":[]}),
            provider_input_hash: json_hash(&json!({"models":[]})),
        })
        .await
        .unwrap();
    let first_page = repository.list_providers(None, None, 1).await.unwrap();
    let second_page = repository
        .list_providers(None, first_page.next_cursor.as_deref(), 1)
        .await
        .unwrap();
    assert_eq!(first_page.items.len(), 1);
    assert_eq!(second_page.items.len(), 1);
    assert!(second_page.next_cursor.is_none());
    assert_eq!(
        BTreeSet::from([
            first_page.items[0].provider_id.clone(),
            second_page.items[0].provider_id.clone(),
        ]),
        BTreeSet::from(["example".to_owned(), "example-backup".to_owned()]),
        "composite pagination must not duplicate or omit same-timestamp Providers"
    );
    let backup_race_a = repository.replace_provider_draft(ReplaceProviderDraftCommand {
        metadata: metadata(
            "backup-race-a",
            "PUT",
            "/v1/admin/providers/example-backup/draft",
        ),
        provider_id: "example-backup".to_owned(),
        expected_draft_version: 1,
        draft_document: json!({"winner":"a"}),
        provider_input_hash: hash("backup-race-a"),
    });
    let backup_race_b = repository.replace_provider_draft(ReplaceProviderDraftCommand {
        metadata: metadata(
            "backup-race-b",
            "PUT",
            "/v1/admin/providers/example-backup/draft",
        ),
        provider_id: "example-backup".to_owned(),
        expected_draft_version: 1,
        draft_document: json!({"winner":"b"}),
        provider_input_hash: hash("backup-race-b"),
    });
    let (backup_race_a, backup_race_b) = tokio::join!(backup_race_a, backup_race_b);
    assert_eq!(
        usize::from(backup_race_a.is_ok()) + usize::from(backup_race_b.is_ok()),
        1
    );
    for rejected in [backup_race_a, backup_race_b]
        .into_iter()
        .filter_map(Result::err)
    {
        assert!(matches!(
            rejected,
            ProviderManagementWriteError::Conflict(ProviderManagementConflict::PreconditionFailed)
        ));
    }

    let configured = json!({
        "adapter_type":"open_ai_compatible",
        "base_url":"https://models.example.test/v1",
        "credential_ref":"secret://providers/example",
        "models":[{"id":"model-a","supports_tools":true}]
    });
    let configured_hash = json_hash(&configured);
    repository
        .replace_provider_draft(ReplaceProviderDraftCommand {
            metadata: metadata("draft-2", "PUT", "/v1/admin/providers/example/draft"),
            provider_id: "example".to_owned(),
            expected_draft_version: 1,
            draft_document: configured,
            provider_input_hash: configured_hash.clone(),
        })
        .await
        .unwrap();

    repository
        .create_provider_discovery(CreateProviderDiscoveryCommand {
            metadata: metadata(
                "discovery-1",
                "POST",
                "/v1/admin/providers/example/discoveries",
            ),
            discovery_id: "pdisc_1".to_owned(),
            provider_id: "example".to_owned(),
            expected_draft_version: 2,
            provider_input_hash: configured_hash.clone(),
            max_pending_operations: 2,
        })
        .await
        .unwrap();
    let discovery_claim = repository
        .claim_provider_discoveries(ClaimProviderOperationsCommand {
            worker_id: "worker-a".to_owned(),
            now: now(),
            lease_expires_at: now() + Duration::seconds(30),
            limit: 1,
        })
        .await
        .unwrap()
        .pop()
        .unwrap();
    let snapshot = json!({"models":[
        {"id":"model-a","supports_tools":true},
        {"id":"model-b","supports_tools":false}
    ]});
    repository
        .complete_provider_discovery(CompleteProviderDiscoveryCommand {
            discovery_id: "pdisc_1".to_owned(),
            claim_token: discovery_claim.claim_token,
            now: now(),
            result: CompleteProviderDiscoveryResult::Succeeded {
                catalog_fingerprint: json_hash(&snapshot),
                snapshot_document: snapshot,
            },
        })
        .await
        .unwrap();
    let candidates = repository
        .list_provider_model_candidates("example", "pdisc_1", None, 1)
        .await
        .unwrap();
    let next_candidates = repository
        .list_provider_model_candidates("example", "pdisc_1", candidates.next_cursor.as_deref(), 1)
        .await
        .unwrap();
    assert_eq!(candidates.items.len(), 1);
    assert_eq!(next_candidates.items.len(), 1);
    assert!(next_candidates.next_cursor.is_none());
    assert_ne!(
        candidates.items[0].model_id,
        next_candidates.items[0].model_id
    );

    repository
        .create_provider_discovery(CreateProviderDiscoveryCommand {
            metadata: metadata(
                "discovery-lease",
                "POST",
                "/v1/admin/providers/example/discoveries",
            ),
            discovery_id: "pdisc_lease".to_owned(),
            provider_id: "example".to_owned(),
            expected_draft_version: 2,
            provider_input_hash: configured_hash.clone(),
            max_pending_operations: 2,
        })
        .await
        .unwrap();
    let first_lease = repository
        .claim_provider_discoveries(ClaimProviderOperationsCommand {
            worker_id: "worker-a".to_owned(),
            now: now(),
            lease_expires_at: now() + Duration::seconds(1),
            limit: 1,
        })
        .await
        .unwrap()
        .pop()
        .unwrap();
    let recovered_lease = repository
        .claim_provider_discoveries(ClaimProviderOperationsCommand {
            worker_id: "worker-b".to_owned(),
            now: now() + Duration::seconds(2),
            lease_expires_at: now() + Duration::seconds(32),
            limit: 1,
        })
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_ne!(first_lease.claim_token, recovered_lease.claim_token);
    let lease_snapshot = json!({"models":[]});
    assert!(matches!(
        repository
            .complete_provider_discovery(CompleteProviderDiscoveryCommand {
                discovery_id: "pdisc_lease".to_owned(),
                claim_token: first_lease.claim_token,
                now: now() + Duration::seconds(2),
                result: CompleteProviderDiscoveryResult::Succeeded {
                    catalog_fingerprint: json_hash(&lease_snapshot),
                    snapshot_document: lease_snapshot.clone(),
                },
            })
            .await,
        Err(ProviderManagementWriteError::Conflict(
            ProviderManagementConflict::FenceLost
        ))
    ));
    repository
        .complete_provider_discovery(CompleteProviderDiscoveryCommand {
            discovery_id: "pdisc_lease".to_owned(),
            claim_token: recovered_lease.claim_token,
            now: now() + Duration::seconds(2),
            result: CompleteProviderDiscoveryResult::Succeeded {
                catalog_fingerprint: json_hash(&lease_snapshot),
                snapshot_document: lease_snapshot,
            },
        })
        .await
        .unwrap();
    assert!(repository
        .get_provider_discovery_snapshot("example", "pdisc_lease")
        .await
        .unwrap()
        .is_some());

    repository
        .create_provider_connection_test(CreateProviderConnectionTestCommand {
            metadata: metadata(
                "test-1",
                "POST",
                "/v1/admin/providers/example/connection-tests",
            ),
            test_id: "ptest_1".to_owned(),
            provider_id: "example".to_owned(),
            expected_draft_version: 2,
            provider_input_hash: configured_hash.clone(),
            mode: ProviderConnectionTestMode::Canary,
            max_pending_operations: 2,
        })
        .await
        .unwrap();
    let test_claim = repository
        .claim_provider_connection_tests(ClaimProviderOperationsCommand {
            worker_id: "worker-a".to_owned(),
            now: now(),
            lease_expires_at: now() + Duration::seconds(30),
            limit: 1,
        })
        .await
        .unwrap()
        .pop()
        .unwrap();
    let test_result = json!({"reachable":true,"model_id":"model-a"});
    repository
        .complete_provider_connection_test(CompleteProviderConnectionTestCommand {
            test_id: "ptest_1".to_owned(),
            claim_token: test_claim.claim_token,
            now: now(),
            result: CompleteProviderConnectionTestResult::Succeeded {
                result_hash: json_hash(&test_result),
                result: test_result,
            },
        })
        .await
        .unwrap();

    let validation_document = json!({"valid":true,"errors":[]});
    repository
        .create_provider_validation(CreateProviderValidationCommand {
            metadata: metadata(
                "validation-1",
                "POST",
                "/v1/admin/providers/example/validations",
            ),
            report: ProviderValidationReport {
                validation_id: "pval_1".to_owned(),
                provider_id: "example".to_owned(),
                draft_version: 2,
                provider_input_hash: configured_hash.clone(),
                report_hash: json_hash(&validation_document),
                valid: true,
                document: validation_document,
                created_at: now(),
                created_by: "operator-a".to_owned(),
            },
            expected_draft_version: 2,
            expected_provider_input_hash: configured_hash,
        })
        .await
        .unwrap();

    let revision = json!({
        "adapter_type":"open_ai_compatible",
        "base_url":"https://models.example.test/v1",
        "credential_ref":"secret://providers/example",
        "models":[{"id":"model-a","supports_tools":true}]
    });
    repository
        .publish_provider_revision(PublishProviderRevisionCommand {
            metadata: metadata("publish-1", "POST", "/v1/admin/providers/example/revisions"),
            revision_id: "prev_1".to_owned(),
            provider_id: "example".to_owned(),
            expected_draft_version: 2,
            validation_id: "pval_1".to_owned(),
            discovery_id: Some("pdisc_1".to_owned()),
            connection_test_id: Some("ptest_1".to_owned()),
            revision_hash: json_hash(&revision),
            document: revision,
        })
        .await
        .unwrap();
    let activation_a = repository.activate_provider_revision(ActivateProviderRevisionCommand {
        metadata: metadata("activate-1", "POST", "/v1/admin/providers/example/activate"),
        provider_id: "example".to_owned(),
        revision_id: "prev_1".to_owned(),
        expected_provider_version: 1,
    });
    let activation_b = repository.activate_provider_revision(ActivateProviderRevisionCommand {
        metadata: metadata("activate-2", "POST", "/v1/admin/providers/example/activate"),
        provider_id: "example".to_owned(),
        revision_id: "prev_1".to_owned(),
        expected_provider_version: 1,
    });
    let (activation_a, activation_b) = tokio::join!(activation_a, activation_b);
    assert_eq!(
        usize::from(activation_a.is_ok()) + usize::from(activation_b.is_ok()),
        1
    );
    for rejected in [activation_a, activation_b]
        .into_iter()
        .filter_map(Result::err)
    {
        assert!(matches!(
            rejected,
            ProviderManagementWriteError::Conflict(ProviderManagementConflict::PreconditionFailed)
        ));
    }
    assert_eq!(
        repository
            .load_active_provider_revisions()
            .await
            .unwrap()
            .len(),
        1
    );

    let plan = admission_plan();
    repository.install_versioned_plan(&plan).await.unwrap();
    let admitted_run = RunId::new("run_provider_fence_active").unwrap();
    let admitted = repository
        .create_run(
            TransitionKey::derive("provider.management.test", &["active-admission"]).unwrap(),
            CreateRunCommand::new(admitted_run, &plan, json!({}))
                .unwrap()
                .with_expected_provider_fences(BTreeMap::from([("example".to_owned(), 0)]))
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(matches!(admitted, TransitionOutcome::Committed { .. }));

    let raced_run = RunId::new("run_provider_fence_race").unwrap();
    let race_admission = repository.create_run(
        TransitionKey::derive("provider.management.test", &["raced-admission"]).unwrap(),
        CreateRunCommand::new(raced_run.clone(), &plan, json!({}))
            .unwrap()
            .with_expected_provider_fences(BTreeMap::from([("example".to_owned(), 0)]))
            .unwrap(),
    );
    let race_suspension = repository.suspend_provider(SuspendProviderCommand {
        metadata: metadata("suspend-1", "POST", "/v1/admin/providers/example/suspend"),
        provider_id: "example".to_owned(),
        expected_provider_version: 2,
        reason_code: "operator_request".to_owned(),
    });
    let (race_admission, race_suspension) = tokio::join!(race_admission, race_suspension);
    race_suspension.unwrap();
    let race_admission = race_admission.unwrap();
    assert!(matches!(
        &race_admission,
        TransitionOutcome::Committed { .. } | TransitionOutcome::StateConflict
    ));
    assert_eq!(
        repository.load_run(&raced_run).await.unwrap().is_some(),
        matches!(&race_admission, TransitionOutcome::Committed { .. }),
        "raced admission must be all-or-nothing on its linearization side"
    );
    let fence = repository
        .load_provider_fence("example")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(fence.suspension_fence, 1);
    assert!(repository
        .load_active_provider_revisions()
        .await
        .unwrap()
        .is_empty());
    let rejected_run = RunId::new("run_provider_fence_suspended").unwrap();
    let rejected = repository
        .create_run(
            TransitionKey::derive("provider.management.test", &["suspended-admission"]).unwrap(),
            CreateRunCommand::new(rejected_run.clone(), &plan, json!({}))
                .unwrap()
                .with_expected_provider_fences(BTreeMap::from([("example".to_owned(), 0)]))
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(matches!(rejected, TransitionOutcome::StateConflict));
    assert!(repository.load_run(&rejected_run).await.unwrap().is_none());

    repository
        .resume_provider(ResumeProviderCommand {
            metadata: metadata("resume-1", "POST", "/v1/admin/providers/example/resume"),
            provider_id: "example".to_owned(),
            expected_provider_version: 3,
        })
        .await
        .unwrap();
    assert_eq!(
        repository
            .load_active_provider_revisions()
            .await
            .unwrap()
            .len(),
        1
    );
    repository
        .retire_provider(RetireProviderCommand {
            metadata: metadata("retire-1", "POST", "/v1/admin/providers/example/retirement"),
            provider_id: "example".to_owned(),
            expected_provider_version: 4,
            reason_code: "decommissioned".to_owned(),
        })
        .await
        .unwrap();
    let retired = repository.get_provider("example").await.unwrap().unwrap();
    assert_eq!(
        retired.operational_state,
        insight_durable::ProviderOperationalState::Retired
    );
    assert!(retired.active_revision_id.is_none());
    assert!(repository
        .get_provider_revision("example", "prev_1")
        .await
        .unwrap()
        .is_some());
    assert!(matches!(
        repository
            .resume_provider(ResumeProviderCommand {
                metadata: metadata(
                    "resume-after-retire",
                    "DELETE",
                    "/v1/admin/providers/example/suspension",
                ),
                provider_id: "example".to_owned(),
                expected_provider_version: 5,
            })
            .await,
        Err(ProviderManagementWriteError::Conflict(
            ProviderManagementConflict::ForbiddenState
        ))
    ));
}

async fn isolated_postgres_repository(
) -> Option<(PostgresDurableRepository, PgPool, PgPool, String)> {
    let database_url = std::env::var("TEST_POSTGRES_URL").ok()?;
    let schema = format!("provider_management_{}", Uuid::new_v4().simple());
    let admin = PgPoolOptions::new()
        .max_connections(4)
        .connect(&database_url)
        .await
        .unwrap();
    sqlx::query(AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
        .execute(&admin)
        .await
        .unwrap();
    let separator = if database_url.contains('?') { '&' } else { '?' };
    let scoped_url = format!("{database_url}{separator}options=-csearch_path%3D{schema}");
    let control = PgPoolOptions::new()
        .max_connections(4)
        .connect(&scoped_url)
        .await
        .unwrap();
    support::provision_postgres_schema(&control).await;
    let repository = PostgresDurableRepository::connect(&scoped_url)
        .await
        .unwrap();
    Some((repository, control, admin, schema))
}

#[tokio::test]
async fn sqlite_provider_lifecycle_is_idempotent_evidence_bound_and_fenced() {
    let (_temporary, repository): (_, SqliteDurableRepository) =
        support::temporary_sqlite_repository().await;
    exercise_provider_lifecycle(&repository).await;
}

#[tokio::test]
async fn sqlite_provider_mutation_fault_rolls_back_state_receipt_audit_and_outbox() {
    let (temporary, repository): (_, SqliteDurableRepository) =
        support::temporary_sqlite_repository().await;
    let database_url = format!(
        "sqlite://{}",
        temporary.path().join("durable.sqlite3").display()
    );
    let control = sqlx::SqlitePool::connect(&database_url).await.unwrap();
    sqlx::query(
        "CREATE TRIGGER inject_provider_audit_failure BEFORE INSERT ON provider_management_audit_events
         BEGIN SELECT RAISE(ABORT,'injected Provider audit failure'); END",
    )
    .execute(&control)
    .await
    .unwrap();
    let draft = json!({"models":[]});
    assert!(matches!(
        repository
            .create_provider(CreateProviderCommand {
                metadata: metadata("fault-provider", "POST", "/v1/admin/providers"),
                provider_id: "fault-provider".to_owned(),
                display_name: "Fault Provider".to_owned(),
                adapter_type: "open_ai_compatible".to_owned(),
                provider_input_hash: json_hash(&draft),
                draft_document: draft,
            })
            .await,
        Err(ProviderManagementWriteError::Repository(_))
    ));
    for table in [
        "managed_providers",
        "provider_drafts",
        "provider_management_requests",
        "provider_management_audit_events",
        "provider_management_outbox",
    ] {
        let count: i64 = sqlx::query_scalar(AssertSqlSafe(format!("SELECT COUNT(*) FROM {table}")))
            .fetch_one(&control)
            .await
            .unwrap();
        assert_eq!(count, 0, "fault left a partial row in {table}");
    }
    control.close().await;
}

#[tokio::test]
async fn sqlite_provider_management_outbox_is_bounded_inside_the_mutation_transaction() {
    let (temporary, repository): (_, SqliteDurableRepository) =
        support::temporary_sqlite_repository().await;
    let database_url = format!(
        "sqlite://{}",
        temporary.path().join("durable.sqlite3").display()
    );
    let control = sqlx::SqlitePool::connect(&database_url).await.unwrap();
    sqlx::query(
        "WITH RECURSIVE seq(value) AS (
             SELECT 1 UNION ALL SELECT value + 1 FROM seq WHERE value < 4096
         )
         INSERT INTO provider_management_outbox(
             event_id,event_kind,provider_id,subject_id,safe_payload,created_at,delivered_at
         )
         SELECT printf('seed-%05d',value),'provider.seed','seed-provider','seed-subject','{}',
                '2025-01-01T00:00:00.000Z',NULL FROM seq",
    )
    .execute(&control)
    .await
    .unwrap();
    let draft = json!({"models":[]});
    repository
        .create_provider(CreateProviderCommand {
            metadata: metadata("bounded-provider", "POST", "/v1/admin/providers"),
            provider_id: "bounded-provider".to_owned(),
            display_name: "Bounded Provider".to_owned(),
            adapter_type: "open_ai_compatible".to_owned(),
            provider_input_hash: json_hash(&draft),
            draft_document: draft,
        })
        .await
        .unwrap();
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM provider_management_outbox")
        .fetch_one(&control)
        .await
        .unwrap();
    assert_eq!(count, 4096);
    let retained_mutation: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM provider_management_outbox WHERE event_kind='provider.created'",
    )
    .fetch_one(&control)
    .await
    .unwrap();
    assert_eq!(retained_mutation, 1);
    control.close().await;
}

#[tokio::test]
async fn postgres_provider_lifecycle_is_idempotent_evidence_bound_and_fenced() {
    let Some((repository, control, admin, schema)) = isolated_postgres_repository().await else {
        assert!(
            std::env::var_os("CI").is_none(),
            "CI must set TEST_POSTGRES_URL for PostgreSQL Provider management conformance"
        );
        return;
    };
    let mut notifications = repository
        .open_provider_management_notification_stream()
        .await
        .unwrap()
        .expect("PostgreSQL must expose Provider management LISTEN hints");
    exercise_provider_lifecycle(&repository).await;
    // Hints are deliberately opaque. Receiving multiple indistinguishable
    // messages proves consumers cannot depend on object identity or order and
    // must reload the final durable state after every wakeup.
    for _ in 0..2 {
        tokio::time::timeout(std::time::Duration::from_secs(2), notifications.recv())
            .await
            .expect("Provider management notification was not delivered")
            .unwrap();
    }
    drop(repository);
    drop(control);
    sqlx::query(AssertSqlSafe(format!("DROP SCHEMA {schema} CASCADE")))
        .execute(&admin)
        .await
        .unwrap();
    admin.close().await;
}

#[tokio::test]
async fn postgres_provider_management_outbox_is_bounded_under_concurrent_mutations() {
    let Some((repository, control, admin, schema)) = isolated_postgres_repository().await else {
        assert!(
            std::env::var_os("CI").is_none(),
            "CI must set TEST_POSTGRES_URL for PostgreSQL Provider outbox conformance"
        );
        return;
    };
    sqlx::query(
        "INSERT INTO provider_management_outbox(
             event_id,event_kind,provider_id,subject_id,safe_payload,created_at,delivered_at
         )
         SELECT 'seed-' || lpad(value::text,5,'0'),'provider.seed','seed-provider',
                'seed-subject','{}'::jsonb,'2025-01-01T00:00:00Z'::timestamptz,NULL
           FROM generate_series(1,4096) AS value",
    )
    .execute(&control)
    .await
    .unwrap();
    let make = |suffix: &'static str| {
        let draft = json!({"models":[]});
        repository.create_provider(CreateProviderCommand {
            metadata: metadata(
                &format!("bounded-provider-{suffix}"),
                "POST",
                "/v1/admin/providers",
            ),
            provider_id: format!("bounded-provider-{suffix}"),
            display_name: format!("Bounded Provider {suffix}"),
            adapter_type: "open_ai_compatible".to_owned(),
            provider_input_hash: json_hash(&draft),
            draft_document: draft,
        })
    };
    let (first, second) = tokio::join!(make("a"), make("b"));
    first.unwrap();
    second.unwrap();
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM provider_management_outbox")
        .fetch_one(&control)
        .await
        .unwrap();
    assert_eq!(count, 4096);
    let retained_mutations: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM provider_management_outbox WHERE event_kind='provider.created'",
    )
    .fetch_one(&control)
    .await
    .unwrap();
    assert_eq!(retained_mutations, 2);
    drop(repository);
    drop(control);
    sqlx::query(AssertSqlSafe(format!("DROP SCHEMA {schema} CASCADE")))
        .execute(&admin)
        .await
        .unwrap();
    admin.close().await;
}
