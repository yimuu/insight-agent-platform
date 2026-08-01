mod support;

use std::collections::BTreeMap;

use chrono::{Duration, Utc};
use insight_dsl::{compile_source, CompileOptions};
use insight_durable::{
    ActivateMcpRevisionCommand, CancelMcpDiscoveryCommand, ClaimMcpDiscoveriesCommand,
    CompleteMcpDiscoveryCommand, CompleteMcpDiscoveryResult, CreateMcpDiscoveryCommand,
    CreateMcpServerCommand, CreateMcpValidationCommand, CreateRunCommand, DisableMcpServerCommand,
    DurableRepository, McpManagedServerState, McpManagementConflict,
    McpManagementDurableRepository, McpManagementWriteError, McpMutationMetadata,
    McpValidationReport, PublishMcpRevisionCommand, ReplaceMcpDraftCommand, VersionedPlan,
};
use insight_engine::{
    DefinitionRevisionId, DeploymentRevisionId, RunId, TransitionKey, TransitionOutcome,
};
use insight_storage::{PostgresDurableRepository, SqliteDurableRepository};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use sqlx::{postgres::PgPoolOptions, AssertSqlSafe, PgPool};
use uuid::Uuid;

fn hash(label: &str) -> String {
    let mut value = String::from("sha256:");
    for byte in Sha256::digest(label.as_bytes()) {
        use std::fmt::Write as _;
        write!(&mut value, "{byte:02x}").unwrap();
    }
    value
}

fn json_hash(value: &Value) -> String {
    let bytes = serde_jcs::to_vec(value).unwrap();
    hash(std::str::from_utf8(&bytes).unwrap())
}

fn now() -> chrono::DateTime<Utc> {
    "2026-07-31T08:00:00Z".parse().unwrap()
}

fn metadata(request_id: &str, method: &str, path: &str, request_hash: &str) -> McpMutationMetadata {
    McpMutationMetadata {
        operator_id: "operator-a".to_owned(),
        method: method.to_owned(),
        canonical_path: path.to_owned(),
        request_id: request_id.to_owned(),
        request_hash: hash(request_hash),
        now: now(),
    }
}

fn draft(endpoint: &str, imports: Value) -> Value {
    json!({
        "transport":{"type":"streamable_http","endpoint":endpoint},
        "discovery":{"type":"live_service_account"},
        "authorization":{"type":"none"},
        "protocol":{"preferred":"2026-07-28","legacy_fallback":[]},
        "imports":imports,
        "limits":{"max_request_bytes":1024,"max_response_bytes":4096,"max_sse_line_bytes":256,"max_sse_event_bytes":1024,"max_content_items":16,"max_catalog_items":32}
    })
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
            DefinitionRevisionId::new("mcp_admission_definition_v1").unwrap(),
            "mcp-admission.yaml",
            source,
        ),
    )
    .unwrap();
    VersionedPlan::from_verified_plan(
        "mcp_admission_definition",
        "mcp_admission_agent",
        "MCP admission fence fixture",
        DeploymentRevisionId::new("mcp_admission_deployment_v1").unwrap(),
        "expression-3.0.0",
        json!({"author":"structured"}),
        &plan,
        json!({}),
        json!([]),
        json!([]),
    )
    .unwrap()
}

async fn exercise_management_lifecycle<R>(repository: &R)
where
    R: McpManagementDurableRepository + DurableRepository + ?Sized,
{
    let initial = draft(
        "https://mcp.example.test/mcp",
        json!({"tools":[],"resources":{"allow":[]},"prompts":[]}),
    );
    let create = CreateMcpServerCommand {
        metadata: metadata("create-1", "POST", "/v1/admin/mcp/servers", "sha256:create"),
        server_id: "engineering".to_owned(),
        display_name: "Engineering MCP".to_owned(),
        draft_document: initial.clone(),
        discovery_input_hash: hash("input-a"),
    };
    let created = repository.create_mcp_server(create.clone()).await.unwrap();
    assert_eq!(created.status, 201);
    assert_eq!(created.etag.as_deref(), Some("\"server-1\""));
    let replayed = repository.create_mcp_server(create).await.unwrap();
    assert!(replayed.replayed);

    let reused = repository
        .create_mcp_server(CreateMcpServerCommand {
            metadata: metadata(
                "create-1",
                "POST",
                "/v1/admin/mcp/servers",
                "sha256:different",
            ),
            server_id: "other".to_owned(),
            display_name: "Other".to_owned(),
            draft_document: initial,
            discovery_input_hash: hash("input-a"),
        })
        .await
        .unwrap_err();
    assert!(matches!(
        reused,
        McpManagementWriteError::Conflict(McpManagementConflict::IdempotencyKeyReused)
    ));

    let imported = draft(
        "https://mcp.example.test/mcp",
        json!({
            "tools":[{"remote":"search","candidate_schema_hash":hash("schema"),"as":"engineering_search"}],
            "resources":{"allow":[]},"prompts":[]
        }),
    );
    let replaced = repository
        .replace_mcp_draft(ReplaceMcpDraftCommand {
            metadata: metadata(
                "draft-1",
                "PUT",
                "/v1/admin/mcp/servers/engineering/draft",
                "sha256:draft",
            ),
            server_id: "engineering".to_owned(),
            expected_draft_version: 1,
            draft_document: imported.clone(),
            discovery_input_hash: hash("input-a"),
        })
        .await
        .unwrap();
    assert_eq!(replaced.etag.as_deref(), Some("\"draft-2\""));

    let discovery_id = "mdisc_fixture".to_owned();
    repository
        .create_mcp_discovery(CreateMcpDiscoveryCommand {
            metadata: metadata(
                "discover-1",
                "POST",
                "/v1/admin/mcp/servers/engineering/discoveries",
                "sha256:empty",
            ),
            discovery_id: discovery_id.clone(),
            server_id: "engineering".to_owned(),
            expected_draft_version: 2,
            discovery_input_hash: hash("input-a"),
            max_pending_discoveries: 2,
        })
        .await
        .unwrap();
    let pending_stats = repository
        .load_mcp_management_runtime_stats()
        .await
        .unwrap();
    assert_eq!(pending_stats.pending_discoveries, 1);
    assert_eq!(pending_stats.running_discoveries, 0);
    assert_eq!(pending_stats.oldest_open_discovery_at, Some(now()));
    let claim = repository
        .claim_mcp_discoveries(ClaimMcpDiscoveriesCommand {
            worker_id: "worker-a".to_owned(),
            now: now(),
            lease_expires_at: now() + Duration::seconds(30),
            limit: 1,
        })
        .await
        .unwrap()
        .pop()
        .unwrap();
    let running_stats = repository
        .load_mcp_management_runtime_stats()
        .await
        .unwrap();
    assert_eq!(running_stats.pending_discoveries, 0);
    assert_eq!(running_stats.running_discoveries, 1);
    assert_eq!(claim.draft_document, imported);
    let snapshot_document = json!({"tools":[{"remote":"search","schema_hash":hash("schema")}],"resources":[],"prompts":[]});
    let forged_snapshot = repository
        .complete_mcp_discovery(CompleteMcpDiscoveryCommand {
            discovery_id: discovery_id.clone(),
            claim_token: claim.claim_token.clone(),
            now: now(),
            result: CompleteMcpDiscoveryResult::Succeeded {
                catalog_fingerprint: hash("forged-catalog"),
                snapshot_document: snapshot_document.clone(),
            },
        })
        .await
        .unwrap_err();
    assert!(matches!(
        forged_snapshot,
        McpManagementWriteError::Conflict(McpManagementConflict::ValidationFailed)
    ));
    repository
        .complete_mcp_discovery(CompleteMcpDiscoveryCommand {
            discovery_id: discovery_id.clone(),
            claim_token: claim.claim_token,
            now: now(),
            result: CompleteMcpDiscoveryResult::Succeeded {
                catalog_fingerprint: json_hash(&snapshot_document),
                snapshot_document,
            },
        })
        .await
        .unwrap();
    let completed_stats = repository
        .load_mcp_management_runtime_stats()
        .await
        .unwrap();
    assert_eq!(completed_stats.pending_discoveries, 0);
    assert_eq!(completed_stats.running_discoveries, 0);
    assert_eq!(completed_stats.oldest_open_discovery_at, None);

    let report_document = json!({"valid":true,"errors":[],"warnings":[]});
    let report = McpValidationReport {
        validation_id: "mval_fixture".to_owned(),
        server_id: "engineering".to_owned(),
        draft_version: 2,
        discovery_id: discovery_id.clone(),
        report_hash: json_hash(&report_document),
        valid: true,
        document: report_document,
        created_at: now(),
        created_by: "operator-a".to_owned(),
    };
    let mut forged_report = report.clone();
    forged_report.report_hash = hash("forged-report");
    let forged_validation = repository
        .create_mcp_validation(CreateMcpValidationCommand {
            metadata: metadata(
                "validate-forged",
                "POST",
                "/v1/admin/mcp/servers/engineering/validations",
                "sha256:validate-forged",
            ),
            report: forged_report,
            expected_draft_version: 2,
            expected_discovery_input_hash: hash("input-a"),
        })
        .await
        .unwrap_err();
    assert!(matches!(
        forged_validation,
        McpManagementWriteError::Conflict(McpManagementConflict::ValidationFailed)
    ));
    repository
        .create_mcp_validation(CreateMcpValidationCommand {
            metadata: metadata(
                "validate-1",
                "POST",
                "/v1/admin/mcp/servers/engineering/validations",
                "sha256:validate",
            ),
            report,
            expected_draft_version: 2,
            expected_discovery_input_hash: hash("input-a"),
        })
        .await
        .unwrap();

    let revision_id = "mrev_fixture".to_owned();
    let revision_document = json!({"contract":"mcp-server-revision/v1","bindings":{"tools":[]}});
    let forged_revision = repository
        .publish_mcp_revision(PublishMcpRevisionCommand {
            metadata: metadata(
                "publish-forged",
                "POST",
                "/v1/admin/mcp/servers/engineering/revisions",
                "sha256:publish-forged",
            ),
            revision_id: "mrev_forged".to_owned(),
            server_id: "engineering".to_owned(),
            expected_draft_version: 2,
            discovery_id: discovery_id.clone(),
            validation_id: "mval_fixture".to_owned(),
            revision_hash: hash("forged-revision"),
            document: revision_document.clone(),
        })
        .await
        .unwrap_err();
    assert!(matches!(
        forged_revision,
        McpManagementWriteError::Conflict(McpManagementConflict::ValidationFailed)
    ));
    repository
        .publish_mcp_revision(PublishMcpRevisionCommand {
            metadata: metadata(
                "publish-1",
                "POST",
                "/v1/admin/mcp/servers/engineering/revisions",
                "sha256:publish",
            ),
            revision_id: revision_id.clone(),
            server_id: "engineering".to_owned(),
            expected_draft_version: 2,
            discovery_id,
            validation_id: "mval_fixture".to_owned(),
            revision_hash: json_hash(&revision_document),
            document: revision_document,
        })
        .await
        .unwrap();
    let revision = repository
        .get_mcp_revision("engineering", &revision_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(revision.revision_number, 1);

    repository
        .activate_mcp_revision(ActivateMcpRevisionCommand {
            metadata: metadata(
                "activate-1",
                "PUT",
                "/v1/admin/mcp/servers/engineering/active-revision",
                "sha256:activate",
            ),
            server_id: "engineering".to_owned(),
            revision_id: revision_id.clone(),
            expected_server_version: 1,
            readiness_hash: hash("ready"),
            readiness_expires_at: now() + Duration::seconds(30),
        })
        .await
        .unwrap();
    let active = repository.load_active_mcp_revisions().await.unwrap();
    assert_eq!(active.len(), 1);
    assert_eq!(active[0].1.revision_id, revision_id);
    let active_stats = repository
        .load_mcp_management_runtime_stats()
        .await
        .unwrap();
    assert_eq!(active_stats.active_servers, 1);
    assert_eq!(active_stats.disabled_servers, 0);

    let plan = admission_plan();
    repository.install_versioned_plan(&plan).await.unwrap();
    let admitted_run = RunId::new("run_mcp_fence_active").unwrap();
    let admitted = repository
        .create_run(
            TransitionKey::derive("mcp.management.test", &["active-admission"]).unwrap(),
            CreateRunCommand::new(admitted_run, &plan, json!({}))
                .unwrap()
                .with_expected_mcp_server_fences(BTreeMap::from([("engineering".to_owned(), 0)]))
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(matches!(admitted, TransitionOutcome::Committed { .. }));

    repository
        .disable_mcp_server(DisableMcpServerCommand {
            metadata: metadata(
                "disable-1",
                "DELETE",
                "/v1/admin/mcp/servers/engineering/active-revision",
                "sha256:disable",
            ),
            server_id: "engineering".to_owned(),
            expected_server_version: 2,
        })
        .await
        .unwrap();
    let fence = repository
        .load_mcp_server_fence("engineering")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(fence.state, McpManagedServerState::Disabled);
    assert_eq!(fence.disable_fence, 1);
    let disabled_stats = repository
        .load_mcp_management_runtime_stats()
        .await
        .unwrap();
    assert_eq!(disabled_stats.active_servers, 0);
    assert_eq!(disabled_stats.disabled_servers, 1);
    let rejected_run = RunId::new("run_mcp_fence_disabled").unwrap();
    let rejected = repository
        .create_run(
            TransitionKey::derive("mcp.management.test", &["disabled-admission"]).unwrap(),
            CreateRunCommand::new(rejected_run.clone(), &plan, json!({}))
                .unwrap()
                .with_expected_mcp_server_fences(BTreeMap::from([("engineering".to_owned(), 0)]))
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(matches!(rejected, TransitionOutcome::StateConflict));
    assert!(repository.load_run(&rejected_run).await.unwrap().is_none());
    assert!(repository
        .load_active_mcp_revisions()
        .await
        .unwrap()
        .is_empty());
}

async fn exercise_terminal_discovery_retention<R>(repository: &R)
where
    R: McpManagementDurableRepository + ?Sized,
{
    repository
        .create_mcp_server(CreateMcpServerCommand {
            metadata: metadata(
                "retention-create",
                "POST",
                "/v1/admin/mcp/servers",
                "retention-create",
            ),
            server_id: "retention".to_owned(),
            display_name: "Retention".to_owned(),
            draft_document: draft(
                "https://retention.example.test/mcp",
                json!({"tools":[],"resources":{"allow":[]},"prompts":[]}),
            ),
            discovery_input_hash: hash("retention-input"),
        })
        .await
        .unwrap();
    repository
        .create_mcp_discovery(CreateMcpDiscoveryCommand {
            metadata: metadata(
                "retention-discover",
                "POST",
                "/v1/admin/mcp/servers/retention/discoveries",
                "retention-discover",
            ),
            discovery_id: "mdisc_retention".to_owned(),
            server_id: "retention".to_owned(),
            expected_draft_version: 1,
            discovery_input_hash: hash("retention-input"),
            max_pending_discoveries: 1,
        })
        .await
        .unwrap();
    repository
        .cancel_mcp_discovery(CancelMcpDiscoveryCommand {
            metadata: metadata(
                "retention-cancel",
                "DELETE",
                "/v1/admin/mcp/servers/retention/discoveries/mdisc_retention",
                "retention-cancel",
            ),
            server_id: "retention".to_owned(),
            discovery_id: "mdisc_retention".to_owned(),
        })
        .await
        .unwrap();
    assert_eq!(
        repository
            .cleanup_terminal_mcp_discoveries(
                now() + Duration::seconds(1),
                now() + Duration::seconds(2),
                16,
            )
            .await
            .unwrap(),
        1
    );
    assert!(repository
        .get_mcp_discovery("retention", "mdisc_retention")
        .await
        .unwrap()
        .is_none());
}

async fn exercise_discovery_lease_takeover<R>(repository: &R)
where
    R: McpManagementDurableRepository + ?Sized,
{
    repository
        .create_mcp_server(CreateMcpServerCommand {
            metadata: metadata(
                "takeover-create",
                "POST",
                "/v1/admin/mcp/servers",
                "takeover-create",
            ),
            server_id: "takeover".to_owned(),
            display_name: "Takeover".to_owned(),
            draft_document: draft(
                "https://takeover.example.test/mcp",
                json!({"tools":[],"resources":{"allow":[]},"prompts":[]}),
            ),
            discovery_input_hash: hash("takeover-input"),
        })
        .await
        .unwrap();
    repository
        .create_mcp_discovery(CreateMcpDiscoveryCommand {
            metadata: metadata(
                "takeover-discover",
                "POST",
                "/v1/admin/mcp/servers/takeover/discoveries",
                "takeover-discover",
            ),
            discovery_id: "mdisc_takeover".to_owned(),
            server_id: "takeover".to_owned(),
            expected_draft_version: 1,
            discovery_input_hash: hash("takeover-input"),
            max_pending_discoveries: 1,
        })
        .await
        .unwrap();
    let first = repository
        .claim_mcp_discoveries(ClaimMcpDiscoveriesCommand {
            worker_id: "worker-before-crash".to_owned(),
            now: now(),
            lease_expires_at: now() + Duration::seconds(1),
            limit: 1,
        })
        .await
        .unwrap()
        .pop()
        .unwrap();
    let second = repository
        .claim_mcp_discoveries(ClaimMcpDiscoveriesCommand {
            worker_id: "worker-after-crash".to_owned(),
            now: now() + Duration::seconds(2),
            lease_expires_at: now() + Duration::seconds(32),
            limit: 1,
        })
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(second.operation.discovery_id, first.operation.discovery_id);
    assert_eq!(second.operation.attempts, 2);
    assert_ne!(second.claim_token, first.claim_token);

    let stale_completion = repository
        .complete_mcp_discovery(CompleteMcpDiscoveryCommand {
            discovery_id: first.operation.discovery_id,
            claim_token: first.claim_token,
            now: now() + Duration::seconds(3),
            result: CompleteMcpDiscoveryResult::Failed(insight_durable::McpDiscoveryFailure {
                code: "MCP_DISCOVERY_REMOTE_ERROR".to_owned(),
                stage: "crashed_worker".to_owned(),
                retryable: true,
                correlation_id: None,
            }),
        })
        .await
        .unwrap_err();
    assert!(matches!(
        stale_completion,
        McpManagementWriteError::Conflict(McpManagementConflict::FenceLost)
    ));

    repository
        .cancel_mcp_discovery(CancelMcpDiscoveryCommand {
            metadata: metadata(
                "takeover-cancel",
                "DELETE",
                "/v1/admin/mcp/servers/takeover/discoveries/mdisc_takeover",
                "takeover-cancel",
            ),
            server_id: "takeover".to_owned(),
            discovery_id: "mdisc_takeover".to_owned(),
        })
        .await
        .unwrap();
    repository
        .complete_mcp_discovery(CompleteMcpDiscoveryCommand {
            discovery_id: "mdisc_takeover".to_owned(),
            claim_token: second.claim_token.clone(),
            now: now() + Duration::seconds(4),
            result: CompleteMcpDiscoveryResult::Failed(insight_durable::McpDiscoveryFailure {
                code: "MCP_DISCOVERY_REMOTE_ERROR".to_owned(),
                stage: "cancel_race".to_owned(),
                retryable: true,
                correlation_id: None,
            }),
        })
        .await
        .unwrap();
    let terminal = repository
        .get_mcp_discovery("takeover", "mdisc_takeover")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        terminal.status,
        insight_durable::McpDiscoveryStatus::Cancelled
    );
    assert!(terminal.failure.is_none());
    let duplicate_completion = repository
        .complete_mcp_discovery(CompleteMcpDiscoveryCommand {
            discovery_id: "mdisc_takeover".to_owned(),
            claim_token: second.claim_token,
            now: now() + Duration::seconds(5),
            result: CompleteMcpDiscoveryResult::Cancelled,
        })
        .await
        .unwrap_err();
    assert!(matches!(
        duplicate_completion,
        McpManagementWriteError::Conflict(McpManagementConflict::FenceLost)
    ));
}

async fn isolated_postgres_repository(
) -> Option<(PostgresDurableRepository, PgPool, PgPool, String)> {
    let database_url = std::env::var("TEST_POSTGRES_URL").ok()?;
    let schema = format!("mcp_management_{}", Uuid::new_v4().simple());
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
async fn sqlite_management_lifecycle_is_cas_idempotent_and_revision_immutable() {
    let (_temporary, repository): (_, SqliteDurableRepository) =
        support::temporary_sqlite_repository().await;
    exercise_management_lifecycle(&repository).await;
    exercise_terminal_discovery_retention(&repository).await;
    exercise_discovery_lease_takeover(&repository).await;
}

#[tokio::test]
async fn postgres_management_lifecycle_is_cas_idempotent_and_revision_immutable() {
    let Some((repository, control, admin, schema)) = isolated_postgres_repository().await else {
        assert!(
            std::env::var_os("CI").is_none(),
            "CI must set TEST_POSTGRES_URL for PostgreSQL MCP management conformance"
        );
        return;
    };
    exercise_management_lifecycle(&repository).await;
    exercise_terminal_discovery_retention(&repository).await;
    exercise_discovery_lease_takeover(&repository).await;
    drop(repository);
    drop(control);
    sqlx::query(AssertSqlSafe(format!("DROP SCHEMA {schema} CASCADE")))
        .execute(&admin)
        .await
        .unwrap();
    admin.close().await;
}

#[tokio::test]
async fn discovery_input_change_marks_successful_snapshot_stale_but_import_only_change_does_not() {
    let (_temporary, repository) = support::temporary_sqlite_repository().await;
    let document = draft(
        "https://one.example.test/mcp",
        json!({"tools":[],"resources":{"allow":[]},"prompts":[]}),
    );
    repository
        .create_mcp_server(CreateMcpServerCommand {
            metadata: metadata("create", "POST", "/v1/admin/mcp/servers", "sha256:create"),
            server_id: "one".to_owned(),
            display_name: "One".to_owned(),
            draft_document: document.clone(),
            discovery_input_hash: hash("input-one"),
        })
        .await
        .unwrap();
    repository
        .create_mcp_discovery(CreateMcpDiscoveryCommand {
            metadata: metadata(
                "discover",
                "POST",
                "/v1/admin/mcp/servers/one/discoveries",
                "sha256:discover",
            ),
            discovery_id: "mdisc_one".to_owned(),
            server_id: "one".to_owned(),
            expected_draft_version: 1,
            discovery_input_hash: hash("input-one"),
            max_pending_discoveries: 1,
        })
        .await
        .unwrap();
    let claim = repository
        .claim_mcp_discoveries(ClaimMcpDiscoveriesCommand {
            worker_id: "worker".to_owned(),
            now: now(),
            lease_expires_at: now() + Duration::seconds(30),
            limit: 1,
        })
        .await
        .unwrap()
        .pop()
        .unwrap();
    let snapshot_document = json!({"tools":[],"resources":[],"prompts":[]});
    repository
        .complete_mcp_discovery(CompleteMcpDiscoveryCommand {
            discovery_id: "mdisc_one".to_owned(),
            claim_token: claim.claim_token,
            now: now(),
            result: CompleteMcpDiscoveryResult::Succeeded {
                catalog_fingerprint: json_hash(&snapshot_document),
                snapshot_document,
            },
        })
        .await
        .unwrap();

    repository
        .replace_mcp_draft(ReplaceMcpDraftCommand {
            metadata: metadata(
                "imports",
                "PUT",
                "/v1/admin/mcp/servers/one/draft/imports/tools",
                "sha256:imports",
            ),
            server_id: "one".to_owned(),
            expected_draft_version: 1,
            draft_document: draft(
                "https://one.example.test/mcp",
                json!({"tools":[{"remote":"x"}],"resources":{"allow":[]},"prompts":[]}),
            ),
            discovery_input_hash: hash("input-one"),
        })
        .await
        .unwrap();
    assert!(
        !repository
            .get_mcp_discovery("one", "mdisc_one")
            .await
            .unwrap()
            .unwrap()
            .stale
    );

    repository
        .replace_mcp_draft(ReplaceMcpDraftCommand {
            metadata: metadata(
                "endpoint",
                "PUT",
                "/v1/admin/mcp/servers/one/draft",
                "sha256:endpoint",
            ),
            server_id: "one".to_owned(),
            expected_draft_version: 2,
            draft_document: draft(
                "https://two.example.test/mcp",
                json!({"tools":[],"resources":{"allow":[]},"prompts":[]}),
            ),
            discovery_input_hash: hash("input-two"),
        })
        .await
        .unwrap();
    let operation = repository
        .get_mcp_discovery("one", "mdisc_one")
        .await
        .unwrap()
        .unwrap();
    assert!(operation.stale);
    assert_eq!(
        operation.stale_reason.as_deref(),
        Some("draft_discovery_input_changed")
    );
}
