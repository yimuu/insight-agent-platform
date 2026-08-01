mod support;

use std::collections::BTreeSet;

use chrono::{Duration, Utc};
use insight_dsl::{compile_source, CompileOptions};
use insight_durable::{
    ActivateManagedAgentDeploymentCommand, AgentAuthoringMode, AgentDebugSession, AgentDebugStatus,
    AgentLifecycle, AgentManagementConflict, AgentManagementDurableRepository,
    AgentManagementWriteError, AgentMutationMetadata, AgentOperationStatus, AgentValidationReport,
    ArchiveAgentCommand, CancelAgentDebugSessionCommand, CreateAgentCommand,
    CreateAgentDebugSessionCommand, CreateAgentResolutionCommand, CreateAgentValidationCommand,
    CreateRunCommand, DurableRepository, InstallAgentDeploymentCommand,
    PublishAgentDefinitionCommand, RecordAgentManagementRejectionCommand, ReplaceAgentDraftCommand,
    ReplaceAgentDraftViewCommand, RestoreAgentCommand, VersionedPlan,
};
use insight_engine::{
    ContentHash, DefinitionRevisionId, DeploymentRevisionId, RunId, TransitionKey,
    TransitionOutcome,
};
use insight_storage::{PostgresDurableRepository, SqliteDurableRepository};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use sqlx::{postgres::PgPoolOptions, AssertSqlSafe, PgPool};
use uuid::Uuid;

const SOURCE: &str = r#"api_version: insight.agent/v1
kind: agent
metadata:
  id: managed-example
  name: Managed Example
  description: managed lifecycle test
inputs:
  question: string
output: string
workflow:
  steps:
    - return: fixed
"#;

fn hash_bytes(bytes: &[u8]) -> String {
    let mut value = String::from("sha256:");
    for byte in Sha256::digest(bytes) {
        use std::fmt::Write as _;
        write!(&mut value, "{byte:02x}").unwrap();
    }
    value
}

fn json_hash(value: &Value) -> String {
    hash_bytes(&serde_jcs::to_vec(value).unwrap())
}

fn now() -> chrono::DateTime<Utc> {
    "2026-08-01T08:00:00Z".parse().unwrap()
}

fn metadata(id: &str, method: &str, path: &str) -> AgentMutationMetadata {
    AgentMutationMetadata {
        operator_id: "operator-a".to_owned(),
        capability: "agent.write".to_owned(),
        method: method.to_owned(),
        canonical_path: path.to_owned(),
        request_id: id.to_owned(),
        request_hash: hash_bytes(id.as_bytes()),
        now: now(),
    }
}

fn versioned_plan(draft: &Value) -> VersionedPlan {
    let definition_revision_id = DefinitionRevisionId::new("defrev_managed_example_v1").unwrap();
    let plan = compile_source(
        SOURCE,
        CompileOptions::new(
            definition_revision_id,
            "managed/managed-example/agent.yaml",
            SOURCE,
        ),
    )
    .unwrap();
    VersionedPlan::from_verified_plan(
        "managed-example",
        "managed-example",
        "Managed Example",
        DeploymentRevisionId::new("deployrev_managed_example_v1").unwrap(),
        "cel-test-v1",
        draft.clone(),
        &plan,
        json!([]),
        json!([]),
        json!({"deployment_policy":{"schema_version":1,"persistence_mode":"full","allow_volatile_waits":false,"execution_budget_ms":0}}),
    )
    .unwrap()
}

async fn exercise_agent_management<R>(repository: &R)
where
    R: AgentManagementDurableRepository + insight_durable::DurableRepository + ?Sized,
{
    repository
        .record_agent_management_rejection(RecordAgentManagementRejectionCommand {
            actor_id: "operator-a".to_owned(),
            capability: "agent.write".to_owned(),
            request_id: "agent-rejected-1".to_owned(),
            agent_id: None,
            subject_id: "agent.write".to_owned(),
            result_code: "http_400".to_owned(),
            now: now(),
        })
        .await
        .unwrap();
    let initial = json!({
        "source":{"type":"yaml_package","agent_yaml":SOURCE,"prompt_files":[]}
    });
    let create = CreateAgentCommand {
        metadata: metadata("agent-create-1", "POST", "/v1/admin/agents"),
        agent_id: "managed-example".to_owned(),
        authoring_mode: AgentAuthoringMode::YamlPackage,
        labels: json!({"team":"platform"}),
        author_hash: json_hash(&initial),
        draft_document: initial,
    };
    assert_eq!(
        repository
            .create_agent(create.clone())
            .await
            .unwrap()
            .status,
        201
    );
    assert!(repository.create_agent(create).await.unwrap().replayed);

    let graph_draft = json!({"source":{"type":"graph","document":{"graph":"opaque"}}});
    let graph_author_hash = json_hash(&graph_draft);
    repository
        .create_agent(CreateAgentCommand {
            metadata: metadata("graph-agent-create-1", "POST", "/v1/admin/agents"),
            agent_id: "managed-graph".to_owned(),
            authoring_mode: AgentAuthoringMode::Graph,
            labels: json!({}),
            draft_document: graph_draft,
            author_hash: graph_author_hash.clone(),
        })
        .await
        .unwrap();
    let first_page = repository.list_agents(None, None, 1).await.unwrap();
    assert_eq!(first_page.items.len(), 1);
    let second_page = repository
        .list_agents(None, first_page.next_cursor.as_deref(), 1)
        .await
        .unwrap();
    assert_eq!(second_page.items.len(), 1);
    assert!(second_page.next_cursor.is_none());
    let listed = BTreeSet::from([
        first_page.items[0].agent_id.clone(),
        second_page.items[0].agent_id.clone(),
    ]);
    assert_eq!(
        listed,
        BTreeSet::from(["managed-example".to_owned(), "managed-graph".to_owned()]),
        "composite pagination must not duplicate or omit same-timestamp Agents"
    );
    repository
        .replace_agent_draft_view(ReplaceAgentDraftViewCommand {
            metadata: metadata(
                "graph-view-1",
                "PUT",
                "/v1/admin/agents/managed-graph/draft/view",
            ),
            agent_id: "managed-graph".to_owned(),
            expected_view_version: 0,
            document: json!({"schema_version":1,"viewport":{"zoom":1}}),
        })
        .await
        .unwrap();
    let view = repository
        .get_agent_draft_view("managed-graph")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(view.view_version, 1);
    let unchanged_graph_draft = repository
        .get_agent_draft("managed-graph")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(unchanged_graph_draft.draft_version, 1);
    assert_eq!(unchanged_graph_draft.author_hash, graph_author_hash);
    let graph_race_a = repository.replace_agent_draft(ReplaceAgentDraftCommand {
        metadata: metadata(
            "graph-race-a",
            "PUT",
            "/v1/admin/agents/managed-graph/draft",
        ),
        agent_id: "managed-graph".to_owned(),
        expected_draft_version: 1,
        draft_document: json!({"source":{"type":"graph","document":{"winner":"a"}}}),
        author_hash: hash_bytes(b"graph-race-a"),
    });
    let graph_race_b = repository.replace_agent_draft(ReplaceAgentDraftCommand {
        metadata: metadata(
            "graph-race-b",
            "PUT",
            "/v1/admin/agents/managed-graph/draft",
        ),
        agent_id: "managed-graph".to_owned(),
        expected_draft_version: 1,
        draft_document: json!({"source":{"type":"graph","document":{"winner":"b"}}}),
        author_hash: hash_bytes(b"graph-race-b"),
    });
    let (graph_race_a, graph_race_b) = tokio::join!(graph_race_a, graph_race_b);
    assert_eq!(
        usize::from(graph_race_a.is_ok()) + usize::from(graph_race_b.is_ok()),
        1
    );
    for rejected in [graph_race_a, graph_race_b]
        .into_iter()
        .filter_map(Result::err)
    {
        assert!(matches!(
            rejected,
            AgentManagementWriteError::Conflict(AgentManagementConflict::PreconditionFailed)
        ));
    }

    let draft = json!({
        "source":{"type":"yaml_package","agent_yaml":SOURCE,"prompt_files":[]},
        "note":"ready"
    });
    let author_hash = json_hash(&draft);
    repository
        .replace_agent_draft(ReplaceAgentDraftCommand {
            metadata: metadata(
                "agent-draft-2",
                "PUT",
                "/v1/admin/agents/managed-example/draft",
            ),
            agent_id: "managed-example".to_owned(),
            expected_draft_version: 1,
            draft_document: draft.clone(),
            author_hash: author_hash.clone(),
        })
        .await
        .unwrap();
    let plan = versioned_plan(&draft);
    let validation_document = json!({"diagnostics":[],"valid":true});
    repository
        .create_agent_validation(CreateAgentValidationCommand {
            metadata: metadata(
                "agent-validation-1",
                "POST",
                "/v1/admin/agents/managed-example/validations",
            ),
            expected_draft_version: 2,
            expected_author_hash: author_hash.clone(),
            report: AgentValidationReport {
                validation_id: "agentval_1".to_owned(),
                agent_id: "managed-example".to_owned(),
                draft_version: 2,
                author_hash: author_hash.clone(),
                policy_digest: hash_bytes(b"policy-v1"),
                status: AgentOperationStatus::Succeeded,
                semantic_hash: Some(plan.plan_hash().as_str().to_owned()),
                report_hash: json_hash(&validation_document),
                document: validation_document,
                created_at: now(),
                created_by: "operator-a".to_owned(),
            },
        })
        .await
        .unwrap();
    repository
        .publish_agent_definition(PublishAgentDefinitionCommand {
            metadata: metadata(
                "agent-publish-1",
                "POST",
                "/v1/admin/agents/managed-example/revisions",
            ),
            expected_draft_version: 2,
            validation_id: "agentval_1".to_owned(),
            validation_policy_digest: hash_bytes(b"policy-v1"),
            plan: plan.clone(),
        })
        .await
        .unwrap();
    let catalog = insight_durable::DurableRepository::load_versioned_plan_catalog(repository)
        .await
        .unwrap();
    assert!(
        catalog.plans().is_empty(),
        "Definition publish must not deploy"
    );
    assert!(
        catalog.heads().is_empty(),
        "Definition publish must not activate"
    );

    let resolution = insight_durable::AgentDeploymentResolution {
        resolution_id: "agentres_1".to_owned(),
        agent_id: "managed-example".to_owned(),
        definition_revision_id: plan.definition_revision_id().as_str().to_owned(),
        status: AgentOperationStatus::Succeeded,
        catalog_snapshot_hash: hash_bytes(b"empty-catalog"),
        resolution_hash: hash_bytes(b"resolution-1"),
        resolved_bindings: json!([]),
        worker_contracts: json!({"deployment_policy":{"schema_version":1,"persistence_mode":"full","allow_volatile_waits":false,"execution_budget_ms":0}}),
        dependency_heads: json!([]),
        risks: json!([]),
        expires_at: now() + Duration::minutes(10),
        created_at: now(),
        created_by: "operator-a".to_owned(),
    };
    repository
        .create_agent_deployment_resolution(CreateAgentResolutionCommand {
            metadata: metadata(
                "agent-resolution-1",
                "POST",
                "/v1/admin/agents/managed-example/deployment-resolutions",
            ),
            resolution: resolution.clone(),
        })
        .await
        .unwrap();
    repository
        .install_agent_deployment(InstallAgentDeploymentCommand {
            metadata: metadata(
                "agent-deploy-1",
                "POST",
                "/v1/admin/agents/managed-example/deployments",
            ),
            resolution_id: resolution.resolution_id,
            expected_resolution_hash: resolution.resolution_hash,
            expected_dependency_heads: json!([]),
            plan: plan.clone(),
        })
        .await
        .unwrap();
    let catalog = insight_durable::DurableRepository::load_versioned_plan_catalog(repository)
        .await
        .unwrap();
    assert_eq!(catalog.plans(), std::slice::from_ref(&plan));
    assert!(
        catalog.heads().is_empty(),
        "Deployment install must not activate"
    );

    let activation_a =
        repository.activate_managed_agent_deployment(ActivateManagedAgentDeploymentCommand {
            metadata: metadata(
                "agent-activate-1",
                "PUT",
                "/v1/admin/agents/managed-example/active-deployment",
            ),
            agent_id: "managed-example".to_owned(),
            expected_entity_version: 1,
            deployment_revision_id: plan.deployment_revision_id().as_str().to_owned(),
        });
    let activation_b =
        repository.activate_managed_agent_deployment(ActivateManagedAgentDeploymentCommand {
            metadata: metadata(
                "agent-activate-2",
                "PUT",
                "/v1/admin/agents/managed-example/active-deployment",
            ),
            agent_id: "managed-example".to_owned(),
            expected_entity_version: 1,
            deployment_revision_id: plan.deployment_revision_id().as_str().to_owned(),
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
            AgentManagementWriteError::Conflict(AgentManagementConflict::PreconditionFailed)
        ));
    }
    let active = repository
        .get_agent("managed-example")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        active.active_deployment_revision_id.as_deref(),
        Some(plan.deployment_revision_id().as_str())
    );

    let head = DurableRepository::load_versioned_plan_catalog(repository)
        .await
        .unwrap()
        .heads()[0]
        .clone();
    let raced_run = RunId::new("run_agent_archive_race").unwrap();
    let race_admission = repository.create_run(
        TransitionKey::derive("agent.management.test", &["archive-raced-admission"]).unwrap(),
        CreateRunCommand::new(raced_run.clone(), &plan, json!({"question":"race"}))
            .unwrap()
            .with_expected_publication_head(head.clone())
            .unwrap(),
    );
    let race_archive = repository.archive_agent(ArchiveAgentCommand {
        metadata: metadata(
            "agent-archive-1",
            "POST",
            "/v1/admin/agents/managed-example/archive",
        ),
        agent_id: "managed-example".to_owned(),
        expected_entity_version: 2,
    });
    let (race_admission, race_archive) = tokio::join!(race_admission, race_archive);
    race_archive.unwrap();
    let race_admission = race_admission.unwrap();
    assert!(matches!(
        &race_admission,
        TransitionOutcome::Committed { .. } | TransitionOutcome::StateConflict
    ));
    assert_eq!(
        repository.load_run(&raced_run).await.unwrap().is_some(),
        matches!(&race_admission, TransitionOutcome::Committed { .. }),
        "archive/admission race must linearize without a partial Run"
    );
    let rejected_run = RunId::new("run_agent_after_archive").unwrap();
    let rejected = repository
        .create_run(
            TransitionKey::derive("agent.management.test", &["after-archive"]).unwrap(),
            CreateRunCommand::new(rejected_run.clone(), &plan, json!({"question":"after"}))
                .unwrap()
                .with_expected_publication_head(head)
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(matches!(rejected, TransitionOutcome::StateConflict));
    assert!(repository.load_run(&rejected_run).await.unwrap().is_none());
    let archived = repository
        .get_agent("managed-example")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(archived.lifecycle, AgentLifecycle::Archived);
    assert!(archived.active_deployment_revision_id.is_none());
    assert!(archived.archived_publication_head.is_some());

    repository
        .restore_agent(RestoreAgentCommand {
            metadata: metadata(
                "agent-restore-1",
                "POST",
                "/v1/admin/agents/managed-example/restore",
            ),
            agent_id: "managed-example".to_owned(),
            expected_entity_version: 3,
        })
        .await
        .unwrap();
    let restored = repository
        .get_agent("managed-example")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(restored.lifecycle, AgentLifecycle::Editable);
    assert!(restored.active_deployment_revision_id.is_none());

    let debug = AgentDebugSession {
        debug_session_id: "agentdebug_1".to_owned(),
        agent_id: "managed-example".to_owned(),
        source: json!({"type":"deployment","deployment_revision_id":plan.deployment_revision_id()}),
        source_hash: hash_bytes(b"debug-source"),
        execution_profile_id: "author-sandbox".to_owned(),
        profile_mode: "sandbox".to_owned(),
        status: AgentDebugStatus::Queued,
        definition_revision_id: None,
        deployment_revision_id: None,
        run_id: None,
        failure_code: None,
        expires_at: now() + Duration::minutes(5),
        created_at: now(),
        finished_at: None,
        created_by: "operator-a".to_owned(),
    };
    repository
        .create_agent_debug_session(CreateAgentDebugSessionCommand {
            metadata: metadata(
                "agent-debug-1",
                "POST",
                "/v1/admin/agents/managed-example/debug-sessions",
            ),
            session: debug,
            max_active_sessions: 2,
            retain_until: now() + Duration::minutes(5),
        })
        .await
        .unwrap();
    repository
        .cancel_agent_debug_session(CancelAgentDebugSessionCommand {
            metadata: metadata(
                "agent-debug-cancel-1",
                "DELETE",
                "/v1/admin/agents/managed-example/debug-sessions/agentdebug_1",
            ),
            agent_id: "managed-example".to_owned(),
            debug_session_id: "agentdebug_1".to_owned(),
        })
        .await
        .unwrap();
    assert_eq!(
        repository
            .get_agent_debug_session("managed-example", "agentdebug_1")
            .await
            .unwrap()
            .unwrap()
            .status,
        AgentDebugStatus::Cancelled
    );
    assert_eq!(
        repository
            .cleanup_expired_agent_debug_sessions(now() + Duration::minutes(6), 10)
            .await
            .unwrap(),
        1
    );
    let redacted = repository
        .get_agent_debug_session("managed-example", "agentdebug_1")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(redacted.source, json!({"content_deleted":true}));
    assert_eq!(redacted.source_hash, hash_bytes(b"debug-source"));
    let replay = repository
        .replay_agent_mutation(&metadata(
            "agent-debug-1",
            "POST",
            "/v1/admin/agents/managed-example/debug-sessions",
        ))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(replay.response["source"], json!({"content_deleted":true}));

    let delete = repository
        .delete_agent(insight_durable::DeleteAgentCommand {
            metadata: metadata(
                "agent-delete-1",
                "DELETE",
                "/v1/admin/agents/managed-example",
            ),
            agent_id: "managed-example".to_owned(),
            expected_entity_version: 4,
        })
        .await
        .unwrap_err();
    assert!(matches!(
        delete,
        AgentManagementWriteError::Conflict(AgentManagementConflict::Referenced)
    ));
    assert_ne!(plan.binding_hash(), &ContentHash::from_bytes(b"mutable"));
}

async fn isolated_postgres_repository(
) -> Option<(PostgresDurableRepository, PgPool, PgPool, String)> {
    let database_url = std::env::var("TEST_POSTGRES_URL").ok()?;
    let schema = format!("agent_management_{}", Uuid::new_v4().simple());
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
async fn sqlite_agent_management_separates_author_publish_deploy_and_activation() {
    let (_temporary, repository): (_, SqliteDurableRepository) =
        support::temporary_sqlite_repository().await;
    exercise_agent_management(&repository).await;
}

#[tokio::test]
async fn sqlite_agent_mutation_fault_rolls_back_state_receipt_audit_and_outbox() {
    let (temporary, repository): (_, SqliteDurableRepository) =
        support::temporary_sqlite_repository().await;
    let database_url = format!(
        "sqlite://{}",
        temporary.path().join("durable.sqlite3").display()
    );
    let control = sqlx::SqlitePool::connect(&database_url).await.unwrap();
    sqlx::query(
        "CREATE TRIGGER inject_agent_audit_failure BEFORE INSERT ON agent_management_audit_events
         BEGIN SELECT RAISE(ABORT,'injected agent audit failure'); END",
    )
    .execute(&control)
    .await
    .unwrap();
    let draft = json!({"source":{"type":"graph","document":{}}});
    assert!(matches!(
        repository
            .create_agent(CreateAgentCommand {
                metadata: metadata("fault-agent", "POST", "/v1/admin/agents"),
                agent_id: "fault-agent".to_owned(),
                authoring_mode: AgentAuthoringMode::Graph,
                labels: json!({}),
                author_hash: json_hash(&draft),
                draft_document: draft,
            })
            .await,
        Err(AgentManagementWriteError::Repository(_))
    ));
    for table in [
        "managed_agents",
        "agent_drafts",
        "agent_management_requests",
        "agent_management_audit_events",
        "agent_management_outbox",
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
async fn postgres_agent_management_separates_author_publish_deploy_and_activation() {
    let Some((repository, control, admin, schema)) = isolated_postgres_repository().await else {
        assert!(
            std::env::var_os("CI").is_none(),
            "CI must set TEST_POSTGRES_URL for PostgreSQL Agent management conformance"
        );
        return;
    };
    exercise_agent_management(&repository).await;
    drop(repository);
    drop(control);
    sqlx::query(AssertSqlSafe(format!("DROP SCHEMA {schema} CASCADE")))
        .execute(&admin)
        .await
        .unwrap();
    admin.close().await;
}
