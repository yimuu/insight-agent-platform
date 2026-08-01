#[path = "support/database.rs"]
mod database;

use std::{path::Path, process::Command};

use insight_agent_platform::catalog::{compile_agent_dir, freeze_managed_agent_definition};
use insight_agent_platform::resources::{
    models::{ModelRegistry, ModelSelector},
    provider_management::{ProviderManagementRuntime, ProviderManagementRuntimeConfig},
};
use insight_durable::{CreateRunCommand, DurableRepository, ProviderManagementDurableRepository};
use insight_engine::{RunId, TransitionKey};
use insight_storage::{PostgresDurableRepository, SqliteDurableRepository};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use sqlx::{postgres::PgPoolOptions, sqlite::SqlitePoolOptions, AssertSqlSafe, Row};
use uuid::Uuid;

fn database_url(path: &Path) -> String {
    format!("sqlite://{}", path.display())
}

async fn legacy_fixture(
    path: &Path,
) -> (
    SqliteDurableRepository,
    insight_durable::VersionedPlan,
    RunId,
) {
    database::provision_sqlite_database(path).await;
    let repository = SqliteDurableRepository::connect_path(path).await.unwrap();
    let published =
        compile_agent_dir(&Path::new(env!("CARGO_MANIFEST_DIR")).join("agents/action_demo"))
            .unwrap();
    let plan = freeze_managed_agent_definition(&published).unwrap();
    repository
        .publish_builtin_versioned_plan(&plan)
        .await
        .unwrap();
    let run_id = RunId::new("run_migration_history_fidelity").unwrap();
    repository
        .create_run(
            TransitionKey::derive("migration.test", &["historical-run"]).unwrap(),
            CreateRunCommand::new(run_id.clone(), &plan, json!({"text":"preserve me"})).unwrap(),
        )
        .await
        .unwrap();
    (repository, plan, run_id)
}

fn run_migration(database_url: &str, extra: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_management-migrate"))
        .args(["adopt-agent-heads", "--database-url", database_url])
        .args(extra)
        .output()
        .unwrap()
}

fn hash_value(value: &Value) -> String {
    let mut output = String::from("sha256:");
    for byte in Sha256::digest(serde_jcs::to_vec(value).unwrap()) {
        use std::fmt::Write as _;
        write!(&mut output, "{byte:02x}").unwrap();
    }
    output
}

fn run_provider_mapping(
    database_url: &str,
    provider_id: &str,
    revision_id: &str,
    extra: &[&str],
) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_management-migrate"))
        .args([
            "map-provider-history",
            "--database-url",
            database_url,
            "--provider-id",
            provider_id,
            "--revision-id",
            revision_id,
        ])
        .args(extra)
        .output()
        .unwrap()
}

#[tokio::test]
async fn sqlite_provider_history_mapping_preserves_deployment_and_installs_immutable_alias() {
    let temporary = tempfile::tempdir().unwrap();
    let path = temporary.path().join("provider-history.sqlite3");
    let (repository, plan, _) = legacy_fixture(&path).await;
    let url = database_url(&path);
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect(&url)
        .await
        .unwrap();
    let now = chrono::Utc::now();
    let provider_id = "legacy-provider";
    let revision_id = "prev_legacy_provider";
    let model_id = "legacy-model";
    let provider_hash = hash_value(&json!({"provider":"legacy"}));
    let revision_document = json!({
        "provider_id":provider_id,
        "adapter":{"type":"open_ai_compatible","version":insight_resources::openai_chat::OPENAI_CHAT_ADAPTER_VERSION},
        "endpoint":"https://legacy.example.test/v1",
        "credential":{"type":"none","reference":null,"reference_hash":null},
        "transport":{"tls":"required","redirects":"deny","connect_timeout_ms":1000,"request_timeout_ms":1000},
        "models":[{"id":model_id,"input":["text"],"capabilities":["complete","streaming"],"provenance":{"type":"operator_asserted"}}],
        "policy_fingerprint":"test-policy",
        "source":{"migration":true}
    });
    let revision_hash = hash_value(&revision_document);
    let model_document = revision_document["models"][0].clone();
    let model_hash = hash_value(&model_document);
    sqlx::query(
        "INSERT INTO managed_providers(provider_id,display_name,adapter_type,operational_state,
           provider_version,draft_version,active_revision_id,suspension_fence,created_at,updated_at)
         VALUES(?,?,'open_ai_compatible','enabled',1,1,NULL,0,?,?)",
    )
    .bind(provider_id)
    .bind("Legacy Provider")
    .bind(now)
    .bind(now)
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO provider_drafts(provider_id,draft_version,provider_input_hash,document,created_at,updated_at)
         VALUES(?,1,?,'{}',?,?)",
    )
    .bind(provider_id)
    .bind(&provider_hash)
    .bind(now)
    .bind(now)
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO provider_validation_reports(validation_id,provider_id,draft_version,
           provider_input_hash,report_hash,valid,document,created_at,created_by)
         VALUES('pval_legacy',?,1,?,?,1,'{}',?,'migration-test')",
    )
    .bind(provider_id)
    .bind(&provider_hash)
    .bind(hash_value(&json!({"valid":true})))
    .bind(now)
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO provider_revisions(revision_id,provider_id,revision_number,source_draft_version,
           validation_id,discovery_id,connection_test_id,revision_hash,document,created_at,created_by)
         VALUES(?,?,1,1,'pval_legacy',NULL,NULL,?,?,?,'migration-test')",
    )
    .bind(revision_id)
    .bind(provider_id)
    .bind(&revision_hash)
    .bind(serde_json::to_string(&revision_document).unwrap())
    .bind(now)
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO provider_revision_models(revision_id,ordinal,model_id,capability_hash,document)
         VALUES(?,0,?,?,?)",
    )
    .bind(revision_id)
    .bind(model_id)
    .bind(&model_hash)
    .bind(serde_json::to_string(&model_document).unwrap())
    .execute(&pool)
    .await
    .unwrap();

    let legacy_evidence =
        json!({"adapter":"open_ai_chat","endpoint_identity":"legacy-v1","model_id":model_id});
    let legacy_hash = hash_value(&legacy_evidence);
    let bindings = json!([{"node_id":"legacy_llm","binding":{
        "adapter":"core.llm","provider_route":provider_id,"model_id":model_id,
        "model_binding_hash":legacy_hash.clone(),"model_binding":legacy_evidence.clone(),
        "request_mode":"complete","request_capabilities":["complete"],"tool_choice":"auto",
        "tool_limits":{"max_rounds":1,"max_calls":1},"tools":[]
    }}]);
    let historical_deployment = "deployrev_legacy_provider_alias";
    sqlx::query(
        "INSERT INTO deployment_revisions(definition_id,definition_revision_id,deployment_revision_id,
           plan_hash,binding_hash,resolved_bindings,worker_contracts,created_at)
         SELECT definition_id,definition_revision_id,?,plan_hash,?,?,worker_contracts,?
         FROM deployment_revisions WHERE deployment_revision_id=?",
    )
    .bind(historical_deployment)
    .bind(hash_value(&json!({"legacy-deployment":true})))
    .bind(serde_json::to_string(&bindings).unwrap())
    .bind(now)
    .bind(plan.deployment_revision_id().as_str())
    .execute(&pool)
    .await
    .unwrap();
    let before: String = sqlx::query_scalar(
        "SELECT resolved_bindings FROM deployment_revisions WHERE deployment_revision_id=?",
    )
    .bind(historical_deployment)
    .fetch_one(&pool)
    .await
    .unwrap();

    let dry = run_provider_mapping(&url, provider_id, revision_id, &["--dry-run"]);
    assert!(
        dry.status.success(),
        "{}",
        String::from_utf8_lossy(&dry.stderr)
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM provider_revision_legacy_model_bindings"
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        0
    );
    let applied = run_provider_mapping(&url, provider_id, revision_id, &[]);
    assert!(
        applied.status.success(),
        "{}",
        String::from_utf8_lossy(&applied.stderr)
    );
    let report: Value = serde_json::from_slice(&applied.stdout).unwrap();
    assert_eq!(report["legacy_bindings_mapped"], 1);
    let aliases = repository
        .load_provider_legacy_model_bindings(revision_id)
        .await
        .unwrap();
    assert_eq!(aliases.len(), 1);
    assert_eq!(aliases[0].legacy_binding_hash, legacy_hash);
    assert_eq!(aliases[0].legacy_binding_evidence, legacy_evidence);
    let models = ModelRegistry::default();
    let mut runtime = ProviderManagementRuntime::start(
        std::sync::Arc::new(repository.clone()),
        models.clone(),
        ProviderManagementRuntimeConfig {
            enabled: true,
            workers: 1,
            poll_interval: std::time::Duration::from_millis(20),
            lease_duration: std::time::Duration::from_secs(30),
            retention: chrono::Duration::days(1),
            max_response_bytes: 1024 * 1024,
            allow_loopback_development: false,
        },
    )
    .await
    .unwrap();
    assert!(
        models
            .resolve_versioned(
                &ModelSelector::new(provider_id, model_id).unwrap(),
                &legacy_hash,
            )
            .is_ok(),
        "runtime restart must recover the exact pre-cutover model binding hash"
    );
    runtime.shutdown().await;
    let after: String = sqlx::query_scalar(
        "SELECT resolved_bindings FROM deployment_revisions WHERE deployment_revision_id=?",
    )
    .bind(historical_deployment)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        after, before,
        "mapping must not rewrite historical resolved bindings"
    );
    pool.close().await;
}

#[tokio::test]
async fn sqlite_migration_dry_run_rolls_back_and_apply_preserves_exact_run_pins() {
    let temporary = tempfile::tempdir().unwrap();
    let path = temporary.path().join("history.sqlite3");
    let (_repository, plan, run_id) = legacy_fixture(&path).await;
    let url = database_url(&path);
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect(&url)
        .await
        .unwrap();
    let before = sqlx::query(
        "SELECT r.definition_revision_id,r.deployment_revision_id,r.input_payload_id,p.content_hash
         FROM workflow_runs r JOIN payloads p
           ON p.run_id=r.run_id AND p.payload_id=r.input_payload_id
         WHERE r.run_id=?",
    )
    .bind(run_id.as_str())
    .fetch_one(&pool)
    .await
    .unwrap();
    let before: (String, String, String, String) = (
        before.try_get("definition_revision_id").unwrap(),
        before.try_get("deployment_revision_id").unwrap(),
        before.try_get("input_payload_id").unwrap(),
        before.try_get("content_hash").unwrap(),
    );

    let dry_run = run_migration(&url, &["--dry-run"]);
    assert!(
        dry_run.status.success(),
        "{}",
        String::from_utf8_lossy(&dry_run.stderr)
    );
    let dry_report: Value = serde_json::from_slice(&dry_run.stdout).unwrap();
    assert_eq!(dry_report["agents_adopted"], 1);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM managed_agents")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0,
        "dry-run must roll back every management row"
    );

    let applied = run_migration(&url, &[]);
    assert!(
        applied.status.success(),
        "{}",
        String::from_utf8_lossy(&applied.stderr)
    );
    let report: Value = serde_json::from_slice(&applied.stdout).unwrap();
    assert_eq!(report["agents_adopted"], 1);
    assert_eq!(report["definitions_preserved"], 1);
    assert_eq!(report["deployments_preserved"], 1);
    let managed = sqlx::query(
        "SELECT lifecycle,active_definition_revision_id,active_deployment_revision_id
         FROM managed_agents WHERE agent_id='action_demo'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        managed.try_get::<String, _>("lifecycle").unwrap(),
        "editable"
    );
    assert_eq!(
        managed
            .try_get::<String, _>("active_definition_revision_id")
            .unwrap(),
        plan.definition_revision_id().as_str()
    );
    assert_eq!(
        managed
            .try_get::<String, _>("active_deployment_revision_id")
            .unwrap(),
        plan.deployment_revision_id().as_str()
    );
    let after = sqlx::query(
        "SELECT r.definition_revision_id,r.deployment_revision_id,r.input_payload_id,p.content_hash
         FROM workflow_runs r JOIN payloads p
           ON p.run_id=r.run_id AND p.payload_id=r.input_payload_id
         WHERE r.run_id=?",
    )
    .bind(run_id.as_str())
    .fetch_one(&pool)
    .await
    .unwrap();
    let after: (String, String, String, String) = (
        after.try_get("definition_revision_id").unwrap(),
        after.try_get("deployment_revision_id").unwrap(),
        after.try_get("input_payload_id").unwrap(),
        after.try_get("content_hash").unwrap(),
    );
    assert_eq!(
        after, before,
        "migration must not rewrite historical Run pins or intent"
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT publication_origin FROM agent_publication_heads WHERE agent_id='action_demo'",
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        "managed"
    );

    let replay = run_migration(&url, &[]);
    assert!(replay.status.success());
    let replay: Value = serde_json::from_slice(&replay.stdout).unwrap();
    assert_eq!(replay["agents_adopted"], 0);
    pool.close().await;
}

#[tokio::test]
async fn sqlite_migration_failure_rolls_back_the_whole_batch() {
    let temporary = tempfile::tempdir().unwrap();
    let path = temporary.path().join("rollback.sqlite3");
    let (repository, plan, _) = legacy_fixture(&path).await;
    // A malformed historical row is inserted as pre-management legacy data;
    // the migration must fail before committing the otherwise-valid Agent.
    let url = database_url(&path);
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect(&url)
        .await
        .unwrap();
    let now = chrono::Utc::now();
    sqlx::query(
        "INSERT INTO workflow_definitions(definition_id,agent_id,created_at,updated_at)
         VALUES('broken_import','broken_import',?,?)",
    )
    .bind(now)
    .bind(now)
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO workflow_definition_revisions(
           definition_id,definition_revision_id,revision_status,author_document,canonical_plan,
           plan_hash,compiler_version,expression_engine_version,descriptor_contracts,created_at,published_at)
         SELECT 'broken_import','defrev_broken_import','published','{\"missing\":true}',canonical_plan,
                plan_hash,compiler_version,expression_engine_version,descriptor_contracts,?,?
         FROM workflow_definition_revisions WHERE definition_id=? LIMIT 1",
    )
    .bind(now)
    .bind(now)
    .bind(plan.definition_id())
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO workflow_definition_public_metadata(
           definition_id,definition_revision_id,display_name,public_description)
         VALUES('broken_import','defrev_broken_import','Broken import','')",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO deployment_revisions(
           definition_id,definition_revision_id,deployment_revision_id,plan_hash,binding_hash,
           resolved_bindings,worker_contracts,created_at)
         SELECT 'broken_import','defrev_broken_import','deployrev_broken_import',plan_hash,binding_hash,
                resolved_bindings,worker_contracts,?
         FROM deployment_revisions WHERE definition_id=? LIMIT 1",
    )
    .bind(now)
    .bind(plan.definition_id())
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO agent_publication_heads(
           agent_id,definition_id,definition_revision_id,deployment_revision_id,publication_origin,updated_at)
         VALUES('broken_import','broken_import','defrev_broken_import','deployrev_broken_import','built_in',?)",
    )
    .bind(now)
    .execute(&pool)
    .await
    .unwrap();
    drop(repository);

    let failed = run_migration(&url, &[]);
    assert!(!failed.status.success());
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM managed_agents")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0,
        "one bad historical Agent must roll back the entire migration batch"
    );
    pool.close().await;
}

async fn exercise_postgres_provider_history_mapping(
    pool: &sqlx::PgPool,
    repository: &PostgresDurableRepository,
    scoped_url: &str,
    plan: &insight_durable::VersionedPlan,
) {
    let now = chrono::Utc::now();
    let provider_id = "legacy-provider";
    let revision_id = "prev_legacy_provider";
    let model_id = "legacy-model";
    let provider_hash = hash_value(&json!({"provider":"legacy"}));
    let model_document = json!({"id":model_id,"input":["text"],"capabilities":["complete","streaming"],"provenance":{"type":"operator_asserted"}});
    let revision_document = json!({
        "provider_id":provider_id,"adapter":{"type":"open_ai_compatible","version":insight_resources::openai_chat::OPENAI_CHAT_ADAPTER_VERSION},
        "endpoint":"https://legacy.example.test/v1",
        "credential":{"type":"none","reference":null,"reference_hash":null},
        "transport":{"tls":"required","redirects":"deny","connect_timeout_ms":1000,"request_timeout_ms":1000},
        "models":[model_document.clone()],"policy_fingerprint":"test-policy","source":{"migration":true}
    });
    sqlx::query(
        "INSERT INTO managed_providers(provider_id,display_name,adapter_type,operational_state,
           provider_version,draft_version,active_revision_id,suspension_fence,created_at,updated_at)
         VALUES($1,'Legacy Provider','open_ai_compatible','enabled',1,1,NULL,0,$2,$2)",
    )
    .bind(provider_id)
    .bind(now)
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO provider_drafts(provider_id,draft_version,provider_input_hash,document,created_at,updated_at)
         VALUES($1,1,$2,'{}'::jsonb,$3,$3)",
    ).bind(provider_id).bind(&provider_hash).bind(now).execute(pool).await.unwrap();
    sqlx::query(
        "INSERT INTO provider_validation_reports(validation_id,provider_id,draft_version,
           provider_input_hash,report_hash,valid,document,created_at,created_by)
         VALUES('pval_legacy',$1,1,$2,$3,TRUE,'{}'::jsonb,$4,'migration-test')",
    )
    .bind(provider_id)
    .bind(&provider_hash)
    .bind(hash_value(&json!({"valid":true})))
    .bind(now)
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO provider_revisions(revision_id,provider_id,revision_number,source_draft_version,
           validation_id,discovery_id,connection_test_id,revision_hash,document,created_at,created_by)
         VALUES($1,$2,1,1,'pval_legacy',NULL,NULL,$3,$4,$5,'migration-test')",
    ).bind(revision_id).bind(provider_id).bind(hash_value(&revision_document)).bind(&revision_document).bind(now).execute(pool).await.unwrap();
    sqlx::query(
        "INSERT INTO provider_revision_models(revision_id,ordinal,model_id,capability_hash,document)
         VALUES($1,0,$2,$3,$4)",
    ).bind(revision_id).bind(model_id).bind(hash_value(&model_document)).bind(&model_document).execute(pool).await.unwrap();
    let legacy_evidence =
        json!({"adapter":"open_ai_chat","endpoint_identity":"legacy-v1","model_id":model_id});
    let legacy_hash = hash_value(&legacy_evidence);
    let bindings = json!([{"node_id":"legacy_llm","binding":{
        "adapter":"core.llm","provider_route":provider_id,"model_id":model_id,
        "model_binding_hash":legacy_hash.clone(),"model_binding":legacy_evidence.clone(),
        "request_mode":"complete","request_capabilities":["complete"],"tool_choice":"auto",
        "tool_limits":{"max_rounds":1,"max_calls":1},"tools":[]
    }}]);
    let historical_deployment = "deployrev_pg_legacy_provider_alias";
    sqlx::query(
        "INSERT INTO deployment_revisions(definition_id,definition_revision_id,deployment_revision_id,
           plan_hash,binding_hash,resolved_bindings,worker_contracts,created_at)
         SELECT definition_id,definition_revision_id,$1,plan_hash,$2,$3,worker_contracts,$4
         FROM deployment_revisions WHERE deployment_revision_id=$5",
    ).bind(historical_deployment).bind(hash_value(&json!({"legacy-deployment":true}))).bind(&bindings).bind(now).bind(plan.deployment_revision_id().as_str()).execute(pool).await.unwrap();
    let before: Value = sqlx::query_scalar(
        "SELECT resolved_bindings FROM deployment_revisions WHERE deployment_revision_id=$1",
    )
    .bind(historical_deployment)
    .fetch_one(pool)
    .await
    .unwrap();
    let dry = run_provider_mapping(scoped_url, provider_id, revision_id, &["--dry-run"]);
    assert!(
        dry.status.success(),
        "{}",
        String::from_utf8_lossy(&dry.stderr)
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM provider_revision_legacy_model_bindings"
        )
        .fetch_one(pool)
        .await
        .unwrap(),
        0
    );
    let applied = run_provider_mapping(scoped_url, provider_id, revision_id, &[]);
    assert!(
        applied.status.success(),
        "{}",
        String::from_utf8_lossy(&applied.stderr)
    );
    let aliases = repository
        .load_provider_legacy_model_bindings(revision_id)
        .await
        .unwrap();
    assert_eq!(aliases.len(), 1);
    assert_eq!(aliases[0].legacy_binding_hash, legacy_hash);
    let after: Value = sqlx::query_scalar(
        "SELECT resolved_bindings FROM deployment_revisions WHERE deployment_revision_id=$1",
    )
    .bind(historical_deployment)
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(after, before);
}

#[tokio::test]
async fn postgres_migration_matches_sqlite_and_preserves_historical_run_pins() {
    let Ok(database_url) = std::env::var("TEST_POSTGRES_URL") else {
        return;
    };
    let schema = format!("management_migration_{}", Uuid::new_v4().simple());
    let admin = PgPoolOptions::new()
        .max_connections(2)
        .connect(&database_url)
        .await
        .unwrap();
    sqlx::query(AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
        .execute(&admin)
        .await
        .unwrap();
    let separator = if database_url.contains('?') { '&' } else { '?' };
    let scoped_url = format!("{database_url}{separator}options=-csearch_path%3D{schema}");
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&scoped_url)
        .await
        .unwrap();
    database::provision_postgres_schema(&pool).await;
    let repository = PostgresDurableRepository::connect(&scoped_url)
        .await
        .unwrap();
    let published =
        compile_agent_dir(&Path::new(env!("CARGO_MANIFEST_DIR")).join("agents/action_demo"))
            .unwrap();
    let plan = freeze_managed_agent_definition(&published).unwrap();
    repository
        .publish_builtin_versioned_plan(&plan)
        .await
        .unwrap();
    let run_id = RunId::new("run_postgres_migration_fidelity").unwrap();
    repository
        .create_run(
            TransitionKey::derive("migration.test", &["postgres-history"]).unwrap(),
            CreateRunCommand::new(run_id.clone(), &plan, json!({"text":"preserve me"})).unwrap(),
        )
        .await
        .unwrap();
    let before = sqlx::query(
        "SELECT definition_revision_id,deployment_revision_id,input_payload_id
         FROM workflow_runs WHERE run_id=$1",
    )
    .bind(run_id.as_str())
    .fetch_one(&pool)
    .await
    .unwrap();
    let before: (String, String, String) = (
        before.try_get("definition_revision_id").unwrap(),
        before.try_get("deployment_revision_id").unwrap(),
        before.try_get("input_payload_id").unwrap(),
    );
    let dry_run = run_migration(&scoped_url, &["--dry-run"]);
    assert!(
        dry_run.status.success(),
        "{}",
        String::from_utf8_lossy(&dry_run.stderr)
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM managed_agents")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0
    );
    let applied = run_migration(&scoped_url, &[]);
    assert!(
        applied.status.success(),
        "{}",
        String::from_utf8_lossy(&applied.stderr)
    );
    let report: Value = serde_json::from_slice(&applied.stdout).unwrap();
    assert_eq!(report["backend"], "postgres");
    assert_eq!(report["agents_adopted"], 1);
    let after = sqlx::query(
        "SELECT definition_revision_id,deployment_revision_id,input_payload_id
         FROM workflow_runs WHERE run_id=$1",
    )
    .bind(run_id.as_str())
    .fetch_one(&pool)
    .await
    .unwrap();
    let after: (String, String, String) = (
        after.try_get("definition_revision_id").unwrap(),
        after.try_get("deployment_revision_id").unwrap(),
        after.try_get("input_payload_id").unwrap(),
    );
    assert_eq!(after, before);
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT publication_origin FROM agent_publication_heads WHERE agent_id='action_demo'",
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        "managed"
    );
    exercise_postgres_provider_history_mapping(&pool, &repository, &scoped_url, &plan).await;
    pool.close().await;
    sqlx::query(AssertSqlSafe(format!("DROP SCHEMA {schema} CASCADE")))
        .execute(&admin)
        .await
        .unwrap();
    admin.close().await;
}
