use std::{
    collections::{BTreeMap, BTreeSet},
    env,
    str::FromStr,
};

use chrono::{DateTime, Utc};
use serde::Serialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use sqlx::{postgres::PgPoolOptions, sqlite::SqliteConnectOptions, Row, SqlitePool};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Debug)]
struct Options {
    operation: Operation,
    database_url: String,
    dry_run: bool,
    preserve_active: bool,
    actor: String,
}

#[derive(Debug)]
enum Operation {
    AdoptAgentHeads,
    MapProviderHistory {
        provider_id: String,
        revision_id: String,
    },
}

#[derive(Debug, Clone, Serialize)]
struct AdoptionReport {
    version: u32,
    operation: &'static str,
    backend: &'static str,
    dry_run: bool,
    preserve_active: bool,
    agents_adopted: usize,
    definitions_preserved: usize,
    deployments_preserved: usize,
    agent_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
struct ProviderHistoryReport {
    version: u32,
    operation: &'static str,
    backend: &'static str,
    dry_run: bool,
    provider_id: String,
    revision_id: String,
    deployments_scanned: usize,
    legacy_bindings_mapped: usize,
    binding_hashes: Vec<String>,
}

#[derive(Debug)]
struct Head {
    agent_id: String,
    definition_id: String,
    definition_revision_id: String,
    deployment_revision_id: String,
    origin: String,
    updated_at: DateTime<Utc>,
}

#[derive(Debug)]
struct Definition {
    revision_id: String,
    author_document: Value,
    plan_hash: String,
    created_at: DateTime<Utc>,
}

#[derive(Debug)]
struct Deployment {
    definition_revision_id: String,
    deployment_revision_id: String,
    plan_hash: String,
    binding_hash: String,
    resolved_bindings: Value,
    worker_contracts: Value,
    created_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
struct LegacyModelBinding {
    model_id: String,
    binding_hash: String,
    evidence: Value,
    source_definition_id: String,
    source_deployment_revision_id: String,
}

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("management-migrate: {error}");
        std::process::exit(1);
    }
}

async fn run() -> Result<()> {
    let options = parse_options()?;
    let postgres = options.database_url.starts_with("postgres://")
        || options.database_url.starts_with("postgresql://");
    let report = match (
        &options.operation,
        postgres,
        options.database_url.starts_with("sqlite:"),
    ) {
        (Operation::AdoptAgentHeads, true, _) => {
            serde_json::to_value(adopt_postgres(&options).await?)?
        }
        (Operation::AdoptAgentHeads, false, true) => {
            serde_json::to_value(adopt_sqlite(&options).await?)?
        }
        (
            Operation::MapProviderHistory {
                provider_id,
                revision_id,
            },
            true,
            _,
        ) => serde_json::to_value(
            map_provider_history_postgres(&options, provider_id, revision_id).await?,
        )?,
        (
            Operation::MapProviderHistory {
                provider_id,
                revision_id,
            },
            false,
            true,
        ) => serde_json::to_value(
            map_provider_history_sqlite(&options, provider_id, revision_id).await?,
        )?,
        _ => return Err("database URL must use sqlite:, postgres://, or postgresql://".into()),
    };
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}

fn parse_options() -> Result<Options> {
    let mut args = env::args().skip(1);
    let operation_name = args.next().ok_or_else(usage)?;
    let mut values = BTreeMap::new();
    let mut dry_run = false;
    let mut preserve_active = true;
    while let Some(argument) = args.next() {
        match argument.as_str() {
            "--dry-run" => dry_run = true,
            "--inactive" => preserve_active = false,
            "--database-url" | "--actor" | "--provider-id" | "--revision-id" => {
                values.insert(argument, args.next().ok_or_else(usage)?);
            }
            _ => return Err(usage().into()),
        }
    }
    let actor = values
        .remove("--actor")
        .unwrap_or_else(|| "offline-management-migration".to_owned());
    if actor.is_empty() || actor.len() > 256 {
        return Err("actor must contain 1..=256 bytes".into());
    }
    let operation = match operation_name.as_str() {
        "adopt-agent-heads" => {
            if values.contains_key("--provider-id") || values.contains_key("--revision-id") {
                return Err(usage().into());
            }
            Operation::AdoptAgentHeads
        }
        "map-provider-history" if preserve_active => Operation::MapProviderHistory {
            provider_id: values.remove("--provider-id").ok_or_else(usage)?,
            revision_id: values.remove("--revision-id").ok_or_else(usage)?,
        },
        _ => return Err(usage().into()),
    };
    Ok(Options {
        operation,
        database_url: values.remove("--database-url").ok_or_else(usage)?,
        dry_run,
        preserve_active,
        actor,
    })
}

fn usage() -> String {
    "usage: management-migrate adopt-agent-heads --database-url URL [--dry-run] [--inactive] [--actor ID]\n       management-migrate map-provider-history --database-url URL --provider-id ID --revision-id ID [--dry-run] [--actor ID]".to_owned()
}

async fn adopt_sqlite(options: &Options) -> Result<AdoptionReport> {
    let connect = SqliteConnectOptions::from_str(&options.database_url)?.foreign_keys(true);
    let pool = SqlitePool::connect_with(connect).await?;
    let mut transaction = pool.begin().await?;
    verify_contract_sqlite(&mut transaction).await?;
    let rows = sqlx::query(
        "SELECT h.agent_id,h.definition_id,h.definition_revision_id,h.deployment_revision_id,
                h.publication_origin,h.updated_at
         FROM agent_publication_heads h
         LEFT JOIN managed_agents a ON a.agent_id=h.agent_id
         WHERE a.agent_id IS NULL ORDER BY h.agent_id",
    )
    .fetch_all(&mut *transaction)
    .await?;
    let mut report = AdoptionReport {
        version: 1,
        operation: "adopt_agent_publication_heads",
        backend: "sqlite",
        dry_run: options.dry_run,
        preserve_active: options.preserve_active,
        agents_adopted: 0,
        definitions_preserved: 0,
        deployments_preserved: 0,
        agent_ids: Vec::new(),
    };
    for row in rows {
        let head = Head {
            agent_id: row.try_get("agent_id")?,
            definition_id: row.try_get("definition_id")?,
            definition_revision_id: row.try_get("definition_revision_id")?,
            deployment_revision_id: row.try_get("deployment_revision_id")?,
            origin: row.try_get("publication_origin")?,
            updated_at: row.try_get("updated_at")?,
        };
        let definitions = load_sqlite_definitions(&mut transaction, &head.definition_id).await?;
        let deployments = load_sqlite_deployments(&mut transaction, &head.definition_id).await?;
        adopt_sqlite_agent(&mut transaction, options, &head, &definitions, &deployments).await?;
        report.agents_adopted += 1;
        report.definitions_preserved += definitions.len();
        report.deployments_preserved += deployments.len();
        report.agent_ids.push(head.agent_id);
    }
    if options.dry_run {
        transaction.rollback().await?;
    } else {
        transaction.commit().await?;
    }
    pool.close().await;
    Ok(report)
}

async fn verify_contract_sqlite(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
) -> Result<()> {
    let backend = sqlx::query_scalar::<_, String>(
        "SELECT backend FROM durable_schema_contract WHERE singleton=1",
    )
    .fetch_one(&mut **transaction)
    .await?;
    if backend != "sqlite" {
        return Err("durable schema backend does not match SQLite".into());
    }
    Ok(())
}

async fn load_sqlite_definitions(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    definition_id: &str,
) -> Result<Vec<Definition>> {
    sqlx::query(
        "SELECT definition_revision_id,author_document,plan_hash,created_at
         FROM workflow_definition_revisions
         WHERE definition_id=? AND revision_status='published'
         ORDER BY created_at,definition_revision_id",
    )
    .bind(definition_id)
    .fetch_all(&mut **transaction)
    .await?
    .into_iter()
    .map(|row| {
        Ok(Definition {
            revision_id: row.try_get("definition_revision_id")?,
            author_document: serde_json::from_str(&row.try_get::<String, _>("author_document")?)?,
            plan_hash: row.try_get("plan_hash")?,
            created_at: row.try_get("created_at")?,
        })
    })
    .collect()
}

async fn load_sqlite_deployments(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    definition_id: &str,
) -> Result<Vec<Deployment>> {
    sqlx::query(
        "SELECT definition_revision_id,deployment_revision_id,plan_hash,binding_hash,
                resolved_bindings,worker_contracts,created_at
         FROM deployment_revisions WHERE definition_id=?
         ORDER BY created_at,deployment_revision_id",
    )
    .bind(definition_id)
    .fetch_all(&mut **transaction)
    .await?
    .into_iter()
    .map(|row| {
        Ok(Deployment {
            definition_revision_id: row.try_get("definition_revision_id")?,
            deployment_revision_id: row.try_get("deployment_revision_id")?,
            plan_hash: row.try_get("plan_hash")?,
            binding_hash: row.try_get("binding_hash")?,
            resolved_bindings: serde_json::from_str(
                &row.try_get::<String, _>("resolved_bindings")?,
            )?,
            worker_contracts: serde_json::from_str(&row.try_get::<String, _>("worker_contracts")?)?,
            created_at: row.try_get("created_at")?,
        })
    })
    .collect()
}

async fn adopt_sqlite_agent(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    options: &Options,
    head: &Head,
    definitions: &[Definition],
    deployments: &[Deployment],
) -> Result<()> {
    validate_history(head, definitions, deployments)?;
    let current = definitions
        .iter()
        .find(|revision| revision.revision_id == head.definition_revision_id)
        .ok_or("current Definition Revision is missing")?;
    let (authoring_mode, draft) = migration_draft(&head.origin, &current.author_document)?;
    let draft_hash = canonical_hash(&draft)?;
    let active_definition = options
        .preserve_active
        .then_some(head.definition_revision_id.as_str());
    let active_deployment = options
        .preserve_active
        .then_some(head.deployment_revision_id.as_str());
    let labels = json!({"migration":"legacy_publication_adoption","original_origin":head.origin});
    sqlx::query(
        "INSERT INTO managed_agents(
           agent_id,authoring_mode,labels,lifecycle,entity_version,draft_version,
           active_definition_revision_id,active_deployment_revision_id,
           archived_publication_head,created_at,updated_at)
         VALUES(?,?,?,'editable',1,1,?,?,NULL,?,?)",
    )
    .bind(&head.agent_id)
    .bind(authoring_mode)
    .bind(serde_json::to_string(&labels)?)
    .bind(active_definition)
    .bind(active_deployment)
    .bind(current.created_at)
    .bind(head.updated_at)
    .execute(&mut **transaction)
    .await?;
    sqlx::query(
        "INSERT INTO agent_drafts(agent_id,draft_version,author_hash,document,created_at,updated_at)
         VALUES(?,1,?,?,?,?)",
    )
    .bind(&head.agent_id)
    .bind(&draft_hash)
    .bind(serde_json::to_string(&draft)?)
    .bind(current.created_at)
    .bind(head.updated_at)
    .execute(&mut **transaction)
    .await?;
    sqlx::query(
        "INSERT INTO agent_draft_views(agent_id,view_version,document,updated_at)
         VALUES(?,0,'{}',?)",
    )
    .bind(&head.agent_id)
    .bind(head.updated_at)
    .execute(&mut **transaction)
    .await?;
    for (index, definition) in definitions.iter().enumerate() {
        let validation_id = stable_id("agentval", &head.agent_id, &definition.revision_id);
        let author_hash = canonical_hash(&json!({
            "authoring_mode":authoring_mode,
            "author_document":definition.author_document,
        }))?;
        let report_document = json!({
            "valid":true,
            "diagnostics":[],
            "migration":"historical_plan_preserved_without_recompile",
        });
        let report_hash = canonical_hash(&report_document)?;
        sqlx::query(
            "INSERT INTO agent_validations(
               validation_id,agent_id,draft_version,author_hash,policy_digest,operation_status,
               semantic_hash,report_hash,document,created_at,created_by)
             VALUES(?,?,1,?,?,'succeeded',?,?,?,?,?)",
        )
        .bind(&validation_id)
        .bind(&head.agent_id)
        .bind(&author_hash)
        .bind(canonical_hash(&json!({"policy":"historical-import-v1"}))?)
        .bind(&definition.plan_hash)
        .bind(&report_hash)
        .bind(serde_json::to_string(&report_document)?)
        .bind(definition.created_at)
        .bind(&options.actor)
        .execute(&mut **transaction)
        .await?;
        sqlx::query(
            "INSERT INTO agent_definition_publications(
               agent_id,definition_id,definition_revision_id,revision_number,
               source_draft_version,validation_id,author_hash,created_at,created_by)
             VALUES(?,?,?,?,1,?,?,?,?)",
        )
        .bind(&head.agent_id)
        .bind(&head.definition_id)
        .bind(&definition.revision_id)
        .bind(i64::try_from(index + 1)?)
        .bind(&validation_id)
        .bind(&author_hash)
        .bind(definition.created_at)
        .bind(&options.actor)
        .execute(&mut **transaction)
        .await?;
    }
    for deployment in deployments {
        let resolution_id = stable_id(
            "agentres",
            &head.agent_id,
            &deployment.deployment_revision_id,
        );
        let catalog_hash = canonical_hash(&json!({
            "migration":"historical-exact-binding-v1",
            "binding_hash":deployment.binding_hash,
        }))?;
        let resolution_hash = canonical_hash(&json!({
            "definition_revision_id":deployment.definition_revision_id,
            "deployment_revision_id":deployment.deployment_revision_id,
            "plan_hash":deployment.plan_hash,
            "binding_hash":deployment.binding_hash,
        }))?;
        let dependency_heads = json!({"migration":"immutable_binding_preserved"});
        let risks = json!({"items":["historical_adapter_availability_required"]});
        sqlx::query(
            "INSERT INTO agent_deployment_resolutions(
               resolution_id,agent_id,definition_revision_id,operation_status,
               catalog_snapshot_hash,resolution_hash,resolved_bindings,worker_contracts,
               dependency_heads,risks,expires_at,created_at,created_by)
             VALUES(?,?,?,'succeeded',?,?,?,?,?,?,?,?,?)",
        )
        .bind(&resolution_id)
        .bind(&head.agent_id)
        .bind(&deployment.definition_revision_id)
        .bind(&catalog_hash)
        .bind(&resolution_hash)
        .bind(serde_json::to_string(&deployment.resolved_bindings)?)
        .bind(serde_json::to_string(&deployment.worker_contracts)?)
        .bind(serde_json::to_string(&dependency_heads)?)
        .bind(serde_json::to_string(&risks)?)
        .bind(DateTime::<Utc>::MAX_UTC)
        .bind(deployment.created_at)
        .bind(&options.actor)
        .execute(&mut **transaction)
        .await?;
        sqlx::query(
            "INSERT INTO agent_deployment_publications(
               agent_id,definition_id,definition_revision_id,deployment_revision_id,
               resolution_id,created_at,created_by)
             VALUES(?,?,?,?,?,?,?)",
        )
        .bind(&head.agent_id)
        .bind(&head.definition_id)
        .bind(&deployment.definition_revision_id)
        .bind(&deployment.deployment_revision_id)
        .bind(&resolution_id)
        .bind(deployment.created_at)
        .bind(&options.actor)
        .execute(&mut **transaction)
        .await?;
    }
    finalize_sqlite_adoption(transaction, options, head).await
}

async fn finalize_sqlite_adoption(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    options: &Options,
    head: &Head,
) -> Result<()> {
    if options.preserve_active {
        sqlx::query(
            "UPDATE agent_publication_heads SET publication_origin='managed',updated_at=?
             WHERE agent_id=? AND definition_revision_id=? AND deployment_revision_id=?",
        )
        .bind(head.updated_at)
        .bind(&head.agent_id)
        .bind(&head.definition_revision_id)
        .bind(&head.deployment_revision_id)
        .execute(&mut **transaction)
        .await?;
    } else {
        sqlx::query("DELETE FROM agent_publication_heads WHERE agent_id=?")
            .bind(&head.agent_id)
            .execute(&mut **transaction)
            .await?;
    }
    let request_hash = canonical_hash(&json!({
        "operation":"adopt_agent_publication_head",
        "agent_id":head.agent_id,
    }))?;
    sqlx::query(
        "INSERT INTO agent_management_audit_events(
           event_kind,agent_id,subject_id,actor_id,capability,request_id_hash,before_hash,
           after_hash,result_code,created_at)
         VALUES('agent.migrated',?,?,?,'agent.deploy',?,?,?, 'ok',?)",
    )
    .bind(&head.agent_id)
    .bind(&head.deployment_revision_id)
    .bind(&options.actor)
    .bind(&request_hash)
    .bind(canonical_hash(&json!({"publication_origin":head.origin}))?)
    .bind(canonical_hash(&json!({"publication_origin":"managed"}))?)
    .bind(head.updated_at)
    .execute(&mut **transaction)
    .await?;
    let event_id = stable_id("agentout", &head.agent_id, &head.deployment_revision_id);
    sqlx::query(
        "INSERT INTO agent_management_outbox(
           event_id,event_kind,agent_id,subject_id,safe_payload,created_at,delivered_at)
         VALUES(?,'agent.migrated',?,?,?, ?,NULL)",
    )
    .bind(event_id)
    .bind(&head.agent_id)
    .bind(&head.deployment_revision_id)
    .bind(serde_json::to_string(&json!({
        "entity_version":1,
        "active":options.preserve_active,
    }))?)
    .bind(head.updated_at)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

async fn adopt_postgres(options: &Options) -> Result<AdoptionReport> {
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&options.database_url)
        .await?;
    let mut transaction = pool.begin().await?;
    let backend = sqlx::query_scalar::<_, String>(
        "SELECT backend FROM durable_schema_contract WHERE singleton=1",
    )
    .fetch_one(&mut *transaction)
    .await?;
    if backend != "postgres" {
        return Err("durable schema backend does not match PostgreSQL".into());
    }
    let rows = sqlx::query(
        "SELECT h.agent_id,h.definition_id,h.definition_revision_id,h.deployment_revision_id,
                h.publication_origin,h.updated_at
         FROM agent_publication_heads h
         LEFT JOIN managed_agents a ON a.agent_id=h.agent_id
         WHERE a.agent_id IS NULL ORDER BY h.agent_id FOR UPDATE OF h",
    )
    .fetch_all(&mut *transaction)
    .await?;
    let mut report = AdoptionReport {
        version: 1,
        operation: "adopt_agent_publication_heads",
        backend: "postgres",
        dry_run: options.dry_run,
        preserve_active: options.preserve_active,
        agents_adopted: 0,
        definitions_preserved: 0,
        deployments_preserved: 0,
        agent_ids: Vec::new(),
    };
    for row in rows {
        let head = Head {
            agent_id: row.try_get("agent_id")?,
            definition_id: row.try_get("definition_id")?,
            definition_revision_id: row.try_get("definition_revision_id")?,
            deployment_revision_id: row.try_get("deployment_revision_id")?,
            origin: row.try_get("publication_origin")?,
            updated_at: row.try_get("updated_at")?,
        };
        let definitions = load_postgres_definitions(&mut transaction, &head.definition_id).await?;
        let deployments = load_postgres_deployments(&mut transaction, &head.definition_id).await?;
        adopt_postgres_agent(&mut transaction, options, &head, &definitions, &deployments).await?;
        report.agents_adopted += 1;
        report.definitions_preserved += definitions.len();
        report.deployments_preserved += deployments.len();
        report.agent_ids.push(head.agent_id);
    }
    if options.dry_run {
        transaction.rollback().await?;
    } else {
        transaction.commit().await?;
    }
    pool.close().await;
    Ok(report)
}

async fn load_postgres_definitions(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    definition_id: &str,
) -> Result<Vec<Definition>> {
    sqlx::query(
        "SELECT definition_revision_id,author_document,plan_hash,created_at
         FROM workflow_definition_revisions
         WHERE definition_id=$1 AND revision_status='published'
         ORDER BY created_at,definition_revision_id",
    )
    .bind(definition_id)
    .fetch_all(&mut **transaction)
    .await?
    .into_iter()
    .map(|row| {
        Ok(Definition {
            revision_id: row.try_get("definition_revision_id")?,
            author_document: row.try_get("author_document")?,
            plan_hash: row.try_get("plan_hash")?,
            created_at: row.try_get("created_at")?,
        })
    })
    .collect()
}

async fn load_postgres_deployments(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    definition_id: &str,
) -> Result<Vec<Deployment>> {
    sqlx::query(
        "SELECT definition_revision_id,deployment_revision_id,plan_hash,binding_hash,
                resolved_bindings,worker_contracts,created_at
         FROM deployment_revisions WHERE definition_id=$1
         ORDER BY created_at,deployment_revision_id",
    )
    .bind(definition_id)
    .fetch_all(&mut **transaction)
    .await?
    .into_iter()
    .map(|row| {
        Ok(Deployment {
            definition_revision_id: row.try_get("definition_revision_id")?,
            deployment_revision_id: row.try_get("deployment_revision_id")?,
            plan_hash: row.try_get("plan_hash")?,
            binding_hash: row.try_get("binding_hash")?,
            resolved_bindings: row.try_get("resolved_bindings")?,
            worker_contracts: row.try_get("worker_contracts")?,
            created_at: row.try_get("created_at")?,
        })
    })
    .collect()
}

async fn adopt_postgres_agent(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    options: &Options,
    head: &Head,
    definitions: &[Definition],
    deployments: &[Deployment],
) -> Result<()> {
    validate_history(head, definitions, deployments)?;
    let current = definitions
        .iter()
        .find(|revision| revision.revision_id == head.definition_revision_id)
        .ok_or("current Definition Revision is missing")?;
    let (authoring_mode, draft) = migration_draft(&head.origin, &current.author_document)?;
    let draft_hash = canonical_hash(&draft)?;
    let active_definition = options
        .preserve_active
        .then_some(head.definition_revision_id.as_str());
    let active_deployment = options
        .preserve_active
        .then_some(head.deployment_revision_id.as_str());
    let labels = json!({"migration":"legacy_publication_adoption","original_origin":head.origin});
    sqlx::query(
        "INSERT INTO managed_agents(
           agent_id,authoring_mode,labels,lifecycle,entity_version,draft_version,
           active_definition_revision_id,active_deployment_revision_id,
           archived_publication_head,created_at,updated_at)
         VALUES($1,$2,$3,'editable',1,1,$4,$5,NULL,$6,$7)",
    )
    .bind(&head.agent_id)
    .bind(authoring_mode)
    .bind(&labels)
    .bind(active_definition)
    .bind(active_deployment)
    .bind(current.created_at)
    .bind(head.updated_at)
    .execute(&mut **transaction)
    .await?;
    sqlx::query(
        "INSERT INTO agent_drafts(agent_id,draft_version,author_hash,document,created_at,updated_at)
         VALUES($1,1,$2,$3,$4,$5)",
    )
    .bind(&head.agent_id)
    .bind(&draft_hash)
    .bind(&draft)
    .bind(current.created_at)
    .bind(head.updated_at)
    .execute(&mut **transaction)
    .await?;
    sqlx::query(
        "INSERT INTO agent_draft_views(agent_id,view_version,document,updated_at)
         VALUES($1,0,'{}'::jsonb,$2)",
    )
    .bind(&head.agent_id)
    .bind(head.updated_at)
    .execute(&mut **transaction)
    .await?;
    for (index, definition) in definitions.iter().enumerate() {
        let validation_id = stable_id("agentval", &head.agent_id, &definition.revision_id);
        let author_hash = canonical_hash(&json!({
            "authoring_mode":authoring_mode,
            "author_document":definition.author_document,
        }))?;
        let report_document = json!({
            "valid":true,"diagnostics":[],
            "migration":"historical_plan_preserved_without_recompile",
        });
        let report_hash = canonical_hash(&report_document)?;
        sqlx::query(
            "INSERT INTO agent_validations(
               validation_id,agent_id,draft_version,author_hash,policy_digest,operation_status,
               semantic_hash,report_hash,document,created_at,created_by)
             VALUES($1,$2,1,$3,$4,'succeeded',$5,$6,$7,$8,$9)",
        )
        .bind(&validation_id)
        .bind(&head.agent_id)
        .bind(&author_hash)
        .bind(canonical_hash(&json!({"policy":"historical-import-v1"}))?)
        .bind(&definition.plan_hash)
        .bind(&report_hash)
        .bind(&report_document)
        .bind(definition.created_at)
        .bind(&options.actor)
        .execute(&mut **transaction)
        .await?;
        sqlx::query(
            "INSERT INTO agent_definition_publications(
               agent_id,definition_id,definition_revision_id,revision_number,
               source_draft_version,validation_id,author_hash,created_at,created_by)
             VALUES($1,$2,$3,$4,1,$5,$6,$7,$8)",
        )
        .bind(&head.agent_id)
        .bind(&head.definition_id)
        .bind(&definition.revision_id)
        .bind(i64::try_from(index + 1)?)
        .bind(&validation_id)
        .bind(&author_hash)
        .bind(definition.created_at)
        .bind(&options.actor)
        .execute(&mut **transaction)
        .await?;
    }
    for deployment in deployments {
        let resolution_id = stable_id(
            "agentres",
            &head.agent_id,
            &deployment.deployment_revision_id,
        );
        let catalog_hash = canonical_hash(&json!({
            "migration":"historical-exact-binding-v1",
            "binding_hash":deployment.binding_hash,
        }))?;
        let resolution_hash = canonical_hash(&json!({
            "definition_revision_id":deployment.definition_revision_id,
            "deployment_revision_id":deployment.deployment_revision_id,
            "plan_hash":deployment.plan_hash,
            "binding_hash":deployment.binding_hash,
        }))?;
        let dependency_heads = json!({"migration":"immutable_binding_preserved"});
        let risks = json!({"items":["historical_adapter_availability_required"]});
        sqlx::query(
            "INSERT INTO agent_deployment_resolutions(
               resolution_id,agent_id,definition_revision_id,operation_status,
               catalog_snapshot_hash,resolution_hash,resolved_bindings,worker_contracts,
               dependency_heads,risks,expires_at,created_at,created_by)
             VALUES($1,$2,$3,'succeeded',$4,$5,$6,$7,$8,$9,$10,$11,$12)",
        )
        .bind(&resolution_id)
        .bind(&head.agent_id)
        .bind(&deployment.definition_revision_id)
        .bind(&catalog_hash)
        .bind(&resolution_hash)
        .bind(&deployment.resolved_bindings)
        .bind(&deployment.worker_contracts)
        .bind(&dependency_heads)
        .bind(&risks)
        .bind(DateTime::<Utc>::MAX_UTC)
        .bind(deployment.created_at)
        .bind(&options.actor)
        .execute(&mut **transaction)
        .await?;
        sqlx::query(
            "INSERT INTO agent_deployment_publications(
               agent_id,definition_id,definition_revision_id,deployment_revision_id,
               resolution_id,created_at,created_by)
             VALUES($1,$2,$3,$4,$5,$6,$7)",
        )
        .bind(&head.agent_id)
        .bind(&head.definition_id)
        .bind(&deployment.definition_revision_id)
        .bind(&deployment.deployment_revision_id)
        .bind(&resolution_id)
        .bind(deployment.created_at)
        .bind(&options.actor)
        .execute(&mut **transaction)
        .await?;
    }
    if options.preserve_active {
        sqlx::query(
            "UPDATE agent_publication_heads SET publication_origin='managed',updated_at=$1
             WHERE agent_id=$2 AND definition_revision_id=$3 AND deployment_revision_id=$4",
        )
        .bind(head.updated_at)
        .bind(&head.agent_id)
        .bind(&head.definition_revision_id)
        .bind(&head.deployment_revision_id)
        .execute(&mut **transaction)
        .await?;
    } else {
        sqlx::query("DELETE FROM agent_publication_heads WHERE agent_id=$1")
            .bind(&head.agent_id)
            .execute(&mut **transaction)
            .await?;
    }
    let request_hash = canonical_hash(&json!({
        "operation":"adopt_agent_publication_head","agent_id":head.agent_id,
    }))?;
    sqlx::query(
        "INSERT INTO agent_management_audit_events(
           event_kind,agent_id,subject_id,actor_id,capability,request_id_hash,before_hash,
           after_hash,result_code,created_at)
         VALUES('agent.migrated',$1,$2,$3,'agent.deploy',$4,$5,$6,'ok',$7)",
    )
    .bind(&head.agent_id)
    .bind(&head.deployment_revision_id)
    .bind(&options.actor)
    .bind(&request_hash)
    .bind(canonical_hash(&json!({"publication_origin":head.origin}))?)
    .bind(canonical_hash(&json!({"publication_origin":"managed"}))?)
    .bind(head.updated_at)
    .execute(&mut **transaction)
    .await?;
    let event_id = stable_id("agentout", &head.agent_id, &head.deployment_revision_id);
    sqlx::query(
        "INSERT INTO agent_management_outbox(
           event_id,event_kind,agent_id,subject_id,safe_payload,created_at,delivered_at)
         VALUES($1,'agent.migrated',$2,$3,$4,$5,NULL)",
    )
    .bind(event_id)
    .bind(&head.agent_id)
    .bind(&head.deployment_revision_id)
    .bind(json!({"entity_version":1,"active":options.preserve_active}))
    .bind(head.updated_at)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

async fn map_provider_history_sqlite(
    options: &Options,
    provider_id: &str,
    revision_id: &str,
) -> Result<ProviderHistoryReport> {
    let connect = SqliteConnectOptions::from_str(&options.database_url)?.foreign_keys(true);
    let pool = SqlitePool::connect_with(connect).await?;
    let mut transaction = pool.begin().await?;
    verify_contract_sqlite(&mut transaction).await?;
    let models = sqlx::query_scalar::<_, String>(
        "SELECT m.model_id FROM provider_revision_models m
         JOIN provider_revisions r ON r.revision_id=m.revision_id
         WHERE r.provider_id=? AND r.revision_id=? ORDER BY m.model_id",
    )
    .bind(provider_id)
    .bind(revision_id)
    .fetch_all(&mut *transaction)
    .await?
    .into_iter()
    .collect::<BTreeSet<_>>();
    if models.is_empty() {
        return Err("Provider Revision is missing or has no models".into());
    }
    let rows = sqlx::query(
        "SELECT definition_id,deployment_revision_id,resolved_bindings FROM deployment_revisions
         ORDER BY deployment_revision_id",
    )
    .fetch_all(&mut *transaction)
    .await?;
    let deployments_scanned = rows.len();
    let mut bindings = BTreeMap::new();
    for row in rows {
        let definition_id: String = row.try_get("definition_id")?;
        let deployment_revision_id: String = row.try_get("deployment_revision_id")?;
        let resolved: Value =
            serde_json::from_str(&row.try_get::<String, _>("resolved_bindings")?)?;
        collect_legacy_provider_bindings(
            &resolved,
            provider_id,
            &models,
            &definition_id,
            &deployment_revision_id,
            &mut bindings,
        )?;
    }
    let now = Utc::now();
    for binding in bindings.values() {
        sqlx::query(
            "INSERT OR IGNORE INTO provider_revision_legacy_model_bindings(
               revision_id,provider_id,model_id,legacy_binding_hash,legacy_binding_evidence,
               source_definition_id,source_deployment_revision_id,created_at) VALUES(?,?,?,?,?,?,?,?)",
        )
        .bind(revision_id)
        .bind(provider_id)
        .bind(&binding.model_id)
        .bind(&binding.binding_hash)
        .bind(serde_json::to_string(&binding.evidence)?)
        .bind(&binding.source_definition_id)
        .bind(&binding.source_deployment_revision_id)
        .bind(now)
        .execute(&mut *transaction)
        .await?;
    }
    let report = provider_history_report(
        "sqlite",
        options,
        provider_id,
        revision_id,
        deployments_scanned,
        &bindings,
    );
    if options.dry_run {
        transaction.rollback().await?;
    } else {
        transaction.commit().await?;
    }
    pool.close().await;
    Ok(report)
}

async fn map_provider_history_postgres(
    options: &Options,
    provider_id: &str,
    revision_id: &str,
) -> Result<ProviderHistoryReport> {
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&options.database_url)
        .await?;
    let mut transaction = pool.begin().await?;
    let models = sqlx::query_scalar::<_, String>(
        "SELECT m.model_id FROM provider_revision_models m
         JOIN provider_revisions r ON r.revision_id=m.revision_id
         WHERE r.provider_id=$1 AND r.revision_id=$2 ORDER BY m.model_id",
    )
    .bind(provider_id)
    .bind(revision_id)
    .fetch_all(&mut *transaction)
    .await?
    .into_iter()
    .collect::<BTreeSet<_>>();
    if models.is_empty() {
        return Err("Provider Revision is missing or has no models".into());
    }
    let rows = sqlx::query(
        "SELECT definition_id,deployment_revision_id,resolved_bindings FROM deployment_revisions
         ORDER BY deployment_revision_id",
    )
    .fetch_all(&mut *transaction)
    .await?;
    let deployments_scanned = rows.len();
    let mut bindings = BTreeMap::new();
    for row in rows {
        let definition_id: String = row.try_get("definition_id")?;
        let deployment_revision_id: String = row.try_get("deployment_revision_id")?;
        let resolved: Value = row.try_get("resolved_bindings")?;
        collect_legacy_provider_bindings(
            &resolved,
            provider_id,
            &models,
            &definition_id,
            &deployment_revision_id,
            &mut bindings,
        )?;
    }
    let now = Utc::now();
    for binding in bindings.values() {
        sqlx::query(
            "INSERT INTO provider_revision_legacy_model_bindings(
               revision_id,provider_id,model_id,legacy_binding_hash,legacy_binding_evidence,
               source_definition_id,source_deployment_revision_id,created_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8)
             ON CONFLICT(revision_id,model_id,legacy_binding_hash) DO NOTHING",
        )
        .bind(revision_id)
        .bind(provider_id)
        .bind(&binding.model_id)
        .bind(&binding.binding_hash)
        .bind(&binding.evidence)
        .bind(&binding.source_definition_id)
        .bind(&binding.source_deployment_revision_id)
        .bind(now)
        .execute(&mut *transaction)
        .await?;
    }
    let report = provider_history_report(
        "postgres",
        options,
        provider_id,
        revision_id,
        deployments_scanned,
        &bindings,
    );
    if options.dry_run {
        transaction.rollback().await?;
    } else {
        transaction.commit().await?;
    }
    pool.close().await;
    Ok(report)
}

fn collect_legacy_provider_bindings(
    value: &Value,
    provider_id: &str,
    revision_models: &BTreeSet<String>,
    definition_id: &str,
    deployment_revision_id: &str,
    output: &mut BTreeMap<(String, String), LegacyModelBinding>,
) -> Result<()> {
    match value {
        Value::Array(values) => {
            for value in values {
                collect_legacy_provider_bindings(
                    value,
                    provider_id,
                    revision_models,
                    definition_id,
                    deployment_revision_id,
                    output,
                )?;
            }
        }
        Value::Object(object) => {
            if object.get("adapter").and_then(Value::as_str) == Some("core.llm")
                && object.get("provider_route").and_then(Value::as_str) == Some(provider_id)
            {
                let model_id = object
                    .get("model_id")
                    .and_then(Value::as_str)
                    .ok_or("historical Provider binding omitted model_id")?;
                let binding_hash = object
                    .get("model_binding_hash")
                    .and_then(Value::as_str)
                    .ok_or("historical Provider binding omitted model_binding_hash")?;
                let evidence = object
                    .get("model_binding")
                    .filter(|value| value.is_object())
                    .cloned()
                    .ok_or("historical Provider binding omitted model_binding evidence")?;
                if evidence.get("provider_revision_id").is_none() {
                    if !revision_models.contains(model_id) {
                        return Err(format!(
                            "historical model '{model_id}' is absent from the selected Provider Revision"
                        )
                        .into());
                    }
                    if canonical_hash(&evidence)? != binding_hash {
                        return Err(
                            "historical Provider binding hash does not match its evidence".into(),
                        );
                    }
                    let key = (model_id.to_owned(), binding_hash.to_owned());
                    let candidate = LegacyModelBinding {
                        model_id: model_id.to_owned(),
                        binding_hash: binding_hash.to_owned(),
                        evidence,
                        source_definition_id: definition_id.to_owned(),
                        source_deployment_revision_id: deployment_revision_id.to_owned(),
                    };
                    if let Some(existing) = output.get(&key) {
                        if existing.evidence != candidate.evidence {
                            return Err(
                                "one legacy Provider binding hash has conflicting evidence".into(),
                            );
                        }
                    } else {
                        output.insert(key, candidate);
                    }
                }
            }
            for value in object.values() {
                collect_legacy_provider_bindings(
                    value,
                    provider_id,
                    revision_models,
                    definition_id,
                    deployment_revision_id,
                    output,
                )?;
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
    }
    Ok(())
}

fn provider_history_report(
    backend: &'static str,
    options: &Options,
    provider_id: &str,
    revision_id: &str,
    deployments_scanned: usize,
    bindings: &BTreeMap<(String, String), LegacyModelBinding>,
) -> ProviderHistoryReport {
    ProviderHistoryReport {
        version: 1,
        operation: "map_provider_history",
        backend,
        dry_run: options.dry_run,
        provider_id: provider_id.to_owned(),
        revision_id: revision_id.to_owned(),
        deployments_scanned,
        legacy_bindings_mapped: bindings.len(),
        binding_hashes: bindings
            .values()
            .map(|binding| binding.binding_hash.clone())
            .collect(),
    }
}

fn validate_history(
    head: &Head,
    definitions: &[Definition],
    deployments: &[Deployment],
) -> Result<()> {
    if definitions.is_empty() || deployments.is_empty() {
        return Err(format!("Agent '{}' has incomplete immutable history", head.agent_id).into());
    }
    if !definitions
        .iter()
        .any(|revision| revision.revision_id == head.definition_revision_id)
        || !deployments.iter().any(|deployment| {
            deployment.deployment_revision_id == head.deployment_revision_id
                && deployment.definition_revision_id == head.definition_revision_id
        })
    {
        return Err(format!(
            "Agent '{}' head does not match its immutable history",
            head.agent_id
        )
        .into());
    }
    for deployment in deployments {
        if !definitions
            .iter()
            .any(|revision| revision.revision_id == deployment.definition_revision_id)
        {
            return Err(format!("Agent '{}' has an orphan Deployment", head.agent_id).into());
        }
    }
    Ok(())
}

fn migration_draft(origin: &str, author: &Value) -> Result<(&'static str, Value)> {
    if origin == "graph" {
        return Ok((
            "graph",
            json!({"source":{"type":"graph","document":author}}),
        ));
    }
    let source = author
        .get("source")
        .and_then(Value::as_str)
        .ok_or("structured historical Definition omitted source")?;
    let prompt_files = author
        .get("prompt_files")
        .and_then(Value::as_object)
        .map(|files| {
            files
                .iter()
                .map(|(path, content)| {
                    content
                        .as_str()
                        .map(|content| json!({"path":path,"content":content}))
                        .ok_or("historical prompt file was not text")
                })
                .collect::<std::result::Result<Vec<_>, _>>()
        })
        .transpose()?
        .unwrap_or_default();
    Ok((
        "yaml_package",
        json!({"source":{
            "type":"yaml_package","agent_yaml":source,"prompt_files":prompt_files,
        }}),
    ))
}

fn stable_id(prefix: &str, left: &str, right: &str) -> String {
    let digest = Sha256::digest(format!("{prefix}\0{left}\0{right}").as_bytes());
    let suffix = digest[..16]
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    format!("{prefix}_{suffix}")
}

fn canonical_hash(value: &impl Serialize) -> Result<String> {
    let bytes = serde_jcs::to_vec(value)?;
    let mut output = String::from("sha256:");
    for byte in Sha256::digest(bytes) {
        use std::fmt::Write as _;
        write!(&mut output, "{byte:02x}")?;
    }
    Ok(output)
}
