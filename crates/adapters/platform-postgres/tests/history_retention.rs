//! Physical retention/concurrency and least-privilege evidence. The deliberately
//! opaque Run body is a canary: none of these maintenance ports may disclose it.
use chrono::{Duration, Utc};
use insight_platform_contracts::{
    ResourceId, ResourceKind, Sha256Digest, TenantConfig, TypedPayload,
};
use insight_platform_orchestrator::history::*;
use insight_platform_postgres::{
    repository::{NewTenant, PgRepository},
    verify_schema,
};
use sqlx::{postgres::PgPoolOptions, PgPool};
static HISTORY_FIXTURE_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
fn id(kind: ResourceKind) -> ResourceId {
    ResourceId::from_uuid_v7(kind, uuid::Uuid::now_v7()).unwrap()
}
fn digest(label: &str) -> Sha256Digest {
    insight_platform_contracts::canonical_digest(&serde_json::json!({"retention_fixture":label}))
        .unwrap()
        .parse()
        .unwrap()
}
async fn denied(pool: &PgPool, sql: &'static str) {
    let error = sqlx::query(sql).execute(pool).await.unwrap_err();
    assert_eq!(
        error
            .as_database_error()
            .and_then(|error| error.code())
            .as_deref(),
        Some("42501"),
        "{error}"
    );
}
#[tokio::test]
async fn maintenance_role_preserves_holds_prefixes_and_append_races() {
    let _fixture = HISTORY_FIXTURE_LOCK.lock().await;
    let url = std::env::var("PLATFORM_TEST_HISTORY_DATABASE_URL")
        .expect("PLATFORM_TEST_HISTORY_DATABASE_URL is required");
    let admin = PgPoolOptions::new()
        .max_connections(6)
        .connect(&url)
        .await
        .unwrap();
    verify_schema(&admin).await.unwrap();
    sqlx::raw_sql("DO $role$ BEGIN IF NOT EXISTS(SELECT 1 FROM pg_catalog.pg_roles WHERE rolname='insight_history_test') THEN CREATE ROLE insight_history_test NOLOGIN; END IF; END $role$;").execute(&admin).await.unwrap();
    let grants = insight_platform_postgres::history_repository::history_role_grants_sql()
        .lines()
        .filter(|line| !line.starts_with('\\'))
        .collect::<Vec<_>>()
        .join("\n")
        .replace(":'history_maintenance_role'", "'insight_history_test'");
    // Audited: checked-in provisioning SQL with one fixed test role literal; no input-derived SQL.
    sqlx::raw_sql(sqlx::AssertSqlSafe(grants))
        .execute(&admin)
        .await
        .unwrap();
    sqlx::raw_sql("DO $role$ BEGIN IF NOT EXISTS(SELECT 1 FROM pg_catalog.pg_roles WHERE rolname='insight_no_history_test') THEN CREATE ROLE insight_no_history_test NOLOGIN; END IF; END $role$; GRANT USAGE ON SCHEMA insight_platform TO insight_no_history_test;").execute(&admin).await.unwrap();
    let outsider = PgPoolOptions::new()
        .max_connections(1)
        .after_connect(|connection, _| {
            Box::pin(async move {
                sqlx::query("SET ROLE insight_no_history_test")
                    .execute(connection)
                    .await?;
                Ok(())
            })
        })
        .connect(&url)
        .await
        .unwrap();
    denied(
        &outsider,
        "SELECT insight_platform.history_scan_runs(NULL,NULL,NULL,NULL,NULL,1)",
    )
    .await;
    let restricted = PgPoolOptions::new()
        .max_connections(4)
        .after_connect(|connection, _| {
            Box::pin(async move {
                sqlx::query("SET ROLE insight_history_test")
                    .execute(connection)
                    .await?;
                Ok(())
            })
        })
        .connect(&url)
        .await
        .unwrap();
    for query in [
        "SELECT payload FROM insight_platform.events LIMIT 1",
        "SELECT current_payload FROM insight_platform.runs LIMIT 1",
        "DELETE FROM insight_platform.events WHERE false",
        "UPDATE insight_platform.runs SET state='failed' WHERE false",
        "SELECT payload FROM insight_platform.tasks LIMIT 1",
    ] {
        denied(&restricted, query).await;
    }
    let tenant = id(ResourceKind::Tenant);
    let principal = id(ResourceKind::Principal);
    let resource = id(ResourceKind::Agent);
    let revision = id(ResourceKind::AgentPlanRevision);
    let deployment = id(ResourceKind::AgentDeployment);
    let run = id(ResourceKind::Run);
    let writer = PgRepository::new(admin.clone());
    writer
        .create_tenant(NewTenant {
            tenant_id: tenant.to_string(),
            state: "active".into(),
            config: TenantConfig::default(),
        })
        .await
        .unwrap();
    let opaque = TypedPayload::new(
        1,
        &serde_json::json!({"sensitive_body":"retention-content-canary","fixture_run":run}),
    )
    .unwrap();
    sqlx::query("INSERT INTO insight_platform.principals(principal_id,state,authentication_authority_digest,subject_digest,payload_schema_version,payload,payload_digest) VALUES($1,'active',$2,$2,1,$3,$2)").bind(principal.to_string()).bind(&opaque.digest).bind(&opaque.value).execute(&admin).await.unwrap();
    sqlx::query("INSERT INTO insight_platform.resources(tenant_id,resource_id,resource_kind,lifecycle_state,gate_state,payload_digest) VALUES($1,$2,'agent','active','enabled',$3)").bind(tenant.to_string()).bind(resource.to_string()).bind(&opaque.digest).execute(&admin).await.unwrap();
    sqlx::query("INSERT INTO insight_platform.resource_versions(tenant_id,resource_version_id,resource_id,resource_version_kind,revision_no,content_digest,payload_schema_version,payload,payload_digest,created_by) VALUES($1,$2,$3,'agent_plan_revision',1,$4,1,$5,$4,$6)").bind(tenant.to_string()).bind(revision.to_string()).bind(resource.to_string()).bind(&opaque.digest).bind(&opaque.value).bind(principal.to_string()).execute(&admin).await.unwrap();
    sqlx::query("INSERT INTO insight_platform.deployments(tenant_id,deployment_id,resource_id,resource_version_id,environment,bindings_digest,payload_schema_version,bindings,created_by) VALUES($1,$2,$3,$4,'test',$5,1,$6,$7)").bind(tenant.to_string()).bind(deployment.to_string()).bind(resource.to_string()).bind(revision.to_string()).bind(&opaque.digest).bind(&opaque.value).bind(principal.to_string()).execute(&admin).await.unwrap();
    let requirement =
        insight_platform_plan::execution::program_execution_requirement(digest("definition"), 6)
            .unwrap();
    let requirement_digest = requirement.canonical_digest().unwrap();
    let old = Utc::now() - Duration::hours(2);
    sqlx::query("INSERT INTO insight_platform.runs(tenant_id,run_id,root_run_id,agent_deployment_id,principal_id,trace_id,state,bindings_schema_version,bindings,bindings_digest,current_schema_version,current_payload,current_payload_digest,deadline,created_at,updated_at,terminal_at,public_sequence,execution_requirement_version,execution_requirement,execution_requirement_digest) VALUES($1,$2,$2,$3,$4,'0123456789abcdef0123456789abcdef','succeeded',1,$5,$6,1,$5,$6,$7,$8,$8,$8,3,1,$9,$10)").bind(tenant.to_string()).bind(run.to_string()).bind(deployment.to_string()).bind(principal.to_string()).bind(&opaque.value).bind(&opaque.digest).bind(Utc::now()+Duration::days(1)).bind(old).bind(serde_json::to_value(requirement).unwrap()).bind(requirement_digest.to_string()).execute(&admin).await.unwrap();
    for sequence in 1..=3_i64 {
        let event = id(ResourceKind::Event);
        let outbox = id(ResourceKind::OutboxEvent);
        sqlx::query("INSERT INTO insight_platform.events(tenant_id,event_id,aggregate_kind,aggregate_id,aggregate_version,trace_id,run_id,public_sequence,event_type,visibility,payload_schema_version,payload,payload_digest,occurred_at) VALUES($1,$2,'run',$3,$4,'0123456789abcdef0123456789abcdef',$3,$4,'run.completed','public',1,$5,$6,$7)").bind(tenant.to_string()).bind(event.to_string()).bind(run.to_string()).bind(sequence).bind(&opaque.value).bind(&opaque.digest).bind(old).execute(&admin).await.unwrap();
        sqlx::query("INSERT INTO insight_platform.outbox_events(tenant_id,outbox_id,event_id,trace_id,state,created_at,updated_at,published_at) VALUES($1,$2,$3,'0123456789abcdef0123456789abcdef',$4,$5,$5,$6)").bind(tenant.to_string()).bind(outbox.to_string()).bind(event.to_string()).bind(if sequence==2{"pending"}else{"published"}).bind(old).bind((sequence!=2).then_some(old)).execute(&admin).await.unwrap();
    }
    let maintenance = PgRepository::new(restricted.clone());
    let policy = HistoryRetentionPolicy {
        schema_version: 2,
        public_event_minimum_seconds: 1,
        audit_event_minimum_seconds: 1,
        receipt_minimum_seconds: 1,
        published_outbox_minimum_seconds: 1,

        cleanup_minimum_seconds: 1,
    };
    let target = PurgePublicRunEventPrefix {
        tenant_id: tenant.clone(),
        run_id: run.clone(),
        through_sequence: 3,
        maximum_events: 2,
    };
    let holds = RunHistoryHolds {
        schema_version: 1,
        holds: std::collections::BTreeMap::from([(
            digest("hold"),
            RunHistoryHold {
                reason_evidence_digest: digest("evidence"),
                placed_by: principal,
                placed_at: Utc::now(),
            },
        )]),
    };
    sqlx::query(
        "UPDATE insight_platform.runs SET history_holds=$3 WHERE tenant_id=$1 AND run_id=$2",
    )
    .bind(tenant.to_string())
    .bind(run.to_string())
    .bind(serde_json::to_value(holds).unwrap())
    .execute(&admin)
    .await
    .unwrap();
    assert_eq!(
        maintenance
            .purge_public_run_event_prefix(target.clone(), &policy)
            .await
            .unwrap()
            .deleted_events,
        0
    );
    sqlx::query(
        "UPDATE insight_platform.runs SET history_holds=$3 WHERE tenant_id=$1 AND run_id=$2",
    )
    .bind(tenant.to_string())
    .bind(run.to_string())
    .bind(serde_json::to_value(RunHistoryHolds::default()).unwrap())
    .execute(&admin)
    .await
    .unwrap();
    let first = maintenance
        .purge_public_run_event_prefix(target.clone(), &policy)
        .await
        .unwrap();
    assert_eq!((first.deleted_events, first.replay_floor), (1, 1));
    let floor: i64 = sqlx::query_scalar(
        "SELECT public_replay_floor FROM insight_platform.runs WHERE tenant_id=$1 AND run_id=$2",
    )
    .bind(tenant.to_string())
    .bind(run.to_string())
    .fetch_one(&admin)
    .await
    .unwrap();
    assert_eq!(floor, 1);
    sqlx::query("UPDATE insight_platform.outbox_events SET state='published',published_at=$2 WHERE tenant_id=$1 AND published_at IS NULL").bind(tenant.to_string()).bind(old).execute(&admin).await.unwrap();
    // The app holds Run while deciding and deleting; a later append survives the
    // frozen target even if it commits immediately before maintenance takes over.
    let mut append = admin.begin().await.unwrap();
    sqlx::query("SELECT 1 FROM insight_platform.runs WHERE tenant_id=$1 AND run_id=$2 FOR UPDATE")
        .bind(tenant.to_string())
        .bind(run.to_string())
        .fetch_one(&mut *append)
        .await
        .unwrap();
    let busy = maintenance
        .purge_public_run_event_prefix(target.clone(), &policy)
        .await
        .unwrap_err();
    assert!(
        matches!(busy, insight_platform_postgres::repository::RepositoryError::Database(sqlx::Error::Database(error)) if error.code().as_deref()==Some("55P03"))
    );
    let event = id(ResourceKind::Event);
    sqlx::query("INSERT INTO insight_platform.events(tenant_id,event_id,aggregate_kind,aggregate_id,aggregate_version,trace_id,run_id,public_sequence,event_type,visibility,payload_schema_version,payload,payload_digest) VALUES($1,$2,'run',$3,4,'0123456789abcdef0123456789abcdef',$3,4,'run.completed','public',1,$4,$5)").bind(tenant.to_string()).bind(event.to_string()).bind(run.to_string()).bind(&opaque.value).bind(&opaque.digest).execute(&mut *append).await.unwrap();
    sqlx::query(
        "UPDATE insight_platform.runs SET public_sequence=4 WHERE tenant_id=$1 AND run_id=$2",
    )
    .bind(tenant.to_string())
    .bind(run.to_string())
    .execute(&mut *append)
    .await
    .unwrap();
    append.commit().await.unwrap();
    let complete = maintenance
        .purge_public_run_event_prefix(target, &policy)
        .await
        .unwrap();
    assert_eq!(
        (
            complete.deleted_events,
            complete.replay_floor,
            complete.target_reached
        ),
        (2, 3, true)
    );
    let remaining: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM insight_platform.events WHERE tenant_id=$1 AND run_id=$2",
    )
    .bind(tenant.to_string())
    .bind(run.to_string())
    .fetch_one(&admin)
    .await
    .unwrap();
    assert_eq!(remaining, 1);
    assert_eq!(PublicRunReadPosition::Initial.resolve(3, 4).unwrap(), 3);
    assert!(matches!(
        PublicRunReadPosition::AfterSequence { sequence: 1 }.resolve(3, 4),
        Err(PublicReplayError::HistoryGap { replay_floor: 3 })
    ));
    let scan = maintenance
        .scan_history_retention_runs(ScanHistoryRetentionRuns {
            cursor: None,
            maximum_runs: 128,
        })
        .await
        .unwrap();
    assert!(scan
        .candidates
        .iter()
        .any(|candidate| candidate.run_id == run));
    // Wrong floor and oversized primitive requests fail without changing history.
    assert!(
        sqlx::query("SELECT insight_platform.history_delete_prefix($1,$2,0,4)")
            .bind(tenant.to_string())
            .bind(run.to_string())
            .execute(&restricted)
            .await
            .is_err()
    );
    assert!(
        sqlx::query("SELECT insight_platform.history_lock_event_prefix($1,$2,3,4,1001)")
            .bind(tenant.to_string())
            .bind(run.to_string())
            .execute(&restricted)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn receipt_event_delivery_retirement_reclaims_repeated_growth_without_deleting_authority() {
    let _fixture = HISTORY_FIXTURE_LOCK.lock().await;
    use insight_platform_orchestrator::history::retirement::*;
    let url = std::env::var("PLATFORM_TEST_HISTORY_DATABASE_URL")
        .expect("PLATFORM_TEST_HISTORY_DATABASE_URL is required");
    let admin = PgPoolOptions::new()
        .max_connections(6)
        .connect(&url)
        .await
        .unwrap();
    verify_schema(&admin).await.unwrap();
    sqlx::raw_sql("DO $r$ BEGIN IF NOT EXISTS(SELECT 1 FROM pg_catalog.pg_roles WHERE rolname='insight_history_growth_test') THEN CREATE ROLE insight_history_growth_test NOLOGIN; END IF; END $r$;").execute(&admin).await.unwrap();
    let grants = insight_platform_postgres::history_repository::history_role_grants_sql()
        .lines()
        .filter(|line| !line.starts_with('\\'))
        .collect::<Vec<_>>()
        .join("\n")
        .replace(
            ":'history_maintenance_role'",
            "'insight_history_growth_test'",
        );
    sqlx::raw_sql(sqlx::AssertSqlSafe(grants))
        .execute(&admin)
        .await
        .unwrap();
    let limited = PgPoolOptions::new()
        .max_connections(3)
        .after_connect(|connection, _| {
            Box::pin(async move {
                sqlx::query("SET ROLE insight_history_growth_test")
                    .execute(connection)
                    .await?;
                Ok(())
            })
        })
        .connect(&url)
        .await
        .unwrap();
    denied(
        &limited,
        "SELECT payload FROM insight_platform.receipts LIMIT 1",
    )
    .await;
    denied(
        &limited,
        "DELETE FROM insight_platform.receipts WHERE false",
    )
    .await;
    let repository = PgRepository::new(limited.clone());
    let tenant = id(ResourceKind::Tenant);
    let resource = id(ResourceKind::Agent);
    PgRepository::new(admin.clone())
        .create_tenant(NewTenant {
            tenant_id: tenant.to_string(),
            state: "active".into(),
            config: TenantConfig::default(),
        })
        .await
        .unwrap();
    let payload =
        TypedPayload::new(1, &serde_json::json!({"body":"must-not-be-disclosed"})).unwrap();
    sqlx::query("INSERT INTO insight_platform.resources(tenant_id,resource_id,resource_kind,lifecycle_state,gate_state,payload_digest) VALUES($1,$2,'agent','active','enabled',$3)").bind(tenant.to_string()).bind(resource.to_string()).bind(&payload.digest).execute(&admin).await.unwrap();
    let policy = HistoryRetentionPolicy {
        schema_version: 2,
        public_event_minimum_seconds: 1,
        audit_event_minimum_seconds: 1,
        receipt_minimum_seconds: 1,
        published_outbox_minimum_seconds: 1,
        cleanup_minimum_seconds: 1,
    };
    let old = Utc::now() - Duration::days(3);
    for round in 0..3 {
        let mut receipts = Vec::new();
        let mut events = Vec::new();
        for iteration in 0..12 {
            let receipt = id(ResourceKind::Receipt);
            let event = id(ResourceKind::Event);
            let outbox = id(ResourceKind::OutboxEvent);
            sqlx::query("INSERT INTO insight_platform.receipts(tenant_id,receipt_id,receipt_kind,scope_kind,scope_id,dedupe_owner_id,operation,idempotency_key_digest,request_digest,state,response_reference_id,payload_digest,created_at,completed_at,expires_at) VALUES($1,$2,'command','resource',$3,$3,'resource.update_draft',$4,$4,'succeeded',$3,$4,$5,$6,$7)").bind(tenant.to_string()).bind(receipt.to_string()).bind(resource.to_string()).bind(digest(&receipt.to_string()).to_string()).bind(old).bind(old+Duration::hours(1)).bind(old+Duration::hours(2)).execute(&admin).await.unwrap();
            sqlx::query("INSERT INTO insight_platform.events(tenant_id,event_id,aggregate_kind,aggregate_id,aggregate_version,trace_id,event_type,visibility,payload_schema_version,payload,payload_digest,occurred_at) VALUES($1,$2,'resource',$3,$7,'0123456789abcdef0123456789abcdef','resource.updated','internal',1,$4,$5,$6)").bind(tenant.to_string()).bind(event.to_string()).bind(resource.to_string()).bind(&payload.value).bind(&payload.digest).bind(old+Duration::hours(1)).bind(i64::from(round*12+iteration+2)).execute(&admin).await.unwrap();
            sqlx::query("UPDATE insight_platform.resources SET version=$3 WHERE tenant_id=$1 AND resource_id=$2").bind(tenant.to_string()).bind(resource.to_string()).bind(i64::from(round*12+iteration+2)).execute(&admin).await.unwrap();
            sqlx::query("INSERT INTO insight_platform.outbox_events(tenant_id,outbox_id,event_id,trace_id,state,published_at,created_at,updated_at) VALUES($1,$2,$3,'0123456789abcdef0123456789abcdef','published',$4,$4,$4)").bind(tenant.to_string()).bind(outbox.to_string()).bind(event.to_string()).bind(old+Duration::hours(1)).execute(&admin).await.unwrap();
            receipts.push(receipt);
            events.push(event);
        }
        // One current replay promise holds Event history, while completed delivery
        // rows independently finish their own retention obligation.
        sqlx::query("UPDATE insight_platform.receipts SET expires_at=$3 WHERE tenant_id=$1 AND receipt_id=$2").bind(tenant.to_string()).bind(receipts[0].to_string()).bind(Utc::now()+Duration::days(1)).execute(&admin).await.unwrap();
        assert_eq!(
            repository
                .retire_history_record(
                    HistoryRetirementLane::Receipt,
                    HistoryRecordKey {
                        tenant_id: tenant.clone(),
                        record_id: receipts[0].clone()
                    },
                    &policy
                )
                .await
                .unwrap(),
            HistoryRetirementOutcome::Retained(HistoryRetainedReason::Window)
        );
        for event in &events {
            assert_eq!(
                repository
                    .retire_history_record(
                        HistoryRetirementLane::EventDelivery,
                        HistoryRecordKey {
                            tenant_id: tenant.clone(),
                            record_id: event.clone()
                        },
                        &policy
                    )
                    .await
                    .unwrap(),
                HistoryRetirementOutcome::Retired {
                    receipts: 0,
                    events: 0,
                    outbox: 1,
                    tasks: 0,
                    jobs: 0
                }
            );
        }
        // Busy is bounded and leaves the target intact; the next candidate is usable.
        let mut lock = admin.begin().await.unwrap();
        sqlx::query("SELECT 1 FROM insight_platform.receipts WHERE tenant_id=$1 AND receipt_id=$2 FOR UPDATE").bind(tenant.to_string()).bind(receipts[1].to_string()).execute(&mut *lock).await.unwrap();
        assert_eq!(
            repository
                .retire_history_record(
                    HistoryRetirementLane::Receipt,
                    HistoryRecordKey {
                        tenant_id: tenant.clone(),
                        record_id: receipts[1].clone()
                    },
                    &policy
                )
                .await
                .unwrap(),
            HistoryRetirementOutcome::Retained(HistoryRetainedReason::Busy)
        );
        lock.rollback().await.unwrap();
        sqlx::query("UPDATE insight_platform.receipts SET expires_at=$3 WHERE tenant_id=$1 AND receipt_id=$2").bind(tenant.to_string()).bind(receipts[0].to_string()).bind(old+Duration::hours(2)).execute(&admin).await.unwrap();
        sqlx::query("UPDATE insight_platform.receipts SET state='processing',completed_at=NULL WHERE tenant_id=$1 AND receipt_id=$2").bind(tenant.to_string()).bind(receipts[0].to_string()).execute(&admin).await.unwrap();
        assert_eq!(
            repository
                .retire_history_record(
                    HistoryRetirementLane::Receipt,
                    HistoryRecordKey {
                        tenant_id: tenant.clone(),
                        record_id: receipts[0].clone()
                    },
                    &policy
                )
                .await
                .unwrap(),
            HistoryRetirementOutcome::Retained(HistoryRetainedReason::ActiveEffect)
        );
        assert_eq!(
            repository
                .retire_history_record(
                    HistoryRetirementLane::EventDelivery,
                    HistoryRecordKey {
                        tenant_id: tenant.clone(),
                        record_id: events[0].clone()
                    },
                    &policy
                )
                .await
                .unwrap(),
            HistoryRetirementOutcome::Retained(HistoryRetainedReason::ActiveEffect)
        );
        sqlx::query("UPDATE insight_platform.receipts SET state='succeeded',completed_at=$3 WHERE tenant_id=$1 AND receipt_id=$2").bind(tenant.to_string()).bind(receipts[0].to_string()).bind(old+Duration::hours(1)).execute(&admin).await.unwrap();
        for receipt in &receipts {
            assert_eq!(
                repository
                    .retire_history_record(
                        HistoryRetirementLane::Receipt,
                        HistoryRecordKey {
                            tenant_id: tenant.clone(),
                            record_id: receipt.clone()
                        },
                        &policy
                    )
                    .await
                    .unwrap(),
                HistoryRetirementOutcome::Retired {
                    receipts: 1,
                    events: 0,
                    outbox: 0,
                    tasks: 0,
                    jobs: 0
                }
            );
        }
        for event in &events {
            assert_eq!(
                repository
                    .retire_history_record(
                        HistoryRetirementLane::EventDelivery,
                        HistoryRecordKey {
                            tenant_id: tenant.clone(),
                            record_id: event.clone()
                        },
                        &policy
                    )
                    .await
                    .unwrap(),
                HistoryRetirementOutcome::Retired {
                    receipts: 0,
                    events: 1,
                    outbox: 0,
                    tasks: 0,
                    jobs: 0
                }
            );
        }
        let counts:(i64,i64,i64,i64)=sqlx::query_as("SELECT (SELECT count(*) FROM insight_platform.receipts WHERE tenant_id=$1),(SELECT count(*) FROM insight_platform.events WHERE tenant_id=$1),(SELECT count(*) FROM insight_platform.outbox_events WHERE tenant_id=$1),(SELECT count(*) FROM insight_platform.resources WHERE tenant_id=$1)").bind(tenant.to_string()).fetch_one(&admin).await.unwrap();
        assert_eq!(
            counts,
            (0, 0, 0, 1),
            "round {round}: recoverable history clears; current Resource remains"
        );
    }
    for lane in HistoryRetirementLane::ALL {
        let page = repository
            .scan_history_retirement(ScanHistoryRetirement {
                lane,
                cursor: None,
                maximum_records: 1,
            })
            .await
            .unwrap();
        assert!(page.candidates.len() <= 1);
    }
}
