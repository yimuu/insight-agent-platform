//! Mixed valid/invalid objects exercise real claim savepoints and recovery cursors.
use super::*;
use insight_platform_jobs::store::SafetyScanPhase;
use insight_platform_orchestrator::store::DeferredOrchestrationModelTurn;
use std::collections::BTreeSet;
use std::time::Instant;

fn fresh(kind: ResourceKind) -> ResourceId {
    ResourceId::from_uuid_v7(kind, uuid::Uuid::now_v7()).unwrap()
}

pub(super) async fn isolated_fixture(pool: &PgPool, original: &Fixture) -> Fixture {
    let mut fixture = original.clone();
    fixture.run_id = fresh(ResourceKind::Run);
    fixture.scope_id = fresh(ResourceKind::ScopeInstance);
    let input = fresh(ResourceKind::RunValue);
    let current = RunCurrentSnapshot::initial(
        fixture.run_id.clone(),
        id(ResourceKind::AgentDeployment, 0x3a),
        input.clone(),
    );
    let current = TypedPayload::from_versioned(1, &current, 1_048_576).unwrap();
    sqlx::query(r#"INSERT INTO insight_platform.runs(tenant_id,run_id,root_run_id,agent_deployment_id,principal_id,state,version,bindings_schema_version,bindings,bindings_digest,current_schema_version,current_payload,current_payload_digest,deadline,started_at,created_at,updated_at,trace_id,execution_requirement_version,execution_requirement,execution_requirement_digest)
        SELECT tenant_id,$3,$3,agent_deployment_id,principal_id,'running',1,bindings_schema_version,bindings,bindings_digest,$4,$5,$6,deadline,statement_timestamp(),statement_timestamp(),statement_timestamp(),trace_id,execution_requirement_version,execution_requirement,execution_requirement_digest FROM insight_platform.runs WHERE tenant_id=$1 AND run_id=$2"#)
        .bind(original.tenant_id.to_string()).bind(original.run_id.to_string()).bind(fixture.run_id.to_string()).bind(current.schema_version).bind(&current.value).bind(&current.digest).execute(pool).await.unwrap();
    sqlx::query("INSERT INTO insight_platform.run_values(tenant_id,value_id,run_id,value_kind,classification,schema_digest,content_digest,inline_value) SELECT tenant_id,$3,$4,value_kind,classification,schema_digest,content_digest,inline_value FROM insight_platform.run_values WHERE tenant_id=$1 AND value_id=$2")
        .bind(original.tenant_id.to_string()).bind(id(ResourceKind::RunValue,0x54).to_string()).bind(input.to_string()).bind(fixture.run_id.to_string()).execute(pool).await.unwrap();
    sqlx::query(
        "UPDATE insight_platform.runs SET input_value_id=$3 WHERE tenant_id=$1 AND run_id=$2",
    )
    .bind(fixture.tenant_id.to_string())
    .bind(fixture.run_id.to_string())
    .bind(input.to_string())
    .execute(pool)
    .await
    .unwrap();
    let scope = TypedPayload::new(1, &json!({"root_run_id":fixture.run_id})).unwrap();
    sqlx::query("INSERT INTO insight_platform.run_nodes(tenant_id,node_id,run_id,record_kind,scope_id,logical_key,node_kind,state,generation,version,payload_schema_version,payload,payload_digest,deadline,started_at,created_at,updated_at) VALUES($1,$2,$3,'scope_instance',$2,'root','root','open',1,1,$4,$5,$6,$7,statement_timestamp(),statement_timestamp(),statement_timestamp())")
        .bind(fixture.tenant_id.to_string()).bind(fixture.scope_id.to_string()).bind(fixture.run_id.to_string()).bind(scope.schema_version).bind(&scope.value).bind(&scope.digest).bind(fixture.deadline).execute(pool).await.unwrap();
    fixture
}

pub(super) async fn admit(
    pool: &PgPool,
    repo: &PgRepository,
    fixture: &Fixture,
    base: u16,
) -> DeferredOrchestrationModelTurn {
    sqlx::query(
        "UPDATE insight_platform.runs SET state='running' WHERE tenant_id=$1 AND run_id=$2",
    )
    .bind(fixture.tenant_id.to_string())
    .bind(fixture.run_id.to_string())
    .execute(pool)
    .await
    .unwrap();
    let (_, command) = seed_running_model_orchestration(pool, repo, fixture, base).await;
    let mut tx = repo.begin_scheduler_transaction().await.unwrap();
    let CommandOutcome::Applied(result) = Box::pin(tx.defer_orchestration_to_model_turn(command))
        .await
        .unwrap()
    else {
        panic!("new isolation leaf")
    };
    tx.commit().await.unwrap();
    result
}

fn claim_command() -> ClaimModelJobs {
    ClaimModelJobs {
        worker_manifest: production_model_worker_manifest(),
        worker_process_generation_id: fresh(ResourceKind::WorkerProcessGeneration),
        worker_manifest_digest: production_model_worker_manifest_digest(),
        limit: 4,
        lease_milliseconds: 30_000,
        slots: (0..4)
            .map(|_| ModelClaimSlot {
                lease_token_digest: named_digest(&fresh(ResourceKind::Job).to_string()),
                usage_reservation_id: fresh(ResourceKind::UsageReservation),
                quota_entry_ids: (0..4)
                    .map(|_| fresh(ResourceKind::QuotaLedgerEntry))
                    .collect(),
                event_id: fresh(ResourceKind::Event),
                outbox_id: fresh(ResourceKind::OutboxEvent),
                resume_mutations: None,
                failure_mutations: None,
                tool_continuation_mutations: None,
            })
            .collect(),
    }
}

fn recovery(
    after: Option<insight_platform_jobs::store::SafetyScanCursor>,
    limit: u16,
) -> DriveExpiredModelJobs {
    DriveExpiredModelJobs {
        shard: SafetyScanShard::whole(),
        after,
        limit,
        retry_backoff_milliseconds: 60_000,
        slots: (0..limit)
            .map(|_| ExpiredModelRecoverySlot {
                quota_entry_ids: (0..4)
                    .map(|_| fresh(ResourceKind::QuotaLedgerEntry))
                    .collect(),
                event_id: fresh(ResourceKind::Event),
                outbox_id: fresh(ResourceKind::OutboxEvent),
                failure_mutations: insight_platform_contracts::ExternalLeafFailureMutationIds {
                    convergence_job_id: fresh(ResourceKind::Job),
                    run_event_id: fresh(ResourceKind::Event),
                    run_outbox_id: fresh(ResourceKind::OutboxEvent),
                    leaf_node_event_id: fresh(ResourceKind::Event),
                    leaf_node_outbox_id: fresh(ResourceKind::OutboxEvent),
                    convergence_job_event_id: fresh(ResourceKind::Event),
                    convergence_job_outbox_id: fresh(ResourceKind::OutboxEvent),
                },
            })
            .collect(),
    }
}

async fn object_snapshot(pool: &PgPool, tenant: &ResourceId, job: &str) -> serde_json::Value {
    sqlx::query_scalar(r#"SELECT jsonb_build_object('job',to_jsonb(job),'turn',to_jsonb(turn),'node',to_jsonb(node),
        'receipts',(SELECT count(*) FROM insight_platform.receipts r WHERE r.tenant_id=job.tenant_id AND r.scope_id=ANY(ARRAY[job.job_id,job.owner_id,job.node_id])),
        'events',(SELECT count(*) FROM insight_platform.events e WHERE e.tenant_id=job.tenant_id AND e.aggregate_id=ANY(ARRAY[job.job_id,job.owner_id,job.node_id])),
        'quota',(SELECT count(*) FROM insight_platform.quota_ledger q WHERE q.tenant_id=job.tenant_id AND q.correlation_id=job.quota_reservation_id))
        FROM insight_platform.jobs job JOIN insight_platform.invocations turn ON turn.tenant_id=job.tenant_id AND turn.invocation_id=job.owner_id
        JOIN insight_platform.run_nodes node ON node.tenant_id=job.tenant_id AND node.node_id=job.node_id WHERE job.tenant_id=$1 AND job.job_id=$2"#)
        .bind(tenant.to_string()).bind(job).fetch_one(pool).await.unwrap()
}

async fn active(pool: &PgPool, fixture: &Fixture) -> i32 {
    sqlx::query_scalar(
        "SELECT active_work_count FROM insight_platform.runs WHERE tenant_id=$1 AND run_id=$2",
    )
    .bind(fixture.tenant_id.to_string())
    .bind(fixture.run_id.to_string())
    .fetch_one(pool)
    .await
    .unwrap()
}

pub(super) async fn verify(pool: &PgPool, repo: &PgRepository, original: &Fixture) {
    Box::pin(verify_inner(pool, repo, original)).await;
    Box::pin(verify_cancellation_pages(pool, repo, original)).await;
    Box::pin(verify_run_control_pages(pool, repo, original)).await;
}

async fn verify_inner(pool: &PgPool, repo: &PgRepository, original: &Fixture) {
    let fixture = isolated_fixture(pool, original).await;
    let mut leaves = Vec::new();
    for base in [0x7000, 0x7200, 0x7400, 0x7600, 0x7800] {
        leaves.push(admit(pool, repo, &fixture, base).await);
    }
    assert_eq!(active(pool, &fixture).await, 0);
    // The first object has a structurally valid envelope with an invalid owning payload.
    let invalid = TypedPayload::new(1, &json!({"invalid_model_job":true})).unwrap();
    sqlx::query("UPDATE insight_platform.jobs SET payload_schema_version=$3,payload=$4,payload_digest=$5 WHERE tenant_id=$1 AND job_id=$2")
        .bind(fixture.tenant_id.to_string()).bind(&leaves[0].model_job.job_id)
        .bind(invalid.schema_version).bind(&invalid.value).bind(&invalid.digest).execute(pool).await.unwrap();
    let bad_before = object_snapshot(pool, &fixture.tenant_id, &leaves[0].model_job.job_id).await;
    let successes_before: i64 = sqlx::query_scalar("SELECT successful_claims FROM insight_platform.scheduler_tenant_state WHERE tenant_id=$1 AND work_class='model'")
        .bind(fixture.tenant_id.to_string()).fetch_one(pool).await.unwrap();
    // Hold the next Job while discovery sees its valid committed version. Its own savepoint
    // will already have acquired the Run permit when the current Job is decoded after release.
    let mut expected_late_bad =
        object_snapshot(pool, &fixture.tenant_id, &leaves[1].model_job.job_id).await;
    expected_late_bad["job"]["payload_digest"] = json!(named_digest("selected invalid Job digest"));
    let mut blocker = pool.begin().await.unwrap();
    let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *blocker)
        .await
        .unwrap();
    sqlx::query(
        "SELECT job_id FROM insight_platform.jobs WHERE tenant_id=$1 AND job_id=$2 FOR UPDATE",
    )
    .bind(fixture.tenant_id.to_string())
    .bind(&leaves[1].model_job.job_id)
    .fetch_one(&mut *blocker)
    .await
    .unwrap();
    let task_repo = repo.clone();
    let task = tokio::spawn(async move {
        task_repo
            .claim_model_jobs_current_rounds(claim_command())
            .await
    });
    let until = Instant::now() + StdDuration::from_secs(10);
    loop {
        let blocked: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND $1=ANY(pg_blocking_pids(pid)) AND query LIKE '%FROM insight_platform.jobs%' AND query LIKE '%FOR UPDATE%')")
            .bind(pid).fetch_one(pool).await.unwrap();
        if blocked {
            break;
        }
        assert!(
            Instant::now() < until,
            "claim did not reach selected Job row lock"
        );
        tokio::time::sleep(StdDuration::from_millis(10)).await;
    }
    sqlx::query(
        "UPDATE insight_platform.jobs SET payload_digest=$3 WHERE tenant_id=$1 AND job_id=$2",
    )
    .bind(fixture.tenant_id.to_string())
    .bind(&leaves[1].model_job.job_id)
    .bind(named_digest("selected invalid Job digest").to_string())
    .execute(&mut *blocker)
    .await
    .unwrap();
    blocker.commit().await.unwrap();
    let mut claims = tokio::time::timeout(StdDuration::from_secs(10), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    while claims.len() < 3 {
        assert!(
            Instant::now() < until,
            "healthy claims were starved by invalid rows"
        );
        claims.extend(
            repo.claim_model_jobs_current_rounds(claim_command())
                .await
                .unwrap(),
        );
    }
    let expected: BTreeSet<_> = leaves[2..]
        .iter()
        .map(|leaf| leaf.model_job.job_id.clone())
        .collect();
    assert_eq!(
        claims
            .iter()
            .map(|claim| claim.job.job_id.clone())
            .collect::<BTreeSet<_>>(),
        expected
    );
    assert!(claims
        .iter()
        .all(|claim| claim.job.attempt_no == 1 && claim.job.state == "running"));
    assert_eq!(
        active(pool, &fixture).await,
        3,
        "invalid selected Job leaked a Run permit"
    );
    assert_eq!(
        object_snapshot(pool, &fixture.tenant_id, &leaves[0].model_job.job_id).await,
        bad_before
    );
    let late_bad = object_snapshot(pool, &fixture.tenant_id, &leaves[1].model_job.job_id).await;
    assert_eq!(
        late_bad, expected_late_bad,
        "savepoint retained an owner, quota, Receipt or Event mutation"
    );
    assert_eq!(late_bad["job"]["state"], "ready");
    assert_eq!(late_bad["job"]["attempt_no"], 0);
    assert!(late_bad["job"]["quota_reservation_id"].is_null());
    assert_eq!(late_bad["turn"]["state"], "ready");
    assert_eq!(late_bad["node"]["state"], "waiting");
    let successes_after: i64 = sqlx::query_scalar("SELECT successful_claims FROM insight_platform.scheduler_tenant_state WHERE tenant_id=$1 AND work_class='model'")
        .bind(fixture.tenant_id.to_string()).fetch_one(pool).await.unwrap();
    assert_eq!(
        successes_after - successes_before,
        3,
        "fairness charged an invalid candidate"
    );
    for claim in &claims {
        let reserves: i64=sqlx::query_scalar("SELECT count(*) FROM insight_platform.quota_ledger WHERE tenant_id=$1 AND correlation_id=$2 AND entry_kind='reserve'")
            .bind(fixture.tenant_id.to_string()).bind(claim.usage_reservation_id.to_string()).fetch_one(pool).await.unwrap();
        assert_eq!(reserves, 4);
    }
    claims.sort_by(|a, b| a.job.job_id.cmp(&b.job.job_id));
    let now: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
        .fetch_one(pool)
        .await
        .unwrap();
    for (index, claim) in claims.iter().enumerate() {
        sqlx::query("UPDATE insight_platform.jobs SET heartbeat_at=$3,lease_expires_at=$4 WHERE tenant_id=$1 AND job_id=$2")
            .bind(fixture.tenant_id.to_string()).bind(&claim.job.job_id)
            .bind(now-Duration::seconds(10)).bind(now-Duration::seconds(3-index as i64)).execute(pool).await.unwrap();
    }
    let malformed_id = &claims[0].job.job_id;
    sqlx::query(
        "UPDATE insight_platform.jobs SET payload_digest=$3 WHERE tenant_id=$1 AND job_id=$2",
    )
    .bind(fixture.tenant_id.to_string())
    .bind(malformed_id)
    .bind(named_digest("expired invalid Job digest").to_string())
    .execute(pool)
    .await
    .unwrap();
    let malformed_before = object_snapshot(pool, &fixture.tenant_id, malformed_id).await;
    let first = repo
        .drive_expired_model_jobs(recovery(None, 2))
        .await
        .unwrap();
    assert_eq!(first.records.len() + first.diagnostics.len(), 2);
    assert_eq!(first.records.len(), 1);
    assert_eq!(first.records[0].job.job_id, claims[1].job.job_id);
    assert_eq!(first.diagnostics.len(), 1);
    assert_eq!(first.diagnostics[0].item_id.to_string(), *malformed_id);
    assert_eq!(first.diagnostics[0].phase, SafetyScanPhase::JobDecode);
    let cursor = first
        .next_cursor
        .clone()
        .expect("full page advances raw cursor");
    assert_eq!(cursor.item_id.to_string(), claims[1].job.job_id);
    let second = repo
        .drive_expired_model_jobs(recovery(Some(cursor), 2))
        .await
        .unwrap();
    assert_eq!(second.records.len(), 1);
    assert_eq!(second.records[0].job.job_id, claims[2].job.job_id);
    assert!(second.exhausted && second.diagnostics.is_empty());
    assert_eq!(active(pool, &fixture).await, 1);
    assert_eq!(
        object_snapshot(pool, &fixture.tenant_id, malformed_id).await,
        malformed_before
    );
    let counts = support::fixture_durable_counts(pool, &fixture.tenant_id).await;
    let repeated = repo
        .drive_expired_model_jobs(recovery(None, 2))
        .await
        .unwrap();
    assert!(repeated.records.is_empty());
    assert_eq!(repeated.diagnostics.len(), 1);
    assert_eq!(
        support::fixture_durable_counts(pool, &fixture.tenant_id).await,
        counts
    );
    assert_eq!(
        object_snapshot(pool, &fixture.tenant_id, malformed_id).await,
        malformed_before
    );
    // A shared quota inconsistency is fatal, even when only one old object is eligible.
    sqlx::query(
        "UPDATE insight_platform.jobs SET payload_digest=$3 WHERE tenant_id=$1 AND job_id=$2",
    )
    .bind(fixture.tenant_id.to_string())
    .bind(malformed_id)
    .bind(&claims[0].job.payload.digest)
    .execute(pool)
    .await
    .unwrap();
    let concurrent = id(ResourceKind::QuotaAccount, 0x40);
    let reserved:i64=sqlx::query_scalar("SELECT reserved_value FROM insight_platform.quota_accounts WHERE tenant_id=$1 AND quota_account_id=$2")
        .bind(fixture.tenant_id.to_string()).bind(concurrent.to_string()).fetch_one(pool).await.unwrap();
    assert!(reserved > 0);
    sqlx::query("UPDATE insight_platform.quota_accounts SET reserved_value=0 WHERE tenant_id=$1 AND quota_account_id=$2")
        .bind(fixture.tenant_id.to_string()).bind(concurrent.to_string()).execute(pool).await.unwrap();
    let before = object_snapshot(pool, &fixture.tenant_id, malformed_id).await;
    assert!(matches!(
        repo.drive_expired_model_jobs(recovery(None, 2)).await,
        Err(RepositoryError::CorruptRow(_))
    ));
    assert_eq!(
        object_snapshot(pool, &fixture.tenant_id, malformed_id).await,
        before
    );
    assert_eq!(
        support::fixture_durable_counts(pool, &fixture.tenant_id).await,
        counts
    );
    sqlx::query("UPDATE insight_platform.quota_accounts SET reserved_value=$3 WHERE tenant_id=$1 AND quota_account_id=$2")
        .bind(fixture.tenant_id.to_string()).bind(concurrent.to_string()).bind(reserved).execute(pool).await.unwrap();
    let final_page = repo
        .drive_expired_model_jobs(recovery(None, 2))
        .await
        .unwrap();
    assert_eq!(final_page.records.len(), 1);
    assert_eq!(final_page.records[0].job.job_id, *malformed_id);
    assert_eq!(active(pool, &fixture).await, 0);
    for claim in &claims {
        let settlements:i64=sqlx::query_scalar("SELECT count(*) FROM insight_platform.quota_ledger WHERE tenant_id=$1 AND correlation_id=$2 AND entry_kind='settle'")
            .bind(fixture.tenant_id.to_string()).bind(claim.usage_reservation_id.to_string()).fetch_one(pool).await.unwrap();
        assert_eq!(settlements, 4);
    }
    assert_eq!(
        object_snapshot(pool, &fixture.tenant_id, &leaves[1].model_job.job_id).await,
        late_bad
    );
}

async fn verify_cancellation_pages(pool: &PgPool, repo: &PgRepository, original: &Fixture) {
    let fixture = isolated_fixture(pool, original).await;
    let first_leaf = admit(pool, repo, &fixture, 0x7a00).await;
    let second_leaf = admit(pool, repo, &fixture, 0x7c00).await;
    let worker = fresh(ResourceKind::WorkerProcessGeneration);
    let mut claims = Vec::new();
    let until = Instant::now() + StdDuration::from_secs(10);
    while claims.len() < 2 {
        assert!(
            Instant::now() < until,
            "cancellation page fixtures were not claimed"
        );
        let mut command = claim_command();
        command.worker_process_generation_id = worker.clone();
        command.limit = u16::try_from(2 - claims.len()).unwrap();
        command.slots.truncate(usize::from(command.limit));
        claims.extend(repo.claim_model_jobs_current_rounds(command).await.unwrap());
    }
    assert_eq!(
        claims
            .iter()
            .map(|claim| claim.job.job_id.clone())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([first_leaf.model_job.job_id, second_leaf.model_job.job_id])
    );
    claims.sort_by(|left, right| left.job.job_id.cmp(&right.job.job_id));
    for (index, claim) in claims.iter().enumerate() {
        let command = ControlModelTurn {
            audit: audit(
                &fixture.tenant_id,
                &fixture.principal_id,
                0x7e00 + u16::try_from(index).unwrap() * 16,
                'a',
                'b',
            ),
            model_turn_id: claim.turn.model_turn_id.clone(),
            expected_turn_version: claim.turn.version,
            quota_entry_ids: Vec::new(),
            kind: ModelControlKind::Cancel,
        };
        let CommandOutcome::Applied(result) = execute_control(repo, command).await.unwrap() else {
            panic!("fresh cancellation");
        };
        assert_eq!(result.turn.state, ModelTurnState::Cancelling);
    }
    assert_eq!(active(pool, &fixture).await, 2);
    let first_job = &claims[0].job.job_id;
    let second_job = &claims[1].job.job_id;
    // Deterministic ordering retains both live leases and their exact original fences.
    let valid_digest: String = sqlx::query_scalar(
        "SELECT payload_digest FROM insight_platform.jobs WHERE tenant_id=$1 AND job_id=$2",
    )
    .bind(fixture.tenant_id.to_string())
    .bind(first_job)
    .fetch_one(pool)
    .await
    .unwrap();
    sqlx::query("UPDATE insight_platform.jobs SET updated_at=created_at,payload_digest=$3 WHERE tenant_id=$1 AND job_id=$2")
        .bind(fixture.tenant_id.to_string()).bind(first_job).bind(named_digest("invalid live cancelling head").to_string()).execute(pool).await.unwrap();
    let bad_before = object_snapshot(pool, &fixture.tenant_id, first_job).await;
    let counts = support::fixture_durable_counts(pool, &fixture.tenant_id).await;
    let wrong_worker = repo
        .scan_cancelling_model_executions(&fresh(ResourceKind::WorkerProcessGeneration), None, 1)
        .await
        .unwrap();
    assert!(
        wrong_worker.records.is_empty()
            && wrong_worker.diagnostics.is_empty()
            && wrong_worker.exhausted
    );
    let first = repo
        .scan_cancelling_model_executions(&worker, None, 1)
        .await
        .unwrap();
    assert!(first.records.is_empty() && !first.exhausted);
    assert_eq!(first.diagnostics.len(), 1);
    assert_eq!(first.diagnostics[0].item_id.to_string(), *first_job);
    assert_eq!(first.diagnostics[0].phase, SafetyScanPhase::JobDecode);
    let cursor = first.next_cursor.unwrap();
    assert_eq!(cursor.item_id.to_string(), *first_job);
    let second = repo
        .scan_cancelling_model_executions(&worker, Some(cursor), 1)
        .await
        .unwrap();
    assert_eq!(second.records.len(), 1);
    assert!(second.diagnostics.is_empty() && !second.exhausted);
    let next_cursor = second.next_cursor.clone().unwrap();
    assert_eq!(next_cursor.item_id.to_string(), *second_job);
    let controlled = &second.records[0];
    assert_eq!(controlled.job.as_ref().unwrap().job_id, *second_job);
    assert_eq!(
        support::fixture_durable_counts(pool, &fixture.tenant_id).await,
        counts
    );
    assert_eq!(
        object_snapshot(pool, &fixture.tenant_id, first_job).await,
        bad_before
    );
    let projection = controlled.job_projection().unwrap().unwrap();
    let lease = projection.lease.as_ref().unwrap();
    assert_eq!(lease.worker_process_generation_id, worker);
    let command = CommitModelCancellationOutcome {
        audit: worker_audit(&fixture.tenant_id, &worker, 0x7e40, 'c', 'd'),
        model_turn_id: controlled.turn.model_turn_id.clone(),
        job_id: projection.job_id,
        expected_turn_version: controlled.turn.version,
        fence: Some(JobFence {
            expected_version: projection.version,
            worker_process_generation_id: worker.clone(),
            lease_generation: lease.lease_generation,
            token_digest: lease.token_digest.clone(),
        }),
        usage_reservation_id: controlled
            .job
            .as_ref()
            .unwrap()
            .quota_reservation_id
            .as_ref()
            .unwrap()
            .parse()
            .unwrap(),
        quota_entry_ids: (0..4)
            .map(|_| fresh(ResourceKind::QuotaLedgerEntry))
            .collect(),
        measurement: ModelAttemptMeasurement::conservative_dispatched(
            &controlled.turn.payload.admission,
            ModelTurnLimits::from_profile(&checked_in_hard_limit_profile()).unwrap(),
        )
        .unwrap(),
    };
    let mut unfenced = command.clone();
    unfenced.fence = None;
    assert!(matches!(
        execute_cancellation_outcome(repo, unfenced).await,
        Err(RepositoryError::StaleFence)
    ));
    let result = execute_cancellation_outcome(repo, command.clone())
        .await
        .unwrap();
    assert!(
        matches!(result, CommandOutcome::Applied(record) if record.turn.state == ModelTurnState::Cancelled && record.job.state == "cancelled")
    );
    assert_eq!(active(pool, &fixture).await, 1);
    let after_commit = support::fixture_durable_counts(pool, &fixture.tenant_id).await;
    assert!(matches!(
        execute_cancellation_outcome(repo, command.clone())
            .await
            .unwrap(),
        CommandOutcome::Replayed(_)
    ));
    assert_eq!(active(pool, &fixture).await, 1);
    assert_eq!(
        support::fixture_durable_counts(pool, &fixture.tenant_id).await,
        after_commit
    );
    let settlements: i64 = sqlx::query_scalar("SELECT count(*) FROM insight_platform.quota_ledger WHERE tenant_id=$1 AND correlation_id=$2 AND entry_kind='settle'")
        .bind(fixture.tenant_id.to_string()).bind(command.usage_reservation_id.to_string()).fetch_one(pool).await.unwrap();
    assert_eq!(settlements, 4);
    let end = repo
        .scan_cancelling_model_executions(&worker, Some(next_cursor), 1)
        .await
        .unwrap();
    assert!(
        end.records.is_empty()
            && end.diagnostics.is_empty()
            && end.exhausted
            && end.next_cursor.is_none()
    );
    let repeated = repo
        .scan_cancelling_model_executions(&worker, None, 2)
        .await
        .unwrap();
    assert!(repeated.records.is_empty() && repeated.exhausted);
    assert_eq!(repeated.diagnostics.len(), 1);
    assert_eq!(
        object_snapshot(pool, &fixture.tenant_id, first_job).await,
        bad_before
    );
    assert_eq!(
        support::fixture_durable_counts(pool, &fixture.tenant_id).await,
        after_commit
    );
    // Restore only the injected corruption, then let the actual fenced owner settle its permit.
    sqlx::query(
        "UPDATE insight_platform.jobs SET payload_digest=$3 WHERE tenant_id=$1 AND job_id=$2",
    )
    .bind(fixture.tenant_id.to_string())
    .bind(first_job)
    .bind(&valid_digest)
    .execute(pool)
    .await
    .unwrap();
    let remaining = repo
        .scan_cancelling_model_executions(&worker, None, 2)
        .await
        .unwrap();
    assert_eq!(remaining.records.len(), 1);
    let controlled = &remaining.records[0];
    let projection = controlled.job_projection().unwrap().unwrap();
    let lease = projection.lease.as_ref().unwrap();
    let mut cleanup = command;
    cleanup.audit = worker_audit(&fixture.tenant_id, &worker, 0x7e60, 'e', 'f');
    cleanup.model_turn_id = controlled.turn.model_turn_id.clone();
    cleanup.job_id = projection.job_id;
    cleanup.expected_turn_version = controlled.turn.version;
    cleanup.fence = Some(JobFence {
        expected_version: projection.version,
        worker_process_generation_id: worker.clone(),
        lease_generation: lease.lease_generation,
        token_digest: lease.token_digest.clone(),
    });
    cleanup.usage_reservation_id = controlled
        .job
        .as_ref()
        .unwrap()
        .quota_reservation_id
        .as_ref()
        .unwrap()
        .parse()
        .unwrap();
    cleanup.quota_entry_ids = (0..4)
        .map(|_| fresh(ResourceKind::QuotaLedgerEntry))
        .collect();
    cleanup.measurement = ModelAttemptMeasurement::conservative_dispatched(
        &controlled.turn.payload.admission,
        ModelTurnLimits::from_profile(&checked_in_hard_limit_profile()).unwrap(),
    )
    .unwrap();
    assert!(matches!(
        execute_cancellation_outcome(repo, cleanup).await.unwrap(),
        CommandOutcome::Applied(_)
    ));
    assert_eq!(active(pool, &fixture).await, 0);
}

async fn verify_run_control_pages(pool: &PgPool, repo: &PgRepository, original: &Fixture) {
    let first = isolated_fixture(pool, original).await;
    let second = isolated_fixture(pool, original).await;
    let first_leaf = admit(pool, repo, &first, 0xd000).await;
    let second_leaf = admit(pool, repo, &second, 0xd200).await;
    for (fixture, base) in [(&first, 0xd400), (&second, 0xd410)] {
        let (version,generation):(i64,i64)=sqlx::query_as("SELECT version,cancel_generation FROM insight_platform.runs WHERE tenant_id=$1 AND run_id=$2")
            .bind(fixture.tenant_id.to_string()).bind(fixture.run_id.to_string()).fetch_one(pool).await.unwrap();
        let mut tx = repo.begin_run_transaction().await.unwrap();
        tx.request_run_cancel(RequestRunCancel {
            audit: audit(&fixture.tenant_id, &fixture.principal_id, base, 'a', 'b'),
            run_id: fixture.run_id.clone(),
            expected_run_version: version,
            expected_cancel_generation: u64::try_from(generation).unwrap(),
            reason_code: "isolated_model_run_cancel".into(),
        })
        .await
        .unwrap();
        tx.commit().await.unwrap();
    }
    leaf_convergence_isolation::verify(
        pool,
        repo,
        [
            leaf_convergence_isolation::Leaf {
                tenant: first.tenant_id.clone(),
                run: first.run_id.clone(),
                job: first_leaf.model_job.job_id.parse().unwrap(),
                expected_state: "cancelled",
                expected_settlements: 0,
            },
            leaf_convergence_isolation::Leaf {
                tenant: second.tenant_id.clone(),
                run: second.run_id.clone(),
                job: second_leaf.model_job.job_id.parse().unwrap(),
                expected_state: "cancelled",
                expected_settlements: 0,
            },
        ],
    )
    .await;
    let until = Instant::now() + StdDuration::from_secs(10);
    let mut after = None;
    loop {
        let terminal:i64=sqlx::query_scalar("SELECT count(*) FROM insight_platform.runs WHERE tenant_id=$1 AND run_id=ANY($2) AND state='cancelled'")
            .bind(first.tenant_id.to_string()).bind(vec![first.run_id.to_string(),second.run_id.to_string()]).fetch_one(pool).await.unwrap();
        if terminal == 2 {
            break;
        }
        assert!(
            Instant::now() < until,
            "Model Runs did not close through bounded convergence pages"
        );
        let mut command = leaf_convergence_isolation::command();
        command.after = after;
        let page = repo.drive_orchestration_convergence(command).await.unwrap();
        after = if page.exhausted {
            None
        } else {
            page.next_cursor
        };
    }
    for (fixture, leaf) in [(&first, &first_leaf), (&second, &second_leaf)] {
        let actual:(String,i32,Option<String>)=sqlx::query_as("SELECT state,attempt_no,quota_reservation_id FROM insight_platform.jobs WHERE tenant_id=$1 AND job_id=$2")
            .bind(fixture.tenant_id.to_string()).bind(&leaf.model_job.job_id).fetch_one(pool).await.unwrap();
        assert_eq!(actual, ("cancelled".into(), 0, None));
        assert_eq!(active(pool, fixture).await, 0);
        let run_state: String = sqlx::query_scalar(
            "SELECT state FROM insight_platform.runs WHERE tenant_id=$1 AND run_id=$2",
        )
        .bind(fixture.tenant_id.to_string())
        .bind(fixture.run_id.to_string())
        .fetch_one(pool)
        .await
        .unwrap();
        assert_eq!(run_state, "cancelled");
        model_public_events::assert_projection(
            pool,
            repo,
            fixture,
            &["model.cancelled", "run.cancelled", "node.cancelled"],
        )
        .await;
    }
}
