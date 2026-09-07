use super::*;

pub(super) async fn advance_until_domain(repository: &PgRepository, query: &ResourceId) {
    let until = std::time::Instant::now() + StdDuration::from_secs(10);
    let mut after = None;
    loop {
        assert!(
            std::time::Instant::now() < until,
            "Context owner not reached through bounded convergence pages"
        );
        let mut command = control_convergence::command();
        command.after = after;
        let page = repository
            .drive_orchestration_convergence(command)
            .await
            .unwrap();
        if page.records.iter().any(|step|matches!(&step.step,insight_platform_orchestrator::store::OrchestrationConvergenceStep::Domain{owner_id,state} if owner_id==&query.to_string() && state=="cancelled")) {return;}
        after = if page.exhausted {
            None
        } else {
            page.next_cursor
        };
    }
}

pub(super) async fn finish_run(pool: &PgPool, repository: &PgRepository, fixture: &Fixture) {
    let until = std::time::Instant::now() + StdDuration::from_secs(10);
    let mut after = None;
    loop {
        let state: String = sqlx::query_scalar(
            "SELECT state FROM insight_platform.runs WHERE tenant_id=$1 AND run_id=$2",
        )
        .bind(fixture.tenant_id.to_string())
        .bind(fixture.run_id.to_string())
        .fetch_one(pool)
        .await
        .unwrap();
        if state == "cancelled" {
            return;
        }
        assert!(
            std::time::Instant::now() < until,
            "Context Run not closed through bounded convergence pages"
        );
        let mut command = control_convergence::command();
        command.after = after;
        let page = repository
            .drive_orchestration_convergence(command)
            .await
            .unwrap();
        after = if page.exhausted {
            None
        } else {
            page.next_cursor
        };
    }
}

pub(super) async fn verify(pool: &PgPool, repository: &PgRepository) {
    let mut leaves = Vec::new();
    let mut fixtures = Vec::new();
    for (namespace, running) in [(0xb2f7, false), (0xb3f7, true)] {
        CONTEXT_FIXTURE_NAMESPACE.store(namespace, Ordering::SeqCst);
        let fixture =
            seed_fixture_with_backend(pool, repository, ContextFixtureBackend::SqlCatalog).await;
        let created = match execute_create(repository, create_command(&fixture, 0x100))
            .await
            .unwrap()
        {
            CommandOutcome::Applied(record) | CommandOutcome::Replayed(record) => record,
        };
        let job = id(ResourceKind::Job, 0x120);
        execute_prepare(
            repository,
            PrepareContextDispatch {
                audit: audit(
                    &fixture.tenant_id,
                    &fixture.principal_id,
                    0x121,
                    "isolated-control-prepare",
                ),
                context_query_id: created.context_query_id.clone(),
                expected_query_version: created.version,
                job_id: job.clone(),
                scheduled_at: created.created_at,
            },
        )
        .await
        .unwrap();
        park_direct_context_leaf(pool, &fixture, &created.context_query_id, &job, 0).await;
        if running {
            claim(repository, &fixture, job.clone(), 0x130).await;
        }
        let version: i64 = sqlx::query_scalar(
            "SELECT version FROM insight_platform.runs WHERE tenant_id=$1 AND run_id=$2",
        )
        .bind(fixture.tenant_id.to_string())
        .bind(fixture.run_id.to_string())
        .fetch_one(pool)
        .await
        .unwrap();
        let mut tx = repository.begin_run_transaction().await.unwrap();
        tx.request_run_cancel(insight_platform_orchestrator::RequestRunCancel {
            audit: audit(
                &fixture.tenant_id,
                &fixture.principal_id,
                0x170,
                "isolated-run-cancel",
            ),
            run_id: fixture.run_id.clone(),
            expected_run_version: version,
            expected_cancel_generation: 0,
            reason_code: "isolation_read_cancel".into(),
        })
        .await
        .unwrap();
        tx.commit().await.unwrap();
        leaves.push(leaf_convergence_isolation::Leaf {
            tenant: fixture.tenant_id.clone(),
            run: fixture.run_id.clone(),
            job,
            expected_state: "cancelled",
            expected_settlements: if running { 3 } else { 0 },
        });
        fixtures.push(fixture);
    }
    leaf_convergence_isolation::verify(pool, repository, leaves.try_into().ok().unwrap()).await;
    for fixture in fixtures {
        finish_run(pool, repository, &fixture).await;
        let actual:(String,i32)=sqlx::query_as("SELECT state,active_work_count FROM insight_platform.runs WHERE tenant_id=$1 AND run_id=$2")
            .bind(fixture.tenant_id.to_string()).bind(fixture.run_id.to_string()).fetch_one(pool).await.unwrap();
        assert_eq!(actual, ("cancelled".into(), 0));
        let reserved:i64=sqlx::query_scalar("SELECT sum(reserved_value)::bigint FROM insight_platform.quota_accounts WHERE tenant_id=$1 AND work_class='context'")
            .bind(fixture.tenant_id.to_string()).fetch_one(pool).await.unwrap();
        assert_eq!(reserved, 0);
    }
}
