//! Production PostgreSQL claim -> Worker driver admission across a binary build change.
use super::*;
use insight_platform_model_worker::{
    ClaimedModelJob, ExecuteClaimedModelJob, ModelCancellationSource, ModelExecutionDisposition,
    ModelJobCommandExecutor, ModelWorkerDriver, ModelWorkerDriverConfig, ModelWorkerDriverTiming,
    UuidModelWorkerIdentityFactory,
};
use insight_platform_worker::LocalWorkerPools;

struct RecordingExecutor(std::sync::Mutex<Option<ExecuteClaimedModelJob<ClaimedModelExecution>>>);
#[async_trait::async_trait]
impl ModelJobCommandExecutor<ClaimedModelExecution> for RecordingExecutor {
    async fn execute(
        &self,
        command: ExecuteClaimedModelJob<ClaimedModelExecution>,
    ) -> ModelExecutionDisposition {
        *self.0.lock().unwrap() = Some(command);
        ModelExecutionDisposition::Settled
    }
}

pub(super) async fn verify() {
    let url =
        std::env::var("PLATFORM_LIVE_TEST_DATABASE_URL").expect("fresh fixture database required");
    assert!(url
        .rsplit('/')
        .next()
        .unwrap()
        .starts_with("insight_live_fixture_"));
    let pool = PgPoolOptions::new()
        .max_connections(8)
        .connect(&url)
        .await
        .unwrap();
    insight_platform_postgres::provision_schema(&pool)
        .await
        .unwrap();
    let repository = Arc::new(PgRepository::new(pool));
    let fixture = seed_fixture(repository.pool(), &repository).await;
    let command = command_for_node(&fixture, &fixture.primary_node_id, 0x100);
    let admitted = match execute_create(&repository, command.clone()).await.unwrap() {
        CommandOutcome::Applied(v) => v,
        _ => panic!("fresh admission"),
    };
    let historical = admitted
        .payload
        .admission
        .provider
        .installed_adapter
        .worker_manifest_digest
        .clone();
    execute_prepare(
        &repository,
        PrepareModelDispatch {
            audit: audit(&fixture.tenant_id, &fixture.principal_id, 0x121, '9', 'a'),
            model_turn_id: command.model_turn_id.clone(),
            expected_turn_version: admitted.version,
            job_id: id(ResourceKind::Job, 0x120),
            scheduled_at: Utc::now() - Duration::seconds(1),
        },
    )
    .await
    .unwrap();
    let mut manifest = production_model_worker_manifest();
    manifest.worker_build_digest = named_digest("new-model-worker-build");
    let current_digest = manifest.canonical_digest().unwrap();
    assert_ne!(current_digest, historical);
    let generation = id(ResourceKind::WorkerProcessGeneration, 0x130);
    let pools = LocalWorkerPools::new(manifest.clone(), generation.clone()).unwrap();
    let executor = Arc::new(RecordingExecutor(std::sync::Mutex::new(None)));
    let profile = insight_platform_contracts::checked_in_hard_limit_profile();
    let driver = ModelWorkerDriver::new(
        repository.clone(),
        executor.clone(),
        Arc::new(UuidModelWorkerIdentityFactory),
        pools,
        ModelWorkerDriverConfig::from_profile(
            &profile,
            StdDuration::from_secs(300),
            ModelWorkerDriverTiming {
                safety_scan_interval: StdDuration::from_millis(100),
                claim_failure_backoff: StdDuration::from_millis(10),
                drain_grace: StdDuration::from_secs(1),
            },
        )
        .unwrap(),
    )
    .unwrap();
    let mut active = tokio::task::JoinSet::new();
    let mut claimed = 0;
    for _ in 0..(usize::from(insight_platform_contracts::SCHEDULER_PARTITION_COUNT) * 8) {
        claimed = driver.drive_once(&mut active).await.unwrap();
        if claimed != 0 {
            break;
        }
    }
    assert_eq!(claimed, 1);
    assert_eq!(
        active.join_next().await.unwrap().unwrap(),
        ModelExecutionDisposition::Settled
    );
    let dispatched = executor
        .0
        .lock()
        .unwrap()
        .take()
        .expect("driver must dispatch its committed claim");
    assert_eq!(dispatched.worker_manifest_digest, current_digest);
    assert_eq!(
        dispatched
            .claim
            .turn
            .payload
            .admission
            .provider
            .installed_adapter
            .worker_manifest_digest,
        historical
    );
    assert_eq!(
        dispatched
            .claim
            .claim_binding()
            .unwrap()
            .worker_build_digest,
        manifest.worker_build_digest
    );
    let mut missing = dispatched.claim.clone();
    missing.job.attempt_build_digest = None;
    assert!(missing.claim_binding().is_err());
    execute_control(
        &repository,
        ControlModelTurn {
            audit: audit(&fixture.tenant_id, &fixture.principal_id, 0x160, 'b', 'c'),
            model_turn_id: command.model_turn_id,
            expected_turn_version: dispatched.claim.turn.version,
            quota_entry_ids: vec![],
            kind: ModelControlKind::Cancel,
        },
    )
    .await
    .unwrap();
    let scanned = repository
        .scan_model_cancellations(
            &generation,
            None,
            16,
            ModelTurnLimits::from_profile(&profile).unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(scanned.records.len(), 1);
    assert!(scanned.diagnostics.is_empty());
    assert_eq!(
        scanned.records[0].worker_build_digest,
        manifest.worker_build_digest
    );
}
