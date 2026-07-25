//! Root facade for the runtime-owned RunService.

pub use insight_runtime::run_service::*;

#[cfg(test)]
mod public_event_multiruntime_tests {
    use std::{
        sync::Arc,
        time::{Duration, Instant},
    };

    use serde_json::json;
    use sqlx::{postgres::PgPoolOptions, AssertSqlSafe, PgPool};
    use uuid::Uuid;

    use super::*;
    use crate::engine::{
        repository::{
            DurableRepository, PostgresDurableRepository, PublicEventOutboxRepository,
            VersionedPlan,
        },
        ContentHash, DefinitionRevisionId, DeploymentRevisionId,
        LocalContentAddressedArtifactStore,
    };
    use insight_durable::model::adapter as durable_model_adapter;
    use insight_durable::{CreateRunCommand, PublicRunAttachment};
    use insight_engine::{
        events::protocol::RunEventType, worker::WorkerExecutorRegistry, AdmissionState,
        ExecutionEventContext, ExecutionEventPayload, PendingExecutionEvent, PublicEventPayload,
        RunId, RunLifecycle as EngineRunLifecycle, TransitionKey, TransitionOutcome,
    };
    use insight_runtime::internal as runtime_internal;

    fn plan(suffix: &str) -> VersionedPlan {
        insight_durable::model::adapter::versioned_plan_for_test(
            format!("definition_public_listener_{suffix}"),
            format!("agent_public_listener_{suffix}"),
            "Public listener test",
            DefinitionRevisionId::new(format!("definition_revision_{suffix}")).unwrap(),
            DeploymentRevisionId::new(format!("deployment_revision_{suffix}")).unwrap(),
            ContentHash::from_bytes(format!("public-listener-plan-{suffix}").as_bytes()),
            ContentHash::from_bytes(format!("public-listener-binding-{suffix}").as_bytes()),
            "compiler-3.0.0",
            "expression-3.0.0",
            json!({"kind": "structured"}),
            json!({"nodes": []}),
            json!({}),
            json!({"model": "fixed"}),
            json!({"worker": "v1"}),
        )
        .unwrap()
    }

    fn config() -> RunServiceConfig {
        let mut config = RunServiceConfig::production(8, 1, 1, 32);
        config.pump_interval = Duration::from_secs(3_600);
        config.run_timeout = Duration::from_secs(3_600);
        config
    }

    fn single_process_development_config() -> RunServiceConfig {
        let mut config = RunServiceConfig::single_process_development(8, 1, 1, 32);
        config.pump_interval = Duration::from_secs(3_600);
        config.run_timeout = Duration::from_secs(3_600);
        config
    }

    fn key(suffix: &str, label: &str) -> TransitionKey {
        TransitionKey::derive("runtime.public-event.pg.test", &[suffix, label]).unwrap()
    }

    fn lifecycle_event(run_id: &RunId, lifecycle: EngineRunLifecycle) -> PendingExecutionEvent {
        PendingExecutionEvent::new(
            ExecutionEventContext::for_run(run_id.clone()),
            ExecutionEventPayload::RunLifecycleChanged { lifecycle },
        )
        .unwrap()
    }

    async fn wait_for_listener_pid(
        admin: &PgPool,
        application_name: &str,
        previous_pid: Option<i32>,
    ) -> i32 {
        let deadline = Instant::now() + Duration::from_secs(8);
        loop {
            let pid = sqlx::query_scalar::<_, i32>(
                "SELECT pid FROM pg_stat_activity
                 WHERE datname = current_database() AND application_name = $1
                   AND query ILIKE '%insight_agent_public_events%'
                 ORDER BY backend_start DESC LIMIT 1",
            )
            .bind(application_name)
            .fetch_optional(admin)
            .await
            .unwrap();
            if let Some(pid) = pid.filter(|pid| Some(*pid) != previous_pid) {
                return pid;
            }
            assert!(
                Instant::now() < deadline,
                "PostgreSQL listener did not connect"
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    #[tokio::test]
    async fn sqlite_live_event_uses_canonical_envelope_occurred_at() {
        let repository = Arc::new(crate::test_database::sqlite_in_memory_repository().await);
        let service = RunService::start(
            DeployedAgentCatalog::new(Vec::new()).unwrap(),
            repository.clone() as Arc<dyn ProductionRunRepository>,
            WorkerExecutorRegistry::new(),
            single_process_development_config(),
        )
        .await
        .unwrap();

        let suffix = Uuid::new_v4().simple().to_string();
        let plan = plan(&suffix);
        repository.install_versioned_plan(&plan).await.unwrap();
        let run_id = RunId::new(format!("run_public_time_{suffix}")).unwrap();
        repository
            .create_run(
                key(&suffix, "time.create"),
                CreateRunCommand::new(run_id.clone(), &plan, json!({"question": "time"}))
                    .unwrap()
                    .with_public_metadata("request-public-time", PublicRunAttachment::Attached)
                    .unwrap(),
            )
            .await
            .unwrap();
        let committed = repository
            .commit_run_transition(
                key(&suffix, "time.start"),
                durable_model_adapter::run_transition_nonterminal(
                    run_id.clone(),
                    0,
                    EngineRunLifecycle::Created,
                    AdmissionState::Open,
                    EngineRunLifecycle::Active,
                    AdmissionState::Open,
                    lifecycle_event(&run_id, EngineRunLifecycle::Active),
                    Some(durable_model_adapter::public_event_intent(
                        PublicEventPayload::RunStarted,
                    )),
                )
                .unwrap(),
            )
            .await
            .unwrap();
        let public_event_id = committed
            .committed_result()
            .unwrap()
            .public_event_id()
            .unwrap()
            .to_owned();
        let created_claim = repository
            .claim_public_events("sqlite-time-dispatcher", 30, 1)
            .await
            .unwrap()
            .pop()
            .unwrap();
        assert_eq!(
            created_claim.safe_envelope().payload(),
            &PublicEventPayload::RunCreated
        );
        assert!(repository
            .publish_public_event(&created_claim, 60)
            .await
            .unwrap());
        let claim = repository
            .claim_public_events("sqlite-time-dispatcher", 30, 1)
            .await
            .unwrap()
            .pop()
            .unwrap();
        assert!(repository.publish_public_event(&claim, 60).await.unwrap());
        let published = repository
            .load_published_public_event(&public_event_id)
            .await
            .unwrap()
            .unwrap();

        let live = runtime_internal::run_event(&service, &run_id, published.safe_envelope())
            .await
            .unwrap();
        assert_eq!(
            live.timestamp,
            published.safe_envelope().occurred_at().to_owned()
        );
        service.shutdown(Duration::from_secs(1)).await.unwrap();
    }

    #[tokio::test]
    async fn sqlite_subscription_uses_durable_order_when_terminal_hint_arrives_first() {
        let repository = Arc::new(crate::test_database::sqlite_in_memory_repository().await);
        let service = RunService::start(
            DeployedAgentCatalog::new(Vec::new()).unwrap(),
            repository.clone() as Arc<dyn ProductionRunRepository>,
            WorkerExecutorRegistry::new(),
            single_process_development_config(),
        )
        .await
        .unwrap();

        let suffix = Uuid::new_v4().simple().to_string();
        let plan = plan(&suffix);
        repository.install_versioned_plan(&plan).await.unwrap();
        let run_id = RunId::new(format!("run_public_reverse_{suffix}")).unwrap();
        repository
            .create_run(
                key(&suffix, "reverse.create"),
                CreateRunCommand::new(run_id.clone(), &plan, json!({"question": "reverse"}))
                    .unwrap()
                    .with_public_metadata("request-public-reverse", PublicRunAttachment::Attached)
                    .unwrap(),
            )
            .await
            .unwrap();
        repository
            .commit_run_transition(
                key(&suffix, "reverse.start"),
                durable_model_adapter::run_transition_nonterminal(
                    run_id.clone(),
                    0,
                    EngineRunLifecycle::Created,
                    AdmissionState::Open,
                    EngineRunLifecycle::Active,
                    AdmissionState::Open,
                    lifecycle_event(&run_id, EngineRunLifecycle::Active),
                    Some(durable_model_adapter::public_event_intent(
                        PublicEventPayload::RunStarted,
                    )),
                )
                .unwrap(),
            )
            .await
            .unwrap();
        repository
            .commit_run_transition(
                key(&suffix, "reverse.completing"),
                durable_model_adapter::run_transition_nonterminal(
                    run_id.clone(),
                    1,
                    EngineRunLifecycle::Active,
                    AdmissionState::Open,
                    EngineRunLifecycle::Completing,
                    AdmissionState::Draining,
                    lifecycle_event(&run_id, EngineRunLifecycle::Completing),
                    None,
                )
                .unwrap(),
            )
            .await
            .unwrap();
        let terminal = repository
            .commit_run_transition(
                key(&suffix, "reverse.terminal"),
                durable_model_adapter::run_transition_terminal_success(
                    run_id.clone(),
                    2,
                    json!({"answer": "done"}),
                    lifecycle_event(&run_id, EngineRunLifecycle::Succeeded),
                    durable_model_adapter::public_event_intent(PublicEventPayload::RunCompleted),
                )
                .unwrap(),
            )
            .await
            .unwrap();
        let terminal_id = terminal
            .committed_result()
            .unwrap()
            .public_event_id()
            .unwrap()
            .to_owned();

        for expected in [
            PublicEventPayload::RunCreated,
            PublicEventPayload::RunStarted,
            PublicEventPayload::RunCompleted,
        ] {
            let claim = repository
                .claim_public_events("sqlite-reverse-dispatcher", 30, 10)
                .await
                .unwrap()
                .pop()
                .unwrap();
            assert_eq!(claim.safe_envelope().payload(), &expected);
            assert!(repository.publish_public_event(&claim, 60).await.unwrap());
        }

        let mut subscription = runtime_internal::subscription(&service, &run_id);

        // Simulate the worst dispatcher race: the terminal hint wins and all
        // earlier hints are lost. The subscriber must still drain the durable
        // order, and the wake sender must remain open until that drain reaches
        // the terminal position.
        runtime_internal::deliver_published_public_event(&service, &terminal_id)
            .await
            .unwrap();
        assert!(runtime_internal::has_live_subscription(&service, &run_id));
        let created = subscription.recv().await.unwrap();
        let started = subscription.recv().await.unwrap();
        assert!(runtime_internal::has_live_subscription(&service, &run_id));
        let completed = subscription.recv().await.unwrap();
        assert_eq!(created.event_type, RunEventType::RunCreated);
        assert_eq!(started.event_type, RunEventType::RunStarted);
        assert_eq!(completed.event_type, RunEventType::RunCompleted);
        assert!(created.seq < started.seq && started.seq < completed.seq);
        assert_eq!(subscription.last_seq(), completed.seq);
        assert!(!runtime_internal::has_live_subscription(&service, &run_id));
        assert_eq!(
            subscription.recv().await.unwrap_err().code(),
            "SUBSCRIPTION_TERMINAL"
        );

        service.shutdown(Duration::from_secs(1)).await.unwrap();
    }

    #[tokio::test]
    async fn postgres_listener_reconnects_and_reversed_remote_hints_remain_ordered() {
        let Some(database_url) = std::env::var("TEST_POSTGRES_URL").ok() else {
            return;
        };
        let suffix = Uuid::new_v4().simple().to_string();
        let schema = format!("public_listener_{}", &suffix[..16]);
        let app_a = format!("pub_a_{}", &suffix[..12]);
        let app_b = format!("pub_b_{}", &suffix[..12]);
        let admin = PgPoolOptions::new()
            .max_connections(2)
            .connect(&database_url)
            .await
            .unwrap();
        let version: String = sqlx::query_scalar("SHOW server_version_num")
            .fetch_one(&admin)
            .await
            .unwrap();
        let version = version.parse::<u32>().unwrap();
        assert!(
            (160_000..170_000).contains(&version),
            "public event multi-runtime gate requires PostgreSQL 16"
        );
        sqlx::query(AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
            .execute(&admin)
            .await
            .unwrap();
        let separator = if database_url.contains('?') { '&' } else { '?' };
        let scoped = format!("{database_url}{separator}options=-csearch_path%3D{schema}");
        crate::test_database::provision_postgres_url(&scoped).await;
        let repository_a = Arc::new(
            PostgresDurableRepository::connect(&format!("{scoped}&application_name={app_a}"))
                .await
                .unwrap(),
        );
        let repository_b = Arc::new(
            PostgresDurableRepository::connect(&format!("{scoped}&application_name={app_b}"))
                .await
                .unwrap(),
        );

        let artifact_directory = tempfile::tempdir().unwrap();
        let artifact_store = Arc::new(
            LocalContentAddressedArtifactStore::open_shared(
                artifact_directory.path().join("objects"),
                1,
                "public_listener",
            )
            .await
            .unwrap(),
        );
        let service_a = RunService::start_with_artifact_store(
            DeployedAgentCatalog::new(Vec::new()).unwrap(),
            repository_a.clone() as Arc<dyn ProductionRunRepository>,
            WorkerExecutorRegistry::new(),
            artifact_store.clone(),
            config(),
        )
        .await
        .unwrap();
        let service_b = RunService::start_with_artifact_store(
            DeployedAgentCatalog::new(Vec::new()).unwrap(),
            repository_b.clone() as Arc<dyn ProductionRunRepository>,
            WorkerExecutorRegistry::new(),
            artifact_store,
            config(),
        )
        .await
        .unwrap();
        let listener_pid = wait_for_listener_pid(&admin, &app_b, None).await;

        let plan = plan(&suffix);
        repository_a.install_versioned_plan(&plan).await.unwrap();
        let run_id = RunId::new(format!("run_public_listener_{suffix}")).unwrap();
        repository_a
            .create_run(
                key(&suffix, "create"),
                CreateRunCommand::new(run_id.clone(), &plan, json!({"question": "listen"}))
                    .unwrap()
                    .with_public_metadata("request-public-listener", PublicRunAttachment::Attached)
                    .unwrap(),
            )
            .await
            .unwrap();
        let mut subscription = runtime_internal::subscription(&service_b, &run_id);

        let start = repository_a
            .commit_run_transition(
                key(&suffix, "start"),
                durable_model_adapter::run_transition_nonterminal(
                    run_id.clone(),
                    0,
                    EngineRunLifecycle::Created,
                    AdmissionState::Open,
                    EngineRunLifecycle::Active,
                    AdmissionState::Open,
                    lifecycle_event(&run_id, EngineRunLifecycle::Active),
                    Some(durable_model_adapter::public_event_intent(
                        PublicEventPayload::RunStarted,
                    )),
                )
                .unwrap(),
            )
            .await
            .unwrap();
        let start_id = match start {
            TransitionOutcome::Committed { result } => result.public_event_id().unwrap().to_owned(),
            TransitionOutcome::ExactReplay { authoritative } => {
                authoritative.public_event_id().unwrap().to_owned()
            }
            TransitionOutcome::StateConflict | TransitionOutcome::StaleLease => {
                panic!("start transition must commit")
            }
        };
        runtime_internal::flush_public_events(&service_a)
            .await
            .unwrap();
        runtime_internal::flush_public_events(&service_a)
            .await
            .unwrap();

        assert!(
            sqlx::query_scalar::<_, bool>("SELECT pg_terminate_backend($1)")
                .bind(listener_pid)
                .fetch_one(&admin)
                .await
                .unwrap()
        );
        let _reconnected_pid = wait_for_listener_pid(&admin, &app_b, Some(listener_pid)).await;

        repository_a
            .commit_run_transition(
                key(&suffix, "completing"),
                durable_model_adapter::run_transition_nonterminal(
                    run_id.clone(),
                    1,
                    EngineRunLifecycle::Active,
                    AdmissionState::Open,
                    EngineRunLifecycle::Completing,
                    AdmissionState::Draining,
                    lifecycle_event(&run_id, EngineRunLifecycle::Completing),
                    None,
                )
                .unwrap(),
            )
            .await
            .unwrap();
        let terminal = repository_a
            .commit_run_transition(
                key(&suffix, "terminal"),
                durable_model_adapter::run_transition_terminal_success(
                    run_id.clone(),
                    2,
                    json!({"answer": "done"}),
                    lifecycle_event(&run_id, EngineRunLifecycle::Succeeded),
                    durable_model_adapter::public_event_intent(PublicEventPayload::RunCompleted),
                )
                .unwrap(),
            )
            .await
            .unwrap();
        let terminal_id = match terminal {
            TransitionOutcome::Committed { result } => result.public_event_id().unwrap().to_owned(),
            TransitionOutcome::ExactReplay { authoritative } => {
                authoritative.public_event_id().unwrap().to_owned()
            }
            TransitionOutcome::StateConflict | TransitionOutcome::StaleLease => {
                panic!("terminal transition must commit")
            }
        };
        runtime_internal::flush_public_events(&service_a)
            .await
            .unwrap();

        // Force the remote runtime's terminal handler to win locally, then
        // emit PostgreSQL hints in terminal-before-start order. Publication
        // notifications may also be duplicated or already buffered; none of
        // those process-local arrival orders may control subscriber order.
        runtime_internal::deliver_published_public_event(&service_b, &terminal_id)
            .await
            .unwrap();
        sqlx::query("SELECT pg_notify($1, $2)")
            .bind(insight_durable::PUBLIC_EVENT_NOTIFY_CHANNEL)
            .bind(&terminal_id)
            .execute(&admin)
            .await
            .unwrap();
        sqlx::query("SELECT pg_notify($1, $2)")
            .bind(insight_durable::PUBLIC_EVENT_NOTIFY_CHANNEL)
            .bind(&start_id)
            .execute(&admin)
            .await
            .unwrap();

        let created = tokio::time::timeout(Duration::from_secs(5), subscription.recv())
            .await
            .unwrap()
            .unwrap();
        let started = tokio::time::timeout(Duration::from_secs(5), subscription.recv())
            .await
            .unwrap()
            .unwrap();
        let completed = tokio::time::timeout(Duration::from_secs(5), subscription.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(created.event_type, RunEventType::RunCreated);
        assert_eq!(started.event_type, RunEventType::RunStarted);
        assert_eq!(completed.event_type, RunEventType::RunCompleted);
        assert!(created.seq < started.seq && started.seq < completed.seq);
        assert_eq!(
            subscription.recv().await.unwrap_err().code(),
            "SUBSCRIPTION_TERMINAL"
        );

        service_a.shutdown(Duration::from_secs(2)).await.unwrap();
        service_b.shutdown(Duration::from_secs(2)).await.unwrap();
        drop(service_a);
        drop(service_b);
        drop(repository_a);
        drop(repository_b);
        sqlx::query(AssertSqlSafe(format!("DROP SCHEMA {schema} CASCADE")))
            .execute(&admin)
            .await
            .unwrap();
    }
}
