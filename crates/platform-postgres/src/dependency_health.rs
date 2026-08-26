//! Bounded, read-only PostgreSQL dependency health sampling for production processes.

use sqlx::PgPool;
use std::{sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;

pub const POSTGRES_HEALTH_SAMPLE_INTERVAL: Duration = Duration::from_secs(15);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PostgresHealthOutcome {
    Success,
    Failure,
}

/// Receives only the fixed health outcome. Database URL/name, role, SQL text and error details are
/// deliberately absent from this port.
pub trait PostgresHealthObserver: Send + Sync {
    fn observe(&self, outcome: PostgresHealthOutcome);
}

pub async fn observe_postgres_health_once(pool: &PgPool, observer: &dyn PostgresHealthObserver) {
    let outcome = match sqlx::query_scalar::<_, i64>("SELECT 1::bigint")
        .fetch_one(pool)
        .await
    {
        Ok(1) => PostgresHealthOutcome::Success,
        Ok(_) | Err(_) => PostgresHealthOutcome::Failure,
    };
    observer.observe(outcome);
}

pub async fn run_postgres_health_sampler(
    pool: PgPool,
    observer: Arc<dyn PostgresHealthObserver>,
    cancellation: CancellationToken,
) {
    let mut interval = tokio::time::interval(POSTGRES_HEALTH_SAMPLE_INTERVAL);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            biased;
            _ = cancellation.cancelled() => return,
            _ = interval.tick() => {
                tokio::select! {
                    biased;
                    _ = cancellation.cancelled() => return,
                    _ = observe_postgres_health_once(&pool, observer.as_ref()) => {}
                }
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::postgres::PgPoolOptions;
    use std::sync::Mutex;

    #[derive(Default)]
    struct RecordingObserver(Mutex<Vec<PostgresHealthOutcome>>);

    impl PostgresHealthObserver for RecordingObserver {
        fn observe(&self, outcome: PostgresHealthOutcome) {
            self.0.lock().unwrap().push(outcome);
        }
    }

    #[tokio::test]
    async fn one_shot_probe_reports_only_a_fixed_failure_for_an_unavailable_pool() {
        let pool = PgPoolOptions::new()
            .acquire_timeout(Duration::from_millis(10))
            .connect_lazy("postgres://unused:unused@127.0.0.1:1/unused")
            .unwrap();
        let observer = RecordingObserver::default();
        observe_postgres_health_once(&pool, &observer).await;
        assert_eq!(
            *observer.0.lock().unwrap(),
            vec![PostgresHealthOutcome::Failure]
        );
    }

    #[tokio::test]
    async fn sampler_stops_without_observing_after_pre_cancel() {
        let pool = PgPoolOptions::new()
            .connect_lazy("postgres://unused:unused@127.0.0.1:1/unused")
            .unwrap();
        let observer = Arc::new(RecordingObserver::default());
        let cancellation = CancellationToken::new();
        cancellation.cancel();
        run_postgres_health_sampler(pool, observer.clone(), cancellation).await;
        assert!(observer.0.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn one_shot_probe_reports_success_against_a_real_database_when_configured() {
        let Ok(database_url) = std::env::var("PLATFORM_TEST_DATABASE_URL") else {
            return;
        };
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(&database_url)
            .await
            .unwrap();
        let observer = RecordingObserver::default();
        observe_postgres_health_once(&pool, &observer).await;
        assert_eq!(
            *observer.0.lock().unwrap(),
            vec![PostgresHealthOutcome::Success]
        );
    }
}
