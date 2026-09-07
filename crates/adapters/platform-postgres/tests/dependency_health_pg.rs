use insight_platform_postgres::dependency_health::{
    observe_postgres_health_once, PostgresHealthObserver, PostgresHealthOutcome,
};
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
async fn one_shot_probe_reports_success_against_a_real_database() {
    let database_url = std::env::var("PLATFORM_TEST_DATABASE_URL")
        .expect("PLATFORM_TEST_DATABASE_URL is required for the PostgreSQL integration target");
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
