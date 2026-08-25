//! Bounded, low-cardinality process telemetry for Platform v2 services.
//!
//! Metric labels are frozen at installation. Request paths, tenant identifiers, resource
//! identifiers, payloads, and error text can never become emitted label values.

use axum::{
    extract::{Extension, Request},
    http::{header::CACHE_CONTROL, HeaderValue, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::get,
    Router,
};
use insight_platform_worker::LocalWorkerPools;
use std::{
    fmt,
    sync::atomic::{AtomicBool, AtomicU64, Ordering},
    sync::Arc,
    time::{Duration, Instant},
};
use tokio_util::sync::CancellationToken;

const OUTCOMES: [&str; 3] = ["success", "rejected", "failure"];
const BUCKETS_MILLISECONDS: [u64; 10] = [5, 10, 25, 50, 100, 250, 500, 1_000, 2_500, 5_000];
const MAX_OPERATIONS: usize = 64;
pub const PROCESS_OBSERVABILITY_OPERATIONS: &[&str] = &["live", "ready", "metrics", "other"];

pub fn process_observability_router(metrics: Arc<ProcessHttpMetrics>) -> Router {
    Router::new()
        .route("/livez", get(live))
        .route("/readyz", get(ready))
        .route("/metrics", get(metrics_response))
        .layer(middleware::from_fn(observe_request))
        .layer(Extension(metrics))
}

async fn live() -> Response {
    no_store(StatusCode::OK, "live")
}

async fn ready(Extension(metrics): Extension<Arc<ProcessHttpMetrics>>) -> Response {
    if metrics.is_ready() {
        no_store(StatusCode::OK, "ready")
    } else {
        no_store(StatusCode::SERVICE_UNAVAILABLE, "not ready")
    }
}

async fn metrics_response(Extension(metrics): Extension<Arc<ProcessHttpMetrics>>) -> Response {
    let mut response = metrics.render_prometheus().into_response();
    response.headers_mut().insert(
        axum::http::header::CONTENT_TYPE,
        HeaderValue::from_static("text/plain; version=0.0.4; charset=utf-8"),
    );
    response
        .headers_mut()
        .insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}

async fn observe_request(request: Request, next: Next) -> Response {
    let metrics = request
        .extensions()
        .get::<Arc<ProcessHttpMetrics>>()
        .cloned()
        .expect("process metrics Extension is installed");
    let operation = match request.uri().path() {
        "/livez" => "live",
        "/readyz" => "ready",
        "/metrics" => "metrics",
        _ => "other",
    };
    let started = Instant::now();
    let response = next.run(request).await;
    metrics.observe(operation, response.status().as_u16(), started.elapsed());
    response
}

fn no_store(status: StatusCode, body: &'static str) -> Response {
    let mut response = (status, body).into_response();
    response
        .headers_mut()
        .insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricsInstallError {
    InvalidComponentRole,
    InvalidOperation,
    DuplicateOperation,
    MissingOtherOperation,
    TooManyOperations,
}

impl fmt::Display for MetricsInstallError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidComponentRole => "invalid component role",
            Self::InvalidOperation => "invalid operation",
            Self::DuplicateOperation => "duplicate operation",
            Self::MissingOtherOperation => "missing other operation",
            Self::TooManyOperations => "too many operations",
        })
    }
}

impl std::error::Error for MetricsInstallError {}

#[derive(Debug)]
pub struct ProcessHttpMetrics {
    component_role: &'static str,
    operations: &'static [&'static str],
    other_operation: usize,
    ready: AtomicBool,
    requests: Vec<AtomicU64>,
    duration_microseconds: Vec<AtomicU64>,
    duration_buckets: Vec<AtomicU64>,
    worker_permits: Option<Arc<WorkerPermitMetrics>>,
    orchestration: Option<Arc<OrchestrationOperationalMetrics>>,
}

impl ProcessHttpMetrics {
    pub fn install(
        component_role: &'static str,
        operations: &'static [&'static str],
    ) -> Result<Self, MetricsInstallError> {
        Self::install_inner(component_role, operations, None, None)
    }

    pub fn install_with_worker_permits(
        component_role: &'static str,
        operations: &'static [&'static str],
        worker_permits: Arc<WorkerPermitMetrics>,
    ) -> Result<Self, MetricsInstallError> {
        Self::install_inner(component_role, operations, Some(worker_permits), None)
    }

    pub fn install_with_orchestration(
        component_role: &'static str,
        operations: &'static [&'static str],
        orchestration: Arc<OrchestrationOperationalMetrics>,
    ) -> Result<Self, MetricsInstallError> {
        Self::install_inner(component_role, operations, None, Some(orchestration))
    }

    fn install_inner(
        component_role: &'static str,
        operations: &'static [&'static str],
        worker_permits: Option<Arc<WorkerPermitMetrics>>,
        orchestration: Option<Arc<OrchestrationOperationalMetrics>>,
    ) -> Result<Self, MetricsInstallError> {
        if !valid_label(component_role) {
            return Err(MetricsInstallError::InvalidComponentRole);
        }
        if operations.is_empty() || operations.len() > MAX_OPERATIONS {
            return Err(MetricsInstallError::TooManyOperations);
        }
        for (index, operation) in operations.iter().enumerate() {
            if !valid_label(operation) {
                return Err(MetricsInstallError::InvalidOperation);
            }
            if operations[..index].contains(operation) {
                return Err(MetricsInstallError::DuplicateOperation);
            }
        }
        let other_operation = operations
            .iter()
            .position(|operation| *operation == "other")
            .ok_or(MetricsInstallError::MissingOtherOperation)?;
        let series = operations.len() * OUTCOMES.len();
        Ok(Self {
            component_role,
            operations,
            other_operation,
            ready: AtomicBool::new(false),
            requests: atomics(series),
            duration_microseconds: atomics(series),
            duration_buckets: atomics(series * (BUCKETS_MILLISECONDS.len() + 1)),
            worker_permits,
            orchestration,
        })
    }

    pub fn mark_ready(&self) {
        self.ready.store(true, Ordering::Release);
    }

    pub fn is_ready(&self) -> bool {
        self.ready.load(Ordering::Acquire)
    }

    pub fn observe(&self, operation: &str, status: u16, elapsed: Duration) {
        let operation = self
            .operations
            .iter()
            .position(|installed| *installed == operation)
            .unwrap_or(self.other_operation);
        let outcome = if status >= 500 {
            2
        } else if status >= 400 {
            1
        } else {
            0
        };
        let series = operation * OUTCOMES.len() + outcome;
        self.requests[series].fetch_add(1, Ordering::Relaxed);
        self.duration_microseconds[series].fetch_add(
            u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        let bucket = BUCKETS_MILLISECONDS
            .iter()
            .position(|maximum| elapsed <= Duration::from_millis(*maximum))
            .unwrap_or(BUCKETS_MILLISECONDS.len());
        let width = BUCKETS_MILLISECONDS.len() + 1;
        self.duration_buckets[series * width + bucket].fetch_add(1, Ordering::Relaxed);
    }

    pub fn render_prometheus(&self) -> String {
        use fmt::Write as _;

        let role = self.component_role;
        let mut output = String::with_capacity(16_384);
        output.push_str(
            "# HELP insight_platform_process_ready Whether startup authority checks completed.\n",
        );
        output.push_str("# TYPE insight_platform_process_ready gauge\n");
        let _ = writeln!(
            output,
            "insight_platform_process_ready{{component_role=\"{role}\"}} {}",
            u8::from(self.is_ready())
        );
        output.push_str(
            "# HELP insight_platform_process_build_info Static public protocol identity.\n",
        );
        output.push_str("# TYPE insight_platform_process_build_info gauge\n");
        let _ = writeln!(output, "insight_platform_process_build_info{{component_role=\"{role}\",protocol=\"insight.platform/v1\"}} 1");
        output.push_str("# HELP insight_platform_http_requests_total Requests by bounded component, operation, and outcome.\n");
        output.push_str("# TYPE insight_platform_http_requests_total counter\n");
        output.push_str("# HELP insight_platform_http_request_duration_seconds Request latency by bounded component, operation, and outcome.\n");
        output.push_str("# TYPE insight_platform_http_request_duration_seconds histogram\n");
        let width = BUCKETS_MILLISECONDS.len() + 1;
        for (operation_index, operation) in self.operations.iter().enumerate() {
            for (outcome_index, outcome) in OUTCOMES.iter().enumerate() {
                let series = operation_index * OUTCOMES.len() + outcome_index;
                let count = self.requests[series].load(Ordering::Relaxed);
                let labels = format!(
                    "component_role=\"{role}\",operation=\"{operation}\",outcome=\"{outcome}\""
                );
                let _ = writeln!(
                    output,
                    "insight_platform_http_requests_total{{{labels}}} {count}"
                );
                let mut cumulative = 0_u64;
                for (bucket, maximum) in BUCKETS_MILLISECONDS.iter().enumerate() {
                    cumulative = cumulative.saturating_add(
                        self.duration_buckets[series * width + bucket].load(Ordering::Relaxed),
                    );
                    let _ = writeln!(output, "insight_platform_http_request_duration_seconds_bucket{{{labels},le=\"{}\"}} {cumulative}", *maximum as f64 / 1_000.0);
                }
                cumulative = cumulative.saturating_add(
                    self.duration_buckets[series * width + width - 1].load(Ordering::Relaxed),
                );
                let _ = writeln!(output, "insight_platform_http_request_duration_seconds_bucket{{{labels},le=\"+Inf\"}} {cumulative}");
                let sum =
                    self.duration_microseconds[series].load(Ordering::Relaxed) as f64 / 1_000_000.0;
                let _ = writeln!(
                    output,
                    "insight_platform_http_request_duration_seconds_sum{{{labels}}} {sum}"
                );
                let _ = writeln!(
                    output,
                    "insight_platform_http_request_duration_seconds_count{{{labels}}} {count}"
                );
            }
        }
        if let Some(worker_permits) = &self.worker_permits {
            worker_permits.render_prometheus(role, &mut output);
        } else if let Some(orchestration) = &self.orchestration {
            orchestration.render_prometheus(role, &mut output);
        }
        output
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct WorkerPermitSnapshot {
    pub business_capacity: u64,
    pub business_available: u64,
    pub critical_control_capacity: u64,
    pub critical_control_available: u64,
}

#[derive(Debug, Default)]
pub struct WorkerPermitMetrics {
    business_capacity: AtomicU64,
    business_available: AtomicU64,
    critical_control_capacity: AtomicU64,
    critical_control_available: AtomicU64,
}

impl WorkerPermitMetrics {
    pub fn update(&self, snapshot: WorkerPermitSnapshot) {
        self.business_capacity
            .store(snapshot.business_capacity, Ordering::Release);
        self.business_available
            .store(snapshot.business_available, Ordering::Release);
        self.critical_control_capacity
            .store(snapshot.critical_control_capacity, Ordering::Release);
        self.critical_control_available
            .store(snapshot.critical_control_available, Ordering::Release);
    }

    fn render_prometheus(&self, role: &str, output: &mut String) {
        render_worker_permits(
            role,
            self.business_capacity.load(Ordering::Acquire),
            self.business_available.load(Ordering::Acquire),
            self.critical_control_capacity.load(Ordering::Acquire),
            self.critical_control_available.load(Ordering::Acquire),
            output,
        );
    }
}

pub fn update_worker_permits(metrics: &WorkerPermitMetrics, pools: &LocalWorkerPools) {
    let snapshot = pools.snapshot();
    metrics.update(WorkerPermitSnapshot {
        business_capacity: u64::try_from(snapshot.business_capacity).unwrap_or(u64::MAX),
        business_available: u64::try_from(snapshot.business_available).unwrap_or(u64::MAX),
        critical_control_capacity: u64::try_from(snapshot.critical_control_capacity)
            .unwrap_or(u64::MAX),
        critical_control_available: u64::try_from(snapshot.critical_control_available)
            .unwrap_or(u64::MAX),
    });
}

pub async fn run_worker_permit_sampler(
    metrics: Arc<WorkerPermitMetrics>,
    pools: LocalWorkerPools,
    cancellation: CancellationToken,
) {
    update_worker_permits(&metrics, &pools);
    let mut interval = tokio::time::interval(Duration::from_secs(1));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _ = cancellation.cancelled() => return,
            _ = interval.tick() => update_worker_permits(&metrics, &pools),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct OrchestrationOperationalSnapshot {
    pub business_capacity: u64,
    pub business_available: u64,
    pub critical_control_capacity: u64,
    pub critical_control_available: u64,
    pub active_jobs: u64,
    pub jobs_claimed: u64,
    pub claim_failures: u64,
    pub recovery_scan_attempts: u64,
    pub recovery_scan_failures: u64,
    pub recovery_capacity_skips: u64,
    pub recovery_mutations: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct DurableJobQueueSnapshot {
    pub due_jobs: u64,
    pub due_oldest_age_seconds: f64,
    pub expired_leases: u64,
    pub expired_oldest_lag_seconds: f64,
}

#[derive(Debug, Default)]
pub struct OrchestrationOperationalMetrics {
    business_capacity: AtomicU64,
    business_available: AtomicU64,
    critical_control_capacity: AtomicU64,
    critical_control_available: AtomicU64,
    active_jobs: AtomicU64,
    jobs_claimed: AtomicU64,
    claim_failures: AtomicU64,
    recovery_scan_attempts: AtomicU64,
    recovery_scan_failures: AtomicU64,
    recovery_capacity_skips: AtomicU64,
    recovery_mutations: AtomicU64,
    durable_due_jobs: AtomicU64,
    durable_due_oldest_age_milliseconds: AtomicU64,
    durable_expired_leases: AtomicU64,
    durable_expired_oldest_lag_milliseconds: AtomicU64,
    database_observation_successes: AtomicU64,
    database_observation_failures: AtomicU64,
}

impl OrchestrationOperationalMetrics {
    pub fn update(&self, snapshot: OrchestrationOperationalSnapshot) {
        self.business_capacity
            .store(snapshot.business_capacity, Ordering::Release);
        self.business_available
            .store(snapshot.business_available, Ordering::Release);
        self.critical_control_capacity
            .store(snapshot.critical_control_capacity, Ordering::Release);
        self.critical_control_available
            .store(snapshot.critical_control_available, Ordering::Release);
        self.active_jobs
            .store(snapshot.active_jobs, Ordering::Release);
        self.jobs_claimed
            .store(snapshot.jobs_claimed, Ordering::Release);
        self.claim_failures
            .store(snapshot.claim_failures, Ordering::Release);
        self.recovery_scan_attempts
            .store(snapshot.recovery_scan_attempts, Ordering::Release);
        self.recovery_scan_failures
            .store(snapshot.recovery_scan_failures, Ordering::Release);
        self.recovery_capacity_skips
            .store(snapshot.recovery_capacity_skips, Ordering::Release);
        self.recovery_mutations
            .store(snapshot.recovery_mutations, Ordering::Release);
    }

    pub fn observe_durable_job_queue(&self, snapshot: DurableJobQueueSnapshot) {
        self.durable_due_jobs
            .store(snapshot.due_jobs, Ordering::Release);
        self.durable_due_oldest_age_milliseconds.store(
            seconds_to_milliseconds(snapshot.due_oldest_age_seconds),
            Ordering::Release,
        );
        self.durable_expired_leases
            .store(snapshot.expired_leases, Ordering::Release);
        self.durable_expired_oldest_lag_milliseconds.store(
            seconds_to_milliseconds(snapshot.expired_oldest_lag_seconds),
            Ordering::Release,
        );
        self.database_observation_successes
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn observe_database_failure(&self) {
        self.database_observation_failures
            .fetch_add(1, Ordering::Relaxed);
    }

    fn render_prometheus(&self, role: &str, output: &mut String) {
        use fmt::Write as _;

        render_worker_permits(
            role,
            self.business_capacity.load(Ordering::Acquire),
            self.business_available.load(Ordering::Acquire),
            self.critical_control_capacity.load(Ordering::Acquire),
            self.critical_control_available.load(Ordering::Acquire),
            output,
        );
        output.push_str(
            "# HELP insight_platform_orchestration_active_jobs Jobs currently executing in the coordinator.\n\
             # TYPE insight_platform_orchestration_active_jobs gauge\n",
        );
        let _ = writeln!(
            output,
            "insight_platform_orchestration_active_jobs{{component_role=\"{role}\"}} {}",
            self.active_jobs.load(Ordering::Acquire)
        );
        output.push_str(
            "# HELP insight_platform_orchestration_claims_total Durable orchestration claim outcomes.\n\
             # TYPE insight_platform_orchestration_claims_total counter\n",
        );
        for (outcome, value) in [
            ("claimed", self.jobs_claimed.load(Ordering::Acquire)),
            ("failure", self.claim_failures.load(Ordering::Acquire)),
        ] {
            let _ = writeln!(output, "insight_platform_orchestration_claims_total{{component_role=\"{role}\",outcome=\"{outcome}\"}} {value}");
        }
        output.push_str(
            "# HELP insight_platform_recovery_operations_total Critical-control recovery operations by fixed outcome.\n\
             # TYPE insight_platform_recovery_operations_total counter\n",
        );
        for (outcome, value) in [
            (
                "scan_attempted",
                self.recovery_scan_attempts.load(Ordering::Acquire),
            ),
            (
                "scan_failed",
                self.recovery_scan_failures.load(Ordering::Acquire),
            ),
            (
                "capacity_skipped",
                self.recovery_capacity_skips.load(Ordering::Acquire),
            ),
            ("mutated", self.recovery_mutations.load(Ordering::Acquire)),
        ] {
            let _ = writeln!(output, "insight_platform_recovery_operations_total{{component_role=\"{role}\",outcome=\"{outcome}\"}} {value}");
        }
        output.push_str(
            "# HELP insight_platform_durable_jobs Durable Job backlog from PostgreSQL authority by fixed queue.\n\
             # TYPE insight_platform_durable_jobs gauge\n\
             # HELP insight_platform_durable_job_lag_seconds Oldest durable Job delay from PostgreSQL authority by fixed queue.\n\
             # TYPE insight_platform_durable_job_lag_seconds gauge\n",
        );
        for (queue, count, lag_milliseconds) in [
            (
                "due",
                self.durable_due_jobs.load(Ordering::Acquire),
                self.durable_due_oldest_age_milliseconds
                    .load(Ordering::Acquire),
            ),
            (
                "expired_lease",
                self.durable_expired_leases.load(Ordering::Acquire),
                self.durable_expired_oldest_lag_milliseconds
                    .load(Ordering::Acquire),
            ),
        ] {
            let _ = writeln!(output, "insight_platform_durable_jobs{{component_role=\"{role}\",queue=\"{queue}\"}} {count}");
            let _ = writeln!(output, "insight_platform_durable_job_lag_seconds{{component_role=\"{role}\",queue=\"{queue}\"}} {}", lag_milliseconds as f64 / 1_000.0);
        }
        output.push_str(
            "# HELP insight_platform_dependency_observations_total Bounded dependency observation outcomes.\n\
             # TYPE insight_platform_dependency_observations_total counter\n",
        );
        for (outcome, count) in [
            (
                "success",
                self.database_observation_successes.load(Ordering::Acquire),
            ),
            (
                "failure",
                self.database_observation_failures.load(Ordering::Acquire),
            ),
        ] {
            let _ = writeln!(output, "insight_platform_dependency_observations_total{{component_role=\"{role}\",dependency=\"postgresql\",outcome=\"{outcome}\"}} {count}");
        }
    }
}

fn seconds_to_milliseconds(seconds: f64) -> u64 {
    if !seconds.is_finite() || seconds <= 0.0 {
        0
    } else {
        (seconds * 1_000.0).min(u64::MAX as f64) as u64
    }
}

fn render_worker_permits(
    role: &str,
    business_capacity: u64,
    business_available: u64,
    critical_control_capacity: u64,
    critical_control_available: u64,
    output: &mut String,
) {
    use fmt::Write as _;

    output.push_str(
        "# HELP insight_platform_worker_permits Process-local worker permits by fixed lane and state.\n\
         # TYPE insight_platform_worker_permits gauge\n",
    );
    for (lane, capacity, available) in [
        ("business", business_capacity, business_available),
        (
            "critical_control",
            critical_control_capacity,
            critical_control_available,
        ),
    ] {
        let used = capacity.saturating_sub(available);
        let _ = writeln!(output, "insight_platform_worker_permits{{component_role=\"{role}\",lane=\"{lane}\",state=\"available\"}} {available}");
        let _ = writeln!(output, "insight_platform_worker_permits{{component_role=\"{role}\",lane=\"{lane}\",state=\"used\"}} {used}");
    }
}

fn atomics(length: usize) -> Vec<AtomicU64> {
    (0..length).map(|_| AtomicU64::new(0)).collect()
}

fn valid_label(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'_' | b'-')
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use tower::ServiceExt as _;

    const OPERATIONS: &[&str] = &["live", "ready", "metrics", "runs", "other"];

    #[test]
    fn install_rejects_unbounded_label_definitions() {
        assert_eq!(
            ProcessHttpMetrics::install("Gateway", OPERATIONS).unwrap_err(),
            MetricsInstallError::InvalidComponentRole
        );
        assert_eq!(
            ProcessHttpMetrics::install("gateway", &["runs", "runs", "other"]).unwrap_err(),
            MetricsInstallError::DuplicateOperation
        );
        assert_eq!(
            ProcessHttpMetrics::install("gateway", &["runs"]).unwrap_err(),
            MetricsInstallError::MissingOtherOperation
        );
    }

    #[test]
    fn unknown_values_are_collapsed_and_histograms_are_cumulative() {
        let metrics = ProcessHttpMetrics::install("public-gateway", OPERATIONS).unwrap();
        metrics.mark_ready();
        metrics.observe("runs", 200, Duration::from_millis(7));
        metrics.observe("tenant_sensitive", 503, Duration::from_secs(8));
        let rendered = metrics.render_prometheus();
        assert!(rendered
            .contains("insight_platform_process_ready{component_role=\"public-gateway\"} 1"));
        assert!(rendered.contains(
            "component_role=\"public-gateway\",operation=\"runs\",outcome=\"success\"} 1"
        ));
        assert!(rendered.contains("component_role=\"public-gateway\",operation=\"other\",outcome=\"failure\",le=\"+Inf\"} 1"));
        assert!(!rendered.contains("tenant_sensitive"));
    }

    #[tokio::test]
    async fn process_router_exposes_fail_closed_readiness_and_bounded_metrics() {
        let metrics = Arc::new(
            ProcessHttpMetrics::install("scheduler-recovery", PROCESS_OBSERVABILITY_OPERATIONS)
                .unwrap(),
        );
        let router = process_observability_router(Arc::clone(&metrics));
        let request = |path| {
            axum::http::Request::builder()
                .uri(path)
                .body(Body::empty())
                .unwrap()
        };
        let response = router.clone().oneshot(request("/readyz")).await.unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(response.headers()[CACHE_CONTROL], "no-store");
        metrics.mark_ready();
        let response = router.oneshot(request("/metrics")).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()[CACHE_CONTROL], "no-store");
    }

    #[test]
    fn orchestration_metrics_export_only_fixed_operational_dimensions() {
        let operational = Arc::new(OrchestrationOperationalMetrics::default());
        operational.update(OrchestrationOperationalSnapshot {
            business_capacity: 8,
            business_available: 3,
            critical_control_capacity: 2,
            critical_control_available: 1,
            active_jobs: 5,
            jobs_claimed: 13,
            claim_failures: 2,
            recovery_scan_attempts: 21,
            recovery_scan_failures: 1,
            recovery_capacity_skips: 4,
            recovery_mutations: 7,
        });
        operational.observe_durable_job_queue(DurableJobQueueSnapshot {
            due_jobs: 9,
            due_oldest_age_seconds: 12.5,
            expired_leases: 2,
            expired_oldest_lag_seconds: 3.25,
        });
        operational.observe_database_failure();
        let metrics = ProcessHttpMetrics::install_with_orchestration(
            "scheduler-recovery",
            PROCESS_OBSERVABILITY_OPERATIONS,
            operational,
        )
        .unwrap();
        let rendered = metrics.render_prometheus();
        assert!(rendered.contains("lane=\"business\",state=\"available\"} 3"));
        assert!(rendered.contains("lane=\"business\",state=\"used\"} 5"));
        assert!(rendered.contains("outcome=\"claimed\"} 13"));
        assert!(rendered.contains("outcome=\"scan_failed\"} 1"));
        assert!(rendered.contains("queue=\"due\"} 9"));
        assert!(rendered.contains("queue=\"expired_lease\"} 2"));
        assert!(rendered.contains("queue=\"due\"} 12.5"));
        assert!(rendered.contains("dependency=\"postgresql\",outcome=\"success\"} 1"));
        assert!(rendered.contains("dependency=\"postgresql\",outcome=\"failure\"} 1"));
        assert!(!rendered.contains("worker_process_generation"));
    }

    #[test]
    fn generic_worker_permits_export_only_fixed_lane_and_state() {
        let permits = Arc::new(WorkerPermitMetrics::default());
        permits.update(WorkerPermitSnapshot {
            business_capacity: 4,
            business_available: 1,
            critical_control_capacity: 2,
            critical_control_available: 2,
        });
        let metrics = ProcessHttpMetrics::install_with_worker_permits(
            "model-worker",
            PROCESS_OBSERVABILITY_OPERATIONS,
            permits,
        )
        .unwrap();
        let rendered = metrics.render_prometheus();
        assert!(rendered.contains("lane=\"business\",state=\"available\"} 1"));
        assert!(rendered.contains("lane=\"business\",state=\"used\"} 3"));
        assert!(!rendered.contains("insight_platform_orchestration_active_jobs"));
    }
}
