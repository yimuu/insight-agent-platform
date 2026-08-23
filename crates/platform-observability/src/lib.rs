//! Bounded, low-cardinality process telemetry for Platform v2 services.
//!
//! Metric labels are frozen at installation. Request paths, tenant identifiers, resource
//! identifiers, payloads, and error text can never become emitted label values.

use std::{
    fmt,
    sync::atomic::{AtomicBool, AtomicU64, Ordering},
    time::Duration,
};

const OUTCOMES: [&str; 3] = ["success", "rejected", "failure"];
const BUCKETS_MILLISECONDS: [u64; 10] = [5, 10, 25, 50, 100, 250, 500, 1_000, 2_500, 5_000];
const MAX_OPERATIONS: usize = 64;

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
}

impl ProcessHttpMetrics {
    pub fn install(
        component_role: &'static str,
        operations: &'static [&'static str],
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
        output
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
}
