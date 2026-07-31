use std::{
    collections::BTreeMap,
    fmt::Write as _,
    sync::{Mutex, OnceLock},
    time::Duration,
};

#[derive(Debug, Clone, Copy, Default)]
struct OperationMetric {
    count: u64,
    duration_nanos: u128,
}

type OperationKey = (&'static str, &'static str, &'static str);

fn operations() -> &'static Mutex<BTreeMap<OperationKey, OperationMetric>> {
    static OPERATIONS: OnceLock<Mutex<BTreeMap<OperationKey, OperationMetric>>> = OnceLock::new();
    OPERATIONS.get_or_init(|| Mutex::new(BTreeMap::new()))
}

fn transport_events() -> &'static Mutex<BTreeMap<(&'static str, &'static str), u64>> {
    static EVENTS: OnceLock<Mutex<BTreeMap<(&'static str, &'static str), u64>>> = OnceLock::new();
    EVENTS.get_or_init(|| Mutex::new(BTreeMap::new()))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum McpOperationalEvent {
    InteractionAccepted,
    InteractionDeclined,
    InteractionCancelled,
    InteractionExpired,
    InteractionRunTerminal,
    InteractionRetryCompleted,
    InteractionRetryFailed,
    OAuthTransactionStarted,
    OAuthRefreshSucceeded,
    OAuthRefreshFailed,
    OAuthRevoked,
    StdioProcessRestarted,
    CacheHit,
    CacheMiss,
    CacheInvalidated,
    BodyLimitRejected,
    FrameLimitRejected,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum McpOperationalGauge {
    ActiveSubscriptions,
    OpenInteractions,
    OldestInteractionAgeSeconds,
    RemoteTasksWorking,
    RemoteTasksInputRequired,
    RemoteTasksTerminal,
    OldestRemoteTaskAgeSeconds,
    StalePublicationCandidates,
}

fn operational_events() -> &'static Mutex<BTreeMap<(String, McpOperationalEvent), u64>> {
    static EVENTS: OnceLock<Mutex<BTreeMap<(String, McpOperationalEvent), u64>>> = OnceLock::new();
    EVENTS.get_or_init(|| Mutex::new(BTreeMap::new()))
}

fn operational_gauges() -> &'static Mutex<BTreeMap<(String, McpOperationalGauge), u64>> {
    static GAUGES: OnceLock<Mutex<BTreeMap<(String, McpOperationalGauge), u64>>> = OnceLock::new();
    GAUGES.get_or_init(|| Mutex::new(BTreeMap::new()))
}

pub fn record_operational_event(server_id: &str, event: McpOperationalEvent) {
    let server_id = bounded_server_id(server_id);
    let mut events = operational_events()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let count = events.entry((server_id, event)).or_default();
    *count = count.saturating_add(1);
}

pub fn set_operational_gauge(server_id: &str, gauge: McpOperationalGauge, value: u64) {
    let server_id = bounded_server_id(server_id);
    operational_gauges()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .insert((server_id, gauge), value);
}

pub fn adjust_operational_gauge(server_id: &str, gauge: McpOperationalGauge, delta: i64) {
    let server_id = bounded_server_id(server_id);
    let mut gauges = operational_gauges()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let value = gauges.entry((server_id, gauge)).or_default();
    *value = if delta >= 0 {
        value.saturating_add(delta as u64)
    } else {
        value.saturating_sub(delta.unsigned_abs())
    };
}

fn bounded_server_id(value: &str) -> String {
    if !value.is_empty()
        && value.len() <= 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
    {
        value.to_owned()
    } else {
        "invalid".to_owned()
    }
}

pub(crate) fn primitive(value: &str) -> &'static str {
    match value {
        "server/discover" => "server_discover",
        "tools/list" => "tools_list",
        "tools/call" => "tools_call",
        "resources/list" => "resources_list",
        "resources/templates/list" => "resource_templates_list",
        "resources/read" => "resources_read",
        "prompts/list" => "prompts_list",
        "prompts/get" => "prompts_get",
        "completion/complete" => "completion_complete",
        "subscriptions/listen" => "subscriptions_listen",
        "elicitation/request" => "elicitation_request",
        "tasks/get" => "tasks_get",
        "tasks/update" => "tasks_update",
        "tasks/cancel" => "tasks_cancel",
        _ => "invalid",
    }
}

pub(crate) fn record_operation(
    primitive: &'static str,
    transport: &'static str,
    outcome: &'static str,
    duration: Duration,
) {
    let mut metrics = operations()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let metric = metrics.entry((primitive, transport, outcome)).or_default();
    metric.count = metric.count.saturating_add(1);
    metric.duration_nanos = metric.duration_nanos.saturating_add(duration.as_nanos());
}

pub(crate) fn record_transport_event(transport: &'static str, event: &'static str) {
    let mut metrics = transport_events()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let count = metrics.entry((transport, event)).or_default();
    *count = count.saturating_add(1);
}

pub fn prometheus_metrics() -> String {
    let metrics = operations()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let mut output = String::new();
    output.push_str(
        "# HELP insight_mcp_operations_total MCP operations by bounded primitive, transport, and outcome.\n\
         # TYPE insight_mcp_operations_total counter\n\
         # HELP insight_mcp_operation_duration_seconds MCP operation duration summary.\n\
         # TYPE insight_mcp_operation_duration_seconds summary\n",
    );
    for ((primitive, transport, outcome), metric) in metrics.iter() {
        let _ = writeln!(
            output,
            "insight_mcp_operations_total{{primitive=\"{primitive}\",transport=\"{transport}\",outcome=\"{outcome}\"}} {}",
            metric.count
        );
        let _ = writeln!(
            output,
            "insight_mcp_operation_duration_seconds_count{{primitive=\"{primitive}\",transport=\"{transport}\",outcome=\"{outcome}\"}} {}",
            metric.count
        );
        let _ = writeln!(
            output,
            "insight_mcp_operation_duration_seconds_sum{{primitive=\"{primitive}\",transport=\"{transport}\",outcome=\"{outcome}\"}} {:.9}",
            metric.duration_nanos as f64 / 1_000_000_000_f64
        );
    }
    drop(metrics);
    output.push_str(
        "# HELP insight_mcp_transport_events_total MCP transport lifecycle and bounded rejection events.\n\
         # TYPE insight_mcp_transport_events_total counter\n",
    );
    let events = transport_events()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    for ((transport, event), count) in events.iter() {
        let _ = writeln!(
            output,
            "insight_mcp_transport_events_total{{transport=\"{transport}\",event=\"{event}\"}} {count}"
        );
    }
    drop(events);
    let events = operational_events()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    for ((server_id, event), count) in events.iter() {
        let (metric, labels) = match event {
            McpOperationalEvent::InteractionAccepted => (
                "insight_mcp_interaction_outcomes_total",
                "outcome=\"accepted\"",
            ),
            McpOperationalEvent::InteractionDeclined => (
                "insight_mcp_interaction_outcomes_total",
                "outcome=\"declined\"",
            ),
            McpOperationalEvent::InteractionCancelled => (
                "insight_mcp_interaction_outcomes_total",
                "outcome=\"cancelled\"",
            ),
            McpOperationalEvent::InteractionExpired => (
                "insight_mcp_interaction_outcomes_total",
                "outcome=\"expired\"",
            ),
            McpOperationalEvent::InteractionRunTerminal => (
                "insight_mcp_interaction_outcomes_total",
                "outcome=\"run_terminal\"",
            ),
            McpOperationalEvent::InteractionRetryCompleted => (
                "insight_mcp_interaction_outcomes_total",
                "outcome=\"retry_completed\"",
            ),
            McpOperationalEvent::InteractionRetryFailed => (
                "insight_mcp_interaction_outcomes_total",
                "outcome=\"retry_failed\"",
            ),
            McpOperationalEvent::OAuthTransactionStarted => (
                "insight_mcp_oauth_events_total",
                "operation=\"transaction\",outcome=\"started\"",
            ),
            McpOperationalEvent::OAuthRefreshSucceeded => (
                "insight_mcp_oauth_events_total",
                "operation=\"refresh\",outcome=\"success\"",
            ),
            McpOperationalEvent::OAuthRefreshFailed => (
                "insight_mcp_oauth_events_total",
                "operation=\"refresh\",outcome=\"failed\"",
            ),
            McpOperationalEvent::OAuthRevoked => (
                "insight_mcp_oauth_events_total",
                "operation=\"revoke\",outcome=\"success\"",
            ),
            McpOperationalEvent::StdioProcessRestarted => (
                "insight_mcp_stdio_process_restarts_total",
                "outcome=\"restart\"",
            ),
            McpOperationalEvent::CacheHit => ("insight_mcp_cache_events_total", "outcome=\"hit\""),
            McpOperationalEvent::CacheMiss => {
                ("insight_mcp_cache_events_total", "outcome=\"miss\"")
            }
            McpOperationalEvent::CacheInvalidated => {
                ("insight_mcp_cache_events_total", "outcome=\"invalidation\"")
            }
            McpOperationalEvent::BodyLimitRejected => {
                ("insight_mcp_limit_rejections_total", "kind=\"body\"")
            }
            McpOperationalEvent::FrameLimitRejected => {
                ("insight_mcp_limit_rejections_total", "kind=\"frame\"")
            }
        };
        let _ = writeln!(
            output,
            "{metric}{{server_id=\"{server_id}\",{labels}}} {count}"
        );
    }
    drop(events);
    let gauges = operational_gauges()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    for ((server_id, gauge), value) in gauges.iter() {
        let (metric, labels) = match gauge {
            McpOperationalGauge::ActiveSubscriptions => ("insight_mcp_active_subscriptions", ""),
            McpOperationalGauge::OpenInteractions => ("insight_mcp_interactions_open", ""),
            McpOperationalGauge::OldestInteractionAgeSeconds => {
                ("insight_mcp_interaction_oldest_age_seconds", "")
            }
            McpOperationalGauge::RemoteTasksWorking => {
                ("insight_mcp_remote_tasks", "state=\"working\"")
            }
            McpOperationalGauge::RemoteTasksInputRequired => {
                ("insight_mcp_remote_tasks", "state=\"input_required\"")
            }
            McpOperationalGauge::RemoteTasksTerminal => {
                ("insight_mcp_remote_tasks", "state=\"terminal\"")
            }
            McpOperationalGauge::OldestRemoteTaskAgeSeconds => {
                ("insight_mcp_remote_task_oldest_age_seconds", "")
            }
            McpOperationalGauge::StalePublicationCandidates => {
                ("insight_mcp_stale_publication_candidates", "")
            }
        };
        if labels.is_empty() {
            let _ = writeln!(output, "{metric}{{server_id=\"{server_id}\"}} {value}");
        } else {
            let _ = writeln!(
                output,
                "{metric}{{server_id=\"{server_id}\",{labels}}} {value}"
            );
        }
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn labels_are_closed_and_never_include_remote_names() {
        assert_eq!(primitive("tools/call"), "tools_call");
        assert_eq!(primitive("private/customer/tool"), "invalid");
        record_operation(
            primitive("private/customer/tool"),
            "stdio",
            "validation",
            Duration::from_millis(2),
        );
        let metrics = prometheus_metrics();
        assert!(metrics.contains("primitive=\"invalid\""));
        assert!(!metrics.contains("private/customer/tool"));
    }

    #[test]
    fn operational_metrics_cover_required_families_with_bounded_server_labels() {
        record_operational_event("calendar", McpOperationalEvent::CacheHit);
        record_operational_event("customer/tool/name", McpOperationalEvent::BodyLimitRejected);
        set_operational_gauge("calendar", McpOperationalGauge::ActiveSubscriptions, 1);
        set_operational_gauge("calendar", McpOperationalGauge::RemoteTasksInputRequired, 2);
        let metrics = prometheus_metrics();
        assert!(metrics
            .contains("insight_mcp_cache_events_total{server_id=\"calendar\",outcome=\"hit\"} 1"));
        assert!(metrics
            .contains("insight_mcp_limit_rejections_total{server_id=\"invalid\",kind=\"body\"} 1"));
        assert!(metrics.contains("insight_mcp_active_subscriptions{server_id=\"calendar\"} 1"));
        assert!(metrics.contains(
            "insight_mcp_remote_tasks{server_id=\"calendar\",state=\"input_required\"} 2"
        ));
        assert!(!metrics.contains("customer/tool/name"));
    }
}
