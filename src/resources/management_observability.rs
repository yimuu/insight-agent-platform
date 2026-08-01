//! Durable, bounded-cardinality Agent/Provider control-plane observability.

use std::{
    fmt::Write as _,
    sync::{Arc, RwLock},
    time::{Duration, Instant},
};

use async_trait::async_trait;
use insight_durable::{
    AgentManagementDurableRepository, AgentManagementRuntimeStats,
    ProviderManagementDurableRepository, ProviderManagementRuntimeStats,
};
use insight_runtime::{RuntimeMetricsSource, RuntimeReadinessProbe, ServiceError};
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;

use super::provider_management::ProviderProjectionHealth;

const MAX_OBSERVATION_AGE: Duration = Duration::from_secs(5);

#[derive(Clone)]
pub struct ManagementObservabilityProbe {
    agent: Arc<dyn AgentManagementDurableRepository>,
    provider: Arc<dyn ProviderManagementDurableRepository>,
    provider_projection: ProviderProjectionHealth,
    snapshot: Arc<RwLock<Snapshot>>,
}

#[derive(Default)]
struct Snapshot {
    agent: Option<AgentManagementRuntimeStats>,
    provider: Option<ProviderManagementRuntimeStats>,
    observed_at: Option<Instant>,
    healthy: bool,
}

pub struct ManagementObservabilityRuntime {
    cancellation: CancellationToken,
    task: Option<tokio::task::JoinHandle<()>>,
    probe: Arc<ManagementObservabilityProbe>,
}

impl ManagementObservabilityRuntime {
    pub async fn start(
        agent: Arc<dyn AgentManagementDurableRepository>,
        provider: Arc<dyn ProviderManagementDurableRepository>,
        provider_projection: ProviderProjectionHealth,
        enabled: bool,
    ) -> Result<Self, ServiceError> {
        let probe = Arc::new(ManagementObservabilityProbe {
            agent,
            provider,
            provider_projection,
            snapshot: Arc::new(RwLock::new(Snapshot::default())),
        });
        if enabled {
            probe.refresh().await?;
        }
        let cancellation = CancellationToken::new();
        let task_cancellation = cancellation.clone();
        let task_probe = Arc::clone(&probe);
        let task = enabled.then(|| {
            tokio::spawn(async move {
                let mut interval = tokio::time::interval(Duration::from_secs(1));
                interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
                loop {
                    tokio::select! {
                        _ = task_cancellation.cancelled() => return,
                        _ = interval.tick() => {}
                    }
                    if task_probe.refresh().await.is_err() {
                        if let Ok(mut snapshot) = task_probe.snapshot.write() {
                            snapshot.healthy = false;
                        }
                        tracing::warn!(
                            code = "MANAGEMENT_OBSERVABILITY_UNAVAILABLE",
                            "Agent/Provider management observability refresh failed"
                        );
                    }
                }
            })
        });
        Ok(Self {
            cancellation,
            task,
            probe,
        })
    }

    pub fn probe(&self) -> Arc<ManagementObservabilityProbe> {
        Arc::clone(&self.probe)
    }

    pub async fn shutdown(&mut self) {
        self.cancellation.cancel();
        if let Some(task) = self.task.take() {
            let _ = tokio::time::timeout(Duration::from_secs(5), task).await;
        }
    }
}

impl Drop for ManagementObservabilityRuntime {
    fn drop(&mut self) {
        self.cancellation.cancel();
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}

impl ManagementObservabilityProbe {
    async fn refresh(&self) -> Result<(), ServiceError> {
        let (agent, provider) = tokio::try_join!(
            self.agent.load_agent_management_runtime_stats(),
            self.provider.load_provider_management_runtime_stats(),
        )
        .map_err(|_| {
            ServiceError::new(
                "MANAGEMENT_REPOSITORY_UNAVAILABLE",
                "management repository observability is unavailable",
            )
        })?;
        let mut snapshot = self.snapshot.write().map_err(|_| {
            ServiceError::new(
                "MANAGEMENT_OBSERVABILITY_UNAVAILABLE",
                "management observability snapshot is unavailable",
            )
        })?;
        snapshot.agent = Some(agent);
        snapshot.provider = Some(provider);
        snapshot.observed_at = Some(Instant::now());
        snapshot.healthy = true;
        Ok(())
    }
}

#[async_trait]
impl RuntimeReadinessProbe for ManagementObservabilityProbe {
    async fn check_readiness(&self, timeout: Duration) -> Result<(), ServiceError> {
        tokio::time::timeout(timeout, self.refresh())
            .await
            .map_err(|_| {
                ServiceError::new(
                    "MANAGEMENT_REPOSITORY_TIMEOUT",
                    "management repository readiness timed out",
                )
            })??;
        let snapshot = self.snapshot.read().map_err(|_| {
            ServiceError::new(
                "MANAGEMENT_OBSERVABILITY_UNAVAILABLE",
                "management observability snapshot is unavailable",
            )
        })?;
        if !snapshot.healthy
            || snapshot
                .observed_at
                .is_none_or(|observed_at| observed_at.elapsed() > MAX_OBSERVATION_AGE)
        {
            return Err(ServiceError::new(
                "MANAGEMENT_PROJECTION_STALE",
                "management projection evidence is stale",
            ));
        }
        let (projection_healthy, projection_age) = self.provider_projection.observation();
        if !projection_healthy || projection_age.is_none_or(|age| age > MAX_OBSERVATION_AGE) {
            return Err(ServiceError::new(
                "PROVIDER_PROJECTION_STALE",
                "Provider registry projection evidence is stale",
            ));
        }
        Ok(())
    }
}

impl RuntimeMetricsSource for ManagementObservabilityProbe {
    fn prometheus_metrics(&self) -> String {
        let Ok(snapshot) = self.snapshot.read() else {
            return "management_observability_healthy 0\n".to_owned();
        };
        let mut output = String::with_capacity(4096);
        output.push_str(
            "# HELP management_observability_healthy Durable Agent/Provider management observation health.\n\
             # TYPE management_observability_healthy gauge\n",
        );
        let fresh = snapshot.healthy
            && snapshot
                .observed_at
                .is_some_and(|observed_at| observed_at.elapsed() <= MAX_OBSERVATION_AGE);
        let _ = writeln!(
            output,
            "management_observability_healthy {}",
            u8::from(fresh)
        );
        if let Some(agent) = &snapshot.agent {
            render_agent_metrics(&mut output, agent);
        }
        if let Some(provider) = &snapshot.provider {
            render_provider_metrics(&mut output, provider);
        }
        let (projection_healthy, projection_age) = self.provider_projection.observation();
        let lag = projection_age.map_or(f64::INFINITY, |age| age.as_secs_f64());
        output.push_str(
            "# HELP provider_registry_projection_lag_seconds Age of the latest successful Provider ModelRegistry projection.\n\
             # TYPE provider_registry_projection_lag_seconds gauge\n\
             # HELP provider_registry_projection_healthy Provider ModelRegistry projection health.\n\
             # TYPE provider_registry_projection_healthy gauge\n",
        );
        let _ = writeln!(output, "provider_registry_projection_lag_seconds {lag:.6}");
        let _ = writeln!(
            output,
            "provider_registry_projection_healthy {}",
            u8::from(projection_healthy)
        );
        output
    }
}

fn render_agent_metrics(output: &mut String, stats: &AgentManagementRuntimeStats) {
    output.push_str(
        "# TYPE agent_drafts_current gauge\n# TYPE agent_validations_pending gauge\n\
         # TYPE agent_deployment_resolutions_pending gauge\n",
    );
    let _ = writeln!(output, "agent_drafts_current {}", stats.drafts_current);
    let _ = writeln!(
        output,
        "agent_validations_pending {}",
        stats.validations_pending
    );
    let _ = writeln!(
        output,
        "agent_deployment_resolutions_pending {}",
        stats.deployment_resolutions_pending
    );
    output.push_str("# TYPE agent_debug_sessions gauge\n");
    for count in &stats.debug_sessions {
        if !matches!(count.profile_mode.as_str(), "sandbox" | "live") {
            continue;
        }
        let _ = writeln!(
            output,
            "agent_debug_sessions{{state=\"{}\",profile_mode=\"{}\"}} {}",
            count.state.as_str(),
            count.profile_mode,
            count.count
        );
    }
    output.push_str(
        "# TYPE agent_management_operations_total counter\n\
         # TYPE agent_activations_total counter\n",
    );
    for count in &stats.operations {
        let Some(operation) = agent_operation(&count.operation) else {
            continue;
        };
        let Some(outcome) = metric_outcome(&count.outcome) else {
            continue;
        };
        let _ = writeln!(
            output,
            "agent_management_operations_total{{operation=\"{operation}\",outcome=\"{outcome}\"}} {}",
            count.count
        );
        if count.operation == "agent.deployment.activated" {
            let _ = writeln!(
                output,
                "agent_activations_total{{outcome=\"{outcome}\"}} {}",
                count.count
            );
        }
    }
}

fn render_provider_metrics(output: &mut String, stats: &ProviderManagementRuntimeStats) {
    output.push_str(
        "# TYPE provider_discoveries_pending gauge\n\
         # TYPE provider_connection_tests_total counter\n\
         # TYPE provider_operational_state gauge\n",
    );
    let _ = writeln!(
        output,
        "provider_discoveries_pending {}",
        stats.pending_discoveries
    );
    for count in &stats.connection_tests {
        let _ = writeln!(
            output,
            "provider_connection_tests_total{{mode=\"{}\",outcome=\"{}\"}} {}",
            count.mode.as_str(),
            count.outcome.as_str(),
            count.count
        );
    }
    for (state, count) in [
        ("enabled", stats.enabled_providers),
        ("suspended", stats.suspended_providers),
        ("retired", stats.retired_providers),
    ] {
        let _ = writeln!(
            output,
            "provider_operational_state{{state=\"{state}\"}} {count}"
        );
    }
    output.push_str(
        "# TYPE provider_management_operations_total counter\n\
         # TYPE provider_activations_total counter\n",
    );
    for count in &stats.operations {
        let Some(operation) = provider_operation(&count.operation) else {
            continue;
        };
        let Some(outcome) = metric_outcome(&count.outcome) else {
            continue;
        };
        let _ = writeln!(
            output,
            "provider_management_operations_total{{operation=\"{operation}\",outcome=\"{outcome}\"}} {}",
            count.count
        );
        if count.operation == "provider.revision.activated" {
            let _ = writeln!(
                output,
                "provider_activations_total{{outcome=\"{outcome}\"}} {}",
                count.count
            );
        }
    }
}

fn metric_outcome(value: &str) -> Option<&'static str> {
    match value {
        "accepted" | "ok" | "created" | "updated" | "deleted" | "cancel_requested"
        | "published" | "activated" | "deactivated" | "suspended" | "resumed" | "retired"
        | "valid" | "invalid" => Some("accepted"),
        "rejected" | "failed" => Some("rejected"),
        _ => None,
    }
}

fn agent_operation(value: &str) -> Option<&'static str> {
    match value {
        "agent.created" => Some("create"),
        "agent.labels.updated" => Some("update_labels"),
        "agent.deleted" => Some("delete"),
        "agent.draft.replaced" => Some("replace_draft"),
        "agent.draft_view.replaced" => Some("replace_view"),
        "agent.validation.created" => Some("validate"),
        "agent.definition.published" => Some("publish_definition"),
        "agent.deployment_resolution.created" => Some("resolve_deployment"),
        "agent.deployment.installed" => Some("create_deployment"),
        "agent.deployment.activated" => Some("activate"),
        "agent.deactivated" => Some("deactivate"),
        "agent.archived" => Some("archive"),
        "agent.restored" => Some("restore"),
        "agent.debug.created" => Some("create_debug"),
        "agent.debug.cancelled" => Some("cancel_debug"),
        "agent.migrated" => Some("migrate"),
        _ => None,
    }
}

fn provider_operation(value: &str) -> Option<&'static str> {
    match value {
        "provider.created" => Some("create"),
        "provider.draft.replaced" => Some("replace_draft"),
        "provider.deleted" => Some("delete"),
        "provider.discovery.created" => Some("discover"),
        "provider.discovery.cancel_requested" => Some("cancel_discovery"),
        "provider.connection_test.created" => Some("connection_test"),
        "provider.connection_test.cancel_requested" => Some("cancel_connection_test"),
        "provider.validation.created" => Some("validate"),
        "provider.revision.published" => Some("publish_revision"),
        "provider.revision.activated" => Some("activate"),
        "provider.revision.deactivated" => Some("deactivate"),
        "provider.suspended" => Some("suspend"),
        "provider.resumed" => Some("resume"),
        "provider.retired" => Some("retire"),
        _ => None,
    }
}
