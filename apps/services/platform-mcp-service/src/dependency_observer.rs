use insight_platform_egress_rpc::{EgressRpcDependencyObserver, EgressRpcDependencyOutcome};
use insight_platform_observability::{
    DependencyObservationMetrics, DependencyObservationOutcome, MetricsInstallError,
    PlatformDependency,
};
use insight_platform_postgres::dependency_health::{PostgresHealthObserver, PostgresHealthOutcome};
use std::sync::Arc;

struct McpDependencyObserver(Arc<DependencyObservationMetrics>);

impl PostgresHealthObserver for McpDependencyObserver {
    fn observe(&self, outcome: PostgresHealthOutcome) {
        self.0
            .observe(
                PlatformDependency::Postgresql,
                match outcome {
                    PostgresHealthOutcome::Success => DependencyObservationOutcome::Success,
                    PostgresHealthOutcome::Failure => DependencyObservationOutcome::Failure,
                },
            )
            .expect("MCP Resource Host installs its PostgreSQL dependency metric");
    }
}

impl EgressRpcDependencyObserver for McpDependencyObserver {
    fn observe(&self, outcome: EgressRpcDependencyOutcome) {
        self.0
            .observe(
                PlatformDependency::Egress,
                match outcome {
                    EgressRpcDependencyOutcome::Success => DependencyObservationOutcome::Success,
                    EgressRpcDependencyOutcome::Failure => DependencyObservationOutcome::Failure,
                },
            )
            .expect("MCP Host installs its Egress dependency metric");
    }
}

pub type InstalledMcpDependencyMetrics = (
    Arc<DependencyObservationMetrics>,
    Option<Arc<dyn PostgresHealthObserver>>,
    Arc<dyn EgressRpcDependencyObserver>,
);

pub fn install_mcp_dependency_metrics(
    include_postgres: bool,
) -> Result<InstalledMcpDependencyMetrics, MetricsInstallError> {
    let dependencies = if include_postgres {
        &[PlatformDependency::Postgresql, PlatformDependency::Egress][..]
    } else {
        &[PlatformDependency::Egress][..]
    };
    let metrics = Arc::new(DependencyObservationMetrics::install(dependencies)?);
    let observer = Arc::new(McpDependencyObserver(Arc::clone(&metrics)));
    let postgres = include_postgres.then(|| {
        let observer: Arc<dyn PostgresHealthObserver> = observer.clone();
        observer
    });
    let egress: Arc<dyn EgressRpcDependencyObserver> = observer;
    Ok((metrics, postgres, egress))
}

#[cfg(test)]
mod tests {
    use super::*;
    use insight_platform_observability::{ProcessHttpMetrics, PROCESS_OBSERVABILITY_OPERATIONS};

    #[test]
    fn adapter_maps_postgresql_without_dynamic_database_labels() {
        let (dependencies, postgres, egress) = install_mcp_dependency_metrics(true).unwrap();
        postgres.unwrap().observe(PostgresHealthOutcome::Failure);
        egress.observe(EgressRpcDependencyOutcome::Success);
        let rendered =
            ProcessHttpMetrics::install("mcp-resource-host", PROCESS_OBSERVABILITY_OPERATIONS)
                .unwrap()
                .with_dependency_observations(dependencies)
                .render_prometheus();
        assert!(rendered.contains("dependency=\"postgresql\",outcome=\"failure\"} 1"));
        assert!(rendered.contains("dependency=\"egress\",outcome=\"success\"} 1"));
        assert!(!rendered.contains("database="));
        assert!(!rendered.contains("pool="));
        assert!(!rendered.contains("error="));
    }

    #[test]
    fn tool_host_installs_egress_without_postgresql() {
        let (dependencies, postgres, egress) = install_mcp_dependency_metrics(false).unwrap();
        assert!(postgres.is_none());
        egress.observe(EgressRpcDependencyOutcome::Failure);
        let rendered = ProcessHttpMetrics::install("mcp-host", PROCESS_OBSERVABILITY_OPERATIONS)
            .unwrap()
            .with_dependency_observations(dependencies)
            .render_prometheus();
        assert!(rendered.contains("dependency=\"egress\",outcome=\"failure\"} 1"));
        assert!(!rendered.contains("dependency=\"postgresql\""));
        assert!(!rendered.contains("endpoint="));
        assert!(!rendered.contains("tenant="));
    }
}
