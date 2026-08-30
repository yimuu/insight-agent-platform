//! Production composition for the durable orchestration and critical-control drivers.

use crate::{
    ExactPlanGenerationDriver, LeaseFencedOrchestrationExecutor,
    MaterializingOrchestrationJobHandler, OrchestrationCoordinatorConfig,
    OrchestrationExecutorConfig, OrchestrationSafetyConfig, OrchestrationSafetyDriver,
    PostgresControllerCapabilityAdmissionProvider, PostgresControllerModelAdmissionProvider,
    PostgresDurablePlanGenerationStore, RunningOrchestrationSafetyDriver, RunningWorkCoordinator,
    SchedulerControllerRunValueMaterializer, SchedulerPlanMaterializer,
    SchedulerPlanMaterializerConfig, UuidCoordinatorIdentityFactory, WorkCoordinator,
};
use insight_platform_artifact_rpc::ArtifactSchedulerGrpcClient;
use insight_platform_orchestrator::ExpressionLimits;
use insight_platform_postgres::repository::PgRepository;
pub use insight_platform_sandbox::SandboxResourceEnvelope;
use insight_platform_worker::LocalWorkerPools;
use std::{error::Error, fmt, sync::Arc, time::Duration};

#[derive(Debug, Clone)]
pub struct ProductionOrchestrationConfig {
    pub plan_materializer: SchedulerPlanMaterializerConfig,
    pub run_value_read_timeout: Duration,
    pub handoff_retry_delay: Duration,
    pub expression_limits: ExpressionLimits,
    pub executor: OrchestrationExecutorConfig,
    pub coordinator: OrchestrationCoordinatorConfig,
    pub safety: OrchestrationSafetyConfig,
    pub sandbox: Option<ProductionSandboxCapabilityConfig>,
}

#[derive(Debug, Clone)]
pub struct ProductionSandboxCapabilityConfig {
    pub executor_worker_manifest_digest: insight_platform_contracts::Sha256Digest,
    pub isolation_backend_contract_digest: insight_platform_contracts::Sha256Digest,
    pub callback_audience_identity_digest: insight_platform_contracts::Sha256Digest,
    pub resources: SandboxResourceEnvelope,
}

impl ProductionOrchestrationConfig {
    pub fn validate(&self) -> Result<(), ProductionOrchestrationError> {
        self.plan_materializer
            .validate()
            .map_err(|_| ProductionOrchestrationError::InvalidConfiguration)?;
        if self.run_value_read_timeout.is_zero()
            || self.handoff_retry_delay.is_zero()
            || !self.expression_limits.bounded_by_absolute()
        {
            return Err(ProductionOrchestrationError::InvalidConfiguration);
        }
        Ok(())
    }
}

pub struct RunningProductionOrchestration {
    pub coordinator: RunningWorkCoordinator,
    pub safety: RunningOrchestrationSafetyDriver,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProductionOrchestrationExit {
    pub coordinator: crate::CoordinatorExit,
    pub safety: crate::SafetyDriverSnapshot,
}

impl RunningProductionOrchestration {
    pub fn is_finished(&self) -> bool {
        self.coordinator.is_finished() || self.safety.is_finished()
    }

    pub fn request_shutdown(&self) {
        self.coordinator.request_drain();
        self.safety.request_stop();
    }

    pub async fn shutdown(
        self,
    ) -> Result<ProductionOrchestrationExit, ProductionOrchestrationError> {
        self.request_shutdown();
        let (coordinator, safety) =
            tokio::join!(self.coordinator.shutdown(), self.safety.shutdown());
        Ok(ProductionOrchestrationExit {
            coordinator: coordinator.map_err(|_| ProductionOrchestrationError::RuntimeFailed)?,
            safety: safety.map_err(|_| ProductionOrchestrationError::RuntimeFailed)?,
        })
    }
}

pub fn start_production_orchestration(
    business_repository: PgRepository,
    critical_control_repository: PgRepository,
    artifact_client: Arc<ArtifactSchedulerGrpcClient>,
    pools: LocalWorkerPools,
    config: ProductionOrchestrationConfig,
) -> Result<RunningProductionOrchestration, ProductionOrchestrationError> {
    config.validate()?;
    let identities = Arc::new(UuidCoordinatorIdentityFactory);
    let critical_authority = Arc::new(critical_control_repository.clone());
    let run_value_materializer = Arc::new(
        SchedulerControllerRunValueMaterializer::new(
            Arc::clone(&critical_authority),
            Arc::clone(&artifact_client),
            config.run_value_read_timeout,
        )
        .map_err(|_| ProductionOrchestrationError::InvalidConfiguration)?,
    );
    let capability_admission = Arc::new(PostgresControllerCapabilityAdmissionProvider::new(
        critical_control_repository.clone(),
        config
            .sandbox
            .clone()
            .map(|sandbox| crate::ControllerSandboxCapabilityAdmission {
                executor_worker_manifest_digest: sandbox.executor_worker_manifest_digest,
                isolation_backend_contract_digest: sandbox.isolation_backend_contract_digest,
                callback_audience_identity_digest: sandbox.callback_audience_identity_digest,
                resources: sandbox.resources,
            }),
    ));
    let model_admission = Arc::new(PostgresControllerModelAdmissionProvider::new(
        critical_control_repository.clone(),
        critical_authority.clone(),
        artifact_client.clone(),
    ));
    let durable_store = Arc::new(
        PostgresDurablePlanGenerationStore::new(
            critical_control_repository.clone(),
            run_value_materializer,
            Arc::clone(&identities),
            capability_admission,
            model_admission,
            config.handoff_retry_delay,
        )
        .map_err(|_| ProductionOrchestrationError::InvalidConfiguration)?,
    );
    let plan_driver = Arc::new(
        ExactPlanGenerationDriver::new(durable_store, config.expression_limits)
            .map_err(|_| ProductionOrchestrationError::InvalidConfiguration)?,
    );
    let plan_materializer = Arc::new(
        SchedulerPlanMaterializer::new(
            critical_authority,
            artifact_client,
            config.plan_materializer,
        )
        .map_err(|_| ProductionOrchestrationError::InvalidConfiguration)?,
    );
    let handler = Arc::new(MaterializingOrchestrationJobHandler::new(
        plan_materializer,
        plan_driver,
    ));
    let executor = Arc::new(
        LeaseFencedOrchestrationExecutor::new(
            Arc::new(business_repository.clone()),
            handler,
            Arc::clone(&identities),
            config.executor,
        )
        .map_err(|_| ProductionOrchestrationError::InvalidConfiguration)?,
    );
    let coordinator = WorkCoordinator::new(
        Arc::new(business_repository),
        executor,
        Arc::clone(&identities),
        pools.clone(),
        config.coordinator,
    )
    .map_err(|_| ProductionOrchestrationError::InvalidConfiguration)?
    .spawn();
    let safety = OrchestrationSafetyDriver::new(
        Arc::new(critical_control_repository),
        identities,
        pools,
        config.safety,
    )
    .map_err(|_| ProductionOrchestrationError::InvalidConfiguration)?
    .spawn();
    Ok(RunningProductionOrchestration {
        coordinator,
        safety,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProductionOrchestrationError {
    InvalidConfiguration,
    RuntimeFailed,
}

impl fmt::Display for ProductionOrchestrationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidConfiguration => "production orchestration composition is invalid",
            Self::RuntimeFailed => "production orchestration runtime failed",
        })
    }
}

impl Error for ProductionOrchestrationError {}
