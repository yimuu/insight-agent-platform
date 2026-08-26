//! Independently deployed Artifact Data Worker composition.
//!
//! The Sandbox Controller receives only exact, credential-free reads. Model-specific Artifact
//! production and materialization are intentionally absent from the first release.

use insight_platform_artifact_broker::{
    ArtifactBrokerLimits, AwsArtifactProviderCatalog, AwsArtifactProviderCatalogConfig,
    BrokeredArtifactScannerReader, BrokeredSandboxArtifactBroker, BrokeredSchedulerRunValueReader,
    BrokeredSchedulerSkillPackageReader, BrokeredSchedulerTypedPlanReader,
};
mod capacity;
mod guest_identity;
mod scan_worker;

use capacity::artifact_capacity_metric;
use guest_identity::GvisorGuestIdentityConfig;
use insight_platform_artifact_rpc::{
    proto::{
        artifact_gvisor_guest_service_server::ArtifactGvisorGuestServiceServer,
        artifact_sandbox_broker_service_server::ArtifactSandboxBrokerServiceServer,
        artifact_scheduler_service_server::ArtifactSchedulerServiceServer,
    },
    ArtifactGvisorGuestGrpcService, ArtifactInternalRpcLimits, ArtifactSandboxBrokerGrpcService,
    ArtifactSchedulerGrpcService, GvisorGuestResponseMaterializer, LeasedArtifactBytes,
    SandboxControllerWorkloadIdentity, SchedulerRunValueResponseBroker,
    SchedulerSkillPackageResponseBroker, SchedulerTypedPlanResponseBroker,
    SchedulerWorkloadIdentity, WasiArtifactBrokerError, WasiArtifactReadRequest,
    WasiArtifactResponseBroker,
};
use insight_platform_artifacts::{
    SchedulerRunValueReadError, SchedulerRunValueReadRequest, SchedulerSkillPackageReadError,
    SchedulerSkillPackageReadRequest, SchedulerTypedPlanReadError, SchedulerTypedPlanReadRequest,
};
use insight_platform_contracts::{
    canonical_digest, checked_in_hard_limit_profile, parse_strict_json, JsonLimits, Sha256Digest,
};
use insight_platform_observability::{
    process_observability_router, ProcessHttpMetrics, PROCESS_OBSERVABILITY_OPERATIONS,
};
use insight_platform_postgres::{repository::PgRepository, verify_schema};
use insight_platform_sandbox::{
    AuthorizedGvisorGuestBootstrap, GvisorGuestBootstrapAuthority, GvisorGuestBootstrapError,
    GvisorGuestBootstrapRequest,
};
use scan_worker::{run_scan_worker, IntegrityArtifactScanner, ScanWorkerConfig};
use serde::Deserialize;
use sqlx::postgres::PgPoolOptions;
use std::{
    error::Error, fmt, future::IntoFuture, io::Read as _, net::SocketAddr, path::PathBuf,
    sync::Arc, time::Duration,
};
use tonic::transport::{Certificate, Identity, Server, ServerTlsConfig};

const CONFIG_PATH_ENV: &str = "PLATFORM_ARTIFACT_DATA_WORKER_CONFIG";
const CONFIG_DIGEST_ENV: &str = "PLATFORM_ARTIFACT_DATA_WORKER_CONFIG_DIGEST";
const AUDIENCE_ENV: &str = "PLATFORM_ARTIFACT_DATA_WORKER_AUDIENCE";
const READ_DATABASE_URL_ENV: &str = "PLATFORM_ARTIFACT_DATA_WORKER_READ_DATABASE_URL";
const WORK_DATABASE_URL_ENV: &str = "PLATFORM_ARTIFACT_DATA_WORKER_WORK_DATABASE_URL";
const CLIENT_CA_PATH_ENV: &str = "PLATFORM_ARTIFACT_DATA_WORKER_CLIENT_CA_PATH";
const CERT_PATH_ENV: &str = "PLATFORM_ARTIFACT_DATA_WORKER_CERT_PATH";
const KEY_PATH_ENV: &str = "PLATFORM_ARTIFACT_DATA_WORKER_KEY_PATH";
const MAX_CONFIG_BYTES: usize = 1_048_576;
const MAX_TLS_FILE_BYTES: usize = 1_048_576;

struct SandboxRpcArtifactBroker {
    broker: BrokeredSandboxArtifactBroker,
}

struct SchedulerRpcArtifactBroker {
    typed_plan_reader: BrokeredSchedulerTypedPlanReader,
    run_value_reader: BrokeredSchedulerRunValueReader,
    skill_package_reader: BrokeredSchedulerSkillPackageReader,
}

impl SandboxRpcArtifactBroker {
    fn capacity_snapshot(
        &self,
    ) -> insight_platform_artifact_broker::ArtifactBrokerCapacitySnapshot {
        self.broker.capacity_snapshot()
    }
}

impl SchedulerRpcArtifactBroker {
    fn typed_plan_capacity(
        &self,
    ) -> insight_platform_artifact_broker::ArtifactBrokerCapacitySnapshot {
        self.typed_plan_reader.capacity_snapshot()
    }

    fn run_value_capacity(
        &self,
    ) -> insight_platform_artifact_broker::ArtifactBrokerCapacitySnapshot {
        self.run_value_reader.capacity_snapshot()
    }

    fn skill_package_capacity(
        &self,
    ) -> insight_platform_artifact_broker::ArtifactBrokerCapacitySnapshot {
        self.skill_package_reader.capacity_snapshot()
    }
}

struct GvisorGuestMaterializer {
    authority: Arc<PgRepository>,
    broker: Arc<SandboxRpcArtifactBroker>,
}

#[async_trait::async_trait]
impl GvisorGuestResponseMaterializer for GvisorGuestMaterializer {
    async fn authorize_guest(
        &self,
        request: &GvisorGuestBootstrapRequest,
    ) -> Result<AuthorizedGvisorGuestBootstrap, GvisorGuestBootstrapError> {
        self.authority.authorize_guest_bootstrap(request).await
    }

    async fn read_guest_for_response(
        &self,
        request: WasiArtifactReadRequest,
    ) -> Result<LeasedArtifactBytes, WasiArtifactBrokerError> {
        self.broker.read_wasi_for_response(request).await
    }
}

#[async_trait::async_trait]
impl WasiArtifactResponseBroker for SandboxRpcArtifactBroker {
    async fn read_wasi_for_response(
        &self,
        request: WasiArtifactReadRequest,
    ) -> Result<LeasedArtifactBytes, WasiArtifactBrokerError> {
        let read = self.broker.read_wasi_for_response(request).await?;
        let (bytes, permit) = read.into_response_parts();
        Ok(LeasedArtifactBytes::new(bytes, permit))
    }
}

#[async_trait::async_trait]
impl SchedulerTypedPlanResponseBroker for SchedulerRpcArtifactBroker {
    async fn read_typed_plan_for_response(
        &self,
        request: SchedulerTypedPlanReadRequest,
    ) -> Result<LeasedArtifactBytes, SchedulerTypedPlanReadError> {
        let read = self.typed_plan_reader.read(&request).await?;
        let (bytes, permit) = read.into_response_parts();
        Ok(LeasedArtifactBytes::new(bytes, permit))
    }
}

#[async_trait::async_trait]
impl SchedulerRunValueResponseBroker for SchedulerRpcArtifactBroker {
    async fn read_run_value_for_response(
        &self,
        request: SchedulerRunValueReadRequest,
    ) -> Result<LeasedArtifactBytes, SchedulerRunValueReadError> {
        let read = self.run_value_reader.read(&request).await?;
        let (bytes, permit) = read.into_response_parts();
        Ok(LeasedArtifactBytes::new(bytes, permit))
    }
}

#[async_trait::async_trait]
impl SchedulerSkillPackageResponseBroker for SchedulerRpcArtifactBroker {
    async fn read_skill_package_for_response(
        &self,
        request: SchedulerSkillPackageReadRequest,
    ) -> Result<LeasedArtifactBytes, SchedulerSkillPackageReadError> {
        let read = self.skill_package_reader.read(&request).await?;
        let (bytes, permit) = read.into_response_parts();
        Ok(LeasedArtifactBytes::new(bytes, permit))
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct ArtifactBrokerProcessConfig {
    schema_version: u32,
    audience: ArtifactBrokerAudience,
    controller_listen_address: String,
    guest_listen_address: String,
    observability_listen_address: String,
    guest_identity: GvisorGuestIdentityConfig,
    read_database_max_connections: u32,
    work_database_max_connections: u32,
    database_acquire_timeout_milliseconds: u64,
    artifact_provider_catalog: AwsArtifactProviderCatalogConfig,
    broker: BrokerLimitsConfig,
    rpc: RpcLimitsConfig,
    scan_worker: ScanWorkerConfig,
    tls_handshake_timeout_milliseconds: u64,
    shutdown_grace_milliseconds: u64,
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum ArtifactBrokerAudience {
    DataWorker,
}

impl ArtifactBrokerAudience {
    const fn as_str(self) -> &'static str {
        match self {
            Self::DataWorker => "data_worker",
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(deny_unknown_fields)]
struct BrokerLimitsConfig {
    maximum_in_flight: usize,
    maximum_read_bytes: usize,
    operation_timeout_milliseconds: u64,
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(deny_unknown_fields)]
struct RpcLimitsConfig {
    maximum_request_bytes: usize,
    maximum_chunk_bytes: usize,
}

impl ArtifactBrokerProcessConfig {
    fn load() -> Result<Self, ProcessError> {
        let path = required_absolute_path(CONFIG_PATH_ENV)?;
        let bytes = read_bounded_file(&path, MAX_CONFIG_BYTES)?;
        let value = parse_strict_json(
            &bytes,
            JsonLimits {
                max_bytes: MAX_CONFIG_BYTES,
                max_depth: 16,
                max_items_per_array: 64,
                max_properties_per_object: 64,
                max_string_bytes: 16_384,
            },
        )
        .map_err(|_| ProcessError::InvalidConfiguration)?;
        let expected: Sha256Digest = required(CONFIG_DIGEST_ENV)?
            .parse()
            .map_err(|_| ProcessError::InvalidConfiguration)?;
        let actual: Sha256Digest = canonical_digest(&value)
            .map_err(|_| ProcessError::InvalidConfiguration)?
            .parse()
            .map_err(|_| ProcessError::InvalidConfiguration)?;
        if actual != expected {
            return Err(ProcessError::InvalidConfiguration);
        }
        let config: Self =
            serde_json::from_value(value).map_err(|_| ProcessError::InvalidConfiguration)?;
        config.validate()?;
        config.validate_deployment_audience(&required(AUDIENCE_ENV)?)?;
        Ok(config)
    }

    fn validate_deployment_audience(&self, deployment_audience: &str) -> Result<(), ProcessError> {
        if deployment_audience != self.audience.as_str() {
            return Err(ProcessError::InvalidConfiguration);
        }
        Ok(())
    }

    fn validate(&self) -> Result<(), ProcessError> {
        let controller_address: SocketAddr = self
            .controller_listen_address
            .parse()
            .map_err(|_| ProcessError::InvalidConfiguration)?;
        let guest_address: SocketAddr = self
            .guest_listen_address
            .parse()
            .map_err(|_| ProcessError::InvalidConfiguration)?;
        let observability_address: SocketAddr = self
            .observability_listen_address
            .parse()
            .map_err(|_| ProcessError::InvalidConfiguration)?;
        self.artifact_provider_catalog
            .validate()
            .map_err(|_| ProcessError::InvalidConfiguration)?;
        let broker = self.broker_limits()?;
        let rpc = self.rpc_limits()?;
        self.scan_worker
            .validate()
            .map_err(|_| ProcessError::InvalidConfiguration)?;
        let hard_limits = checked_in_hard_limit_profile();
        let sandbox_input_bytes =
            usize::try_from(hard_limits.capability_sandbox.input_bytes.hard_max)
                .map_err(|_| ProcessError::InvalidConfiguration)?;
        let sandbox_runtime_bundle_bytes =
            usize::try_from(hard_limits.capability_sandbox.runtime_bundle_bytes.hard_max)
                .map_err(|_| ProcessError::InvalidConfiguration)?;
        let required_read_bytes = sandbox_input_bytes.max(sandbox_runtime_bundle_bytes);
        if self.schema_version != 1
            || controller_address == guest_address
            || observability_address.port() == 0
            || observability_address == controller_address
            || observability_address == guest_address
            || self.guest_identity.clone().install().is_err()
            || !(2..=64).contains(&self.read_database_max_connections)
            || !(2..=64).contains(&self.work_database_max_connections)
            || self.database_acquire_timeout_milliseconds == 0
            || self.database_acquire_timeout_milliseconds > 30_000
            || self.tls_handshake_timeout_milliseconds == 0
            || self.tls_handshake_timeout_milliseconds > 30_000
            || self.shutdown_grace_milliseconds == 0
            || self.shutdown_grace_milliseconds > 120_000
            || broker.maximum_read_bytes < required_read_bytes
            || self
                .artifact_provider_catalog
                .s3_storage_bindings
                .iter()
                .any(|binding| {
                    usize::try_from(binding.maximum_object_bytes)
                        .map_or(true, |maximum| maximum < broker.maximum_read_bytes)
                })
            || rpc.maximum_request_bytes() == 0
        {
            return Err(ProcessError::InvalidConfiguration);
        }
        Ok(())
    }

    fn broker_limits(&self) -> Result<ArtifactBrokerLimits, ProcessError> {
        let limits = ArtifactBrokerLimits {
            maximum_in_flight: self.broker.maximum_in_flight,
            maximum_read_bytes: self.broker.maximum_read_bytes,
            operation_timeout: Duration::from_millis(self.broker.operation_timeout_milliseconds),
        };
        limits
            .validate()
            .map_err(|_| ProcessError::InvalidConfiguration)?;
        Ok(limits)
    }

    fn rpc_limits(&self) -> Result<ArtifactInternalRpcLimits, ProcessError> {
        ArtifactInternalRpcLimits::new(self.rpc.maximum_request_bytes, self.rpc.maximum_chunk_bytes)
            .map_err(|_| ProcessError::InvalidConfiguration)
    }
}

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("platform-artifact-data-worker failed: {error}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), ProcessError> {
    let config = ArtifactBrokerProcessConfig::load()?;
    let read_pool = PgPoolOptions::new()
        .max_connections(config.read_database_max_connections)
        .acquire_timeout(Duration::from_millis(
            config.database_acquire_timeout_milliseconds,
        ))
        .connect(&required(READ_DATABASE_URL_ENV)?)
        .await
        .map_err(|_| ProcessError::DatabaseUnavailable)?;
    verify_schema(&read_pool)
        .await
        .map_err(|_| ProcessError::SchemaMismatch)?;
    let work_pool = PgPoolOptions::new()
        .max_connections(config.work_database_max_connections)
        .acquire_timeout(Duration::from_millis(
            config.database_acquire_timeout_milliseconds,
        ))
        .connect(&required(WORK_DATABASE_URL_ENV)?)
        .await
        .map_err(|_| ProcessError::DatabaseUnavailable)?;
    verify_schema(&work_pool)
        .await
        .map_err(|_| ProcessError::SchemaMismatch)?;
    let read_repository = Arc::new(PgRepository::new(read_pool));
    let work_repository = Arc::new(PgRepository::new(work_pool));
    let providers = AwsArtifactProviderCatalog::install(config.artifact_provider_catalog.clone())
        .await
        .map_err(|_| ProcessError::InvalidConfiguration)?;
    providers
        .check_readiness()
        .await
        .map_err(|_| ProcessError::ProviderUnavailable)?;
    let (unsealer, stores) = providers.into_components();
    let broker_limits = config.broker_limits()?;
    let scan_reader = Arc::new(
        BrokeredArtifactScannerReader::new(
            work_repository.clone(),
            Arc::clone(&unsealer),
            stores.clone(),
            broker_limits,
        )
        .map_err(|_| ProcessError::InvalidConfiguration)?,
    );
    let scanner = IntegrityArtifactScanner::new(Arc::clone(&scan_reader), &config.scan_worker);
    let scheduler_reader = Arc::new(SchedulerRpcArtifactBroker {
        typed_plan_reader: BrokeredSchedulerTypedPlanReader::new(
            read_repository.clone(),
            Arc::clone(&unsealer),
            stores.clone(),
            broker_limits,
        )
        .map_err(|_| ProcessError::InvalidConfiguration)?,
        run_value_reader: BrokeredSchedulerRunValueReader::new(
            read_repository.clone(),
            Arc::clone(&unsealer),
            stores.clone(),
            broker_limits,
        )
        .map_err(|_| ProcessError::InvalidConfiguration)?,
        skill_package_reader: BrokeredSchedulerSkillPackageReader::new(
            read_repository.clone(),
            Arc::clone(&unsealer),
            stores.clone(),
            broker_limits,
        )
        .map_err(|_| ProcessError::InvalidConfiguration)?,
    });
    let broker = BrokeredSandboxArtifactBroker::new(
        read_repository.clone(),
        unsealer,
        stores,
        broker_limits,
    )
    .map_err(|_| ProcessError::InvalidConfiguration)?;
    let sandbox_broker = Arc::new(SandboxRpcArtifactBroker { broker });
    let rpc_limits = config.rpc_limits()?;
    let maximum = rpc_limits.maximum_message_bytes();
    let sandbox_service = {
        let service = ArtifactSandboxBrokerServiceServer::new(
            ArtifactSandboxBrokerGrpcService::new(Arc::clone(&sandbox_broker), rpc_limits),
        )
        .max_encoding_message_size(maximum)
        .max_decoding_message_size(maximum);
        tonic::service::interceptor::InterceptedService::new(
            service,
            SandboxControllerWorkloadIdentity,
        )
    };
    let scheduler_service = {
        let service = ArtifactSchedulerServiceServer::new(ArtifactSchedulerGrpcService::new(
            Arc::clone(&scheduler_reader),
            rpc_limits,
        ))
        .max_encoding_message_size(maximum)
        .max_decoding_message_size(maximum);
        tonic::service::interceptor::InterceptedService::new(service, SchedulerWorkloadIdentity)
    };
    let guest_materializer = Arc::new(GvisorGuestMaterializer {
        authority: Arc::clone(&read_repository),
        broker: Arc::clone(&sandbox_broker),
    });
    let guest_service = {
        let service = ArtifactGvisorGuestServiceServer::new(ArtifactGvisorGuestGrpcService::new(
            guest_materializer,
            rpc_limits,
        ))
        .max_encoding_message_size(maximum)
        .max_decoding_message_size(maximum);
        tonic::service::interceptor::InterceptedService::new(
            service,
            config
                .guest_identity
                .clone()
                .install()
                .map_err(|_| ProcessError::InvalidConfiguration)?,
        )
    };
    let cert = read_bounded_file(&required_absolute_path(CERT_PATH_ENV)?, MAX_TLS_FILE_BYTES)?;
    let key = read_bounded_file(&required_absolute_path(KEY_PATH_ENV)?, MAX_TLS_FILE_BYTES)?;
    let controller_tls = ServerTlsConfig::new()
        .identity(Identity::from_pem(cert.clone(), key.clone()))
        .client_ca_root(Certificate::from_pem(read_bounded_file(
            &required_absolute_path(CLIENT_CA_PATH_ENV)?,
            MAX_TLS_FILE_BYTES,
        )?))
        .timeout(Duration::from_millis(
            config.tls_handshake_timeout_milliseconds,
        ));
    let guest_tls = ServerTlsConfig::new()
        .identity(Identity::from_pem(cert, key))
        .timeout(Duration::from_millis(
            config.tls_handshake_timeout_milliseconds,
        ));
    let controller_address = config
        .controller_listen_address
        .parse()
        .map_err(|_| ProcessError::InvalidConfiguration)?;
    let guest_address = config
        .guest_listen_address
        .parse()
        .map_err(|_| ProcessError::InvalidConfiguration)?;
    eprintln!(
        "platform-artifact-data-worker started for {} audience",
        config.audience.as_str()
    );
    let (controller_shutdown_sender, controller_shutdown_receiver) =
        tokio::sync::oneshot::channel();
    let (guest_shutdown_sender, guest_shutdown_receiver) = tokio::sync::oneshot::channel();
    let (observability_shutdown_sender, observability_shutdown_receiver) =
        tokio::sync::oneshot::channel();
    let (scan_shutdown_sender, scan_shutdown_receiver) = tokio::sync::watch::channel(false);
    let controller_server = Server::builder()
        .tls_config(controller_tls)
        .map_err(|_| ProcessError::InvalidTls)?
        .add_service(sandbox_service)
        .add_service(scheduler_service)
        .serve_with_shutdown(controller_address, async {
            let _ = controller_shutdown_receiver.await;
        });
    let guest_server = Server::builder()
        .tls_config(guest_tls)
        .map_err(|_| ProcessError::InvalidTls)?
        .add_service(guest_service)
        .serve_with_shutdown(guest_address, async {
            let _ = guest_shutdown_receiver.await;
        });
    let scan_worker = run_scan_worker(
        Arc::clone(&work_repository),
        scanner,
        config.scan_worker.clone(),
        scan_shutdown_receiver,
    );
    let metrics = Arc::new(
        ProcessHttpMetrics::install_with_capacities(
            "artifact-data-worker",
            PROCESS_OBSERVABILITY_OPERATIONS,
            vec![
                artifact_capacity_metric(
                    "scan_read",
                    scan_reader,
                    BrokeredArtifactScannerReader::capacity_snapshot,
                ),
                artifact_capacity_metric(
                    "scheduler_typed_plan",
                    Arc::clone(&scheduler_reader),
                    SchedulerRpcArtifactBroker::typed_plan_capacity,
                ),
                artifact_capacity_metric(
                    "scheduler_run_value",
                    Arc::clone(&scheduler_reader),
                    SchedulerRpcArtifactBroker::run_value_capacity,
                ),
                artifact_capacity_metric(
                    "scheduler_skill_package",
                    scheduler_reader,
                    SchedulerRpcArtifactBroker::skill_package_capacity,
                ),
                artifact_capacity_metric(
                    "sandbox_read",
                    sandbox_broker,
                    SandboxRpcArtifactBroker::capacity_snapshot,
                ),
            ],
        )
        .map_err(|_| ProcessError::InvalidConfiguration)?,
    );
    let observability_listener =
        tokio::net::TcpListener::bind(&config.observability_listen_address)
            .await
            .map_err(|_| ProcessError::ObservabilityUnavailable)?;
    let observability = axum::serve(
        observability_listener,
        process_observability_router(Arc::clone(&metrics)),
    )
    .with_graceful_shutdown(async {
        let _ = observability_shutdown_receiver.await;
    })
    .into_future();
    tokio::pin!(controller_server);
    tokio::pin!(guest_server);
    tokio::pin!(scan_worker);
    tokio::pin!(observability);
    metrics.mark_ready();
    tokio::select! {
        result = &mut controller_server => result.map_err(|_| ProcessError::RpcUnavailable),
        result = &mut guest_server => result.map_err(|_| ProcessError::RpcUnavailable),
        result = &mut scan_worker => result.map_err(|_| ProcessError::WorkerUnavailable),
        result = &mut observability => result.map_err(|_| ProcessError::ObservabilityUnavailable),
        signal = shutdown_signal() => {
            signal?;
            let _ = controller_shutdown_sender.send(());
            let _ = guest_shutdown_sender.send(());
            let _ = observability_shutdown_sender.send(());
            let _ = scan_shutdown_sender.send(true);
            tokio::time::timeout(
                Duration::from_millis(config.shutdown_grace_milliseconds),
                async {
                    let (controller, guest, worker, metrics_server) = tokio::join!(
                        &mut controller_server,
                        &mut guest_server,
                        &mut scan_worker,
                        &mut observability,
                    );
                    controller.map_err(|_| ProcessError::RpcUnavailable)?;
                    guest.map_err(|_| ProcessError::RpcUnavailable)?;
                    worker.map_err(|_| ProcessError::WorkerUnavailable)?;
                    metrics_server.map_err(|_| ProcessError::ObservabilityUnavailable)
                },
            )
            .await
            .map_err(|_| ProcessError::ShutdownDeadlineExceeded)?
        }
    }
}

async fn shutdown_signal() -> Result<(), ProcessError> {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .map_err(|_| ProcessError::SignalUnavailable)?;
        tokio::select! {
            received = terminate.recv() => received.ok_or(ProcessError::SignalUnavailable),
            received = tokio::signal::ctrl_c() => received.map_err(|_| ProcessError::SignalUnavailable),
        }
    }
    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c()
            .await
            .map_err(|_| ProcessError::SignalUnavailable)
    }
}

fn required(name: &'static str) -> Result<String, ProcessError> {
    std::env::var(name)
        .ok()
        .filter(|value| !value.is_empty() && value.len() <= 16_384)
        .ok_or(ProcessError::MissingConfiguration(name))
}

fn required_absolute_path(name: &'static str) -> Result<PathBuf, ProcessError> {
    let path = PathBuf::from(required(name)?);
    if !path.is_absolute() {
        return Err(ProcessError::InvalidConfiguration);
    }
    Ok(path)
}

fn read_bounded_file(path: &PathBuf, maximum: usize) -> Result<Vec<u8>, ProcessError> {
    let file = std::fs::File::open(path).map_err(|_| ProcessError::FileUnavailable)?;
    let metadata = file.metadata().map_err(|_| ProcessError::FileUnavailable)?;
    if !metadata.is_file()
        || metadata.len() == 0
        || metadata.len() > u64::try_from(maximum).unwrap_or(u64::MAX)
    {
        return Err(ProcessError::FileUnavailable);
    }
    let mut bytes = Vec::with_capacity(usize::try_from(metadata.len()).unwrap_or(maximum));
    file.take(u64::try_from(maximum).unwrap_or(u64::MAX) + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| ProcessError::FileUnavailable)?;
    if bytes.is_empty() || bytes.len() > maximum {
        return Err(ProcessError::FileUnavailable);
    }
    Ok(bytes)
}

#[derive(Debug)]
enum ProcessError {
    MissingConfiguration(&'static str),
    InvalidConfiguration,
    FileUnavailable,
    DatabaseUnavailable,
    SchemaMismatch,
    ProviderUnavailable,
    InvalidTls,
    RpcUnavailable,
    WorkerUnavailable,
    ObservabilityUnavailable,
    SignalUnavailable,
    ShutdownDeadlineExceeded,
}

impl fmt::Display for ProcessError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingConfiguration(name) => write!(formatter, "missing configuration {name}"),
            Self::InvalidConfiguration => formatter.write_str("invalid configuration"),
            Self::FileUnavailable => formatter.write_str("configuration file unavailable"),
            Self::DatabaseUnavailable => formatter.write_str("database unavailable"),
            Self::SchemaMismatch => formatter.write_str("database schema mismatch"),
            Self::ProviderUnavailable => formatter.write_str("Artifact provider unavailable"),
            Self::InvalidTls => formatter.write_str("invalid TLS configuration"),
            Self::RpcUnavailable => formatter.write_str("Artifact RPC unavailable"),
            Self::WorkerUnavailable => formatter.write_str("Artifact worker unavailable"),
            Self::ObservabilityUnavailable => {
                formatter.write_str("Artifact observability unavailable")
            }
            Self::SignalUnavailable => formatter.write_str("shutdown signal unavailable"),
            Self::ShutdownDeadlineExceeded => formatter.write_str("shutdown deadline exceeded"),
        }
    }
}

impl Error for ProcessError {}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_config() -> ArtifactBrokerProcessConfig {
        serde_json::from_value(serde_json::json!({
            "schema_version": 1,
            "audience": "data_worker",
            "controller_listen_address": "0.0.0.0:9443",
            "guest_listen_address": "0.0.0.0:9444",
            "observability_listen_address": "0.0.0.0:9090",
            "guest_identity": {
                "issuer": "https://kubernetes.default.svc.cluster.local",
                "audience": "insight-platform-gvisor-guest",
                "namespace": "insight-platform-sandbox-guests",
                "service_account_name": "insight-platform-gvisor-guest",
                "jwks": {"keys": [{
                    "kty": "RSA",
                    "kid": "guest-key-1",
                    "use": "sig",
                    "alg": "RS256",
                    "n": "sXch4-7u-lQpR0lJHJj3-JpGcC7dCqHj8P5mW52w8GQ",
                    "e": "AQAB"
                }]},
                "jwks_digest": "sha256:ed90fd7173d7d068236917e3cf1f9f58a55977a88a6355375591ee694347ad49"
            },
            "read_database_max_connections": 8,
            "work_database_max_connections": 8,
            "database_acquire_timeout_milliseconds": 5000,
            "artifact_provider_catalog": {
                "schema_version": 1,
                "write_storage_binding_digest": "sha256:b5d4ea2254a7770284738b6ca76a8e7833899ebbe796d61a113f57bd5609b630",
                "s3_storage_bindings": [{
                    "schema_version": 1,
                    "storage_binding_digest": "sha256:b5d4ea2254a7770284738b6ca76a8e7833899ebbe796d61a113f57bd5609b630",
                    "endpoint": "https://s3.example.invalid",
                    "region": "us-east-1",
                    "bucket": "insight-platform",
                    "force_path_style": false,
                    "kms_binding_digest": "sha256:ee8b022acf9fbbb8134127266a11a93cbca559aea8ff563d9cb95965e40719d8",
                    "connect_timeout_milliseconds": 5000,
                    "operation_timeout_milliseconds": 30000,
                    "maximum_object_bytes": 67108864
                }],
                "kms_key_bindings": [{
                    "schema_version": 1,
                    "kms_binding_digest": "sha256:ee8b022acf9fbbb8134127266a11a93cbca559aea8ff563d9cb95965e40719d8",
                    "endpoint": "https://kms.us-east-1.amazonaws.com",
                    "region": "us-east-1",
                    "key_id": "arn:aws:kms:us-east-1:111122223333:key/12345678-1234-1234-1234-123456789012",
                    "connect_timeout_milliseconds": 5000,
                    "operation_timeout_milliseconds": 30000
                }]
            },
            "broker": {
                "maximum_in_flight": 128,
                "maximum_read_bytes": 67108864,
                "operation_timeout_milliseconds": 30000
            },
            "rpc": {
                "maximum_request_bytes": 1048576,
                "maximum_chunk_bytes": 262144
            },
            "scan_worker": {
                "scanner_contract_digest": "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "ruleset_digest": "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                "claim_batch": 8,
                "lease_milliseconds": 120000,
                "receipt_ttl_milliseconds": 3600000,
                "poll_milliseconds": 250
            },
            "tls_handshake_timeout_milliseconds": 5000,
            "shutdown_grace_milliseconds": 30000
        }))
        .unwrap()
    }

    #[test]
    fn process_config_closes_provider_database_and_rpc_bounds() {
        let valid = valid_config();
        valid.validate().unwrap();
        valid.validate_deployment_audience("data_worker").unwrap();
        assert!(valid.validate_deployment_audience("model").is_err());
        assert!(
            serde_json::from_value::<ArtifactBrokerAudience>(serde_json::json!("other")).is_err()
        );

        let mut invalid_database = valid.clone();
        invalid_database.read_database_max_connections = 1;
        assert!(invalid_database.validate().is_err());

        let mut invalid_read = valid.clone();
        invalid_read.broker.maximum_read_bytes = 1024;
        assert!(invalid_read.validate().is_err());

        let mut invalid_sandbox_read = valid.clone();
        invalid_sandbox_read.audience = ArtifactBrokerAudience::DataWorker;
        invalid_sandbox_read.broker.maximum_read_bytes = 1024;
        assert!(invalid_sandbox_read.validate().is_err());

        let mut invalid_chunk = valid;
        invalid_chunk.rpc.maximum_chunk_bytes = 0;
        assert!(invalid_chunk.validate().is_err());
    }
}
