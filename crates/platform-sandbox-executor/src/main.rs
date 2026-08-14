use hyper_util::rt::TokioIo;
use insight_platform_contracts::{
    canonical_digest, checked_in_hard_limit_profile, parse_strict_json, JsonLimits, ResourceId,
    ResourceKind, SandboxIsolationClass, Sha256Digest, WorkClass, WorkerManifest,
};
use insight_platform_sandbox::{
    InstalledSandboxBackendDescriptor, InstalledSandboxBackendRegistry,
    RegisterWasiExecutorProcessGeneration, SandboxCommandLimits, SandboxExecutionControlRouter,
    SandboxExecutorBackend, SandboxExecutorHost, SandboxExecutorWorker, SandboxHeartbeatConfig,
    SandboxIsolationBackendKind, SandboxProcessGenerationIsolation, WasiArtifactBroker,
    WasiExecutorProcessRegistrar, WasiGrantRevoker, WasiValueValidator,
};
use insight_platform_sandbox_executor::{
    ManagedMcpSandboxSessionExecutorDriver, ManagedMcpSandboxSessionRecoveryConfig,
    ManagedMcpSandboxSessionRecoveryDriver, ManagedMcpSandboxSessionRecoveryTiming,
    RegisteredManagedMcpSandboxSessionExecutor, RegisteredSandboxJobExecutor,
    SandboxExecutorBinding, SandboxExecutorDriver, SandboxExecutorDriverConfig,
    SandboxExecutorDriverTiming, UuidExecutorIdentityFactory,
};
use insight_platform_sandbox_rpc::{
    NatsSandboxControlListener, NatsSandboxControlTransportConfig, SandboxAuthorityGrpcClient,
    SandboxBrokerGrpcClient, SandboxExecutorProcessRegistrationGrpcClient,
    SandboxInternalRpcLimits, SandboxIsolationProviderGrpcClient,
    SandboxManagedMcpSessionAuthorityGrpcClient, SandboxManagedMcpSessionProviderGrpcClient,
    SandboxMicroVmExecutorProcessRegistrationGrpcClient,
};
use insight_platform_sandbox_wasi::{
    SystemWasiExecutorClock, WasiExecutorBackendConfig, WasmtimeSandboxExecutorBackend,
    WASI_ABI_V1_RUNTIME_VERSION,
};
use insight_platform_worker::LocalWorkerPools;
use serde::Deserialize;
use std::{error::Error, fmt, path::PathBuf, sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity};
use url::Url;
use uuid::Uuid;

const CONFIG_PATH_ENV: &str = "PLATFORM_SANDBOX_EXECUTOR_CONFIG";
const CONFIG_DIGEST_ENV: &str = "PLATFORM_SANDBOX_EXECUTOR_CONFIG_DIGEST";
const GRPC_CA_PATH_ENV: &str = "PLATFORM_SANDBOX_GRPC_CA_PATH";
const GRPC_CERT_PATH_ENV: &str = "PLATFORM_SANDBOX_GRPC_CERT_PATH";
const GRPC_KEY_PATH_ENV: &str = "PLATFORM_SANDBOX_GRPC_KEY_PATH";
const NATS_CA_PATH_ENV: &str = "PLATFORM_SANDBOX_NATS_CA_PATH";
const NATS_CERT_PATH_ENV: &str = "PLATFORM_SANDBOX_NATS_CERT_PATH";
const NATS_KEY_PATH_ENV: &str = "PLATFORM_SANDBOX_NATS_KEY_PATH";
const ATTESTOR_CA_PATH_ENV: &str = "PLATFORM_SANDBOX_ATTESTOR_CA_PATH";
const PROVIDER_CA_PATH_ENV: &str = "PLATFORM_SANDBOX_PROVIDER_CA_PATH";
const MAX_CONFIG_BYTES: usize = 64 * 1024;
const MAX_TLS_FILE_BYTES: usize = 1024 * 1024;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct ExecutorProcessConfig {
    schema_version: u32,
    worker_manifest: WorkerManifest,
    backend: ExecutorBackendProcessConfig,
    backend_contract_digest: Sha256Digest,
    authority_endpoint: String,
    authority_tls_server_name: String,
    process_registration_attestor_socket_path: String,
    process_registration_attestor_tls_server_name: String,
    process_registration_attestor_identity_digest: Sha256Digest,
    nats_endpoint: String,
    receipt_ttl_seconds: u64,
    claim_scan_milliseconds: u64,
    claim_failure_backoff_milliseconds: u64,
    drain_grace_milliseconds: u64,
    control_request_timeout_milliseconds: u64,
    connect_timeout_milliseconds: u64,
    request_timeout_milliseconds: u64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum ExecutorBackendProcessConfig {
    Wasi {
        runtime_version: String,
    },
    MicroVm {
        provider_socket_path: String,
        provider_tls_server_name: String,
        managed_recovery: ManagedMcpRecoveryProcessConfig,
    },
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(deny_unknown_fields)]
struct ManagedMcpRecoveryProcessConfig {
    shard_index: u16,
    shard_count: u16,
    scan_milliseconds: u64,
    scan_jitter_milliseconds: u64,
    failure_backoff_milliseconds: u64,
}

impl ManagedMcpRecoveryProcessConfig {
    fn validate(self) -> bool {
        self.shard_count > 0
            && self.shard_index < self.shard_count
            && self.scan_milliseconds > 0
            && self.scan_jitter_milliseconds < self.scan_milliseconds
            && self.failure_backoff_milliseconds > 0
            && self.failure_backoff_milliseconds <= self.scan_milliseconds
    }
}

impl ExecutorBackendProcessConfig {
    fn descriptor(
        &self,
        worker_manifest_digest: Sha256Digest,
        backend_contract_digest: Sha256Digest,
    ) -> InstalledSandboxBackendDescriptor {
        let (backend_kind, isolation_class) = match self {
            Self::Wasi { .. } => (
                SandboxIsolationBackendKind::Wasi,
                SandboxIsolationClass::Wasm,
            ),
            Self::MicroVm { .. } => (
                SandboxIsolationBackendKind::MicroVm,
                SandboxIsolationClass::MicroVm,
            ),
        };
        InstalledSandboxBackendDescriptor {
            backend_kind,
            isolation_class,
            worker_manifest_digest,
            backend_contract_digest,
        }
    }

    fn worker_role(&self) -> &'static str {
        match self {
            Self::Wasi { .. } => "sandbox-executor.wasi",
            Self::MicroVm { .. } => "sandbox-executor.microvm",
        }
    }

    fn validate(&self) -> bool {
        match self {
            Self::Wasi { runtime_version } => runtime_version == WASI_ABI_V1_RUNTIME_VERSION,
            Self::MicroVm {
                provider_socket_path,
                provider_tls_server_name,
                managed_recovery,
            } => {
                closed_unix_socket_path(provider_socket_path)
                    && stable_dns_name(provider_tls_server_name)
                    && managed_recovery.validate()
            }
        }
    }

    fn managed_recovery(&self) -> Option<ManagedMcpRecoveryProcessConfig> {
        match self {
            Self::Wasi { .. } => None,
            Self::MicroVm {
                managed_recovery, ..
            } => Some(*managed_recovery),
        }
    }
}

impl ExecutorProcessConfig {
    fn load() -> Result<Self, ProcessError> {
        let path = required_absolute_path(CONFIG_PATH_ENV)?;
        let bytes = read_bounded(&path, MAX_CONFIG_BYTES)?;
        let value = parse_strict_json(
            &bytes,
            JsonLimits {
                max_bytes: MAX_CONFIG_BYTES,
                max_depth: 16,
                max_items_per_array: 64,
                max_properties_per_object: 64,
                max_string_bytes: 8_192,
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
        Ok(config)
    }

    fn validate(&self) -> Result<(), ProcessError> {
        self.worker_manifest
            .validate()
            .map_err(|_| ProcessError::InvalidConfiguration)?;
        if self.schema_version != 1
            || self.worker_manifest.worker_role != self.backend.worker_role()
            || self.worker_manifest.work_class != WorkClass::Sandbox
            || !self.backend.validate()
            || !closed_service_endpoint(&self.authority_endpoint, "https")
            || !stable_dns_name(&self.authority_tls_server_name)
            || !closed_unix_socket_path(&self.process_registration_attestor_socket_path)
            || !stable_dns_name(&self.process_registration_attestor_tls_server_name)
            || !closed_service_endpoint(&self.nats_endpoint, "tls")
            || self.receipt_ttl_seconds == 0
            || self.claim_scan_milliseconds == 0
            || self.claim_failure_backoff_milliseconds == 0
            || self.claim_failure_backoff_milliseconds > self.claim_scan_milliseconds
            || self.drain_grace_milliseconds == 0
            || self.control_request_timeout_milliseconds == 0
            || self.connect_timeout_milliseconds == 0
            || self.request_timeout_milliseconds == 0
        {
            return Err(ProcessError::InvalidConfiguration);
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("platform-sandbox-executor failed: {error}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), ProcessError> {
    let config = ExecutorProcessConfig::load()?;
    let profile = checked_in_hard_limit_profile();
    let limits = SandboxCommandLimits::from_profile(&profile)
        .map_err(|_| ProcessError::InvalidConfiguration)?;
    let process_generation_id =
        ResourceId::from_uuid_v7(ResourceKind::WorkerProcessGeneration, Uuid::now_v7())
            .map_err(|_| ProcessError::InvalidConfiguration)?;
    let pools = LocalWorkerPools::new(
        config.worker_manifest.clone(),
        process_generation_id.clone(),
    )
    .map_err(|_| ProcessError::InvalidConfiguration)?;
    let worker_manifest_digest = config
        .worker_manifest
        .canonical_digest()
        .map_err(|_| ProcessError::InvalidConfiguration)?;

    let rpc_limits = SandboxInternalRpcLimits::from_profile(&profile)
        .map_err(|_| ProcessError::InvalidConfiguration)?;
    let registration_channel = connect_process_registration_attestor(&config).await?;
    let registration: Arc<dyn WasiExecutorProcessRegistrar> = match &config.backend {
        ExecutorBackendProcessConfig::Wasi { .. } => {
            Arc::new(SandboxExecutorProcessRegistrationGrpcClient::new(
                registration_channel,
                rpc_limits,
                config.process_registration_attestor_identity_digest.clone(),
            ))
        }
        ExecutorBackendProcessConfig::MicroVm { .. } => {
            Arc::new(SandboxMicroVmExecutorProcessRegistrationGrpcClient::new(
                registration_channel,
                rpc_limits,
                config.process_registration_attestor_identity_digest.clone(),
            ))
        }
    };
    let process_identity = registration
        .register(RegisterWasiExecutorProcessGeneration {
            worker_process_generation_id: process_generation_id.clone(),
            worker_manifest_digest: worker_manifest_digest.clone(),
            isolation_backend_contract_digest: config.backend_contract_digest.clone(),
        })
        .await
        .map_err(|_| ProcessError::AttestorUnavailable)?;

    let channel = connect_authority(&config).await?;
    let authority = Arc::new(SandboxAuthorityGrpcClient::new(channel.clone(), rpc_limits));
    let managed_authority = matches!(
        &config.backend,
        ExecutorBackendProcessConfig::MicroVm { .. }
    )
    .then(|| {
        Arc::new(SandboxManagedMcpSessionAuthorityGrpcClient::new(
            channel.clone(),
            rpc_limits,
        ))
    });
    let descriptor = config.backend.descriptor(
        worker_manifest_digest,
        config.backend_contract_digest.clone(),
    );
    let (backend, managed_provider): (
        Arc<dyn SandboxExecutorBackend>,
        Option<Arc<SandboxManagedMcpSessionProviderGrpcClient>>,
    ) = match &config.backend {
        ExecutorBackendProcessConfig::Wasi { .. } => {
            let broker = Arc::new(SandboxBrokerGrpcClient::new(channel, rpc_limits));
            let artifacts: Arc<dyn WasiArtifactBroker> = broker.clone();
            let value_validator: Arc<dyn WasiValueValidator> = broker.clone();
            let grant_revoker: Arc<dyn WasiGrantRevoker> = broker.clone();
            let process_isolation: Arc<dyn SandboxProcessGenerationIsolation> = broker;
            let mut backend_config = WasiExecutorBackendConfig::production(
                descriptor.clone(),
                process_generation_id.clone(),
            );
            backend_config.maximum_concurrent_executions =
                usize::from(config.worker_manifest.max_concurrency);
            let backend: Arc<dyn SandboxExecutorBackend> = Arc::new(
                WasmtimeSandboxExecutorBackend::new(
                    backend_config,
                    artifacts,
                    value_validator,
                    grant_revoker,
                    process_isolation,
                    Arc::new(SystemWasiExecutorClock),
                )
                .map_err(|_| ProcessError::InvalidConfiguration)?,
            );
            (backend, None)
        }
        ExecutorBackendProcessConfig::MicroVm { .. } => {
            let provider_channel = connect_isolation_provider(&config).await?;
            let managed_provider = Arc::new(SandboxManagedMcpSessionProviderGrpcClient::new(
                provider_channel.clone(),
                rpc_limits,
            ));
            let backend: Arc<dyn SandboxExecutorBackend> = Arc::new(
                SandboxIsolationProviderGrpcClient::new(
                    provider_channel,
                    rpc_limits,
                    descriptor,
                    process_generation_id.clone(),
                )
                .map_err(|_| ProcessError::InvalidConfiguration)?,
            );
            (backend, Some(managed_provider))
        }
    };
    let mut registry = InstalledSandboxBackendRegistry::default();
    registry
        .install(backend)
        .map_err(|_| ProcessError::InvalidConfiguration)?;
    let host = Arc::new(SandboxExecutorHost::new(registry, limits));
    let heartbeat = SandboxHeartbeatConfig::from_profile(&profile)
        .map_err(|_| ProcessError::InvalidConfiguration)?;
    let worker = Arc::new(
        SandboxExecutorWorker::with_heartbeat(host, Arc::clone(&authority), limits, heartbeat)
            .map_err(|_| ProcessError::InvalidConfiguration)?,
    );
    let control_router = Arc::new(
        SandboxExecutionControlRouter::new(usize::from(config.worker_manifest.max_concurrency))
            .map_err(|_| ProcessError::InvalidConfiguration)?,
    );
    let executor = Arc::new(RegisteredSandboxJobExecutor::new(
        worker,
        Arc::clone(&control_router),
    ));
    let identities = Arc::new(UuidExecutorIdentityFactory);
    let binding = SandboxExecutorBinding {
        isolation_backend_contract_digest: config.backend_contract_digest.clone(),
        executor_identity_digest: process_identity.executor_identity_digest,
        attestor_route: process_identity.attestor_route,
    };
    let driver_config = SandboxExecutorDriverConfig::from_profile(
        &profile,
        Duration::from_secs(config.receipt_ttl_seconds),
        SandboxExecutorDriverTiming {
            safety_scan_interval: Duration::from_millis(config.claim_scan_milliseconds),
            claim_failure_backoff: Duration::from_millis(config.claim_failure_backoff_milliseconds),
            drain_grace: Duration::from_millis(config.drain_grace_milliseconds),
        },
    )
    .map_err(|_| ProcessError::InvalidConfiguration)?;
    let driver = SandboxExecutorDriver::new(
        Arc::clone(&authority),
        executor,
        Arc::clone(&identities),
        pools.clone(),
        binding.clone(),
        driver_config,
    )
    .map_err(|_| ProcessError::InvalidConfiguration)?;
    let (managed_driver, managed_recovery_driver) = match (managed_authority, managed_provider) {
        (Some(authority), Some(provider)) => {
            let executor = Arc::new(
                RegisteredManagedMcpSandboxSessionExecutor::new(
                    Arc::clone(&authority),
                    Arc::clone(&provider),
                    Arc::clone(&identities),
                    limits,
                    heartbeat,
                    Duration::from_secs(config.receipt_ttl_seconds),
                )
                .map_err(|_| ProcessError::InvalidConfiguration)?,
            );
            let managed_driver = ManagedMcpSandboxSessionExecutorDriver::new(
                Arc::clone(&authority),
                executor,
                Arc::clone(&identities),
                pools.clone(),
                binding.clone(),
                driver_config,
            )
            .map_err(|_| ProcessError::InvalidConfiguration)?;
            let recovery = config
                .backend
                .managed_recovery()
                .ok_or(ProcessError::InvalidConfiguration)?;
            let recovery_config = ManagedMcpSandboxSessionRecoveryConfig::from_profile(
                &profile,
                insight_platform_sandbox::ManagedMcpSandboxSessionRecoveryShard {
                    index: recovery.shard_index,
                    count: recovery.shard_count,
                },
                Duration::from_secs(config.receipt_ttl_seconds),
                ManagedMcpSandboxSessionRecoveryTiming {
                    scan_interval: Duration::from_millis(recovery.scan_milliseconds),
                    scan_jitter: Duration::from_millis(recovery.scan_jitter_milliseconds),
                    failure_backoff: Duration::from_millis(recovery.failure_backoff_milliseconds),
                },
            )
            .map_err(|_| ProcessError::InvalidConfiguration)?;
            let recovery_driver = ManagedMcpSandboxSessionRecoveryDriver::new(
                authority,
                provider,
                identities,
                pools,
                binding,
                recovery_config,
            )
            .map_err(|_| ProcessError::InvalidConfiguration)?;
            (Some(managed_driver), Some(recovery_driver))
        }
        (None, None) => (None, None),
        _ => return Err(ProcessError::InvalidConfiguration),
    };

    let nats = connect_nats(&config).await?;
    let local_sink: Arc<dyn insight_platform_sandbox::SandboxControlSignalSink> = control_router;
    let listener = NatsSandboxControlListener::bind(
        nats,
        NatsSandboxControlTransportConfig::from_profile(
            &profile,
            Duration::from_millis(config.control_request_timeout_milliseconds),
        )
        .map_err(|_| ProcessError::InvalidConfiguration)?,
        process_generation_id,
        local_sink,
    )
    .await
    .map_err(|_| ProcessError::NatsUnavailable)?;

    let shutdown = CancellationToken::new();
    let mut driver_task = tokio::spawn(driver.run(shutdown.child_token()));
    let managed_shutdown = shutdown.child_token();
    let mut managed_driver_task = tokio::spawn(async move {
        match managed_driver {
            Some(driver) => driver.run(managed_shutdown).await,
            None => {
                managed_shutdown.cancelled().await;
                Ok(Default::default())
            }
        }
    });
    let recovery_shutdown = shutdown.child_token();
    let mut managed_recovery_task = tokio::spawn(async move {
        match managed_recovery_driver {
            Some(driver) => driver.run(recovery_shutdown).await,
            None => {
                recovery_shutdown.cancelled().await;
                Ok(())
            }
        }
    });
    let mut control_task = tokio::spawn(listener.run(shutdown.child_token()));
    let trigger = tokio::select! {
        signal = tokio::signal::ctrl_c() => SupervisorTrigger::Signal(signal.is_ok()),
        joined = &mut driver_task => SupervisorTrigger::Driver(joined),
        joined = &mut managed_driver_task => SupervisorTrigger::ManagedDriver(joined),
        joined = &mut managed_recovery_task => SupervisorTrigger::ManagedRecovery(joined),
        joined = &mut control_task => SupervisorTrigger::Control(joined),
    };
    shutdown.cancel();
    let grace = Duration::from_millis(config.drain_grace_milliseconds);
    match trigger {
        SupervisorTrigger::Signal(signal_received) => {
            let drained = tokio::time::timeout(grace, async {
                map_driver_join((&mut driver_task).await)?;
                map_driver_join((&mut managed_driver_task).await)?;
                map_recovery_join((&mut managed_recovery_task).await)?;
                map_control_join((&mut control_task).await)
            })
            .await
            .map_err(|_| ProcessError::DrainRequiresProcessTermination)?;
            drained?;
            if signal_received {
                Ok(())
            } else {
                Err(ProcessError::SignalUnavailable)
            }
        }
        SupervisorTrigger::Driver(joined) => {
            let primary =
                map_driver_join(joined).and(Err(ProcessError::ExecutorExitedUnexpectedly));
            await_driver_shutdown(&mut managed_driver_task, grace).await?;
            await_recovery_shutdown(&mut managed_recovery_task, grace).await?;
            await_control_shutdown(&mut control_task, grace).await?;
            primary
        }
        SupervisorTrigger::ManagedDriver(joined) => {
            let primary =
                map_driver_join(joined).and(Err(ProcessError::ExecutorExitedUnexpectedly));
            await_driver_shutdown(&mut driver_task, grace).await?;
            await_recovery_shutdown(&mut managed_recovery_task, grace).await?;
            await_control_shutdown(&mut control_task, grace).await?;
            primary
        }
        SupervisorTrigger::ManagedRecovery(joined) => {
            let primary =
                map_recovery_join(joined).and(Err(ProcessError::ExecutorExitedUnexpectedly));
            await_driver_shutdown(&mut driver_task, grace).await?;
            await_driver_shutdown(&mut managed_driver_task, grace).await?;
            await_control_shutdown(&mut control_task, grace).await?;
            primary
        }
        SupervisorTrigger::Control(joined) => {
            let primary =
                map_control_join(joined).and(Err(ProcessError::ControlExitedUnexpectedly));
            await_driver_shutdown(&mut driver_task, grace).await?;
            await_driver_shutdown(&mut managed_driver_task, grace).await?;
            await_recovery_shutdown(&mut managed_recovery_task, grace).await?;
            primary
        }
    }
}

enum SupervisorTrigger {
    Signal(bool),
    Driver(
        Result<
            Result<
                insight_platform_sandbox_executor::SandboxExecutorCycleReport,
                insight_platform_sandbox_executor::SandboxExecutorDriverError,
            >,
            tokio::task::JoinError,
        >,
    ),
    ManagedDriver(
        Result<
            Result<
                insight_platform_sandbox_executor::SandboxExecutorCycleReport,
                insight_platform_sandbox_executor::SandboxExecutorDriverError,
            >,
            tokio::task::JoinError,
        >,
    ),
    ManagedRecovery(
        Result<
            Result<(), insight_platform_sandbox_executor::SandboxExecutorDriverError>,
            tokio::task::JoinError,
        >,
    ),
    Control(
        Result<Result<(), insight_platform_sandbox::SandboxControlError>, tokio::task::JoinError>,
    ),
}

async fn await_driver_shutdown(
    task: &mut tokio::task::JoinHandle<
        Result<
            insight_platform_sandbox_executor::SandboxExecutorCycleReport,
            insight_platform_sandbox_executor::SandboxExecutorDriverError,
        >,
    >,
    grace: Duration,
) -> Result<(), ProcessError> {
    tokio::time::timeout(grace, task)
        .await
        .map_err(|_| ProcessError::DrainRequiresProcessTermination)
        .and_then(map_driver_join)
}

async fn await_control_shutdown(
    task: &mut tokio::task::JoinHandle<Result<(), insight_platform_sandbox::SandboxControlError>>,
    grace: Duration,
) -> Result<(), ProcessError> {
    tokio::time::timeout(grace, task)
        .await
        .map_err(|_| ProcessError::DrainRequiresProcessTermination)
        .and_then(map_control_join)
}

async fn await_recovery_shutdown(
    task: &mut tokio::task::JoinHandle<
        Result<(), insight_platform_sandbox_executor::SandboxExecutorDriverError>,
    >,
    grace: Duration,
) -> Result<(), ProcessError> {
    tokio::time::timeout(grace, task)
        .await
        .map_err(|_| ProcessError::DrainRequiresProcessTermination)
        .and_then(map_recovery_join)
}

async fn connect_authority(config: &ExecutorProcessConfig) -> Result<Channel, ProcessError> {
    let ca = read_bounded(
        &required_absolute_path(GRPC_CA_PATH_ENV)?,
        MAX_TLS_FILE_BYTES,
    )?;
    let cert = read_bounded(
        &required_absolute_path(GRPC_CERT_PATH_ENV)?,
        MAX_TLS_FILE_BYTES,
    )?;
    let key = read_bounded(
        &required_absolute_path(GRPC_KEY_PATH_ENV)?,
        MAX_TLS_FILE_BYTES,
    )?;
    let tls = ClientTlsConfig::new()
        .domain_name(config.authority_tls_server_name.clone())
        .ca_certificate(Certificate::from_pem(ca))
        .identity(Identity::from_pem(cert, key));
    Endpoint::from_shared(config.authority_endpoint.clone())
        .map_err(|_| ProcessError::InvalidConfiguration)?
        .connect_timeout(Duration::from_millis(config.connect_timeout_milliseconds))
        .timeout(Duration::from_millis(config.request_timeout_milliseconds))
        .tls_config(tls)
        .map_err(|_| ProcessError::InvalidConfiguration)?
        .connect()
        .await
        .map_err(|_| ProcessError::AuthorityUnavailable)
}

async fn connect_process_registration_attestor(
    config: &ExecutorProcessConfig,
) -> Result<Channel, ProcessError> {
    let ca = read_bounded(
        &required_absolute_path(ATTESTOR_CA_PATH_ENV)?,
        MAX_TLS_FILE_BYTES,
    )?;
    let cert = read_bounded(
        &required_absolute_path(GRPC_CERT_PATH_ENV)?,
        MAX_TLS_FILE_BYTES,
    )?;
    let key = read_bounded(
        &required_absolute_path(GRPC_KEY_PATH_ENV)?,
        MAX_TLS_FILE_BYTES,
    )?;
    let tls = ClientTlsConfig::new()
        .domain_name(config.process_registration_attestor_tls_server_name.clone())
        .ca_certificate(Certificate::from_pem(ca))
        .identity(Identity::from_pem(cert, key));
    let endpoint = Endpoint::from_shared(format!(
        "https://{}",
        config.process_registration_attestor_tls_server_name
    ))
    .map_err(|_| ProcessError::InvalidConfiguration)?
    .connect_timeout(Duration::from_millis(config.connect_timeout_milliseconds))
    .timeout(Duration::from_millis(config.request_timeout_milliseconds))
    .tls_config(tls)
    .map_err(|_| ProcessError::InvalidConfiguration)?;
    let socket_path = PathBuf::from(&config.process_registration_attestor_socket_path);
    endpoint
        .connect_with_connector(tower::service_fn(move |_| {
            let socket_path = socket_path.clone();
            async move {
                tokio::net::UnixStream::connect(socket_path)
                    .await
                    .map(TokioIo::new)
            }
        }))
        .await
        .map_err(|_| ProcessError::AttestorUnavailable)
}

async fn connect_isolation_provider(
    config: &ExecutorProcessConfig,
) -> Result<Channel, ProcessError> {
    let ExecutorBackendProcessConfig::MicroVm {
        provider_socket_path,
        provider_tls_server_name,
        ..
    } = &config.backend
    else {
        return Err(ProcessError::InvalidConfiguration);
    };
    let ca = read_bounded(
        &required_absolute_path(PROVIDER_CA_PATH_ENV)?,
        MAX_TLS_FILE_BYTES,
    )?;
    let cert = read_bounded(
        &required_absolute_path(GRPC_CERT_PATH_ENV)?,
        MAX_TLS_FILE_BYTES,
    )?;
    let key = read_bounded(
        &required_absolute_path(GRPC_KEY_PATH_ENV)?,
        MAX_TLS_FILE_BYTES,
    )?;
    let tls = ClientTlsConfig::new()
        .domain_name(provider_tls_server_name.clone())
        .ca_certificate(Certificate::from_pem(ca))
        .identity(Identity::from_pem(cert, key));
    let endpoint = Endpoint::from_shared(format!("https://{provider_tls_server_name}"))
        .map_err(|_| ProcessError::InvalidConfiguration)?
        .connect_timeout(Duration::from_millis(config.connect_timeout_milliseconds))
        .timeout(Duration::from_millis(config.request_timeout_milliseconds))
        .tls_config(tls)
        .map_err(|_| ProcessError::InvalidConfiguration)?;
    let socket_path = PathBuf::from(provider_socket_path);
    endpoint
        .connect_with_connector(tower::service_fn(move |_| {
            let socket_path = socket_path.clone();
            async move {
                tokio::net::UnixStream::connect(socket_path)
                    .await
                    .map(TokioIo::new)
            }
        }))
        .await
        .map_err(|_| ProcessError::ProviderUnavailable)
}

async fn connect_nats(config: &ExecutorProcessConfig) -> Result<async_nats::Client, ProcessError> {
    async_nats::ConnectOptions::new()
        .require_tls(true)
        .add_root_certificates(required_absolute_path(NATS_CA_PATH_ENV)?)
        .add_client_certificate(
            required_absolute_path(NATS_CERT_PATH_ENV)?,
            required_absolute_path(NATS_KEY_PATH_ENV)?,
        )
        .connect(&config.nats_endpoint)
        .await
        .map_err(|_| ProcessError::NatsUnavailable)
}

fn required(name: &'static str) -> Result<String, ProcessError> {
    std::env::var(name)
        .ok()
        .filter(|value| !value.is_empty())
        .ok_or(ProcessError::MissingConfiguration(name))
}

fn closed_service_endpoint(value: &str, required_scheme: &str) -> bool {
    if value.is_empty() || value.len() > 2_048 {
        return false;
    }
    let Ok(endpoint) = Url::parse(value) else {
        return false;
    };
    endpoint.scheme() == required_scheme
        && endpoint.host_str().is_some()
        && endpoint.username().is_empty()
        && endpoint.password().is_none()
        && endpoint.query().is_none()
        && endpoint.fragment().is_none()
        && matches!(endpoint.path(), "" | "/")
}

fn stable_dns_name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 253
        && value.split('.').all(|label| {
            !label.is_empty()
                && label.len() <= 63
                && label
                    .as_bytes()
                    .first()
                    .is_some_and(u8::is_ascii_alphanumeric)
                && label
                    .as_bytes()
                    .last()
                    .is_some_and(u8::is_ascii_alphanumeric)
                && label
                    .bytes()
                    .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        })
}

fn closed_unix_socket_path(value: &str) -> bool {
    let path = std::path::Path::new(value);
    path.is_absolute()
        && value.len() <= 100
        && !value.as_bytes().contains(&0)
        && path
            .components()
            .all(|component| !matches!(component, std::path::Component::ParentDir))
}

fn required_absolute_path(name: &'static str) -> Result<PathBuf, ProcessError> {
    let path = PathBuf::from(required(name)?);
    if !path.is_absolute() {
        return Err(ProcessError::InvalidConfiguration);
    }
    Ok(path)
}

fn read_bounded(path: &PathBuf, maximum: usize) -> Result<Vec<u8>, ProcessError> {
    let metadata = std::fs::metadata(path).map_err(|_| ProcessError::ConfigurationUnreadable)?;
    if !metadata.is_file()
        || metadata.len() == 0
        || !usize::try_from(metadata.len()).is_ok_and(|length| length <= maximum)
    {
        return Err(ProcessError::ConfigurationUnreadable);
    }
    let value = std::fs::read(path).map_err(|_| ProcessError::ConfigurationUnreadable)?;
    if value.is_empty() || value.len() > maximum {
        return Err(ProcessError::ConfigurationUnreadable);
    }
    Ok(value)
}

fn map_driver_join(
    result: Result<
        Result<
            insight_platform_sandbox_executor::SandboxExecutorCycleReport,
            insight_platform_sandbox_executor::SandboxExecutorDriverError,
        >,
        tokio::task::JoinError,
    >,
) -> Result<(), ProcessError> {
    match result {
        Ok(Ok(_)) => Ok(()),
        Ok(Err(_)) => Err(ProcessError::ExecutorFailed),
        Err(_) => Err(ProcessError::ExecutorPanicked),
    }
}

fn map_control_join(
    result: Result<
        Result<(), insight_platform_sandbox::SandboxControlError>,
        tokio::task::JoinError,
    >,
) -> Result<(), ProcessError> {
    match result {
        Ok(Ok(())) => Ok(()),
        Ok(Err(_)) => Err(ProcessError::NatsUnavailable),
        Err(_) => Err(ProcessError::ExecutorPanicked),
    }
}

fn map_recovery_join(
    result: Result<
        Result<(), insight_platform_sandbox_executor::SandboxExecutorDriverError>,
        tokio::task::JoinError,
    >,
) -> Result<(), ProcessError> {
    match result {
        Ok(Ok(())) => Ok(()),
        Ok(Err(_)) => Err(ProcessError::ExecutorFailed),
        Err(_) => Err(ProcessError::ExecutorPanicked),
    }
}

#[derive(Debug)]
enum ProcessError {
    MissingConfiguration(&'static str),
    InvalidConfiguration,
    ConfigurationUnreadable,
    AuthorityUnavailable,
    AttestorUnavailable,
    ProviderUnavailable,
    NatsUnavailable,
    SignalUnavailable,
    ExecutorFailed,
    ExecutorPanicked,
    ExecutorExitedUnexpectedly,
    ControlExitedUnexpectedly,
    DrainRequiresProcessTermination,
}

impl fmt::Display for ProcessError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingConfiguration(name) => {
                write!(formatter, "required setting {name} is absent")
            }
            Self::InvalidConfiguration => formatter.write_str("configuration is invalid"),
            Self::ConfigurationUnreadable => {
                formatter.write_str("configuration material is unreadable")
            }
            Self::AuthorityUnavailable => formatter.write_str("Sandbox authority is unavailable"),
            Self::AttestorUnavailable => {
                formatter.write_str("Sandbox process-registration attestor is unavailable")
            }
            Self::ProviderUnavailable => {
                formatter.write_str("Sandbox isolation provider is unavailable")
            }
            Self::NatsUnavailable => {
                formatter.write_str("Sandbox control transport is unavailable")
            }
            Self::SignalUnavailable => {
                formatter.write_str("process signal handling is unavailable")
            }
            Self::ExecutorFailed => formatter.write_str("Sandbox Executor driver failed"),
            Self::ExecutorPanicked => {
                formatter.write_str("Sandbox Executor task did not complete safely")
            }
            Self::ExecutorExitedUnexpectedly => {
                formatter.write_str("Sandbox Executor driver exited before shutdown")
            }
            Self::ControlExitedUnexpectedly => {
                formatter.write_str("Sandbox control listener exited before shutdown")
            }
            Self::DrainRequiresProcessTermination => {
                formatter.write_str("Sandbox drain grace expired; process termination is required")
            }
        }
    }
}

impl Error for ProcessError {}

#[cfg(test)]
mod tests {
    use super::*;
    use insight_platform_contracts::{WORKER_MANIFEST_VERSION, WORKER_PROTOCOL_VERSION};

    fn digest(character: char) -> Sha256Digest {
        format!("sha256:{}", character.to_string().repeat(64))
            .parse()
            .unwrap()
    }

    fn config() -> ExecutorProcessConfig {
        ExecutorProcessConfig {
            schema_version: 1,
            worker_manifest: WorkerManifest {
                manifest_version: WORKER_MANIFEST_VERSION,
                worker_role: "sandbox-executor.wasi".to_owned(),
                work_class: WorkClass::Sandbox,
                adapter_runtime_digest: digest('a'),
                protocol_version: WORKER_PROTOCOL_VERSION,
                max_concurrency: 8,
                critical_control_reserved_slots: 1,
            },
            backend: ExecutorBackendProcessConfig::Wasi {
                runtime_version: WASI_ABI_V1_RUNTIME_VERSION.to_owned(),
            },
            backend_contract_digest: digest('b'),
            authority_endpoint: "https://sandbox-controller.platform.svc:7443".to_owned(),
            authority_tls_server_name: "sandbox-controller.platform.svc".to_owned(),
            process_registration_attestor_socket_path:
                "/run/insight-sandbox-attestor/registration.sock".to_owned(),
            process_registration_attestor_tls_server_name: "sandbox-attestor.platform.svc"
                .to_owned(),
            process_registration_attestor_identity_digest: digest('c'),
            nats_endpoint: "tls://nats.platform.svc:4222".to_owned(),
            receipt_ttl_seconds: 600,
            claim_scan_milliseconds: 500,
            claim_failure_backoff_milliseconds: 100,
            drain_grace_milliseconds: 5_000,
            control_request_timeout_milliseconds: 1_000,
            connect_timeout_milliseconds: 3_000,
            request_timeout_milliseconds: 10_000,
        }
    }

    #[test]
    fn process_config_accepts_only_exact_role_runtime_and_closed_endpoints() {
        config().validate().unwrap();

        for endpoint in [
            "http://sandbox-controller.platform.svc:7443",
            "https://user:secret@sandbox-controller.platform.svc:7443",
            "https://sandbox-controller.platform.svc:7443/path",
            "https://sandbox-controller.platform.svc:7443?token=secret",
            "https://sandbox-controller.platform.svc:7443#fragment",
        ] {
            let mut invalid = config();
            invalid.authority_endpoint = endpoint.to_owned();
            assert!(invalid.validate().is_err(), "accepted {endpoint}");
        }

        let mut invalid = config();
        invalid.nats_endpoint = "tls://user:secret@nats.platform.svc:4222".to_owned();
        assert!(invalid.validate().is_err());

        let mut invalid = config();
        invalid.backend = ExecutorBackendProcessConfig::Wasi {
            runtime_version: "wasmtime-latest".to_owned(),
        };
        assert!(invalid.validate().is_err());

        let mut micro_vm = config();
        micro_vm.worker_manifest.worker_role = "sandbox-executor.microvm".to_owned();
        micro_vm.backend = ExecutorBackendProcessConfig::MicroVm {
            provider_socket_path: "/run/insight-sandbox-provider/provider.sock".to_owned(),
            provider_tls_server_name: "sandbox-provider.platform.svc".to_owned(),
            managed_recovery: ManagedMcpRecoveryProcessConfig {
                shard_index: 0,
                shard_count: 1,
                scan_milliseconds: 1_000,
                scan_jitter_milliseconds: 250,
                failure_backoff_milliseconds: 250,
            },
        };
        micro_vm.validate().unwrap();

        let mut invalid_recovery = micro_vm.clone();
        let ExecutorBackendProcessConfig::MicroVm {
            managed_recovery, ..
        } = &mut invalid_recovery.backend
        else {
            unreachable!()
        };
        managed_recovery.shard_index = managed_recovery.shard_count;
        assert!(invalid_recovery.validate().is_err());

        let mut invalid_recovery = micro_vm.clone();
        let ExecutorBackendProcessConfig::MicroVm {
            managed_recovery, ..
        } = &mut invalid_recovery.backend
        else {
            unreachable!()
        };
        managed_recovery.scan_jitter_milliseconds = managed_recovery.scan_milliseconds;
        assert!(invalid_recovery.validate().is_err());

        micro_vm.worker_manifest.worker_role = "sandbox-executor.wasi".to_owned();
        assert!(micro_vm.validate().is_err());
    }

    #[test]
    fn tls_server_name_is_a_canonical_lowercase_dns_name() {
        for name in [
            "",
            ".sandbox-controller",
            "sandbox-controller.",
            "-sandbox-controller.platform.svc",
            "sandbox-controller-.platform.svc",
            "Sandbox-Controller.platform.svc",
        ] {
            let mut invalid = config();
            invalid.authority_tls_server_name = name.to_owned();
            assert!(invalid.validate().is_err(), "accepted {name}");
        }
    }
}
