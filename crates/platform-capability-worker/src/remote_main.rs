//! Deployable HTTP/gRPC Capability Worker for the clean-cut Platform v1 candidate.
//!
//! The process owns bounded local permits and protocol codecs. PostgreSQL remains the durable
//! Invocation/Job authority and all network traffic crosses the independently deployed mTLS Egress
//! Broker. MCP uses the same durable worker owner but crosses an independent mTLS MCP Host RPC
//! boundary; the worker never owns Streamable HTTP, OAuth, session, Task, or subscription wire
//! semantics.

mod dependency_observer;

use dependency_observer::install_capability_dependency_metrics;

use insight_platform_capability_adapters::{
    CapabilityAdapterWorker, CapabilityDispatcher, GrpcCapabilityAdapter, GrpcNetworkTransport,
    HttpCapabilityAdapter, HttpNetworkTransport, InstalledGrpcCodecDescriptor,
    InstalledGrpcCodecRegistry, InstalledHttpCodecDescriptor, InstalledHttpCodecRegistry,
    InstalledMcpToolCodecDescriptor, InstalledMcpToolCodecRegistry, InstalledNativeRegistry,
    McpCapabilityAdapter,
};
use insight_platform_capability_worker::{
    builtin_json_codec_module_digest, builtin_json_grpc_error_mapping_digest,
    builtin_json_grpc_protobuf_contract_digest, builtin_json_grpc_request_mapping_digest,
    builtin_json_grpc_response_mapping_digest, builtin_json_http_error_mapping_digest,
    builtin_json_http_protocol_contract_digest, builtin_json_http_request_mapping_digest,
    builtin_json_http_response_mapping_digest, builtin_json_mcp_output_mapping_digest,
    install_builtin_grpc_json_codecs, install_builtin_http_json_codecs,
    install_builtin_mcp_json_codecs, CapabilityWorkerDriver, CapabilityWorkerDriverConfig,
    CapabilityWorkerDriverTiming, RegisteredCapabilityJobExecutor,
    UuidCapabilityWorkerIdentityFactory, BUILTIN_JSON_CODEC_ID, BUILTIN_JSON_CODEC_VERSION,
    CAPABILITY_REMOTE_WORKER_ROLE,
};
use insight_platform_contracts::{
    canonical_digest, checked_in_hard_limit_profile, parse_strict_json, JsonLimits,
    McpTransportKind, ResourceId, ResourceKind, Sha256Digest, WorkClass, WorkerManifest,
    WORKER_PROTOCOL_VERSION,
};
use insight_platform_egress_rpc::{EgressBrokerGrpcClient, EgressInternalRpcLimits};
use insight_platform_mcp_host::{McpExecutionContractResolver, McpHostClient};
use insight_platform_mcp_rpc::{McpHostGrpcClient, McpHostInternalRpcLimits};
use insight_platform_observability::{
    process_observability_router, run_worker_permit_sampler, update_worker_permits,
    ProcessHttpMetrics, WorkerPermitMetrics, PROCESS_OBSERVABILITY_OPERATIONS,
};
use insight_platform_postgres::{
    dependency_health::run_postgres_health_sampler, repository::PgRepository, verify_schema,
};
use insight_platform_worker::LocalWorkerPools;
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgPoolOptions;
use std::{
    collections::BTreeSet, error::Error, fmt, io::Read as _, path::PathBuf, sync::Arc,
    time::Duration,
};
use tokio_util::sync::CancellationToken;
use tonic::transport::{Certificate, ClientTlsConfig, Endpoint, Identity};
use url::Url;
use uuid::Uuid;

const CONFIG_PATH_ENV: &str = "PLATFORM_CAPABILITY_REMOTE_WORKER_CONFIG";
const CONFIG_DIGEST_ENV: &str = "PLATFORM_CAPABILITY_REMOTE_WORKER_CONFIG_DIGEST";
const DATABASE_URL_ENV: &str = "PLATFORM_CAPABILITY_REMOTE_WORKER_DATABASE_URL";
const EGRESS_CA_PATH_ENV: &str = "PLATFORM_CAPABILITY_REMOTE_WORKER_EGRESS_CA_PATH";
const EGRESS_CERT_PATH_ENV: &str = "PLATFORM_CAPABILITY_REMOTE_WORKER_EGRESS_CERT_PATH";
const EGRESS_KEY_PATH_ENV: &str = "PLATFORM_CAPABILITY_REMOTE_WORKER_EGRESS_KEY_PATH";
const MCP_HOST_CA_PATH_ENV: &str = "PLATFORM_CAPABILITY_REMOTE_WORKER_MCP_HOST_CA_PATH";
const MCP_HOST_CERT_PATH_ENV: &str = "PLATFORM_CAPABILITY_REMOTE_WORKER_MCP_HOST_CERT_PATH";
const MCP_HOST_KEY_PATH_ENV: &str = "PLATFORM_CAPABILITY_REMOTE_WORKER_MCP_HOST_KEY_PATH";
const MAX_CONFIG_BYTES: usize = 1_048_576;
const MAX_TLS_FILE_BYTES: usize = 1_048_576;
const MAX_INSTALLED_CODECS: usize = 64;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProcessConfig {
    schema_version: u32,
    observability_listen_address: String,
    worker_manifest: WorkerManifest,
    installed_http_codecs: Vec<InstalledHttpCodecConfig>,
    installed_grpc_codecs: Vec<InstalledGrpcCodecConfig>,
    installed_mcp_codecs: Vec<InstalledMcpCodecConfig>,
    database: DatabaseConfig,
    egress: EgressConfig,
    mcp_host: McpHostConfig,
    timing: TimingConfig,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct InstalledHttpCodecConfig {
    codec_id: String,
    codec_version: String,
    module_digest: Sha256Digest,
    worker_protocol_version: u32,
    descriptor_digest: Sha256Digest,
    protocol_contract_digest: Sha256Digest,
    request_mapping_digest: Sha256Digest,
    response_mapping_digest: Sha256Digest,
    error_mapping_digest: Sha256Digest,
}

impl From<&InstalledHttpCodecConfig> for InstalledHttpCodecDescriptor {
    fn from(value: &InstalledHttpCodecConfig) -> Self {
        Self {
            codec_id: value.codec_id.clone(),
            codec_version: value.codec_version.clone(),
            module_digest: value.module_digest.clone(),
            worker_protocol_version: value.worker_protocol_version,
            descriptor_digest: value.descriptor_digest.clone(),
            protocol_contract_digest: value.protocol_contract_digest.clone(),
            request_mapping_digest: value.request_mapping_digest.clone(),
            response_mapping_digest: value.response_mapping_digest.clone(),
            error_mapping_digest: value.error_mapping_digest.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct InstalledGrpcCodecConfig {
    codec_id: String,
    codec_version: String,
    module_digest: Sha256Digest,
    worker_protocol_version: u32,
    descriptor_digest: Sha256Digest,
    protobuf_contract_digest: Sha256Digest,
    service_name: String,
    method_name: String,
    request_mapping_digest: Sha256Digest,
    response_mapping_digest: Sha256Digest,
    error_mapping_digest: Sha256Digest,
}

impl From<&InstalledGrpcCodecConfig> for InstalledGrpcCodecDescriptor {
    fn from(value: &InstalledGrpcCodecConfig) -> Self {
        Self {
            codec_id: value.codec_id.clone(),
            codec_version: value.codec_version.clone(),
            module_digest: value.module_digest.clone(),
            worker_protocol_version: value.worker_protocol_version,
            descriptor_digest: value.descriptor_digest.clone(),
            protobuf_contract_digest: value.protobuf_contract_digest.clone(),
            service_name: value.service_name.clone(),
            method_name: value.method_name.clone(),
            request_mapping_digest: value.request_mapping_digest.clone(),
            response_mapping_digest: value.response_mapping_digest.clone(),
            error_mapping_digest: value.error_mapping_digest.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct InstalledMcpCodecConfig {
    codec_id: String,
    codec_version: String,
    module_digest: Sha256Digest,
    worker_protocol_version: u32,
    descriptor_digest: Sha256Digest,
    remote_tool_name: String,
    remote_input_schema_digest: Sha256Digest,
    output_mapping_digest: Sha256Digest,
    protocol_profile_id: ResourceId,
    protocol_profile_digest: Sha256Digest,
    discovery_semantic_evidence_digest: Sha256Digest,
}

impl From<&InstalledMcpCodecConfig> for InstalledMcpToolCodecDescriptor {
    fn from(value: &InstalledMcpCodecConfig) -> Self {
        Self {
            codec_id: value.codec_id.clone(),
            codec_version: value.codec_version.clone(),
            module_digest: value.module_digest.clone(),
            worker_protocol_version: value.worker_protocol_version,
            descriptor_digest: value.descriptor_digest.clone(),
            remote_tool_name: value.remote_tool_name.clone(),
            remote_input_schema_digest: value.remote_input_schema_digest.clone(),
            output_mapping_digest: value.output_mapping_digest.clone(),
            protocol_profile_id: value.protocol_profile_id.clone(),
            protocol_profile_digest: value.protocol_profile_digest.clone(),
            discovery_semantic_evidence_digest: value.discovery_semantic_evidence_digest.clone(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct DatabaseConfig {
    business_max_connections: u32,
    critical_control_max_connections: u32,
    process_connection_budget: u32,
    acquire_timeout_milliseconds: u64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct EgressConfig {
    endpoint: String,
    tls_server_name: String,
    connect_timeout_milliseconds: u64,
    request_timeout_milliseconds: u64,
    maximum_rpc_metadata_bytes: usize,
    maximum_rpc_payload_bytes: usize,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct McpHostConfig {
    endpoint: String,
    tls_server_name: String,
    connect_timeout_milliseconds: u64,
    request_timeout_milliseconds: u64,
    maximum_rpc_message_bytes: usize,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct TimingConfig {
    initial_scan_delay_milliseconds: u64,
    receipt_ttl_milliseconds: u64,
    safety_scan_milliseconds: u64,
    claim_failure_backoff_milliseconds: u64,
    drain_grace_milliseconds: u64,
}

#[derive(Serialize)]
#[serde(deny_unknown_fields)]
struct InstalledCodecClosure<'a> {
    schema_version: u32,
    http: &'a [InstalledHttpCodecConfig],
    grpc: &'a [InstalledGrpcCodecConfig],
    mcp: &'a [InstalledMcpCodecConfig],
}

impl ProcessConfig {
    fn load() -> Result<Self, ProcessError> {
        let bytes = read_bounded_file(&required_absolute_path(CONFIG_PATH_ENV)?, MAX_CONFIG_BYTES)?;
        let value = parse_strict_json(
            &bytes,
            JsonLimits {
                max_bytes: MAX_CONFIG_BYTES,
                max_depth: 16,
                max_properties_per_object: 64,
                max_items_per_array: MAX_INSTALLED_CODECS,
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
        Ok(config)
    }

    fn validate(&self) -> Result<(), ProcessError> {
        let observability: std::net::SocketAddr = self
            .observability_listen_address
            .parse()
            .map_err(|_| ProcessError::InvalidConfiguration)?;
        self.worker_manifest
            .validate()
            .map_err(|_| ProcessError::InvalidConfiguration)?;
        let codec_count = self
            .installed_http_codecs
            .len()
            .checked_add(self.installed_grpc_codecs.len())
            .and_then(|count| count.checked_add(self.installed_mcp_codecs.len()))
            .ok_or(ProcessError::InvalidConfiguration)?;
        let installed_value = serde_json::to_value(InstalledCodecClosure {
            schema_version: 1,
            http: &self.installed_http_codecs,
            grpc: &self.installed_grpc_codecs,
            mcp: &self.installed_mcp_codecs,
        })
        .map_err(|_| ProcessError::InvalidConfiguration)?;
        let installed_digest: Sha256Digest = canonical_digest(&installed_value)
            .map_err(|_| ProcessError::InvalidConfiguration)?
            .parse()
            .map_err(|_| ProcessError::InvalidConfiguration)?;
        if self.schema_version != 1
            || observability.port() == 0
            || self.worker_manifest.worker_role != CAPABILITY_REMOTE_WORKER_ROLE
            || self.worker_manifest.work_class != WorkClass::CapabilityRemote
            || self.worker_manifest.protocol_version != WORKER_PROTOCOL_VERSION
            || self.worker_manifest.adapter_runtime_digest != installed_digest
            || self.installed_http_codecs.is_empty()
            || self.installed_grpc_codecs.is_empty()
            || self.installed_mcp_codecs.is_empty()
            || codec_count > MAX_INSTALLED_CODECS
            || self.database.business_max_connections == 0
            || self.database.critical_control_max_connections == 0
            || self.database.process_connection_budget == 0
            || self
                .database
                .business_max_connections
                .checked_add(self.database.critical_control_max_connections)
                != Some(self.database.process_connection_budget)
            || self.database.acquire_timeout_milliseconds == 0
            || self.database.acquire_timeout_milliseconds > 30_000
        {
            return Err(ProcessError::InvalidConfiguration);
        }
        self.validate_codecs()?;
        self.validate_egress()?;
        self.validate_mcp_host()?;
        self.driver_config()?;
        Ok(())
    }

    fn validate_codecs(&self) -> Result<(), ProcessError> {
        let mut http = BTreeSet::new();
        for configured in &self.installed_http_codecs {
            let descriptor = InstalledHttpCodecDescriptor::from(configured);
            if configured.codec_id != BUILTIN_JSON_CODEC_ID
                || configured.codec_version != BUILTIN_JSON_CODEC_VERSION
                || configured.module_digest != builtin_json_codec_module_digest()
                || configured.worker_protocol_version != WORKER_PROTOCOL_VERSION
                || configured.protocol_contract_digest
                    != builtin_json_http_protocol_contract_digest()
                || configured.request_mapping_digest != builtin_json_http_request_mapping_digest()
                || configured.response_mapping_digest != builtin_json_http_response_mapping_digest()
                || configured.error_mapping_digest != builtin_json_http_error_mapping_digest()
                || !http.insert(descriptor)
            {
                return Err(ProcessError::InvalidConfiguration);
            }
        }
        let mut grpc = BTreeSet::new();
        for configured in &self.installed_grpc_codecs {
            let descriptor = InstalledGrpcCodecDescriptor::from(configured);
            if configured.codec_id != BUILTIN_JSON_CODEC_ID
                || configured.codec_version != BUILTIN_JSON_CODEC_VERSION
                || configured.module_digest != builtin_json_codec_module_digest()
                || configured.worker_protocol_version != WORKER_PROTOCOL_VERSION
                || configured.protobuf_contract_digest
                    != builtin_json_grpc_protobuf_contract_digest()
                || configured.request_mapping_digest != builtin_json_grpc_request_mapping_digest()
                || configured.response_mapping_digest != builtin_json_grpc_response_mapping_digest()
                || configured.error_mapping_digest != builtin_json_grpc_error_mapping_digest()
                || !valid_grpc_name(&configured.service_name)
                || !valid_grpc_name(&configured.method_name)
                || !grpc.insert(descriptor)
            {
                return Err(ProcessError::InvalidConfiguration);
            }
        }
        let mut mcp = BTreeSet::new();
        for configured in &self.installed_mcp_codecs {
            let descriptor = InstalledMcpToolCodecDescriptor::from(configured);
            if configured.codec_id != BUILTIN_JSON_CODEC_ID
                || configured.codec_version != BUILTIN_JSON_CODEC_VERSION
                || configured.module_digest != builtin_json_codec_module_digest()
                || configured.worker_protocol_version != WORKER_PROTOCOL_VERSION
                || configured.output_mapping_digest != builtin_json_mcp_output_mapping_digest()
                || configured.protocol_profile_id.kind() != ResourceKind::PolicyRevision
                || !mcp.insert(descriptor)
            {
                return Err(ProcessError::InvalidConfiguration);
            }
        }
        Ok(())
    }

    fn validate_egress(&self) -> Result<(), ProcessError> {
        let endpoint =
            Url::parse(&self.egress.endpoint).map_err(|_| ProcessError::InvalidConfiguration)?;
        if endpoint.scheme() != "https"
            || endpoint.host_str().is_none()
            || endpoint.port().is_none()
            || !endpoint.username().is_empty()
            || endpoint.password().is_some()
            || endpoint.path() != "/"
            || endpoint.query().is_some()
            || endpoint.fragment().is_some()
            || !valid_tls_server_name(&self.egress.tls_server_name)
            || self.egress.connect_timeout_milliseconds == 0
            || self.egress.connect_timeout_milliseconds > 30_000
            || self.egress.request_timeout_milliseconds == 0
            || self.egress.request_timeout_milliseconds > 120_000
        {
            return Err(ProcessError::InvalidConfiguration);
        }
        self.rpc_limits().map(|_| ())
    }

    fn rpc_limits(&self) -> Result<EgressInternalRpcLimits, ProcessError> {
        EgressInternalRpcLimits::new(
            self.egress.maximum_rpc_metadata_bytes,
            self.egress.maximum_rpc_payload_bytes,
        )
        .map_err(|_| ProcessError::InvalidConfiguration)
    }

    fn validate_mcp_host(&self) -> Result<(), ProcessError> {
        let endpoint =
            Url::parse(&self.mcp_host.endpoint).map_err(|_| ProcessError::InvalidConfiguration)?;
        if endpoint.scheme() != "https"
            || endpoint.host_str().is_none()
            || endpoint.port().is_none()
            || !endpoint.username().is_empty()
            || endpoint.password().is_some()
            || endpoint.path() != "/"
            || endpoint.query().is_some()
            || endpoint.fragment().is_some()
            || !valid_tls_server_name(&self.mcp_host.tls_server_name)
            || self.mcp_host.connect_timeout_milliseconds == 0
            || self.mcp_host.connect_timeout_milliseconds > 30_000
            || self.mcp_host.request_timeout_milliseconds == 0
            || self.mcp_host.request_timeout_milliseconds > 120_000
        {
            return Err(ProcessError::InvalidConfiguration);
        }
        self.mcp_host_rpc_limits().map(|_| ())
    }

    fn mcp_host_rpc_limits(&self) -> Result<McpHostInternalRpcLimits, ProcessError> {
        McpHostInternalRpcLimits::new(self.mcp_host.maximum_rpc_message_bytes)
            .map_err(|_| ProcessError::InvalidConfiguration)
    }

    fn driver_config(&self) -> Result<CapabilityWorkerDriverConfig, ProcessError> {
        CapabilityWorkerDriverConfig::from_profile(
            &checked_in_hard_limit_profile(),
            Duration::from_millis(self.timing.receipt_ttl_milliseconds),
            CapabilityWorkerDriverTiming {
                initial_scan_delay: Duration::from_millis(
                    self.timing.initial_scan_delay_milliseconds,
                ),
                safety_scan_interval: Duration::from_millis(self.timing.safety_scan_milliseconds),
                claim_failure_backoff: Duration::from_millis(
                    self.timing.claim_failure_backoff_milliseconds,
                ),
                drain_grace: Duration::from_millis(self.timing.drain_grace_milliseconds),
            },
        )
        .map_err(|_| ProcessError::InvalidConfiguration)
    }
}

#[tokio::main]
async fn main() {
    if let Err(failure) = run().await {
        eprintln!("platform-capability-remote-worker failed: {failure}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), ProcessError> {
    let config = ProcessConfig::load()?;
    let database_url = required(DATABASE_URL_ENV)?;
    let acquire_timeout = Duration::from_millis(config.database.acquire_timeout_milliseconds);
    let business_pool = PgPoolOptions::new()
        .max_connections(config.database.business_max_connections)
        .acquire_timeout(acquire_timeout)
        .connect(&database_url)
        .await
        .map_err(|_| ProcessError::DatabaseUnavailable)?;
    let critical_control_pool = PgPoolOptions::new()
        .max_connections(config.database.critical_control_max_connections)
        .acquire_timeout(acquire_timeout)
        .connect(&database_url)
        .await
        .map_err(|_| ProcessError::DatabaseUnavailable)?;
    verify_schema(&critical_control_pool)
        .await
        .map_err(|_| ProcessError::SchemaMismatch)?;

    let process_generation_id =
        ResourceId::from_uuid_v7(ResourceKind::WorkerProcessGeneration, Uuid::now_v7())
            .map_err(|_| ProcessError::InvalidConfiguration)?;
    let pools = LocalWorkerPools::new(
        config.worker_manifest.clone(),
        process_generation_id.clone(),
    )
    .map_err(|_| ProcessError::InvalidConfiguration)?;
    let observability_pools = pools.clone();
    let business_health_pool = business_pool.clone();
    let critical_control_health_pool = critical_control_pool.clone();
    let dependency_metrics = install_capability_dependency_metrics(true)
        .map_err(|_| ProcessError::InvalidConfiguration)?;
    let egress = Arc::new(EgressBrokerGrpcClient::new_with_observer(
        connect_egress(&config).await?,
        config.rpc_limits()?,
        dependency_metrics
            .egress
            .ok_or(ProcessError::InvalidConfiguration)?,
    ));
    let http_transport: Arc<dyn HttpNetworkTransport> = egress.clone();
    let grpc_transport: Arc<dyn GrpcNetworkTransport> = egress;
    let mut http_codecs = InstalledHttpCodecRegistry::default();
    install_builtin_http_json_codecs(
        &mut http_codecs,
        config
            .installed_http_codecs
            .iter()
            .map(InstalledHttpCodecDescriptor::from),
    )
    .map_err(|_| ProcessError::InvalidConfiguration)?;
    let mut grpc_codecs = InstalledGrpcCodecRegistry::default();
    install_builtin_grpc_json_codecs(
        &mut grpc_codecs,
        config
            .installed_grpc_codecs
            .iter()
            .map(InstalledGrpcCodecDescriptor::from),
    )
    .map_err(|_| ProcessError::InvalidConfiguration)?;
    let mut mcp_codecs = InstalledMcpToolCodecRegistry::default();
    install_builtin_mcp_json_codecs(
        &mut mcp_codecs,
        config
            .installed_mcp_codecs
            .iter()
            .map(InstalledMcpToolCodecDescriptor::from),
    )
    .map_err(|_| ProcessError::InvalidConfiguration)?;
    let mcp_host: Arc<dyn McpHostClient> = Arc::new(McpHostGrpcClient::new(
        connect_mcp_host(&config)?,
        config.mcp_host_rpc_limits()?,
    ));
    let mcp_resolver: Arc<dyn McpExecutionContractResolver> =
        Arc::new(PgRepository::new(business_pool.clone()));
    let mut mcp_adapter = McpCapabilityAdapter::new(mcp_codecs, mcp_resolver);
    mcp_adapter
        .install_host(McpTransportKind::StreamableHttp, mcp_host)
        .map_err(|_| ProcessError::InvalidConfiguration)?;
    let mut dispatcher = CapabilityDispatcher::new(InstalledNativeRegistry::default());
    dispatcher
        .install_port(Arc::new(HttpCapabilityAdapter::new(
            http_codecs,
            http_transport,
        )))
        .map_err(|_| ProcessError::InvalidConfiguration)?;
    dispatcher
        .install_port(Arc::new(GrpcCapabilityAdapter::new(
            grpc_codecs,
            grpc_transport,
        )))
        .map_err(|_| ProcessError::InvalidConfiguration)?;
    dispatcher
        .install_port(Arc::new(mcp_adapter))
        .map_err(|_| ProcessError::InvalidConfiguration)?;

    let claim_repository = Arc::new(PgRepository::new(business_pool.clone()));
    let worker = Arc::new(CapabilityAdapterWorker::new(
        Arc::new(dispatcher),
        PgRepository::new(business_pool.clone()),
    ));
    let executor = Arc::new(RegisteredCapabilityJobExecutor::new(
        worker,
        Arc::new(PgRepository::new(critical_control_pool.clone())),
    ));
    let driver = CapabilityWorkerDriver::new(
        claim_repository,
        executor,
        Arc::new(UuidCapabilityWorkerIdentityFactory),
        pools,
        config.driver_config()?,
    )
    .map_err(|_| ProcessError::InvalidConfiguration)?;
    let permit_metrics = Arc::new(WorkerPermitMetrics::default());
    update_worker_permits(&permit_metrics, &observability_pools);
    let metrics = Arc::new(
        ProcessHttpMetrics::install_with_worker_permits(
            "capability-remote-worker",
            PROCESS_OBSERVABILITY_OPERATIONS,
            Arc::clone(&permit_metrics),
        )
        .map_err(|_| ProcessError::InvalidConfiguration)?
        .with_dependency_observations(dependency_metrics.process),
    );
    let listener = tokio::net::TcpListener::bind(&config.observability_listen_address)
        .await
        .map_err(|_| ProcessError::ObservabilityFailed)?;
    eprintln!("platform-capability-remote-worker started");

    let cancellation = CancellationToken::new();
    let sampler_cancellation = cancellation.child_token();
    let mut sampler_task = tokio::spawn(async move {
        tokio::join!(
            run_worker_permit_sampler(
                permit_metrics,
                observability_pools,
                sampler_cancellation.child_token(),
            ),
            run_postgres_health_sampler(
                business_health_pool,
                Arc::clone(&dependency_metrics.postgres),
                sampler_cancellation.child_token(),
            ),
            run_postgres_health_sampler(
                critical_control_health_pool,
                dependency_metrics.postgres,
                sampler_cancellation,
            ),
        );
    });
    let worker_cancellation = cancellation.child_token();
    let mut worker_task = tokio::spawn(async move { driver.run(worker_cancellation).await });
    let http_cancellation = cancellation.child_token();
    let router = process_observability_router(Arc::clone(&metrics));
    let mut http_task = tokio::spawn(async move {
        axum::serve(listener, router)
            .with_graceful_shutdown(http_cancellation.cancelled_owned())
            .await
    });
    metrics.mark_ready();
    tokio::select! {
        signal = shutdown_signal() => {
            signal?;
            cancellation.cancel();
            worker_task.await
                .map_err(|_| ProcessError::WorkerFailed)?
                .map_err(|_| ProcessError::WorkerFailed)?;
            http_task.await
                .map_err(|_| ProcessError::ObservabilityFailed)?
                .map_err(|_| ProcessError::ObservabilityFailed)?;
            sampler_task.await.map_err(|_| ProcessError::DependencyObserverFailed)?;
        }
        result = &mut worker_task => {
            cancellation.cancel();
            result.map_err(|_| ProcessError::WorkerFailed)?
                .map_err(|_| ProcessError::WorkerFailed)?;
            http_task.await
                .map_err(|_| ProcessError::ObservabilityFailed)?
                .map_err(|_| ProcessError::ObservabilityFailed)?;
            sampler_task.await.map_err(|_| ProcessError::DependencyObserverFailed)?;
            return Err(ProcessError::WorkerExitedUnexpectedly);
        }
        result = &mut http_task => {
            cancellation.cancel();
            result.map_err(|_| ProcessError::ObservabilityFailed)?
                .map_err(|_| ProcessError::ObservabilityFailed)?;
            worker_task.await
                .map_err(|_| ProcessError::WorkerFailed)?
                .map_err(|_| ProcessError::WorkerFailed)?;
            sampler_task.await.map_err(|_| ProcessError::DependencyObserverFailed)?;
            return Err(ProcessError::ObservabilityFailed);
        }
        result = &mut sampler_task => {
            cancellation.cancel();
            result.map_err(|_| ProcessError::DependencyObserverFailed)?;
            worker_task.await
                .map_err(|_| ProcessError::WorkerFailed)?
                .map_err(|_| ProcessError::WorkerFailed)?;
            http_task.await
                .map_err(|_| ProcessError::ObservabilityFailed)?
                .map_err(|_| ProcessError::ObservabilityFailed)?;
            return Err(ProcessError::DependencyObserverFailed);
        }
    }
    business_pool.close().await;
    critical_control_pool.close().await;
    Ok(())
}

async fn connect_egress(config: &ProcessConfig) -> Result<tonic::transport::Channel, ProcessError> {
    let tls = ClientTlsConfig::new()
        .domain_name(config.egress.tls_server_name.clone())
        .ca_certificate(Certificate::from_pem(read_bounded_file(
            &required_absolute_path(EGRESS_CA_PATH_ENV)?,
            MAX_TLS_FILE_BYTES,
        )?))
        .identity(Identity::from_pem(
            read_bounded_file(
                &required_absolute_path(EGRESS_CERT_PATH_ENV)?,
                MAX_TLS_FILE_BYTES,
            )?,
            read_bounded_file(
                &required_absolute_path(EGRESS_KEY_PATH_ENV)?,
                MAX_TLS_FILE_BYTES,
            )?,
        ));
    Endpoint::from_shared(config.egress.endpoint.clone())
        .map_err(|_| ProcessError::InvalidConfiguration)?
        .connect_timeout(Duration::from_millis(
            config.egress.connect_timeout_milliseconds,
        ))
        .timeout(Duration::from_millis(
            config.egress.request_timeout_milliseconds,
        ))
        .tls_config(tls)
        .map_err(|_| ProcessError::InvalidConfiguration)?
        .connect()
        .await
        .map_err(|_| ProcessError::EgressUnavailable)
}

fn connect_mcp_host(config: &ProcessConfig) -> Result<tonic::transport::Channel, ProcessError> {
    let tls = ClientTlsConfig::new()
        .domain_name(config.mcp_host.tls_server_name.clone())
        .ca_certificate(Certificate::from_pem(read_bounded_file(
            &required_absolute_path(MCP_HOST_CA_PATH_ENV)?,
            MAX_TLS_FILE_BYTES,
        )?))
        .identity(Identity::from_pem(
            read_bounded_file(
                &required_absolute_path(MCP_HOST_CERT_PATH_ENV)?,
                MAX_TLS_FILE_BYTES,
            )?,
            read_bounded_file(
                &required_absolute_path(MCP_HOST_KEY_PATH_ENV)?,
                MAX_TLS_FILE_BYTES,
            )?,
        ));
    Ok(Endpoint::from_shared(config.mcp_host.endpoint.clone())
        .map_err(|_| ProcessError::InvalidConfiguration)?
        .connect_timeout(Duration::from_millis(
            config.mcp_host.connect_timeout_milliseconds,
        ))
        .timeout(Duration::from_millis(
            config.mcp_host.request_timeout_milliseconds,
        ))
        .tls_config(tls)
        .map_err(|_| ProcessError::InvalidConfiguration)?
        .connect_lazy())
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

fn valid_tls_server_name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 253
        && !value.starts_with('.')
        && !value.ends_with('.')
        && value.split('.').all(|label| {
            !label.is_empty()
                && label.len() <= 63
                && label
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        })
}

fn valid_grpc_name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 256
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-' | b'/'))
}

fn required(name: &'static str) -> Result<String, ProcessError> {
    std::env::var(name)
        .ok()
        .filter(|value| !value.is_empty() && value.len() <= 16_384)
        .ok_or(ProcessError::MissingEnvironment(name))
}

fn required_absolute_path(name: &'static str) -> Result<PathBuf, ProcessError> {
    let path = PathBuf::from(required(name)?);
    if !path.is_absolute() {
        return Err(ProcessError::InvalidConfiguration);
    }
    Ok(path)
}

fn read_bounded_file(path: &PathBuf, maximum: usize) -> Result<Vec<u8>, ProcessError> {
    let mut file = std::fs::File::open(path).map_err(|_| ProcessError::FileUnavailable)?;
    let metadata = file.metadata().map_err(|_| ProcessError::FileUnavailable)?;
    if !metadata.is_file()
        || metadata.len() == 0
        || metadata.len() > u64::try_from(maximum).unwrap_or(u64::MAX)
    {
        return Err(ProcessError::FileUnavailable);
    }
    let mut bytes = Vec::with_capacity(usize::try_from(metadata.len()).unwrap_or(0));
    file.by_ref()
        .take(u64::try_from(maximum).unwrap_or(u64::MAX).saturating_add(1))
        .read_to_end(&mut bytes)
        .map_err(|_| ProcessError::FileUnavailable)?;
    if bytes.is_empty() || bytes.len() > maximum {
        return Err(ProcessError::FileUnavailable);
    }
    Ok(bytes)
}

#[derive(Debug)]
enum ProcessError {
    MissingEnvironment(&'static str),
    InvalidConfiguration,
    FileUnavailable,
    DatabaseUnavailable,
    SchemaMismatch,
    EgressUnavailable,
    SignalUnavailable,
    WorkerFailed,
    WorkerExitedUnexpectedly,
    DependencyObserverFailed,
    ObservabilityFailed,
}

impl fmt::Display for ProcessError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingEnvironment(name) => {
                write!(formatter, "required environment {name} is missing")
            }
            Self::InvalidConfiguration => formatter.write_str("process configuration is invalid"),
            Self::FileUnavailable => formatter.write_str("required file is unavailable"),
            Self::DatabaseUnavailable => formatter.write_str("PostgreSQL is unavailable"),
            Self::SchemaMismatch => {
                formatter.write_str("PostgreSQL schema contract does not match")
            }
            Self::EgressUnavailable => formatter.write_str("Egress Broker is unavailable"),
            Self::SignalUnavailable => {
                formatter.write_str("shutdown signal handler is unavailable")
            }
            Self::WorkerFailed => formatter.write_str("Capability Worker failed"),
            Self::WorkerExitedUnexpectedly => {
                formatter.write_str("Capability Worker exited unexpectedly")
            }
            Self::DependencyObserverFailed => {
                formatter.write_str("Capability dependency observer failed")
            }
            Self::ObservabilityFailed => formatter.write_str("observability server failed"),
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

    fn id(kind: ResourceKind) -> ResourceId {
        ResourceId::from_uuid_v7(kind, Uuid::now_v7()).unwrap()
    }

    fn http_codec() -> InstalledHttpCodecConfig {
        InstalledHttpCodecConfig {
            codec_id: BUILTIN_JSON_CODEC_ID.to_owned(),
            codec_version: BUILTIN_JSON_CODEC_VERSION.to_owned(),
            module_digest: builtin_json_codec_module_digest(),
            worker_protocol_version: WORKER_PROTOCOL_VERSION,
            descriptor_digest: digest('1'),
            protocol_contract_digest: builtin_json_http_protocol_contract_digest(),
            request_mapping_digest: builtin_json_http_request_mapping_digest(),
            response_mapping_digest: builtin_json_http_response_mapping_digest(),
            error_mapping_digest: builtin_json_http_error_mapping_digest(),
        }
    }

    fn grpc_codec() -> InstalledGrpcCodecConfig {
        InstalledGrpcCodecConfig {
            codec_id: BUILTIN_JSON_CODEC_ID.to_owned(),
            codec_version: BUILTIN_JSON_CODEC_VERSION.to_owned(),
            module_digest: builtin_json_codec_module_digest(),
            worker_protocol_version: WORKER_PROTOCOL_VERSION,
            descriptor_digest: digest('6'),
            protobuf_contract_digest: builtin_json_grpc_protobuf_contract_digest(),
            service_name: "insight.fixture.v1.Lookup".to_owned(),
            method_name: "Get".to_owned(),
            request_mapping_digest: builtin_json_grpc_request_mapping_digest(),
            response_mapping_digest: builtin_json_grpc_response_mapping_digest(),
            error_mapping_digest: builtin_json_grpc_error_mapping_digest(),
        }
    }

    fn mcp_codec() -> InstalledMcpCodecConfig {
        InstalledMcpCodecConfig {
            codec_id: BUILTIN_JSON_CODEC_ID.to_owned(),
            codec_version: BUILTIN_JSON_CODEC_VERSION.to_owned(),
            module_digest: builtin_json_codec_module_digest(),
            worker_protocol_version: WORKER_PROTOCOL_VERSION,
            descriptor_digest: digest('b'),
            remote_tool_name: "fixture_lookup".to_owned(),
            remote_input_schema_digest: digest('c'),
            output_mapping_digest: builtin_json_mcp_output_mapping_digest(),
            protocol_profile_id: id(ResourceKind::PolicyRevision),
            protocol_profile_digest: digest('e'),
            discovery_semantic_evidence_digest: digest('f'),
        }
    }

    fn config() -> ProcessConfig {
        let installed_http_codecs = vec![http_codec()];
        let installed_grpc_codecs = vec![grpc_codec()];
        let installed_mcp_codecs = vec![mcp_codec()];
        let installed_value = serde_json::to_value(InstalledCodecClosure {
            schema_version: 1,
            http: &installed_http_codecs,
            grpc: &installed_grpc_codecs,
            mcp: &installed_mcp_codecs,
        })
        .unwrap();
        let adapter_runtime_digest = canonical_digest(&installed_value).unwrap().parse().unwrap();
        ProcessConfig {
            schema_version: 1,
            observability_listen_address: "127.0.0.1:9090".to_owned(),
            worker_manifest: WorkerManifest {
                manifest_version: WORKER_MANIFEST_VERSION,
                worker_role: CAPABILITY_REMOTE_WORKER_ROLE.to_owned(),
                work_class: WorkClass::CapabilityRemote,
                adapter_runtime_digest,
                protocol_version: WORKER_PROTOCOL_VERSION,
                max_concurrency: 4,
                critical_control_reserved_slots: 1,
            },
            installed_http_codecs,
            installed_grpc_codecs,
            installed_mcp_codecs,
            database: DatabaseConfig {
                business_max_connections: 4,
                critical_control_max_connections: 2,
                process_connection_budget: 6,
                acquire_timeout_milliseconds: 1_000,
            },
            egress: EgressConfig {
                endpoint: "https://platform-egress-broker.egress.svc:7443/".to_owned(),
                tls_server_name: "platform-egress-broker.egress.svc".to_owned(),
                connect_timeout_milliseconds: 1_000,
                request_timeout_milliseconds: 30_000,
                maximum_rpc_metadata_bytes: 65_536,
                maximum_rpc_payload_bytes: 1_048_576,
            },
            mcp_host: McpHostConfig {
                endpoint: "https://platform-mcp-host.platform-mcp-host.svc:9443/".to_owned(),
                tls_server_name: "platform-mcp-host.platform-mcp-host.svc".to_owned(),
                connect_timeout_milliseconds: 1_000,
                request_timeout_milliseconds: 30_000,
                maximum_rpc_message_bytes: 1_048_576,
            },
            timing: TimingConfig {
                initial_scan_delay_milliseconds: 0,
                receipt_ttl_milliseconds: 60_000,
                safety_scan_milliseconds: 100,
                claim_failure_backoff_milliseconds: 10,
                drain_grace_milliseconds: 5_000,
            },
        }
    }

    #[test]
    fn config_binds_role_codecs_egress_and_database_budget() {
        config().validate().unwrap();

        let mut wrong_codec = config();
        wrong_codec.installed_http_codecs[0].module_digest = digest('f');
        assert!(wrong_codec.validate().is_err());

        let mut wrong_mapping = config();
        wrong_mapping.installed_grpc_codecs[0].response_mapping_digest = digest('f');
        assert!(wrong_mapping.validate().is_err());

        let mut wrong_manifest = config();
        wrong_manifest.worker_manifest.adapter_runtime_digest = digest('e');
        assert!(wrong_manifest.validate().is_err());

        let mut wrong_egress = config();
        wrong_egress.egress.endpoint = "http://platform-egress-broker:7443/".to_owned();
        assert!(wrong_egress.validate().is_err());

        let mut wrong_mcp_host = config();
        wrong_mcp_host.mcp_host.endpoint = "http://platform-mcp-host:9443/".to_owned();
        assert!(wrong_mcp_host.validate().is_err());

        let mut wrong_budget = config();
        wrong_budget.database.process_connection_budget += 1;
        assert!(wrong_budget.validate().is_err());
    }
}
