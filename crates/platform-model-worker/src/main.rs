//! Deployable Model Worker process for the clean-cut Platform v1 candidate.
//!
//! The process owns only Model-local permits and adapter orchestration. PostgreSQL remains the
//! durable Job/ModelTurn authority, Provider traffic crosses the Model Worker mTLS Egress RPC,
//! and this first production composition deliberately accepts only Inline RunValue material.

use insight_platform_contracts::{
    canonical_digest, checked_in_hard_limit_profile, parse_strict_json, InstalledModelAdapter,
    JsonLimits, ResourceId, ResourceKind, Sha256Digest, WorkClass, WorkerManifest,
};
use insight_platform_egress_rpc::{EgressBrokerGrpcClient, EgressInternalRpcLimits};
use insight_platform_model_adapters::{
    AnthropicMessagesAdapter, DropModelLiveDeltas, InstalledModelAdapterDescriptor,
    InstalledModelAdapterRegistry, ModelAdapterHost, ModelAdapterWorker,
    ModelProviderWireConnector, OpenAiResponsesAdapter, ANTHROPIC_MESSAGES_ADAPTER_NAME,
    OPENAI_RESPONSES_ADAPTER_NAME,
};
use insight_platform_model_worker::{
    InlineModelOutputMaterializer, InlineModelRequestMaterializer, ModelCancellationDriver,
    ModelCancellationDriverConfig, ModelWorkerDriver, ModelWorkerDriverConfig,
    ModelWorkerDriverTiming, RegisteredModelJobExecutor, UuidModelWorkerIdentityFactory,
    MODEL_WORKER_ROLE,
};
use insight_platform_models::ModelTurnLimits;
use insight_platform_postgres::{repository::PgRepository, verify_schema};
use insight_platform_worker::LocalWorkerPools;
use serde::Deserialize;
use sqlx::postgres::PgPoolOptions;
use std::{
    collections::BTreeSet, error::Error, fmt, io::Read as _, path::PathBuf, sync::Arc,
    time::Duration,
};
use tokio_util::sync::CancellationToken;
use tonic::transport::{Certificate, ClientTlsConfig, Endpoint, Identity};
use url::Url;
use uuid::Uuid;

const CONFIG_PATH_ENV: &str = "PLATFORM_MODEL_WORKER_CONFIG";
const CONFIG_DIGEST_ENV: &str = "PLATFORM_MODEL_WORKER_CONFIG_DIGEST";
const DATABASE_URL_ENV: &str = "PLATFORM_MODEL_WORKER_DATABASE_URL";
const EGRESS_CA_PATH_ENV: &str = "PLATFORM_MODEL_WORKER_EGRESS_CA_PATH";
const EGRESS_CERT_PATH_ENV: &str = "PLATFORM_MODEL_WORKER_EGRESS_CERT_PATH";
const EGRESS_KEY_PATH_ENV: &str = "PLATFORM_MODEL_WORKER_EGRESS_KEY_PATH";
const MAX_CONFIG_BYTES: usize = 1_048_576;
const MAX_TLS_FILE_BYTES: usize = 1_048_576;
const MAX_INSTALLED_MODEL_ADAPTERS: usize = 8;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct ModelWorkerProcessConfig {
    schema_version: u32,
    worker_manifest: WorkerManifest,
    installed_adapters: Vec<InstalledModelAdapter>,
    database_max_connections: u32,
    database_acquire_timeout_milliseconds: u64,
    egress_endpoint: String,
    egress_tls_server_name: String,
    egress_connect_timeout_milliseconds: u64,
    egress_request_timeout_milliseconds: u64,
    maximum_rpc_metadata_bytes: usize,
    maximum_rpc_payload_bytes: usize,
    receipt_ttl_seconds: u64,
    claim_scan_milliseconds: u64,
    claim_failure_backoff_milliseconds: u64,
    drain_grace_milliseconds: u64,
}

impl ModelWorkerProcessConfig {
    fn load() -> Result<Self, ProcessError> {
        let path = required_absolute_path(CONFIG_PATH_ENV)?;
        let bytes = read_bounded_file(&path, MAX_CONFIG_BYTES)?;
        let value = parse_strict_json(
            &bytes,
            JsonLimits {
                max_bytes: MAX_CONFIG_BYTES,
                max_depth: 16,
                max_items_per_array: MAX_INSTALLED_MODEL_ADAPTERS,
                max_properties_per_object: 32,
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
        self.worker_manifest
            .validate()
            .map_err(|_| ProcessError::InvalidConfiguration)?;
        let worker_manifest_digest = self
            .worker_manifest
            .canonical_digest()
            .map_err(|_| ProcessError::InvalidConfiguration)?;
        let egress =
            Url::parse(&self.egress_endpoint).map_err(|_| ProcessError::InvalidConfiguration)?;
        if self.schema_version != 1
            || self.worker_manifest.worker_role != MODEL_WORKER_ROLE
            || self.worker_manifest.work_class != WorkClass::Model
            || self.installed_adapters.len() != 2
            || self.installed_adapters.len() > MAX_INSTALLED_MODEL_ADAPTERS
            || !(2..=64).contains(&self.database_max_connections)
            || self.database_acquire_timeout_milliseconds == 0
            || self.database_acquire_timeout_milliseconds > 30_000
            || egress.scheme() != "https"
            || egress.host_str().is_none()
            || egress.port().is_none()
            || egress.username() != ""
            || egress.password().is_some()
            || egress.path() != "/"
            || egress.query().is_some()
            || egress.fragment().is_some()
            || !valid_tls_server_name(&self.egress_tls_server_name)
            || self.egress_connect_timeout_milliseconds == 0
            || self.egress_connect_timeout_milliseconds > 30_000
            || self.egress_request_timeout_milliseconds == 0
            || self.egress_request_timeout_milliseconds > 120_000
            || self.receipt_ttl_seconds == 0
            || self.claim_scan_milliseconds == 0
            || self.claim_scan_milliseconds > 60_000
            || self.claim_failure_backoff_milliseconds == 0
            || self.claim_failure_backoff_milliseconds > self.claim_scan_milliseconds
            || self.drain_grace_milliseconds == 0
            || self.drain_grace_milliseconds > 120_000
        {
            return Err(ProcessError::InvalidConfiguration);
        }
        EgressInternalRpcLimits::new(
            self.maximum_rpc_metadata_bytes,
            self.maximum_rpc_payload_bytes,
        )
        .map_err(|_| ProcessError::InvalidConfiguration)?;
        let mut names = BTreeSet::new();
        for adapter in &self.installed_adapters {
            adapter
                .validate()
                .map_err(|_| ProcessError::InvalidConfiguration)?;
            if adapter.worker_manifest_digest != worker_manifest_digest
                || !names.insert(adapter.qualified_name.as_str())
                || !matches!(
                    adapter.qualified_name.as_str(),
                    OPENAI_RESPONSES_ADAPTER_NAME | ANTHROPIC_MESSAGES_ADAPTER_NAME
                )
            {
                return Err(ProcessError::InvalidConfiguration);
            }
        }
        if !names.contains(OPENAI_RESPONSES_ADAPTER_NAME)
            || !names.contains(ANTHROPIC_MESSAGES_ADAPTER_NAME)
        {
            return Err(ProcessError::InvalidConfiguration);
        }
        self.driver_config()
            .map(|_| ())
            .and_then(|_| self.cancellation_driver_config().map(|_| ()))
            .map_err(|_| ProcessError::InvalidConfiguration)
    }

    fn rpc_limits(&self) -> Result<EgressInternalRpcLimits, ProcessError> {
        EgressInternalRpcLimits::new(
            self.maximum_rpc_metadata_bytes,
            self.maximum_rpc_payload_bytes,
        )
        .map_err(|_| ProcessError::InvalidConfiguration)
    }

    fn driver_config(&self) -> Result<ModelWorkerDriverConfig, ProcessError> {
        ModelWorkerDriverConfig::from_profile(
            &checked_in_hard_limit_profile(),
            Duration::from_secs(self.receipt_ttl_seconds),
            ModelWorkerDriverTiming {
                safety_scan_interval: Duration::from_millis(self.claim_scan_milliseconds),
                claim_failure_backoff: Duration::from_millis(
                    self.claim_failure_backoff_milliseconds,
                ),
                drain_grace: Duration::from_millis(self.drain_grace_milliseconds),
            },
        )
        .map_err(|_| ProcessError::InvalidConfiguration)
    }

    fn cancellation_driver_config(&self) -> Result<ModelCancellationDriverConfig, ProcessError> {
        ModelCancellationDriverConfig::from_profile(
            &checked_in_hard_limit_profile(),
            Duration::from_secs(self.receipt_ttl_seconds),
            Duration::from_millis(self.claim_scan_milliseconds),
            Duration::from_millis(self.claim_failure_backoff_milliseconds),
        )
        .map_err(|_| ProcessError::InvalidConfiguration)
    }
}

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("platform-model-worker failed: {error}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), ProcessError> {
    let config = ModelWorkerProcessConfig::load()?;
    let profile = checked_in_hard_limit_profile();
    let limits =
        ModelTurnLimits::from_profile(&profile).map_err(|_| ProcessError::InvalidConfiguration)?;
    let database_url = required(DATABASE_URL_ENV)?;
    let pool = PgPoolOptions::new()
        .max_connections(config.database_max_connections)
        .acquire_timeout(Duration::from_millis(
            config.database_acquire_timeout_milliseconds,
        ))
        .connect(&database_url)
        .await
        .map_err(|_| ProcessError::DatabaseUnavailable)?;
    verify_schema(&pool)
        .await
        .map_err(|_| ProcessError::SchemaMismatch)?;
    let repository = Arc::new(PgRepository::new(pool));
    let process_generation_id =
        ResourceId::from_uuid_v7(ResourceKind::WorkerProcessGeneration, Uuid::now_v7())
            .map_err(|_| ProcessError::InvalidConfiguration)?;
    let pools = LocalWorkerPools::new(
        config.worker_manifest.clone(),
        process_generation_id.clone(),
    )
    .map_err(|_| ProcessError::InvalidConfiguration)?;
    let manifest_digest = config
        .worker_manifest
        .canonical_digest()
        .map_err(|_| ProcessError::InvalidConfiguration)?;
    let egress = Arc::new(EgressBrokerGrpcClient::new(
        connect_egress(&config).await?,
        config.rpc_limits()?,
    ));
    let connector: Arc<dyn ModelProviderWireConnector> = egress.clone();
    let mut registry = InstalledModelAdapterRegistry::default();
    for installed in &config.installed_adapters {
        let descriptor = InstalledModelAdapterDescriptor::from(installed);
        match installed.qualified_name.as_str() {
            OPENAI_RESPONSES_ADAPTER_NAME => registry
                .install(Arc::new(
                    OpenAiResponsesAdapter::new(descriptor, Arc::clone(&connector))
                        .map_err(|_| ProcessError::InvalidConfiguration)?,
                ))
                .map_err(|_| ProcessError::InvalidConfiguration)?,
            ANTHROPIC_MESSAGES_ADAPTER_NAME => registry
                .install(Arc::new(
                    AnthropicMessagesAdapter::new(descriptor, Arc::clone(&connector))
                        .map_err(|_| ProcessError::InvalidConfiguration)?,
                ))
                .map_err(|_| ProcessError::InvalidConfiguration)?,
            _ => return Err(ProcessError::InvalidConfiguration),
        }
    }
    let identities = Arc::new(UuidModelWorkerIdentityFactory);
    let host = Arc::new(ModelAdapterHost::new(
        registry,
        Arc::new(DropModelLiveDeltas),
        limits,
    ));
    let worker = Arc::new(ModelAdapterWorker::new(
        host,
        InlineModelOutputMaterializer::new(Arc::clone(&identities), limits),
        repository.as_ref().clone(),
    ));
    let executor = Arc::new(RegisteredModelJobExecutor::new(
        InlineModelRequestMaterializer,
        worker,
        Arc::clone(&repository),
    ));
    let driver = ModelWorkerDriver::new(
        Arc::clone(&repository),
        executor,
        Arc::clone(&identities),
        pools.clone(),
        config.driver_config()?,
    )
    .map_err(|_| ProcessError::InvalidConfiguration)?;
    let cancellation_driver = ModelCancellationDriver::new(
        Arc::clone(&repository),
        Arc::clone(&repository),
        egress,
        identities,
        pools,
        config.cancellation_driver_config()?,
    )
    .map_err(|_| ProcessError::InvalidConfiguration)?;

    eprintln!(
        "platform-model-worker started generation={} manifest={}",
        process_generation_id, manifest_digest
    );
    let cancellation = CancellationToken::new();
    let mut driver_task = tokio::spawn(driver.run(cancellation.child_token()));
    let mut cancellation_task = tokio::spawn(cancellation_driver.run(cancellation.child_token()));
    tokio::select! {
        signal = shutdown_signal() => {
            cancellation.cancel();
            let (driver_result, cancellation_result) = tokio::join!(driver_task, cancellation_task);
            signal?;
            driver_result
                .map_err(|_| ProcessError::WorkerFailed)?
                .map_err(|_| ProcessError::WorkerFailed)?;
            cancellation_result
                .map_err(|_| ProcessError::WorkerFailed)?
                .map_err(|_| ProcessError::WorkerFailed)?;
            Ok(())
        }
        result = &mut driver_task => {
            cancellation.cancel();
            let cancellation_result = cancellation_task.await;
            result
                .map_err(|_| ProcessError::WorkerFailed)?
                .map_err(|_| ProcessError::WorkerFailed)?;
            cancellation_result
                .map_err(|_| ProcessError::WorkerFailed)?
                .map_err(|_| ProcessError::WorkerFailed)?;
            Err(ProcessError::WorkerExitedUnexpectedly)
        }
        result = &mut cancellation_task => {
            cancellation.cancel();
            let driver_result = driver_task.await;
            result
                .map_err(|_| ProcessError::WorkerFailed)?
                .map_err(|_| ProcessError::WorkerFailed)?;
            driver_result
                .map_err(|_| ProcessError::WorkerFailed)?
                .map_err(|_| ProcessError::WorkerFailed)?;
            Err(ProcessError::WorkerExitedUnexpectedly)
        }
    }
}

async fn connect_egress(
    config: &ModelWorkerProcessConfig,
) -> Result<tonic::transport::Channel, ProcessError> {
    let tls = ClientTlsConfig::new()
        .domain_name(config.egress_tls_server_name.clone())
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
    Endpoint::from_shared(config.egress_endpoint.clone())
        .map_err(|_| ProcessError::InvalidConfiguration)?
        .connect_timeout(Duration::from_millis(
            config.egress_connect_timeout_milliseconds,
        ))
        .timeout(Duration::from_millis(
            config.egress_request_timeout_milliseconds,
        ))
        .tls_config(tls)
        .map_err(|_| ProcessError::InvalidConfiguration)?
        .connect()
        .await
        .map_err(|_| ProcessError::EgressUnavailable)
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
        return Err(ProcessError::InvalidConfiguration);
    }
    let mut bytes = Vec::with_capacity(maximum.min(64 * 1_024));
    let read_result = file
        .take(u64::try_from(maximum.saturating_add(1)).unwrap_or(u64::MAX))
        .read_to_end(&mut bytes);
    if read_result.is_err() || bytes.is_empty() || bytes.len() > maximum {
        bytes.fill(0);
        return Err(ProcessError::InvalidConfiguration);
    }
    Ok(bytes)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProcessError {
    MissingConfiguration(&'static str),
    InvalidConfiguration,
    FileUnavailable,
    DatabaseUnavailable,
    SchemaMismatch,
    EgressUnavailable,
    SignalUnavailable,
    WorkerFailed,
    WorkerExitedUnexpectedly,
}

impl fmt::Display for ProcessError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingConfiguration(name) => {
                write!(formatter, "required configuration {name} is missing")
            }
            Self::InvalidConfiguration => formatter.write_str("configuration is invalid"),
            Self::FileUnavailable => formatter.write_str("configuration file is unavailable"),
            Self::DatabaseUnavailable => formatter.write_str("database is unavailable"),
            Self::SchemaMismatch => formatter.write_str("database schema does not match candidate"),
            Self::EgressUnavailable => formatter.write_str("Egress Broker is unavailable"),
            Self::SignalUnavailable => formatter.write_str("shutdown signal is unavailable"),
            Self::WorkerFailed => formatter.write_str("Model Worker driver failed"),
            Self::WorkerExitedUnexpectedly => {
                formatter.write_str("Model Worker exited unexpectedly")
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

    fn fixture_config() -> ModelWorkerProcessConfig {
        let manifest = WorkerManifest {
            manifest_version: WORKER_MANIFEST_VERSION,
            worker_role: MODEL_WORKER_ROLE.to_owned(),
            work_class: WorkClass::Model,
            adapter_runtime_digest: digest('a'),
            protocol_version: WORKER_PROTOCOL_VERSION,
            max_concurrency: 16,
            critical_control_reserved_slots: 2,
        };
        let manifest_digest = manifest.canonical_digest().unwrap();
        ModelWorkerProcessConfig {
            schema_version: 1,
            worker_manifest: manifest,
            installed_adapters: vec![
                InstalledModelAdapter {
                    qualified_name: ANTHROPIC_MESSAGES_ADAPTER_NAME.to_owned(),
                    worker_manifest_digest: manifest_digest.clone(),
                    adapter_contract_digest: digest('b'),
                },
                InstalledModelAdapter {
                    qualified_name: OPENAI_RESPONSES_ADAPTER_NAME.to_owned(),
                    worker_manifest_digest: manifest_digest,
                    adapter_contract_digest: digest('c'),
                },
            ],
            database_max_connections: 16,
            database_acquire_timeout_milliseconds: 5_000,
            egress_endpoint: "https://egress-broker.platform-egress.svc:8443/".to_owned(),
            egress_tls_server_name: "egress-broker.platform-egress.svc".to_owned(),
            egress_connect_timeout_milliseconds: 5_000,
            egress_request_timeout_milliseconds: 120_000,
            maximum_rpc_metadata_bytes: 1_048_576,
            maximum_rpc_payload_bytes: 64 * 1_048_576,
            receipt_ttl_seconds: 3_600,
            claim_scan_milliseconds: 250,
            claim_failure_backoff_milliseconds: 100,
            drain_grace_milliseconds: 120_000,
        }
    }

    #[test]
    fn process_config_requires_exact_two_adapter_manifest() {
        let mut config = fixture_config();
        config.validate().unwrap();
        config.installed_adapters[0].worker_manifest_digest = digest('d');
        assert_eq!(config.validate(), Err(ProcessError::InvalidConfiguration));
        let mut config = fixture_config();
        config.installed_adapters[1].qualified_name = ANTHROPIC_MESSAGES_ADAPTER_NAME.to_owned();
        assert_eq!(config.validate(), Err(ProcessError::InvalidConfiguration));
    }

    #[test]
    fn process_config_rejects_cross_role_and_unbounded_transport() {
        let mut config = fixture_config();
        config.worker_manifest.work_class = WorkClass::Sandbox;
        assert_eq!(config.validate(), Err(ProcessError::InvalidConfiguration));
        let mut config = fixture_config();
        config.egress_endpoint = "http://egress-broker:8080/".to_owned();
        assert_eq!(config.validate(), Err(ProcessError::InvalidConfiguration));
        let mut config = fixture_config();
        config.maximum_rpc_payload_bytes = usize::MAX;
        assert_eq!(config.validate(), Err(ProcessError::InvalidConfiguration));
        let mut config = fixture_config();
        config.drain_grace_milliseconds = 120_001;
        assert_eq!(config.validate(), Err(ProcessError::InvalidConfiguration));
    }
}
