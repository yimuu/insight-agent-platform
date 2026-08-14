//! Dedicated SecretBinding authority process.
//!
//! This role has a restricted PostgreSQL credential and exposes exactly two internal methods. It
//! deliberately has no HTTP client, DNS resolver, KMS, Secret Manager, provider catalog or public
//! API dependency. The Egress Broker is the only accepted mTLS caller.

use async_trait::async_trait;
use insight_platform_contracts::{
    canonical_digest, parse_strict_json, JsonLimits, ResourceId, Sha256Digest,
};
use insight_platform_postgres::{repository::PgRepository, verify_schema};
use insight_platform_security::{
    PreparedSecretBindingAuthority, PreparedSecretBindingRegistrationError,
    PreparedSecretBindingRegistrationOutcome, RegisterPreparedSecretBinding,
    SecretBindingResolutionAuthority, SecretBindingResolutionError, SecretBindingResolutionRecord,
};
use insight_platform_security_rpc::{
    proto::security_secret_authority_service_server::SecuritySecretAuthorityServiceServer,
    EgressBrokerWorkloadIdentity, SecurityInternalRpcLimits, SecuritySecretAuthorityGrpcService,
    MAX_SECURITY_INTERNAL_RPC_BYTES_HARD,
};
use serde::Deserialize;
use sqlx::postgres::PgPoolOptions;
use std::{
    error::Error, fmt, io::Read as _, net::SocketAddr, path::PathBuf, sync::Arc, time::Duration,
};
use tokio::sync::oneshot;
use tonic::transport::{Certificate, Identity, Server, ServerTlsConfig};

const CONFIG_PATH_ENV: &str = "PLATFORM_SECURITY_AUTHORITY_CONFIG";
const CONFIG_DIGEST_ENV: &str = "PLATFORM_SECURITY_AUTHORITY_CONFIG_DIGEST";
const DATABASE_URL_ENV: &str = "PLATFORM_SECURITY_AUTHORITY_DATABASE_URL";
const CLIENT_CA_PATH_ENV: &str = "PLATFORM_SECURITY_AUTHORITY_CLIENT_CA_PATH";
const SERVER_CERT_PATH_ENV: &str = "PLATFORM_SECURITY_AUTHORITY_CERT_PATH";
const SERVER_KEY_PATH_ENV: &str = "PLATFORM_SECURITY_AUTHORITY_KEY_PATH";
const MAX_CONFIG_BYTES: usize = 64 * 1024;
const MAX_TLS_FILE_BYTES: usize = 1024 * 1024;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProcessConfig {
    schema_version: u32,
    listen_address: String,
    service_principal_id: ResourceId,
    database_max_connections: u32,
    database_acquire_timeout_milliseconds: u64,
    maximum_rpc_message_bytes: usize,
    tls_handshake_timeout_milliseconds: u64,
    shutdown_grace_milliseconds: u64,
}

impl ProcessConfig {
    fn load() -> Result<Self, ProcessError> {
        let path = required_absolute_path(CONFIG_PATH_ENV)?;
        let bytes = read_bounded(&path, MAX_CONFIG_BYTES)?;
        let value = parse_strict_json(
            &bytes,
            JsonLimits {
                max_bytes: MAX_CONFIG_BYTES,
                max_depth: 8,
                max_items_per_array: 8,
                max_properties_per_object: 16,
                max_string_bytes: 4_096,
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
        let listen: SocketAddr = self
            .listen_address
            .parse()
            .map_err(|_| ProcessError::InvalidConfiguration)?;
        if self.schema_version != 1
            || listen.port() == 0
            || self.service_principal_id.kind()
                != insight_platform_contracts::ResourceKind::Principal
            || !(2..=32).contains(&self.database_max_connections)
            || self.database_acquire_timeout_milliseconds == 0
            || self.database_acquire_timeout_milliseconds > 30_000
            || !(1..=MAX_SECURITY_INTERNAL_RPC_BYTES_HARD).contains(&self.maximum_rpc_message_bytes)
            || self.tls_handshake_timeout_milliseconds == 0
            || self.tls_handshake_timeout_milliseconds > 30_000
            || self.shutdown_grace_milliseconds == 0
            || self.shutdown_grace_milliseconds > 60_000
        {
            return Err(ProcessError::InvalidConfiguration);
        }
        Ok(())
    }
}

/// Private composition wrapper: no caller can obtain the repository or any unrelated command
/// trait through this process surface.
struct RestrictedSecretAuthority {
    repository: PgRepository,
    service_principal_id: ResourceId,
}

#[async_trait]
impl SecretBindingResolutionAuthority for RestrictedSecretAuthority {
    async fn load_for_resolution(
        &self,
        tenant_id: &ResourceId,
        secret_binding_id: &ResourceId,
    ) -> Result<SecretBindingResolutionRecord, SecretBindingResolutionError> {
        self.repository
            .load_for_resolution(tenant_id, secret_binding_id)
            .await
    }
}

#[async_trait]
impl PreparedSecretBindingAuthority for RestrictedSecretAuthority {
    async fn register_prepared(
        &self,
        command: RegisterPreparedSecretBinding,
    ) -> Result<PreparedSecretBindingRegistrationOutcome, PreparedSecretBindingRegistrationError>
    {
        if command.audit.principal_id != self.service_principal_id
            || command.audit.principal_kind
                != insight_platform_contracts::PrincipalKind::ServiceIdentity
        {
            return Err(PreparedSecretBindingRegistrationError::Rejected);
        }
        self.repository.register_prepared(command).await
    }
}

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("platform-security-authority failed: {error}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), ProcessError> {
    let config = ProcessConfig::load()?;
    let limits = SecurityInternalRpcLimits::new(config.maximum_rpc_message_bytes)
        .map_err(|_| ProcessError::InvalidConfiguration)?;
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
    let authority = Arc::new(RestrictedSecretAuthority {
        repository: PgRepository::new(pool),
        service_principal_id: config.service_principal_id.clone(),
    });

    let maximum = limits.maximum_message_bytes();
    let service =
        SecuritySecretAuthorityServiceServer::new(SecuritySecretAuthorityGrpcService::new(
            Arc::clone(&authority),
            Arc::clone(&authority),
            limits,
        ))
        .max_encoding_message_size(maximum)
        .max_decoding_message_size(maximum);
    let service =
        tonic::service::interceptor::InterceptedService::new(service, EgressBrokerWorkloadIdentity);
    let tls = ServerTlsConfig::new()
        .identity(Identity::from_pem(
            read_bounded(
                &required_absolute_path(SERVER_CERT_PATH_ENV)?,
                MAX_TLS_FILE_BYTES,
            )?,
            read_bounded(
                &required_absolute_path(SERVER_KEY_PATH_ENV)?,
                MAX_TLS_FILE_BYTES,
            )?,
        ))
        .client_ca_root(Certificate::from_pem(read_bounded(
            &required_absolute_path(CLIENT_CA_PATH_ENV)?,
            MAX_TLS_FILE_BYTES,
        )?))
        .timeout(Duration::from_millis(
            config.tls_handshake_timeout_milliseconds,
        ));
    let listen: SocketAddr = config
        .listen_address
        .parse()
        .map_err(|_| ProcessError::InvalidConfiguration)?;
    let (shutdown_sender, shutdown_receiver) = oneshot::channel::<()>();
    let server = Server::builder()
        .tls_config(tls)
        .map_err(|_| ProcessError::InvalidConfiguration)?
        .add_service(service)
        .serve_with_shutdown(listen, async move {
            let _ = shutdown_receiver.await;
        });
    let mut server = tokio::spawn(server);
    shutdown_signal().await;
    let _ = shutdown_sender.send(());
    match tokio::time::timeout(
        Duration::from_millis(config.shutdown_grace_milliseconds),
        &mut server,
    )
    .await
    {
        Ok(joined) => joined
            .map_err(|_| ProcessError::ServerFailure)?
            .map_err(|_| ProcessError::ServerFailure),
        Err(_) => {
            server.abort();
            let _ = server.await;
            Err(ProcessError::ShutdownTimeout)
        }
    }
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        let mut terminate =
            match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
                Ok(signal) => signal,
                Err(_) => {
                    let _ = tokio::signal::ctrl_c().await;
                    return;
                }
            };
        tokio::select! {
            _ = terminate.recv() => {},
            _ = tokio::signal::ctrl_c() => {},
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
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

fn read_bounded(path: &PathBuf, maximum: usize) -> Result<Vec<u8>, ProcessError> {
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

#[derive(Debug)]
enum ProcessError {
    MissingConfiguration(&'static str),
    InvalidConfiguration,
    FileUnavailable,
    DatabaseUnavailable,
    SchemaMismatch,
    ServerFailure,
    ShutdownTimeout,
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
            Self::SchemaMismatch => {
                formatter.write_str("database schema does not match the candidate")
            }
            Self::ServerFailure => formatter.write_str("internal gRPC server failed"),
            Self::ShutdownTimeout => formatter.write_str("graceful shutdown timed out"),
        }
    }
}

impl Error for ProcessError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn process_config_has_no_external_or_secret_provider_fields() {
        let config: ProcessConfig = serde_json::from_value(serde_json::json!({
            "schema_version": 1,
            "listen_address": "0.0.0.0:9443",
            "service_principal_id": "prn_0198f1c3-8f49-7c3e-b1f3-773c28367b90",
            "database_max_connections": 8,
            "database_acquire_timeout_milliseconds": 5000,
            "maximum_rpc_message_bytes": 65536,
            "tls_handshake_timeout_milliseconds": 5000,
            "shutdown_grace_milliseconds": 10000
        }))
        .unwrap();
        config.validate().unwrap();

        assert!(serde_json::from_value::<ProcessConfig>(serde_json::json!({
            "schema_version": 1,
            "listen_address": "0.0.0.0:9443",
            "service_principal_id": "prn_0198f1c3-8f49-7c3e-b1f3-773c28367b90",
            "database_max_connections": 8,
            "database_acquire_timeout_milliseconds": 5000,
            "maximum_rpc_message_bytes": 65536,
            "tls_handshake_timeout_milliseconds": 5000,
            "shutdown_grace_milliseconds": 10000,
            "secret_manager_endpoint": "https://forbidden.example"
        }))
        .is_err());
    }
}
