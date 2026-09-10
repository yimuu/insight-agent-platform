use super::*;
use insight_platform_egress_rpc::{EgressBrokerGrpcClient, EgressInternalRpcLimits};
use insight_platform_security::{ModelConnectionProbe, ModelCredentialImporter};
pub(super) trait ModelManagementEgress:
    ModelCredentialImporter + ModelConnectionProbe
{
}
impl<T: ModelCredentialImporter + ModelConnectionProbe> ModelManagementEgress for T {}
use tonic::transport::{Certificate, ClientTlsConfig, Endpoint, Identity};

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct GatewayEgressConfig {
    endpoint: String,
    tls_server_name: String,
    connect_timeout_milliseconds: u64,
    request_timeout_milliseconds: u64,
    maximum_rpc_metadata_bytes: usize,
    maximum_rpc_payload_bytes: usize,
}
impl GatewayEgressConfig {
    pub(super) fn validate(&self) -> Result<(), ProcessError> {
        ArtifactGatewayConfig {
            endpoint: self.endpoint.clone(),
        }
        .validate()?;
        if self.endpoint.len() > 2048
            || self.tls_server_name.is_empty()
            || self.tls_server_name.len() > 253
            || !self
                .tls_server_name
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.'))
            || !(1..=10000).contains(&self.connect_timeout_milliseconds)
            || self.request_timeout_milliseconds <= self.connect_timeout_milliseconds
            || self.request_timeout_milliseconds > 30000
            || self.maximum_rpc_metadata_bytes != 4096
            || self.maximum_rpc_payload_bytes != 4096
        {
            return Err(ProcessError::InvalidConfiguration);
        }
        Ok(())
    }
    pub(super) async fn install(&self) -> Result<Arc<dyn ModelManagementEgress>, ProcessError> {
        self.validate()?;
        let tls = ClientTlsConfig::new()
            .domain_name(self.tls_server_name.clone())
            .ca_certificate(Certificate::from_pem(read_bounded_file(
                &required_absolute_path("PLATFORM_GATEWAY_EGRESS_CA_PATH")?,
                MAX_TLS_FILE_BYTES,
            )?))
            .identity(Identity::from_pem(
                read_bounded_file(
                    &required_absolute_path("PLATFORM_GATEWAY_EGRESS_CERT_PATH")?,
                    MAX_TLS_FILE_BYTES,
                )?,
                read_bounded_file(
                    &required_absolute_path("PLATFORM_GATEWAY_EGRESS_KEY_PATH")?,
                    MAX_TLS_FILE_BYTES,
                )?,
            ));
        let channel = Endpoint::from_shared(self.endpoint.clone())
            .map_err(|_| ProcessError::InvalidConfiguration)?
            .connect_timeout(Duration::from_millis(self.connect_timeout_milliseconds))
            .timeout(Duration::from_millis(self.request_timeout_milliseconds))
            .tls_config(tls)
            .map_err(|_| ProcessError::InvalidConfiguration)?
            .connect()
            .await
            .map_err(|_| ProcessError::EgressUnavailable)?;
        let limits = EgressInternalRpcLimits::new(
            self.maximum_rpc_metadata_bytes,
            self.maximum_rpc_payload_bytes,
        )
        .map_err(|_| ProcessError::InvalidConfiguration)?;
        Ok(public_management_egress(Arc::new(
            EgressBrokerGrpcClient::new(channel, limits),
        )))
    }
}

// The wrapper is the Gateway's public-to-internal request boundary.
fn public_management_egress(
    inner: Arc<dyn ModelManagementEgress>,
) -> Arc<dyn ModelManagementEgress> {
    Arc::new(PublicTraceEgress(inner))
}
struct PublicTraceEgress(Arc<dyn ModelManagementEgress>);
fn public_execution_trace() -> insight_platform_execution_context::ExecutionTraceContext {
    insight_platform_execution_context::ExecutionTraceContext::receive(
        insight_platform_api::trace::current_trace_context().outbound_parent(),
    )
}
#[async_trait]
impl ModelCredentialImporter for PublicTraceEgress {
    async fn import_model_credential(
        &self,
        request: insight_platform_contracts::ModelCredentialImportAuthorizationV1,
        key: insight_platform_contracts::SensitiveModelApiKey,
    ) -> Result<
        insight_platform_contracts::ExactSecretBindingRef,
        insight_platform_contracts::ModelCredentialImportError,
    > {
        insight_platform_execution_context::scope_trace(
            public_execution_trace(),
            self.0.import_model_credential(request, key),
        )
        .await
    }
}
#[async_trait]
impl ModelConnectionProbe for PublicTraceEgress {
    async fn probe_model_connection(
        &self,
        request: insight_platform_contracts::ModelConnectionProbeAuthorizationV1,
    ) -> Result<
        insight_platform_contracts::ModelConnectionObservationV1,
        insight_platform_contracts::ModelConnectionError,
    > {
        insight_platform_execution_context::scope_trace(
            public_execution_trace(),
            self.0.probe_model_connection(request),
        )
        .await
    }
}

#[cfg(test)]
#[path = "model_credentials_trace_tests.rs"]
mod trace_tests;

#[cfg(test)]
pub(super) fn fixture_config() -> GatewayEgressConfig {
    GatewayEgressConfig {
        endpoint: "https://localhost:7443/".to_owned(),
        tls_server_name: "localhost".to_owned(),
        connect_timeout_milliseconds: 1000,
        request_timeout_milliseconds: 30000,
        maximum_rpc_metadata_bytes: 4096,
        maximum_rpc_payload_bytes: 4096,
    }
}
#[cfg(test)]
pub(super) struct UnavailableImporter;
#[cfg(test)]
#[async_trait]
impl ModelCredentialImporter for UnavailableImporter {
    async fn import_model_credential(
        &self,
        _request: insight_platform_contracts::ModelCredentialImportAuthorizationV1,
        _key: insight_platform_contracts::SensitiveModelApiKey,
    ) -> Result<
        insight_platform_contracts::ExactSecretBindingRef,
        insight_platform_contracts::ModelCredentialImportError,
    > {
        Err(insight_platform_contracts::ModelCredentialImportError::TemporarilyUnavailable)
    }
}
#[cfg(test)]
#[async_trait]
impl ModelConnectionProbe for UnavailableImporter {
    async fn probe_model_connection(
        &self,
        _: insight_platform_contracts::ModelConnectionProbeAuthorizationV1,
    ) -> Result<
        insight_platform_contracts::ModelConnectionObservationV1,
        insight_platform_contracts::ModelConnectionError,
    > {
        Err(insight_platform_contracts::ModelConnectionError::Unavailable)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn import_process_target_and_management_routes_are_closed() {
        for path in ["/v1/model-default", "/v1/model-credentials"] {
            assert!(ProcessRole::ManagementApi.permits_path(path));
            assert!(!ProcessRole::RuntimeApi.permits_path(path));
        }
        let config = fixture_config();
        assert!(config.validate().is_ok());
        for endpoint in [
            "http://localhost:7443/",
            "https://localhost/",
            "https://u:p@localhost:7443/",
            "https://localhost:7443/path",
            "https://localhost:7443/?key=secret",
        ] {
            let mut invalid = config.clone();
            invalid.endpoint = endpoint.to_owned();
            assert!(invalid.validate().is_err());
        }
        let mut invalid = config.clone();
        invalid.maximum_rpc_payload_bytes = 4097;
        assert!(invalid.validate().is_err());
        let mut invalid = config;
        invalid.request_timeout_milliseconds = 30001;
        assert!(invalid.validate().is_err());
    }
}
