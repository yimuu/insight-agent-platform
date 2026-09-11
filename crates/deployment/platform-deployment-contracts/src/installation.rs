//! Current installation inputs. These describe physical composition, not business authority.
use insight_platform_contracts::{canonical_digest, parse_strict_json, JsonLimits, Sha256Digest};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeSet,
    error::Error,
    fmt,
    net::SocketAddr,
    path::{Component, Path},
};

pub const INSTALLATION_VERSION: u32 = 1;
pub const INSTALLATION_MAX_BYTES: usize = 262_144;
pub const INSTALLATION_SESSION_SECONDS: u64 = 900;
pub const INSTALLATION_LIMITS: JsonLimits = JsonLimits {
    max_bytes: INSTALLATION_MAX_BYTES,
    max_depth: 16,
    max_properties_per_object: 64,
    max_items_per_array: 256,
    max_string_bytes: insight_platform_contracts::MAX_REMOTE_CONTEXT_INSTALLATION_TRUST_BYTES,
};

/// A process, rather than the coarser release ComponentRole family, owns a private mount.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum InstallationProcess {
    GatewayManagement,
    GatewayRuntime,
    Orchestration,
    RegistryValidation,
    ArtifactGateway,
    ArtifactData,
    ArtifactMaintenance,
    CapabilityNative,
    CapabilityRemote,
    ModelWorker,
    ContextNative,
    ContextRemote,
    ContextDataset,
    ContextSubscription,
    SecurityAuthority,
    EgressBroker,
    McpHost,
    McpResourceHost,
    McpDiscovery,
    McpSubscription,
    McpCleanup,
    CallbackApi,
    Outbox,
    HistoryMaintenance,
}
impl InstallationProcess {
    pub const ALL: &'static [Self] = &[
        Self::GatewayManagement,
        Self::GatewayRuntime,
        Self::Orchestration,
        Self::RegistryValidation,
        Self::ArtifactGateway,
        Self::ArtifactData,
        Self::ArtifactMaintenance,
        Self::CapabilityNative,
        Self::CapabilityRemote,
        Self::ModelWorker,
        Self::ContextNative,
        Self::ContextRemote,
        Self::ContextDataset,
        Self::ContextSubscription,
        Self::SecurityAuthority,
        Self::EgressBroker,
        Self::McpHost,
        Self::McpResourceHost,
        Self::McpDiscovery,
        Self::McpSubscription,
        Self::McpCleanup,
        Self::CallbackApi,
        Self::Outbox,
        Self::HistoryMaintenance,
    ];
    pub const BASE: &'static [Self] = &[
        Self::GatewayManagement,
        Self::GatewayRuntime,
        Self::Orchestration,
        Self::RegistryValidation,
        Self::ArtifactGateway,
        Self::ArtifactData,
        Self::ArtifactMaintenance,
        Self::CapabilityNative,
        Self::ModelWorker,
        Self::ContextNative,
        Self::ContextDataset,
        Self::SecurityAuthority,
        Self::EgressBroker,
        Self::Outbox,
        Self::HistoryMaintenance,
    ];
    pub const fn name(self) -> &'static str {
        match self {
            Self::GatewayManagement => "gateway-management",
            Self::GatewayRuntime => "gateway-runtime",
            Self::Orchestration => "orchestration",
            Self::RegistryValidation => "registry-validation",
            Self::ArtifactGateway => "artifact-gateway",
            Self::ArtifactData => "artifact-data",
            Self::ArtifactMaintenance => "artifact-maintenance",
            Self::CapabilityNative => "capability-native",
            Self::CapabilityRemote => "capability-remote",
            Self::ModelWorker => "model-worker",
            Self::ContextNative => "context-native",
            Self::ContextRemote => "context-remote",
            Self::ContextDataset => "context-dataset",
            Self::ContextSubscription => "context-subscription",
            Self::SecurityAuthority => "security-authority",
            Self::EgressBroker => "egress-broker",
            Self::McpHost => "mcp-host",
            Self::McpResourceHost => "mcp-resource-host",
            Self::McpDiscovery => "mcp-discovery",
            Self::McpSubscription => "mcp-subscription",
            Self::McpCleanup => "mcp-cleanup",
            Self::CallbackApi => "callback-api",
            Self::Outbox => "outbox",
            Self::HistoryMaintenance => "history-maintenance",
        }
    }
    pub const fn binary(self) -> &'static str {
        match self {
            Self::GatewayManagement | Self::GatewayRuntime => "platform-gateway",
            Self::Orchestration => "platform-orchestration-worker",
            Self::RegistryValidation => "platform-registry-validation-worker",
            Self::ArtifactGateway => "platform-artifact-gateway",
            Self::ArtifactData => "platform-artifact-data-worker",
            Self::ArtifactMaintenance => "platform-artifact-maintenance",
            Self::CapabilityNative => "platform-capability-native-worker",
            Self::CapabilityRemote => "platform-capability-remote-worker",
            Self::ModelWorker => "platform-model-worker",
            Self::ContextNative => "platform-context-worker",
            Self::ContextRemote => "platform-remote-context-worker",
            Self::ContextDataset => "platform-context-dataset-worker",
            Self::ContextSubscription => "platform-subscription-context-worker",
            Self::SecurityAuthority => "platform-security-authority",
            Self::EgressBroker => "platform-egress-broker",
            Self::McpHost => "platform-mcp-host",
            Self::McpResourceHost => "platform-mcp-resource-host",
            Self::McpDiscovery => "platform-mcp-discovery-worker",
            Self::McpSubscription => "platform-mcp-subscription-worker",
            Self::McpCleanup => "platform-mcp-cleanup-worker",
            Self::CallbackApi => "platform-callback-api",
            Self::Outbox => "platform-outbox-worker",
            Self::HistoryMaintenance => "platform-history-maintenance",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InstallationTopology {
    Native,
    Compose,
    KubernetesLocal,
}

/// A validated origin has no path, credential, query or fragment component.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ServiceOrigin(String);
impl ServiceOrigin {
    pub fn parse(value: &str) -> Result<Self, InstallationError> {
        let url = url::Url::parse(value).map_err(|_| InstallationError::InvalidEndpoint)?;
        if value.len() > 2048
            || value.trim() != value
            || !matches!(url.scheme(), "http" | "https")
            || url.host_str().is_none()
            || !url.username().is_empty()
            || url.password().is_some()
            || url.path() != "/"
            || url.query().is_some()
            || url.fragment().is_some()
            || value.contains('\\')
            || value.contains('%')
            || value.contains('@')
        {
            return Err(InstallationError::InvalidEndpoint);
        }
        Ok(Self(url.as_str().trim_end_matches('/').to_owned()))
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
    pub fn validate(&self) -> Result<(), InstallationError> {
        if Self::parse(&self.0)? != *self {
            return Err(InstallationError::InvalidEndpoint);
        }
        Ok(())
    }
    pub fn host(&self) -> Result<String, InstallationError> {
        self.validate()?;
        url::Url::parse(&self.0)
            .ok()
            .and_then(|url| url.host_str().map(str::to_owned))
            .ok_or(InstallationError::InvalidEndpoint)
    }
    pub fn is_tls(&self) -> bool {
        self.0.starts_with("https://")
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProcessNetworkV1 {
    pub process: InstallationProcess,
    pub listen_address: Option<SocketAddr>,
    pub observability_address: SocketAddr,
    pub service_origin: Option<ServiceOrigin>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DatabaseEndpointV1 {
    pub host: String,
    pub port: u16,
    pub database: String,
}
impl DatabaseEndpointV1 {
    pub fn validate(&self) -> Result<(), InstallationError> {
        if !dns_name(&self.host) || self.port == 0 || !label(&self.database, 63) {
            return Err(InstallationError::InvalidEndpoint);
        }
        Ok(())
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(tag = "backend", rename_all = "snake_case")]
pub enum ProviderNetworkV1 {
    /// Explicit physical AWS configuration. The installation bootstrap does not provision it.
    Aws {
        artifact: ServiceOrigin,
        kms: ServiceOrigin,
        secrets: ServiceOrigin,
    },
    S3OpenBao {
        artifact: ServiceOrigin,
        openbao: ServiceOrigin,
    },
}
impl ProviderNetworkV1 {
    pub fn validate(&self) -> Result<(), InstallationError> {
        let origins = match self {
            Self::Aws {
                artifact,
                kms,
                secrets,
            } => vec![artifact, kms, secrets],
            Self::S3OpenBao { artifact, openbao } => vec![artifact, openbao],
        };
        for origin in origins {
            origin.validate()?;
            if !origin.is_tls() {
                return Err(InstallationError::InvalidEndpoint);
            }
        }
        Ok(())
    }
    pub fn artifact(&self) -> &ServiceOrigin {
        match self {
            Self::Aws { artifact, .. } | Self::S3OpenBao { artifact, .. } => artifact,
        }
    }
    pub fn openbao(&self) -> Result<&ServiceOrigin, InstallationError> {
        match self {
            Self::S3OpenBao { openbao, .. } => Ok(openbao),
            Self::Aws { .. } => Err(InstallationError::UnsupportedTopology),
        }
    }
    pub fn aws(
        &self,
    ) -> Result<(&ServiceOrigin, &ServiceOrigin, &ServiceOrigin), InstallationError> {
        match self {
            Self::Aws {
                artifact,
                kms,
                secrets,
            } => Ok((artifact, kms, secrets)),
            Self::S3OpenBao { .. } => Err(InstallationError::UnsupportedTopology),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NetworkTopologyV1 {
    pub topology: InstallationTopology,
    pub processes: Vec<ProcessNetworkV1>,
    pub database: DatabaseEndpointV1,
    pub nats_host: String,
    pub nats_port: u16,
    pub console_origin: ServiceOrigin,
    pub providers: ProviderNetworkV1,
}
impl NetworkTopologyV1 {
    pub fn validate(&self) -> Result<(), InstallationError> {
        self.database.validate()?;
        if self.processes.is_empty()
            || self.processes.len() > InstallationProcess::ALL.len()
            || !dns_name(&self.nats_host)
            || self.nats_port == 0
        {
            return Err(InstallationError::InvalidEndpoint);
        }
        let mut seen = BTreeSet::new();
        let mut native_ports = BTreeSet::new();
        for process in &self.processes {
            if !seen.insert(process.process) || process.observability_address.port() == 0 {
                return Err(InstallationError::InvalidRoleClosure);
            }
            if self.topology == InstallationTopology::Native {
                for address in [Some(process.observability_address), process.listen_address]
                    .into_iter()
                    .flatten()
                    .collect::<BTreeSet<_>>()
                {
                    if !address.ip().is_loopback() || !native_ports.insert(address.port()) {
                        return Err(InstallationError::InvalidEndpoint);
                    }
                }
            }
            match (&process.service_origin, process.listen_address) {
                (Some(origin), Some(listen)) if listen.port() != 0 => origin.validate()?,
                (None, None) => (),
                _ => return Err(InstallationError::InvalidEndpoint),
            }
        }
        self.console_origin.validate()?;
        self.providers.validate()?;
        Ok(())
    }
    pub fn process(
        &self,
        process: InstallationProcess,
    ) -> Result<&ProcessNetworkV1, InstallationError> {
        self.processes
            .iter()
            .find(|entry| entry.process == process)
            .ok_or(InstallationError::InvalidRoleClosure)
    }
    pub fn observability(&self, process: InstallationProcess) -> Result<String, InstallationError> {
        Ok(self.process(process)?.observability_address.to_string())
    }
    pub fn listen(&self, process: InstallationProcess) -> Result<String, InstallationError> {
        self.process(process)?
            .listen_address
            .map(|address| address.to_string())
            .ok_or(InstallationError::InvalidEndpoint)
    }
    pub fn origin(
        &self,
        process: InstallationProcess,
    ) -> Result<&ServiceOrigin, InstallationError> {
        self.process(process)?
            .service_origin
            .as_ref()
            .ok_or(InstallationError::InvalidEndpoint)
    }
    pub fn endpoint(&self, process: InstallationProcess) -> Result<String, InstallationError> {
        Ok(format!("{}/", self.origin(process)?.as_str()))
    }
    pub fn tls_server_name(
        &self,
        process: InstallationProcess,
    ) -> Result<String, InstallationError> {
        let origin = self.origin(process)?;
        if !origin.is_tls() {
            return Err(InstallationError::InvalidEndpoint);
        }
        origin.host()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RolePathsV1 {
    pub process: InstallationProcess,
    pub configuration_directory: String,
    pub credential_directory: String,
    pub temporary_directory: String,
}
impl RolePathsV1 {
    pub fn validate(&self) -> Result<(), InstallationError> {
        let paths = [
            &self.configuration_directory,
            &self.credential_directory,
            &self.temporary_directory,
        ];
        for path in paths {
            if !absolute_directory(path) {
                return Err(InstallationError::InvalidPath);
            }
        }
        for (index, left) in paths.iter().enumerate() {
            for right in paths.iter().skip(index + 1) {
                if Path::new(left).starts_with(right) || Path::new(right).starts_with(left) {
                    return Err(InstallationError::InvalidPath);
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CredentialUse {
    Database,
    ReadDatabase,
    WorkDatabase,
    TlsClientKey,
    EgressClientKey,
    OpenBaoClientKey,
    TlsServerKey,
    NatsClientKey,
    Aws,
    CursorKey,
    McpStateKey,
    McpOAuthStateKey,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CredentialReferenceV1 {
    pub process: InstallationProcess,
    pub purpose: CredentialUse,
    pub file_name: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CredentialReferencesV1 {
    pub files: Vec<CredentialReferenceV1>,
}
impl CredentialReferencesV1 {
    pub fn validate(&self, paths: &[RolePathsV1]) -> Result<(), InstallationError> {
        if self.files.len() > 256 {
            return Err(InstallationError::CredentialInvalid);
        }
        let mut uses = BTreeSet::new();
        for file in &self.files {
            if !label(&file.file_name, 128)
                || !uses.insert((file.process, file.purpose))
                || !paths.iter().any(|path| path.process == file.process)
            {
                return Err(InstallationError::CredentialInvalid);
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InstallationAuth {
    LocalTenantAdmin,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LocalSessionKind {
    AgentAuthor,
    TenantAdmin,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LocalSessionIdentityV1 {
    pub schema_version: u32,
    pub issuer: String,
    pub audience: String,
    pub key_id: String,
    pub tenant_id: insight_platform_contracts::ResourceId,
    pub subject: String,
    pub principal_kind: LocalSessionKind,
}
impl LocalSessionIdentityV1 {
    pub fn validate(&self) -> Result<(), InstallationError> {
        let issuer = url::Url::parse(&self.issuer).map_err(|_| InstallationError::InvalidInput)?;
        if self.schema_version != INSTALLATION_VERSION
            || self.issuer.len() > 2048
            || issuer.scheme() != "https"
            || issuer.host_str().is_none()
            || !issuer.username().is_empty()
            || issuer.password().is_some()
            || issuer.query().is_some()
            || issuer.fragment().is_some()
            || self.issuer.contains('@')
            || self.issuer.contains('\\')
            || self.audience != "insight.platform/v1"
            || !label(&self.key_id, 128)
            || self.tenant_id.kind() != insight_platform_contracts::ResourceKind::Tenant
            || self.subject.is_empty()
            || self.subject.len() > 256
            || !self.subject.bytes().all(|b| b.is_ascii_graphic())
        {
            return Err(InstallationError::InvalidInput);
        }
        Ok(())
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstallationModelDestinationV1 {
    pub protocol: insight_platform_contracts::ModelProviderWireProtocol,
    pub endpoint: insight_platform_contracts::CanonicalHttpEndpoint,
    pub region: insight_platform_contracts::DataRegion,
}
impl InstallationModelDestinationV1 {
    pub fn validate(&self) -> Result<(), InstallationError> {
        use insight_platform_contracts::CapabilityEndpointScheme;
        self.endpoint
            .validate()
            .map_err(|_| InstallationError::InvalidEndpoint)?;
        if self.endpoint.scheme != CapabilityEndpointScheme::Https
            || self.endpoint.host == "localhost"
            || self.endpoint.host.ends_with(".localhost")
            || self.endpoint.host.parse::<std::net::IpAddr>().is_ok()
        {
            return Err(InstallationError::InvalidEndpoint);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstallationInputV1 {
    pub schema_version: u32,
    pub name: String,
    pub environment_class: String,
    pub package_digest: Sha256Digest,
    pub authentication: InstallationAuth,
    pub network: NetworkTopologyV1,
    pub paths: Vec<RolePathsV1>,
    pub credentials: CredentialReferencesV1,
    pub model_destinations: Vec<InstallationModelDestinationV1>,
    pub remote_context_destinations:
        Vec<insight_platform_contracts::InstalledRemoteContextDestinationV1>,
}
impl InstallationInputV1 {
    pub fn decode(bytes: &[u8]) -> Result<Self, InstallationError> {
        let value = parse_strict_json(bytes, INSTALLATION_LIMITS)
            .map_err(|_| InstallationError::InvalidInput)?;
        let input: Self =
            serde_json::from_value(value).map_err(|_| InstallationError::InvalidInput)?;
        input.validate()?;
        Ok(input)
    }
    pub fn validate(&self) -> Result<(), InstallationError> {
        if self.schema_version != INSTALLATION_VERSION
            || self.environment_class != "development"
            || !label(&self.name, 63)
            || serde_json::to_vec(self)
                .map_err(|_| InstallationError::InvalidInput)?
                .len()
                > INSTALLATION_MAX_BYTES
        {
            return Err(InstallationError::InvalidInput);
        }
        self.network.validate()?;
        if !matches!(self.network.providers, ProviderNetworkV1::S3OpenBao { .. }) {
            return Err(InstallationError::UnsupportedTopology);
        }
        if self.model_destinations.len()
            > insight_platform_contracts::MAX_MODEL_INSTALLATION_DESTINATIONS
        {
            return Err(InstallationError::InvalidInput);
        }
        let mut model_destinations = BTreeSet::new();
        for destination in &self.model_destinations {
            destination.validate()?;
            if !model_destinations.insert((
                destination.protocol,
                destination
                    .endpoint
                    .canonical_digest()
                    .map_err(|_| InstallationError::InvalidEndpoint)?,
                destination.region.clone(),
            )) {
                return Err(InstallationError::InvalidInput);
            }
        }
        if self.remote_context_destinations.len()
            > insight_platform_contracts::MAX_REMOTE_CONTEXT_INSTALLATION_DESTINATIONS
        {
            return Err(InstallationError::InvalidInput);
        }
        for (index, destination) in self.remote_context_destinations.iter().enumerate() {
            if !destination.validate_shape()
                || destination.endpoint.host == "localhost"
                || destination.endpoint.host.ends_with(".localhost")
                || destination
                    .endpoint
                    .host
                    .parse::<std::net::IpAddr>()
                    .is_ok()
                || self.remote_context_destinations[..index]
                    .iter()
                    .any(|previous| previous.same_selector(destination))
            {
                return Err(InstallationError::InvalidEndpoint);
            }
        }
        let expected: BTreeSet<_> = self
            .network
            .processes
            .iter()
            .map(|entry| entry.process)
            .collect();
        if !InstallationProcess::BASE
            .iter()
            .all(|role| expected.contains(role))
        {
            return Err(InstallationError::InvalidRoleClosure);
        }
        if !self.remote_context_destinations.is_empty()
            && !expected.contains(&InstallationProcess::ContextRemote)
        {
            return Err(InstallationError::InvalidRoleClosure);
        }
        let mut seen = BTreeSet::new();
        for path in &self.paths {
            path.validate()?;
            if !seen.insert(path.process) {
                return Err(InstallationError::InvalidRoleClosure);
            }
        }
        if seen != expected {
            return Err(InstallationError::InvalidRoleClosure);
        }
        self.credentials.validate(&self.paths)
    }
    pub fn digest(&self) -> Result<Sha256Digest, InstallationError> {
        self.validate()?;
        canonical_digest(&serde_json::to_value(self).map_err(|_| InstallationError::InvalidInput)?)
            .map_err(|_| InstallationError::InvalidInput)?
            .parse()
            .map_err(|_| InstallationError::InvalidInput)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InstallationPhase {
    Prepared,
    DependenciesVerified,
    SchemaVerified,
    RolesProvisioned,
    AuthorityBootstrapped,
    Ready,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstallationProgressV1 {
    pub schema_version: u32,
    pub input_digest: Sha256Digest,
    pub identity_digest: Sha256Digest,
    pub phase: InstallationPhase,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum InstallationDatabasePurpose {
    Runtime,
    Outbox,
    History,
    SecurityAuthority,
    Artifact,
}
impl InstallationDatabasePurpose {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Runtime => "runtime",
            Self::Outbox => "outbox",
            Self::History => "history",
            Self::SecurityAuthority => "security-authority",
            Self::Artifact => "artifact",
        }
    }
    pub const fn role_names(self) -> &'static [&'static str] {
        match self {
            Self::Runtime => &["insight_runtime_dev"],
            Self::Outbox => &["insight_outbox_dev"],
            Self::History => &["insight_history_dev"],
            Self::SecurityAuthority => &["insight_security_authority_dev"],
            Self::Artifact => &[
                "insight_artifact_gateway_dev",
                "insight_artifact_data_reader_dev",
                "insight_artifact_data_worker_dev",
                "insight_artifact_maintenance_dev",
            ],
        }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstallationDatabaseRoleEvidenceV1 {
    pub role_name: String,
    pub effective_privileges_digest: Sha256Digest,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstallationDatabaseEvidenceV1 {
    pub schema_version: u32,
    pub input_digest: Sha256Digest,
    pub identity_digest: Sha256Digest,
    pub purpose: InstallationDatabasePurpose,
    pub roles: Vec<InstallationDatabaseRoleEvidenceV1>,
}
impl InstallationDatabaseEvidenceV1 {
    pub fn validate(&self) -> Result<(), InstallationError> {
        let expected = self
            .purpose
            .role_names()
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        let actual = self
            .roles
            .iter()
            .map(|r| r.role_name.as_str())
            .collect::<BTreeSet<_>>();
        if self.schema_version != INSTALLATION_VERSION
            || expected != actual
            || self.roles.len() != expected.len()
        {
            return Err(InstallationError::InvalidRoleClosure);
        }
        Ok(())
    }
}

/// Public installation identity. This contains no signing keys or provider credentials.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstallationIdentityV1 {
    pub schema_version: u32,
    pub installation_id: insight_platform_contracts::ResourceId,
    pub input_digest: Sha256Digest,
    pub session: LocalSessionIdentityV1,
    pub bootstrap: InstallationAdministratorBootstrapV1,
    pub artifact_encryption_domain_id: insight_platform_contracts::ResourceId,
    pub secret_provider_id: insight_platform_contracts::ResourceId,
    pub jwks_digest: Sha256Digest,
    pub certificate_authority_digest: Sha256Digest,
}
impl InstallationIdentityV1 {
    pub fn validate(&self) -> Result<(), InstallationError> {
        use insight_platform_contracts::ResourceKind;
        self.session.validate()?;
        self.bootstrap.validate()?;
        if self.schema_version != INSTALLATION_VERSION
            || self.installation_id.kind() != ResourceKind::InstallationService
            || self.artifact_encryption_domain_id.kind() != ResourceKind::EncryptionDomain
            || self.secret_provider_id.kind() != ResourceKind::SecretProvider
            || self.session.principal_kind != LocalSessionKind::TenantAdmin
            || self.session.tenant_id != self.bootstrap.tenant_id
        {
            return Err(InstallationError::InvalidInput);
        }
        let expected_authority = canonical_digest(&serde_json::json!({"schema_version":1,"tag":"oidc_authentication_authority_v1","value":self.session.issuer})).map_err(|_|InstallationError::InvalidInput)?;
        let expected_subject = canonical_digest(&serde_json::json!({"schema_version":1,"tag":"oidc_subject_v1","value":self.session.subject})).map_err(|_|InstallationError::InvalidInput)?;
        if self
            .bootstrap
            .administrator
            .authentication_authority_digest
            .as_str()
            != expected_authority
            || self.bootstrap.administrator.subject_digest.as_str() != expected_subject
        {
            return Err(InstallationError::IdentityDrift);
        }
        Ok(())
    }
    pub fn digest(&self) -> Result<Sha256Digest, InstallationError> {
        self.validate()?;
        canonical_digest(&serde_json::to_value(self).map_err(|_| InstallationError::InvalidInput)?)
            .map_err(|_| InstallationError::InvalidInput)?
            .parse()
            .map_err(|_| InstallationError::InvalidInput)
    }
}
impl InstallationProgressV1 {
    pub fn validate_for(
        &self,
        input: &InstallationInputV1,
        identity: &InstallationIdentityV1,
    ) -> Result<(), InstallationError> {
        if self.schema_version != INSTALLATION_VERSION
            || self.input_digest != input.digest()?
            || self.identity_digest != identity.digest()?
            || identity.input_digest != self.input_digest
        {
            return Err(InstallationError::IdentityDrift);
        }
        Ok(())
    }
}

/// Explicit new-installation authority input; native development keeps its existing author input.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BootstrapPrincipalV1 {
    pub principal_id: insight_platform_contracts::ResourceId,
    pub authentication_authority_digest: Sha256Digest,
    pub subject_digest: Sha256Digest,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstallationAdministratorBootstrapV1 {
    pub schema_version: u32,
    pub environment_class: String,
    pub installation: BootstrapPrincipalV1,
    pub installation_request_id: insight_platform_contracts::ResourceId,
    pub installation_evidence_digest: Sha256Digest,
    pub tenant_id: insight_platform_contracts::ResourceId,
    pub administrator: BootstrapPrincipalV1,
    pub registry_validator: BootstrapPrincipalV1,
    pub egress_broker: BootstrapPrincipalV1,
}
impl InstallationAdministratorBootstrapV1 {
    pub fn decode(bytes: &[u8]) -> Result<Self, InstallationError> {
        let value = parse_strict_json(bytes, INSTALLATION_LIMITS)
            .map_err(|_| InstallationError::InvalidInput)?;
        let input: Self =
            serde_json::from_value(value).map_err(|_| InstallationError::InvalidInput)?;
        input.validate()?;
        Ok(input)
    }
    pub fn validate(&self) -> Result<(), InstallationError> {
        use insight_platform_contracts::ResourceKind;
        if self.schema_version != INSTALLATION_VERSION
            || self.environment_class != "development"
            || self.tenant_id.kind() != ResourceKind::Tenant
            || self.installation_request_id.kind() != ResourceKind::ServerRequest
        {
            return Err(InstallationError::InvalidInput);
        }
        let mut identities = BTreeSet::new();
        for principal in [
            &self.installation,
            &self.administrator,
            &self.registry_validator,
            &self.egress_broker,
        ] {
            if principal.principal_id.kind() != ResourceKind::Principal
                || !identities.insert(&principal.principal_id)
            {
                return Err(InstallationError::InvalidInput);
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InstallationError {
    InvalidInput,
    InvalidEndpoint,
    InvalidRoleClosure,
    InvalidPath,
    UnsupportedTopology,
    IdentityDrift,
    ConfigurationDrift,
    ForeignState,
    CredentialInvalid,
    PrerequisiteUnavailable,
    SchemaMismatch,
    ExternalOutcomeUnknown,
    Conflict,
    Incomplete,
}

/// The initial read-only PostgreSQL snapshot used once when freezing the model Policy seed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstallationModelPolicyArtifactInputsV1 {
    pub schema_version: u32,
    pub input_digest: Sha256Digest,
    pub identity_digest: Sha256Digest,
    pub retention_policy: insight_platform_contracts::ExactVersionRef,
    pub encryption_domain_id: insight_platform_contracts::ResourceId,
    pub storage_binding_digest: Sha256Digest,
}
impl InstallationModelPolicyArtifactInputsV1 {
    pub fn decode(bytes: &[u8]) -> Result<Self, InstallationError> {
        let value = parse_strict_json(bytes, INSTALLATION_LIMITS)
            .map_err(|_| InstallationError::InvalidInput)?;
        let value: Self =
            serde_json::from_value(value).map_err(|_| InstallationError::InvalidInput)?;
        value.validate()?;
        Ok(value)
    }
    pub fn validate(&self) -> Result<(), InstallationError> {
        use insight_platform_contracts::ResourceKind;
        if self.schema_version != INSTALLATION_VERSION
            || self.retention_policy.resource_kind != ResourceKind::PolicyRevision
            || self.retention_policy.validate().is_err()
            || self.encryption_domain_id.kind() != ResourceKind::EncryptionDomain
        {
            return Err(InstallationError::InvalidInput);
        }
        Ok(())
    }
    pub fn validate_for(
        &self,
        input: &InstallationInputV1,
        identity: &InstallationIdentityV1,
    ) -> Result<(), InstallationError> {
        self.validate()?;
        if self.input_digest != input.digest()?
            || self.identity_digest != identity.digest()?
            || self.encryption_domain_id != identity.artifact_encryption_domain_id
        {
            return Err(InstallationError::IdentityDrift);
        }
        Ok(())
    }
}
impl fmt::Display for InstallationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "installation {:?}", self)
    }
}
impl Error for InstallationError {}
fn label(value: &str, max: usize) -> bool {
    !value.is_empty()
        && value.len() <= max
        && value != "."
        && value != ".."
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
}
fn dns_name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 253
        && value.split('.').all(|part| {
            !part.is_empty()
                && part.len() <= 63
                && !part.starts_with('-')
                && !part.ends_with('-')
                && part
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        })
}
fn absolute_directory(value: &str) -> bool {
    value.len() <= 1024
        && value.starts_with('/')
        && value != "/"
        && !value.ends_with('/')
        && !value.contains("//")
        && !value.as_bytes().contains(&0)
        && Path::new(value)
            .components()
            .all(|part| matches!(part, Component::RootDir | Component::Normal(_)))
        && !value.split('/').any(|part| matches!(part, "." | ".."))
}

/// Non-secret evidence of the exact files delivered to one serving process.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RenderedCredentialFileV1 {
    pub file_name: String,
    pub bytes_digest: Sha256Digest,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RenderedInstallationProcessV1 {
    pub process: InstallationProcess,
    pub executable_digest: Sha256Digest,
    pub configuration_file: String,
    pub configuration_digest: Sha256Digest,
    pub environment_bytes_digest: Sha256Digest,
    pub credential_files: Vec<RenderedCredentialFileV1>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RenderedInstallationV1 {
    pub schema_version: u32,
    pub input_digest: Sha256Digest,
    pub identity_digest: Sha256Digest,
    pub package_digest: Sha256Digest,
    pub processes: Vec<RenderedInstallationProcessV1>,
}
impl RenderedInstallationV1 {
    pub fn validate_for(
        &self,
        input: &InstallationInputV1,
        identity: &InstallationIdentityV1,
    ) -> Result<(), InstallationError> {
        input.validate()?;
        identity.validate()?;
        let expected = input
            .network
            .processes
            .iter()
            .map(|entry| entry.process)
            .collect::<BTreeSet<_>>();
        if self.schema_version != INSTALLATION_VERSION
            || self.input_digest != input.digest()?
            || self.identity_digest != identity.digest()?
            || self.package_digest != input.package_digest
            || self.processes.len() != expected.len()
            || self
                .processes
                .iter()
                .map(|entry| entry.process)
                .collect::<BTreeSet<_>>()
                != expected
        {
            return Err(InstallationError::InvalidRoleClosure);
        }
        for entry in &self.processes {
            if !label(&entry.configuration_file, 128) || entry.credential_files.len() > 32 {
                return Err(InstallationError::InvalidInput);
            }
            let mut names = BTreeSet::new();
            for file in &entry.credential_files {
                if !label(&file.file_name, 128) || !names.insert(&file.file_name) {
                    return Err(InstallationError::CredentialInvalid);
                }
            }
        }
        Ok(())
    }
}

/// Private session handoff metadata. The signed token remains in its separate bounded file.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstallationSessionDeliveryV1 {
    pub schema_version: u32,
    pub input_digest: Sha256Digest,
    pub identity_digest: Sha256Digest,
    pub session_file: String,
    pub tenant_id: insight_platform_contracts::ResourceId,
    pub endpoint: ServiceOrigin,
    pub expires_at_unix_seconds: u64,
}
impl InstallationSessionDeliveryV1 {
    pub fn validate_for(
        &self,
        input: &InstallationInputV1,
        identity: &InstallationIdentityV1,
        now: u64,
    ) -> Result<(), InstallationError> {
        input.validate()?;
        identity.validate()?;
        if self.schema_version != INSTALLATION_VERSION
            || self.input_digest != input.digest()?
            || self.identity_digest != identity.digest()?
            || self.tenant_id != identity.session.tenant_id
            || self.endpoint != input.network.console_origin
            || !absolute_directory(&self.session_file)
            || !self.session_file.ends_with("/session-token")
            || self.expires_at_unix_seconds <= now
            || self.expires_at_unix_seconds.saturating_sub(now) > INSTALLATION_SESSION_SECONDS
        {
            return Err(InstallationError::InvalidInput);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn origins_and_paths_reject_credential_and_traversal_input() {
        for value in [
            "https://u:p@service",
            "https://service/path",
            "https://service?x=1",
            "https://service#f",
            "ftp://service",
            "https://service/%2e",
        ] {
            assert!(ServiceOrigin::parse(value).is_err(), "{value}");
        }
        assert_eq!(
            ServiceOrigin::parse("https://artifact-gateway:8443/")
                .unwrap()
                .as_str(),
            "https://artifact-gateway:8443"
        );
        for value in [
            "relative",
            "/",
            "/etc/../state",
            "/etc/./state",
            "/etc//state",
        ] {
            assert!(!absolute_directory(value));
        }
    }
    #[test]
    fn physical_worker_identities_match_existing_owner_catalog() {
        let names = InstallationProcess::ALL
            .iter()
            .map(|role| role.name())
            .collect::<BTreeSet<_>>();
        assert_eq!(names.len(), InstallationProcess::ALL.len());
        for role in InstallationProcess::ALL {
            if let Some(worker) = crate::workers::WORKER_EXECUTABLES
                .iter()
                .find(|worker| worker.binary == role.binary())
            {
                assert!(!worker.worker_role.is_empty());
            }
            assert_eq!(
                serde_json::from_str::<InstallationProcess>(&format!("\"{}\"", role.name()))
                    .unwrap(),
                *role
            );
        }
    }
    #[test]
    fn model_destination_prefix_is_distinct_from_closed_protocol_request_path() {
        let source = serde_json::json!({"protocol":"open_ai_responses","endpoint":{"scheme":"https","host":"dashscope.aliyuncs.com","port":443,"base_path":"/compatible-mode"},"region":"cn-beijing"});
        let destination: InstallationModelDestinationV1 =
            serde_json::from_value(source.clone()).unwrap();
        destination.validate().unwrap();
        for (field, value) in [
            ("scheme", "http"),
            ("host", "localhost"),
            ("host", "127.0.0.1"),
            ("base_path", "/../v1"),
            ("base_path", "/v1?token=private"),
        ] {
            let mut invalid = source.clone();
            invalid["endpoint"][field] = serde_json::json!(value);
            assert!(
                serde_json::from_value::<InstallationModelDestinationV1>(invalid)
                    .map(|input| input.validate().is_err())
                    .unwrap_or(true)
            );
        }
    }
}
