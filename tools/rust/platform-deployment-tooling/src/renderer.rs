//! Deterministic per-process rendering. Physical observation and persistence belong to the installer.
use crate::{
    base_profile, full_profile, openbao_profile,
    process_environment::{process_launch, ProcessDatabaseUrls, ProcessEnvironmentInputs},
    provider_config, role_material, tls,
    worker_profile::WorkerBuilds,
};
use insight_platform_contracts::{
    canonical_digest, ModelInstallationCatalogV1, ResourceId, Sha256Digest,
};
use insight_platform_deployment_contracts::{
    development::DevelopmentArtifactAuthorityConfigV1,
    installation::*,
    installation_provider::{InstallationProviderReadyV1, OpenBaoInstallationRole},
};
use serde_json::Value;
use sha2::{Digest as _, Sha256};
use std::{collections::BTreeMap, path::Path};

pub struct InstallationRenderInputs<'a> {
    pub input: &'a InstallationInputV1,
    pub identity: &'a InstallationIdentityV1,
    pub builds: &'a WorkerBuilds,
    pub binary_directory: &'a Path,
    pub jwks: &'a Value,
    pub provider_ready: &'a InstallationProviderReadyV1,
    pub artifact_bucket: &'a str,
    pub artifact_bootstrap: &'a DevelopmentArtifactAuthorityConfigV1,
    pub model_installation: Option<&'a ModelInstallationCatalogV1>,
    pub capability_protocol_profile: Option<&'a ResourceId>,
    /// Exact private prepared files; no automatic credential generation is permitted here.
    pub private_files: &'a BTreeMap<String, Vec<u8>>,
}
/// Secret-bearing output has no Debug or Serialize implementation.
pub struct RenderedProcessFiles {
    pub configuration_file: String,
    pub configuration: Vec<u8>,
    pub environment: Vec<u8>,
    pub credentials: BTreeMap<String, Vec<u8>>,
}
pub struct RenderedInstallationFiles {
    pub evidence: RenderedInstallationV1,
    pub roles: BTreeMap<InstallationProcess, RenderedProcessFiles>,
    pub console: Vec<u8>,
}
fn bad() -> InstallationError {
    InstallationError::InvalidInput
}
fn digest(value: &Value) -> Result<Sha256Digest, InstallationError> {
    canonical_digest(value)
        .map_err(|_| bad())?
        .parse()
        .map_err(|_| bad())
}
fn bytes_digest(bytes: &[u8]) -> Sha256Digest {
    format!("sha256:{}", crate::lower_hex(&Sha256::digest(bytes)))
        .parse()
        .expect("actual SHA256")
}
fn encoded(value: &Value) -> Result<Vec<u8>, InstallationError> {
    let bytes = serde_json::to_vec_pretty(value).map_err(|_| bad())?;
    if bytes.len() > INSTALLATION_MAX_BYTES {
        return Err(bad());
    }
    Ok(bytes)
}
fn private<'a>(
    files: &'a BTreeMap<String, Vec<u8>>,
    name: &str,
) -> Result<&'a [u8], InstallationError> {
    files
        .get(name)
        .map(Vec::as_slice)
        .ok_or(InstallationError::CredentialInvalid)
}
fn database(
    input: &InstallationInputV1,
    files: &BTreeMap<String, Vec<u8>>,
    name: &str,
) -> Result<String, InstallationError> {
    let value = private(files, name)?;
    if value.len() != 32 || !value.iter().all(u8::is_ascii_hexdigit) {
        return Err(InstallationError::CredentialInvalid);
    }
    let role = match name {
        "runtime-password" => "insight_runtime_dev",
        "outbox-password" => "insight_outbox_dev",
        "history-password" => "insight_history_dev",
        "security-authority-password" => "insight_security_authority_dev",
        "artifact-gateway-password" => "insight_artifact_gateway_dev",
        "artifact-data-reader-password" => "insight_artifact_data_reader_dev",
        "artifact-data-worker-password" => "insight_artifact_data_worker_dev",
        "artifact-maintenance-password" => "insight_artifact_maintenance_dev",
        _ => return Err(InstallationError::CredentialInvalid),
    };
    let password = std::str::from_utf8(value).map_err(|_| InstallationError::CredentialInvalid)?;
    Ok(format!(
        "postgresql://{role}:{password}@{}:{}/{}",
        input.network.database.host, input.network.database.port, input.network.database.database
    ))
}
fn environment_file(launch: &full_profile::ProcessLaunch) -> Result<Vec<u8>, InstallationError> {
    let mut variables = BTreeMap::new();
    for (key, value) in launch
        .environment
        .iter()
        .map(|(k, v)| (*k, v.as_str()))
        .chain(
            launch
                .extra_environment
                .iter()
                .map(|(k, v)| (k.as_str(), v.as_str())),
        )
    {
        if !key.starts_with("PLATFORM_")
            && !matches!(
                key,
                "AWS_ACCESS_KEY_ID"
                    | "AWS_SECRET_ACCESS_KEY"
                    | "AWS_EC2_METADATA_DISABLED"
                    | "AWS_SHARED_CREDENTIALS_FILE"
                    | "AWS_PROFILE"
                    | "SSL_CERT_FILE"
                    | "SSL_CERT_DIR"
            )
        {
            return Err(InstallationError::InvalidInput);
        }
        if value.contains('\0') || value.len() > 16384 || variables.insert(key, value).is_some() {
            return Err(InstallationError::InvalidInput);
        }
    }
    let text = variables
        .into_iter()
        .map(|(key, value)| format!("{key}='{}'\n", value.replace('\'', "'\\''")))
        .collect::<String>();
    if text.len() > INSTALLATION_MAX_BYTES {
        return Err(InstallationError::InvalidInput);
    }
    Ok(text.into_bytes())
}
pub fn render_installation(
    inputs: InstallationRenderInputs<'_>,
) -> Result<RenderedInstallationFiles, InstallationError> {
    let InstallationRenderInputs {
        input,
        identity,
        builds,
        binary_directory,
        jwks,
        provider_ready,
        artifact_bucket,
        artifact_bootstrap,
        model_installation,
        capability_protocol_profile,
        private_files,
    } = inputs;
    input.validate()?;
    crate::installation::validate_remote_context_destinations(&input.remote_context_destinations)?;
    identity.validate()?;
    role_material::validate_credentials(&input.network, &input.credentials)?;
    if identity.input_digest != input.digest()? || digest(jwks)? != identity.jwks_digest {
        return Err(InstallationError::IdentityDrift);
    }
    if model_installation.is_some_and(|catalog| {
        !catalog.validate() || catalog.secret_provider_id != identity.secret_provider_id
    }) {
        return Err(bad());
    }
    provider_ready.validate_for(input)?;
    let client_for = |process, role| {
        let paths = input
            .paths
            .iter()
            .find(|paths| paths.process == process)
            .ok_or(InstallationError::InvalidRoleClosure)?;
        openbao_profile::role_client(provider_ready, role, Path::new(&paths.credential_directory))
    };
    let artifact_catalog_for = |process, role| {
        provider_config::openbao_artifact_provider_catalog(
            input.network.providers.artifact(),
            artifact_bucket,
            &client_for(process, role)?,
            &provider_ready.artifact_key,
        )
    };
    let artifact_catalog = artifact_catalog_for(
        InstallationProcess::ArtifactGateway,
        OpenBaoInstallationRole::ArtifactGateway,
    )?;
    let secret_catalog = provider_config::openbao_secret_provider_catalog(
        provider_ready,
        &client_for(
            InstallationProcess::EgressBroker,
            OpenBaoInstallationRole::EgressBroker,
        )?,
        &identity.secret_provider_id,
    )?;
    artifact_bootstrap.validate().map_err(|_| bad())?;
    if artifact_catalog
        .get("write_storage_binding_digest")
        .and_then(Value::as_str)
        != Some(
            artifact_bootstrap
                .artifact_io_policy
                .write_storage_binding_digest
                .as_str(),
        )
        || artifact_bootstrap.artifact_io_policy.encryption_domain_id
            != identity.artifact_encryption_domain_id
    {
        return Err(InstallationError::ConfigurationDrift);
    }
    let oidc = serde_json::json!({"issuer":identity.session.issuer,"audience":identity.session.audience,"jwks_digest":identity.jwks_digest,"jwks":jwks});
    let mut configurations = base_profile::configurations(base_profile::BaseConfigInputs {
        network: &input.network,
        worker_builds: builds,
        identity: base_profile::BaseIdentity {
            encryption_domain_id: &identity.artifact_encryption_domain_id,
            registry_validator_principal_id: &identity.bootstrap.registry_validator.principal_id,
        },
        oidc: &oidc,
        artifact_provider_catalog: &artifact_catalog,
        artifact_bootstrap,
        model_installation,
    })
    .map_err(|_| bad())?;
    let context_adapter =
        base_profile::local_digest("context-native-adapter").map_err(|_| bad())?;
    let context_contract =
        base_profile::local_digest("context-native-contract").map_err(|_| bad())?;
    let egress_paths = input
        .paths
        .iter()
        .find(|paths| paths.process == InstallationProcess::EgressBroker)
        .ok_or(InstallationError::InvalidRoleClosure)?;
    let egress_credentials = Path::new(&egress_paths.credential_directory);
    let mcp_key_path = egress_credentials.join("mcp-state-key");
    let mcp_key_digest = bytes_digest(private(private_files, "mcp-state-key")?);
    let callback_paths = input
        .paths
        .iter()
        .find(|paths| paths.process == InstallationProcess::CallbackApi)
        .unwrap_or(egress_paths);
    let callback_credentials = Path::new(&callback_paths.credential_directory);
    let oauth_key_path = callback_credentials.join("mcp-oauth-state-key");
    let oauth_key_digest = bytes_digest(private(private_files, "mcp-oauth-state-key")?);
    let egress_principal_id = identity.bootstrap.egress_broker.principal_id.to_string();
    configurations.extend(full_profile::initial_configs(
        builds,
        &input.network,
        &artifact_catalog,
        capability_protocol_profile,
        full_profile::WorkerDigests {
            context_adapter: &context_adapter,
            context_contract: &context_contract,
        },
        full_profile::EgressConfigInputs {
            model_installation,
            remote_context_destinations: &input.remote_context_destinations,
            service_principal_id: egress_principal_id.as_str(),
            secret_provider_catalog: &secret_catalog,
            mcp_state_key_root: egress_credentials,
            mcp_state_key_path: &mcp_key_path,
            mcp_state_key_reference_digest: mcp_key_digest.as_str(),
            mcp_oauth_state_key_root: callback_credentials,
            mcp_oauth_state_key_path: &oauth_key_path,
            mcp_oauth_state_key_reference_digest: oauth_key_digest.as_str(),
        },
    )?);
    let mut roles = BTreeMap::new();
    let mut records = Vec::new();
    for entry in &input.network.processes {
        let process = entry.process;
        let paths = input
            .paths
            .iter()
            .find(|paths| paths.process == process)
            .ok_or(InstallationError::InvalidRoleClosure)?;
        let (configuration_file, mut configuration) = configurations
            .remove(process.name())
            .ok_or(InstallationError::InvalidRoleClosure)?;
        if let Some(
            role @ (OpenBaoInstallationRole::ArtifactGateway
            | OpenBaoInstallationRole::ArtifactData
            | OpenBaoInstallationRole::ArtifactMaintenance),
        ) = role_material::openbao_role(&input.network, process)
        {
            let catalog = configuration
                .as_object_mut()
                .and_then(|value| value.get_mut("artifact_provider_catalog"))
                .ok_or(InstallationError::InvalidRoleClosure)?;
            *catalog = artifact_catalog_for(process, role)?;
        }
        let configuration_digest = digest(&configuration)?;
        let executable_digest = builds
            .executable(process.binary())
            .ok_or(InstallationError::InvalidRoleClosure)?
            .clone();
        let mut credentials = BTreeMap::new();
        let mut primary = String::new();
        let mut read = None;
        let mut work = None;
        for reference in input
            .credentials
            .files
            .iter()
            .filter(|reference| reference.process == process)
        {
            credentials.insert(
                reference.file_name.clone(),
                private(private_files, &reference.file_name)?.to_vec(),
            );
            match reference.purpose {
                CredentialUse::Database => {
                    primary = database(input, private_files, &reference.file_name)?
                }
                CredentialUse::ReadDatabase => {
                    read = Some(database(input, private_files, &reference.file_name)?)
                }
                CredentialUse::WorkDatabase => {
                    work = Some(database(input, private_files, &reference.file_name)?)
                }
                _ => (),
            }
        }
        for (_, tls_identity) in role_material::tls_identities(process) {
            credentials.insert(
                tls_identity.certificate.into(),
                private(private_files, tls_identity.certificate)?.to_vec(),
            );
            credentials.insert(
                tls::RUNTIME_CA_CERTIFICATE_FILE.into(),
                private(private_files, tls::RUNTIME_CA_CERTIFICATE_FILE)?.to_vec(),
            );
        }
        if role_material::requires_aws_ca(&input.network, process) {
            credentials.insert(
                tls::RUNTIME_CA_CERTIFICATE_FILE.into(),
                private(private_files, tls::RUNTIME_CA_CERTIFICATE_FILE)?.to_vec(),
            );
        }
        if let Some(role) = role_material::openbao_role(&input.network, process) {
            let certificate = crate::openbao_profile::certificate_file(role);
            credentials.insert(
                certificate.clone(),
                private(private_files, &certificate)?.to_vec(),
            );
        }
        let cursor_path = Path::new(&paths.credential_directory).join("cursor-key");
        let cursor_digest = bytes_digest(private(private_files, "cursor-key")?);
        let aws_credentials_path = input
            .credentials
            .files
            .iter()
            .find(|file| file.process == process && file.purpose == CredentialUse::Aws)
            .map(|file| Path::new(&paths.credential_directory).join(&file.file_name));
        let launch = process_launch(ProcessEnvironmentInputs {
            process,
            paths: full_profile::ProcessPaths {
                release: binary_directory,
                configuration: Path::new(&paths.configuration_directory),
                tls: Path::new(&paths.credential_directory),
                ca_certificate_file: tls::RUNTIME_CA_CERTIFICATE_FILE,
                nats_client_certificate_file: tls::RUNTIME_NATS_CLIENT_CERTIFICATE_FILE,
                nats_client_private_key_file: tls::RUNTIME_NATS_CLIENT_PRIVATE_KEY_FILE,
            },
            network: &input.network,
            configuration_digest: configuration_digest.as_str(),
            database: ProcessDatabaseUrls {
                primary: &primary,
                read: read.as_deref(),
                work: work.as_deref(),
            },
            cursor_key_path: Some(&cursor_path),
            cursor_key_digest: Some(cursor_digest.as_str()),
            aws_credentials_path: aws_credentials_path.as_deref(),
        })?;
        let environment = environment_file(&launch)?;
        records.push(RenderedInstallationProcessV1 {
            process,
            executable_digest,
            configuration_file: configuration_file.into(),
            configuration_digest,
            environment_bytes_digest: bytes_digest(&environment),
            credential_files: credentials
                .iter()
                .map(|(name, bytes)| RenderedCredentialFileV1 {
                    file_name: name.clone(),
                    bytes_digest: bytes_digest(bytes),
                })
                .collect(),
        });
        roles.insert(
            process,
            RenderedProcessFiles {
                configuration_file: configuration_file.into(),
                configuration: encoded(&configuration)?,
                environment,
                credentials,
            },
        );
    }
    let evidence = RenderedInstallationV1 {
        schema_version: INSTALLATION_VERSION,
        input_digest: input.digest()?,
        identity_digest: identity.digest()?,
        package_digest: input.package_digest.clone(),
        processes: records,
    };
    evidence.validate_for(input, identity)?;
    let console_port = if input.network.topology == InstallationTopology::Native {
        input
            .network
            .console_origin
            .as_str()
            .rsplit(':')
            .next()
            .ok_or(InstallationError::InvalidEndpoint)?
            .parse::<u16>()
            .map_err(|_| InstallationError::InvalidEndpoint)?
    } else {
        8080
    };
    let console = encoded(
        &serde_json::json!({"schema_version":1,"topology":match input.network.topology{InstallationTopology::Native=>"native",InstallationTopology::Compose=>"compose",InstallationTopology::KubernetesLocal=>"kubernetes_local"},"listen_host":if input.network.topology==InstallationTopology::Native{"127.0.0.1"}else{"0.0.0.0"},"listen_port":console_port,"runtime_origin":input.network.origin(InstallationProcess::GatewayRuntime)?,"management_origin":input.network.origin(InstallationProcess::GatewayManagement)?,"max_request_bytes":1048576,"max_buffered_request_bytes":8388608,"request_timeout_ms":15000,"upstream_header_timeout_ms":30000,"idle_timeout_ms":60000,"max_connections":128,"max_header_bytes":32768}),
    )?;
    Ok(RenderedInstallationFiles {
        evidence,
        roles,
        console,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        installation::{compose_input, PreparedInstallation},
        provider_config,
    };
    #[test]
    fn base_compose_render_is_complete_deterministic_and_role_private() {
        let temp = tempfile::tempdir().unwrap();
        let root = std::fs::canonicalize(temp.path()).unwrap();
        let input = compose_input(
            "renderer-test",
            format!("sha256:{}", "a".repeat(64)).parse().unwrap(),
        )
        .unwrap();
        let prepared = PreparedInstallation::prepare(&input, &root.join("private")).unwrap();
        let binaries = crate::worker_profile::fixture_binaries(&root);
        let processes = input
            .network
            .processes
            .iter()
            .map(|entry| entry.process)
            .collect::<Vec<_>>();
        let builds = WorkerBuilds::read_processes(&binaries, &processes).unwrap();
        let ready = crate::openbao_profile::fixture_ready(
            &input,
            &root.join("private"),
            format!("sha256:{}", "a".repeat(64)).parse().unwrap(),
        );
        let bucket = format!(
            "insight-platform-artifacts-{}",
            prepared.identity().installation_id.uuid().simple()
        );
        let artifact = provider_config::openbao_artifact_provider_catalog(
            input.network.providers.artifact(),
            &bucket,
            &ready.client,
            &ready.artifact_key,
        )
        .unwrap();
        let (bootstrap, _) = prepared
            .prepare_artifact_authority(
                artifact["write_storage_binding_digest"]
                    .as_str()
                    .unwrap()
                    .parse()
                    .unwrap(),
                true,
            )
            .unwrap();
        let files = prepared.renderer_private_files().unwrap();
        let jwks = prepared.jwks().unwrap();
        let render = |files| {
            render_installation(InstallationRenderInputs {
                input: &input,
                identity: prepared.identity(),
                builds: &builds,
                binary_directory: &binaries,
                jwks: &jwks,
                provider_ready: &ready,
                artifact_bucket: &bucket,
                artifact_bootstrap: &bootstrap,
                model_installation: None,
                capability_protocol_profile: None,
                private_files: files,
            })
        };
        let first = render(&files).unwrap();
        let second = render(&files).unwrap();
        assert_eq!(first.evidence, second.evidence);
        assert_eq!(first.roles.len(), InstallationProcess::BASE.len());
        for (process, output) in &first.roles {
            assert_eq!(output.environment, second.roles[process].environment);
            assert!(!output.credentials.contains_key("postgres-admin-password"));
            assert!(!output.credentials.contains_key("ca-key.pem"));
            assert!(!output
                .credentials
                .contains_key("outbox-provision-client-key.pem"));
            assert!(!output.credentials.contains_key("nats-server-key.pem"));
            let env = std::str::from_utf8(&output.environment).unwrap();
            assert!(!env.contains("insight_installation_admin"));
            let config: Value = serde_json::from_slice(&output.configuration).unwrap();
            if let Some(manifest) = config.get("worker_manifest") {
                assert_eq!(
                    manifest["worker_build_digest"].as_str(),
                    Some(builds.executable(process.binary()).unwrap().as_str())
                );
            }
        }
        for (process, role) in [
            (
                InstallationProcess::ArtifactGateway,
                OpenBaoInstallationRole::ArtifactGateway,
            ),
            (
                InstallationProcess::ArtifactData,
                OpenBaoInstallationRole::ArtifactData,
            ),
            (
                InstallationProcess::ArtifactMaintenance,
                OpenBaoInstallationRole::ArtifactMaintenance,
            ),
        ] {
            let output = &first.roles[&process];
            let config: Value = serde_json::from_slice(&output.configuration).unwrap();
            let catalog = &config["artifact_provider_catalog"];
            assert_eq!(
                catalog["write_storage_binding_digest"],
                artifact["write_storage_binding_digest"]
            );
            let client = &catalog["reference_key_bindings"][0]["config"]["client"];
            assert_eq!(client["auth_role"], role.name());
            let paths = input
                .paths
                .iter()
                .find(|entry| entry.process == process)
                .unwrap();
            assert_eq!(
                client["client_private_key_file"],
                Path::new(&paths.credential_directory)
                    .join(openbao_profile::private_key_file(role))
                    .display()
                    .to_string()
            );
            assert!(!output
                .credentials
                .contains_key(&openbao_profile::private_key_file(
                    OpenBaoInstallationRole::Initializer
                )));
            for other in OpenBaoInstallationRole::ALL
                .iter()
                .filter(|other| **other != role)
            {
                assert!(!output
                    .credentials
                    .contains_key(&openbao_profile::private_key_file(*other)));
            }
        }
        let gateway = &first.roles[&InstallationProcess::GatewayManagement];
        let config: Value = serde_json::from_slice(&gateway.configuration).unwrap();
        assert_eq!(
            config["model_credential_egress"]["endpoint"],
            "https://egress-broker:8446/"
        );
        assert!(config["model_credential_egress"]
            .get("schema_version")
            .is_none());
        assert!(config["model_installation"].is_null());
        assert!(gateway
            .credentials
            .contains_key(full_profile::GATEWAY_EGRESS_CLIENT_PRIVATE_KEY_FILE));
        assert!(!first.roles[&InstallationProcess::GatewayRuntime]
            .credentials
            .contains_key(full_profile::GATEWAY_EGRESS_CLIENT_PRIVATE_KEY_FILE));
        let data =
            std::str::from_utf8(&first.roles[&InstallationProcess::ArtifactData].environment)
                .unwrap();
        assert!(data.contains("insight_artifact_data_reader_dev:"));
        assert!(data.contains("insight_artifact_data_worker_dev:"));
        assert!(!data.contains("insight_runtime_dev:"));
        let mut missing = files.clone();
        missing.remove(full_profile::GATEWAY_EGRESS_CLIENT_PRIVATE_KEY_FILE);
        assert!(matches!(
            render(&missing),
            Err(InstallationError::CredentialInvalid)
        ));
        use x509_parser::prelude::FromDer as _;
        let cert = &first.roles[&InstallationProcess::EgressBroker].credentials
            [full_profile::EGRESS_BROKER_CERTIFICATE_FILE];
        let (_, pem) = x509_parser::pem::parse_x509_pem(cert).unwrap();
        let (_, certificate) =
            x509_parser::certificate::X509Certificate::from_der(&pem.contents).unwrap();
        assert!(certificate
            .subject_alternative_name()
            .unwrap()
            .unwrap()
            .value
            .general_names
            .iter()
            .any(|name| matches!(
                name,
                x509_parser::extensions::GeneralName::DNSName("egress-broker")
            )));
        let ca = private(&files, "ca.pem").unwrap();
        let (_, pem) = x509_parser::pem::parse_x509_pem(ca).unwrap();
        let (_, ca) = x509_parser::certificate::X509Certificate::from_der(&pem.contents).unwrap();
        certificate.verify_signature(Some(ca.public_key())).unwrap();
    }
}
