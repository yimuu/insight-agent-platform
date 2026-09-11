//! Closed per-process credential scopes. Deployment-only keys never appear in this registry.
use crate::{full_profile::*, tls::*, DevProfile};
use insight_platform_deployment_contracts::installation::{
    CredentialReferenceV1, CredentialReferencesV1, CredentialUse as Use, InstallationError,
    InstallationProcess as Process, NetworkTopologyV1, ProviderNetworkV1,
};
use insight_platform_deployment_contracts::installation_provider::OpenBaoInstallationRole;
use std::collections::BTreeMap;

pub const LOCALSTACK_CERTIFICATE_FILE: &str = "localstack-server.pem";
pub const LOCALSTACK_PRIVATE_KEY_FILE: &str = "localstack-server-key.pem";
pub fn requires_aws_ca(network: &NetworkTopologyV1, process: Process) -> bool {
    (network.topology == insight_platform_deployment_contracts::installation::InstallationTopology::KubernetesLocal
        || matches!(network.providers, ProviderNetworkV1::S3OpenBao { .. }))
        && matches!(process, Process::ArtifactGateway | Process::ArtifactData | Process::ArtifactMaintenance | Process::EgressBroker)
}

pub fn openbao_role(
    network: &NetworkTopologyV1,
    process: Process,
) -> Option<OpenBaoInstallationRole> {
    if !matches!(network.providers, ProviderNetworkV1::S3OpenBao { .. }) {
        return None;
    }
    match process {
        Process::ArtifactGateway => Some(OpenBaoInstallationRole::ArtifactGateway),
        Process::ArtifactData => Some(OpenBaoInstallationRole::ArtifactData),
        Process::ArtifactMaintenance => Some(OpenBaoInstallationRole::ArtifactMaintenance),
        Process::EgressBroker => Some(OpenBaoInstallationRole::EgressBroker),
        _ => None,
    }
}

pub fn tls_identities(process: Process) -> Vec<(Use, LocalTlsIdentitySpec)> {
    let all = native_identity_specs(
        DevProfile::parse(Some("model,context,remote-capability,mcp"), false, false)
            .expect("closed profile"),
    );
    let certificates: Vec<(Use, &str)> = match process {
        Process::GatewayManagement => vec![
            (Use::TlsClientKey, RUNTIME_GATEWAY_CLIENT_CERTIFICATE_FILE),
            (Use::EgressClientKey, GATEWAY_EGRESS_CLIENT_CERTIFICATE_FILE),
        ],
        Process::GatewayRuntime => {
            vec![(Use::TlsClientKey, RUNTIME_GATEWAY_CLIENT_CERTIFICATE_FILE)]
        }
        Process::Orchestration => vec![(
            Use::TlsClientKey,
            RUNTIME_ORCHESTRATION_CLIENT_CERTIFICATE_FILE,
        )],
        Process::RegistryValidation => vec![(
            Use::TlsClientKey,
            RUNTIME_REGISTRY_VALIDATION_CLIENT_CERTIFICATE_FILE,
        )],
        Process::ArtifactGateway => {
            vec![(Use::TlsServerKey, RUNTIME_ARTIFACT_GATEWAY_CERTIFICATE_FILE)]
        }
        Process::ArtifactData => vec![(Use::TlsServerKey, RUNTIME_ARTIFACT_DATA_CERTIFICATE_FILE)],
        Process::SecurityAuthority => {
            vec![(Use::TlsServerKey, SECURITY_AUTHORITY_CERTIFICATE_FILE)]
        }
        Process::EgressBroker => vec![
            (Use::TlsServerKey, EGRESS_BROKER_CERTIFICATE_FILE),
            (Use::TlsClientKey, EGRESS_BROKER_CLIENT_CERTIFICATE_FILE),
        ],
        Process::ModelWorker => vec![
            (Use::TlsClientKey, MODEL_WORKER_CLIENT_CERTIFICATE_FILE),
            (Use::NatsClientKey, RUNTIME_NATS_CLIENT_CERTIFICATE_FILE),
        ],
        Process::ContextRemote => vec![(Use::TlsClientKey, CONTEXT_WORKER_CLIENT_CERTIFICATE_FILE)],
        Process::ContextDataset => {
            vec![(Use::TlsClientKey, CONTEXT_DATASET_CLIENT_CERTIFICATE_FILE)]
        }
        Process::ContextSubscription => vec![(
            Use::TlsClientKey,
            CONTEXT_SUBSCRIPTION_CLIENT_CERTIFICATE_FILE,
        )],
        Process::CapabilityRemote => {
            vec![(Use::TlsClientKey, CAPABILITY_REMOTE_CLIENT_CERTIFICATE_FILE)]
        }
        Process::McpHost => vec![
            (Use::TlsServerKey, MCP_HOST_CERTIFICATE_FILE),
            (Use::TlsClientKey, MCP_HOST_EGRESS_CLIENT_CERTIFICATE_FILE),
        ],
        Process::McpResourceHost => vec![
            (Use::TlsServerKey, MCP_RESOURCE_HOST_CERTIFICATE_FILE),
            (
                Use::TlsClientKey,
                MCP_RESOURCE_EGRESS_CLIENT_CERTIFICATE_FILE,
            ),
        ],
        Process::McpDiscovery => vec![(Use::TlsClientKey, MCP_DISCOVERY_CLIENT_CERTIFICATE_FILE)],
        Process::McpSubscription => {
            vec![(Use::TlsClientKey, MCP_SUBSCRIPTION_CLIENT_CERTIFICATE_FILE)]
        }
        Process::McpCleanup => vec![(Use::TlsClientKey, MCP_CLEANUP_CLIENT_CERTIFICATE_FILE)],
        Process::CallbackApi => vec![(Use::TlsClientKey, CALLBACK_CLIENT_CERTIFICATE_FILE)],
        Process::Outbox => vec![(Use::NatsClientKey, "outbox-client.pem")],
        Process::ArtifactMaintenance
        | Process::CapabilityNative
        | Process::ContextNative
        | Process::HistoryMaintenance => vec![],
    };
    certificates
        .into_iter()
        .map(|(purpose, name)| {
            (
                purpose,
                *all.get(name).expect("closed native identity registry"),
            )
        })
        .collect()
}

pub fn credentials(network: &NetworkTopologyV1) -> CredentialReferencesV1 {
    let mut files = Vec::new();
    for entry in &network.processes {
        let process = entry.process;
        let mut add = |purpose, file_name: &str| {
            files.push(CredentialReferenceV1 {
                process,
                purpose,
                file_name: file_name.into(),
            })
        };
        match process {
            Process::EgressBroker | Process::McpHost => (),
            Process::ArtifactData => {
                add(Use::ReadDatabase, "artifact-data-reader-password");
                add(Use::WorkDatabase, "artifact-data-worker-password");
            }
            Process::ArtifactGateway => add(Use::Database, "artifact-gateway-password"),
            Process::ArtifactMaintenance => add(Use::Database, "artifact-maintenance-password"),
            Process::SecurityAuthority => add(Use::Database, "security-authority-password"),
            Process::Outbox => add(Use::Database, "outbox-password"),
            Process::HistoryMaintenance => add(Use::Database, "history-password"),
            _ => add(Use::Database, "runtime-password"),
        }
        for (purpose, identity) in tls_identities(process) {
            add(purpose, identity.private_key);
        }
        if let Some(role) = openbao_role(network, process) {
            add(
                Use::OpenBaoClientKey,
                &crate::openbao_profile::private_key_file(role),
            );
        }
        if matches!(
            process,
            Process::GatewayManagement | Process::GatewayRuntime
        ) {
            add(Use::CursorKey, "cursor-key");
        }
        if process == Process::EgressBroker {
            add(Use::McpStateKey, "mcp-state-key");
        }
        if process == Process::CallbackApi {
            add(Use::McpOAuthStateKey, "mcp-oauth-state-key");
        }
        if matches!(
            process,
            Process::ArtifactGateway
                | Process::ArtifactData
                | Process::ArtifactMaintenance
                | Process::EgressBroker
        ) && !(process == Process::EgressBroker
            && matches!(network.providers, ProviderNetworkV1::S3OpenBao { .. }))
        {
            if matches!(network.providers, ProviderNetworkV1::S3OpenBao { .. }) {
                add(Use::Aws, &format!("s3-{}-credentials", process.name()));
            } else {
                add(Use::Aws, "aws-credentials");
            }
        }
    }
    CredentialReferencesV1 { files }
}

pub fn validate_credentials(
    network: &NetworkTopologyV1,
    actual: &CredentialReferencesV1,
) -> Result<(), InstallationError> {
    let set = |refs: &CredentialReferencesV1| {
        refs.files
            .iter()
            .map(|file| ((file.process, file.purpose), file.file_name.clone()))
            .collect::<BTreeMap<_, _>>()
    };
    let expected = credentials(network);
    if actual.files.len() != expected.files.len() || set(actual) != set(&expected) {
        return Err(InstallationError::CredentialInvalid);
    }
    Ok(())
}

/// All prepared material is private, but only each role's selected leafs are copied to its volume.
pub fn generate_leaf_files(
    network: &NetworkTopologyV1,
    issuer: &rcgen::Issuer<'_, rcgen::KeyPair>,
) -> Result<BTreeMap<String, Vec<u8>>, InstallationError> {
    network.validate()?;
    let mut specs = BTreeMap::new();
    for entry in &network.processes {
        for (_, spec) in tls_identities(entry.process) {
            let dns = match spec.usage {
                LocalTlsUsage::Server => vec![network.tls_server_name(entry.process)?],
                LocalTlsUsage::Client => vec![],
            };
            specs.insert(spec.certificate, (spec, dns));
        }
    }
    let native = native_identity_specs(DevProfile::starter());
    specs.insert(
        RUNTIME_NATS_SERVER_CERTIFICATE_FILE,
        (
            *native
                .get(RUNTIME_NATS_SERVER_CERTIFICATE_FILE)
                .expect("NATS server identity"),
            vec![network.nats_host.clone()],
        ),
    );
    let provision = outbox_identity_specs()[1];
    specs.insert(provision.certificate, (provision, vec![]));
    let mut files = BTreeMap::new();
    for (_, (spec, dns)) in specs {
        let dns = dns.iter().map(String::as_str).collect::<Vec<_>>();
        let usage = match spec.usage {
            LocalTlsUsage::Client => rcgen::ExtendedKeyUsagePurpose::ClientAuth,
            LocalTlsUsage::Server => rcgen::ExtendedKeyUsagePurpose::ServerAuth,
        };
        let material = crate::tls::create_leaf(&dns, spec.workload_identity, usage, issuer)?;
        files.insert(
            spec.certificate.into(),
            material.certificate_pem.into_bytes(),
        );
        files.insert(
            spec.private_key.into(),
            material.private_key_pem.into_bytes(),
        );
    }
    if let ProviderNetworkV1::S3OpenBao { artifact, openbao } = &network.providers {
        // The optional Seaweed gRPC surface trusts a separate authority. No platform role
        // receives a client certificate or issuer key from this authority.
        let grpc_authority = crate::tls::create_authority()?;
        let grpc_issuer = rcgen::Issuer::new(crate::tls::authority_parameters()?,
            rcgen::KeyPair::from_pem(&grpc_authority.private_key_pem).map_err(|_| InstallationError::CredentialInvalid)?);
        let grpc_host = artifact.host()?;
        let grpc_leaf = crate::tls::create_leaf(&[&grpc_host], None, rcgen::ExtendedKeyUsagePurpose::ServerAuth, &grpc_issuer)?;
        files.insert("s3-grpc-ca.pem".into(), grpc_authority.certificate_pem.into_bytes());
        files.insert("s3-grpc-server.crt".into(), grpc_leaf.certificate_pem.into_bytes());
        files.insert("s3-grpc-server.key".into(), grpc_leaf.private_key_pem.into_bytes());
        for (origin, certificate_file, key_file) in [
            (openbao, crate::openbao_profile::OPENBAO_SERVER_CERTIFICATE, crate::openbao_profile::OPENBAO_SERVER_KEY),
            (artifact, "s3-server.pem", "s3-server-key.pem"),
        ] {
            let host = origin.host()?;
            let material = crate::tls::create_leaf(&[&host], None, rcgen::ExtendedKeyUsagePurpose::ServerAuth, issuer)?;
            files.insert(certificate_file.into(), material.certificate_pem.into_bytes());
            files.insert(key_file.into(), material.private_key_pem.into_bytes());
        }
        for role in OpenBaoInstallationRole::ALL {
            let uri = crate::openbao_profile::workload_identity(*role);
            let material = crate::tls::create_leaf(&[], Some(&uri), rcgen::ExtendedKeyUsagePurpose::ClientAuth, issuer)?;
            files.insert(crate::openbao_profile::certificate_file(*role), material.certificate_pem.into_bytes());
            files.insert(crate::openbao_profile::private_key_file(*role), material.private_key_pem.into_bytes());
        }
    } else if network.topology == insight_platform_deployment_contracts::installation::InstallationTopology::KubernetesLocal {
        let host = network.providers.artifact().host()?;
        let material = crate::tls::create_leaf(&[&host], None, rcgen::ExtendedKeyUsagePurpose::ServerAuth, issuer)?;
        files.insert(LOCALSTACK_CERTIFICATE_FILE.into(), material.certificate_pem.into_bytes());
        files.insert(LOCALSTACK_PRIVATE_KEY_FILE.into(), material.private_key_pem.into_bytes());
    }
    Ok(files)
}
