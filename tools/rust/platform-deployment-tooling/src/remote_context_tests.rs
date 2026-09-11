//! Configuration evidence, not provider network or dispatch qualification.
use crate::{installation::*, renderer::*, worker_profile::WorkerBuilds};
use insight_platform_contracts::*;
use insight_platform_deployment_contracts::installation::*;
use serde_json::{json, Value};
use std::path::Path;

fn package() -> Sha256Digest {
    format!("sha256:{}", "a".repeat(64)).parse().unwrap()
}
fn destination() -> InstalledRemoteContextDestinationV1 {
    let endpoint = CanonicalHttpEndpoint {
        scheme: CapabilityEndpointScheme::Https,
        host: "documents.example.test".into(),
        port: 443,
        base_path: "/search".into(),
    };
    InstalledRemoteContextDestinationV1 {
        schema_version: 1,
        protocol_contract_digest: remote_context_protocol_contract_digest(),
        result_mapping_digest: remote_context_result_mapping_digest(),
        endpoint_identity_digest: endpoint.canonical_digest().unwrap(),
        endpoint,
        region: "global".parse().unwrap(),
        credential_injections: vec![],
        trusted_root_pem: crate::tls::create_authority().unwrap().certificate_pem,
        maximum_request_bytes: MAX_REMOTE_CONTEXT_INSTALLATION_REQUEST_BYTES,
        maximum_response_bytes: MAX_REMOTE_CONTEXT_INSTALLATION_RESPONSE_BYTES,
    }
}
fn selected(input: InstallationInputV1) -> InstallationInputV1 {
    with_remote_context_destinations(input, vec![destination()]).unwrap()
}

#[test]
fn required_destination_field_is_closed_and_default_is_deny_all() {
    let input = compose_input("context-input", package()).unwrap();
    assert!(input.remote_context_destinations.is_empty());
    assert!(!input
        .network
        .processes
        .iter()
        .any(|p| p.process == InstallationProcess::ContextRemote));
    let mut wire = serde_json::to_value(&input).unwrap();
    assert!(InstallationInputV1::decode(&serde_json::to_vec(&wire).unwrap()).is_ok());
    wire.as_object_mut()
        .unwrap()
        .remove("remote_context_destinations");
    assert!(InstallationInputV1::decode(&serde_json::to_vec(&wire).unwrap()).is_err());
    let mut wire = serde_json::to_value(destination()).unwrap();
    wire["context_deployment"] = json!({});
    assert!(serde_json::from_value::<InstalledRemoteContextDestinationV1>(wire).is_err());
}

#[test]
fn destination_selection_rejects_missing_worker_conflicts_credentials_and_address_drift() {
    let input = compose_input("context-input", package()).unwrap();
    let grant = destination();
    let mut missing_worker = input.clone();
    missing_worker
        .remote_context_destinations
        .push(grant.clone());
    assert!(matches!(
        missing_worker.validate(),
        Err(InstallationError::InvalidRoleClosure)
    ));
    assert!(
        with_remote_context_destinations(input.clone(), vec![grant.clone(), grant.clone()])
            .is_err()
    );
    let mut conflicting = grant.clone();
    conflicting.maximum_response_bytes = 1;
    assert!(
        with_remote_context_destinations(input.clone(), vec![grant.clone(), conflicting]).is_err()
    );
    assert!(with_remote_context_destinations(
        input.clone(),
        vec![grant.clone(); MAX_REMOTE_CONTEXT_INSTALLATION_DESTINATIONS + 1]
    )
    .is_err());
    let mut credential = grant.clone();
    credential
        .credential_injections
        .push(InstalledHttpCredentialInjection::BearerAuthorization {
            purpose: "api_key".parse().unwrap(),
        });
    assert!(with_remote_context_destinations(input.clone(), vec![credential]).is_ok());
    for host in ["localhost", "a.localhost", "127.0.0.1", "::1"] {
        let mut wrong = grant.clone();
        wrong.endpoint.host = host.into();
        wrong.endpoint_identity_digest = wrong.endpoint.canonical_digest().unwrap();
        assert!(with_remote_context_destinations(input.clone(), vec![wrong]).is_err());
    }
    let mut wrong = grant;
    wrong.endpoint.host = "other.example.test".into();
    assert!(with_remote_context_destinations(input, vec![wrong]).is_err());
}

#[test]
fn actual_public_roots_are_parsed_before_any_private_state_is_created() {
    let input = compose_input("context-roots", package()).unwrap();
    let mut grant = destination();
    // A real multi-certificate bundle may exceed the former unrelated 2 KiB string ceiling.
    grant.trusted_root_pem = grant.trusted_root_pem.repeat(5);
    assert!(grant.trusted_root_pem.len() > 2048);
    let selected = with_remote_context_destinations(input.clone(), vec![grant.clone()]).unwrap();
    assert!(InstallationInputV1::decode(&serde_json::to_vec(&selected).unwrap()).is_ok());
    for roots in [
        "not a certificate".to_owned(),
        "-----BEGIN CERTIFICATE-----\nYWJj\n-----END CERTIFICATE-----\n".into(),
        format!("{}private suffix", grant.trusted_root_pem),
        crate::tls::create_authority().unwrap().private_key_pem,
        "x".repeat(MAX_REMOTE_CONTEXT_INSTALLATION_TRUST_BYTES + 1),
    ] {
        let mut wrong = grant.clone();
        wrong.trusted_root_pem = roots;
        assert!(with_remote_context_destinations(input.clone(), vec![wrong.clone()]).is_err());
        let temp = tempfile::tempdir().unwrap();
        let root = std::fs::canonicalize(temp.path()).unwrap().join("private");
        let mut wrong_input = selected.clone();
        wrong_input.remote_context_destinations = vec![wrong];
        assert!(PreparedInstallation::prepare(&wrong_input, &root).is_err());
        assert!(!root.exists());
    }
}

#[test]
fn declaration_file_is_regular_bounded_and_strict_before_selection() {
    use std::os::unix::fs::symlink;
    let temp = tempfile::tempdir().unwrap();
    let root = std::fs::canonicalize(temp.path()).unwrap();
    let file = root.join("destinations.json");
    let grants = vec![destination()];
    let wire = serde_json::to_vec(&grants).unwrap();
    std::fs::write(&file, &wire).unwrap();
    assert_eq!(read_remote_context_destinations(&file).unwrap(), grants);
    let link = root.join("link");
    symlink(&file, &link).unwrap();
    assert!(read_remote_context_destinations(&link).is_err());
    std::fs::remove_file(&link).unwrap();
    std::fs::hard_link(&file, &link).unwrap();
    assert!(read_remote_context_destinations(&file).is_err());
    std::fs::remove_file(&link).unwrap();
    let alias = root.join("alias");
    symlink(&root, &alias).unwrap();
    assert!(read_remote_context_destinations(&alias.join("destinations.json")).is_err());
    let duplicate = String::from_utf8(wire).unwrap().replacen(
        "\"schema_version\":1",
        "\"schema_version\":1,\"schema_version\":1",
        1,
    );
    std::fs::write(&file, duplicate).unwrap();
    assert!(read_remote_context_destinations(&file).is_err());
    std::fs::write(&file, vec![b' '; INSTALLATION_MAX_BYTES + 1]).unwrap();
    assert!(read_remote_context_destinations(&file).is_err());
}

#[test]
fn physical_destination_change_cannot_reuse_a_prepared_identity() {
    let temp = tempfile::tempdir().unwrap();
    let root = std::fs::canonicalize(temp.path()).unwrap().join("private");
    let input = selected(compose_input("context-frozen", package()).unwrap());
    let prepared = PreparedInstallation::prepare(&input, &root).unwrap();
    let identity = prepared.identity().digest().unwrap();
    drop(prepared);
    let mut changed = input.clone();
    changed.remote_context_destinations[0].maximum_response_bytes /= 2;
    assert!(PreparedInstallation::prepare(&changed, &root).is_err());
    assert_eq!(
        PreparedInstallation::open(&input, &root)
            .unwrap()
            .identity()
            .digest()
            .unwrap(),
        identity
    );
}

#[test]
fn compose_and_helm_consume_only_the_selected_worker_role_and_own_private_mount() {
    let image = format!("example/runtime@{}", package());
    let input = selected(compose_input("context-compose", package()).unwrap());
    let compose =
        crate::compose::compose_document(&input, Path::new("/declared/input.json"), &image, &image)
            .unwrap();
    let role = &compose["services"]["context-remote"];
    assert!(role["command"][0]
        .as_str()
        .unwrap()
        .ends_with("platform-remote-context-worker"));
    assert_eq!(
        role["volumes"],
        json!([{"type":"volume","source":"role-context-remote","target":"/run/insight/role","read_only":true,"volume":{"nocopy":true}}])
    );
    assert!(compose["volumes"].get("role-context-remote").is_some());
    let input = selected(crate::kubernetes::kubernetes_input("context-helm", package()).unwrap());
    let plan = crate::kubernetes::helm_plan(&input, &image, &image).unwrap();
    let role = plan["processes"]
        .as_array()
        .unwrap()
        .iter()
        .find(|p| p["name"] == "context-remote")
        .unwrap();
    assert_eq!(role["binary"], "platform-remote-context-worker");
    assert_eq!(
        role["paths"]["credential_directory"],
        "/run/insight/role/credentials"
    );
    assert_eq!(
        plan["input"]["remote_context_destinations"],
        serde_json::to_value(&input.remote_context_destinations).unwrap()
    );
    let mut foreign = input;
    foreign
        .network
        .processes
        .last_mut()
        .unwrap()
        .observability_address
        .set_port(9091);
    assert!(crate::kubernetes::helm_plan(&foreign, &image, &image).is_err());
}

#[test]
fn all_three_renderers_share_actual_remote_manifest_tls_and_destination_configuration() {
    use InstallationProcess as P;
    for topology in [
        InstallationTopology::Compose,
        InstallationTopology::KubernetesLocal,
        InstallationTopology::Native,
    ] {
        let temp = tempfile::tempdir().unwrap();
        let root = std::fs::canonicalize(temp.path()).unwrap();
        let mut input = selected(match topology {
            InstallationTopology::Compose => compose_input("context-render", package()).unwrap(),
            InstallationTopology::KubernetesLocal => {
                crate::kubernetes::kubernetes_input("context-render", package()).unwrap()
            }
            InstallationTopology::Native => crate::native::native_input(
                "context-render",
                package(),
                &root.join("output"),
                28000,
            )
            .unwrap(),
        });
        input.remote_context_destinations[0].credential_injections =
            vec![InstalledHttpCredentialInjection::Header {
                purpose: "document_search_key".parse().unwrap(),
                name: "x-document-key".into(),
            }];
        let prepared = PreparedInstallation::prepare(&input, &root.join("private")).unwrap();
        let binaries = crate::worker_profile::fixture_binaries(&root);
        let processes = input
            .network
            .processes
            .iter()
            .map(|p| p.process)
            .collect::<Vec<_>>();
        let builds = WorkerBuilds::read_processes(&binaries, &processes).unwrap();
        let ready = crate::openbao_profile::fixture_ready(&input, &root.join("private"), package());
        let bucket = format!(
            "insight-platform-artifacts-{}",
            prepared.identity().installation_id.uuid().simple()
        );
        let artifact = crate::provider_config::openbao_artifact_provider_catalog(
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
        let output = render_installation(InstallationRenderInputs {
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
            private_files: &files,
        })
        .unwrap();
        let broker: Value =
            serde_json::from_slice(&output.roles[&P::EgressBroker].configuration).unwrap();
        assert!(broker.get("remote_context_endpoints").is_none());
        assert_eq!(
            broker["remote_context_destinations"],
            serde_json::to_value(&input.remote_context_destinations).unwrap()
        );
        let remote = &output.roles[&P::ContextRemote];
        let config: Value = serde_json::from_slice(&remote.configuration).unwrap();
        let expected = builds.manifest(
            "platform-remote-context-worker",
            "context-worker",
            WorkClass::Context,
            &crate::base_profile::local_digest("context-native-adapter").unwrap(),
            (4, 1),
            crate::worker_profile::remote_context(),
        );
        assert_eq!(config["worker_manifest"], expected.unwrap());
        assert_eq!(
            config["maximum_rpc_metadata_bytes"],
            limits::MAX_EGRESS_METADATA_BYTES_HARD
        );
        assert_eq!(
            config["egress_endpoint"],
            input.network.endpoint(P::EgressBroker).unwrap()
        );
        assert!(remote
            .credentials
            .contains_key("context-worker-client-key.pem"));
        assert!(remote.credentials.contains_key("context-worker-client.pem"));
        assert!(!remote
            .credentials
            .contains_key("egress-broker-client-key.pem"));
        assert!(!remote.credentials.contains_key("ca-key.pem"));
        let environment = std::str::from_utf8(&remote.environment).unwrap();
        let paths = input
            .paths
            .iter()
            .find(|p| p.process == P::ContextRemote)
            .unwrap();
        assert!(environment.contains(&format!(
            "{}/context-worker-client-key.pem",
            paths.credential_directory
        )));
        if topology == InstallationTopology::Native {
            assert!(paths
                .configuration_directory
                .ends_with("/roles/context-remote/config"));
            assert!(input
                .network
                .process(P::ContextRemote)
                .unwrap()
                .observability_address
                .ip()
                .is_loopback());
        }
    }
}
