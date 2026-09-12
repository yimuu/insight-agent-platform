//! Pure OpenBao API bootstrap requests and ordinary persistent-server configuration.
use insight_platform_contracts::{canonical_digest, Sha256Digest};
use insight_platform_deployment_contracts::openbao::BaoClientConfigV1;
use insight_platform_deployment_contracts::{installation::*, installation_provider::*};
use serde_json::{json, Value};
use std::collections::BTreeMap;

pub const OPENBAO_SEAL_FILE: &str = "openbao-seal.key";
pub const OPENBAO_IMAGE: &str = "ghcr.io/openbao/openbao@sha256:5b2486ab0fb90bbc788cc345b0a08616dfb375873ee8be5df3a2fd4d378a67e0";
pub const OPENBAO_SERVER_CERTIFICATE: &str = "openbao-server.pem";
pub const OPENBAO_SERVER_KEY: &str = "openbao-server-key.pem";
pub const OPENBAO_INITIALIZE_FILE: &str = "bootstrap.json";
pub const OPENBAO_SERVE_FILE: &str = "serve.json";
pub const OPENBAO_DIRECTORY: &str = "/run/insight-openbao";
pub const OPENBAO_DATA_DIRECTORY: &str = "/var/lib/openbao";

pub fn certificate_file(role: OpenBaoInstallationRole) -> String {
    format!("openbao-{}.pem", role.name())
}
pub fn private_key_file(role: OpenBaoInstallationRole) -> String {
    format!("openbao-{}-key.pem", role.name())
}
pub fn workload_identity(role: OpenBaoInstallationRole) -> String {
    format!(
        "spiffe://insight.platform/installation/openbao/{}",
        role.name()
    )
}

/// Select an already generated role leaf without changing the physical cluster or key binding.
pub fn role_client(
    observed: &InstallationProviderReadyV1,
    role: OpenBaoInstallationRole,
    credential_directory: &std::path::Path,
) -> Result<BaoClientConfigV1, InstallationError> {
    let mut client = observed.client.clone();
    client.auth_role = role.name().into();
    client.expected_token_policies = vec![role.name().into()];
    client.ca_file = credential_directory.join("ca.pem").display().to_string();
    client.client_certificate_file = credential_directory
        .join(certificate_file(role))
        .display()
        .to_string();
    client.client_private_key_file = credential_directory
        .join(private_key_file(role))
        .display()
        .to_string();
    client
        .validate()
        .map_err(|_| InstallationError::InvalidInput)?;
    Ok(client)
}

pub struct OpenBaoDocuments {
    pub initialize: Vec<u8>,
    pub serve: Vec<u8>,
    pub canary: Vec<u8>,
    pub configuration_digest: Sha256Digest,
}

fn read_path(path: &str) -> String {
    format!("path {path:?} {{ capabilities = [\"read\"] }}\n")
}
fn action_path(path: &str, action: &str) -> String {
    format!("path {path:?} {{ capabilities = [{action:?}] }}\n")
}

/// Closed least-privilege physical policies; no caller-supplied ACL text is accepted.
pub fn policy(role: OpenBaoInstallationRole) -> String {
    let mut result = read_path(&format!("sys/mounts/auth/{OPENBAO_AUTH_MOUNT}"));
    result.push_str(&read_path(&format!("sys/mounts/{OPENBAO_TRANSIT_MOUNT}")));
    for key in [OPENBAO_ARTIFACT_KEY, OPENBAO_SECRET_KEY] {
        if role == OpenBaoInstallationRole::Initializer
            || (key == OPENBAO_SECRET_KEY) == (role == OpenBaoInstallationRole::EgressBroker)
        {
            result.push_str(&read_path(&format!("{OPENBAO_TRANSIT_MOUNT}/keys/{key}")));
        }
    }
    match role {
        OpenBaoInstallationRole::Initializer => {
            result.push_str(&read_path(&format!("sys/mounts/{OPENBAO_KV_MOUNT}")));
            result.push_str(&read_path(&format!(
                "{OPENBAO_KV_MOUNT}/data/{OPENBAO_CANARY_PATH}"
            )));
            result.push_str(&read_path(&format!(
                "{OPENBAO_KV_MOUNT}/metadata/{OPENBAO_CANARY_PATH}"
            )));
            for name in OpenBaoInstallationRole::ALL {
                result.push_str(&read_path(&format!(
                    "auth/{OPENBAO_AUTH_MOUNT}/certs/{}",
                    name.name()
                )));
                result.push_str(&read_path(&format!("sys/policies/acl/{}", name.name())));
            }
        }
        OpenBaoInstallationRole::ArtifactGateway | OpenBaoInstallationRole::ArtifactData => {
            for operation in ["encrypt", "decrypt"] {
                result.push_str(&action_path(
                    &format!("{OPENBAO_TRANSIT_MOUNT}/{operation}/{OPENBAO_ARTIFACT_KEY}"),
                    "update",
                ));
            }
        }
        OpenBaoInstallationRole::ArtifactMaintenance => {
            result.push_str(&action_path(
                &format!("{OPENBAO_TRANSIT_MOUNT}/decrypt/{OPENBAO_ARTIFACT_KEY}"),
                "update",
            ));
        }
        OpenBaoInstallationRole::EgressBroker => {
            for operation in ["encrypt", "decrypt"] {
                result.push_str(&action_path(
                    &format!("{OPENBAO_TRANSIT_MOUNT}/{operation}/{OPENBAO_SECRET_KEY}"),
                    "update",
                ));
            }
            result.push_str(&read_path(&format!("sys/mounts/{OPENBAO_KV_MOUNT}")));
            result.push_str(&read_path(&format!(
                "{OPENBAO_KV_MOUNT}/data/{OPENBAO_CANARY_PATH}"
            )));
            result.push_str(&format!("path \"{OPENBAO_KV_MOUNT}/data/prepared/*\" {{ capabilities = [\"create\", \"read\"] }}\n"));
            result.push_str(&read_path(&format!(
                "{OPENBAO_KV_MOUNT}/metadata/prepared/*"
            )));
            result.push_str(&action_path(
                &format!("{OPENBAO_KV_MOUNT}/destroy/prepared/*"),
                "update",
            ));
        }
    }
    result
}

fn encoded(value: &Value) -> Result<Vec<u8>, InstallationError> {
    let bytes = serde_json::to_vec(value).map_err(|_| InstallationError::InvalidInput)?;
    if bytes.is_empty() || bytes.len() > INSTALLATION_MAX_BYTES {
        return Err(InstallationError::InvalidInput);
    }
    Ok(bytes)
}

pub fn render(
    input: &InstallationInputV1,
    identity: &InstallationIdentityV1,
    certificates: &BTreeMap<OpenBaoInstallationRole, String>,
) -> Result<OpenBaoDocuments, InstallationError> {
    input.validate()?;
    identity.validate()?;
    if identity.input_digest != input.digest()?
        || certificates.len() != OpenBaoInstallationRole::ALL.len()
        || !OpenBaoInstallationRole::ALL
            .iter()
            .all(|role| certificates.contains_key(role))
    {
        return Err(InstallationError::IdentityDrift);
    }
    for certificate in certificates.values() {
        if certificate.len() > 16_384
            || !certificate.starts_with("-----BEGIN CERTIFICATE-----\n")
            || !certificate
                .trim_end()
                .ends_with("-----END CERTIFICATE-----")
            || certificate.contains("PRIVATE KEY")
        {
            return Err(InstallationError::CredentialInvalid);
        }
    }
    let origin = input.network.providers.openbao()?;
    let host = origin.host()?;
    let serve = json!({
        "ui": false,
        "disable_mlock": true,
        "log_level": "warn",
        "api_addr": origin.as_str(),
        "cluster_addr": format!("https://{host}:8201"),
        "storage": {"raft": {"path":OPENBAO_DATA_DIRECTORY,"node_id":input.name}},
        "seal": {"static": {"current_key_id":identity.installation_id.to_string(),"current_key":format!("file://{OPENBAO_DIRECTORY}/{OPENBAO_SEAL_FILE}")}},
        "listener": [{"tcp": {
            "address":"0.0.0.0:8200",
            "tls_disable":false,
            "tls_min_version":"tls12",
            "tls_cert_file":format!("{OPENBAO_DIRECTORY}/{OPENBAO_SERVER_CERTIFICATE}"),
            "tls_key_file":format!("{OPENBAO_DIRECTORY}/{OPENBAO_SERVER_KEY}"),
            "tls_client_ca_file":format!("{OPENBAO_DIRECTORY}/ca.pem"),
            "tls_require_and_verify_client_cert":true,
        }}],
    });
    let canary = json!({
        "schema_version":1,
        "purpose":"insight_openbao_installation_readiness",
        "input_digest":input.digest()?,
        "identity_digest":identity.digest()?,
    });
    let mut requests = Vec::new();
    let mut request = |name: &str, path: String, data: Value| {
        requests.push(
            json!({name:{"operation":"update","path":path,"data":data,"allow_failure":false}}),
        );
    };
    request(
        "cert-auth",
        format!("sys/auth/{OPENBAO_AUTH_MOUNT}"),
        json!({"type":"cert"}),
    );
    request(
        "transit",
        format!("sys/mounts/{OPENBAO_TRANSIT_MOUNT}"),
        json!({"type":"transit"}),
    );
    request(
        "secrets",
        format!("sys/mounts/{OPENBAO_KV_MOUNT}"),
        json!({"type":"kv","options":{"version":"2"}}),
    );
    for key in [OPENBAO_ARTIFACT_KEY, OPENBAO_SECRET_KEY] {
        request(
            key,
            format!("{OPENBAO_TRANSIT_MOUNT}/keys/{key}"),
            json!({"type":"aes256-gcm96","derived":false,"convergent_encryption":false,"exportable":false,"allow_plaintext_backup":false}),
        );
    }
    for role in OpenBaoInstallationRole::ALL {
        request(
            &format!("{}-policy", role.name()),
            format!("sys/policies/acl/{}", role.name()),
            json!({"policy":policy(*role)}),
        );
        request(
            &format!("{}-certificate", role.name()),
            format!("auth/{OPENBAO_AUTH_MOUNT}/certs/{}", role.name()),
            json!({
                "display_name":role.name(),
                "certificate":certificates[role],
                "allowed_uri_sans":[workload_identity(*role)],
                "token_policies":[role.name()],
                "token_no_default_policy":true,
                "token_ttl":300,
                "token_max_ttl":300,
                "token_explicit_max_ttl":300,
                "token_type":"service",
            }),
        );
    }
    request(
        "readiness-canary",
        format!("{OPENBAO_KV_MOUNT}/data/{OPENBAO_CANARY_PATH}"),
        json!({"options":{"cas":0},"data":canary}),
    );
    let initialize = json!({"schema_version":1,"requests":requests});
    let configuration_digest =
        canonical_digest(&json!({"schema_version":1,"initialize":initialize,"serve":serve}))
            .map_err(|_| InstallationError::InvalidInput)?
            .parse()
            .map_err(|_| InstallationError::InvalidInput)?;
    Ok(OpenBaoDocuments {
        initialize: encoded(&initialize)?,
        serve: encoded(&serve)?,
        canary: encoded(&canary)?,
        configuration_digest,
    })
}

#[cfg(test)]
pub(crate) fn fixture_ready(
    input: &InstallationInputV1,
    private: &std::path::Path,
    canary_digest: Sha256Digest,
) -> InstallationProviderReadyV1 {
    use insight_platform_deployment_contracts::openbao::{KvV2BindingV1, TransitBindingV1};
    let role = OpenBaoInstallationRole::Initializer;
    let client = BaoClientConfigV1 {
        schema_version: 1,
        endpoint: input.network.providers.openbao().unwrap().as_str().into(),
        expected_cluster_id: "092036e7-f9ab-41fd-8122-077ea91db8b0".into(),
        auth_mount: OPENBAO_AUTH_MOUNT.into(),
        auth_mount_accessor: "auth_cert_a12b34".into(),
        auth_role: role.name().into(),
        expected_token_policies: vec![role.name().into()],
        ca_file: private.join("ca.pem").display().to_string(),
        client_certificate_file: private.join(certificate_file(role)).display().to_string(),
        client_private_key_file: private.join(private_key_file(role)).display().to_string(),
        connect_timeout_milliseconds: 5000,
        operation_timeout_milliseconds: 30000,
        maximum_response_bytes: 131072,
    };
    let key = |name: &str| {
        let mut key = TransitBindingV1 {
            schema_version: 1,
            mount: OPENBAO_TRANSIT_MOUNT.into(),
            mount_accessor: "transit_a12b34".into(),
            name: name.into(),
            key_version: 1,
            identity_digest: canary_digest.clone(),
        };
        key.identity_digest = key.calculated_digest(&client.expected_cluster_id).unwrap();
        key
    };
    let mut secrets = KvV2BindingV1 {
        schema_version: 1,
        mount: OPENBAO_KV_MOUNT.into(),
        mount_accessor: "kv_a12b34".into(),
        identity_digest: canary_digest.clone(),
    };
    secrets.identity_digest = secrets
        .calculated_digest(&client.expected_cluster_id)
        .unwrap();
    InstallationProviderReadyV1 {
        artifact_key: key(OPENBAO_ARTIFACT_KEY),
        secret_key: key(OPENBAO_SECRET_KEY),
        client,
        secrets,
        canary_version: 1,
        canary_digest,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn initializer_and_runtime_acl_cannot_change_configuration_or_destroy_key_material() {
        for role in OpenBaoInstallationRole::ALL {
            let acl = policy(*role);
            assert!(
                !acl.contains("sudo") && !acl.contains("list") && !acl.contains("[\"delete\"]")
            );
            assert!(
                !acl.contains("sys/auth") && !acl.contains("keys/*") && !acl.contains("metadata/*")
            );
            for line in acl.lines().filter(|line| line.contains("sys/")) {
                assert!(line.ends_with("capabilities = [\"read\"] }"));
            }
        }
        let initializer = policy(OpenBaoInstallationRole::Initializer);
        assert!(!initializer.contains("\"update\"") && !initializer.contains("\"create\""));
        let maintenance = policy(OpenBaoInstallationRole::ArtifactMaintenance);
        assert!(!maintenance.contains("/encrypt/") && !maintenance.contains("secret-reference"));
        let egress = policy(OpenBaoInstallationRole::EgressBroker);
        assert!(egress.contains("capabilities = [\"create\", \"read\"]"));
        assert!(!egress.contains("artifact-reference"));
    }
}
