//! Read-only bootstrap discovery for the already started, installation-owned OpenBao instance.
//! It never initializes, mounts, grants, rotates or writes a secret, including on uncertain startup.
use insight_platform_contracts::{canonical_digest, parse_strict_json, JsonLimits, Sha256Digest};
use insight_platform_deployment_contracts::{installation::*, installation_provider::*};
use insight_platform_deployment_tooling::{installation::PreparedInstallation, openbao_profile};
use insight_platform_openbao::{
    BaoClient, BaoClientConfigV1, BaoError, BaoSecretPath, KvV2BindingV1, SensitiveBytes,
    TransitBindingV1,
};
use reqwest::{header::HeaderValue, Method};
use serde_json::Value;
use std::{path::Path, time::Duration};
use tokio::time::Instant;
use zeroize::{Zeroize, Zeroizing};

const LIMIT: usize = 262_144;
const LIMITS: JsonLimits = JsonLimits {
    max_bytes: LIMIT,
    max_depth: 16,
    max_properties_per_object: 64,
    max_items_per_array: 256,
    max_string_bytes: 65_536,
};
struct ProtectedJson(Value);
impl Drop for ProtectedJson {
    fn drop(&mut self) {
        fn clear(value: &mut Value) {
            match value {
                Value::String(value) => value.zeroize(),
                Value::Array(values) => values.iter_mut().for_each(clear),
                Value::Object(values) => values.values_mut().for_each(clear),
                _ => (),
            }
        }
        clear(&mut self.0);
    }
}
fn invalid() -> InstallationError {
    InstallationError::ConfigurationDrift
}
fn provider_error(error: BaoError) -> InstallationError {
    match error {
        BaoError::Unavailable => InstallationError::PrerequisiteUnavailable,
        BaoError::UnknownOutcome => InstallationError::ExternalOutcomeUnknown,
        BaoError::InvalidConfig
        | BaoError::Denied
        | BaoError::NotFound
        | BaoError::Conflict
        | BaoError::InvalidEvidence => InstallationError::ConfigurationDrift,
    }
}
fn canonical(value: &Value) -> Result<Sha256Digest, InstallationError> {
    canonical_digest(value)
        .map_err(|_| invalid())?
        .parse()
        .map_err(|_| invalid())
}
fn field(value: &Value, pointer: &str, maximum: usize) -> Result<String, InstallationError> {
    let value = value
        .pointer(pointer)
        .and_then(Value::as_str)
        .ok_or_else(invalid)?;
    if value.is_empty() || value.len() > maximum || value.chars().any(char::is_control) {
        return Err(invalid());
    }
    Ok(value.to_owned())
}
struct BootstrapReader {
    client: reqwest::Client,
    origin: String,
    token: Option<HeaderValue>,
    deadline: Instant,
}
impl BootstrapReader {
    fn install(prepared: &PreparedInstallation) -> Result<Self, InstallationError> {
        let read = |name| -> Result<SensitiveBytes, InstallationError> {
            SensitiveBytes::new(
                prepared
                    .directory()
                    .read(name, 65_536)?
                    .ok_or(InstallationError::Incomplete)?,
            )
            .map_err(|_| InstallationError::CredentialInvalid)
        };
        let role = OpenBaoInstallationRole::Initializer;
        let certificate_file = openbao_profile::certificate_file(role);
        let private_key_file = openbao_profile::private_key_file(role);
        let ca = read("ca.pem")?;
        let certificate = read(&certificate_file)?;
        let key = read(&private_key_file)?;
        let mut pem = certificate.into_bytes();
        pem.push(b'\n');
        pem.extend_from_slice(key.as_bytes());
        let pem = SensitiveBytes::new(pem).map_err(|_| InstallationError::CredentialInvalid)?;
        let identity = reqwest::Identity::from_pem(pem.as_bytes())
            .map_err(|_| InstallationError::CredentialInvalid)?;
        let roots = reqwest::Certificate::from_pem_bundle(ca.as_bytes())
            .map_err(|_| InstallationError::CredentialInvalid)?;
        if roots.is_empty() {
            return Err(InstallationError::CredentialInvalid);
        }
        let client = reqwest::Client::builder()
            .tls_backend_rustls()
            .tls_certs_only(roots)
            .identity(identity)
            .tls_sslkeylogfile(false)
            .tls_version_min(reqwest::tls::Version::TLS_1_2)
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .no_proxy()
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(30))
            .build()
            .map_err(|_| InstallationError::CredentialInvalid)?;
        Ok(Self {
            client,
            origin: prepared
                .input()
                .network
                .providers
                .openbao()?
                .as_str()
                .into(),
            token: None,
            deadline: Instant::now() + Duration::from_secs(30),
        })
    }
    async fn request(
        &self,
        method: Method,
        path: &str,
        body: Option<Value>,
    ) -> Result<ProtectedJson, InstallationError> {
        // Every caller below uses one of this module's fixed read paths or the fixed cert login.
        let future = async {
            let mut request = self
                .client
                .request(method, format!("{}/v1/{path}", self.origin));
            if let Some(token) = &self.token {
                request = request.header("X-Vault-Token", token.clone());
            }
            if let Some(body) = body {
                request = request.json(&body);
            }
            let mut response = request
                .send()
                .await
                .map_err(|_| InstallationError::PrerequisiteUnavailable)?;
            if !response.status().is_success() {
                return Err(if response.status().is_server_error() {
                    InstallationError::PrerequisiteUnavailable
                } else {
                    invalid()
                });
            }
            if response
                .content_length()
                .is_some_and(|size| size > LIMIT as u64)
            {
                return Err(invalid());
            }
            let mut bytes = Zeroizing::new(Vec::new());
            while let Some(chunk) = response
                .chunk()
                .await
                .map_err(|_| InstallationError::PrerequisiteUnavailable)?
            {
                if bytes.len() + chunk.len() > LIMIT {
                    return Err(invalid());
                }
                bytes.extend_from_slice(&chunk);
            }
            Ok(ProtectedJson(
                parse_strict_json(&bytes, LIMITS).map_err(|_| invalid())?,
            ))
        };
        tokio::time::timeout_at(self.deadline, future)
            .await
            .map_err(|_| InstallationError::PrerequisiteUnavailable)?
    }
    async fn get(&self, path: &str) -> Result<ProtectedJson, InstallationError> {
        self.request(Method::GET, path, None).await
    }
    async fn login(&mut self) -> Result<(), InstallationError> {
        let response = self
            .request(
                Method::POST,
                &format!("auth/{OPENBAO_AUTH_MOUNT}/login"),
                Some(serde_json::json!({"name":OPENBAO_INITIALIZER_ROLE})),
            )
            .await?;
        if response.0.pointer("/auth/policies")
            != Some(&serde_json::json!([OPENBAO_INITIALIZER_ROLE]))
            || response.0.pointer("/auth/token_policies")
                != Some(&serde_json::json!([OPENBAO_INITIALIZER_ROLE]))
            || response
                .0
                .pointer("/auth/lease_duration")
                .and_then(Value::as_u64)
                .is_none_or(|ttl| ttl == 0 || ttl > 300)
        {
            return Err(invalid());
        }
        let token = response
            .0
            .pointer("/auth/client_token")
            .and_then(Value::as_str)
            .ok_or_else(invalid)?;
        if token.is_empty() || token.len() > 4096 {
            return Err(invalid());
        }
        let mut token = HeaderValue::from_str(token).map_err(|_| invalid())?;
        token.set_sensitive(true);
        self.token = Some(token);
        Ok(())
    }
}

/// Observe an initialized provider or verify an already recorded one; no provider mutation exists.
pub async fn observe(
    prepared: &PreparedInstallation,
    private_path: &Path,
) -> Result<InstallationProviderReadyV1, InstallationError> {
    let state = prepared.provider_state()?;
    if state.state == InstallationProviderStateV1::Prepared {
        return Err(InstallationError::Incomplete);
    }
    let mut reader = BootstrapReader::install(prepared)?;
    let health = reader.get("sys/health").await?;
    if health.0.get("initialized").and_then(Value::as_bool) != Some(true)
        || health.0.get("sealed").and_then(Value::as_bool) != Some(false)
    {
        return Err(InstallationError::Incomplete);
    }
    let cluster = field(&health.0, "/cluster_id", 36)?;
    reader.login().await?;
    let auth = reader
        .get(&format!("sys/mounts/auth/{OPENBAO_AUTH_MOUNT}"))
        .await?;
    let transit = reader
        .get(&format!("sys/mounts/{OPENBAO_TRANSIT_MOUNT}"))
        .await?;
    let kv = reader
        .get(&format!("sys/mounts/{OPENBAO_KV_MOUNT}"))
        .await?;
    if auth.0.pointer("/data/type").and_then(Value::as_str) != Some("cert")
        || transit.0.pointer("/data/type").and_then(Value::as_str) != Some("transit")
        || kv.0.pointer("/data/type").and_then(Value::as_str) != Some("kv")
        || kv
            .0
            .pointer("/data/options/version")
            .and_then(Value::as_str)
            != Some("2")
    {
        return Err(invalid());
    }
    let role = OpenBaoInstallationRole::Initializer;
    let client = BaoClientConfigV1 {
        schema_version: 1,
        endpoint: reader.origin.clone(),
        expected_cluster_id: cluster,
        auth_mount: OPENBAO_AUTH_MOUNT.into(),
        auth_mount_accessor: field(&auth.0, "/data/accessor", 128)?,
        auth_role: role.name().into(),
        expected_token_policies: vec![role.name().into()],
        ca_file: private_path.join("ca.pem").display().to_string(),
        client_certificate_file: private_path
            .join(openbao_profile::certificate_file(role))
            .display()
            .to_string(),
        client_private_key_file: private_path
            .join(openbao_profile::private_key_file(role))
            .display()
            .to_string(),
        connect_timeout_milliseconds: 5000,
        operation_timeout_milliseconds: 30000,
        maximum_response_bytes: LIMIT,
    };
    client.validate().map_err(|_| invalid())?;
    let placeholder: Sha256Digest = format!("sha256:{}", "0".repeat(64))
        .parse()
        .map_err(|_| invalid())?;
    let key = |name: &str| -> Result<TransitBindingV1, InstallationError> {
        let mut key = TransitBindingV1 {
            schema_version: 1,
            mount: OPENBAO_TRANSIT_MOUNT.into(),
            mount_accessor: field(&transit.0, "/data/accessor", 128)?,
            name: name.into(),
            key_version: 1,
            identity_digest: placeholder.clone(),
        };
        key.identity_digest = key
            .calculated_digest(&client.expected_cluster_id)
            .map_err(|_| invalid())?;
        Ok(key)
    };
    let artifact_key = key(OPENBAO_ARTIFACT_KEY)?;
    let secret_key = key(OPENBAO_SECRET_KEY)?;
    let mut secrets = KvV2BindingV1 {
        schema_version: 1,
        mount: OPENBAO_KV_MOUNT.into(),
        mount_accessor: field(&kv.0, "/data/accessor", 128)?,
        identity_digest: placeholder,
    };
    secrets.identity_digest = secrets
        .calculated_digest(&client.expected_cluster_id)
        .map_err(|_| invalid())?;
    let current = BaoClient::install(client.clone()).map_err(provider_error)?;
    current
        .check_transit(&artifact_key, reader.deadline)
        .await
        .map_err(provider_error)?;
    current
        .check_transit(&secret_key, reader.deadline)
        .await
        .map_err(provider_error)?;
    current
        .check_kv(&secrets, reader.deadline)
        .await
        .map_err(provider_error)?;
    let canary = current
        .read_exact(
            &secrets,
            &BaoSecretPath::parse(OPENBAO_CANARY_PATH).map_err(provider_error)?,
            1,
            reader.deadline,
        )
        .await
        .map_err(provider_error)?;
    let expected = prepared.provider_documents()?.canary;
    let actual =
        ProtectedJson(parse_strict_json(canary.bytes.as_bytes(), LIMITS).map_err(|_| invalid())?);
    let expected = parse_strict_json(&expected, LIMITS).map_err(|_| invalid())?;
    if actual.0 != expected {
        return Err(invalid());
    }
    for role in OpenBaoInstallationRole::ALL {
        let policy = reader
            .get(&format!("sys/policies/acl/{}", role.name()))
            .await?;
        if policy.0.pointer("/data/policy").and_then(Value::as_str)
            != Some(openbao_profile::policy(*role).as_str())
        {
            return Err(invalid());
        }
        let certificate = reader
            .get(&format!("auth/{OPENBAO_AUTH_MOUNT}/certs/{}", role.name()))
            .await?;
        let bytes = prepared
            .directory()
            .read(&openbao_profile::certificate_file(*role), 16_384)?
            .ok_or(InstallationError::Incomplete)?;
        if certificate
            .0
            .pointer("/data/certificate")
            .and_then(Value::as_str)
            .map(str::trim)
            != std::str::from_utf8(&bytes).ok().map(str::trim)
            || certificate
                .0
                .pointer("/data/token_no_default_policy")
                .and_then(Value::as_bool)
                != Some(true)
            || certificate.0.pointer("/data/token_policies")
                != Some(&serde_json::json!([role.name()]))
            || certificate.0.pointer("/data/allowed_uri_sans")
                != Some(&serde_json::json!([openbao_profile::workload_identity(
                    *role
                )]))
        {
            return Err(invalid());
        }
    }
    let evidence = InstallationProviderReadyV1 {
        client,
        artifact_key,
        secret_key,
        secrets,
        canary_version: 1,
        canary_digest: canonical(&expected)?,
    };
    evidence.validate_for(prepared.input())?;
    if let InstallationProviderStateV1::ProviderReady { evidence: original } = state.state {
        if *original != evidence {
            return Err(invalid());
        }
    }
    Ok(evidence)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn transient_provider_failure_does_not_claim_persistent_identity_drift() {
        assert_eq!(
            provider_error(BaoError::Unavailable),
            InstallationError::PrerequisiteUnavailable
        );
        assert_eq!(
            provider_error(BaoError::UnknownOutcome),
            InstallationError::ExternalOutcomeUnknown
        );
        for error in [
            BaoError::InvalidConfig,
            BaoError::Denied,
            BaoError::NotFound,
            BaoError::Conflict,
            BaoError::InvalidEvidence,
        ] {
            assert_eq!(provider_error(error), InstallationError::ConfigurationDrift);
        }
    }
}
