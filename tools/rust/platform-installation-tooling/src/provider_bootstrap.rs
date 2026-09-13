//! Finite initialization of one ordinary OpenBao server. No workload lifecycle operations.
use crate::openbao_setup::{self, BootstrapReader};
use insight_platform_contracts::{parse_strict_json, Sha256Digest};
use insight_platform_deployment_contracts::{installation::*, installation_provider::*};
use insight_platform_deployment_tooling::installation::PreparedInstallation;
use reqwest::{header::HeaderValue, Method, StatusCode};
use serde_json::Value;
use std::{path::Path, time::Duration};
use tokio::time::Instant;
use zeroize::{Zeroize, Zeroizing};

const CREDENTIALS: &str = "openbao-bootstrap-credentials.json";

fn credentials(
    prepared: &PreparedInstallation,
) -> Result<OpenBaoBootstrapCredentialsV1, InstallationError> {
    let bytes = Zeroizing::new(
        prepared
            .directory()
            .read(CREDENTIALS, 16384)?
            .ok_or(InstallationError::ExternalOutcomeUnknown)?,
    );
    let value = parse_strict_json(&bytes, INSTALLATION_LIMITS)
        .map_err(|_| InstallationError::CredentialInvalid)?;
    let material: OpenBaoBootstrapCredentialsV1 =
        serde_json::from_value(value).map_err(|_| InstallationError::CredentialInvalid)?;
    material.validate_for(prepared.input(), prepared.identity())?;
    Ok(material)
}

fn persist(
    prepared: &PreparedInstallation,
    material: &OpenBaoBootstrapCredentialsV1,
) -> Result<(), InstallationError> {
    material.validate_for(prepared.input(), prepared.identity())?;
    let bytes =
        Zeroizing::new(serde_json::to_vec(material).map_err(|_| InstallationError::InvalidInput)?);
    prepared.directory().replace(CREDENTIALS, &bytes)
}

async fn revoke(
    prepared: &PreparedInstallation,
    reader: &mut BootstrapReader,
) -> Result<(), InstallationError> {
    let mut material = credentials(prepared)?;
    if material.root_revoked {
        return Ok(());
    }
    let mut token = HeaderValue::from_str(&material.root_token)
        .map_err(|_| InstallationError::CredentialInvalid)?;
    token.set_sensitive(true);
    // A lost revoke response is resolved by the same token's authenticated lookup. Never mint
    // another token, adopt another cluster, or replay any provider configuration operation.
    let lookup = reader
        .client
        .get(format!("{}/v1/auth/token/lookup-self", reader.origin))
        .header("X-Vault-Token", token.clone())
        .send()
        .await
        .map_err(|_| InstallationError::ExternalOutcomeUnknown)?;
    match lookup.status() {
        StatusCode::FORBIDDEN => (),
        StatusCode::OK => {
            let response = reader
                .client
                .post(format!("{}/v1/auth/token/revoke-self", reader.origin))
                .header("X-Vault-Token", token)
                .send()
                .await
                .map_err(|_| InstallationError::ExternalOutcomeUnknown)?;
            if response.status() != StatusCode::NO_CONTENT {
                return Err(InstallationError::ExternalOutcomeUnknown);
            }
        }
        _ => return Err(InstallationError::ExternalOutcomeUnknown),
    }
    material.root_revoked = true;
    persist(prepared, &material)?;
    material.root_token.zeroize();
    for key in &mut material.recovery_keys_base64 {
        key.zeroize();
    }
    reader.token = None;
    Ok(())
}

async fn write_once(
    prepared: &PreparedInstallation,
    reader: &mut BootstrapReader,
) -> Result<(), InstallationError> {
    let response = reader
        .request(
            Method::PUT,
            "sys/init",
            Some(serde_json::json!({
                "recovery_shares": 1, "recovery_threshold": 1
            })),
        )
        .await
        .map_err(|_| InstallationError::ExternalOutcomeUnknown)?;
    let root = response
        .0
        .get("root_token")
        .and_then(Value::as_str)
        .ok_or(InstallationError::ExternalOutcomeUnknown)?;
    let keys = response
        .0
        .get("recovery_keys_base64")
        .and_then(Value::as_array)
        .ok_or(InstallationError::ExternalOutcomeUnknown)?;
    let mut material = OpenBaoBootstrapCredentialsV1 {
        schema_version: 1,
        input_digest: prepared.input().digest()?,
        identity_digest: prepared.identity().digest()?,
        root_token: root.into(),
        recovery_keys_base64: keys
            .iter()
            .map(|key| {
                key.as_str()
                    .map(str::to_owned)
                    .ok_or(InstallationError::ExternalOutcomeUnknown)
            })
            .collect::<Result<_, _>>()?,
        root_revoked: false,
    };
    persist(prepared, &material)?;
    let mut token =
        HeaderValue::from_str(root).map_err(|_| InstallationError::CredentialInvalid)?;
    token.set_sensitive(true);
    reader.token = Some(token);
    material.root_token.zeroize();
    for key in &mut material.recovery_keys_base64 {
        key.zeroize();
    }
    wait_active(reader).await?;
    let documents = prepared.provider_documents()?;
    let document: Value = serde_json::from_slice(&documents.initialize)
        .map_err(|_| InstallationError::InvalidInput)?;
    let requests = document["requests"]
        .as_array()
        .ok_or(InstallationError::InvalidInput)?;
    for entry in requests {
        let request = entry
            .as_object()
            .and_then(|entry| entry.values().next())
            .ok_or(InstallationError::InvalidInput)?;
        let path = request["path"]
            .as_str()
            .ok_or(InstallationError::InvalidInput)?;
        reader
            .request(Method::POST, path, Some(request["data"].clone()))
            .await
            .map_err(|_| InstallationError::ExternalOutcomeUnknown)?;
    }
    Ok(())
}

async fn wait_active(reader: &BootstrapReader) -> Result<(), InstallationError> {
    // /sys/init returning does not mean Raft has finished the transition from its bootstrap
    // leader to the ordinary active server. Only read health while waiting; never replay writes.
    loop {
        match reader.get("sys/health?standbyok=true").await {
            Ok(value)
                if value.0.get("initialized") == Some(&Value::Bool(true))
                    && value.0.get("sealed") == Some(&Value::Bool(false))
                    && value.0.get("standby") == Some(&Value::Bool(false)) =>
            {
                return Ok(())
            }
            Ok(_) | Err(InstallationError::PrerequisiteUnavailable) => (),
            Err(error) => return Err(error),
        }
        if Instant::now() >= reader.deadline {
            return Err(InstallationError::PrerequisiteUnavailable);
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

pub async fn bootstrap(
    input: &InstallationInputV1,
    state: &Path,
) -> Result<Sha256Digest, InstallationError> {
    let prepared = PreparedInstallation::open(input, state)?;
    let mut reader = BootstrapReader::install(&prepared)?;
    reader.deadline = Instant::now() + Duration::from_secs(INSTALLATION_STARTUP_SECONDS);
    initialize(&prepared, &mut reader).await?;
    let observed = openbao_setup::observe(&prepared, state).await?;
    prepared.complete_provider(observed)?;
    prepared.identity().digest()
}

async fn initialize(
    prepared: &PreparedInstallation,
    reader: &mut BootstrapReader,
) -> Result<(), InstallationError> {
    let current = prepared.provider_state()?;
    let initialized = loop {
        match reader.get("sys/init").await {
            Ok(value) => {
                break value
                    .0
                    .get("initialized")
                    .and_then(Value::as_bool)
                    .ok_or(InstallationError::ConfigurationDrift)?
            }
            Err(InstallationError::PrerequisiteUnavailable) if Instant::now() < reader.deadline => {
                tokio::time::sleep(Duration::from_millis(500)).await
            }
            Err(error) => return Err(error),
        }
    };
    match current.state {
        InstallationProviderStateV1::Prepared => {
            if initialized {
                return Err(InstallationError::ForeignState);
            }
            prepared.request_provider_start()?;
            let result = write_once(prepared, reader).await;
            // Cleanup is attempted after any configuration result, provided init material was
            // durably delivered. If init's response was lost, preserve the unknown attempt.
            if prepared.directory().read(CREDENTIALS, 16384)?.is_some() {
                revoke(prepared, reader).await?;
            }
            result?;
        }
        InstallationProviderStateV1::Requested => {
            if !initialized {
                return Err(InstallationError::ExternalOutcomeUnknown);
            }
            wait_active(reader).await?;
            revoke(prepared, reader).await?;
        }
        InstallationProviderStateV1::ProviderReady { .. } => {
            if !initialized {
                return Err(InstallationError::IdentityDrift);
            }
            wait_active(reader).await?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    async fn fixture_server(
        replies: Vec<(&'static str, u16, Value)>,
    ) -> (BootstrapReader, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap());
        let task = tokio::spawn(async move {
            for (expected, status, body) in replies {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut bytes = Vec::new();
                loop {
                    let mut chunk = [0; 4096];
                    let n = stream.read(&mut chunk).await.unwrap();
                    assert!(n > 0);
                    bytes.extend_from_slice(&chunk[..n]);
                    if let Some(end) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
                        let headers = String::from_utf8_lossy(&bytes[..end]);
                        let length: usize = headers
                            .lines()
                            .find_map(|line| {
                                line.to_ascii_lowercase()
                                    .strip_prefix("content-length: ")
                                    .map(str::to_owned)
                            })
                            .map(|v| v.parse().unwrap())
                            .unwrap_or(0);
                        if bytes.len() >= end + 4 + length {
                            break;
                        }
                    }
                }
                assert_eq!(
                    String::from_utf8_lossy(&bytes).lines().next().unwrap(),
                    format!("{expected} HTTP/1.1")
                );
                if status == 0 {
                    continue;
                } // The server accepted init, but its reply was lost.
                let body = if status == 204 {
                    String::new()
                } else {
                    body.to_string()
                };
                stream.write_all(format!("HTTP/1.1 {status} Test\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes()).await.unwrap();
            }
        });
        (
            BootstrapReader {
                client: reqwest::Client::builder()
                    .no_proxy()
                    .retry(reqwest::retry::never())
                    .build()
                    .unwrap(),
                origin,
                token: None,
                deadline: Instant::now() + Duration::from_secs(10),
            },
            task,
        )
    }
    fn prepared(root: &Path) -> PreparedInstallation {
        let input = insight_platform_deployment_tooling::installation::compose_input(
            "bootstrap-test",
            format!("sha256:{}", "a".repeat(64)).parse().unwrap(),
        )
        .unwrap();
        PreparedInstallation::prepare(&input, &root.canonicalize().unwrap().join("private"))
            .unwrap()
    }
    fn active() -> Value {
        serde_json::json!({"initialized":true,"sealed":false,"standby":false})
    }

    #[tokio::test]
    async fn failed_configuration_revokes_root_and_retry_never_repeats_writes() {
        let root = tempfile::tempdir().unwrap();
        let prepared = prepared(root.path());
        let (mut reader, task) = fixture_server(vec![
            ("GET /v1/sys/init",200,serde_json::json!({"initialized":false})),
            ("PUT /v1/sys/init",200,serde_json::json!({"root_token":"fixture-root","recovery_keys_base64":["fixture-recovery"]})),
            ("GET /v1/sys/health?standbyok=true",503,Value::Null),
            ("GET /v1/sys/health?standbyok=true",200,active()),
            ("POST /v1/sys/auth/insight-cert",500,Value::Null),
            ("GET /v1/auth/token/lookup-self",200,serde_json::json!({"data":{}})),
            ("POST /v1/auth/token/revoke-self",204,Value::Null),
            ("GET /v1/sys/init",200,serde_json::json!({"initialized":true})),
            ("GET /v1/sys/health?standbyok=true",200,active()),
        ]).await;
        assert_eq!(
            initialize(&prepared, &mut reader).await,
            Err(InstallationError::ExternalOutcomeUnknown)
        );
        assert!(credentials(&prepared).unwrap().root_revoked);
        assert_eq!(initialize(&prepared, &mut reader).await, Ok(()));
        assert_eq!(
            prepared.provider_state().unwrap().state,
            InstallationProviderStateV1::Requested
        );
        task.await.unwrap();
    }

    #[tokio::test]
    async fn lost_init_response_stays_unknown_without_another_initialization() {
        let root = tempfile::tempdir().unwrap();
        let prepared = prepared(root.path());
        let (mut reader, task) = fixture_server(vec![
            (
                "GET /v1/sys/init",
                200,
                serde_json::json!({"initialized":false}),
            ),
            ("PUT /v1/sys/init", 0, Value::Null),
            (
                "GET /v1/sys/init",
                200,
                serde_json::json!({"initialized":true}),
            ),
            ("GET /v1/sys/health?standbyok=true", 200, active()),
        ])
        .await;
        for _ in 0..2 {
            assert_eq!(
                initialize(&prepared, &mut reader).await,
                Err(InstallationError::ExternalOutcomeUnknown)
            );
        }
        assert!(prepared
            .directory()
            .read(CREDENTIALS, 16384)
            .unwrap()
            .is_none());
        assert_eq!(
            prepared.provider_state().unwrap().state,
            InstallationProviderStateV1::Requested
        );
        task.await.unwrap();
    }

    #[tokio::test]
    async fn prepared_installation_refuses_an_already_initialized_provider() {
        let root = tempfile::tempdir().unwrap();
        let prepared = prepared(root.path());
        let (mut reader, task) = fixture_server(vec![(
            "GET /v1/sys/init",
            200,
            serde_json::json!({"initialized":true}),
        )])
        .await;
        assert_eq!(
            initialize(&prepared, &mut reader).await,
            Err(InstallationError::ForeignState)
        );
        assert_eq!(
            prepared.provider_state().unwrap().state,
            InstallationProviderStateV1::Prepared
        );
        task.await.unwrap();
    }
}
