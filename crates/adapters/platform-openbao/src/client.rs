use crate::{
    sensitive::SensitiveJson, BaoClientConfigV1, BaoError, SensitiveBytes, MAX_PRIVATE_FILE_BYTES,
    MAX_PROVIDER_REQUEST_BYTES,
};
use insight_platform_contracts::{parse_strict_json, JsonLimits};
use reqwest::{header::HeaderValue, Method};
use serde_json::Value;
use std::{io::Read, sync::Arc, time::Duration};
use tokio::{sync::Mutex, time::Instant};
use zeroize::Zeroize;

pub(crate) const MAX_TOKEN_SECONDS: u64 = 900;

#[derive(Clone)]
pub struct BaoClient(Arc<ClientInner>);

struct ClientInner {
    config: BaoClientConfigV1,
    http: reqwest::Client,
    token: Mutex<Option<Token>>,
}

struct Token {
    value: String,
    expires_at: Instant,
}

impl Drop for Token {
    fn drop(&mut self) {
        self.value.zeroize();
    }
}

impl std::fmt::Debug for BaoClient {
    fn fmt(&self, output: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        output.write_str("BaoClient([redacted])")
    }
}

impl BaoClient {
    /// Reads only the explicitly installed files; never loads environment credentials or proxies.
    pub fn install(config: BaoClientConfigV1) -> Result<Self, BaoError> {
        config.validate()?;
        let ca = read_file(&config.ca_file, false)?;
        let certificate = read_file(&config.client_certificate_file, false)?;
        let key = read_file(&config.client_private_key_file, true)?;
        let certificates = reqwest::Certificate::from_pem_bundle(ca.as_bytes())
            .map_err(|_| BaoError::InvalidConfig)?;
        if certificates.is_empty() {
            return Err(BaoError::InvalidConfig);
        }
        let mut identity = certificate.into_bytes();
        identity.push(b'\n');
        identity.extend_from_slice(key.as_bytes());
        let identity = SensitiveBytes::new(identity)?;
        let identity = reqwest::Identity::from_pem(identity.as_bytes())
            .map_err(|_| BaoError::InvalidConfig)?;
        let http = reqwest::Client::builder()
            .tls_backend_rustls()
            .tls_certs_only(certificates)
            .identity(identity)
            .tls_sslkeylogfile(false)
            .tls_version_min(reqwest::tls::Version::TLS_1_2)
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .no_proxy()
            .connect_timeout(Duration::from_millis(config.connect_timeout_milliseconds))
            .timeout(Duration::from_millis(config.operation_timeout_milliseconds))
            .build()
            .map_err(|_| BaoError::InvalidConfig)?;
        Ok(Self(Arc::new(ClientInner {
            config,
            http,
            token: Mutex::new(None),
        })))
    }

    pub fn config(&self) -> &BaoClientConfigV1 {
        &self.0.config
    }

    pub(crate) fn deadline(&self, caller: Instant) -> Result<Instant, BaoError> {
        let now = Instant::now();
        if caller <= now {
            return Err(BaoError::Unavailable);
        }
        Ok(caller.min(now + Duration::from_millis(self.config().operation_timeout_milliseconds)))
    }

    /// These observations never change provider configuration or object state. Certificate
    /// authentication may create a short-lived provider token and provider-owned audit records.
    pub(crate) async fn check_identity(&self, deadline: Instant) -> Result<(), BaoError> {
        let health = self
            .request(Method::GET, "sys/health", None, None, false, deadline)
            .await
            .map_err(identity_error)?;
        if health.0.get("initialized").and_then(Value::as_bool) != Some(true)
            || health.0.get("sealed").and_then(Value::as_bool) != Some(false)
            || health.0.get("cluster_id").and_then(Value::as_str)
                != Some(self.config().expected_cluster_id.as_str())
        {
            return Err(BaoError::InvalidEvidence);
        }
        let mount = self
            .authorized(
                Method::GET,
                &format!("sys/mounts/auth/{}", self.config().auth_mount),
                None,
                false,
                deadline,
            )
            .await
            .map_err(identity_error)?;
        check_mount(&mount.0, &self.config().auth_mount_accessor, "cert", None)
    }

    pub(crate) async fn check_mount(
        &self,
        mount: &str,
        accessor: &str,
        kind: &str,
        version: Option<&str>,
        deadline: Instant,
    ) -> Result<(), BaoError> {
        self.check_identity(deadline).await?;
        let metadata = self
            .authorized(
                Method::GET,
                &format!("sys/mounts/{mount}"),
                None,
                false,
                deadline,
            )
            .await
            .map_err(identity_error)?;
        check_mount(&metadata.0, accessor, kind, version)
    }

    async fn token(&self, deadline: Instant) -> Result<HeaderValue, BaoError> {
        let mut cached = tokio::time::timeout_at(deadline, self.0.token.lock())
            .await
            .map_err(|_| BaoError::Unavailable)?;
        if cached
            .as_ref()
            .is_none_or(|token| token.expires_at <= Instant::now())
        {
            *cached = None;
            let started = Instant::now();
            let body = encode(serde_json::json!({"name": self.config().auth_role}))?;
            let login = self
                .request(
                    Method::POST,
                    &format!("auth/{}/login", self.config().auth_mount),
                    Some(body),
                    None,
                    false,
                    deadline,
                )
                .await?;
            *cached = Some(decode_token(&login.0, self.config(), started)?);
        }
        let token = cached.as_ref().ok_or(BaoError::Unavailable)?;
        if token.expires_at <= Instant::now() {
            return Err(BaoError::Unavailable);
        }
        let mut header =
            HeaderValue::from_str(&token.value).map_err(|_| BaoError::InvalidEvidence)?;
        header.set_sensitive(true);
        Ok(header)
    }

    pub(crate) async fn authorized(
        &self,
        method: Method,
        path: &str,
        body: Option<SensitiveBytes>,
        uncertain_write: bool,
        deadline: Instant,
    ) -> Result<SensitiveJson, BaoError> {
        let token = self.token(deadline).await?;
        let result = self
            .request(method, path, body, Some(token), uncertain_write, deadline)
            .await;
        if matches!(result, Err(BaoError::Denied)) {
            // A later caller may authenticate again. This call is never resent.
            if let Ok(mut cached) = self.0.token.try_lock() {
                *cached = None;
            }
        }
        result
    }

    async fn request(
        &self,
        method: Method,
        path: &str,
        body: Option<SensitiveBytes>,
        token: Option<HeaderValue>,
        uncertain_write: bool,
        deadline: Instant,
    ) -> Result<SensitiveJson, BaoError> {
        if Instant::now() >= deadline {
            return Err(BaoError::Unavailable);
        }
        let mut request = self
            .0
            .http
            .request(method, format!("{}/v1/{path}", self.config().endpoint));
        if let Some(token) = token {
            request = request.header("X-Vault-Token", token);
        }
        if let Some(body) = body {
            if body.as_bytes().len() > MAX_PROVIDER_REQUEST_BYTES {
                return Err(BaoError::InvalidConfig);
            }
            request = request
                .header("content-type", "application/json")
                .body(body.into_bytes());
        }
        let uncertain = if uncertain_write {
            BaoError::UnknownOutcome
        } else {
            BaoError::Unavailable
        };
        tokio::time::timeout_at(deadline, async {
            let mut response = request.send().await.map_err(|_| uncertain)?;
            let status = response.status().as_u16();
            // Do not read or propagate vendor error messages; a generic 400 is not CAS evidence.
            match status {
                200 | 204 => {}
                401 | 403 => return Err(BaoError::Denied),
                404 => {
                    return Err(if uncertain_write {
                        BaoError::UnknownOutcome
                    } else {
                        BaoError::NotFound
                    })
                }
                409 | 412 => return Err(BaoError::Conflict),
                400 => {
                    return Err(if uncertain_write {
                        BaoError::UnknownOutcome
                    } else {
                        BaoError::InvalidEvidence
                    })
                }
                300..=399 => {
                    return Err(if uncertain_write {
                        BaoError::UnknownOutcome
                    } else {
                        BaoError::InvalidEvidence
                    })
                }
                _ => return Err(uncertain),
            }
            if status == 204 {
                return Ok(SensitiveJson(serde_json::json!({})));
            }
            let malformed = if uncertain_write {
                BaoError::UnknownOutcome
            } else {
                BaoError::InvalidEvidence
            };
            if response
                .content_length()
                .is_some_and(|length| length > self.config().maximum_response_bytes as u64)
            {
                return Err(malformed);
            }
            let mut bytes = zeroize::Zeroizing::new(Vec::new());
            while let Some(chunk) = response.chunk().await.map_err(|_| uncertain)? {
                if bytes.len().saturating_add(chunk.len()) > self.config().maximum_response_bytes {
                    return Err(malformed);
                }
                bytes.extend_from_slice(&chunk);
            }
            let value = parse_strict_json(&bytes, limits(self.config().maximum_response_bytes))
                .map_err(|_| malformed)?;
            if !value.is_object() {
                return Err(malformed);
            }
            Ok(SensitiveJson(value))
        })
        .await
        .map_err(|_| uncertain)?
    }
}

pub(crate) fn encode(value: Value) -> Result<SensitiveBytes, BaoError> {
    let value = SensitiveJson(value);
    let bytes = serde_jcs::to_vec(&value.0).map_err(|_| BaoError::InvalidEvidence)?;
    SensitiveBytes::new(bytes)
}

pub(crate) fn limits(maximum: usize) -> JsonLimits {
    JsonLimits {
        max_bytes: maximum,
        max_depth: 16,
        max_properties_per_object: 128,
        max_items_per_array: 128,
        max_string_bytes: maximum,
    }
}

fn decode_token(
    value: &Value,
    config: &BaoClientConfigV1,
    started: Instant,
) -> Result<Token, BaoError> {
    let auth = value.get("auth").ok_or(BaoError::InvalidEvidence)?;
    let value = auth
        .get("client_token")
        .and_then(Value::as_str)
        .ok_or(BaoError::InvalidEvidence)?;
    let ttl = auth
        .get("lease_duration")
        .and_then(Value::as_u64)
        .ok_or(BaoError::InvalidEvidence)?;
    let policies = auth
        .get("token_policies")
        .and_then(Value::as_array)
        .ok_or(BaoError::InvalidEvidence)?;
    let mut observed = policies
        .iter()
        .map(|policy| policy.as_str().ok_or(BaoError::InvalidEvidence))
        .collect::<Result<Vec<_>, _>>()?;
    observed.sort_unstable();
    let combined = auth
        .get("policies")
        .and_then(Value::as_array)
        .ok_or(BaoError::InvalidEvidence)?;
    let mut combined = combined
        .iter()
        .map(|policy| policy.as_str().ok_or(BaoError::InvalidEvidence))
        .collect::<Result<Vec<_>, _>>()?;
    combined.sort_unstable();
    if !(8..=4096).contains(&value.len())
        || !value.bytes().all(|byte| byte.is_ascii_graphic())
        || !(2..=MAX_TOKEN_SECONDS).contains(&ttl)
        || observed
            != config
                .expected_token_policies
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>()
        || combined != observed
    {
        return Err(BaoError::InvalidEvidence);
    }
    Ok(Token {
        value: value.to_owned(),
        expires_at: started + Duration::from_secs(ttl - 1),
    })
}

fn check_mount(
    value: &Value,
    accessor: &str,
    kind: &str,
    version: Option<&str>,
) -> Result<(), BaoError> {
    let data = value.get("data").ok_or(BaoError::InvalidEvidence)?;
    if data.get("accessor").and_then(Value::as_str) != Some(accessor)
        || data.get("type").and_then(Value::as_str) != Some(kind)
        || version.is_some_and(|version| {
            data.pointer("/options/version").and_then(Value::as_str) != Some(version)
        })
    {
        return Err(BaoError::InvalidEvidence);
    }
    Ok(())
}

fn identity_error(error: BaoError) -> BaoError {
    if error == BaoError::NotFound {
        BaoError::InvalidEvidence
    } else {
        error
    }
}

#[cfg(unix)]
fn read_file(path: &str, private: bool) -> Result<SensitiveBytes, BaoError> {
    use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK)
        .open(path)
        .map_err(|_| BaoError::InvalidConfig)?;
    let metadata = file.metadata().map_err(|_| BaoError::InvalidConfig)?;
    // Public certificates may be readable; no provider file may be group/world writable.
    let unsafe_bits = if private { 0o077 } else { 0o022 };
    if !metadata.is_file()
        || metadata.len() == 0
        || metadata.len() > MAX_PRIVATE_FILE_BYTES as u64
        || metadata.permissions().mode() & unsafe_bits != 0
        || metadata.nlink() != 1
    {
        return Err(BaoError::InvalidConfig);
    }
    let mut bytes = zeroize::Zeroizing::new(Vec::new());
    (&mut file)
        .take(MAX_PRIVATE_FILE_BYTES as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| BaoError::InvalidConfig)?;
    if bytes.len() > MAX_PRIVATE_FILE_BYTES {
        return Err(BaoError::InvalidConfig);
    }
    SensitiveBytes::new(std::mem::take(&mut *bytes))
}

#[cfg(not(unix))]
fn read_file(_path: &str, _private: bool) -> Result<SensitiveBytes, BaoError> {
    Err(BaoError::InvalidConfig)
}
