//! Owning process contract for the installed OpenBao physical transport.
use insight_platform_contracts::{canonical_digest, Sha256Digest};
use serde::{Deserialize, Serialize};
use std::path::{Component, Path};
use url::Url;

pub const MAX_PROVIDER_RESPONSE_BYTES: usize = 256 * 1024;
pub const MAX_PROVIDER_REQUEST_BYTES: usize = 128 * 1024;
pub const MAX_PRIVATE_FILE_BYTES: usize = 64 * 1024;
pub const MAX_OPERATION_TIMEOUT_MILLISECONDS: u64 = 30_000;
pub const MAX_SECRET_PATH_BYTES: usize = 512;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BaoError {
    InvalidConfig,
    Unavailable,
    Denied,
    NotFound,
    Conflict,
    InvalidEvidence,
    UnknownOutcome,
}

impl std::fmt::Display for BaoError {
    fn fmt(&self, output: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        output.write_str(match self {
            Self::InvalidConfig => "OpenBao configuration rejected",
            Self::Unavailable => "OpenBao unavailable",
            Self::Denied => "OpenBao access denied",
            Self::NotFound => "OpenBao exact object absent",
            Self::Conflict => "OpenBao conditional write conflicted",
            Self::InvalidEvidence => "OpenBao evidence rejected",
            Self::UnknownOutcome => "OpenBao write outcome unknown",
        })
    }
}
impl std::error::Error for BaoError {}

/// All credential values are delivered through explicitly named role-local files.
/// Token policies are an exact login-response expectation, never a requested grant.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BaoClientConfigV1 {
    pub schema_version: u32,
    pub endpoint: String,
    pub expected_cluster_id: String,
    pub auth_mount: String,
    pub auth_mount_accessor: String,
    pub auth_role: String,
    pub expected_token_policies: Vec<String>,
    pub ca_file: String,
    pub client_certificate_file: String,
    pub client_private_key_file: String,
    pub connect_timeout_milliseconds: u64,
    pub operation_timeout_milliseconds: u64,
    pub maximum_response_bytes: usize,
}

impl BaoClientConfigV1 {
    pub fn validate(&self) -> Result<(), BaoError> {
        let endpoint = Url::parse(&self.endpoint).map_err(|_| BaoError::InvalidConfig)?;
        if self.schema_version != 1
            || self.endpoint.len() > 2_048
            || endpoint.scheme() != "https"
            || endpoint.host_str().is_none()
            || !endpoint.username().is_empty()
            || endpoint.password().is_some()
            || endpoint.query().is_some()
            || endpoint.fragment().is_some()
            || endpoint.path() != "/"
            || self.endpoint != endpoint.origin().ascii_serialization()
            || !valid_cluster_id(&self.expected_cluster_id)
            || !valid_segment(&self.auth_mount, 64)
            || !valid_segment(&self.auth_mount_accessor, 128)
            || !valid_segment(&self.auth_role, 128)
            || self.expected_token_policies.is_empty()
            || self.expected_token_policies.len() > 8
            || self.expected_token_policies.iter().any(|policy| {
                !valid_segment(policy, 128) || matches!(policy.as_str(), "root" | "default")
            })
            || self
                .expected_token_policies
                .windows(2)
                .any(|pair| pair[0] >= pair[1])
            || !valid_file_path(&self.ca_file)
            || !valid_file_path(&self.client_certificate_file)
            || !valid_file_path(&self.client_private_key_file)
            || self.ca_file == self.client_certificate_file
            || self.ca_file == self.client_private_key_file
            || self.client_certificate_file == self.client_private_key_file
            || self.connect_timeout_milliseconds == 0
            || self.connect_timeout_milliseconds > self.operation_timeout_milliseconds
            || self.operation_timeout_milliseconds > MAX_OPERATION_TIMEOUT_MILLISECONDS
            || !(1_024..=MAX_PROVIDER_RESPONSE_BYTES).contains(&self.maximum_response_bytes)
        {
            return Err(BaoError::InvalidConfig);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TransitBindingV1 {
    pub schema_version: u32,
    pub mount: String,
    pub mount_accessor: String,
    pub name: String,
    pub key_version: u32,
    pub identity_digest: Sha256Digest,
}

impl TransitBindingV1 {
    pub fn calculated_digest(&self, cluster_id: &str) -> Result<Sha256Digest, BaoError> {
        digest(&serde_json::json!({
            "schema_version": self.schema_version,
            "provider": "openbao_transit",
            "cluster_id": cluster_id,
            "mount": self.mount,
            "mount_accessor": self.mount_accessor,
            "name": self.name,
            "key_version": self.key_version,
        }))
    }

    pub fn validate_for(&self, client: &BaoClientConfigV1) -> Result<(), BaoError> {
        client.validate()?;
        if self.schema_version != 1
            || !valid_segment(&self.mount, 64)
            || !valid_segment(&self.mount_accessor, 128)
            || !valid_segment(&self.name, 128)
            || self.key_version == 0
            || self.key_version > i32::MAX as u32
            || self.calculated_digest(&client.expected_cluster_id)? != self.identity_digest
        {
            return Err(BaoError::InvalidConfig);
        }
        Ok(())
    }

    /// A provider identity, not an AWS ARN or a mutable Transit key alias.
    pub fn key_id(&self) -> String {
        format!(
            "openbao:transit:{}:v{}",
            self.identity_digest, self.key_version
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KvV2BindingV1 {
    pub schema_version: u32,
    pub mount: String,
    pub mount_accessor: String,
    pub identity_digest: Sha256Digest,
}

impl KvV2BindingV1 {
    pub fn calculated_digest(&self, cluster_id: &str) -> Result<Sha256Digest, BaoError> {
        digest(&serde_json::json!({
            "schema_version": self.schema_version,
            "provider": "openbao_kv_v2",
            "cluster_id": cluster_id,
            "mount": self.mount,
            "mount_accessor": self.mount_accessor,
        }))
    }

    pub fn validate_for(&self, client: &BaoClientConfigV1) -> Result<(), BaoError> {
        client.validate()?;
        if self.schema_version != 1
            || !valid_segment(&self.mount, 64)
            || !valid_segment(&self.mount_accessor, 128)
            || self.calculated_digest(&client.expected_cluster_id)? != self.identity_digest
        {
            return Err(BaoError::InvalidConfig);
        }
        Ok(())
    }
}

/// Relative KV path only; callers cannot provide a URL, mount or query component.
#[derive(Clone, PartialEq, Eq)]
pub struct BaoSecretPath(String);

impl std::fmt::Debug for BaoSecretPath {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("BaoSecretPath([redacted])")
    }
}

impl BaoSecretPath {
    pub fn parse(value: &str) -> Result<Self, BaoError> {
        if value.is_empty()
            || value.len() > MAX_SECRET_PATH_BYTES
            || value.split('/').any(|part| !valid_segment(part, 128))
        {
            return Err(BaoError::InvalidConfig);
        }
        Ok(Self(value.to_owned()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

pub fn valid_segment(value: &str, maximum: usize) -> bool {
    !value.is_empty()
        && value.len() <= maximum
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
}

fn valid_cluster_id(value: &str) -> bool {
    value.len() == 36
        && value.bytes().enumerate().all(|(index, byte)| {
            if matches!(index, 8 | 13 | 18 | 23) {
                byte == b'-'
            } else {
                byte.is_ascii_digit() || matches!(byte, b'a'..=b'f')
            }
        })
        && value != "00000000-0000-0000-0000-000000000000"
}

fn valid_file_path(value: &str) -> bool {
    let path = Path::new(value);
    !value.is_empty()
        && value.len() <= 4_096
        && !value.chars().any(char::is_control)
        && path.is_absolute()
        && !value.contains("//")
        && !value.ends_with('/')
        && !value.split('/').any(|part| matches!(part, "." | ".."))
        && path
            .components()
            .all(|part| matches!(part, Component::RootDir | Component::Normal(_)))
}

fn digest(value: &serde_json::Value) -> Result<Sha256Digest, BaoError> {
    canonical_digest(value)
        .map_err(|_| BaoError::InvalidConfig)?
        .parse()
        .map_err(|_| BaoError::InvalidConfig)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> BaoClientConfigV1 {
        BaoClientConfigV1 {
            schema_version: 1,
            endpoint: "https://openbao:8200".into(),
            expected_cluster_id: "092036e7-f9ab-41fd-8122-077ea91db8b0".into(),
            auth_mount: "insight-cert".into(),
            auth_mount_accessor: "auth_cert_a12b34".into(),
            auth_role: "insight-egress".into(),
            expected_token_policies: vec!["insight-egress".into()],
            ca_file: "/private/role/ca.pem".into(),
            client_certificate_file: "/private/role/client.pem".into(),
            client_private_key_file: "/private/role/client-key.pem".into(),
            connect_timeout_milliseconds: 1000,
            operation_timeout_milliseconds: 3000,
            maximum_response_bytes: 131072,
        }
    }

    #[test]
    fn installed_origin_files_and_auth_policy_have_no_implicit_fallback() {
        assert_eq!(config().validate(), Ok(()));
        for endpoint in [
            "http://openbao:8200",
            "https://openbao:8200/",
            "https://user@openbao:8200",
            "https://openbao:8200/v1",
            "https://openbao:8200?key=x",
            "https://openbao:8200#x",
        ] {
            let mut changed = config();
            changed.endpoint = endpoint.into();
            assert_eq!(changed.validate(), Err(BaoError::InvalidConfig));
        }
        for path in [
            "relative",
            "/private/../key",
            "/private/./key",
            "/private//key",
            "/private/key/",
            "/private/key\n",
        ] {
            let mut changed = config();
            changed.client_private_key_file = path.into();
            assert_eq!(changed.validate(), Err(BaoError::InvalidConfig));
        }
        for policies in [
            vec![],
            vec!["root"],
            vec!["default"],
            vec!["z", "a"],
            vec!["a", "a"],
        ] {
            let mut changed = config();
            changed.expected_token_policies = policies.into_iter().map(str::to_owned).collect();
            assert_eq!(changed.validate(), Err(BaoError::InvalidConfig));
        }
    }

    #[test]
    fn physical_key_identity_survives_role_paths_but_not_cluster_mount_or_version_changes() {
        let client = config();
        let mut key = TransitBindingV1 {
            schema_version: 1,
            mount: "transit".into(),
            mount_accessor: "transit_abc".into(),
            name: "artifact-reference".into(),
            key_version: 1,
            identity_digest: format!("sha256:{}", "a".repeat(64)).parse().unwrap(),
        };
        key.identity_digest = key.calculated_digest(&client.expected_cluster_id).unwrap();
        assert_eq!(key.validate_for(&client), Ok(()));
        let mut other_role = client.clone();
        other_role.auth_role = "artifact-reader".into();
        other_role.client_private_key_file = "/private/reader/key.pem".into();
        assert_eq!(key.validate_for(&other_role), Ok(()));
        let mut other_cluster = client.clone();
        other_cluster.expected_cluster_id = "192036e7-f9ab-41fd-8122-077ea91db8b0".into();
        assert_eq!(
            key.validate_for(&other_cluster),
            Err(BaoError::InvalidConfig)
        );
        let mut replacement = key.clone();
        replacement.mount_accessor = "transit_def".into();
        assert_eq!(
            replacement.validate_for(&client),
            Err(BaoError::InvalidConfig)
        );
        let mut replacement = key.clone();
        replacement.key_version = 2;
        assert_eq!(
            replacement.validate_for(&client),
            Err(BaoError::InvalidConfig)
        );
        assert!(key.key_id().len() <= 255);
        assert!(!key.key_id().starts_with("arn:"));
    }

    #[test]
    fn secret_paths_cannot_change_the_installed_mount_or_supply_queries() {
        assert!(BaoSecretPath::parse("prepared/tenant-1/request_1").is_ok());
        for path in [
            "",
            "/prepared",
            "a/",
            "a//b",
            "a/../b",
            "a/./b",
            "a%2Fb",
            "a?version=2",
            "https://example.com",
            "a\\b",
        ] {
            assert!(BaoSecretPath::parse(path).is_err());
        }
        assert_eq!(
            format!("{:?}", BaoSecretPath::parse("canary").unwrap()),
            "BaoSecretPath([redacted])"
        );
    }
}
