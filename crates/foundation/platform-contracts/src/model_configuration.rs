//! Shared model-configuration input contracts. These inputs grant no outbound access.
//!
//! Vendor names, source aliases, transport protocols and environment variable names are separate
//! facts. Environment values and plaintext credentials are deliberately absent from this module.

use crate::{CanonicalHttpEndpoint, CapabilityEndpointScheme};
use serde::{de, Deserialize, Deserializer, Serialize};
use std::{error::Error, fmt, str::FromStr};

pub const MAX_RESOURCE_ALIAS_BYTES: usize = 64;
pub const MAX_ENVIRONMENT_VARIABLE_NAME_BYTES: usize = 128;
pub const MAX_MODEL_BASE_URL_BYTES: usize = 2_048;

/// Immutable name within a tenant and Registry resource kind. Display names remain independent.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
pub struct ResourceAlias(String);

impl ResourceAlias {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl FromStr for ResourceAlias {
    type Err = ModelConfigurationInputError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.is_empty()
            || value.len() > MAX_RESOURCE_ALIAS_BYTES
            || !value.as_bytes()[0].is_ascii_lowercase()
            || !value.bytes().all(|byte| {
                byte.is_ascii_lowercase()
                    || byte.is_ascii_digit()
                    || matches!(byte, b'-' | b'_' | b'.')
            })
        {
            return Err(ModelConfigurationInputError::InvalidAlias);
        }
        Ok(Self(value.to_owned()))
    }
}

impl<'de> Deserialize<'de> for ResourceAlias {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        String::deserialize(deserializer)?
            .parse()
            .map_err(de::Error::custom)
    }
}

/// A nominated input variable, never a credential value or a lookup expression.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(transparent)]
pub struct EnvironmentVariableName(String);

impl EnvironmentVariableName {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl FromStr for EnvironmentVariableName {
    type Err = ModelConfigurationInputError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.is_empty()
            || value.len() > MAX_ENVIRONMENT_VARIABLE_NAME_BYTES
            || !(value.as_bytes()[0].is_ascii_alphabetic() || value.starts_with('_'))
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
        {
            return Err(ModelConfigurationInputError::InvalidEnvironmentVariableName);
        }
        Ok(Self(value.to_owned()))
    }
}

impl<'de> Deserialize<'de> for EnvironmentVariableName {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        String::deserialize(deserializer)?
            .parse()
            .map_err(de::Error::custom)
    }
}

/// Explicit import mappings; presets may fill missing mappings before validation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelEnvironmentMappingV1 {
    pub schema_version: u16,
    pub api_key: EnvironmentVariableName,
    pub base_url: Option<EnvironmentVariableName>,
    pub model: Option<EnvironmentVariableName>,
}

impl ModelEnvironmentMappingV1 {
    pub fn validate(&self) -> Result<(), ModelConfigurationInputError> {
        let names: Vec<&str> = std::iter::once(self.api_key.as_str())
            .chain(self.base_url.as_ref().map(EnvironmentVariableName::as_str))
            .chain(self.model.as_ref().map(EnvironmentVariableName::as_str))
            .collect();
        let unique = names.iter().collect::<std::collections::BTreeSet<_>>();
        if self.schema_version != 1 || unique.len() != names.len() {
            return Err(ModelConfigurationInputError::InvalidEnvironmentMapping);
        }
        Ok(())
    }
}

/// Parse an SDK base URL into the existing broker base-path contract. `/v1` is supplied by the
/// installed Responses/Messages protocol exactly once. This is syntax normalization, not network
/// authorization: installed destination grants and the Egress DNS/TLS policy remain mandatory.
pub fn normalize_model_base_url(
    value: &str,
) -> Result<CanonicalHttpEndpoint, ModelConfigurationInputError> {
    if value.is_empty()
        || value.len() > MAX_MODEL_BASE_URL_BYTES
        || !value.is_ascii()
        || value
            .bytes()
            .any(|byte| byte.is_ascii_whitespace() || byte.is_ascii_control())
        || value.contains(['%', '\\', '?', '#', '@'])
        || !value.starts_with("https://")
        || value
            .split('/')
            .any(|segment| matches!(segment, "." | ".."))
    {
        return Err(ModelConfigurationInputError::InvalidBaseUrl);
    }
    let url = url::Url::parse(value).map_err(|_| ModelConfigurationInputError::InvalidBaseUrl)?;
    if !url.username().is_empty() || url.password().is_some() {
        return Err(ModelConfigurationInputError::InvalidBaseUrl);
    }
    if url.path().contains("//")
        || !url.path().bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'/' | b'-' | b'_' | b'.' | b'~')
        })
    {
        return Err(ModelConfigurationInputError::InvalidBaseUrl);
    }
    let path = url.path().strip_suffix('/').unwrap_or(url.path());
    if path.ends_with("/responses") || path.ends_with("/messages") {
        return Err(ModelConfigurationInputError::InvalidBaseUrl);
    }
    let base = path.strip_suffix("/v1").unwrap_or(path);
    let endpoint = CanonicalHttpEndpoint {
        scheme: CapabilityEndpointScheme::Https,
        host: url
            .host_str()
            .ok_or(ModelConfigurationInputError::InvalidBaseUrl)?
            .to_owned(),
        port: url
            .port_or_known_default()
            .filter(|port| *port != 0)
            .ok_or(ModelConfigurationInputError::InvalidBaseUrl)?,
        base_path: if base.is_empty() {
            "/".to_owned()
        } else {
            base.to_owned()
        },
    };
    endpoint
        .validate()
        .map_err(|_| ModelConfigurationInputError::InvalidBaseUrl)?;
    Ok(endpoint)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModelConfigurationInputError {
    InvalidAlias,
    InvalidEnvironmentVariableName,
    InvalidEnvironmentMapping,
    InvalidBaseUrl,
}

impl fmt::Display for ModelConfigurationInputError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidAlias => "resource alias is invalid",
            Self::InvalidEnvironmentVariableName => "environment variable name is invalid",
            Self::InvalidEnvironmentMapping => "environment mapping is invalid",
            Self::InvalidBaseUrl => "model base URL is invalid",
        })
    }
}

impl Error for ModelConfigurationInputError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sdk_v1_prefix_is_supplied_exactly_once() {
        let endpoint =
            normalize_model_base_url("https://dashscope.aliyuncs.com/compatible-mode/v1").unwrap();
        assert_eq!(endpoint.host, "dashscope.aliyuncs.com");
        assert_eq!(endpoint.base_path, "/compatible-mode");
        assert_eq!(endpoint.port, 443);
        assert_eq!(
            endpoint,
            normalize_model_base_url("https://dashscope.aliyuncs.com:443/compatible-mode/v1/")
                .unwrap()
        );
        assert_eq!(
            normalize_model_base_url("https://api.example.com/v1")
                .unwrap()
                .base_path,
            "/"
        );
        assert_eq!(
            normalize_model_base_url("https://api.example.com/")
                .unwrap()
                .base_path,
            "/"
        );
    }

    #[test]
    fn malformed_or_ambiguous_urls_fail_without_echoing_input() {
        for input in [
            "http://example.com/v1",
            "https://@example.com",
            "https://u:p@example.com",
            "https://example.com//",
            "https://example.com/prefix//",
            "https://example.com/a/../v1",
            "https://example.com/%2e/v1",
            "https://example.com/a;b/v1",
            "https://example.com/a:b/v1",
            "https://example.com/v1?key=secret",
            "https://example.com/v1#secret",
            "https://example.com:0/v1",
            " https://example.com/v1",
            "https://example.com/v1/responses",
            "https://example.com/v1/messages",
            "https://example.com\\@elsewhere/v1",
        ] {
            let error = normalize_model_base_url(input).unwrap_err();
            assert_eq!(error.to_string(), "model base URL is invalid");
        }
    }

    #[test]
    fn aliases_and_variable_names_have_independent_grammars() {
        assert!("aliyun-main".parse::<ResourceAlias>().is_ok());
        assert!("OPENAI_API_KEY".parse::<EnvironmentVariableName>().is_ok());
        assert!("VendorTwo_key".parse::<EnvironmentVariableName>().is_ok());
        for value in [
            "",
            "../x",
            "project/default-model",
            "Upper",
            "1model",
            "模型",
        ] {
            assert!(value.parse::<ResourceAlias>().is_err());
        }
        for value in ["", "1KEY", "$(cat key)", "key-name", "KEY\nVALUE"] {
            assert!(value.parse::<EnvironmentVariableName>().is_err());
        }
        let mut mapping = ModelEnvironmentMappingV1 {
            schema_version: 1,
            api_key: "OPENAI_API_KEY".parse().unwrap(),
            base_url: Some("OPENAI_BASE_URL".parse().unwrap()),
            model: Some("OPENAI_DEFAULT_MODEL".parse().unwrap()),
        };
        mapping.validate().unwrap();
        mapping.model = Some(mapping.api_key.clone());
        assert_eq!(
            mapping.validate(),
            Err(ModelConfigurationInputError::InvalidEnvironmentMapping)
        );
    }
}
