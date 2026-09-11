//! Installed header mapping. Exact SecretBinding identities remain in business deployments.
use crate::SecretPurpose;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum InstalledHttpCredentialInjection {
    BearerAuthorization {
        purpose: SecretPurpose,
    },
    Header {
        purpose: SecretPurpose,
        name: String,
    },
}
impl InstalledHttpCredentialInjection {
    pub fn purpose(&self) -> &SecretPurpose {
        match self {
            Self::BearerAuthorization { purpose } | Self::Header { purpose, .. } => purpose,
        }
    }
    pub fn header_name(&self) -> &str {
        match self {
            Self::BearerAuthorization { .. } => "authorization",
            Self::Header { name, .. } => name,
        }
    }
    pub fn validate_shape(&self) -> bool {
        match self {
            Self::BearerAuthorization { .. } => true,
            Self::Header { name, .. } => {
                crate::capability::valid_http_header_name(name)
                    && name == &name.to_ascii_lowercase()
                    && !matches!(
                        name.as_str(),
                        "authorization"
                            | "connection"
                            | "content-length"
                            | "cookie"
                            | "host"
                            | "proxy-authorization"
                            | "set-cookie"
                            | "transfer-encoding"
                    )
            }
        }
    }
}
