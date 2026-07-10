use std::fmt;

use axum::http::{header::AUTHORIZATION, HeaderMap};

use crate::config::AuthConfig;

#[derive(Clone)]
pub enum ApiAuth {
    Disabled,
    Bearer { token: String },
}

impl ApiAuth {
    pub fn disabled() -> Self {
        Self::Disabled
    }

    pub fn bearer_token(token: impl Into<String>) -> Self {
        Self::Bearer {
            token: token.into(),
        }
    }

    pub(crate) fn accepts(&self, headers: &HeaderMap) -> bool {
        match self {
            Self::Disabled => true,
            Self::Bearer { token } => headers
                .get(AUTHORIZATION)
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.strip_prefix("Bearer "))
                .is_some_and(|candidate| constant_time_eq(candidate.as_bytes(), token.as_bytes())),
        }
    }
}

impl From<&AuthConfig> for ApiAuth {
    fn from(config: &AuthConfig) -> Self {
        match config {
            AuthConfig::Disabled => Self::Disabled,
            AuthConfig::Bearer { token } => Self::bearer_token(token.expose()),
        }
    }
}

impl fmt::Debug for ApiAuth {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Disabled => formatter.write_str("Disabled"),
            Self::Bearer { .. } => formatter
                .debug_struct("Bearer")
                .field("token", &"[REDACTED]")
                .finish(),
        }
    }
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    let mut difference = left.len() ^ right.len();
    let length = left.len().max(right.len());
    for index in 0..length {
        let left_byte = left.get(index).copied().unwrap_or_default();
        let right_byte = right.get(index).copied().unwrap_or_default();
        difference |= usize::from(left_byte ^ right_byte);
    }
    difference == 0
}
