//! Compatibility facade for API-owned authentication primitives.

pub use insight_api::formal::{
    ApiAuth, BearerHumanPrincipalResolver, HumanPrincipalResolver, ResolvedHumanPrincipal,
};

use crate::config::AuthConfig;

impl From<&AuthConfig> for ApiAuth {
    fn from(config: &AuthConfig) -> Self {
        match config {
            AuthConfig::Disabled => Self::disabled(),
            AuthConfig::Bearer { token } => Self::bearer_token(token.expose()),
        }
    }
}
