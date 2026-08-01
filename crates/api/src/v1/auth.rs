//! Authentication primitives for the v1 HTTP API.

use std::{fmt, sync::Arc};

use axum::http::{header::AUTHORIZATION, HeaderMap};
use std::collections::BTreeSet;

pub const MCP_SERVER_READ: &str = "mcp.server.read";
pub const MCP_SERVER_WRITE: &str = "mcp.server.write";
pub const MCP_SERVER_DISCOVER: &str = "mcp.server.discover";
pub const MCP_SERVER_PUBLISH: &str = "mcp.server.publish";
pub const AGENT_READ: &str = "agent.read";
pub const AGENT_WRITE: &str = "agent.write";
pub const AGENT_VALIDATE: &str = "agent.validate";
pub const AGENT_PUBLISH: &str = "agent.publish";
pub const AGENT_DEPLOY: &str = "agent.deploy";
pub const AGENT_ACTIVATE: &str = "agent.activate";
pub const AGENT_ARCHIVE: &str = "agent.archive";
pub const AGENT_DEBUG_SANDBOX: &str = "agent.debug.sandbox";
pub const AGENT_DEBUG_LIVE: &str = "agent.debug.live";
pub const PROVIDER_READ: &str = "provider.read";
pub const PROVIDER_WRITE: &str = "provider.write";
pub const PROVIDER_DISCOVER: &str = "provider.discover";
pub const PROVIDER_TEST: &str = "provider.test";
pub const PROVIDER_PUBLISH: &str = "provider.publish";
pub const PROVIDER_ACTIVATE: &str = "provider.activate";
pub const PROVIDER_SUSPEND: &str = "provider.suspend";
pub const PROVIDER_RETIRE: &str = "provider.retire";

#[derive(Clone)]
pub enum ApiAuth {
    Disabled,
    Bearer {
        token: String,
    },
    WithHumanResolver {
        base: Box<ApiAuth>,
        resolver: Arc<dyn HumanPrincipalResolver>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedHumanPrincipal {
    identity: String,
    groups: Vec<String>,
}

impl ResolvedHumanPrincipal {
    pub fn new(identity: impl Into<String>, mut groups: Vec<String>) -> Option<Self> {
        let identity = identity.into();
        if !valid_principal_label(&identity)
            || groups.iter().any(|group| !valid_principal_label(group))
        {
            return None;
        }
        groups.sort();
        groups.dedup();
        Some(Self { identity, groups })
    }
    pub fn identity(&self) -> &str {
        &self.identity
    }
    pub fn groups(&self) -> &[String] {
        &self.groups
    }
}

/// Request-scoped authenticated identity resolver. Implementations own token
/// or IdP verification; returning a principal is an authentication decision.
pub trait HumanPrincipalResolver: Send + Sync {
    fn resolve(&self, headers: &HeaderMap) -> Option<ResolvedHumanPrincipal>;
}

/// Closed in-memory resolver useful for deployments that provision a bounded
/// set of independent human API credentials. Tokens never appear in Debug.
pub struct BearerHumanPrincipalResolver {
    entries: Vec<(String, ResolvedHumanPrincipal)>,
}

/// Installation-scoped Operator identity used exclusively by the shared
/// Agent, Provider, and MCP management control plane. It is intentionally
/// independent from ordinary API and MCP resource-server credentials.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperatorPrincipal {
    identity: String,
    capabilities: BTreeSet<String>,
}

impl OperatorPrincipal {
    pub fn identity(&self) -> &str {
        &self.identity
    }

    pub fn has_capability(&self, capability: &str) -> bool {
        self.capabilities.contains(capability)
    }

    pub fn capabilities(&self) -> &BTreeSet<String> {
        &self.capabilities
    }
}

#[derive(Clone)]
pub struct OperatorAuth {
    entries: Arc<Vec<(String, OperatorPrincipal)>>,
}

impl OperatorAuth {
    pub fn new(
        entries: impl IntoIterator<Item = (String, String, BTreeSet<String>)>,
    ) -> Option<Self> {
        let allowed = BTreeSet::from([
            AGENT_READ.to_owned(),
            AGENT_WRITE.to_owned(),
            AGENT_VALIDATE.to_owned(),
            AGENT_PUBLISH.to_owned(),
            AGENT_DEPLOY.to_owned(),
            AGENT_ACTIVATE.to_owned(),
            AGENT_ARCHIVE.to_owned(),
            AGENT_DEBUG_SANDBOX.to_owned(),
            AGENT_DEBUG_LIVE.to_owned(),
            MCP_SERVER_READ.to_owned(),
            MCP_SERVER_WRITE.to_owned(),
            MCP_SERVER_DISCOVER.to_owned(),
            MCP_SERVER_PUBLISH.to_owned(),
            PROVIDER_READ.to_owned(),
            PROVIDER_WRITE.to_owned(),
            PROVIDER_DISCOVER.to_owned(),
            PROVIDER_TEST.to_owned(),
            PROVIDER_PUBLISH.to_owned(),
            PROVIDER_ACTIVATE.to_owned(),
            PROVIDER_SUSPEND.to_owned(),
            PROVIDER_RETIRE.to_owned(),
        ]);
        let mut resolved: Vec<(String, OperatorPrincipal)> = Vec::new();
        let mut identities = BTreeSet::new();
        for (token, identity, capabilities) in entries {
            if token.trim().is_empty()
                || !valid_principal_label(&identity)
                || capabilities.is_empty()
                || !capabilities.is_subset(&allowed)
                || !identities.insert(identity.clone())
                || resolved
                    .iter()
                    .any(|(existing, _)| constant_time_eq(token.as_bytes(), existing.as_bytes()))
            {
                return None;
            }
            resolved.push((
                token,
                OperatorPrincipal {
                    identity,
                    capabilities,
                },
            ));
        }
        (!resolved.is_empty()).then(|| Self {
            entries: Arc::new(resolved),
        })
    }

    pub(crate) fn resolve(&self, headers: &HeaderMap) -> Option<OperatorPrincipal> {
        let candidate = bearer_candidate(headers)?;
        self.entries
            .iter()
            .find(|(token, _)| constant_time_eq(candidate.as_bytes(), token.as_bytes()))
            .map(|(_, principal)| principal.clone())
    }
}

impl fmt::Debug for OperatorAuth {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OperatorAuth")
            .field("credential_count", &self.entries.len())
            .finish()
    }
}

impl BearerHumanPrincipalResolver {
    pub fn new(entries: impl IntoIterator<Item = (String, String, Vec<String>)>) -> Option<Self> {
        let mut resolved = Vec::new();
        let mut identities = std::collections::BTreeSet::new();
        for (token, identity, groups) in entries {
            if token.trim().is_empty()
                || resolved.iter().any(|(existing, _)| existing == &token)
                || !identities.insert(identity.clone())
            {
                return None;
            }
            resolved.push((token, ResolvedHumanPrincipal::new(identity, groups)?));
        }
        (!resolved.is_empty()).then_some(Self { entries: resolved })
    }
}

impl HumanPrincipalResolver for BearerHumanPrincipalResolver {
    fn resolve(&self, headers: &HeaderMap) -> Option<ResolvedHumanPrincipal> {
        let candidate = bearer_candidate(headers)?;
        self.entries
            .iter()
            .find(|(token, _)| constant_time_eq(candidate.as_bytes(), token.as_bytes()))
            .map(|(_, principal)| principal.clone())
    }
}

impl fmt::Debug for BearerHumanPrincipalResolver {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BearerHumanPrincipalResolver")
            .field("credential_count", &self.entries.len())
            .finish()
    }
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

    pub fn with_human_principal_resolver(self, resolver: Arc<dyn HumanPrincipalResolver>) -> Self {
        Self::WithHumanResolver {
            base: Box::new(self),
            resolver,
        }
    }

    pub(crate) fn accepts(&self, headers: &HeaderMap) -> bool {
        match self {
            Self::Disabled => true,
            Self::Bearer { token } => bearer_candidate(headers)
                .is_some_and(|candidate| constant_time_eq(candidate.as_bytes(), token.as_bytes())),
            Self::WithHumanResolver { base, .. } => base.accepts(headers),
        }
    }

    pub(crate) fn human_principal(&self, headers: &HeaderMap) -> Option<(String, Vec<String>)> {
        match self {
            Self::WithHumanResolver { resolver, .. } => resolver
                .resolve(headers)
                .map(|principal| (principal.identity, principal.groups)),
            Self::Disabled | Self::Bearer { .. } => None,
        }
    }

    pub(crate) fn accepts_human_task(&self, headers: &HeaderMap) -> bool {
        self.human_principal(headers).is_some()
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
            Self::WithHumanResolver { base, .. } => formatter
                .debug_struct("WithHumanResolver")
                .field("base", base)
                .field("resolver", &"[REDACTED]")
                .finish(),
        }
    }
}

fn bearer_candidate(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
}

fn valid_principal_label(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 256
        && !value
            .chars()
            .any(|character| character.is_control() || character.is_whitespace())
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

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::{header::AUTHORIZATION, HeaderValue};

    fn headers(token: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {token}")).unwrap(),
        );
        headers
    }

    #[test]
    fn human_credentials_resolve_per_request_without_general_api_escalation() {
        let resolver = BearerHumanPrincipalResolver::new([
            (
                "alice-token".to_owned(),
                "alice".to_owned(),
                vec!["medical".to_owned()],
            ),
            (
                "bob-token".to_owned(),
                "bob".to_owned(),
                vec!["legal".to_owned()],
            ),
        ])
        .unwrap();
        let auth =
            ApiAuth::bearer_token("admin-token").with_human_principal_resolver(Arc::new(resolver));

        assert!(auth.accepts(&headers("admin-token")));
        assert!(!auth.accepts_human_task(&headers("admin-token")));
        assert!(!auth.accepts(&headers("alice-token")));
        assert!(auth.accepts_human_task(&headers("alice-token")));
        assert!(auth.accepts_human_task(&headers("bob-token")));
        assert_eq!(
            auth.human_principal(&headers("alice-token")),
            Some(("alice".to_owned(), vec!["medical".to_owned()]))
        );
        assert_eq!(
            auth.human_principal(&headers("bob-token")),
            Some(("bob".to_owned(), vec!["legal".to_owned()]))
        );
    }

    #[test]
    fn human_task_auth_fails_closed_without_a_principal_resolver() {
        let disabled = ApiAuth::disabled();
        assert!(disabled.accepts(&HeaderMap::new()));
        assert!(!disabled.accepts_human_task(&headers("any-human-token")));
        assert_eq!(disabled.human_principal(&headers("any-human-token")), None);

        let bearer = ApiAuth::bearer_token("admin-token");
        assert!(bearer.accepts(&headers("admin-token")));
        assert!(!bearer.accepts_human_task(&headers("admin-token")));
        assert_eq!(bearer.human_principal(&headers("admin-token")), None);
    }

    #[test]
    fn human_resolver_rejects_ambiguous_credentials_without_disclosing_tokens() {
        assert!(BearerHumanPrincipalResolver::new([
            ("same-token".to_owned(), "alice".to_owned(), Vec::new()),
            ("same-token".to_owned(), "bob".to_owned(), Vec::new()),
        ])
        .is_none());
        assert!(BearerHumanPrincipalResolver::new([
            ("alice-token".to_owned(), "alice".to_owned(), Vec::new()),
            ("other-token".to_owned(), "alice".to_owned(), Vec::new()),
        ])
        .is_none());
        assert!(BearerHumanPrincipalResolver::new([(
            "   ".to_owned(),
            "alice".to_owned(),
            Vec::new(),
        )])
        .is_none());

        let resolver = BearerHumanPrincipalResolver::new([(
            "private-human-token".to_owned(),
            "alice".to_owned(),
            vec!["medical".to_owned()],
        )])
        .unwrap();
        let debug = format!("{resolver:?}");
        assert!(!debug.contains("private-human-token"));
        assert!(debug.contains("credential_count"));
    }
}
