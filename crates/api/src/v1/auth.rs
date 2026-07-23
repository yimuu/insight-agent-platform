//! Authentication primitives for the v1 HTTP API.

use std::{fmt, sync::Arc};

use axum::http::{header::AUTHORIZATION, HeaderMap};

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
