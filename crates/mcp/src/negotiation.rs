use std::fmt;

/// Selects the first locally preferred version also offered by the peer.
pub fn negotiate_version<'a>(
    local_preference: &'a [&'a str],
    peer_supported: &[String],
) -> Result<&'a str, VersionNegotiationError> {
    local_preference
        .iter()
        .copied()
        .find(|candidate| peer_supported.iter().any(|value| value == candidate))
        .ok_or(VersionNegotiationError)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VersionNegotiationError;

impl fmt::Display for VersionNegotiationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("no mutually supported MCP protocol version")
    }
}

impl std::error::Error for VersionNegotiationError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn negotiation_respects_local_preference_and_is_exact() {
        let peer = vec!["2025-11-25".to_owned(), "2026-07-28".to_owned()];
        assert_eq!(
            negotiate_version(&["2026-07-28", "2025-11-25"], &peer).unwrap(),
            "2026-07-28"
        );
        assert!(negotiate_version(&["2026-07"], &peer).is_err());
    }
}
