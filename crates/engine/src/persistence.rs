//! Closed persistence semantics shared by deployment, runtime and API contracts.

use serde::{Deserialize, Serialize};

/// Immutable persistence policy selected by one Deployment Revision.
///
/// `Full` retains the durable recovery/event contract. `TerminalOnly` keeps
/// execution state in-process and persists only admission and terminal data.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PersistenceMode {
    #[default]
    Full,
    TerminalOnly,
}

impl PersistenceMode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Full => "full",
            Self::TerminalOnly => "terminal_only",
        }
    }

    pub const fn supports_recovery(self) -> bool {
        matches!(self, Self::Full)
    }

    pub const fn supports_event_replay(self) -> bool {
        matches!(self, Self::Full)
    }
}

#[cfg(test)]
mod tests {
    use super::PersistenceMode;

    #[test]
    fn persistence_mode_has_a_closed_strict_wire_contract() {
        assert_eq!(
            serde_json::to_string(&PersistenceMode::Full).unwrap(),
            "\"full\""
        );
        assert_eq!(
            serde_json::to_string(&PersistenceMode::TerminalOnly).unwrap(),
            "\"terminal_only\""
        );
        assert_eq!(
            serde_json::from_str::<PersistenceMode>("\"terminal_only\"").unwrap(),
            PersistenceMode::TerminalOnly
        );
        assert!(serde_json::from_str::<PersistenceMode>("\"durable\"").is_err());
        assert!(serde_json::from_str::<PersistenceMode>("{}").is_err());
        assert_eq!(PersistenceMode::default(), PersistenceMode::Full);
    }
}
