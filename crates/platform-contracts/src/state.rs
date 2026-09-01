use serde::{de, Deserialize, Deserializer, Serialize, Serializer};
use std::{error::Error, fmt, str::FromStr};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AttemptCommitDisposition {
    Committed,
    RejectedStaleFence,
    RejectedTerminalWinner,
}

impl AttemptCommitDisposition {
    pub const ALL: &'static [Self] = &[
        Self::Committed,
        Self::RejectedStaleFence,
        Self::RejectedTerminalWinner,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Committed => "committed",
            Self::RejectedStaleFence => "rejected_stale_fence",
            Self::RejectedTerminalWinner => "rejected_terminal_winner",
        }
    }
}

pub const fn attempt_commit_disposition(
    stored_epoch: u64,
    attempt_epoch: u64,
    terminal_already_committed: bool,
) -> AttemptCommitDisposition {
    if attempt_epoch != stored_epoch {
        AttemptCommitDisposition::RejectedStaleFence
    } else if terminal_already_committed {
        AttemptCommitDisposition::RejectedTerminalWinner
    } else {
        AttemptCommitDisposition::Committed
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct StateMachineDescriptor {
    pub name: &'static str,
    pub states: Vec<&'static str>,
    pub transitions: Vec<[&'static str; 2]>,
    pub terminal_states: Vec<&'static str>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StateParseError {
    machine: &'static str,
    value: String,
}

impl fmt::Display for StateParseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "unknown state {:?} for closed {} state machine",
            self.value, self.machine
        )
    }
}

impl Error for StateParseError {}

macro_rules! closed_state_machine {
    (
        pub enum $name:ident, $machine:literal {
            $($variant:ident => $wire:literal => [$($target:ident),*]),+ $(,)?
        }
    ) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub enum $name {
            $($variant),+
        }

        impl $name {
            pub const ALL: &'static [Self] = &[$(Self::$variant),+];

            pub const fn as_str(self) -> &'static str {
                match self {
                    $(Self::$variant => $wire),+
                }
            }

            pub const fn can_transition_to(self, target: Self) -> bool {
                match self {
                    $(Self::$variant => false $(|| matches!(target, Self::$target))*),+
                }
            }

            pub fn descriptor() -> StateMachineDescriptor {
                let transitions = Self::ALL
                    .iter()
                    .flat_map(|source| {
                        Self::ALL.iter().filter_map(move |target| {
                            source
                                .can_transition_to(*target)
                                .then_some([source.as_str(), target.as_str()])
                        })
                    })
                    .collect::<Vec<_>>();
                let terminal_states = Self::ALL
                    .iter()
                    .filter(|source| !Self::ALL.iter().any(|target| source.can_transition_to(*target)))
                    .map(|state| state.as_str())
                    .collect();
                StateMachineDescriptor {
                    name: $machine,
                    states: Self::ALL.iter().map(|state| state.as_str()).collect(),
                    transitions,
                    terminal_states,
                }
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(self.as_str())
            }
        }

        impl FromStr for $name {
            type Err = StateParseError;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                match value {
                    $($wire => Ok(Self::$variant)),+,
                    _ => Err(StateParseError {
                        machine: $machine,
                        value: value.to_owned(),
                    }),
                }
            }
        }

        impl Serialize for $name {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: Serializer,
            {
                serializer.serialize_str(self.as_str())
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                let value = String::deserialize(deserializer)?;
                value.parse().map_err(de::Error::custom)
            }
        }
    };
}

closed_state_machine! {
    pub enum EntityLifecycle, "entity_lifecycle" {
        Active => "active" => [Archived, Retired],
        Archived => "archived" => [Active, Retired],
        Retired => "retired" => []
    }
}

closed_state_machine! {
    pub enum AdministrativeGate, "administrative_gate" {
        Enabled => "enabled" => [Suspended],
        Suspended => "suspended" => [Enabled]
    }
}

closed_state_machine! {
    pub enum PrincipalIdentityState, "principal_identity" {
        Active => "active" => [Revoked],
        Revoked => "revoked" => []
    }
}

closed_state_machine! {
    pub enum PrincipalBindingState, "principal_binding" {
        Active => "active" => [Revoked],
        Revoked => "revoked" => []
    }
}

closed_state_machine! {
    pub enum SecretBindingState, "secret_binding" {
        Active => "active" => [Revoked],
        Revoked => "revoked" => []
    }
}

closed_state_machine! {
    pub enum ApprovalState, "approval_task" {
        Pending => "pending" => [Approved, Rejected, Expired, Cancelled],
        Approved => "approved" => [],
        Rejected => "rejected" => [],
        Expired => "expired" => [],
        Cancelled => "cancelled" => []
    }
}

closed_state_machine! {
    pub enum RunState, "run" {
        Queued => "queued" => [Running, Cancelling, TimedOut],
        Running => "running" => [Waiting, Cancelling, Succeeded, Failed, TimedOut],
        Waiting => "waiting" => [Running, Cancelling, Failed, TimedOut],
        Cancelling => "cancelling" => [Cancelled, Failed, TimedOut],
        Succeeded => "succeeded" => [],
        Failed => "failed" => [],
        Cancelled => "cancelled" => [],
        TimedOut => "timed_out" => []
    }
}

closed_state_machine! {
    pub enum ScopeState, "scope_instance" {
        Open => "open" => [Closing],
        Closing => "closing" => [Succeeded, Failed, Cancelled],
        Succeeded => "succeeded" => [],
        Failed => "failed" => [],
        Cancelled => "cancelled" => []
    }
}

closed_state_machine! {
    pub enum NodeExecutionState, "node_execution" {
        Pending => "pending" => [Ready, Failed, Cancelled, TimedOut],
        Ready => "ready" => [Running, Cancelled, TimedOut],
        Running => "running" => [Waiting, RetryScheduled, Cancelling, Succeeded, Failed, TimedOut],
        Waiting => "waiting" => [Ready, Cancelling, Failed, Cancelled, TimedOut],
        RetryScheduled => "retry_scheduled" => [Ready, Cancelling, Cancelled, TimedOut],
        Cancelling => "cancelling" => [Cancelled, Failed, TimedOut],
        Succeeded => "succeeded" => [],
        Failed => "failed" => [],
        Cancelled => "cancelled" => [],
        TimedOut => "timed_out" => []
    }
}

closed_state_machine! {
    pub enum AttemptObservationState, "attempt_observation" {
        Leased => "leased" => [Started, Cancelled, TimedOut, Lost],
        Started => "started" => [Succeeded, Failed, Cancelled, TimedOut, Lost],
        Succeeded => "succeeded" => [],
        Failed => "failed" => [],
        Cancelled => "cancelled" => [],
        TimedOut => "timed_out" => [],
        Lost => "lost" => []
    }
}

closed_state_machine! {
    pub enum JobState, "job" {
        Ready => "ready" => [Leased, Cancelled, TimedOut],
        Leased => "leased" => [Running, Ready, Cancelled, TimedOut],
        Running => "running" => [Ready, Waiting, RetryScheduled, Cancelling, Succeeded, Failed, Cancelled, TimedOut, ReconciliationRequired],
        Waiting => "waiting" => [Ready, Cancelling, Succeeded, Failed, Cancelled, TimedOut, ReconciliationRequired],
        RetryScheduled => "retry_scheduled" => [Ready, Cancelling, Cancelled, TimedOut],
        Cancelling => "cancelling" => [Cancelled, Failed, TimedOut, ReconciliationRequired],
        ReconciliationRequired => "reconciliation_required" => [Succeeded, Failed, Cancelled],
        Succeeded => "succeeded" => [],
        Failed => "failed" => [],
        Cancelled => "cancelled" => [],
        TimedOut => "timed_out" => []
    }
}

closed_state_machine! {
    pub enum WakeContractState, "wake_contract" {
        Pending => "pending" => [Consumed, Cancelled, TimedOut],
        Consumed => "consumed" => [],
        Cancelled => "cancelled" => [],
        TimedOut => "timed_out" => []
    }
}

closed_state_machine! {
    pub enum InteractionState, "interaction_task" {
        Pending => "pending" => [Responded, Declined, Cancelled, Expired],
        Responded => "responded" => [],
        Declined => "declined" => [],
        Cancelled => "cancelled" => [],
        Expired => "expired" => []
    }
}

closed_state_machine! {
    pub enum InvocationState, "capability_invocation" {
        Created => "created" => [AwaitingApproval, Ready, Failed],
        AwaitingApproval => "awaiting_approval" => [Ready, Failed, Cancelled, TimedOut],
        Ready => "ready" => [InFlight, Cancelling, TimedOut],
        InFlight => "in_flight" => [Succeeded, Failed, Deferred, AwaitingInput, RetryScheduled, ReconciliationRequired, Cancelling, TimedOut],
        Deferred => "deferred" => [InFlight, Succeeded, Failed, AwaitingInput, RetryScheduled, ReconciliationRequired, Cancelling, TimedOut],
        AwaitingInput => "awaiting_input" => [Ready, Deferred, Failed, Cancelled, TimedOut],
        RetryScheduled => "retry_scheduled" => [Ready, Deferred, Cancelling, TimedOut],
        Cancelling => "cancelling" => [Cancelled, Failed, ReconciliationRequired, TimedOut],
        ReconciliationRequired => "reconciliation_required" => [Succeeded, Failed, Cancelled],
        Succeeded => "succeeded" => [],
        Failed => "failed" => [],
        Cancelled => "cancelled" => [],
        TimedOut => "timed_out" => []
    }
}

closed_state_machine! {
    pub enum SkillActivationState, "skill_activation" {
        Proposed => "proposed" => [Active, Rejected],
        Active => "active" => [Superseded],
        Rejected => "rejected" => [],
        Superseded => "superseded" => []
    }
}

closed_state_machine! {
    pub enum ContextQueryState, "context_query" {
        Created => "created" => [AwaitingAuthorization, Ready, Failed],
        AwaitingAuthorization => "awaiting_authorization" => [Ready, Failed, Cancelled, TimedOut],
        Ready => "ready" => [InFlight, Cancelled, TimedOut],
        InFlight => "in_flight" => [Succeeded, Deferred, RetryScheduled, Failed, Cancelled, TimedOut],
        Deferred => "deferred" => [InFlight, Succeeded, RetryScheduled, Failed, Cancelled, TimedOut],
        RetryScheduled => "retry_scheduled" => [Ready, Cancelled, TimedOut],
        Succeeded => "succeeded" => [],
        Failed => "failed" => [],
        Cancelled => "cancelled" => [],
        TimedOut => "timed_out" => []
    }
}

closed_state_machine! {
    pub enum McpSessionState, "mcp_session" {
        Disconnected => "disconnected" => [Connecting],
        Connecting => "connecting" => [Initializing, ReauthRequired, Failed],
        Initializing => "initializing" => [Ready, ReauthRequired, Failed],
        Ready => "ready" => [Degraded, ReauthRequired, Draining, Failed],
        ReauthRequired => "reauth_required" => [Connecting, Draining, Failed],
        Degraded => "degraded" => [Ready, ReauthRequired, Draining, Failed],
        Draining => "draining" => [Closed, Failed],
        Closed => "closed" => [],
        Failed => "failed" => []
    }
}

closed_state_machine! {
    pub enum McpAuthorizationState, "mcp_authorization" {
        Active => "active" => [ReauthRequired, Revoked, Expired],
        ReauthRequired => "reauth_required" => [Active, Revoked, Expired],
        Revoked => "revoked" => [],
        Expired => "expired" => []
    }
}

closed_state_machine! {
    pub enum ChildLinkState, "child_run_link" {
        Running => "running" => [Waiting, Cancelling, Succeeded, Failed, Cancelled, TimedOut],
        Waiting => "waiting" => [Running, Cancelling, Succeeded, Failed, Cancelled, TimedOut],
        Cancelling => "cancelling" => [Succeeded, Cancelled, Failed, TimedOut],
        Succeeded => "succeeded" => [],
        Failed => "failed" => [],
        Cancelled => "cancelled" => [],
        TimedOut => "timed_out" => []
    }
}

closed_state_machine! {
    pub enum ModelTurnState, "model_turn" {
        Created => "created" => [AwaitingBudget, Ready, Failed],
        AwaitingBudget => "awaiting_budget" => [Ready, Failed, Cancelled, TimedOut],
        Ready => "ready" => [InFlight, Cancelling, TimedOut],
        InFlight => "in_flight" => [Succeeded, RetryScheduled, Failed, Cancelling, TimedOut],
        RetryScheduled => "retry_scheduled" => [Ready, Cancelling, TimedOut],
        Cancelling => "cancelling" => [Cancelled, Failed, TimedOut],
        Succeeded => "succeeded" => [],
        Failed => "failed" => [],
        Cancelled => "cancelled" => [],
        TimedOut => "timed_out" => []
    }
}

closed_state_machine! {
    pub enum SandboxJobState, "sandbox_job" {
        Accepted => "accepted" => [Preparing, Cancelling, TimedOut, Failed],
        Preparing => "preparing" => [Starting, Cancelling, TimedOut, Failed, Lost],
        Starting => "starting" => [Running, Cancelling, TimedOut, Failed, Lost],
        Running => "running" => [Collecting, Cancelling, TimedOut, Failed, Lost],
        Collecting => "collecting" => [Succeeded, Cancelling, TimedOut, Failed, Lost],
        Cancelling => "cancelling" => [Cancelled, Failed, TimedOut, Lost],
        Succeeded => "succeeded" => [],
        Failed => "failed" => [],
        Cancelled => "cancelled" => [],
        TimedOut => "timed_out" => [],
        Lost => "lost" => []
    }
}

closed_state_machine! {
    pub enum ArtifactState, "artifact" {
        Staging => "staging" => [Uploaded, Rejected, Deleting],
        Uploaded => "uploaded" => [Verifying, Rejected, Deleting],
        Verifying => "verifying" => [Verified, Quarantined, Rejected],
        Verified => "verified" => [Ready, Quarantined, Rejected, Deleting],
        Ready => "ready" => [Quarantined, Deleting, Corrupt],
        Quarantined => "quarantined" => [Ready, Rejected, Deleting, Corrupt],
        Rejected => "rejected" => [Deleting],
        Deleting => "deleting" => [Deleted],
        Deleted => "deleted" => [],
        Corrupt => "corrupt" => [Quarantined, Deleting]
    }
}

pub fn all_state_machines() -> Vec<StateMachineDescriptor> {
    vec![
        EntityLifecycle::descriptor(),
        AdministrativeGate::descriptor(),
        PrincipalIdentityState::descriptor(),
        PrincipalBindingState::descriptor(),
        SecretBindingState::descriptor(),
        ApprovalState::descriptor(),
        RunState::descriptor(),
        ScopeState::descriptor(),
        NodeExecutionState::descriptor(),
        AttemptObservationState::descriptor(),
        JobState::descriptor(),
        WakeContractState::descriptor(),
        InteractionState::descriptor(),
        InvocationState::descriptor(),
        SkillActivationState::descriptor(),
        ContextQueryState::descriptor(),
        McpSessionState::descriptor(),
        McpAuthorizationState::descriptor(),
        ChildLinkState::descriptor(),
        ModelTurnState::descriptor(),
        SandboxJobState::descriptor(),
        ArtifactState::descriptor(),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_machine_has_unique_states_and_no_outgoing_terminal_edges() {
        for machine in all_state_machines() {
            let unique = machine
                .states
                .iter()
                .copied()
                .collect::<std::collections::BTreeSet<_>>();
            assert_eq!(unique.len(), machine.states.len(), "{}", machine.name);
            for terminal in machine.terminal_states {
                assert!(
                    !machine.transitions.iter().any(|edge| edge[0] == terminal),
                    "{} terminal state {} has an outgoing edge",
                    machine.name,
                    terminal
                );
            }
        }
    }

    #[test]
    fn stale_fence_rejection_is_not_an_attempt_state() {
        assert!("rejected_stale_fence"
            .parse::<AttemptObservationState>()
            .is_err());
        assert!(AttemptObservationState::Started.can_transition_to(AttemptObservationState::Lost));
        assert!(!AttemptObservationState::Lost.can_transition_to(AttemptObservationState::Started));
        assert_eq!(
            attempt_commit_disposition(8, 7, false),
            AttemptCommitDisposition::RejectedStaleFence
        );
    }
}
