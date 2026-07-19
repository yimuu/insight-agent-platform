use serde::{de::Error as _, Deserialize, Deserializer, Serialize, Serializer};

use crate::engine::{
    plan::{ControlPortId, ScopeId, SemanticHash},
    ActivationId, ContentHash, ControlTokenId, EffectId, ForkGroupId, ModelError, NodeId, RunId,
    ScopeInstanceId, SignalId, TimerId,
};

use super::{SchedulerError, SCHEDULER_ID_INVALID};

const SCHEDULER_ID_DOMAIN: &[u8] = b"insight-agent/scheduler-id/v1";
const MAX_OCCURRENCE_SEGMENTS: usize = 128;
const MAX_OCCURRENCE_SEGMENT_BYTES: usize = 512;

/// A semantic path, never an invocation counter allocated by the scheduler.
///
/// Every child segment must come from immutable graph identity (for example a
/// control edge or a branch case) or a repository-owned dynamic identity (for
/// example a map item key). This keeps IDs stable across process restarts and
/// independent of traversal order.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
pub struct LogicalOccurrence {
    segments: Vec<String>,
}

impl<'de> Deserialize<'de> for LogicalOccurrence {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            segments: Vec<String>,
        }

        let wire = Wire::deserialize(deserializer)?;
        if wire.segments.is_empty() || wire.segments.len() > MAX_OCCURRENCE_SEGMENTS {
            return Err(D::Error::custom(
                "logical occurrence must contain a bounded non-empty semantic path",
            ));
        }
        for segment in &wire.segments {
            validate_occurrence_segment(segment).map_err(D::Error::custom)?;
        }
        Ok(Self {
            segments: wire.segments,
        })
    }
}

impl LogicalOccurrence {
    pub fn entry() -> Self {
        Self {
            segments: vec!["entry".to_owned()],
        }
    }

    pub fn root_scope() -> Self {
        Self {
            segments: vec!["root_scope".to_owned()],
        }
    }

    pub fn child(&self, segment: impl Into<String>) -> Result<Self, SchedulerError> {
        let segment = segment.into();
        validate_occurrence_segment(&segment)?;
        if self.segments.len() >= MAX_OCCURRENCE_SEGMENTS {
            return Err(SchedulerError::new(
                SCHEDULER_ID_INVALID,
                "logical occurrence exceeds the supported nesting depth",
            ));
        }
        let mut segments = self.segments.clone();
        segments.push(segment);
        Ok(Self { segments })
    }

    pub fn segments(&self) -> &[String] {
        &self.segments
    }

    pub fn parent(&self) -> Option<Self> {
        (self.segments.len() > 1).then(|| Self {
            segments: self.segments[..self.segments.len() - 1].to_vec(),
        })
    }

    pub fn is_ancestor_of(&self, descendant: &Self) -> bool {
        self.segments.len() <= descendant.segments.len()
            && descendant.segments[..self.segments.len()] == self.segments
    }
}

fn validate_occurrence_segment(value: &str) -> Result<(), SchedulerError> {
    if value.is_empty()
        || value.len() > MAX_OCCURRENCE_SEGMENT_BYTES
        || value.chars().any(char::is_control)
    {
        return Err(SchedulerError::new(
            SCHEDULER_ID_INVALID,
            "logical occurrence segment must be non-empty, bounded, and body-free",
        ));
    }
    Ok(())
}

macro_rules! scheduler_hash_id {
    ($name:ident, $prefix:literal, $label:literal) => {
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(String);

        impl $name {
            fn from_hex(hex: &str) -> Self {
                debug_assert!(is_lower_sha256(hex));
                Self(format!(concat!($prefix, "{}"), hex))
            }

            pub fn parse(value: impl Into<String>) -> Result<Self, SchedulerError> {
                let value = value.into();
                let Some(hex) = value.strip_prefix($prefix) else {
                    return Err(SchedulerError::new(
                        SCHEDULER_ID_INVALID,
                        concat!($label, " has an invalid prefix"),
                    ));
                };
                if !is_lower_sha256(hex) {
                    return Err(SchedulerError::new(
                        SCHEDULER_ID_INVALID,
                        concat!($label, " must contain 64 lowercase hexadecimal digits"),
                    ));
                }
                Ok(Self(value))
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str(&self.0)
            }
        }

        impl Serialize for $name {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: Serializer,
            {
                serializer.serialize_str(&self.0)
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                let value = String::deserialize(deserializer)?;
                Self::parse(value).map_err(D::Error::custom)
            }
        }
    };
}

scheduler_hash_id!(SchedulerTaskId, "task_", "scheduler task ID");
scheduler_hash_id!(
    SchedulerCheckpointId,
    "checkpoint_",
    "scheduler checkpoint ID"
);
scheduler_hash_id!(SchedulerWaitId, "wait_", "scheduler wait ID");

/// Domain-separated deterministic ID factory for one immutable run/Plan pair.
pub struct DeterministicIds<'a> {
    run_id: &'a RunId,
    plan_semantic_id: &'a SemanticHash,
}

impl<'a> DeterministicIds<'a> {
    pub fn new(run_id: &'a RunId, plan_semantic_id: &'a SemanticHash) -> Self {
        Self {
            run_id,
            plan_semantic_id,
        }
    }

    pub(crate) fn run_id(&self) -> &RunId {
        self.run_id
    }

    pub fn scope_instance(
        &self,
        scope_id: &ScopeId,
        occurrence: &LogicalOccurrence,
    ) -> Result<ScopeInstanceId, ModelError> {
        ScopeInstanceId::new(format!(
            "scope_{}",
            self.hash("scope", scope_id.as_str(), occurrence, &[])
        ))
    }

    pub fn activation(
        &self,
        node_id: &NodeId,
        scope_instance_id: &ScopeInstanceId,
        occurrence: &LogicalOccurrence,
    ) -> Result<ActivationId, ModelError> {
        ActivationId::new(format!(
            "activation_{}",
            self.hash(
                "activation",
                node_id.as_str(),
                occurrence,
                &[scope_instance_id.as_str()],
            )
        ))
    }

    pub fn control_token(
        &self,
        node_id: &NodeId,
        scope_instance_id: &ScopeInstanceId,
        occurrence: &LogicalOccurrence,
        output_port: &ControlPortId,
    ) -> Result<ControlTokenId, ModelError> {
        ControlTokenId::new(format!(
            "token_{}",
            self.hash(
                "control_token",
                node_id.as_str(),
                occurrence,
                &[scope_instance_id.as_str(), output_port.as_str()],
            )
        ))
    }

    pub fn effect(
        &self,
        node_id: &NodeId,
        scope_instance_id: &ScopeInstanceId,
        occurrence: &LogicalOccurrence,
    ) -> Result<EffectId, ModelError> {
        EffectId::new(format!(
            "effect_{}",
            self.hash(
                "effect",
                node_id.as_str(),
                occurrence,
                &[scope_instance_id.as_str()],
            )
        ))
    }

    pub fn task(
        &self,
        node_id: &NodeId,
        scope_instance_id: &ScopeInstanceId,
        occurrence: &LogicalOccurrence,
    ) -> SchedulerTaskId {
        SchedulerTaskId::from_hex(&self.hash(
            "task",
            node_id.as_str(),
            occurrence,
            &[scope_instance_id.as_str()],
        ))
    }

    pub fn timer(
        &self,
        node_id: &NodeId,
        scope_instance_id: &ScopeInstanceId,
        occurrence: &LogicalOccurrence,
        timer_role: &str,
    ) -> Result<TimerId, ModelError> {
        TimerId::new(format!(
            "timer_{}",
            self.hash(
                "timer",
                node_id.as_str(),
                occurrence,
                &[scope_instance_id.as_str(), timer_role],
            )
        ))
    }

    pub fn signal(
        &self,
        node_id: &NodeId,
        scope_instance_id: &ScopeInstanceId,
        occurrence: &LogicalOccurrence,
        signal_name: &str,
    ) -> Result<SignalId, ModelError> {
        SignalId::new(format!(
            "signal_{}",
            self.hash(
                "signal",
                node_id.as_str(),
                occurrence,
                &[scope_instance_id.as_str(), signal_name],
            )
        ))
    }

    pub fn wait(
        &self,
        node_id: &NodeId,
        scope_instance_id: &ScopeInstanceId,
        occurrence: &LogicalOccurrence,
    ) -> SchedulerWaitId {
        SchedulerWaitId::from_hex(&self.hash(
            "wait",
            node_id.as_str(),
            occurrence,
            &[scope_instance_id.as_str()],
        ))
    }

    pub fn fork_group(
        &self,
        node_id: &NodeId,
        scope_instance_id: &ScopeInstanceId,
        occurrence: &LogicalOccurrence,
    ) -> Result<ForkGroupId, ModelError> {
        ForkGroupId::new(format!(
            "fork_{}",
            self.hash(
                "fork_group",
                node_id.as_str(),
                occurrence,
                &[scope_instance_id.as_str()],
            )
        ))
    }

    pub fn child_run(
        &self,
        node_id: &NodeId,
        scope_instance_id: &ScopeInstanceId,
        occurrence: &LogicalOccurrence,
    ) -> Result<RunId, ModelError> {
        RunId::new(format!(
            "child_{}",
            self.hash(
                "child_run",
                node_id.as_str(),
                occurrence,
                &[scope_instance_id.as_str()],
            )
        ))
    }

    pub fn checkpoint(
        &self,
        node_id: &NodeId,
        scope_instance_id: &ScopeInstanceId,
        occurrence: &LogicalOccurrence,
        phase: &str,
    ) -> SchedulerCheckpointId {
        SchedulerCheckpointId::from_hex(&self.hash(
            "checkpoint",
            node_id.as_str(),
            occurrence,
            &[scope_instance_id.as_str(), phase],
        ))
    }

    fn hash(
        &self,
        kind: &str,
        semantic_owner: &str,
        occurrence: &LogicalOccurrence,
        extra: &[&str],
    ) -> String {
        let mut framed = Vec::new();
        for value in [
            SCHEDULER_ID_DOMAIN,
            kind.as_bytes(),
            self.run_id.as_str().as_bytes(),
            self.plan_semantic_id.as_str().as_bytes(),
            semantic_owner.as_bytes(),
        ] {
            append_framed(&mut framed, value);
        }
        append_count(&mut framed, occurrence.segments.len());
        for segment in &occurrence.segments {
            append_framed(&mut framed, segment.as_bytes());
        }
        append_count(&mut framed, extra.len());
        for value in extra {
            append_framed(&mut framed, value.as_bytes());
        }
        ContentHash::from_bytes(&framed)
            .as_str()
            .trim_start_matches("sha256:")
            .to_owned()
    }
}

fn append_count(target: &mut Vec<u8>, value: usize) {
    target.extend_from_slice(
        &u64::try_from(value)
            .expect("supported Rust targets cannot address more than u64::MAX items")
            .to_be_bytes(),
    );
}

fn append_framed(target: &mut Vec<u8>, value: &[u8]) {
    target.extend_from_slice(
        &u64::try_from(value.len())
            .expect("supported Rust targets cannot address more than u64::MAX bytes")
            .to_be_bytes(),
    );
    target.extend_from_slice(value);
}

fn is_lower_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}
