use std::{error::Error, fmt};

use crate::NodeId;

use super::{ControlEdgeId, ControlPortId, DataPortId, SourceSpan};

pub const PLAN_WIRE_INVALID: &str = "ENGINE_PLAN_WIRE_INVALID";
pub const PLAN_VERSION_UNSUPPORTED: &str = "ENGINE_PLAN_VERSION_UNSUPPORTED";
pub const PLAN_ID_DUPLICATE: &str = "ENGINE_PLAN_ID_DUPLICATE";
pub const PLAN_REFERENCE_INVALID: &str = "ENGINE_PLAN_REFERENCE_INVALID";
pub const PLAN_PORT_INVALID: &str = "ENGINE_PLAN_PORT_INVALID";
pub const PLAN_TYPE_MISMATCH: &str = "ENGINE_PLAN_TYPE_MISMATCH";
pub const PLAN_SCOPE_INVALID: &str = "ENGINE_PLAN_SCOPE_INVALID";
pub const PLAN_DOMINANCE_INVALID: &str = "ENGINE_PLAN_DOMINANCE_INVALID";
pub const PLAN_DATA_CYCLE: &str = "ENGINE_PLAN_DATA_CYCLE";
pub const PLAN_REACHABILITY_INVALID: &str = "ENGINE_PLAN_REACHABILITY_INVALID";
pub const PLAN_CONTROL_CYCLE: &str = "ENGINE_PLAN_CONTROL_CYCLE";
pub const PLAN_BRANCH_INVALID: &str = "ENGINE_PLAN_BRANCH_INVALID";
pub const PLAN_MERGE_INVALID: &str = "ENGINE_PLAN_MERGE_INVALID";
pub const PLAN_PHI_INVALID: &str = "ENGINE_PLAN_PHI_INVALID";
pub const PLAN_FORK_INVALID: &str = "ENGINE_PLAN_FORK_INVALID";
pub const PLAN_JOIN_INVALID: &str = "ENGINE_PLAN_JOIN_INVALID";
pub const PLAN_TERMINAL_INVALID: &str = "ENGINE_PLAN_TERMINAL_INVALID";
pub const PLAN_LOOP_INVALID: &str = "ENGINE_PLAN_LOOP_INVALID";
pub const PLAN_POLICY_INVALID: &str = "ENGINE_PLAN_POLICY_INVALID";
pub const PLAN_DESCRIPTOR_INVALID: &str = "ENGINE_PLAN_DESCRIPTOR_INVALID";
pub const PLAN_HASH_MISMATCH: &str = "ENGINE_PLAN_HASH_MISMATCH";
pub const PLAN_STABLE_ID_COLLISION: &str = "ENGINE_PLAN_STABLE_ID_COLLISION";
pub const PLAN_INDEX_INVALID: &str = "ENGINE_PLAN_INDEX_INVALID";
pub const PLAN_CONTEXT_LINK_INVALID: &str = "ENGINE_PLAN_CONTEXT_LINK_INVALID";

/// Stable, body-free validation error for the Canonical Typed Plan boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlanError {
    code: &'static str,
    message: String,
    diagnostic: Option<Box<PlanDiagnostic>>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct PlanDiagnostic {
    target: Option<PlanDiagnosticTarget>,
    source_span: Option<SourceSpan>,
}

/// Stable semantic identity of the Canvas element responsible for a Plan
/// diagnostic. A target is intentionally independent from an error message so
/// graph editors never need to parse prose to select the offending element.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlanDiagnosticTarget {
    Node {
        node_id: NodeId,
    },
    ControlPort {
        port_id: ControlPortId,
        node_id: Option<NodeId>,
    },
    DataPort {
        port_id: DataPortId,
        node_id: Option<NodeId>,
    },
    ControlEdge {
        edge_id: ControlEdgeId,
    },
}

impl PlanDiagnosticTarget {
    pub fn node_id(&self) -> Option<&NodeId> {
        match self {
            Self::Node { node_id } => Some(node_id),
            Self::ControlPort { node_id, .. } | Self::DataPort { node_id, .. } => node_id.as_ref(),
            Self::ControlEdge { .. } => None,
        }
    }

    pub fn port_id(&self) -> Option<&str> {
        match self {
            Self::ControlPort { port_id, .. } => Some(port_id.as_str()),
            Self::DataPort { port_id, .. } => Some(port_id.as_str()),
            Self::Node { .. } | Self::ControlEdge { .. } => None,
        }
    }

    pub fn edge_id(&self) -> Option<&ControlEdgeId> {
        match self {
            Self::ControlEdge { edge_id } => Some(edge_id),
            Self::Node { .. } | Self::ControlPort { .. } | Self::DataPort { .. } => None,
        }
    }
}

impl PlanError {
    pub(crate) fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            diagnostic: None,
        }
    }

    pub(crate) fn with_target(mut self, target: PlanDiagnosticTarget) -> Self {
        self.diagnostic_mut().target = Some(target);
        self
    }

    pub(crate) fn with_target_if_absent(mut self, target: PlanDiagnosticTarget) -> Self {
        if self.target().is_none() {
            self.diagnostic_mut().target = Some(target);
        }
        self
    }

    pub(crate) fn with_source_span_if_absent(mut self, source_span: SourceSpan) -> Self {
        if self.source_span().is_none() {
            self.diagnostic_mut().source_span = Some(source_span);
        }
        self
    }

    fn diagnostic_mut(&mut self) -> &mut PlanDiagnostic {
        self.diagnostic
            .get_or_insert_with(|| Box::new(PlanDiagnostic::default()))
    }

    pub fn code(&self) -> &'static str {
        self.code
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    pub fn target(&self) -> Option<&PlanDiagnosticTarget> {
        self.diagnostic
            .as_deref()
            .and_then(|diagnostic| diagnostic.target.as_ref())
    }

    pub fn source_span(&self) -> Option<&SourceSpan> {
        self.diagnostic
            .as_deref()
            .and_then(|diagnostic| diagnostic.source_span.as_ref())
    }
}

impl fmt::Display for PlanError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.code, self.message)
    }
}

impl Error for PlanError {}
