use super::{
    semantic::semantic_hash_for_plan, verify::verify_plan, ControlEdge, ControlPort, DataBinding,
    DataPort, Node, NodeKind, PhiBinding, Plan, PlanDiagnosticTarget, PlanError, PlanMetadata,
    Policy, ScopeMetadata, SemanticHash, SourceMap, PLAN_REFERENCE_INVALID,
};
use crate::NodeId;

/// Programmatic fixture/compiler builder. It only yields an authoritative
/// `Plan` after normalization, verification, and semantic-hash computation.
#[derive(Debug, Clone)]
pub struct PlanBuilder {
    metadata: PlanMetadata,
    nodes: Vec<Node>,
    control_ports: Vec<ControlPort>,
    data_ports: Vec<DataPort>,
    control_edges: Vec<ControlEdge>,
    data_bindings: Vec<DataBinding>,
    phi_bindings: Vec<PhiBinding>,
    scopes: Vec<ScopeMetadata>,
    policies: Vec<Policy>,
    source_map: SourceMap,
}

impl PlanBuilder {
    pub fn new(metadata: PlanMetadata) -> Self {
        Self {
            metadata,
            nodes: Vec::new(),
            control_ports: Vec::new(),
            data_ports: Vec::new(),
            control_edges: Vec::new(),
            data_bindings: Vec::new(),
            phi_bindings: Vec::new(),
            scopes: Vec::new(),
            policies: Vec::new(),
            source_map: SourceMap::new(),
        }
    }

    /// Starts a semantic edit transaction from an already-authoritative Plan.
    /// The resulting builder is still inert: `build` recomputes the semantic
    /// hash and runs the full verifier before any edited Plan can escape.
    pub fn from_verified_plan(plan: &Plan) -> Result<Self, PlanError> {
        plan.verify()?;
        Ok(Self {
            metadata: plan.metadata.clone(),
            nodes: plan.nodes.clone(),
            control_ports: plan.control_ports.clone(),
            data_ports: plan.data_ports.clone(),
            control_edges: plan.control_edges.clone(),
            data_bindings: plan.data_bindings.clone(),
            phi_bindings: plan.phi_bindings.clone(),
            scopes: plan.scopes.clone(),
            policies: plan.policies.clone(),
            source_map: plan.source_map.clone(),
        })
    }

    /// Replaces one node descriptor while preserving its stable node and scope
    /// identities. Ports, edges and bindings remain explicit graph data and
    /// are checked against the replacement when `build` commits the edit.
    pub fn replace_node_kind(
        &mut self,
        node_id: &NodeId,
        kind: NodeKind,
    ) -> Result<&mut Self, PlanError> {
        let node = self
            .nodes
            .iter_mut()
            .find(|node| node.id() == node_id)
            .ok_or_else(|| {
                PlanError::new(
                    PLAN_REFERENCE_INVALID,
                    format!("graph edit references missing node '{node_id}'"),
                )
                .with_target(PlanDiagnosticTarget::Node {
                    node_id: node_id.clone(),
                })
            })?;
        *node = Node::new(node.id().clone(), node.scope_id().clone(), kind);
        Ok(self)
    }

    pub fn add_node(&mut self, value: Node) -> &mut Self {
        self.nodes.push(value);
        self
    }

    pub fn add_control_port(&mut self, value: ControlPort) -> &mut Self {
        self.control_ports.push(value);
        self
    }

    pub fn add_data_port(&mut self, value: DataPort) -> &mut Self {
        self.data_ports.push(value);
        self
    }

    pub fn add_control_edge(&mut self, value: ControlEdge) -> &mut Self {
        self.control_edges.push(value);
        self
    }

    pub fn add_data_binding(&mut self, value: DataBinding) -> &mut Self {
        self.data_bindings.push(value);
        self
    }

    pub fn add_phi_binding(&mut self, value: PhiBinding) -> &mut Self {
        self.phi_bindings.push(value);
        self
    }

    pub fn add_scope(&mut self, value: ScopeMetadata) -> &mut Self {
        self.scopes.push(value);
        self
    }

    pub fn add_policy(&mut self, value: Policy) -> &mut Self {
        self.policies.push(value);
        self
    }

    pub fn set_source_map(&mut self, value: SourceMap) -> &mut Self {
        self.source_map = value;
        self
    }

    pub fn with_node(mut self, value: Node) -> Self {
        self.add_node(value);
        self
    }

    pub fn with_control_port(mut self, value: ControlPort) -> Self {
        self.add_control_port(value);
        self
    }

    pub fn with_data_port(mut self, value: DataPort) -> Self {
        self.add_data_port(value);
        self
    }

    pub fn with_control_edge(mut self, value: ControlEdge) -> Self {
        self.add_control_edge(value);
        self
    }

    pub fn with_data_binding(mut self, value: DataBinding) -> Self {
        self.add_data_binding(value);
        self
    }

    pub fn with_phi_binding(mut self, value: PhiBinding) -> Self {
        self.add_phi_binding(value);
        self
    }

    pub fn with_scope(mut self, value: ScopeMetadata) -> Self {
        self.add_scope(value);
        self
    }

    pub fn with_policy(mut self, value: Policy) -> Self {
        self.add_policy(value);
        self
    }

    pub fn with_source_map(mut self, value: SourceMap) -> Self {
        self.set_source_map(value);
        self
    }

    pub fn build(self) -> Result<Plan, PlanError> {
        let placeholder = SemanticHash::from_digest(format!("sha256:{}", "0".repeat(64)));
        let mut plan = Plan::from_parts(
            self.metadata,
            self.nodes,
            self.control_ports,
            self.data_ports,
            self.control_edges,
            self.data_bindings,
            self.phi_bindings,
            self.scopes,
            self.policies,
            self.source_map,
            placeholder,
        );
        verify_plan(&plan)?;
        plan.semantic_hash = semantic_hash_for_plan(&plan)?;
        Ok(plan)
    }
}
