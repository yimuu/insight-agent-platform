use std::collections::{BTreeMap, BTreeSet};

use super::{
    BranchCaseId, BranchDescriptor, ControlEdge, ControlPort, ControlPortId, DataBinding,
    DataBindingId, DataPort, DataPortId, LeafTaskDescriptor, MergeDescriptor, Node, NodeKind,
    PhiBinding, Plan, PlanError, Policy, PolicyId, PortDirection, PortName, ScopeId, ScopeMetadata,
    ValueSource, PLAN_INDEX_INVALID,
};
use crate::engine::NodeId;

/// Closed leaf categories understood by worker registries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum LeafTaskKind {
    Llm,
    Action,
    Retrieval,
    Http,
    Tool,
}

impl LeafTaskKind {
    pub fn name(self) -> &'static str {
        match self {
            Self::Llm => "llm_task",
            Self::Action => "action_task",
            Self::Retrieval => "retrieval_task",
            Self::Http => "http_task",
            Self::Tool => "tool_task",
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct LeafDescriptorRef<'a> {
    kind: LeafTaskKind,
    descriptor: &'a LeafTaskDescriptor,
}

impl<'a> LeafDescriptorRef<'a> {
    pub fn kind(self) -> LeafTaskKind {
        self.kind
    }

    pub fn descriptor(self) -> &'a LeafTaskDescriptor {
        self.descriptor
    }
}

/// A single, unambiguous control edge resolved to its endpoint objects.
#[derive(Debug, Clone, Copy)]
pub struct ControlRoute<'a> {
    edge: &'a ControlEdge,
    output: &'a ControlPort,
    input: &'a ControlPort,
    predecessor: &'a Node,
    successor: &'a Node,
}

impl<'a> ControlRoute<'a> {
    pub fn edge(self) -> &'a ControlEdge {
        self.edge
    }

    pub fn output(self) -> &'a ControlPort {
        self.output
    }

    pub fn input(self) -> &'a ControlPort {
        self.input
    }

    pub fn predecessor(self) -> &'a Node {
        self.predecessor
    }

    pub fn successor(self) -> &'a Node {
        self.successor
    }
}

#[derive(Debug, Clone, Copy)]
pub struct MergeCorrelation<'a> {
    merge_node: &'a Node,
    merge: &'a MergeDescriptor,
    branch_node: &'a Node,
    branch: &'a BranchDescriptor,
}

impl<'a> MergeCorrelation<'a> {
    pub fn merge_node(self) -> &'a Node {
        self.merge_node
    }

    pub fn merge(self) -> &'a MergeDescriptor {
        self.merge
    }

    pub fn branch_node(self) -> &'a Node {
        self.branch_node
    }

    pub fn branch(self) -> &'a BranchDescriptor {
        self.branch
    }

    pub fn input_for_case(self, case_id: &BranchCaseId) -> Option<&'a ControlPortId> {
        self.merge.arms.get(case_id)
    }
}

/// Immutable scheduler-facing projection over a verified Canonical Plan.
///
/// Construction always re-verifies the Plan, then builds ordered maps. Public
/// APIs only return shared references, so callers cannot mutate the authority
/// that was verified. All ID lookups are O(log n); returned vectors are sorted.
#[derive(Debug)]
pub struct PlanIndex<'a> {
    plan: &'a Plan,
    nodes: BTreeMap<NodeId, &'a Node>,
    control_ports: BTreeMap<ControlPortId, &'a ControlPort>,
    data_ports: BTreeMap<DataPortId, &'a DataPort>,
    control_edges: BTreeMap<super::ControlEdgeId, &'a ControlEdge>,
    data_bindings: BTreeMap<DataBindingId, &'a DataBinding>,
    phi_bindings: BTreeMap<super::PhiBindingId, &'a PhiBinding>,
    scopes: BTreeMap<ScopeId, &'a ScopeMetadata>,
    policies: BTreeMap<PolicyId, &'a Policy>,
    control_inputs: BTreeMap<NodeId, Vec<ControlPortId>>,
    control_outputs: BTreeMap<NodeId, Vec<ControlPortId>>,
    data_inputs: BTreeMap<NodeId, Vec<DataPortId>>,
    data_outputs: BTreeMap<NodeId, Vec<DataPortId>>,
    control_ports_by_name: BTreeMap<(NodeId, PortDirection, PortName), ControlPortId>,
    data_ports_by_name: BTreeMap<(NodeId, PortDirection, PortName), DataPortId>,
    route_by_output: BTreeMap<ControlPortId, &'a ControlEdge>,
    incoming_by_input: BTreeMap<ControlPortId, &'a ControlEdge>,
    successors: BTreeMap<NodeId, Vec<NodeId>>,
    predecessors: BTreeMap<NodeId, Vec<NodeId>>,
    binding_by_input: BTreeMap<DataPortId, &'a DataBinding>,
    phi_by_output: BTreeMap<DataPortId, &'a PhiBinding>,
    policies_by_node: BTreeMap<NodeId, Vec<PolicyId>>,
    branch_case_outputs: BTreeMap<(NodeId, BranchCaseId), ControlPortId>,
}

impl<'a> PlanIndex<'a> {
    pub fn new(plan: &'a Plan) -> Result<Self, PlanError> {
        plan.verify()?;

        let nodes = plan.nodes().iter().map(|v| (v.id().clone(), v)).collect();
        let control_ports: BTreeMap<_, _> = plan
            .control_ports()
            .iter()
            .map(|v| (v.id().clone(), v))
            .collect();
        let data_ports: BTreeMap<_, _> = plan
            .data_ports()
            .iter()
            .map(|v| (v.id().clone(), v))
            .collect();
        let control_edges = plan
            .control_edges()
            .iter()
            .map(|v| (v.id().clone(), v))
            .collect();
        let data_bindings = plan
            .data_bindings()
            .iter()
            .map(|v| (v.id().clone(), v))
            .collect();
        let phi_bindings = plan
            .phi_bindings()
            .iter()
            .map(|v| (v.id().clone(), v))
            .collect();
        let scopes = plan.scopes().iter().map(|v| (v.id().clone(), v)).collect();
        let policies = plan
            .policies()
            .iter()
            .map(|v| (v.id().clone(), v))
            .collect();

        let mut index = Self {
            plan,
            nodes,
            control_ports,
            data_ports,
            control_edges,
            data_bindings,
            phi_bindings,
            scopes,
            policies,
            control_inputs: BTreeMap::new(),
            control_outputs: BTreeMap::new(),
            data_inputs: BTreeMap::new(),
            data_outputs: BTreeMap::new(),
            control_ports_by_name: BTreeMap::new(),
            data_ports_by_name: BTreeMap::new(),
            route_by_output: BTreeMap::new(),
            incoming_by_input: BTreeMap::new(),
            successors: BTreeMap::new(),
            predecessors: BTreeMap::new(),
            binding_by_input: BTreeMap::new(),
            phi_by_output: BTreeMap::new(),
            policies_by_node: BTreeMap::new(),
            branch_case_outputs: BTreeMap::new(),
        };
        index.build_runtime_projection()?;
        Ok(index)
    }

    fn build_runtime_projection(&mut self) -> Result<(), PlanError> {
        for port in self.control_ports.values() {
            let ports = match port.direction() {
                PortDirection::Input => &mut self.control_inputs,
                PortDirection::Output => &mut self.control_outputs,
            };
            ports
                .entry(port.owner().clone())
                .or_default()
                .push(port.id().clone());
            if self
                .control_ports_by_name
                .insert(
                    (port.owner().clone(), port.direction(), port.name().clone()),
                    port.id().clone(),
                )
                .is_some()
            {
                return Err(index_error("ambiguous named control port"));
            }
        }
        for port in self.data_ports.values() {
            let ports = match port.direction() {
                PortDirection::Input => &mut self.data_inputs,
                PortDirection::Output => &mut self.data_outputs,
            };
            ports
                .entry(port.owner().clone())
                .or_default()
                .push(port.id().clone());
            if self
                .data_ports_by_name
                .insert(
                    (port.owner().clone(), port.direction(), port.name().clone()),
                    port.id().clone(),
                )
                .is_some()
            {
                return Err(index_error("ambiguous named data port"));
            }
        }
        for values in self
            .control_inputs
            .values_mut()
            .chain(self.control_outputs.values_mut())
        {
            values.sort();
        }
        for values in self
            .data_inputs
            .values_mut()
            .chain(self.data_outputs.values_mut())
        {
            values.sort();
        }

        let mut successor_sets: BTreeMap<NodeId, BTreeSet<NodeId>> = BTreeMap::new();
        let mut predecessor_sets: BTreeMap<NodeId, BTreeSet<NodeId>> = BTreeMap::new();
        for edge in self.control_edges.values() {
            if self
                .route_by_output
                .insert(edge.from().clone(), edge)
                .is_some()
            {
                return Err(index_error(format!(
                    "control output '{}' has multiple successors",
                    edge.from()
                )));
            }
            if self
                .incoming_by_input
                .insert(edge.to().clone(), edge)
                .is_some()
            {
                return Err(index_error(format!(
                    "control input '{}' has multiple predecessors",
                    edge.to()
                )));
            }
            let from = self
                .control_port(edge.from())
                .ok_or_else(|| index_error("control edge source disappeared"))?;
            let to = self
                .control_port(edge.to())
                .ok_or_else(|| index_error("control edge target disappeared"))?;
            successor_sets
                .entry(from.owner().clone())
                .or_default()
                .insert(to.owner().clone());
            predecessor_sets
                .entry(to.owner().clone())
                .or_default()
                .insert(from.owner().clone());
        }
        self.successors = successor_sets
            .into_iter()
            .map(|(id, values)| (id, values.into_iter().collect()))
            .collect();
        self.predecessors = predecessor_sets
            .into_iter()
            .map(|(id, values)| (id, values.into_iter().collect()))
            .collect();

        for binding in self.data_bindings.values() {
            if self
                .binding_by_input
                .insert(binding.to().clone(), binding)
                .is_some()
            {
                return Err(index_error(format!(
                    "data input '{}' has multiple bindings",
                    binding.to()
                )));
            }
        }
        for phi in self.phi_bindings.values() {
            if self
                .phi_by_output
                .insert(phi.output().clone(), phi)
                .is_some()
            {
                return Err(index_error(format!(
                    "data output '{}' has multiple Phi bindings",
                    phi.output()
                )));
            }
        }
        for policy in self.policies.values() {
            self.policies_by_node
                .entry(policy.node_id().clone())
                .or_default()
                .push(policy.id().clone());
        }
        for ids in self.policies_by_node.values_mut() {
            ids.sort();
        }
        for node in self.nodes.values() {
            if let NodeKind::Branch(descriptor) = node.kind() {
                for case in &descriptor.cases {
                    if self
                        .branch_case_outputs
                        .insert(
                            (node.id().clone(), case.case_id.clone()),
                            case.output_port.clone(),
                        )
                        .is_some()
                    {
                        return Err(index_error(format!(
                            "Branch '{}' has an ambiguous case '{}'",
                            node.id(),
                            case.case_id
                        )));
                    }
                }
            }
        }
        Ok(())
    }

    pub fn semantic_hash(&self) -> &super::SemanticHash {
        self.plan.semantic_hash()
    }

    pub fn metadata(&self) -> &'a super::PlanMetadata {
        self.plan.metadata()
    }

    pub fn entry_node(&self) -> &'a Node {
        self.nodes
            .get(self.plan.metadata().entry_node_id())
            .copied()
            .expect("PlanIndex construction verified entry existence")
    }

    pub fn node(&self, id: &NodeId) -> Option<&'a Node> {
        self.nodes.get(id).copied()
    }

    pub fn nodes(&self) -> impl Iterator<Item = &'a Node> + '_ {
        self.nodes.values().copied()
    }

    pub fn control_port(&self, id: &ControlPortId) -> Option<&'a ControlPort> {
        self.control_ports.get(id).copied()
    }

    pub fn data_port(&self, id: &DataPortId) -> Option<&'a DataPort> {
        self.data_ports.get(id).copied()
    }

    pub fn control_edge(&self, id: &super::ControlEdgeId) -> Option<&'a ControlEdge> {
        self.control_edges.get(id).copied()
    }

    pub fn data_binding(&self, id: &DataBindingId) -> Option<&'a DataBinding> {
        self.data_bindings.get(id).copied()
    }

    pub fn phi_binding(&self, id: &super::PhiBindingId) -> Option<&'a PhiBinding> {
        self.phi_bindings.get(id).copied()
    }

    pub fn scope(&self, id: &ScopeId) -> Option<&'a ScopeMetadata> {
        self.scopes.get(id).copied()
    }

    pub fn policy(&self, id: &PolicyId) -> Option<&'a Policy> {
        self.policies.get(id).copied()
    }

    pub fn control_inputs(&self, node: &NodeId) -> &[ControlPortId] {
        self.control_inputs
            .get(node)
            .map(Vec::as_slice)
            .unwrap_or(&[])
    }

    pub fn control_outputs(&self, node: &NodeId) -> &[ControlPortId] {
        self.control_outputs
            .get(node)
            .map(Vec::as_slice)
            .unwrap_or(&[])
    }

    pub fn data_inputs(&self, node: &NodeId) -> &[DataPortId] {
        self.data_inputs.get(node).map(Vec::as_slice).unwrap_or(&[])
    }

    pub fn data_outputs(&self, node: &NodeId) -> &[DataPortId] {
        self.data_outputs
            .get(node)
            .map(Vec::as_slice)
            .unwrap_or(&[])
    }

    pub fn control_port_named(
        &self,
        node: &NodeId,
        direction: PortDirection,
        name: &PortName,
    ) -> Option<&'a ControlPort> {
        self.control_ports_by_name
            .get(&(node.clone(), direction, name.clone()))
            .and_then(|id| self.control_port(id))
    }

    pub fn data_port_named(
        &self,
        node: &NodeId,
        direction: PortDirection,
        name: &PortName,
    ) -> Option<&'a DataPort> {
        self.data_ports_by_name
            .get(&(node.clone(), direction, name.clone()))
            .and_then(|id| self.data_port(id))
    }

    pub fn successors(&self, node: &NodeId) -> &[NodeId] {
        self.successors.get(node).map(Vec::as_slice).unwrap_or(&[])
    }

    pub fn predecessors(&self, node: &NodeId) -> &[NodeId] {
        self.predecessors
            .get(node)
            .map(Vec::as_slice)
            .unwrap_or(&[])
    }

    pub fn successor_for_output(
        &self,
        output: &ControlPortId,
    ) -> Result<Option<ControlRoute<'a>>, PlanError> {
        let Some(edge) = self.route_by_output.get(output).copied() else {
            return Ok(None);
        };
        self.route(edge).map(Some)
    }

    pub fn predecessor_for_input(
        &self,
        input: &ControlPortId,
    ) -> Result<Option<ControlRoute<'a>>, PlanError> {
        let Some(edge) = self.incoming_by_input.get(input).copied() else {
            return Ok(None);
        };
        self.route(edge).map(Some)
    }

    pub fn branch_case_route(
        &self,
        branch_id: &NodeId,
        case_id: &BranchCaseId,
    ) -> Result<ControlRoute<'a>, PlanError> {
        let branch = self
            .node(branch_id)
            .ok_or_else(|| index_error(format!("unknown Branch node '{branch_id}'")))?;
        let NodeKind::Branch(_) = branch.kind() else {
            return Err(index_error(format!("node '{branch_id}' is not a Branch")));
        };
        let output = self
            .branch_case_outputs
            .get(&(branch_id.clone(), case_id.clone()))
            .ok_or_else(|| index_error(format!("Branch '{branch_id}' has no case '{case_id}'")))?;
        self.successor_for_output(output)?.ok_or_else(|| {
            index_error(format!(
                "Branch '{branch_id}' case '{case_id}' has no successor"
            ))
        })
    }

    pub fn merge_correlation(&self, merge_id: &NodeId) -> Result<MergeCorrelation<'a>, PlanError> {
        let merge_node = self
            .node(merge_id)
            .ok_or_else(|| index_error(format!("unknown Merge node '{merge_id}'")))?;
        let NodeKind::Merge(merge) = merge_node.kind() else {
            return Err(index_error(format!("node '{merge_id}' is not a Merge")));
        };
        let branch_node = self.node(&merge.branch_node_id).ok_or_else(|| {
            index_error(format!(
                "Merge '{merge_id}' references missing Branch '{}'",
                merge.branch_node_id
            ))
        })?;
        let NodeKind::Branch(branch) = branch_node.kind() else {
            return Err(index_error(format!(
                "Merge '{merge_id}' correlation target is not a Branch"
            )));
        };
        Ok(MergeCorrelation {
            merge_node,
            merge,
            branch_node,
            branch,
        })
    }

    pub fn binding_for_input(&self, input: &DataPortId) -> Option<&'a DataBinding> {
        self.binding_by_input.get(input).copied()
    }

    pub fn source_for_input(&self, input: &DataPortId) -> Option<&'a ValueSource> {
        self.binding_for_input(input).map(DataBinding::source)
    }

    pub fn phi_for_output(&self, output: &DataPortId) -> Option<&'a PhiBinding> {
        self.phi_by_output.get(output).copied()
    }

    pub fn policies_for_node(&self, node: &NodeId) -> Vec<&'a Policy> {
        self.policies_by_node
            .get(node)
            .into_iter()
            .flatten()
            .filter_map(|id| self.policy(id))
            .collect()
    }

    pub fn leaf_descriptor(&self, node: &NodeId) -> Option<LeafDescriptorRef<'a>> {
        let node = self.node(node)?;
        let (kind, descriptor) = match node.kind() {
            NodeKind::LlmTask(value) => (LeafTaskKind::Llm, value),
            NodeKind::ActionTask(value) => (LeafTaskKind::Action, value),
            NodeKind::RetrievalTask(value) => (LeafTaskKind::Retrieval, value),
            NodeKind::HttpTask(value) => (LeafTaskKind::Http, value),
            NodeKind::ToolTask(value) => (LeafTaskKind::Tool, value),
            _ => return None,
        };
        Some(LeafDescriptorRef { kind, descriptor })
    }

    fn route(&self, edge: &'a ControlEdge) -> Result<ControlRoute<'a>, PlanError> {
        let output = self
            .control_port(edge.from())
            .ok_or_else(|| index_error("control route output is missing"))?;
        let input = self
            .control_port(edge.to())
            .ok_or_else(|| index_error("control route input is missing"))?;
        let predecessor = self
            .node(output.owner())
            .ok_or_else(|| index_error("control route predecessor is missing"))?;
        let successor = self
            .node(input.owner())
            .ok_or_else(|| index_error("control route successor is missing"))?;
        Ok(ControlRoute {
            edge,
            output,
            input,
            predecessor,
            successor,
        })
    }
}

fn index_error(message: impl Into<String>) -> PlanError {
    PlanError::new(PLAN_INDEX_INVALID, message)
}
