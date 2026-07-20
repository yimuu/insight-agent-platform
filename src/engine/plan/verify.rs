use std::collections::{BTreeMap, BTreeSet, VecDeque};

use serde_json::Value;

use super::{
    expression::{
        analyze_cel_expression, analyze_match_program, analyze_value_program, MatchProgram,
        ValueProgram,
    },
    BranchDescriptor, CollectDescriptor, CollectSource, ControlEdgeId, ControlPort, ControlPortId,
    DataBindingId, DataPort, DataPortId, DescriptorValue, ExpressionLanguage, ForkDescriptor,
    JoinDescriptor, LoopDescriptor, MergeDescriptor, Node, NodeKind, Plan, PlanDiagnosticTarget,
    PlanError, PlanJoinMode, PlanProperty, PlanType, PolicyKind, PortDirection, PureExpression,
    ScopeId, ScopeKind, ScopeMetadata, SourceMapPolicy, SourceSpan, ValueSource,
    CEL_EXPRESSION_ENGINE_VERSION, DSL_MAJOR_VERSION, LITERAL_EXPRESSION_ENGINE_VERSION,
    MATCH_EXPRESSION_ENGINE_VERSION, PLAN_BRANCH_INVALID, PLAN_CONTROL_CYCLE, PLAN_DATA_CYCLE,
    PLAN_DESCRIPTOR_INVALID, PLAN_DOMINANCE_INVALID, PLAN_FORK_INVALID, PLAN_ID_DUPLICATE,
    PLAN_JOIN_INVALID, PLAN_LOOP_INVALID, PLAN_MERGE_INVALID, PLAN_PHI_INVALID,
    PLAN_POLICY_INVALID, PLAN_PORT_INVALID, PLAN_REACHABILITY_INVALID, PLAN_REFERENCE_INVALID,
    PLAN_SCOPE_INVALID, PLAN_TERMINAL_INVALID, PLAN_TYPE_MISMATCH, PLAN_VERSION_UNSUPPORTED,
    PLAN_WIRE_INVALID, PLAN_WIRE_VERSION, VALUE_EXPRESSION_ENGINE_VERSION,
};
use crate::engine::NodeId;

const MAX_EXPRESSION_BYTES: usize = 64 * 1024;
const MAX_DESCRIPTOR_STRING_BYTES: usize = 64 * 1024;
const MAX_DESCRIPTOR_COLLECTION_ITEMS: usize = 4096;
const MAX_DESCRIPTOR_DEPTH: usize = 64;
const MAX_RUN_INPUT_PATH_SEGMENTS: usize = 64;
const MAX_NAME_BYTES: usize = 256;
const MAX_SAFE_SEMANTIC_INTEGER: u64 = (1_u64 << 53) - 1;

struct Index<'a> {
    nodes: BTreeMap<NodeId, &'a Node>,
    control_ports: BTreeMap<ControlPortId, &'a ControlPort>,
    data_ports: BTreeMap<DataPortId, &'a DataPort>,
    scopes: BTreeMap<ScopeId, &'a ScopeMetadata>,
    node_control_inputs: BTreeMap<NodeId, Vec<ControlPortId>>,
    node_control_outputs: BTreeMap<NodeId, Vec<ControlPortId>>,
    node_data_inputs: BTreeMap<NodeId, Vec<DataPortId>>,
    node_data_outputs: BTreeMap<NodeId, Vec<DataPortId>>,
    node_successors: BTreeMap<NodeId, BTreeSet<NodeId>>,
    node_predecessors: BTreeMap<NodeId, BTreeSet<NodeId>>,
    port_graph: BTreeMap<ControlPortId, BTreeSet<ControlPortId>>,
    incoming_control: BTreeMap<ControlPortId, ControlEdgeId>,
    outgoing_control: BTreeMap<ControlPortId, Vec<ControlEdgeId>>,
    bound_data_inputs: BTreeMap<DataPortId, DataBindingId>,
    loop_continue_inputs: BTreeSet<ControlPortId>,
}

pub(super) fn verify_plan(plan: &Plan) -> Result<(), PlanError> {
    verify_plan_inner(plan).map_err(|error| attach_source_diagnostic(plan, error))
}

fn verify_plan_inner(plan: &Plan) -> Result<(), PlanError> {
    verify_metadata(plan)?;
    let mut index = build_index(plan)?;
    verify_scopes(plan, &index)?;
    verify_control_edges(plan, &mut index)?;
    verify_control_scope_crossings(plan, &index)?;
    verify_reachability(plan, &index)?;
    verify_cycles(plan, &index)?;
    let dominators = compute_dominators(plan, &index);
    verify_scope_capture_dominance(plan, &index, &dominators)?;
    verify_data_bindings(plan, &mut index, &dominators)?;
    verify_node_descriptors(plan, &index, &dominators)?;
    verify_phi_bindings(plan, &index, &dominators)?;
    verify_terminal_coverage(plan, &index)?;
    verify_policies(plan, &index)?;
    verify_source_map(plan, &index)?;
    Ok(())
}

fn attach_source_diagnostic(plan: &Plan, error: PlanError) -> PlanError {
    let source_span = match error.target() {
        Some(PlanDiagnosticTarget::Node { node_id }) => plan.source_map.node(node_id),
        Some(PlanDiagnosticTarget::ControlPort { port_id, .. }) => {
            plan.source_map.control_port(port_id)
        }
        Some(PlanDiagnosticTarget::DataPort { port_id, .. }) => plan.source_map.data_port(port_id),
        Some(PlanDiagnosticTarget::ControlEdge { edge_id }) => {
            plan.source_map.control_edge(edge_id)
        }
        None => None,
    };
    match source_span {
        Some(source_span) => error.with_source_span_if_absent(source_span.clone()),
        None => error,
    }
}

fn verify_metadata(plan: &Plan) -> Result<(), PlanError> {
    if plan.metadata.wire_version != PLAN_WIRE_VERSION
        || plan.metadata.dsl_version != DSL_MAJOR_VERSION
    {
        return Err(PlanError::new(
            PLAN_VERSION_UNSUPPORTED,
            format!(
                "unsupported Plan versions: wire={}, dsl={}",
                plan.metadata.wire_version, plan.metadata.dsl_version
            ),
        ));
    }
    verify_input_contract(plan)?;
    verify_canonical_type(&plan.metadata.output_type, "workflow output")?;
    verify_canonical_type(&plan.metadata.error_type, "workflow error")?;
    let safe_error = PlanType::safe_error().map_err(|failure| {
        PlanError::new(
            PLAN_TYPE_MISMATCH,
            format!("canonical safe error contract is invalid: {failure}"),
        )
    })?;
    if plan.metadata.error_type != safe_error {
        return Err(PlanError::new(
            PLAN_TYPE_MISMATCH,
            "workflow error contract must equal the canonical SafeError contract",
        ));
    }
    Ok(())
}

fn verify_input_contract(plan: &Plan) -> Result<(), PlanError> {
    let contract = plan.metadata.input_contract();
    verify_canonical_type(contract.accepted_type(), "workflow accepted input")?;
    let run_type = contract.run_type().map_err(|failure| {
        PlanError::new(
            PLAN_TYPE_MISMATCH,
            format!("workflow normalized input contract is invalid: {failure}"),
        )
    })?;
    verify_canonical_type(&run_type, "workflow normalized input")?;
    if contract.defaults().is_empty() {
        return Ok(());
    }
    let PlanType::Object {
        properties,
        additional_properties: _,
    } = contract.accepted_type()
    else {
        return Err(PlanError::new(
            PLAN_TYPE_MISMATCH,
            "workflow input defaults require a named object input contract",
        ));
    };
    for (name, value) in contract.defaults() {
        validate_name("input default field", name)?;
        validate_json_literal(value)?;
        let property = properties.get(name).ok_or_else(|| {
            PlanError::new(
                PLAN_TYPE_MISMATCH,
                format!("input default references unknown field '{name}'"),
            )
        })?;
        if property.required || !property.value_type.accepts_literal(value).unwrap_or(false) {
            return Err(PlanError::new(
                PLAN_TYPE_MISMATCH,
                format!("input default for '{name}' violates its presence or value contract"),
            ));
        }
    }
    Ok(())
}

fn build_index(plan: &Plan) -> Result<Index<'_>, PlanError> {
    let mut nodes = BTreeMap::new();
    for node in &plan.nodes {
        if nodes.insert(node.id.clone(), node).is_some() {
            return duplicate("node", &node.id).map_err(|error| {
                error.with_target(PlanDiagnosticTarget::Node {
                    node_id: node.id.clone(),
                })
            });
        }
    }
    if !nodes.contains_key(&plan.metadata.entry_node_id) {
        return Err(PlanError::new(
            PLAN_REFERENCE_INVALID,
            format!(
                "entry node '{}' does not exist",
                plan.metadata.entry_node_id
            ),
        )
        .with_target(PlanDiagnosticTarget::Node {
            node_id: plan.metadata.entry_node_id.clone(),
        }));
    }

    let mut scopes = BTreeMap::new();
    for scope in &plan.scopes {
        if scopes.insert(scope.id.clone(), scope).is_some() {
            return duplicate("scope", &scope.id);
        }
    }

    let mut control_ports = BTreeMap::new();
    let mut data_ports = BTreeMap::new();
    let mut all_port_ids = BTreeSet::new();
    let mut node_control_inputs = BTreeMap::new();
    let mut node_control_outputs = BTreeMap::new();
    let mut node_data_inputs = BTreeMap::new();
    let mut node_data_outputs = BTreeMap::new();
    let mut port_names = BTreeSet::new();

    for port in &plan.control_ports {
        if !all_port_ids.insert(port.id.as_str().to_owned()) {
            return duplicate("port", &port.id).map_err(|error| {
                error.with_target(PlanDiagnosticTarget::ControlPort {
                    port_id: port.id.clone(),
                    node_id: Some(port.owner.clone()),
                })
            });
        }
        if !nodes.contains_key(&port.owner) {
            return Err(PlanError::new(
                PLAN_REFERENCE_INVALID,
                format!(
                    "control port '{}' references missing owner '{}'",
                    port.id, port.owner
                ),
            )
            .with_target(PlanDiagnosticTarget::ControlPort {
                port_id: port.id.clone(),
                node_id: Some(port.owner.clone()),
            }));
        }
        let name_key = (
            port.owner.clone(),
            port.direction,
            "control",
            port.name.clone(),
        );
        if !port_names.insert(name_key) {
            return Err(PlanError::new(
                PLAN_PORT_INVALID,
                format!(
                    "node '{}' has duplicate {:?} control port name '{}'",
                    port.owner, port.direction, port.name
                ),
            )
            .with_target(PlanDiagnosticTarget::ControlPort {
                port_id: port.id.clone(),
                node_id: Some(port.owner.clone()),
            }));
        }
        match port.direction {
            PortDirection::Input => node_control_inputs
                .entry(port.owner.clone())
                .or_insert_with(Vec::new)
                .push(port.id.clone()),
            PortDirection::Output => node_control_outputs
                .entry(port.owner.clone())
                .or_insert_with(Vec::new)
                .push(port.id.clone()),
        }
        control_ports.insert(port.id.clone(), port);
    }

    for port in &plan.data_ports {
        if !all_port_ids.insert(port.id.as_str().to_owned()) {
            return duplicate("port", &port.id).map_err(|error| {
                error.with_target(PlanDiagnosticTarget::DataPort {
                    port_id: port.id.clone(),
                    node_id: Some(port.owner.clone()),
                })
            });
        }
        if !nodes.contains_key(&port.owner) {
            return Err(PlanError::new(
                PLAN_REFERENCE_INVALID,
                format!(
                    "data port '{}' references missing owner '{}'",
                    port.id, port.owner
                ),
            )
            .with_target(PlanDiagnosticTarget::DataPort {
                port_id: port.id.clone(),
                node_id: Some(port.owner.clone()),
            }));
        }
        verify_canonical_type(&port.value_type, &format!("data port '{}'", port.id)).map_err(
            |error| {
                error.with_target(PlanDiagnosticTarget::DataPort {
                    port_id: port.id.clone(),
                    node_id: Some(port.owner.clone()),
                })
            },
        )?;
        if port.direction == PortDirection::Output && port.required {
            return Err(PlanError::new(
                PLAN_PORT_INVALID,
                format!(
                    "data output '{}' cannot be marked required; required is an input contract",
                    port.id
                ),
            )
            .with_target(PlanDiagnosticTarget::DataPort {
                port_id: port.id.clone(),
                node_id: Some(port.owner.clone()),
            }));
        }
        let name_key = (
            port.owner.clone(),
            port.direction,
            "data",
            port.name.clone(),
        );
        if !port_names.insert(name_key) {
            return Err(PlanError::new(
                PLAN_PORT_INVALID,
                format!(
                    "node '{}' has duplicate {:?} data port name '{}'",
                    port.owner, port.direction, port.name
                ),
            )
            .with_target(PlanDiagnosticTarget::DataPort {
                port_id: port.id.clone(),
                node_id: Some(port.owner.clone()),
            }));
        }
        match port.direction {
            PortDirection::Input => node_data_inputs
                .entry(port.owner.clone())
                .or_insert_with(Vec::new)
                .push(port.id.clone()),
            PortDirection::Output => node_data_outputs
                .entry(port.owner.clone())
                .or_insert_with(Vec::new)
                .push(port.id.clone()),
        }
        data_ports.insert(port.id.clone(), port);
    }

    for ports in node_control_inputs.values_mut() {
        ports.sort();
    }
    for ports in node_control_outputs.values_mut() {
        ports.sort();
    }
    for ports in node_data_inputs.values_mut() {
        ports.sort();
    }
    for ports in node_data_outputs.values_mut() {
        ports.sort();
    }

    let loop_continue_inputs = plan
        .nodes
        .iter()
        .flat_map(|node| match &node.kind {
            NodeKind::Loop(descriptor) => vec![descriptor.continue_input.clone()],
            NodeKind::ErrorBoundary(descriptor) => descriptor
                .protected_completed_input
                .iter()
                .chain(descriptor.handler_completed_input.iter())
                .chain(descriptor.finalizer_completed_input.iter())
                .cloned()
                .collect(),
            _ => Vec::new(),
        })
        .collect();

    Ok(Index {
        nodes,
        control_ports,
        data_ports,
        scopes,
        node_control_inputs,
        node_control_outputs,
        node_data_inputs,
        node_data_outputs,
        node_successors: BTreeMap::new(),
        node_predecessors: BTreeMap::new(),
        port_graph: BTreeMap::new(),
        incoming_control: BTreeMap::new(),
        outgoing_control: BTreeMap::new(),
        bound_data_inputs: BTreeMap::new(),
        loop_continue_inputs,
    })
}

fn verify_scopes(plan: &Plan, index: &Index<'_>) -> Result<(), PlanError> {
    let roots = plan
        .scopes
        .iter()
        .filter(|scope| matches!(scope.kind, ScopeKind::Root))
        .collect::<Vec<_>>();
    if roots.len() != 1 {
        return Err(PlanError::new(
            PLAN_SCOPE_INVALID,
            format!(
                "Plan must contain exactly one root scope, found {}",
                roots.len()
            ),
        ));
    }
    let root = roots[0];
    if root.parent.is_some() || root.owner_node.is_some() {
        return Err(PlanError::new(
            PLAN_SCOPE_INVALID,
            "root scope cannot have a parent or owner node",
        ));
    }

    for scope in &plan.scopes {
        match &scope.kind {
            ScopeKind::Root => {
                if scope.id != root.id {
                    return Err(PlanError::new(
                        PLAN_SCOPE_INVALID,
                        "only the unique root scope may use kind=root",
                    ));
                }
            }
            kind => {
                let Some(parent_id) = &scope.parent else {
                    return Err(PlanError::new(
                        PLAN_SCOPE_INVALID,
                        format!("non-root scope '{}' is missing a parent", scope.id),
                    ));
                };
                let Some(owner_id) = &scope.owner_node else {
                    return Err(PlanError::new(
                        PLAN_SCOPE_INVALID,
                        format!("non-root scope '{}' is missing an owner node", scope.id),
                    ));
                };
                if parent_id == &scope.id || !index.scopes.contains_key(parent_id) {
                    return Err(PlanError::new(
                        PLAN_SCOPE_INVALID,
                        format!("scope '{}' has an invalid parent '{}'", scope.id, parent_id),
                    ));
                }
                let owner = index.nodes.get(owner_id).ok_or_else(|| {
                    PlanError::new(
                        PLAN_SCOPE_INVALID,
                        format!("scope '{}' owner '{}' does not exist", scope.id, owner_id),
                    )
                })?;
                if &owner.scope_id != parent_id {
                    return Err(PlanError::new(
                        PLAN_SCOPE_INVALID,
                        format!(
                            "scope '{}' owner '{}' must belong to parent scope '{}'",
                            scope.id, owner_id, parent_id
                        ),
                    ));
                }
                verify_scope_kind_correlation(scope, kind, owner, index)?;
            }
        }

        let mut seen = BTreeSet::new();
        let mut cursor = Some(&scope.id);
        while let Some(id) = cursor {
            if !seen.insert(id.clone()) {
                return Err(PlanError::new(
                    PLAN_SCOPE_INVALID,
                    format!("scope parent cycle contains '{id}'"),
                ));
            }
            let current = index.scopes.get(id).ok_or_else(|| {
                PlanError::new(
                    PLAN_SCOPE_INVALID,
                    format!("scope ancestry references missing scope '{id}'"),
                )
            })?;
            cursor = current.parent.as_ref();
        }
        if !seen.contains(&root.id) {
            return Err(PlanError::new(
                PLAN_SCOPE_INVALID,
                format!("scope '{}' is not connected to the root scope", scope.id),
            ));
        }
    }

    for node in &plan.nodes {
        if !index.scopes.contains_key(&node.scope_id) {
            return Err(PlanError::new(
                PLAN_SCOPE_INVALID,
                format!(
                    "node '{}' references missing scope '{}'",
                    node.id, node.scope_id
                ),
            )
            .with_target(PlanDiagnosticTarget::Node {
                node_id: node.id.clone(),
            }));
        }
    }

    for node in &plan.nodes {
        let NodeKind::SubflowCall(descriptor) = &node.kind else {
            continue;
        };
        let correlated = plan
            .scopes
            .iter()
            .filter(|scope| {
                matches!(
                    &scope.kind,
                    ScopeKind::Subflow { call_node_id } if call_node_id == &node.id
                )
            })
            .collect::<Vec<_>>();
        if correlated.len() != 1 || correlated[0].id != descriptor.invocation_scope_id {
            return Err(PlanError::new(
                PLAN_SCOPE_INVALID,
                format!(
                    "SubflowCall '{}' must own exactly one declared invocation scope '{}'",
                    node.id, descriptor.invocation_scope_id
                ),
            )
            .with_target(PlanDiagnosticTarget::Node {
                node_id: node.id.clone(),
            }));
        }
    }
    let entry = index
        .nodes
        .get(&plan.metadata.entry_node_id)
        .expect("entry existence checked while indexing");
    if entry.scope_id != root.id {
        return Err(PlanError::new(
            PLAN_SCOPE_INVALID,
            "entry node must belong to the root scope",
        ));
    }

    for scope in &plan.scopes {
        for capture in &scope.captures {
            let port = require_data_port(index, capture)?;
            if port.direction != PortDirection::Output {
                return Err(PlanError::new(
                    PLAN_SCOPE_INVALID,
                    format!(
                        "scope '{}' capture '{}' is not an output",
                        scope.id, capture
                    ),
                ));
            }
            let source_node = index
                .nodes
                .get(&port.owner)
                .expect("data-port owners checked while indexing");
            let Some(parent) = &scope.parent else {
                return Err(PlanError::new(
                    PLAN_SCOPE_INVALID,
                    "root scope cannot declare captures",
                ));
            };
            if !is_scope_ancestor(&source_node.scope_id, parent, index) {
                return Err(PlanError::new(
                    PLAN_SCOPE_INVALID,
                    format!(
                        "scope '{}' capture '{}' does not originate in an ancestor scope",
                        scope.id, capture
                    ),
                ));
            }
        }
    }
    Ok(())
}

fn verify_scope_kind_correlation(
    scope: &ScopeMetadata,
    kind: &ScopeKind,
    owner: &Node,
    index: &Index<'_>,
) -> Result<(), PlanError> {
    let valid = match (kind, &owner.kind) {
        (ScopeKind::Lexical, _) => true,
        (
            ScopeKind::BranchArm {
                branch_node_id,
                case_id,
            },
            NodeKind::Branch(descriptor),
        ) => {
            branch_node_id == &owner.id
                && descriptor.cases.iter().any(|case| &case.case_id == case_id)
        }
        (
            ScopeKind::ForkLeg {
                fork_node_id,
                leg_id,
            },
            NodeKind::Fork(descriptor),
        ) => {
            fork_node_id == &owner.id
                && descriptor
                    .legs
                    .iter()
                    .any(|leg| &leg.leg_id == leg_id && leg.scope_id == scope.id)
        }
        (ScopeKind::MapBody { map_node_id }, NodeKind::Map(descriptor)) => {
            map_node_id == &owner.id && descriptor.body_scope_id == scope.id
        }
        (ScopeKind::LoopBody { loop_node_id }, NodeKind::Loop(_)) => loop_node_id == &owner.id,
        (ScopeKind::ErrorProtected { boundary_node_id }, NodeKind::ErrorBoundary(descriptor)) => {
            boundary_node_id == &owner.id && descriptor.protected_scope_id == scope.id
        }
        (ScopeKind::ErrorHandler { boundary_node_id }, NodeKind::ErrorBoundary(descriptor)) => {
            boundary_node_id == &owner.id && descriptor.handler_scope_id == scope.id
        }
        (ScopeKind::ErrorFinalizer { boundary_node_id }, NodeKind::ErrorBoundary(descriptor)) => {
            boundary_node_id == &owner.id
                && descriptor.finalizer_scope_id.as_ref() == Some(&scope.id)
        }
        (ScopeKind::Subflow { call_node_id }, NodeKind::SubflowCall(_)) => {
            call_node_id == &owner.id
        }
        _ => false,
    };
    if !valid {
        return Err(PlanError::new(
            PLAN_SCOPE_INVALID,
            format!(
                "scope '{}' kind is not correlated with owner node '{}' ({})",
                scope.id,
                owner.id,
                owner.kind.name()
            ),
        ));
    }
    let contains_nodes = index.nodes.values().any(|node| node.scope_id == scope.id);
    match kind {
        // A Subflow scope is a static contract for a dynamic child Run. Child
        // Plan nodes never become nodes of the parent Plan.
        ScopeKind::Subflow { .. } if contains_nodes => {
            return Err(PlanError::new(
                PLAN_SCOPE_INVALID,
                format!(
                    "Subflow invocation scope '{}' cannot contain Plan nodes",
                    scope.id
                ),
            ));
        }
        ScopeKind::Lexical | ScopeKind::Subflow { .. } => {}
        _ if !contains_nodes => {
            return Err(PlanError::new(
                PLAN_SCOPE_INVALID,
                format!("correlated child scope '{}' contains no nodes", scope.id),
            ));
        }
        _ => {}
    }
    Ok(())
}

fn verify_control_edges(plan: &Plan, index: &mut Index<'_>) -> Result<(), PlanError> {
    let mut ids = BTreeSet::new();
    let mut endpoints = BTreeSet::new();
    for edge in &plan.control_edges {
        (|| -> Result<(), PlanError> {
            if !ids.insert(edge.id.clone()) {
                return duplicate("control edge", &edge.id);
            }
            let from = require_control_port(index, &edge.from)?;
            let to = require_control_port(index, &edge.to)?;
            let from_owner = from.owner.clone();
            let to_owner = to.owner.clone();
            if from.direction != PortDirection::Output || to.direction != PortDirection::Input {
                return Err(PlanError::new(
                    PLAN_PORT_INVALID,
                    format!(
                        "control edge '{}' must connect output '{}' to input '{}'",
                        edge.id, edge.from, edge.to
                    ),
                ));
            }
            if !endpoints.insert((edge.from.clone(), edge.to.clone())) {
                return Err(PlanError::new(
                    PLAN_ID_DUPLICATE,
                    format!(
                        "duplicate control edge endpoints '{} -> {}'",
                        edge.from, edge.to
                    ),
                ));
            }
            if index
                .incoming_control
                .insert(edge.to.clone(), edge.id.clone())
                .is_some()
            {
                return Err(PlanError::new(
                    PLAN_PORT_INVALID,
                    format!(
                        "control input '{}' has more than one incoming edge",
                        edge.to
                    ),
                ));
            }
            index
                .outgoing_control
                .entry(edge.from.clone())
                .or_default()
                .push(edge.id.clone());
            index
                .node_successors
                .entry(from_owner.clone())
                .or_default()
                .insert(to_owner.clone());
            index
                .node_predecessors
                .entry(to_owner)
                .or_default()
                .insert(from_owner);
            index
                .port_graph
                .entry(edge.from.clone())
                .or_default()
                .insert(edge.to.clone());
            Ok(())
        })()
        .map_err(|error| {
            error.with_target(PlanDiagnosticTarget::ControlEdge {
                edge_id: edge.id.clone(),
            })
        })?;
    }

    // Traversal through a node connects each input to each declared output.
    for node in &plan.nodes {
        let inputs = index
            .node_control_inputs
            .get(&node.id)
            .map(Vec::as_slice)
            .unwrap_or_default();
        let outputs = index
            .node_control_outputs
            .get(&node.id)
            .map(Vec::as_slice)
            .unwrap_or_default();
        for input in inputs {
            for output in outputs {
                index
                    .port_graph
                    .entry(input.clone())
                    .or_default()
                    .insert(output.clone());
            }
        }
    }

    for node in &plan.nodes {
        if node.kind.is_terminal()
            && index
                .node_control_outputs
                .get(&node.id)
                .is_some_and(|ports| !ports.is_empty())
        {
            return Err(PlanError::new(
                PLAN_TERMINAL_INVALID,
                format!("terminal node '{}' cannot have control outputs", node.id),
            )
            .with_target(PlanDiagnosticTarget::Node {
                node_id: node.id.clone(),
            }));
        }
    }
    for (port, edges) in &index.outgoing_control {
        if edges.len() > 1 {
            return Err(PlanError::new(
                PLAN_PORT_INVALID,
                format!(
                    "control output '{}' fans out to {} edges; explicit Fork/Branch ports are required",
                    port,
                    edges.len()
                ),
            )
            .with_target(PlanDiagnosticTarget::ControlPort {
                port_id: port.clone(),
                node_id: index.control_ports.get(port).map(|port| port.owner.clone()),
            }));
        }
    }
    Ok(())
}

fn verify_control_scope_crossings(plan: &Plan, index: &Index<'_>) -> Result<(), PlanError> {
    for edge in &plan.control_edges {
        let from_port = require_control_port(index, &edge.from)?;
        let to_port = require_control_port(index, &edge.to)?;
        let from_node = index
            .nodes
            .get(&from_port.owner)
            .expect("port owners were checked");
        let to_node = index
            .nodes
            .get(&to_port.owner)
            .expect("port owners were checked");
        if from_node.scope_id == to_node.scope_id {
            continue;
        }
        let from_scope = index
            .scopes
            .get(&from_node.scope_id)
            .expect("node scopes were checked");
        let to_scope = index
            .scopes
            .get(&to_node.scope_id)
            .expect("node scopes were checked");

        let entering_child = to_scope.parent.as_ref() == Some(&from_scope.id)
            && to_scope.owner_node.as_ref() == Some(&from_node.id)
            && is_valid_scope_entry(to_scope, from_node, &edge.from);
        let leaving_child = from_scope.parent.as_ref() == Some(&to_scope.id)
            && is_valid_scope_exit(from_scope, to_node, &edge.to);
        if !entering_child && !leaving_child {
            return Err(PlanError::new(
                PLAN_SCOPE_INVALID,
                format!(
                    "control edge '{}' illegally crosses scope '{}' -> '{}'",
                    edge.id, from_scope.id, to_scope.id
                ),
            ));
        }
    }
    Ok(())
}

fn is_valid_scope_entry(child: &ScopeMetadata, source: &Node, source_port: &ControlPortId) -> bool {
    match (&child.kind, &source.kind) {
        (
            ScopeKind::BranchArm {
                branch_node_id,
                case_id,
            },
            NodeKind::Branch(descriptor),
        ) => {
            branch_node_id == &source.id
                && descriptor
                    .cases
                    .iter()
                    .any(|case| &case.case_id == case_id && &case.output_port == source_port)
        }
        (
            ScopeKind::ForkLeg {
                fork_node_id,
                leg_id,
            },
            NodeKind::Fork(descriptor),
        ) => {
            fork_node_id == &source.id
                && descriptor.legs.iter().any(|leg| {
                    &leg.leg_id == leg_id
                        && leg.scope_id == child.id
                        && &leg.output_port == source_port
                })
        }
        (ScopeKind::LoopBody { loop_node_id }, NodeKind::Loop(descriptor)) => {
            loop_node_id == &source.id && &descriptor.body_output == source_port
        }
        (ScopeKind::MapBody { map_node_id }, NodeKind::Map(_)) => map_node_id == &source.id,
        (ScopeKind::ErrorProtected { boundary_node_id }, NodeKind::ErrorBoundary(descriptor)) => {
            boundary_node_id == &source.id && &descriptor.protected_output == source_port
        }
        (ScopeKind::ErrorHandler { boundary_node_id }, NodeKind::ErrorBoundary(descriptor)) => {
            boundary_node_id == &source.id && &descriptor.handler_output == source_port
        }
        (ScopeKind::ErrorFinalizer { boundary_node_id }, NodeKind::ErrorBoundary(descriptor)) => {
            boundary_node_id == &source.id
                && descriptor.finalizer_output.as_ref() == Some(source_port)
        }
        (ScopeKind::Subflow { call_node_id }, NodeKind::SubflowCall(_)) => {
            call_node_id == &source.id
        }
        (ScopeKind::Lexical, _) => true,
        _ => false,
    }
}

fn is_valid_scope_exit(child: &ScopeMetadata, target: &Node, target_port: &ControlPortId) -> bool {
    match (&child.kind, &target.kind) {
        (
            ScopeKind::BranchArm {
                branch_node_id,
                case_id,
            },
            NodeKind::Merge(descriptor),
        ) => {
            &descriptor.branch_node_id == branch_node_id
                && descriptor.arms.get(case_id) == Some(target_port)
        }
        (
            ScopeKind::ForkLeg {
                fork_node_id,
                leg_id,
            },
            NodeKind::Join(descriptor),
        ) => {
            &descriptor.fork_node_id == fork_node_id
                // A missing member is diagnosed by the Join exact-set check;
                // if present, its statically correlated port must still match.
                && descriptor
                    .legs
                    .get(leg_id)
                    .is_none_or(|port| port == target_port)
        }
        (ScopeKind::MapBody { map_node_id }, NodeKind::Collect(descriptor)) => {
            matches!(
                &descriptor.source,
                CollectSource::Map { map_node_id: source }
                    | CollectSource::DynamicMap { map_node_id: source, .. }
                    if source == map_node_id
            )
        }
        (ScopeKind::LoopBody { loop_node_id }, NodeKind::Loop(descriptor)) => {
            &target.id == loop_node_id && &descriptor.continue_input == target_port
        }
        (ScopeKind::LoopBody { loop_node_id }, NodeKind::Collect(descriptor)) => {
            matches!(
                &descriptor.source,
                CollectSource::Loop {
                    loop_node_id: source,
                    break_input: Some(input),
                    ..
                } if source == loop_node_id && input == target_port
            )
        }
        (ScopeKind::ErrorProtected { boundary_node_id }, NodeKind::ErrorBoundary(descriptor)) => {
            &target.id == boundary_node_id
                && descriptor.protected_completed_input.as_ref() == Some(target_port)
        }
        (ScopeKind::ErrorHandler { boundary_node_id }, NodeKind::ErrorBoundary(descriptor)) => {
            &target.id == boundary_node_id
                && descriptor.handler_completed_input.as_ref() == Some(target_port)
        }
        (ScopeKind::ErrorFinalizer { boundary_node_id }, NodeKind::ErrorBoundary(descriptor)) => {
            &target.id == boundary_node_id
                && descriptor.finalizer_completed_input.as_ref() == Some(target_port)
        }
        (ScopeKind::Lexical, _) => true,
        _ => false,
    }
}

fn verify_reachability(plan: &Plan, index: &Index<'_>) -> Result<(), PlanError> {
    let mut reached = BTreeSet::new();
    let mut queue = VecDeque::from([plan.metadata.entry_node_id.clone()]);
    while let Some(node) = queue.pop_front() {
        if !reached.insert(node.clone()) {
            continue;
        }
        if let Some(next) = index.node_successors.get(&node) {
            queue.extend(next.iter().cloned());
        }
    }
    if reached.len() != plan.nodes.len() {
        let missing = plan
            .nodes
            .iter()
            .filter(|node| !reached.contains(&node.id))
            .map(|node| node.id.as_str())
            .collect::<Vec<_>>()
            .join(", ");
        return Err(PlanError::new(
            PLAN_REACHABILITY_INVALID,
            format!("unreachable Plan nodes: {missing}"),
        ));
    }
    Ok(())
}

fn verify_cycles(plan: &Plan, index: &Index<'_>) -> Result<(), PlanError> {
    let mut designated_back_edges = BTreeSet::new();
    for node in &plan.nodes {
        let NodeKind::ErrorBoundary(descriptor) = &node.kind else {
            continue;
        };
        let mut completions = Vec::new();
        if let Some(input) = &descriptor.protected_completed_input {
            completions.push((input, "protected"));
        }
        if let Some(input) = &descriptor.handler_completed_input {
            completions.push((input, "handler"));
        }
        if let Some(input) = &descriptor.finalizer_completed_input {
            completions.push((input, "finalizer"));
        }
        for (input, child_kind) in completions {
            let edge = plan
                .control_edges
                .iter()
                .find(|edge| &edge.to == input)
                .ok_or_else(|| {
                    PlanError::new(
                        PLAN_CONTROL_CYCLE,
                        format!(
                            "ErrorBoundary '{}' completion input has no child return edge",
                            node.id
                        ),
                    )
                })?;
            let source = require_control_port(index, &edge.from)?;
            let source_node = index
                .nodes
                .get(&source.owner)
                .expect("completion source owner exists");
            let source_scope = index
                .scopes
                .get(&source_node.scope_id)
                .expect("completion source scope exists");
            let correct_scope = match child_kind {
                "protected" => matches!(
                    &source_scope.kind,
                    ScopeKind::ErrorProtected { boundary_node_id }
                        if boundary_node_id == &node.id
                ),
                "handler" => matches!(
                    &source_scope.kind,
                    ScopeKind::ErrorHandler { boundary_node_id }
                        if boundary_node_id == &node.id
                ),
                "finalizer" => matches!(
                    &source_scope.kind,
                    ScopeKind::ErrorFinalizer { boundary_node_id }
                        if boundary_node_id == &node.id
                ),
                _ => unreachable!("closed boundary child kind"),
            };
            if !correct_scope {
                return Err(PlanError::new(
                    PLAN_CONTROL_CYCLE,
                    format!(
                        "ErrorBoundary '{}' completion edge returns from the wrong scope",
                        node.id
                    ),
                ));
            }
            designated_back_edges.insert(edge.id.clone());
        }
    }
    for node in &plan.nodes {
        let NodeKind::Loop(descriptor) = &node.kind else {
            continue;
        };
        for edge in &plan.control_edges {
            if edge.to == descriptor.continue_input {
                let source = require_control_port(index, &edge.from)?;
                let source_node = index
                    .nodes
                    .get(&source.owner)
                    .expect("port owners were checked");
                let source_scope = index
                    .scopes
                    .get(&source_node.scope_id)
                    .expect("node scopes were checked");
                if !matches!(
                    &source_scope.kind,
                    ScopeKind::LoopBody { loop_node_id } if loop_node_id == &node.id
                ) {
                    return Err(PlanError::new(
                        PLAN_CONTROL_CYCLE,
                        format!(
                            "edge '{}' enters Loop continue port from outside its LoopBody scope",
                            edge.id
                        ),
                    ));
                }
                designated_back_edges.insert(edge.id.clone());
            }
        }
        let has_break_exit = plan.nodes.iter().any(|candidate| {
            matches!(
                &candidate.kind,
                NodeKind::Collect(CollectDescriptor {
                    source: CollectSource::Loop {
                        loop_node_id,
                        break_input: Some(_),
                        ..
                    },
                    ..
                }) if loop_node_id == &node.id
            )
        });
        if !index
            .incoming_control
            .contains_key(&descriptor.continue_input)
            && !has_break_exit
        {
            return Err(PlanError::new(
                PLAN_LOOP_INVALID,
                format!("Loop '{}' continue input has no body back-edge", node.id),
            ));
        }
    }

    let mut successors: BTreeMap<NodeId, BTreeSet<NodeId>> = BTreeMap::new();
    let mut indegree = plan
        .nodes
        .iter()
        .map(|node| (node.id.clone(), 0_usize))
        .collect::<BTreeMap<_, _>>();
    for edge in &plan.control_edges {
        if designated_back_edges.contains(&edge.id) {
            continue;
        }
        let from = require_control_port(index, &edge.from)?;
        let to = require_control_port(index, &edge.to)?;
        if successors
            .entry(from.owner.clone())
            .or_default()
            .insert(to.owner.clone())
        {
            *indegree
                .get_mut(&to.owner)
                .expect("control-port owner exists") += 1;
        }
    }
    let mut queue = indegree
        .iter()
        .filter(|(_, degree)| **degree == 0)
        .map(|(node, _)| node.clone())
        .collect::<VecDeque<_>>();
    let mut visited = 0;
    while let Some(node) = queue.pop_front() {
        visited += 1;
        for next in successors.get(&node).into_iter().flatten() {
            let degree = indegree.get_mut(next).expect("successor owner exists");
            *degree -= 1;
            if *degree == 0 {
                queue.push_back(next.clone());
            }
        }
    }
    if visited != plan.nodes.len() {
        return Err(PlanError::new(
            PLAN_CONTROL_CYCLE,
            "control graph contains an arbitrary cycle; repetition must use a first-class Loop continue port",
        ));
    }
    Ok(())
}

fn verify_terminal_coverage(plan: &Plan, index: &Index<'_>) -> Result<(), PlanError> {
    let terminal_inputs = plan
        .nodes
        .iter()
        .filter(|node| node.kind.is_terminal())
        .flat_map(|node| {
            index
                .node_control_inputs
                .get(&node.id)
                .into_iter()
                .flatten()
                .cloned()
        })
        .collect::<BTreeSet<_>>();
    if !plan.nodes.iter().any(|node| node.kind.is_terminal()) {
        return Err(PlanError::new(
            PLAN_TERMINAL_INVALID,
            "Plan has no explicit Return or Raise terminal",
        ));
    }
    for node in &plan.nodes {
        if node.kind.is_terminal() {
            continue;
        }
        let outputs = index
            .node_control_outputs
            .get(&node.id)
            .map(Vec::as_slice)
            .unwrap_or_default();
        if outputs.is_empty() {
            return Err(PlanError::new(
                PLAN_TERMINAL_INVALID,
                format!("non-terminal node '{}' is a control dead-end", node.id),
            ));
        }
        for output in outputs {
            if !port_reaches_any(output, &terminal_inputs, index, true) {
                return Err(PlanError::new(
                    PLAN_TERMINAL_INVALID,
                    format!("control output '{}' has no path to Return/Raise", output),
                ));
            }
        }
    }
    Ok(())
}

fn compute_dominators(plan: &Plan, index: &Index<'_>) -> BTreeMap<NodeId, BTreeSet<NodeId>> {
    let all = plan
        .nodes
        .iter()
        .map(|node| node.id.clone())
        .collect::<BTreeSet<_>>();
    let entry = &plan.metadata.entry_node_id;
    let mut dominators = BTreeMap::new();
    for node in &plan.nodes {
        dominators.insert(
            node.id.clone(),
            if &node.id == entry {
                BTreeSet::from([node.id.clone()])
            } else {
                all.clone()
            },
        );
    }
    loop {
        let mut changed = false;
        for node in &plan.nodes {
            if &node.id == entry {
                continue;
            }
            let predecessors = index
                .node_predecessors
                .get(&node.id)
                .expect("all non-entry reachable nodes have predecessors");
            let mut values = predecessors.iter();
            let first = values.next().expect("reachable non-entry has predecessor");
            let mut next = dominators
                .get(first)
                .expect("predecessor has dominator set")
                .clone();
            for predecessor in values {
                next = next
                    .intersection(
                        dominators
                            .get(predecessor)
                            .expect("predecessor has dominator set"),
                    )
                    .cloned()
                    .collect();
            }
            next.insert(node.id.clone());
            if dominators.get(&node.id) != Some(&next) {
                dominators.insert(node.id.clone(), next);
                changed = true;
            }
        }
        if !changed {
            return dominators;
        }
    }
}

fn verify_scope_capture_dominance(
    plan: &Plan,
    index: &Index<'_>,
    dominators: &BTreeMap<NodeId, BTreeSet<NodeId>>,
) -> Result<(), PlanError> {
    for scope in &plan.scopes {
        let Some(owner_id) = &scope.owner_node else {
            continue;
        };
        for capture in &scope.captures {
            let source = require_data_port(index, capture)?;
            if !dominators
                .get(owner_id)
                .is_some_and(|values| values.contains(&source.owner))
            {
                return Err(PlanError::new(
                    PLAN_DOMINANCE_INVALID,
                    format!(
                        "captured output '{}' does not dominate scope '{}' creation owner '{}'",
                        capture, scope.id, owner_id
                    ),
                ));
            }
        }
    }
    Ok(())
}

fn verify_data_bindings(
    plan: &Plan,
    index: &mut Index<'_>,
    dominators: &BTreeMap<NodeId, BTreeSet<NodeId>>,
) -> Result<(), PlanError> {
    let mut ids = BTreeSet::new();
    for binding in &plan.data_bindings {
        if !ids.insert(binding.id.clone()) {
            return duplicate("data binding", &binding.id);
        }
        let target = require_data_port(index, &binding.to)?;
        if target.direction != PortDirection::Input {
            return Err(PlanError::new(
                PLAN_PORT_INVALID,
                format!("data binding '{}' target is not an input", binding.id),
            ));
        }
        if index
            .bound_data_inputs
            .insert(binding.to.clone(), binding.id.clone())
            .is_some()
        {
            return Err(PlanError::new(
                PLAN_PORT_INVALID,
                format!("data input '{}' has more than one binding", binding.to),
            ));
        }
    }
    // Resolve sources only after the complete input-binding set is known;
    // expression dependency validity must not depend on binding ID order.
    for binding in &plan.data_bindings {
        let target = require_data_port(index, &binding.to)?;
        let target_owner = target.owner.clone();
        let target_type = target.value_type.clone();
        if let ValueSource::Expression { expression } = &binding.source {
            if expression
                .dependencies
                .values()
                .any(|dependency| dependency == &binding.to)
            {
                return Err(PlanError::new(
                    PLAN_DOMINANCE_INVALID,
                    format!(
                        "data binding '{}' expression recursively depends on its own target input",
                        binding.id
                    ),
                ));
            }
        }
        let target_node = index
            .nodes
            .get(&target_owner)
            .expect("port owners checked while indexing");
        if matches!(binding.source, ValueSource::OptionalRunInput { .. }) {
            if target.required {
                return Err(PlanError::new(
                    PLAN_TYPE_MISMATCH,
                    format!(
                        "optional RunInput source cannot bind required input '{}'",
                        binding.to
                    ),
                ));
            }
            match target_node.kind() {
                NodeKind::LlmTask(descriptor) => {
                    verify_optional_llm_binding(descriptor, &binding.to)?;
                }
                NodeKind::SubflowCall(_) => {}
                _ => {
                    return Err(PlanError::new(
                        PLAN_TYPE_MISMATCH,
                        "optional RunInput sources require an absence-aware LLM or Subflow input",
                    ));
                }
            }
        }
        let source_type =
            verify_value_source(&binding.source, target_node, plan, index, dominators, None)?;
        if target.required && source_type == PlanType::Never {
            return Err(PlanError::new(
                PLAN_TYPE_MISMATCH,
                format!(
                    "required data input '{}' cannot be bound from Never",
                    binding.to
                ),
            ));
        }
        if !source_type.is_assignable_to(&target_type) {
            return Err(PlanError::new(
                PLAN_TYPE_MISMATCH,
                format!(
                    "data binding '{}' source type is not assignable to input '{}'",
                    binding.id, binding.to
                ),
            ));
        }
    }
    verify_data_binding_cycles(plan, index)?;
    for port in &plan.data_ports {
        if port.direction == PortDirection::Input
            && port.required
            && !index.bound_data_inputs.contains_key(&port.id)
        {
            return Err(PlanError::new(
                PLAN_PORT_INVALID,
                format!("required data input '{}' has no explicit binding", port.id),
            ));
        }
    }
    Ok(())
}

fn verify_optional_llm_binding(
    descriptor: &super::LeafTaskDescriptor,
    target: &DataPortId,
) -> Result<(), PlanError> {
    let Some(DescriptorValue::Object(bindings)) =
        descriptor.public_configuration.get("runtime_bindings")
    else {
        return Err(PlanError::new(
            PLAN_DESCRIPTOR_INVALID,
            "optional LLM input requires a runtime_bindings descriptor map",
        ));
    };
    let references = bindings
        .iter()
        .filter_map(|(reference, port)| match port {
            DescriptorValue::String(port) if port == target.as_str() => Some(reference),
            _ => None,
        })
        .collect::<Vec<_>>();
    if references.len() != 1 {
        return Err(PlanError::new(
            PLAN_DESCRIPTOR_INVALID,
            "optional LLM input must have exactly one runtime reference",
        ));
    }
    let Some(DescriptorValue::Array(optional)) = descriptor
        .public_configuration
        .get("optional_runtime_bindings")
    else {
        return Err(PlanError::new(
            PLAN_DESCRIPTOR_INVALID,
            "optional LLM input requires optional_runtime_bindings metadata",
        ));
    };
    let expected = references[0].as_str();
    let matches = optional
        .iter()
        .filter(|value| matches!(value, DescriptorValue::String(value) if value == expected))
        .count();
    if matches != 1 {
        return Err(PlanError::new(
            PLAN_DESCRIPTOR_INVALID,
            "optional LLM runtime reference must be declared exactly once",
        ));
    }
    Ok(())
}

fn verify_data_binding_cycles(plan: &Plan, index: &Index<'_>) -> Result<(), PlanError> {
    let mut dependencies = plan
        .data_bindings
        .iter()
        .map(|binding| (binding.to.clone(), BTreeSet::new()))
        .collect::<BTreeMap<_, _>>();
    for binding in &plan.data_bindings {
        let ValueSource::Expression { expression } = &binding.source else {
            continue;
        };
        for dependency in expression.dependencies.values() {
            let port = require_data_port(index, dependency)?;
            if port.direction == PortDirection::Input
                && index.bound_data_inputs.contains_key(dependency)
            {
                dependencies
                    .get_mut(&binding.to)
                    .expect("binding target was indexed")
                    .insert(dependency.clone());
            }
        }
    }

    let mut indegree = dependencies
        .keys()
        .map(|port| (port.clone(), 0_usize))
        .collect::<BTreeMap<_, _>>();
    for targets in dependencies.values() {
        for target in targets {
            if let Some(value) = indegree.get_mut(target) {
                *value += 1;
            }
        }
    }
    let mut queue = indegree
        .iter()
        .filter(|(_, degree)| **degree == 0)
        .map(|(port, _)| port.clone())
        .collect::<VecDeque<_>>();
    let mut visited = 0;
    while let Some(port) = queue.pop_front() {
        visited += 1;
        for dependency in dependencies.get(&port).into_iter().flatten() {
            let Some(degree) = indegree.get_mut(dependency) else {
                continue;
            };
            *degree -= 1;
            if *degree == 0 {
                queue.push_back(dependency.clone());
            }
        }
    }
    if visited != dependencies.len() {
        return Err(PlanError::new(
            PLAN_DATA_CYCLE,
            "pure-expression data bindings contain a dependency cycle",
        ));
    }
    Ok(())
}

fn verify_value_source(
    source: &ValueSource,
    evaluating_node: &Node,
    plan: &Plan,
    index: &Index<'_>,
    dominators: &BTreeMap<NodeId, BTreeSet<NodeId>>,
    arm: Option<(&ControlPortId, &ControlPortId)>,
) -> Result<PlanType, PlanError> {
    match source {
        ValueSource::RunInput { path } => {
            let run_type = plan
                .metadata
                .input_contract()
                .run_type()
                .map_err(|failure| {
                    PlanError::new(
                        PLAN_TYPE_MISMATCH,
                        format!("workflow normalized input contract is invalid: {failure}"),
                    )
                })?;
            type_at_input_path(&run_type, path)
        }
        ValueSource::OptionalRunInput { path } => {
            type_at_optional_input_path(plan.metadata.input_contract(), path)
        }
        ValueSource::Port { port_id } => {
            let port = require_data_port(index, port_id)?;
            if port.direction != PortDirection::Output {
                return Err(PlanError::new(
                    PLAN_PORT_INVALID,
                    format!("value source '{}' is not a data output", port_id),
                ));
            }
            if let Some((start, target)) = arm {
                // Values produced before the Branch may be reused by every
                // arm; arm-local values instead need path-specific dominance.
                if verify_data_output_available(port, evaluating_node, index, dominators).is_err() {
                    verify_arm_value_available(start, target, &port.owner, index)?;
                }
            } else {
                verify_data_output_available(port, evaluating_node, index, dominators)?;
            }
            Ok(port.value_type.clone())
        }
        ValueSource::Literal { value } => {
            validate_json_literal(value)?;
            let actual = PlanType::literal(value.clone()).map_err(|error| {
                PlanError::new(
                    PLAN_TYPE_MISMATCH,
                    format!("invalid literal value source: {error}"),
                )
            })?;
            Ok(actual)
        }
        ValueSource::Expression { expression } => {
            verify_expression(expression, evaluating_node, index, dominators, arm)?;
            Ok(expression.result_type.clone())
        }
    }
}

fn verify_data_output_available(
    source: &DataPort,
    target: &Node,
    index: &Index<'_>,
    dominators: &BTreeMap<NodeId, BTreeSet<NodeId>>,
) -> Result<(), PlanError> {
    if source.owner == target.id {
        return Err(PlanError::new(
            PLAN_DOMINANCE_INVALID,
            format!(
                "node '{}' cannot feed one of its own inputs from its not-yet-produced output '{}'",
                target.id, source.id
            ),
        ));
    }
    let source_node = index
        .nodes
        .get(&source.owner)
        .expect("data-port owner exists");
    if source_node.scope_id == target.scope_id {
        if !dominators
            .get(&target.id)
            .is_some_and(|set| set.contains(&source_node.id))
        {
            return Err(PlanError::new(
                PLAN_DOMINANCE_INVALID,
                format!(
                    "data output '{}' does not dominate consumer node '{}'",
                    source.id, target.id
                ),
            ));
        }
        return Ok(());
    }
    if !is_scope_ancestor(&source_node.scope_id, &target.scope_id, index) {
        return Err(PlanError::new(
            PLAN_SCOPE_INVALID,
            format!(
                "data output '{}' crosses from scope '{}' to unrelated/ancestor scope '{}' without Phi/Collect",
                source.id, source_node.scope_id, target.scope_id
            ),
        ));
    }

    let mut cursor = target.scope_id.clone();
    while cursor != source_node.scope_id {
        let scope = index.scopes.get(&cursor).expect("scope ancestry exists");
        if !scope.captures.contains(&source.id) {
            return Err(PlanError::new(
                PLAN_SCOPE_INVALID,
                format!(
                    "scope '{}' does not explicitly capture data output '{}'",
                    scope.id, source.id
                ),
            ));
        }
        let owner = scope
            .owner_node
            .as_ref()
            .expect("non-root captured scope has owner");
        if !dominators
            .get(owner)
            .is_some_and(|set| set.contains(&source_node.id))
        {
            return Err(PlanError::new(
                PLAN_DOMINANCE_INVALID,
                format!(
                    "captured output '{}' does not dominate scope '{}' creation owner",
                    source.id, scope.id
                ),
            ));
        }
        cursor = scope.parent.clone().expect("ancestor walk stops at source");
    }
    Ok(())
}

fn verify_expression(
    expression: &PureExpression,
    evaluating_node: &Node,
    index: &Index<'_>,
    dominators: &BTreeMap<NodeId, BTreeSet<NodeId>>,
    arm: Option<(&ControlPortId, &ControlPortId)>,
) -> Result<(), PlanError> {
    verify_canonical_type(&expression.result_type, "expression result")?;
    if expression.source.is_empty()
        || expression.source.len() > MAX_EXPRESSION_BYTES
        || expression.source.chars().any(|character| character == '\0')
    {
        return Err(PlanError::new(
            PLAN_DESCRIPTOR_INVALID,
            "pure expression source must be non-empty, bounded, and contain no NUL",
        ));
    }
    match expression.language {
        ExpressionLanguage::Cel => {
            if expression.engine_version.as_str() != CEL_EXPRESSION_ENGINE_VERSION {
                return Err(PlanError::new(
                    PLAN_VERSION_UNSUPPORTED,
                    format!(
                        "unsupported CEL engine version '{}'; this Plan wire requires '{}'",
                        expression.engine_version, CEL_EXPRESSION_ENGINE_VERSION
                    ),
                ));
            }
            let dependency_types = expression
                .dependencies
                .iter()
                .map(|(name, dependency)| {
                    Ok((
                        name.clone(),
                        require_data_port(index, dependency)?.value_type.clone(),
                    ))
                })
                .collect::<Result<BTreeMap<_, _>, PlanError>>()?;
            let analysis =
                analyze_cel_expression(&expression.source, &dependency_types).map_err(|error| {
                    PlanError::new(
                        PLAN_DESCRIPTOR_INVALID,
                        format!("CEL expression is outside the fixed typed profile: {error}"),
                    )
                })?;
            if !analysis
                .result_type
                .is_assignable_to(&expression.result_type)
            {
                return Err(PlanError::new(
                    PLAN_TYPE_MISMATCH,
                    "statically inferred CEL result does not match its declared type",
                ));
            }
            if analysis.references.is_empty() {
                let program = cel::Program::compile(&expression.source)
                    .expect("typed CEL analysis already parsed this source");
                let value = program.execute(&cel::Context::default()).map_err(|error| {
                    PlanError::new(
                        PLAN_DESCRIPTOR_INVALID,
                        format!("constant CEL expression cannot be evaluated: {error}"),
                    )
                })?;
                let (_actual, canonical_source) =
                    primitive_cel_contract(&value).ok_or_else(|| {
                        PlanError::new(
                            PLAN_DESCRIPTOR_INVALID,
                            "constant CEL composite values are outside the fixed typed profile",
                        )
                    })?;
                if canonical_source != expression.source {
                    return Err(PlanError::new(
                        PLAN_DESCRIPTOR_INVALID,
                        "constant CEL source is not in its canonical expression form",
                    ));
                }
            }
        }
        ExpressionLanguage::Literal => {
            if expression.engine_version.as_str() != LITERAL_EXPRESSION_ENGINE_VERSION {
                return Err(PlanError::new(
                    PLAN_VERSION_UNSUPPORTED,
                    format!(
                        "unsupported literal engine version '{}'; this Plan wire requires '{}'",
                        expression.engine_version, LITERAL_EXPRESSION_ENGINE_VERSION
                    ),
                ));
            }
            let value: Value = serde_json::from_str(&expression.source).map_err(|error| {
                PlanError::new(
                    PLAN_DESCRIPTOR_INVALID,
                    format!("literal expression is not canonical JSON: {error}"),
                )
            })?;
            let canonical = serde_jcs::to_vec(&value).map_err(|error| {
                PlanError::new(
                    PLAN_DESCRIPTOR_INVALID,
                    format!("literal expression cannot be canonicalized: {error}"),
                )
            })?;
            if canonical.as_slice() != expression.source.as_bytes() {
                return Err(PlanError::new(
                    PLAN_DESCRIPTOR_INVALID,
                    "literal expression source must exactly equal its RFC 8785 representation",
                ));
            }
            let actual = PlanType::literal(value).map_err(|error| {
                PlanError::new(
                    PLAN_TYPE_MISMATCH,
                    format!("literal expression is not a valid Plan literal: {error}"),
                )
            })?;
            if !actual.is_assignable_to(&expression.result_type) {
                return Err(PlanError::new(
                    PLAN_TYPE_MISMATCH,
                    "literal expression result does not match its declared type",
                ));
            }
        }
        ExpressionLanguage::Match => {
            if expression.engine_version.as_str() != MATCH_EXPRESSION_ENGINE_VERSION {
                return Err(PlanError::new(
                    PLAN_VERSION_UNSUPPORTED,
                    format!(
                        "unsupported Match engine version '{}'; this Plan wire requires '{}'",
                        expression.engine_version, MATCH_EXPRESSION_ENGINE_VERSION
                    ),
                ));
            }
            let program: MatchProgram =
                serde_json::from_str(&expression.source).map_err(|error| {
                    PlanError::new(
                        PLAN_DESCRIPTOR_INVALID,
                        format!("Match expression is not valid closed JSON: {error}"),
                    )
                })?;
            let canonical = serde_jcs::to_vec(&program).map_err(|error| {
                PlanError::new(
                    PLAN_DESCRIPTOR_INVALID,
                    format!("Match expression cannot be canonicalized: {error}"),
                )
            })?;
            if canonical.as_slice() != expression.source.as_bytes() {
                return Err(PlanError::new(
                    PLAN_DESCRIPTOR_INVALID,
                    "Match expression source must exactly equal its RFC 8785 representation",
                ));
            }
            let dependency_types = expression
                .dependencies
                .iter()
                .map(|(name, dependency)| {
                    Ok((
                        name.clone(),
                        require_data_port(index, dependency)?.value_type.clone(),
                    ))
                })
                .collect::<Result<BTreeMap<_, _>, PlanError>>()?;
            let actual = analyze_match_program(&program, &dependency_types).map_err(|error| {
                PlanError::new(
                    PLAN_DESCRIPTOR_INVALID,
                    format!("Match expression is outside the fixed typed profile: {error}"),
                )
            })?;
            if !actual.is_assignable_to(&expression.result_type) {
                return Err(PlanError::new(
                    PLAN_TYPE_MISMATCH,
                    "statically inferred Match result does not match its declared type",
                ));
            }
        }
        ExpressionLanguage::Value => {
            if expression.engine_version.as_str() != VALUE_EXPRESSION_ENGINE_VERSION {
                return Err(PlanError::new(
                    PLAN_VERSION_UNSUPPORTED,
                    format!(
                        "unsupported Value engine version '{}'; this Plan wire requires '{}'",
                        expression.engine_version, VALUE_EXPRESSION_ENGINE_VERSION
                    ),
                ));
            }
            let program: ValueProgram =
                serde_json::from_str(&expression.source).map_err(|error| {
                    PlanError::new(
                        PLAN_DESCRIPTOR_INVALID,
                        format!("Value expression is not valid closed JSON: {error}"),
                    )
                })?;
            let canonical = serde_jcs::to_vec(&program).map_err(|error| {
                PlanError::new(
                    PLAN_DESCRIPTOR_INVALID,
                    format!("Value expression cannot be canonicalized: {error}"),
                )
            })?;
            if canonical.as_slice() != expression.source.as_bytes() {
                return Err(PlanError::new(
                    PLAN_DESCRIPTOR_INVALID,
                    "Value expression source must exactly equal its RFC 8785 representation",
                ));
            }
            let dependency_types = expression
                .dependencies
                .iter()
                .map(|(name, dependency)| {
                    Ok((
                        name.clone(),
                        require_data_port(index, dependency)?.value_type.clone(),
                    ))
                })
                .collect::<Result<BTreeMap<_, _>, PlanError>>()?;
            let actual = analyze_value_program(&program, &dependency_types).map_err(|error| {
                PlanError::new(
                    PLAN_DESCRIPTOR_INVALID,
                    format!("Value expression is outside the fixed typed profile: {error}"),
                )
            })?;
            if !actual.is_assignable_to(&expression.result_type) {
                return Err(PlanError::new(
                    PLAN_TYPE_MISMATCH,
                    "statically inferred Value result does not match its declared type",
                ));
            }
        }
        ExpressionLanguage::Template => {
            return Err(PlanError::new(
                PLAN_VERSION_UNSUPPORTED,
                "Template does not yet have a published typed expression engine",
            ));
        }
    }
    if expression.language == ExpressionLanguage::Literal && !expression.dependencies.is_empty() {
        return Err(PlanError::new(
            PLAN_DESCRIPTOR_INVALID,
            "literal expression cannot declare data dependencies",
        ));
    }
    for (name, dependency) in &expression.dependencies {
        validate_name("expression dependency", name)?;
        let port = require_data_port(index, dependency)?;
        if port.direction == PortDirection::Input && port.owner == evaluating_node.id {
            if !index.bound_data_inputs.contains_key(&port.id) {
                return Err(PlanError::new(
                    PLAN_PORT_INVALID,
                    format!(
                        "expression dependency '{}' references unbound input '{}'",
                        name, port.id
                    ),
                ));
            }
            continue;
        }
        if port.direction != PortDirection::Output {
            return Err(PlanError::new(
                PLAN_PORT_INVALID,
                format!(
                    "expression dependency '{}' must reference an owned input or data output",
                    name
                ),
            ));
        }
        if let Some((start, target)) = arm {
            if verify_data_output_available(port, evaluating_node, index, dominators).is_err() {
                verify_arm_value_available(start, target, &port.owner, index)?;
            }
        } else {
            verify_data_output_available(port, evaluating_node, index, dominators)?;
        }
    }
    Ok(())
}

fn primitive_cel_contract(value: &cel::Value) -> Option<(PlanType, String)> {
    match value {
        cel::Value::Int(value) => Some((PlanType::Integer, value.to_string())),
        cel::Value::UInt(value) => Some((PlanType::Integer, format!("{value}u"))),
        cel::Value::Float(value) if value.is_finite() => {
            let source = serde_jcs::to_string(value).ok()?;
            Some((PlanType::Number, source))
        }
        cel::Value::String(value) => {
            let source = serde_json::to_string(value.as_ref()).ok()?;
            Some((PlanType::String, source))
        }
        cel::Value::Bool(value) => Some((PlanType::Boolean, value.to_string())),
        cel::Value::Null => Some((PlanType::Null, "null".to_owned())),
        _ => None,
    }
}

fn verify_node_descriptors(
    plan: &Plan,
    index: &Index<'_>,
    dominators: &BTreeMap<NodeId, BTreeSet<NodeId>>,
) -> Result<(), PlanError> {
    for node in &plan.nodes {
        (|| -> Result<(), PlanError> {
            match &node.kind {
            NodeKind::LlmTask(descriptor)
            | NodeKind::ActionTask(descriptor)
            | NodeKind::RetrievalTask(descriptor)
            | NodeKind::HttpTask(descriptor)
            | NodeKind::ToolTask(descriptor) => {
                verify_leaf_descriptor(descriptor)?;
                verify_linear_leaf_control(plan, node, index)?;
            }
            NodeKind::Branch(descriptor) => {
                verify_branch(plan, node, descriptor, index, dominators)?
            }
            NodeKind::Merge(descriptor) => verify_merge(node, descriptor, index)?,
            NodeKind::Fork(descriptor) => verify_fork(plan, node, descriptor, index)?,
            NodeKind::Join(descriptor) => verify_join(node, descriptor, index)?,
            NodeKind::Map(descriptor) => {
                verify_expression(&descriptor.items, node, index, dominators, None)?;
                let Some((items, _, _)) = descriptor.items.result_type.array_constraints() else {
                    return Err(PlanError::new(
                        PLAN_TYPE_MISMATCH,
                        format!(
                            "Map '{}' items expression must have one concrete canonical array type",
                            node.id
                        ),
                    ));
                };
                let item_port = require_owned_data_port(
                    index,
                    node,
                    &descriptor.item_port,
                    PortDirection::Output,
                )?;
                if &item_port.value_type != items || item_port.value_type == PlanType::Never {
                    return Err(PlanError::new(
                        PLAN_TYPE_MISMATCH,
                        format!(
                            "Map '{}' item port must exactly match its array item type",
                            node.id
                        ),
                    ));
                }
                let scope = index.scopes.get(&descriptor.body_scope_id).ok_or_else(|| {
                    PlanError::new(
                        PLAN_SCOPE_INVALID,
                        format!("Map '{}' body scope does not exist", node.id),
                    )
                })?;
                if !matches!(
                    &scope.kind,
                    ScopeKind::MapBody { map_node_id } if map_node_id == &node.id
                ) || scope.parent.as_ref() != Some(&node.scope_id)
                    || scope.owner_node.as_ref() != Some(&node.id)
                    || !scope.captures.contains(&descriptor.item_port)
                    || descriptor.max_concurrency == Some(0)
                {
                    return Err(PlanError::new(
                        PLAN_DESCRIPTOR_INVALID,
                        format!("Map '{}' has invalid body scope or concurrency", node.id),
                    ));
                }
                let yielded = require_data_port(index, &descriptor.yield_port)?;
                let yield_owner = index
                    .nodes
                    .get(&yielded.owner)
                    .expect("data-port owners were indexed");
                if yielded.direction != PortDirection::Output
                    || yielded.value_type == PlanType::Never
                    || !is_scope_ancestor(&descriptor.body_scope_id, &yield_owner.scope_id, index)
                {
                    return Err(PlanError::new(
                        PLAN_TYPE_MISMATCH,
                        format!(
                            "Map '{}' yield must be a non-Never output produced inside its body scope",
                            node.id
                        ),
                    ));
                }
                let collect_count = index
                    .nodes
                    .values()
                    .filter(|candidate| {
                        matches!(
                            &candidate.kind,
                            NodeKind::Collect(collect)
                                if matches!(
                                    &collect.source,
                                    CollectSource::Map { map_node_id }
                                        | CollectSource::DynamicMap { map_node_id, .. }
                                        if map_node_id == &node.id
                                )
                        )
                    })
                    .count();
                if collect_count != 1 {
                    return Err(PlanError::new(
                        PLAN_DESCRIPTOR_INVALID,
                        format!(
                            "Map '{}' must have exactly one typed Collect, found {collect_count}",
                            node.id
                        ),
                    ));
                }
            }
            NodeKind::Collect(descriptor) => {
                require_owned_data_port(
                    index,
                    node,
                    &descriptor.output_port,
                    PortDirection::Output,
                )?;
                match &descriptor.source {
                    CollectSource::StaticFork {
                        fork_node_id,
                        join_node_id,
                        mode,
                    } => {
                        verify_linear_control_node(plan, node, index, "Collect")?;
                        let fork = index.nodes.get(fork_node_id).ok_or_else(|| {
                            PlanError::new(PLAN_REFERENCE_INVALID, "Collect Fork does not exist")
                        })?;
                        let join = index.nodes.get(join_node_id).ok_or_else(|| {
                            PlanError::new(PLAN_REFERENCE_INVALID, "Collect Join does not exist")
                        })?;
                        let NodeKind::Fork(fork_descriptor) = &fork.kind else {
                            return Err(PlanError::new(
                                PLAN_JOIN_INVALID,
                                "static Collect references a non-Fork node",
                            ));
                        };
                        let NodeKind::Join(join_descriptor) = &join.kind else {
                            return Err(PlanError::new(
                                PLAN_JOIN_INVALID,
                                "static Collect references a non-Join node",
                            ));
                        };
                        if &join_descriptor.fork_node_id != fork_node_id
                            || &join_descriptor.mode != mode
                            || &fork_descriptor.join_mode != mode
                        {
                            return Err(PlanError::new(
                                PLAN_JOIN_INVALID,
                                "static Collect correlation/mode does not match Fork and Join",
                            ));
                        }
                        verify_collect_follows_control(join, node, index)?;
                        let expected = static_collect_type(plan, fork_descriptor, index)?;
                        let output = require_owned_data_port(
                            index,
                            node,
                            &descriptor.output_port,
                            PortDirection::Output,
                        )?;
                        if output.value_type != expected {
                            return Err(PlanError::new(
                                PLAN_TYPE_MISMATCH,
                                "static Collect output must exactly equal the closed ordered Fork result contract",
                            ));
                        }
                    }
                    CollectSource::Map { map_node_id } => {
                        verify_linear_control_node(plan, node, index, "Collect")?;
                        let map = index.nodes.get(map_node_id).ok_or_else(|| {
                            PlanError::new(PLAN_REFERENCE_INVALID, "Map Collect source is missing")
                        })?;
                        let NodeKind::Map(map_descriptor) = &map.kind else {
                            return Err(PlanError::new(
                                PLAN_REFERENCE_INVALID,
                                "Map Collect references a non-Map node",
                            ));
                        };
                        let yielded = require_data_port(index, &map_descriptor.yield_port)?;
                        let expected = PlanType::Array {
                            items: Box::new(yielded.value_type.clone()),
                            min_items: 0,
                        };
                        let output = require_owned_data_port(
                            index,
                            node,
                            &descriptor.output_port,
                            PortDirection::Output,
                        )?;
                        if output.value_type != expected {
                            return Err(PlanError::new(
                                PLAN_TYPE_MISMATCH,
                                "Map Collect output must exactly equal the typed yield array",
                            ));
                        }
                    }
                    CollectSource::DynamicMap {
                        map_node_id,
                        key_field,
                        empty_output,
                        body_input,
                        empty_input,
                    } => {
                        let map = index.nodes.get(map_node_id).ok_or_else(|| {
                            PlanError::new(PLAN_REFERENCE_INVALID, "Keyed Map source is missing")
                        })?;
                        let NodeKind::Map(map_descriptor) = &map.kind else {
                            return Err(PlanError::new(
                                PLAN_REFERENCE_INVALID,
                                "Keyed Map Collect references a non-Map node",
                            ));
                        };
                        let Some((item_type, _, _)) =
                            map_descriptor.items.result_type.array_constraints()
                        else {
                            return Err(PlanError::new(
                                PLAN_TYPE_MISMATCH,
                                "Keyed Map items must be a concrete array",
                            ));
                        };
                        if let Some(key_field) = key_field {
                            let valid_key = matches!(item_type, PlanType::Object { properties, additional_properties: None }
                                if properties.get(key_field).is_some_and(|property|
                                    property.required
                                        && property.value_type != PlanType::Never
                                        && property.value_type.is_assignable_to(&PlanType::String)));
                            if !valid_key {
                                return Err(PlanError::new(
                                    PLAN_TYPE_MISMATCH,
                                    "Map key must be a required non-null string field on a closed item object",
                                ));
                            }
                        }
                        require_owned_control_port(
                            index,
                            map,
                            empty_output,
                            PortDirection::Output,
                        )?;
                        require_owned_control_port(index, node, body_input, PortDirection::Input)?;
                        require_owned_control_port(index, node, empty_input, PortDirection::Input)?;
                        let inputs = index
                            .node_control_inputs
                            .get(&node.id)
                            .cloned()
                            .unwrap_or_default()
                            .into_iter()
                            .collect::<BTreeSet<_>>();
                        if body_input == empty_input
                            || inputs != BTreeSet::from([body_input.clone(), empty_input.clone()])
                            || !port_reaches(empty_output, empty_input, index, None)
                        {
                            return Err(PlanError::new(
                                PLAN_DESCRIPTOR_INVALID,
                                "Keyed Map Collect must expose distinct correlated body and empty inputs",
                            ));
                        }
                        let yielded = require_data_port(index, &map_descriptor.yield_port)?;
                        let expected = PlanType::Array {
                            items: Box::new(yielded.value_type.clone()),
                            min_items: 0,
                        };
                        let output = require_owned_data_port(
                            index,
                            node,
                            &descriptor.output_port,
                            PortDirection::Output,
                        )?;
                        if output.value_type != expected {
                            return Err(PlanError::new(
                                PLAN_TYPE_MISMATCH,
                                "Keyed Map Collect output must equal the typed yield array",
                            ));
                        }
                        let outputs = index
                            .node_control_outputs
                            .get(&node.id)
                            .map(Vec::as_slice)
                            .unwrap_or_default();
                        if outputs.len() != 1 {
                            return Err(PlanError::new(
                                PLAN_DESCRIPTOR_INVALID,
                                "Keyed Map Collect must have one control output",
                            ));
                        }
                    }
                    CollectSource::Loop {
                        loop_node_id,
                        initial_input,
                        state_port,
                        yield_port,
                        completed_input,
                        break_input,
                    } => {
                        let loop_node = index.nodes.get(loop_node_id).ok_or_else(|| {
                            PlanError::new(PLAN_REFERENCE_INVALID, "Loop Collect source is missing")
                        })?;
                        let NodeKind::Loop(loop_descriptor) = &loop_node.kind else {
                            return Err(PlanError::new(
                                PLAN_REFERENCE_INVALID,
                                "Loop Collect references a non-Loop node",
                            ));
                        };
                        let initial = require_owned_data_port(
                            index,
                            loop_node,
                            initial_input,
                            PortDirection::Input,
                        )?;
                        let state = require_owned_data_port(
                            index,
                            loop_node,
                            state_port,
                            PortDirection::Output,
                        )?;
                        let yielded = require_data_port(index, yield_port)?;
                        let yield_owner = index
                            .nodes
                            .get(&yielded.owner)
                            .expect("yield data-port owner exists");
                        let body_scope = index.scopes.values().find(|scope| {
                            matches!(
                                &scope.kind,
                                ScopeKind::LoopBody { loop_node_id: owner }
                                    if owner == loop_node_id
                            )
                        });
                        if !initial.required
                            || initial.value_type == PlanType::Never
                            || state.value_type != initial.value_type
                            || yielded.value_type != initial.value_type
                            || yielded.direction != PortDirection::Output
                            || body_scope.is_none_or(|scope| {
                                !is_scope_ancestor(&scope.id, &yield_owner.scope_id, index)
                                    || !scope.captures.contains(state_port)
                            })
                        {
                            return Err(PlanError::new(
                                PLAN_TYPE_MISMATCH,
                                "Loop initial, occurrence state, body yield, and final result must share one concrete type",
                            ));
                        }
                        require_owned_control_port(
                            index,
                            node,
                            completed_input,
                            PortDirection::Input,
                        )?;
                        if !port_reaches(
                            &loop_descriptor.completed_output,
                            completed_input,
                            index,
                            None,
                        ) {
                            return Err(PlanError::new(
                                PLAN_LOOP_INVALID,
                                "Loop completed output is not correlated to its result Collect",
                            ));
                        }
                        if let Some(input) = break_input {
                            require_owned_control_port(index, node, input, PortDirection::Input)?;
                        }
                        let expected_inputs = std::iter::once(completed_input.clone())
                            .chain(break_input.iter().cloned())
                            .collect::<BTreeSet<_>>();
                        let actual_inputs = index
                            .node_control_inputs
                            .get(&node.id)
                            .cloned()
                            .unwrap_or_default()
                            .into_iter()
                            .collect::<BTreeSet<_>>();
                        let output = require_owned_data_port(
                            index,
                            node,
                            &descriptor.output_port,
                            PortDirection::Output,
                        )?;
                        let outputs = index
                            .node_control_outputs
                            .get(&node.id)
                            .map(Vec::as_slice)
                            .unwrap_or_default();
                        if actual_inputs != expected_inputs
                            || output.value_type != initial.value_type
                            || outputs.len() != 1
                        {
                            return Err(PlanError::new(
                                PLAN_LOOP_INVALID,
                                "Loop result Collect has invalid control inputs or final value type",
                            ));
                        }
                    }
                }
            }
            NodeKind::Loop(descriptor) => verify_loop(plan, node, descriptor, index, dominators)?,
            NodeKind::ErrorBoundary(descriptor) => {
                if descriptor.protected_scope_id == descriptor.handler_scope_id {
                    return Err(PlanError::new(
                        PLAN_SCOPE_INVALID,
                        format!(
                            "ErrorBoundary '{}' protected and handler scopes must be distinct",
                            node.id
                        ),
                    ));
                }
                if descriptor.finalizer_scope_id.is_some() != descriptor.finalizer_output.is_some()
                    || descriptor.finalizer_completed_input.is_some()
                        && descriptor.finalizer_scope_id.is_none()
                {
                    return Err(PlanError::new(
                        PLAN_DESCRIPTOR_INVALID,
                        format!(
                            "ErrorBoundary '{}' finalizer scope/output must agree and completion requires a finalizer",
                            node.id
                        ),
                    ));
                }
                let child_can_complete = descriptor.protected_completed_input.is_some()
                    || descriptor.handler_completed_input.is_some();
                let finalizer_can_complete = descriptor.finalizer_scope_id.is_none()
                    || descriptor.finalizer_completed_input.is_some();
                if descriptor.completed_output.is_some()
                    != (child_can_complete && finalizer_can_complete)
                {
                    return Err(PlanError::new(
                        PLAN_DESCRIPTOR_INVALID,
                        format!(
                            "ErrorBoundary '{}' completed output does not match its normal child/finalizer exits",
                            node.id
                        ),
                    ));
                }
                let mut child_scopes = vec![
                    (&descriptor.protected_scope_id, "protected"),
                    (&descriptor.handler_scope_id, "handler"),
                ];
                if let Some(scope) = &descriptor.finalizer_scope_id {
                    child_scopes.push((scope, "finalizer"));
                }
                for (scope_id, child_kind) in child_scopes {
                    let scope = index.scopes.get(scope_id).ok_or_else(|| {
                        PlanError::new(
                            PLAN_SCOPE_INVALID,
                            format!(
                                "ErrorBoundary '{}' scope '{}' is missing",
                                node.id, scope_id
                            ),
                        )
                    })?;
                    if scope.parent.as_ref() != Some(&node.scope_id)
                        || scope.owner_node.as_ref() != Some(&node.id)
                        || !match child_kind {
                            "protected" => matches!(
                                &scope.kind,
                                ScopeKind::ErrorProtected { boundary_node_id }
                                    if boundary_node_id == &node.id
                            ),
                            "handler" => matches!(
                                &scope.kind,
                                ScopeKind::ErrorHandler { boundary_node_id }
                                    if boundary_node_id == &node.id
                            ),
                            "finalizer" => matches!(
                                &scope.kind,
                                ScopeKind::ErrorFinalizer { boundary_node_id }
                                    if boundary_node_id == &node.id
                            ),
                            _ => unreachable!("closed boundary child kind"),
                        }
                    {
                        return Err(PlanError::new(
                            PLAN_SCOPE_INVALID,
                            format!("ErrorBoundary '{}' has invalid child scope", node.id),
                        ));
                    }
                }
                for (port, direction) in [
                    (&descriptor.protected_output, PortDirection::Output),
                    (&descriptor.handler_output, PortDirection::Output),
                ] {
                    require_owned_control_port(index, node, port, direction)?;
                }
                for port in descriptor
                    .protected_completed_input
                    .iter()
                    .chain(descriptor.handler_completed_input.iter())
                    .chain(descriptor.finalizer_completed_input.iter())
                {
                    require_owned_control_port(index, node, port, PortDirection::Input)?;
                }
                if let Some(port) = &descriptor.finalizer_output {
                    require_owned_control_port(index, node, port, PortDirection::Output)?;
                }
                if let Some(port) = &descriptor.completed_output {
                    require_owned_control_port(index, node, port, PortDirection::Output)?;
                }
                let expected_inputs = descriptor
                    .protected_completed_input
                    .iter()
                    .chain(descriptor.handler_completed_input.iter())
                    .chain(descriptor.finalizer_completed_input.iter())
                    .cloned()
                    .collect::<BTreeSet<_>>();
                let actual_inputs = index
                    .node_control_inputs
                    .get(&node.id)
                    .cloned()
                    .unwrap_or_default()
                    .into_iter()
                    .filter(|input| expected_inputs.contains(input))
                    .collect::<BTreeSet<_>>();
                let all_inputs = index.node_control_inputs.get(&node.id).map_or(0, Vec::len);
                let expected_all_inputs =
                    expected_inputs.len() + usize::from(node.id != plan.metadata.entry_node_id);
                let outputs = index
                    .node_control_outputs
                    .get(&node.id)
                    .cloned()
                    .unwrap_or_default()
                    .into_iter()
                    .collect::<BTreeSet<_>>();
                let mut expected_outputs = BTreeSet::from([
                    descriptor.protected_output.clone(),
                    descriptor.handler_output.clone(),
                ]);
                if let Some(port) = &descriptor.finalizer_output {
                    expected_outputs.insert(port.clone());
                }
                if let Some(port) = &descriptor.completed_output {
                    expected_outputs.insert(port.clone());
                }
                let error = require_owned_data_port(
                    index,
                    node,
                    &descriptor.error_port,
                    PortDirection::Output,
                )?;
                let handler_scope = index
                    .scopes
                    .get(&descriptor.handler_scope_id)
                    .expect("handler scope was verified above");
                if actual_inputs != expected_inputs
                    || all_inputs != expected_all_inputs
                    || outputs != expected_outputs
                    || error.value_type != plan.metadata.error_type
                    || !handler_scope.captures.contains(&descriptor.error_port)
                    || descriptor
                        .protected_completed_input
                        .as_ref()
                        .is_some_and(|input| {
                            !port_reaches(&descriptor.protected_output, input, index, None)
                        })
                    || descriptor
                        .handler_completed_input
                        .as_ref()
                        .is_some_and(|input| {
                            !port_reaches(&descriptor.handler_output, input, index, None)
                        })
                    || descriptor
                        .finalizer_completed_input
                        .as_ref()
                        .is_some_and(|input| {
                            !port_reaches(
                                descriptor
                                    .finalizer_output
                                    .as_ref()
                                    .expect("finalizer completion requires its output"),
                                input,
                                index,
                                None,
                            )
                        })
                {
                    return Err(PlanError::new(
                        PLAN_DESCRIPTOR_INVALID,
                        "ErrorBoundary ports, safe error contract, or child completion correlation is invalid",
                    ));
                }
            }
            NodeKind::SubflowCall(descriptor) => {
                verify_linear_control_node(plan, node, index, "SubflowCall")?;
                verify_subflow_interface_inputs(node, descriptor, index)?;
                if descriptor.timeout_ms == 0
                    || descriptor.timeout_ms > 10 * 365 * 24 * 60 * 60 * 1_000
                {
                    return Err(PlanError::new(
                        PLAN_DESCRIPTOR_INVALID,
                        "SubflowCall timeout must be between one millisecond and ten years",
                    ));
                }
            }
            NodeKind::WaitSignal(descriptor) => {
                verify_linear_control_node(plan, node, index, "WaitSignal")?;
                validate_name("signal name", &descriptor.signal_name)?;
                verify_canonical_type(&descriptor.payload_type, "signal payload")?;
            }
            NodeKind::HumanTask(descriptor) => {
                verify_linear_control_node(plan, node, index, "HumanTask")?;
                validate_name(
                    "human task completion signal",
                    &descriptor.completion_signal,
                )?;
                verify_canonical_type(&descriptor.request_type, "human task request")?;
                verify_canonical_type(&descriptor.response_type, "human task response")?;
                let request = require_owned_data_port(
                    index,
                    node,
                    &descriptor.request_input,
                    PortDirection::Input,
                )?;
                if request.value_type() != &descriptor.request_type {
                    return Err(PlanError::new(
                        PLAN_DESCRIPTOR_INVALID,
                        "HumanTask request input contract is invalid",
                    ));
                }
                if descriptor.claim_lease_ms == 0
                    || descriptor.claim_lease_ms > 30 * 24 * 60 * 60 * 1_000
                {
                    return Err(PlanError::new(
                        PLAN_DESCRIPTOR_INVALID,
                        "HumanTask claim lease must be between one millisecond and thirty days",
                    ));
                }
                for identity in descriptor
                    .assignees
                    .iter()
                    .chain(descriptor.candidate_groups.iter())
                {
                    validate_name("human task assignment identity", identity)?;
                }
                let mut assignees = descriptor.assignees.clone();
                assignees.sort();
                assignees.dedup();
                let mut groups = descriptor.candidate_groups.clone();
                groups.sort();
                groups.dedup();
                if assignees != descriptor.assignees || groups != descriptor.candidate_groups {
                    return Err(PlanError::new(
                        PLAN_DESCRIPTOR_INVALID,
                        "HumanTask assignment lists must be sorted and duplicate-free",
                    ));
                }
            }
            NodeKind::Timer(descriptor) => {
                verify_linear_control_node(plan, node, index, "Timer")?;
                verify_expression(&descriptor.delay_ms, node, index, dominators, None)?;
                if !descriptor
                    .delay_ms
                    .result_type
                    .is_assignable_to(&PlanType::Number)
                    || descriptor.delay_ms.result_type == PlanType::Never
                {
                    return Err(PlanError::new(
                        PLAN_TYPE_MISMATCH,
                        "Timer delay expression must produce a non-null number",
                    ));
                }
            }
            NodeKind::Return(descriptor) => verify_return(plan, node, descriptor, index)?,
                NodeKind::Raise(descriptor) => verify_raise(plan, node, descriptor, index)?,
            }
            Ok(())
        })()
        .map_err(|error| {
            error.with_target_if_absent(PlanDiagnosticTarget::Node {
                node_id: node.id.clone(),
            })
        })?;
    }
    Ok(())
}

fn verify_subflow_interface_inputs(
    node: &Node,
    descriptor: &super::SubflowCallDescriptor,
    index: &Index<'_>,
) -> Result<(), PlanError> {
    let mut seen = BTreeSet::new();
    for (name, port_id) in &descriptor.inputs {
        if !seen.insert(port_id) {
            return Err(PlanError::new(
                PLAN_DESCRIPTOR_INVALID,
                "SubflowCall interface input map aliases one data port under multiple names",
            ));
        }
        let port = require_data_port(index, port_id)?;
        if &port.owner != node.id() || port.direction != PortDirection::Input || &port.name != name
        {
            return Err(PlanError::new(
                PLAN_DESCRIPTOR_INVALID,
                "SubflowCall interface inputs must name distinct input ports owned by the call node",
            ));
        }
    }
    Ok(())
}

fn verify_linear_leaf_control(
    plan: &Plan,
    node: &Node,
    index: &Index<'_>,
) -> Result<(), PlanError> {
    verify_linear_control_node(plan, node, index, "ordinary leaf")
}

fn verify_linear_control_node(
    plan: &Plan,
    node: &Node,
    index: &Index<'_>,
    label: &str,
) -> Result<(), PlanError> {
    let inputs = index
        .node_control_inputs
        .get(&node.id)
        .map(Vec::as_slice)
        .unwrap_or_default();
    let outputs = index
        .node_control_outputs
        .get(&node.id)
        .map(Vec::as_slice)
        .unwrap_or_default();
    let expected_inputs = usize::from(node.id != plan.metadata.entry_node_id);
    if inputs.len() != expected_inputs || outputs.len() != 1 {
        return Err(PlanError::new(
            PLAN_DESCRIPTOR_INVALID,
            format!(
                "{label} '{}' must have {} control input(s) and exactly one output; control branching requires a dedicated control node",
                node.id, expected_inputs,
            ),
        ));
    }
    let output = &outputs[0];
    if index
        .outgoing_control
        .get(output)
        .is_none_or(|edges| edges.len() != 1)
    {
        return Err(PlanError::new(
            PLAN_DESCRIPTOR_INVALID,
            format!(
                "{label} '{}' output must have exactly one successor",
                node.id,
            ),
        ));
    }
    Ok(())
}

fn verify_leaf_descriptor(descriptor: &super::LeafTaskDescriptor) -> Result<(), PlanError> {
    validate_name("leaf implementation", &descriptor.implementation)?;
    let mut items = 0;
    validate_descriptor_map(&descriptor.public_configuration, 0, &mut items)?;
    for field in descriptor.secret_configuration.keys() {
        validate_name("secret descriptor field", field)?;
        if descriptor.public_configuration.contains_key(field) {
            return Err(PlanError::new(
                PLAN_DESCRIPTOR_INVALID,
                format!(
                    "descriptor field '{field}' cannot be both public configuration and a SecretRef"
                ),
            ));
        }
        items += 1;
        if items > MAX_DESCRIPTOR_COLLECTION_ITEMS {
            return Err(PlanError::new(
                PLAN_DESCRIPTOR_INVALID,
                "descriptor configuration contains too many values",
            ));
        }
    }
    Ok(())
}

fn verify_branch(
    plan: &Plan,
    node: &Node,
    descriptor: &BranchDescriptor,
    index: &Index<'_>,
    dominators: &BTreeMap<NodeId, BTreeSet<NodeId>>,
) -> Result<(), PlanError> {
    verify_linear_control_inputs(&plan.metadata.entry_node_id, node, index, "Branch")?;
    if descriptor.cases.is_empty() {
        return Err(PlanError::new(
            PLAN_BRANCH_INVALID,
            format!("Branch '{}' has no cases", node.id),
        ));
    }
    let mut case_ids = BTreeSet::new();
    let mut ports = BTreeSet::new();
    for (position, case) in descriptor.cases.iter().enumerate() {
        if !case_ids.insert(case.case_id.clone()) || !ports.insert(case.output_port.clone()) {
            return Err(PlanError::new(
                PLAN_BRANCH_INVALID,
                format!(
                    "Branch '{}' has duplicate case IDs or output ports",
                    node.id
                ),
            ));
        }
        let port =
            require_owned_control_port(index, node, &case.output_port, PortDirection::Output)?;
        if port.name.as_str() != case.case_id.as_str() {
            return Err(PlanError::new(
                PLAN_BRANCH_INVALID,
                format!(
                    "Branch '{}' case '{}' must use an equally named output port",
                    node.id, case.case_id
                ),
            ));
        }
        if index
            .outgoing_control
            .get(&case.output_port)
            .is_none_or(Vec::is_empty)
        {
            return Err(PlanError::new(
                PLAN_BRANCH_INVALID,
                format!(
                    "Branch case '{}' has no outgoing control path",
                    case.case_id
                ),
            ));
        }
        match &case.condition {
            Some(condition) => {
                if position + 1 == descriptor.cases.len() {
                    return Err(PlanError::new(
                        PLAN_BRANCH_INVALID,
                        "last Branch case must be an explicit default/else",
                    ));
                }
                verify_expression(condition, node, index, dominators, None)?;
                if condition.result_type == PlanType::Never
                    || !condition.result_type.is_assignable_to(&PlanType::Boolean)
                {
                    return Err(PlanError::new(
                        PLAN_TYPE_MISMATCH,
                        "Branch conditions must produce a non-null boolean",
                    ));
                }
            }
            None if position + 1 != descriptor.cases.len() => {
                return Err(PlanError::new(
                    PLAN_BRANCH_INVALID,
                    "Branch default/else must be unique and last",
                ));
            }
            None => {}
        }
    }
    let actual = index
        .node_control_outputs
        .get(&node.id)
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .collect::<BTreeSet<_>>();
    if actual != ports {
        return Err(PlanError::new(
            PLAN_BRANCH_INVALID,
            format!(
                "Branch '{}' control outputs must exactly equal its ordered named cases",
                node.id
            ),
        ));
    }
    Ok(())
}

fn verify_merge(
    node: &Node,
    descriptor: &MergeDescriptor,
    index: &Index<'_>,
) -> Result<(), PlanError> {
    let branch = index
        .nodes
        .get(&descriptor.branch_node_id)
        .ok_or_else(|| PlanError::new(PLAN_MERGE_INVALID, "Merge references a missing Branch"))?;
    let NodeKind::Branch(branch_descriptor) = &branch.kind else {
        return Err(PlanError::new(
            PLAN_MERGE_INVALID,
            "Merge correlation must reference a Branch node",
        ));
    };
    if descriptor.arms.is_empty() {
        return Err(PlanError::new(
            PLAN_MERGE_INVALID,
            format!("Merge '{}' has no correlated arms", node.id),
        ));
    }
    let mut inputs = BTreeSet::new();
    for (case_id, input) in &descriptor.arms {
        let case = branch_descriptor
            .cases
            .iter()
            .find(|case| &case.case_id == case_id)
            .ok_or_else(|| {
                PlanError::new(
                    PLAN_MERGE_INVALID,
                    format!("Merge arm '{}' is not a case of its Branch", case_id),
                )
            })?;
        require_owned_control_port(index, node, input, PortDirection::Input)?;
        if !inputs.insert(input.clone()) {
            return Err(PlanError::new(
                PLAN_MERGE_INVALID,
                "Merge arms must use distinct input ports",
            ));
        }
        if !port_reaches(&case.output_port, input, index, None) {
            return Err(PlanError::new(
                PLAN_MERGE_INVALID,
                format!(
                    "Merge arm '{}' is not reachable from the correlated Branch port",
                    case_id
                ),
            ));
        }
        verify_provenance_closed_path(
            &case.output_port,
            input,
            &branch.id,
            &node.id,
            index,
            PLAN_MERGE_INVALID,
            "Merge arm",
        )?;
        for other in &branch_descriptor.cases {
            if other.case_id != *case_id && port_reaches(&other.output_port, input, index, None) {
                return Err(PlanError::new(
                    PLAN_MERGE_INVALID,
                    format!(
                        "Merge input '{}' can be reached from non-correlated Branch case '{}'",
                        input, other.case_id
                    ),
                ));
            }
        }
    }
    let actual_inputs = index
        .node_control_inputs
        .get(&node.id)
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .collect::<BTreeSet<_>>();
    if actual_inputs != inputs {
        return Err(PlanError::new(
            PLAN_MERGE_INVALID,
            "Merge input ports must exactly equal its correlated arm map",
        ));
    }
    require_owned_control_port(index, node, &descriptor.output_port, PortDirection::Output)?;
    let outputs = index
        .node_control_outputs
        .get(&node.id)
        .cloned()
        .unwrap_or_default();
    if outputs != vec![descriptor.output_port.clone()] {
        return Err(PlanError::new(
            PLAN_MERGE_INVALID,
            "Merge must have exactly its declared single control output",
        ));
    }
    Ok(())
}

fn verify_fork(
    plan: &Plan,
    node: &Node,
    descriptor: &ForkDescriptor,
    index: &Index<'_>,
) -> Result<(), PlanError> {
    verify_linear_control_inputs(&plan.metadata.entry_node_id, node, index, "Fork")?;
    if descriptor.legs.is_empty() {
        return Err(PlanError::new(
            PLAN_FORK_INVALID,
            format!("Fork '{}' cannot have zero legs", node.id),
        ));
    }
    let correlated_joins = index
        .nodes
        .values()
        .filter(|candidate| {
            matches!(
                &candidate.kind,
                NodeKind::Join(join) if join.fork_node_id == node.id
            )
        })
        .count();
    if correlated_joins != 1 {
        return Err(PlanError::new(
            PLAN_FORK_INVALID,
            format!(
                "Fork '{}' must have exactly one statically correlated Join, found {}",
                node.id, correlated_joins
            ),
        ));
    }
    let mut leg_ids = BTreeSet::new();
    let mut ports = BTreeSet::new();
    let mut scopes = BTreeSet::new();
    let mut yields = BTreeSet::new();
    let correlated_join = index
        .nodes
        .values()
        .find_map(|candidate| match &candidate.kind {
            NodeKind::Join(join) if join.fork_node_id == node.id => Some((*candidate, join)),
            _ => None,
        })
        .expect("exactly one correlated Join was established");
    for leg in &descriptor.legs {
        if !leg_ids.insert(leg.leg_id.clone())
            || !ports.insert(leg.output_port.clone())
            || !scopes.insert(leg.scope_id.clone())
            || !yields.insert(leg.yield_port.clone())
        {
            return Err(PlanError::new(
                PLAN_FORK_INVALID,
                "Fork leg IDs, scopes, control ports, and yield ports must be unique",
            ));
        }
        let port =
            require_owned_control_port(index, node, &leg.output_port, PortDirection::Output)?;
        if port.name.as_str() != leg.leg_id.as_str() {
            return Err(PlanError::new(
                PLAN_FORK_INVALID,
                format!("Fork leg '{}' must use an equally named output", leg.leg_id),
            ));
        }
        let scope = index.scopes.get(&leg.scope_id).ok_or_else(|| {
            PlanError::new(
                PLAN_SCOPE_INVALID,
                format!("Fork leg '{}' scope is missing", leg.leg_id),
            )
        })?;
        if scope.parent.as_ref() != Some(&node.scope_id)
            || scope.owner_node.as_ref() != Some(&node.id)
            || !matches!(
                &scope.kind,
                ScopeKind::ForkLeg { fork_node_id, leg_id }
                    if fork_node_id == &node.id && leg_id == &leg.leg_id
            )
        {
            return Err(PlanError::new(
                PLAN_SCOPE_INVALID,
                format!("Fork leg '{}' has wrong scope correlation", leg.leg_id),
            ));
        }
        let yielded = require_data_port(index, &leg.yield_port)?;
        let yield_owner = index
            .nodes
            .get(&yielded.owner)
            .expect("data-port owners were indexed");
        if yielded.direction != PortDirection::Output
            || yielded.value_type == PlanType::Never
            || !is_scope_ancestor(&leg.scope_id, &yield_owner.scope_id, index)
        {
            return Err(PlanError::new(
                PLAN_FORK_INVALID,
                format!(
                    "Fork leg '{}' yield must be a non-Never data output produced in that leg scope",
                    leg.leg_id
                ),
            ));
        }
        let join_input = correlated_join.1.legs.get(&leg.leg_id).ok_or_else(|| {
            PlanError::new(
                PLAN_JOIN_INVALID,
                format!("correlated Join is missing Fork leg '{}'", leg.leg_id),
            )
        })?;
        let yield_reaches_leg_exit =
            index
                .node_control_outputs
                .get(&yield_owner.id)
                .is_some_and(|outputs| {
                    outputs
                        .iter()
                        .any(|output| port_reaches(output, join_input, index, None))
                });
        if !port_reaches_owner(&leg.output_port, &yield_owner.id, index) || !yield_reaches_leg_exit
        {
            return Err(PlanError::new(
                PLAN_FORK_INVALID,
                format!(
                    "Fork leg '{}' yield owner must lie on its correlated control path",
                    leg.leg_id
                ),
            ));
        }
    }
    let actual = index
        .node_control_outputs
        .get(&node.id)
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .collect::<BTreeSet<_>>();
    if actual != ports {
        return Err(PlanError::new(
            PLAN_FORK_INVALID,
            "Fork outputs must exactly equal its ordered leg descriptors",
        ));
    }
    Ok(())
}

fn verify_collect_follows_control(
    source: &Node,
    collect: &Node,
    index: &Index<'_>,
) -> Result<(), PlanError> {
    let source_outputs = index
        .node_control_outputs
        .get(&source.id)
        .map(Vec::as_slice)
        .unwrap_or_default();
    if source_outputs.len() != 1 || !port_reaches_owner(&source_outputs[0], &collect.id, index) {
        return Err(PlanError::new(
            PLAN_JOIN_INVALID,
            format!(
                "Collect '{}' must be control-reachable from its correlated Join '{}'",
                collect.id, source.id
            ),
        ));
    }
    Ok(())
}

fn static_collect_type(
    plan: &Plan,
    fork: &ForkDescriptor,
    index: &Index<'_>,
) -> Result<PlanType, PlanError> {
    let mut properties = BTreeMap::new();
    for leg in &fork.legs {
        let yielded = require_data_port(index, &leg.yield_port)?;
        let value_type = match fork.join_mode {
            PlanJoinMode::AllSuccess => yielded.value_type.clone(),
            PlanJoinMode::AllSettled => {
                let ok = PlanType::Object {
                    properties: BTreeMap::from([
                        (
                            "kind".to_owned(),
                            PlanProperty::new(
                                PlanType::literal(Value::String("ok".to_owned())).map_err(
                                    |error| PlanError::new(PLAN_TYPE_MISMATCH, error.to_string()),
                                )?,
                                true,
                            )
                            .map_err(|error| {
                                PlanError::new(PLAN_TYPE_MISMATCH, error.to_string())
                            })?,
                        ),
                        (
                            "value".to_owned(),
                            PlanProperty::new(yielded.value_type.clone(), true).map_err(
                                |error| PlanError::new(PLAN_TYPE_MISMATCH, error.to_string()),
                            )?,
                        ),
                    ]),
                    additional_properties: None,
                };
                let error_variant = PlanType::Object {
                    properties: BTreeMap::from([
                        (
                            "error".to_owned(),
                            PlanProperty::new(plan.metadata.error_type.clone(), true).map_err(
                                |error| PlanError::new(PLAN_TYPE_MISMATCH, error.to_string()),
                            )?,
                        ),
                        (
                            "kind".to_owned(),
                            PlanProperty::new(
                                PlanType::literal(Value::String("error".to_owned())).map_err(
                                    |error| PlanError::new(PLAN_TYPE_MISMATCH, error.to_string()),
                                )?,
                                true,
                            )
                            .map_err(|error| {
                                PlanError::new(PLAN_TYPE_MISMATCH, error.to_string())
                            })?,
                        ),
                    ]),
                    additional_properties: None,
                };
                PlanType::union([ok, error_variant])
                    .map_err(|error| PlanError::new(PLAN_TYPE_MISMATCH, error.to_string()))?
            }
        };
        properties.insert(
            leg.leg_id.as_str().to_owned(),
            PlanProperty::new(value_type, true)
                .map_err(|error| PlanError::new(PLAN_TYPE_MISMATCH, error.to_string()))?,
        );
    }
    Ok(PlanType::Object {
        properties,
        additional_properties: None,
    })
}

fn verify_join(
    node: &Node,
    descriptor: &JoinDescriptor,
    index: &Index<'_>,
) -> Result<(), PlanError> {
    let fork = index
        .nodes
        .get(&descriptor.fork_node_id)
        .ok_or_else(|| PlanError::new(PLAN_JOIN_INVALID, "Join references a missing Fork"))?;
    let NodeKind::Fork(fork_descriptor) = &fork.kind else {
        return Err(PlanError::new(
            PLAN_JOIN_INVALID,
            "Join correlation must reference a Fork node",
        ));
    };
    if node.scope_id != fork.scope_id {
        return Err(PlanError::new(
            PLAN_JOIN_INVALID,
            "Join and correlated Fork must belong to the same parent scope",
        ));
    }
    let expected = fork_descriptor
        .legs
        .iter()
        .map(|leg| leg.leg_id.clone())
        .collect::<BTreeSet<_>>();
    let actual = descriptor.legs.keys().cloned().collect::<BTreeSet<_>>();
    if expected != actual {
        return Err(PlanError::new(
            PLAN_JOIN_INVALID,
            "Join legs must exactly equal its correlated Fork member set (no missing/duplicate/extra leg)",
        ));
    }
    if descriptor.mode != fork_descriptor.join_mode {
        return Err(PlanError::new(
            PLAN_JOIN_INVALID,
            "Join mode does not match the mode frozen by its correlated Fork",
        ));
    }
    let mut inputs = BTreeSet::new();
    for fork_leg in &fork_descriptor.legs {
        let input = descriptor
            .legs
            .get(&fork_leg.leg_id)
            .expect("exact leg set checked");
        require_owned_control_port(index, node, input, PortDirection::Input)?;
        if !inputs.insert(input.clone()) {
            return Err(PlanError::new(
                PLAN_JOIN_INVALID,
                "Join legs must use distinct input ports",
            ));
        }
        if !port_reaches(&fork_leg.output_port, input, index, None) {
            return Err(PlanError::new(
                PLAN_JOIN_INVALID,
                format!(
                    "Join leg '{}' is not reachable from its correlated Fork output",
                    fork_leg.leg_id
                ),
            ));
        }
        verify_provenance_closed_path(
            &fork_leg.output_port,
            input,
            &fork.id,
            &node.id,
            index,
            PLAN_JOIN_INVALID,
            "Join leg",
        )?;
        for other in &fork_descriptor.legs {
            if other.leg_id != fork_leg.leg_id
                && port_reaches(&other.output_port, input, index, None)
            {
                return Err(PlanError::new(
                    PLAN_JOIN_INVALID,
                    format!(
                        "Join input '{}' can be reached from wrong Fork leg '{}'",
                        input, other.leg_id
                    ),
                ));
            }
        }
    }
    let actual_inputs = index
        .node_control_inputs
        .get(&node.id)
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .collect::<BTreeSet<_>>();
    if actual_inputs != inputs {
        return Err(PlanError::new(
            PLAN_JOIN_INVALID,
            "Join input ports must exactly equal its correlated leg map",
        ));
    }
    require_owned_control_port(index, node, &descriptor.output_port, PortDirection::Output)?;
    let outputs = index
        .node_control_outputs
        .get(&node.id)
        .cloned()
        .unwrap_or_default();
    if outputs != vec![descriptor.output_port.clone()] {
        return Err(PlanError::new(
            PLAN_JOIN_INVALID,
            "Join must have exactly its declared single control output",
        ));
    }
    if !matches!(
        descriptor.mode,
        PlanJoinMode::AllSuccess | PlanJoinMode::AllSettled
    ) {
        return Err(PlanError::new(PLAN_JOIN_INVALID, "unsupported Join mode"));
    }
    Ok(())
}

fn verify_loop(
    plan: &Plan,
    node: &Node,
    descriptor: &LoopDescriptor,
    index: &Index<'_>,
    dominators: &BTreeMap<NodeId, BTreeSet<NodeId>>,
) -> Result<(), PlanError> {
    let body_scopes = index
        .scopes
        .values()
        .filter(|scope| {
            matches!(
                &scope.kind,
                ScopeKind::LoopBody { loop_node_id } if loop_node_id == &node.id
            )
        })
        .count();
    if body_scopes != 1 {
        return Err(PlanError::new(
            PLAN_LOOP_INVALID,
            format!(
                "Loop '{}' must own exactly one correlated LoopBody scope",
                node.id
            ),
        ));
    }
    require_owned_control_port(
        index,
        node,
        &descriptor.continue_input,
        PortDirection::Input,
    )?;
    require_owned_control_port(index, node, &descriptor.body_output, PortDirection::Output)?;
    require_owned_control_port(
        index,
        node,
        &descriptor.completed_output,
        PortDirection::Output,
    )?;
    if descriptor.body_output == descriptor.completed_output {
        return Err(PlanError::new(
            PLAN_LOOP_INVALID,
            "Loop body and completed ports must be distinct",
        ));
    }
    let inputs = index
        .node_control_inputs
        .get(&node.id)
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .collect::<BTreeSet<_>>();
    let expected_input_count = 1 + usize::from(node.id != plan.metadata.entry_node_id);
    if !inputs.contains(&descriptor.continue_input) || inputs.len() != expected_input_count {
        return Err(PlanError::new(
            PLAN_LOOP_INVALID,
            format!(
                "Loop '{}' must have its continue input plus exactly one initial input when it is not the Plan entry",
                node.id
            ),
        ));
    }
    if descriptor.max_iterations.is_none() && descriptor.deadline_ms.is_none()
        || descriptor.max_iterations == Some(0)
        || descriptor.deadline_ms == Some(0)
    {
        return Err(PlanError::new(
            PLAN_LOOP_INVALID,
            format!(
                "Loop '{}' must declare a positive max_iterations and/or deadline",
                node.id
            ),
        ));
    }
    if descriptor
        .deadline_ms
        .is_some_and(|value| value > MAX_SAFE_SEMANTIC_INTEGER)
    {
        return Err(PlanError::new(
            PLAN_WIRE_INVALID,
            "Loop deadline exceeds the canonical JSON safe-integer range",
        ));
    }
    verify_expression(&descriptor.exit_condition, node, index, dominators, None)?;
    if descriptor.exit_condition.result_type == PlanType::Never
        || !descriptor
            .exit_condition
            .result_type
            .is_assignable_to(&PlanType::Boolean)
    {
        return Err(PlanError::new(
            PLAN_TYPE_MISMATCH,
            "Loop exit condition must produce a non-null boolean",
        ));
    }
    let outputs = index
        .node_control_outputs
        .get(&node.id)
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .collect::<BTreeSet<_>>();
    if outputs
        != BTreeSet::from([
            descriptor.body_output.clone(),
            descriptor.completed_output.clone(),
        ])
    {
        return Err(PlanError::new(
            PLAN_LOOP_INVALID,
            "Loop outputs must exactly equal body and completed ports",
        ));
    }
    Ok(())
}

fn verify_return(
    plan: &Plan,
    node: &Node,
    descriptor: &super::ReturnDescriptor,
    index: &Index<'_>,
) -> Result<(), PlanError> {
    let port = require_owned_data_port(index, node, &descriptor.value_input, PortDirection::Input)?;
    if !port.required
        || port.value_type == PlanType::Never
        || !port.value_type.is_assignable_to(&plan.metadata.output_type)
    {
        return Err(PlanError::new(
            PLAN_TERMINAL_INVALID,
            format!(
                "Return '{}' value does not satisfy workflow output contract",
                node.id
            ),
        ));
    }
    verify_terminal_shape(plan, node, &descriptor.value_input, index)
}

fn verify_raise(
    plan: &Plan,
    node: &Node,
    descriptor: &super::RaiseDescriptor,
    index: &Index<'_>,
) -> Result<(), PlanError> {
    let port = require_owned_data_port(index, node, &descriptor.error_input, PortDirection::Input)?;
    if !port.required
        || port.value_type == PlanType::Never
        || !port.value_type.is_assignable_to(&plan.metadata.error_type)
    {
        return Err(PlanError::new(
            PLAN_TERMINAL_INVALID,
            format!(
                "Raise '{}' value does not satisfy workflow error contract",
                node.id
            ),
        ));
    }
    verify_terminal_shape(plan, node, &descriptor.error_input, index)
}

fn verify_terminal_shape(
    plan: &Plan,
    node: &Node,
    declared_input: &DataPortId,
    index: &Index<'_>,
) -> Result<(), PlanError> {
    verify_linear_control_inputs(&plan.metadata.entry_node_id, node, index, "terminal")?;
    let data_inputs = index
        .node_data_inputs
        .get(&node.id)
        .cloned()
        .unwrap_or_default();
    let data_outputs = index
        .node_data_outputs
        .get(&node.id)
        .cloned()
        .unwrap_or_default();
    let declared_binding = plan
        .data_bindings
        .iter()
        .find(|binding| &binding.to == declared_input)
        .ok_or_else(|| {
            PlanError::new(
                PLAN_TERMINAL_INVALID,
                format!("terminal node '{}' declared input is unbound", node.id),
            )
        })?;
    let auxiliary_inputs = match &declared_binding.source {
        ValueSource::Expression { expression } => expression
            .dependencies
            .values()
            .filter(|dependency| {
                index.data_ports.get(*dependency).is_some_and(|port| {
                    port.owner == node.id && port.direction == PortDirection::Input
                })
            })
            .cloned()
            .collect::<BTreeSet<_>>(),
        _ => BTreeSet::new(),
    };
    let expected_inputs = std::iter::once(declared_input.clone())
        .chain(auxiliary_inputs)
        .collect::<BTreeSet<_>>();
    let actual_inputs = data_inputs.into_iter().collect::<BTreeSet<_>>();
    if actual_inputs != expected_inputs || !data_outputs.is_empty() {
        return Err(PlanError::new(
            PLAN_TERMINAL_INVALID,
            format!(
                "terminal node '{}' may only own its declared input plus explicitly referenced expression inputs, and no data outputs",
                node.id
            ),
        ));
    }
    Ok(())
}

fn verify_linear_control_inputs(
    entry: &NodeId,
    node: &Node,
    index: &Index<'_>,
    label: &str,
) -> Result<(), PlanError> {
    let inputs = index.node_control_inputs.get(&node.id).map_or(0, Vec::len);
    let expected = usize::from(&node.id != entry);
    if inputs != expected {
        return Err(PlanError::new(
            PLAN_PORT_INVALID,
            format!(
                "{label} node '{}' must have exactly {expected} control input(s); explicit Merge/Join is required for correlation",
                node.id
            ),
        ));
    }
    Ok(())
}

fn verify_phi_bindings(
    plan: &Plan,
    index: &Index<'_>,
    dominators: &BTreeMap<NodeId, BTreeSet<NodeId>>,
) -> Result<(), PlanError> {
    let mut ids = BTreeSet::new();
    let mut outputs = BTreeSet::new();
    for phi in &plan.phi_bindings {
        if !ids.insert(phi.id.clone()) {
            return duplicate("Phi binding", &phi.id);
        }
        if !outputs.insert(phi.output.clone()) {
            return Err(PlanError::new(
                PLAN_PHI_INVALID,
                format!("data output '{}' has more than one Phi binding", phi.output),
            ));
        }
        let merge = index.nodes.get(&phi.merge_node_id).ok_or_else(|| {
            PlanError::new(PLAN_PHI_INVALID, "Phi references a missing Merge node")
        })?;
        let NodeKind::Merge(merge_descriptor) = &merge.kind else {
            return Err(PlanError::new(
                PLAN_PHI_INVALID,
                "Phi owner must be a Merge node",
            ));
        };
        let output = require_owned_data_port(index, merge, &phi.output, PortDirection::Output)?;
        let expected = merge_descriptor
            .arms
            .keys()
            .cloned()
            .collect::<BTreeSet<_>>();
        let actual = phi.sources.keys().cloned().collect::<BTreeSet<_>>();
        if expected != actual {
            return Err(PlanError::new(
                PLAN_PHI_INVALID,
                format!(
                    "Phi '{}' sources must exactly cover every Merge-correlated arm",
                    phi.id
                ),
            ));
        }
        let branch = index
            .nodes
            .get(&merge_descriptor.branch_node_id)
            .expect("Merge verifier checked Branch correlation");
        let NodeKind::Branch(branch_descriptor) = &branch.kind else {
            unreachable!("Merge verifier checked Branch kind")
        };
        for (case_id, source) in &phi.sources {
            let start = &branch_descriptor
                .cases
                .iter()
                .find(|case| &case.case_id == case_id)
                .expect("Phi keys equal Merge arms, which are checked against Branch")
                .output_port;
            let target = merge_descriptor
                .arms
                .get(case_id)
                .expect("Phi keys equal Merge arms");
            let source_type = verify_value_source(
                source,
                merge,
                plan,
                index,
                dominators,
                Some((start, target)),
            )?;
            if source_type == PlanType::Never || !source_type.is_assignable_to(&output.value_type) {
                return Err(PlanError::new(
                    PLAN_TYPE_MISMATCH,
                    format!(
                        "Phi '{}' arm '{}' source is not assignable to output '{}'",
                        phi.id, case_id, phi.output
                    ),
                ));
            }
        }
    }

    for node in &plan.nodes {
        if matches!(node.kind, NodeKind::Merge(_)) {
            let data_outputs = index
                .node_data_outputs
                .get(&node.id)
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .collect::<BTreeSet<_>>();
            let phi_outputs = plan
                .phi_bindings
                .iter()
                .filter(|phi| phi.merge_node_id == node.id)
                .map(|phi| phi.output.clone())
                .collect::<BTreeSet<_>>();
            if data_outputs != phi_outputs {
                return Err(PlanError::new(
                    PLAN_PHI_INVALID,
                    format!(
                        "Merge '{}' data outputs and Phi bindings must be complete and exact",
                        node.id
                    ),
                ));
            }
        }
    }
    Ok(())
}

fn verify_arm_value_available(
    start: &ControlPortId,
    target: &ControlPortId,
    producer: &NodeId,
    index: &Index<'_>,
) -> Result<(), PlanError> {
    verify_phi_scope_path(target, producer, index)?;
    if !port_reaches_owner(start, producer, index) {
        return Err(PlanError::new(
            PLAN_DOMINANCE_INVALID,
            format!("Phi source node '{producer}' is not on its correlated Branch arm"),
        ));
    }
    if port_reaches(start, target, index, Some(producer)) {
        return Err(PlanError::new(
            PLAN_DOMINANCE_INVALID,
            format!(
                "Phi source node '{producer}' does not dominate every path of its correlated arm"
            ),
        ));
    }
    Ok(())
}

fn verify_phi_scope_path(
    merge_input: &ControlPortId,
    producer: &NodeId,
    index: &Index<'_>,
) -> Result<(), PlanError> {
    let merge_port = require_control_port(index, merge_input)?;
    let merge = index
        .nodes
        .get(&merge_port.owner)
        .expect("Merge input owner exists");
    let NodeKind::Merge(descriptor) = &merge.kind else {
        return Err(PlanError::new(
            PLAN_PHI_INVALID,
            "arm-specific value target is not a Merge input",
        ));
    };
    let case_id = descriptor
        .arms
        .iter()
        .find_map(|(case_id, input)| (input == merge_input).then_some(case_id))
        .expect("Merge descriptor validation matched every input");
    let producer_node = index
        .nodes
        .get(producer)
        .expect("Phi source producer exists");
    if producer_node.scope_id == merge.scope_id {
        return Ok(());
    }

    let mut cursor = producer_node.scope_id.clone();
    while cursor != merge.scope_id {
        let scope = index
            .scopes
            .get(&cursor)
            .ok_or_else(|| PlanError::new(PLAN_SCOPE_INVALID, "Phi source scope does not exist"))?;
        match &scope.kind {
            ScopeKind::Lexical => {}
            ScopeKind::BranchArm {
                branch_node_id,
                case_id: scope_case,
            } if branch_node_id == &descriptor.branch_node_id && scope_case == case_id => {}
            ScopeKind::MapBody { .. }
            | ScopeKind::LoopBody { .. }
            | ScopeKind::ForkLeg { .. }
            | ScopeKind::ErrorProtected { .. }
            | ScopeKind::ErrorHandler { .. }
            | ScopeKind::ErrorFinalizer { .. }
            | ScopeKind::Subflow { .. } => {
                return Err(PlanError::new(
                    PLAN_SCOPE_INVALID,
                    format!(
                        "Phi source '{}' crosses a dynamic multi-instance scope without typed Collect",
                        producer
                    ),
                ));
            }
            _ => {
                return Err(PlanError::new(
                    PLAN_SCOPE_INVALID,
                    format!(
                        "Phi source '{}' is not in the correlated Branch arm scope",
                        producer
                    ),
                ));
            }
        }
        cursor = scope.parent.clone().ok_or_else(|| {
            PlanError::new(
                PLAN_SCOPE_INVALID,
                "Phi source scope is not a descendant of its Merge scope",
            )
        })?;
    }
    Ok(())
}

fn verify_policies(plan: &Plan, index: &Index<'_>) -> Result<(), PlanError> {
    let mut ids = BTreeSet::new();
    let mut per_node_kind = BTreeSet::new();
    for policy in &plan.policies {
        if !ids.insert(policy.id.clone()) {
            return duplicate("policy", &policy.id);
        }
        if !index.nodes.contains_key(&policy.node_id) {
            return Err(PlanError::new(
                PLAN_POLICY_INVALID,
                format!("policy '{}' references a missing node", policy.id),
            ));
        }
        let target = index
            .nodes
            .get(&policy.node_id)
            .expect("policy target existence checked");
        policy_execution_contract(&policy.kind, &target.kind).map_err(|reason| {
            PlanError::new(
                PLAN_POLICY_INVALID,
                format!(
                    "policy '{}' kind '{}' is not executable for node kind '{}': {reason}",
                    policy.id,
                    policy.kind.name(),
                    target.kind.name()
                ),
            )
            .with_target(PlanDiagnosticTarget::Node {
                node_id: policy.node_id.clone(),
            })
        })?;
        if !per_node_kind.insert((policy.node_id.clone(), policy.kind.name())) {
            return Err(PlanError::new(
                PLAN_POLICY_INVALID,
                format!(
                    "node '{}' has more than one '{}' policy",
                    policy.node_id,
                    policy.kind.name()
                ),
            ));
        }
        match &policy.kind {
            PolicyKind::Retry(value) => {
                if value.max_attempts == 0 || value.max_backoff_ms < value.initial_backoff_ms {
                    return Err(PlanError::new(
                        PLAN_POLICY_INVALID,
                        "Retry policy must have positive attempts and monotonic backoff bounds",
                    ));
                }
                verify_safe_u64(value.initial_backoff_ms, "Retry initial backoff")?;
                verify_safe_u64(value.max_backoff_ms, "Retry maximum backoff")?;
            }
            PolicyKind::Timeout(value) if value.timeout_ms == 0 => {
                return Err(PlanError::new(
                    PLAN_POLICY_INVALID,
                    "Timeout policy must be positive",
                ));
            }
            PolicyKind::Budget(value)
                if value.max_tokens.is_none() && value.max_cost_microunits.is_none() =>
            {
                return Err(PlanError::new(
                    PLAN_POLICY_INVALID,
                    "Budget policy must define at least one bound",
                ));
            }
            PolicyKind::Timeout(value) => {
                verify_safe_u64(value.timeout_ms, "Timeout policy")?;
            }
            PolicyKind::Budget(value) => {
                if let Some(tokens) = value.max_tokens {
                    verify_safe_u64(tokens, "Budget token bound")?;
                }
                if let Some(cost) = value.max_cost_microunits {
                    verify_safe_u64(cost, "Budget cost bound")?;
                }
            }
        }
    }
    Ok(())
}

/// Runtime contracts implemented for authored policies. Keeping this matrix
/// closed at the verified Plan boundary prevents a syntactically valid policy
/// from being published and then silently discarded by the scheduler.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PolicyExecutionContract {
    LeafRetry,
    LeafTimeout,
    DurableWaitTimeout,
}

fn policy_execution_contract(
    policy: &PolicyKind,
    node: &NodeKind,
) -> Result<PolicyExecutionContract, &'static str> {
    let leaf = matches!(
        node,
        NodeKind::LlmTask(_)
            | NodeKind::ActionTask(_)
            | NodeKind::RetrievalTask(_)
            | NodeKind::HttpTask(_)
            | NodeKind::ToolTask(_)
    );
    match (policy, node) {
        (PolicyKind::Retry(_), _) if leaf => Ok(PolicyExecutionContract::LeafRetry),
        (PolicyKind::Timeout(_), _) if leaf => Ok(PolicyExecutionContract::LeafTimeout),
        (PolicyKind::Timeout(_), NodeKind::HumanTask(_) | NodeKind::WaitSignal(_)) => {
            Ok(PolicyExecutionContract::DurableWaitTimeout)
        }
        (PolicyKind::Budget(_), _) => Err("budget enforcement has no durable runtime contract"),
        (PolicyKind::Retry(_), _) => Err("retry is supported only for leaf task nodes"),
        (
            PolicyKind::Timeout(_),
            NodeKind::Map(_) | NodeKind::Loop(_) | NodeKind::SubflowCall(_) | NodeKind::Timer(_),
        ) => Err("structural timeout enforcement has no durable runtime contract"),
        (PolicyKind::Timeout(_), _) => {
            Err("timeout is supported only for leaf task, human_task, and wait_signal nodes")
        }
    }
}

fn verify_source_map(plan: &Plan, index: &Index<'_>) -> Result<(), PlanError> {
    let control_edges = plan
        .control_edges
        .iter()
        .map(|value| &value.id)
        .collect::<BTreeSet<_>>();
    let data_bindings = plan
        .data_bindings
        .iter()
        .map(|value| &value.id)
        .collect::<BTreeSet<_>>();
    let phi_bindings = plan
        .phi_bindings
        .iter()
        .map(|value| &value.id)
        .collect::<BTreeSet<_>>();
    let policies = plan
        .policies
        .iter()
        .map(|value| &value.id)
        .collect::<BTreeSet<_>>();

    for (id, span) in plan.source_map.nodes() {
        if !index.nodes.contains_key(id) {
            return invalid_source_ref("node", id);
        }
        verify_span(span)?;
    }
    for (id, span) in plan.source_map.control_ports() {
        if !index.control_ports.contains_key(id) {
            return invalid_source_ref("control port", id);
        }
        verify_span(span)?;
    }
    for (id, span) in plan.source_map.data_ports() {
        if !index.data_ports.contains_key(id) {
            return invalid_source_ref("data port", id);
        }
        verify_span(span)?;
    }
    for (id, span) in plan.source_map.control_edges() {
        if !control_edges.contains(id) {
            return invalid_source_ref("control edge", id);
        }
        verify_span(span)?;
    }
    for (id, span) in plan.source_map.data_bindings() {
        if !data_bindings.contains(id) {
            return invalid_source_ref("data binding", id);
        }
        verify_span(span)?;
    }
    for (id, span) in plan.source_map.phi_bindings() {
        if !phi_bindings.contains(id) {
            return invalid_source_ref("Phi binding", id);
        }
        verify_span(span)?;
    }
    for (id, span) in plan.source_map.scopes() {
        if !index.scopes.contains_key(id) {
            return invalid_source_ref("scope", id);
        }
        verify_span(span)?;
    }
    for (id, span) in plan.source_map.policies() {
        if !policies.contains(id) {
            return invalid_source_ref("policy", id);
        }
        verify_span(span)?;
    }
    match plan.source_map.coverage_policy() {
        SourceMapPolicy::ProgrammaticExempt => {
            if plan.metadata.author_format != super::AuthorFormat::Programmatic {
                return Err(PlanError::new(
                    PLAN_WIRE_INVALID,
                    "Structured/Graph Plans require an authored-complete SourceMap",
                ));
            }
        }
        SourceMapPolicy::AuthoredComplete => {
            verify_authored_source_map_completeness(plan)?;
        }
    }
    Ok(())
}

fn verify_authored_source_map_completeness(plan: &Plan) -> Result<(), PlanError> {
    if plan.source_map.documents().is_empty() {
        return Err(PlanError::new(
            PLAN_WIRE_INVALID,
            "authored-complete SourceMap must bind at least one source document content hash",
        ));
    }

    let complete = plan.source_map.nodes().len() == plan.nodes.len()
        && plan.source_map.control_ports().len() == plan.control_ports.len()
        && plan.source_map.data_ports().len() == plan.data_ports.len()
        && plan.source_map.control_edges().len() == plan.control_edges.len()
        && plan.source_map.data_bindings().len() == plan.data_bindings.len()
        && plan.source_map.phi_bindings().len() == plan.phi_bindings.len()
        && plan.source_map.scopes().len() == plan.scopes.len()
        && plan.source_map.policies().len() == plan.policies.len();
    if !complete {
        return Err(PlanError::new(
            PLAN_WIRE_INVALID,
            "authored-complete SourceMap must cover every Node, Port, Edge, Binding, Scope, and Policy",
        ));
    }

    let spans = plan
        .source_map
        .nodes()
        .values()
        .chain(plan.source_map.control_ports().values())
        .chain(plan.source_map.data_ports().values())
        .chain(plan.source_map.control_edges().values())
        .chain(plan.source_map.data_bindings().values())
        .chain(plan.source_map.phi_bindings().values())
        .chain(plan.source_map.scopes().values())
        .chain(plan.source_map.policies().values());
    let referenced_documents = spans
        .map(|span| span.source_id.clone())
        .collect::<BTreeSet<_>>();
    let declared_documents = plan
        .source_map
        .documents()
        .keys()
        .cloned()
        .collect::<BTreeSet<_>>();
    if referenced_documents != declared_documents {
        return Err(PlanError::new(
            PLAN_WIRE_INVALID,
            "authored SourceMap document IDs must exactly match its content-hash registry",
        ));
    }
    Ok(())
}

fn verify_span(span: &SourceSpan) -> Result<(), PlanError> {
    if span.start.line == 0
        || span.start.column == 0
        || span.end.line == 0
        || span.end.column == 0
        || span.end.offset < span.start.offset
        || (span.end.line, span.end.column) < (span.start.line, span.start.column)
    {
        return Err(PlanError::new(
            PLAN_WIRE_INVALID,
            "SourceSpan uses 1-based line/column and must have end >= start",
        ));
    }
    Ok(())
}

fn type_at_input_path(root: &PlanType, path: &[String]) -> Result<PlanType, PlanError> {
    if path.len() > MAX_RUN_INPUT_PATH_SEGMENTS {
        return Err(PlanError::new(
            PLAN_DESCRIPTOR_INVALID,
            "RunInput path contains too many segments",
        ));
    }
    let mut current = root.clone();
    for segment in path {
        validate_name("RunInput path segment", segment)?;
        current = match current {
            PlanType::Object {
                properties,
                additional_properties,
            } => match properties.get(segment) {
                Some(property) if property.required => property.value_type.clone(),
                Some(_) => {
                    return Err(PlanError::new(
                        PLAN_TYPE_MISMATCH,
                        format!(
                            "RunInput path field '{segment}' is optional; missing must be handled by an explicit typed expression/default"
                        ),
                    ));
                }
                None if additional_properties.is_some() => {
                    return Err(PlanError::new(
                        PLAN_TYPE_MISMATCH,
                        format!(
                            "RunInput path field '{segment}' comes from open additional properties and may be missing; use an explicit typed expression/default"
                        ),
                    ));
                }
                None => {
                    return Err(PlanError::new(
                        PLAN_TYPE_MISMATCH,
                        format!("RunInput path field '{segment}' is not present in input type"),
                    ));
                }
            },
            PlanType::Array { items, min_items }
            | PlanType::ArrayBounded {
                items, min_items, ..
            } => {
                let ordinal = segment.parse::<u64>().map_err(|_| {
                    PlanError::new(
                        PLAN_TYPE_MISMATCH,
                        format!("RunInput array path segment '{segment}' is not an ordinal"),
                    )
                })?;
                if ordinal >= min_items {
                    return Err(PlanError::new(
                        PLAN_TYPE_MISMATCH,
                        format!(
                            "RunInput array index {ordinal} is not guaranteed present by min_items={min_items}; use an explicit typed expression/default"
                        ),
                    ));
                }
                *items
            }
            PlanType::Union { variants } => {
                let selected = variants
                    .iter()
                    .map(|variant| type_at_input_path(variant, std::slice::from_ref(segment)))
                    .collect::<Result<Vec<_>, _>>()?;
                PlanType::unify(selected).map_err(|error| {
                    PlanError::new(
                        PLAN_TYPE_MISMATCH,
                        format!("cannot type RunInput union path: {error}"),
                    )
                })?
            }
            _ => {
                return Err(PlanError::new(
                    PLAN_TYPE_MISMATCH,
                    format!("RunInput path cannot traverse segment '{segment}'"),
                ));
            }
        };
    }
    Ok(current)
}

fn type_at_optional_input_path(
    contract: &super::PlanInputContract,
    path: &[String],
) -> Result<PlanType, PlanError> {
    if path.is_empty() || path.len() > MAX_RUN_INPUT_PATH_SEGMENTS {
        return Err(PlanError::new(
            PLAN_DESCRIPTOR_INVALID,
            "OptionalRunInput path must contain a bounded top-level field",
        ));
    }
    let root = &path[0];
    validate_name("OptionalRunInput path segment", root)?;
    let PlanType::Object { properties, .. } = contract.accepted_type() else {
        return Err(PlanError::new(
            PLAN_TYPE_MISMATCH,
            "OptionalRunInput requires an object input contract",
        ));
    };
    let property = properties.get(root).ok_or_else(|| {
        PlanError::new(
            PLAN_TYPE_MISMATCH,
            format!("OptionalRunInput field '{root}' is not declared"),
        )
    })?;
    if property.required || contract.defaults().contains_key(root) {
        return Err(PlanError::new(
            PLAN_TYPE_MISMATCH,
            format!("OptionalRunInput field '{root}' is not optional"),
        ));
    }
    type_at_input_path(&property.value_type, &path[1..])
}

fn validate_json_literal(value: &Value) -> Result<(), PlanError> {
    fn visit(value: &Value, depth: usize, items: &mut usize) -> Result<(), PlanError> {
        if depth > MAX_DESCRIPTOR_DEPTH {
            return Err(PlanError::new(
                PLAN_DESCRIPTOR_INVALID,
                "literal nesting exceeds Plan limit",
            ));
        }
        *items += 1;
        if *items > MAX_DESCRIPTOR_COLLECTION_ITEMS {
            return Err(PlanError::new(
                PLAN_DESCRIPTOR_INVALID,
                "literal contains too many values",
            ));
        }
        match value {
            Value::String(value) if value.len() > MAX_DESCRIPTOR_STRING_BYTES => Err(
                PlanError::new(PLAN_DESCRIPTOR_INVALID, "literal string exceeds Plan limit"),
            ),
            Value::Array(values) => {
                for value in values {
                    visit(value, depth + 1, items)?;
                }
                Ok(())
            }
            Value::Object(values) => {
                for (key, value) in values {
                    validate_name("literal object key", key)?;
                    visit(value, depth + 1, items)?;
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }
    visit(value, 0, &mut 0)
}

fn validate_descriptor_map(
    values: &BTreeMap<String, DescriptorValue>,
    depth: usize,
    items: &mut usize,
) -> Result<(), PlanError> {
    if depth > MAX_DESCRIPTOR_DEPTH {
        return Err(PlanError::new(
            PLAN_DESCRIPTOR_INVALID,
            "descriptor configuration nesting exceeds Plan limit",
        ));
    }
    for (key, value) in values {
        validate_name("descriptor key", key)?;
        validate_descriptor_value(value, depth + 1, items)?;
    }
    Ok(())
}

fn validate_descriptor_value(
    value: &DescriptorValue,
    depth: usize,
    items: &mut usize,
) -> Result<(), PlanError> {
    *items += 1;
    if *items > MAX_DESCRIPTOR_COLLECTION_ITEMS || depth > MAX_DESCRIPTOR_DEPTH {
        return Err(PlanError::new(
            PLAN_DESCRIPTOR_INVALID,
            "descriptor configuration exceeds Plan collection/depth limit",
        ));
    }
    match value {
        DescriptorValue::String(value) if value.len() > MAX_DESCRIPTOR_STRING_BYTES => {
            Err(PlanError::new(
                PLAN_DESCRIPTOR_INVALID,
                "descriptor string exceeds Plan limit",
            ))
        }
        DescriptorValue::Integer(value)
            if *value < -(MAX_SAFE_SEMANTIC_INTEGER as i64)
                || *value > MAX_SAFE_SEMANTIC_INTEGER as i64 =>
        {
            Err(PlanError::new(
                PLAN_WIRE_INVALID,
                "descriptor integer exceeds the canonical JSON safe-integer range",
            ))
        }
        DescriptorValue::Array(values) => {
            for value in values {
                validate_descriptor_value(value, depth + 1, items)?;
            }
            Ok(())
        }
        DescriptorValue::Object(values) => validate_descriptor_map(values, depth + 1, items),
        _ => Ok(()),
    }
}

fn verify_safe_u64(value: u64, label: &str) -> Result<(), PlanError> {
    if value > MAX_SAFE_SEMANTIC_INTEGER {
        return Err(PlanError::new(
            PLAN_WIRE_INVALID,
            format!("{label} exceeds the canonical JSON safe-integer range"),
        ));
    }
    Ok(())
}

fn verify_canonical_type(value: &PlanType, label: &str) -> Result<(), PlanError> {
    let normalized = value.normalized().map_err(|error| {
        PlanError::new(
            PLAN_TYPE_MISMATCH,
            format!("{label} contains an invalid type: {error}"),
        )
    })?;
    if &normalized != value {
        return Err(PlanError::new(
            PLAN_TYPE_MISMATCH,
            format!("{label} type is not in canonical normalized form"),
        ));
    }
    Ok(())
}

fn validate_name(label: &str, value: &str) -> Result<(), PlanError> {
    if value.is_empty() || value.len() > MAX_NAME_BYTES || value.chars().any(char::is_control) {
        return Err(PlanError::new(
            PLAN_DESCRIPTOR_INVALID,
            format!("{label} must be non-empty, bounded, and contain no controls"),
        ));
    }
    Ok(())
}

fn port_reaches(
    start: &ControlPortId,
    target: &ControlPortId,
    index: &Index<'_>,
    removed_owner: Option<&NodeId>,
) -> bool {
    let mut seen = BTreeSet::new();
    let mut queue = VecDeque::from([start.clone()]);
    while let Some(port) = queue.pop_front() {
        if &port == target {
            return true;
        }
        if !seen.insert(port.clone()) {
            continue;
        }
        if removed_owner.is_some_and(|owner| {
            index
                .control_ports
                .get(&port)
                .is_some_and(|value| &value.owner == owner)
        }) {
            continue;
        }
        if let Some(next) = index.port_graph.get(&port) {
            queue.extend(
                next.iter()
                    .filter(|candidate| {
                        // Correlation is per dynamic scope instance. Never
                        // walk a static Loop back-edge into a later iteration
                        // while proving a Branch arm or Fork leg correlation.
                        (!index.loop_continue_inputs.contains(*candidate) || *candidate == target)
                            && !removed_owner.is_some_and(|owner| {
                                index
                                    .control_ports
                                    .get(*candidate)
                                    .is_some_and(|value| &value.owner == owner)
                            })
                    })
                    .cloned(),
            );
        }
    }
    false
}

fn port_reaches_any(
    start: &ControlPortId,
    targets: &BTreeSet<ControlPortId>,
    index: &Index<'_>,
    cross_loop_iterations: bool,
) -> bool {
    let mut seen = BTreeSet::new();
    let mut queue = VecDeque::from([start.clone()]);
    while let Some(port) = queue.pop_front() {
        if targets.contains(&port) {
            return true;
        }
        if !seen.insert(port.clone()) {
            continue;
        }
        if let Some(next) = index.port_graph.get(&port) {
            queue.extend(
                next.iter()
                    .filter(|candidate| {
                        cross_loop_iterations || !index.loop_continue_inputs.contains(*candidate)
                    })
                    .cloned(),
            );
        }
    }
    false
}

#[allow(clippy::too_many_arguments)]
fn verify_provenance_closed_path(
    start: &ControlPortId,
    target: &ControlPortId,
    source_node: &NodeId,
    target_node: &NodeId,
    index: &Index<'_>,
    error_code: &'static str,
    label: &str,
) -> Result<(), PlanError> {
    let forward = port_reachable_set(start, index);
    let mut reverse_graph: BTreeMap<ControlPortId, BTreeSet<ControlPortId>> = BTreeMap::new();
    for (from, targets) in &index.port_graph {
        for to in targets {
            if index.loop_continue_inputs.contains(to) {
                continue;
            }
            reverse_graph
                .entry(to.clone())
                .or_default()
                .insert(from.clone());
        }
    }
    let mut reverse = BTreeSet::new();
    let mut queue = VecDeque::from([target.clone()]);
    while let Some(port) = queue.pop_front() {
        if !reverse.insert(port.clone()) {
            continue;
        }
        if let Some(previous) = reverse_graph.get(&port) {
            queue.extend(previous.iter().cloned());
        }
    }
    let corridor_nodes = forward
        .intersection(&reverse)
        .filter_map(|port| {
            index
                .control_ports
                .get(port)
                .map(|value| value.owner.clone())
        })
        .collect::<BTreeSet<_>>();

    for owner in corridor_nodes {
        if &owner == source_node || &owner == target_node {
            continue;
        }
        let node = index.nodes.get(&owner).expect("corridor port owner exists");
        for input in index.node_control_inputs.get(&owner).into_iter().flatten() {
            if matches!(
                &node.kind,
                NodeKind::Loop(descriptor) if &descriptor.continue_input == input
            ) {
                continue;
            }
            if index.incoming_control.contains_key(input) && !forward.contains(input) {
                return Err(PlanError::new(
                    error_code,
                    format!(
                        "{label} path admits an uncorrelated bypass through node '{}' input '{}'",
                        owner, input
                    ),
                ));
            }
        }
    }
    Ok(())
}

fn port_reachable_set(start: &ControlPortId, index: &Index<'_>) -> BTreeSet<ControlPortId> {
    let mut reached = BTreeSet::new();
    let mut queue = VecDeque::from([start.clone()]);
    while let Some(port) = queue.pop_front() {
        if !reached.insert(port.clone()) {
            continue;
        }
        if let Some(next) = index.port_graph.get(&port) {
            queue.extend(
                next.iter()
                    .filter(|candidate| !index.loop_continue_inputs.contains(*candidate))
                    .cloned(),
            );
        }
    }
    reached
}

fn port_reaches_owner(start: &ControlPortId, owner: &NodeId, index: &Index<'_>) -> bool {
    let targets = index
        .node_control_inputs
        .get(owner)
        .cloned()
        .unwrap_or_default();
    targets
        .iter()
        .any(|target| port_reaches(start, target, index, None))
}

fn is_scope_ancestor(ancestor: &ScopeId, descendant: &ScopeId, index: &Index<'_>) -> bool {
    let mut cursor = Some(descendant);
    while let Some(id) = cursor {
        if id == ancestor {
            return true;
        }
        cursor = index.scopes.get(id).and_then(|scope| scope.parent.as_ref());
    }
    false
}

fn require_control_port<'a>(
    index: &'a Index<'a>,
    id: &ControlPortId,
) -> Result<&'a ControlPort, PlanError> {
    index.control_ports.get(id).copied().ok_or_else(|| {
        PlanError::new(
            PLAN_REFERENCE_INVALID,
            format!("control port '{id}' does not exist"),
        )
        .with_target(PlanDiagnosticTarget::ControlPort {
            port_id: id.clone(),
            node_id: None,
        })
    })
}

fn require_data_port<'a>(index: &'a Index<'a>, id: &DataPortId) -> Result<&'a DataPort, PlanError> {
    index.data_ports.get(id).copied().ok_or_else(|| {
        PlanError::new(
            PLAN_REFERENCE_INVALID,
            format!("data port '{id}' does not exist"),
        )
        .with_target(PlanDiagnosticTarget::DataPort {
            port_id: id.clone(),
            node_id: None,
        })
    })
}

fn require_owned_control_port<'a>(
    index: &'a Index<'a>,
    owner: &Node,
    id: &ControlPortId,
    direction: PortDirection,
) -> Result<&'a ControlPort, PlanError> {
    let port = require_control_port(index, id)?;
    if port.owner != owner.id || port.direction != direction {
        return Err(PlanError::new(
            PLAN_PORT_INVALID,
            format!(
                "control port '{}' is not a {:?} owned by node '{}'",
                id, direction, owner.id
            ),
        )
        .with_target(PlanDiagnosticTarget::ControlPort {
            port_id: id.clone(),
            node_id: Some(owner.id.clone()),
        }));
    }
    Ok(port)
}

fn require_owned_data_port<'a>(
    index: &'a Index<'a>,
    owner: &Node,
    id: &DataPortId,
    direction: PortDirection,
) -> Result<&'a DataPort, PlanError> {
    let port = require_data_port(index, id)?;
    if port.owner != owner.id || port.direction != direction {
        return Err(PlanError::new(
            PLAN_PORT_INVALID,
            format!(
                "data port '{}' is not a {:?} owned by node '{}'",
                id, direction, owner.id
            ),
        )
        .with_target(PlanDiagnosticTarget::DataPort {
            port_id: id.clone(),
            node_id: Some(owner.id.clone()),
        }));
    }
    Ok(port)
}

fn duplicate<T: std::fmt::Display, R>(kind: &str, id: &T) -> Result<R, PlanError> {
    Err(PlanError::new(
        PLAN_ID_DUPLICATE,
        format!("duplicate {kind} ID '{id}'"),
    ))
}

fn invalid_source_ref<T: std::fmt::Display, R>(kind: &str, id: &T) -> Result<R, PlanError> {
    Err(PlanError::new(
        PLAN_REFERENCE_INVALID,
        format!("SourceMap references missing {kind} '{id}'"),
    ))
}
