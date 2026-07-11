use std::collections::{BTreeMap, BTreeSet};

use super::{
    compiled::{CompiledNode, ExecutionPlan, NodeRegion},
    CompileError,
};

pub fn validate_graph(
    entry: &str,
    nodes: &BTreeMap<String, CompiledNode>,
    plan: &ExecutionPlan,
) -> Result<(), CompileError> {
    validate_graph_structure(entry, nodes)?;
    validate_references(entry, nodes, plan)
}

pub fn validate_graph_structure(
    entry: &str,
    nodes: &BTreeMap<String, CompiledNode>,
) -> Result<(), CompileError> {
    if !nodes.contains_key(entry) {
        return Err(CompileError::new(
            "ENTRY_NOT_FOUND",
            format!("entry node '{entry}' does not exist"),
        ));
    }

    for (node_id, node) in nodes {
        for edge in &node.edges {
            if !nodes.contains_key(edge) {
                return Err(CompileError::new(
                    "NODE_EDGE_NOT_FOUND",
                    format!("node '{node_id}' points to missing node '{edge}'"),
                ));
            }
        }
    }

    reject_cycles(entry, nodes)?;
    let reachable = reachable_from(entry, nodes);
    if let Some(unreachable) = nodes.keys().find(|node_id| !reachable.contains(*node_id)) {
        return Err(CompileError::new(
            "NODE_UNREACHABLE",
            format!("node '{unreachable}' is unreachable from entry '{entry}'"),
        ));
    }

    for (node_id, node) in nodes {
        if node.terminal && node.kind != "core.output" {
            return Err(CompileError::new(
                "OUTPUT_REQUIRED",
                format!("terminal node '{node_id}' must use type 'core.output'"),
            ));
        }
        if node.terminal && !node.edges.is_empty() {
            return Err(CompileError::new(
                "OUTPUT_INVALID",
                format!("output node '{node_id}' cannot have outgoing edges"),
            ));
        }
        if !node.terminal && node.edges.is_empty() {
            return Err(CompileError::new(
                "OUTPUT_REQUIRED",
                format!("reachable path ends at non-output node '{node_id}'"),
            ));
        }
    }

    Ok(())
}

fn reject_cycles(entry: &str, nodes: &BTreeMap<String, CompiledNode>) -> Result<(), CompileError> {
    fn visit(
        node_id: &str,
        nodes: &BTreeMap<String, CompiledNode>,
        visiting: &mut BTreeSet<String>,
        visited: &mut BTreeSet<String>,
    ) -> Result<(), CompileError> {
        if visiting.contains(node_id) {
            return Err(CompileError::new(
                "GRAPH_CYCLE",
                format!("graph cycle detected at node '{node_id}'"),
            ));
        }
        if visited.contains(node_id) {
            return Ok(());
        }
        visiting.insert(node_id.to_string());
        if let Some(node) = nodes.get(node_id) {
            for edge in &node.edges {
                visit(edge, nodes, visiting, visited)?;
            }
        }
        visiting.remove(node_id);
        visited.insert(node_id.to_string());
        Ok(())
    }

    visit(entry, nodes, &mut BTreeSet::new(), &mut BTreeSet::new())
}

fn reachable_from(entry: &str, nodes: &BTreeMap<String, CompiledNode>) -> BTreeSet<String> {
    let mut reachable = BTreeSet::new();
    let mut pending = vec![entry.to_string()];
    while let Some(node_id) = pending.pop() {
        if !reachable.insert(node_id.clone()) {
            continue;
        }
        if let Some(node) = nodes.get(&node_id) {
            pending.extend(node.edges.iter().cloned());
        }
    }
    reachable
}

pub fn validate_references(
    entry: &str,
    nodes: &BTreeMap<String, CompiledNode>,
    plan: &ExecutionPlan,
) -> Result<(), CompileError> {
    let all = nodes.keys().cloned().collect::<BTreeSet<_>>();
    let mut predecessors = nodes
        .keys()
        .map(|node_id| (node_id.clone(), BTreeSet::new()))
        .collect::<BTreeMap<_, _>>();
    for (node_id, node) in nodes {
        for edge in &node.edges {
            predecessors
                .get_mut(edge)
                .expect("edges were validated")
                .insert(node_id.clone());
        }
    }

    let mut dominators = nodes
        .keys()
        .map(|node_id| {
            let values = if node_id == entry {
                BTreeSet::from([entry.to_string()])
            } else {
                all.clone()
            };
            (node_id.clone(), values)
        })
        .collect::<BTreeMap<_, _>>();

    loop {
        let mut changed = false;
        for node_id in nodes.keys().filter(|node_id| node_id.as_str() != entry) {
            let predecessors = &predecessors[node_id];
            let mut updated = all.clone();
            for predecessor in predecessors {
                updated = updated
                    .intersection(&dominators[predecessor])
                    .cloned()
                    .collect();
            }
            updated.insert(node_id.clone());
            if updated != dominators[node_id] {
                dominators.insert(node_id.clone(), updated);
                changed = true;
            }
        }
        if !changed {
            break;
        }
    }

    for (node_id, node) in nodes {
        for reference in &node.references {
            if let Some(reference_region) = plan.node_regions.get(reference) {
                match (&plan.node_regions[node_id], reference_region) {
                    (
                        NodeRegion::Branch { fork_id, branch_id },
                        NodeRegion::Branch {
                            fork_id: other_fork,
                            branch_id: other_branch,
                        },
                    ) if fork_id != other_fork || branch_id != other_branch => {
                        return Err(CompileError::new(
                            "CROSS_BRANCH_REFERENCE",
                            format!(
                                "node '{node_id}' cannot reference branch node '{reference}' from another branch"
                            ),
                        ));
                    }
                    (NodeRegion::Linear | NodeRegion::Join { .. }, NodeRegion::Branch { .. }) => {
                        return Err(CompileError::new(
                            "POST_JOIN_BRANCH_REFERENCE",
                            format!(
                                "node '{node_id}' must reference joined output instead of branch node '{reference}'"
                            ),
                        ));
                    }
                    _ => {}
                }
            }
            if reference == node_id
                || !nodes.contains_key(reference)
                || !dominators[node_id].contains(reference)
            {
                return Err(CompileError::new(
                    "INVALID_NODE_REFERENCE",
                    format!(
                        "node '{node_id}' references '{reference}', which is not completed on every incoming path"
                    ),
                ));
            }
        }
    }
    Ok(())
}
