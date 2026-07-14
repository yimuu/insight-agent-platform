use std::collections::{BTreeMap, BTreeSet};

use super::{
    compiled::{CompiledNode, ExecutionPlan, NodeControl},
    CompileError,
};

pub(crate) fn validate_selects(
    nodes: &BTreeMap<String, CompiledNode>,
    plan: &ExecutionPlan,
) -> Result<(), CompileError> {
    let predecessors = node_predecessors(nodes);

    for (select_id, node) in nodes {
        let NodeControl::Select { sources } = &node.control else {
            continue;
        };
        if sources.len() < 2 {
            return Err(CompileError::new(
                "SELECT_SOURCE_COUNT_INVALID",
                format!("select node '{select_id}' must define at least two sources"),
            ));
        }
        if sources.contains(select_id) {
            return Err(CompileError::new(
                "SELECT_SOURCE_ID_INVALID",
                format!("select node '{select_id}' cannot select itself"),
            ));
        }
        for source in sources {
            if !nodes.contains_key(source) {
                return Err(CompileError::new(
                    "SELECT_SOURCE_NOT_FOUND",
                    format!("select node '{select_id}' declares missing source '{source}'"),
                ));
            }
        }

        if &predecessors[select_id] != sources {
            return Err(CompileError::new(
                "SELECT_PREDECESSOR_MISMATCH",
                format!(
                    "select node '{select_id}' sources must exactly match its direct predecessors"
                ),
            ));
        }

        let select_region = &plan.node_regions[select_id];
        for source in sources {
            if plan.node_regions.get(source) != Some(select_region) {
                return Err(CompileError::new(
                    "SELECT_REGION_INVALID",
                    format!(
                        "select node '{select_id}' and source '{source}' must share one execution region"
                    ),
                ));
            }
        }

        let sources = sources.iter().collect::<Vec<_>>();
        for (index, left) in sources.iter().enumerate() {
            for right in sources.iter().skip(index + 1) {
                if is_reachable(left, right, nodes) || is_reachable(right, left, nodes) {
                    return Err(CompileError::new(
                        "SELECT_SOURCES_NOT_EXCLUSIVE",
                        format!(
                            "select node '{select_id}' sources '{left}' and '{right}' are connected by a path"
                        ),
                    ));
                }
            }
        }
    }

    Ok(())
}

fn node_predecessors(nodes: &BTreeMap<String, CompiledNode>) -> BTreeMap<String, BTreeSet<String>> {
    let mut predecessors = nodes
        .keys()
        .map(|node_id| (node_id.clone(), BTreeSet::new()))
        .collect::<BTreeMap<_, _>>();
    for (node_id, node) in nodes {
        for edge in node.edges.iter().filter(|edge| edge.is_direct_executable()) {
            predecessors
                .get_mut(edge.target())
                .expect("graph edges were validated before Select validation")
                .insert(node_id.clone());
        }
    }
    predecessors
}

fn is_reachable(from: &str, target: &str, nodes: &BTreeMap<String, CompiledNode>) -> bool {
    let mut visited = BTreeSet::new();
    let mut pending = nodes[from]
        .direct_executable_targets()
        .map(str::to_string)
        .collect::<Vec<_>>();
    while let Some(node_id) = pending.pop() {
        if node_id == target {
            return true;
        }
        if visited.insert(node_id.clone()) {
            pending.extend(
                nodes[&node_id]
                    .direct_executable_targets()
                    .map(str::to_string),
            );
        }
    }
    false
}
