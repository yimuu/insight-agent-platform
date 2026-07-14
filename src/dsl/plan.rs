use std::collections::{BTreeMap, BTreeSet};

use super::{
    compiled::{BranchPlan, CompiledNode, ExecutionPlan, ForkPlan, NodeControl, NodeRegion},
    compiler::CompileLimits,
    CompileError,
};

#[derive(Debug)]
struct ForkTopology<'a> {
    join_id: &'a str,
    branches: BTreeMap<String, BranchPlan>,
}

pub fn compile_execution_plan(
    entry: &str,
    nodes: &BTreeMap<String, CompiledNode>,
    limits: CompileLimits,
) -> Result<ExecutionPlan, CompileError> {
    let join_claims = validate_fork_declarations(nodes, limits)?;
    let topologies = collect_fork_topologies(nodes)?;
    let branch_owners = validate_branch_regions(nodes, &join_claims, &topologies)?;
    validate_join_predecessors(nodes, &topologies, &branch_owners)?;

    let mut plan = ExecutionPlan::sequential(entry, nodes.keys().cloned());
    for (node_id, (fork_id, branch_id)) in branch_owners {
        plan.node_regions
            .insert(node_id, NodeRegion::Branch { fork_id, branch_id });
    }
    for (join_id, fork_id) in join_claims {
        plan.node_regions
            .insert(join_id, NodeRegion::Join { fork_id });
    }
    for (fork_id, topology) in topologies {
        let NodeControl::Join { policy } = &nodes[topology.join_id].control else {
            unreachable!("fork declarations were validated before plan construction");
        };
        plan.forks.insert(
            fork_id.clone(),
            ForkPlan {
                fork_id,
                join_id: topology.join_id.to_string(),
                branches: topology.branches,
                policy: *policy,
            },
        );
    }

    Ok(plan)
}

fn validate_fork_declarations(
    nodes: &BTreeMap<String, CompiledNode>,
    limits: CompileLimits,
) -> Result<BTreeMap<String, String>, CompileError> {
    let mut join_claims = BTreeMap::new();

    for (fork_id, node) in nodes {
        let NodeControl::Fork { branches, join } = &node.control else {
            continue;
        };
        if branches.len() < 2 {
            return Err(CompileError::new(
                "FORK_BRANCH_COUNT_INVALID",
                format!("fork node '{fork_id}' must define at least two branches"),
            ));
        }
        if branches.len() > limits.max_fork_branches {
            return Err(CompileError::new(
                "FORK_BRANCH_LIMIT_EXCEEDED",
                format!(
                    "fork node '{fork_id}' defines {} branches, exceeding the configured limit of {}",
                    branches.len(),
                    limits.max_fork_branches
                ),
            ));
        }

        let Some(join_node) = nodes.get(join) else {
            return Err(CompileError::new(
                "FORK_JOIN_NOT_FOUND",
                format!("fork node '{fork_id}' declares missing join node '{join}'"),
            ));
        };
        if !matches!(join_node.control, NodeControl::Join { .. }) {
            return Err(CompileError::new(
                "FORK_JOIN_KIND_INVALID",
                format!("fork node '{fork_id}' target '{join}' is not a join node"),
            ));
        }
        if let Some(first_fork) = join_claims.insert(join.clone(), fork_id.clone()) {
            return Err(CompileError::new(
                "JOIN_PAIRING_INVALID",
                format!(
                    "join node '{join}' is claimed by both fork '{first_fork}' and fork '{fork_id}'"
                ),
            ));
        }
    }

    if let Some(join_id) = nodes.iter().find_map(|(node_id, node)| {
        matches!(node.control, NodeControl::Join { .. })
            .then_some(node_id)
            .filter(|node_id| !join_claims.contains_key(*node_id))
    }) {
        return Err(CompileError::new(
            "JOIN_PAIRING_INVALID",
            format!("join node '{join_id}' is not paired with a fork node"),
        ));
    }

    Ok(join_claims)
}

fn collect_fork_topologies<'a>(
    nodes: &'a BTreeMap<String, CompiledNode>,
) -> Result<BTreeMap<String, ForkTopology<'a>>, CompileError> {
    let mut topologies = BTreeMap::new();

    for (fork_id, node) in nodes {
        let NodeControl::Fork { branches, join } = &node.control else {
            continue;
        };
        let mut branch_plans = BTreeMap::new();
        for (branch_id, branch_entry) in branches {
            let branch_nodes = collect_branch_nodes(fork_id, branch_id, branch_entry, join, nodes)?;
            branch_plans.insert(
                branch_id.clone(),
                BranchPlan {
                    branch_id: branch_id.clone(),
                    entry: branch_entry.clone(),
                    nodes: branch_nodes,
                },
            );
        }
        validate_sibling_regions(fork_id, &branch_plans, nodes)?;
        topologies.insert(
            fork_id.clone(),
            ForkTopology {
                join_id: join,
                branches: branch_plans,
            },
        );
    }

    Ok(topologies)
}

fn collect_branch_nodes(
    fork_id: &str,
    branch_id: &str,
    branch_entry: &str,
    join_id: &str,
    nodes: &BTreeMap<String, CompiledNode>,
) -> Result<BTreeSet<String>, CompileError> {
    let mut visited = BTreeSet::new();
    let mut pending = vec![branch_entry];

    while let Some(node_id) = pending.pop() {
        if node_id == join_id || !visited.insert(node_id.to_string()) {
            continue;
        }
        let node = nodes
            .get(node_id)
            .expect("graph edges were validated before plan construction");
        if matches!(node.control, NodeControl::Fork { .. }) {
            return Err(CompileError::new(
                "BRANCH_NESTED_FORK",
                format!(
                    "fork node '{fork_id}' branch '{branch_id}' contains nested fork '{node_id}'"
                ),
            ));
        }
        if matches!(node.control, NodeControl::End { .. }) || node.edges.is_empty() {
            return Err(CompileError::new(
                "BRANCH_PATH_MISSING_JOIN",
                format!(
                    "fork node '{fork_id}' branch '{branch_id}' has a path ending at '{node_id}' before join '{join_id}'"
                ),
            ));
        }
        pending.extend(node.edges.iter().map(String::as_str));
    }

    Ok(visited)
}

fn validate_sibling_regions(
    fork_id: &str,
    branches: &BTreeMap<String, BranchPlan>,
    nodes: &BTreeMap<String, CompiledNode>,
) -> Result<(), CompileError> {
    let branches = branches.values().collect::<Vec<_>>();
    for (index, left) in branches.iter().enumerate() {
        for right in branches.iter().skip(index + 1) {
            if left.entry == right.entry {
                return Err(CompileError::new(
                    "BRANCH_REGION_OVERLAP",
                    format!(
                        "fork node '{fork_id}' branches '{}' and '{}' share entry node '{}'",
                        left.branch_id, right.branch_id, left.entry
                    ),
                ));
            }
            if branch_has_edge_into(left, right, nodes)
                || branch_has_edge_into(right, left, nodes)
                || left.nodes.contains(&right.entry)
                || right.nodes.contains(&left.entry)
            {
                return Err(CompileError::new(
                    "BRANCH_CROSS_REGION_EDGE",
                    format!(
                        "fork node '{fork_id}' has an edge crossing branches '{}' and '{}'",
                        left.branch_id, right.branch_id
                    ),
                ));
            }
            if !left.nodes.is_disjoint(&right.nodes) {
                return Err(CompileError::new(
                    "BRANCH_REGION_OVERLAP",
                    format!(
                        "fork node '{fork_id}' branches '{}' and '{}' have overlapping regions",
                        left.branch_id, right.branch_id
                    ),
                ));
            }
        }
    }
    Ok(())
}

fn branch_has_edge_into(
    source: &BranchPlan,
    target: &BranchPlan,
    nodes: &BTreeMap<String, CompiledNode>,
) -> bool {
    source.nodes.difference(&target.nodes).any(|node_id| {
        nodes[node_id]
            .edges
            .iter()
            .any(|edge| target.nodes.contains(edge))
    })
}

fn validate_branch_regions(
    nodes: &BTreeMap<String, CompiledNode>,
    join_claims: &BTreeMap<String, String>,
    topologies: &BTreeMap<String, ForkTopology<'_>>,
) -> Result<BTreeMap<String, (String, String)>, CompileError> {
    let mut owners = BTreeMap::new();

    for (fork_id, topology) in topologies {
        for (branch_id, branch) in &topology.branches {
            for node_id in &branch.nodes {
                if let Some(other_fork) = join_claims.get(node_id) {
                    return Err(CompileError::new(
                        "BRANCH_CROSS_REGION_EDGE",
                        format!(
                            "fork node '{fork_id}' branch '{branch_id}' enters join '{node_id}' owned by fork '{other_fork}'"
                        ),
                    ));
                }
                if let Some((other_fork, other_branch)) =
                    owners.insert(node_id.clone(), (fork_id.clone(), branch_id.clone()))
                {
                    return Err(CompileError::new(
                        "BRANCH_REGION_OVERLAP",
                        format!(
                            "node '{node_id}' belongs to both '{other_fork}.{other_branch}' and '{fork_id}.{branch_id}'"
                        ),
                    ));
                }
            }
        }
    }

    let predecessors = node_predecessors(nodes);
    for (node_id, (fork_id, branch_id)) in &owners {
        let branch_entry = &topologies[fork_id].branches[branch_id].entry;
        for predecessor in &predecessors[node_id.as_str()] {
            let same_branch = branch_owners_match(&owners, predecessor, fork_id, branch_id);
            let owning_fork_enters_branch = node_id == branch_entry && *predecessor == fork_id;
            if !same_branch && !owning_fork_enters_branch {
                return Err(CompileError::new(
                    "BRANCH_CROSS_REGION_EDGE",
                    format!(
                        "fork node '{fork_id}' branch '{branch_id}' has an incoming edge from '{predecessor}' to '{node_id}'"
                    ),
                ));
            }
        }
    }

    for (node_id, (fork_id, branch_id)) in &owners {
        for edge in &nodes[node_id].edges {
            if edge == topologies[fork_id].join_id {
                continue;
            }
            if owners.get(edge) != Some(&(fork_id.clone(), branch_id.clone())) {
                return Err(CompileError::new(
                    "BRANCH_CROSS_REGION_EDGE",
                    format!(
                        "fork node '{fork_id}' branch '{branch_id}' has an edge from '{node_id}' outside its region to '{edge}'"
                    ),
                ));
            }
        }
    }

    Ok(owners)
}

fn branch_owners_match(
    owners: &BTreeMap<String, (String, String)>,
    node_id: &str,
    fork_id: &str,
    branch_id: &str,
) -> bool {
    owners
        .get(node_id)
        .is_some_and(|(owner_fork, owner_branch)| {
            owner_fork == fork_id && owner_branch == branch_id
        })
}

fn validate_join_predecessors(
    nodes: &BTreeMap<String, CompiledNode>,
    topologies: &BTreeMap<String, ForkTopology<'_>>,
    branch_owners: &BTreeMap<String, (String, String)>,
) -> Result<(), CompileError> {
    let predecessors = node_predecessors(nodes);

    for (fork_id, topology) in topologies {
        let mut contributing_branches = BTreeSet::new();
        for predecessor in &predecessors[topology.join_id] {
            let Some((owner_fork, owner_branch)) = branch_owners.get(*predecessor) else {
                return Err(invalid_join_predecessor(
                    fork_id,
                    topology.join_id,
                    predecessor,
                ));
            };
            if owner_fork != fork_id {
                return Err(invalid_join_predecessor(
                    fork_id,
                    topology.join_id,
                    predecessor,
                ));
            }
            contributing_branches.insert(owner_branch.as_str());
        }
        if contributing_branches.len() != topology.branches.len() {
            return Err(CompileError::new(
                "JOIN_PREDECESSOR_INVALID",
                format!(
                    "join node '{}' does not have a predecessor from every branch of fork '{fork_id}'",
                    topology.join_id
                ),
            ));
        }
    }

    Ok(())
}

fn node_predecessors(nodes: &BTreeMap<String, CompiledNode>) -> BTreeMap<&str, BTreeSet<&str>> {
    let mut predecessors = nodes
        .keys()
        .map(|node_id| (node_id.as_str(), BTreeSet::new()))
        .collect::<BTreeMap<_, _>>();
    for (node_id, node) in nodes {
        for edge in &node.edges {
            predecessors
                .get_mut(edge.as_str())
                .expect("graph edges were validated before plan construction")
                .insert(node_id.as_str());
        }
    }
    predecessors
}

fn invalid_join_predecessor(fork_id: &str, join_id: &str, predecessor: &str) -> CompileError {
    CompileError::new(
        "JOIN_PREDECESSOR_INVALID",
        format!(
            "join node '{join_id}' for fork '{fork_id}' has predecessor '{predecessor}' outside its branches"
        ),
    )
}
