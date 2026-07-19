use std::fmt::Write as _;

use serde::Serialize;
use sha2::{Digest, Sha256};

use super::{
    ControlEdge, ControlPort, DataBinding, DataPort, Node, PhiBinding, Plan, PlanError,
    PlanInputContract, PlanType, Policy, ScopeMetadata, SemanticHash,
    PLAN_SEMANTIC_PROJECTION_VERSION, PLAN_WIRE_INVALID,
};
use crate::engine::NodeId;

const SEMANTIC_HASH_DOMAIN: &[u8] = b"insight-agent/canonical-typed-plan/semantic/v4";

#[derive(Serialize)]
struct SemanticMetadata<'a> {
    dsl_version: u32,
    entry_node_id: &'a NodeId,
    input_contract: &'a PlanInputContract,
    output_type: &'a PlanType,
    error_type: &'a PlanType,
}

#[derive(Serialize)]
struct SemanticProjection<'a> {
    projection_version: u32,
    metadata: SemanticMetadata<'a>,
    nodes: &'a [Node],
    control_ports: &'a [ControlPort],
    data_ports: &'a [DataPort],
    control_edges: &'a [ControlEdge],
    data_bindings: &'a [DataBinding],
    phi_bindings: &'a [PhiBinding],
    scopes: &'a [ScopeMetadata],
    policies: &'a [Policy],
}

pub(super) fn canonical_semantic_bytes(plan: &Plan) -> Result<Vec<u8>, PlanError> {
    // Definition revision, compiler provenance, author format, SourceMap, and
    // the stored hash are deliberately absent. All execution semantics,
    // including ordered Branch/Fork descriptors and policy values, remain.
    let projection = SemanticProjection {
        projection_version: PLAN_SEMANTIC_PROJECTION_VERSION,
        metadata: SemanticMetadata {
            dsl_version: plan.metadata.dsl_version,
            entry_node_id: &plan.metadata.entry_node_id,
            input_contract: &plan.metadata.input_contract,
            output_type: &plan.metadata.output_type,
            error_type: &plan.metadata.error_type,
        },
        nodes: &plan.nodes,
        control_ports: &plan.control_ports,
        data_ports: &plan.data_ports,
        control_edges: &plan.control_edges,
        data_bindings: &plan.data_bindings,
        phi_bindings: &plan.phi_bindings,
        scopes: &plan.scopes,
        policies: &plan.policies,
    };
    serde_jcs::to_vec(&projection).map_err(|error| {
        PlanError::new(
            PLAN_WIRE_INVALID,
            format!("failed to canonicalize Plan semantic projection: {error}"),
        )
    })
}

pub(super) fn semantic_hash_for_plan(plan: &Plan) -> Result<SemanticHash, PlanError> {
    let projection = canonical_semantic_bytes(plan)?;
    let mut hasher = Sha256::new();
    for part in [SEMANTIC_HASH_DOMAIN, projection.as_slice()] {
        let length = u64::try_from(part.len())
            .expect("supported Rust targets cannot address more than u64::MAX bytes");
        hasher.update(length.to_be_bytes());
        hasher.update(part);
    }
    let digest = hasher.finalize();
    let mut value = String::with_capacity("sha256:".len() + digest.len() * 2);
    value.push_str("sha256:");
    for byte in digest {
        write!(&mut value, "{byte:02x}").expect("writing to a String cannot fail");
    }
    Ok(SemanticHash::from_digest(value))
}
