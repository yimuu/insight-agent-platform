//! Published Program interpreter compatibility catalog.
//!
//! This catalog names the interpreter semantics already associated with the
//! released IR. It is independent of process/build identity. An unknown IR is an
//! upgrade error, never an instruction to substitute the installed worker.

use insight_platform_contracts::{
    canonical_digest, ExecutionRequirement, Sha256Digest, WorkerExecutionCapability,
};

pub const PROGRAM_IR_ABI_V6: u32 = 6;
pub const PROGRAM_SEMANTIC_PROFILE_V6: &str =
    "insight.platform/program-interpreter/ir-v6/semantics-v3";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UnsupportedProgramAbi(pub u32);

pub fn program_semantic_identity(
    ir_abi_version: u32,
) -> Result<Sha256Digest, UnsupportedProgramAbi> {
    let profile = match ir_abi_version {
        PROGRAM_IR_ABI_V6 => PROGRAM_SEMANTIC_PROFILE_V6,
        other => return Err(UnsupportedProgramAbi(other)),
    };
    let descriptor = serde_json::json!({"contract":"insight.platform/program-semantic-identity","identity_version":1,"profile":profile,"ir_abi_version":ir_abi_version,
        "frozen_schema_documents_abi":1,"internal_value_schema_profile":insight_platform_contracts::CLOSED_VALUE_SCHEMA_PROFILE_ID,"human_task_typed_eligibility_abi":1,"human_task_frozen_response_schema_abi":1,"orchestration_job_payload_abi":2,"external_leaf_success_same_node_structural_resume_abi":1,"bounded_run_convergence_and_parent_control_abi":1,"child_budget_admission_anchor_abi":1,"model_loop_zero_tool_budget_abi":1,"model_node_response_schema_abi":1});
    Ok(canonical_digest(&descriptor)
        .expect("static semantic catalog is canonical JSON")
        .parse()
        .expect("canonical SHA-256"))
}

pub fn program_execution_requirement(
    definition_digest: Sha256Digest,
    ir_abi_version: u32,
) -> Result<ExecutionRequirement, UnsupportedProgramAbi> {
    Ok(ExecutionRequirement::Program {
        definition_digest,
        program_semantic_identity: program_semantic_identity(ir_abi_version)?,
        ir_abi_version,
    })
}

pub fn program_execution_capability(
    ir_abi_version: u32,
) -> Result<WorkerExecutionCapability, UnsupportedProgramAbi> {
    Ok(WorkerExecutionCapability::Program {
        program_semantic_identity: program_semantic_identity(ir_abi_version)?,
        ir_abi_version,
    })
}

/// Exact supported reader set for the interpreter in this release. Deployment
/// validation separately bounds all live release generations.
pub fn program_execution_capabilities() -> insight_platform_contracts::WorkerExecutionCapabilities {
    insight_platform_contracts::WorkerExecutionCapabilities {
        schema_version: insight_platform_contracts::EXECUTION_REQUIREMENT_VERSION,
        capabilities: [PROGRAM_IR_ABI_V6]
            .into_iter()
            .map(|abi| program_execution_capability(abi).expect("closed catalog ABI"))
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn prior_tool_required_semantics_cannot_claim_zero_tool_plans() {
        let old_descriptor = serde_json::json!({"contract":"insight.platform/program-semantic-identity","identity_version":1,
            "profile":"insight.platform/program-interpreter/ir-v6/semantics-v1","ir_abi_version":6,
            "frozen_schema_documents_abi":1,"internal_value_schema_profile":insight_platform_contracts::CLOSED_VALUE_SCHEMA_PROFILE_ID,
            "human_task_typed_eligibility_abi":1,"human_task_frozen_response_schema_abi":1,"orchestration_job_payload_abi":2,
            "external_leaf_success_same_node_structural_resume_abi":1,"bounded_run_convergence_and_parent_control_abi":1,"child_budget_admission_anchor_abi":1});
        let old = WorkerExecutionCapability::Program {
            program_semantic_identity: canonical_digest(&old_descriptor).unwrap().parse().unwrap(),
            ir_abi_version: 6,
        };
        let requirement =
            program_execution_requirement(format!("sha256:{}", "a".repeat(64)).parse().unwrap(), 6)
                .unwrap();
        assert!(!old.supports(&requirement));
        assert!(program_execution_capability(6)
            .unwrap()
            .supports(&requirement));
    }

    #[test]
    fn catalog_requires_an_explicitly_known_ir() {
        assert!(program_execution_capability(0).is_err());
        assert!(program_execution_capability(5).is_err());
        assert!(program_execution_capability(PROGRAM_IR_ABI_V6 + 1).is_err());
        let requirement = program_execution_requirement(
            format!("sha256:{}", "a".repeat(64)).parse().unwrap(),
            PROGRAM_IR_ABI_V6,
        )
        .unwrap();
        assert!(program_execution_capability(PROGRAM_IR_ABI_V6)
            .unwrap()
            .supports(&requirement));
    }
}
