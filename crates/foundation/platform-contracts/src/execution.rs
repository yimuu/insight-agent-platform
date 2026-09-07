//! Execution compatibility is independent of deployment build identity.
//!
//! These types own the requirements frozen by a domain when it creates work. Worker
//! capabilities select compatible work; they never rewrite an admitted requirement.

use crate::{canonical_digest, Sha256Digest};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeSet, error::Error, fmt};

pub const EXECUTION_REQUIREMENT_VERSION: u32 = 1;
pub const MAX_LIVE_PROGRAM_SEMANTICS: usize = 1;
pub const MAX_PAYLOAD_READER_GENERATIONS: usize = 1;
pub const MAX_WORKER_EXECUTION_CAPABILITIES: usize = 128;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "family", rename_all = "snake_case", deny_unknown_fields)]
pub enum ExecutionRequirement {
    Program {
        definition_digest: Sha256Digest,
        program_semantic_identity: Sha256Digest,
        ir_abi_version: u32,
    },
    AgentCompilation {
        validation_input_digest: Sha256Digest,
        compiler_semantic_identity: Sha256Digest,
        compile_policy_inputs_digest: Sha256Digest,
    },
    DomainOperation {
        operation_abi_identity: Sha256Digest,
        requirements: DomainOperationRequirements,
    },
}

/// Pure validators need policy inputs; physical adapters additionally bind their protocol.
/// An inapplicable protocol is a different variant, not a fabricated digest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum DomainOperationRequirements {
    Validator {
        validation_policy_digest: Sha256Digest,
    },
    Adapter {
        protocol_adapter_identity: Sha256Digest,
        policy_inputs_digest: Sha256Digest,
    },
    Control {
        control_policy_digest: Sha256Digest,
    },
}

impl ExecutionRequirement {
    pub fn validate(&self) -> Result<(), ExecutionCompatibilityError> {
        if matches!(
            self,
            Self::Program {
                ir_abi_version: 0,
                ..
            }
        ) {
            return Err(ExecutionCompatibilityError::InvalidRequirement);
        }
        Ok(())
    }

    pub const fn family(&self) -> &'static str {
        match self {
            Self::Program { .. } => "program",
            Self::AgentCompilation { .. } => "agent_compilation",
            Self::DomainOperation { .. } => "domain_operation",
        }
    }

    pub fn canonical_digest(&self) -> Result<Sha256Digest, ExecutionCompatibilityError> {
        self.validate()?;
        canonical_digest(&serde_json::json!({
            "schema_version": EXECUTION_REQUIREMENT_VERSION,
            "requirement": self,
        }))
        .map_err(|_| ExecutionCompatibilityError::InvalidRequirement)?
        .parse()
        .map_err(|_| ExecutionCompatibilityError::InvalidRequirement)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "family", rename_all = "snake_case", deny_unknown_fields)]
pub enum WorkerExecutionCapability {
    Program {
        program_semantic_identity: Sha256Digest,
        ir_abi_version: u32,
    },
    AgentCompilation {
        compiler_semantic_identity: Sha256Digest,
    },
    DomainOperation {
        operation_abi_identity: Sha256Digest,
        adapter: Option<Sha256Digest>,
    },
}

impl WorkerExecutionCapability {
    pub fn supports(&self, requirement: &ExecutionRequirement) -> bool {
        if requirement.validate().is_err() {
            return false;
        }
        match (self, requirement) {
            (
                Self::Program {
                    program_semantic_identity: supported,
                    ir_abi_version: abi,
                },
                ExecutionRequirement::Program {
                    program_semantic_identity: required,
                    ir_abi_version,
                    ..
                },
            ) => supported == required && abi == ir_abi_version && *abi != 0,
            (
                Self::AgentCompilation {
                    compiler_semantic_identity: supported,
                },
                ExecutionRequirement::AgentCompilation {
                    compiler_semantic_identity: required,
                    ..
                },
            ) => supported == required,
            (
                Self::DomainOperation {
                    operation_abi_identity: supported,
                    adapter,
                },
                ExecutionRequirement::DomainOperation {
                    operation_abi_identity: required,
                    requirements,
                },
            ) => {
                supported == required
                    && match requirements {
                        DomainOperationRequirements::Adapter {
                            protocol_adapter_identity,
                            ..
                        } => adapter.as_ref() == Some(protocol_adapter_identity),
                        DomainOperationRequirements::Validator { .. }
                        | DomainOperationRequirements::Control { .. } => adapter.is_none(),
                    }
            }
            _ => false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerExecutionCapabilities {
    pub schema_version: u32,
    pub capabilities: Vec<WorkerExecutionCapability>,
}

impl WorkerExecutionCapabilities {
    pub fn validate(&self) -> Result<(), ExecutionCompatibilityError> {
        if self.schema_version != EXECUTION_REQUIREMENT_VERSION
            || self.capabilities.is_empty()
            || self.capabilities.len() > MAX_WORKER_EXECUTION_CAPABILITIES
        {
            return Err(ExecutionCompatibilityError::InvalidCapabilities);
        }
        let mut unique = BTreeSet::new();
        let mut program_semantics = BTreeSet::new();
        for capability in &self.capabilities {
            let digest = canonical_digest(
                &serde_json::to_value(capability)
                    .map_err(|_| ExecutionCompatibilityError::InvalidCapabilities)?,
            )
            .map_err(|_| ExecutionCompatibilityError::InvalidCapabilities)?;
            if !unique.insert(digest) {
                return Err(ExecutionCompatibilityError::InvalidCapabilities);
            }
            if let WorkerExecutionCapability::Program {
                program_semantic_identity,
                ir_abi_version,
            } = capability
            {
                if *ir_abi_version == 0 {
                    return Err(ExecutionCompatibilityError::InvalidCapabilities);
                }
                program_semantics.insert(program_semantic_identity.to_string());
            }
        }
        if program_semantics.len() > MAX_LIVE_PROGRAM_SEMANTICS {
            return Err(ExecutionCompatibilityError::SemanticWindowExceeded);
        }
        Ok(())
    }

    pub fn supports(&self, requirement: &ExecutionRequirement) -> bool {
        self.validate().is_ok()
            && self
                .capabilities
                .iter()
                .any(|capability| capability.supports(requirement))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionAuthorizationPurpose {
    NewBusinessDispatch,
    ContentDisclosure,
    RestrictedCompletionReconcileCleanup,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionCompatibilityError {
    InvalidRequirement,
    InvalidCapabilities,
    SemanticWindowExceeded,
    UnsupportedRequirement,
}

impl fmt::Display for ExecutionCompatibilityError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "execution compatibility: {self:?}")
    }
}
impl Error for ExecutionCompatibilityError {}

#[cfg(test)]
mod tests {
    use super::*;
    fn digest(value: char) -> Sha256Digest {
        format!("sha256:{}", value.to_string().repeat(64))
            .parse()
            .unwrap()
    }

    #[test]
    fn compilation_support_does_not_rebind_policy_or_input() {
        let capability = WorkerExecutionCapability::AgentCompilation {
            compiler_semantic_identity: digest('1'),
        };
        let first = ExecutionRequirement::AgentCompilation {
            validation_input_digest: digest('2'),
            compiler_semantic_identity: digest('1'),
            compile_policy_inputs_digest: digest('3'),
        };
        let second = ExecutionRequirement::AgentCompilation {
            validation_input_digest: digest('2'),
            compiler_semantic_identity: digest('1'),
            compile_policy_inputs_digest: digest('4'),
        };
        assert!(capability.supports(&first) && capability.supports(&second));
        assert_ne!(
            first.canonical_digest().unwrap(),
            second.canonical_digest().unwrap()
        );
    }

    #[test]
    fn incompatible_semantics_and_adapter_protocols_fail_closed() {
        let capability = WorkerExecutionCapability::Program {
            program_semantic_identity: digest('1'),
            ir_abi_version: 5,
        };
        assert!(!capability.supports(&ExecutionRequirement::Program {
            definition_digest: digest('2'),
            program_semantic_identity: digest('3'),
            ir_abi_version: 5
        }));
        let capability = WorkerExecutionCapability::DomainOperation {
            operation_abi_identity: digest('1'),
            adapter: Some(digest('2')),
        };
        assert!(
            !capability.supports(&ExecutionRequirement::DomainOperation {
                operation_abi_identity: digest('1'),
                requirements: DomainOperationRequirements::Adapter {
                    protocol_adapter_identity: digest('3'),
                    policy_inputs_digest: digest('4')
                }
            })
        );
    }
}
