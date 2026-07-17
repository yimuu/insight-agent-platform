use std::{collections::BTreeMap, collections::BTreeSet, fmt};

use semver::Version;
use serde_json::Value;

use crate::{dsl::DslPath, resources::models::ModelCapability};

use super::{
    ir::{TypedContract, ValueId},
    shape::DynamicMessageShapeProof,
    template::CompiledTemplate,
    types::ValueType,
    value::{Identifier, LocalInputPath},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CallTarget {
    AiChat,
    ActionCall,
}

impl CallTarget {
    pub fn operation_type(self) -> &'static str {
        match self {
            Self::AiChat => "ai.chat",
            Self::ActionCall => "action.call",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum CallPlan {
    Llm(CompiledLlmPlan),
    Action(CompiledActionPlan),
}

impl CallPlan {
    pub fn target(&self) -> CallTarget {
        match self {
            Self::Llm(_) => CallTarget::AiChat,
            Self::Action(_) => CallTarget::ActionCall,
        }
    }

    pub fn output_contract(&self) -> &TypedContract {
        match self {
            Self::Llm(plan) => &plan.output_contract,
            Self::Action(plan) => &plan.output_contract,
        }
    }

    /// Every SSA value the verified executor may read from the current Region.
    ///
    /// The scheduler must evaluate exactly this set instead of exposing the
    /// Region value map or smuggling compiler-generated projections into
    /// authored `Call.inputs`.
    pub fn dependencies(&self) -> BTreeSet<ValueId> {
        let mut dependencies = BTreeSet::new();
        match self {
            Self::Llm(plan) => {
                dependencies.extend(plan.local_inputs.values().cloned());
                for source in &plan.message_sources {
                    match source {
                        MessageSourcePlan::Authored { content, .. } => {
                            for atom in content {
                                match atom {
                                    CompiledContentAtom::Template { bindings, .. } => {
                                        dependencies.extend(bindings.values().cloned());
                                    }
                                    CompiledContentAtom::RuntimeText { value }
                                    | CompiledContentAtom::Image { value } => {
                                        dependencies.insert(value.clone());
                                    }
                                }
                            }
                        }
                        MessageSourcePlan::Dynamic { value, .. } => {
                            dependencies.insert(value.clone());
                        }
                    }
                }
            }
            Self::Action(plan) => {
                dependencies.insert(plan.input_object.clone());
            }
        }
        dependencies
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ResolvedModelId(String);

impl ResolvedModelId {
    pub fn parse(value: impl Into<String>) -> Result<Self, String> {
        let value = value.into();
        if value.is_empty() || value.trim() != value || value.len() > 256 {
            return Err("resolved model id must be a non-empty canonical string".to_string());
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ResolvedModelId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CompiledTemplateId(String);

impl CompiledTemplateId {
    pub fn catalog(prompt: &Identifier) -> Self {
        Self(format!("catalog:{prompt}"))
    }

    pub fn inline(path: &DslPath, ordinal: usize) -> Self {
        Self(format!("inline:{path}:{ordinal}"))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TemplateProfileVersion {
    V1,
}

impl TemplateProfileVersion {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::V1 => "llm-template/v1",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TemplateProvenance {
    Catalog {
        prompt_id: Identifier,
        asset_hash: String,
    },
    Inline {
        dsl_path: DslPath,
        source_hash: String,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct PlannedTemplate {
    pub provenance: TemplateProvenance,
    pub compiled: CompiledTemplate,
    /// Exact statically verified binding types, keyed by syntactic root slot.
    pub slot_signature: BTreeMap<Identifier, ValueType>,
    pub profile_version: TemplateProfileVersion,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ValidatedModelParameters(Value);

impl ValidatedModelParameters {
    pub fn new(value: Value) -> Result<Self, String> {
        if value.is_object() {
            Ok(Self(value))
        } else {
            Err("validated model parameters must be an object".to_string())
        }
    }

    pub fn value(&self) -> &Value {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ValidatedResponseContract {
    Text,
    Json { data: TypedContract },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResolvedRequestLimits {
    pub max_messages: usize,
    pub max_message_bytes: usize,
    pub max_image_url_bytes: usize,
    pub max_request_bytes: usize,
    pub max_template_context_bytes: usize,
    pub max_template_output_bytes: usize,
}

pub const ABSOLUTE_MAX_MESSAGES: usize = 128;
pub const ABSOLUTE_MAX_MESSAGE_BYTES: usize = 65_536;
pub const ABSOLUTE_MAX_IMAGE_URL_BYTES: usize = 32_768;
pub const ABSOLUTE_MAX_REQUEST_BYTES: usize = 262_144;
pub const ABSOLUTE_MAX_TEMPLATE_CONTEXT_BYTES: usize = 262_144;
pub const ABSOLUTE_MAX_TEMPLATE_OUTPUT_BYTES: usize = 262_144;

impl ResolvedRequestLimits {
    pub fn new(
        max_messages: usize,
        max_message_bytes: usize,
        max_image_url_bytes: usize,
        max_request_bytes: usize,
        max_template_context_bytes: usize,
        max_template_output_bytes: usize,
    ) -> Result<Self, &'static str> {
        let limits = Self {
            max_messages,
            max_message_bytes,
            max_image_url_bytes,
            max_request_bytes,
            max_template_context_bytes,
            max_template_output_bytes,
        };
        if limits.is_valid() {
            Ok(limits)
        } else {
            Err("resolved request limits exceed the frozen platform profile")
        }
    }

    pub fn is_valid(self) -> bool {
        self.max_messages > 0
            && self.max_messages <= ABSOLUTE_MAX_MESSAGES
            && self.max_message_bytes > 0
            && self.max_message_bytes <= ABSOLUTE_MAX_MESSAGE_BYTES
            && self.max_image_url_bytes > 0
            && self.max_image_url_bytes <= ABSOLUTE_MAX_IMAGE_URL_BYTES
            && self.max_request_bytes > 0
            && self.max_request_bytes <= ABSOLUTE_MAX_REQUEST_BYTES
            && self.max_template_context_bytes > 0
            && self.max_template_context_bytes <= ABSOLUTE_MAX_TEMPLATE_CONTEXT_BYTES
            && self.max_template_output_bytes > 0
            && self.max_template_output_bytes <= ABSOLUTE_MAX_TEMPLATE_OUTPUT_BYTES
            && self.max_message_bytes <= self.max_request_bytes
            && self.max_image_url_bytes <= self.max_request_bytes
            && self.max_template_output_bytes <= self.max_request_bytes
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct CompiledLlmPlan {
    pub model: ResolvedModelId,
    pub local_inputs: BTreeMap<Identifier, ValueId>,
    pub message_sources: Vec<MessageSourcePlan>,
    pub templates: BTreeMap<CompiledTemplateId, PlannedTemplate>,
    pub parameters: ValidatedModelParameters,
    pub response: ValidatedResponseContract,
    pub output_contract: TypedContract,
    pub capabilities: BTreeSet<ModelCapability>,
    pub limits: ResolvedRequestLimits,
}

#[derive(Debug, Clone, PartialEq)]
pub enum MessageSourcePlan {
    Authored {
        role: PlannedRole,
        content: Vec<CompiledContentAtom>,
    },
    Dynamic {
        source: LocalInputPath,
        value: ValueId,
        proven_shape: DynamicMessageShapeProof,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlannedRole {
    System,
    User,
    Assistant,
}

#[derive(Debug, Clone, PartialEq)]
pub enum CompiledContentAtom {
    Template {
        template_id: CompiledTemplateId,
        bindings: BTreeMap<Identifier, ValueId>,
    },
    RuntimeText {
        value: ValueId,
    },
    Image {
        value: ValueId,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct CompiledActionPlan {
    pub action_id: String,
    pub descriptor_version: Version,
    pub descriptor_hash: String,
    pub input_object: ValueId,
    pub input_contract: TypedContract,
    pub output_contract: TypedContract,
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use serde_json::json;

    use super::*;
    use crate::dsl::vnext::{ir::ValueId, types::ValueType};

    fn value(path: &str) -> ValueId {
        ValueId::output(path).expect("test path is valid")
    }

    fn contract(value_type: ValueType) -> TypedContract {
        TypedContract {
            schema: json!({}),
            value_type,
        }
    }

    #[test]
    fn call_plan_target_output_and_dependencies_are_exhaustive() {
        let input = value("/workflow/answer");
        let projection = value("/workflow/answer/project");
        let plan = CallPlan::Llm(CompiledLlmPlan {
            model: ResolvedModelId::parse("general_chat").unwrap(),
            local_inputs: BTreeMap::from([(Identifier::parse("question").unwrap(), input.clone())]),
            message_sources: vec![MessageSourcePlan::Authored {
                role: PlannedRole::User,
                content: vec![CompiledContentAtom::RuntimeText {
                    value: projection.clone(),
                }],
            }],
            templates: BTreeMap::new(),
            parameters: ValidatedModelParameters::new(json!({})).unwrap(),
            response: ValidatedResponseContract::Text,
            output_contract: contract(ValueType::String),
            capabilities: BTreeSet::new(),
            limits: ResolvedRequestLimits {
                max_messages: 10,
                max_message_bytes: 1_024,
                max_image_url_bytes: 1_024,
                max_request_bytes: 4_096,
                max_template_context_bytes: 4_096,
                max_template_output_bytes: 1_024,
            },
        });
        assert_eq!(plan.target(), CallTarget::AiChat);
        assert_eq!(plan.target().operation_type(), "ai.chat");
        assert_eq!(plan.dependencies(), BTreeSet::from([input, projection]));
    }

    #[test]
    fn action_plan_uses_only_its_constructed_object_dependency() {
        let input = value("/workflow/fetch/input");
        let plan = CallPlan::Action(CompiledActionPlan {
            action_id: "http_get".to_string(),
            descriptor_version: Version::parse("1.0.0").unwrap(),
            descriptor_hash: "00".repeat(32),
            input_object: input.clone(),
            input_contract: contract(ValueType::Any),
            output_contract: contract(ValueType::Any),
        });
        assert_eq!(plan.target(), CallTarget::ActionCall);
        assert_eq!(plan.dependencies(), BTreeSet::from([input]));
    }

    #[test]
    fn resolved_request_limits_enforce_every_frozen_absolute_ceiling() {
        let exact = ResolvedRequestLimits::new(
            ABSOLUTE_MAX_MESSAGES,
            ABSOLUTE_MAX_MESSAGE_BYTES,
            ABSOLUTE_MAX_IMAGE_URL_BYTES,
            ABSOLUTE_MAX_REQUEST_BYTES,
            ABSOLUTE_MAX_TEMPLATE_CONTEXT_BYTES,
            ABSOLUTE_MAX_TEMPLATE_OUTPUT_BYTES,
        )
        .unwrap();
        assert!(exact.is_valid());

        for invalid in [
            ResolvedRequestLimits {
                max_messages: ABSOLUTE_MAX_MESSAGES + 1,
                ..exact
            },
            ResolvedRequestLimits {
                max_message_bytes: ABSOLUTE_MAX_MESSAGE_BYTES + 1,
                ..exact
            },
            ResolvedRequestLimits {
                max_image_url_bytes: ABSOLUTE_MAX_IMAGE_URL_BYTES + 1,
                ..exact
            },
            ResolvedRequestLimits {
                max_request_bytes: ABSOLUTE_MAX_REQUEST_BYTES + 1,
                ..exact
            },
            ResolvedRequestLimits {
                max_template_context_bytes: ABSOLUTE_MAX_TEMPLATE_CONTEXT_BYTES + 1,
                ..exact
            },
            ResolvedRequestLimits {
                max_template_output_bytes: ABSOLUTE_MAX_TEMPLATE_OUTPUT_BYTES + 1,
                ..exact
            },
        ] {
            assert!(!invalid.is_valid());
            assert!(ResolvedRequestLimits::new(
                invalid.max_messages,
                invalid.max_message_bytes,
                invalid.max_image_url_bytes,
                invalid.max_request_bytes,
                invalid.max_template_context_bytes,
                invalid.max_template_output_bytes,
            )
            .is_err());
        }
    }
}
