use insight_platform_contracts::{canonical_digest, ClosedJsonValue, Sha256Digest};
use serde::{de, Deserialize, Deserializer, Serialize};
use std::{collections::BTreeSet, error::Error, fmt};

pub const MAX_EXPRESSION_INSTRUCTIONS: usize = 4_096;
pub const MAX_EXPRESSION_INPUT_PORTS: usize = 64;
pub const MAX_EXPRESSION_STACK_DEPTH: u16 = 256;
pub const MAX_EXPRESSION_FIELD_BYTES: usize = 128;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
pub struct DataPortKey(String);

impl DataPortKey {
    pub fn new(value: String) -> Result<Self, ExpressionError> {
        let bytes = value.as_bytes();
        if bytes.is_empty()
            || bytes.len() > 128
            || !bytes[0].is_ascii_alphanumeric()
            || !bytes.iter().all(|byte| {
                byte.is_ascii_alphanumeric() || matches!(*byte, b'_' | b'-' | b'.' | b':')
            })
        {
            return Err(ExpressionError::InvalidPort);
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl<'de> Deserialize<'de> for DataPortKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::new(String::deserialize(deserializer)?).map_err(de::Error::custom)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExactDataPortRef {
    pub node_id: super::PlanNodeKey,
    pub port_id: DataPortKey,
    pub schema_digest: Sha256Digest,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
pub struct ExpressionFieldName(String);

impl ExpressionFieldName {
    pub fn new(value: String) -> Result<Self, ExpressionError> {
        if value.is_empty()
            || value.len() > MAX_EXPRESSION_FIELD_BYTES
            || value.chars().any(char::is_control)
        {
            return Err(ExpressionError::InvalidFieldName);
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl<'de> Deserialize<'de> for ExpressionFieldName {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::new(String::deserialize(deserializer)?).map_err(de::Error::custom)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum TypedInstruction {
    LoadPort {
        port: ExactDataPortRef,
    },
    Literal {
        value: ClosedJsonValue,
    },
    GetField {
        field: ExpressionFieldName,
    },
    GetIndex,
    ArrayLength,
    MakeArray {
        item_count: u16,
    },
    MakeObject {
        ordered_fields: Vec<ExpressionFieldName>,
    },
    Equal,
    NotEqual,
    Less,
    LessOrEqual,
    Greater,
    GreaterOrEqual,
    BooleanAnd,
    BooleanOr,
    BooleanNot,
    IntegerAdd,
    IntegerSubtract,
    DecimalAdd,
    DecimalSubtract,
    StringConcat,
    Coalesce,
    Select,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExpressionLimits {
    pub maximum_instructions: usize,
    pub maximum_input_ports: usize,
    pub maximum_stack_depth: u16,
}

impl ExpressionLimits {
    pub const ABSOLUTE: Self = Self {
        maximum_instructions: MAX_EXPRESSION_INSTRUCTIONS,
        maximum_input_ports: MAX_EXPRESSION_INPUT_PORTS,
        maximum_stack_depth: MAX_EXPRESSION_STACK_DEPTH,
    };

    pub const fn bounded_by_absolute(self) -> bool {
        self.maximum_instructions > 0
            && self.maximum_instructions <= MAX_EXPRESSION_INSTRUCTIONS
            && self.maximum_input_ports > 0
            && self.maximum_input_ports <= MAX_EXPRESSION_INPUT_PORTS
            && self.maximum_stack_depth > 0
            && self.maximum_stack_depth <= MAX_EXPRESSION_STACK_DEPTH
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TypedExpressionProgram {
    pub expression_version: u32,
    pub input_ports: Vec<ExactDataPortRef>,
    pub instructions: Vec<TypedInstruction>,
    pub output_schema_digest: Sha256Digest,
    pub maximum_stack_depth: u16,
    pub semantic_digest: Sha256Digest,
}

impl TypedExpressionProgram {
    pub fn build(
        input_ports: Vec<ExactDataPortRef>,
        instructions: Vec<TypedInstruction>,
        output_schema_digest: Sha256Digest,
        limits: ExpressionLimits,
    ) -> Result<Self, ExpressionError> {
        let maximum_stack_depth = validate_program(&input_ports, &instructions, limits)?;
        let semantic_digest = program_digest(
            1,
            &input_ports,
            &instructions,
            &output_schema_digest,
            maximum_stack_depth,
        )?;
        Ok(Self {
            expression_version: 1,
            input_ports,
            instructions,
            output_schema_digest,
            maximum_stack_depth,
            semantic_digest,
        })
    }

    pub fn validate(&self, limits: ExpressionLimits) -> Result<(), ExpressionError> {
        if self.expression_version != 1 {
            return Err(ExpressionError::UnsupportedVersion);
        }
        let maximum_stack_depth = validate_program(&self.input_ports, &self.instructions, limits)?;
        if maximum_stack_depth != self.maximum_stack_depth
            || program_digest(
                self.expression_version,
                &self.input_ports,
                &self.instructions,
                &self.output_schema_digest,
                self.maximum_stack_depth,
            )? != self.semantic_digest
        {
            return Err(ExpressionError::DigestMismatch);
        }
        Ok(())
    }
}

#[derive(Serialize)]
#[serde(deny_unknown_fields)]
struct UnsignedProgram<'a> {
    expression_version: u32,
    input_ports: &'a [ExactDataPortRef],
    instructions: &'a [TypedInstruction],
    output_schema_digest: &'a Sha256Digest,
    maximum_stack_depth: u16,
}

fn program_digest(
    expression_version: u32,
    input_ports: &[ExactDataPortRef],
    instructions: &[TypedInstruction],
    output_schema_digest: &Sha256Digest,
    maximum_stack_depth: u16,
) -> Result<Sha256Digest, ExpressionError> {
    let value = serde_json::to_value(UnsignedProgram {
        expression_version,
        input_ports,
        instructions,
        output_schema_digest,
        maximum_stack_depth,
    })
    .map_err(|_| ExpressionError::Canonicalization)?;
    canonical_digest(&value)
        .map_err(|_| ExpressionError::Canonicalization)?
        .parse()
        .map_err(|_| ExpressionError::Canonicalization)
}

fn validate_program(
    input_ports: &[ExactDataPortRef],
    instructions: &[TypedInstruction],
    limits: ExpressionLimits,
) -> Result<u16, ExpressionError> {
    if !limits.bounded_by_absolute()
        || input_ports.len() > limits.maximum_input_ports
        || instructions.is_empty()
        || instructions.len() > limits.maximum_instructions
        || input_ports.iter().collect::<BTreeSet<_>>().len() != input_ports.len()
    {
        return Err(ExpressionError::LimitExceeded);
    }

    let declared = input_ports.iter().collect::<BTreeSet<_>>();
    let mut depth = 0_usize;
    let mut maximum_depth = 0_usize;
    for instruction in instructions {
        let (consumed, produced) = stack_effect(instruction)?;
        if depth < consumed {
            return Err(ExpressionError::StackUnderflow);
        }
        if let TypedInstruction::LoadPort { port } = instruction {
            if !declared.contains(port) {
                return Err(ExpressionError::UndeclaredInput);
            }
        }
        if let TypedInstruction::Literal { value } = instruction {
            value
                .validate()
                .map_err(|_| ExpressionError::InvalidLiteral)?;
        }
        depth = depth - consumed + produced;
        maximum_depth = maximum_depth.max(depth);
        if maximum_depth > usize::from(limits.maximum_stack_depth) {
            return Err(ExpressionError::LimitExceeded);
        }
    }
    if depth != 1 {
        return Err(ExpressionError::InvalidResultArity);
    }
    u16::try_from(maximum_depth).map_err(|_| ExpressionError::LimitExceeded)
}

fn stack_effect(instruction: &TypedInstruction) -> Result<(usize, usize), ExpressionError> {
    match instruction {
        TypedInstruction::LoadPort { .. } | TypedInstruction::Literal { .. } => Ok((0, 1)),
        TypedInstruction::GetField { .. }
        | TypedInstruction::ArrayLength
        | TypedInstruction::BooleanNot => Ok((1, 1)),
        TypedInstruction::GetIndex
        | TypedInstruction::Equal
        | TypedInstruction::NotEqual
        | TypedInstruction::Less
        | TypedInstruction::LessOrEqual
        | TypedInstruction::Greater
        | TypedInstruction::GreaterOrEqual
        | TypedInstruction::BooleanAnd
        | TypedInstruction::BooleanOr
        | TypedInstruction::IntegerAdd
        | TypedInstruction::IntegerSubtract
        | TypedInstruction::DecimalAdd
        | TypedInstruction::DecimalSubtract
        | TypedInstruction::StringConcat
        | TypedInstruction::Coalesce => Ok((2, 1)),
        TypedInstruction::Select => Ok((3, 1)),
        TypedInstruction::MakeArray { item_count } => Ok((usize::from(*item_count), 1)),
        TypedInstruction::MakeObject { ordered_fields } => {
            if ordered_fields.len() > u16::MAX.into()
                || ordered_fields.iter().collect::<BTreeSet<_>>().len() != ordered_fields.len()
            {
                return Err(ExpressionError::InvalidObjectFields);
            }
            Ok((ordered_fields.len(), 1))
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExpressionError {
    InvalidPort,
    InvalidFieldName,
    UnsupportedVersion,
    LimitExceeded,
    StackUnderflow,
    InvalidResultArity,
    UndeclaredInput,
    InvalidLiteral,
    InvalidObjectFields,
    DigestMismatch,
    Canonicalization,
}

impl fmt::Display for ExpressionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidPort => "expression data port is invalid",
            Self::InvalidFieldName => "expression field name is invalid",
            Self::UnsupportedVersion => "expression version is unsupported",
            Self::LimitExceeded => "expression exceeds a closed hard limit",
            Self::StackUnderflow => "expression stack underflow",
            Self::InvalidResultArity => "expression must leave exactly one result",
            Self::UndeclaredInput => "expression reads an undeclared exact input port",
            Self::InvalidLiteral => "expression literal is not closed canonical JSON",
            Self::InvalidObjectFields => "expression object fields are invalid or duplicated",
            Self::DigestMismatch => "expression semantic digest or stack depth mismatches",
            Self::Canonicalization => "expression cannot be canonically serialized",
        })
    }
}

impl Error for ExpressionError {}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::str::FromStr;

    fn digest(character: char) -> Sha256Digest {
        Sha256Digest::from_str(&format!("sha256:{}", character.to_string().repeat(64))).unwrap()
    }

    fn port(value: &str) -> ExactDataPortRef {
        ExactDataPortRef {
            node_id: super::super::PlanNodeKey::new("compute".to_owned()).unwrap(),
            port_id: DataPortKey::new(value.to_owned()).unwrap(),
            schema_digest: digest('1'),
        }
    }

    #[test]
    fn program_freezes_exact_inputs_stack_and_digest() {
        let input = port("count");
        let literal = ClosedJsonValue::build(digest('1'), json!(1)).unwrap();
        let program = TypedExpressionProgram::build(
            vec![input.clone()],
            vec![
                TypedInstruction::LoadPort { port: input },
                TypedInstruction::Literal { value: literal },
                TypedInstruction::IntegerAdd,
            ],
            digest('1'),
            ExpressionLimits::ABSOLUTE,
        )
        .unwrap();
        assert_eq!(program.maximum_stack_depth, 2);
        assert!(program.validate(ExpressionLimits::ABSOLUTE).is_ok());

        let encoded = serde_json::to_value(&program).unwrap();
        assert!(encoded.get("semantic_digest").is_some());
        assert_eq!(
            serde_json::from_value::<TypedExpressionProgram>(encoded).unwrap(),
            program
        );
    }

    #[test]
    fn program_rejects_dynamic_or_forged_shapes() {
        let input = port("count");
        assert_eq!(
            TypedExpressionProgram::build(
                vec![],
                vec![TypedInstruction::LoadPort {
                    port: input.clone()
                }],
                digest('1'),
                ExpressionLimits::ABSOLUTE,
            ),
            Err(ExpressionError::UndeclaredInput)
        );

        let dynamic = json!({"op":"call", "function":"network"});
        assert!(serde_json::from_value::<TypedInstruction>(dynamic).is_err());

        let mut program = TypedExpressionProgram::build(
            vec![input.clone()],
            vec![TypedInstruction::LoadPort { port: input }],
            digest('1'),
            ExpressionLimits::ABSOLUTE,
        )
        .unwrap();
        program.semantic_digest = digest('f');
        assert_eq!(
            program.validate(ExpressionLimits::ABSOLUTE),
            Err(ExpressionError::DigestMismatch)
        );
    }

    #[test]
    fn program_rejects_stack_and_limit_violations() {
        assert_eq!(
            TypedExpressionProgram::build(
                vec![],
                vec![TypedInstruction::BooleanAnd],
                digest('1'),
                ExpressionLimits::ABSOLUTE,
            ),
            Err(ExpressionError::StackUnderflow)
        );
        assert_eq!(
            TypedExpressionProgram::build(
                vec![],
                vec![
                    TypedInstruction::Literal {
                        value: ClosedJsonValue::build(digest('1'), json!(true)).unwrap(),
                    },
                    TypedInstruction::Literal {
                        value: ClosedJsonValue::build(digest('1'), json!(false)).unwrap(),
                    },
                ],
                digest('1'),
                ExpressionLimits::ABSOLUTE,
            ),
            Err(ExpressionError::InvalidResultArity)
        );
    }
}
