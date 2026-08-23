use insight_platform_contracts::{
    canonical_digest, ClosedJsonValue, Sha256Digest, MAX_SAFE_JSON_INTEGER,
};
use serde::{de, Deserialize, Deserializer, Serialize};
use serde_json::{Map, Number, Value};
use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet},
    error::Error,
    fmt,
    str::FromStr,
};

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

    pub fn evaluate(
        &self,
        inputs: &BTreeMap<ExactDataPortRef, ClosedJsonValue>,
        limits: ExpressionLimits,
    ) -> Result<ClosedJsonValue, ExpressionError> {
        self.validate(limits)?;
        let declared = self.input_ports.iter().collect::<BTreeSet<_>>();
        if inputs.keys().any(|port| !declared.contains(port)) {
            return Err(ExpressionError::UnexpectedInput);
        }
        if inputs.len() != declared.len() {
            return Err(ExpressionError::MissingInput);
        }

        let mut stack = Vec::with_capacity(usize::from(self.maximum_stack_depth));
        for instruction in &self.instructions {
            evaluate_instruction(instruction, inputs, &mut stack)?;
        }
        let value = stack.pop().ok_or(ExpressionError::InvalidResultArity)?;
        if !stack.is_empty() {
            return Err(ExpressionError::InvalidResultArity);
        }
        ClosedJsonValue::build(self.output_schema_digest.clone(), value)
            .map_err(|_| ExpressionError::InvalidResult)
    }
}

fn evaluate_instruction(
    instruction: &TypedInstruction,
    inputs: &BTreeMap<ExactDataPortRef, ClosedJsonValue>,
    stack: &mut Vec<Value>,
) -> Result<(), ExpressionError> {
    match instruction {
        TypedInstruction::LoadPort { port } => {
            let value = inputs.get(port).ok_or(ExpressionError::MissingInput)?;
            value
                .validate()
                .map_err(|_| ExpressionError::InvalidInput)?;
            if value.schema_digest != port.schema_digest {
                return Err(ExpressionError::SchemaMismatch);
            }
            stack.push(value.value.clone());
        }
        TypedInstruction::Literal { value } => stack.push(value.value.clone()),
        TypedInstruction::GetField { field } => {
            let object = pop(stack)?
                .as_object()
                .ok_or(ExpressionError::TypeMismatch)?
                .clone();
            stack.push(
                object
                    .get(field.as_str())
                    .cloned()
                    .ok_or(ExpressionError::MissingField)?,
            );
        }
        TypedInstruction::GetIndex => {
            let index = json_index(pop(stack)?)?;
            let array = pop(stack)?
                .as_array()
                .ok_or(ExpressionError::TypeMismatch)?
                .clone();
            stack.push(
                array
                    .get(index)
                    .cloned()
                    .ok_or(ExpressionError::InvalidIndex)?,
            );
        }
        TypedInstruction::ArrayLength => {
            let length = pop(stack)?
                .as_array()
                .ok_or(ExpressionError::TypeMismatch)?
                .len();
            let length = u64::try_from(length).map_err(|_| ExpressionError::ArithmeticOverflow)?;
            stack.push(Value::Number(Number::from(length)));
        }
        TypedInstruction::MakeArray { item_count } => {
            let start = stack
                .len()
                .checked_sub(usize::from(*item_count))
                .ok_or(ExpressionError::StackUnderflow)?;
            let values = stack.split_off(start);
            stack.push(Value::Array(values));
        }
        TypedInstruction::MakeObject { ordered_fields } => {
            let start = stack
                .len()
                .checked_sub(ordered_fields.len())
                .ok_or(ExpressionError::StackUnderflow)?;
            let values = stack.split_off(start);
            let object = ordered_fields
                .iter()
                .zip(values)
                .map(|(field, value)| (field.as_str().to_owned(), value))
                .collect::<Map<_, _>>();
            stack.push(Value::Object(object));
        }
        TypedInstruction::Equal | TypedInstruction::NotEqual => {
            let right = pop(stack)?;
            let left = pop(stack)?;
            let equal = if left.is_number() && right.is_number() {
                compare_values(&left, &right)? == Ordering::Equal
            } else {
                left == right
            };
            stack.push(Value::Bool(
                if matches!(instruction, TypedInstruction::Equal) {
                    equal
                } else {
                    !equal
                },
            ));
        }
        TypedInstruction::Less
        | TypedInstruction::LessOrEqual
        | TypedInstruction::Greater
        | TypedInstruction::GreaterOrEqual => {
            let right = pop(stack)?;
            let left = pop(stack)?;
            let ordering = compare_values(&left, &right)?;
            let result = match instruction {
                TypedInstruction::Less => ordering == Ordering::Less,
                TypedInstruction::LessOrEqual => ordering != Ordering::Greater,
                TypedInstruction::Greater => ordering == Ordering::Greater,
                TypedInstruction::GreaterOrEqual => ordering != Ordering::Less,
                _ => unreachable!(),
            };
            stack.push(Value::Bool(result));
        }
        TypedInstruction::BooleanAnd | TypedInstruction::BooleanOr => {
            let right = pop_bool(stack)?;
            let left = pop_bool(stack)?;
            stack.push(Value::Bool(
                if matches!(instruction, TypedInstruction::BooleanAnd) {
                    left && right
                } else {
                    left || right
                },
            ));
        }
        TypedInstruction::BooleanNot => {
            let value = pop_bool(stack)?;
            stack.push(Value::Bool(!value));
        }
        TypedInstruction::IntegerAdd | TypedInstruction::IntegerSubtract => {
            let right = pop_integer(stack)?;
            let left = pop_integer(stack)?;
            let result = if matches!(instruction, TypedInstruction::IntegerAdd) {
                left.checked_add(right)
            } else {
                left.checked_sub(right)
            }
            .filter(|value| value.unsigned_abs() <= u128::from(MAX_SAFE_JSON_INTEGER))
            .ok_or(ExpressionError::ArithmeticOverflow)?;
            stack.push(Value::Number(Number::from(
                i64::try_from(result).map_err(|_| ExpressionError::ArithmeticOverflow)?,
            )));
        }
        TypedInstruction::DecimalAdd | TypedInstruction::DecimalSubtract => {
            let right = ExactDecimal::from_value(pop(stack)?)?;
            let left = ExactDecimal::from_value(pop(stack)?)?;
            let result = if matches!(instruction, TypedInstruction::DecimalAdd) {
                left.checked_add(right)
            } else {
                left.checked_sub(right)
            }?;
            stack.push(Value::Number(result.into_number()?));
        }
        TypedInstruction::StringConcat => {
            let right = pop(stack)?
                .as_str()
                .ok_or(ExpressionError::TypeMismatch)?
                .to_owned();
            let mut left = pop(stack)?
                .as_str()
                .ok_or(ExpressionError::TypeMismatch)?
                .to_owned();
            left.push_str(&right);
            stack.push(Value::String(left));
        }
        TypedInstruction::Coalesce => {
            let right = pop(stack)?;
            let left = pop(stack)?;
            stack.push(if left.is_null() { right } else { left });
        }
        TypedInstruction::Select => {
            let otherwise = pop(stack)?;
            let selected = pop(stack)?;
            let condition = pop_bool(stack)?;
            stack.push(if condition { selected } else { otherwise });
        }
    }
    Ok(())
}

fn pop(stack: &mut Vec<Value>) -> Result<Value, ExpressionError> {
    stack.pop().ok_or(ExpressionError::StackUnderflow)
}

fn pop_bool(stack: &mut Vec<Value>) -> Result<bool, ExpressionError> {
    pop(stack)?.as_bool().ok_or(ExpressionError::TypeMismatch)
}

fn pop_integer(stack: &mut Vec<Value>) -> Result<i128, ExpressionError> {
    let value = pop(stack)?;
    let value = value.as_i64().ok_or(ExpressionError::TypeMismatch)?;
    Ok(i128::from(value))
}

fn json_index(value: Value) -> Result<usize, ExpressionError> {
    let value = value.as_u64().ok_or(ExpressionError::TypeMismatch)?;
    usize::try_from(value).map_err(|_| ExpressionError::InvalidIndex)
}

fn compare_values(left: &Value, right: &Value) -> Result<Ordering, ExpressionError> {
    match (left, right) {
        (Value::String(left), Value::String(right)) => Ok(left.cmp(right)),
        (Value::Number(_), Value::Number(_)) => ExactDecimal::from_value(left.clone())?
            .compare(ExactDecimal::from_value(right.clone())?),
        _ => Err(ExpressionError::TypeMismatch),
    }
}

#[derive(Debug, Clone, Copy)]
struct ExactDecimal {
    coefficient: i128,
    scale: u32,
}

impl ExactDecimal {
    const MAX_SCALE: u32 = 28;

    fn from_value(value: Value) -> Result<Self, ExpressionError> {
        let source = value
            .as_number()
            .ok_or(ExpressionError::TypeMismatch)?
            .to_string();
        Self::parse(&source)
    }

    fn parse(source: &str) -> Result<Self, ExpressionError> {
        let (mantissa, exponent) = if let Some((mantissa, exponent)) = source.split_once(['e', 'E'])
        {
            (
                mantissa,
                exponent
                    .parse::<i32>()
                    .map_err(|_| ExpressionError::ArithmeticOverflow)?,
            )
        } else {
            (source, 0_i32)
        };
        let negative = mantissa.starts_with('-');
        let unsigned = mantissa.strip_prefix('-').unwrap_or(mantissa);
        let (whole, fraction) = unsigned.split_once('.').unwrap_or((unsigned, ""));
        let digits = format!("{whole}{fraction}");
        if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(ExpressionError::TypeMismatch);
        }
        let coefficient = digits
            .parse::<i128>()
            .map_err(|_| ExpressionError::ArithmeticOverflow)?;
        let coefficient = if negative { -coefficient } else { coefficient };
        let scale = i32::try_from(fraction.len())
            .map_err(|_| ExpressionError::ArithmeticOverflow)?
            .checked_sub(exponent)
            .ok_or(ExpressionError::ArithmeticOverflow)?;
        if scale < 0 {
            let factor = checked_power_of_ten(scale.unsigned_abs())?;
            return Ok(Self {
                coefficient: coefficient
                    .checked_mul(factor)
                    .ok_or(ExpressionError::ArithmeticOverflow)?,
                scale: 0,
            });
        }
        let scale = u32::try_from(scale).map_err(|_| ExpressionError::ArithmeticOverflow)?;
        if scale > Self::MAX_SCALE {
            return Err(ExpressionError::ArithmeticOverflow);
        }
        Ok(Self { coefficient, scale }.normalized())
    }

    fn normalized(mut self) -> Self {
        while self.scale > 0 && self.coefficient % 10 == 0 {
            self.coefficient /= 10;
            self.scale -= 1;
        }
        self
    }

    fn aligned(self, other: Self) -> Result<(i128, i128, u32), ExpressionError> {
        let scale = self.scale.max(other.scale);
        let left = self
            .coefficient
            .checked_mul(checked_power_of_ten(scale - self.scale)?)
            .ok_or(ExpressionError::ArithmeticOverflow)?;
        let right = other
            .coefficient
            .checked_mul(checked_power_of_ten(scale - other.scale)?)
            .ok_or(ExpressionError::ArithmeticOverflow)?;
        Ok((left, right, scale))
    }

    fn checked_add(self, other: Self) -> Result<Self, ExpressionError> {
        let (left, right, scale) = self.aligned(other)?;
        Ok(Self {
            coefficient: left
                .checked_add(right)
                .ok_or(ExpressionError::ArithmeticOverflow)?,
            scale,
        }
        .normalized())
    }

    fn checked_sub(self, other: Self) -> Result<Self, ExpressionError> {
        let (left, right, scale) = self.aligned(other)?;
        Ok(Self {
            coefficient: left
                .checked_sub(right)
                .ok_or(ExpressionError::ArithmeticOverflow)?,
            scale,
        }
        .normalized())
    }

    fn compare(self, other: Self) -> Result<Ordering, ExpressionError> {
        let (left, right, _) = self.aligned(other)?;
        Ok(left.cmp(&right))
    }

    fn into_number(self) -> Result<Number, ExpressionError> {
        let negative = self.coefficient < 0;
        let digits = self.coefficient.unsigned_abs().to_string();
        let source = if self.scale == 0 {
            format!("{}{digits}", if negative { "-" } else { "" })
        } else {
            let scale =
                usize::try_from(self.scale).map_err(|_| ExpressionError::ArithmeticOverflow)?;
            let padded = if digits.len() <= scale {
                format!("{}{}", "0".repeat(scale + 1 - digits.len()), digits)
            } else {
                digits
            };
            let split = padded.len() - scale;
            format!(
                "{}{}.{}",
                if negative { "-" } else { "" },
                &padded[..split],
                &padded[split..]
            )
        };
        Number::from_str(&source).map_err(|_| ExpressionError::ArithmeticOverflow)
    }
}

fn checked_power_of_ten(power: u32) -> Result<i128, ExpressionError> {
    if power > ExactDecimal::MAX_SCALE {
        return Err(ExpressionError::ArithmeticOverflow);
    }
    10_i128
        .checked_pow(power)
        .ok_or(ExpressionError::ArithmeticOverflow)
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
    MissingInput,
    UnexpectedInput,
    InvalidInput,
    SchemaMismatch,
    TypeMismatch,
    ArithmeticOverflow,
    InvalidIndex,
    MissingField,
    InvalidResult,
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
            Self::MissingInput => "expression exact input is missing",
            Self::UnexpectedInput => "expression received an undeclared input",
            Self::InvalidInput => "expression input is not closed canonical JSON",
            Self::SchemaMismatch => "expression input schema digest mismatches its exact port",
            Self::TypeMismatch => "expression opcode received a value of the wrong type",
            Self::ArithmeticOverflow => "expression arithmetic exceeds its exact bounded domain",
            Self::InvalidIndex => "expression array index is outside the array",
            Self::MissingField => "expression object field is absent",
            Self::InvalidResult => "expression result is not bounded canonical JSON",
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

    #[test]
    fn evaluator_is_pure_exact_and_rejects_input_injection() {
        let count = port("count");
        let program = TypedExpressionProgram::build(
            vec![count.clone()],
            vec![
                TypedInstruction::LoadPort {
                    port: count.clone(),
                },
                TypedInstruction::Literal {
                    value: ClosedJsonValue::build(digest('1'), json!(2)).unwrap(),
                },
                TypedInstruction::IntegerAdd,
            ],
            digest('1'),
            ExpressionLimits::ABSOLUTE,
        )
        .unwrap();
        let inputs = BTreeMap::from([(
            count,
            ClosedJsonValue::build(digest('1'), json!(40)).unwrap(),
        )]);
        let first = program
            .evaluate(&inputs, ExpressionLimits::ABSOLUTE)
            .unwrap();
        let second = program
            .evaluate(&inputs, ExpressionLimits::ABSOLUTE)
            .unwrap();
        assert_eq!(first, second);
        assert_eq!(first.value, json!(42));

        let injected = BTreeMap::from([(
            port("other"),
            ClosedJsonValue::build(digest('1'), json!(40)).unwrap(),
        )]);
        assert_eq!(
            program.evaluate(&injected, ExpressionLimits::ABSOLUTE),
            Err(ExpressionError::UnexpectedInput)
        );
    }

    #[test]
    fn evaluator_uses_exact_decimal_and_closed_composite_operations() {
        let program = TypedExpressionProgram::build(
            vec![],
            vec![
                TypedInstruction::Literal {
                    value: ClosedJsonValue::build(digest('1'), json!(0.1)).unwrap(),
                },
                TypedInstruction::Literal {
                    value: ClosedJsonValue::build(digest('1'), json!(0.2)).unwrap(),
                },
                TypedInstruction::DecimalAdd,
                TypedInstruction::Literal {
                    value: ClosedJsonValue::build(digest('1'), json!(0.3)).unwrap(),
                },
                TypedInstruction::Equal,
            ],
            digest('2'),
            ExpressionLimits::ABSOLUTE,
        )
        .unwrap();
        assert_eq!(
            program
                .evaluate(&BTreeMap::new(), ExpressionLimits::ABSOLUTE)
                .unwrap()
                .value,
            json!(true)
        );

        let object = TypedExpressionProgram::build(
            vec![],
            vec![
                TypedInstruction::Literal {
                    value: ClosedJsonValue::build(digest('1'), json!("exact")).unwrap(),
                },
                TypedInstruction::MakeObject {
                    ordered_fields: vec![ExpressionFieldName::new("value".to_owned()).unwrap()],
                },
                TypedInstruction::GetField {
                    field: ExpressionFieldName::new("value".to_owned()).unwrap(),
                },
            ],
            digest('1'),
            ExpressionLimits::ABSOLUTE,
        )
        .unwrap();
        assert_eq!(
            object
                .evaluate(&BTreeMap::new(), ExpressionLimits::ABSOLUTE)
                .unwrap()
                .value,
            json!("exact")
        );
    }
}
