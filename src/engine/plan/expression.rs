use std::collections::{BTreeMap, BTreeSet};

use cel::common::ast::{operators, CallExpr, Expr, IdedExpr, LiteralValue, SelectExpr};
use serde::{Deserialize, Serialize};
use serde_json::{Number, Value};

use super::PlanType;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CelAnalysis {
    pub result_type: PlanType,
    pub references: BTreeSet<String>,
}

/// The closed CEL profile shared by DSL lowering and Canonical Plan
/// verification. The parser/runtime version is pinned separately by the Plan
/// model; this pass proves the expression shape and result type without
/// executing authored input.
pub fn analyze_cel_expression(
    source: &str,
    dependencies: &BTreeMap<String, PlanType>,
) -> Result<CelAnalysis, String> {
    if source.trim().is_empty() || source.trim() != source {
        return Err("CEL source must be non-empty and have no surrounding whitespace".to_owned());
    }
    let program = cel::Program::compile(source)
        .map_err(|error| format!("CEL expression does not parse under cel-rs 0.14: {error}"))?;
    let references = program
        .references()
        .variables()
        .into_iter()
        .map(str::to_owned)
        .collect::<BTreeSet<_>>();
    let declared = dependencies.keys().cloned().collect::<BTreeSet<_>>();
    if references != declared {
        return Err(format!(
            "CEL variable references {references:?} do not exactly match declared dependencies {declared:?}"
        ));
    }
    let result_type = infer_cel_type(program.expression(), dependencies)?
        .normalized()
        .map_err(|error| error.to_string())?;
    Ok(CelAnalysis {
        result_type,
        references,
    })
}

fn infer_cel_type(
    expression: &IdedExpr,
    dependencies: &BTreeMap<String, PlanType>,
) -> Result<PlanType, String> {
    match &expression.expr {
        Expr::Ident(name) => dependencies
            .get(name)
            .cloned()
            .ok_or_else(|| format!("CEL references undeclared dependency '{name}'")),
        Expr::Literal(literal) => cel_literal_type(literal),
        Expr::Select(select) => infer_select_type(select, dependencies),
        Expr::Call(call) => infer_call_type(call, dependencies),
        Expr::Unspecified
        | Expr::Comprehension(_)
        | Expr::List(_)
        | Expr::Map(_)
        | Expr::Struct(_) => Err(
            "CEL expression form is outside the fixed typed profile (logic, comparison, field selection, and size)"
                .to_owned(),
        ),
    }
}

fn infer_select_type(
    select: &SelectExpr,
    dependencies: &BTreeMap<String, PlanType>,
) -> Result<PlanType, String> {
    if select.test {
        return Err("CEL presence tests are outside the fixed typed profile".to_owned());
    }
    let operand = infer_cel_type(&select.operand, dependencies)?;
    project_field(&operand, &select.field)
}

fn project_field(value_type: &PlanType, field: &str) -> Result<PlanType, String> {
    match value_type {
        PlanType::Object {
            properties,
            additional_properties,
        } => {
            if let Some(property) = properties.get(field) {
                if !property.required {
                    return Err(format!(
                        "CEL field '{field}' is optional and cannot be read without an explicit presence contract"
                    ));
                }
                Ok(property.value_type.clone())
            } else if let Some(additional) = additional_properties {
                if **additional == PlanType::Any {
                    Err(format!(
                        "CEL field '{field}' has Any type and is not statically readable"
                    ))
                } else {
                    Ok((**additional).clone())
                }
            } else {
                Err(format!("CEL field '{field}' is not declared"))
            }
        }
        PlanType::Union { variants } => {
            let projected = variants
                .iter()
                .map(|variant| project_field(variant, field))
                .collect::<Result<Vec<_>, _>>()?;
            PlanType::unify(projected).map_err(|error| error.to_string())
        }
        _ => Err(format!(
            "CEL field '{field}' cannot be selected from its static operand type"
        )),
    }
}

fn infer_call_type(
    call: &CallExpr,
    dependencies: &BTreeMap<String, PlanType>,
) -> Result<PlanType, String> {
    if call.target.is_some() {
        return Err("CEL method calls are outside the fixed typed profile".to_owned());
    }
    match call.func_name.as_str() {
        operators::LOGICAL_AND | operators::LOGICAL_OR => {
            require_arity(call, 2)?;
            for argument in &call.args {
                let argument_type = infer_cel_type(argument, dependencies)?;
                if !argument_type.is_assignable_to(&PlanType::Boolean) {
                    return Err("CEL logical operators require boolean operands".to_owned());
                }
            }
            Ok(PlanType::Boolean)
        }
        operators::LOGICAL_NOT => {
            require_arity(call, 1)?;
            let argument_type = infer_cel_type(&call.args[0], dependencies)?;
            if !argument_type.is_assignable_to(&PlanType::Boolean) {
                return Err("CEL logical negation requires a boolean operand".to_owned());
            }
            Ok(PlanType::Boolean)
        }
        operators::EQUALS | operators::NOT_EQUALS => {
            require_arity(call, 2)?;
            let left = infer_cel_type(&call.args[0], dependencies)?;
            let right = infer_cel_type(&call.args[1], dependencies)?;
            if !types_overlap(&left, &right) {
                return Err("CEL equality operands have incompatible static types".to_owned());
            }
            Ok(PlanType::Boolean)
        }
        operators::GREATER
        | operators::GREATER_EQUALS
        | operators::LESS
        | operators::LESS_EQUALS => {
            require_arity(call, 2)?;
            let left = infer_cel_type(&call.args[0], dependencies)?;
            let right = infer_cel_type(&call.args[1], dependencies)?;
            if !types_overlap(&left, &right) || !is_orderable(&left) || !is_orderable(&right) {
                return Err(
                    "CEL ordering operands must have compatible string or numeric types".to_owned(),
                );
            }
            Ok(PlanType::Boolean)
        }
        "size" => {
            require_arity(call, 1)?;
            let argument = infer_cel_type(&call.args[0], dependencies)?;
            if !supports_size(&argument) {
                return Err(
                    "CEL size() requires a statically typed string, array, or object".to_owned(),
                );
            }
            Ok(PlanType::Integer)
        }
        name => Err(format!(
            "CEL function '{name}' is outside the fixed typed profile"
        )),
    }
}

fn require_arity(call: &CallExpr, expected: usize) -> Result<(), String> {
    if call.args.len() == expected {
        Ok(())
    } else {
        Err(format!(
            "CEL function '{}' requires {expected} argument(s)",
            call.func_name
        ))
    }
}

fn cel_literal_type(literal: &LiteralValue) -> Result<PlanType, String> {
    let value = match literal {
        LiteralValue::Boolean(value) => Value::Bool(*value.inner()),
        LiteralValue::Double(value) => Number::from_f64(*value.inner())
            .map(Value::Number)
            .ok_or_else(|| "CEL contains a non-finite number literal".to_owned())?,
        LiteralValue::Int(value) => Value::Number(Number::from(*value.inner())),
        LiteralValue::Null => Value::Null,
        LiteralValue::String(value) => Value::String(value.inner().to_string()),
        LiteralValue::UInt(value) => Value::Number(Number::from(*value.inner())),
        LiteralValue::Bytes(_) => {
            return Err("CEL byte literals are outside the fixed typed profile".to_owned());
        }
    };
    PlanType::literal(value).map_err(|error| error.to_string())
}

fn types_overlap(left: &PlanType, right: &PlanType) -> bool {
    !matches!(left, PlanType::Any | PlanType::Never)
        && !matches!(right, PlanType::Any | PlanType::Never)
        && (left.is_assignable_to(right) || right.is_assignable_to(left))
}

fn is_orderable(value_type: &PlanType) -> bool {
    match value_type {
        PlanType::Integer
        | PlanType::Number
        | PlanType::String
        | PlanType::StringRefined { .. } => true,
        PlanType::Literal {
            value: Value::Number(_) | Value::String(_),
        } => true,
        PlanType::Union { variants } => !variants.is_empty() && variants.iter().all(is_orderable),
        _ => false,
    }
}

fn supports_size(value_type: &PlanType) -> bool {
    match value_type {
        PlanType::String
        | PlanType::StringRefined { .. }
        | PlanType::Array { .. }
        | PlanType::ArrayBounded { .. }
        | PlanType::Object { .. } => true,
        PlanType::Literal {
            value: Value::String(_) | Value::Array(_) | Value::Object(_),
        } => true,
        PlanType::Union { variants } => !variants.is_empty() && variants.iter().all(supports_size),
        _ => false,
    }
}

/// Canonical source model for the fixed Match engine. Match is lazy pure value
/// selection: it never owns steps or creates graph activations.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MatchProgram {
    pub selector: MatchValue,
    pub cases: BTreeMap<String, MatchValue>,
    pub default: MatchValue,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum MatchValue {
    Dependency {
        name: String,
    },
    Literal {
        value: Value,
    },
    Match {
        selector: Box<MatchValue>,
        cases: BTreeMap<String, MatchValue>,
        default: Box<MatchValue>,
    },
}

pub fn analyze_match_program(
    program: &MatchProgram,
    dependencies: &BTreeMap<String, PlanType>,
) -> Result<PlanType, String> {
    if program.cases.is_empty() {
        return Err("match must declare at least one case".to_owned());
    }
    let mut referenced = BTreeSet::new();
    let result = infer_match(
        &program.selector,
        &program.cases,
        &program.default,
        dependencies,
        &mut referenced,
    )?;
    let declared = dependencies.keys().cloned().collect::<BTreeSet<_>>();
    if referenced != declared {
        return Err(format!(
            "Match dependency references {referenced:?} do not exactly match declared dependencies {declared:?}"
        ));
    }
    result.normalized().map_err(|error| error.to_string())
}

/// Canonical, recursively typed construction program for natural YAML values.
/// It covers Object, Array, Project and text-template lowering without
/// introducing executable graph nodes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ValueProgram {
    Dependency {
        name: String,
        path: Vec<String>,
    },
    Literal {
        value: Value,
    },
    Array {
        items: Vec<ValueProgram>,
    },
    Object {
        fields: BTreeMap<String, ValueProgram>,
    },
    Template {
        parts: Vec<TemplatePart>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum TemplatePart {
    Text { text: String },
    Value { value: Box<ValueProgram> },
}

pub fn analyze_value_program(
    program: &ValueProgram,
    dependencies: &BTreeMap<String, PlanType>,
) -> Result<PlanType, String> {
    let mut referenced = BTreeSet::new();
    let result = infer_value_program(program, dependencies, &mut referenced)?;
    let declared = dependencies.keys().cloned().collect::<BTreeSet<_>>();
    if referenced != declared {
        return Err(format!(
            "Value dependency references {referenced:?} do not exactly match declared dependencies {declared:?}"
        ));
    }
    result.normalized().map_err(|error| error.to_string())
}

fn infer_value_program(
    program: &ValueProgram,
    dependencies: &BTreeMap<String, PlanType>,
    referenced: &mut BTreeSet<String>,
) -> Result<PlanType, String> {
    match program {
        ValueProgram::Dependency { name, path } => {
            referenced.insert(name.clone());
            let mut value_type = dependencies.get(name).cloned().ok_or_else(|| {
                format!("value program references undeclared dependency '{name}'")
            })?;
            for field in path {
                value_type = project_value_field(&value_type, field)?;
            }
            Ok(value_type)
        }
        ValueProgram::Literal { value } => {
            PlanType::literal(value.clone()).map_err(|error| error.to_string())
        }
        ValueProgram::Array { items } => {
            let item_type = if items.is_empty() {
                PlanType::Never
            } else {
                PlanType::unify(
                    items
                        .iter()
                        .map(|item| infer_value_program(item, dependencies, referenced))
                        .collect::<Result<Vec<_>, _>>()?,
                )
                .map_err(|error| error.to_string())?
            };
            let length = u64::try_from(items.len())
                .map_err(|_| "value array exceeds the type bound".to_owned())?;
            PlanType::array(item_type, length, Some(length)).map_err(|error| error.to_string())
        }
        ValueProgram::Object { fields } => Ok(PlanType::Object {
            properties: fields
                .iter()
                .map(|(name, value)| {
                    Ok((
                        name.clone(),
                        super::PlanProperty::new(
                            infer_value_program(value, dependencies, referenced)?,
                            true,
                        )
                        .map_err(|error| error.to_string())?,
                    ))
                })
                .collect::<Result<BTreeMap<_, _>, String>>()?,
            additional_properties: None,
        }),
        ValueProgram::Template { parts } => {
            let mut static_minimum = 0_u64;
            for part in parts {
                match part {
                    TemplatePart::Text { text } => {
                        static_minimum = static_minimum
                            .checked_add(
                                u64::try_from(text.chars().count())
                                    .map_err(|_| "template text is too large".to_owned())?,
                            )
                            .ok_or_else(|| "template length bound overflowed".to_owned())?;
                    }
                    TemplatePart::Value { value } => {
                        let value_type = infer_value_program(value, dependencies, referenced)?;
                        if !value_type.is_assignable_to(&PlanType::String) {
                            return Err(
                                "text-template interpolation requires non-null string values"
                                    .to_owned(),
                            );
                        }
                    }
                }
            }
            PlanType::string(static_minimum, None, None, None).map_err(|error| error.to_string())
        }
    }
}

fn project_value_field(value_type: &PlanType, field: &str) -> Result<PlanType, String> {
    match value_type {
        PlanType::Object {
            properties,
            additional_properties,
        } => properties
            .get(field)
            .map(|property| property.value_type.clone())
            .or_else(|| additional_properties.as_deref().cloned())
            .filter(|value_type| value_type != &PlanType::Any)
            .ok_or_else(|| format!("value projection references unknown field '{field}'")),
        PlanType::Union { variants } => {
            let projected = variants
                .iter()
                .filter_map(|variant| project_value_field(variant, field).ok())
                .collect::<Vec<_>>();
            if projected.is_empty() {
                Err(format!(
                    "value projection field '{field}' is absent from every union variant"
                ))
            } else {
                PlanType::unify(projected).map_err(|error| error.to_string())
            }
        }
        _ => Err(format!(
            "value projection field '{field}' requires an object operand"
        )),
    }
}

fn infer_match(
    selector: &MatchValue,
    cases: &BTreeMap<String, MatchValue>,
    default: &MatchValue,
    dependencies: &BTreeMap<String, PlanType>,
    referenced: &mut BTreeSet<String>,
) -> Result<PlanType, String> {
    if cases.is_empty() {
        return Err("nested match must declare at least one case".to_owned());
    }
    let selector_type = infer_match_value(selector, dependencies, referenced)?;
    if !selector_type.is_assignable_to(&PlanType::String) {
        return Err("match selector must have a statically non-null string type".to_owned());
    }
    let mut arm_types = cases
        .values()
        .map(|value| infer_match_value(value, dependencies, referenced))
        .collect::<Result<Vec<_>, _>>()?;
    arm_types.push(infer_match_value(default, dependencies, referenced)?);
    PlanType::unify(arm_types).map_err(|error| error.to_string())
}

fn infer_match_value(
    value: &MatchValue,
    dependencies: &BTreeMap<String, PlanType>,
    referenced: &mut BTreeSet<String>,
) -> Result<PlanType, String> {
    match value {
        MatchValue::Dependency { name } => {
            referenced.insert(name.clone());
            dependencies
                .get(name)
                .cloned()
                .ok_or_else(|| format!("match references undeclared dependency '{name}'"))
        }
        MatchValue::Literal { value } => {
            PlanType::literal(value.clone()).map_err(|e| e.to_string())
        }
        MatchValue::Match {
            selector,
            cases,
            default,
        } => infer_match(selector, cases, default, dependencies, referenced),
    }
}
