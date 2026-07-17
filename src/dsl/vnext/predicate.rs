//! Static validation and flow-sensitive narrowing for authored CEL predicates.
//!
//! vNext intentionally supports a small, typed CEL predicate profile. The
//! runtime still evaluates CEL, but lowering and IR verification first prove
//! that the expression is boolean, references only the declared `scope`, and
//! reads only statically safe fields. Equality checks on scalar discriminators
//! in an `&&` chain narrow captured union values for that switch arm.

use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error,
    fmt,
};

use cel::common::ast::{operators, CallExpr, Expr, IdedExpr, LiteralValue, SelectExpr};
use serde_json::{Number, Value};

use super::{
    types::{ObjectType, ValueType},
    value::Identifier,
};

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PredicateAnalysis {
    pub(crate) narrowed_scope: BTreeMap<Identifier, ValueType>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PredicateError {
    message: String,
}

impl PredicateError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for PredicateError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl Error for PredicateError {}

pub(crate) fn analyze_predicate(
    source: &str,
    scope: &BTreeMap<Identifier, ValueType>,
) -> Result<PredicateAnalysis, PredicateError> {
    if source.trim().is_empty() {
        return Err(PredicateError::new("CEL predicate must not be empty"));
    }
    let program = cel::Program::compile(source)
        .map_err(|_| PredicateError::new("CEL predicate contains invalid syntax"))?;
    let scope_type = ValueType::Object(ObjectType {
        properties: scope
            .iter()
            .map(|(name, value_type)| {
                (
                    name.as_str().to_string(),
                    super::types::PropertyType {
                        value_type: value_type.clone(),
                        required: true,
                    },
                )
            })
            .collect(),
        additional_properties: None,
    });
    let result_type = infer_type(program.expression(), &scope_type)?;
    if !result_type.is_assignable_to(&ValueType::Boolean) {
        return Err(PredicateError::new(
            "CEL predicate must have a statically boolean result",
        ));
    }

    let mut narrowed_scope = scope.clone();
    apply_conjunctive_narrowings(program.expression(), &mut narrowed_scope);
    Ok(PredicateAnalysis { narrowed_scope })
}

/// Returns the top-level `scope.<binding>` names read by a CEL predicate.
///
/// This syntactic pass is intentionally separate from typed predicate
/// analysis. Semantic validation uses it only to prove that a switch capture
/// is consumed; lowering remains responsible for rejecting expressions that
/// are outside the typed CEL profile.
pub(crate) fn referenced_scope_bindings(
    source: &str,
) -> Result<BTreeSet<Identifier>, PredicateError> {
    if source.trim().is_empty() {
        return Err(PredicateError::new("CEL predicate must not be empty"));
    }
    let program = cel::Program::compile(source)
        .map_err(|_| PredicateError::new("CEL predicate contains invalid syntax"))?;
    let mut bindings = BTreeSet::new();
    collect_scope_bindings(program.expression(), &mut bindings);
    Ok(bindings)
}

fn collect_scope_bindings(expression: &IdedExpr, bindings: &mut BTreeSet<Identifier>) {
    if let Some(path) = scope_path(expression) {
        if let Some(binding) = path.first().and_then(|name| Identifier::parse(name).ok()) {
            bindings.insert(binding);
        }
    }
    match &expression.expr {
        Expr::Select(select) => collect_scope_bindings(&select.operand, bindings),
        Expr::Call(call) => {
            if let Some(target) = &call.target {
                collect_scope_bindings(target, bindings);
            }
            for argument in &call.args {
                collect_scope_bindings(argument, bindings);
            }
        }
        Expr::Ident(_)
        | Expr::Literal(_)
        | Expr::Unspecified
        | Expr::Comprehension(_)
        | Expr::List(_)
        | Expr::Map(_)
        | Expr::Struct(_) => {}
    }
}

fn infer_type(expression: &IdedExpr, scope: &ValueType) -> Result<ValueType, PredicateError> {
    match &expression.expr {
        Expr::Ident(name) if name == "scope" => Ok(scope.clone()),
        Expr::Ident(name) => Err(PredicateError::new(format!(
            "CEL predicate references unknown root '{name}'; only 'scope' is available"
        ))),
        Expr::Literal(literal) => literal_type(literal),
        Expr::Select(select) => infer_select_type(select, scope),
        Expr::Call(call) => infer_call_type(call, scope),
        _ => Err(PredicateError::new(
            "CEL predicate uses an expression form outside the typed vNext profile",
        )),
    }
}

fn infer_select_type(select: &SelectExpr, scope: &ValueType) -> Result<ValueType, PredicateError> {
    if select.test {
        return Err(PredicateError::new(
            "CEL presence tests are not supported in typed vNext predicates",
        ));
    }
    let operand = infer_type(&select.operand, scope)?;
    operand
        .require_decoded_segments([select.field.as_str()])
        .map_err(|error| {
            PredicateError::new(format!(
                "CEL predicate field '{}' is not statically readable: {error}",
                select.field
            ))
        })
}

fn infer_call_type(call: &CallExpr, scope: &ValueType) -> Result<ValueType, PredicateError> {
    if call.target.is_some() {
        return Err(PredicateError::new(
            "CEL method calls are not supported in typed vNext predicates",
        ));
    }
    match call.func_name.as_str() {
        operators::LOGICAL_AND | operators::LOGICAL_OR => {
            require_arity(call, 2)?;
            for argument in &call.args {
                let argument_type = infer_type(argument, scope)?;
                if !argument_type.is_assignable_to(&ValueType::Boolean) {
                    return Err(PredicateError::new(
                        "CEL logical operators require boolean operands",
                    ));
                }
            }
            Ok(ValueType::Boolean)
        }
        operators::LOGICAL_NOT => {
            require_arity(call, 1)?;
            let argument_type = infer_type(&call.args[0], scope)?;
            if !argument_type.is_assignable_to(&ValueType::Boolean) {
                return Err(PredicateError::new(
                    "CEL logical negation requires a boolean operand",
                ));
            }
            Ok(ValueType::Boolean)
        }
        operators::EQUALS | operators::NOT_EQUALS => {
            require_arity(call, 2)?;
            let left = infer_type(&call.args[0], scope)?;
            let right = infer_type(&call.args[1], scope)?;
            if !types_overlap(&left, &right) {
                return Err(PredicateError::new(
                    "CEL equality operands have incompatible static types",
                ));
            }
            Ok(ValueType::Boolean)
        }
        operators::GREATER
        | operators::GREATER_EQUALS
        | operators::LESS
        | operators::LESS_EQUALS => {
            require_arity(call, 2)?;
            let left = infer_type(&call.args[0], scope)?;
            let right = infer_type(&call.args[1], scope)?;
            if !types_overlap(&left, &right) || !is_orderable(&left) || !is_orderable(&right) {
                return Err(PredicateError::new(
                    "CEL ordering operands must have compatible string or numeric types",
                ));
            }
            Ok(ValueType::Boolean)
        }
        "size" => {
            require_arity(call, 1)?;
            let argument = infer_type(&call.args[0], scope)?;
            if !supports_size(&argument) {
                return Err(PredicateError::new(
                    "CEL size() requires a string, array, or object operand",
                ));
            }
            Ok(ValueType::Integer)
        }
        name => Err(PredicateError::new(format!(
            "CEL function '{name}' is outside the typed vNext predicate profile"
        ))),
    }
}

fn require_arity(call: &CallExpr, expected: usize) -> Result<(), PredicateError> {
    if call.args.len() == expected {
        Ok(())
    } else {
        Err(PredicateError::new(format!(
            "CEL function '{}' requires {expected} argument(s)",
            call.func_name
        )))
    }
}

fn literal_type(literal: &LiteralValue) -> Result<ValueType, PredicateError> {
    literal_json(literal).map(ValueType::Literal)
}

fn literal_json(literal: &LiteralValue) -> Result<Value, PredicateError> {
    match literal {
        LiteralValue::Boolean(value) => Ok(Value::Bool(*value.inner())),
        LiteralValue::Double(value) => Number::from_f64(*value.inner())
            .map(Value::Number)
            .ok_or_else(|| PredicateError::new("CEL contains a non-finite number literal")),
        LiteralValue::Int(value) => Ok(Value::Number(Number::from(*value.inner()))),
        LiteralValue::Null => Ok(Value::Null),
        LiteralValue::String(value) => Ok(Value::String(value.inner().to_string())),
        LiteralValue::UInt(value) => Ok(Value::Number(Number::from(*value.inner()))),
        LiteralValue::Bytes(_) => Err(PredicateError::new(
            "CEL byte literals are outside the typed vNext predicate profile",
        )),
    }
}

fn types_overlap(left: &ValueType, right: &ValueType) -> bool {
    left.is_assignable_to(right) || right.is_assignable_to(left)
}

fn is_orderable(value_type: &ValueType) -> bool {
    match value_type {
        ValueType::Integer | ValueType::Number | ValueType::String => true,
        ValueType::Literal(Value::Number(_) | Value::String(_)) => true,
        ValueType::Union(variants) => !variants.is_empty() && variants.iter().all(is_orderable),
        _ => false,
    }
}

fn supports_size(value_type: &ValueType) -> bool {
    match value_type {
        ValueType::String | ValueType::Array(_) | ValueType::Object(_) => true,
        ValueType::Literal(Value::String(_)) => true,
        ValueType::Union(variants) => !variants.is_empty() && variants.iter().all(supports_size),
        _ => false,
    }
}

fn apply_conjunctive_narrowings(
    expression: &IdedExpr,
    scope: &mut BTreeMap<Identifier, ValueType>,
) {
    let Expr::Call(call) = &expression.expr else {
        return;
    };
    if call.func_name == operators::LOGICAL_AND && call.target.is_none() && call.args.len() == 2 {
        apply_conjunctive_narrowings(&call.args[0], scope);
        apply_conjunctive_narrowings(&call.args[1], scope);
        return;
    }
    if call.func_name != operators::EQUALS || call.target.is_some() || call.args.len() != 2 {
        return;
    }

    let constraint = scope_path(&call.args[0])
        .and_then(|path| literal(&call.args[1]).map(|literal| (path, literal)))
        .or_else(|| {
            scope_path(&call.args[1])
                .and_then(|path| literal(&call.args[0]).map(|literal| (path, literal)))
        });
    let Some((path, expected)) = constraint else {
        return;
    };
    let Some((binding_name, nested_path)) = path.split_first() else {
        return;
    };
    if nested_path.is_empty() {
        return;
    }
    let Ok(binding) = Identifier::parse(binding_name) else {
        return;
    };
    let Some(current) = scope.get(&binding) else {
        return;
    };
    if let Some(narrowed) = narrow_at_path(current, nested_path, &expected) {
        scope.insert(binding, narrowed);
    }
}

fn scope_path(expression: &IdedExpr) -> Option<Vec<String>> {
    match &expression.expr {
        Expr::Ident(root) if root == "scope" => Some(Vec::new()),
        Expr::Select(select) if !select.test => {
            let mut path = scope_path(&select.operand)?;
            path.push(select.field.clone());
            Some(path)
        }
        _ => None,
    }
}

fn literal(expression: &IdedExpr) -> Option<Value> {
    let Expr::Literal(value) = &expression.expr else {
        return None;
    };
    literal_json(value).ok()
}

fn narrow_at_path(value_type: &ValueType, path: &[String], expected: &Value) -> Option<ValueType> {
    if path.len() == 1 {
        return value_type.narrow_discriminator(&path[0], expected).ok();
    }
    match value_type {
        ValueType::Object(object) => {
            let property = object.properties.get(&path[0])?;
            let narrowed = narrow_at_path(&property.value_type, &path[1..], expected)?;
            let mut object = object.clone();
            object.properties.get_mut(&path[0])?.value_type = narrowed;
            Some(ValueType::Object(object))
        }
        ValueType::Union(variants) => {
            let narrowed = variants
                .iter()
                .filter_map(|variant| narrow_at_path(variant, path, expected))
                .collect::<Vec<_>>();
            ValueType::unify(narrowed).ok()
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use serde_json::json;

    use super::analyze_predicate;
    use crate::dsl::vnext::{
        types::{ObjectType, PropertyType, SchemaType, ValueType},
        value::Identifier,
    };

    fn id(value: &str) -> Identifier {
        Identifier::parse(value).unwrap()
    }

    fn settled() -> ValueType {
        SchemaType::compile(&json!({
            "oneOf": [
                {
                    "type": "object",
                    "required": ["status", "value"],
                    "properties": {
                        "status": {"const": "ok"},
                        "value": {"type": "string"}
                    },
                    "additionalProperties": false
                },
                {
                    "type": "object",
                    "required": ["status", "error"],
                    "properties": {
                        "status": {"const": "error"},
                        "error": {"type": "string"}
                    },
                    "additionalProperties": false
                }
            ]
        }))
        .unwrap()
        .into_value_type()
    }

    #[test]
    fn conjunctive_status_checks_narrow_each_capture() {
        let scope = BTreeMap::from([(id("technical"), settled()), (id("risk"), settled())]);
        let analysis = analyze_predicate(
            "scope.technical.status == 'ok' && scope.risk.status == 'ok'",
            &scope,
        )
        .unwrap();

        assert_eq!(
            analysis
                .narrowed_scope
                .get(&id("technical"))
                .unwrap()
                .require_decoded_segments(["value"])
                .unwrap(),
            ValueType::String
        );
        assert_eq!(
            analysis
                .narrowed_scope
                .get(&id("risk"))
                .unwrap()
                .require_decoded_segments(["value"])
                .unwrap(),
            ValueType::String
        );
    }

    #[test]
    fn rejects_non_boolean_unknown_root_and_unknown_field() {
        let scope = BTreeMap::from([(id("answer"), ValueType::String)]);
        assert!(analyze_predicate("scope.answer", &scope).is_err());
        assert!(analyze_predicate("input.answer == 'yes'", &scope).is_err());
        assert!(analyze_predicate("scope.missing == 'yes'", &scope).is_err());
    }

    #[test]
    fn supports_size_profile_used_by_checked_in_agents() {
        let messages = ValueType::Array(super::super::types::ArrayType {
            items: Box::new(ValueType::String),
            min_items: 0,
        });
        let scope = BTreeMap::from([(id("messages"), messages)]);
        analyze_predicate("size(scope.messages) > 0", &scope).unwrap();
    }

    #[test]
    fn nested_discriminator_rebuilds_the_containing_object() {
        let nested = ValueType::Object(ObjectType {
            properties: BTreeMap::from([(
                "result".to_string(),
                PropertyType {
                    value_type: settled(),
                    required: true,
                },
            )]),
            additional_properties: None,
        });
        let scope = BTreeMap::from([(id("container"), nested)]);
        let analysis = analyze_predicate("scope.container.result.status == 'ok'", &scope).unwrap();
        assert_eq!(
            analysis
                .narrowed_scope
                .get(&id("container"))
                .unwrap()
                .require_decoded_segments(["result", "value"])
                .unwrap(),
            ValueType::String
        );
    }
}
