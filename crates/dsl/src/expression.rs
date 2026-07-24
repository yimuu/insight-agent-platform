//! Versioned expression-compiler isolation for DSL Core.
//!
//! Canonical Plan wire v1 currently publishes only primitive CEL constants and
//! one canonical bare dependency. This module is the single choke point for
//! that limitation: unsupported CEL and dynamic `match` fail closed with a
//! stable blocker code instead of being reinterpreted or typed as `Any`.

use std::collections::BTreeSet;

use serde_json::Value;

use crate::CompileError;
use insight_engine::plan::PlanType;

use super::{ast::ValueExpr, EXPRESSION_ENGINE_BLOCKED, INVALID_TYPE};

#[derive(Debug, Clone, PartialEq)]
pub enum RestrictedExpression {
    Literal { value: Value, value_type: PlanType },
    BareName(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompiledCondition {
    pub source: String,
    pub dependencies: BTreeSet<String>,
}

pub fn compile_condition(source: &str) -> Result<CompiledCondition, CompileError> {
    if source.trim() != source || source.is_empty() {
        return Err(CompileError::new(
            INVALID_TYPE,
            "CEL condition must be non-empty and have no surrounding whitespace",
        ));
    }
    let program = cel::Program::compile(source).map_err(|error| {
        CompileError::new(
            INVALID_TYPE,
            format!("CEL condition contains invalid cel-rs 0.14 syntax: {error}"),
        )
    })?;
    Ok(CompiledCondition {
        source: source.to_owned(),
        dependencies: program
            .references()
            .variables()
            .into_iter()
            .map(str::to_owned)
            .collect(),
    })
}

pub fn compile_numeric(value: &ValueExpr) -> Result<RestrictedExpression, CompileError> {
    let value = fold_static_match(value)?;
    match value {
        ValueExpr::Reference(path) if path.segments.is_empty() => {
            Ok(RestrictedExpression::BareName(path.root))
        }
        ValueExpr::Reference(_) => Err(blocked(
            "timer field projection requires the versioned Project expression engine",
        )),
        ValueExpr::Literal(value) if value.is_i64() || value.is_u64() || value.is_f64() => {
            let value_type = if value.is_i64() || value.is_u64() {
                PlanType::Integer
            } else {
                PlanType::Number
            };
            Ok(RestrictedExpression::Literal { value, value_type })
        }
        ValueExpr::Literal(_)
        | ValueExpr::Array(_)
        | ValueExpr::Object(_)
        | ValueExpr::Template(_)
        | ValueExpr::Match(_)
        | ValueExpr::ErrorRef(_) => Err(CompileError::new(
            INVALID_TYPE,
            "timer duration_ms must be a numeric literal or one bare numeric name",
        )),
    }
}

/// Fold matches whose selectors are authored literal strings. Dynamic matches
/// remain explicit and are lowered by the fixed Match expression engine.
pub fn fold_static_match(value: &ValueExpr) -> Result<ValueExpr, CompileError> {
    match value {
        ValueExpr::Reference(name) => Ok(ValueExpr::Reference(name.clone())),
        ValueExpr::Literal(value) => Ok(ValueExpr::Literal(value.clone())),
        ValueExpr::Array(values) => values
            .iter()
            .map(fold_static_match)
            .collect::<Result<Vec<_>, _>>()
            .map(ValueExpr::Array),
        ValueExpr::Object(values) => values
            .iter()
            .map(|(key, value)| Ok((key.clone(), fold_static_match(value)?)))
            .collect::<Result<_, CompileError>>()
            .map(ValueExpr::Object),
        ValueExpr::Template(value) => Ok(ValueExpr::Template(value.clone())),
        ValueExpr::ErrorRef(value) => Ok(ValueExpr::ErrorRef(value.clone())),
        ValueExpr::Match(value) => {
            let selector = fold_static_match(&value.selector)?;
            if let ValueExpr::Literal(Value::String(selector)) = &selector {
                let selected = value.cases.get(selector).unwrap_or(&value.default);
                return fold_static_match(selected);
            }
            Ok(ValueExpr::Match(super::ast::MatchExpr {
                selector: Box::new(selector),
                cases: value
                    .cases
                    .iter()
                    .map(|(key, value)| Ok((key.clone(), fold_static_match(value)?)))
                    .collect::<Result<_, CompileError>>()?,
                default: Box::new(fold_static_match(&value.default)?),
            }))
        }
    }
}

pub fn canonical_json(value: &Value) -> Result<String, CompileError> {
    serde_jcs::to_string(value).map_err(|failure| {
        CompileError::new(
            INVALID_TYPE,
            format!("literal cannot be canonicalized: {failure}"),
        )
    })
}

pub fn literal_type(value: &Value) -> Result<PlanType, CompileError> {
    PlanType::literal(value.clone()).map_err(|failure| {
        CompileError::new(
            INVALID_TYPE,
            format!("literal is outside the canonical type profile: {failure}"),
        )
    })
}

fn blocked(message: &'static str) -> CompileError {
    CompileError::new(EXPRESSION_ENGINE_BLOCKED, message)
}

#[cfg(test)]
mod tests {
    use super::{compile_condition, fold_static_match};
    use crate::ast::parse_value_expr;
    use serde_json::json;

    #[test]
    fn condition_profile_parses_fixed_cel_and_collects_dependencies() {
        let condition = compile_condition("ready && size(messages) > 0").unwrap();
        assert_eq!(condition.source, "ready && size(messages) > 0");
        assert_eq!(
            condition.dependencies.into_iter().collect::<Vec<_>>(),
            ["messages", "ready"]
        );
    }

    #[test]
    fn literal_match_folds_without_creating_control_flow() {
        let expression = parse_value_expr(&json!({
            "match": "image",
            "cases": {"image": "vision"},
            "default": "report"
        }))
        .unwrap();
        assert_eq!(
            fold_static_match(&expression).unwrap(),
            parse_value_expr(&json!("vision")).unwrap()
        );
    }
}
