use std::collections::BTreeSet;

use serde_json::Value;

use crate::engine::plan::{
    expression::{MatchProgram, MatchValue, TemplatePart, ValueProgram},
    DataPortId, ExpressionLanguage, Node, NodeKind, PlanIndex, PortDirection, PureExpression,
    ValueSource,
};
use crate::engine::ActivationId;

use super::{
    LogicalOccurrence, RuntimeValue, SchedulerError, SchedulerFacts, SCHEDULER_EXPRESSION_INVALID,
    SCHEDULER_FACT_MISSING, SCHEDULER_GRAPH_INVALID, SCHEDULER_VALUE_TYPE_MISMATCH,
};

pub(crate) struct DataResolver<'index, 'plan, 'facts> {
    index: &'index PlanIndex<'plan>,
    facts: &'facts SchedulerFacts,
    occurrence: Option<LogicalOccurrence>,
}

impl<'index, 'plan, 'facts> DataResolver<'index, 'plan, 'facts> {
    pub(crate) fn for_occurrence(
        index: &'index PlanIndex<'plan>,
        facts: &'facts SchedulerFacts,
        occurrence: &LogicalOccurrence,
    ) -> Self {
        Self {
            index,
            facts,
            occurrence: Some(occurrence.clone()),
        }
    }

    pub(crate) fn resolve_input(
        &self,
        input: &DataPortId,
        evaluating_node: &Node,
    ) -> Result<RuntimeValue, SchedulerError> {
        self.resolve_input_if_present(input, evaluating_node)?
            .ok_or_else(|| {
                SchedulerError::new(
                    SCHEDULER_FACT_MISSING,
                    "required data input has no committed value",
                )
            })
    }

    /// Resolves a data input without collapsing source absence into JSON
    /// `null`. Only a non-required DataPort may remain absent; explicit null
    /// and empty-string values are returned as ordinary RuntimeValues.
    pub(crate) fn resolve_input_if_present(
        &self,
        input: &DataPortId,
        evaluating_node: &Node,
    ) -> Result<Option<RuntimeValue>, SchedulerError> {
        let port = self.index.data_port(input).ok_or_else(|| {
            SchedulerError::new(
                SCHEDULER_GRAPH_INVALID,
                "data input is absent from PlanIndex",
            )
        })?;
        if port.direction() != PortDirection::Input || port.owner() != evaluating_node.id() {
            return Err(SchedulerError::new(
                SCHEDULER_GRAPH_INVALID,
                "data input is not owned by the evaluating node",
            ));
        }
        let value = if let Some(source) = self.index.source_for_input(input) {
            let mut resolving = BTreeSet::new();
            self.resolve_source_if_present(source, evaluating_node, &mut resolving)?
        } else {
            None
        };
        let Some(value) = enforce_input_presence(value, port.required())? else {
            return Ok(None);
        };
        require_type(&value, port.value_type(), "resolved input")?;
        Ok(Some(value))
    }

    /// Resolve an input and independently derive the exact committed
    /// Activation identities that supplied it. Values remain the compatibility
    /// contract; identities are durable Redrive closure evidence.
    pub(crate) fn resolve_input_with_dependencies(
        &self,
        input: &DataPortId,
        evaluating_node: &Node,
    ) -> Result<Option<(RuntimeValue, BTreeSet<ActivationId>)>, SchedulerError> {
        let Some(value) = self.resolve_input_if_present(input, evaluating_node)? else {
            return Ok(None);
        };
        let source = self.index.source_for_input(input).ok_or_else(|| {
            SchedulerError::new(SCHEDULER_FACT_MISSING, "required data input is unbound")
        })?;
        let mut resolving = BTreeSet::new();
        let mut dependencies = BTreeSet::new();
        self.collect_source_activations(
            source,
            evaluating_node,
            &mut resolving,
            &mut dependencies,
        )?;
        Ok(Some((value, dependencies)))
    }

    fn collect_source_activations(
        &self,
        source: &ValueSource,
        evaluating_node: &Node,
        resolving: &mut BTreeSet<DataPortId>,
        dependencies: &mut BTreeSet<ActivationId>,
    ) -> Result<(), SchedulerError> {
        match source {
            ValueSource::RunInput { .. }
            | ValueSource::OptionalRunInput { .. }
            | ValueSource::Literal { .. } => Ok(()),
            ValueSource::Port { port_id } => {
                self.collect_port_activations(port_id, evaluating_node, resolving, dependencies)
            }
            ValueSource::Expression { expression } => {
                for port_id in expression.dependencies.values() {
                    self.collect_port_activations(
                        port_id,
                        evaluating_node,
                        resolving,
                        dependencies,
                    )?;
                }
                Ok(())
            }
        }
    }

    fn collect_port_activations(
        &self,
        port_id: &DataPortId,
        evaluating_node: &Node,
        resolving: &mut BTreeSet<DataPortId>,
        dependencies: &mut BTreeSet<ActivationId>,
    ) -> Result<(), SchedulerError> {
        if !resolving.insert(port_id.clone()) {
            return Err(SchedulerError::new(
                SCHEDULER_GRAPH_INVALID,
                "data dependency evidence encountered a cycle",
            ));
        }
        let result = (|| {
            let port = self.index.data_port(port_id).ok_or_else(|| {
                SchedulerError::new(SCHEDULER_GRAPH_INVALID, "referenced data port is missing")
            })?;
            if port.direction() == PortDirection::Input {
                let source = self.index.source_for_input(port_id).ok_or_else(|| {
                    SchedulerError::new(SCHEDULER_FACT_MISSING, "referenced data input is unbound")
                })?;
                return self.collect_source_activations(
                    source,
                    evaluating_node,
                    resolving,
                    dependencies,
                );
            }

            if let Some(phi) = self.index.phi_for_output(port_id) {
                let merge = self.index.node(phi.merge_node_id()).ok_or_else(|| {
                    SchedulerError::new(
                        SCHEDULER_GRAPH_INVALID,
                        "Phi owner is absent from PlanIndex",
                    )
                })?;
                let selected = self
                    .occurrence
                    .as_ref()
                    .and_then(|occurrence| self.facts.branch_selection_at(merge.id(), occurrence))
                    .or_else(|| self.facts.branch_selections().get(merge.id()))
                    .ok_or_else(|| {
                        SchedulerError::new(
                            SCHEDULER_FACT_MISSING,
                            "Phi dependency has no committed Branch selection",
                        )
                    })?;
                let source = phi.sources().get(selected).ok_or_else(|| {
                    SchedulerError::new(
                        SCHEDULER_GRAPH_INVALID,
                        "Phi dependency has no selected source",
                    )
                })?;
                return self.collect_source_activations(
                    source,
                    evaluating_node,
                    resolving,
                    dependencies,
                );
            }

            let owner_node = self.index.node(port.owner()).ok_or_else(|| {
                SchedulerError::new(SCHEDULER_GRAPH_INVALID, "data-port owner is missing")
            })?;
            if self.index.leaf_descriptor(owner_node.id()).is_some()
                || matches!(
                    owner_node.kind(),
                    NodeKind::SubflowCall(_)
                        | NodeKind::HumanTask(_)
                        | NodeKind::WaitSignal(_)
                        | NodeKind::Timer(_)
                )
            {
                if let Some(owner) = self
                    .occurrence
                    .as_ref()
                    .and_then(|occurrence| self.facts.value_owner_at(port_id, occurrence))
                {
                    dependencies.insert(owner.clone());
                }
                return Ok(());
            }

            let mut traversed = false;
            for input_id in self.index.data_inputs(owner_node.id()) {
                let Some(source) = self.index.source_for_input(input_id) else {
                    continue;
                };
                traversed = true;
                self.collect_source_activations(source, owner_node, resolving, dependencies)?;
            }
            // Collect/Map/Loop outputs may be runtime aggregates rather than
            // ordinary data bindings. Retain their native owner as an explicit
            // closed dependency so downstream reuse fails closed until that
            // structural class gains a first-class reusable candidate.
            if !traversed {
                if let Some(owner) = self
                    .occurrence
                    .as_ref()
                    .and_then(|occurrence| self.facts.value_owner_at(port_id, occurrence))
                {
                    dependencies.insert(owner.clone());
                }
            }
            Ok(())
        })();
        resolving.remove(port_id);
        result
    }

    pub(crate) fn evaluate_expression(
        &self,
        expression: &PureExpression,
        evaluating_node: &Node,
    ) -> Result<RuntimeValue, SchedulerError> {
        let mut resolving = BTreeSet::new();
        self.resolve_expression(expression, evaluating_node, &mut resolving)
    }

    pub(crate) fn resolve_phi(
        &self,
        output: &DataPortId,
        merge_node: &Node,
    ) -> Result<RuntimeValue, SchedulerError> {
        let phi = self.index.phi_for_output(output).ok_or_else(|| {
            SchedulerError::new(SCHEDULER_GRAPH_INVALID, "Merge output has no Phi binding")
        })?;
        if phi.merge_node_id() != merge_node.id() {
            return Err(SchedulerError::new(
                SCHEDULER_GRAPH_INVALID,
                "Phi binding belongs to another Merge",
            ));
        }
        let NodeKind::Merge(merge) = merge_node.kind() else {
            return Err(SchedulerError::new(
                SCHEDULER_GRAPH_INVALID,
                "Phi binding target is not a Merge",
            ));
        };
        let selected = self
            .occurrence
            .as_ref()
            .and_then(|occurrence| {
                self.facts
                    .branch_selection_at(&merge.branch_node_id, occurrence)
            })
            .or_else(|| self.facts.branch_selections().get(&merge.branch_node_id))
            .ok_or_else(|| {
                SchedulerError::new(
                    SCHEDULER_FACT_MISSING,
                    "Merge correlation has no committed Branch selection",
                )
            })?;
        let source = phi.sources().get(selected).ok_or_else(|| {
            SchedulerError::new(
                SCHEDULER_GRAPH_INVALID,
                "Phi binding has no source for the committed Branch selection",
            )
        })?;
        let mut resolving = BTreeSet::new();
        let value = self.resolve_source(source, merge_node, &mut resolving)?;
        let port = self.index.data_port(output).ok_or_else(|| {
            SchedulerError::new(SCHEDULER_GRAPH_INVALID, "Phi output port is missing")
        })?;
        require_type(&value, port.value_type(), "Phi result")?;
        Ok(value)
    }

    fn resolve_source(
        &self,
        source: &ValueSource,
        evaluating_node: &Node,
        resolving: &mut BTreeSet<DataPortId>,
    ) -> Result<RuntimeValue, SchedulerError> {
        self.resolve_source_if_present(source, evaluating_node, resolving)?
            .ok_or_else(|| {
                SchedulerError::new(
                    SCHEDULER_FACT_MISSING,
                    "optional data source is absent in a required value context",
                )
            })
    }

    fn resolve_source_if_present(
        &self,
        source: &ValueSource,
        evaluating_node: &Node,
        resolving: &mut BTreeSet<DataPortId>,
    ) -> Result<Option<RuntimeValue>, SchedulerError> {
        match source {
            ValueSource::RunInput { path } => {
                resolve_run_input(self.facts.run_input(), path).map(Some)
            }
            ValueSource::OptionalRunInput { path } => {
                resolve_optional_run_input(self.facts.run_input(), path)
            }
            ValueSource::Port { port_id } => self
                .resolve_port(port_id, evaluating_node, resolving)
                .map(Some),
            ValueSource::Literal { value } => RuntimeValue::new(value.clone()).map(Some),
            ValueSource::Expression { expression } => self
                .resolve_expression(expression, evaluating_node, resolving)
                .map(Some),
        }
    }

    fn resolve_port(
        &self,
        port_id: &DataPortId,
        evaluating_node: &Node,
        resolving: &mut BTreeSet<DataPortId>,
    ) -> Result<RuntimeValue, SchedulerError> {
        if !resolving.insert(port_id.clone()) {
            return Err(SchedulerError::new(
                SCHEDULER_GRAPH_INVALID,
                "data resolution encountered a dependency cycle",
            ));
        }
        let result = (|| {
            let port = self.index.data_port(port_id).ok_or_else(|| {
                SchedulerError::new(SCHEDULER_GRAPH_INVALID, "referenced data port is missing")
            })?;
            let value = match port.direction() {
                PortDirection::Input => {
                    let source = self.index.source_for_input(port_id).ok_or_else(|| {
                        SchedulerError::new(
                            SCHEDULER_FACT_MISSING,
                            "referenced data input is unbound",
                        )
                    })?;
                    self.resolve_source(source, evaluating_node, resolving)?
                }
                PortDirection::Output => {
                    if let Some(value) = self
                        .occurrence
                        .as_ref()
                        .and_then(|occurrence| self.facts.value_at(port_id, occurrence))
                        .or_else(|| self.facts.values().get(port_id))
                    {
                        value.clone()
                    } else if let Some(phi) = self.index.phi_for_output(port_id) {
                        let merge = self.index.node(phi.merge_node_id()).ok_or_else(|| {
                            SchedulerError::new(
                                SCHEDULER_GRAPH_INVALID,
                                "Phi owner is absent from PlanIndex",
                            )
                        })?;
                        self.resolve_phi(port_id, merge)?
                    } else {
                        return Err(SchedulerError::new(
                            SCHEDULER_FACT_MISSING,
                            "referenced node output is not committed",
                        ));
                    }
                }
            };
            require_type(&value, port.value_type(), "referenced port value")?;
            Ok(value)
        })();
        resolving.remove(port_id);
        result
    }

    fn resolve_expression(
        &self,
        expression: &PureExpression,
        evaluating_node: &Node,
        resolving: &mut BTreeSet<DataPortId>,
    ) -> Result<RuntimeValue, SchedulerError> {
        let value = match expression.language {
            ExpressionLanguage::Literal => {
                let value: Value = serde_json::from_str(&expression.source).map_err(|_| {
                    SchedulerError::new(
                        SCHEDULER_EXPRESSION_INVALID,
                        "literal expression could not be decoded",
                    )
                })?;
                RuntimeValue::new(value)?
            }
            ExpressionLanguage::Cel => {
                let program = cel::Program::compile(&expression.source).map_err(|error| {
                    SchedulerError::new(
                        SCHEDULER_EXPRESSION_INVALID,
                        format!("verified CEL source no longer parses: {error}"),
                    )
                })?;
                let mut context = cel::Context::default();
                for (name, port) in &expression.dependencies {
                    let value = self.resolve_port(port, evaluating_node, resolving)?;
                    context
                        .add_variable(name, value.value().clone())
                        .map_err(|error| {
                            SchedulerError::new(
                                SCHEDULER_EXPRESSION_INVALID,
                                format!("CEL dependency cannot be serialized: {error}"),
                            )
                        })?;
                }
                let value = program.execute(&context).map_err(|error| {
                    SchedulerError::new(
                        SCHEDULER_EXPRESSION_INVALID,
                        format!("CEL evaluation failed under its fixed engine: {error}"),
                    )
                })?;
                let value = value.json().map_err(|error| {
                    SchedulerError::new(
                        SCHEDULER_EXPRESSION_INVALID,
                        format!("CEL result is outside the JSON value contract: {error}"),
                    )
                })?;
                RuntimeValue::new(value)?
            }
            ExpressionLanguage::Match => {
                let program: MatchProgram =
                    serde_json::from_str(&expression.source).map_err(|error| {
                        SchedulerError::new(
                            SCHEDULER_EXPRESSION_INVALID,
                            format!("verified Match source no longer decodes: {error}"),
                        )
                    })?;
                self.resolve_match_program(&program, expression, evaluating_node, resolving)?
            }
            ExpressionLanguage::Value => {
                let program: ValueProgram =
                    serde_json::from_str(&expression.source).map_err(|error| {
                        SchedulerError::new(
                            SCHEDULER_EXPRESSION_INVALID,
                            format!("verified Value source no longer decodes: {error}"),
                        )
                    })?;
                self.resolve_value_program(&program, expression, evaluating_node, resolving)?
            }
            ExpressionLanguage::Template => {
                return Err(SchedulerError::new(
                    SCHEDULER_EXPRESSION_INVALID,
                    "Template is not published by the scheduler expression contract",
                ));
            }
        };
        require_type(&value, &expression.result_type, "expression result")?;
        Ok(value)
    }

    fn resolve_match_program(
        &self,
        program: &MatchProgram,
        expression: &PureExpression,
        evaluating_node: &Node,
        resolving: &mut BTreeSet<DataPortId>,
    ) -> Result<RuntimeValue, SchedulerError> {
        self.resolve_match(
            &program.selector,
            &program.cases,
            &program.default,
            expression,
            evaluating_node,
            resolving,
        )
    }

    fn resolve_match(
        &self,
        selector: &MatchValue,
        cases: &std::collections::BTreeMap<String, MatchValue>,
        default: &MatchValue,
        expression: &PureExpression,
        evaluating_node: &Node,
        resolving: &mut BTreeSet<DataPortId>,
    ) -> Result<RuntimeValue, SchedulerError> {
        let selector =
            self.resolve_match_value(selector, expression, evaluating_node, resolving)?;
        let Value::String(selector) = selector.value() else {
            return Err(SchedulerError::new(
                SCHEDULER_EXPRESSION_INVALID,
                "verified Match selector did not evaluate to string",
            ));
        };
        let selected = cases.get(selector).unwrap_or(default);
        self.resolve_match_value(selected, expression, evaluating_node, resolving)
    }

    fn resolve_match_value(
        &self,
        value: &MatchValue,
        expression: &PureExpression,
        evaluating_node: &Node,
        resolving: &mut BTreeSet<DataPortId>,
    ) -> Result<RuntimeValue, SchedulerError> {
        match value {
            MatchValue::Dependency { name } => {
                let port = expression.dependencies.get(name).ok_or_else(|| {
                    SchedulerError::new(
                        SCHEDULER_EXPRESSION_INVALID,
                        "Match references an undeclared dependency",
                    )
                })?;
                self.resolve_port(port, evaluating_node, resolving)
            }
            MatchValue::Literal { value } => RuntimeValue::new(value.clone()),
            MatchValue::Match {
                selector,
                cases,
                default,
            } => self.resolve_match(
                selector,
                cases,
                default,
                expression,
                evaluating_node,
                resolving,
            ),
        }
    }

    fn resolve_value_program(
        &self,
        program: &ValueProgram,
        expression: &PureExpression,
        evaluating_node: &Node,
        resolving: &mut BTreeSet<DataPortId>,
    ) -> Result<RuntimeValue, SchedulerError> {
        match program {
            ValueProgram::Dependency { name, path } => {
                let port = expression.dependencies.get(name).ok_or_else(|| {
                    SchedulerError::new(
                        SCHEDULER_EXPRESSION_INVALID,
                        "Value program references an undeclared dependency",
                    )
                })?;
                let value = self.resolve_port(port, evaluating_node, resolving)?;
                project_runtime_value(value, path)
            }
            ValueProgram::Literal { value } => RuntimeValue::new(value.clone()),
            ValueProgram::Array { items } => RuntimeValue::new(Value::Array(
                items
                    .iter()
                    .map(|item| {
                        self.resolve_value_program(item, expression, evaluating_node, resolving)
                            .map(|value| value.value().clone())
                    })
                    .collect::<Result<Vec<_>, _>>()?,
            )),
            ValueProgram::Object { fields } => RuntimeValue::new(Value::Object(
                fields
                    .iter()
                    .map(|(name, value)| {
                        Ok((
                            name.clone(),
                            self.resolve_value_program(
                                value,
                                expression,
                                evaluating_node,
                                resolving,
                            )?
                            .value()
                            .clone(),
                        ))
                    })
                    .collect::<Result<serde_json::Map<_, _>, SchedulerError>>()?,
            )),
            ValueProgram::Template { parts } => {
                let mut rendered = String::new();
                for part in parts {
                    match part {
                        TemplatePart::Text { text } => rendered.push_str(text),
                        TemplatePart::Value { value } => {
                            let value = self.resolve_value_program(
                                value,
                                expression,
                                evaluating_node,
                                resolving,
                            )?;
                            let Value::String(value) = value.value() else {
                                return Err(SchedulerError::new(
                                    SCHEDULER_EXPRESSION_INVALID,
                                    "verified template interpolation did not evaluate to string",
                                ));
                            };
                            rendered.push_str(value);
                        }
                    }
                }
                RuntimeValue::new(Value::String(rendered))
            }
        }
    }
}

fn project_runtime_value(
    value: RuntimeValue,
    path: &[String],
) -> Result<RuntimeValue, SchedulerError> {
    let mut current = value.value();
    for field in path {
        let Value::Object(object) = current else {
            return Err(SchedulerError::new(
                SCHEDULER_EXPRESSION_INVALID,
                "Value projection encountered a non-object runtime value",
            ));
        };
        current = object.get(field).ok_or_else(|| {
            SchedulerError::new(
                SCHEDULER_EXPRESSION_INVALID,
                "Value projection encountered a missing runtime field",
            )
        })?;
    }
    RuntimeValue::new(current.clone())
}

fn resolve_run_input(
    run_input: &RuntimeValue,
    path: &[String],
) -> Result<RuntimeValue, SchedulerError> {
    let mut value = run_input.value();
    for segment in path {
        value = match value {
            Value::Object(object) => object.get(segment),
            Value::Array(array) => segment
                .parse::<usize>()
                .ok()
                .and_then(|index| array.get(index)),
            _ => None,
        }
        .ok_or_else(|| {
            SchedulerError::new(
                SCHEDULER_FACT_MISSING,
                "RunInput path has no committed value",
            )
        })?;
    }
    RuntimeValue::new(value.clone())
}

fn resolve_optional_run_input(
    run_input: &RuntimeValue,
    path: &[String],
) -> Result<Option<RuntimeValue>, SchedulerError> {
    let mut value = run_input.value();
    for segment in path {
        value = match value {
            Value::Object(object) => match object.get(segment) {
                Some(value) => value,
                None => return Ok(None),
            },
            Value::Array(array) => match segment
                .parse::<usize>()
                .ok()
                .and_then(|index| array.get(index))
            {
                Some(value) => value,
                None => return Ok(None),
            },
            _ => {
                return Err(SchedulerError::new(
                    SCHEDULER_FACT_MISSING,
                    "optional RunInput path traversed a present non-container value",
                ));
            }
        };
    }
    RuntimeValue::new(value.clone()).map(Some)
}

fn enforce_input_presence(
    value: Option<RuntimeValue>,
    required: bool,
) -> Result<Option<RuntimeValue>, SchedulerError> {
    if value.is_none() && required {
        return Err(SchedulerError::new(
            SCHEDULER_FACT_MISSING,
            "required data input source is absent",
        ));
    }
    Ok(value)
}

pub(crate) fn require_type(
    value: &RuntimeValue,
    expected: &crate::engine::plan::PlanType,
    subject: &str,
) -> Result<(), SchedulerError> {
    if value.matches(expected) {
        Ok(())
    } else {
        Err(SchedulerError::new(
            SCHEDULER_VALUE_TYPE_MISMATCH,
            format!("{subject} does not match its verified Plan type"),
        ))
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn optional_run_input_distinguishes_absence_from_explicit_values() {
        let missing = RuntimeValue::new(json!({})).unwrap();
        assert_eq!(
            resolve_optional_run_input(&missing, &["image_url".to_owned()]).unwrap(),
            None
        );

        let explicit_null = RuntimeValue::new(json!({"image_url": null})).unwrap();
        assert_eq!(
            resolve_optional_run_input(&explicit_null, &["image_url".to_owned()])
                .unwrap()
                .unwrap()
                .value(),
            &Value::Null
        );

        let explicit_empty = RuntimeValue::new(json!({"image_url": ""})).unwrap();
        assert_eq!(
            resolve_optional_run_input(&explicit_empty, &["image_url".to_owned()])
                .unwrap()
                .unwrap()
                .value(),
            &json!("")
        );
    }

    #[test]
    fn input_presence_omits_only_non_required_ports() {
        assert_eq!(enforce_input_presence(None, false).unwrap(), None);
        assert_eq!(
            enforce_input_presence(None, true).unwrap_err().code(),
            SCHEDULER_FACT_MISSING
        );

        let explicit_null = RuntimeValue::new(Value::Null).unwrap();
        assert_eq!(
            enforce_input_presence(Some(explicit_null.clone()), false)
                .unwrap()
                .unwrap(),
            explicit_null
        );
    }
}
