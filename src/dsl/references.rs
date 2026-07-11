use std::collections::BTreeSet;

use handlebars::{
    template::{HelperTemplate, Parameter, TemplateElement},
    Template,
};

use crate::dsl::CompileError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum NodeReferencePath {
    None,
    Reference(String),
    Invalid,
}

pub(crate) fn is_dsl_identifier(value: &str) -> bool {
    let mut characters = value.chars();
    let Some(first) = characters.next() else {
        return false;
    };
    (first.is_ascii_alphabetic() || first == '_')
        && characters.all(|character| character.is_ascii_alphanumeric() || character == '_')
}

pub(crate) fn validate_node_id(node_id: &str) -> Result<(), CompileError> {
    if is_dsl_identifier(node_id) {
        return Ok(());
    }
    Err(CompileError::new(
        "NODE_ID_INVALID",
        format!("node id '{node_id}' must match [A-Za-z_][A-Za-z0-9_]*"),
    ))
}

pub(crate) fn classify_node_reference_path(path: &str) -> NodeReferencePath {
    if path == "nodes" || path.starts_with("nodes[") {
        return NodeReferencePath::Invalid;
    }
    let Some(rest) = path.strip_prefix("nodes.") else {
        return NodeReferencePath::None;
    };
    let mut parts = rest.split('.');
    let Some(node_id) = parts.next() else {
        return NodeReferencePath::Invalid;
    };
    let Some(output) = parts.next() else {
        return NodeReferencePath::Invalid;
    };
    if !is_dsl_identifier(node_id) || output != "output" {
        return NodeReferencePath::Invalid;
    }
    NodeReferencePath::Reference(node_id.to_string())
}

pub(crate) fn extract_handlebars_references(
    template: &Template,
    owner: &str,
    field: &str,
) -> Result<BTreeSet<String>, CompileError> {
    let mut references = BTreeSet::new();
    for element in &template.elements {
        collect_handlebars_element(element, &mut references, owner, field)?;
    }
    Ok(references)
}

pub(crate) fn handlebars_static_text(template: &Template) -> Option<String> {
    let mut output = String::new();
    for element in &template.elements {
        match element {
            TemplateElement::RawString(value) => output.push_str(value),
            TemplateElement::Comment(_) => {}
            _ => return None,
        }
    }
    Some(output)
}

fn collect_handlebars_element(
    element: &TemplateElement,
    references: &mut BTreeSet<String>,
    owner: &str,
    field: &str,
) -> Result<(), CompileError> {
    match element {
        TemplateElement::RawString(_) | TemplateElement::Comment(_) => Ok(()),
        TemplateElement::Expression(template)
        | TemplateElement::HtmlExpression(template)
        | TemplateElement::HelperBlock(template) => {
            collect_handlebars_helper(template, references, owner, field)
        }
        TemplateElement::DecoratorExpression(template)
        | TemplateElement::DecoratorBlock(template)
        | TemplateElement::PartialExpression(template)
        | TemplateElement::PartialBlock(template) => {
            collect_handlebars_parameter(&template.name, references, owner, field)?;
            for parameter in &template.params {
                collect_handlebars_parameter(parameter, references, owner, field)?;
            }
            for parameter in template.hash.values() {
                collect_handlebars_parameter(parameter, references, owner, field)?;
            }
            if let Some(template) = &template.template {
                for element in &template.elements {
                    collect_handlebars_element(element, references, owner, field)?;
                }
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

fn collect_handlebars_helper(
    template: &HelperTemplate,
    references: &mut BTreeSet<String>,
    owner: &str,
    field: &str,
) -> Result<(), CompileError> {
    collect_handlebars_parameter(&template.name, references, owner, field)?;
    for parameter in &template.params {
        collect_handlebars_parameter(parameter, references, owner, field)?;
    }
    for parameter in template.hash.values() {
        collect_handlebars_parameter(parameter, references, owner, field)?;
    }
    if let Some(template) = &template.template {
        for element in &template.elements {
            collect_handlebars_element(element, references, owner, field)?;
        }
    }
    if let Some(template) = &template.inverse {
        for element in &template.elements {
            collect_handlebars_element(element, references, owner, field)?;
        }
    }
    Ok(())
}

fn collect_handlebars_parameter(
    parameter: &Parameter,
    references: &mut BTreeSet<String>,
    owner: &str,
    field: &str,
) -> Result<(), CompileError> {
    match parameter {
        Parameter::Name(name) => collect_handlebars_path(name, references, owner, field),
        Parameter::Path(_) => {
            if let Some(path) = parameter.as_name() {
                collect_handlebars_path(path, references, owner, field)
            } else {
                Ok(())
            }
        }
        Parameter::Literal(_) => Ok(()),
        Parameter::Subexpression(expression) => {
            collect_handlebars_element(&expression.element, references, owner, field)
        }
        _ => Ok(()),
    }
}

fn collect_handlebars_path(
    path: &str,
    references: &mut BTreeSet<String>,
    owner: &str,
    field: &str,
) -> Result<(), CompileError> {
    match classify_node_reference_path(path) {
        NodeReferencePath::None => Ok(()),
        NodeReferencePath::Reference(node_id) => {
            references.insert(node_id);
            Ok(())
        }
        NodeReferencePath::Invalid => Err(CompileError::new(
            "TEMPLATE_REFERENCE_INVALID",
            format!("template '{owner}.{field}' must use nodes.<node_id>.output references"),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::{classify_node_reference_path, is_dsl_identifier, NodeReferencePath};

    #[test]
    fn validates_dsl_identifiers() {
        for value in ["node", "node_1", "_start", "A0"] {
            assert!(is_dsl_identifier(value), "{value} should be valid");
        }
        for value in ["", "1node", "node-name", "node.name", "node name", "节点"] {
            assert!(!is_dsl_identifier(value), "{value} should be invalid");
        }
    }

    #[test]
    fn classifies_canonical_node_output_paths() {
        assert_eq!(
            classify_node_reference_path("nodes.prepare.output"),
            NodeReferencePath::Reference("prepare".to_string())
        );
        assert_eq!(
            classify_node_reference_path("nodes.prepare.output.text"),
            NodeReferencePath::Reference("prepare".to_string())
        );
        assert_eq!(
            classify_node_reference_path("input.question"),
            NodeReferencePath::None
        );
    }

    #[test]
    fn rejects_non_canonical_nodes_paths() {
        for value in [
            "nodes",
            "nodes.prepare",
            "nodes.prepare.value",
            "nodes.prepare[\"output\"]",
            "nodes[\"prepare\"].output",
            "nodes.prepare-name.output",
        ] {
            assert_eq!(
                classify_node_reference_path(value),
                NodeReferencePath::Invalid,
                "{value} should be invalid"
            );
        }
    }
}
