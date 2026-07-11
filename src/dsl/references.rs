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
