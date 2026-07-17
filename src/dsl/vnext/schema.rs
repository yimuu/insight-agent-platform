use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error,
    fmt,
};

use serde_json::{Map, Value};

use crate::schema::{compile_schema_2020, JsonSchemaValidator};

use super::{
    shape::{SchemaShape, ShapeError},
    value::Identifier,
};

pub const SCHEMA_DEFS_CONFLICT: &str = "VNEXT_SCHEMA_DEFS_CONFLICT";
pub const SCHEMA_REF_INVALID: &str = "VNEXT_SCHEMA_REF_INVALID";
pub const SCHEMA_REF_UNKNOWN: &str = "VNEXT_SCHEMA_REF_UNKNOWN";
pub const SCHEMA_REF_SIBLINGS: &str = "VNEXT_SCHEMA_REF_SIBLINGS";
pub const SCHEMA_REF_CYCLE: &str = "VNEXT_SCHEMA_REF_CYCLE";
pub const SCHEMA_VALIDATOR_INVALID: &str = "VNEXT_SCHEMA_VALIDATOR_INVALID";

const SINGLE_SCHEMA_KEYWORDS: &[&str] = &[
    "additionalItems",
    "additionalProperties",
    "contains",
    "contentSchema",
    "else",
    "if",
    "items",
    "not",
    "propertyNames",
    "then",
    "unevaluatedItems",
    "unevaluatedProperties",
];
const ARRAY_SCHEMA_KEYWORDS: &[&str] = &["allOf", "anyOf", "oneOf", "prefixItems"];
const MAP_SCHEMA_KEYWORDS: &[&str] = &[
    "$defs",
    "definitions",
    "dependentSchemas",
    "patternProperties",
    "properties",
];

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContractSchemaError {
    code: &'static str,
    message: &'static str,
}

impl ContractSchemaError {
    fn new(code: &'static str, message: &'static str) -> Self {
        Self { code, message }
    }

    pub fn code(&self) -> &'static str {
        self.code
    }

    pub fn message(&self) -> &'static str {
        self.message
    }
}

impl fmt::Display for ContractSchemaError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.message)
    }
}

impl Error for ContractSchemaError {}

/// The two representations of one authored Draft 2020-12 contract.
///
/// The validator retains top-level `$defs` and therefore enforces the authored
/// runtime contract. The expanded schema contains no supported `$ref` nodes and
/// is suitable for the conservative `SchemaType` compiler.
#[derive(Debug, Clone)]
pub struct ContractSchemaBundle {
    validator: JsonSchemaValidator,
    expanded_schema: Value,
}

impl ContractSchemaBundle {
    pub fn compile(
        definitions: &BTreeMap<Identifier, Value>,
        contract_schema: &Value,
    ) -> Result<Self, ContractSchemaError> {
        compile_contract_schema(definitions, contract_schema)
    }

    pub fn validator(&self) -> &JsonSchemaValidator {
        &self.validator
    }

    pub fn validator_document(&self) -> &Value {
        self.validator.document()
    }

    pub fn expanded_schema(&self) -> &Value {
        &self.expanded_schema
    }

    /// Returns the value-refinement-free structural view used for contracts
    /// such as dynamic message list assignability.
    pub fn shape(&self) -> Result<SchemaShape, ShapeError> {
        SchemaShape::compile(&self.expanded_schema)
    }

    pub fn into_parts(self) -> (JsonSchemaValidator, Value) {
        (self.validator, self.expanded_schema)
    }
}

pub fn compile_contract_schema(
    definitions: &BTreeMap<Identifier, Value>,
    contract_schema: &Value,
) -> Result<ContractSchemaBundle, ContractSchemaError> {
    if contract_schema
        .as_object()
        .is_some_and(|object| object.contains_key("$defs"))
    {
        return Err(ContractSchemaError::new(
            SCHEMA_DEFS_CONFLICT,
            "contract schema cannot define its own top-level $defs",
        ));
    }

    let mut contract_targets = BTreeSet::new();
    collect_schema_references(contract_schema, definitions, &mut contract_targets)?;

    let mut graph = BTreeMap::new();
    for (name, schema) in definitions {
        let mut targets = BTreeSet::new();
        collect_schema_references(schema, definitions, &mut targets)?;
        graph.insert(name.clone(), targets);
    }
    reject_reference_cycles(&graph)?;

    let reachable_definitions = reachable_definitions(definitions, &graph, &contract_targets);

    let expanded_schema = expand_schema(
        contract_schema,
        definitions,
        &mut BTreeMap::new(),
        &mut BTreeSet::new(),
    )?;
    let validator_document = inject_definitions(&reachable_definitions, contract_schema);
    let validator = compile_schema_2020(&validator_document).map_err(|_| {
        ContractSchemaError::new(
            SCHEMA_VALIDATOR_INVALID,
            "contract schema is not a valid Draft 2020-12 schema",
        )
    })?;

    Ok(ContractSchemaBundle {
        validator,
        expanded_schema,
    })
}

fn reachable_definitions(
    definitions: &BTreeMap<Identifier, Value>,
    graph: &BTreeMap<Identifier, BTreeSet<Identifier>>,
    roots: &BTreeSet<Identifier>,
) -> BTreeMap<Identifier, Value> {
    let mut reachable = BTreeSet::new();
    let mut pending = roots.iter().cloned().collect::<Vec<_>>();
    while let Some(definition) = pending.pop() {
        if !reachable.insert(definition.clone()) {
            continue;
        }
        if let Some(targets) = graph.get(&definition) {
            pending.extend(targets.iter().cloned());
        }
    }

    definitions
        .iter()
        .filter(|(name, _)| reachable.contains(*name))
        .map(|(name, schema)| (name.clone(), schema.clone()))
        .collect()
}

fn collect_schema_references(
    schema: &Value,
    definitions: &BTreeMap<Identifier, Value>,
    targets: &mut BTreeSet<Identifier>,
) -> Result<(), ContractSchemaError> {
    let Value::Object(object) = schema else {
        return Ok(());
    };

    if let Some(target) = reference_target(object, definitions)? {
        targets.insert(target);
        return Ok(());
    }

    for keyword in SINGLE_SCHEMA_KEYWORDS {
        if let Some(child) = object.get(*keyword) {
            collect_schema_references(child, definitions, targets)?;
        }
    }
    for keyword in ARRAY_SCHEMA_KEYWORDS {
        if let Some(Value::Array(children)) = object.get(*keyword) {
            for child in children {
                collect_schema_references(child, definitions, targets)?;
            }
        }
    }
    for keyword in MAP_SCHEMA_KEYWORDS {
        if let Some(Value::Object(children)) = object.get(*keyword) {
            for child in children.values() {
                collect_schema_references(child, definitions, targets)?;
            }
        }
    }
    if let Some(Value::Object(dependencies)) = object.get("dependencies") {
        for dependency in dependencies.values() {
            if dependency.is_boolean() || dependency.is_object() {
                collect_schema_references(dependency, definitions, targets)?;
            }
        }
    }
    Ok(())
}

fn reference_target(
    object: &Map<String, Value>,
    definitions: &BTreeMap<Identifier, Value>,
) -> Result<Option<Identifier>, ContractSchemaError> {
    let Some(reference) = object.get("$ref") else {
        return Ok(None);
    };
    if object.keys().any(|keyword| {
        !matches!(
            keyword.as_str(),
            "$ref"
                | "default"
                | "minItems"
                | "maxItems"
                | "minLength"
                | "maxLength"
                | "pattern"
                | "enum"
        )
    }) {
        return Err(ContractSchemaError::new(
            SCHEMA_REF_SIBLINGS,
            "schema $ref objects cannot contain sibling keywords",
        ));
    }
    let Some(reference) = reference.as_str() else {
        return Err(invalid_reference());
    };
    let Some(name) = reference.strip_prefix("#/$defs/") else {
        return Err(invalid_reference());
    };
    if name.is_empty() || name.contains('/') || name.contains('~') {
        return Err(invalid_reference());
    }
    let target = Identifier::parse(name).map_err(|_| invalid_reference())?;
    if !definitions.contains_key(&target) {
        return Err(ContractSchemaError::new(
            SCHEMA_REF_UNKNOWN,
            "schema reference targets an unknown top-level definition",
        ));
    }
    Ok(Some(target))
}

fn invalid_reference() -> ContractSchemaError {
    ContractSchemaError::new(
        SCHEMA_REF_INVALID,
        "schema reference must be exactly #/$defs/<Identifier>",
    )
}

fn reject_reference_cycles(
    graph: &BTreeMap<Identifier, BTreeSet<Identifier>>,
) -> Result<(), ContractSchemaError> {
    let mut active = BTreeSet::new();
    let mut complete = BTreeSet::new();
    for definition in graph.keys() {
        visit_definition(definition, graph, &mut active, &mut complete)?;
    }
    Ok(())
}

fn visit_definition(
    definition: &Identifier,
    graph: &BTreeMap<Identifier, BTreeSet<Identifier>>,
    active: &mut BTreeSet<Identifier>,
    complete: &mut BTreeSet<Identifier>,
) -> Result<(), ContractSchemaError> {
    if complete.contains(definition) {
        return Ok(());
    }
    if !active.insert(definition.clone()) {
        return Err(ContractSchemaError::new(
            SCHEMA_REF_CYCLE,
            "top-level schema definitions must not contain reference cycles",
        ));
    }
    if let Some(targets) = graph.get(definition) {
        for target in targets {
            visit_definition(target, graph, active, complete)?;
        }
    }
    active.remove(definition);
    complete.insert(definition.clone());
    Ok(())
}

fn expand_schema(
    schema: &Value,
    definitions: &BTreeMap<Identifier, Value>,
    memoized: &mut BTreeMap<Identifier, Value>,
    active: &mut BTreeSet<Identifier>,
) -> Result<Value, ContractSchemaError> {
    let Value::Object(object) = schema else {
        return Ok(schema.clone());
    };
    if let Some(target) = reference_target(object, definitions)? {
        let expanded = expand_definition(&target, definitions, memoized, active)?;
        return Ok(merge_reference_refinements(expanded, object));
    }

    let mut expanded = object.clone();
    for keyword in SINGLE_SCHEMA_KEYWORDS {
        if let Some(child) = object.get(*keyword) {
            expanded.insert(
                (*keyword).to_string(),
                expand_schema(child, definitions, memoized, active)?,
            );
        }
    }
    for keyword in ARRAY_SCHEMA_KEYWORDS {
        if let Some(Value::Array(children)) = object.get(*keyword) {
            expanded.insert(
                (*keyword).to_string(),
                Value::Array(
                    children
                        .iter()
                        .map(|child| expand_schema(child, definitions, memoized, active))
                        .collect::<Result<Vec<_>, _>>()?,
                ),
            );
        }
    }
    for keyword in MAP_SCHEMA_KEYWORDS {
        if let Some(Value::Object(children)) = object.get(*keyword) {
            let children = children
                .iter()
                .map(|(name, child)| {
                    Ok((
                        name.clone(),
                        expand_schema(child, definitions, memoized, active)?,
                    ))
                })
                .collect::<Result<Map<_, _>, ContractSchemaError>>()?;
            expanded.insert((*keyword).to_string(), Value::Object(children));
        }
    }
    if let Some(Value::Object(dependencies)) = object.get("dependencies") {
        let dependencies = dependencies
            .iter()
            .map(|(name, dependency)| {
                let expanded = if dependency.is_boolean() || dependency.is_object() {
                    expand_schema(dependency, definitions, memoized, active)?
                } else {
                    dependency.clone()
                };
                Ok((name.clone(), expanded))
            })
            .collect::<Result<Map<_, _>, ContractSchemaError>>()?;
        expanded.insert("dependencies".to_string(), Value::Object(dependencies));
    }
    Ok(Value::Object(expanded))
}

fn merge_reference_refinements(expanded: Value, reference: &Map<String, Value>) -> Value {
    let Value::Object(mut expanded) = expanded else {
        return expanded;
    };
    for (keyword, value) in reference {
        match keyword.as_str() {
            "$ref" | "default" => {}
            "minItems" | "minLength" => {
                let merged = expanded
                    .get(keyword)
                    .and_then(Value::as_u64)
                    .zip(value.as_u64())
                    .map_or_else(
                        || value.clone(),
                        |(left, right)| Value::from(left.max(right)),
                    );
                expanded.insert(keyword.clone(), merged);
            }
            "maxItems" | "maxLength" => {
                let merged = expanded
                    .get(keyword)
                    .and_then(Value::as_u64)
                    .zip(value.as_u64())
                    .map_or_else(
                        || value.clone(),
                        |(left, right)| Value::from(left.min(right)),
                    );
                expanded.insert(keyword.clone(), merged);
            }
            "enum" => {
                let merged = match (
                    expanded.get("enum").and_then(Value::as_array),
                    value.as_array(),
                ) {
                    (Some(left), Some(right)) => Value::Array(
                        left.iter()
                            .filter(|candidate| right.contains(candidate))
                            .cloned()
                            .collect(),
                    ),
                    _ => value.clone(),
                };
                expanded.insert(keyword.clone(), merged);
            }
            "pattern" => {
                expanded
                    .entry(keyword.clone())
                    .or_insert_with(|| value.clone());
            }
            _ => unreachable!("reference siblings are allowlisted before expansion"),
        }
    }
    Value::Object(expanded)
}

fn expand_definition(
    definition: &Identifier,
    definitions: &BTreeMap<Identifier, Value>,
    memoized: &mut BTreeMap<Identifier, Value>,
    active: &mut BTreeSet<Identifier>,
) -> Result<Value, ContractSchemaError> {
    if let Some(expanded) = memoized.get(definition) {
        return Ok(expanded.clone());
    }
    if !active.insert(definition.clone()) {
        return Err(ContractSchemaError::new(
            SCHEMA_REF_CYCLE,
            "top-level schema definitions must not contain reference cycles",
        ));
    }
    let schema = definitions.get(definition).ok_or_else(|| {
        ContractSchemaError::new(
            SCHEMA_REF_UNKNOWN,
            "schema reference targets an unknown top-level definition",
        )
    })?;
    let expanded = expand_schema(schema, definitions, memoized, active)?;
    active.remove(definition);
    memoized.insert(definition.clone(), expanded.clone());
    Ok(expanded)
}

fn inject_definitions(definitions: &BTreeMap<Identifier, Value>, contract_schema: &Value) -> Value {
    let definitions = definitions
        .iter()
        .map(|(name, schema)| (name.to_string(), schema.clone()))
        .collect::<Map<_, _>>();
    match contract_schema {
        Value::Object(contract) => {
            let mut document = contract.clone();
            document.insert("$defs".to_string(), Value::Object(definitions));
            Value::Object(document)
        }
        _ => {
            let mut document = Map::new();
            document.insert("$defs".to_string(), Value::Object(definitions));
            document.insert(
                "allOf".to_string(),
                Value::Array(vec![contract_schema.clone()]),
            );
            Value::Object(document)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use serde_json::{json, Value};

    use super::super::shape::SchemaShape;
    use super::super::types::{SchemaType, ValueType};
    use super::{
        compile_contract_schema, ContractSchemaBundle, ContractSchemaError, SCHEMA_DEFS_CONFLICT,
        SCHEMA_REF_CYCLE, SCHEMA_REF_INVALID, SCHEMA_REF_SIBLINGS, SCHEMA_REF_UNKNOWN,
        SCHEMA_VALIDATOR_INVALID,
    };
    use crate::dsl::vnext::value::Identifier;

    fn identifier(value: &str) -> Identifier {
        Identifier::parse(value).unwrap()
    }

    fn definitions(values: &[(&str, Value)]) -> BTreeMap<Identifier, Value> {
        values
            .iter()
            .map(|(name, schema)| (identifier(name), schema.clone()))
            .collect()
    }

    fn assert_code(
        result: Result<ContractSchemaBundle, ContractSchemaError>,
        expected: &'static str,
    ) -> ContractSchemaError {
        let error = result.unwrap_err();
        assert_eq!(error.code(), expected);
        error
    }

    fn contains_ref(value: &Value) -> bool {
        match value {
            Value::Object(object) => {
                object.contains_key("$ref") || object.values().any(contains_ref)
            }
            Value::Array(values) => values.iter().any(contains_ref),
            _ => false,
        }
    }

    #[test]
    fn reuses_definitions_at_arbitrary_schema_positions_and_expands_for_types() {
        let definitions = definitions(&[
            ("DisplayName", json!({"type":"string","minLength":1})),
            (
                "Item",
                json!({
                    "type":"object",
                    "required":["display-name"],
                    "properties":{
                        "display-name":{"$ref":"#/$defs/DisplayName"}
                    },
                    "additionalProperties":false
                }),
            ),
        ]);
        let contract = json!({
            "type":"object",
            "required":["primary","items","choice"],
            "properties":{
                "primary":{"$ref":"#/$defs/Item"},
                "items":{
                    "type":"array",
                    "items":{"$ref":"#/$defs/Item"}
                },
                "choice":{
                    "oneOf":[
                        {"$ref":"#/$defs/DisplayName"},
                        {"type":"null"}
                    ]
                }
            },
            "additionalProperties":false
        });

        let bundle = compile_contract_schema(&definitions, &contract).unwrap();
        assert_eq!(
            bundle.validator_document()["$defs"]["DisplayName"],
            definitions[&identifier("DisplayName")]
        );
        assert!(!contains_ref(bundle.expanded_schema()));
        assert!(matches!(bundle.shape().unwrap(), SchemaShape::Object(_)));
        let static_type = SchemaType::compile(bundle.expanded_schema())
            .unwrap()
            .into_value_type();
        assert_eq!(
            static_type
                .require_path_str("primary/display-name")
                .unwrap(),
            ValueType::String
        );
        assert_eq!(
            static_type
                .require_path_str("items/0/display-name")
                .unwrap_err()
                .code(),
            super::super::types::TYPE_PATH_OPTIONAL_ACCESS
        );
    }

    #[test]
    fn runtime_validator_uses_the_injected_draft_2020_definitions() {
        let definitions = definitions(&[("Name", json!({"type":"string","minLength":2}))]);
        let bundle = ContractSchemaBundle::compile(
            &definitions,
            &json!({
                "type":"object",
                "required":["name"],
                "properties":{"name":{"$ref":"#/$defs/Name"}},
                "additionalProperties":false
            }),
        )
        .unwrap();

        assert!(bundle.validator().is_valid(&json!({"name":"Ada"})));
        assert!(!bundle.validator().is_valid(&json!({"name":"A"})));
        assert!(!bundle
            .validator()
            .is_valid(&json!({"name":"Ada","extra":true})));
    }

    #[test]
    fn validator_document_injects_only_definitions_reachable_from_the_contract() {
        let definitions = definitions(&[
            ("Name", json!({"type":"string"})),
            ("Unused", json!({"type":"integer"})),
        ]);
        let bundle = compile_contract_schema(
            &definitions,
            &json!({
                "type":"object",
                "required":["name"],
                "properties":{"name":{"$ref":"#/$defs/Name"}},
                "additionalProperties":false
            }),
        )
        .unwrap();

        assert_eq!(
            bundle.validator_document()["$defs"],
            json!({"Name":{"type":"string"}})
        );
    }

    #[test]
    fn rejects_unknown_external_and_arbitrary_internal_references() {
        let definitions = definitions(&[("Known", json!({"type":"string"}))]);
        assert_code(
            compile_contract_schema(&definitions, &json!({"$ref":"#/$defs/Missing"})),
            SCHEMA_REF_UNKNOWN,
        );

        for reference in [
            "https://example.invalid/schema.json",
            "#/properties/name",
            "#/$defs/Known/more",
            "#/$defs/display-name",
        ] {
            let error = assert_code(
                compile_contract_schema(&definitions, &json!({"$ref":reference})),
                SCHEMA_REF_INVALID,
            );
            assert!(!error.message().contains(reference));
        }
    }

    #[test]
    fn rejects_ref_siblings_without_echoing_schema_data() {
        let definitions = definitions(&[("Name", json!({"type":"string"}))]);
        let error = assert_code(
            compile_contract_schema(
                &definitions,
                &json!({
                    "type":"object",
                    "properties":{
                        "name":{
                            "$ref":"#/$defs/Name",
                            "description":"sensitive marker"
                        }
                    }
                }),
            ),
            SCHEMA_REF_SIBLINGS,
        );
        assert!(!error.message().contains("sensitive marker"));
    }

    #[test]
    fn allows_default_annotation_next_to_a_local_ref() {
        let definitions = definitions(&[(
            "Config",
            json!({
                "type":"object",
                "required":["mode"],
                "properties":{"mode":{"type":"string"}},
                "additionalProperties":false
            }),
        )]);
        let default = json!({"mode":"safe"});
        let bundle = compile_contract_schema(
            &definitions,
            &json!({"$ref":"#/$defs/Config","default":default}),
        )
        .unwrap();

        assert!(bundle.validator().is_valid(&json!({"mode":"safe"})));
        assert_eq!(bundle.validator_document()["default"], default);
        assert_eq!(
            bundle.expanded_schema(),
            &definitions[&identifier("Config")]
        );
    }

    #[test]
    fn allows_typed_refinements_next_to_a_local_ref() {
        let definitions = definitions(&[(
            "Names",
            json!({"type":"array","items":{"type":"string"},"minItems":1}),
        )]);
        let bundle = compile_contract_schema(
            &definitions,
            &json!({"$ref":"#/$defs/Names","minItems":2,"maxItems":3}),
        )
        .unwrap();

        assert!(!bundle.validator().is_valid(&json!(["one"])));
        assert!(bundle.validator().is_valid(&json!(["one", "two"])));
        assert_eq!(bundle.expanded_schema()["minItems"], json!(2));
        assert_eq!(bundle.expanded_schema()["maxItems"], json!(3));
    }

    #[test]
    fn rejects_definition_reference_cycles_even_when_unused() {
        let definitions = definitions(&[
            ("A", json!({"$ref":"#/$defs/B"})),
            (
                "B",
                json!({
                    "type":"object",
                    "properties":{"next":{"$ref":"#/$defs/A"}}
                }),
            ),
        ]);
        assert_code(
            compile_contract_schema(&definitions, &json!({"type":"null"})),
            SCHEMA_REF_CYCLE,
        );
    }

    #[test]
    fn rejects_contract_owned_defs_and_invalid_runtime_schemas() {
        let definitions = BTreeMap::new();
        assert_code(
            compile_contract_schema(&definitions, &json!({"$defs":{},"type":"object"})),
            SCHEMA_DEFS_CONFLICT,
        );

        let error = assert_code(
            compile_contract_schema(&definitions, &json!({"type":"sensitive-invalid-type"})),
            SCHEMA_VALIDATOR_INVALID,
        );
        assert!(!error.message().contains("sensitive-invalid-type"));
    }
}
