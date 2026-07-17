use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Write as _,
    sync::Arc,
};

use async_trait::async_trait;
use semver::Version;
use serde::Serialize;
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::{
    dsl::{
        vnext::{schema::compile_contract_schema, Identifier},
        CompileError,
    },
    runtime::{ExecutionControl, RunError},
    schema::JsonSchemaValidator,
};

/// A platform capability which must be granted before an action may run.
///
/// Capability names use the same stable, namespace-friendly grammar as action
/// IDs. Construction is intentionally cheap; the registry is the trust
/// boundary which validates every descriptor before publishing it.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
pub struct ActionCapability(&'static str);

impl ActionCapability {
    pub const fn new(value: &'static str) -> Self {
        Self(value)
    }

    pub fn as_str(&self) -> &'static str {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EffectClass {
    Pure,
    ReadOnly,
    Mutating,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum IdempotencyClass {
    Idempotent,
    NonIdempotent,
}

impl IdempotencyClass {
    pub fn is_idempotent(self) -> bool {
        matches!(self, Self::Idempotent)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CancellationClass {
    Cooperative,
    NotSupported,
}

/// The complete, closed metadata which defines one action contract.
///
/// `descriptor_hash` deliberately is not a field here: it is derived by the
/// registry from exactly these fields after validation and is exposed through
/// [`ActionDescriptorIdentity`].
#[derive(Debug, Clone, PartialEq)]
pub struct ActionDescriptor {
    pub id: &'static str,
    pub version: &'static str,
    pub input_schema: Value,
    pub output_schema: Value,
    pub effect: EffectClass,
    pub idempotency: IdempotencyClass,
    pub cancellation: CancellationClass,
    pub required_capabilities: BTreeSet<ActionCapability>,
}

/// Frozen identity consumed by compiled action call plans.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActionDescriptorIdentity {
    pub id: String,
    pub version: Version,
    /// Lower-case hexadecimal SHA-256 of the RFC 8785 canonical descriptor.
    pub descriptor_hash: String,
}

#[derive(Clone)]
pub struct ActionContext {
    pub run_id: String,
    /// Stable qualified operation identity.
    pub operation_id: String,
    pub attempt: u32,
    pub attempt_id: String,
    /// Stable across retry attempts for the same logical operation.
    pub idempotency_key: String,
    pub control: ExecutionControl,
}

impl ActionContext {
    pub fn for_operation(
        run_id: impl Into<String>,
        operation_id: impl Into<String>,
        attempt: u32,
        control: ExecutionControl,
    ) -> Self {
        let run_id = run_id.into();
        let operation_id = operation_id.into();
        Self {
            attempt_id: format!("{run_id}:{operation_id}:{attempt}"),
            idempotency_key: format!("{run_id}:{operation_id}"),
            run_id,
            operation_id,
            attempt,
            control,
        }
    }
}

#[async_trait]
pub trait Action: Send + Sync {
    fn descriptor(&self) -> ActionDescriptor;
    async fn call(&self, input: Value, context: ActionContext) -> Result<Value, RunError>;
}

pub struct RegisteredAction {
    descriptor: ActionDescriptor,
    identity: ActionDescriptorIdentity,
    action: Arc<dyn Action>,
    input_validator: JsonSchemaValidator,
    output_validator: JsonSchemaValidator,
}

impl RegisteredAction {
    pub fn descriptor(&self) -> &ActionDescriptor {
        &self.descriptor
    }

    pub fn identity(&self) -> &ActionDescriptorIdentity {
        &self.identity
    }

    pub fn validate_input(&self, input: &Value) -> Result<(), RunError> {
        validate_json(
            &self.input_validator,
            input,
            "VNEXT_ACTION_INPUT_CONTRACT_INVALID",
            "action input validation failed",
        )
    }

    pub async fn call(&self, input: Value, context: ActionContext) -> Result<Value, RunError> {
        self.validate_input(&input)?;
        let output = self.action.call(input, context).await?;
        validate_json(
            &self.output_validator,
            &output,
            "VNEXT_ACTION_OUTPUT_CONTRACT_INVALID",
            "action output validation failed",
        )?;
        Ok(output)
    }
}

fn validate_json(
    validator: &JsonSchemaValidator,
    value: &Value,
    code: &'static str,
    message: &'static str,
) -> Result<(), RunError> {
    if !validator.is_valid(value) {
        return Err(RunError::operation(code, message));
    }
    Ok(())
}

#[derive(Clone, Default)]
pub struct ActionRegistry {
    actions: BTreeMap<String, Arc<RegisteredAction>>,
}

impl ActionRegistry {
    pub fn register<A>(&mut self, action: A) -> Result<(), CompileError>
    where
        A: Action + 'static,
    {
        let mut descriptor = action.descriptor();
        let id = descriptor.id;
        if !is_qualified_identifier(id) {
            return Err(CompileError::new(
                "ACTION_ID_INVALID",
                "action id must be a valid qualified identifier",
            ));
        }
        if self.actions.contains_key(id) {
            return Err(CompileError::new(
                "DUPLICATE_ACTION",
                format!("action '{id}' is already registered"),
            ));
        }
        let version = Version::parse(descriptor.version).map_err(|_| {
            CompileError::new(
                "ACTION_VERSION_INVALID",
                format!("action '{id}' version is not valid SemVer"),
            )
        })?;
        for capability in &descriptor.required_capabilities {
            if !is_qualified_identifier(capability.as_str()) {
                return Err(CompileError::new(
                    "ACTION_CAPABILITY_INVALID",
                    format!("action '{id}' declares an invalid required capability"),
                ));
            }
        }
        let (input_schema, input_validator) = normalize_action_schema(&descriptor.input_schema)
            .map_err(|error| {
                CompileError::new(
                    "ACTION_INPUT_SCHEMA_INVALID",
                    format!("action '{id}' input schema is invalid: {error}"),
                )
            })?;
        if !is_closed_object_schema(&input_schema) {
            return Err(CompileError::new(
                "ACTION_INPUT_SCHEMA_NOT_CLOSED",
                format!("action '{id}' input schema must describe a closed object"),
            ));
        }
        let (output_schema, output_validator) = normalize_action_schema(&descriptor.output_schema)
            .map_err(|error| {
                CompileError::new(
                    "ACTION_OUTPUT_SCHEMA_INVALID",
                    format!("action '{id}' output schema is invalid: {error}"),
                )
            })?;
        descriptor.input_schema = input_schema;
        descriptor.output_schema = output_schema;
        let descriptor_hash = descriptor_hash(&descriptor)?;
        self.actions.insert(
            id.to_string(),
            Arc::new(RegisteredAction {
                identity: ActionDescriptorIdentity {
                    id: id.to_string(),
                    version,
                    descriptor_hash,
                },
                descriptor,
                action: Arc::new(action),
                input_validator,
                output_validator,
            }),
        );
        Ok(())
    }

    pub fn resolve(&self, id: &str) -> Result<Arc<RegisteredAction>, CompileError> {
        self.actions.get(id).cloned().ok_or_else(|| {
            CompileError::new(
                "ACTION_NOT_FOUND",
                format!("action '{id}' is not registered"),
            )
        })
    }

    pub fn names(&self) -> impl Iterator<Item = &str> {
        self.actions.keys().map(String::as_str)
    }
}

/// Produces the same self-contained schema document consumed by vNext
/// compilation. Local `$defs` references remain local; only the document
/// boundary is normalized, so Draft 2020-12 validation semantics are retained.
fn normalize_action_schema(authored: &Value) -> Result<(Value, JsonSchemaValidator), String> {
    reject_dynamic_schema_references(authored)?;

    let mut contract = authored.clone();
    let definitions = match &mut contract {
        Value::Object(object) => object
            .remove("$defs")
            .map(|definitions| {
                let Value::Object(definitions) = definitions else {
                    return Err("top-level $defs must be an object".to_string());
                };
                definitions
                    .into_iter()
                    .map(|(name, schema)| {
                        Identifier::parse(name)
                            .map(|name| (name, schema))
                            .map_err(|_| {
                                "top-level $defs names must be canonical identifiers".to_string()
                            })
                    })
                    .collect::<Result<BTreeMap<_, _>, _>>()
            })
            .transpose()?
            .unwrap_or_default(),
        _ => BTreeMap::new(),
    };

    let bundle = compile_contract_schema(&definitions, &contract)
        .map_err(|error| error.message().to_string())?;
    let document = bundle.validator_document().clone();
    let (validator, _) = bundle.into_parts();
    debug_assert_eq!(validator.document(), &document);
    Ok((document, validator))
}

fn reject_dynamic_schema_references(value: &Value) -> Result<(), String> {
    match value {
        Value::Object(object) => {
            if object.contains_key("$dynamicRef") || object.contains_key("$recursiveRef") {
                return Err("dynamic JSON Schema references are not supported".to_string());
            }
            for value in object.values() {
                reject_dynamic_schema_references(value)?;
            }
        }
        Value::Array(values) => {
            for value in values {
                reject_dynamic_schema_references(value)?;
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
    }
    Ok(())
}

#[derive(Serialize)]
struct CanonicalActionDescriptor<'a> {
    id: &'a str,
    version: &'a str,
    input_schema: &'a Value,
    output_schema: &'a Value,
    effect: EffectClass,
    idempotency: IdempotencyClass,
    cancellation: CancellationClass,
    required_capabilities: Vec<&'a str>,
}

fn descriptor_hash(descriptor: &ActionDescriptor) -> Result<String, CompileError> {
    // Sorting explicitly by UTF-8 bytes makes the Set -> JSON array rule part
    // of this boundary instead of relying on a collection's incidental order.
    let mut required_capabilities = descriptor
        .required_capabilities
        .iter()
        .map(ActionCapability::as_str)
        .collect::<Vec<_>>();
    required_capabilities.sort_unstable_by(|left, right| left.as_bytes().cmp(right.as_bytes()));

    let canonical = serde_jcs::to_vec(&CanonicalActionDescriptor {
        id: descriptor.id,
        version: descriptor.version,
        input_schema: &descriptor.input_schema,
        output_schema: &descriptor.output_schema,
        effect: descriptor.effect,
        idempotency: descriptor.idempotency,
        cancellation: descriptor.cancellation,
        required_capabilities,
    })
    .map_err(|_| {
        CompileError::new(
            "ACTION_DESCRIPTOR_IDENTITY_INVALID",
            "action descriptor could not be canonicalized",
        )
    })?;
    let digest = Sha256::digest(canonical);
    let mut encoded = String::with_capacity(digest.len() * 2);
    for byte in digest {
        write!(&mut encoded, "{byte:02x}").expect("writing to a String cannot fail");
    }
    Ok(encoded)
}

fn is_qualified_identifier(value: &str) -> bool {
    if value.is_empty() || value.len() > 128 || !value.is_ascii() {
        return false;
    }
    value.split('.').all(|segment| {
        let mut characters = segment.chars();
        matches!(characters.next(), Some(first) if first.is_ascii_alphabetic() || first == '_')
            && characters.all(|character| {
                character.is_ascii_alphanumeric() || matches!(character, '_' | '-')
            })
    })
}

fn is_closed_object_schema(schema: &Value) -> bool {
    let mut references = BTreeSet::new();
    closed_object_facts(schema, schema, &mut references) == (true, true)
}

/// Returns `(object_only, closed)` for the root-level input contract.
fn closed_object_facts(
    schema: &Value,
    root: &Value,
    references: &mut BTreeSet<String>,
) -> (bool, bool) {
    let Some(object) = schema.as_object() else {
        return (false, false);
    };

    let direct_object = object.get("type").and_then(Value::as_str) == Some("object");
    let direct_closed = object.get("additionalProperties") == Some(&Value::Bool(false))
        || object.get("unevaluatedProperties") == Some(&Value::Bool(false));

    let referenced = object
        .get("$ref")
        .and_then(Value::as_str)
        .and_then(|reference| {
            let pointer = reference.strip_prefix('#')?;
            if !references.insert(reference.to_string()) {
                return None;
            }
            let target = root.pointer(pointer)?;
            let facts = closed_object_facts(target, root, references);
            references.remove(reference);
            Some(facts)
        })
        .unwrap_or((false, false));

    (direct_object || referenced.0, direct_closed || referenced.1)
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeSet, HashMap};

    use async_trait::async_trait;
    use semver::Version;
    use serde_json::{json, Map, Value};

    use super::{
        descriptor_hash, normalize_action_schema, Action, ActionCapability, ActionContext,
        ActionDescriptor, ActionRegistry, CancellationClass, EffectClass, IdempotencyClass,
    };
    use crate::runtime::RunError;

    #[derive(Clone)]
    struct StaticAction(ActionDescriptor);

    #[async_trait]
    impl Action for StaticAction {
        fn descriptor(&self) -> ActionDescriptor {
            self.0.clone()
        }

        async fn call(&self, input: Value, _context: ActionContext) -> Result<Value, RunError> {
            Ok(input)
        }
    }

    fn descriptor(
        id: &'static str,
        version: &'static str,
        input_schema: Value,
    ) -> ActionDescriptor {
        ActionDescriptor {
            id,
            version,
            input_schema,
            output_schema: json!({
                "type": "object",
                "additionalProperties": false
            }),
            effect: EffectClass::Pure,
            idempotency: IdempotencyClass::Idempotent,
            cancellation: CancellationClass::NotSupported,
            required_capabilities: BTreeSet::from([
                ActionCapability::new("storage.read"),
                ActionCapability::new("network.https"),
            ]),
        }
    }

    fn closed_input_schema() -> Value {
        json!({
            "type": "object",
            "properties": {"value": {"type": "string"}},
            "additionalProperties": false
        })
    }

    #[test]
    fn registry_freezes_parsed_version_and_rfc8785_sha256_identity() {
        let mut registry = ActionRegistry::default();
        registry
            .register(StaticAction(descriptor(
                "example.identity",
                "1.2.3-alpha.1+build.7",
                closed_input_schema(),
            )))
            .unwrap();

        let identity = registry.resolve("example.identity").unwrap();
        assert_eq!(identity.identity().id, "example.identity");
        assert_eq!(
            identity.identity().version,
            Version::parse("1.2.3-alpha.1+build.7").unwrap()
        );
        assert_eq!(identity.identity().descriptor_hash.len(), 64);
        assert!(identity
            .identity()
            .descriptor_hash
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase()));
    }

    #[test]
    fn descriptor_hash_is_stable_across_json_map_insertion_order() {
        let mut first_properties = Map::new();
        first_properties.insert("alpha".to_string(), json!({"type":"string"}));
        first_properties.insert("beta".to_string(), json!({"type":"integer"}));

        let mut second_properties = Map::new();
        second_properties.insert("beta".to_string(), json!({"type":"integer"}));
        second_properties.insert("alpha".to_string(), json!({"type":"string"}));

        let first_schema = Value::Object(Map::from_iter([
            ("type".to_string(), json!("object")),
            ("properties".to_string(), Value::Object(first_properties)),
            ("additionalProperties".to_string(), Value::Bool(false)),
        ]));
        let second_schema = Value::Object(Map::from_iter([
            ("additionalProperties".to_string(), Value::Bool(false)),
            ("properties".to_string(), Value::Object(second_properties)),
            ("type".to_string(), json!("object")),
        ]));

        let mut first_registry = ActionRegistry::default();
        first_registry
            .register(StaticAction(descriptor(
                "example.canonical",
                "1.0.0",
                first_schema,
            )))
            .unwrap();
        let mut second_registry = ActionRegistry::default();
        second_registry
            .register(StaticAction(descriptor(
                "example.canonical",
                "1.0.0",
                second_schema,
            )))
            .unwrap();

        assert_eq!(
            first_registry
                .resolve("example.canonical")
                .unwrap()
                .identity()
                .descriptor_hash,
            second_registry
                .resolve("example.canonical")
                .unwrap()
                .identity()
                .descriptor_hash
        );
    }

    #[test]
    fn descriptor_hash_normalizes_implicit_and_explicit_empty_definitions_equally() {
        let implicit = closed_input_schema();
        let mut explicit = closed_input_schema();
        explicit
            .as_object_mut()
            .unwrap()
            .insert("$defs".to_string(), json!({}));

        let mut implicit_registry = ActionRegistry::default();
        implicit_registry
            .register(StaticAction(descriptor(
                "example.normalized",
                "1.0.0",
                implicit,
            )))
            .unwrap();
        let mut explicit_registry = ActionRegistry::default();
        explicit_registry
            .register(StaticAction(descriptor(
                "example.normalized",
                "1.0.0",
                explicit,
            )))
            .unwrap();

        assert_eq!(
            implicit_registry
                .resolve("example.normalized")
                .unwrap()
                .identity()
                .descriptor_hash,
            explicit_registry
                .resolve("example.normalized")
                .unwrap()
                .identity()
                .descriptor_hash
        );
    }

    #[test]
    fn descriptor_hash_changes_when_the_normalized_schema_changes() {
        let mut first = closed_input_schema();
        first["properties"]["value"]["minLength"] = json!(1);
        let mut second = closed_input_schema();
        second["properties"]["value"]["minLength"] = json!(2);

        let mut first_registry = ActionRegistry::default();
        first_registry
            .register(StaticAction(descriptor(
                "example.schema_identity",
                "1.0.0",
                first,
            )))
            .unwrap();
        let mut second_registry = ActionRegistry::default();
        second_registry
            .register(StaticAction(descriptor(
                "example.schema_identity",
                "1.0.0",
                second,
            )))
            .unwrap();

        assert_ne!(
            first_registry
                .resolve("example.schema_identity")
                .unwrap()
                .identity()
                .descriptor_hash,
            second_registry
                .resolve("example.schema_identity")
                .unwrap()
                .identity()
                .descriptor_hash
        );
    }

    #[test]
    fn registry_stores_compiles_and_hashes_one_normalized_schema_document() {
        let mut registry = ActionRegistry::default();
        registry
            .register(StaticAction(descriptor(
                "example.one_document",
                "1.0.0",
                closed_input_schema(),
            )))
            .unwrap();

        let registered = registry.resolve("example.one_document").unwrap();
        assert_eq!(registered.descriptor.input_schema["$defs"], json!({}));
        assert_eq!(
            registered.input_validator.document(),
            &registered.descriptor.input_schema
        );
        assert_eq!(
            registered.output_validator.document(),
            &registered.descriptor.output_schema
        );
        assert_eq!(
            registered.identity.descriptor_hash,
            descriptor_hash(&registered.descriptor).unwrap()
        );
        assert_eq!(
            normalize_action_schema(&registered.descriptor.input_schema)
                .unwrap()
                .0,
            registered.descriptor.input_schema
        );
    }

    #[test]
    fn every_closed_metadata_field_participates_in_identity() {
        let base = descriptor("example.metadata", "1.0.0", closed_input_schema());
        let mut hashes = HashMap::new();

        let variants = [
            base.clone(),
            ActionDescriptor {
                version: "1.0.1",
                ..base.clone()
            },
            ActionDescriptor {
                effect: EffectClass::ReadOnly,
                ..base.clone()
            },
            ActionDescriptor {
                idempotency: IdempotencyClass::NonIdempotent,
                ..base.clone()
            },
            ActionDescriptor {
                cancellation: CancellationClass::Cooperative,
                ..base.clone()
            },
            ActionDescriptor {
                required_capabilities: BTreeSet::new(),
                ..base
            },
        ];

        for (index, variant) in variants.into_iter().enumerate() {
            let mut registry = ActionRegistry::default();
            registry.register(StaticAction(variant)).unwrap();
            let hash = registry
                .resolve("example.metadata")
                .unwrap()
                .identity()
                .descriptor_hash
                .clone();
            assert!(hashes.insert(hash, index).is_none());
        }
    }

    #[test]
    fn registry_rejects_invalid_identity_capability_and_open_input_contracts() {
        let cases = [
            (
                descriptor("bad id", "1.0.0", closed_input_schema()),
                "ACTION_ID_INVALID",
            ),
            (
                descriptor("example.bad_version", "1.0", closed_input_schema()),
                "ACTION_VERSION_INVALID",
            ),
            (
                ActionDescriptor {
                    required_capabilities: BTreeSet::from([ActionCapability::new("bad cap")]),
                    ..descriptor("example.bad_capability", "1.0.0", closed_input_schema())
                },
                "ACTION_CAPABILITY_INVALID",
            ),
            (
                descriptor("example.open", "1.0.0", json!({"type":"object"})),
                "ACTION_INPUT_SCHEMA_NOT_CLOSED",
            ),
            (
                descriptor("example.not_object", "1.0.0", json!({"type":"string"})),
                "ACTION_INPUT_SCHEMA_NOT_CLOSED",
            ),
        ];

        for (descriptor, expected_code) in cases {
            let mut registry = ActionRegistry::default();
            let error = registry.register(StaticAction(descriptor)).unwrap_err();
            assert_eq!(error.code(), expected_code);
        }
    }

    #[test]
    fn registry_accepts_a_local_ref_to_a_closed_input_object() {
        let mut registry = ActionRegistry::default();
        registry
            .register(StaticAction(descriptor(
                "example.local_ref",
                "1.0.0",
                json!({
                    "$ref": "#/$defs/Input",
                    "$defs": {
                        "Input": {
                            "type": "object",
                            "additionalProperties": false
                        }
                    }
                }),
            )))
            .unwrap();

        let registered = registry.resolve("example.local_ref").unwrap();
        assert_eq!(
            registered.descriptor().input_schema["$ref"],
            json!("#/$defs/Input")
        );
        assert_eq!(
            registered.descriptor().input_schema["$defs"]["Input"],
            json!({
                "type": "object",
                "additionalProperties": false
            })
        );
    }

    #[test]
    fn registry_rejects_remote_and_dynamic_schema_references() {
        for reference_schema in [
            json!({"$ref": "https://example.invalid/schema.json"}),
            json!({"$dynamicRef": "#node"}),
            json!({"$recursiveRef": "#"}),
        ] {
            let input = json!({
                "type": "object",
                "properties": {"value": reference_schema},
                "additionalProperties": false
            });
            let mut registry = ActionRegistry::default();
            let error = registry
                .register(StaticAction(descriptor(
                    "example.unsafe_ref",
                    "1.0.0",
                    input,
                )))
                .unwrap_err();
            assert_eq!(error.code(), "ACTION_INPUT_SCHEMA_INVALID");
        }
    }
}
