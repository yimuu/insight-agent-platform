//! First-class retrieval resource contracts.
//!
//! Retrievals are deliberately independent from Actions. A retrieval
//! descriptor freezes the model-facing input/output contract and the
//! caller-visible projection policy, while [`RetrievalExecutionResult`]
//! keeps the model output and the explicitly authored public candidate on
//! separate data paths. Nothing in this module infers public data from an
//! executor's JSON shape.

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Write as _,
    sync::{Arc, RwLock},
};

use async_trait::async_trait;
use semver::Version;
use serde::Serialize;
use serde_json::Value;
use sha2::{Digest, Sha256};

pub use insight_engine::resource_policy::RetrievalPublicPolicy;
use insight_engine::{
    author::CompileError,
    execution::{ExecutionControl, RunError},
    retrieval::RegisteredRetrievalView,
    schema::{compile_schema_2020, JsonSchemaValidator},
    worker::{WorkerArtifactPayload, WorkerOperationPermitHandle},
};

use super::actions::{CancellationClass, EffectClass, IdempotencyClass};

const MAX_RETRIEVAL_SCHEMA_BYTES: usize = 1024 * 1024;
const MAX_RETRIEVAL_SCHEMA_DEPTH: usize = 64;
const MAX_RETRIEVAL_SCHEMA_VALUES: usize = 65_536;

/// A platform capability which must be granted before a retrieval may run.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
pub struct RetrievalCapability(String);

impl RetrievalCapability {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Complete, versioned metadata for one retrieval implementation.
///
/// `query_field` names a required string property of the closed input object.
/// The registry verifies that relationship before calculating the descriptor
/// identity.
#[derive(Debug, Clone, PartialEq)]
pub struct RetrievalDescriptor {
    pub id: String,
    pub version: String,
    pub input_schema: Value,
    pub output_schema: Value,
    pub query_field: String,
    pub effect: EffectClass,
    pub idempotency: IdempotencyClass,
    pub cancellation: CancellationClass,
    pub required_capabilities: BTreeSet<RetrievalCapability>,
}

/// Frozen identity consumed by a future compiled retrieval plan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetrievalDescriptorIdentity {
    pub id: String,
    pub version: Version,
    /// Lower-case hexadecimal SHA-256 of the RFC 8785 canonical descriptor.
    pub descriptor_hash: String,
}

/// Durable execution context for one logical retrieval operation.
#[derive(Clone)]
pub struct RetrievalContext {
    pub run_id: String,
    pub operation_id: String,
    pub attempt: u32,
    pub attempt_id: String,
    pub idempotency_key: String,
    pub control: ExecutionControl,
    operation_permit: Option<WorkerOperationPermitHandle>,
}

impl RetrievalContext {
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
            operation_permit: None,
        }
    }

    pub fn for_durable_effect(
        run_id: impl Into<String>,
        operation_id: impl Into<String>,
        attempt: u32,
        effect_id: impl Into<String>,
        control: ExecutionControl,
    ) -> Self {
        let run_id = run_id.into();
        let operation_id = operation_id.into();
        Self {
            attempt_id: format!("{run_id}:{operation_id}:{attempt}"),
            idempotency_key: effect_id.into(),
            run_id,
            operation_id,
            attempt,
            control,
            operation_permit: None,
        }
    }

    #[doc(hidden)]
    pub fn with_operation_permit(mut self, permit: WorkerOperationPermitHandle) -> Self {
        self.operation_permit = Some(permit);
        self
    }

    pub fn operation_permit(&self) -> Option<&WorkerOperationPermitHandle> {
        self.operation_permit.as_ref()
    }
}

/// The two intentionally separate products of a retrieval execution.
///
/// `model_output` is validated against the descriptor output schema and is
/// suitable for downstream workflow/model consumption. `public_candidate`
/// is never inferred from it; a publication layer may inspect the candidate
/// only when the frozen public policy authorizes result publication.
///
/// The type intentionally implements neither `Debug` nor `Serialize`: both
/// values are private until their separate consumers validate them.
///
/// ```compile_fail
/// # use insight_resources::retrievals::RetrievalExecutionResult;
/// fn requires_debug<T: std::fmt::Debug>() {}
/// requires_debug::<RetrievalExecutionResult>();
/// ```
///
/// ```compile_fail
/// # use insight_resources::retrievals::RetrievalExecutionResult;
/// fn requires_serialize<T: serde::Serialize>() {}
/// requires_serialize::<RetrievalExecutionResult>();
/// ```
#[derive(Clone, PartialEq)]
pub struct RetrievalExecutionResult {
    pub model_output: Value,
    pub public_candidate: Option<Value>,
    artifact_payloads: Vec<WorkerArtifactPayload>,
}

impl RetrievalExecutionResult {
    pub fn new(model_output: Value, public_candidate: Option<Value>) -> Self {
        Self {
            model_output,
            public_candidate,
            artifact_payloads: Vec::new(),
        }
    }

    pub fn with_artifact_payloads(
        model_output: Value,
        public_candidate: Option<Value>,
        artifact_payloads: Vec<WorkerArtifactPayload>,
    ) -> Self {
        Self {
            model_output,
            public_candidate,
            artifact_payloads,
        }
    }

    pub fn into_parts(self) -> (Value, Option<Value>, Vec<WorkerArtifactPayload>) {
        (
            self.model_output,
            self.public_candidate,
            self.artifact_payloads,
        )
    }
}

#[async_trait]
pub trait Retrieval: Send + Sync {
    fn descriptor(&self) -> RetrievalDescriptor;

    fn public_policy(&self) -> RetrievalPublicPolicy {
        RetrievalPublicPolicy::private()
    }

    async fn retrieve(
        &self,
        input: Value,
        context: RetrievalContext,
    ) -> Result<RetrievalExecutionResult, RunError>;
}

pub struct RegisteredRetrieval {
    descriptor: RetrievalDescriptor,
    identity: RetrievalDescriptorIdentity,
    retrieval: Arc<dyn Retrieval>,
    input_validator: JsonSchemaValidator,
    output_validator: JsonSchemaValidator,
    public_policy: RetrievalPublicPolicy,
}

impl RegisteredRetrieval {
    pub fn descriptor(&self) -> &RetrievalDescriptor {
        &self.descriptor
    }

    pub fn identity(&self) -> &RetrievalDescriptorIdentity {
        &self.identity
    }

    pub fn public_policy(&self) -> &RetrievalPublicPolicy {
        &self.public_policy
    }

    pub fn validate_input(&self, input: &Value) -> Result<(), RunError> {
        validate_json(
            &self.input_validator,
            input,
            "VNEXT_RETRIEVAL_INPUT_CONTRACT_INVALID",
            "retrieval input validation failed",
        )
    }

    pub async fn retrieve(
        &self,
        input: Value,
        context: RetrievalContext,
    ) -> Result<RetrievalExecutionResult, RunError> {
        self.validate_input(&input)?;
        let result = self.retrieval.retrieve(input, context).await?;
        validate_json(
            &self.output_validator,
            &result.model_output,
            "VNEXT_RETRIEVAL_OUTPUT_CONTRACT_INVALID",
            "retrieval output validation failed",
        )?;
        // Deliberately do not inspect `public_candidate` here. The pure public
        // projection owns that check and skips it entirely for private policy.
        Ok(result)
    }
}

impl RegisteredRetrievalView for RegisteredRetrieval {
    fn resource_id(&self) -> &str {
        &self.identity.id
    }

    fn resource_version(&self) -> &Version {
        &self.identity.version
    }

    fn descriptor_hash(&self) -> &str {
        &self.identity.descriptor_hash
    }

    fn input_schema(&self) -> &Value {
        &self.descriptor.input_schema
    }

    fn output_schema(&self) -> &Value {
        &self.descriptor.output_schema
    }

    fn query_field(&self) -> &str {
        &self.descriptor.query_field
    }

    fn effect(&self) -> &str {
        match self.descriptor.effect {
            EffectClass::Pure => "pure",
            EffectClass::ReadOnly => "read_only",
            EffectClass::Mutating => "mutating",
        }
    }

    fn idempotency(&self) -> &str {
        match self.descriptor.idempotency {
            IdempotencyClass::Idempotent => "idempotent",
            IdempotencyClass::NonIdempotent => "non_idempotent",
        }
    }

    fn cancellation(&self) -> &str {
        match self.descriptor.cancellation {
            CancellationClass::Cooperative => "cooperative",
            CancellationClass::NotSupported => "not_supported",
        }
    }

    fn required_capabilities(&self) -> Vec<&str> {
        self.descriptor
            .required_capabilities
            .iter()
            .map(RetrievalCapability::as_str)
            .collect()
    }

    fn public_policy(&self) -> &RetrievalPublicPolicy {
        &self.public_policy
    }
}

fn validate_json(
    validator: &JsonSchemaValidator,
    value: &Value,
    code: &'static str,
    message: &'static str,
) -> Result<(), RunError> {
    if validator.is_valid(value) {
        Ok(())
    } else {
        Err(RunError::operation(code, message))
    }
}

type RevisionRetrievalMap = BTreeMap<(String, String, String), Arc<RegisteredRetrieval>>;

#[derive(Clone, Default)]
pub struct RetrievalRegistry {
    retrievals: Arc<RwLock<BTreeMap<String, Arc<RegisteredRetrieval>>>>,
    revisions: Arc<RwLock<RevisionRetrievalMap>>,
}

impl RetrievalRegistry {
    pub fn register<R>(&self, retrieval: R) -> Result<(), CompileError>
    where
        R: Retrieval + 'static,
    {
        let mut descriptor = retrieval.descriptor();
        let mut public_policy = retrieval.public_policy();
        let id = descriptor.id.clone();

        if !is_qualified_identifier(&id) {
            return Err(CompileError::new(
                "RETRIEVAL_ID_INVALID",
                "retrieval id must be a valid qualified identifier",
            ));
        }
        if self
            .retrievals
            .read()
            .map_err(|_| registry_unavailable())?
            .contains_key(&id)
        {
            return Err(CompileError::new(
                "DUPLICATE_RETRIEVAL",
                format!("retrieval '{id}' is already registered"),
            ));
        }
        let version = Version::parse(&descriptor.version).map_err(|_| {
            CompileError::new(
                "RETRIEVAL_VERSION_INVALID",
                format!("retrieval '{id}' version is not valid SemVer"),
            )
        })?;
        for capability in &descriptor.required_capabilities {
            if !is_qualified_identifier(capability.as_str()) {
                return Err(CompileError::new(
                    "RETRIEVAL_CAPABILITY_INVALID",
                    format!("retrieval '{id}' declares an invalid required capability"),
                ));
            }
        }
        if !is_schema_identifier(&descriptor.query_field) {
            return Err(CompileError::new(
                "RETRIEVAL_QUERY_FIELD_INVALID",
                format!("retrieval '{id}' query_field must be a canonical identifier"),
            ));
        }

        let (input_schema, input_validator) = normalize_retrieval_schema(&descriptor.input_schema)
            .map_err(|error| {
                CompileError::new(
                    "RETRIEVAL_INPUT_SCHEMA_INVALID",
                    format!("retrieval '{id}' input schema is invalid: {error}"),
                )
            })?;
        if !is_closed_object_schema(&input_schema) {
            return Err(CompileError::new(
                "RETRIEVAL_INPUT_SCHEMA_NOT_CLOSED",
                format!("retrieval '{id}' input schema must describe a closed object"),
            ));
        }
        validate_required_string_query_field(&id, &descriptor.query_field, &input_schema)?;

        let (output_schema, output_validator) =
            normalize_retrieval_schema(&descriptor.output_schema).map_err(|error| {
                CompileError::new(
                    "RETRIEVAL_OUTPUT_SCHEMA_INVALID",
                    format!("retrieval '{id}' output schema is invalid: {error}"),
                )
            })?;
        if !is_closed_object_schema(&output_schema) {
            return Err(CompileError::new(
                "RETRIEVAL_OUTPUT_SCHEMA_NOT_CLOSED",
                format!("retrieval '{id}' output schema must describe a closed object"),
            ));
        }

        if let Some(schema) = public_policy.result_schema.take() {
            let (schema, _) = normalize_retrieval_schema(&schema).map_err(|error| {
                CompileError::new(
                    "RETRIEVAL_PUBLIC_RESULT_SCHEMA_INVALID",
                    format!("retrieval '{id}' public result schema is invalid: {error}"),
                )
            })?;
            if !is_recursively_closed_public_object_schema(&schema) {
                return Err(CompileError::new(
                    "RETRIEVAL_PUBLIC_RESULT_SCHEMA_NOT_CLOSED",
                    format!(
                        "retrieval '{id}' public result schema must describe a recursively closed object"
                    ),
                ));
            }
            if !has_canonical_public_result_identity(&schema) {
                return Err(CompileError::new(
                    "RETRIEVAL_PUBLIC_RESULT_CONTRACT_INVALID",
                    format!(
                        "retrieval '{id}' public result schema must require string id and declare closed object metadata"
                    ),
                ));
            }
            public_policy.result_schema = Some(schema);
        }

        descriptor.input_schema = input_schema;
        descriptor.output_schema = output_schema;
        let descriptor_hash = descriptor_hash(&descriptor, &public_policy)?;
        let registered = Arc::new(RegisteredRetrieval {
            identity: RetrievalDescriptorIdentity {
                id: id.clone(),
                version,
                descriptor_hash,
            },
            descriptor,
            retrieval: Arc::new(retrieval),
            input_validator,
            output_validator,
            public_policy,
        });
        let identity = registered.identity();
        let mut revisions = self.revisions.write().map_err(|_| registry_unavailable())?;
        let mut retrievals = self
            .retrievals
            .write()
            .map_err(|_| registry_unavailable())?;
        if retrievals.contains_key(&id) {
            return Err(CompileError::new(
                "DUPLICATE_RETRIEVAL",
                format!("retrieval '{id}' is already registered"),
            ));
        }
        revisions.insert(
            (
                identity.id.clone(),
                identity.version.to_string(),
                identity.descriptor_hash.clone(),
            ),
            Arc::clone(&registered),
        );
        retrievals.insert(id, registered);
        Ok(())
    }

    pub fn publish_revision<R>(
        &self,
        retrieval: R,
    ) -> Result<Arc<RegisteredRetrieval>, CompileError>
    where
        R: Retrieval + 'static,
    {
        let isolated = RetrievalRegistry::default();
        isolated.register(retrieval)?;
        let registered = isolated
            .retrievals
            .read()
            .map_err(|_| registry_unavailable())?
            .values()
            .next()
            .cloned()
            .ok_or_else(registry_unavailable)?;
        if !registered.identity().id.starts_with("mcp.") {
            return Err(CompileError::new(
                "RETRIEVAL_ID_INVALID",
                "dynamic revision Retrieval must use the mcp namespace",
            ));
        }
        let identity = registered.identity();
        self.revisions
            .write()
            .map_err(|_| registry_unavailable())?
            .insert(
                (
                    identity.id.clone(),
                    identity.version.to_string(),
                    identity.descriptor_hash.clone(),
                ),
                Arc::clone(&registered),
            );
        self.retrievals
            .write()
            .map_err(|_| registry_unavailable())?
            .insert(identity.id.clone(), Arc::clone(&registered));
        Ok(registered)
    }

    pub fn resolve(&self, id: &str) -> Result<Arc<RegisteredRetrieval>, CompileError> {
        self.retrievals
            .read()
            .map_err(|_| registry_unavailable())?
            .get(id)
            .cloned()
            .ok_or_else(|| {
                CompileError::new(
                    "RETRIEVAL_NOT_FOUND",
                    format!("retrieval '{id}' is not registered"),
                )
            })
    }

    pub fn resolve_frozen(
        &self,
        id: &str,
        version: &str,
        descriptor_hash: &str,
    ) -> Result<Arc<RegisteredRetrieval>, CompileError> {
        self.revisions
            .read()
            .map_err(|_| registry_unavailable())?
            .get(&(
                id.to_owned(),
                version.to_owned(),
                descriptor_hash.to_owned(),
            ))
            .cloned()
            .ok_or_else(|| {
                CompileError::new(
                    "RETRIEVAL_NOT_FOUND",
                    format!("retrieval '{id}' revision is not registered"),
                )
            })
    }

    pub fn withdraw_current(&self, id: &str, version: &str) -> Result<bool, CompileError> {
        let mut retrievals = self
            .retrievals
            .write()
            .map_err(|_| registry_unavailable())?;
        if retrievals
            .get(id)
            .is_some_and(|retrieval| retrieval.identity().version.to_string() == version)
        {
            retrievals.remove(id);
            return Ok(true);
        }
        Ok(false)
    }

    pub fn names(&self) -> impl Iterator<Item = String> {
        self.retrievals
            .read()
            .map(|retrievals| retrievals.keys().cloned().collect::<Vec<_>>())
            .unwrap_or_default()
            .into_iter()
    }
}

fn registry_unavailable() -> CompileError {
    CompileError::new(
        "RETRIEVAL_REGISTRY_UNAVAILABLE",
        "Retrieval registry lock is unavailable",
    )
}

fn validate_required_string_query_field(
    retrieval_id: &str,
    query_field: &str,
    input_schema: &Value,
) -> Result<(), CompileError> {
    let root = resolve_object_schema(input_schema, input_schema, &mut BTreeSet::new()).ok_or_else(
        || {
            CompileError::new(
                "RETRIEVAL_QUERY_CONTRACT_INVALID",
                format!("retrieval '{retrieval_id}' input schema has no root object"),
            )
        },
    )?;
    let required = root
        .get("required")
        .and_then(Value::as_array)
        .is_some_and(|required| {
            required
                .iter()
                .any(|field| field.as_str() == Some(query_field))
        });
    let property = root
        .get("properties")
        .and_then(Value::as_object)
        .and_then(|properties| properties.get(query_field));
    if !required
        || !property
            .is_some_and(|schema| schema_is_string_only(schema, input_schema, &mut BTreeSet::new()))
    {
        return Err(CompileError::new(
            "RETRIEVAL_QUERY_CONTRACT_INVALID",
            format!(
                "retrieval '{retrieval_id}' query_field '{query_field}' must be a required string property"
            ),
        ));
    }
    Ok(())
}

fn schema_is_string_only(schema: &Value, root: &Value, references: &mut BTreeSet<String>) -> bool {
    let Some(object) = schema.as_object() else {
        return false;
    };
    if let Some(reference) = object.get("$ref").and_then(Value::as_str) {
        let Some(pointer) = reference.strip_prefix('#') else {
            return false;
        };
        if !references.insert(reference.to_owned()) {
            return false;
        }
        let result = root
            .pointer(pointer)
            .is_some_and(|target| schema_is_string_only(target, root, references));
        references.remove(reference);
        return result;
    }
    object.get("type").and_then(Value::as_str) == Some("string")
}

fn resolve_object_schema<'a>(
    schema: &'a Value,
    root: &'a Value,
    references: &mut BTreeSet<String>,
) -> Option<&'a serde_json::Map<String, Value>> {
    let object = schema.as_object()?;
    if object.get("type").and_then(Value::as_str) == Some("object") {
        return Some(object);
    }
    let reference = object.get("$ref")?.as_str()?;
    let pointer = reference.strip_prefix('#')?;
    if !references.insert(reference.to_owned()) {
        return None;
    }
    let result = resolve_object_schema(root.pointer(pointer)?, root, references);
    references.remove(reference);
    result
}

fn normalize_retrieval_schema(authored: &Value) -> Result<(Value, JsonSchemaValidator), String> {
    reject_dynamic_schema_references(authored)?;
    validate_value_bounds(
        authored,
        MAX_RETRIEVAL_SCHEMA_BYTES,
        MAX_RETRIEVAL_SCHEMA_DEPTH,
        MAX_RETRIEVAL_SCHEMA_VALUES,
    )?;

    let Value::Object(mut document) = authored.clone() else {
        return Err("retrieval schema must be an object".to_owned());
    };
    let definitions = match document.remove("$defs") {
        Some(Value::Object(definitions)) => definitions,
        Some(_) => return Err("top-level $defs must be an object".to_owned()),
        None => serde_json::Map::new(),
    };
    if definitions.keys().any(|name| !is_schema_identifier(name)) {
        return Err("top-level $defs names must be canonical identifiers".to_owned());
    }
    document.entry("$schema".to_owned()).or_insert_with(|| {
        Value::String("https://json-schema.org/draft/2020-12/schema".to_owned())
    });
    document.insert("$defs".to_owned(), Value::Object(definitions));
    let document = Value::Object(document);
    validate_value_bounds(
        &document,
        MAX_RETRIEVAL_SCHEMA_BYTES,
        MAX_RETRIEVAL_SCHEMA_DEPTH,
        MAX_RETRIEVAL_SCHEMA_VALUES,
    )?;
    let validator = compile_schema_2020(&document).map_err(|error| {
        if error.starts_with("unsupported JSON Schema draft") {
            "contract schema is not a valid Draft 2020-12 schema".to_owned()
        } else {
            error
        }
    })?;
    Ok((document, validator))
}

fn validate_value_bounds(
    value: &Value,
    max_bytes: usize,
    max_depth: usize,
    max_values: usize,
) -> Result<(), String> {
    let encoded = serde_jcs::to_vec(value)
        .map_err(|_| "retrieval contract must be canonicalizable".to_owned())?;
    if encoded.len() > max_bytes {
        return Err("retrieval contract exceeds the byte limit".to_owned());
    }
    let mut stack = vec![(value, 0_usize)];
    let mut values = 0_usize;
    while let Some((current, depth)) = stack.pop() {
        values = values.saturating_add(1);
        if depth > max_depth || values > max_values {
            return Err("retrieval contract exceeds the structural limit".to_owned());
        }
        match current {
            Value::Array(items) => {
                stack.extend(items.iter().map(|item| (item, depth.saturating_add(1))))
            }
            Value::Object(object) => {
                stack.extend(object.values().map(|item| (item, depth.saturating_add(1))))
            }
            Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
        }
    }
    Ok(())
}

fn is_schema_identifier(value: &str) -> bool {
    let mut characters = value.chars();
    matches!(characters.next(), Some(first) if first.is_ascii_alphabetic() || first == '_')
        && characters
            .all(|character| character.is_ascii_alphanumeric() || matches!(character, '_' | '-'))
}

fn reject_dynamic_schema_references(value: &Value) -> Result<(), String> {
    match value {
        Value::Object(object) => {
            if object.contains_key("$dynamicRef") || object.contains_key("$recursiveRef") {
                return Err("dynamic JSON Schema references are not supported".to_owned());
            }
            if let Some(reference) = object.get("$ref") {
                let valid = reference
                    .as_str()
                    .and_then(|value| value.strip_prefix("#/$defs/"))
                    .is_some_and(|name| !name.contains('/') && is_schema_identifier(name));
                if !valid {
                    return Err("schema reference must be exactly #/$defs/<Identifier>".to_owned());
                }
            }
            for nested in object.values() {
                reject_dynamic_schema_references(nested)?;
            }
        }
        Value::Array(values) => {
            for nested in values {
                reject_dynamic_schema_references(nested)?;
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
    }
    Ok(())
}

fn is_qualified_identifier(value: &str) -> bool {
    if value.is_empty() || value.len() > 128 || !value.is_ascii() {
        return false;
    }
    value.split('.').all(is_schema_identifier)
}

fn is_closed_object_schema(schema: &Value) -> bool {
    resolve_object_schema(schema, schema, &mut BTreeSet::new()).is_some_and(|object| {
        object.get("additionalProperties") == Some(&Value::Bool(false))
            || object.get("unevaluatedProperties") == Some(&Value::Bool(false))
    })
}

fn is_recursively_closed_public_object_schema(schema: &Value) -> bool {
    let Some(root_object) = resolve_object_schema(schema, schema, &mut BTreeSet::new()) else {
        return false;
    };
    (root_object.get("additionalProperties") == Some(&Value::Bool(false))
        || root_object.get("unevaluatedProperties") == Some(&Value::Bool(false)))
        && safe_public_schema_value(schema, schema, &mut BTreeSet::new())
}

fn has_canonical_public_result_identity(schema: &Value) -> bool {
    let Some(root) = resolve_object_schema(schema, schema, &mut BTreeSet::new()) else {
        return false;
    };
    let required = root
        .get("required")
        .and_then(Value::as_array)
        .is_some_and(|required| required.iter().any(|field| field.as_str() == Some("id")));
    let Some(properties) = root.get("properties").and_then(Value::as_object) else {
        return false;
    };
    required
        && properties
            .get("id")
            .is_some_and(|id| schema_is_string_only(id, schema, &mut BTreeSet::new()))
        && properties.get("metadata").is_some_and(|metadata| {
            resolve_object_schema(metadata, schema, &mut BTreeSet::new()).is_some_and(|object| {
                object.get("additionalProperties") == Some(&Value::Bool(false))
                    || object.get("unevaluatedProperties") == Some(&Value::Bool(false))
            })
        })
}

fn safe_public_schema_value(
    schema: &Value,
    root: &Value,
    references: &mut BTreeSet<String>,
) -> bool {
    let Value::Object(object) = schema else {
        return schema == &Value::Bool(false);
    };
    if let Some(reference) = object.get("$ref").and_then(Value::as_str) {
        let Some(pointer) = reference.strip_prefix('#') else {
            return false;
        };
        if !references.insert(reference.to_owned()) {
            return false;
        }
        let safe = root
            .pointer(pointer)
            .is_some_and(|target| safe_public_schema_value(target, root, references));
        references.remove(reference);
        return safe;
    }
    if object.contains_key("allOf") || !object.contains_key("type") {
        return object.contains_key("const") || object.contains_key("enum");
    }
    let types = match object.get("type") {
        Some(Value::String(kind)) => vec![kind.as_str()],
        Some(Value::Array(kinds)) if !kinds.is_empty() => {
            let Some(kinds) = kinds.iter().map(Value::as_str).collect::<Option<Vec<_>>>() else {
                return false;
            };
            kinds
        }
        _ => return false,
    };
    if types.iter().any(|kind| {
        !matches!(
            *kind,
            "null" | "boolean" | "integer" | "number" | "string" | "object" | "array"
        )
    }) {
        return false;
    }
    let object_safe = !types.contains(&"object")
        || (object.get("additionalProperties") == Some(&Value::Bool(false))
            || object.get("unevaluatedProperties") == Some(&Value::Bool(false)))
            && object
                .get("properties")
                .and_then(Value::as_object)
                .is_none_or(|properties| {
                    properties
                        .values()
                        .all(|property| safe_public_schema_value(property, root, references))
                })
            && !object.contains_key("patternProperties");
    let array_safe = !types.contains(&"array")
        || object
            .get("items")
            .is_some_and(|items| safe_public_schema_value(items, root, references));
    object_safe && array_safe
}

#[derive(Serialize)]
struct CanonicalRetrievalDescriptor<'a> {
    id: &'a str,
    version: &'a str,
    input_schema: &'a Value,
    output_schema: &'a Value,
    query_field: &'a str,
    effect: EffectClass,
    idempotency: IdempotencyClass,
    cancellation: CancellationClass,
    required_capabilities: Vec<&'a str>,
    public: &'a RetrievalPublicPolicy,
}

fn descriptor_hash(
    descriptor: &RetrievalDescriptor,
    public: &RetrievalPublicPolicy,
) -> Result<String, CompileError> {
    let mut capabilities = descriptor
        .required_capabilities
        .iter()
        .map(RetrievalCapability::as_str)
        .collect::<Vec<_>>();
    capabilities.sort_unstable_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
    let canonical = serde_jcs::to_vec(&CanonicalRetrievalDescriptor {
        id: &descriptor.id,
        version: &descriptor.version,
        input_schema: &descriptor.input_schema,
        output_schema: &descriptor.output_schema,
        query_field: &descriptor.query_field,
        effect: descriptor.effect,
        idempotency: descriptor.idempotency,
        cancellation: descriptor.cancellation,
        required_capabilities: capabilities,
        public,
    })
    .map_err(|_| {
        CompileError::new(
            "RETRIEVAL_DESCRIPTOR_IDENTITY_INVALID",
            "retrieval descriptor could not be canonicalized",
        )
    })?;
    let digest = Sha256::digest(canonical);
    let mut encoded = String::with_capacity(digest.len() * 2);
    for byte in digest {
        write!(&mut encoded, "{byte:02x}").expect("writing to a String cannot fail");
    }
    Ok(encoded)
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeSet, time::Duration};

    use async_trait::async_trait;
    use serde_json::{json, Value};

    use super::{
        CancellationClass, EffectClass, IdempotencyClass, Retrieval, RetrievalCapability,
        RetrievalContext, RetrievalDescriptor, RetrievalExecutionResult, RetrievalPublicPolicy,
        RetrievalRegistry,
    };
    use insight_engine::execution::{stop_pair, ExecutionControl, RunError};

    #[derive(Clone)]
    struct StaticRetrieval {
        descriptor: RetrievalDescriptor,
        public: RetrievalPublicPolicy,
        result: RetrievalExecutionResult,
    }

    #[async_trait]
    impl Retrieval for StaticRetrieval {
        fn descriptor(&self) -> RetrievalDescriptor {
            self.descriptor.clone()
        }

        fn public_policy(&self) -> RetrievalPublicPolicy {
            self.public.clone()
        }

        async fn retrieve(
            &self,
            _input: Value,
            _context: RetrievalContext,
        ) -> Result<RetrievalExecutionResult, RunError> {
            Ok(self.result.clone())
        }
    }

    fn object_schema(properties: Value, required: &[&str]) -> Value {
        json!({
            "type": "object",
            "properties": properties,
            "required": required,
            "additionalProperties": false
        })
    }

    fn retrieval(id: impl Into<String>) -> StaticRetrieval {
        StaticRetrieval {
            descriptor: RetrievalDescriptor {
                id: id.into(),
                version: "1.2.3".to_owned(),
                input_schema: object_schema(
                    json!({
                        "query": {"type": "string", "minLength": 1},
                        "limit": {"type": "integer", "minimum": 1}
                    }),
                    &["query"],
                ),
                output_schema: object_schema(
                    json!({"documents": {"type": "array", "items": {"type": "string"}}}),
                    &["documents"],
                ),
                query_field: "query".to_owned(),
                effect: EffectClass::ReadOnly,
                idempotency: IdempotencyClass::Idempotent,
                cancellation: CancellationClass::Cooperative,
                required_capabilities: BTreeSet::from([RetrievalCapability::new("network.read")]),
            },
            public: RetrievalPublicPolicy::private(),
            result: RetrievalExecutionResult::new(
                json!({"documents": ["doc_1"]}),
                Some(json!([{"raw_secret": "not inspected here"}])),
            ),
        }
    }

    #[test]
    fn registry_normalizes_contract_and_builds_stable_identity() {
        let first = RetrievalRegistry::default();
        first.register(retrieval("search.documents")).unwrap();
        let registered = first.resolve("search.documents").unwrap();
        assert_eq!(registered.identity().version.to_string(), "1.2.3");
        assert_eq!(registered.identity().descriptor_hash.len(), 64);
        assert_eq!(
            registered.descriptor().input_schema["$schema"],
            "https://json-schema.org/draft/2020-12/schema"
        );
        assert_eq!(registered.descriptor().input_schema["$defs"], json!({}));

        let second = RetrievalRegistry::default();
        second.register(retrieval("search.documents")).unwrap();
        assert_eq!(
            registered.identity(),
            second.resolve("search.documents").unwrap().identity()
        );
        assert_eq!(first.names().collect::<Vec<_>>(), vec!["search.documents"]);
    }

    #[test]
    fn dynamic_revision_switch_keeps_exact_frozen_retrieval_available() {
        let registry = RetrievalRegistry::default();
        let mut first = retrieval("mcp.engineering.resource_search");
        first.descriptor.version = "0.0.0+mcp.revision1".to_owned();
        let first = registry.publish_revision(first).unwrap();
        let first_identity = first.identity();

        let mut second = retrieval("mcp.engineering.resource_search");
        second.descriptor.version = "0.0.0+mcp.revision2".to_owned();
        second.descriptor.output_schema = object_schema(
            json!({
                "documents": {"type": "array", "items": {"type": "string"}},
                "revision": {"type": "string"}
            }),
            &["documents", "revision"],
        );
        let second = registry.publish_revision(second).unwrap();
        assert_eq!(
            registry
                .resolve("mcp.engineering.resource_search")
                .unwrap()
                .identity(),
            second.identity()
        );
        assert_eq!(
            registry
                .resolve_frozen(
                    &first_identity.id,
                    &first_identity.version.to_string(),
                    &first_identity.descriptor_hash,
                )
                .unwrap()
                .identity(),
            first_identity
        );
        assert!(registry
            .withdraw_current(
                &second.identity().id,
                &second.identity().version.to_string()
            )
            .unwrap());
        assert!(registry
            .resolve_frozen(
                &first_identity.id,
                &first_identity.version.to_string(),
                &first_identity.descriptor_hash,
            )
            .is_ok());
    }

    #[test]
    fn registry_rejects_duplicate_invalid_identity_version_and_capability() {
        let registry = RetrievalRegistry::default();
        registry.register(retrieval("search.documents")).unwrap();
        assert_eq!(
            registry
                .register(retrieval("search.documents"))
                .unwrap_err()
                .code(),
            "DUPLICATE_RETRIEVAL"
        );
        assert_eq!(
            RetrievalRegistry::default()
                .register(retrieval("not valid"))
                .unwrap_err()
                .code(),
            "RETRIEVAL_ID_INVALID"
        );
        let mut invalid_version = retrieval("search.version");
        invalid_version.descriptor.version = "latest".to_owned();
        assert_eq!(
            RetrievalRegistry::default()
                .register(invalid_version)
                .unwrap_err()
                .code(),
            "RETRIEVAL_VERSION_INVALID"
        );
        let mut invalid_capability = retrieval("search.capability");
        invalid_capability.descriptor.required_capabilities =
            BTreeSet::from([RetrievalCapability::new("network read")]);
        assert_eq!(
            RetrievalRegistry::default()
                .register(invalid_capability)
                .unwrap_err()
                .code(),
            "RETRIEVAL_CAPABILITY_INVALID"
        );
    }

    #[test]
    fn query_field_must_be_a_required_string_property() {
        let cases = [
            (
                "query field",
                object_schema(json!({"query field": {"type": "string"}}), &["query field"]),
            ),
            (
                "query",
                object_schema(json!({"query": {"type": "string"}}), &[]),
            ),
            (
                "query",
                object_schema(json!({"query": {"type": "integer"}}), &["query"]),
            ),
            (
                "query",
                object_schema(json!({"other": {"type": "string"}}), &["other"]),
            ),
        ];
        for (index, (query_field, input_schema)) in cases.into_iter().enumerate() {
            let mut candidate = retrieval(format!("search.invalid_{index}"));
            candidate.descriptor.query_field = query_field.to_owned();
            candidate.descriptor.input_schema = input_schema;
            let code = RetrievalRegistry::default()
                .register(candidate)
                .unwrap_err()
                .code()
                .to_owned();
            assert!(matches!(
                code.as_str(),
                "RETRIEVAL_QUERY_FIELD_INVALID" | "RETRIEVAL_QUERY_CONTRACT_INVALID"
            ));
        }
    }

    #[test]
    fn input_output_and_public_result_schemas_must_be_closed() {
        let mut open_input = retrieval("search.open_input");
        open_input.descriptor.input_schema["additionalProperties"] = json!(true);
        assert_eq!(
            RetrievalRegistry::default()
                .register(open_input)
                .unwrap_err()
                .code(),
            "RETRIEVAL_INPUT_SCHEMA_NOT_CLOSED"
        );

        let mut open_output = retrieval("search.open_output");
        open_output.descriptor.output_schema["additionalProperties"] = json!(true);
        assert_eq!(
            RetrievalRegistry::default()
                .register(open_output)
                .unwrap_err()
                .code(),
            "RETRIEVAL_OUTPUT_SCHEMA_NOT_CLOSED"
        );

        let mut nested_public = retrieval("search.open_public");
        nested_public.public.result_schema = Some(object_schema(
            json!({"metadata": {"type": "object"}}),
            &["metadata"],
        ));
        assert_eq!(
            RetrievalRegistry::default()
                .register(nested_public)
                .unwrap_err()
                .code(),
            "RETRIEVAL_PUBLIC_RESULT_SCHEMA_NOT_CLOSED"
        );

        let mut missing_id = retrieval("search.public_missing_id");
        missing_id.public.result_schema = Some(object_schema(
            json!({
                "title": {"type": "string"},
                "metadata": {
                    "type": "object",
                    "properties": {},
                    "additionalProperties": false
                }
            }),
            &["title"],
        ));
        assert_eq!(
            RetrievalRegistry::default()
                .register(missing_id)
                .unwrap_err()
                .code(),
            "RETRIEVAL_PUBLIC_RESULT_CONTRACT_INVALID"
        );

        let mut missing_metadata = retrieval("search.public_missing_metadata");
        missing_metadata.public.result_schema =
            Some(object_schema(json!({"id": {"type": "string"}}), &["id"]));
        assert_eq!(
            RetrievalRegistry::default()
                .register(missing_metadata)
                .unwrap_err()
                .code(),
            "RETRIEVAL_PUBLIC_RESULT_CONTRACT_INVALID"
        );
    }

    #[tokio::test]
    async fn execution_validates_model_contracts_without_inspecting_public_candidate() {
        let registry = RetrievalRegistry::default();
        registry.register(retrieval("search.execute")).unwrap();
        let registered = registry.resolve("search.execute").unwrap();
        let (_, signal) = stop_pair();
        let control = ExecutionControl::new(signal, Duration::from_secs(30));
        let context = RetrievalContext::for_operation("run_1", "search", 1, control.clone());
        let result = registered
            .retrieve(json!({"query": "WBC"}), context)
            .await
            .unwrap();
        assert_eq!(result.model_output, json!({"documents": ["doc_1"]}));
        assert_eq!(
            result.public_candidate,
            Some(json!([{"raw_secret": "not inspected here"}]))
        );

        let context = RetrievalContext::for_operation("run_1", "search", 2, control.clone());
        let error = registered
            .retrieve(json!({"query": "WBC", "unknown": true}), context)
            .await
            .err()
            .expect("invalid input must fail before retrieval execution");
        assert_eq!(error.code(), "VNEXT_RETRIEVAL_INPUT_CONTRACT_INVALID");

        let mut bad_output = retrieval("search.bad_output");
        bad_output.result.model_output = json!({"unexpected": true});
        let registry = RetrievalRegistry::default();
        registry.register(bad_output).unwrap();
        let context = RetrievalContext::for_operation("run_1", "search", 3, control);
        let error = registry
            .resolve("search.bad_output")
            .unwrap()
            .retrieve(json!({"query": "WBC"}), context)
            .await
            .err()
            .expect("invalid private model output must fail closed");
        assert_eq!(error.code(), "VNEXT_RETRIEVAL_OUTPUT_CONTRACT_INVALID");
    }

    #[test]
    fn descriptor_hash_covers_query_and_public_policy() {
        let baseline = RetrievalRegistry::default();
        baseline.register(retrieval("search.hash")).unwrap();
        let baseline_hash = baseline
            .resolve("search.hash")
            .unwrap()
            .identity()
            .descriptor_hash
            .clone();

        let mut changed = retrieval("search.hash");
        changed.public.query = true;
        let registry = RetrievalRegistry::default();
        registry.register(changed).unwrap();
        assert_ne!(
            baseline_hash,
            registry
                .resolve("search.hash")
                .unwrap()
                .identity()
                .descriptor_hash
        );
    }

    #[test]
    fn public_policy_wire_is_closed_and_explicitly_normalizable() {
        assert!(serde_json::from_value::<RetrievalPublicPolicy>(json!({
            "query": true,
            "result": null,
            "future": true
        }))
        .is_err());
        assert_eq!(
            serde_json::to_value(RetrievalPublicPolicy::private()).unwrap(),
            json!({"query": false, "result": null})
        );
    }
}
