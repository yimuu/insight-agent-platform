use std::{collections::BTreeMap, error::Error, fmt, sync::Arc};

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::Value;

use crate::{
    resources::actions::{ActionContext, ActionRegistry},
    runtime::{ExecutionControl, RunError},
};

use super::{
    ir::OperationId,
    types::{SchemaType, ValueType},
    value::Identifier,
};

pub const OPERATION_USES_INVALID: &str = "VNEXT_OPERATION_USES_INVALID";
pub const OPERATION_DUPLICATE: &str = "VNEXT_OPERATION_DUPLICATE";
pub const OPERATION_NOT_FOUND: &str = "VNEXT_OPERATION_NOT_FOUND";
pub const ACTION_CALL_CONFIG_INVALID: &str = "VNEXT_ACTION_CALL_CONFIG_INVALID";
pub const ACTION_CALL_INPUT_CONTRACT_INVALID: &str = "VNEXT_ACTION_CALL_INPUT_CONTRACT_INVALID";
pub const ACTION_CALL_ACTION_NOT_FOUND: &str = "VNEXT_ACTION_CALL_ACTION_NOT_FOUND";
pub const ACTION_CALL_SCHEMA_INVALID: &str = "VNEXT_ACTION_CALL_SCHEMA_INVALID";
pub const ACTION_CALL_INPUT_TYPE_MISMATCH: &str = "VNEXT_ACTION_CALL_INPUT_TYPE_MISMATCH";

const OPERATION_USES_INVALID_MESSAGE: &str = "operation uses must not be empty";
const OPERATION_DUPLICATE_MESSAGE: &str = "operation uses is already registered";
const OPERATION_NOT_FOUND_MESSAGE: &str = "operation uses is not registered";
const ACTION_CALL_CONFIG_INVALID_MESSAGE: &str = "action.call config is invalid";
const ACTION_CALL_INPUT_CONTRACT_INVALID_MESSAGE: &str =
    "action.call requires exactly one input named input";
const ACTION_CALL_ACTION_NOT_FOUND_MESSAGE: &str = "action.call action is not registered";
const ACTION_CALL_SCHEMA_INVALID_MESSAGE: &str = "action.call action schema is invalid";
const ACTION_CALL_INPUT_TYPE_MISMATCH_MESSAGE: &str =
    "action.call input type is not assignable to the action input schema";

/// A stable, data-free compile or registration failure at the operation boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperationError {
    code: &'static str,
    message: &'static str,
}

impl OperationError {
    pub fn new(code: &'static str, message: &'static str) -> Self {
        Self { code, message }
    }

    pub fn code(&self) -> &'static str {
        self.code
    }

    pub fn message(&self) -> &'static str {
        self.message
    }
}

impl fmt::Display for OperationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.message)
    }
}

impl Error for OperationError {}

/// The externally visible side-effect class used by the scheduler and policy layer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperationEffect {
    Pure,
    ExternalModel,
    ExternalAction,
}

/// The statically compiled contract for one leaf operation invocation.
#[derive(Debug, Clone, PartialEq)]
pub struct CompiledOperationContract {
    pub output_schema: Value,
    pub output_type: ValueType,
    pub effect: OperationEffect,
    pub idempotent: bool,
}

/// Runtime identity and cancellation/deadline control for one operation attempt.
#[derive(Clone)]
pub struct OperationContext {
    pub run_id: String,
    pub operation_id: OperationId,
    pub attempt: u32,
    pub control: ExecutionControl,
}

impl OperationContext {
    pub fn new(
        run_id: impl Into<String>,
        operation_id: OperationId,
        attempt: u32,
        control: ExecutionControl,
    ) -> Self {
        Self {
            run_id: run_id.into(),
            operation_id,
            attempt,
            control,
        }
    }
}

/// A leaf capability. Implementations cannot create control edges or terminal transitions.
#[async_trait]
pub trait Operation: Send + Sync {
    fn uses(&self) -> &'static str;

    fn compile(
        &self,
        config: &Value,
        inputs: &BTreeMap<Identifier, ValueType>,
    ) -> Result<CompiledOperationContract, OperationError>;

    async fn execute(
        &self,
        config: &Value,
        inputs: BTreeMap<Identifier, Value>,
        context: OperationContext,
    ) -> Result<Value, RunError>;
}

/// Registry for leaf operations only. Structured control never enters this registry.
#[derive(Clone, Default)]
pub struct OperationRegistry {
    operations: BTreeMap<String, Arc<dyn Operation>>,
}

impl OperationRegistry {
    pub fn register<O>(&mut self, operation: O) -> Result<(), OperationError>
    where
        O: Operation + 'static,
    {
        let uses = operation.uses();
        if uses.trim().is_empty() {
            return Err(OperationError::new(
                OPERATION_USES_INVALID,
                OPERATION_USES_INVALID_MESSAGE,
            ));
        }
        if self.operations.contains_key(uses) {
            return Err(OperationError::new(
                OPERATION_DUPLICATE,
                OPERATION_DUPLICATE_MESSAGE,
            ));
        }
        self.operations
            .insert(uses.to_string(), Arc::new(operation));
        Ok(())
    }

    pub fn resolve(&self, uses: &str) -> Result<Arc<dyn Operation>, OperationError> {
        if uses.trim().is_empty() {
            return Err(OperationError::new(
                OPERATION_USES_INVALID,
                OPERATION_USES_INVALID_MESSAGE,
            ));
        }
        self.operations
            .get(uses)
            .cloned()
            .ok_or_else(|| OperationError::new(OPERATION_NOT_FOUND, OPERATION_NOT_FOUND_MESSAGE))
    }

    pub fn names(&self) -> impl Iterator<Item = &str> {
        self.operations.keys().map(String::as_str)
    }
}

pub const ACTION_CALL_USES: &str = "action.call";

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ActionCallConfig {
    action: String,
}

#[derive(Clone)]
pub struct ActionCallOperation {
    actions: ActionRegistry,
}

impl ActionCallOperation {
    pub fn new(actions: ActionRegistry) -> Self {
        Self { actions }
    }

    fn parse_config(config: &Value) -> Result<ActionCallConfig, OperationError> {
        let config: ActionCallConfig = serde_json::from_value(config.clone()).map_err(|_| {
            OperationError::new(
                ACTION_CALL_CONFIG_INVALID,
                ACTION_CALL_CONFIG_INVALID_MESSAGE,
            )
        })?;
        if config.action.trim().is_empty() {
            return Err(OperationError::new(
                ACTION_CALL_CONFIG_INVALID,
                ACTION_CALL_CONFIG_INVALID_MESSAGE,
            ));
        }
        Ok(config)
    }

    fn compile_input(
        inputs: &BTreeMap<Identifier, ValueType>,
    ) -> Result<&ValueType, OperationError> {
        exactly_one_input(inputs).ok_or_else(|| {
            OperationError::new(
                ACTION_CALL_INPUT_CONTRACT_INVALID,
                ACTION_CALL_INPUT_CONTRACT_INVALID_MESSAGE,
            )
        })
    }

    fn evaluated_input(mut inputs: BTreeMap<Identifier, Value>) -> Result<Value, RunError> {
        if inputs.len() != 1 {
            return Err(action_call_input_contract_run_error());
        }
        let input = Identifier::parse("input").expect("input is a valid identifier");
        inputs
            .remove(&input)
            .ok_or_else(action_call_input_contract_run_error)
    }
}

#[async_trait]
impl Operation for ActionCallOperation {
    fn uses(&self) -> &'static str {
        ACTION_CALL_USES
    }

    fn compile(
        &self,
        config: &Value,
        inputs: &BTreeMap<Identifier, ValueType>,
    ) -> Result<CompiledOperationContract, OperationError> {
        let config = Self::parse_config(config)?;
        let input_type = Self::compile_input(inputs)?;
        let action = self.actions.resolve(&config.action).map_err(|_| {
            OperationError::new(
                ACTION_CALL_ACTION_NOT_FOUND,
                ACTION_CALL_ACTION_NOT_FOUND_MESSAGE,
            )
        })?;
        let descriptor = action.descriptor();
        let action_input_type = SchemaType::compile(&descriptor.input_schema)
            .map_err(|_| {
                OperationError::new(
                    ACTION_CALL_SCHEMA_INVALID,
                    ACTION_CALL_SCHEMA_INVALID_MESSAGE,
                )
            })?
            .into_value_type();
        let output_type = SchemaType::compile(&descriptor.output_schema)
            .map_err(|_| {
                OperationError::new(
                    ACTION_CALL_SCHEMA_INVALID,
                    ACTION_CALL_SCHEMA_INVALID_MESSAGE,
                )
            })?
            .into_value_type();
        if !input_type.is_assignable_to(&action_input_type) {
            return Err(OperationError::new(
                ACTION_CALL_INPUT_TYPE_MISMATCH,
                ACTION_CALL_INPUT_TYPE_MISMATCH_MESSAGE,
            ));
        }

        Ok(CompiledOperationContract {
            output_schema: descriptor.output_schema.clone(),
            output_type,
            effect: OperationEffect::ExternalAction,
            idempotent: descriptor.idempotent,
        })
    }

    async fn execute(
        &self,
        config: &Value,
        inputs: BTreeMap<Identifier, Value>,
        context: OperationContext,
    ) -> Result<Value, RunError> {
        let config = Self::parse_config(config).map_err(operation_error_to_run_error)?;
        let input = Self::evaluated_input(inputs)?;
        let action = self.actions.resolve(&config.action).map_err(|_| {
            RunError::operation(
                ACTION_CALL_ACTION_NOT_FOUND,
                ACTION_CALL_ACTION_NOT_FOUND_MESSAGE,
            )
        })?;
        let action_context = ActionContext::for_operation(
            context.run_id,
            context.operation_id.to_string(),
            context.attempt,
            context.control,
        );
        action.call(input, action_context).await
    }
}

fn exactly_one_input<T>(inputs: &BTreeMap<Identifier, T>) -> Option<&T> {
    if inputs.len() != 1 {
        return None;
    }
    inputs
        .iter()
        .next()
        .filter(|(name, _)| name.as_str() == "input")
        .map(|(_, input)| input)
}

fn action_call_input_contract_run_error() -> RunError {
    RunError::operation(
        ACTION_CALL_INPUT_CONTRACT_INVALID,
        ACTION_CALL_INPUT_CONTRACT_INVALID_MESSAGE,
    )
}

fn operation_error_to_run_error(error: OperationError) -> RunError {
    RunError::operation(error.code(), error.message())
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        sync::{Arc, Mutex},
        time::Duration,
    };

    use async_trait::async_trait;
    use serde_json::{json, Value};

    use crate::{
        resources::actions::{Action, ActionContext, ActionDescriptor, ActionRegistry},
        runtime::{stop_pair, ExecutionControl, RunError},
    };

    use super::*;

    type ActionCalls = Arc<Mutex<Vec<(String, String, Value)>>>;

    const ECHO_ACTION: &str = "test.echo";

    #[derive(Clone)]
    struct EchoAction {
        calls: ActionCalls,
    }

    #[async_trait]
    impl Action for EchoAction {
        fn descriptor(&self) -> ActionDescriptor {
            ActionDescriptor {
                name: ECHO_ACTION,
                input_schema: input_schema(),
                output_schema: output_schema(),
                idempotent: true,
            }
        }

        async fn call(&self, input: Value, context: ActionContext) -> Result<Value, RunError> {
            self.calls
                .lock()
                .unwrap()
                .push((context.run_id, context.operation_id, input.clone()));
            Ok(json!({"echoed": input["name"]}))
        }
    }

    struct StubOperation {
        uses: &'static str,
    }

    #[async_trait]
    impl Operation for StubOperation {
        fn uses(&self) -> &'static str {
            self.uses
        }

        fn compile(
            &self,
            _config: &Value,
            _inputs: &BTreeMap<Identifier, ValueType>,
        ) -> Result<CompiledOperationContract, OperationError> {
            Ok(CompiledOperationContract {
                output_schema: Value::Bool(true),
                output_type: ValueType::Any,
                effect: OperationEffect::Pure,
                idempotent: true,
            })
        }

        async fn execute(
            &self,
            _config: &Value,
            _inputs: BTreeMap<Identifier, Value>,
            _context: OperationContext,
        ) -> Result<Value, RunError> {
            Ok(Value::Null)
        }
    }

    #[test]
    fn action_call_compile_maps_action_contract_metadata() {
        let (operation, _) = action_call_operation();
        let contract = operation
            .compile(
                &json!({"action": ECHO_ACTION}),
                &BTreeMap::from([(
                    identifier("input"),
                    SchemaType::compile(&input_schema())
                        .unwrap()
                        .into_value_type(),
                )]),
            )
            .unwrap();

        assert_eq!(contract.output_schema, output_schema());
        assert_eq!(
            contract.output_type,
            SchemaType::compile(&output_schema())
                .unwrap()
                .into_value_type()
        );
        assert_eq!(contract.effect, OperationEffect::ExternalAction);
        assert!(contract.idempotent);
    }

    #[tokio::test]
    async fn action_call_execute_uses_registered_validation_and_qualified_identity() {
        let (operation, calls) = action_call_operation();
        let (_, stop) = stop_pair();
        let control = ExecutionControl::new(stop, Duration::from_secs(1));
        let operation_id = OperationId::authored("/workflow/analyze").unwrap();
        let context = OperationContext::new("run-1", operation_id.clone(), 3, control);
        assert_eq!(context.attempt, 3);

        let output = operation
            .execute(
                &json!({"action": ECHO_ACTION}),
                BTreeMap::from([(identifier("input"), json!({"name": "Ada"}))]),
                context,
            )
            .await
            .unwrap();

        assert_eq!(output, json!({"echoed": "Ada"}));
        assert_eq!(
            *calls.lock().unwrap(),
            vec![(
                "run-1".to_string(),
                operation_id.to_string(),
                json!({"name": "Ada"}),
            )]
        );
    }

    #[tokio::test]
    async fn action_call_execute_validates_input_and_output() {
        let (operation, _) = action_call_operation();
        let error = operation
            .execute(
                &json!({"action": ECHO_ACTION}),
                BTreeMap::from([(identifier("input"), json!({"name": 7}))]),
                operation_context(),
            )
            .await
            .unwrap_err();
        assert_eq!(error.code(), "ACTION_INPUT_INVALID");

        let mut actions = ActionRegistry::default();
        actions
            .register(InvalidOutputAction)
            .expect("fake action should register");
        let error = ActionCallOperation::new(actions)
            .execute(
                &json!({"action": "test.invalid_output"}),
                BTreeMap::from([(identifier("input"), json!({"name": "Ada"}))]),
                operation_context(),
            )
            .await
            .unwrap_err();
        assert_eq!(error.code(), "ACTION_OUTPUT_INVALID");
    }

    #[test]
    fn registry_rejects_empty_duplicate_and_unknown_uses_without_echoing_data() {
        let mut registry = OperationRegistry::default();
        let error = registry.register(StubOperation { uses: "  " }).unwrap_err();
        assert_eq!(error.code(), OPERATION_USES_INVALID);
        assert!(!error.message().contains("  "));

        registry
            .register(StubOperation { uses: "test.leaf" })
            .unwrap();
        let error = registry
            .register(StubOperation { uses: "test.leaf" })
            .unwrap_err();
        assert_eq!(error.code(), OPERATION_DUPLICATE);
        assert!(!error.message().contains("test.leaf"));

        let error = registry.resolve("secret.unknown").err().unwrap();
        assert_eq!(error.code(), OPERATION_NOT_FOUND);
        assert!(!error.message().contains("secret.unknown"));
    }

    #[test]
    fn action_call_compile_rejects_type_mismatch_and_invalid_input_shape() {
        let (operation, _) = action_call_operation();
        let error = operation
            .compile(
                &json!({"action": ECHO_ACTION}),
                &BTreeMap::from([(identifier("input"), ValueType::String)]),
            )
            .unwrap_err();
        assert_eq!(error.code(), ACTION_CALL_INPUT_TYPE_MISMATCH);

        let error = operation
            .compile(
                &json!({"action": ECHO_ACTION}),
                &BTreeMap::from([
                    (identifier("input"), ValueType::Any),
                    (identifier("extra"), ValueType::Any),
                ]),
            )
            .unwrap_err();
        assert_eq!(error.code(), ACTION_CALL_INPUT_CONTRACT_INVALID);
    }

    #[test]
    fn action_call_config_is_strict_and_unknown_action_is_data_free() {
        let (operation, _) = action_call_operation();
        let input = BTreeMap::from([(identifier("input"), ValueType::Any)]);
        let error = operation
            .compile(&json!({"action": ECHO_ACTION, "extra": true}), &input)
            .unwrap_err();
        assert_eq!(error.code(), ACTION_CALL_CONFIG_INVALID);

        let error = operation
            .compile(&json!({"action": "secret.action"}), &input)
            .unwrap_err();
        assert_eq!(error.code(), ACTION_CALL_ACTION_NOT_FOUND);
        assert!(!error.message().contains("secret.action"));
    }

    struct InvalidOutputAction;

    #[async_trait]
    impl Action for InvalidOutputAction {
        fn descriptor(&self) -> ActionDescriptor {
            ActionDescriptor {
                name: "test.invalid_output",
                input_schema: input_schema(),
                output_schema: output_schema(),
                idempotent: false,
            }
        }

        async fn call(&self, _input: Value, _context: ActionContext) -> Result<Value, RunError> {
            Ok(json!({"echoed": 7}))
        }
    }

    fn action_call_operation() -> (ActionCallOperation, ActionCalls) {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let mut actions = ActionRegistry::default();
        actions
            .register(EchoAction {
                calls: Arc::clone(&calls),
            })
            .expect("fake action should register");
        (ActionCallOperation::new(actions), calls)
    }

    fn input_schema() -> Value {
        json!({
            "type": "object",
            "properties": {"name": {"type": "string"}},
            "required": ["name"],
            "additionalProperties": false
        })
    }

    fn output_schema() -> Value {
        json!({
            "type": "object",
            "properties": {"echoed": {"type": "string"}},
            "required": ["echoed"],
            "additionalProperties": false
        })
    }

    fn identifier(value: &str) -> Identifier {
        Identifier::parse(value).unwrap()
    }

    fn operation_context() -> OperationContext {
        let (_, stop) = stop_pair();
        OperationContext::new(
            "run-1",
            OperationId::authored("/workflow/action").unwrap(),
            1,
            ExecutionControl::new(stop, Duration::from_secs(1)),
        )
    }
}
