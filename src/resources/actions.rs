use std::{collections::BTreeMap, sync::Arc};

use async_trait::async_trait;
use serde_json::Value;

use crate::{
    dsl::CompileError,
    runtime::{ExecutionControl, RunError},
    schema::{compile_schema, JsonSchemaValidator},
};

#[derive(Debug, Clone)]
pub struct ActionDescriptor {
    pub name: &'static str,
    pub input_schema: Value,
    pub output_schema: Value,
    pub idempotent: bool,
    pub streams_content: bool,
}

#[derive(Clone)]
pub struct ActionContext {
    pub run_id: String,
    pub node_id: String,
    pub control: ExecutionControl,
}

impl ActionContext {
    pub fn new(
        run_id: impl Into<String>,
        node_id: impl Into<String>,
        control: ExecutionControl,
    ) -> Self {
        Self {
            run_id: run_id.into(),
            node_id: node_id.into(),
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
    action: Arc<dyn Action>,
    input_validator: JsonSchemaValidator,
    output_validator: JsonSchemaValidator,
}

impl RegisteredAction {
    pub fn descriptor(&self) -> &ActionDescriptor {
        &self.descriptor
    }

    pub fn validate_input(&self, input: &Value) -> Result<(), RunError> {
        validate_json(
            &self.input_validator,
            input,
            "ACTION_INPUT_INVALID",
            "action input validation failed",
        )
    }

    pub async fn call(&self, input: Value, context: ActionContext) -> Result<Value, RunError> {
        self.validate_input(&input)?;
        let output = self.action.call(input, context).await?;
        validate_json(
            &self.output_validator,
            &output,
            "ACTION_OUTPUT_INVALID",
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
        return Err(RunError::new(code, message));
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
        let descriptor = action.descriptor();
        let name = descriptor.name;
        if name.trim().is_empty() {
            return Err(CompileError::new(
                "ACTION_NAME_INVALID",
                "action name must not be empty",
            ));
        }
        if self.actions.contains_key(name) {
            return Err(CompileError::new(
                "DUPLICATE_ACTION",
                format!("action '{name}' is already registered"),
            ));
        }
        let input_validator = compile_schema(&descriptor.input_schema).map_err(|error| {
            CompileError::new(
                "ACTION_INPUT_SCHEMA_INVALID",
                format!("action '{name}' input schema is invalid: {error}"),
            )
        })?;
        let output_validator = compile_schema(&descriptor.output_schema).map_err(|error| {
            CompileError::new(
                "ACTION_OUTPUT_SCHEMA_INVALID",
                format!("action '{name}' output schema is invalid: {error}"),
            )
        })?;
        self.actions.insert(
            name.to_string(),
            Arc::new(RegisteredAction {
                descriptor,
                action: Arc::new(action),
                input_validator,
                output_validator,
            }),
        );
        Ok(())
    }

    pub fn resolve(&self, name: &str) -> Result<Arc<RegisteredAction>, CompileError> {
        self.actions.get(name).cloned().ok_or_else(|| {
            CompileError::new(
                "ACTION_NOT_FOUND",
                format!("action '{name}' is not registered"),
            )
        })
    }

    pub fn names(&self) -> impl Iterator<Item = &str> {
        self.actions.keys().map(String::as_str)
    }
}
