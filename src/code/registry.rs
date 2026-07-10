use std::{collections::BTreeMap, future::Future, pin::Pin, sync::Arc};

use async_trait::async_trait;
use serde_json::Value;

use crate::error::AppError;

type EmitText =
    Arc<dyn Fn(String) -> Pin<Box<dyn Future<Output = Result<(), AppError>> + Send>> + Send + Sync>;

#[derive(Clone)]
pub struct CodeContext {
    run_id: String,
    emit_text: EmitText,
}

impl CodeContext {
    pub(crate) fn new(run_id: String, emit_text: EmitText) -> Self {
        Self { run_id, emit_text }
    }

    pub fn run_id(&self) -> &str {
        &self.run_id
    }

    pub async fn emit_text(&self, content: impl Into<String>) -> Result<(), AppError> {
        (self.emit_text)(content.into()).await
    }
}

#[async_trait]
pub trait CodeHandler: Send + Sync {
    fn name(&self) -> &'static str;
    async fn call(&self, input: Value, ctx: CodeContext) -> Result<Value, AppError>;
}

#[derive(Clone, Default)]
pub struct CodeRegistry {
    handlers: BTreeMap<String, Arc<dyn CodeHandler>>,
}

impl CodeRegistry {
    pub fn register<T: CodeHandler + 'static>(&mut self, handler: T) {
        self.handlers
            .insert(handler.name().to_string(), Arc::new(handler));
    }

    pub fn get(&self, name: &str) -> Option<Arc<dyn CodeHandler>> {
        self.handlers.get(name).cloned()
    }

    pub fn len(&self) -> usize {
        self.handlers.len()
    }

    pub fn is_empty(&self) -> bool {
        self.handlers.is_empty()
    }

    pub fn names(&self) -> impl Iterator<Item = &str> {
        self.handlers.keys().map(String::as_str)
    }
}
