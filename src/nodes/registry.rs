use std::{collections::BTreeMap, sync::Arc};

use async_trait::async_trait;
use serde_json::Value;

use crate::{
    dsl::{
        compiled::{CompiledNode, NodeCompilation, NodeOutcome},
        compiler::CompileContext,
        CompileError,
    },
    runtime::{ExecutionControl, RunContext, RunError},
};

pub trait NodeType: Send + Sync {
    fn kind(&self) -> &'static str;
    fn compile(
        &self,
        node_id: &str,
        config: Value,
        context: &mut CompileContext<'_>,
    ) -> Result<NodeCompilation, CompileError>;
}

#[async_trait]
pub trait NodeExecutor: Send + Sync {
    async fn execute(
        &self,
        node: &CompiledNode,
        context: &RunContext,
        control: &ExecutionControl,
    ) -> Result<NodeOutcome, RunError>;
}

#[derive(Clone, Default)]
pub struct NodeTypeRegistry {
    types: BTreeMap<String, Arc<dyn NodeType>>,
}

impl NodeTypeRegistry {
    pub fn register<T>(&mut self, node_type: T) -> Result<(), CompileError>
    where
        T: NodeType + 'static,
    {
        let kind = node_type.kind();
        if kind.trim().is_empty() {
            return Err(CompileError::new(
                "NODE_TYPE_INVALID",
                "node type must not be empty",
            ));
        }
        if self.types.contains_key(kind) {
            return Err(CompileError::new(
                "DUPLICATE_NODE_TYPE",
                format!("node type '{kind}' is already registered"),
            ));
        }
        self.types.insert(kind.to_string(), Arc::new(node_type));
        Ok(())
    }

    pub fn resolve(&self, kind: &str) -> Result<Arc<dyn NodeType>, CompileError> {
        self.types.get(kind).cloned().ok_or_else(|| {
            CompileError::new(
                "NODE_TYPE_NOT_FOUND",
                format!("node type '{kind}' is not registered"),
            )
        })
    }
}

#[derive(Clone, Default)]
pub struct NodeExecutorRegistry {
    executors: BTreeMap<String, Arc<dyn NodeExecutor>>,
}

impl NodeExecutorRegistry {
    pub fn register<T>(&mut self, executor: T) -> Result<(), CompileError>
    where
        T: NodeType + NodeExecutor + 'static,
    {
        let kind = executor.kind();
        if self.executors.contains_key(kind) {
            return Err(CompileError::new(
                "DUPLICATE_NODE_EXECUTOR",
                format!("node executor '{kind}' is already registered"),
            ));
        }
        self.executors.insert(kind.to_string(), Arc::new(executor));
        Ok(())
    }

    pub fn resolve(&self, kind: &str) -> Result<Arc<dyn NodeExecutor>, RunError> {
        self.executors.get(kind).cloned().ok_or_else(|| {
            RunError::new(
                "NODE_EXECUTOR_NOT_FOUND",
                format!("node executor '{kind}' is not registered"),
            )
        })
    }
}
