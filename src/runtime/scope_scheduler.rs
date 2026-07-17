//! Recursive structured scheduler for verified vNext Region/SSA workflows.
//!
//! Every child future is owned by its parent scope. Parallel scopes close
//! admission, cancel admitted children, and drain every future before they
//! return; this module never creates detached Tokio tasks.

use std::{
    collections::{BTreeMap, VecDeque},
    future::Future,
    io::Write,
    panic::AssertUnwindSafe,
    pin::Pin,
    sync::Arc,
    time::Duration,
};

use cel::{Context as CelContext, Program as CelProgram, Value as CelValue};
use futures::{future::BoxFuture, stream::FuturesUnordered, FutureExt, StreamExt};
use handlebars::{no_escape, Handlebars, Template};
use serde::Serialize;
use serde_json::{json, Map, Value};
use tokio::{
    sync::Semaphore,
    time::{sleep_until, Instant},
};
use tokio_util::sync::CancellationToken;

use crate::{
    dsl::vnext::{
        compiler::CompiledWorkflow,
        ir::{
            self, Branch, Operation as IrOperation, OperationKind, Parallel, ParameterSource, Phi,
            Region, RegionId, RootReturn as IrRootReturn, Terminator, ValueId, WorkflowIr,
        },
        operation::{EvaluatedCall, OperationContext, OperationRegistry},
        raw::{is_valid_error_code, OutputFormat, ParallelSettle},
        types::ValueType,
        value::Identifier,
    },
    events::{
        hub::EventHub,
        operation::{OperationEventPublisher, OperationEventScope},
        protocol::RunEventScope,
    },
    observability::{duration_ms, json_size_bytes},
    outcome::{RunOutput, TerminalOutcome, WorkflowError},
    schema::{compile_schema_2020, JsonSchemaValidator},
};

use super::{
    stop_pair, ExecutionControl, RunError, RunErrorKind, RunExecutionResult, RunMetadata,
    StopReason, StopSignal,
};

const IR_INVALID: &str = "VNEXT_IR_INVALID";
const CONFIG_INVALID: &str = "VNEXT_SCOPE_SCHEDULER_CONFIG_INVALID";
const INPUT_INVALID: &str = "VNEXT_INPUT_INVALID";
const OUTPUT_INVALID: &str = "VNEXT_OUTPUT_INVALID";
const REGION_SCHEMA_INVALID: &str = "VNEXT_REGION_SCHEMA_INVALID";
const REGION_OUTPUT_INVALID: &str = "VNEXT_REGION_OUTPUT_INVALID";
const OPERATION_REGISTRY_INVALID: &str = "VNEXT_OPERATION_REGISTRY_INVALID";
const OPERATION_CONTRACT_INVALID: &str = "VNEXT_OPERATION_CONTRACT_INVALID";
const OPERATION_OUTPUT_INVALID: &str = "VNEXT_OPERATION_OUTPUT_INVALID";
const OPERATION_EVENT_FAILED: &str = "VNEXT_OPERATION_EVENT_FAILED";
const OPERATION_CANCELLED: &str = "VNEXT_OPERATION_CANCELLED";
const STOP_DEADLINE_INVALID: &str = "VNEXT_STOP_DEADLINE_INVALID";
const SCOPE_INVARIANT: &str = "VNEXT_SCOPE_INVARIANT";
const SCOPE_TASK_PANICKED: &str = "VNEXT_SCOPE_TASK_PANICKED";
const EXPRESSION_FAILED: &str = "VNEXT_EXPRESSION_FAILED";
const TEMPLATE_FAILED: &str = "VNEXT_TEMPLATE_FAILED";
const TEMPLATE_OUTPUT_TOO_LARGE: &str = "VNEXT_TEMPLATE_OUTPUT_TOO_LARGE";
const SWITCH_PROGRAM_INVALID: &str = "VNEXT_SWITCH_PROGRAM_INVALID";
const SWITCH_EVALUATION_FAILED: &str = "VNEXT_SWITCH_EVALUATION_FAILED";
const INFRASTRUCTURE_FAILURE: &str = "INFRASTRUCTURE_FAILURE";

#[derive(Debug, Clone)]
pub struct ScopeSchedulerConfig {
    /// Leaf-operation limit local to one Run. The process-wide limit is a
    /// separate semaphore supplied by RunService.
    pub max_concurrent_operations_per_run: usize,
    /// Per-attempt timeout. Every vNext Call currently has exactly one attempt.
    pub operation_timeout: Duration,
    /// Bounded cooperative cleanup window after any attempt stop request.
    pub operation_cancel_grace_period: Duration,
    pub max_template_output_bytes: usize,
}

impl Default for ScopeSchedulerConfig {
    fn default() -> Self {
        Self {
            max_concurrent_operations_per_run: 8,
            operation_timeout: Duration::from_secs(60),
            operation_cancel_grace_period: Duration::from_secs(5),
            max_template_output_bytes: 262_144,
        }
    }
}

#[derive(Clone)]
pub struct ScopeScheduler {
    ir: Arc<WorkflowIr>,
    operations: OperationRegistry,
    input_validator: Arc<JsonSchemaValidator>,
    output_validator: Arc<JsonSchemaValidator>,
    global_operation_permits: Arc<Semaphore>,
    config: ScopeSchedulerConfig,
    operation_events: Option<OperationEventPublisher>,
}

impl ScopeScheduler {
    pub fn new(
        workflow: Arc<CompiledWorkflow>,
        global_operation_permits: Arc<Semaphore>,
        events: EventHub,
        config: ScopeSchedulerConfig,
    ) -> Self {
        Self {
            ir: Arc::clone(&workflow.ir),
            operations: workflow.operations().clone(),
            input_validator: workflow.input_validator_arc(),
            output_validator: workflow.output_validator_arc(),
            global_operation_permits,
            config,
            operation_events: Some(OperationEventPublisher::new(events)),
        }
    }

    #[cfg(test)]
    fn for_test(
        ir: Arc<WorkflowIr>,
        operations: OperationRegistry,
        config: ScopeSchedulerConfig,
    ) -> Self {
        let global_operation_permits =
            Arc::new(Semaphore::new(config.max_concurrent_operations_per_run));
        Self::for_test_with_global(ir, operations, global_operation_permits, config)
    }

    #[cfg(test)]
    fn for_test_with_global(
        ir: Arc<WorkflowIr>,
        operations: OperationRegistry,
        global_operation_permits: Arc<Semaphore>,
        config: ScopeSchedulerConfig,
    ) -> Self {
        let input_validator = Arc::new(
            compile_schema_2020(&ir.input.schema).expect("test IR input schema must compile"),
        );
        let output_validator = Arc::new(
            compile_schema_2020(&ir.output.schema).expect("test IR output schema must compile"),
        );
        Self {
            ir,
            operations,
            input_validator,
            output_validator,
            global_operation_permits,
            config,
            operation_events: None,
        }
    }

    /// Connect durable public operation events to this scheduler. Keeping the
    /// publisher explicit makes the pure scope runtime usable in compiler and
    /// verifier tests while the production coordinator always opts in.
    #[cfg(test)]
    fn with_events(mut self, events: EventHub) -> Self {
        self.operation_events = Some(OperationEventPublisher::new(events));
        self
    }

    pub fn ir(&self) -> &Arc<WorkflowIr> {
        &self.ir
    }

    pub async fn run(
        &self,
        metadata: RunMetadata,
        input: Value,
        stop: StopSignal,
    ) -> Result<RunExecutionResult, RunError> {
        self.validate_config()?;
        stop.bind_deadline(metadata.execution_deadline)
            .map_err(|_| {
                RunError::infrastructure(
                    STOP_DEADLINE_INVALID,
                    "Run stop signal was bound to a different execution deadline",
                )
            })?;
        ir::validate(&self.ir).map_err(|_| {
            RunError::infrastructure(IR_INVALID, "vNext IR failed pre-execution verification")
        })?;

        let region_validators = compile_region_validators(&self.ir.root)?;
        if !self.input_validator.is_valid(&input) {
            return Ok(RunExecutionResult::Failed(RunError::operation(
                INPUT_INVALID,
                "run input does not satisfy the vNext input contract",
            )));
        }

        let runtime = ScopeRuntime::new(
            self,
            metadata,
            input,
            stop,
            Arc::clone(&self.output_validator),
            region_validators,
        );
        let parameters = runtime.root_parameters()?;
        let root_cancel = CancellationToken::new();
        let execution =
            AssertUnwindSafe(runtime.execute_region(&self.ir.root, parameters, root_cancel))
                .catch_unwind()
                .await
                .unwrap_or_else(|_| Err(ScopeFailure::Infrastructure(scope_panic())));
        match execution {
            Ok(ScopeCompletion::WorkflowReturn(output)) => {
                Ok(RunExecutionResult::Ended(TerminalOutcome::Success {
                    output,
                }))
            }
            Ok(ScopeCompletion::Yield(_)) => Err(infrastructure(
                "workflow root returned a child RegionYield completion",
            )),
            Err(ScopeFailure::Authored { error, .. }) => {
                let declaration = self.ir.errors.get(&error).ok_or_else(|| {
                    infrastructure("verified Raise referenced an undeclared workflow error")
                })?;
                Ok(RunExecutionResult::Ended(TerminalOutcome::Failure {
                    error: WorkflowError {
                        code: declaration.code.clone(),
                        message: declaration.public_message.clone(),
                    },
                }))
            }
            Err(ScopeFailure::Operation { error, .. }) => Ok(RunExecutionResult::Failed(error)),
            Err(ScopeFailure::Stop(error)) => Ok(RunExecutionResult::Stopped(error)),
            Err(ScopeFailure::Infrastructure(error)) => Err(error),
            Err(ScopeFailure::InternalCancelled) => Err(infrastructure(
                "workflow root observed an internal scope cancellation",
            )),
        }
    }

    fn validate_config(&self) -> Result<(), RunError> {
        if self.config.max_concurrent_operations_per_run == 0
            || self.config.operation_timeout.is_zero()
            || self.config.operation_cancel_grace_period.is_zero()
            || self.config.max_template_output_bytes == 0
        {
            return Err(RunError::infrastructure(
                CONFIG_INVALID,
                "scope scheduler limits and timeouts must be positive",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
enum RuntimeValue {
    Json(Value),
    Control {
        selected_region: RegionId,
        value: Value,
    },
}

#[derive(Debug)]
enum ScopeCompletion {
    Yield(Value),
    WorkflowReturn(RunOutput),
}

#[derive(Debug, Clone)]
enum ScopeFailure {
    Authored { error: Identifier, origin: String },
    Operation { error: RunError, origin: String },
    Stop(RunError),
    Infrastructure(RunError),
    InternalCancelled,
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
enum SafeBranchErrorCategory {
    Workflow,
    Operation,
    Timeout,
}

#[derive(Debug, Serialize)]
struct SafeBranchErrorRecord<'a> {
    category: SafeBranchErrorCategory,
    code: &'a str,
    retryable: bool,
    origin: &'a str,
}

fn safe_branch_error_value(
    category: SafeBranchErrorCategory,
    code: &str,
    origin: &str,
) -> Option<Value> {
    if !is_valid_error_code(code) || !ir::is_safe_branch_origin(origin) {
        return None;
    }
    Some(
        serde_json::to_value(SafeBranchErrorRecord {
            category,
            code,
            retryable: false,
            origin,
        })
        .expect("SafeBranchError contains only infallibly serializable scalar fields"),
    )
}

type BranchFuture<'a> = BoxFuture<'a, (Identifier, Result<Value, ScopeFailure>)>;

struct ScopeRuntime<'a> {
    scheduler: &'a ScopeScheduler,
    metadata: RunMetadata,
    input: Value,
    run: Value,
    stop: StopSignal,
    output_validator: Arc<JsonSchemaValidator>,
    region_validators: BTreeMap<RegionId, JsonSchemaValidator>,
    per_run_operation_permits: Arc<Semaphore>,
    value_types: BTreeMap<ValueId, ValueType>,
}

impl<'a> ScopeRuntime<'a> {
    fn new(
        scheduler: &'a ScopeScheduler,
        metadata: RunMetadata,
        input: Value,
        stop: StopSignal,
        output_validator: Arc<JsonSchemaValidator>,
        region_validators: BTreeMap<RegionId, JsonSchemaValidator>,
    ) -> Self {
        let run = json!({
            "id": metadata.run_id,
            "request_id": metadata.request_id,
            "agent_id": metadata.agent_id,
            "agent_version": metadata.agent_version,
            "started_at": metadata.started_at,
        });
        let mut value_types = BTreeMap::new();
        collect_data_types(&scheduler.ir.root, &mut value_types);
        Self {
            scheduler,
            metadata,
            input,
            run,
            stop,
            output_validator,
            region_validators,
            per_run_operation_permits: Arc::new(Semaphore::new(
                scheduler.config.max_concurrent_operations_per_run,
            )),
            value_types,
        }
    }

    fn root_parameters(&self) -> Result<BTreeMap<ValueId, RuntimeValue>, RunError> {
        self.scheduler
            .ir
            .root
            .parameters
            .iter()
            .map(|parameter| {
                let value = match parameter.source {
                    ParameterSource::WorkflowInput => self.input.clone(),
                    ParameterSource::RunMetadata => self.run.clone(),
                    ParameterSource::Capture { .. } => {
                        return Err(infrastructure(
                            "workflow root contained a child capture parameter",
                        ))
                    }
                };
                Ok((parameter.value.id.clone(), RuntimeValue::Json(value)))
            })
            .collect()
    }

    fn execute_region<'b>(
        &'b self,
        region: &'b Region,
        parameters: BTreeMap<ValueId, RuntimeValue>,
        cancel: CancellationToken,
    ) -> BoxFuture<'b, Result<ScopeCompletion, ScopeFailure>> {
        Box::pin(async move {
            self.check_control(&cancel)?;
            let mut values = parameters;
            for operation in &region.operations {
                self.check_control(&cancel)?;
                let value = self
                    .execute_operation(region, operation, &values, cancel.clone())
                    .await?;
                values.insert(operation.output.id.clone(), value);
                self.check_control(&cancel)?;
            }
            self.execute_terminator(region, &values)
        })
    }

    async fn execute_operation(
        &self,
        region: &Region,
        operation: &IrOperation,
        values: &BTreeMap<ValueId, RuntimeValue>,
        cancel: CancellationToken,
    ) -> Result<RuntimeValue, ScopeFailure> {
        let value = match &operation.kind {
            OperationKind::Const { value } => RuntimeValue::Json(value.clone()),
            OperationKind::Project { source, path } => RuntimeValue::Json(
                project_json(
                    self.json_value(region, operation, values, source)?,
                    path.segments(),
                )
                .map_err(ScopeFailure::Infrastructure)?,
            ),
            OperationKind::Object { fields } => RuntimeValue::Json(Value::Object(
                fields
                    .iter()
                    .map(|(name, value)| {
                        Ok((
                            name.clone(),
                            self.json_value(region, operation, values, value)?.clone(),
                        ))
                    })
                    .collect::<Result<Map<_, _>, ScopeFailure>>()?,
            )),
            OperationKind::Array { items } => RuntimeValue::Json(Value::Array(
                items
                    .iter()
                    .map(|value| self.json_value(region, operation, values, value).cloned())
                    .collect::<Result<Vec<_>, _>>()?,
            )),
            OperationKind::Template { text, bindings } => RuntimeValue::Json(Value::String(
                self.render_template(region, operation, text, bindings, values)?,
            )),
            OperationKind::Call(call) => RuntimeValue::Json(
                self.execute_call(region, operation, call, values, cancel)
                    .await?,
            ),
            OperationKind::Parallel(parallel) => RuntimeValue::Json(
                self.execute_parallel(region, operation, parallel, values, cancel)
                    .await?,
            ),
            OperationKind::Branch(branch) => {
                self.execute_branch(region, operation, branch, values, cancel)
                    .await?
            }
            OperationKind::Phi(phi) => self.execute_phi(region, operation, phi, values)?,
        };
        Ok(value)
    }

    fn render_template(
        &self,
        region: &Region,
        operation: &IrOperation,
        text: &str,
        bindings: &BTreeMap<Identifier, ValueId>,
        values: &BTreeMap<ValueId, RuntimeValue>,
    ) -> Result<String, ScopeFailure> {
        let data = bindings
            .iter()
            .map(|(name, value)| {
                Ok((
                    name.as_str().to_string(),
                    self.json_value(region, operation, values, value)?.clone(),
                ))
            })
            .collect::<Result<Map<_, _>, ScopeFailure>>()?;
        let template = Template::compile(text).map_err(|_| {
            ScopeFailure::operation(
                operation,
                RunError::operation(TEMPLATE_FAILED, "vNext template could not be compiled"),
            )
        })?;
        let mut handlebars = Handlebars::new();
        handlebars.set_strict_mode(true);
        handlebars.register_escape_fn(no_escape);
        handlebars.register_template("value", template);
        let mut output =
            BoundedTemplateWriter::new(self.scheduler.config.max_template_output_bytes);
        let rendered = handlebars.render_to_write("value", &Value::Object(data), &mut output);
        if output.exceeded() {
            return Err(ScopeFailure::operation(
                operation,
                RunError::operation(
                    TEMPLATE_OUTPUT_TOO_LARGE,
                    "vNext template output exceeds the configured byte limit",
                ),
            ));
        }
        rendered.map_err(|_| {
            ScopeFailure::operation(
                operation,
                RunError::operation(TEMPLATE_FAILED, "vNext template could not be rendered"),
            )
        })?;
        String::from_utf8(output.into_bytes()).map_err(|_| {
            ScopeFailure::operation(
                operation,
                RunError::operation(TEMPLATE_FAILED, "vNext template output was not valid UTF-8"),
            )
        })
    }

    async fn execute_call(
        &self,
        region: &Region,
        operation: &IrOperation,
        call: &ir::Call,
        values: &BTreeMap<ValueId, RuntimeValue>,
        cancel: CancellationToken,
    ) -> Result<Value, ScopeFailure> {
        self.check_control(&cancel)?;
        let evaluated_inputs = call
            .inputs
            .iter()
            .map(|(name, value)| {
                Ok((
                    name.clone(),
                    self.json_value(region, operation, values, value)?.clone(),
                ))
            })
            .collect::<Result<BTreeMap<_, _>, ScopeFailure>>()?;
        let evaluated_dependencies = call
            .plan
            .dependencies()
            .into_iter()
            .map(|value| {
                Ok((
                    value.clone(),
                    self.json_value(region, operation, values, &value)?.clone(),
                ))
            })
            .collect::<Result<BTreeMap<_, _>, ScopeFailure>>()?;
        let input_types = call
            .inputs
            .iter()
            .map(|(name, value)| {
                self.value_types
                    .get(value)
                    .cloned()
                    .map(|value_type| (name.clone(), value_type))
                    .ok_or_else(|| {
                        ScopeFailure::Infrastructure(infrastructure(
                            "verified Call input did not have a static data type",
                        ))
                    })
            })
            .collect::<Result<BTreeMap<_, _>, _>>()?;
        let executor = self
            .scheduler
            .operations
            .resolve_plan(call.target, &call.plan)
            .map_err(|_| {
                ScopeFailure::Infrastructure(RunError::infrastructure(
                    OPERATION_REGISTRY_INVALID,
                    "verified Call target has no registered executor",
                ))
            })?;
        executor
            .preflight_plan(&call.plan, &input_types)
            .map_err(|error| {
                ScopeFailure::Infrastructure(RunError::infrastructure(
                    error.code(),
                    error.message(),
                ))
            })?;
        let contract = call.plan.output_contract();
        let expected_output = self.value_types.get(&operation.output.id).ok_or_else(|| {
            ScopeFailure::Infrastructure(infrastructure(
                "verified Call output did not have a static data type",
            ))
        })?;
        if !contract.value_type.is_assignable_to(expected_output)
            || !expected_output.is_assignable_to(&contract.value_type)
        {
            return Err(ScopeFailure::Infrastructure(RunError::infrastructure(
                OPERATION_CONTRACT_INVALID,
                "registered operation output type differs from verified IR",
            )));
        }
        let output_validator = compile_schema_2020(&contract.schema).map_err(|_| {
            ScopeFailure::Infrastructure(RunError::infrastructure(
                OPERATION_CONTRACT_INVALID,
                "registered operation output schema is invalid",
            ))
        })?;

        let per_run_permit = self
            .acquire_operation_permit(
                Arc::clone(&self.per_run_operation_permits),
                &cancel,
                "per-Run operation semaphore was closed",
            )
            .await?;
        let global_permit = self
            .acquire_operation_permit(
                Arc::clone(&self.scheduler.global_operation_permits),
                &cancel,
                "process operation semaphore was closed",
            )
            .await?;

        self.check_control(&cancel)?;
        let timeout = self.scheduler.config.operation_timeout;
        let operation_deadline = Instant::now() + timeout;
        let execution_deadline = self.metadata.execution_deadline;
        let attempt_deadline = std::cmp::min(operation_deadline, execution_deadline);
        let run_deadline_is_effective = execution_deadline <= operation_deadline;

        let event_scope = OperationEventScope::new(
            RunEventScope::for_run(
                &self.metadata.request_id,
                &self.metadata.run_id,
                &self.metadata.agent_id,
                &self.metadata.agent_version,
            ),
            operation.id.to_string(),
            call.target.operation_type(),
            1,
        );
        let attempt_started = Instant::now();
        if let Some(events) = &self.scheduler.operation_events {
            events
                .started(&event_scope)
                .await
                .map_err(|_| ScopeFailure::Infrastructure(operation_event_publish_failure()))?;
        }

        let (attempt_stop_controller, attempt_stop) = stop_pair();
        let control = ExecutionControl::with_deadline(attempt_stop, attempt_deadline);
        let context = OperationContext::new(
            self.metadata.run_id.clone(),
            operation.id.clone(),
            1,
            control.clone(),
        );
        let cancel_grace = self.scheduler.config.operation_cancel_grace_period;
        let result = {
            // Keep the attempt future inside this scope so every exit path drops it
            // before releasing either concurrency permit.
            let execution = AssertUnwindSafe(executor.execute_plan(
                &call.plan,
                EvaluatedCall {
                    inputs: evaluated_inputs,
                    dependencies: evaluated_dependencies,
                },
                context,
            ))
            .catch_unwind();
            tokio::pin!(execution);
            tokio::select! {
                biased;
                _ = self.stop.stopped() => {
                    let reason = self.stop.reason().unwrap_or(StopReason::Interrupted);
                    attempt_stop_controller.request(reason);
                    if let Some(error) = cleanup_infrastructure(
                        drain_operation(&mut execution, cancel_grace).await,
                    ) {
                        Err(ScopeFailure::Infrastructure(error))
                    } else {
                        Err(ScopeFailure::Stop(RunError::stopped(reason)))
                    }
                },
                _ = cancel.cancelled() => {
                    attempt_stop_controller.request(StopReason::Cancelled);
                    if let Some(error) = cleanup_infrastructure(
                        drain_operation(&mut execution, cancel_grace).await,
                    ) {
                        Err(ScopeFailure::Infrastructure(error))
                    } else {
                        Err(ScopeFailure::InternalCancelled)
                    }
                },
                _ = sleep_until(attempt_deadline) => {
                    attempt_stop_controller.request(StopReason::TimedOut);
                    if let Some(error) = cleanup_infrastructure(
                        drain_operation(&mut execution, cancel_grace).await,
                    ) {
                        Err(ScopeFailure::Infrastructure(error))
                    } else if run_deadline_is_effective {
                        Err(ScopeFailure::Stop(RunError::stopped(StopReason::TimedOut)))
                    } else {
                        Err(ScopeFailure::operation(
                            operation,
                            RunError::operation_timeout(),
                        ))
                    }
                },
                result = &mut execution => match result {
                    Ok(result) => result.map_err(|error| {
                        self.classify_operation_error(operation, error)
                    }),
                    Err(_) => Err(ScopeFailure::Infrastructure(scope_panic())),
                },
            }
        };
        drop(global_permit);
        drop(per_run_permit);

        let result = result.and_then(|output| {
            self.check_control(&cancel)?;
            if output_validator.is_valid(&output) {
                Ok(output)
            } else {
                Err(ScopeFailure::operation(
                    operation,
                    RunError::operation(
                        OPERATION_OUTPUT_INVALID,
                        "operation output does not satisfy its runtime contract",
                    ),
                ))
            }
        });
        let elapsed_ms = duration_ms(attempt_started.elapsed());
        match result {
            Ok(output) => {
                if let Some(events) = &self.scheduler.operation_events {
                    events
                        .completed(&event_scope, elapsed_ms, json_size_bytes(&output))
                        .await
                        .map_err(|_| {
                            ScopeFailure::Infrastructure(operation_event_publish_failure())
                        })?;
                }
                Ok(output)
            }
            Err(failure) => {
                if let Some(events) = &self.scheduler.operation_events {
                    let error = operation_event_error(&failure);
                    events
                        .failed(&event_scope, elapsed_ms, &error)
                        .await
                        .map_err(|_| {
                            ScopeFailure::Infrastructure(operation_event_publish_failure())
                        })?;
                }
                Err(failure)
            }
        }
    }

    async fn acquire_operation_permit(
        &self,
        semaphore: Arc<Semaphore>,
        cancel: &CancellationToken,
        closed_message: &'static str,
    ) -> Result<tokio::sync::OwnedSemaphorePermit, ScopeFailure> {
        let acquire = semaphore.acquire_owned();
        tokio::pin!(acquire);
        tokio::select! {
            biased;
            _ = self.stop.stopped() => {
                Err(ScopeFailure::Stop(stopped_error(&self.stop)))
            }
            _ = cancel.cancelled() => {
                Err(ScopeFailure::InternalCancelled)
            }
            permit = &mut acquire => permit.map_err(|_| {
                ScopeFailure::Infrastructure(infrastructure(closed_message))
            }),
        }
    }

    async fn execute_parallel(
        &self,
        parent: &Region,
        operation: &IrOperation,
        parallel: &Parallel,
        values: &BTreeMap<ValueId, RuntimeValue>,
        parent_cancel: CancellationToken,
    ) -> Result<Value, ScopeFailure> {
        let branch_limit = parallel
            .max_concurrency
            .unwrap_or(parallel.branches.len())
            .min(parallel.branches.len())
            .max(1);
        let mut pending = parallel
            .branches
            .iter()
            .map(|(name, child)| {
                Ok((
                    name.clone(),
                    child,
                    self.child_parameters(parent, operation, &parallel.inputs, child, values)?,
                ))
            })
            .collect::<Result<VecDeque<_>, ScopeFailure>>()?;
        let parallel_cancel = parent_cancel.child_token();
        let mut admitted = FuturesUnordered::<BranchFuture<'_>>::new();
        let mut results = BTreeMap::<Identifier, Value>::new();
        let mut recoverable_failure = None;
        let mut fatal_failure = None;

        while admitted.len() < branch_limit {
            let Some((name, child, parameters)) = pending.pop_front() else {
                break;
            };
            admitted.push(self.admit_branch(
                name,
                child,
                parameters,
                parallel_cancel.child_token(),
            ));
        }

        while let Some((name, result)) = admitted.next().await {
            match result {
                Ok(value) => {
                    let value = match parallel.settle {
                        ParallelSettle::All => value,
                        ParallelSettle::AllSettled => json!({
                            "status": "ok",
                            "value": value,
                        }),
                    };
                    results.insert(name, value);
                }
                Err(failure) if matches!(parallel.settle, ParallelSettle::AllSettled) => {
                    if let Some(error) = self.safe_settled_error(&failure) {
                        results.insert(name, json!({"status": "error", "error": error}));
                    } else if fatal_failure.is_none() {
                        fatal_failure = Some(failure);
                        pending.clear();
                        parallel_cancel.cancel();
                    }
                }
                Err(failure) => {
                    if matches!(failure, ScopeFailure::InternalCancelled)
                        && (recoverable_failure.is_some() || fatal_failure.is_some())
                    {
                        // Expected peer-cancellation cleanup after admission closed.
                    } else if failure.is_fatal() {
                        if fatal_failure.is_none() {
                            fatal_failure = Some(failure);
                        }
                    } else if recoverable_failure.is_none() {
                        recoverable_failure = Some(failure);
                    }
                    pending.clear();
                    parallel_cancel.cancel();
                }
            }

            if fatal_failure.is_none() && recoverable_failure.is_none() {
                while admitted.len() < branch_limit {
                    let Some((next_name, child, parameters)) = pending.pop_front() else {
                        break;
                    };
                    admitted.push(self.admit_branch(
                        next_name,
                        child,
                        parameters,
                        parallel_cancel.child_token(),
                    ));
                }
            }
        }

        if let Some(failure) = fatal_failure {
            return Err(failure);
        }
        if let Some(failure) = recoverable_failure {
            return Err(failure);
        }
        if results.len() != parallel.branches.len() {
            return Err(ScopeFailure::Infrastructure(infrastructure(
                "Parallel drained without producing every required branch result",
            )));
        }
        Ok(Value::Object(
            results
                .into_iter()
                .map(|(name, value)| (name.as_str().to_string(), value))
                .collect(),
        ))
    }

    fn admit_branch<'b>(
        &'b self,
        name: Identifier,
        child: &'b Region,
        parameters: BTreeMap<ValueId, RuntimeValue>,
        cancel: CancellationToken,
    ) -> BranchFuture<'b> {
        Box::pin(async move {
            let execution = AssertUnwindSafe(self.execute_region(child, parameters, cancel))
                .catch_unwind()
                .await;
            let result = match execution {
                Ok(Ok(ScopeCompletion::Yield(value))) => Ok(value),
                Ok(Ok(ScopeCompletion::WorkflowReturn(_))) => Err(ScopeFailure::Infrastructure(
                    infrastructure("child region produced WorkflowReturn"),
                )),
                Ok(Err(failure)) => Err(failure),
                Err(_) => Err(ScopeFailure::Infrastructure(scope_panic())),
            };
            (name, result)
        })
    }

    async fn execute_branch(
        &self,
        parent: &Region,
        operation: &IrOperation,
        branch: &Branch,
        values: &BTreeMap<ValueId, RuntimeValue>,
        cancel: CancellationToken,
    ) -> Result<RuntimeValue, ScopeFailure> {
        let scope = branch
            .inputs
            .iter()
            .map(|(name, value)| {
                Ok((
                    name.as_str().to_string(),
                    self.json_value(parent, operation, values, value)?.clone(),
                ))
            })
            .collect::<Result<Map<_, _>, ScopeFailure>>()?;
        let mut context = CelContext::default();
        let scope_value = cel::to_value(Value::Object(scope)).map_err(|_| {
            ScopeFailure::Infrastructure(RunError::infrastructure(
                SWITCH_EVALUATION_FAILED,
                "switch scope could not be converted to CEL",
            ))
        })?;
        context.add_variable_from_value("scope", scope_value);

        let mut selected = None;
        for case in &branch.cases {
            self.check_control(&cancel)?;
            let program = CelProgram::compile(&case.predicate.source).map_err(|_| {
                ScopeFailure::Infrastructure(RunError::infrastructure(
                    SWITCH_PROGRAM_INVALID,
                    "verified switch contains an invalid CEL program",
                ))
            })?;
            match program.execute(&context).map_err(|_| {
                ScopeFailure::operation(
                    operation,
                    RunError::operation(SWITCH_EVALUATION_FAILED, "switch CEL evaluation failed"),
                )
            })? {
                CelValue::Bool(true) => {
                    selected = Some(&case.region);
                    break;
                }
                CelValue::Bool(false) => {}
                _ => {
                    return Err(ScopeFailure::operation(
                        operation,
                        RunError::operation(
                            SWITCH_EVALUATION_FAILED,
                            "switch CEL predicate must return a boolean",
                        ),
                    ))
                }
            }
        }
        let selected = selected.unwrap_or(&branch.default.region);
        let parameters =
            self.child_parameters(parent, operation, &branch.inputs, selected, values)?;
        match self.execute_region(selected, parameters, cancel).await? {
            ScopeCompletion::Yield(value) => Ok(RuntimeValue::Control {
                selected_region: selected.id.clone(),
                value,
            }),
            ScopeCompletion::WorkflowReturn(_) => Err(ScopeFailure::Infrastructure(
                infrastructure("switch arm produced WorkflowReturn"),
            )),
        }
    }

    fn execute_phi(
        &self,
        region: &Region,
        operation: &IrOperation,
        phi: &Phi,
        values: &BTreeMap<ValueId, RuntimeValue>,
    ) -> Result<RuntimeValue, ScopeFailure> {
        let value = values.get(&phi.token).ok_or_else(|| {
            ScopeFailure::Infrastructure(infrastructure("Phi control token was not defined"))
        })?;
        let RuntimeValue::Control {
            selected_region,
            value,
        } = value
        else {
            return Err(ScopeFailure::Infrastructure(infrastructure(
                "Phi consumed a JSON value instead of a control token",
            )));
        };
        if !phi.incomings.contains(selected_region) {
            return Err(ScopeFailure::Infrastructure(infrastructure(
                "Phi control token selected a non-incoming region",
            )));
        }
        if operation.output.id == phi.token {
            return Err(ScopeFailure::Infrastructure(infrastructure(
                "Phi output aliases its internal control token",
            )));
        }
        let _ = region;
        Ok(RuntimeValue::Json(value.clone()))
    }

    fn child_parameters(
        &self,
        parent: &Region,
        operation: &IrOperation,
        inputs: &BTreeMap<Identifier, ValueId>,
        child: &Region,
        values: &BTreeMap<ValueId, RuntimeValue>,
    ) -> Result<BTreeMap<ValueId, RuntimeValue>, ScopeFailure> {
        child
            .parameters
            .iter()
            .map(|parameter| {
                let source = inputs.get(&parameter.name).ok_or_else(|| {
                    ScopeFailure::Infrastructure(infrastructure(
                        "verified child capture was absent from structured inputs",
                    ))
                })?;
                match &parameter.source {
                    ParameterSource::Capture {
                        source: declared_source,
                    } if declared_source == source => {}
                    _ => {
                        return Err(ScopeFailure::Infrastructure(infrastructure(
                            "verified child parameter had an invalid capture source",
                        )))
                    }
                }
                let value = self.json_value(parent, operation, values, source)?.clone();
                Ok((parameter.value.id.clone(), RuntimeValue::Json(value)))
            })
            .collect()
    }

    fn execute_terminator(
        &self,
        region: &Region,
        values: &BTreeMap<ValueId, RuntimeValue>,
    ) -> Result<ScopeCompletion, ScopeFailure> {
        match region.terminator.as_ref().ok_or_else(|| {
            ScopeFailure::Infrastructure(infrastructure("region terminator was missing"))
        })? {
            Terminator::RegionYield { value } => {
                let value = self.terminator_json(region, values, value)?;
                let validator = self.region_validators.get(&region.id).ok_or_else(|| {
                    ScopeFailure::Infrastructure(infrastructure(
                        "verified region result validator was not precompiled",
                    ))
                })?;
                if !validator.is_valid(value) {
                    return Err(ScopeFailure::Infrastructure(RunError::infrastructure(
                        REGION_OUTPUT_INVALID,
                        "region result does not satisfy its Draft 2020-12 contract",
                    )));
                }
                Ok(ScopeCompletion::Yield(value.clone()))
            }
            Terminator::WorkflowReturn(root_return) => {
                self.execute_root_return(region, root_return, values)
            }
            Terminator::Raise { error } => Err(ScopeFailure::Authored {
                error: error.clone(),
                origin: region.id.to_string(),
            }),
        }
    }

    fn execute_root_return(
        &self,
        region: &Region,
        root_return: &IrRootReturn,
        values: &BTreeMap<ValueId, RuntimeValue>,
    ) -> Result<ScopeCompletion, ScopeFailure> {
        let data = self
            .terminator_json(region, values, &root_return.data)?
            .clone();
        if !self.output_validator.is_valid(&data) {
            return Err(ScopeFailure::region_operation(
                region,
                RunError::operation(
                    OUTPUT_INVALID,
                    "workflow result does not satisfy the vNext output contract",
                ),
            ));
        }
        let content = root_return
            .content
            .as_ref()
            .map(|value| {
                self.terminator_json(region, values, value)?
                    .as_str()
                    .map(str::to_string)
                    .ok_or_else(|| {
                        ScopeFailure::Infrastructure(infrastructure(
                            "verified root content was not a string",
                        ))
                    })
            })
            .transpose()?;
        let format = root_return.format.map(|format| match format {
            OutputFormat::Text => "text".to_string(),
            OutputFormat::Markdown => "markdown".to_string(),
        });
        Ok(ScopeCompletion::WorkflowReturn(RunOutput {
            content,
            format,
            data,
        }))
    }

    fn json_value<'b>(
        &self,
        region: &Region,
        operation: &IrOperation,
        values: &'b BTreeMap<ValueId, RuntimeValue>,
        value: &ValueId,
    ) -> Result<&'b Value, ScopeFailure> {
        match values.get(value) {
            Some(RuntimeValue::Json(value)) => Ok(value),
            Some(RuntimeValue::Control { .. }) => {
                Err(ScopeFailure::Infrastructure(RunError::infrastructure(
                    SCOPE_INVARIANT,
                    "internal control token reached a JSON operation boundary",
                )))
            }
            None => Err(ScopeFailure::Infrastructure(RunError::infrastructure(
                SCOPE_INVARIANT,
                format!(
                    "operation '{}' in region '{}' used an unavailable value",
                    operation.id, region.id
                ),
            ))),
        }
    }

    fn terminator_json<'b>(
        &self,
        region: &Region,
        values: &'b BTreeMap<ValueId, RuntimeValue>,
        value: &ValueId,
    ) -> Result<&'b Value, ScopeFailure> {
        match values.get(value) {
            Some(RuntimeValue::Json(value)) => Ok(value),
            Some(RuntimeValue::Control { .. }) => {
                Err(ScopeFailure::Infrastructure(RunError::infrastructure(
                    SCOPE_INVARIANT,
                    "internal control token reached a region terminator",
                )))
            }
            None => Err(ScopeFailure::Infrastructure(RunError::infrastructure(
                SCOPE_INVARIANT,
                format!("region '{}' returned an unavailable value", region.id),
            ))),
        }
    }

    fn safe_settled_error(&self, failure: &ScopeFailure) -> Option<Value> {
        match failure {
            ScopeFailure::Authored { error, origin } => {
                let declaration = self.scheduler.ir.errors.get(error)?;
                safe_branch_error_value(
                    SafeBranchErrorCategory::Workflow,
                    &declaration.code,
                    origin,
                )
            }
            ScopeFailure::Operation { error, origin }
                if error.kind() == RunErrorKind::Operation =>
            {
                safe_branch_error_value(SafeBranchErrorCategory::Operation, error.code(), origin)
            }
            ScopeFailure::Operation { error, origin } if error.kind() == RunErrorKind::Timeout => {
                safe_branch_error_value(SafeBranchErrorCategory::Timeout, error.code(), origin)
            }
            ScopeFailure::Operation { .. }
            | ScopeFailure::Stop(_)
            | ScopeFailure::Infrastructure(_)
            | ScopeFailure::InternalCancelled => None,
        }
    }

    fn classify_operation_error(&self, operation: &IrOperation, error: RunError) -> ScopeFailure {
        if !is_valid_error_code(error.code()) {
            return ScopeFailure::Infrastructure(RunError::infrastructure(
                INFRASTRUCTURE_FAILURE,
                "operation returned an invalid stable error code",
            ));
        }
        match error.kind() {
            RunErrorKind::Timeout if Instant::now() >= self.metadata.execution_deadline => {
                ScopeFailure::Stop(RunError::stopped(StopReason::TimedOut))
            }
            RunErrorKind::Operation | RunErrorKind::Timeout => {
                ScopeFailure::operation(operation, error)
            }
            RunErrorKind::Stop
                if self.stop.reason().is_some() && self.stop.reason() == error.stop_reason() =>
            {
                ScopeFailure::Stop(error)
            }
            RunErrorKind::Stop => ScopeFailure::Infrastructure(RunError::infrastructure(
                SCOPE_INVARIANT,
                "operation returned an unbacked Stop failure",
            )),
            RunErrorKind::Infrastructure => ScopeFailure::Infrastructure(error),
        }
    }

    fn check_control(&self, cancel: &CancellationToken) -> Result<(), ScopeFailure> {
        if let Some(reason) = self.stop.reason() {
            return Err(ScopeFailure::Stop(RunError::stopped(reason)));
        }
        if cancel.is_cancelled() {
            return Err(ScopeFailure::InternalCancelled);
        }
        Ok(())
    }
}

impl ScopeFailure {
    fn operation(operation: &IrOperation, error: RunError) -> Self {
        Self::Operation {
            error,
            origin: operation.id.to_string(),
        }
    }

    fn region_operation(region: &Region, error: RunError) -> Self {
        Self::Operation {
            error,
            origin: region.id.to_string(),
        }
    }

    fn is_fatal(&self) -> bool {
        matches!(
            self,
            Self::Stop(_) | Self::Infrastructure(_) | Self::InternalCancelled
        )
    }
}

async fn drain_operation<F>(
    execution: &mut Pin<&mut F>,
    grace_period: Duration,
) -> Option<F::Output>
where
    F: Future,
{
    tokio::select! {
        biased;
        output = execution.as_mut() => Some(output),
        _ = tokio::time::sleep(grace_period) => None,
    }
}

fn cleanup_infrastructure(
    completion: Option<std::thread::Result<Result<Value, RunError>>>,
) -> Option<RunError> {
    match completion {
        Some(Err(payload)) => {
            drop(payload);
            Some(scope_panic())
        }
        Some(Ok(Err(error))) if error.kind() == RunErrorKind::Infrastructure => Some(error),
        Some(Ok(Ok(_))) | Some(Ok(Err(_))) | None => None,
    }
}

struct BoundedTemplateWriter {
    bytes: Vec<u8>,
    max_bytes: usize,
    exceeded: bool,
}

impl BoundedTemplateWriter {
    fn new(max_bytes: usize) -> Self {
        Self {
            bytes: Vec::with_capacity(max_bytes.min(4_096)),
            max_bytes,
            exceeded: false,
        }
    }

    fn exceeded(&self) -> bool {
        self.exceeded
    }

    fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }
}

impl Write for BoundedTemplateWriter {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        if buffer.len() > self.max_bytes.saturating_sub(self.bytes.len()) {
            self.exceeded = true;
            return Err(std::io::Error::other(
                "template output exceeds configured limit",
            ));
        }
        self.bytes.extend_from_slice(buffer);
        Ok(buffer.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn stopped_error(stop: &StopSignal) -> RunError {
    RunError::stopped(stop.reason().unwrap_or(StopReason::Interrupted))
}

fn operation_event_error(failure: &ScopeFailure) -> RunError {
    match failure {
        ScopeFailure::Operation { error, .. }
        | ScopeFailure::Stop(error)
        | ScopeFailure::Infrastructure(error) => error.clone(),
        ScopeFailure::InternalCancelled => RunError::operation(
            OPERATION_CANCELLED,
            "operation was cancelled by its parent scope",
        ),
        ScopeFailure::Authored { .. } => RunError::infrastructure(
            SCOPE_INVARIANT,
            "authored workflow failure crossed a leaf operation boundary",
        ),
    }
}

fn operation_event_publish_failure() -> RunError {
    RunError::infrastructure(
        OPERATION_EVENT_FAILED,
        "failed to publish a vNext operation event",
    )
}

fn infrastructure(message: impl Into<String>) -> RunError {
    RunError::infrastructure(SCOPE_INVARIANT, message)
}

fn scope_panic() -> RunError {
    RunError::infrastructure(
        SCOPE_TASK_PANICKED,
        "vNext structured scope task panicked during execution",
    )
}

fn project_json(source: &Value, segments: &[String]) -> Result<Value, RunError> {
    let mut value = source;
    for segment in segments {
        value = match value {
            Value::Object(object) => object.get(segment).ok_or_else(|| {
                RunError::infrastructure(
                    EXPRESSION_FAILED,
                    "verified Project referenced a missing object field",
                )
            })?,
            Value::Array(array) => {
                let index = parse_index(segment).ok_or_else(|| {
                    RunError::infrastructure(
                        EXPRESSION_FAILED,
                        "verified Project used a non-canonical array index",
                    )
                })?;
                array.get(index).ok_or_else(|| {
                    RunError::infrastructure(
                        EXPRESSION_FAILED,
                        "verified Project referenced a missing array item",
                    )
                })?
            }
            _ => {
                return Err(RunError::infrastructure(
                    EXPRESSION_FAILED,
                    "verified Project traversed a scalar value",
                ))
            }
        };
    }
    Ok(value.clone())
}

fn parse_index(value: &str) -> Option<usize> {
    if value == "0" {
        return Some(0);
    }
    if value.is_empty()
        || value.starts_with('0')
        || !value.bytes().all(|byte| byte.is_ascii_digit())
    {
        return None;
    }
    value.parse().ok()
}

fn compile_region_validators(
    root: &Region,
) -> Result<BTreeMap<RegionId, JsonSchemaValidator>, RunError> {
    fn visit(
        region: &Region,
        validators: &mut BTreeMap<RegionId, JsonSchemaValidator>,
    ) -> Result<(), RunError> {
        let validator = compile_schema_2020(&region.result.schema).map_err(|_| {
            RunError::infrastructure(
                REGION_SCHEMA_INVALID,
                "verified region contains an invalid Draft 2020-12 result schema",
            )
        })?;
        validators.insert(region.id.clone(), validator);
        for operation in &region.operations {
            match &operation.kind {
                OperationKind::Parallel(parallel) => {
                    for child in parallel.branches.values() {
                        visit(child, validators)?;
                    }
                }
                OperationKind::Branch(branch) => {
                    for case in &branch.cases {
                        visit(&case.region, validators)?;
                    }
                    visit(&branch.default.region, validators)?;
                }
                OperationKind::Const { .. }
                | OperationKind::Project { .. }
                | OperationKind::Object { .. }
                | OperationKind::Array { .. }
                | OperationKind::Template { .. }
                | OperationKind::Call(_)
                | OperationKind::Phi(_) => {}
            }
        }
        Ok(())
    }

    let mut validators = BTreeMap::new();
    visit(root, &mut validators)?;
    Ok(validators)
}

fn collect_data_types(region: &Region, output: &mut BTreeMap<ValueId, ValueType>) {
    for parameter in &region.parameters {
        if let ir::IrValueType::Data(value_type) = &parameter.value.value_type {
            output.insert(parameter.value.id.clone(), value_type.clone());
        }
    }
    for operation in &region.operations {
        if let ir::IrValueType::Data(value_type) = &operation.output.value_type {
            output.insert(operation.output.id.clone(), value_type.clone());
        }
        match &operation.kind {
            OperationKind::Parallel(parallel) => {
                for child in parallel.branches.values() {
                    collect_data_types(child, output);
                }
            }
            OperationKind::Branch(branch) => {
                for case in &branch.cases {
                    collect_data_types(&case.region, output);
                }
                collect_data_types(&branch.default.region, output);
            }
            OperationKind::Const { .. }
            | OperationKind::Project { .. }
            | OperationKind::Object { .. }
            | OperationKind::Array { .. }
            | OperationKind::Template { .. }
            | OperationKind::Call(_)
            | OperationKind::Phi(_) => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Mutex,
    };

    use async_trait::async_trait;
    use chrono::Utc;
    use semver::Version;
    use serde_json::json;
    use tokio::sync::Notify;

    use crate::{
        dsl::vnext::{
            ir::{
                BranchCase, BranchDefault, Call, CelProgram, CompiledPrompt, IrValueType,
                OperationId, ParameterSource, RegionKind, RegionParameter, TypedContract,
                ValueDefinition,
            },
            operation::{CompiledOperationContract, Operation, OperationEffect, OperationError},
            plan::{CallPlan, CallTarget, CompiledActionPlan},
            raw::{ErrorCategory, ErrorDeclaration, Metadata},
            types::{ObjectType, PropertyType},
        },
        events::hub::{EventHub, EventHubConfig},
        history::{
            repository::RunRepository,
            sqlite::SqliteRunRepository,
            types::{NewRun, RunAttachment},
        },
        runtime::{stop_pair, StopReason},
    };

    use super::*;

    #[derive(Default)]
    struct Tracker {
        started: AtomicUsize,
        cleaned: AtomicUsize,
        remaining_millis: AtomicU64,
        attempts: Mutex<Vec<(u32, String)>>,
        notify: Notify,
    }

    impl Tracker {
        fn start(&self, context: &OperationContext) {
            self.started.fetch_add(1, Ordering::SeqCst);
            self.attempts
                .lock()
                .unwrap()
                .push((context.attempt, context.operation_id.to_string()));
            self.notify.notify_waiters();
        }

        async fn wait_started(&self) {
            while self.started.load(Ordering::SeqCst) == 0 {
                self.notify.notified().await;
            }
        }

        async fn wait_started_count(&self, expected: usize) {
            while self.started.load(Ordering::SeqCst) < expected {
                self.notify.notified().await;
            }
        }
    }

    #[derive(Clone)]
    enum FakeBehavior {
        Return(&'static str),
        Fail,
        FailWithCode(&'static str),
        FailAfterStarted { other: Arc<Tracker> },
        PanicAfterStarted { other: Arc<Tracker> },
        PanicAfterStop { delay: Duration },
        InfrastructureAfterStop,
        WaitForStop { cleanup: Duration },
    }

    #[derive(Clone)]
    struct FakeOperation {
        uses: &'static str,
        behavior: FakeBehavior,
        tracker: Arc<Tracker>,
    }

    #[async_trait]
    impl Operation for FakeOperation {
        fn uses(&self) -> &'static str {
            self.uses
        }

        fn compile(
            &self,
            _config: &Value,
            _inputs: &BTreeMap<Identifier, ValueType>,
        ) -> Result<CompiledOperationContract, OperationError> {
            Ok(CompiledOperationContract {
                output_schema: json!({"type": "string"}),
                output_type: ValueType::String,
                effect: OperationEffect::ExternalAction,
                idempotent: true,
            })
        }

        async fn execute(
            &self,
            _config: &Value,
            _inputs: BTreeMap<Identifier, Value>,
            context: OperationContext,
        ) -> Result<Value, RunError> {
            self.tracker.start(&context);
            match &self.behavior {
                FakeBehavior::Return(value) => Ok(json!(value)),
                FakeBehavior::Fail => Err(RunError::operation(
                    "FAKE_OPERATION_FAILED",
                    "diagnostic that must not enter a settled envelope",
                )),
                FakeBehavior::FailWithCode(code) => Err(RunError::operation(
                    code,
                    "invalid-code diagnostic that must not enter a settled envelope",
                )),
                FakeBehavior::FailAfterStarted { other } => {
                    other.wait_started().await;
                    Err(RunError::operation(
                        "FAKE_OPERATION_FAILED",
                        "fake operation failed",
                    ))
                }
                FakeBehavior::PanicAfterStarted { other } => {
                    other.wait_started().await;
                    panic!("diagnostic panic that must become infrastructure")
                }
                FakeBehavior::PanicAfterStop { delay } => {
                    context.control.stopped().await;
                    tokio::time::sleep(*delay).await;
                    panic!("cleanup panic that must become infrastructure")
                }
                FakeBehavior::InfrastructureAfterStop => {
                    context.control.stopped().await;
                    Err(RunError::infrastructure(
                        "FAKE_CLEANUP_INFRASTRUCTURE",
                        "cleanup infrastructure failure",
                    ))
                }
                FakeBehavior::WaitForStop { cleanup } => {
                    self.tracker.remaining_millis.store(
                        context.control.remaining().as_millis() as u64,
                        Ordering::SeqCst,
                    );
                    context.control.stopped().await;
                    tokio::time::sleep(*cleanup).await;
                    self.tracker.cleaned.fetch_add(1, Ordering::SeqCst);
                    Err(RunError::stopped(
                        context
                            .control
                            .stop_reason()
                            .unwrap_or(StopReason::Interrupted),
                    ))
                }
            }
        }
    }

    fn identifier(value: &str) -> Identifier {
        Identifier::parse(value).unwrap()
    }

    fn region_id(path: &str) -> RegionId {
        RegionId::new(path).unwrap()
    }

    fn input_id() -> ValueId {
        ValueId::parameter("/workflow", 0).unwrap()
    }

    fn run_id() -> ValueId {
        ValueId::parameter("/workflow", 1).unwrap()
    }

    fn data(id: ValueId, value_type: ValueType) -> ValueDefinition {
        ValueDefinition {
            id,
            value_type: IrValueType::Data(value_type),
        }
    }

    fn control(id: ValueId, result_type: ValueType) -> ValueDefinition {
        ValueDefinition {
            id,
            value_type: IrValueType::Control { result_type },
        }
    }

    fn object_type(fields: impl IntoIterator<Item = (&'static str, ValueType)>) -> ValueType {
        ValueType::Object(ObjectType {
            properties: fields
                .into_iter()
                .map(|(name, value_type)| {
                    (
                        name.to_string(),
                        PropertyType {
                            value_type,
                            required: true,
                        },
                    )
                })
                .collect(),
            additional_properties: None,
        })
    }

    fn settled_type(value_type: ValueType) -> ValueType {
        ir::settled_type(value_type)
    }

    fn schema_for_type(value_type: &ValueType) -> Value {
        match value_type {
            ValueType::Never => Value::Bool(false),
            ValueType::Any => Value::Bool(true),
            ValueType::Null => json!({"type": "null"}),
            ValueType::Boolean => json!({"type": "boolean"}),
            ValueType::Integer => json!({"type": "integer"}),
            ValueType::Number => json!({"type": "number"}),
            ValueType::String => json!({"type": "string"}),
            ValueType::Literal(value) => json!({"const": value}),
            ValueType::Array(array) => json!({
                "type": "array",
                "items": schema_for_type(&array.items),
                "minItems": array.min_items,
            }),
            ValueType::Object(object) => {
                let properties = object
                    .properties
                    .iter()
                    .map(|(name, property)| (name.clone(), schema_for_type(&property.value_type)))
                    .collect::<Map<_, _>>();
                let required = object
                    .properties
                    .iter()
                    .filter(|(_, property)| property.required)
                    .map(|(name, _)| Value::String(name.clone()))
                    .collect::<Vec<_>>();
                let additional = object
                    .additional_properties
                    .as_deref()
                    .map(schema_for_type)
                    .unwrap_or(Value::Bool(false));
                json!({
                    "type": "object",
                    "properties": properties,
                    "required": required,
                    "additionalProperties": additional,
                })
            }
            ValueType::Union(variants) => json!({
                "oneOf": variants.iter().map(schema_for_type).collect::<Vec<_>>(),
            }),
        }
    }

    fn metadata() -> RunMetadata {
        RunMetadata {
            run_id: "run-vnext".to_string(),
            request_id: "request-vnext".to_string(),
            agent_id: "agent-vnext".to_string(),
            agent_version: "version-vnext".to_string(),
            started_at: Utc::now(),
            execution_deadline: Instant::now() + Duration::from_secs(5),
        }
    }

    fn workflow(
        operations: Vec<IrOperation>,
        result: ValueId,
        result_type: ValueType,
        input_schema: Value,
        output_schema: Value,
    ) -> WorkflowIr {
        let output = TypedContract {
            schema: output_schema,
            value_type: result_type,
        };
        WorkflowIr {
            metadata: Metadata {
                id: identifier("test_agent"),
                name: "Test Agent".to_string(),
                description: String::new(),
            },
            input: TypedContract {
                schema: input_schema,
                value_type: ValueType::String,
            },
            output: output.clone(),
            prompts: BTreeMap::<Identifier, CompiledPrompt>::new(),
            errors: BTreeMap::from([(
                identifier("declared_failure"),
                ErrorDeclaration {
                    category: ErrorCategory::Workflow,
                    code: "DECLARED_FAILURE".to_string(),
                    public_message: "A declared branch failed.".to_string(),
                },
            )]),
            root: Region {
                id: region_id("/workflow"),
                kind: RegionKind::Workflow,
                parameters: vec![
                    RegionParameter {
                        name: identifier("input"),
                        value: data(input_id(), ValueType::String),
                        source: ParameterSource::WorkflowInput,
                    },
                    RegionParameter {
                        name: identifier("run"),
                        value: data(run_id(), crate::dsl::vnext::types::safe_run_metadata_type()),
                        source: ParameterSource::RunMetadata,
                    },
                ],
                operations,
                result: output,
                terminator: Some(Terminator::WorkflowReturn(IrRootReturn {
                    content: None,
                    format: None,
                    data: result,
                })),
            },
        }
    }

    fn config() -> ScopeSchedulerConfig {
        ScopeSchedulerConfig {
            max_concurrent_operations_per_run: 16,
            operation_timeout: Duration::from_millis(200),
            operation_cancel_grace_period: Duration::from_millis(100),
            max_template_output_bytes: 16_384,
        }
    }

    fn register(
        registry: &mut OperationRegistry,
        uses: &'static str,
        behavior: FakeBehavior,
    ) -> Arc<Tracker> {
        let tracker = Arc::new(Tracker::default());
        registry
            .register(FakeOperation {
                uses,
                behavior,
                tracker: Arc::clone(&tracker),
            })
            .unwrap();
        tracker
    }

    fn capture_parameter(region_path: &str, name: &str, source: ValueId) -> RegionParameter {
        RegionParameter {
            name: identifier(name),
            value: data(
                ValueId::parameter(region_path, 0).unwrap(),
                ValueType::String,
            ),
            source: ParameterSource::Capture { source },
        }
    }

    fn call_operations(path: &str, uses: &'static str) -> Vec<IrOperation> {
        let input_object = ValueId::expression(path, 0).unwrap();
        let input_type = object_type([]);
        let input_contract = TypedContract {
            schema: schema_for_type(&input_type),
            value_type: input_type.clone(),
        };
        let output_contract = TypedContract {
            schema: json!({"type":"string"}),
            value_type: ValueType::String,
        };
        vec![
            IrOperation {
                id: OperationId::expression(path, 0).unwrap(),
                output: data(input_object.clone(), input_type),
                kind: OperationKind::Object {
                    fields: BTreeMap::new(),
                },
            },
            IrOperation {
                id: OperationId::authored(path).unwrap(),
                output: data(ValueId::output(path).unwrap(), ValueType::String),
                kind: OperationKind::Call(Box::new(Call {
                    target: CallTarget::ActionCall,
                    inputs: BTreeMap::from([(identifier("input"), input_object.clone())]),
                    plan: CallPlan::Action(CompiledActionPlan {
                        action_id: uses.to_string(),
                        descriptor_version: Version::new(1, 0, 0),
                        descriptor_hash: "ab".repeat(32),
                        input_object,
                        input_contract,
                        output_contract,
                    }),
                })),
            },
        ]
    }

    fn call_branch(
        path: &str,
        name: &str,
        source: ValueId,
        uses: &'static str,
        kind: RegionKind,
    ) -> Region {
        let call_path = format!("{path}/call");
        let output = ValueId::output(&call_path).unwrap();
        Region {
            id: region_id(path),
            kind,
            parameters: vec![capture_parameter(path, name, source)],
            operations: call_operations(&call_path, uses),
            result: TypedContract {
                schema: json!({"type": "string"}),
                value_type: ValueType::String,
            },
            terminator: Some(Terminator::RegionYield { value: output }),
        }
    }

    fn nested_switch_workflow() -> WorkflowIr {
        let nested_path = "/workflow/outer/branches/nested";
        let nested_input = ValueId::parameter(nested_path, 0).unwrap();
        let switch_path = format!("{nested_path}/select");
        let first = call_branch(
            &format!("{switch_path}/cases/first"),
            "value",
            nested_input.clone(),
            "test.first",
            RegionKind::SwitchArm {
                name: identifier("first"),
                is_default: false,
            },
        );
        let second = call_branch(
            &format!("{switch_path}/cases/second"),
            "value",
            nested_input.clone(),
            "test.second",
            RegionKind::SwitchArm {
                name: identifier("second"),
                is_default: false,
            },
        );
        let fallback = call_branch(
            &format!("{switch_path}/default/fallback"),
            "value",
            nested_input.clone(),
            "test.fallback",
            RegionKind::SwitchArm {
                name: identifier("fallback"),
                is_default: true,
            },
        );
        let incomings = vec![first.id.clone(), second.id.clone(), fallback.id.clone()];
        let token = ValueId::control(&switch_path).unwrap();
        let merged = ValueId::phi(&switch_path).unwrap();
        let nested = Region {
            id: region_id(nested_path),
            kind: RegionKind::ParallelBranch {
                name: identifier("nested"),
            },
            parameters: vec![capture_parameter(nested_path, "input", input_id())],
            operations: vec![
                IrOperation {
                    id: OperationId::authored(&switch_path).unwrap(),
                    output: control(token.clone(), ValueType::String),
                    kind: OperationKind::Branch(Box::new(Branch {
                        inputs: BTreeMap::from([(identifier("value"), nested_input)]),
                        cases: vec![
                            BranchCase {
                                id: identifier("first"),
                                predicate: CelProgram {
                                    source: "true".to_string(),
                                },
                                region: first,
                            },
                            BranchCase {
                                id: identifier("second"),
                                predicate: CelProgram {
                                    source: "true".to_string(),
                                },
                                region: second,
                            },
                        ],
                        default: BranchDefault {
                            id: identifier("fallback"),
                            region: fallback,
                        },
                    })),
                },
                IrOperation {
                    id: OperationId::phi(&switch_path).unwrap(),
                    output: data(merged.clone(), ValueType::String),
                    kind: OperationKind::Phi(Phi {
                        branch: OperationId::authored(&switch_path).unwrap(),
                        token,
                        incomings,
                    }),
                },
            ],
            result: TypedContract {
                schema: json!({"type": "string"}),
                value_type: ValueType::String,
            },
            terminator: Some(Terminator::RegionYield { value: merged }),
        };
        let plain = call_branch(
            "/workflow/outer/branches/plain",
            "input",
            input_id(),
            "test.plain",
            RegionKind::ParallelBranch {
                name: identifier("plain"),
            },
        );
        let output_type =
            object_type([("nested", ValueType::String), ("plain", ValueType::String)]);
        let output = ValueId::output("/workflow/outer").unwrap();
        workflow(
            vec![IrOperation {
                id: OperationId::authored("/workflow/outer").unwrap(),
                output: data(output.clone(), output_type.clone()),
                kind: OperationKind::Parallel(Parallel {
                    inputs: BTreeMap::from([(identifier("input"), input_id())]),
                    settle: ParallelSettle::All,
                    max_concurrency: Some(2),
                    branches: BTreeMap::from([
                        (identifier("nested"), nested),
                        (identifier("plain"), plain),
                    ]),
                }),
            }],
            output,
            output_type,
            json!({"type": "string"}),
            json!({
                "type": "object",
                "required": ["nested", "plain"],
                "properties": {
                    "nested": {"type": "string"},
                    "plain": {"type": "string"}
                },
                "additionalProperties": false
            }),
        )
    }

    #[tokio::test]
    async fn executes_nested_parallel_switch_in_order_and_phi_keeps_control_internal() {
        let mut registry = OperationRegistry::default();
        let first = register(&mut registry, "test.first", FakeBehavior::Return("first"));
        let second = register(&mut registry, "test.second", FakeBehavior::Return("second"));
        let fallback = register(
            &mut registry,
            "test.fallback",
            FakeBehavior::Return("fallback"),
        );
        let plain = register(&mut registry, "test.plain", FakeBehavior::Return("plain"));
        let scheduler =
            ScopeScheduler::for_test(Arc::new(nested_switch_workflow()), registry, config());
        let (_, stop) = stop_pair();

        let result = scheduler
            .run(metadata(), json!("question"), stop)
            .await
            .unwrap();

        let RunExecutionResult::Ended(TerminalOutcome::Success { output }) = result else {
            panic!("expected successful terminal output")
        };
        assert_eq!(output.data, json!({"nested": "first", "plain": "plain"}));
        assert_eq!(first.started.load(Ordering::SeqCst), 1);
        assert_eq!(plain.started.load(Ordering::SeqCst), 1);
        assert_eq!(second.started.load(Ordering::SeqCst), 0);
        assert_eq!(fallback.started.load(Ordering::SeqCst), 0);
        assert_eq!(first.attempts.lock().unwrap()[0].0, 1);
        assert!(first.attempts.lock().unwrap()[0]
            .1
            .starts_with("/workflow/outer/branches/nested/select/cases/first/call#"));
    }

    fn nested_switch_contract_failure_workflow() -> WorkflowIr {
        let mut ir = nested_switch_workflow();
        let branch_type = settled_type(ValueType::String);
        let output_type = object_type([("nested", branch_type.clone()), ("plain", branch_type)]);
        let operation = &mut ir.root.operations[0];
        operation.output.value_type = IrValueType::Data(output_type.clone());
        let OperationKind::Parallel(parallel) = &mut operation.kind else {
            unreachable!()
        };
        parallel.settle = ParallelSettle::AllSettled;
        let nested = parallel.branches.get_mut(&identifier("nested")).unwrap();
        let OperationKind::Branch(branch) = &mut nested.operations[0].kind else {
            unreachable!()
        };
        let constraint = json!({"type": "string", "minLength": 64});
        for case in &mut branch.cases {
            case.region.result.schema = constraint.clone();
        }
        branch.default.region.result.schema = constraint;

        let output = TypedContract {
            schema: schema_for_type(&output_type),
            value_type: output_type,
        };
        ir.output = output.clone();
        ir.root.result = output;
        ir
    }

    #[tokio::test]
    async fn nested_region_schema_failure_escapes_all_settled_as_infrastructure() {
        let mut registry = OperationRegistry::default();
        register(
            &mut registry,
            "test.first",
            FakeBehavior::Return("PRIVATE_INVALID_VALUE"),
        );
        let second = register(&mut registry, "test.second", FakeBehavior::Return("second"));
        let fallback = register(
            &mut registry,
            "test.fallback",
            FakeBehavior::Return("fallback"),
        );
        register(&mut registry, "test.plain", FakeBehavior::Return("plain"));
        let scheduler = ScopeScheduler::for_test(
            Arc::new(nested_switch_contract_failure_workflow()),
            registry,
            config(),
        );
        let (_, stop) = stop_pair();

        let error = scheduler
            .run(metadata(), json!("question"), stop)
            .await
            .unwrap_err();

        assert_eq!(error.kind(), RunErrorKind::Infrastructure);
        assert_eq!(error.code(), REGION_OUTPUT_INVALID);
        assert!(!error.to_string().contains("PRIVATE_INVALID_VALUE"));
        assert_eq!(second.started.load(Ordering::SeqCst), 0);
        assert_eq!(fallback.started.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn publishes_only_operation_lifecycle_metadata_without_output() {
        let output = ValueId::output("/workflow/emit").unwrap();
        let ir = workflow(
            call_operations("/workflow/emit", "test.emit"),
            output,
            ValueType::String,
            json!({"type": "string"}),
            json!({"type": "string"}),
        );
        let mut registry = OperationRegistry::default();
        register(
            &mut registry,
            "test.emit",
            FakeBehavior::Return("protected-output-value"),
        );

        let metadata = metadata();
        let repository = Arc::new(SqliteRunRepository::in_memory().await.unwrap());
        repository
            .create_run(NewRun {
                run_id: metadata.run_id.clone(),
                request_id: metadata.request_id.clone(),
                agent_id: metadata.agent_id.clone(),
                agent_version: metadata.agent_version.clone(),
                attachment: RunAttachment::Attached,
                created_at: metadata.started_at,
                input_summary: json!({}),
            })
            .await
            .unwrap();
        let events = EventHub::new(
            repository.clone(),
            EventHubConfig {
                subscriber_capacity: 8,
                journal_capacity: 8,
                journal_batch_size: 4,
                operation_timeout: Duration::from_secs(1),
            },
        );
        let scheduler =
            ScopeScheduler::for_test(Arc::new(ir), registry, config()).with_events(events.clone());
        let (_, stop) = stop_pair();

        let result = scheduler
            .run(metadata, json!("question"), stop)
            .await
            .unwrap();
        assert!(matches!(
            result,
            RunExecutionResult::Ended(TerminalOutcome::Success { .. })
        ));
        events.flush().await.unwrap();

        let stored = repository
            .list_events_after("run-vnext", 0, 10)
            .await
            .unwrap();
        assert_eq!(
            stored
                .iter()
                .map(|event| event.event_type)
                .collect::<Vec<_>>(),
            vec![
                crate::events::RunEventType::OperationStarted,
                crate::events::RunEventType::OperationCompleted,
            ]
        );
        let completed = &stored[1];
        assert_eq!(completed.data["operation_type"], json!("action.call"));
        assert!(completed.data.get("output_bytes").is_some());
        assert!(!serde_json::to_string(completed)
            .unwrap()
            .contains("protected-output-value"));
    }

    fn all_parallel_workflow() -> WorkflowIr {
        let branch = |name: &str, uses: &'static str| {
            call_branch(
                &format!("/workflow/all/branches/{name}"),
                "input",
                input_id(),
                uses,
                RegionKind::ParallelBranch {
                    name: identifier(name),
                },
            )
        };
        let output_type = object_type([
            ("a_fail", ValueType::String),
            ("b_slow", ValueType::String),
            ("c_never", ValueType::String),
        ]);
        let output = ValueId::output("/workflow/all").unwrap();
        workflow(
            vec![IrOperation {
                id: OperationId::authored("/workflow/all").unwrap(),
                output: data(output.clone(), output_type.clone()),
                kind: OperationKind::Parallel(Parallel {
                    inputs: BTreeMap::from([(identifier("input"), input_id())]),
                    settle: ParallelSettle::All,
                    max_concurrency: Some(2),
                    branches: BTreeMap::from([
                        (identifier("a_fail"), branch("a_fail", "test.fail")),
                        (identifier("b_slow"), branch("b_slow", "test.slow")),
                        (identifier("c_never"), branch("c_never", "test.never")),
                    ]),
                }),
            }],
            output,
            output_type.clone(),
            json!({"type": "string"}),
            schema_for_type(&output_type),
        )
    }

    #[tokio::test]
    async fn all_closes_admission_cooperatively_cancels_and_drains_admitted_children() {
        let mut registry = OperationRegistry::default();
        let slow = Arc::new(Tracker::default());
        let fail = Arc::new(Tracker::default());
        registry
            .register(FakeOperation {
                uses: "test.fail",
                behavior: FakeBehavior::FailAfterStarted {
                    other: Arc::clone(&slow),
                },
                tracker: Arc::clone(&fail),
            })
            .unwrap();
        registry
            .register(FakeOperation {
                uses: "test.slow",
                behavior: FakeBehavior::WaitForStop {
                    cleanup: Duration::from_millis(25),
                },
                tracker: Arc::clone(&slow),
            })
            .unwrap();
        let never = register(
            &mut registry,
            "test.never",
            FakeBehavior::Return("should-not-run"),
        );
        let scheduler =
            ScopeScheduler::for_test(Arc::new(all_parallel_workflow()), registry, config());
        let (_, stop) = stop_pair();

        let result = scheduler
            .run(metadata(), json!("question"), stop)
            .await
            .unwrap();

        let RunExecutionResult::Failed(error) = result else {
            panic!("expected primary operation failure")
        };
        assert_eq!(error.code(), "FAKE_OPERATION_FAILED");
        assert_eq!(fail.started.load(Ordering::SeqCst), 1);
        assert_eq!(slow.started.load(Ordering::SeqCst), 1);
        assert_eq!(slow.cleaned.load(Ordering::SeqCst), 1);
        assert_eq!(never.started.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn branch_panic_becomes_infrastructure_after_peer_drain() {
        let mut registry = OperationRegistry::default();
        let slow = Arc::new(Tracker::default());
        let panicking = Arc::new(Tracker::default());
        registry
            .register(FakeOperation {
                uses: "test.fail",
                behavior: FakeBehavior::PanicAfterStarted {
                    other: Arc::clone(&slow),
                },
                tracker: Arc::clone(&panicking),
            })
            .unwrap();
        registry
            .register(FakeOperation {
                uses: "test.slow",
                behavior: FakeBehavior::WaitForStop {
                    cleanup: Duration::from_millis(10),
                },
                tracker: Arc::clone(&slow),
            })
            .unwrap();
        let never = register(
            &mut registry,
            "test.never",
            FakeBehavior::Return("should-not-run"),
        );
        let scheduler =
            ScopeScheduler::for_test(Arc::new(all_parallel_workflow()), registry, config());
        let (_, stop) = stop_pair();

        let error = scheduler
            .run(metadata(), json!("question"), stop)
            .await
            .unwrap_err();

        assert_eq!(error.kind(), RunErrorKind::Infrastructure);
        assert_eq!(error.code(), SCOPE_TASK_PANICKED);
        assert_eq!(panicking.started.load(Ordering::SeqCst), 1);
        assert_eq!(slow.cleaned.load(Ordering::SeqCst), 1);
        assert_eq!(never.started.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn external_stop_cleanup_panic_becomes_infrastructure_before_permit_release() {
        let output = ValueId::output("/workflow/external_cleanup_panic").unwrap();
        let ir = Arc::new(workflow(
            call_operations("/workflow/external_cleanup_panic", "test.cleanup_panic"),
            output,
            ValueType::String,
            json!({"type": "string"}),
            json!({"type": "string"}),
        ));
        let mut registry = OperationRegistry::default();
        let tracker = register(
            &mut registry,
            "test.cleanup_panic",
            FakeBehavior::PanicAfterStop {
                delay: Duration::ZERO,
            },
        );
        let global = Arc::new(Semaphore::new(1));
        let scheduler =
            ScopeScheduler::for_test_with_global(ir, registry, Arc::clone(&global), config());
        let (controller, stop) = stop_pair();
        let request_stop = async {
            tracker.wait_started().await;
            assert_eq!(global.available_permits(), 0);
            assert!(controller.request(StopReason::Cancelled));
        };

        let (result, ()) = tokio::join!(
            scheduler.run(metadata(), json!("question"), stop),
            request_stop
        );

        let error = result.unwrap_err();
        assert_eq!(error.kind(), RunErrorKind::Infrastructure);
        assert_eq!(error.code(), SCOPE_TASK_PANICKED);
        assert_eq!(global.available_permits(), 1);
    }

    #[tokio::test]
    async fn external_stop_cleanup_infrastructure_failure_is_preserved() {
        let output = ValueId::output("/workflow/external_cleanup_infrastructure").unwrap();
        let ir = workflow(
            call_operations(
                "/workflow/external_cleanup_infrastructure",
                "test.cleanup_infrastructure",
            ),
            output,
            ValueType::String,
            json!({"type": "string"}),
            json!({"type": "string"}),
        );
        let mut registry = OperationRegistry::default();
        let tracker = register(
            &mut registry,
            "test.cleanup_infrastructure",
            FakeBehavior::InfrastructureAfterStop,
        );
        let scheduler = ScopeScheduler::for_test(Arc::new(ir), registry, config());
        let (controller, stop) = stop_pair();
        let request_stop = async {
            tracker.wait_started().await;
            assert!(controller.request(StopReason::Cancelled));
        };

        let (result, ()) = tokio::join!(
            scheduler.run(metadata(), json!("question"), stop),
            request_stop
        );

        let error = result.unwrap_err();
        assert_eq!(error.kind(), RunErrorKind::Infrastructure);
        assert_eq!(error.code(), "FAKE_CLEANUP_INFRASTRUCTURE");
    }

    #[tokio::test]
    async fn peer_cancel_cleanup_panic_overrides_settleable_branch_failure() {
        let mut registry = OperationRegistry::default();
        let panicking = Arc::new(Tracker::default());
        registry
            .register(FakeOperation {
                uses: "test.fail",
                behavior: FakeBehavior::FailAfterStarted {
                    other: Arc::clone(&panicking),
                },
                tracker: Arc::new(Tracker::default()),
            })
            .unwrap();
        registry
            .register(FakeOperation {
                uses: "test.slow",
                behavior: FakeBehavior::PanicAfterStop {
                    delay: Duration::ZERO,
                },
                tracker: Arc::clone(&panicking),
            })
            .unwrap();
        let never = register(
            &mut registry,
            "test.never",
            FakeBehavior::Return("should-not-run"),
        );
        let scheduler =
            ScopeScheduler::for_test(Arc::new(all_parallel_workflow()), registry, config());
        let (_, stop) = stop_pair();

        let error = scheduler
            .run(metadata(), json!("question"), stop)
            .await
            .unwrap_err();

        assert_eq!(error.kind(), RunErrorKind::Infrastructure);
        assert_eq!(error.code(), SCOPE_TASK_PANICKED);
        assert_eq!(panicking.started.load(Ordering::SeqCst), 1);
        assert_eq!(never.started.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn operation_timeout_cleanup_panic_becomes_infrastructure() {
        let output = ValueId::output("/workflow/timeout_cleanup_panic").unwrap();
        let ir = workflow(
            call_operations("/workflow/timeout_cleanup_panic", "test.cleanup_panic"),
            output,
            ValueType::String,
            json!({"type": "string"}),
            json!({"type": "string"}),
        );
        let mut registry = OperationRegistry::default();
        register(
            &mut registry,
            "test.cleanup_panic",
            FakeBehavior::PanicAfterStop {
                delay: Duration::ZERO,
            },
        );
        let mut scheduler_config = config();
        scheduler_config.operation_timeout = Duration::from_millis(10);
        let scheduler = ScopeScheduler::for_test(Arc::new(ir), registry, scheduler_config);
        let (_, stop) = stop_pair();

        let error = scheduler
            .run(metadata(), json!("question"), stop)
            .await
            .unwrap_err();

        assert_eq!(error.kind(), RunErrorKind::Infrastructure);
        assert_eq!(error.code(), SCOPE_TASK_PANICKED);
    }

    #[tokio::test]
    async fn cleanup_panic_after_grace_is_dropped_and_preserves_timeout() {
        let output = ValueId::output("/workflow/late_cleanup_panic").unwrap();
        let ir = workflow(
            call_operations("/workflow/late_cleanup_panic", "test.late_cleanup_panic"),
            output,
            ValueType::String,
            json!({"type": "string"}),
            json!({"type": "string"}),
        );
        let mut registry = OperationRegistry::default();
        register(
            &mut registry,
            "test.late_cleanup_panic",
            FakeBehavior::PanicAfterStop {
                delay: Duration::from_secs(1),
            },
        );
        let mut scheduler_config = config();
        scheduler_config.operation_timeout = Duration::from_millis(10);
        scheduler_config.operation_cancel_grace_period = Duration::from_millis(10);
        let scheduler = ScopeScheduler::for_test(Arc::new(ir), registry, scheduler_config);
        let (_, stop) = stop_pair();

        let result = scheduler
            .run(metadata(), json!("question"), stop)
            .await
            .unwrap();

        let RunExecutionResult::Failed(error) = result else {
            panic!("grace expiry must preserve the original operation timeout")
        };
        assert_eq!(error.kind(), RunErrorKind::Timeout);
        assert_eq!(error.code(), "OPERATION_TIMEOUT");
    }

    fn all_settled_workflow() -> WorkflowIr {
        let branch = |name: &str, uses: &'static str| {
            call_branch(
                &format!("/workflow/settled/branches/{name}"),
                "input",
                input_id(),
                uses,
                RegionKind::ParallelBranch {
                    name: identifier(name),
                },
            )
        };
        let raise_path = "/workflow/settled/branches/b_raise";
        let raising_branch = Region {
            id: region_id(raise_path),
            kind: RegionKind::ParallelBranch {
                name: identifier("b_raise"),
            },
            parameters: vec![capture_parameter(raise_path, "input", input_id())],
            operations: vec![],
            result: TypedContract {
                schema: json!({"type": "string"}),
                value_type: ValueType::String,
            },
            terminator: Some(Terminator::Raise {
                error: identifier("declared_failure"),
            }),
        };
        let branch_type = settled_type(ValueType::String);
        let output_type = object_type([
            ("a_ok", branch_type.clone()),
            ("b_raise", branch_type.clone()),
            ("c_operation", branch_type.clone()),
            ("d_timeout", branch_type),
        ]);
        let output = ValueId::output("/workflow/settled").unwrap();
        workflow(
            vec![IrOperation {
                id: OperationId::authored("/workflow/settled").unwrap(),
                output: data(output.clone(), output_type.clone()),
                kind: OperationKind::Parallel(Parallel {
                    inputs: BTreeMap::from([(identifier("input"), input_id())]),
                    settle: ParallelSettle::AllSettled,
                    max_concurrency: Some(4),
                    branches: BTreeMap::from([
                        (identifier("a_ok"), branch("a_ok", "test.ok")),
                        (identifier("b_raise"), raising_branch),
                        (
                            identifier("c_operation"),
                            branch("c_operation", "test.operation"),
                        ),
                        (identifier("d_timeout"), branch("d_timeout", "test.timeout")),
                    ]),
                }),
            }],
            output,
            output_type.clone(),
            json!({"type": "string"}),
            schema_for_type(&output_type),
        )
    }

    #[tokio::test]
    async fn all_settled_returns_exact_safe_success_authored_operation_and_timeout_shape() {
        let mut registry = OperationRegistry::default();
        register(&mut registry, "test.ok", FakeBehavior::Return("ok"));
        register(&mut registry, "test.operation", FakeBehavior::Fail);
        let timed_out = register(
            &mut registry,
            "test.timeout",
            FakeBehavior::WaitForStop {
                cleanup: Duration::from_millis(25),
            },
        );
        let mut scheduler_config = config();
        scheduler_config.operation_timeout = Duration::from_millis(20);
        let scheduler =
            ScopeScheduler::for_test(Arc::new(all_settled_workflow()), registry, scheduler_config);
        let (_, stop) = stop_pair();

        let result = scheduler
            .run(metadata(), json!("question"), stop)
            .await
            .unwrap();

        let RunExecutionResult::Ended(TerminalOutcome::Success { output }) = result else {
            panic!("expected successful all_settled envelope")
        };
        assert_eq!(
            output.data,
            json!({
                "a_ok": {"status": "ok", "value": "ok"},
                "b_raise": {
                    "status": "error",
                    "error": {
                        "category": "workflow",
                        "code": "DECLARED_FAILURE",
                        "retryable": false,
                        "origin": "/workflow/settled/branches/b_raise"
                    }
                },
                "c_operation": {
                    "status": "error",
                    "error": {
                        "category": "operation",
                        "code": "FAKE_OPERATION_FAILED",
                        "retryable": false,
                        "origin": "/workflow/settled/branches/c_operation/call#Authored"
                    }
                },
                "d_timeout": {
                    "status": "error",
                    "error": {
                        "category": "timeout",
                        "code": "OPERATION_TIMEOUT",
                        "retryable": false,
                        "origin": "/workflow/settled/branches/d_timeout/call#Authored"
                    }
                }
            })
        );
        assert!(!output.data.to_string().contains("message"));
        assert!(!output.data.to_string().contains("diagnostic"));
        assert_eq!(
            timed_out.cleaned.load(Ordering::SeqCst),
            1,
            "operation timeout must allow bounded non-zero cooperative cleanup"
        );
    }

    #[tokio::test]
    async fn invalid_operation_error_code_is_not_collected_as_safe_branch_data() {
        let mut registry = OperationRegistry::default();
        register(&mut registry, "test.ok", FakeBehavior::Return("ok"));
        register(
            &mut registry,
            "test.operation",
            FakeBehavior::FailWithCode("invalid-private-code"),
        );
        register(
            &mut registry,
            "test.timeout",
            FakeBehavior::WaitForStop {
                cleanup: Duration::ZERO,
            },
        );
        let scheduler =
            ScopeScheduler::for_test(Arc::new(all_settled_workflow()), registry, config());
        let (_, stop) = stop_pair();

        let error = scheduler
            .run(metadata(), json!("question"), stop)
            .await
            .expect_err("invalid operation codes must become infrastructure failures");
        assert_eq!(error.code(), INFRASTRUCTURE_FAILURE);
        assert_eq!(error.kind(), RunErrorKind::Infrastructure);
        assert_eq!(
            error.message(),
            "operation returned an invalid stable error code"
        );
    }

    #[tokio::test]
    async fn run_deadline_caps_attempt_remaining_and_still_drains_cleanup_after_expiry() {
        let output = ValueId::output("/workflow/deadline").unwrap();
        let ir = workflow(
            call_operations("/workflow/deadline", "test.deadline"),
            output,
            ValueType::String,
            json!({"type": "string"}),
            json!({"type": "string"}),
        );
        let mut registry = OperationRegistry::default();
        let tracker = register(
            &mut registry,
            "test.deadline",
            FakeBehavior::WaitForStop {
                cleanup: Duration::from_millis(25),
            },
        );
        let mut scheduler_config = config();
        scheduler_config.operation_timeout = Duration::from_secs(2);
        let scheduler = ScopeScheduler::for_test(Arc::new(ir), registry, scheduler_config);
        let (_, stop) = stop_pair();
        let started = Instant::now();
        let mut metadata = metadata();
        metadata.execution_deadline = started + Duration::from_millis(80);

        let result = scheduler
            .run(metadata, json!("question"), stop)
            .await
            .unwrap();

        let RunExecutionResult::Stopped(error) = result else {
            panic!("run deadline must produce a stopped timeout result")
        };
        assert_eq!(error.code(), "RUN_TIMEOUT");
        assert_eq!(error.stop_reason(), Some(StopReason::TimedOut));
        assert_eq!(tracker.cleaned.load(Ordering::SeqCst), 1);
        let remaining = tracker.remaining_millis.load(Ordering::SeqCst);
        assert!(
            (1..=80).contains(&remaining),
            "attempt remaining must be capped by the absolute Run deadline"
        );
        assert!(
            started.elapsed() >= Duration::from_millis(100),
            "Run deadline cleanup must be allowed to finish after execution expiry"
        );
    }

    fn externally_stopped_all_settled_workflow() -> WorkflowIr {
        let branch = |name: &str, uses: &'static str| {
            call_branch(
                &format!("/workflow/stopped/branches/{name}"),
                "input",
                input_id(),
                uses,
                RegionKind::ParallelBranch {
                    name: identifier(name),
                },
            )
        };
        let branch_type = settled_type(ValueType::String);
        let output_type = object_type([("left", branch_type.clone()), ("right", branch_type)]);
        let output = ValueId::output("/workflow/stopped").unwrap();
        workflow(
            vec![IrOperation {
                id: OperationId::authored("/workflow/stopped").unwrap(),
                output: data(output.clone(), output_type.clone()),
                kind: OperationKind::Parallel(Parallel {
                    inputs: BTreeMap::from([(identifier("input"), input_id())]),
                    settle: ParallelSettle::AllSettled,
                    max_concurrency: Some(2),
                    branches: BTreeMap::from([
                        (identifier("left"), branch("left", "test.wait_left")),
                        (identifier("right"), branch("right", "test.wait_right")),
                    ]),
                }),
            }],
            output,
            output_type.clone(),
            json!({"type": "string"}),
            schema_for_type(&output_type),
        )
    }

    #[tokio::test]
    async fn external_stop_propagates_through_all_settled_and_drains_children() {
        let mut registry = OperationRegistry::default();
        let left = register(
            &mut registry,
            "test.wait_left",
            FakeBehavior::WaitForStop {
                cleanup: Duration::from_millis(10),
            },
        );
        let right = register(
            &mut registry,
            "test.wait_right",
            FakeBehavior::WaitForStop {
                cleanup: Duration::from_millis(10),
            },
        );
        let scheduler = ScopeScheduler::for_test(
            Arc::new(externally_stopped_all_settled_workflow()),
            registry,
            config(),
        );
        let (controller, stop) = stop_pair();
        let wait_for_children = async {
            left.wait_started().await;
            right.wait_started().await;
            assert!(controller.request(StopReason::Cancelled));
        };

        let (result, ()) = tokio::join!(
            scheduler.run(metadata(), json!("question"), stop),
            wait_for_children
        );

        let RunExecutionResult::Stopped(error) = result.unwrap() else {
            panic!("expected external stop to escape all_settled")
        };
        assert_eq!(error.kind(), RunErrorKind::Stop);
        assert_eq!(error.code(), "RUN_CANCELLED");
        assert_eq!(error.stop_reason(), Some(StopReason::Cancelled));
        assert_eq!(left.cleaned.load(Ordering::SeqCst), 1);
        assert_eq!(right.cleaned.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn process_operation_capacity_is_shared_across_runs() {
        let output = ValueId::output("/workflow/wait").unwrap();
        let ir = Arc::new(workflow(
            call_operations("/workflow/wait", "test.wait"),
            output,
            ValueType::String,
            json!({"type": "string"}),
            json!({"type": "string"}),
        ));
        let mut registry = OperationRegistry::default();
        let tracker = register(
            &mut registry,
            "test.wait",
            FakeBehavior::WaitForStop {
                cleanup: Duration::ZERO,
            },
        );
        let global = Arc::new(Semaphore::new(1));
        let scheduler_a = ScopeScheduler::for_test_with_global(
            Arc::clone(&ir),
            registry.clone(),
            Arc::clone(&global),
            config(),
        );
        let scheduler_b =
            ScopeScheduler::for_test_with_global(ir, registry, Arc::clone(&global), config());
        let (controller_a, stop_a) = stop_pair();
        let (controller_b, stop_b) = stop_pair();
        let mut metadata_a = metadata();
        metadata_a.run_id = "run-a".to_string();
        let mut metadata_b = metadata();
        metadata_b.run_id = "run-b".to_string();

        let run_a =
            tokio::spawn(
                async move { scheduler_a.run(metadata_a, json!("question"), stop_a).await },
            );
        tracker.wait_started_count(1).await;
        let run_b =
            tokio::spawn(
                async move { scheduler_b.run(metadata_b, json!("question"), stop_b).await },
            );
        tokio::task::yield_now().await;
        assert_eq!(tracker.started.load(Ordering::SeqCst), 1);
        assert_eq!(global.available_permits(), 0);

        assert!(controller_a.request(StopReason::Cancelled));
        tracker.wait_started_count(2).await;
        assert_eq!(tracker.started.load(Ordering::SeqCst), 2);
        assert_eq!(global.available_permits(), 0);
        assert!(controller_b.request(StopReason::Cancelled));

        let (result_a, result_b) =
            tokio::time::timeout(Duration::from_secs(1), async { tokio::join!(run_a, run_b) })
                .await
                .expect("both runs must stop and drain");

        assert!(matches!(
            result_a.unwrap().unwrap(),
            RunExecutionResult::Stopped(_)
        ));
        assert!(matches!(
            result_b.unwrap().unwrap(),
            RunExecutionResult::Stopped(_)
        ));
        assert_eq!(tracker.cleaned.load(Ordering::SeqCst), 2);
        assert_eq!(global.available_permits(), 1);
    }

    fn validation_workflow() -> WorkflowIr {
        let path = "/workflow/result";
        let result = ValueId::expression(path, 0).unwrap();
        workflow(
            vec![IrOperation {
                id: OperationId::expression(path, 0).unwrap(),
                output: data(result.clone(), ValueType::String),
                kind: OperationKind::Const {
                    value: json!("bad-output"),
                },
            }],
            result,
            ValueType::String,
            json!({"type": "string", "minLength": 1}),
            json!({"type": "string", "pattern": "^ok$"}),
        )
    }

    #[tokio::test]
    async fn draft_2020_input_and_output_contracts_are_enforced() {
        let scheduler = ScopeScheduler::for_test(
            Arc::new(validation_workflow()),
            OperationRegistry::default(),
            config(),
        );
        let (_, invalid_input_stop) = stop_pair();
        let invalid_input = scheduler
            .run(metadata(), json!(""), invalid_input_stop)
            .await
            .unwrap();
        let RunExecutionResult::Failed(error) = invalid_input else {
            panic!("expected input validation failure")
        };
        assert_eq!(error.code(), INPUT_INVALID);

        let (_, invalid_output_stop) = stop_pair();
        let invalid_output = scheduler
            .run(metadata(), json!("question"), invalid_output_stop)
            .await
            .unwrap();
        let RunExecutionResult::Failed(error) = invalid_output else {
            panic!("expected output validation failure")
        };
        assert_eq!(error.code(), OUTPUT_INVALID);
    }
}
