use std::{
    collections::{BTreeMap, VecDeque},
    sync::Arc,
};

use tokio::task::JoinSet;

use crate::{
    dsl::compiled::{CompiledAgent, ForkPlan, NodeTransition, RunOutput},
    events::hub::EventHub,
    events::protocol::{RunEventScope, RunEventType},
    nodes::registry::NodeExecutorRegistry,
};
use serde_json::json;

use super::{
    execute_node, BranchError, BranchResult, BranchState, ExecutionLimiter, NodeExecutionFailure,
    NodeExecutionResult, NodeState, RunContext, RunError, StopSignal,
};

#[derive(Debug, Clone, PartialEq)]
pub enum SchedulerResult {
    Completed(RunOutput),
    Failed(RunError),
    Stopped(RunError),
}

pub struct Scheduler {
    agent: Arc<CompiledAgent>,
    executors: NodeExecutorRegistry,
    events: EventHub,
    limiter: ExecutionLimiter,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum WorkScope {
    Main,
    Branch { fork_id: String, branch_id: String },
}

#[derive(Debug)]
struct ReadyNode {
    node_id: String,
    scope: WorkScope,
    context: RunContext,
}

struct SchedulerState {
    ready: VecDeque<ReadyNode>,
    node_states: BTreeMap<String, NodeState>,
    branch_states: BTreeMap<WorkScope, BranchState>,
    active_fork: Option<ActiveFork>,
}

struct ActiveFork {
    plan: ForkPlan,
    main_context: RunContext,
    results: BTreeMap<String, BranchResult>,
}

impl Scheduler {
    pub fn new(
        agent: Arc<CompiledAgent>,
        executors: NodeExecutorRegistry,
        events: EventHub,
        limiter: ExecutionLimiter,
    ) -> Self {
        Self {
            agent,
            executors,
            events,
            limiter,
        }
    }

    pub async fn run(
        &self,
        context: RunContext,
        stop: StopSignal,
    ) -> Result<SchedulerResult, RunError> {
        let mut state = SchedulerState::new(&self.agent);
        state.activate(
            &self.agent,
            Some(&self.agent.entry),
            WorkScope::Main,
            context,
        )?;
        let mut executions = JoinSet::new();

        loop {
            if let Some(reason) = stop.reason() {
                return Ok(SchedulerResult::Stopped(RunError::stopped(reason)));
            }

            while let Some(ready) = state.ready.pop_front() {
                state.start(&ready.node_id)?;
                let node = self
                    .agent
                    .nodes
                    .get(&ready.node_id)
                    .cloned()
                    .ok_or_else(|| {
                        invariant(format!(
                            "ready node '{}' does not exist in the compiled agent",
                            ready.node_id
                        ))
                    })?;
                let executors = self.executors.clone();
                let events = self.events.clone();
                let task_stop = stop.clone();
                let limiter = self.limiter.clone();
                executions.spawn(async move {
                    let result =
                        execute_node(node, ready.context, executors, events, task_stop, limiter)
                            .await;
                    (ready.node_id, ready.scope, result)
                });
            }

            let Some(joined) = executions.join_next().await else {
                return Err(invariant(
                    "scheduler became idle before reaching a terminal transition",
                ));
            };
            let (scheduled_node_id, scope, execution) = joined.map_err(|error| {
                RunError::infrastructure(
                    "SCHEDULER_TASK_FAILED",
                    format!("node execution task failed: {error}"),
                )
            })?;
            let result = match execution {
                Ok(result) => result,
                Err(NodeExecutionFailure::Node { node_id, error }) => {
                    state.fail(&scheduled_node_id, &node_id)?;
                    if let WorkScope::Branch { fork_id, branch_id } = &scope {
                        let result = BranchResult::Failed {
                            terminal_node_id: node_id.clone(),
                            error: BranchError {
                                code: error.code().to_string(),
                                message: error.message().to_string(),
                            },
                        };
                        state.settle_branch(&scope, result)?;
                        self.events
                            .publish_error(
                                branch_scope(&state, fork_id)?,
                                RunEventType::BranchFailed,
                                error.code(),
                                error.message(),
                                json!({
                                    "fork_id": fork_id,
                                    "branch_id": branch_id,
                                    "terminal_node_id": node_id,
                                    "error": {
                                        "code": error.code(),
                                        "message": error.message(),
                                    }
                                }),
                            )
                            .await
                            .map_err(event_error)?;
                        state.activate_join_if_settled(&self.agent)?;
                        continue;
                    }
                    return Ok(SchedulerResult::Failed(error));
                }
                Err(NodeExecutionFailure::Stop { node_id, error }) => {
                    state.fail(&scheduled_node_id, &node_id)?;
                    return Ok(SchedulerResult::Stopped(error));
                }
                Err(NodeExecutionFailure::Infrastructure(error)) => {
                    state.transition(&scheduled_node_id, NodeState::Running, NodeState::Failed)?;
                    return Err(error);
                }
            };
            state.succeed(&scheduled_node_id, &result)?;

            let node = self
                .agent
                .nodes
                .get(&scheduled_node_id)
                .expect("scheduled nodes are checked before execution");
            let mut context = result.context;
            context.set_node_output(&result.node_id, result.outcome.output.clone());
            if let WorkScope::Branch { fork_id, branch_id } = &scope {
                let target = match &result.outcome.transition {
                    NodeTransition::Next => node.next.as_deref().ok_or_else(|| {
                        invariant(format!(
                            "successful branch node '{}' did not identify a successor",
                            node.id
                        ))
                    })?,
                    NodeTransition::Goto(target) => target,
                    NodeTransition::ActivateFork => {
                        return Err(invariant(format!(
                            "branch node '{}' requested nested fork activation",
                            node.id
                        )));
                    }
                    NodeTransition::Complete(_) => {
                        return Err(invariant(format!(
                            "branch node '{}' completed the run before its paired join",
                            node.id
                        )));
                    }
                };
                if state.is_paired_join(fork_id, target)? {
                    let branch_result = BranchResult::Succeeded {
                        terminal_node_id: result.node_id.clone(),
                        output: result.outcome.output.clone(),
                    };
                    state.settle_branch(&scope, branch_result)?;
                    self.events
                        .publish(
                            run_scope(&context),
                            RunEventType::BranchCompleted,
                            json!({
                                "fork_id": fork_id,
                                "branch_id": branch_id,
                                "terminal_node_id": result.node_id,
                            }),
                        )
                        .await
                        .map_err(event_error)?;
                    state.activate_join_if_settled(&self.agent)?;
                } else {
                    state.validate_branch_target(fork_id, branch_id, target)?;
                    state.activate(&self.agent, Some(target), scope, context)?;
                }
                continue;
            }

            state.finish_join_if_completed(&scheduled_node_id);
            match result.outcome.transition {
                NodeTransition::Next => {
                    state.activate(&self.agent, node.next.as_deref(), scope, context)?
                }
                NodeTransition::Goto(target) => {
                    state.activate(&self.agent, Some(&target), scope, context)?
                }
                NodeTransition::ActivateFork => {
                    let plan = self
                        .agent
                        .execution_plan
                        .forks
                        .get(&node.id)
                        .cloned()
                        .ok_or_else(|| {
                            invariant(format!(
                                "node '{}' requested fork activation without a compiled fork plan",
                                node.id
                            ))
                        })?;
                    state.begin_fork(plan.clone(), context.clone())?;
                    for (branch_id, branch) in &plan.branches {
                        let branch_scope = WorkScope::Branch {
                            fork_id: plan.fork_id.clone(),
                            branch_id: branch_id.clone(),
                        };
                        state.start_branch(&branch_scope)?;
                        self.events
                            .publish(
                                run_scope(&context),
                                RunEventType::BranchStarted,
                                json!({
                                    "fork_id": plan.fork_id,
                                    "branch_id": branch_id,
                                }),
                            )
                            .await
                            .map_err(event_error)?;
                        state.activate(
                            &self.agent,
                            Some(&branch.entry),
                            branch_scope,
                            context.fork_branch(),
                        )?;
                    }
                }
                NodeTransition::Complete(output) => {
                    return Ok(SchedulerResult::Completed(output));
                }
            }
        }
    }
}

impl SchedulerState {
    fn new(agent: &CompiledAgent) -> Self {
        let node_states = agent
            .nodes
            .keys()
            .cloned()
            .map(|node_id| (node_id, NodeState::Pending))
            .collect();
        let branch_states = agent
            .execution_plan
            .forks
            .values()
            .flat_map(|fork| {
                fork.branches.keys().map(|branch_id| {
                    (
                        WorkScope::Branch {
                            fork_id: fork.fork_id.clone(),
                            branch_id: branch_id.clone(),
                        },
                        BranchState::Pending,
                    )
                })
            })
            .collect();
        Self {
            ready: VecDeque::new(),
            node_states,
            branch_states,
            active_fork: None,
        }
    }

    fn begin_fork(&mut self, plan: ForkPlan, main_context: RunContext) -> Result<(), RunError> {
        if self.active_fork.is_some() {
            return Err(invariant(format!(
                "fork '{}' cannot activate while another fork is active",
                plan.fork_id
            )));
        }
        self.active_fork = Some(ActiveFork {
            plan,
            main_context,
            results: BTreeMap::new(),
        });
        Ok(())
    }

    fn start_branch(&mut self, scope: &WorkScope) -> Result<(), RunError> {
        self.transition_branch(scope, BranchState::Pending, BranchState::Running)
    }

    fn settle_branch(&mut self, scope: &WorkScope, result: BranchResult) -> Result<(), RunError> {
        let WorkScope::Branch { fork_id, branch_id } = scope else {
            return Err(invariant("only a branch scope can settle a branch"));
        };
        let next = match result {
            BranchResult::Succeeded { .. } => BranchState::Succeeded,
            BranchResult::Failed { .. } => BranchState::Failed,
        };
        let active = self.active_fork(fork_id)?;
        if active.results.contains_key(branch_id) {
            return Err(invariant(format!(
                "branch '{fork_id}.{branch_id}' cannot settle more than once"
            )));
        }
        self.transition_branch(scope, BranchState::Running, next)?;
        self.active_fork
            .as_mut()
            .expect("the active fork was validated before branch settlement")
            .results
            .insert(branch_id.clone(), result);
        Ok(())
    }

    fn is_paired_join(&self, fork_id: &str, target: &str) -> Result<bool, RunError> {
        let active = self.active_fork(fork_id)?;
        Ok(active.plan.join_id == target)
    }

    fn validate_branch_target(
        &self,
        fork_id: &str,
        branch_id: &str,
        target: &str,
    ) -> Result<(), RunError> {
        let active = self.active_fork(fork_id)?;
        let branch = active.plan.branches.get(branch_id).ok_or_else(|| {
            invariant(format!(
                "active fork '{fork_id}' has no branch '{branch_id}'"
            ))
        })?;
        if !branch.nodes.contains(target) {
            return Err(invariant(format!(
                "branch '{fork_id}.{branch_id}' cannot activate node '{target}' outside its region"
            )));
        }
        Ok(())
    }

    fn activate_join_if_settled(&mut self, agent: &CompiledAgent) -> Result<(), RunError> {
        let Some(active) = &self.active_fork else {
            return Err(invariant("cannot activate a join without an active fork"));
        };
        if active.results.len() != active.plan.branches.len() {
            return Ok(());
        }
        let join_id = active.plan.join_id.clone();
        let context = active
            .main_context
            .with_join_results(active.results.clone());
        self.activate(agent, Some(&join_id), WorkScope::Main, context)
    }

    fn finish_join_if_completed(&mut self, node_id: &str) {
        let is_active_join = self
            .active_fork
            .as_ref()
            .is_some_and(|active| active.plan.join_id == node_id);
        if is_active_join {
            self.active_fork = None;
        }
    }

    fn active_fork(&self, fork_id: &str) -> Result<&ActiveFork, RunError> {
        self.active_fork
            .as_ref()
            .filter(|active| active.plan.fork_id == fork_id)
            .ok_or_else(|| invariant(format!("fork '{fork_id}' is not active")))
    }

    fn transition_branch(
        &mut self,
        scope: &WorkScope,
        expected: BranchState,
        next: BranchState,
    ) -> Result<(), RunError> {
        let state = self
            .branch_states
            .get_mut(scope)
            .ok_or_else(|| invariant(format!("branch scope '{scope:?}' has no scheduler state")))?;
        if *state != expected {
            return Err(invariant(format!(
                "branch scope '{scope:?}' cannot transition from {state:?} to {next:?}"
            )));
        }
        *state = next;
        Ok(())
    }

    fn activate(
        &mut self,
        agent: &CompiledAgent,
        node_id: Option<&str>,
        scope: WorkScope,
        context: RunContext,
    ) -> Result<(), RunError> {
        let node_id = node_id.ok_or_else(|| {
            invariant("a successful non-terminal node did not identify a next node")
        })?;
        if !agent.nodes.contains_key(node_id) {
            return Err(invariant(format!(
                "activation target '{node_id}' does not exist in the compiled agent"
            )));
        }
        let state = self.node_states.get_mut(node_id).ok_or_else(|| {
            invariant(format!(
                "activation target '{node_id}' has no scheduler state"
            ))
        })?;
        if *state != NodeState::Pending {
            return Err(invariant(format!(
                "node '{node_id}' cannot be activated from state {state:?}"
            )));
        }
        *state = NodeState::Ready;
        self.ready.push_back(ReadyNode {
            node_id: node_id.to_string(),
            scope,
            context,
        });
        Ok(())
    }

    fn start(&mut self, node_id: &str) -> Result<(), RunError> {
        self.transition(node_id, NodeState::Ready, NodeState::Running)
    }

    fn succeed(
        &mut self,
        scheduled_node_id: &str,
        result: &NodeExecutionResult,
    ) -> Result<(), RunError> {
        if scheduled_node_id != result.node_id {
            return Err(invariant(format!(
                "node task for '{scheduled_node_id}' returned result for '{}'",
                result.node_id
            )));
        }
        self.transition(scheduled_node_id, NodeState::Running, NodeState::Succeeded)
    }

    fn fail(&mut self, scheduled_node_id: &str, failed_node_id: &str) -> Result<(), RunError> {
        if scheduled_node_id != failed_node_id {
            return Err(invariant(format!(
                "node task for '{scheduled_node_id}' returned failure for '{failed_node_id}'"
            )));
        }
        self.transition(scheduled_node_id, NodeState::Running, NodeState::Failed)
    }

    fn transition(
        &mut self,
        node_id: &str,
        expected: NodeState,
        next: NodeState,
    ) -> Result<(), RunError> {
        let state = self
            .node_states
            .get_mut(node_id)
            .ok_or_else(|| invariant(format!("node '{node_id}' has no scheduler state")))?;
        if *state != expected {
            return Err(invariant(format!(
                "node '{node_id}' cannot transition from {state:?} to {next:?}"
            )));
        }
        *state = next;
        Ok(())
    }
}

fn invariant(message: impl Into<String>) -> RunError {
    RunError::infrastructure("SCHEDULER_INVARIANT_VIOLATION", message)
}

fn run_scope(context: &RunContext) -> RunEventScope {
    let metadata = context.metadata();
    RunEventScope::for_run(
        &metadata.request_id,
        &metadata.run_id,
        &metadata.agent_id,
        &metadata.agent_version,
    )
}

fn branch_scope(state: &SchedulerState, fork_id: &str) -> Result<RunEventScope, RunError> {
    Ok(run_scope(&state.active_fork(fork_id)?.main_context))
}

fn event_error(error: crate::events::hub::EventError) -> RunError {
    RunError::infrastructure(error.code(), error.to_string())
}
