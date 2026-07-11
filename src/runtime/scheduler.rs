use std::{
    collections::{BTreeMap, VecDeque},
    sync::Arc,
};

use tokio::task::JoinSet;

use crate::{
    dsl::compiled::{CompiledAgent, NodeTransition, RunOutput},
    events::hub::EventHub,
    nodes::registry::NodeExecutorRegistry,
};

use super::{
    execute_node, BranchState, ExecutionLimiter, NodeExecutionFailure, NodeExecutionResult,
    NodeState, RunContext, RunError, StopSignal,
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
    _branch_states: BTreeMap<WorkScope, BranchState>,
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
            match result.outcome.transition {
                NodeTransition::Next => {
                    state.activate(&self.agent, node.next.as_deref(), scope, context)?
                }
                NodeTransition::Goto(target) => {
                    state.activate(&self.agent, Some(&target), scope, context)?
                }
                NodeTransition::ActivateFork => {
                    if !self.agent.execution_plan.forks.contains_key(&node.id) {
                        return Err(invariant(format!(
                            "node '{}' requested fork activation without a compiled fork plan",
                            node.id
                        )));
                    }
                    return Err(RunError::infrastructure(
                        "SCHEDULER_FORK_UNSUPPORTED",
                        format!(
                            "fork activation for node '{}' is not available yet",
                            node.id
                        ),
                    ));
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
            _branch_states: branch_states,
        }
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
