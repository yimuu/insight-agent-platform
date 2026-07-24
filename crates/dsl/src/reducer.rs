//! Structural Canonical Plan -> structured-author reducer.
//!
//! This is intentionally a partial inverse of `compiler`: it recognizes only
//! properly nested compiler-shaped regions. The public conversion boundary
//! recompiles the emitted document and compares semantic hashes, so accepting
//! a graph here can never silently discard an edge, port, scope, or binding.

use std::collections::{BTreeMap, BTreeSet};

use serde_json::{json, Map, Value};

use insight_engine::{
    plan::{
        expression::{MatchProgram, MatchValue, TemplatePart, ValueProgram},
        BranchCaseId, CollectSource, ControlPortId, DataPortId, DescriptorValue,
        ExpressionLanguage, ForkDescriptor, LeafTaskDescriptor, LoopFlavor, Node, NodeKind, Plan,
        PlanIndex, PlanJoinMode, PlanType, PortDirection, PureExpression, ScopeId, ScopeKind,
        ValueSource,
    },
    NodeId,
};

type ReductionResult<T> = Result<T, String>;

pub(super) fn reduce_plan(plan: &Plan) -> ReductionResult<String> {
    Reducer::new(plan)?.reduce()
}

#[derive(Debug, Clone)]
struct Cursor {
    node_id: NodeId,
    via_input: Option<ControlPortId>,
}

#[derive(Debug)]
enum BlockBoundary {
    Stop(Cursor),
    Terminated,
}

#[derive(Debug)]
struct ReducedBlock {
    steps: Vec<Value>,
    boundary: BlockBoundary,
}

#[derive(Debug)]
enum ReducedNodeFlow {
    Next(Cursor),
    Terminated,
}

struct Reducer<'a> {
    plan: &'a Plan,
    index: PlanIndex<'a>,
    root_scope: ScopeId,
    visited: BTreeSet<NodeId>,
    local_names: BTreeMap<DataPortId, String>,
    type_emitter: TypeEmitter,
    prompts: BTreeMap<String, Value>,
}

impl<'a> Reducer<'a> {
    fn new(plan: &'a Plan) -> ReductionResult<Self> {
        let index = PlanIndex::new(plan)
            .map_err(|error| format!("Plan indexing failed: {}", error.code()))?;
        let roots = plan
            .scopes()
            .iter()
            .filter(|scope| matches!(scope.kind(), ScopeKind::Root))
            .map(|scope| scope.id().clone())
            .collect::<Vec<_>>();
        let [root_scope] = roots.as_slice() else {
            return Err("the graph must contain exactly one root scope".to_owned());
        };
        Ok(Self {
            plan,
            index,
            root_scope: root_scope.clone(),
            visited: BTreeSet::new(),
            local_names: BTreeMap::new(),
            type_emitter: TypeEmitter::default(),
            prompts: BTreeMap::new(),
        })
    }

    fn reduce(mut self) -> ReductionResult<String> {
        if !self.plan.policies().is_empty() {
            return Err("authored policy syntax cannot yet represent this Plan".to_owned());
        }
        let entry = Cursor {
            node_id: self.plan.metadata().entry_node_id().clone(),
            via_input: None,
        };
        let root_scope = self.root_scope.clone();
        let block = self.reduce_sequence(entry, &root_scope, &BTreeSet::new())?;
        if !matches!(block.boundary, BlockBoundary::Terminated) {
            return Err("the root region does not terminate".to_owned());
        }
        if self.visited.len() != self.plan.nodes().len() {
            let missing = self
                .plan
                .nodes()
                .iter()
                .filter(|node| !self.visited.contains(node.id()))
                .map(|node| node.id().as_str())
                .collect::<Vec<_>>()
                .join(", ");
            return Err(format!(
                "the graph contains nodes outside recognized structured regions: {missing}"
            ));
        }

        let input_contract = self.plan.metadata().input_contract();
        let input = input_contract.accepted_type();
        let PlanType::Object {
            properties,
            additional_properties,
        } = input
        else {
            return Err("structured workflows require a closed named input object".to_owned());
        };
        if additional_properties.is_some() {
            return Err("open workflow input objects are not representable".to_owned());
        }
        let mut inputs = Map::new();
        for (name, property) in properties {
            let type_name = self.type_emitter.reference(&property.value_type)?;
            let declaration = if let Some(default) = input_contract.defaults().get(name) {
                json!({"type": type_name, "default": default})
            } else if !property.required {
                json!({"type": type_name, "optional": true})
            } else {
                Value::String(type_name)
            };
            inputs.insert(name.clone(), declaration);
        }
        let output = self
            .type_emitter
            .reference(self.plan.metadata().output_type())?;

        let mut document = Map::new();
        document.insert(
            "api_version".to_owned(),
            Value::String("insight.agent/v1".to_owned()),
        );
        document.insert("kind".to_owned(), Value::String("agent".to_owned()));
        if !self.type_emitter.declarations.is_empty() {
            document.insert(
                "types".to_owned(),
                Value::Object(self.type_emitter.declarations),
            );
        }
        if !self.prompts.is_empty() {
            document.insert(
                "prompts".to_owned(),
                Value::Object(self.prompts.into_iter().collect()),
            );
        }
        document.insert("inputs".to_owned(), Value::Object(inputs));
        document.insert("output".to_owned(), Value::String(output));
        document.insert(
            "workflow".to_owned(),
            json!({"steps": Value::Array(block.steps)}),
        );
        serde_json::to_string_pretty(&Value::Object(document))
            .map_err(|_| "the reduced document could not be encoded".to_owned())
    }

    fn reduce_sequence(
        &mut self,
        mut cursor: Cursor,
        expected_scope: &ScopeId,
        stops: &BTreeSet<NodeId>,
    ) -> ReductionResult<ReducedBlock> {
        let mut steps = Vec::new();
        loop {
            if stops.contains(&cursor.node_id) {
                return Ok(ReducedBlock {
                    steps,
                    boundary: BlockBoundary::Stop(cursor),
                });
            }
            let node = self
                .index
                .node(&cursor.node_id)
                .ok_or_else(|| format!("control reaches missing node '{}'", cursor.node_id))?;
            if node.scope_id() != expected_scope {
                return Err(format!(
                    "control crosses from structured scope '{expected_scope}' into '{}' at node '{}'",
                    node.scope_id(),
                    node.id()
                ));
            }
            if !self.visited.insert(node.id().clone()) {
                return Err(format!(
                    "node '{}' is shared by overlapping or crossing regions",
                    node.id()
                ));
            }
            let (step, flow) = self.reduce_node(node)?;
            steps.push(step);
            match flow {
                ReducedNodeFlow::Next(next) => cursor = next,
                ReducedNodeFlow::Terminated => {
                    return Ok(ReducedBlock {
                        steps,
                        boundary: BlockBoundary::Terminated,
                    });
                }
            }
        }
    }

    fn reduce_node(&mut self, node: &Node) -> ReductionResult<(Value, ReducedNodeFlow)> {
        match node.kind() {
            NodeKind::LlmTask(descriptor) => {
                let step = self.reduce_leaf(node, "llm", descriptor)?;
                Ok((step, self.linear_next(node)?))
            }
            NodeKind::ActionTask(descriptor) => {
                let step = self.reduce_leaf(node, "action", descriptor)?;
                Ok((step, self.linear_next(node)?))
            }
            NodeKind::RetrievalTask(descriptor) => {
                let step = self.reduce_leaf(node, "retrieval", descriptor)?;
                Ok((step, self.linear_next(node)?))
            }
            NodeKind::HttpTask(descriptor) => {
                let step = self.reduce_leaf(node, "http", descriptor)?;
                Ok((step, self.linear_next(node)?))
            }
            NodeKind::ToolTask(descriptor) => {
                let step = self.reduce_leaf(node, "tool", descriptor)?;
                Ok((step, self.linear_next(node)?))
            }
            NodeKind::Branch(descriptor) => self.reduce_branch(node, descriptor),
            NodeKind::Fork(descriptor) => self.reduce_parallel(node, descriptor),
            NodeKind::Map(descriptor) => self.reduce_map(node, descriptor),
            NodeKind::Loop(descriptor) => self.reduce_loop(node, descriptor),
            NodeKind::Return(descriptor) => {
                let source = self.required_input_source(&descriptor.value_input)?;
                Ok((
                    json!({"return": self.source_value(source)?}),
                    ReducedNodeFlow::Terminated,
                ))
            }
            NodeKind::Raise(descriptor) => {
                let source = self.required_input_source(&descriptor.error_input)?;
                Ok((
                    json!({"raise": self.source_value(source)?}),
                    ReducedNodeFlow::Terminated,
                ))
            }
            NodeKind::Merge(_)
            | NodeKind::Join(_)
            | NodeKind::Collect(_)
            | NodeKind::ErrorBoundary(_)
            | NodeKind::SubflowCall(_)
            | NodeKind::HumanTask(_)
            | NodeKind::WaitSignal(_)
            | NodeKind::Timer(_) => Err(format!(
                "node '{}' ({}) is not at a valid structured region boundary",
                node.id(),
                node.kind().name()
            )),
        }
    }

    fn reduce_leaf(
        &mut self,
        node: &Node,
        kind: &str,
        descriptor: &LeafTaskDescriptor,
    ) -> ReductionResult<Value> {
        let expected_descriptor_version = if kind == "llm" { "2" } else { "1" };
        if descriptor.descriptor_version.as_str() != expected_descriptor_version {
            return Err(format!(
                "leaf '{}' uses descriptor version '{}' without the expected author-surface inverse '{}'",
                node.id(),
                descriptor.descriptor_version,
                expected_descriptor_version,
            ));
        }
        if !descriptor.secret_configuration.is_empty() {
            return Err(format!(
                "leaf '{}' contains secret bindings that cannot be emitted into source",
                node.id()
            ));
        }
        let mut configuration = descriptor
            .public_configuration
            .iter()
            .map(|(name, value)| Ok((name.clone(), descriptor_json(value)?)))
            .collect::<ReductionResult<Map<_, _>>>()?;
        configuration.remove("runtime_bindings");
        configuration.remove("optional_runtime_bindings");
        configuration.remove("prompt_catalog");
        let message_program = configuration.remove("message_program");

        match kind {
            "action" => {
                if configuration.get("call").and_then(Value::as_str)
                    != Some(descriptor.implementation.as_str())
                {
                    return Err(format!(
                        "action '{}' implementation and authored call do not match",
                        node.id()
                    ));
                }
            }
            "llm" if descriptor.implementation != "core.llm" => {
                return Err(format!("LLM '{}' has a non-core implementation", node.id()));
            }
            "retrieval" => {
                if configuration.get("retrieval").and_then(Value::as_str)
                    != Some(descriptor.implementation.as_str())
                {
                    return Err(format!(
                        "retrieval '{}' implementation and authored resource do not match",
                        node.id()
                    ));
                }
                if !matches!(configuration.get("publish"), Some(Value::Bool(_))) {
                    return Err(format!(
                        "retrieval '{}' has no normalized publish decision",
                        node.id()
                    ));
                }
            }
            "http" if descriptor.implementation != "core.http" => {
                return Err(format!(
                    "HTTP '{}' has a non-core implementation",
                    node.id()
                ));
            }
            "tool" if descriptor.implementation != "core.tool" => {
                return Err(format!(
                    "tool '{}' has a non-core implementation",
                    node.id()
                ));
            }
            _ => {}
        }

        if kind == "llm" {
            self.merge_prompt_catalog(descriptor)?;
            let messages = message_program
                .ok_or_else(|| format!("LLM '{}' has no reversible message program", node.id()))?;
            configuration.insert("messages".to_owned(), decode_message_program(&messages)?);
        } else if message_program.is_some() {
            return Err(format!(
                "non-LLM leaf '{}' unexpectedly contains a message program",
                node.id()
            ));
        }

        let result_type = self
            .data_output_named(node.id(), "result")?
            .value_type()
            .clone();
        let response = self.type_emitter.reference(&result_type)?;
        let mut step = Map::from_iter([
            (
                "id".to_owned(),
                Value::String(node.id().as_str().to_owned()),
            ),
            ("type".to_owned(), Value::String(kind.to_owned())),
        ]);
        step.extend(configuration);
        step.insert("response".to_owned(), Value::String(response));
        Ok(Value::Object(step))
    }

    fn merge_prompt_catalog(&mut self, descriptor: &LeafTaskDescriptor) -> ReductionResult<()> {
        let Some(catalog) = descriptor.public_configuration.get("prompt_catalog") else {
            return Ok(());
        };
        let DescriptorValue::Object(catalog) = catalog else {
            return Err("LLM prompt_catalog is not an object".to_owned());
        };
        for (prompt_id, value) in catalog {
            let DescriptorValue::Object(fields) = value else {
                return Err(format!("prompt '{prompt_id}' descriptor is not an object"));
            };
            if fields.contains_key("source_path") {
                return Err(format!(
                    "file-backed prompt '{prompt_id}' requires an external resource and cannot be reduced losslessly"
                ));
            }
            let Some(DescriptorValue::String(content)) = fields.get("content") else {
                return Err(format!("prompt '{prompt_id}' has no embedded content"));
            };
            let declaration = json!({"inline": content});
            if let Some(existing) = self.prompts.insert(prompt_id.clone(), declaration.clone()) {
                if existing != declaration {
                    return Err(format!(
                        "prompt '{prompt_id}' has conflicting embedded definitions"
                    ));
                }
            }
        }
        Ok(())
    }

    fn reduce_branch(
        &mut self,
        node: &Node,
        descriptor: &insight_engine::plan::BranchDescriptor,
    ) -> ReductionResult<(Value, ReducedNodeFlow)> {
        let Some(first) = descriptor.cases.first() else {
            return Err(format!("Branch '{}' has no cases", node.id()));
        };
        let Some(last) = descriptor.cases.last() else {
            unreachable!("the first case exists")
        };
        if first.case_id.as_str() != "then"
            || first.condition.is_none()
            || last.case_id.as_str() != "else"
            || last.condition.is_some()
        {
            return Err(format!(
                "Branch '{}' does not use the structured then/elif/else case contract",
                node.id()
            ));
        }
        if descriptor.cases[..descriptor.cases.len() - 1]
            .iter()
            .any(|case| case.condition.is_none())
        {
            return Err(format!(
                "Branch '{}' has a non-final default case",
                node.id()
            ));
        }

        let merges = self
            .plan
            .nodes()
            .iter()
            .filter_map(|candidate| match candidate.kind() {
                NodeKind::Merge(value) if value.branch_node_id == *node.id() => {
                    Some((candidate, value))
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        if merges.len() > 1 {
            return Err(format!(
                "Branch '{}' has multiple correlated merges",
                node.id()
            ));
        }
        let merge = merges.first().copied();
        let merge_stop = merge
            .map(|(merge_node, _)| BTreeSet::from([merge_node.id().clone()]))
            .unwrap_or_default();
        let phi = merge
            .map(|(merge_node, _)| {
                self.plan
                    .phi_bindings()
                    .iter()
                    .filter(|binding| binding.merge_node_id() == merge_node.id())
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        if phi.len() > 1 {
            return Err(format!(
                "Branch '{}' has multiple yielded values",
                node.id()
            ));
        }

        let mut arms = Vec::with_capacity(descriptor.cases.len());
        let mut all_terminal = true;
        for case in &descriptor.cases {
            let route = self.route_from(&case.output_port)?.ok_or_else(|| {
                format!(
                    "Branch '{}' case '{}' has no control route",
                    node.id(),
                    case.case_id
                )
            })?;
            let arm_scope = self.branch_arm_scope(node.id(), &case.case_id);
            let mut block = if merge_stop.contains(&route.node_id) {
                ReducedBlock {
                    steps: Vec::new(),
                    boundary: BlockBoundary::Stop(route),
                }
            } else {
                let scope = arm_scope.ok_or_else(|| {
                    format!(
                        "Branch '{}' executable case '{}' has no correlated arm scope",
                        node.id(),
                        case.case_id
                    )
                })?;
                self.reduce_sequence(route, &scope, &merge_stop)?
            };
            match &block.boundary {
                BlockBoundary::Stop(cursor) => {
                    all_terminal = false;
                    let Some((merge_node, merge_descriptor)) = merge else {
                        return Err(format!(
                            "Branch '{}' arm reaches a missing correlated Merge",
                            node.id()
                        ));
                    };
                    let expected = merge_descriptor.arms.get(&case.case_id).ok_or_else(|| {
                        format!("Merge '{}' omits case '{}'", merge_node.id(), case.case_id)
                    })?;
                    if cursor.via_input.as_ref() != Some(expected) {
                        return Err(format!(
                            "Branch '{}' case '{}' reaches the wrong Merge input",
                            node.id(),
                            case.case_id
                        ));
                    }
                    if let Some(phi) = phi.first() {
                        let source = phi
                            .sources()
                            .get(&case.case_id)
                            .ok_or_else(|| format!("Phi omits Branch case '{}'", case.case_id))?;
                        block
                            .steps
                            .push(json!({"yield": self.source_value(source)?}));
                    }
                }
                BlockBoundary::Terminated => {
                    if phi
                        .first()
                        .is_some_and(|binding| binding.sources().contains_key(&case.case_id))
                    {
                        return Err(format!(
                            "terminal Branch case '{}' unexpectedly participates in Phi",
                            case.case_id
                        ));
                    }
                }
            }
            arms.push((case, block.steps));
        }

        let mut step = Map::from_iter([
            (
                "id".to_owned(),
                Value::String(node.id().as_str().to_owned()),
            ),
            (
                "if".to_owned(),
                Value::String(
                    first
                        .condition
                        .as_ref()
                        .expect("validated first condition")
                        .source
                        .clone(),
                ),
            ),
            ("then".to_owned(), Value::Array(arms[0].1.clone())),
        ]);
        if arms.len() > 2 {
            let elif = arms[1..arms.len() - 1]
                .iter()
                .map(|(case, steps)| {
                    json!({
                        "id": case.case_id.as_str(),
                        "when": case.condition.as_ref().expect("non-default elif").source,
                        "then": steps,
                    })
                })
                .collect::<Vec<_>>();
            step.insert("elif".to_owned(), Value::Array(elif));
        }
        step.insert(
            "else".to_owned(),
            Value::Array(arms.last().expect("Branch has cases").1.clone()),
        );

        if all_terminal {
            return Ok((Value::Object(step), ReducedNodeFlow::Terminated));
        }
        let Some((merge_node, merge_descriptor)) = merge else {
            return Err(format!("Branch '{}' has no Merge", node.id()));
        };
        if !self.visited.insert(merge_node.id().clone()) {
            return Err(format!(
                "Merge '{}' belongs to overlapping regions",
                merge_node.id()
            ));
        }
        let next = self
            .route_from(&merge_descriptor.output_port)?
            .ok_or_else(|| format!("Merge '{}' has no successor", merge_node.id()))?;
        Ok((Value::Object(step), ReducedNodeFlow::Next(next)))
    }

    fn reduce_parallel(
        &mut self,
        node: &Node,
        descriptor: &ForkDescriptor,
    ) -> ReductionResult<(Value, ReducedNodeFlow)> {
        let (join_node, join) = exactly_one(
            self.plan
                .nodes()
                .iter()
                .filter_map(|candidate| match candidate.kind() {
                    NodeKind::Join(value) if value.fork_node_id == *node.id() => {
                        Some((candidate, value))
                    }
                    _ => None,
                }),
            &format!("Fork '{}' correlated Join", node.id()),
        )?;
        let (collect_node, collect) = exactly_one(
            self.plan
                .nodes()
                .iter()
                .filter_map(|candidate| match candidate.kind() {
                    NodeKind::Collect(value)
                        if matches!(
                            &value.source,
                            CollectSource::StaticFork { fork_node_id, join_node_id, .. }
                                if fork_node_id == node.id() && join_node_id == join_node.id()
                        ) =>
                    {
                        Some((candidate, value))
                    }
                    _ => None,
                }),
            &format!("Fork '{}' result Collect", node.id()),
        )?;
        let stops = BTreeSet::from([join_node.id().clone()]);
        let mut legs = Map::new();
        for leg in &descriptor.legs {
            let route = self
                .route_from(&leg.output_port)?
                .ok_or_else(|| format!("Fork '{}' leg '{}' has no route", node.id(), leg.leg_id))?;
            let mut block = self.reduce_sequence(route, &leg.scope_id, &stops)?;
            let BlockBoundary::Stop(cursor) = &block.boundary else {
                return Err(format!(
                    "Fork '{}' leg '{}' terminates instead of joining",
                    node.id(),
                    leg.leg_id
                ));
            };
            let expected = join
                .legs
                .get(&leg.leg_id)
                .ok_or_else(|| format!("Join '{}' omits leg '{}'", join_node.id(), leg.leg_id))?;
            if cursor.via_input.as_ref() != Some(expected) {
                return Err(format!(
                    "Fork '{}' leg '{}' reaches the wrong Join input",
                    node.id(),
                    leg.leg_id
                ));
            }
            block
                .steps
                .push(json!({"yield": self.port_value(&leg.yield_port)?}));
            legs.insert(leg.leg_id.as_str().to_owned(), Value::Array(block.steps));
        }
        for internal in [join_node, collect_node] {
            if !self.visited.insert(internal.id().clone()) {
                return Err(format!(
                    "parallel internal node '{}' belongs to overlapping regions",
                    internal.id()
                ));
            }
        }
        let next = self.route_from(&collect.output_port_owner_control(self)?)?;
        let next =
            next.ok_or_else(|| format!("Collect '{}' has no successor", collect_node.id()))?;
        let step = json!({
            "id": node.id().as_str(),
            "settle": match descriptor.join_mode {
                PlanJoinMode::AllSuccess => "all_success",
                PlanJoinMode::AllSettled => "all_settled",
            },
            "parallel": Value::Object(legs),
        });
        Ok((step, ReducedNodeFlow::Next(next)))
    }

    fn reduce_map(
        &mut self,
        node: &Node,
        descriptor: &insight_engine::plan::MapDescriptor,
    ) -> ReductionResult<(Value, ReducedNodeFlow)> {
        let (collect_node, collect, key_field, empty_output, body_input, empty_input) =
            exactly_one(
                self.plan
                    .nodes()
                    .iter()
                    .filter_map(|candidate| match candidate.kind() {
                        NodeKind::Collect(value) => match &value.source {
                            CollectSource::DynamicMap {
                                map_node_id,
                                key_field,
                                empty_output,
                                body_input,
                                empty_input,
                                ..
                            } if map_node_id == node.id() => Some((
                                candidate,
                                value,
                                key_field.as_deref(),
                                empty_output,
                                body_input,
                                empty_input,
                            )),
                            _ => None,
                        },
                        _ => None,
                    }),
                &format!("Map '{}' result Collect", node.id()),
            )?;
        let item_name = self.infer_local_name(&descriptor.item_port, "item")?;
        self.local_names
            .insert(descriptor.item_port.clone(), item_name.clone());
        let body_route = self
            .route_from(&self.control_output_named(node.id(), "body")?)?
            .ok_or_else(|| format!("Map '{}' body has no control route", node.id()))?;
        let stops = BTreeSet::from([collect_node.id().clone()]);
        let mut body = self.reduce_sequence(body_route, &descriptor.body_scope_id, &stops)?;
        let BlockBoundary::Stop(body_boundary) = &body.boundary else {
            return Err(format!(
                "Map '{}' body terminates instead of collecting",
                node.id()
            ));
        };
        if body_boundary.node_id != *collect_node.id()
            || body_boundary.via_input.as_ref() != Some(body_input)
        {
            return Err(format!(
                "Map '{}' body reaches the wrong Collect input",
                node.id()
            ));
        }
        let empty_boundary = self
            .route_from(empty_output)?
            .ok_or_else(|| format!("Map '{}' empty path has no Collect route", node.id()))?;
        if empty_boundary.node_id != *collect_node.id()
            || empty_boundary.via_input.as_ref() != Some(empty_input)
        {
            return Err(format!(
                "Map '{}' empty path reaches the wrong Collect input",
                node.id()
            ));
        }
        body.steps
            .push(json!({"yield": self.port_value(&descriptor.yield_port)?}));
        if !self.visited.insert(collect_node.id().clone()) {
            return Err(format!(
                "Map Collect '{}' overlaps another region",
                collect_node.id()
            ));
        }
        let next = self
            .route_from(&collect.output_port_owner_control(self)?)?
            .ok_or_else(|| format!("Map Collect '{}' has no successor", collect_node.id()))?;
        let mut declaration = Map::from_iter([
            (
                "items".to_owned(),
                self.expression_value(&descriptor.items)?,
            ),
            ("as".to_owned(), Value::String(item_name)),
            ("steps".to_owned(), Value::Array(body.steps)),
        ]);
        if let Some(key_field) = key_field {
            declaration.insert("key".to_owned(), Value::String(key_field.to_owned()));
        }
        if let Some(maximum) = descriptor.max_concurrency {
            declaration.insert("max_concurrency".to_owned(), Value::from(maximum));
        }
        Ok((
            json!({"id": node.id().as_str(), "map": Value::Object(declaration)}),
            ReducedNodeFlow::Next(next),
        ))
    }

    fn reduce_loop(
        &mut self,
        node: &Node,
        descriptor: &insight_engine::plan::LoopDescriptor,
    ) -> ReductionResult<(Value, ReducedNodeFlow)> {
        let (collect_node, collect, source) = exactly_one(
            self.plan
                .nodes()
                .iter()
                .filter_map(|candidate| match candidate.kind() {
                    NodeKind::Collect(value) => match &value.source {
                        source @ CollectSource::Loop { loop_node_id, .. }
                            if loop_node_id == node.id() =>
                        {
                            Some((candidate, value, source))
                        }
                        _ => None,
                    },
                    _ => None,
                }),
            &format!("Loop '{}' result Collect", node.id()),
        )?;
        let CollectSource::Loop {
            initial_input,
            state_port,
            yield_port,
            completed_input,
            break_input,
            ..
        } = source
        else {
            unreachable!("filtered Loop Collect source")
        };
        let body_scope = exactly_one(
            self.plan
                .scopes()
                .iter()
                .filter_map(|scope| match scope.kind() {
                    ScopeKind::LoopBody { loop_node_id } if loop_node_id == node.id() => {
                        Some(scope.id().clone())
                    }
                    _ => None,
                }),
            &format!("Loop '{}' body scope", node.id()),
        )?;
        let state_name = self.infer_local_name(state_port, "state")?;
        self.local_names
            .insert(state_port.clone(), state_name.clone());
        let body_route = self
            .route_from(&descriptor.body_output)?
            .ok_or_else(|| format!("Loop '{}' body has no control route", node.id()))?;
        let stops = BTreeSet::from([node.id().clone(), collect_node.id().clone()]);
        let mut body = self.reduce_sequence(body_route, &body_scope, &stops)?;
        let BlockBoundary::Stop(cursor) = &body.boundary else {
            return Err(format!("Loop '{}' body terminates the workflow", node.id()));
        };
        let terminator = if cursor.node_id == *node.id()
            && cursor.via_input.as_ref() == Some(&descriptor.continue_input)
        {
            "continue"
        } else if cursor.node_id == *collect_node.id()
            && break_input.as_ref() == cursor.via_input.as_ref()
        {
            "break"
        } else {
            return Err(format!("Loop '{}' body reaches an invalid exit", node.id()));
        };
        let completed = self
            .route_from(&descriptor.completed_output)?
            .ok_or_else(|| format!("Loop '{}' completed path has no Collect route", node.id()))?;
        if completed.node_id != *collect_node.id()
            || completed.via_input.as_ref() != Some(completed_input)
        {
            return Err(format!(
                "Loop '{}' completed path reaches the wrong Collect input",
                node.id()
            ));
        }
        body.steps
            .push(json!({terminator: self.port_value(yield_port)?}));
        if !self.visited.insert(collect_node.id().clone()) {
            return Err(format!(
                "Loop Collect '{}' overlaps another region",
                collect_node.id()
            ));
        }
        let next = self
            .route_from(&collect.output_port_owner_control(self)?)?
            .ok_or_else(|| format!("Loop Collect '{}' has no successor", collect_node.id()))?;
        let initial = self.required_input_source(initial_input)?;
        let mut declaration = Map::from_iter([
            ("initial".to_owned(), self.source_value(initial)?),
            ("as".to_owned(), Value::String(state_name)),
            (
                "until".to_owned(),
                Value::String(descriptor.exit_condition.source.clone()),
            ),
            ("steps".to_owned(), Value::Array(body.steps)),
        ]);
        if let Some(maximum) = descriptor.max_iterations {
            declaration.insert("max_iterations".to_owned(), Value::from(maximum));
        }
        if let Some(deadline) = descriptor.deadline_ms {
            declaration.insert("deadline_ms".to_owned(), Value::from(deadline));
        }
        let field = match descriptor.flavor {
            LoopFlavor::Workflow => "loop",
            LoopFlavor::Agent => "agent_loop",
        };
        Ok((
            json!({"id": node.id().as_str(), field: Value::Object(declaration)}),
            ReducedNodeFlow::Next(next),
        ))
    }

    fn linear_next(&self, node: &Node) -> ReductionResult<ReducedNodeFlow> {
        let output = self.control_output_named(node.id(), "out")?;
        self.route_from(&output)?
            .map(ReducedNodeFlow::Next)
            .ok_or_else(|| format!("linear node '{}' has no successor", node.id()))
    }

    fn route_from(&self, output: &ControlPortId) -> ReductionResult<Option<Cursor>> {
        self.index
            .successor_for_output(output)
            .map_err(|error| format!("control route is invalid: {}", error.code()))
            .map(|route| {
                route.map(|route| Cursor {
                    node_id: route.successor().id().clone(),
                    via_input: Some(route.input().id().clone()),
                })
            })
    }

    fn control_output_named(&self, node_id: &NodeId, name: &str) -> ReductionResult<ControlPortId> {
        self.plan
            .control_ports()
            .iter()
            .find(|port| {
                port.owner() == node_id
                    && port.direction() == PortDirection::Output
                    && port.name().as_str() == name
            })
            .map(|port| port.id().clone())
            .ok_or_else(|| format!("node '{node_id}' has no '{name}' control output"))
    }

    fn data_output_named(
        &self,
        node_id: &NodeId,
        name: &str,
    ) -> ReductionResult<&insight_engine::plan::DataPort> {
        self.plan
            .data_ports()
            .iter()
            .find(|port| {
                port.owner() == node_id
                    && port.direction() == PortDirection::Output
                    && port.name().as_str() == name
            })
            .ok_or_else(|| format!("node '{node_id}' has no '{name}' data output"))
    }

    fn required_input_source(&self, input: &DataPortId) -> ReductionResult<&ValueSource> {
        self.index
            .source_for_input(input)
            .ok_or_else(|| format!("data input '{input}' is not bound"))
    }

    fn source_value(&self, source: &ValueSource) -> ReductionResult<Value> {
        match source {
            ValueSource::RunInput { path } | ValueSource::OptionalRunInput { path } => {
                if path.is_empty() {
                    return Err(
                        "whole-workflow input references are not authored values".to_owned()
                    );
                }
                Ok(Value::String(format!("${}", path.join("."))))
            }
            ValueSource::Port { port_id } => self.port_value(port_id),
            ValueSource::Literal { value } => Ok(value.clone()),
            ValueSource::Expression { expression } => self.expression_value(expression),
        }
    }

    fn port_value(&self, port_id: &DataPortId) -> ReductionResult<Value> {
        if let Some(name) = self.local_names.get(port_id) {
            return Ok(Value::String(format!("${name}")));
        }
        let port = self
            .index
            .data_port(port_id)
            .ok_or_else(|| format!("value references missing data port '{port_id}'"))?;
        if port.direction() == PortDirection::Input {
            return self.source_value(self.required_input_source(port_id)?);
        }
        let owner = self
            .index
            .node(port.owner())
            .ok_or_else(|| format!("data port '{port_id}' has no owner"))?;
        let symbol = match owner.kind() {
            NodeKind::Merge(descriptor) => descriptor.branch_node_id.as_str(),
            NodeKind::Collect(descriptor) => match &descriptor.source {
                CollectSource::StaticFork { fork_node_id, .. } => fork_node_id.as_str(),
                CollectSource::Map { map_node_id }
                | CollectSource::DynamicMap { map_node_id, .. } => map_node_id.as_str(),
                CollectSource::Loop { loop_node_id, .. } => loop_node_id.as_str(),
            },
            NodeKind::LlmTask(_)
            | NodeKind::ActionTask(_)
            | NodeKind::RetrievalTask(_)
            | NodeKind::HttpTask(_)
            | NodeKind::ToolTask(_)
            | NodeKind::SubflowCall(_)
            | NodeKind::HumanTask(_)
            | NodeKind::WaitSignal(_) => owner.id().as_str(),
            _ => {
                return Err(format!(
                    "output port '{port_id}' has no structured value symbol"
                ));
            }
        };
        Ok(Value::String(format!("${symbol}")))
    }

    fn expression_value(&self, expression: &PureExpression) -> ReductionResult<Value> {
        match expression.language {
            ExpressionLanguage::Literal => serde_json::from_str(&expression.source)
                .map_err(|_| "literal expression contains invalid canonical JSON".to_owned()),
            ExpressionLanguage::Cel => {
                if expression.dependencies.len() != 1 {
                    return Err(
                        "value-position CEL is not a reversible collection reference".to_owned(),
                    );
                }
                let (name, port) = expression
                    .dependencies
                    .iter()
                    .next()
                    .expect("one dependency");
                if expression.source != *name {
                    return Err("value-position CEL is not a bare reference".to_owned());
                }
                self.port_value(port)
            }
            ExpressionLanguage::Value => {
                let program: ValueProgram = serde_json::from_str(&expression.source)
                    .map_err(|_| "value expression program is malformed".to_owned())?;
                self.value_program(&program, expression)
            }
            ExpressionLanguage::Match => {
                let program: MatchProgram = serde_json::from_str(&expression.source)
                    .map_err(|_| "match expression program is malformed".to_owned())?;
                Ok(json!({
                    "match": self.match_value(&program.selector, expression)?,
                    "cases": program.cases.iter().map(|(key, value)| {
                        Ok((key.clone(), self.match_value(value, expression)?))
                    }).collect::<ReductionResult<Map<_, _>>>()?,
                    "default": self.match_value(&program.default, expression)?,
                }))
            }
            ExpressionLanguage::Template => {
                Err("legacy template expressions are not reversible".to_owned())
            }
        }
    }

    fn value_program(
        &self,
        program: &ValueProgram,
        expression: &PureExpression,
    ) -> ReductionResult<Value> {
        match program {
            ValueProgram::Dependency { name, path } => {
                let port = expression.dependencies.get(name).ok_or_else(|| {
                    format!("value program references missing dependency '{name}'")
                })?;
                let Value::String(mut reference) = self.port_value(port)? else {
                    return Err("value dependency is not a reference".to_owned());
                };
                for segment in path {
                    reference.push('.');
                    reference.push_str(segment);
                }
                Ok(Value::String(reference))
            }
            ValueProgram::Literal { value } => Ok(value.clone()),
            ValueProgram::Array { items } => items
                .iter()
                .map(|item| self.value_program(item, expression))
                .collect::<ReductionResult<Vec<_>>>()
                .map(Value::Array),
            ValueProgram::Object { fields } => fields
                .iter()
                .map(|(name, value)| Ok((name.clone(), self.value_program(value, expression)?)))
                .collect::<ReductionResult<Map<_, _>>>()
                .map(Value::Object),
            ValueProgram::Template { parts } => {
                let mut source = String::new();
                for part in parts {
                    match part {
                        TemplatePart::Text { text } => source.push_str(text),
                        TemplatePart::Value { value } => {
                            let Value::String(reference) = self.value_program(value, expression)?
                            else {
                                return Err("template interpolation is not a reference".to_owned());
                            };
                            let Some(reference) = reference.strip_prefix('$') else {
                                return Err(
                                    "template interpolation is not a runtime reference".to_owned()
                                );
                            };
                            source.push_str("{{ ");
                            source.push_str(reference);
                            source.push_str(" }}");
                        }
                    }
                }
                Ok(Value::String(source))
            }
        }
    }

    fn match_value(
        &self,
        value: &MatchValue,
        expression: &PureExpression,
    ) -> ReductionResult<Value> {
        match value {
            MatchValue::Dependency { name } => expression
                .dependencies
                .get(name)
                .ok_or_else(|| format!("match references missing dependency '{name}'"))
                .and_then(|port| self.port_value(port)),
            MatchValue::Literal { value } => Ok(value.clone()),
            MatchValue::Match {
                selector,
                cases,
                default,
            } => Ok(json!({
                "match": self.match_value(selector, expression)?,
                "cases": cases.iter().map(|(key, value)| {
                    Ok((key.clone(), self.match_value(value, expression)?))
                }).collect::<ReductionResult<Map<_, _>>>()?,
                "default": self.match_value(default, expression)?,
            })),
        }
    }

    fn infer_local_name(&self, port: &DataPortId, fallback: &str) -> ReductionResult<String> {
        let mut candidates = BTreeSet::new();
        for node in self.plan.nodes() {
            let descriptor = match node.kind() {
                NodeKind::LlmTask(value)
                | NodeKind::ActionTask(value)
                | NodeKind::RetrievalTask(value)
                | NodeKind::HttpTask(value)
                | NodeKind::ToolTask(value) => value,
                _ => continue,
            };
            let Some(DescriptorValue::Object(bindings)) =
                descriptor.public_configuration.get("runtime_bindings")
            else {
                continue;
            };
            for (reference, target) in bindings {
                let DescriptorValue::String(target) = target else {
                    continue;
                };
                let Some(target) = self
                    .plan
                    .data_ports()
                    .iter()
                    .find(|candidate| candidate.id().as_str() == target)
                else {
                    continue;
                };
                if self.index.source_for_input(target.id()).is_some_and(
                    |source| matches!(source, ValueSource::Port { port_id } if port_id == port),
                ) {
                    candidates.insert(reference.split('.').next().unwrap_or(reference).to_owned());
                }
            }
        }
        for expression in self.plan_expressions() {
            for (name, dependency) in &expression.dependencies {
                if name.starts_with('d') && name[1..].bytes().all(|byte| byte.is_ascii_digit()) {
                    continue;
                }
                let resolves = dependency == port
                    || self.index.source_for_input(dependency).is_some_and(
                        |source| matches!(source, ValueSource::Port { port_id } if port_id == port),
                    );
                if resolves {
                    candidates.insert(name.split('.').next().unwrap_or(name).to_owned());
                }
            }
        }
        match candidates.len() {
            0 => Ok(fallback.to_owned()),
            1 => Ok(candidates.into_iter().next().expect("one candidate")),
            _ => Err(format!(
                "data port '{port}' is referenced through conflicting local names {candidates:?}"
            )),
        }
    }

    fn plan_expressions(&self) -> Vec<&PureExpression> {
        let mut values = Vec::new();
        for node in self.plan.nodes() {
            match node.kind() {
                NodeKind::Branch(descriptor) => values.extend(
                    descriptor
                        .cases
                        .iter()
                        .filter_map(|case| case.condition.as_ref()),
                ),
                NodeKind::Map(descriptor) => values.push(&descriptor.items),
                NodeKind::Loop(descriptor) => values.push(&descriptor.exit_condition),
                NodeKind::Timer(descriptor) => values.push(&descriptor.delay_ms),
                _ => {}
            }
        }
        values
    }

    fn branch_arm_scope(&self, branch: &NodeId, case: &BranchCaseId) -> Option<ScopeId> {
        self.plan
            .scopes()
            .iter()
            .find_map(|scope| match scope.kind() {
                ScopeKind::BranchArm {
                    branch_node_id,
                    case_id,
                } if branch_node_id == branch && case_id == case => Some(scope.id().clone()),
                _ => None,
            })
    }
}

trait CollectControlOutput {
    fn output_port_owner_control(&self, reducer: &Reducer<'_>) -> ReductionResult<ControlPortId>;
}

impl CollectControlOutput for insight_engine::plan::CollectDescriptor {
    fn output_port_owner_control(&self, reducer: &Reducer<'_>) -> ReductionResult<ControlPortId> {
        let owner = reducer
            .index
            .data_port(&self.output_port)
            .ok_or_else(|| format!("Collect output '{}' is missing", self.output_port))?
            .owner();
        reducer.control_output_named(owner, "out")
    }
}

#[derive(Default)]
struct TypeEmitter {
    declarations: Map<String, Value>,
    names: BTreeMap<String, String>,
    next_id: usize,
}

impl TypeEmitter {
    fn reference(&mut self, value_type: &PlanType) -> ReductionResult<String> {
        match value_type {
            PlanType::Any => Ok("any".to_owned()),
            PlanType::Null => Ok("null".to_owned()),
            PlanType::Boolean => Ok("boolean".to_owned()),
            PlanType::Integer => Ok("integer".to_owned()),
            PlanType::Number => Ok("number".to_owned()),
            PlanType::String => Ok("string".to_owned()),
            PlanType::Array { items, min_items } if *min_items == 0 => {
                Ok(format!("{}[]", self.reference(items)?))
            }
            PlanType::StringRefined { .. }
            | PlanType::Array { .. }
            | PlanType::ArrayBounded { .. }
            | PlanType::Object { .. } => self.named(value_type),
            PlanType::Never | PlanType::Literal { .. } | PlanType::Union { .. } => Err(format!(
                "Plan type {value_type:?} has no lossless structured type spelling"
            )),
        }
    }

    fn named(&mut self, value_type: &PlanType) -> ReductionResult<String> {
        let key = serde_jcs::to_string(value_type)
            .map_err(|_| "Plan type could not be canonicalized".to_owned())?;
        if let Some(name) = self.names.get(&key) {
            return Ok(name.clone());
        }
        self.next_id += 1;
        let name = format!("ReducedType{}", self.next_id);
        self.names.insert(key, name.clone());
        let declaration = match value_type {
            PlanType::StringRefined {
                min_length,
                max_length,
                pattern,
                enum_values,
            } => {
                let mut value =
                    Map::from_iter([("type".to_owned(), Value::String("string".to_owned()))]);
                if *min_length > 0 {
                    value.insert("min_length".to_owned(), Value::from(*min_length));
                }
                if let Some(maximum) = max_length {
                    value.insert("max_length".to_owned(), Value::from(*maximum));
                }
                if let Some(pattern) = pattern {
                    value.insert("pattern".to_owned(), Value::String(pattern.clone()));
                }
                if let Some(values) = enum_values {
                    value.insert("enum".to_owned(), Value::Array(values.clone()));
                }
                Value::Object(value)
            }
            PlanType::Array { items, min_items } => {
                let item = self.reference(items)?;
                json!({"type": format!("{item}[]"), "min_items": min_items})
            }
            PlanType::ArrayBounded {
                items,
                min_items,
                max_items,
            } => {
                let item = self.reference(items)?;
                json!({
                    "type": format!("{item}[]"),
                    "min_items": min_items,
                    "max_items": max_items,
                })
            }
            PlanType::Object {
                properties,
                additional_properties,
            } => {
                if additional_properties.is_some() {
                    return Err("open object types are not representable".to_owned());
                }
                let fields = properties
                    .iter()
                    .map(|(field, property)| {
                        if !property.required {
                            return Err(format!(
                                "optional object field '{field}' is not representable"
                            ));
                        }
                        Ok((
                            field.clone(),
                            Value::String(self.reference(&property.value_type)?),
                        ))
                    })
                    .collect::<ReductionResult<Map<_, _>>>()?;
                json!({"fields": Value::Object(fields)})
            }
            _ => return Err("type was routed to the wrong emitter".to_owned()),
        };
        self.declarations.insert(name.clone(), declaration);
        Ok(name)
    }
}

fn descriptor_json(value: &DescriptorValue) -> ReductionResult<Value> {
    Ok(match value {
        DescriptorValue::Null => Value::Null,
        DescriptorValue::Boolean(value) => Value::Bool(*value),
        DescriptorValue::Integer(value) => Value::from(*value),
        DescriptorValue::Number(value) => Value::Number(value.clone()),
        DescriptorValue::String(value) => Value::String(value.clone()),
        DescriptorValue::Array(values) => Value::Array(
            values
                .iter()
                .map(descriptor_json)
                .collect::<ReductionResult<Vec<_>>>()?,
        ),
        DescriptorValue::Object(values) => Value::Object(
            values
                .iter()
                .map(|(name, value)| Ok((name.clone(), descriptor_json(value)?)))
                .collect::<ReductionResult<Map<_, _>>>()?,
        ),
    })
}

fn decode_message_program(value: &Value) -> ReductionResult<Value> {
    let values = value
        .as_array()
        .ok_or_else(|| "LLM message program is not a list".to_owned())?;
    values
        .iter()
        .map(|message| {
            let object = message
                .as_object()
                .ok_or_else(|| "LLM message program entry is not an object".to_owned())?;
            match object.get("kind").and_then(Value::as_str) {
                Some("message_splice") => object
                    .get("path")
                    .and_then(Value::as_str)
                    .map(|path| Value::String(format!("${path}")))
                    .ok_or_else(|| "message splice has no path".to_owned()),
                Some("message") => {
                    let role = object
                        .get("role")
                        .and_then(Value::as_str)
                        .ok_or_else(|| "message has no role".to_owned())?;
                    let content = object
                        .get("content")
                        .and_then(Value::as_array)
                        .ok_or_else(|| "message has no content list".to_owned())?
                        .iter()
                        .map(decode_content_part)
                        .collect::<ReductionResult<Vec<_>>>()?;
                    Ok(json!({"role": role, "content": content}))
                }
                _ => Err("message program contains an unknown entry kind".to_owned()),
            }
        })
        .collect::<ReductionResult<Vec<_>>>()
        .map(Value::Array)
}

fn decode_content_part(value: &Value) -> ReductionResult<Value> {
    let object = value
        .as_object()
        .ok_or_else(|| "message content part is not an object".to_owned())?;
    let kind = object
        .get("kind")
        .and_then(Value::as_str)
        .ok_or_else(|| "message content part has no kind".to_owned())?;
    if let Some(text) = object.get("text").and_then(Value::as_str) {
        let text = match kind {
            "prompt_ref" | "template" | "literal" => text.to_owned(),
            "value_ref" => format!("${text}"),
            _ => return Err(format!("unsupported text content kind '{kind}'")),
        };
        return Ok(json!({"text": text}));
    }
    if let Some(image) = object.get("image_url").and_then(Value::as_str) {
        let image = match kind {
            "literal" => image.to_owned(),
            "value_ref" => format!("${image}"),
            _ => return Err(format!("unsupported image content kind '{kind}'")),
        };
        return Ok(json!({"image_url": image}));
    }
    Err("message content part has no text or image_url".to_owned())
}

fn exactly_one<T>(values: impl IntoIterator<Item = T>, label: &str) -> ReductionResult<T> {
    let mut values = values.into_iter();
    let value = values.next().ok_or_else(|| format!("{label} is missing"))?;
    if values.next().is_some() {
        return Err(format!("{label} is ambiguous"));
    }
    Ok(value)
}
