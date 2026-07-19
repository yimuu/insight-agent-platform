use std::collections::{BTreeMap, BTreeSet};

use insight_agent_platform::engine::{
    plan::*, ContentHash, DefinitionRevisionId, DeploymentRevisionId, ExecutionRevisionPin, LegId,
    NodeId,
};
use serde_json::{json, Value};

fn node_id(value: &str) -> NodeId {
    NodeId::new(value).unwrap()
}

fn control_port_id(value: &str) -> ControlPortId {
    ControlPortId::new(value).unwrap()
}

fn data_port_id(value: &str) -> DataPortId {
    DataPortId::new(value).unwrap()
}

fn control_edge_id(value: &str) -> ControlEdgeId {
    ControlEdgeId::new(value).unwrap()
}

fn data_binding_id(value: &str) -> DataBindingId {
    DataBindingId::new(value).unwrap()
}

fn phi_binding_id(value: &str) -> PhiBindingId {
    PhiBindingId::new(value).unwrap()
}

fn scope_id(value: &str) -> ScopeId {
    ScopeId::new(value).unwrap()
}

fn policy_id(value: &str) -> PolicyId {
    PolicyId::new(value).unwrap()
}

fn case_id(value: &str) -> BranchCaseId {
    BranchCaseId::new(value).unwrap()
}

fn port_name(value: &str) -> PortName {
    PortName::new(value).unwrap()
}

fn version(value: &str) -> VersionTag {
    VersionTag::new(value).unwrap()
}

fn safe_error_type() -> PlanType {
    PlanType::Object {
        properties: BTreeMap::from([
            (
                "code".to_owned(),
                PlanProperty::new(PlanType::String, true).unwrap(),
            ),
            (
                "kind".to_owned(),
                PlanProperty::new(PlanType::literal(json!("safe_error")).unwrap(), true).unwrap(),
            ),
            (
                "message".to_owned(),
                PlanProperty::new(PlanType::String, true).unwrap(),
            ),
        ]),
        additional_properties: None,
    }
}

fn metadata(
    entry: &str,
    input_type: PlanType,
    output_type: PlanType,
    author_format: AuthorFormat,
    revision: &str,
    compiler_version: &str,
) -> PlanMetadata {
    PlanMetadata::new(
        DefinitionRevisionId::new(revision).unwrap(),
        version(compiler_version),
        author_format,
        node_id(entry),
        PlanInputContract::new(input_type),
        output_type,
        safe_error_type(),
    )
}

fn leaf(implementation: &str) -> LeafTaskDescriptor {
    LeafTaskDescriptor::new(implementation, version("1.0.0"), BTreeMap::new())
}

fn minimal_return_plan_with(
    author_format: AuthorFormat,
    revision: &str,
    compiler_version: &str,
    source_map: SourceMap,
    expression_version: Option<&str>,
) -> Plan {
    let root = scope_id("root_scope");
    let return_id = node_id("return_node");
    let value_input = data_port_id("return_value");
    let mut builder = PlanBuilder::new(metadata(
        "return_node",
        PlanType::String,
        PlanType::String,
        author_format,
        revision,
        compiler_version,
    ));
    builder
        .add_scope(ScopeMetadata::root(root.clone()))
        .add_node(Node::new(
            return_id.clone(),
            root,
            NodeKind::Return(ReturnDescriptor {
                value_input: value_input.clone(),
            }),
        ))
        .add_data_port(DataPort::new(
            value_input.clone(),
            return_id,
            port_name("value"),
            PortDirection::Input,
            PlanType::String,
            true,
        ));
    let source = expression_version.map_or_else(
        || ValueSource::RunInput { path: vec![] },
        |engine_version| ValueSource::Expression {
            expression: PureExpression::new(
                ExpressionLanguage::Cel,
                version(engine_version),
                r#""ok""#,
                PlanType::String,
            ),
        },
    );
    builder.add_data_binding(DataBinding::new(
        data_binding_id("bind_return"),
        source,
        value_input,
    ));
    if author_format == AuthorFormat::Programmatic {
        builder.set_source_map(source_map);
    } else {
        let source_id = SourceDocumentId::new("workflow.yaml").unwrap();
        let span = SourceSpan::new(
            source_id.clone(),
            SourcePosition::new(0, 1, 1),
            SourcePosition::new(1, 1, 2),
        );
        let mut complete = SourceMap::authored(
            source_id,
            ContentHash::from_bytes(match author_format {
                AuthorFormat::Structured => b"minimal structured workflow",
                AuthorFormat::Graph => b"minimal graph workflow",
                AuthorFormat::Programmatic => unreachable!("handled above"),
            }),
        );
        complete.insert_node(node_id("return_node"), span.clone());
        complete.insert_data_port(data_port_id("return_value"), span.clone());
        complete.insert_data_binding(data_binding_id("bind_return"), span.clone());
        complete.insert_scope(scope_id("root_scope"), span);
        builder.set_source_map(complete);
    }
    builder.build().unwrap()
}

fn minimal_return_plan() -> Plan {
    minimal_return_plan_with(
        AuthorFormat::Programmatic,
        "revision_one",
        "compiler-1",
        SourceMap::new(),
        None,
    )
}

fn linear_builder(reverse: bool, retry_attempts: u32, config_value: &str) -> PlanBuilder {
    linear_builder_with_descriptor_version(reverse, retry_attempts, config_value, "1")
}

fn linear_builder_with_descriptor_version(
    reverse: bool,
    retry_attempts: u32,
    config_value: &str,
    descriptor_version: &str,
) -> PlanBuilder {
    let root = scope_id("root_scope");
    let task = node_id("task_node");
    let ret = node_id("return_node");
    let task_out = control_port_id("task_out");
    let return_in = control_port_id("return_in");
    let task_value = data_port_id("task_value");
    let return_value = data_port_id("return_value");

    let mut configuration = BTreeMap::new();
    configuration.insert(
        "model".to_owned(),
        DescriptorValue::String(config_value.to_owned()),
    );
    let values = (
        Node::new(
            task.clone(),
            root.clone(),
            NodeKind::ActionTask(LeafTaskDescriptor::new(
                "fixture.action",
                version(descriptor_version),
                configuration,
            )),
        ),
        Node::new(
            ret.clone(),
            root.clone(),
            NodeKind::Return(ReturnDescriptor {
                value_input: return_value.clone(),
            }),
        ),
        ControlPort::new(
            task_out.clone(),
            task.clone(),
            port_name("out"),
            PortDirection::Output,
        ),
        ControlPort::new(
            return_in.clone(),
            ret.clone(),
            port_name("in"),
            PortDirection::Input,
        ),
        DataPort::new(
            task_value.clone(),
            task.clone(),
            port_name("value"),
            PortDirection::Output,
            PlanType::String,
            false,
        ),
        DataPort::new(
            return_value.clone(),
            ret,
            port_name("value"),
            PortDirection::Input,
            PlanType::String,
            true,
        ),
    );

    let mut builder = PlanBuilder::new(metadata(
        "task_node",
        PlanType::Null,
        PlanType::String,
        AuthorFormat::Programmatic,
        "linear_revision",
        "compiler-1",
    ));
    builder.add_scope(ScopeMetadata::root(root));
    if reverse {
        builder
            .add_node(values.1)
            .add_node(values.0)
            .add_control_port(values.3)
            .add_control_port(values.2)
            .add_data_port(values.5)
            .add_data_port(values.4);
    } else {
        builder
            .add_node(values.0)
            .add_node(values.1)
            .add_control_port(values.2)
            .add_control_port(values.3)
            .add_data_port(values.4)
            .add_data_port(values.5);
    }
    builder
        .add_control_edge(ControlEdge::new(
            control_edge_id("edge_task_return"),
            task_out,
            return_in,
        ))
        .add_data_binding(DataBinding::from_port(
            data_binding_id("bind_task_return"),
            task_value,
            return_value,
        ))
        .add_policy(Policy::new(
            policy_id("retry_task"),
            task,
            PolicyKind::Retry(RetryPolicy {
                max_attempts: retry_attempts,
                initial_backoff_ms: 10,
                max_backoff_ms: 100,
            }),
        ));
    builder
}

struct BranchOptions<'a> {
    order: &'a [&'a str],
    expression_version: &'a str,
    omit_phi_case: Option<&'a str>,
    swap_merge_inputs: bool,
    bypass_phi_with_case: Option<&'a str>,
}

fn branch_builder(options: BranchOptions<'_>) -> PlanBuilder {
    let root = scope_id("root_scope");
    let branch = node_id("branch_node");
    let merge = node_id("merge_node");
    let ret = node_id("return_node");
    let merge_out = control_port_id("merge_out");
    let return_in = control_port_id("return_in");
    let merge_value = data_port_id("merge_value");
    let return_value = data_port_id("return_value");
    let branch_a = data_port_id("branch_a");
    let branch_b = data_port_id("branch_b");

    let mut input_properties = BTreeMap::new();
    input_properties.insert(
        "a".to_owned(),
        PlanProperty::new(PlanType::Boolean, true).unwrap(),
    );
    input_properties.insert(
        "b".to_owned(),
        PlanProperty::new(PlanType::Boolean, true).unwrap(),
    );
    let input_type = PlanType::Object {
        properties: input_properties,
        additional_properties: None,
    };
    let mut builder = PlanBuilder::new(metadata(
        "branch_node",
        input_type,
        PlanType::String,
        AuthorFormat::Programmatic,
        "branch_revision",
        "compiler-1",
    ));
    builder.add_scope(ScopeMetadata::root(root.clone()));

    let mut cases = Vec::new();
    for name in options.order {
        let output = control_port_id(&format!("branch_{name}_out"));
        let case = if *name == "else" {
            BranchCase::otherwise(case_id("else"), output.clone())
        } else {
            let dependency = if *name == "a" {
                branch_a.clone()
            } else {
                branch_b.clone()
            };
            BranchCase::when(
                case_id(name),
                PureExpression::new(
                    ExpressionLanguage::Cel,
                    version(options.expression_version),
                    *name,
                    PlanType::Boolean,
                )
                .with_dependency(*name, dependency),
                output.clone(),
            )
        };
        cases.push(case);
        builder.add_control_port(ControlPort::new(
            output,
            branch.clone(),
            port_name(name),
            PortDirection::Output,
        ));
    }
    builder.add_node(Node::new(
        branch.clone(),
        root.clone(),
        NodeKind::Branch(BranchDescriptor { cases }),
    ));
    for (name, port) in [("a", branch_a.clone()), ("b", branch_b.clone())] {
        builder
            .add_data_port(DataPort::new(
                port.clone(),
                branch.clone(),
                port_name(name),
                PortDirection::Input,
                PlanType::Boolean,
                true,
            ))
            .add_data_binding(DataBinding::new(
                data_binding_id(&format!("bind_input_{name}")),
                ValueSource::RunInput {
                    path: vec![name.to_owned()],
                },
                port,
            ));
    }

    let mut merge_arms = BTreeMap::new();
    let mut phi_sources = BTreeMap::new();
    for name in ["a", "b", "else"] {
        let task = node_id(&format!("task_{name}"));
        let task_in = control_port_id(&format!("task_{name}_in"));
        let task_out = control_port_id(&format!("task_{name}_out"));
        let task_value = data_port_id(&format!("task_{name}_value"));
        let branch_out = control_port_id(&format!("branch_{name}_out"));
        let merge_input_name = if options.swap_merge_inputs {
            match name {
                "a" => "b",
                "b" => "a",
                other => other,
            }
        } else {
            name
        };
        let merge_in = control_port_id(&format!("merge_{merge_input_name}_in"));
        builder
            .add_node(Node::new(
                task.clone(),
                root.clone(),
                NodeKind::ActionTask(leaf(&format!("fixture.{name}"))),
            ))
            .add_control_port(ControlPort::new(
                task_in.clone(),
                task.clone(),
                port_name("in"),
                PortDirection::Input,
            ))
            .add_control_port(ControlPort::new(
                task_out.clone(),
                task.clone(),
                port_name("out"),
                PortDirection::Output,
            ))
            .add_data_port(DataPort::new(
                task_value.clone(),
                task,
                port_name("value"),
                PortDirection::Output,
                PlanType::String,
                false,
            ))
            .add_control_edge(ControlEdge::new(
                control_edge_id(&format!("edge_branch_{name}")),
                branch_out,
                task_in,
            ))
            .add_control_edge(ControlEdge::new(
                control_edge_id(&format!("edge_{name}_merge")),
                task_out,
                merge_in.clone(),
            ));
        let canonical_merge_in = control_port_id(&format!("merge_{name}_in"));
        merge_arms.insert(case_id(name), canonical_merge_in);
        if options.omit_phi_case != Some(name) {
            phi_sources.insert(
                case_id(name),
                ValueSource::Port {
                    port_id: task_value,
                },
            );
        }
    }

    builder.add_node(Node::new(
        merge.clone(),
        root.clone(),
        NodeKind::Merge(MergeDescriptor {
            branch_node_id: branch,
            arms: merge_arms,
            output_port: merge_out.clone(),
        }),
    ));
    for name in ["a", "b", "else"] {
        builder.add_control_port(ControlPort::new(
            control_port_id(&format!("merge_{name}_in")),
            merge.clone(),
            port_name(name),
            PortDirection::Input,
        ));
    }
    builder
        .add_control_port(ControlPort::new(
            merge_out.clone(),
            merge.clone(),
            port_name("out"),
            PortDirection::Output,
        ))
        .add_data_port(DataPort::new(
            merge_value.clone(),
            merge.clone(),
            port_name("result"),
            PortDirection::Output,
            PlanType::String,
            false,
        ))
        .add_phi_binding(PhiBinding::new(
            phi_binding_id("phi_result"),
            merge,
            merge_value.clone(),
            phi_sources,
        ))
        .add_node(Node::new(
            ret.clone(),
            root,
            NodeKind::Return(ReturnDescriptor {
                value_input: return_value.clone(),
            }),
        ))
        .add_control_port(ControlPort::new(
            return_in.clone(),
            ret.clone(),
            port_name("in"),
            PortDirection::Input,
        ))
        .add_data_port(DataPort::new(
            return_value.clone(),
            ret,
            port_name("value"),
            PortDirection::Input,
            PlanType::String,
            true,
        ))
        .add_control_edge(ControlEdge::new(
            control_edge_id("edge_merge_return"),
            merge_out,
            return_in,
        ));
    let return_source = options.bypass_phi_with_case.map_or(
        ValueSource::Port {
            port_id: merge_value,
        },
        |name| ValueSource::Port {
            port_id: data_port_id(&format!("task_{name}_value")),
        },
    );
    builder.add_data_binding(DataBinding::new(
        data_binding_id("bind_return"),
        return_source,
        return_value,
    ));
    builder
}

fn valid_branch_plan() -> Plan {
    branch_builder(BranchOptions {
        order: &["a", "b", "else"],
        expression_version: CEL_EXPRESSION_ENGINE_VERSION,
        omit_phi_case: None,
        swap_merge_inputs: false,
        bypass_phi_with_case: None,
    })
    .build()
    .unwrap()
}

fn fork_builder(
    leg_order: &[&str],
    fork_mode: PlanJoinMode,
    join_mode: PlanJoinMode,
    omit_join_leg: Option<&str>,
    with_collect: bool,
) -> PlanBuilder {
    let root = scope_id("root_scope");
    let fork = node_id("fork_node");
    let join = node_id("join_node");
    let ret = node_id("return_node");
    let join_out = control_port_id("join_out");
    let return_in = control_port_id("return_in");
    let return_value = data_port_id("return_value");
    let mut builder = PlanBuilder::new(metadata(
        "fork_node",
        PlanType::Null,
        PlanType::String,
        AuthorFormat::Programmatic,
        "fork_revision",
        "compiler-1",
    ));
    builder.add_scope(ScopeMetadata::root(root.clone()));

    let mut legs = Vec::new();
    let mut join_legs = BTreeMap::new();
    for name in leg_order {
        let leg = LegId::new(*name).unwrap();
        let leg_scope = scope_id(&format!("scope_{name}"));
        let fork_out = control_port_id(&format!("fork_{name}_out"));
        let yield_port = data_port_id(&format!("task_{name}_yield"));
        legs.push(ForkLegDescriptor {
            leg_id: leg.clone(),
            scope_id: leg_scope.clone(),
            output_port: fork_out.clone(),
            yield_port: yield_port.clone(),
        });
        builder
            .add_scope(ScopeMetadata::child(
                leg_scope.clone(),
                root.clone(),
                fork.clone(),
                ScopeKind::ForkLeg {
                    fork_node_id: fork.clone(),
                    leg_id: leg.clone(),
                },
                BTreeSet::new(),
            ))
            .add_control_port(ControlPort::new(
                fork_out.clone(),
                fork.clone(),
                port_name(name),
                PortDirection::Output,
            ));
        let task = node_id(&format!("task_{name}"));
        let task_in = control_port_id(&format!("task_{name}_in"));
        let task_out = control_port_id(&format!("task_{name}_out"));
        let join_in = control_port_id(&format!("join_{name}_in"));
        builder
            .add_node(Node::new(
                task.clone(),
                leg_scope,
                NodeKind::ActionTask(leaf(&format!("fixture.{name}"))),
            ))
            .add_control_port(ControlPort::new(
                task_in.clone(),
                task.clone(),
                port_name("in"),
                PortDirection::Input,
            ))
            .add_control_port(ControlPort::new(
                task_out.clone(),
                task.clone(),
                port_name("out"),
                PortDirection::Output,
            ))
            .add_data_port(DataPort::new(
                yield_port,
                task,
                port_name("yield"),
                PortDirection::Output,
                PlanType::String,
                false,
            ))
            .add_control_port(ControlPort::new(
                join_in.clone(),
                join.clone(),
                port_name(name),
                PortDirection::Input,
            ))
            .add_control_edge(ControlEdge::new(
                control_edge_id(&format!("edge_fork_{name}")),
                fork_out,
                task_in,
            ))
            .add_control_edge(ControlEdge::new(
                control_edge_id(&format!("edge_{name}_join")),
                task_out,
                join_in.clone(),
            ));
        if omit_join_leg != Some(*name) {
            join_legs.insert(leg, join_in);
        }
    }
    builder
        .add_node(Node::new(
            fork.clone(),
            root.clone(),
            NodeKind::Fork(ForkDescriptor {
                legs,
                join_mode: fork_mode,
            }),
        ))
        .add_node(Node::new(
            join.clone(),
            root.clone(),
            NodeKind::Join(JoinDescriptor {
                fork_node_id: fork,
                mode: join_mode,
                legs: join_legs,
                output_port: join_out.clone(),
            }),
        ))
        .add_control_port(ControlPort::new(
            join_out.clone(),
            join,
            port_name("out"),
            PortDirection::Output,
        ))
        .add_node(Node::new(
            ret.clone(),
            root,
            NodeKind::Return(ReturnDescriptor {
                value_input: return_value.clone(),
            }),
        ))
        .add_control_port(ControlPort::new(
            return_in.clone(),
            ret.clone(),
            port_name("in"),
            PortDirection::Input,
        ))
        .add_data_port(DataPort::new(
            return_value.clone(),
            ret,
            port_name("value"),
            PortDirection::Input,
            PlanType::String,
            true,
        ));
    if with_collect {
        let collect = node_id("collect_node");
        let collect_in = control_port_id("collect_in");
        let collect_out = control_port_id("collect_out");
        let collect_value = data_port_id("collect_value");
        builder
            .add_node(Node::new(
                collect.clone(),
                scope_id("root_scope"),
                NodeKind::Collect(CollectDescriptor {
                    source: CollectSource::StaticFork {
                        fork_node_id: node_id("fork_node"),
                        join_node_id: node_id("join_node"),
                        mode: fork_mode,
                    },
                    output_port: collect_value.clone(),
                }),
            ))
            .add_control_port(ControlPort::new(
                collect_in.clone(),
                collect.clone(),
                port_name("in"),
                PortDirection::Input,
            ))
            .add_control_port(ControlPort::new(
                collect_out.clone(),
                collect.clone(),
                port_name("out"),
                PortDirection::Output,
            ))
            .add_data_port(DataPort::new(
                collect_value,
                collect,
                port_name("value"),
                PortDirection::Output,
                static_fork_collect_type(leg_order, fork_mode),
                false,
            ))
            .add_control_edge(ControlEdge::new(
                control_edge_id("edge_join_collect"),
                join_out,
                collect_in,
            ))
            .add_control_edge(ControlEdge::new(
                control_edge_id("edge_collect_return"),
                collect_out,
                return_in,
            ));
    } else {
        builder.add_control_edge(ControlEdge::new(
            control_edge_id("edge_join_return"),
            join_out,
            return_in,
        ));
    }
    builder.add_data_binding(DataBinding::new(
        data_binding_id("bind_return"),
        ValueSource::Literal {
            value: json!("done"),
        },
        return_value,
    ));
    builder
}

fn static_fork_collect_type(legs: &[&str], mode: PlanJoinMode) -> PlanType {
    let fields = legs
        .iter()
        .map(|leg| {
            let value_type = match mode {
                PlanJoinMode::AllSuccess => PlanType::String,
                PlanJoinMode::AllSettled => PlanType::union([
                    PlanType::Object {
                        properties: BTreeMap::from([
                            (
                                "kind".to_owned(),
                                PlanProperty::new(PlanType::literal(json!("ok")).unwrap(), true)
                                    .unwrap(),
                            ),
                            (
                                "value".to_owned(),
                                PlanProperty::new(PlanType::String, true).unwrap(),
                            ),
                        ]),
                        additional_properties: None,
                    },
                    PlanType::Object {
                        properties: BTreeMap::from([
                            (
                                "error".to_owned(),
                                PlanProperty::new(safe_error_type(), true).unwrap(),
                            ),
                            (
                                "kind".to_owned(),
                                PlanProperty::new(PlanType::literal(json!("error")).unwrap(), true)
                                    .unwrap(),
                            ),
                        ]),
                        additional_properties: None,
                    },
                ])
                .unwrap(),
            };
            (
                (*leg).to_owned(),
                PlanProperty::new(value_type, true).unwrap(),
            )
        })
        .collect();
    PlanType::Object {
        properties: fields,
        additional_properties: None,
    }
}

fn loop_builder(with_budget: bool, flavor: LoopFlavor) -> PlanBuilder {
    let root = scope_id("root_scope");
    let body_scope = scope_id("loop_body_scope");
    let loop_node = node_id("loop_node");
    let body = node_id("body_node");
    let ret = node_id("return_node");
    let loop_body = control_port_id("loop_body_out");
    let loop_done = control_port_id("loop_completed_out");
    let loop_continue = control_port_id("loop_continue_in");
    let body_in = control_port_id("body_in");
    let body_out = control_port_id("body_out");
    let return_in = control_port_id("return_in");
    let return_value = data_port_id("return_value");
    let mut builder = PlanBuilder::new(metadata(
        "loop_node",
        PlanType::Null,
        PlanType::String,
        AuthorFormat::Programmatic,
        "loop_revision",
        "compiler-1",
    ));
    builder
        .add_scope(ScopeMetadata::root(root.clone()))
        .add_scope(ScopeMetadata::child(
            body_scope.clone(),
            root.clone(),
            loop_node.clone(),
            ScopeKind::LoopBody {
                loop_node_id: loop_node.clone(),
            },
            BTreeSet::new(),
        ))
        .add_node(Node::new(
            loop_node.clone(),
            root.clone(),
            NodeKind::Loop(LoopDescriptor {
                flavor,
                continue_input: loop_continue.clone(),
                body_output: loop_body.clone(),
                completed_output: loop_done.clone(),
                exit_condition: PureExpression::new(
                    ExpressionLanguage::Cel,
                    version(CEL_EXPRESSION_ENGINE_VERSION),
                    "false",
                    PlanType::Boolean,
                ),
                max_iterations: with_budget.then_some(3),
                deadline_ms: None,
            }),
        ))
        .add_control_port(ControlPort::new(
            loop_continue.clone(),
            loop_node.clone(),
            port_name("continue"),
            PortDirection::Input,
        ))
        .add_control_port(ControlPort::new(
            loop_body.clone(),
            loop_node.clone(),
            port_name("body"),
            PortDirection::Output,
        ))
        .add_control_port(ControlPort::new(
            loop_done.clone(),
            loop_node,
            port_name("completed"),
            PortDirection::Output,
        ))
        .add_node(Node::new(
            body.clone(),
            body_scope,
            NodeKind::ActionTask(leaf("fixture.loop_body")),
        ))
        .add_control_port(ControlPort::new(
            body_in.clone(),
            body.clone(),
            port_name("in"),
            PortDirection::Input,
        ))
        .add_control_port(ControlPort::new(
            body_out.clone(),
            body,
            port_name("out"),
            PortDirection::Output,
        ))
        .add_node(Node::new(
            ret.clone(),
            root,
            NodeKind::Return(ReturnDescriptor {
                value_input: return_value.clone(),
            }),
        ))
        .add_control_port(ControlPort::new(
            return_in.clone(),
            ret.clone(),
            port_name("in"),
            PortDirection::Input,
        ))
        .add_data_port(DataPort::new(
            return_value.clone(),
            ret,
            port_name("value"),
            PortDirection::Input,
            PlanType::String,
            true,
        ))
        .add_control_edge(ControlEdge::new(
            control_edge_id("edge_loop_body"),
            loop_body,
            body_in,
        ))
        .add_control_edge(ControlEdge::new(
            control_edge_id("edge_body_continue"),
            body_out,
            loop_continue,
        ))
        .add_control_edge(ControlEdge::new(
            control_edge_id("edge_loop_return"),
            loop_done,
            return_in,
        ))
        .add_data_binding(DataBinding::new(
            data_binding_id("bind_return"),
            ValueSource::Literal {
                value: json!("done"),
            },
            return_value,
        ));
    builder
}

fn map_collect_builder(collect_type: PlanType) -> PlanBuilder {
    let root = scope_id("root_scope");
    let body_scope = scope_id("map_body_scope");
    let map = node_id("map_node");
    let body = node_id("map_body_node");
    let collect = node_id("collect_node");
    let ret = node_id("return_node");
    let map_body = control_port_id("map_body_out");
    let body_in = control_port_id("map_task_in");
    let body_out = control_port_id("map_task_out");
    let collect_in = control_port_id("collect_in");
    let collect_out = control_port_id("collect_out");
    let return_in = control_port_id("return_in");
    let item_port = data_port_id("map_item");
    let yield_port = data_port_id("map_yield");
    let collect_value = data_port_id("collect_value");
    let return_value = data_port_id("return_value");
    let mut builder = PlanBuilder::new(metadata(
        "map_node",
        PlanType::Null,
        PlanType::String,
        AuthorFormat::Programmatic,
        "map_revision",
        "compiler-1",
    ));
    builder
        .add_scope(ScopeMetadata::root(root.clone()))
        .add_scope(ScopeMetadata::child(
            body_scope.clone(),
            root.clone(),
            map.clone(),
            ScopeKind::MapBody {
                map_node_id: map.clone(),
            },
            BTreeSet::from([item_port.clone()]),
        ))
        .add_node(Node::new(
            map.clone(),
            root.clone(),
            NodeKind::Map(MapDescriptor {
                items: PureExpression::new(
                    ExpressionLanguage::Literal,
                    version(LITERAL_EXPRESSION_ENGINE_VERSION),
                    r#"["x"]"#,
                    PlanType::Array {
                        items: Box::new(PlanType::String),
                        min_items: 1,
                    },
                ),
                body_scope_id: body_scope.clone(),
                item_port: item_port.clone(),
                yield_port: yield_port.clone(),
                max_concurrency: Some(2),
            }),
        ))
        .add_control_port(ControlPort::new(
            map_body.clone(),
            map.clone(),
            port_name("body"),
            PortDirection::Output,
        ))
        .add_data_port(DataPort::new(
            item_port,
            map.clone(),
            port_name("item"),
            PortDirection::Output,
            PlanType::String,
            false,
        ))
        .add_node(Node::new(
            body.clone(),
            body_scope,
            NodeKind::ActionTask(leaf("fixture.map_body")),
        ))
        .add_control_port(ControlPort::new(
            body_in.clone(),
            body.clone(),
            port_name("in"),
            PortDirection::Input,
        ))
        .add_control_port(ControlPort::new(
            body_out.clone(),
            body.clone(),
            port_name("out"),
            PortDirection::Output,
        ))
        .add_data_port(DataPort::new(
            yield_port,
            body,
            port_name("yield"),
            PortDirection::Output,
            PlanType::String,
            false,
        ))
        .add_node(Node::new(
            collect.clone(),
            root.clone(),
            NodeKind::Collect(CollectDescriptor {
                source: CollectSource::Map {
                    map_node_id: map.clone(),
                },
                output_port: collect_value.clone(),
            }),
        ))
        .add_control_port(ControlPort::new(
            collect_in.clone(),
            collect.clone(),
            port_name("in"),
            PortDirection::Input,
        ))
        .add_control_port(ControlPort::new(
            collect_out.clone(),
            collect.clone(),
            port_name("out"),
            PortDirection::Output,
        ))
        .add_data_port(DataPort::new(
            collect_value,
            collect,
            port_name("value"),
            PortDirection::Output,
            collect_type,
            false,
        ))
        .add_node(Node::new(
            ret.clone(),
            root,
            NodeKind::Return(ReturnDescriptor {
                value_input: return_value.clone(),
            }),
        ))
        .add_control_port(ControlPort::new(
            return_in.clone(),
            ret.clone(),
            port_name("in"),
            PortDirection::Input,
        ))
        .add_data_port(DataPort::new(
            return_value.clone(),
            ret,
            port_name("value"),
            PortDirection::Input,
            PlanType::String,
            true,
        ))
        .add_control_edge(ControlEdge::new(
            control_edge_id("edge_map_body"),
            map_body,
            body_in,
        ))
        .add_control_edge(ControlEdge::new(
            control_edge_id("edge_body_collect"),
            body_out,
            collect_in,
        ))
        .add_control_edge(ControlEdge::new(
            control_edge_id("edge_collect_return"),
            collect_out,
            return_in,
        ))
        .add_data_binding(DataBinding::new(
            data_binding_id("bind_return"),
            ValueSource::Literal {
                value: json!("done"),
            },
            return_value,
        ));
    builder
}

#[test]
fn minimal_plan_is_versioned_immutable_and_serde_is_fail_closed() {
    let plan = minimal_return_plan();
    assert_eq!(plan.metadata().wire_version(), PLAN_WIRE_VERSION);
    assert_eq!(plan.metadata().dsl_version(), DSL_MAJOR_VERSION);
    assert!(plan.verify().is_ok());

    let value = serde_json::to_value(&plan).unwrap();
    assert_eq!(serde_json::from_value::<Plan>(value.clone()).unwrap(), plan);
    let encoded = serde_json::to_vec(&plan).unwrap();
    assert_eq!(Plan::decode_json(&encoded).unwrap(), plan);
    let encoded = String::from_utf8(encoded).unwrap();
    let duplicate = encoded.replacen(
        r#""compiler_version":"compiler-1""#,
        r#""compiler_version":"compiler-1","compiler_version":"compiler-1""#,
        1,
    );
    let error = Plan::decode_json(duplicate.as_bytes()).unwrap_err();
    assert_eq!(error.code(), PLAN_WIRE_INVALID);
    assert!(error.message().contains("duplicate JSON object member"));
    let configured =
        serde_json::to_string(&linear_builder(false, 3, "alpha").build().unwrap()).unwrap();
    let duplicate_map_key = configured.replacen(
        r#""model":{"type":"string","value":"alpha"}"#,
        r#""model":{"type":"string","value":"alpha"},"model":{"type":"string","value":"alpha"}"#,
        1,
    );
    assert!(Plan::decode_json(duplicate_map_key.as_bytes())
        .unwrap_err()
        .message()
        .contains("duplicate JSON object member 'model'"));

    let mut unknown = value.clone();
    unknown
        .as_object_mut()
        .unwrap()
        .insert("future_field".into(), Value::Bool(true));
    assert!(serde_json::from_value::<Plan>(unknown).is_err());

    let mut tampered = value.clone();
    tampered["metadata"]["output_type"] = json!({"type": "number"});
    assert!(serde_json::from_value::<Plan>(tampered).is_err());
    assert!(ControlPortId::new("bad port").is_err());
    assert!(serde_json::from_value::<DataPortId>(json!("bad/port")).is_err());
}

#[test]
fn canonical_projection_has_a_golden_hash_and_normalizes_unordered_collections() {
    let first = linear_builder(false, 3, "alpha").build().unwrap();
    let second = linear_builder(true, 3, "alpha").build().unwrap();
    assert_eq!(first.semantic_hash(), second.semantic_hash());
    assert_eq!(
        first.canonical_semantic_bytes().unwrap(),
        second.canonical_semantic_bytes().unwrap()
    );
    assert_eq!(
        serde_json::to_value(&first).unwrap(),
        serde_json::to_value(&second).unwrap()
    );
    assert_eq!(
        first.semantic_hash().as_str(),
        "sha256:ed000952377a93d69a3070c5efb7fd3361eb3128229bf5fb9cb996e493122041"
    );
}

#[test]
fn provenance_and_source_map_are_excluded_but_semantics_enter_hash() {
    let mut source_map = SourceMap::new();
    source_map.insert_node(
        node_id("return_node"),
        SourceSpan::new(
            SourceDocumentId::new("workflow.yaml").unwrap(),
            SourcePosition::new(0, 1, 1),
            SourcePosition::new(10, 1, 11),
        ),
    );
    let structured = minimal_return_plan_with(
        AuthorFormat::Structured,
        "revision_a",
        "compiler-a",
        source_map,
        None,
    );
    let graph = minimal_return_plan_with(
        AuthorFormat::Graph,
        "revision_b",
        "compiler-b",
        SourceMap::new(),
        None,
    );
    assert_eq!(structured.semantic_hash(), graph.semantic_hash());

    let expression_v1 = minimal_return_plan_with(
        AuthorFormat::Programmatic,
        "revision_a",
        "compiler-a",
        SourceMap::new(),
        Some(CEL_EXPRESSION_ENGINE_VERSION),
    );
    let mut unsupported_engine = serde_json::to_value(&expression_v1).unwrap();
    unsupported_engine["data_bindings"][0]["source"]["expression"]["engine_version"] =
        json!("cel-999");
    let error = serde_json::from_value::<Plan>(unsupported_engine).unwrap_err();
    assert!(error.to_string().contains(PLAN_VERSION_UNSUPPORTED));

    let policy_v1 = linear_builder(false, 2, "alpha").build().unwrap();
    let policy_v2 = linear_builder(false, 3, "alpha").build().unwrap();
    let descriptor_v2 = linear_builder(false, 2, "beta").build().unwrap();
    let descriptor_engine_v2 = linear_builder_with_descriptor_version(false, 2, "alpha", "2")
        .build()
        .unwrap();
    assert_ne!(policy_v1.semantic_hash(), policy_v2.semantic_hash());
    assert_ne!(policy_v1.semantic_hash(), descriptor_v2.semantic_hash());
    assert_ne!(
        policy_v1.semantic_hash(),
        descriptor_engine_v2.semantic_hash()
    );
}

#[test]
fn verified_plan_rejects_budget_policy_without_a_runtime_contract() {
    let mut builder = linear_builder(false, 2, "alpha");
    builder.add_policy(Policy::new(
        policy_id("unsupported_budget"),
        node_id("task_node"),
        PolicyKind::Budget(BudgetPolicy {
            max_tokens: Some(1_000),
            max_cost_microunits: None,
        }),
    ));

    let error = builder.build().unwrap_err();
    assert_eq!(error.code(), PLAN_POLICY_INVALID);
    assert!(error.message().contains("kind 'budget' is not executable"));
    assert!(error
        .message()
        .contains("budget enforcement has no durable runtime contract"));
    assert_eq!(
        error.target().and_then(PlanDiagnosticTarget::node_id),
        Some(&node_id("task_node"))
    );
}

#[test]
fn expression_engines_are_pinned_canonical_and_type_checked_fail_closed() {
    let expression_plan = minimal_return_plan_with(
        AuthorFormat::Programmatic,
        "expression_revision",
        "compiler-a",
        SourceMap::new(),
        Some(CEL_EXPRESSION_ENGINE_VERSION),
    );
    let mut noncanonical_literal = serde_json::to_value(&expression_plan).unwrap();
    let expression_wire = &mut noncanonical_literal["data_bindings"][0]["source"]["expression"];
    expression_wire["language"] = json!("literal");
    expression_wire["engine_version"] = json!(LITERAL_EXPRESSION_ENGINE_VERSION);
    expression_wire["source"] = json!("\"ok\" ");
    assert!(serde_json::from_value::<Plan>(noncanonical_literal)
        .unwrap_err()
        .to_string()
        .contains("RFC 8785"));

    let mut untyped_operator = serde_json::to_value(valid_branch_plan()).unwrap();
    let branch = untyped_operator["nodes"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|node| node["id"] == json!("branch_node"))
        .unwrap();
    branch["kind"]["descriptor"]["cases"][0]["condition"]["source"] = json!("size(a) > 0");
    assert!(serde_json::from_value::<Plan>(untyped_operator)
        .unwrap_err()
        .to_string()
        .contains("fixed typed profile"));

    let mut unsupported_language = serde_json::to_value(expression_plan).unwrap();
    unsupported_language["data_bindings"][0]["source"]["expression"]["language"] =
        json!("template");
    assert!(serde_json::from_value::<Plan>(unsupported_language)
        .unwrap_err()
        .to_string()
        .contains(PLAN_VERSION_UNSUPPORTED));
}

#[test]
fn ordered_branch_cases_and_fork_legs_enter_hash() {
    let branch_ab = valid_branch_plan();
    let branch_ba = branch_builder(BranchOptions {
        order: &["b", "a", "else"],
        expression_version: CEL_EXPRESSION_ENGINE_VERSION,
        omit_phi_case: None,
        swap_merge_inputs: false,
        bypass_phi_with_case: None,
    })
    .build()
    .unwrap();
    assert_ne!(branch_ab.semantic_hash(), branch_ba.semantic_hash());

    let fork_lr = fork_builder(
        &["left", "right"],
        PlanJoinMode::AllSuccess,
        PlanJoinMode::AllSuccess,
        None,
        false,
    )
    .build()
    .unwrap();
    let fork_rl = fork_builder(
        &["right", "left"],
        PlanJoinMode::AllSuccess,
        PlanJoinMode::AllSuccess,
        None,
        false,
    )
    .build()
    .unwrap();
    assert_ne!(fork_lr.semantic_hash(), fork_rl.semantic_hash());
}

#[test]
fn verifier_rejects_bad_merge_phi_and_non_dominating_branch_value() {
    let wrong_correlation = branch_builder(BranchOptions {
        order: &["a", "b", "else"],
        expression_version: CEL_EXPRESSION_ENGINE_VERSION,
        omit_phi_case: None,
        swap_merge_inputs: true,
        bypass_phi_with_case: None,
    })
    .build()
    .unwrap_err();
    assert_eq!(wrong_correlation.code(), PLAN_MERGE_INVALID);

    let incomplete_phi = branch_builder(BranchOptions {
        order: &["a", "b", "else"],
        expression_version: CEL_EXPRESSION_ENGINE_VERSION,
        omit_phi_case: Some("b"),
        swap_merge_inputs: false,
        bypass_phi_with_case: None,
    })
    .build()
    .unwrap_err();
    assert_eq!(incomplete_phi.code(), PLAN_PHI_INVALID);

    let non_dominating = branch_builder(BranchOptions {
        order: &["a", "b", "else"],
        expression_version: CEL_EXPRESSION_ENGINE_VERSION,
        omit_phi_case: None,
        swap_merge_inputs: false,
        bypass_phi_with_case: Some("a"),
    })
    .build()
    .unwrap_err();
    assert_eq!(non_dominating.code(), PLAN_DOMINANCE_INVALID);
}

#[test]
fn verifier_rejects_fork_join_member_and_mode_mismatches() {
    assert!(fork_builder(
        &["left", "right"],
        PlanJoinMode::AllSuccess,
        PlanJoinMode::AllSuccess,
        None,
        true,
    )
    .build()
    .is_ok());
    let settled = fork_builder(
        &["left", "right"],
        PlanJoinMode::AllSettled,
        PlanJoinMode::AllSettled,
        None,
        true,
    )
    .build()
    .unwrap();
    let collect = settled
        .nodes()
        .iter()
        .find(|node| node.id().as_str() == "collect_node")
        .unwrap();
    let NodeKind::Collect(descriptor) = collect.kind() else {
        panic!("fixture Collect node changed kind")
    };
    let output = settled
        .data_ports()
        .iter()
        .find(|port| port.id() == &descriptor.output_port)
        .unwrap();
    assert_eq!(
        output.value_type(),
        &static_fork_collect_type(&["left", "right"], PlanJoinMode::AllSettled)
    );

    let missing = fork_builder(
        &["left", "right"],
        PlanJoinMode::AllSuccess,
        PlanJoinMode::AllSuccess,
        Some("right"),
        false,
    )
    .build()
    .unwrap_err();
    assert_eq!(missing.code(), PLAN_JOIN_INVALID);

    let mode = fork_builder(
        &["left", "right"],
        PlanJoinMode::AllSuccess,
        PlanJoinMode::AllSettled,
        None,
        false,
    )
    .build()
    .unwrap_err();
    assert_eq!(mode.code(), PLAN_JOIN_INVALID);

    let root = scope_id("root_scope");
    let fork = node_id("fork_node");
    let mut zero = PlanBuilder::new(metadata(
        "fork_node",
        PlanType::Null,
        PlanType::String,
        AuthorFormat::Programmatic,
        "zero_fork",
        "compiler-1",
    ));
    zero.add_scope(ScopeMetadata::root(root.clone()))
        .add_node(Node::new(
            fork,
            root,
            NodeKind::Fork(ForkDescriptor {
                legs: vec![],
                join_mode: PlanJoinMode::AllSuccess,
            }),
        ));
    assert_eq!(zero.build().unwrap_err().code(), PLAN_FORK_INVALID);
}

#[test]
fn static_fork_and_map_collect_have_closed_typed_results() {
    let map_type = PlanType::Array {
        items: Box::new(PlanType::String),
        min_items: 0,
    };
    assert!(map_collect_builder(map_type).build().is_ok());
    assert_eq!(
        map_collect_builder(PlanType::Array {
            items: Box::new(PlanType::Number),
            min_items: 0,
        })
        .build()
        .unwrap_err()
        .code(),
        PLAN_TYPE_MISMATCH
    );

    let mut missing_yield = serde_json::to_value(
        fork_builder(
            &["left", "right"],
            PlanJoinMode::AllSuccess,
            PlanJoinMode::AllSuccess,
            None,
            true,
        )
        .build()
        .unwrap(),
    )
    .unwrap();
    missing_yield["nodes"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|node| node["id"] == json!("fork_node"))
        .unwrap()["kind"]["descriptor"]["legs"][0]
        .as_object_mut()
        .unwrap()
        .remove("yield_port");
    assert!(serde_json::from_value::<Plan>(missing_yield).is_err());
}

#[test]
fn loop_is_the_only_control_cycle_and_requires_an_exit_budget() {
    assert!(loop_builder(true, LoopFlavor::Workflow).build().is_ok());
    assert_eq!(
        loop_builder(false, LoopFlavor::Workflow)
            .build()
            .unwrap_err()
            .code(),
        PLAN_LOOP_INVALID
    );

    let root = scope_id("root_scope");
    let a = node_id("node_a");
    let b = node_id("node_b");
    let a_in = control_port_id("a_in");
    let a_out = control_port_id("a_out");
    let b_in = control_port_id("b_in");
    let b_out = control_port_id("b_out");
    let mut arbitrary = PlanBuilder::new(metadata(
        "node_a",
        PlanType::Null,
        PlanType::Null,
        AuthorFormat::Programmatic,
        "cycle_revision",
        "compiler-1",
    ));
    arbitrary
        .add_scope(ScopeMetadata::root(root.clone()))
        .add_node(Node::new(
            a.clone(),
            root.clone(),
            NodeKind::ActionTask(leaf("fixture.a")),
        ))
        .add_node(Node::new(
            b.clone(),
            root,
            NodeKind::ActionTask(leaf("fixture.b")),
        ));
    for port in [
        ControlPort::new(
            a_in.clone(),
            a.clone(),
            port_name("in"),
            PortDirection::Input,
        ),
        ControlPort::new(a_out.clone(), a, port_name("out"), PortDirection::Output),
        ControlPort::new(
            b_in.clone(),
            b.clone(),
            port_name("in"),
            PortDirection::Input,
        ),
        ControlPort::new(b_out.clone(), b, port_name("out"), PortDirection::Output),
    ] {
        arbitrary.add_control_port(port);
    }
    arbitrary
        .add_control_edge(ControlEdge::new(control_edge_id("edge_ab"), a_out, b_in))
        .add_control_edge(ControlEdge::new(control_edge_id("edge_ba"), b_out, a_in));
    assert_eq!(arbitrary.build().unwrap_err().code(), PLAN_CONTROL_CYCLE);
}

#[test]
fn loop_flavor_is_closed_on_the_wire_and_part_of_plan_semantics() {
    let workflow = loop_builder(true, LoopFlavor::Workflow).build().unwrap();
    let agent = loop_builder(true, LoopFlavor::Agent).build().unwrap();
    assert_ne!(workflow.semantic_hash(), agent.semantic_hash());

    let mut wire = serde_json::to_value(&workflow).unwrap();
    let descriptor = wire["nodes"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|node| node["id"] == json!("loop_node"))
        .map(|node| &mut node["kind"]["descriptor"])
        .unwrap();
    assert_eq!(descriptor["flavor"], json!("workflow"));
    descriptor["flavor"] = json!("future_loop_flavor");
    let error = serde_json::from_value::<Plan>(wire).unwrap_err();
    assert!(error.to_string().contains("unknown variant"));
}

#[test]
fn cross_scope_values_require_explicit_capture_and_dominance() {
    fn builder(capture: bool) -> PlanBuilder {
        let root = scope_id("root_scope");
        let child = scope_id("child_scope");
        let producer = node_id("producer_node");
        let ret = node_id("return_node");
        let producer_out = control_port_id("producer_out");
        let return_in = control_port_id("return_in");
        let producer_value = data_port_id("producer_value");
        let return_value = data_port_id("return_value");
        let captures = if capture {
            BTreeSet::from([producer_value.clone()])
        } else {
            BTreeSet::new()
        };
        let mut builder = PlanBuilder::new(metadata(
            "producer_node",
            PlanType::Null,
            PlanType::String,
            AuthorFormat::Programmatic,
            "capture_revision",
            "compiler-1",
        ));
        builder
            .add_scope(ScopeMetadata::root(root.clone()))
            .add_scope(ScopeMetadata::child(
                child.clone(),
                root.clone(),
                producer.clone(),
                ScopeKind::Lexical,
                captures,
            ))
            .add_node(Node::new(
                producer.clone(),
                root,
                NodeKind::ActionTask(leaf("fixture.producer")),
            ))
            .add_node(Node::new(
                ret.clone(),
                child,
                NodeKind::Return(ReturnDescriptor {
                    value_input: return_value.clone(),
                }),
            ))
            .add_control_port(ControlPort::new(
                producer_out.clone(),
                producer.clone(),
                port_name("out"),
                PortDirection::Output,
            ))
            .add_control_port(ControlPort::new(
                return_in.clone(),
                ret.clone(),
                port_name("in"),
                PortDirection::Input,
            ))
            .add_data_port(DataPort::new(
                producer_value.clone(),
                producer,
                port_name("value"),
                PortDirection::Output,
                PlanType::String,
                false,
            ))
            .add_data_port(DataPort::new(
                return_value.clone(),
                ret,
                port_name("value"),
                PortDirection::Input,
                PlanType::String,
                true,
            ))
            .add_control_edge(ControlEdge::new(
                control_edge_id("edge_enter_child"),
                producer_out,
                return_in,
            ))
            .add_data_binding(DataBinding::from_port(
                data_binding_id("bind_capture"),
                producer_value,
                return_value,
            ));
        builder
    }
    assert!(builder(true).build().is_ok());
    assert_eq!(
        builder(false).build().unwrap_err().code(),
        PLAN_SCOPE_INVALID
    );
}

#[test]
fn ports_types_terminals_source_spans_and_secrets_are_checked_before_publish() {
    let root = scope_id("root_scope");
    let ret = node_id("return_node");
    let value = data_port_id("return_value");
    let mut wrong_type = PlanBuilder::new(metadata(
        "return_node",
        PlanType::Number,
        PlanType::String,
        AuthorFormat::Programmatic,
        "bad_type",
        "compiler-1",
    ));
    wrong_type
        .add_scope(ScopeMetadata::root(root.clone()))
        .add_node(Node::new(
            ret.clone(),
            root.clone(),
            NodeKind::Return(ReturnDescriptor {
                value_input: value.clone(),
            }),
        ))
        .add_data_port(DataPort::new(
            value.clone(),
            ret,
            port_name("value"),
            PortDirection::Input,
            PlanType::String,
            true,
        ))
        .add_data_binding(DataBinding::new(
            data_binding_id("bind_return"),
            ValueSource::RunInput { path: vec![] },
            value,
        ));
    assert_eq!(wrong_type.build().unwrap_err().code(), PLAN_TYPE_MISMATCH);

    let mut bad_span = SourceMap::new();
    bad_span.insert_node(
        node_id("return_node"),
        SourceSpan::new(
            SourceDocumentId::new("workflow.yaml").unwrap(),
            SourcePosition::new(10, 2, 5),
            SourcePosition::new(1, 1, 1),
        ),
    );
    let mut bad_span_wire = serde_json::to_value(minimal_return_plan()).unwrap();
    bad_span_wire["source_map"] = serde_json::to_value(bad_span).unwrap();
    let error = serde_json::from_value::<Plan>(bad_span_wire).unwrap_err();
    assert!(error.to_string().contains("SourceSpan"));

    let mut explicitly_public_config = BTreeMap::new();
    explicitly_public_config.insert(
        "api_key".to_owned(),
        DescriptorValue::String("business-field-not-a-secret".to_owned()),
    );
    let secret_ref = SecretRef::new("vault:service_key").unwrap();
    let ambiguous_leaf =
        LeafTaskDescriptor::new("fixture.secret", version("1"), explicitly_public_config)
            .with_secret("api_key", secret_ref.clone());
    let mut secret_wire =
        serde_json::to_value(linear_builder(false, 1, "safe").build().unwrap()).unwrap();
    let nodes = secret_wire["nodes"].as_array_mut().unwrap();
    let task = nodes
        .iter_mut()
        .find(|node| node["id"] == json!("task_node"))
        .unwrap();
    task["kind"] = serde_json::to_value(NodeKind::ToolTask(ambiguous_leaf)).unwrap();
    let error = serde_json::from_value::<Plan>(secret_wire).unwrap_err();
    assert!(error.to_string().contains("both public configuration"));

    let safe = LeafTaskDescriptor::new("fixture.safe", version("1"), BTreeMap::new())
        .with_secret("api_key", secret_ref);
    assert!(safe.secret_configuration.contains_key("api_key"));
}

#[test]
fn stable_generated_ids_use_length_frames_and_detect_collisions() {
    let parent = node_id("parent");
    let mut first = StableNodeIdGenerator::new();
    let id_ab_c = first.compiler_node_id(&parent, "ab", Some("c")).unwrap();
    let id_a_bc = first.compiler_node_id(&parent, "a", Some("bc")).unwrap();
    assert_ne!(id_ab_c, id_a_bc);

    let mut replay = StableNodeIdGenerator::new();
    assert_eq!(
        replay.compiler_node_id(&parent, "ab", Some("c")).unwrap(),
        id_ab_c
    );
    assert_eq!(
        replay
            .compiler_node_id(&parent, "ab", Some("c"))
            .unwrap_err()
            .code(),
        PLAN_STABLE_ID_COLLISION
    );

    let mut authored_collision = StableNodeIdGenerator::with_reserved([id_a_bc.clone()]).unwrap();
    assert_eq!(
        authored_collision
            .compiler_node_id(&parent, "a", Some("bc"))
            .unwrap_err()
            .code(),
        PLAN_STABLE_ID_COLLISION
    );
}

#[test]
fn structural_forgery_is_rejected_before_a_stale_hash_can_be_authoritative() {
    let base = serde_json::to_value(valid_branch_plan()).unwrap();

    let mut duplicate_node = base.clone();
    let duplicate = duplicate_node["nodes"].as_array().unwrap()[0].clone();
    duplicate_node["nodes"]
        .as_array_mut()
        .unwrap()
        .push(duplicate);
    assert!(serde_json::from_value::<Plan>(duplicate_node)
        .unwrap_err()
        .to_string()
        .contains("duplicate node"));

    let mut wrong_edge_domain = base.clone();
    wrong_edge_domain["control_edges"].as_array_mut().unwrap()[0]["from"] = json!("task_a_value");
    assert!(serde_json::from_value::<Plan>(wrong_edge_domain)
        .unwrap_err()
        .to_string()
        .contains("control port"));

    let mut wrong_direction = base.clone();
    let port = wrong_direction["control_ports"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|port| port["id"] == json!("branch_a_out"))
        .unwrap();
    port["direction"] = json!("input");
    assert!(serde_json::from_value::<Plan>(wrong_direction).is_err());

    let mut unnamed_case = base.clone();
    let port = unnamed_case["control_ports"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|port| port["id"] == json!("branch_a_out"))
        .unwrap();
    port["name"] = json!("wrong_name");
    assert!(serde_json::from_value::<Plan>(unnamed_case)
        .unwrap_err()
        .to_string()
        .contains("equally named"));

    let mut default_not_last = base;
    let branch = default_not_last["nodes"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|node| node["id"] == json!("branch_node"))
        .unwrap();
    let cases = branch["kind"]["descriptor"]["cases"]
        .as_array_mut()
        .unwrap();
    let default = cases.pop().unwrap();
    cases.insert(0, default);
    assert!(serde_json::from_value::<Plan>(default_not_last)
        .unwrap_err()
        .to_string()
        .contains("default/else"));
}

#[test]
fn reachability_policy_and_terminal_contracts_fail_at_build_time() {
    let mut unreachable_wire = serde_json::to_value(minimal_return_plan()).unwrap();
    unreachable_wire["nodes"].as_array_mut().unwrap().push(
        serde_json::to_value(Node::new(
            node_id("orphan_node"),
            scope_id("root_scope"),
            NodeKind::ActionTask(leaf("fixture.orphan")),
        ))
        .unwrap(),
    );
    assert!(serde_json::from_value::<Plan>(unreachable_wire)
        .unwrap_err()
        .to_string()
        .contains("unreachable"));

    assert_eq!(
        linear_builder(false, 0, "safe").build().unwrap_err().code(),
        PLAN_POLICY_INVALID
    );

    let mut wrong_return = serde_json::to_value(minimal_return_plan()).unwrap();
    wrong_return["metadata"]["output_type"] = json!({"type": "number"});
    let error = serde_json::from_value::<Plan>(wrong_return).unwrap_err();
    assert!(error.to_string().contains("Return"));

    let root = scope_id("root_scope");
    let raise = node_id("raise_node");
    let error_input = data_port_id("raise_error");
    let mut valid_raise = PlanBuilder::new(metadata(
        "raise_node",
        safe_error_type(),
        PlanType::Null,
        AuthorFormat::Programmatic,
        "raise_revision",
        "compiler-1",
    ));
    valid_raise
        .add_scope(ScopeMetadata::root(root))
        .add_node(Node::new(
            raise.clone(),
            scope_id("root_scope"),
            NodeKind::Raise(RaiseDescriptor {
                error_input: error_input.clone(),
            }),
        ))
        .add_data_port(DataPort::new(
            error_input.clone(),
            raise,
            port_name("error"),
            PortDirection::Input,
            safe_error_type(),
            true,
        ))
        .add_data_binding(DataBinding::new(
            data_binding_id("bind_raise"),
            ValueSource::RunInput { path: vec![] },
            error_input,
        ));
    let raise_plan = valid_raise.build().unwrap();
    assert!(raise_plan.verify().is_ok());
    let mut wrong_raise = serde_json::to_value(raise_plan).unwrap();
    wrong_raise["metadata"]["error_type"] = json!({"type": "number"});
    assert!(serde_json::from_value::<Plan>(wrong_raise)
        .unwrap_err()
        .to_string()
        .contains("workflow error contract"));
}

#[test]
fn source_map_references_every_semantic_element_by_validated_id() {
    let plan = linear_builder(false, 1, "safe").build().unwrap();
    let span = SourceSpan::new(
        SourceDocumentId::new("workflow.yaml").unwrap(),
        SourcePosition::new(0, 1, 1),
        SourcePosition::new(1, 1, 2),
    );
    let mut source_map = SourceMap::new();
    source_map.insert_node(node_id("task_node"), span.clone());
    source_map.insert_control_port(control_port_id("task_out"), span.clone());
    source_map.insert_data_port(data_port_id("task_value"), span.clone());
    source_map.insert_control_edge(control_edge_id("edge_task_return"), span.clone());
    source_map.insert_data_binding(data_binding_id("bind_task_return"), span.clone());
    source_map.insert_scope(scope_id("root_scope"), span.clone());
    source_map.insert_policy(policy_id("retry_task"), span);

    let mut wire = serde_json::to_value(plan).unwrap();
    wire["source_map"] = serde_json::to_value(source_map).unwrap();
    assert!(serde_json::from_value::<Plan>(wire).is_ok());

    let mut forged = serde_json::to_value(minimal_return_plan()).unwrap();
    forged["source_map"]["policies"] = json!({
        "missing_policy": {
            "source_id": "workflow.yaml",
            "start": {"offset": 0, "line": 1, "column": 1},
            "end": {"offset": 1, "line": 1, "column": 2}
        }
    });
    assert!(serde_json::from_value::<Plan>(forged)
        .unwrap_err()
        .to_string()
        .contains("missing policy"));
}

#[test]
fn run_input_paths_do_not_conflate_missing_optional_or_out_of_bounds_values() {
    fn return_builder(input_type: PlanType, path: Vec<String>) -> PlanBuilder {
        let root = scope_id("root_scope");
        let ret = node_id("return_node");
        let value = data_port_id("return_value");
        let mut builder = PlanBuilder::new(metadata(
            "return_node",
            input_type,
            PlanType::String,
            AuthorFormat::Programmatic,
            "input_path_revision",
            "compiler-1",
        ));
        builder
            .add_scope(ScopeMetadata::root(root.clone()))
            .add_node(Node::new(
                ret.clone(),
                root,
                NodeKind::Return(ReturnDescriptor {
                    value_input: value.clone(),
                }),
            ))
            .add_data_port(DataPort::new(
                value.clone(),
                ret,
                port_name("value"),
                PortDirection::Input,
                PlanType::String,
                true,
            ))
            .add_data_binding(DataBinding::new(
                data_binding_id("bind_return"),
                ValueSource::RunInput { path },
                value,
            ));
        builder
    }

    let optional = PlanType::Object {
        properties: BTreeMap::from([(
            "answer".to_owned(),
            PlanProperty::new(PlanType::String, false).unwrap(),
        )]),
        additional_properties: None,
    };
    assert_eq!(
        return_builder(optional, vec!["answer".to_owned()])
            .build()
            .unwrap_err()
            .code(),
        PLAN_TYPE_MISMATCH
    );

    let array = PlanType::Array {
        items: Box::new(PlanType::String),
        min_items: 1,
    };
    assert!(return_builder(array.clone(), vec!["0".to_owned()])
        .build()
        .is_ok());
    assert_eq!(
        return_builder(array, vec!["1".to_owned()])
            .build()
            .unwrap_err()
            .code(),
        PLAN_TYPE_MISMATCH
    );
}

#[test]
fn data_dependency_cycles_and_noncanonical_semantic_integers_are_rejected() {
    let mut cycle =
        serde_json::to_value(linear_builder(false, 1, "safe").build().unwrap()).unwrap();
    let input_a = data_port_id("task_input_a");
    let input_b = data_port_id("task_input_b");
    for port in [
        DataPort::new(
            input_a.clone(),
            node_id("task_node"),
            port_name("input_a"),
            PortDirection::Input,
            PlanType::String,
            true,
        ),
        DataPort::new(
            input_b.clone(),
            node_id("task_node"),
            port_name("input_b"),
            PortDirection::Input,
            PlanType::String,
            true,
        ),
    ] {
        cycle["data_ports"]
            .as_array_mut()
            .unwrap()
            .push(serde_json::to_value(port).unwrap());
    }
    for binding in [
        DataBinding::new(
            data_binding_id("bind_input_a"),
            ValueSource::Expression {
                expression: PureExpression::new(
                    ExpressionLanguage::Cel,
                    version(CEL_EXPRESSION_ENGINE_VERSION),
                    "b",
                    PlanType::String,
                )
                .with_dependency("b", input_b.clone()),
            },
            input_a.clone(),
        ),
        DataBinding::new(
            data_binding_id("bind_input_b"),
            ValueSource::Expression {
                expression: PureExpression::new(
                    ExpressionLanguage::Cel,
                    version(CEL_EXPRESSION_ENGINE_VERSION),
                    "a",
                    PlanType::String,
                )
                .with_dependency("a", input_a.clone()),
            },
            input_b,
        ),
    ] {
        cycle["data_bindings"]
            .as_array_mut()
            .unwrap()
            .push(serde_json::to_value(binding).unwrap());
    }
    assert!(serde_json::from_value::<Plan>(cycle)
        .unwrap_err()
        .to_string()
        .contains("dependency cycle"));

    let mut unsafe_integer =
        serde_json::to_value(linear_builder(false, 1, "safe").build().unwrap()).unwrap();
    let task = unsafe_integer["nodes"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|node| node["id"] == json!("task_node"))
        .unwrap();
    task["kind"]["descriptor"]["public_configuration"]["count"] = json!({
        "type": "integer",
        "value": 9_007_199_254_740_992_i64
    });
    assert!(serde_json::from_value::<Plan>(unsafe_integer)
        .unwrap_err()
        .to_string()
        .contains("safe-integer"));

    let mut unsafe_policy =
        serde_json::to_value(linear_builder(false, 1, "safe").build().unwrap()).unwrap();
    unsafe_policy["policies"][0]["kind"]["descriptor"]["max_backoff_ms"] =
        json!(9_007_199_254_740_992_u64);
    assert!(serde_json::from_value::<Plan>(unsafe_policy)
        .unwrap_err()
        .to_string()
        .contains("safe-integer"));
}

fn matching_linear_descriptor_contract(
    descriptor_version: &str,
    model_schema: DescriptorValueSchema,
) -> DescriptorContract {
    DescriptorContract::new(
        "fixture.action",
        version(descriptor_version),
        DescriptorConfigurationContract::closed(
            BTreeMap::from([(
                "model".to_owned(),
                DescriptorFieldContract::required(model_schema),
            )]),
            BTreeMap::new(),
        ),
        WorkerContract::new(
            LeafTaskKind::Action,
            version("worker-1"),
            BTreeMap::new(),
            BTreeMap::from([(port_name("value"), PlanType::String)]),
        ),
    )
}

fn subflow_plan(interface_version: &str) -> Plan {
    let root = scope_id("root_scope");
    let invocation_scope = scope_id("subflow_invocation_scope");
    let call = node_id("call_node");
    let ret = node_id("return_node");
    let call_out = control_port_id("call_out");
    let return_in = control_port_id("return_in");
    let call_question = data_port_id("call_question");
    let call_value = data_port_id("call_value");
    let return_value = data_port_id("return_value");
    let mut builder = PlanBuilder::new(metadata(
        "call_node",
        PlanType::Null,
        PlanType::String,
        AuthorFormat::Programmatic,
        "parent_revision",
        "compiler-1",
    ));
    builder
        .add_scope(ScopeMetadata::root(root.clone()))
        .add_scope(ScopeMetadata::child(
            invocation_scope.clone(),
            root.clone(),
            call.clone(),
            ScopeKind::Subflow {
                call_node_id: call.clone(),
            },
            BTreeSet::new(),
        ))
        .add_node(Node::new(
            call.clone(),
            root.clone(),
            NodeKind::SubflowCall(SubflowCallDescriptor {
                definition_revision_id: DefinitionRevisionId::new("child_revision").unwrap(),
                interface_version: version(interface_version),
                invocation_scope_id: invocation_scope,
                inputs: BTreeMap::from([(port_name("question"), call_question.clone())]),
                timeout_ms: 300_000,
            }),
        ))
        .add_control_port(ControlPort::new(
            call_out.clone(),
            call.clone(),
            port_name("out"),
            PortDirection::Output,
        ))
        .add_data_port(DataPort::new(
            call_question.clone(),
            call.clone(),
            port_name("question"),
            PortDirection::Input,
            PlanType::String,
            true,
        ))
        .add_data_port(DataPort::new(
            call_value.clone(),
            call,
            port_name("value"),
            PortDirection::Output,
            PlanType::String,
            false,
        ))
        .add_node(Node::new(
            ret.clone(),
            root,
            NodeKind::Return(ReturnDescriptor {
                value_input: return_value.clone(),
            }),
        ))
        .add_control_port(ControlPort::new(
            return_in.clone(),
            ret.clone(),
            port_name("in"),
            PortDirection::Input,
        ))
        .add_data_port(DataPort::new(
            return_value.clone(),
            ret,
            port_name("value"),
            PortDirection::Input,
            PlanType::String,
            true,
        ))
        .add_control_edge(ControlEdge::new(
            control_edge_id("edge_call_return"),
            call_out,
            return_in,
        ))
        .add_data_binding(DataBinding::from_port(
            data_binding_id("bind_return"),
            call_value,
            return_value,
        ))
        .add_data_binding(DataBinding::new(
            data_binding_id("bind_call_question"),
            ValueSource::Literal {
                value: json!("question"),
            },
            call_question,
        ));
    builder.build().unwrap()
}

#[test]
fn subflow_call_requires_one_owned_invocation_scope_with_the_exact_parent() {
    let plan = subflow_plan("1");
    let wire = serde_json::to_value(&plan).unwrap();

    let mut invalid_timeout = wire.clone();
    invalid_timeout["nodes"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|node| node["id"] == json!("call_node"))
        .unwrap()["kind"]["descriptor"]["timeout_ms"] = json!(0);
    assert!(serde_json::from_value::<Plan>(invalid_timeout)
        .unwrap_err()
        .to_string()
        .contains("timeout"));

    let mut missing = wire.clone();
    missing["scopes"]
        .as_array_mut()
        .unwrap()
        .retain(|scope| scope["id"] != json!("subflow_invocation_scope"));
    let error = serde_json::from_value::<Plan>(missing).unwrap_err();
    assert!(error
        .to_string()
        .contains("must own exactly one declared invocation scope"));

    let mut duplicate = wire.clone();
    let mut duplicate_scope = duplicate["scopes"]
        .as_array()
        .unwrap()
        .iter()
        .find(|scope| scope["id"] == json!("subflow_invocation_scope"))
        .unwrap()
        .clone();
    duplicate_scope["id"] = json!("second_subflow_invocation_scope");
    duplicate["scopes"]
        .as_array_mut()
        .unwrap()
        .push(duplicate_scope);
    let error = serde_json::from_value::<Plan>(duplicate).unwrap_err();
    assert!(error
        .to_string()
        .contains("must own exactly one declared invocation scope"));

    let mut wrong_parent = wire;
    let scope = wrong_parent["scopes"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|scope| scope["id"] == json!("subflow_invocation_scope"))
        .unwrap();
    scope["parent"] = json!("subflow_invocation_scope");
    let error = serde_json::from_value::<Plan>(wrong_parent).unwrap_err();
    assert!(error.to_string().contains("invalid parent"));
}

#[test]
fn runtime_plan_index_is_verified_deterministic_and_scheduler_ready() {
    let first = linear_builder(false, 3, "alpha").build().unwrap();
    let second = linear_builder(true, 3, "alpha").build().unwrap();
    let first_index = PlanIndex::new(&first).unwrap();
    let second_index = PlanIndex::new(&second).unwrap();

    assert_eq!(first_index.entry_node().id(), &node_id("task_node"));
    assert_eq!(first_index.semantic_hash(), second_index.semantic_hash());
    assert_eq!(
        first_index.control_outputs(&node_id("task_node")),
        second_index.control_outputs(&node_id("task_node"))
    );
    let route = first_index
        .successor_for_output(&control_port_id("task_out"))
        .unwrap()
        .unwrap();
    assert_eq!(route.successor().id(), &node_id("return_node"));
    assert_eq!(route.predecessor().id(), &node_id("task_node"));
    assert_eq!(route.input().id(), &control_port_id("return_in"));
    assert_eq!(
        first_index
            .predecessor_for_input(&control_port_id("return_in"))
            .unwrap()
            .unwrap()
            .predecessor()
            .id(),
        &node_id("task_node")
    );
    assert!(matches!(
        first_index.source_for_input(&data_port_id("return_value")),
        Some(ValueSource::Port { port_id }) if port_id == &data_port_id("task_value")
    ));
    assert_eq!(
        first_index.policies_for_node(&node_id("task_node"))[0].id(),
        &policy_id("retry_task")
    );
    let leaf = first_index.leaf_descriptor(&node_id("task_node")).unwrap();
    assert_eq!(leaf.kind(), LeafTaskKind::Action);
    assert_eq!(leaf.descriptor().descriptor_version.as_str(), "1");

    let branch = valid_branch_plan();
    let branch_index = PlanIndex::new(&branch).unwrap();
    assert_eq!(
        branch_index
            .branch_case_route(&node_id("branch_node"), &case_id("a"))
            .unwrap()
            .successor()
            .id(),
        &node_id("task_a")
    );
    let merge = branch_index
        .merge_correlation(&node_id("merge_node"))
        .unwrap();
    assert_eq!(merge.branch_node().id(), &node_id("branch_node"));
    assert_eq!(
        merge.input_for_case(&case_id("b")),
        Some(&control_port_id("merge_b_in"))
    );
    assert_eq!(
        branch_index
            .branch_case_route(&node_id("branch_node"), &case_id("unknown"))
            .unwrap_err()
            .code(),
        PLAN_INDEX_INVALID
    );
}

#[test]
fn contextual_descriptor_linker_rejects_unknown_version_and_config_mismatch() {
    let plan = linear_builder(false, 3, "alpha").build().unwrap();
    let subflows = SubflowContractRegistry::new();

    let error = LinkedPlan::link(&plan, &DescriptorContractRegistry::new(), &subflows).unwrap_err();
    assert_eq!(error.code(), PLAN_CONTEXT_LINK_INVALID);
    assert!(error.message().contains("unknown descriptor"));

    let mut wrong_version = DescriptorContractRegistry::new();
    wrong_version
        .register(matching_linear_descriptor_contract(
            "2",
            DescriptorValueSchema::String,
        ))
        .unwrap();
    assert!(LinkedPlan::link(&plan, &wrong_version, &subflows)
        .unwrap_err()
        .message()
        .contains("unknown descriptor"));

    let mut wrong_config = DescriptorContractRegistry::new();
    wrong_config
        .register(matching_linear_descriptor_contract(
            "1",
            DescriptorValueSchema::Integer,
        ))
        .unwrap();
    assert!(LinkedPlan::link(&plan, &wrong_config, &subflows)
        .unwrap_err()
        .message()
        .contains("public descriptor configuration"));

    let mut wrong_worker = DescriptorContractRegistry::new();
    wrong_worker
        .register(DescriptorContract::new(
            "fixture.action",
            version("1"),
            DescriptorConfigurationContract::closed(
                BTreeMap::from([(
                    "model".to_owned(),
                    DescriptorFieldContract::required(DescriptorValueSchema::String),
                )]),
                BTreeMap::new(),
            ),
            WorkerContract::new(
                LeafTaskKind::Action,
                version("worker-1"),
                BTreeMap::new(),
                BTreeMap::from([(port_name("value"), PlanType::Number)]),
            ),
        ))
        .unwrap();
    assert!(LinkedPlan::link(&plan, &wrong_worker, &subflows)
        .unwrap_err()
        .message()
        .contains("worker data-port contract"));

    let mut matching = DescriptorContractRegistry::new();
    matching
        .register(matching_linear_descriptor_contract(
            "1",
            DescriptorValueSchema::String,
        ))
        .unwrap();
    let linked = LinkedPlan::link(&plan, &matching, &subflows).unwrap();
    let contract = linked.descriptor(&node_id("task_node")).unwrap();
    assert_eq!(contract.worker().worker_version(), &version("worker-1"));
    assert_eq!(linked.index().entry_node().id(), &node_id("task_node"));
}

#[test]
fn contextual_subflow_linker_rejects_unknown_or_wrong_interface_version() {
    let plan = subflow_plan("1");
    let descriptors = DescriptorContractRegistry::new();
    let empty = SubflowContractRegistry::new();
    assert!(LinkedPlan::link(&plan, &descriptors, &empty)
        .unwrap_err()
        .message()
        .contains("unknown subflow"));

    let contract = |interface_version: &str| {
        SubflowInterfaceContract::new(
            ExecutionRevisionPin::new(
                DefinitionRevisionId::new("child_revision").unwrap(),
                DeploymentRevisionId::new("child_deployment").unwrap(),
                ContentHash::from_bytes(b"child-plan"),
                ContentHash::from_bytes(b"child-binding"),
            ),
            version(interface_version),
            PlanInputContract::new(PlanType::Object {
                properties: BTreeMap::from([(
                    "question".to_owned(),
                    PlanProperty::new(PlanType::String, true).unwrap(),
                )]),
                additional_properties: None,
            }),
            BTreeMap::from([(port_name("value"), PlanType::String)]),
            safe_error_type(),
        )
    };
    let mut wrong = SubflowContractRegistry::new();
    wrong.register(contract("2")).unwrap();
    assert!(LinkedPlan::link(&plan, &descriptors, &wrong)
        .unwrap_err()
        .message()
        .contains("unknown subflow"));

    let mut matching = SubflowContractRegistry::new();
    matching.register(contract("1")).unwrap();
    let linked = LinkedPlan::link(&plan, &descriptors, &matching).unwrap();
    assert_eq!(
        linked
            .subflow(&node_id("call_node"))
            .unwrap()
            .interface_version(),
        &version("1")
    );
}

#[test]
fn subflow_linker_uses_child_presence_contract_instead_of_exact_parent_port_equality() {
    let plan = subflow_plan("1");
    let descriptors = DescriptorContractRegistry::new();
    let execution_revision = ExecutionRevisionPin::new(
        DefinitionRevisionId::new("child_revision").unwrap(),
        DeploymentRevisionId::new("child_deployment").unwrap(),
        ContentHash::from_bytes(b"child-plan"),
        ContentHash::from_bytes(b"child-binding"),
    );
    let accepted_type = PlanType::Object {
        properties: BTreeMap::from([
            (
                "note".to_owned(),
                PlanProperty::new(PlanType::String, false).unwrap(),
            ),
            (
                "question".to_owned(),
                PlanProperty::new(PlanType::String, true).unwrap(),
            ),
            (
                "tone".to_owned(),
                PlanProperty::new(PlanType::String, false).unwrap(),
            ),
        ]),
        additional_properties: None,
    };
    let input_contract = PlanInputContract::new(accepted_type)
        .with_defaults(BTreeMap::from([("tone".to_owned(), json!("concise"))]));
    let mut matching = SubflowContractRegistry::new();
    matching
        .register(SubflowInterfaceContract::new(
            execution_revision.clone(),
            version("1"),
            input_contract.clone(),
            BTreeMap::from([(port_name("value"), PlanType::String)]),
            safe_error_type(),
        ))
        .unwrap();
    let linked = LinkedPlan::link(&plan, &descriptors, &matching).unwrap();
    assert_eq!(
        linked
            .subflow(&node_id("call_node"))
            .unwrap()
            .input_contract(),
        &input_contract
    );

    let mut missing_required = SubflowContractRegistry::new();
    missing_required
        .register(SubflowInterfaceContract::new(
            execution_revision,
            version("1"),
            PlanInputContract::new(PlanType::Object {
                properties: BTreeMap::from([
                    (
                        "question".to_owned(),
                        PlanProperty::new(PlanType::String, true).unwrap(),
                    ),
                    (
                        "tenant".to_owned(),
                        PlanProperty::new(PlanType::String, true).unwrap(),
                    ),
                ]),
                additional_properties: None,
            }),
            BTreeMap::from([(port_name("value"), PlanType::String)]),
            safe_error_type(),
        ))
        .unwrap();
    assert!(LinkedPlan::link(&plan, &descriptors, &missing_required)
        .unwrap_err()
        .message()
        .contains("omits required child input 'tenant'"));
}

#[test]
fn authored_source_maps_are_complete_and_content_hash_bound() {
    let authored = minimal_return_plan_with(
        AuthorFormat::Structured,
        "authored_revision",
        "compiler-1",
        SourceMap::new(),
        None,
    );
    assert_eq!(
        authored.source_map().coverage_policy(),
        SourceMapPolicy::AuthoredComplete
    );
    assert_eq!(authored.source_map().documents().len(), 1);

    let mut incomplete = serde_json::to_value(&authored).unwrap();
    incomplete["source_map"]["nodes"] = json!({});
    let error = serde_json::from_value::<Plan>(incomplete).unwrap_err();
    assert!(error.to_string().contains("must cover every"));

    let mut exempt_authored = serde_json::to_value(&authored).unwrap();
    exempt_authored["source_map"] = serde_json::to_value(SourceMap::new()).unwrap();
    let error = serde_json::from_value::<Plan>(exempt_authored).unwrap_err();
    assert!(error.to_string().contains("authored-complete SourceMap"));

    let mut invalid_hash = serde_json::to_value(&authored).unwrap();
    let document = invalid_hash["source_map"]["documents"]
        .as_object_mut()
        .unwrap()
        .values_mut()
        .next()
        .unwrap();
    *document = json!("sha256:not-a-content-hash");
    assert!(serde_json::from_value::<Plan>(invalid_hash).is_err());

    let programmatic = minimal_return_plan();
    assert_eq!(
        programmatic.source_map().coverage_policy(),
        SourceMapPolicy::ProgrammaticExempt
    );
}
