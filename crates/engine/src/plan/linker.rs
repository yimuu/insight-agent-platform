use std::collections::{btree_map::Entry, BTreeMap};

use super::{
    DescriptorValue, LeafTaskKind, Plan, PlanError, PlanIndex, PlanInputContract, PlanType,
    PortName, VersionTag, PLAN_CONTEXT_LINK_INVALID,
};
use crate::{
    DefinitionRevisionId, EffectIdempotency, ExecutionRevisionPin, NodeId, WorkerCancellation,
    WorkerEffectClass, WorkerEffectPolicy,
};

const MAX_CONTRACT_FIELDS: usize = 4096;
const MAX_CONTRACT_DEPTH: usize = 64;
const MAX_CONTRACT_NAME_BYTES: usize = 256;

/// Recursive closed schema for non-secret descriptor configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DescriptorValueSchema {
    /// Deliberately explicit open value slot. This is primarily useful for
    /// heterogeneous descriptor arrays such as a compiled chat message
    /// program; the enclosing configuration object remains closed.
    Any,
    Null,
    Boolean,
    Integer,
    Number,
    String,
    Array(Box<DescriptorValueSchema>),
    Object(BTreeMap<String, DescriptorFieldContract>),
}

impl DescriptorValueSchema {
    fn accepts(&self, value: &DescriptorValue) -> bool {
        match (self, value) {
            (Self::Any, _)
            | (Self::Null, DescriptorValue::Null)
            | (Self::Boolean, DescriptorValue::Boolean(_))
            | (Self::Integer, DescriptorValue::Integer(_))
            | (Self::Number, DescriptorValue::Integer(_) | DescriptorValue::Number(_))
            | (Self::String, DescriptorValue::String(_)) => true,
            (Self::Array(item), DescriptorValue::Array(values)) => {
                values.iter().all(|value| item.accepts(value))
            }
            (Self::Object(fields), DescriptorValue::Object(values)) => {
                closed_public_configuration_matches(fields, values)
            }
            _ => false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescriptorFieldContract {
    value_schema: DescriptorValueSchema,
    required: bool,
}

impl DescriptorFieldContract {
    pub fn required(value_schema: DescriptorValueSchema) -> Self {
        Self {
            value_schema,
            required: true,
        }
    }

    pub fn optional(value_schema: DescriptorValueSchema) -> Self {
        Self {
            value_schema,
            required: false,
        }
    }

    pub fn value_schema(&self) -> &DescriptorValueSchema {
        &self.value_schema
    }

    pub fn is_required(&self) -> bool {
        self.required
    }
}

/// Descriptor configuration is closed by default. Secret fields describe only
/// which named SecretRef slots are legal/required; no resolver or secret value
/// is available at the Plan linking boundary.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DescriptorConfigurationContract {
    public_fields: BTreeMap<String, DescriptorFieldContract>,
    secret_fields: BTreeMap<String, bool>,
}

impl DescriptorConfigurationContract {
    pub fn closed(
        public_fields: BTreeMap<String, DescriptorFieldContract>,
        secret_fields: BTreeMap<String, bool>,
    ) -> Self {
        Self {
            public_fields,
            secret_fields,
        }
    }

    pub fn empty() -> Self {
        Self::default()
    }

    pub fn public_fields(&self) -> &BTreeMap<String, DescriptorFieldContract> {
        &self.public_fields
    }

    /// The bool is true for a required SecretRef slot.
    pub fn secret_fields(&self) -> &BTreeMap<String, bool> {
        &self.secret_fields
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerInputPortContract {
    value_type: PlanType,
    required: bool,
}

impl WorkerInputPortContract {
    pub fn new(value_type: PlanType, required: bool) -> Self {
        Self {
            value_type,
            required,
        }
    }

    pub fn value_type(&self) -> &PlanType {
        &self.value_type
    }

    pub fn required(&self) -> bool {
        self.required
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerContract {
    task_kind: LeafTaskKind,
    worker_version: VersionTag,
    effect_policy: WorkerEffectPolicy,
    inputs: BTreeMap<PortName, WorkerInputPortContract>,
    outputs: BTreeMap<PortName, PlanType>,
}

impl WorkerContract {
    pub fn new(
        task_kind: LeafTaskKind,
        worker_version: VersionTag,
        inputs: BTreeMap<PortName, WorkerInputPortContract>,
        outputs: BTreeMap<PortName, PlanType>,
    ) -> Self {
        let effect_policy = WorkerEffectPolicy::frozen(
            WorkerEffectClass::Mutating,
            EffectIdempotency::NonIdempotent,
            1,
            0,
            0,
            60_000,
            WorkerCancellation::LeaseOnly,
        )
        .expect("the closed single-attempt fallback worker policy is valid");
        Self {
            task_kind,
            worker_version,
            effect_policy,
            inputs,
            outputs,
        }
    }

    pub fn with_effect_policy(mut self, effect_policy: WorkerEffectPolicy) -> Self {
        self.effect_policy = effect_policy;
        self
    }

    pub fn task_kind(&self) -> LeafTaskKind {
        self.task_kind
    }

    pub fn worker_version(&self) -> &VersionTag {
        &self.worker_version
    }

    pub fn effect_policy(&self) -> &WorkerEffectPolicy {
        &self.effect_policy
    }

    pub fn inputs(&self) -> &BTreeMap<PortName, WorkerInputPortContract> {
        &self.inputs
    }

    pub fn outputs(&self) -> &BTreeMap<PortName, PlanType> {
        &self.outputs
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescriptorContract {
    implementation: String,
    descriptor_version: VersionTag,
    configuration: DescriptorConfigurationContract,
    worker: WorkerContract,
    deployment_binding: serde_json::Value,
}

impl DescriptorContract {
    pub fn new(
        implementation: impl Into<String>,
        descriptor_version: VersionTag,
        configuration: DescriptorConfigurationContract,
        worker: WorkerContract,
    ) -> Self {
        Self {
            implementation: implementation.into(),
            descriptor_version,
            configuration,
            worker,
            deployment_binding: serde_json::Value::Object(Default::default()),
        }
    }

    /// Freezes publication-time, non-secret provider/tool binding evidence
    /// into every durable worker dispatch. Author configuration and deployment
    /// binding deliberately remain separate trust domains.
    pub fn with_deployment_binding(
        mut self,
        binding: serde_json::Value,
    ) -> Result<Self, PlanError> {
        if !binding.is_object() || serde_jcs::to_vec(&binding).is_err() {
            return Err(link_error(
                "descriptor deployment binding must be one canonical non-secret object",
            ));
        }
        self.deployment_binding = binding;
        Ok(self)
    }

    pub fn implementation(&self) -> &str {
        &self.implementation
    }

    pub fn descriptor_version(&self) -> &VersionTag {
        &self.descriptor_version
    }

    pub fn configuration(&self) -> &DescriptorConfigurationContract {
        &self.configuration
    }

    pub fn worker(&self) -> &WorkerContract {
        &self.worker
    }

    pub fn deployment_binding(&self) -> &serde_json::Value {
        &self.deployment_binding
    }
}

/// Minimal runtime registry contract. Implementations return inert metadata,
/// never a secret resolver and never secret material.
pub trait DescriptorRegistry {
    fn resolve(
        &self,
        node_id: &NodeId,
        implementation: &str,
        descriptor_version: &VersionTag,
    ) -> Option<DescriptorContract>;
}

#[derive(Debug, Clone, Default)]
pub struct DescriptorContractRegistry {
    entries: BTreeMap<(String, VersionTag), DescriptorContract>,
    node_entries: BTreeMap<(NodeId, String, VersionTag), DescriptorContract>,
}

impl DescriptorContractRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&mut self, contract: DescriptorContract) -> Result<(), PlanError> {
        let key = (
            contract.implementation.clone(),
            contract.descriptor_version.clone(),
        );
        match self.entries.entry(key) {
            Entry::Vacant(entry) => {
                entry.insert(contract);
            }
            Entry::Occupied(_) => {
                return Err(link_error("duplicate descriptor registry entry"));
            }
        }
        Ok(())
    }

    /// Registers an exact deployment contract for one Plan node. This is
    /// required for polymorphic adapters such as `core.llm`: the executable
    /// worker version is shared, while each node freezes different typed ports
    /// and configuration in its Deployment Revision.
    pub fn register_for_node(
        &mut self,
        node_id: NodeId,
        contract: DescriptorContract,
    ) -> Result<(), PlanError> {
        let key = (
            node_id,
            contract.implementation.clone(),
            contract.descriptor_version.clone(),
        );
        match self.node_entries.entry(key) {
            Entry::Vacant(entry) => {
                entry.insert(contract);
            }
            Entry::Occupied(_) => {
                return Err(link_error("duplicate node descriptor registry entry"));
            }
        }
        Ok(())
    }
}

impl DescriptorRegistry for DescriptorContractRegistry {
    fn resolve(
        &self,
        node_id: &NodeId,
        implementation: &str,
        descriptor_version: &VersionTag,
    ) -> Option<DescriptorContract> {
        self.node_entries
            .get(&(
                node_id.clone(),
                implementation.to_owned(),
                descriptor_version.clone(),
            ))
            .or_else(|| {
                self.entries
                    .get(&(implementation.to_owned(), descriptor_version.clone()))
            })
            .cloned()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubflowInterfaceContract {
    execution_revision: ExecutionRevisionPin,
    interface_version: VersionTag,
    input_contract: PlanInputContract,
    outputs: BTreeMap<PortName, PlanType>,
    error_type: PlanType,
}

impl SubflowInterfaceContract {
    pub fn new(
        execution_revision: ExecutionRevisionPin,
        interface_version: VersionTag,
        input_contract: PlanInputContract,
        outputs: BTreeMap<PortName, PlanType>,
        error_type: PlanType,
    ) -> Self {
        Self {
            execution_revision,
            interface_version,
            input_contract,
            outputs,
            error_type,
        }
    }

    pub fn definition_revision_id(&self) -> &DefinitionRevisionId {
        self.execution_revision.definition_revision_id()
    }

    pub fn execution_revision(&self) -> &ExecutionRevisionPin {
        &self.execution_revision
    }

    pub fn interface_version(&self) -> &VersionTag {
        &self.interface_version
    }

    pub fn input_contract(&self) -> &PlanInputContract {
        &self.input_contract
    }

    pub fn outputs(&self) -> &BTreeMap<PortName, PlanType> {
        &self.outputs
    }

    pub fn error_type(&self) -> &PlanType {
        &self.error_type
    }
}

pub trait SubflowInterfaceRegistry {
    fn resolve(
        &self,
        definition_revision_id: &DefinitionRevisionId,
        interface_version: &VersionTag,
    ) -> Option<SubflowInterfaceContract>;
}

#[derive(Debug, Clone, Default)]
pub struct SubflowContractRegistry {
    entries: BTreeMap<(DefinitionRevisionId, VersionTag), SubflowInterfaceContract>,
}

impl SubflowContractRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&mut self, contract: SubflowInterfaceContract) -> Result<(), PlanError> {
        let key = (
            contract.definition_revision_id().clone(),
            contract.interface_version.clone(),
        );
        match self.entries.entry(key) {
            Entry::Vacant(entry) => {
                entry.insert(contract);
            }
            Entry::Occupied(_) => {
                return Err(link_error("duplicate subflow registry entry"));
            }
        }
        Ok(())
    }
}

impl SubflowInterfaceRegistry for SubflowContractRegistry {
    fn resolve(
        &self,
        definition_revision_id: &DefinitionRevisionId,
        interface_version: &VersionTag,
    ) -> Option<SubflowInterfaceContract> {
        self.entries
            .get(&(definition_revision_id.clone(), interface_version.clone()))
            .cloned()
    }
}

/// Runtime-ready Plan. It can only be constructed after both structural Plan
/// verification and contextual registry linking succeed.
#[derive(Debug)]
pub struct LinkedPlan<'a> {
    index: PlanIndex<'a>,
    descriptors: BTreeMap<NodeId, DescriptorContract>,
    subflows: BTreeMap<NodeId, SubflowInterfaceContract>,
}

impl<'a> LinkedPlan<'a> {
    pub fn link(
        plan: &'a Plan,
        descriptors: &dyn DescriptorRegistry,
        subflows: &dyn SubflowInterfaceRegistry,
    ) -> Result<Self, PlanError> {
        let index = PlanIndex::new(plan)?;
        let mut linked_descriptors = BTreeMap::new();
        let mut linked_subflows = BTreeMap::new();

        for node in plan.nodes() {
            if let Some(leaf) = index.leaf_descriptor(node.id()) {
                let descriptor = leaf.descriptor();
                let contract = descriptors
                    .resolve(
                        node.id(),
                        &descriptor.implementation,
                        &descriptor.descriptor_version,
                    )
                    .ok_or_else(|| {
                        link_error(format!(
                            "node '{}' requires unknown descriptor '{}@{}'",
                            node.id(),
                            descriptor.implementation,
                            descriptor.descriptor_version
                        ))
                    })?;
                validate_descriptor_identity(descriptor, &contract, node.id())?;
                validate_configuration(descriptor, &contract, node.id())?;
                validate_worker_contract(&index, node.id(), leaf.kind(), contract.worker())?;
                linked_descriptors.insert(node.id().clone(), contract);
            }

            if let super::NodeKind::SubflowCall(call) = node.kind() {
                let contract = subflows
                    .resolve(&call.definition_revision_id, &call.interface_version)
                    .ok_or_else(|| {
                        link_error(format!(
                            "node '{}' requires unknown subflow '{}@{}'",
                            node.id(),
                            call.definition_revision_id,
                            call.interface_version
                        ))
                    })?;
                if contract.definition_revision_id() != &call.definition_revision_id
                    || contract.interface_version != call.interface_version
                {
                    return Err(link_error(format!(
                        "subflow registry returned a mismatched contract for node '{}'",
                        node.id()
                    )));
                }
                validate_subflow_port_contract(
                    &index,
                    node.id(),
                    call,
                    &contract.input_contract,
                    &contract.outputs,
                )?;
                validate_canonical_type(&contract.error_type, "subflow error contract")?;
                linked_subflows.insert(node.id().clone(), contract);
            }
        }

        Ok(Self {
            index,
            descriptors: linked_descriptors,
            subflows: linked_subflows,
        })
    }

    pub fn index(&self) -> &PlanIndex<'a> {
        &self.index
    }

    pub fn descriptor(&self, node: &NodeId) -> Option<&DescriptorContract> {
        self.descriptors.get(node)
    }

    pub fn subflow(&self, node: &NodeId) -> Option<&SubflowInterfaceContract> {
        self.subflows.get(node)
    }
}

fn validate_descriptor_identity(
    plan: &super::LeafTaskDescriptor,
    contract: &DescriptorContract,
    node: &NodeId,
) -> Result<(), PlanError> {
    if contract.implementation != plan.implementation
        || contract.descriptor_version != plan.descriptor_version
    {
        return Err(link_error(format!(
            "descriptor registry returned a mismatched contract for node '{node}'"
        )));
    }
    Ok(())
}

fn validate_configuration(
    descriptor: &super::LeafTaskDescriptor,
    contract: &DescriptorContract,
    node: &NodeId,
) -> Result<(), PlanError> {
    validate_configuration_contract(&contract.configuration)?;
    if contract
        .configuration
        .public_fields
        .keys()
        .any(|field| contract.configuration.secret_fields.contains_key(field))
    {
        return Err(link_error(format!(
            "descriptor contract for node '{node}' declares a field as both public and secret"
        )));
    }
    if !closed_public_configuration_matches(
        &contract.configuration.public_fields,
        &descriptor.public_configuration,
    ) {
        return Err(link_error(format!(
            "public descriptor configuration does not match the closed schema for node '{node}'"
        )));
    }
    if descriptor
        .secret_configuration
        .keys()
        .any(|field| !contract.configuration.secret_fields.contains_key(field))
        || contract
            .configuration
            .secret_fields
            .iter()
            .any(|(field, required)| {
                *required && !descriptor.secret_configuration.contains_key(field)
            })
    {
        return Err(link_error(format!(
            "SecretRef slots do not match the closed descriptor schema for node '{node}'"
        )));
    }
    Ok(())
}

fn validate_configuration_contract(
    contract: &DescriptorConfigurationContract,
) -> Result<(), PlanError> {
    let mut items = 0;
    validate_schema_fields(&contract.public_fields, 0, &mut items)?;
    for field in contract.secret_fields.keys() {
        validate_contract_field_name(field)?;
        items += 1;
        if items > MAX_CONTRACT_FIELDS {
            return Err(link_error("descriptor contract contains too many fields"));
        }
    }
    Ok(())
}

fn validate_schema_fields(
    fields: &BTreeMap<String, DescriptorFieldContract>,
    depth: usize,
    items: &mut usize,
) -> Result<(), PlanError> {
    if depth > MAX_CONTRACT_DEPTH {
        return Err(link_error(
            "descriptor contract schema is nested too deeply",
        ));
    }
    for (field, contract) in fields {
        validate_contract_field_name(field)?;
        *items += 1;
        if *items > MAX_CONTRACT_FIELDS {
            return Err(link_error("descriptor contract contains too many fields"));
        }
        match &contract.value_schema {
            DescriptorValueSchema::Array(item) => {
                validate_value_schema(item, depth + 1, items)?;
            }
            DescriptorValueSchema::Object(nested) => {
                validate_schema_fields(nested, depth + 1, items)?;
            }
            DescriptorValueSchema::Any
            | DescriptorValueSchema::Null
            | DescriptorValueSchema::Boolean
            | DescriptorValueSchema::Integer
            | DescriptorValueSchema::Number
            | DescriptorValueSchema::String => {}
        }
    }
    Ok(())
}

fn validate_value_schema(
    schema: &DescriptorValueSchema,
    depth: usize,
    items: &mut usize,
) -> Result<(), PlanError> {
    if depth > MAX_CONTRACT_DEPTH {
        return Err(link_error(
            "descriptor contract schema is nested too deeply",
        ));
    }
    *items += 1;
    if *items > MAX_CONTRACT_FIELDS {
        return Err(link_error("descriptor contract contains too many fields"));
    }
    match schema {
        DescriptorValueSchema::Array(item) => validate_value_schema(item, depth + 1, items),
        DescriptorValueSchema::Object(fields) => validate_schema_fields(fields, depth + 1, items),
        DescriptorValueSchema::Any
        | DescriptorValueSchema::Null
        | DescriptorValueSchema::Boolean
        | DescriptorValueSchema::Integer
        | DescriptorValueSchema::Number
        | DescriptorValueSchema::String => Ok(()),
    }
}

fn validate_contract_field_name(field: &str) -> Result<(), PlanError> {
    if field.is_empty()
        || field.len() > MAX_CONTRACT_NAME_BYTES
        || field.chars().any(char::is_control)
    {
        return Err(link_error(
            "descriptor contract field names must be non-empty, bounded, and contain no controls",
        ));
    }
    Ok(())
}

fn closed_public_configuration_matches(
    fields: &BTreeMap<String, DescriptorFieldContract>,
    values: &BTreeMap<String, DescriptorValue>,
) -> bool {
    !values.keys().any(|field| !fields.contains_key(field))
        && !fields
            .iter()
            .any(|(field, contract)| contract.required && !values.contains_key(field))
        && values.iter().all(|(field, value)| {
            fields
                .get(field)
                .is_some_and(|contract| contract.value_schema.accepts(value))
        })
}

fn validate_worker_contract(
    index: &PlanIndex<'_>,
    node: &NodeId,
    actual_kind: LeafTaskKind,
    contract: &WorkerContract,
) -> Result<(), PlanError> {
    if contract.task_kind != actual_kind {
        return Err(link_error(format!(
            "worker kind '{}' does not match node '{}' kind '{}'",
            contract.task_kind.name(),
            node,
            actual_kind.name()
        )));
    }
    validate_port_contract(index, node, &contract.inputs, &contract.outputs, "worker")
}

fn validate_port_contract(
    index: &PlanIndex<'_>,
    node: &NodeId,
    expected_inputs: &BTreeMap<PortName, WorkerInputPortContract>,
    expected_outputs: &BTreeMap<PortName, PlanType>,
    label: &str,
) -> Result<(), PlanError> {
    let mut actual_inputs = BTreeMap::new();
    for id in index.data_inputs(node) {
        let port = index
            .data_port(id)
            .ok_or_else(|| link_error("indexed data input disappeared"))?;
        actual_inputs.insert(
            port.name().clone(),
            WorkerInputPortContract::new(port.value_type().clone(), port.required()),
        );
    }
    let mut actual_outputs = BTreeMap::new();
    for id in index.data_outputs(node) {
        let port = index
            .data_port(id)
            .ok_or_else(|| link_error("indexed data output disappeared"))?;
        actual_outputs.insert(port.name().clone(), port.value_type().clone());
    }

    for contract in expected_inputs.values() {
        validate_canonical_type(contract.value_type(), &format!("{label} input"))?;
    }
    for value_type in expected_outputs.values() {
        validate_canonical_type(value_type, &format!("{label} output"))?;
    }
    if &actual_inputs != expected_inputs || &actual_outputs != expected_outputs {
        return Err(link_error(format!(
            "{label} data-port contract does not exactly match node '{node}'"
        )));
    }
    Ok(())
}

fn validate_subflow_port_contract(
    index: &PlanIndex<'_>,
    node: &NodeId,
    descriptor: &super::SubflowCallDescriptor,
    input_contract: &PlanInputContract,
    expected_outputs: &BTreeMap<PortName, PlanType>,
) -> Result<(), PlanError> {
    validate_canonical_type(input_contract.accepted_type(), "subflow accepted input")?;
    let PlanType::Object {
        properties,
        additional_properties: None,
    } = input_contract.accepted_type()
    else {
        return Err(link_error(
            "subflow accepted input contract must be a closed object",
        ));
    };
    validate_canonical_type(
        &input_contract
            .run_type()
            .map_err(|error| link_error(format!("subflow normalized input is invalid: {error}")))?,
        "subflow normalized input",
    )?;
    for (name, value) in input_contract.defaults() {
        let property = properties.get(name).ok_or_else(|| {
            link_error(format!(
                "subflow input default references unknown field '{name}'"
            ))
        })?;
        if property.required || !property.value_type.accepts_literal(value).unwrap_or(false) {
            return Err(link_error(format!(
                "subflow input default for '{name}' violates its presence or value contract"
            )));
        }
    }

    let mut supplied_inputs = BTreeMap::new();
    for (name, id) in &descriptor.inputs {
        let port = index
            .data_port(id)
            .ok_or_else(|| link_error("subflow interface input disappeared"))?;
        if port.owner() != node
            || port.direction() != super::PortDirection::Input
            || port.name() != name
        {
            return Err(link_error(format!(
                "subflow interface input map is invalid for node '{node}'"
            )));
        }
        let property = properties.get(name.as_str()).ok_or_else(|| {
            link_error(format!(
                "subflow call node '{node}' supplies unknown input '{name}'"
            ))
        })?;
        if !port.value_type().is_assignable_to(&property.value_type) {
            return Err(link_error(format!(
                "subflow call node '{node}' input '{name}' is not assignable to the child contract"
            )));
        }
        if property.required && !port.required() {
            return Err(link_error(format!(
                "subflow call node '{node}' cannot bind required child input '{name}' from an optional source"
            )));
        }
        supplied_inputs.insert(name.as_str().to_owned(), port.required());
    }
    for (name, property) in properties {
        if property.required && supplied_inputs.get(name) != Some(&true) {
            return Err(link_error(format!(
                "subflow call node '{node}' omits required child input '{name}'"
            )));
        }
    }
    let mut actual_outputs = BTreeMap::new();
    for id in index.data_outputs(node) {
        let port = index
            .data_port(id)
            .ok_or_else(|| link_error("indexed subflow output disappeared"))?;
        actual_outputs.insert(port.name().clone(), port.value_type().clone());
    }

    for value_type in expected_outputs.values() {
        validate_canonical_type(value_type, "subflow output")?;
    }
    if &actual_outputs != expected_outputs {
        return Err(link_error(format!(
            "subflow output data-port contract does not exactly match node '{node}'"
        )));
    }
    Ok(())
}

fn validate_canonical_type(value: &PlanType, label: &str) -> Result<(), PlanError> {
    match value.normalized() {
        Ok(normalized) if &normalized == value => Ok(()),
        Ok(_) => Err(link_error(format!("{label} is not canonical"))),
        Err(error) => Err(link_error(format!("{label} is invalid: {error}"))),
    }
}

fn link_error(message: impl Into<String>) -> PlanError {
    PlanError::new(PLAN_CONTEXT_LINK_INVALID, message)
}
