use super::RepositoryErrorExt as _;

use std::{collections::BTreeSet, time::Duration};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub use insight_engine::run_stream::{DurableRunStreamSnapshot, RunTerminalKind, RunUsageStatus};

use insight_engine::{
    plan::{Plan, PlanInputContract},
    AdmissionState, ContentHash, DefinitionRevisionId, DeploymentRevisionId, PendingExecutionEvent,
    PublicErrorCode, PublicEventPayload, RunId, RunLifecycle, TerminationReason,
};

use insight_engine::plan::PlanType;

use super::{common::canonical_value, RepositoryError, REPOSITORY_CONFIGURATION_INVALID};

fn invalid_command() -> RepositoryError {
    RepositoryError::new(
        REPOSITORY_CONFIGURATION_INVALID,
        "durable repository command is invalid",
    )
}

fn validate_body_free_label(value: &str) -> Result<(), RepositoryError> {
    if value.is_empty()
        || value.len() > 256
        || value
            .chars()
            .any(|character| character.is_control() || character.is_whitespace())
    {
        return Err(invalid_command());
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct VersionedPlan {
    definition_id: String,
    agent_id: String,
    display_name: String,
    public_description: String,
    definition_revision_id: DefinitionRevisionId,
    deployment_revision_id: DeploymentRevisionId,
    plan_hash: ContentHash,
    binding_hash: ContentHash,
    compiler_version: String,
    expression_engine_version: String,
    author_document: Value,
    canonical_plan: Value,
    descriptor_contracts: Value,
    resolved_bindings: Value,
    worker_contracts: Value,
    #[serde(skip_serializing)]
    input_contract: PlanInputContract,
}

impl VersionedPlan {
    /// Publication constructor for any author format that proves the encoded
    /// document and verified Canonical Plan belong to the same authority.
    #[allow(clippy::too_many_arguments)]
    pub fn from_verified_graph<G: insight_engine::internal::VerifiedAuthorPlanView + ?Sized>(
        definition_id: impl Into<String>,
        agent_id: impl Into<String>,
        display_name: impl Into<String>,
        deployment_revision_id: DeploymentRevisionId,
        expression_engine_version: impl Into<String>,
        graph: &G,
        descriptor_contracts: Value,
        resolved_bindings: Value,
        worker_contracts: Value,
    ) -> Result<Self, RepositoryError> {
        let bytes = graph.encode_author_document()?;
        let author_document =
            serde_json::from_slice(&bytes).map_err(|_| RepositoryError::canonicalization())?;
        Self::from_verified_plan(
            definition_id,
            agent_id,
            display_name,
            deployment_revision_id,
            expression_engine_version,
            author_document,
            graph.verified_plan(),
            descriptor_contracts,
            resolved_bindings,
            worker_contracts,
        )
    }

    /// Builds a durable revision only from the verified Canonical Typed Plan.
    ///
    /// The definition revision, compiler version, and semantic hash are all
    /// derived from `plan`; the deployment binding hash is derived from the
    /// canonical resolved bindings. Callers cannot pair an arbitrary JSON
    /// document with a caller-selected authority hash.
    #[allow(clippy::too_many_arguments)]
    pub fn from_verified_plan(
        definition_id: impl Into<String>,
        agent_id: impl Into<String>,
        display_name: impl Into<String>,
        deployment_revision_id: DeploymentRevisionId,
        expression_engine_version: impl Into<String>,
        author_document: Value,
        plan: &Plan,
        descriptor_contracts: Value,
        resolved_bindings: Value,
        worker_contracts: Value,
    ) -> Result<Self, RepositoryError> {
        plan.verify().map_err(|_| invalid_command())?;
        let canonical_plan =
            serde_json::to_value(plan).map_err(|_| RepositoryError::canonicalization())?;
        let plan_hash = ContentHash::parse(plan.semantic_hash().as_str())
            .map_err(|_| RepositoryError::canonicalization())?;
        let binding_projection = serde_json::json!({
            "schema_version": 1,
            "resolved_bindings": &resolved_bindings,
            "worker_contracts": &worker_contracts,
        });
        let (_, binding_hash) = canonical_value(&binding_projection)?;
        Self::build(
            definition_id,
            agent_id,
            display_name,
            plan.metadata().definition_revision_id().clone(),
            deployment_revision_id,
            plan_hash,
            binding_hash,
            plan.metadata().compiler_version().as_str(),
            expression_engine_version,
            author_document,
            canonical_plan,
            descriptor_contracts,
            resolved_bindings,
            worker_contracts,
            plan.metadata().input_contract().clone(),
        )
    }

    /// Reconstructs one immutable deployment from durable rows and verifies
    /// every derived identity before the value can re-enter the runtime.
    ///
    /// Database JSON and hash columns are evidence, not trusted execution
    /// input: Canonical Plan decoding re-runs structural/hash verification,
    /// while this constructor recomputes both Plan and binding hashes and
    /// rejects any disagreement with the stored identity tuple.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn from_stored_parts(
        definition_id: String,
        agent_id: String,
        display_name: String,
        definition_revision_id: DefinitionRevisionId,
        deployment_revision_id: DeploymentRevisionId,
        plan_hash: ContentHash,
        binding_hash: ContentHash,
        compiler_version: String,
        expression_engine_version: String,
        public_description: String,
        author_document: Value,
        canonical_plan: Value,
        descriptor_contracts: Value,
        resolved_bindings: Value,
        worker_contracts: Value,
    ) -> Result<Self, RepositoryError> {
        let plan_bytes =
            serde_json::to_vec(&canonical_plan).map_err(|_| RepositoryError::canonicalization())?;
        let plan = Plan::decode_json(&plan_bytes).map_err(|_| RepositoryError::invalid_data())?;
        let reconstructed = Self::from_verified_plan(
            definition_id,
            agent_id,
            display_name,
            deployment_revision_id,
            expression_engine_version,
            author_document,
            &plan,
            descriptor_contracts,
            resolved_bindings,
            worker_contracts,
        )?
        .with_public_description(public_description)?;
        if reconstructed.definition_revision_id != definition_revision_id
            || reconstructed.plan_hash != plan_hash
            || reconstructed.binding_hash != binding_hash
            || reconstructed.compiler_version != compiler_version
            || reconstructed.canonical_plan != canonical_plan
        {
            return Err(RepositoryError::invalid_data());
        }
        Ok(reconstructed)
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new_for_test(
        definition_id: impl Into<String>,
        agent_id: impl Into<String>,
        display_name: impl Into<String>,
        definition_revision_id: DefinitionRevisionId,
        deployment_revision_id: DeploymentRevisionId,
        plan_hash: ContentHash,
        binding_hash: ContentHash,
        compiler_version: impl Into<String>,
        expression_engine_version: impl Into<String>,
        author_document: Value,
        canonical_plan: Value,
        descriptor_contracts: Value,
        resolved_bindings: Value,
        worker_contracts: Value,
    ) -> Result<Self, RepositoryError> {
        Self::build(
            definition_id,
            agent_id,
            display_name,
            definition_revision_id,
            deployment_revision_id,
            plan_hash,
            binding_hash,
            compiler_version,
            expression_engine_version,
            author_document,
            canonical_plan,
            descriptor_contracts,
            resolved_bindings,
            worker_contracts,
            PlanInputContract::new(PlanType::Any),
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn build(
        definition_id: impl Into<String>,
        agent_id: impl Into<String>,
        display_name: impl Into<String>,
        definition_revision_id: DefinitionRevisionId,
        deployment_revision_id: DeploymentRevisionId,
        plan_hash: ContentHash,
        binding_hash: ContentHash,
        compiler_version: impl Into<String>,
        expression_engine_version: impl Into<String>,
        author_document: Value,
        canonical_plan: Value,
        descriptor_contracts: Value,
        resolved_bindings: Value,
        worker_contracts: Value,
        input_contract: PlanInputContract,
    ) -> Result<Self, RepositoryError> {
        let value = Self {
            definition_id: definition_id.into(),
            agent_id: agent_id.into(),
            display_name: display_name.into(),
            public_description: String::new(),
            definition_revision_id,
            deployment_revision_id,
            plan_hash,
            binding_hash,
            compiler_version: compiler_version.into(),
            expression_engine_version: expression_engine_version.into(),
            author_document,
            canonical_plan,
            descriptor_contracts,
            resolved_bindings,
            worker_contracts,
            input_contract,
        };
        validate_body_free_label(&value.definition_id)?;
        validate_body_free_label(&value.agent_id)?;
        if value.display_name.is_empty()
            || value.display_name.len() > 256
            || value.display_name.chars().any(char::is_control)
        {
            return Err(invalid_command());
        }
        validate_body_free_label(&value.compiler_version)?;
        validate_body_free_label(&value.expression_engine_version)?;
        Ok(value)
    }

    pub fn definition_id(&self) -> &str {
        &self.definition_id
    }

    pub fn agent_id(&self) -> &str {
        &self.agent_id
    }

    pub fn display_name(&self) -> &str {
        &self.display_name
    }

    pub fn public_description(&self) -> &str {
        &self.public_description
    }

    pub(crate) fn with_public_description(
        mut self,
        description: impl Into<String>,
    ) -> Result<Self, RepositoryError> {
        self.public_description = description.into();
        if self.public_description.len() > 4096
            || self.public_description.chars().any(char::is_control)
        {
            return Err(invalid_command());
        }
        Ok(self)
    }

    pub fn definition_revision_id(&self) -> &DefinitionRevisionId {
        &self.definition_revision_id
    }

    pub fn deployment_revision_id(&self) -> &DeploymentRevisionId {
        &self.deployment_revision_id
    }

    pub fn plan_hash(&self) -> &ContentHash {
        &self.plan_hash
    }

    pub fn binding_hash(&self) -> &ContentHash {
        &self.binding_hash
    }

    pub(crate) fn compiler_version(&self) -> &str {
        &self.compiler_version
    }

    pub(crate) fn expression_engine_version(&self) -> &str {
        &self.expression_engine_version
    }

    pub(crate) fn author_document(&self) -> &Value {
        &self.author_document
    }

    pub(crate) fn canonical_plan(&self) -> &Value {
        &self.canonical_plan
    }

    pub(crate) fn descriptor_contracts(&self) -> &Value {
        &self.descriptor_contracts
    }

    pub(crate) fn resolved_bindings(&self) -> &Value {
        &self.resolved_bindings
    }

    pub(crate) fn worker_contracts(&self) -> &Value {
        &self.worker_contracts
    }

    pub(crate) fn input_contract(&self) -> &PlanInputContract {
        &self.input_contract
    }

    pub(crate) fn is_graph_authored(&self) -> bool {
        self.author_document
            .get("authoring_mode")
            .and_then(Value::as_str)
            == Some("graph")
    }
}

/// Workspace-internal constructors used by cross-crate conformance tests.
#[doc(hidden)]
pub mod adapter {
    use super::*;

    #[allow(clippy::too_many_arguments)]
    pub fn versioned_plan_for_test(
        definition_id: impl Into<String>,
        agent_id: impl Into<String>,
        display_name: impl Into<String>,
        definition_revision_id: DefinitionRevisionId,
        deployment_revision_id: DeploymentRevisionId,
        plan_hash: ContentHash,
        binding_hash: ContentHash,
        compiler_version: impl Into<String>,
        expression_engine_version: impl Into<String>,
        author_document: Value,
        canonical_plan: Value,
        descriptor_contracts: Value,
        resolved_bindings: Value,
        worker_contracts: Value,
    ) -> Result<VersionedPlan, RepositoryError> {
        VersionedPlan::new_for_test(
            definition_id,
            agent_id,
            display_name,
            definition_revision_id,
            deployment_revision_id,
            plan_hash,
            binding_hash,
            compiler_version,
            expression_engine_version,
            author_document,
            canonical_plan,
            descriptor_contracts,
            resolved_bindings,
            worker_contracts,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn versioned_plan_from_stored_parts(
        definition_id: String,
        agent_id: String,
        display_name: String,
        definition_revision_id: DefinitionRevisionId,
        deployment_revision_id: DeploymentRevisionId,
        plan_hash: ContentHash,
        binding_hash: ContentHash,
        compiler_version: String,
        expression_engine_version: String,
        public_description: String,
        author_document: Value,
        canonical_plan: Value,
        descriptor_contracts: Value,
        resolved_bindings: Value,
        worker_contracts: Value,
    ) -> Result<VersionedPlan, RepositoryError> {
        VersionedPlan::from_stored_parts(
            definition_id,
            agent_id,
            display_name,
            definition_revision_id,
            deployment_revision_id,
            plan_hash,
            binding_hash,
            compiler_version,
            expression_engine_version,
            public_description,
            author_document,
            canonical_plan,
            descriptor_contracts,
            resolved_bindings,
            worker_contracts,
        )
    }

    pub fn versioned_plan_with_public_description(
        plan: VersionedPlan,
        description: impl Into<String>,
    ) -> Result<VersionedPlan, RepositoryError> {
        plan.with_public_description(description)
    }

    pub fn versioned_plan_compiler_version(plan: &VersionedPlan) -> &str {
        plan.compiler_version()
    }

    pub fn versioned_plan_expression_engine_version(plan: &VersionedPlan) -> &str {
        plan.expression_engine_version()
    }

    pub fn versioned_plan_author_document(plan: &VersionedPlan) -> &Value {
        plan.author_document()
    }

    pub fn versioned_plan_canonical_plan(plan: &VersionedPlan) -> &Value {
        plan.canonical_plan()
    }

    pub fn versioned_plan_descriptor_contracts(plan: &VersionedPlan) -> &Value {
        plan.descriptor_contracts()
    }

    pub fn versioned_plan_resolved_bindings(plan: &VersionedPlan) -> &Value {
        plan.resolved_bindings()
    }

    pub fn versioned_plan_worker_contracts(plan: &VersionedPlan) -> &Value {
        plan.worker_contracts()
    }

    pub fn versioned_plan_input_contract(plan: &VersionedPlan) -> &PlanInputContract {
        plan.input_contract()
    }

    pub fn versioned_plan_is_graph_authored(plan: &VersionedPlan) -> bool {
        plan.is_graph_authored()
    }

    pub fn publication_origin_parse(value: &str) -> Result<PublicationOrigin, RepositoryError> {
        PublicationOrigin::parse(value)
    }

    pub fn publication_head(
        agent_id: String,
        definition_id: String,
        definition_revision_id: DefinitionRevisionId,
        deployment_revision_id: DeploymentRevisionId,
        origin: PublicationOrigin,
    ) -> Result<PublicationHead, RepositoryError> {
        PublicationHead::new(
            agent_id,
            definition_id,
            definition_revision_id,
            deployment_revision_id,
            origin,
        )
    }

    pub fn versioned_plan_catalog(
        plans: Vec<VersionedPlan>,
        heads: Vec<PublicationHead>,
    ) -> Result<VersionedPlanCatalog, RepositoryError> {
        VersionedPlanCatalog::new(plans, heads)
    }

    pub fn public_run_attachment_parse(
        value: &str,
    ) -> Result<PublicRunAttachment, RepositoryError> {
        PublicRunAttachment::parse(value)
    }

    pub fn create_run_definition_id(command: &CreateRunCommand) -> &str {
        command.definition_id()
    }

    pub fn create_run_definition_revision_id(command: &CreateRunCommand) -> &DefinitionRevisionId {
        command.definition_revision_id()
    }

    pub fn create_run_deployment_revision_id(command: &CreateRunCommand) -> &DeploymentRevisionId {
        command.deployment_revision_id()
    }

    pub fn create_run_plan_hash(command: &CreateRunCommand) -> &ContentHash {
        command.plan_hash()
    }

    pub fn create_run_binding_hash(command: &CreateRunCommand) -> &ContentHash {
        command.binding_hash()
    }

    pub fn create_run_request_id(command: &CreateRunCommand) -> &str {
        command.request_id()
    }

    pub fn create_run_attachment(command: &CreateRunCommand) -> PublicRunAttachment {
        command.attachment()
    }

    pub fn create_run_input(command: &CreateRunCommand) -> &Value {
        command.input()
    }

    pub fn create_run_timeout_ms(command: &CreateRunCommand) -> Option<u64> {
        command.run_timeout_ms()
    }

    pub fn create_run_artifact_reference_retention_seconds(command: &CreateRunCommand) -> u32 {
        command.artifact_reference_retention_seconds()
    }

    pub fn create_run_expected_publication_head(
        command: &CreateRunCommand,
    ) -> Option<&PublicationHead> {
        command.expected_publication_head()
    }

    pub fn create_run_full_conversation(
        command: &CreateRunCommand,
    ) -> Option<&FullConversationRunAdmission> {
        command.full_conversation()
    }

    pub fn public_event_intent(payload: PublicEventPayload) -> PublicEventIntent {
        PublicEventIntent::new(payload)
    }

    pub fn public_event_intent_event_kind(intent: &PublicEventIntent) -> &str {
        intent.event_kind()
    }

    pub fn public_event_intent_is_terminal(intent: &PublicEventIntent) -> bool {
        intent.is_terminal()
    }

    pub fn public_event_intent_payload(intent: &PublicEventIntent) -> &PublicEventPayload {
        intent.payload()
    }

    #[allow(clippy::too_many_arguments)]
    pub fn run_transition_nonterminal(
        run_id: RunId,
        expected_projection_version: u64,
        expected_lifecycle: RunLifecycle,
        expected_admission: AdmissionState,
        next_lifecycle: RunLifecycle,
        next_admission: AdmissionState,
        event: PendingExecutionEvent,
        public_event: Option<PublicEventIntent>,
    ) -> Result<RunTransitionCommand, RepositoryError> {
        RunTransitionCommand::nonterminal(
            run_id,
            expected_projection_version,
            expected_lifecycle,
            expected_admission,
            next_lifecycle,
            next_admission,
            event,
            public_event,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn run_transition_termination_intent(
        run_id: RunId,
        expected_projection_version: u64,
        expected_lifecycle: RunLifecycle,
        expected_admission: AdmissionState,
        reason: TerminationReason,
        event: PendingExecutionEvent,
        public_event: Option<PublicEventIntent>,
    ) -> Result<RunTransitionCommand, RepositoryError> {
        RunTransitionCommand::termination_intent(
            run_id,
            expected_projection_version,
            expected_lifecycle,
            expected_admission,
            reason,
            event,
            public_event,
        )
    }

    pub fn run_transition_terminal_success(
        run_id: RunId,
        expected_projection_version: u64,
        output: Value,
        event: PendingExecutionEvent,
        public_event: PublicEventIntent,
    ) -> Result<RunTransitionCommand, RepositoryError> {
        RunTransitionCommand::terminal_success(
            run_id,
            expected_projection_version,
            output,
            event,
            public_event,
        )
    }

    pub fn run_transition_terminal_failure(
        run_id: RunId,
        expected_projection_version: u64,
        reason: TerminationReason,
        error_code: PublicErrorCode,
        event: PendingExecutionEvent,
        public_event: PublicEventIntent,
    ) -> Result<RunTransitionCommand, RepositoryError> {
        RunTransitionCommand::terminal_failure(
            run_id,
            expected_projection_version,
            reason,
            error_code,
            event,
            public_event,
        )
    }

    pub fn run_transition_expected_lifecycle(command: &RunTransitionCommand) -> RunLifecycle {
        command.expected_lifecycle()
    }

    pub fn run_transition_expected_admission(command: &RunTransitionCommand) -> AdmissionState {
        command.expected_admission()
    }

    pub fn run_transition_next_lifecycle(command: &RunTransitionCommand) -> RunLifecycle {
        command.next_lifecycle()
    }

    pub fn run_transition_next_admission(command: &RunTransitionCommand) -> AdmissionState {
        command.next_admission()
    }

    pub fn run_transition_termination_intent_reason(
        command: &RunTransitionCommand,
    ) -> Option<&'static str> {
        command.termination_intent_reason()
    }

    pub fn run_transition_event(command: &RunTransitionCommand) -> &PendingExecutionEvent {
        command.event()
    }

    pub fn run_transition_public_event(
        command: &RunTransitionCommand,
    ) -> Option<&PublicEventIntent> {
        command.public_event()
    }

    pub fn run_transition_terminal_reason(command: &RunTransitionCommand) -> Option<&'static str> {
        command.terminal().and_then(TerminalRunMutation::reason)
    }

    pub fn run_transition_terminal_error_code(command: &RunTransitionCommand) -> Option<&str> {
        command.terminal().and_then(TerminalRunMutation::error_code)
    }

    pub fn run_transition_terminal_output(command: &RunTransitionCommand) -> Option<&Value> {
        command.terminal().and_then(TerminalRunMutation::output)
    }

    pub fn commit_receipt(
        event_seq: u64,
        event_id: String,
        projection_version: u64,
        public_event_id: Option<String>,
    ) -> CommitReceipt {
        CommitReceipt::new(event_seq, event_id, projection_version, public_event_id)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn run_projection(
        run_id: RunId,
        response_id: String,
        definition_id: String,
        agent_id: String,
        definition_revision_id: DefinitionRevisionId,
        deployment_revision_id: DeploymentRevisionId,
        plan_hash: ContentHash,
        binding_hash: ContentHash,
        request_id: String,
        attachment: PublicRunAttachment,
        lifecycle: RunLifecycle,
        admission: AdmissionState,
        projection_version: u64,
        next_event_seq: u64,
        termination_intent_reason: Option<String>,
        output: Option<Value>,
        error_code: Option<String>,
        created_at: DateTime<Utc>,
        started_at: Option<DateTime<Utc>>,
        updated_at: DateTime<Utc>,
        terminal_at: Option<DateTime<Utc>>,
        deadline_at: Option<DateTime<Utc>>,
    ) -> RunProjection {
        RunProjection::new(
            run_id,
            response_id,
            definition_id,
            agent_id,
            definition_revision_id,
            deployment_revision_id,
            plan_hash,
            binding_hash,
            request_id,
            attachment,
            lifecycle,
            admission,
            projection_version,
            next_event_seq,
            termination_intent_reason,
            output,
            error_code,
            created_at,
            started_at,
            updated_at,
            terminal_at,
            deadline_at,
        )
    }

    pub const fn termination_reason_as_str(reason: TerminationReason) -> &'static str {
        super::termination_reason_as_str(reason)
    }
}

/// Durable public-route head. Archive installation never mutates this value;
/// only explicit publication or first-start seeding may do so.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PublicationHead {
    agent_id: String,
    definition_id: String,
    definition_revision_id: DefinitionRevisionId,
    deployment_revision_id: DeploymentRevisionId,
    origin: PublicationOrigin,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PublicationOrigin {
    BuiltIn,
    Graph,
}

impl PublicationOrigin {
    pub(crate) fn parse(value: &str) -> Result<Self, RepositoryError> {
        match value {
            "built_in" => Ok(Self::BuiltIn),
            "graph" => Ok(Self::Graph),
            _ => Err(RepositoryError::invalid_data()),
        }
    }
}

impl PublicationHead {
    pub(crate) fn new(
        agent_id: String,
        definition_id: String,
        definition_revision_id: DefinitionRevisionId,
        deployment_revision_id: DeploymentRevisionId,
        origin: PublicationOrigin,
    ) -> Result<Self, RepositoryError> {
        validate_body_free_label(&agent_id)?;
        validate_body_free_label(&definition_id)?;
        Ok(Self {
            agent_id,
            definition_id,
            definition_revision_id,
            deployment_revision_id,
            origin,
        })
    }

    pub fn agent_id(&self) -> &str {
        &self.agent_id
    }

    pub fn definition_id(&self) -> &str {
        &self.definition_id
    }

    pub fn definition_revision_id(&self) -> &DefinitionRevisionId {
        &self.definition_revision_id
    }

    pub fn deployment_revision_id(&self) -> &DeploymentRevisionId {
        &self.deployment_revision_id
    }

    pub fn origin(&self) -> PublicationOrigin {
        self.origin
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct VersionedPlanCatalog {
    plans: Vec<VersionedPlan>,
    heads: Vec<PublicationHead>,
}

impl VersionedPlanCatalog {
    pub(crate) fn new(
        plans: Vec<VersionedPlan>,
        heads: Vec<PublicationHead>,
    ) -> Result<Self, RepositoryError> {
        let mut deployments = BTreeSet::new();
        for plan in &plans {
            if !deployments.insert(plan.deployment_revision_id().clone()) {
                return Err(RepositoryError::invalid_data());
            }
        }
        let mut routed_agents = BTreeSet::new();
        for head in &heads {
            let matching = plans.iter().find(|plan| {
                plan.agent_id() == head.agent_id
                    && plan.definition_id() == head.definition_id
                    && plan.definition_revision_id() == &head.definition_revision_id
                    && plan.deployment_revision_id() == &head.deployment_revision_id
            });
            if !routed_agents.insert(head.agent_id.clone())
                || matching.is_none()
                || (head.origin == PublicationOrigin::Graph
                    && matching.is_some_and(|plan| !plan.is_graph_authored()))
            {
                return Err(RepositoryError::invalid_data());
            }
        }
        Ok(Self { plans, heads })
    }

    pub fn plans(&self) -> &[VersionedPlan] {
        &self.plans
    }

    pub fn heads(&self) -> &[PublicationHead] {
        &self.heads
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlanInstallOutcome {
    Installed,
    AlreadyInstalled,
}

/// One graph publication proposal together with the durable route heads used
/// to freeze its direct subflow dependencies.
///
/// Construction proves that the expectations cover exactly the distinct
/// `durable_subflow` pins in the immutable deployment binding. Repositories
/// can therefore compare every referenced route in the publication
/// transaction without trusting a process-local catalog.
#[derive(Debug, Clone, PartialEq)]
pub struct PublishVersionedPlanCommand {
    plan: VersionedPlan,
    expected_dependency_heads: Vec<PublicationHead>,
    expected_target_publication_head: Option<PublicationHead>,
}

impl PublishVersionedPlanCommand {
    pub fn new(
        plan: VersionedPlan,
        mut expected_dependency_heads: Vec<PublicationHead>,
    ) -> Result<Self, RepositoryError> {
        expected_dependency_heads.sort_by(|left, right| left.agent_id.cmp(&right.agent_id));

        let mut expected_agents = BTreeSet::new();
        let mut expected_pins = BTreeSet::new();
        for head in &expected_dependency_heads {
            if !expected_agents.insert(head.agent_id.clone())
                || !expected_pins.insert((
                    head.definition_revision_id.as_str().to_owned(),
                    head.deployment_revision_id.as_str().to_owned(),
                ))
            {
                return Err(invalid_command());
            }
        }

        let bindings = plan
            .resolved_bindings
            .as_array()
            .ok_or_else(invalid_command)?;
        let mut actual_pins = BTreeSet::new();
        for document in bindings {
            let Some(binding) = document.get("binding").and_then(Value::as_object) else {
                continue;
            };
            if binding.get("adapter").and_then(Value::as_str) != Some("durable_subflow") {
                continue;
            }
            let definition_revision_id = binding
                .get("definition_revision_id")
                .and_then(Value::as_str)
                .ok_or_else(invalid_command)?;
            let deployment_revision_id = binding
                .get("deployment_revision_id")
                .and_then(Value::as_str)
                .ok_or_else(invalid_command)?;
            actual_pins.insert((
                definition_revision_id.to_owned(),
                deployment_revision_id.to_owned(),
            ));
        }
        if actual_pins != expected_pins {
            return Err(invalid_command());
        }

        Ok(Self {
            plan,
            expected_dependency_heads,
            expected_target_publication_head: None,
        })
    }

    /// Adds a compare-and-swap guard for the route this command advances.
    /// Repositories compare it in the same transaction, before installing any
    /// immutable archive row or changing the route head.
    pub fn with_expected_target_publication_head(
        mut self,
        head: PublicationHead,
    ) -> Result<Self, RepositoryError> {
        if head.agent_id() != self.plan.agent_id()
            || head.definition_id() != self.plan.definition_id()
        {
            return Err(invalid_command());
        }
        self.expected_target_publication_head = Some(head);
        Ok(self)
    }

    pub fn plan(&self) -> &VersionedPlan {
        &self.plan
    }

    pub fn expected_dependency_heads(&self) -> &[PublicationHead] {
        &self.expected_dependency_heads
    }

    pub fn expected_target_publication_head(&self) -> Option<&PublicationHead> {
        self.expected_target_publication_head.as_ref()
    }
}

/// The durable publication transaction either advances the route or proves
/// that at least one guarded route no longer matches the proposal's snapshot.
/// A stale proposal never installs even an archive row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlanPublicationOutcome {
    Published(PlanInstallOutcome),
    DependencyHeadChanged,
}

/// Public transport attachment recorded atomically with Run creation.
///
/// It is durable metadata, not scheduler control state: after a process
/// restart both variants resume through the same scheduler recovery path.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PublicRunAttachment {
    Attached,
    Detached,
}

impl PublicRunAttachment {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Attached => "attached",
            Self::Detached => "detached",
        }
    }

    pub(crate) fn parse(value: &str) -> Result<Self, RepositoryError> {
        match value {
            "attached" => Ok(Self::Attached),
            "detached" => Ok(Self::Detached),
            _ => Err(RepositoryError::invalid_data()),
        }
    }
}

/// Conversation metadata that is committed in the same transaction as a
/// durable/full Run admission.
///
/// This lives in the durable contract rather than the lightweight terminal
/// store so the full scheduler can preserve its existing execution authority
/// while sharing the Conversation message tables.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct FullConversationRunAdmission {
    conversation_id: String,
    tenant_id: String,
    user_id: String,
    agent_id: String,
    user_message_id: String,
    assistant_message_id: String,
    user_content_inline: Option<Value>,
    user_content_ref: Option<String>,
    user_content_hash: ContentHash,
    selected_context_hash: ContentHash,
    context_message_order: i64,
    created_at: DateTime<Utc>,
}

impl FullConversationRunAdmission {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        conversation_id: impl Into<String>,
        tenant_id: impl Into<String>,
        user_id: impl Into<String>,
        agent_id: impl Into<String>,
        user_message_id: impl Into<String>,
        assistant_message_id: impl Into<String>,
        user_content_inline: Option<Value>,
        user_content_ref: Option<String>,
        user_content_hash: ContentHash,
        selected_context_hash: ContentHash,
        context_message_order: i64,
        created_at: DateTime<Utc>,
    ) -> Result<Self, RepositoryError> {
        let admission = Self {
            conversation_id: conversation_id.into(),
            tenant_id: tenant_id.into(),
            user_id: user_id.into(),
            agent_id: agent_id.into(),
            user_message_id: user_message_id.into(),
            assistant_message_id: assistant_message_id.into(),
            user_content_inline,
            user_content_ref,
            user_content_hash,
            selected_context_hash,
            context_message_order,
            created_at,
        };
        for value in [
            &admission.conversation_id,
            &admission.tenant_id,
            &admission.user_id,
            &admission.agent_id,
            &admission.user_message_id,
            &admission.assistant_message_id,
        ] {
            validate_body_free_label(value)?;
        }
        if admission.context_message_order < 0 {
            return Err(RepositoryError::invalid_data());
        }
        if admission.user_content_inline.is_some() == admission.user_content_ref.is_some()
            || admission
                .user_content_ref
                .as_ref()
                .is_some_and(|reference| {
                    reference.is_empty()
                        || reference.len() > 16 * 1024
                        || reference.chars().any(char::is_control)
                })
        {
            return Err(RepositoryError::invalid_data());
        }
        Ok(admission)
    }

    pub fn conversation_id(&self) -> &str {
        &self.conversation_id
    }

    pub fn tenant_id(&self) -> &str {
        &self.tenant_id
    }

    pub fn user_id(&self) -> &str {
        &self.user_id
    }

    pub fn agent_id(&self) -> &str {
        &self.agent_id
    }

    pub fn user_message_id(&self) -> &str {
        &self.user_message_id
    }

    pub fn assistant_message_id(&self) -> &str {
        &self.assistant_message_id
    }

    pub fn user_content_inline(&self) -> Option<&Value> {
        self.user_content_inline.as_ref()
    }

    pub fn user_content_ref(&self) -> Option<&str> {
        self.user_content_ref.as_deref()
    }

    pub fn user_content_hash(&self) -> &ContentHash {
        &self.user_content_hash
    }

    pub fn selected_context_hash(&self) -> &ContentHash {
        &self.selected_context_hash
    }

    pub fn context_message_order(&self) -> i64 {
        self.context_message_order
    }

    pub fn created_at(&self) -> DateTime<Utc> {
        self.created_at
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct CreateRunCommand {
    run_id: RunId,
    definition_id: String,
    definition_revision_id: DefinitionRevisionId,
    deployment_revision_id: DeploymentRevisionId,
    plan_hash: ContentHash,
    binding_hash: ContentHash,
    request_id: String,
    attachment: PublicRunAttachment,
    input: Value,
    run_timeout_ms: Option<u64>,
    artifact_reference_retention_seconds: u32,
    /// When present, Run admission is an alias resolution and the repository
    /// must prove this exact durable publication head is still authoritative in
    /// the same transaction that creates the Run.
    #[serde(skip_serializing_if = "Option::is_none")]
    expected_publication_head: Option<PublicationHead>,
    #[serde(skip_serializing_if = "Option::is_none")]
    full_conversation: Option<FullConversationRunAdmission>,
}

impl CreateRunCommand {
    pub fn new(run_id: RunId, plan: &VersionedPlan, input: Value) -> Result<Self, RepositoryError> {
        let run_type = plan
            .input_contract()
            .run_type()
            .map_err(|_| invalid_command())?;
        if !run_type.accepts_literal(&input).unwrap_or(false) {
            return Err(invalid_command());
        }
        let request_id = run_id.as_str().to_owned();
        Ok(Self {
            run_id,
            definition_id: plan.definition_id.clone(),
            definition_revision_id: plan.definition_revision_id.clone(),
            deployment_revision_id: plan.deployment_revision_id.clone(),
            plan_hash: plan.plan_hash.clone(),
            binding_hash: plan.binding_hash.clone(),
            request_id,
            attachment: PublicRunAttachment::Detached,
            input,
            run_timeout_ms: None,
            artifact_reference_retention_seconds: 30 * 24 * 60 * 60,
            expected_publication_head: None,
            full_conversation: None,
        })
    }

    pub fn with_expected_publication_head(
        mut self,
        head: PublicationHead,
    ) -> Result<Self, RepositoryError> {
        if head.definition_id() != self.definition_id
            || head.definition_revision_id() != &self.definition_revision_id
            || head.deployment_revision_id() != &self.deployment_revision_id
        {
            return Err(invalid_command());
        }
        self.expected_publication_head = Some(head);
        Ok(self)
    }

    pub fn with_run_timeout(mut self, timeout: Duration) -> Result<Self, RepositoryError> {
        let milliseconds = u64::try_from(timeout.as_millis()).map_err(|_| invalid_command())?;
        if milliseconds == 0 || milliseconds > i64::MAX as u64 {
            return Err(invalid_command());
        }
        self.run_timeout_ms = Some(milliseconds);
        Ok(self)
    }

    /// Freezes the referenced-Artifact retention policy into Run admission.
    /// Terminal ownership must never consult mutable process configuration.
    pub fn with_artifact_reference_retention(
        mut self,
        retention: Duration,
    ) -> Result<Self, RepositoryError> {
        let seconds = u32::try_from(retention.as_secs()).map_err(|_| invalid_command())?;
        if seconds == 0 || seconds > 10 * 365 * 24 * 60 * 60 {
            return Err(invalid_command());
        }
        self.artifact_reference_retention_seconds = seconds;
        Ok(self)
    }

    pub fn with_public_metadata(
        mut self,
        request_id: impl Into<String>,
        attachment: PublicRunAttachment,
    ) -> Result<Self, RepositoryError> {
        let request_id = request_id.into();
        if request_id.is_empty()
            || request_id.len() > 256
            || request_id.chars().any(char::is_control)
        {
            return Err(invalid_command());
        }
        self.request_id = request_id;
        self.attachment = attachment;
        Ok(self)
    }

    pub fn with_full_conversation(
        mut self,
        conversation: FullConversationRunAdmission,
    ) -> Result<Self, RepositoryError> {
        self.full_conversation = Some(conversation);
        Ok(self)
    }

    pub fn run_id(&self) -> &RunId {
        &self.run_id
    }

    pub(crate) fn definition_id(&self) -> &str {
        &self.definition_id
    }

    pub(crate) fn definition_revision_id(&self) -> &DefinitionRevisionId {
        &self.definition_revision_id
    }

    pub(crate) fn deployment_revision_id(&self) -> &DeploymentRevisionId {
        &self.deployment_revision_id
    }

    pub(crate) fn plan_hash(&self) -> &ContentHash {
        &self.plan_hash
    }

    pub(crate) fn binding_hash(&self) -> &ContentHash {
        &self.binding_hash
    }

    pub(crate) fn request_id(&self) -> &str {
        &self.request_id
    }

    pub(crate) fn attachment(&self) -> PublicRunAttachment {
        self.attachment
    }

    pub(crate) fn input(&self) -> &Value {
        &self.input
    }

    pub(crate) fn run_timeout_ms(&self) -> Option<u64> {
        self.run_timeout_ms
    }

    pub(crate) fn artifact_reference_retention_seconds(&self) -> u32 {
        self.artifact_reference_retention_seconds
    }

    pub(crate) fn expected_publication_head(&self) -> Option<&PublicationHead> {
        self.expected_publication_head.as_ref()
    }

    pub(crate) fn full_conversation(&self) -> Option<&FullConversationRunAdmission> {
        self.full_conversation.as_ref()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct PublicEventIntent {
    payload: PublicEventPayload,
}

impl PublicEventIntent {
    /// Scheduler/repository adapter constructor. Public payloads are closed;
    /// provider bodies and arbitrary JSON have no representation here.
    pub(crate) fn new(payload: PublicEventPayload) -> Self {
        Self { payload }
    }

    pub(crate) fn event_kind(&self) -> &str {
        self.payload.kind().as_str()
    }

    pub(crate) fn is_terminal(&self) -> bool {
        matches!(
            self.payload,
            PublicEventPayload::RunCompleted
                | PublicEventPayload::RunFailed { .. }
                | PublicEventPayload::RunCancelled { .. }
                | PublicEventPayload::RunInterrupted { .. }
        )
    }

    pub(crate) fn payload(&self) -> &PublicEventPayload {
        &self.payload
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "terminal_kind", rename_all = "snake_case")]
#[allow(dead_code)]
pub(crate) enum TerminalRunMutation {
    Succeeded {
        output: Value,
    },
    Failed {
        reason: TerminationReason,
        error_code: PublicErrorCode,
    },
}

impl TerminalRunMutation {
    pub(crate) fn reason(&self) -> Option<&'static str> {
        match self {
            Self::Succeeded { .. } => None,
            Self::Failed { reason, .. } => Some(termination_reason_as_str(*reason)),
        }
    }

    pub(crate) fn error_code(&self) -> Option<&str> {
        match self {
            Self::Succeeded { .. } => None,
            Self::Failed { error_code, .. } => Some(error_code.as_str()),
        }
    }

    pub(crate) fn output(&self) -> Option<&Value> {
        match self {
            Self::Succeeded { output } => Some(output),
            Self::Failed { .. } => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct RunTransitionCommand {
    run_id: RunId,
    expected_projection_version: u64,
    expected_lifecycle: RunLifecycle,
    expected_admission: AdmissionState,
    next_lifecycle: RunLifecycle,
    next_admission: AdmissionState,
    termination_intent_reason: Option<TerminationReason>,
    terminal: Option<TerminalRunMutation>,
    event: PendingExecutionEvent,
    public_event: Option<PublicEventIntent>,
}

impl RunTransitionCommand {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn nonterminal(
        run_id: RunId,
        expected_projection_version: u64,
        expected_lifecycle: RunLifecycle,
        expected_admission: AdmissionState,
        next_lifecycle: RunLifecycle,
        next_admission: AdmissionState,
        event: PendingExecutionEvent,
        public_event: Option<PublicEventIntent>,
    ) -> Result<Self, RepositoryError> {
        let lifecycle_allowed = expected_lifecycle == next_lifecycle
            || matches!(
                (expected_lifecycle, next_lifecycle),
                (RunLifecycle::Created, RunLifecycle::Active)
                    | (RunLifecycle::Active, RunLifecycle::Waiting)
                    | (RunLifecycle::Waiting, RunLifecycle::Active)
                    | (RunLifecycle::Active, RunLifecycle::Completing)
                    | (RunLifecycle::Waiting, RunLifecycle::Completing)
            );
        if expected_lifecycle.is_terminal()
            || expected_lifecycle == RunLifecycle::Terminating
            || next_lifecycle.is_terminal()
            || next_lifecycle == RunLifecycle::Terminating
            || !lifecycle_allowed
            || (next_lifecycle == RunLifecycle::Completing
                && !matches!(
                    next_admission,
                    AdmissionState::Draining | AdmissionState::Closed
                ))
            || (next_lifecycle != RunLifecycle::Completing
                && matches!(
                    next_admission,
                    AdmissionState::Draining | AdmissionState::Closed
                ))
        {
            return Err(invalid_command());
        }
        Self::build(
            run_id,
            expected_projection_version,
            expected_lifecycle,
            expected_admission,
            next_lifecycle,
            next_admission,
            None,
            None,
            event,
            public_event,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn termination_intent(
        run_id: RunId,
        expected_projection_version: u64,
        expected_lifecycle: RunLifecycle,
        expected_admission: AdmissionState,
        reason: TerminationReason,
        event: PendingExecutionEvent,
        public_event: Option<PublicEventIntent>,
    ) -> Result<Self, RepositoryError> {
        if expected_lifecycle.is_terminal() || expected_lifecycle == RunLifecycle::Terminating {
            return Err(invalid_command());
        }
        Self::build(
            run_id,
            expected_projection_version,
            expected_lifecycle,
            expected_admission,
            RunLifecycle::Terminating,
            AdmissionState::Draining,
            Some(reason),
            None,
            event,
            public_event,
        )
    }

    #[allow(dead_code, clippy::too_many_arguments)]
    pub(crate) fn terminal_success(
        run_id: RunId,
        expected_projection_version: u64,
        output: Value,
        event: PendingExecutionEvent,
        public_event: PublicEventIntent,
    ) -> Result<Self, RepositoryError> {
        if !public_event.is_terminal() {
            return Err(invalid_command());
        }
        Self::build(
            run_id,
            expected_projection_version,
            RunLifecycle::Completing,
            AdmissionState::Draining,
            RunLifecycle::Succeeded,
            AdmissionState::Closed,
            None,
            Some(TerminalRunMutation::Succeeded { output }),
            event,
            Some(public_event),
        )
    }

    #[allow(dead_code, clippy::too_many_arguments)]
    pub(crate) fn terminal_failure(
        run_id: RunId,
        expected_projection_version: u64,
        reason: TerminationReason,
        error_code: PublicErrorCode,
        event: PendingExecutionEvent,
        public_event: PublicEventIntent,
    ) -> Result<Self, RepositoryError> {
        if !public_event.is_terminal() {
            return Err(invalid_command());
        }
        let terminal_lifecycle = terminal_lifecycle_for(reason);
        Self::build(
            run_id,
            expected_projection_version,
            RunLifecycle::Terminating,
            AdmissionState::Draining,
            terminal_lifecycle,
            AdmissionState::Closed,
            Some(reason),
            Some(TerminalRunMutation::Failed { reason, error_code }),
            event,
            Some(public_event),
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn build(
        run_id: RunId,
        expected_projection_version: u64,
        expected_lifecycle: RunLifecycle,
        expected_admission: AdmissionState,
        next_lifecycle: RunLifecycle,
        next_admission: AdmissionState,
        termination_intent_reason: Option<TerminationReason>,
        terminal: Option<TerminalRunMutation>,
        event: PendingExecutionEvent,
        public_event: Option<PublicEventIntent>,
    ) -> Result<Self, RepositoryError> {
        if event.context().run_id() != &run_id {
            return Err(invalid_command());
        }
        if public_event
            .as_ref()
            .is_some_and(|event| event.is_terminal() != next_lifecycle.is_terminal())
        {
            return Err(invalid_command());
        }
        Ok(Self {
            run_id,
            expected_projection_version,
            expected_lifecycle,
            expected_admission,
            next_lifecycle,
            next_admission,
            termination_intent_reason,
            terminal,
            event,
            public_event,
        })
    }

    pub fn run_id(&self) -> &RunId {
        &self.run_id
    }

    pub fn expected_projection_version(&self) -> u64 {
        self.expected_projection_version
    }

    pub(crate) fn expected_lifecycle(&self) -> RunLifecycle {
        self.expected_lifecycle
    }

    pub(crate) fn expected_admission(&self) -> AdmissionState {
        self.expected_admission
    }

    pub(crate) fn next_lifecycle(&self) -> RunLifecycle {
        self.next_lifecycle
    }

    pub(crate) fn next_admission(&self) -> AdmissionState {
        self.next_admission
    }

    pub(crate) fn termination_intent_reason(&self) -> Option<&'static str> {
        self.termination_intent_reason
            .map(termination_reason_as_str)
    }

    pub(crate) fn terminal(&self) -> Option<&TerminalRunMutation> {
        self.terminal.as_ref()
    }

    pub(crate) fn event(&self) -> &PendingExecutionEvent {
        &self.event
    }

    pub(crate) fn public_event(&self) -> Option<&PublicEventIntent> {
        self.public_event.as_ref()
    }
}

pub(crate) const fn termination_reason_as_str(reason: TerminationReason) -> &'static str {
    match reason {
        TerminationReason::Failure => "failure",
        TerminationReason::Cancelled => "cancelled",
        TerminationReason::Interrupted => "interrupted",
        TerminationReason::TimedOut => "timed_out",
    }
}

#[allow(dead_code)]
const fn terminal_lifecycle_for(reason: TerminationReason) -> RunLifecycle {
    match reason {
        TerminationReason::Failure => RunLifecycle::Failed,
        TerminationReason::Cancelled => RunLifecycle::Cancelled,
        TerminationReason::Interrupted => RunLifecycle::Interrupted,
        TerminationReason::TimedOut => RunLifecycle::TimedOut,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitReceipt {
    event_seq: u64,
    event_id: String,
    projection_version: u64,
    public_event_id: Option<String>,
}

impl CommitReceipt {
    pub(crate) fn new(
        event_seq: u64,
        event_id: String,
        projection_version: u64,
        public_event_id: Option<String>,
    ) -> Self {
        Self {
            event_seq,
            event_id,
            projection_version,
            public_event_id,
        }
    }

    pub fn event_seq(&self) -> u64 {
        self.event_seq
    }

    pub fn event_id(&self) -> &str {
        &self.event_id
    }

    pub fn projection_version(&self) -> u64 {
        self.projection_version
    }

    pub fn public_event_id(&self) -> Option<&str> {
        self.public_event_id.as_deref()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunProjection {
    run_id: RunId,
    response_id: String,
    definition_id: String,
    agent_id: String,
    definition_revision_id: DefinitionRevisionId,
    deployment_revision_id: DeploymentRevisionId,
    plan_hash: ContentHash,
    binding_hash: ContentHash,
    request_id: String,
    attachment: PublicRunAttachment,
    lifecycle: RunLifecycle,
    admission: AdmissionState,
    projection_version: u64,
    next_event_seq: u64,
    termination_intent_reason: Option<String>,
    output: Option<Value>,
    error_code: Option<String>,
    created_at: DateTime<Utc>,
    started_at: Option<DateTime<Utc>>,
    updated_at: DateTime<Utc>,
    terminal_at: Option<DateTime<Utc>>,
    deadline_at: Option<DateTime<Utc>>,
}

impl RunProjection {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        run_id: RunId,
        response_id: String,
        definition_id: String,
        agent_id: String,
        definition_revision_id: DefinitionRevisionId,
        deployment_revision_id: DeploymentRevisionId,
        plan_hash: ContentHash,
        binding_hash: ContentHash,
        request_id: String,
        attachment: PublicRunAttachment,
        lifecycle: RunLifecycle,
        admission: AdmissionState,
        projection_version: u64,
        next_event_seq: u64,
        termination_intent_reason: Option<String>,
        output: Option<Value>,
        error_code: Option<String>,
        created_at: DateTime<Utc>,
        started_at: Option<DateTime<Utc>>,
        updated_at: DateTime<Utc>,
        terminal_at: Option<DateTime<Utc>>,
        deadline_at: Option<DateTime<Utc>>,
    ) -> Self {
        Self {
            run_id,
            response_id,
            definition_id,
            agent_id,
            definition_revision_id,
            deployment_revision_id,
            plan_hash,
            binding_hash,
            request_id,
            attachment,
            lifecycle,
            admission,
            projection_version,
            next_event_seq,
            termination_intent_reason,
            output,
            error_code,
            created_at,
            started_at,
            updated_at,
            terminal_at,
            deadline_at,
        }
    }

    pub fn run_id(&self) -> &RunId {
        &self.run_id
    }

    pub fn response_id(&self) -> &str {
        &self.response_id
    }

    pub fn definition_id(&self) -> &str {
        &self.definition_id
    }

    pub fn agent_id(&self) -> &str {
        &self.agent_id
    }

    pub fn definition_revision_id(&self) -> &DefinitionRevisionId {
        &self.definition_revision_id
    }

    pub fn deployment_revision_id(&self) -> &DeploymentRevisionId {
        &self.deployment_revision_id
    }

    pub fn plan_hash(&self) -> &ContentHash {
        &self.plan_hash
    }

    pub fn binding_hash(&self) -> &ContentHash {
        &self.binding_hash
    }

    pub fn request_id(&self) -> &str {
        &self.request_id
    }

    pub fn attachment(&self) -> PublicRunAttachment {
        self.attachment
    }

    pub fn lifecycle(&self) -> RunLifecycle {
        self.lifecycle
    }

    pub fn admission(&self) -> AdmissionState {
        self.admission
    }

    pub fn projection_version(&self) -> u64 {
        self.projection_version
    }

    pub fn next_event_seq(&self) -> u64 {
        self.next_event_seq
    }

    pub fn termination_intent_reason(&self) -> Option<&str> {
        self.termination_intent_reason.as_deref()
    }

    pub fn output(&self) -> Option<&Value> {
        self.output.as_ref()
    }

    pub fn error_code(&self) -> Option<&str> {
        self.error_code.as_deref()
    }

    pub fn created_at(&self) -> DateTime<Utc> {
        self.created_at
    }

    pub fn started_at(&self) -> Option<DateTime<Utc>> {
        self.started_at
    }

    pub fn updated_at(&self) -> DateTime<Utc> {
        self.updated_at
    }

    pub fn terminal_at(&self) -> Option<DateTime<Utc>> {
        self.terminal_at
    }

    pub fn deadline_at(&self) -> Option<DateTime<Utc>> {
        self.deadline_at
    }
}
