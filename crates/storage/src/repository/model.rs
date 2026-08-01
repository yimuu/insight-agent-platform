use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use serde_json::Value;

use insight_durable::model::adapter as durable_model_adapter;
pub use insight_durable::{
    CommitReceipt, CreateRunCommand, FullConversationRunAdmission, PublicEventIntent,
    PublicRunAttachment, PublicationHead, PublicationOrigin, RunProjection, RunTransitionCommand,
    VersionedPlan, VersionedPlanCatalog,
};

use super::RepositoryError;
use insight_engine::{
    plan::PlanInputContract, AdmissionState, ContentHash, DefinitionRevisionId,
    DeploymentRevisionId, PendingExecutionEvent, PublicErrorCode, PublicEventPayload, RunId,
    RunLifecycle, TerminationReason,
};

#[allow(dead_code)]
pub(crate) trait VersionedPlanAdapter: Sized {
    #[allow(clippy::too_many_arguments)]
    fn from_stored_parts(
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
    ) -> Result<Self, RepositoryError>;
    fn with_public_description(
        self,
        description: impl Into<String>,
    ) -> Result<Self, RepositoryError>;
    fn compiler_version(&self) -> &str;
    fn expression_engine_version(&self) -> &str;
    fn author_document(&self) -> &Value;
    fn canonical_plan(&self) -> &Value;
    fn descriptor_contracts(&self) -> &Value;
    fn resolved_bindings(&self) -> &Value;
    fn worker_contracts(&self) -> &Value;
    fn input_contract(&self) -> &PlanInputContract;
    fn is_graph_authored(&self) -> bool;
}

impl VersionedPlanAdapter for VersionedPlan {
    fn from_stored_parts(
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
        durable_model_adapter::versioned_plan_from_stored_parts(
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

    fn with_public_description(
        self,
        description: impl Into<String>,
    ) -> Result<Self, RepositoryError> {
        durable_model_adapter::versioned_plan_with_public_description(self, description)
    }

    fn compiler_version(&self) -> &str {
        durable_model_adapter::versioned_plan_compiler_version(self)
    }
    fn expression_engine_version(&self) -> &str {
        durable_model_adapter::versioned_plan_expression_engine_version(self)
    }
    fn author_document(&self) -> &Value {
        durable_model_adapter::versioned_plan_author_document(self)
    }
    fn canonical_plan(&self) -> &Value {
        durable_model_adapter::versioned_plan_canonical_plan(self)
    }
    fn descriptor_contracts(&self) -> &Value {
        durable_model_adapter::versioned_plan_descriptor_contracts(self)
    }
    fn resolved_bindings(&self) -> &Value {
        durable_model_adapter::versioned_plan_resolved_bindings(self)
    }
    fn worker_contracts(&self) -> &Value {
        durable_model_adapter::versioned_plan_worker_contracts(self)
    }
    fn input_contract(&self) -> &PlanInputContract {
        durable_model_adapter::versioned_plan_input_contract(self)
    }
    fn is_graph_authored(&self) -> bool {
        durable_model_adapter::versioned_plan_is_graph_authored(self)
    }
}

pub(crate) trait PublicationOriginAdapter: Sized {
    fn parse(value: &str) -> Result<Self, RepositoryError>;
}

impl PublicationOriginAdapter for PublicationOrigin {
    fn parse(value: &str) -> Result<Self, RepositoryError> {
        durable_model_adapter::publication_origin_parse(value)
    }
}

pub(crate) trait PublicationHeadAdapter: Sized {
    fn new(
        agent_id: String,
        definition_id: String,
        definition_revision_id: DefinitionRevisionId,
        deployment_revision_id: DeploymentRevisionId,
        origin: PublicationOrigin,
    ) -> Result<Self, RepositoryError>;
}

impl PublicationHeadAdapter for PublicationHead {
    fn new(
        agent_id: String,
        definition_id: String,
        definition_revision_id: DefinitionRevisionId,
        deployment_revision_id: DeploymentRevisionId,
        origin: PublicationOrigin,
    ) -> Result<Self, RepositoryError> {
        durable_model_adapter::publication_head(
            agent_id,
            definition_id,
            definition_revision_id,
            deployment_revision_id,
            origin,
        )
    }
}

pub(crate) trait VersionedPlanCatalogAdapter: Sized {
    fn new(plans: Vec<VersionedPlan>, heads: Vec<PublicationHead>)
        -> Result<Self, RepositoryError>;
}

impl VersionedPlanCatalogAdapter for VersionedPlanCatalog {
    fn new(
        plans: Vec<VersionedPlan>,
        heads: Vec<PublicationHead>,
    ) -> Result<Self, RepositoryError> {
        durable_model_adapter::versioned_plan_catalog(plans, heads)
    }
}

pub(crate) trait PublicRunAttachmentAdapter: Sized {
    fn parse(value: &str) -> Result<Self, RepositoryError>;
}

impl PublicRunAttachmentAdapter for PublicRunAttachment {
    fn parse(value: &str) -> Result<Self, RepositoryError> {
        durable_model_adapter::public_run_attachment_parse(value)
    }
}

pub(crate) trait CreateRunCommandAdapter {
    fn definition_id(&self) -> &str;
    fn definition_revision_id(&self) -> &DefinitionRevisionId;
    fn deployment_revision_id(&self) -> &DeploymentRevisionId;
    fn plan_hash(&self) -> &ContentHash;
    fn binding_hash(&self) -> &ContentHash;
    fn request_id(&self) -> &str;
    fn attachment(&self) -> PublicRunAttachment;
    fn input(&self) -> &Value;
    fn run_timeout_ms(&self) -> Option<u64>;
    fn artifact_reference_retention_seconds(&self) -> u32;
    fn expected_publication_head(&self) -> Option<&PublicationHead>;
    fn expected_mcp_server_fences(&self) -> &BTreeMap<String, u64>;
    fn expected_provider_fences(&self) -> &BTreeMap<String, u64>;
    fn full_conversation(&self) -> Option<&FullConversationRunAdmission>;
}

impl CreateRunCommandAdapter for CreateRunCommand {
    fn definition_id(&self) -> &str {
        durable_model_adapter::create_run_definition_id(self)
    }
    fn definition_revision_id(&self) -> &DefinitionRevisionId {
        durable_model_adapter::create_run_definition_revision_id(self)
    }
    fn deployment_revision_id(&self) -> &DeploymentRevisionId {
        durable_model_adapter::create_run_deployment_revision_id(self)
    }
    fn plan_hash(&self) -> &ContentHash {
        durable_model_adapter::create_run_plan_hash(self)
    }
    fn binding_hash(&self) -> &ContentHash {
        durable_model_adapter::create_run_binding_hash(self)
    }
    fn request_id(&self) -> &str {
        durable_model_adapter::create_run_request_id(self)
    }
    fn attachment(&self) -> PublicRunAttachment {
        durable_model_adapter::create_run_attachment(self)
    }
    fn input(&self) -> &Value {
        durable_model_adapter::create_run_input(self)
    }
    fn run_timeout_ms(&self) -> Option<u64> {
        durable_model_adapter::create_run_timeout_ms(self)
    }
    fn artifact_reference_retention_seconds(&self) -> u32 {
        durable_model_adapter::create_run_artifact_reference_retention_seconds(self)
    }
    fn expected_publication_head(&self) -> Option<&PublicationHead> {
        durable_model_adapter::create_run_expected_publication_head(self)
    }
    fn expected_mcp_server_fences(&self) -> &BTreeMap<String, u64> {
        durable_model_adapter::create_run_expected_mcp_server_fences(self)
    }
    fn expected_provider_fences(&self) -> &BTreeMap<String, u64> {
        durable_model_adapter::create_run_expected_provider_fences(self)
    }
    fn full_conversation(&self) -> Option<&FullConversationRunAdmission> {
        durable_model_adapter::create_run_full_conversation(self)
    }
}

pub(crate) trait PublicEventIntentAdapter: Sized {
    #[cfg(test)]
    fn new(payload: PublicEventPayload) -> Self;
    fn event_kind(&self) -> &str;
    fn is_terminal(&self) -> bool;
    fn payload(&self) -> &PublicEventPayload;
}

impl PublicEventIntentAdapter for PublicEventIntent {
    #[cfg(test)]
    fn new(payload: PublicEventPayload) -> Self {
        durable_model_adapter::public_event_intent(payload)
    }
    fn event_kind(&self) -> &str {
        durable_model_adapter::public_event_intent_event_kind(self)
    }
    fn is_terminal(&self) -> bool {
        durable_model_adapter::public_event_intent_is_terminal(self)
    }
    fn payload(&self) -> &PublicEventPayload {
        durable_model_adapter::public_event_intent_payload(self)
    }
}

#[allow(dead_code)]
pub(crate) trait RunTransitionCommandAdapter: Sized {
    #[allow(clippy::too_many_arguments)]
    fn nonterminal(
        run_id: RunId,
        expected_projection_version: u64,
        expected_lifecycle: RunLifecycle,
        expected_admission: AdmissionState,
        next_lifecycle: RunLifecycle,
        next_admission: AdmissionState,
        event: PendingExecutionEvent,
        public_event: Option<PublicEventIntent>,
    ) -> Result<Self, RepositoryError>;
    #[allow(clippy::too_many_arguments)]
    fn termination_intent(
        run_id: RunId,
        expected_projection_version: u64,
        expected_lifecycle: RunLifecycle,
        expected_admission: AdmissionState,
        reason: TerminationReason,
        event: PendingExecutionEvent,
        public_event: Option<PublicEventIntent>,
    ) -> Result<Self, RepositoryError>;
    fn terminal_success(
        run_id: RunId,
        expected_projection_version: u64,
        output: Value,
        event: PendingExecutionEvent,
        public_event: PublicEventIntent,
    ) -> Result<Self, RepositoryError>;
    fn terminal_failure(
        run_id: RunId,
        expected_projection_version: u64,
        reason: TerminationReason,
        error_code: PublicErrorCode,
        event: PendingExecutionEvent,
        public_event: PublicEventIntent,
    ) -> Result<Self, RepositoryError>;
    fn expected_lifecycle(&self) -> RunLifecycle;
    fn expected_admission(&self) -> AdmissionState;
    fn next_lifecycle(&self) -> RunLifecycle;
    fn next_admission(&self) -> AdmissionState;
    fn termination_intent_reason(&self) -> Option<&'static str>;
    fn event(&self) -> &PendingExecutionEvent;
    fn public_event(&self) -> Option<&PublicEventIntent>;
}

impl RunTransitionCommandAdapter for RunTransitionCommand {
    fn nonterminal(
        run_id: RunId,
        expected_projection_version: u64,
        expected_lifecycle: RunLifecycle,
        expected_admission: AdmissionState,
        next_lifecycle: RunLifecycle,
        next_admission: AdmissionState,
        event: PendingExecutionEvent,
        public_event: Option<PublicEventIntent>,
    ) -> Result<Self, RepositoryError> {
        durable_model_adapter::run_transition_nonterminal(
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
    fn termination_intent(
        run_id: RunId,
        expected_projection_version: u64,
        expected_lifecycle: RunLifecycle,
        expected_admission: AdmissionState,
        reason: TerminationReason,
        event: PendingExecutionEvent,
        public_event: Option<PublicEventIntent>,
    ) -> Result<Self, RepositoryError> {
        durable_model_adapter::run_transition_termination_intent(
            run_id,
            expected_projection_version,
            expected_lifecycle,
            expected_admission,
            reason,
            event,
            public_event,
        )
    }
    fn terminal_success(
        run_id: RunId,
        expected_projection_version: u64,
        output: Value,
        event: PendingExecutionEvent,
        public_event: PublicEventIntent,
    ) -> Result<Self, RepositoryError> {
        durable_model_adapter::run_transition_terminal_success(
            run_id,
            expected_projection_version,
            output,
            event,
            public_event,
        )
    }
    fn terminal_failure(
        run_id: RunId,
        expected_projection_version: u64,
        reason: TerminationReason,
        error_code: PublicErrorCode,
        event: PendingExecutionEvent,
        public_event: PublicEventIntent,
    ) -> Result<Self, RepositoryError> {
        durable_model_adapter::run_transition_terminal_failure(
            run_id,
            expected_projection_version,
            reason,
            error_code,
            event,
            public_event,
        )
    }
    fn expected_lifecycle(&self) -> RunLifecycle {
        durable_model_adapter::run_transition_expected_lifecycle(self)
    }
    fn expected_admission(&self) -> AdmissionState {
        durable_model_adapter::run_transition_expected_admission(self)
    }
    fn next_lifecycle(&self) -> RunLifecycle {
        durable_model_adapter::run_transition_next_lifecycle(self)
    }
    fn next_admission(&self) -> AdmissionState {
        durable_model_adapter::run_transition_next_admission(self)
    }
    fn termination_intent_reason(&self) -> Option<&'static str> {
        durable_model_adapter::run_transition_termination_intent_reason(self)
    }
    fn event(&self) -> &PendingExecutionEvent {
        durable_model_adapter::run_transition_event(self)
    }
    fn public_event(&self) -> Option<&PublicEventIntent> {
        durable_model_adapter::run_transition_public_event(self)
    }
}

pub(crate) trait CommitReceiptAdapter: Sized {
    fn new(
        event_seq: u64,
        event_id: String,
        projection_version: u64,
        public_event_id: Option<String>,
    ) -> Self;
}

impl CommitReceiptAdapter for CommitReceipt {
    fn new(
        event_seq: u64,
        event_id: String,
        projection_version: u64,
        public_event_id: Option<String>,
    ) -> Self {
        durable_model_adapter::commit_receipt(
            event_seq,
            event_id,
            projection_version,
            public_event_id,
        )
    }
}

pub(crate) trait RunProjectionAdapter: Sized {
    #[allow(clippy::too_many_arguments)]
    fn new(
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
    ) -> Self;
}

impl RunProjectionAdapter for RunProjection {
    fn new(
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
        durable_model_adapter::run_projection(
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
}
