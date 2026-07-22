use super::RepositoryErrorExt as _;

use std::{collections::BTreeSet, time::Duration};

use async_trait::async_trait;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;

use insight_engine::{
    ContentHash, ExecutionEventId, ExecutionRevisionPin, MigrationNodeMapping, ModelError, RunId,
    RunLineage, SchedulerCheckpointId, SchedulerTaskId, TransitionKey, TransitionOutcome,
    RECOVERY_MIGRATION_SCHEMA_INCOMPATIBLE,
};

use super::{
    CreateReuseCandidateCommand, PublicationHead, RepositoryError, REPOSITORY_CONFIGURATION_INVALID,
};

const MAX_RECOVERY_CANDIDATES: usize = 10_000;
const MAX_MIGRATION_MAPPINGS: usize = 10_000;
const DEFAULT_RECOVERY_RUN_TIMEOUT_MS: u64 = 5 * 60 * 1_000;
const MAX_RECOVERY_RUN_TIMEOUT_MS: u64 = 10 * 365 * 24 * 60 * 60 * 1_000;

pub(crate) fn recovery_task_completion_checkpoint(
    task_id: &SchedulerTaskId,
) -> SchedulerCheckpointId {
    let digest = ContentHash::from_bytes(
        format!(
            "insight-agent/scheduler/task-completed/v1/{}",
            task_id.as_str()
        )
        .as_bytes(),
    );
    SchedulerCheckpointId::parse(format!(
        "checkpoint_{}",
        digest
            .as_str()
            .strip_prefix("sha256:")
            .unwrap_or(digest.as_str())
    ))
    .expect("a SHA-256 scheduler checkpoint is valid")
}

pub(crate) fn recovery_candidate_id(
    source_run_id: &RunId,
    target_run_id: &RunId,
    source_activation_id: &insight_engine::ActivationId,
) -> Result<String, RepositoryError> {
    let identity = TransitionKey::derive(
        "repository.recovery.derived_candidate",
        &[
            source_run_id.as_str(),
            target_run_id.as_str(),
            source_activation_id.as_str(),
        ],
    )
    .map_err(|_| invalid_command())?;
    Ok(format!(
        "candidate_{}",
        identity
            .as_str()
            .strip_prefix("transition_")
            .unwrap_or(identity.as_str())
    ))
}

fn duration_ms(timeout: Duration) -> Result<u64, RepositoryError> {
    let milliseconds = u64::try_from(timeout.as_millis()).map_err(|_| invalid_command())?;
    if milliseconds == 0 || milliseconds > MAX_RECOVERY_RUN_TIMEOUT_MS {
        return Err(invalid_command());
    }
    Ok(milliseconds)
}

fn invalid_command() -> RepositoryError {
    RepositoryError::new(
        REPOSITORY_CONFIGURATION_INVALID,
        "durable recovery command is invalid",
    )
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
struct RecoveryLabel(String);

impl RecoveryLabel {
    fn new(value: impl Into<String>) -> Result<Self, RepositoryError> {
        let value = value.into();
        if value.is_empty()
            || value.len() > 256
            || value
                .chars()
                .any(|character| character.is_control() || character.is_whitespace())
        {
            return Err(invalid_command());
        }
        Ok(Self(value))
    }

    fn as_str(&self) -> &str {
        &self.0
    }
}

impl<'de> Deserialize<'de> for RecoveryLabel {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::new(value).map_err(serde::de::Error::custom)
    }
}

/// Closed identity of one already-installed execution revision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecoveryRevisionSpec {
    definition_id: RecoveryLabel,
    revision: ExecutionRevisionPin,
}

impl RecoveryRevisionSpec {
    pub fn new(
        definition_id: impl Into<String>,
        revision: ExecutionRevisionPin,
    ) -> Result<Self, RepositoryError> {
        Ok(Self {
            definition_id: RecoveryLabel::new(definition_id)?,
            revision,
        })
    }

    pub fn definition_id(&self) -> &str {
        self.definition_id.as_str()
    }

    pub fn revision(&self) -> &ExecutionRevisionPin {
        &self.revision
    }
}

/// Source and target contracts for one migrated node. Compatibility is derived
/// from the closed hashes and rebuild declarations; callers cannot assert it
/// with a boolean proof.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MigrationMappingCompatibility {
    source: MigrationNodeMapping,
    target: MigrationNodeMapping,
}

impl MigrationMappingCompatibility {
    pub fn new(
        source: MigrationNodeMapping,
        target: MigrationNodeMapping,
    ) -> Result<Self, ModelError> {
        if source.source_node_id() != target.source_node_id()
            || source.target_node_id() != target.target_node_id()
            || !source.is_compatible_with(&target)
        {
            return Err(insight_engine::internal::model_error(
                RECOVERY_MIGRATION_SCHEMA_INCOMPATIBLE,
                "migration node schemas, effect policy, or wait/timer rebuild contract differ",
            ));
        }
        Ok(Self { source, target })
    }

    pub fn source(&self) -> &MigrationNodeMapping {
        &self.source
    }

    pub fn target(&self) -> &MigrationNodeMapping {
        &self.target
    }

    pub(crate) fn validate(&self) -> Result<(), RepositoryError> {
        Self::new(self.source.clone(), self.target.clone())
            .map(|_| ())
            .map_err(|_| invalid_command())
    }
}

impl<'de> Deserialize<'de> for MigrationMappingCompatibility {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            source: MigrationNodeMapping,
            target: MigrationNodeMapping,
        }
        let wire = Wire::deserialize(deserializer)?;
        Self::new(wire.source, wire.target).map_err(serde::de::Error::custom)
    }
}

fn validate_run_pair(source_run_id: &RunId, target_run_id: &RunId) -> Result<(), RepositoryError> {
    if source_run_id == target_run_id {
        return Err(invalid_command());
    }
    Ok(())
}

fn validate_candidates(
    source_run_id: &RunId,
    target_run_id: &RunId,
    candidates: &[CreateReuseCandidateCommand],
) -> Result<(), RepositoryError> {
    if candidates.len() > MAX_RECOVERY_CANDIDATES {
        return Err(invalid_command());
    }
    let mut candidate_ids = BTreeSet::new();
    let mut admission_keys = BTreeSet::new();
    for candidate in candidates {
        if candidate.source_run_id() != source_run_id
            || candidate.run_id() != target_run_id
            || !candidate_ids.insert(candidate.candidate_id())
            || !admission_keys.insert((
                candidate.target_scope_instance_id().as_str(),
                candidate.target_node_id().as_str(),
                candidate.stable_activation_key(),
            ))
        {
            return Err(invalid_command());
        }
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RedriveRunCommand {
    source_run_id: RunId,
    target_run_id: RunId,
    expected_source_projection_version: u64,
    reuse_candidates: Vec<CreateReuseCandidateCommand>,
    target_timeout_ms: u64,
}

impl RedriveRunCommand {
    pub fn new(
        source_run_id: RunId,
        target_run_id: RunId,
        expected_source_projection_version: u64,
        reuse_candidates: Vec<CreateReuseCandidateCommand>,
    ) -> Result<Self, RepositoryError> {
        validate_run_pair(&source_run_id, &target_run_id)?;
        validate_candidates(&source_run_id, &target_run_id, &reuse_candidates)?;
        Ok(Self {
            source_run_id,
            target_run_id,
            expected_source_projection_version,
            reuse_candidates,
            target_timeout_ms: DEFAULT_RECOVERY_RUN_TIMEOUT_MS,
        })
    }

    pub fn with_run_timeout(mut self, timeout: Duration) -> Result<Self, RepositoryError> {
        self.target_timeout_ms = duration_ms(timeout)?;
        Ok(self)
    }

    pub fn source_run_id(&self) -> &RunId {
        &self.source_run_id
    }

    pub fn target_run_id(&self) -> &RunId {
        &self.target_run_id
    }

    pub fn expected_source_projection_version(&self) -> u64 {
        self.expected_source_projection_version
    }

    pub fn reuse_candidates(&self) -> &[CreateReuseCandidateCommand] {
        &self.reuse_candidates
    }

    pub fn target_timeout_ms(&self) -> u64 {
        self.target_timeout_ms
    }

    pub(crate) fn validate(&self) -> Result<(), RepositoryError> {
        validate_run_pair(&self.source_run_id, &self.target_run_id)?;
        validate_candidates(
            &self.source_run_id,
            &self.target_run_id,
            &self.reuse_candidates,
        )?;
        if self.target_timeout_ms == 0 || self.target_timeout_ms > MAX_RECOVERY_RUN_TIMEOUT_MS {
            return Err(invalid_command());
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ForkRunCommand {
    source_run_id: RunId,
    target_run_id: RunId,
    expected_source_projection_version: u64,
    target_revision: RecoveryRevisionSpec,
    source_checkpoint_hash: ContentHash,
    target_input: Option<Value>,
    reuse_candidates: Vec<CreateReuseCandidateCommand>,
    target_timeout_ms: u64,
}

impl ForkRunCommand {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        source_run_id: RunId,
        target_run_id: RunId,
        expected_source_projection_version: u64,
        target_revision: RecoveryRevisionSpec,
        source_checkpoint_hash: ContentHash,
        reuse_candidates: Vec<CreateReuseCandidateCommand>,
    ) -> Result<Self, RepositoryError> {
        validate_run_pair(&source_run_id, &target_run_id)?;
        validate_candidates(&source_run_id, &target_run_id, &reuse_candidates)?;
        Ok(Self {
            source_run_id,
            target_run_id,
            expected_source_projection_version,
            target_revision,
            source_checkpoint_hash,
            target_input: None,
            reuse_candidates,
            target_timeout_ms: DEFAULT_RECOVERY_RUN_TIMEOUT_MS,
        })
    }

    pub fn with_run_timeout(mut self, timeout: Duration) -> Result<Self, RepositoryError> {
        self.target_timeout_ms = duration_ms(timeout)?;
        Ok(self)
    }

    pub fn with_target_input(mut self, target_input: Value) -> Result<Self, RepositoryError> {
        serde_jcs::to_vec(&target_input).map_err(|_| invalid_command())?;
        self.target_input = Some(target_input);
        Ok(self)
    }

    pub fn source_run_id(&self) -> &RunId {
        &self.source_run_id
    }

    pub fn target_run_id(&self) -> &RunId {
        &self.target_run_id
    }

    pub fn expected_source_projection_version(&self) -> u64 {
        self.expected_source_projection_version
    }

    pub fn target_revision(&self) -> &RecoveryRevisionSpec {
        &self.target_revision
    }

    pub fn source_checkpoint_hash(&self) -> &ContentHash {
        &self.source_checkpoint_hash
    }

    pub fn target_input(&self) -> Option<&Value> {
        self.target_input.as_ref()
    }

    pub fn reuse_candidates(&self) -> &[CreateReuseCandidateCommand] {
        &self.reuse_candidates
    }

    pub fn target_timeout_ms(&self) -> u64 {
        self.target_timeout_ms
    }

    pub(crate) fn validate(&self) -> Result<(), RepositoryError> {
        validate_run_pair(&self.source_run_id, &self.target_run_id)?;
        validate_candidates(
            &self.source_run_id,
            &self.target_run_id,
            &self.reuse_candidates,
        )?;
        if self.target_timeout_ms == 0 || self.target_timeout_ms > MAX_RECOVERY_RUN_TIMEOUT_MS {
            return Err(invalid_command());
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BeginMigrationCommand {
    source_run_id: RunId,
    target_run_id: RunId,
    expected_source_projection_version: u64,
    target_revision: RecoveryRevisionSpec,
    expected_target_publication_agent: Option<RecoveryLabel>,
    target_input: Value,
    mappings: Vec<MigrationMappingCompatibility>,
    reuse_candidates: Vec<CreateReuseCandidateCommand>,
    target_timeout_ms: u64,
}

impl BeginMigrationCommand {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        source_run_id: RunId,
        target_run_id: RunId,
        expected_source_projection_version: u64,
        target_revision: RecoveryRevisionSpec,
        target_input: Value,
        mappings: Vec<MigrationMappingCompatibility>,
        reuse_candidates: Vec<CreateReuseCandidateCommand>,
    ) -> Result<Self, RepositoryError> {
        validate_run_pair(&source_run_id, &target_run_id)?;
        validate_candidates(&source_run_id, &target_run_id, &reuse_candidates)?;
        validate_mappings(&mappings)?;
        Ok(Self {
            source_run_id,
            target_run_id,
            expected_source_projection_version,
            target_revision,
            expected_target_publication_agent: None,
            target_input,
            mappings,
            reuse_candidates,
            target_timeout_ms: DEFAULT_RECOVERY_RUN_TIMEOUT_MS,
        })
    }

    pub fn with_run_timeout(mut self, timeout: Duration) -> Result<Self, RepositoryError> {
        self.target_timeout_ms = duration_ms(timeout)?;
        Ok(self)
    }

    /// Fences a fresh alias-based migration against the exact durable route
    /// head used to resolve its target. Pending-intent replay keeps this frozen
    /// identity and is checked for exact replay before the current head is
    /// consulted again.
    pub fn with_expected_target_publication_head(
        mut self,
        head: PublicationHead,
    ) -> Result<Self, RepositoryError> {
        if head.definition_id() != self.target_revision.definition_id()
            || head.definition_revision_id()
                != self.target_revision.revision().definition_revision_id()
            || head.deployment_revision_id()
                != self.target_revision.revision().deployment_revision_id()
        {
            return Err(invalid_command());
        }
        self.expected_target_publication_agent = Some(RecoveryLabel::new(head.agent_id())?);
        Ok(self)
    }

    pub(crate) fn with_expected_target_publication_agent(
        mut self,
        agent_id: impl Into<String>,
    ) -> Result<Self, RepositoryError> {
        self.expected_target_publication_agent = Some(RecoveryLabel::new(agent_id)?);
        Ok(self)
    }

    pub fn source_run_id(&self) -> &RunId {
        &self.source_run_id
    }

    pub fn target_run_id(&self) -> &RunId {
        &self.target_run_id
    }

    pub fn expected_source_projection_version(&self) -> u64 {
        self.expected_source_projection_version
    }

    pub fn target_revision(&self) -> &RecoveryRevisionSpec {
        &self.target_revision
    }

    pub fn expected_target_publication_agent(&self) -> Option<&str> {
        self.expected_target_publication_agent
            .as_ref()
            .map(RecoveryLabel::as_str)
    }

    pub fn target_input(&self) -> &Value {
        &self.target_input
    }

    pub fn mappings(&self) -> &[MigrationMappingCompatibility] {
        &self.mappings
    }

    pub fn reuse_candidates(&self) -> &[CreateReuseCandidateCommand] {
        &self.reuse_candidates
    }

    pub fn target_timeout_ms(&self) -> u64 {
        self.target_timeout_ms
    }

    pub(crate) fn validate(&self) -> Result<(), RepositoryError> {
        validate_run_pair(&self.source_run_id, &self.target_run_id)?;
        validate_candidates(
            &self.source_run_id,
            &self.target_run_id,
            &self.reuse_candidates,
        )?;
        validate_mappings(&self.mappings)?;
        if self.target_timeout_ms == 0 || self.target_timeout_ms > MAX_RECOVERY_RUN_TIMEOUT_MS {
            return Err(invalid_command());
        }
        Ok(())
    }
}

fn validate_mappings(mappings: &[MigrationMappingCompatibility]) -> Result<(), RepositoryError> {
    // An empty mapping set is the closed, safe "re-execute everything" migration
    // policy. It transfers no Activation, Wait, Timer, Signal, or effect state;
    // only the target's validated workflow input crosses the generation boundary.
    // Non-empty mappings remain subject to the complete compatibility contract.
    if mappings.len() > MAX_MIGRATION_MAPPINGS {
        return Err(invalid_command());
    }
    let mut source_nodes = BTreeSet::new();
    let mut target_nodes = BTreeSet::new();
    for mapping in mappings {
        mapping.validate()?;
        if !source_nodes.insert(mapping.source().source_node_id())
            || !target_nodes.insert(mapping.source().target_node_id())
        {
            return Err(invalid_command());
        }
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FinalizeMigrationCommand {
    source_run_id: RunId,
    target_run_id: RunId,
    expected_source_projection_version: u64,
    migration_intent_transition_key: TransitionKey,
}

impl FinalizeMigrationCommand {
    pub fn new(
        source_run_id: RunId,
        target_run_id: RunId,
        expected_source_projection_version: u64,
        migration_intent_transition_key: TransitionKey,
    ) -> Result<Self, RepositoryError> {
        validate_run_pair(&source_run_id, &target_run_id)?;
        Ok(Self {
            source_run_id,
            target_run_id,
            expected_source_projection_version,
            migration_intent_transition_key,
        })
    }

    pub fn source_run_id(&self) -> &RunId {
        &self.source_run_id
    }

    pub fn target_run_id(&self) -> &RunId {
        &self.target_run_id
    }

    pub fn expected_source_projection_version(&self) -> u64 {
        self.expected_source_projection_version
    }

    pub fn migration_intent_transition_key(&self) -> &TransitionKey {
        &self.migration_intent_transition_key
    }

    pub(crate) fn validate(&self) -> Result<(), RepositoryError> {
        validate_run_pair(&self.source_run_id, &self.target_run_id)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContinueAsNewCommand {
    source_run_id: RunId,
    target_run_id: RunId,
    expected_source_projection_version: u64,
    target_input: Value,
    reuse_candidates: Vec<CreateReuseCandidateCommand>,
    target_timeout_ms: u64,
}

impl ContinueAsNewCommand {
    pub fn new(
        source_run_id: RunId,
        target_run_id: RunId,
        expected_source_projection_version: u64,
        target_input: Value,
        reuse_candidates: Vec<CreateReuseCandidateCommand>,
    ) -> Result<Self, RepositoryError> {
        validate_run_pair(&source_run_id, &target_run_id)?;
        validate_candidates(&source_run_id, &target_run_id, &reuse_candidates)?;
        Ok(Self {
            source_run_id,
            target_run_id,
            expected_source_projection_version,
            target_input,
            reuse_candidates,
            target_timeout_ms: DEFAULT_RECOVERY_RUN_TIMEOUT_MS,
        })
    }

    pub fn with_run_timeout(mut self, timeout: Duration) -> Result<Self, RepositoryError> {
        self.target_timeout_ms = duration_ms(timeout)?;
        Ok(self)
    }

    pub fn source_run_id(&self) -> &RunId {
        &self.source_run_id
    }

    pub fn target_run_id(&self) -> &RunId {
        &self.target_run_id
    }

    pub fn expected_source_projection_version(&self) -> u64 {
        self.expected_source_projection_version
    }

    pub fn target_input(&self) -> &Value {
        &self.target_input
    }

    pub fn reuse_candidates(&self) -> &[CreateReuseCandidateCommand] {
        &self.reuse_candidates
    }

    pub fn target_timeout_ms(&self) -> u64 {
        self.target_timeout_ms
    }

    pub(crate) fn validate(&self) -> Result<(), RepositoryError> {
        validate_run_pair(&self.source_run_id, &self.target_run_id)?;
        validate_candidates(
            &self.source_run_id,
            &self.target_run_id,
            &self.reuse_candidates,
        )?;
        if self.target_timeout_ms == 0 || self.target_timeout_ms > MAX_RECOVERY_RUN_TIMEOUT_MS {
            return Err(invalid_command());
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecoveryEventReceipt {
    run_id: RunId,
    event_seq: u64,
    event_id: ExecutionEventId,
    projection_version: u64,
}

impl RecoveryEventReceipt {
    pub(crate) fn new(
        run_id: RunId,
        event_seq: u64,
        event_id: ExecutionEventId,
        projection_version: u64,
    ) -> Self {
        Self {
            run_id,
            event_seq,
            event_id,
            projection_version,
        }
    }

    pub fn run_id(&self) -> &RunId {
        &self.run_id
    }

    pub fn event_seq(&self) -> u64 {
        self.event_seq
    }

    pub fn event_id(&self) -> &ExecutionEventId {
        &self.event_id
    }

    pub fn projection_version(&self) -> u64 {
        self.projection_version
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecoveryRunReceipt {
    lineage: RunLineage,
    source_terminal: Option<RecoveryEventReceipt>,
    target_created: RecoveryEventReceipt,
    target_input_hash: ContentHash,
    candidates_created: u32,
}

impl RecoveryRunReceipt {
    pub(crate) fn new(
        lineage: RunLineage,
        source_terminal: Option<RecoveryEventReceipt>,
        target_created: RecoveryEventReceipt,
        target_input_hash: ContentHash,
        candidates_created: u32,
    ) -> Self {
        Self {
            lineage,
            source_terminal,
            target_created,
            target_input_hash,
            candidates_created,
        }
    }

    pub fn lineage(&self) -> &RunLineage {
        &self.lineage
    }

    pub fn source_terminal(&self) -> Option<&RecoveryEventReceipt> {
        self.source_terminal.as_ref()
    }

    pub fn target_created(&self) -> &RecoveryEventReceipt {
        &self.target_created
    }

    pub fn target_input_hash(&self) -> &ContentHash {
        &self.target_input_hash
    }

    pub fn candidates_created(&self) -> u32 {
        self.candidates_created
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MigrationIntentReceipt {
    source_event: RecoveryEventReceipt,
    target_run_id: RunId,
    target_revision: RecoveryRevisionSpec,
    target_input_hash: ContentHash,
    mapping_hash: ContentHash,
}

impl MigrationIntentReceipt {
    pub(crate) fn new(
        source_event: RecoveryEventReceipt,
        target_run_id: RunId,
        target_revision: RecoveryRevisionSpec,
        target_input_hash: ContentHash,
        mapping_hash: ContentHash,
    ) -> Self {
        Self {
            source_event,
            target_run_id,
            target_revision,
            target_input_hash,
            mapping_hash,
        }
    }

    pub fn source_event(&self) -> &RecoveryEventReceipt {
        &self.source_event
    }

    pub fn target_run_id(&self) -> &RunId {
        &self.target_run_id
    }

    pub fn target_revision(&self) -> &RecoveryRevisionSpec {
        &self.target_revision
    }

    pub fn target_input_hash(&self) -> &ContentHash {
        &self.target_input_hash
    }

    pub fn mapping_hash(&self) -> &ContentHash {
        &self.mapping_hash
    }
}

pub(crate) fn migration_mapping_hash(
    mappings: &[MigrationMappingCompatibility],
) -> Result<ContentHash, RepositoryError> {
    serde_jcs::to_vec(mappings)
        .map(|encoded| ContentHash::from_bytes(&encoded))
        .map_err(|_| RepositoryError::canonicalization())
}

/// Compute the greatest dependency-closed subset of repository-derived reuse
/// candidates. A downstream candidate cannot become durable when any exact
/// source Activation that supplied its input was filtered out for retention,
/// contract, mapping, or output-validity reasons. Repeating to a fixed point
/// also closes transitive chains rather than only checking one predecessor.
pub(crate) fn dependency_closed_reuse_candidates(
    mut candidates: Vec<CreateReuseCandidateCommand>,
) -> Vec<CreateReuseCandidateCommand> {
    loop {
        let available = candidates
            .iter()
            .map(|candidate| candidate.source_activation_id().clone())
            .collect::<BTreeSet<_>>();
        let before = candidates.len();
        candidates.retain(|candidate| candidate.source_data_dependencies().is_subset(&available));
        if candidates.len() == before {
            return candidates;
        }
    }
}

#[async_trait]
pub trait RecoveryDurableRepository:
    super::ControlDurableRepository + super::ProjectionDurableRepository
{
    /// Select the newest scheduler checkpoint whose execution event and
    /// projection-checkpoint batch both verify at the requested source
    /// projection. Public recovery callers never supply a checkpoint hash.
    async fn latest_authoritative_scheduler_checkpoint(
        &self,
        source_run_id: &RunId,
        expected_source_projection_version: u64,
    ) -> Result<Option<ContentHash>, RepositoryError>;

    async fn authoritative_scheduler_checkpoint_by_id(
        &self,
        source_run_id: &RunId,
        expected_source_projection_version: u64,
        checkpoint_id: &SchedulerCheckpointId,
    ) -> Result<Option<ContentHash>, RepositoryError>;

    async fn derive_compatible_reuse_candidates(
        &self,
        source_run_id: &RunId,
        target_run_id: &RunId,
        expected_source_projection_version: u64,
        target_revision: &RecoveryRevisionSpec,
        source_checkpoint_hash: Option<&ContentHash>,
    ) -> Result<Option<Vec<CreateReuseCandidateCommand>>, RepositoryError>;

    /// Derive candidates for an explicit Migrate mapping. Source and target
    /// node identities may differ; dynamic Map/Loop/Subflow occurrences are
    /// re-derived against the target Plan and Run rather than copied from the
    /// source scope instance ID.
    async fn derive_migration_reuse_candidates(
        &self,
        source_run_id: &RunId,
        target_run_id: &RunId,
        expected_source_projection_version: u64,
        target_revision: &RecoveryRevisionSpec,
        mappings: &[MigrationMappingCompatibility],
    ) -> Result<Option<Vec<CreateReuseCandidateCommand>>, RepositoryError>;

    async fn redrive_run(
        &self,
        transition_key: TransitionKey,
        command: RedriveRunCommand,
    ) -> Result<TransitionOutcome<RecoveryRunReceipt>, RepositoryError>;

    async fn fork_run(
        &self,
        transition_key: TransitionKey,
        command: ForkRunCommand,
    ) -> Result<TransitionOutcome<RecoveryRunReceipt>, RepositoryError>;

    /// Load the exact command frozen by an incomplete migration begin commit.
    /// The repository, rather than a mutable agent alias, is authoritative for
    /// every target revision, schema, input, mapping, candidate, and timeout
    /// field needed to replay that begin transition after a process crash.
    async fn load_pending_migration_intent(
        &self,
        source_run_id: &RunId,
        transition_key: &TransitionKey,
    ) -> Result<Option<BeginMigrationCommand>, RepositoryError>;

    async fn begin_migration(
        &self,
        transition_key: TransitionKey,
        command: BeginMigrationCommand,
    ) -> Result<TransitionOutcome<MigrationIntentReceipt>, RepositoryError>;

    async fn finalize_migration(
        &self,
        transition_key: TransitionKey,
        command: FinalizeMigrationCommand,
    ) -> Result<TransitionOutcome<RecoveryRunReceipt>, RepositoryError>;

    async fn continue_as_new(
        &self,
        transition_key: TransitionKey,
        command: ContinueAsNewCommand,
    ) -> Result<TransitionOutcome<RecoveryRunReceipt>, RepositoryError>;
}

/// Cross-crate construction and validation hooks for storage/runtime adapters.
#[doc(hidden)]
pub mod adapter {
    use super::*;

    pub fn recovery_task_completion_checkpoint(task_id: &SchedulerTaskId) -> SchedulerCheckpointId {
        super::recovery_task_completion_checkpoint(task_id)
    }

    pub fn recovery_candidate_id(
        source_run_id: &RunId,
        target_run_id: &RunId,
        source_activation_id: &insight_engine::ActivationId,
    ) -> Result<String, RepositoryError> {
        super::recovery_candidate_id(source_run_id, target_run_id, source_activation_id)
    }

    pub fn validate_redrive(command: &RedriveRunCommand) -> Result<(), RepositoryError> {
        command.validate()
    }

    pub fn validate_fork(command: &ForkRunCommand) -> Result<(), RepositoryError> {
        command.validate()
    }

    pub fn validate_begin_migration(
        command: &BeginMigrationCommand,
    ) -> Result<(), RepositoryError> {
        command.validate()
    }

    pub fn validate_finalize_migration(
        command: &FinalizeMigrationCommand,
    ) -> Result<(), RepositoryError> {
        command.validate()
    }

    pub fn validate_continue_as_new(command: &ContinueAsNewCommand) -> Result<(), RepositoryError> {
        command.validate()
    }

    pub fn with_expected_target_publication_agent(
        command: BeginMigrationCommand,
        agent_id: impl Into<String>,
    ) -> Result<BeginMigrationCommand, RepositoryError> {
        command.with_expected_target_publication_agent(agent_id)
    }

    pub fn recovery_event_receipt(
        run_id: RunId,
        event_seq: u64,
        event_id: ExecutionEventId,
        projection_version: u64,
    ) -> RecoveryEventReceipt {
        RecoveryEventReceipt::new(run_id, event_seq, event_id, projection_version)
    }

    pub fn recovery_run_receipt(
        lineage: RunLineage,
        source_terminal: Option<RecoveryEventReceipt>,
        target_created: RecoveryEventReceipt,
        target_input_hash: ContentHash,
        candidates_created: u32,
    ) -> RecoveryRunReceipt {
        RecoveryRunReceipt::new(
            lineage,
            source_terminal,
            target_created,
            target_input_hash,
            candidates_created,
        )
    }

    pub fn migration_intent_receipt(
        source_event: RecoveryEventReceipt,
        target_run_id: RunId,
        target_revision: RecoveryRevisionSpec,
        target_input_hash: ContentHash,
        mapping_hash: ContentHash,
    ) -> MigrationIntentReceipt {
        MigrationIntentReceipt::new(
            source_event,
            target_run_id,
            target_revision,
            target_input_hash,
            mapping_hash,
        )
    }

    pub fn migration_mapping_hash(
        mappings: &[MigrationMappingCompatibility],
    ) -> Result<ContentHash, RepositoryError> {
        super::migration_mapping_hash(mappings)
    }

    pub fn dependency_closed_reuse_candidates(
        candidates: Vec<CreateReuseCandidateCommand>,
    ) -> Vec<CreateReuseCandidateCommand> {
        super::dependency_closed_reuse_candidates(candidates)
    }
}
