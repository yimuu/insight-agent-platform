use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Deserializer, Serialize};

use super::{
    ActivationId, AdmissionState, ArtifactId, ContentHash, DefinitionRevisionId,
    DeploymentRevisionId, EffectEvidence, EffectId, EffectIdempotency, Generation, ModelError,
    NodeId, RunId, RunLifecycle,
};

pub const RECOVERY_LINEAGE_INVALID: &str = "ENGINE_RECOVERY_LINEAGE_INVALID";
pub const RECOVERY_REVISION_MISMATCH: &str = "ENGINE_RECOVERY_REVISION_MISMATCH";
pub const RECOVERY_REUSE_INELIGIBLE: &str = "ENGINE_RECOVERY_REUSE_INELIGIBLE";
pub const RECOVERY_MATERIALIZATION_MISMATCH: &str = "ENGINE_RECOVERY_MATERIALIZATION_MISMATCH";
pub const RECOVERY_MIGRATION_NOT_READY: &str = "ENGINE_RECOVERY_MIGRATION_NOT_READY";
pub const RECOVERY_MIGRATION_SCHEMA_INCOMPATIBLE: &str =
    "ENGINE_RECOVERY_MIGRATION_SCHEMA_INCOMPATIBLE";

/// Immutable definition and deployment identity pinned by every Run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionRevisionPin {
    definition_revision_id: DefinitionRevisionId,
    deployment_revision_id: DeploymentRevisionId,
    plan_hash: ContentHash,
    binding_hash: ContentHash,
}

impl ExecutionRevisionPin {
    pub fn new(
        definition_revision_id: DefinitionRevisionId,
        deployment_revision_id: DeploymentRevisionId,
        plan_hash: ContentHash,
        binding_hash: ContentHash,
    ) -> Self {
        Self {
            definition_revision_id,
            deployment_revision_id,
            plan_hash,
            binding_hash,
        }
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

    pub fn execution_identity_matches(&self, other: &Self) -> bool {
        self.plan_hash == other.plan_hash && self.binding_hash == other.binding_hash
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunLineageKind {
    Redrive,
    Fork,
    Migrate,
    ContinueAsNew,
}

/// Closed lineage contract. It creates a new Run and never reopens a terminal
/// source Run. Redrive and continue-as-new are revision-pinned by construction.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RunLineage {
    source_run_id: RunId,
    target_run_id: RunId,
    kind: RunLineageKind,
    source_generation: Generation,
    target_generation: Generation,
    source_revision: ExecutionRevisionPin,
    target_revision: ExecutionRevisionPin,
    source_checkpoint_hash: Option<ContentHash>,
}

impl RunLineage {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        source_run_id: RunId,
        target_run_id: RunId,
        kind: RunLineageKind,
        source_generation: Generation,
        source_revision: ExecutionRevisionPin,
        target_revision: ExecutionRevisionPin,
        source_checkpoint_hash: Option<ContentHash>,
    ) -> Result<Self, ModelError> {
        if source_run_id == target_run_id {
            return Err(ModelError::new(
                RECOVERY_LINEAGE_INVALID,
                "a recovery lineage must create a distinct target Run",
            ));
        }
        if matches!(
            kind,
            RunLineageKind::Redrive | RunLineageKind::ContinueAsNew
        ) && source_revision != target_revision
        {
            return Err(ModelError::new(
                RECOVERY_REVISION_MISMATCH,
                "redrive and continue-as-new must retain the exact source revision pin",
            ));
        }
        if kind == RunLineageKind::Migrate
            && source_revision.execution_identity_matches(&target_revision)
        {
            return Err(ModelError::new(
                RECOVERY_REVISION_MISMATCH,
                "migrate must target a different effective Plan or binding identity",
            ));
        }
        if kind == RunLineageKind::ContinueAsNew && source_checkpoint_hash.is_some() {
            return Err(ModelError::new(
                RECOVERY_LINEAGE_INVALID,
                "continue-as-new starts a generation boundary, not a historical checkpoint fork",
            ));
        }
        if kind == RunLineageKind::Fork && source_checkpoint_hash.is_none() {
            return Err(ModelError::new(
                RECOVERY_LINEAGE_INVALID,
                "fork requires one authoritative source checkpoint",
            ));
        }
        let target_generation = match kind {
            RunLineageKind::ContinueAsNew => source_generation.next()?,
            RunLineageKind::Redrive | RunLineageKind::Fork | RunLineageKind::Migrate => {
                Generation::FIRST
            }
        };
        Ok(Self {
            source_run_id,
            target_run_id,
            kind,
            source_generation,
            target_generation,
            source_revision,
            target_revision,
            source_checkpoint_hash,
        })
    }

    pub fn source_run_id(&self) -> &RunId {
        &self.source_run_id
    }

    pub fn target_run_id(&self) -> &RunId {
        &self.target_run_id
    }

    pub fn kind(&self) -> RunLineageKind {
        self.kind
    }

    pub fn source_generation(&self) -> Generation {
        self.source_generation
    }

    pub fn target_generation(&self) -> Generation {
        self.target_generation
    }

    pub fn source_revision(&self) -> &ExecutionRevisionPin {
        &self.source_revision
    }

    pub fn target_revision(&self) -> &ExecutionRevisionPin {
        &self.target_revision
    }

    pub fn source_checkpoint_hash(&self) -> Option<&ContentHash> {
        self.source_checkpoint_hash.as_ref()
    }

    fn validate(&self) -> Result<(), ModelError> {
        let rebuilt = Self::new(
            self.source_run_id.clone(),
            self.target_run_id.clone(),
            self.kind,
            self.source_generation,
            self.source_revision.clone(),
            self.target_revision.clone(),
            self.source_checkpoint_hash.clone(),
        )?;
        if rebuilt.target_generation != self.target_generation {
            return Err(ModelError::new(
                RECOVERY_LINEAGE_INVALID,
                "target generation is not canonical for the lineage kind",
            ));
        }
        Ok(())
    }
}

impl<'de> Deserialize<'de> for RunLineage {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            source_run_id: RunId,
            target_run_id: RunId,
            kind: RunLineageKind,
            source_generation: Generation,
            target_generation: Generation,
            source_revision: ExecutionRevisionPin,
            target_revision: ExecutionRevisionPin,
            source_checkpoint_hash: Option<ContentHash>,
        }
        let wire = Wire::deserialize(deserializer)?;
        let lineage = Self {
            source_run_id: wire.source_run_id,
            target_run_id: wire.target_run_id,
            kind: wire.kind,
            source_generation: wire.source_generation,
            target_generation: wire.target_generation,
            source_revision: wire.source_revision,
            target_revision: wire.target_revision,
            source_checkpoint_hash: wire.source_checkpoint_hash,
        };
        lineage.validate().map_err(serde::de::Error::custom)?;
        Ok(lineage)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReusableNodeClass {
    Pure,
    IdempotentEffect,
    NonIdempotentEffect,
    Timer,
    Wait,
    Terminal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReuseRejection {
    SourceNotSucceeded,
    RevisionMismatch,
    NodeOrScopeMismatch,
    InputMismatch,
    ContractMismatch,
    DependenciesOpen,
    ArtifactMissing,
    EffectOutcomeUnknown,
    NodeClassForbidden,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
struct StableScopeKey(String);

impl StableScopeKey {
    fn new(value: impl Into<String>) -> Result<Self, ModelError> {
        let value = value.into();
        validate_scope_key(&value)?;
        Ok(Self(value))
    }

    fn as_str(&self) -> &str {
        &self.0
    }
}

impl<'de> Deserialize<'de> for StableScopeKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::new(value).map_err(serde::de::Error::custom)
    }
}

/// Immutable evidence used to create a durable reuse candidate. It deliberately
/// has no target Activation ID: materialization is permitted only when normal
/// control-flow admission actually reaches the same node/scope/input tuple.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReuseEvidence {
    source_activation_id: ActivationId,
    source_node_id: NodeId,
    stable_scope_key: StableScopeKey,
    source_revision: ExecutionRevisionPin,
    input_hash: ContentHash,
    node_configuration_hash: ContentHash,
    descriptor_hash: ContentHash,
    output_schema_hash: ContentHash,
    effect_policy_hash: ContentHash,
    output_hash: ContentHash,
    effect_id: EffectId,
    effect_evidence: EffectEvidence,
    node_class: ReusableNodeClass,
    dependencies: BTreeSet<ActivationId>,
    artifacts: BTreeSet<ArtifactId>,
    source_succeeded: bool,
    dependencies_closed: bool,
    artifacts_verified: bool,
}

impl ReuseEvidence {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        source_activation_id: ActivationId,
        source_node_id: NodeId,
        stable_scope_key: impl Into<String>,
        source_revision: ExecutionRevisionPin,
        input_hash: ContentHash,
        node_configuration_hash: ContentHash,
        descriptor_hash: ContentHash,
        output_schema_hash: ContentHash,
        effect_policy_hash: ContentHash,
        output_hash: ContentHash,
        effect_id: EffectId,
        effect_evidence: EffectEvidence,
        node_class: ReusableNodeClass,
        dependencies: BTreeSet<ActivationId>,
        artifacts: BTreeSet<ArtifactId>,
        source_succeeded: bool,
        dependencies_closed: bool,
        artifacts_verified: bool,
    ) -> Result<Self, ModelError> {
        Ok(Self {
            source_activation_id,
            source_node_id,
            stable_scope_key: StableScopeKey::new(stable_scope_key)?,
            source_revision,
            input_hash,
            node_configuration_hash,
            descriptor_hash,
            output_schema_hash,
            effect_policy_hash,
            output_hash,
            effect_id,
            effect_evidence,
            node_class,
            dependencies,
            artifacts,
            source_succeeded,
            dependencies_closed,
            artifacts_verified,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReuseTargetContract {
    node_id: NodeId,
    stable_scope_key: StableScopeKey,
    revision: ExecutionRevisionPin,
    input_hash: ContentHash,
    node_configuration_hash: ContentHash,
    descriptor_hash: ContentHash,
    output_schema_hash: ContentHash,
    effect_policy_hash: ContentHash,
}

impl ReuseTargetContract {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        node_id: NodeId,
        stable_scope_key: impl Into<String>,
        revision: ExecutionRevisionPin,
        input_hash: ContentHash,
        node_configuration_hash: ContentHash,
        descriptor_hash: ContentHash,
        output_schema_hash: ContentHash,
        effect_policy_hash: ContentHash,
    ) -> Result<Self, ModelError> {
        let stable_scope_key = StableScopeKey::new(stable_scope_key)?;
        Ok(Self {
            node_id,
            stable_scope_key,
            revision,
            input_hash,
            node_configuration_hash,
            descriptor_hash,
            output_schema_hash,
            effect_policy_hash,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReuseCandidate {
    source_activation_id: ActivationId,
    source_node_id: NodeId,
    stable_scope_key: StableScopeKey,
    input_hash: ContentHash,
    output_hash: ContentHash,
    effect_id: EffectId,
    dependencies: BTreeSet<ActivationId>,
    artifacts: BTreeSet<ArtifactId>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReuseMaterialization {
    reused_from_activation_id: ActivationId,
    inherited_effect_id: EffectId,
    output_hash: ContentHash,
    artifacts: BTreeSet<ArtifactId>,
}

impl ReuseMaterialization {
    pub fn reused_from_activation_id(&self) -> &ActivationId {
        &self.reused_from_activation_id
    }

    pub fn inherited_effect_id(&self) -> &EffectId {
        &self.inherited_effect_id
    }

    pub fn output_hash(&self) -> &ContentHash {
        &self.output_hash
    }

    pub fn artifacts(&self) -> &BTreeSet<ArtifactId> {
        &self.artifacts
    }
}

impl ReuseCandidate {
    pub fn evaluate(
        evidence: ReuseEvidence,
        target: &ReuseTargetContract,
    ) -> Result<Self, ReuseRejection> {
        if !evidence.source_succeeded {
            return Err(ReuseRejection::SourceNotSucceeded);
        }
        if evidence.source_revision != target.revision {
            return Err(ReuseRejection::RevisionMismatch);
        }
        if evidence.source_node_id != target.node_id
            || evidence.stable_scope_key != target.stable_scope_key
        {
            return Err(ReuseRejection::NodeOrScopeMismatch);
        }
        if evidence.input_hash != target.input_hash {
            return Err(ReuseRejection::InputMismatch);
        }
        if evidence.node_configuration_hash != target.node_configuration_hash
            || evidence.descriptor_hash != target.descriptor_hash
            || evidence.output_schema_hash != target.output_schema_hash
            || evidence.effect_policy_hash != target.effect_policy_hash
        {
            return Err(ReuseRejection::ContractMismatch);
        }
        if !evidence.dependencies_closed {
            return Err(ReuseRejection::DependenciesOpen);
        }
        if !evidence.artifacts_verified {
            return Err(ReuseRejection::ArtifactMissing);
        }
        if matches!(evidence.effect_evidence, EffectEvidence::Unknown) {
            return Err(ReuseRejection::EffectOutcomeUnknown);
        }
        if matches!(
            evidence.node_class,
            ReusableNodeClass::Timer | ReusableNodeClass::Wait | ReusableNodeClass::Terminal
        ) {
            return Err(ReuseRejection::NodeClassForbidden);
        }
        Ok(Self {
            source_activation_id: evidence.source_activation_id,
            source_node_id: evidence.source_node_id,
            stable_scope_key: evidence.stable_scope_key,
            input_hash: evidence.input_hash,
            output_hash: evidence.output_hash,
            effect_id: evidence.effect_id,
            dependencies: evidence.dependencies,
            artifacts: evidence.artifacts,
        })
    }

    pub fn source_activation_id(&self) -> &ActivationId {
        &self.source_activation_id
    }

    pub fn materialize_at_admission(
        &self,
        admitted_node_id: &NodeId,
        admitted_scope_key: &str,
        admitted_input_hash: &ContentHash,
        available_artifacts: &BTreeSet<ArtifactId>,
    ) -> Result<ReuseMaterialization, ModelError> {
        if &self.source_node_id != admitted_node_id
            || self.stable_scope_key.as_str() != admitted_scope_key
            || &self.input_hash != admitted_input_hash
            || !self.artifacts.is_subset(available_artifacts)
        {
            return Err(ModelError::new(
                RECOVERY_MATERIALIZATION_MISMATCH,
                "reuse candidate no longer matches the actually admitted node, scope, input, or artifacts",
            ));
        }
        Ok(ReuseMaterialization {
            reused_from_activation_id: self.source_activation_id.clone(),
            inherited_effect_id: self.effect_id.clone(),
            output_hash: self.output_hash.clone(),
            artifacts: self.artifacts.clone(),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RedriveEffectDecision {
    ReuseCommittedResult,
    RetryWithInheritedEffectId,
    BlockOutcomeUnknown,
    RequireNewForkEffectLineage,
}

pub fn decide_redrive_effect(
    source_succeeded: bool,
    evidence: EffectEvidence,
    idempotency: EffectIdempotency,
) -> RedriveEffectDecision {
    if source_succeeded && evidence == EffectEvidence::Committed {
        return RedriveEffectDecision::ReuseCommittedResult;
    }
    match (evidence, idempotency) {
        (EffectEvidence::Unknown, EffectIdempotency::NonIdempotent) => {
            RedriveEffectDecision::BlockOutcomeUnknown
        }
        (_, EffectIdempotency::Idempotent) | (EffectEvidence::NotStarted, _) => {
            RedriveEffectDecision::RetryWithInheritedEffectId
        }
        (_, EffectIdempotency::NonIdempotent) => RedriveEffectDecision::RequireNewForkEffectLineage,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MigrationReadiness {
    lifecycle: RunLifecycle,
    admission: AdmissionState,
    live_attempts: u32,
    unsettled_children: u32,
}

impl MigrationReadiness {
    pub fn new(
        lifecycle: RunLifecycle,
        admission: AdmissionState,
        live_attempts: u32,
        unsettled_children: u32,
    ) -> Self {
        Self {
            lifecycle,
            admission,
            live_attempts,
            unsettled_children,
        }
    }

    pub fn require_ready(self) -> Result<(), ModelError> {
        if !matches!(self.lifecycle, RunLifecycle::Active | RunLifecycle::Waiting)
            || self.admission != AdmissionState::Paused
            || self.live_attempts != 0
            || self.unsettled_children != 0
        {
            return Err(ModelError::new(
                RECOVERY_MIGRATION_NOT_READY,
                "migrate requires a paused active/waiting Run with all Attempts and children drained",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MigrationNodeMapping {
    source_node_id: NodeId,
    target_node_id: NodeId,
    port_mapping: BTreeMap<crate::engine::plan::DataPortId, crate::engine::plan::DataPortId>,
    input_schema_hash: ContentHash,
    output_schema_hash: ContentHash,
    effect_policy_hash: ContentHash,
    signal_wait_rebuild_declared: bool,
    timer_rebuild_declared: bool,
}

impl MigrationNodeMapping {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        source_node_id: NodeId,
        target_node_id: NodeId,
        port_mapping: BTreeMap<crate::engine::plan::DataPortId, crate::engine::plan::DataPortId>,
        input_schema_hash: ContentHash,
        output_schema_hash: ContentHash,
        effect_policy_hash: ContentHash,
        signal_wait_rebuild_declared: bool,
        timer_rebuild_declared: bool,
    ) -> Self {
        Self {
            source_node_id,
            target_node_id,
            port_mapping,
            input_schema_hash,
            output_schema_hash,
            effect_policy_hash,
            signal_wait_rebuild_declared,
            timer_rebuild_declared,
        }
    }

    pub fn is_compatible_with(&self, target: &Self) -> bool {
        self.port_mapping.values().collect::<BTreeSet<_>>().len() == self.port_mapping.len()
            && self.port_mapping == target.port_mapping
            && self.input_schema_hash == target.input_schema_hash
            && self.output_schema_hash == target.output_schema_hash
            && self.effect_policy_hash == target.effect_policy_hash
            && (!self.signal_wait_rebuild_declared || target.signal_wait_rebuild_declared)
            && (!self.timer_rebuild_declared || target.timer_rebuild_declared)
    }

    pub fn source_node_id(&self) -> &NodeId {
        &self.source_node_id
    }

    pub fn target_node_id(&self) -> &NodeId {
        &self.target_node_id
    }

    pub fn port_mapping(
        &self,
    ) -> &BTreeMap<crate::engine::plan::DataPortId, crate::engine::plan::DataPortId> {
        &self.port_mapping
    }

    pub fn input_schema_hash(&self) -> &ContentHash {
        &self.input_schema_hash
    }

    pub fn output_schema_hash(&self) -> &ContentHash {
        &self.output_schema_hash
    }

    pub fn effect_policy_hash(&self) -> &ContentHash {
        &self.effect_policy_hash
    }

    pub fn signal_wait_rebuild_declared(&self) -> bool {
        self.signal_wait_rebuild_declared
    }

    pub fn timer_rebuild_declared(&self) -> bool {
        self.timer_rebuild_declared
    }
}

fn validate_scope_key(value: &str) -> Result<(), ModelError> {
    if value.is_empty() || value.len() > 512 || value.chars().any(char::is_control) {
        return Err(ModelError::new(
            RECOVERY_REUSE_INELIGIBLE,
            "stable scope key must be non-empty, bounded, and body-free",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pin(label: &str) -> ExecutionRevisionPin {
        ExecutionRevisionPin::new(
            DefinitionRevisionId::new(format!("definition-{label}")).unwrap(),
            DeploymentRevisionId::new(format!("deployment-{label}")).unwrap(),
            ContentHash::from_bytes(format!("plan-{label}").as_bytes()),
            ContentHash::from_bytes(format!("binding-{label}").as_bytes()),
        )
    }

    fn reuse_pair() -> (ReuseEvidence, ReuseTargetContract) {
        let revision = pin("source");
        let target = ReuseTargetContract::new(
            NodeId::new("analyze").unwrap(),
            "map:item-1",
            revision.clone(),
            ContentHash::from_bytes(b"input"),
            ContentHash::from_bytes(b"config"),
            ContentHash::from_bytes(b"descriptor"),
            ContentHash::from_bytes(b"schema"),
            ContentHash::from_bytes(b"effect-policy"),
        )
        .unwrap();
        let evidence = ReuseEvidence {
            source_activation_id: ActivationId::new("activation_source").unwrap(),
            source_node_id: NodeId::new("analyze").unwrap(),
            stable_scope_key: StableScopeKey::new("map:item-1").unwrap(),
            source_revision: revision,
            input_hash: ContentHash::from_bytes(b"input"),
            node_configuration_hash: ContentHash::from_bytes(b"config"),
            descriptor_hash: ContentHash::from_bytes(b"descriptor"),
            output_schema_hash: ContentHash::from_bytes(b"schema"),
            effect_policy_hash: ContentHash::from_bytes(b"effect-policy"),
            output_hash: ContentHash::from_bytes(b"output"),
            effect_id: EffectId::new("effect_source").unwrap(),
            effect_evidence: EffectEvidence::Committed,
            node_class: ReusableNodeClass::IdempotentEffect,
            dependencies: BTreeSet::new(),
            artifacts: BTreeSet::from([ArtifactId::new("artifact_source").unwrap()]),
            source_succeeded: true,
            dependencies_closed: true,
            artifacts_verified: true,
        };
        (evidence, target)
    }

    #[test]
    fn redrive_and_continue_as_new_cannot_silently_change_revision() {
        for kind in [RunLineageKind::Redrive, RunLineageKind::ContinueAsNew] {
            let error = RunLineage::new(
                RunId::new("run_source").unwrap(),
                RunId::new("run_target").unwrap(),
                kind,
                Generation::FIRST,
                pin("source"),
                pin("target"),
                None,
            )
            .unwrap_err();
            assert_eq!(error.code(), RECOVERY_REVISION_MISMATCH);
        }
        let fork = RunLineage::new(
            RunId::new("run_source").unwrap(),
            RunId::new("run_target").unwrap(),
            RunLineageKind::Fork,
            Generation::FIRST,
            pin("source"),
            pin("target"),
            Some(ContentHash::from_bytes(b"authoritative-checkpoint")),
        )
        .unwrap();
        assert_ne!(fork.source_revision(), fork.target_revision());
        let revision = pin("same");
        let lineage = RunLineage::new(
            RunId::new("run_source").unwrap(),
            RunId::new("run_target").unwrap(),
            RunLineageKind::ContinueAsNew,
            Generation::new(7).unwrap(),
            revision.clone(),
            revision,
            None,
        )
        .unwrap();
        assert_eq!(lineage.target_generation().get(), 8);
    }

    #[test]
    fn lineage_deserialization_rejects_a_forged_generation() {
        let revision = pin("same");
        let lineage = RunLineage::new(
            RunId::new("run_source").unwrap(),
            RunId::new("run_target").unwrap(),
            RunLineageKind::ContinueAsNew,
            Generation::FIRST,
            revision.clone(),
            revision,
            None,
        )
        .unwrap();
        let mut wire = serde_json::to_value(lineage).unwrap();
        wire["target_generation"] = serde_json::json!(9);
        assert!(serde_json::from_value::<RunLineage>(wire).is_err());
    }

    #[test]
    fn reuse_is_only_materialized_after_matching_control_admission() {
        let (evidence, target) = reuse_pair();
        let candidate = ReuseCandidate::evaluate(evidence, &target).unwrap();
        let artifacts = BTreeSet::from([ArtifactId::new("artifact_source").unwrap()]);
        let materialized = candidate
            .materialize_at_admission(
                &NodeId::new("analyze").unwrap(),
                "map:item-1",
                &ContentHash::from_bytes(b"input"),
                &artifacts,
            )
            .unwrap();
        assert_eq!(
            materialized.reused_from_activation_id().as_str(),
            "activation_source"
        );
        assert_eq!(materialized.inherited_effect_id().as_str(), "effect_source");
        assert_eq!(
            candidate
                .materialize_at_admission(
                    &NodeId::new("analyze").unwrap(),
                    "map:unselected-item",
                    &ContentHash::from_bytes(b"input"),
                    &artifacts,
                )
                .unwrap_err()
                .code(),
            RECOVERY_MATERIALIZATION_MISMATCH
        );
    }

    #[test]
    fn reuse_rejects_unknown_effects_and_forbidden_wait_nodes() {
        let (mut evidence, target) = reuse_pair();
        evidence.effect_evidence = EffectEvidence::Unknown;
        assert_eq!(
            ReuseCandidate::evaluate(evidence, &target).unwrap_err(),
            ReuseRejection::EffectOutcomeUnknown
        );

        let (mut evidence, target) = reuse_pair();
        evidence.node_class = ReusableNodeClass::Wait;
        assert_eq!(
            ReuseCandidate::evaluate(evidence, &target).unwrap_err(),
            ReuseRejection::NodeClassForbidden
        );
    }

    #[test]
    fn migrate_requires_pause_and_complete_drain() {
        MigrationReadiness::new(RunLifecycle::Waiting, AdmissionState::Paused, 0, 0)
            .require_ready()
            .unwrap();
        for readiness in [
            MigrationReadiness::new(RunLifecycle::Waiting, AdmissionState::Open, 0, 0),
            MigrationReadiness::new(RunLifecycle::Active, AdmissionState::Paused, 1, 0),
            MigrationReadiness::new(RunLifecycle::Active, AdmissionState::Paused, 0, 1),
            MigrationReadiness::new(RunLifecycle::Succeeded, AdmissionState::Closed, 0, 0),
        ] {
            assert_eq!(
                readiness.require_ready().unwrap_err().code(),
                RECOVERY_MIGRATION_NOT_READY
            );
        }
    }

    #[test]
    fn migration_node_mapping_allows_explicit_zero_data_port_wait_nodes() {
        let source = MigrationNodeMapping::new(
            NodeId::new("wait_v1").unwrap(),
            NodeId::new("wait_v2").unwrap(),
            BTreeMap::new(),
            ContentHash::from_bytes(b"wait-input"),
            ContentHash::from_bytes(b"wait-output"),
            ContentHash::from_bytes(b"wait-effect"),
            true,
            true,
        );
        let target = source.clone();
        assert!(source.is_compatible_with(&target));
    }

    #[test]
    fn redrive_effect_decision_reuses_provider_key_for_idempotent_unknown_outcomes() {
        assert_eq!(
            decide_redrive_effect(
                false,
                EffectEvidence::Unknown,
                EffectIdempotency::Idempotent
            ),
            RedriveEffectDecision::RetryWithInheritedEffectId
        );
        assert_eq!(
            decide_redrive_effect(
                false,
                EffectEvidence::Unknown,
                EffectIdempotency::NonIdempotent
            ),
            RedriveEffectDecision::BlockOutcomeUnknown
        );
        assert_eq!(
            decide_redrive_effect(
                false,
                EffectEvidence::Started,
                EffectIdempotency::NonIdempotent
            ),
            RedriveEffectDecision::RequireNewForkEffectLineage
        );
    }
}
