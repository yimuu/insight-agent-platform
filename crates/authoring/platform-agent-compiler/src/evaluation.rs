//! Evaluation dataset and result Artifact contracts. These contain evidence and
//! authoring input only: Run/ChildRun/Task/Job remain the sole execution owners.
use insight_platform_contracts::{
    ArtifactRef, ClosedJsonSchema, ExactDeploymentRef, ResourceId, Sha256Digest,
};
use serde::{Deserialize, Serialize};

pub const EVALUATION_MANIFEST_VERSION: u32 = 1;
pub const MAX_EVALUATION_MANIFEST_BYTES: usize = 1_048_576;
pub const MAX_EVALUATION_SAMPLES: usize = 64;
pub const MAX_EVALUATION_REPETITIONS: u16 = 16;
pub const MAX_EVALUATION_TRIALS: usize = 256;
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EvaluationSampleV1 {
    pub sample_id: String,
    pub input: ArtifactRef,
    pub expected: Option<ArtifactRef>,
    pub input_schema_digest: Sha256Digest,
    pub expected_schema_digest: Option<Sha256Digest>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EvaluationManifestV1 {
    pub schema_version: u32,
    pub dataset_id: String,
    pub samples: Vec<EvaluationSampleV1>,
    pub repetitions: u16,
    /// Exact Agent deployment transitively freezes its Model/Context/Policy closure.
    pub subject: ExactDeploymentRef,
    /// A normal, exact evaluator Agent; its own deployment freezes evaluator behavior.
    pub evaluator: ExactDeploymentRef,
    pub metric_schema: ClosedJsonSchema,
}
/// Stable identity is derived from exact manifest digest + sample ID + repetition.
/// A retry is another attempt of this same trial, never another sample.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EvaluationTrialIdentityV1 {
    pub manifest_digest: Sha256Digest,
    pub sample_id: String,
    pub repetition: u16,
    pub trial_digest: Sha256Digest,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum EvaluationTrialEvidenceV1 {
    Scored {
        subject_input: Box<EvaluationRunValueEvidenceV1>,
        evaluator_input: Box<EvaluationRunValueEvidenceV1>,
        output: Box<EvaluationRunValueEvidenceV1>,
        score: Box<EvaluationRunValueEvidenceV1>,
    },
    Failed {
        stage: EvaluationFailureStage,
        failed_run_id: ResourceId,
        subject_run_id: Option<ResourceId>,
        terminal_state: insight_platform_contracts::RunState,
        terminal_version: u64,
        failure_value: Option<Box<EvaluationRunValueEvidenceV1>>,
    },
    Missing {
        reason: EvaluationMissingReason,
    },
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EvaluationMissingReason {
    NotStarted,
    Cancelled,
    DeadlineExceeded,
    EvidenceUnavailable,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EvaluationTrialResultV1 {
    pub trial: EvaluationTrialIdentityV1,
    pub input: ArtifactRef,
    pub expected: Option<ArtifactRef>,
    pub evidence: EvaluationTrialEvidenceV1,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EvaluationReportV1 {
    pub schema_version: u32,
    pub manifest: ArtifactRef,
    pub parent_run_id: ResourceId,
    pub trials: Vec<EvaluationTrialResultV1>,
    pub scored_trials: u32,
    pub failed_trials: u32,
    pub missing_trials: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EvaluationFailureStage {
    Subject,
    Evaluator,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EvaluationRunValueEvidenceV1 {
    pub run_id: ResourceId,
    pub value_id: ResourceId,
    pub schema_digest: Sha256Digest,
    pub content_digest: Sha256Digest,
    pub artifact: Option<ArtifactRef>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidEvaluationEvidence;
fn stable_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
}
impl EvaluationManifestV1 {
    pub fn validate(&self) -> Result<(), InvalidEvaluationEvidence> {
        use insight_platform_contracts::ResourceKind;
        if self.schema_version != EVALUATION_MANIFEST_VERSION
            || !stable_id(&self.dataset_id)
            || self.samples.is_empty()
            || self.samples.len() > MAX_EVALUATION_SAMPLES
            || self.repetitions == 0
            || self.repetitions > MAX_EVALUATION_REPETITIONS
            || self.samples.len() * usize::from(self.repetitions) > MAX_EVALUATION_TRIALS
            || [&self.subject, &self.evaluator].iter().any(|target| {
                target.resource_kind != ResourceKind::AgentDeployment || target.validate().is_err()
            })
            || self.metric_schema.validate().is_err()
            || serde_json::to_vec(self)
                .map_err(|_| InvalidEvaluationEvidence)?
                .len()
                > MAX_EVALUATION_MANIFEST_BYTES
        {
            return Err(InvalidEvaluationEvidence);
        }
        let mut ids = std::collections::BTreeSet::new();
        for sample in &self.samples {
            if !stable_id(&sample.sample_id)
                || !ids.insert(&sample.sample_id)
                || sample.input.validate().is_err()
                || sample
                    .expected
                    .as_ref()
                    .is_some_and(|value| value.validate().is_err())
                || sample.expected.is_some() != sample.expected_schema_digest.is_some()
            {
                return Err(InvalidEvaluationEvidence);
            }
        }
        Ok(())
    }
    pub fn canonical_digest(&self) -> Result<Sha256Digest, InvalidEvaluationEvidence> {
        self.validate()?;
        insight_platform_contracts::canonical_digest(
            &serde_json::to_value(self).map_err(|_| InvalidEvaluationEvidence)?,
        )
        .map_err(|_| InvalidEvaluationEvidence)?
        .parse()
        .map_err(|_| InvalidEvaluationEvidence)
    }
    pub fn trials(&self) -> Result<Vec<EvaluationTrialIdentityV1>, InvalidEvaluationEvidence> {
        let digest = self.canonical_digest()?;
        let mut trials = Vec::new();
        for sample in &self.samples {
            for repetition in 0..self.repetitions {
                trials.push(EvaluationTrialIdentityV1::new(
                    digest.clone(),
                    sample.sample_id.clone(),
                    repetition,
                )?);
            }
        }
        Ok(trials)
    }
}
impl EvaluationTrialIdentityV1 {
    pub fn new(
        manifest_digest: Sha256Digest,
        sample_id: String,
        repetition: u16,
    ) -> Result<Self, InvalidEvaluationEvidence> {
        if !stable_id(&sample_id) || repetition >= MAX_EVALUATION_REPETITIONS {
            return Err(InvalidEvaluationEvidence);
        }
        let trial_digest=insight_platform_contracts::canonical_digest(&serde_json::json!({"schema_version":1,"manifest_digest":manifest_digest,"sample_id":sample_id,"repetition":repetition})).map_err(|_|InvalidEvaluationEvidence)?.parse().map_err(|_|InvalidEvaluationEvidence)?;
        Ok(Self {
            manifest_digest,
            sample_id,
            repetition,
            trial_digest,
        })
    }
}
impl EvaluationRunValueEvidenceV1 {
    fn validate(&self) -> Result<(), InvalidEvaluationEvidence> {
        if self.run_id.kind() != insight_platform_contracts::ResourceKind::Run
            || self.value_id.kind() != insight_platform_contracts::ResourceKind::RunValue
            || self.artifact.as_ref().is_some_and(|artifact| {
                artifact.validate().is_err() || artifact.content_digest() != &self.content_digest
            })
        {
            return Err(InvalidEvaluationEvidence);
        }
        Ok(())
    }
}
impl EvaluationReportV1 {
    pub fn validate_for(
        &self,
        manifest: &EvaluationManifestV1,
    ) -> Result<(), InvalidEvaluationEvidence> {
        use insight_platform_contracts::ResourceKind;
        let trials = manifest.trials()?;
        if self.schema_version != 1
            || self.parent_run_id.kind() != ResourceKind::Run
            || self.manifest.validate().is_err()
            || !matches_manifest_artifact(&self.manifest, manifest)?
            || self.trials.len() != trials.len()
            || serde_json::to_vec(self)
                .map_err(|_| InvalidEvaluationEvidence)?
                .len()
                > MAX_EVALUATION_MANIFEST_BYTES
        {
            return Err(InvalidEvaluationEvidence);
        }
        let mut counts = (0u32, 0u32, 0u32);
        let mut seen = std::collections::BTreeSet::new();
        for actual in &self.trials {
            let expected = trials
                .iter()
                .find(|trial| trial.trial_digest == actual.trial.trial_digest)
                .ok_or(InvalidEvaluationEvidence)?;
            let sample = manifest
                .samples
                .iter()
                .find(|sample| sample.sample_id == expected.sample_id)
                .ok_or(InvalidEvaluationEvidence)?;
            if !seen.insert(&actual.trial.trial_digest)
                || &actual.trial != expected
                || actual.input != sample.input
                || actual.expected != sample.expected
            {
                return Err(InvalidEvaluationEvidence);
            }
            match &actual.evidence {
                EvaluationTrialEvidenceV1::Scored {
                    subject_input,
                    evaluator_input,
                    output,
                    score,
                } => {
                    subject_input.validate()?;
                    evaluator_input.validate()?;
                    if subject_input.run_id != output.run_id
                        || evaluator_input.run_id != score.run_id
                        || subject_input.content_digest != sample.input.content_digest().clone()
                        || subject_input.schema_digest != sample.input_schema_digest
                    {
                        return Err(InvalidEvaluationEvidence);
                    }
                    output.validate()?;
                    score.validate()?;
                    if score.schema_digest != manifest.metric_schema.canonical_digest
                        || output.run_id == score.run_id
                    {
                        return Err(InvalidEvaluationEvidence);
                    }
                    counts.0 += 1;
                }
                EvaluationTrialEvidenceV1::Failed {
                    stage,
                    failed_run_id,
                    subject_run_id,
                    terminal_state,
                    terminal_version,
                    failure_value,
                } => {
                    if failed_run_id.kind() != ResourceKind::Run
                        || !matches!(
                            terminal_state,
                            insight_platform_contracts::RunState::Failed
                                | insight_platform_contracts::RunState::TimedOut
                                | insight_platform_contracts::RunState::Cancelled
                        )
                        || *terminal_version == 0
                        || *terminal_version > insight_platform_contracts::MAX_SAFE_JSON_INTEGER
                        || failure_value.as_ref().is_some_and(|failure| {
                            failure.run_id != *failed_run_id || failure.validate().is_err()
                        })
                        || subject_run_id
                            .as_ref()
                            .is_some_and(|id| id.kind() != ResourceKind::Run)
                        || (*stage == EvaluationFailureStage::Evaluator
                            && subject_run_id.as_ref().is_none_or(|id| id == failed_run_id))
                        || (*stage == EvaluationFailureStage::Subject
                            && subject_run_id
                                .as_ref()
                                .is_some_and(|id| id != failed_run_id))
                    {
                        return Err(InvalidEvaluationEvidence);
                    }
                    counts.1 += 1;
                }
                EvaluationTrialEvidenceV1::Missing { .. } => counts.2 += 1,
            }
        }
        if counts != (self.scored_trials, self.failed_trials, self.missing_trials) {
            return Err(InvalidEvaluationEvidence);
        }
        Ok(())
    }
}

fn matches_manifest_artifact(
    reference: &ArtifactRef,
    manifest: &EvaluationManifestV1,
) -> Result<bool, InvalidEvaluationEvidence> {
    let bytes = insight_platform_contracts::canonical_json(
        &serde_json::to_value(manifest).map_err(|_| InvalidEvaluationEvidence)?,
    )
    .map_err(|_| InvalidEvaluationEvidence)?;
    Ok(reference.content_digest() == &manifest.canonical_digest()?
        && reference.byte_length() == bytes.len() as u64
        && reference.media_type() == "application/json")
}

/// Authoring inputs for an ordinary parent Agent. Dataset bodies are deliberately
/// absent: they enter the parent Run through its normal protected input value.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EvaluationPlanRequestV1 {
    pub schema_version: u32,
    pub name: String,
    pub display_name: String,
    pub manifest: EvaluationManifestV1,
    pub manifest_artifact: ArtifactRef,
    pub subject_input_schema: ClosedJsonSchema,
    pub subject_output_schema: ClosedJsonSchema,
    pub expected_schema: Option<ClosedJsonSchema>,
    pub subject_selection_policy: insight_platform_contracts::ExactPolicyBinding,
    pub evaluator_selection_policy: insight_platform_contracts::ExactPolicyBinding,
    pub deployment_features: Vec<insight_platform_contracts::AgentDeploymentFeaturesV1>,
    pub child_budget: insight_platform_plan::ChildBudgetLimit,
    pub profile: crate::AgentCompilerProfile,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EvaluationPlanV1 {
    pub schema_version: u32,
    pub manifest_digest: Sha256Digest,
    pub parent_input_schema: ClosedJsonSchema,
    pub evaluator_input_schema: ClosedJsonSchema,
    pub source_bundle: crate::AgentSourceBundleV1,
}
/// Node names contain the entire trial digest, so retry identity never depends on
/// process-local ordinals or a newly generated Run ID.
pub fn trial_subject_node(trial: &EvaluationTrialIdentityV1) -> insight_platform_plan::PlanNodeKey {
    trial_node(trial, "subject")
}
pub fn trial_evaluator_node(
    trial: &EvaluationTrialIdentityV1,
) -> insight_platform_plan::PlanNodeKey {
    trial_node(trial, "evaluator")
}
fn trial_node(trial: &EvaluationTrialIdentityV1, role: &str) -> insight_platform_plan::PlanNodeKey {
    insight_platform_plan::PlanNodeKey::new(format!(
        "{role}-{}",
        trial.trial_digest.as_str().trim_start_matches("sha256:")
    ))
    .expect("bounded digest-based node name")
}
/// Embed one checked schema as a root definition. The closed profile resolves
/// local references against root $defs, so source definitions get independent,
/// deterministic names rather than nested JSON Pointers. Only schema positions
/// are visited: a const or enum may contain literal "$ref" application data.
fn embed_schema(
    schema: &ClosedJsonSchema,
    prefix: &str,
    destination: &mut serde_json::Map<String, serde_json::Value>,
) -> Result<(), InvalidEvaluationEvidence> {
    use serde_json::Value;
    use std::collections::BTreeMap;
    fn rewrite(
        value: &mut Value,
        names: &BTreeMap<String, String>,
    ) -> Result<(), InvalidEvaluationEvidence> {
        let map = value.as_object_mut().ok_or(InvalidEvaluationEvidence)?;
        // An embedded document must not establish a new JSON Schema resource.
        map.remove("$schema");
        map.remove("$id");
        if let Some(Value::String(reference)) = map.get_mut("$ref") {
            if let Some(name) = reference.strip_prefix("#/$defs/") {
                *reference = format!(
                    "#/$defs/{}",
                    names.get(name).ok_or(InvalidEvaluationEvidence)?
                );
            }
        }
        for keyword in ["properties", "$defs"] {
            if let Some(children) = map.get_mut(keyword).and_then(Value::as_object_mut) {
                for child in children.values_mut() {
                    rewrite(child, names)?;
                }
            }
        }
        if let Some(items) = map.get_mut("items") {
            rewrite(items, names)?;
        }
        if let Some(branches) = map.get_mut("oneOf").and_then(Value::as_array_mut) {
            for branch in branches {
                rewrite(branch, names)?;
            }
        }
        Ok(())
    }
    let mut root = schema.schema.clone();
    let definitions = root
        .as_object_mut()
        .ok_or(InvalidEvaluationEvidence)?
        .remove("$defs")
        .unwrap_or_else(|| Value::Object(serde_json::Map::new()));
    let definitions = definitions.as_object().ok_or(InvalidEvaluationEvidence)?;
    let names = definitions
        .keys()
        .enumerate()
        .map(|(index, name)| (name.clone(), format!("{prefix}__{index}")))
        .collect::<BTreeMap<_, _>>();
    rewrite(&mut root, &names)?;
    for (name, definition) in definitions {
        let mut definition = definition.clone();
        rewrite(&mut definition, &names)?;
        if destination
            .insert(names[name].clone(), definition)
            .is_some()
        {
            return Err(InvalidEvaluationEvidence);
        }
    }
    if destination.insert(prefix.into(), root).is_some() {
        return Err(InvalidEvaluationEvidence);
    }
    Ok(())
}

fn object_schema(
    properties: serde_json::Map<String, serde_json::Value>,
    required: Vec<String>,
    defs: serde_json::Map<String, serde_json::Value>,
) -> Result<ClosedJsonSchema, InvalidEvaluationEvidence> {
    ClosedJsonSchema::build(serde_json::json!({"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object","additionalProperties":false,"properties":properties,"required":required,"$defs":defs})).map_err(|_|InvalidEvaluationEvidence)
}

/// The evaluator protocol can be authored before any Agent deployment exists.
/// This produces only a schema; it cannot produce or validate an executable parent Plan.
pub fn evaluation_evaluator_input_schema(
    input: &ClosedJsonSchema,
    output: &ClosedJsonSchema,
    expected: Option<&ClosedJsonSchema>,
) -> Result<ClosedJsonSchema, InvalidEvaluationEvidence> {
    input.validate().map_err(|_| InvalidEvaluationEvidence)?;
    output.validate().map_err(|_| InvalidEvaluationEvidence)?;
    if expected.is_some_and(|schema| schema.validate().is_err()) {
        return Err(InvalidEvaluationEvidence);
    }
    let mut defs = serde_json::Map::new();
    embed_schema(input, "subject_input", &mut defs)?;
    embed_schema(output, "subject_output", &mut defs)?;
    if let Some(schema) = expected {
        embed_schema(schema, "expected", &mut defs)?;
    }
    let reference = |name: &str| serde_json::json!({"$ref":format!("#/$defs/{name}")});
    let bounded_string = |length: usize| serde_json::json!({"type":"string","minLength":1,"maxLength":length,"x-platform-max-bytes":length});
    let mut properties = serde_json::Map::from_iter([
        ("trial_digest".into(), bounded_string(71)),
        ("sample_id".into(), bounded_string(128)),
        (
            "repetition".into(),
            serde_json::json!({"type":"integer","minimum":0,"maximum":15}),
        ),
        ("input".into(), reference("subject_input")),
        ("output".into(), reference("subject_output")),
    ]);
    if expected.is_some() {
        properties.insert("expected".into(), reference("expected"));
    }
    object_schema(
        properties,
        ["trial_digest", "sample_id", "repetition", "input", "output"]
            .into_iter()
            .map(str::to_owned)
            .collect(),
        defs,
    )
}
/// The exact dependency inputs shared by authoring queries and final Plan generation.
/// It does not resolve deployments or assert any installed feature.
pub fn evaluation_dependency_bindings(
    request: &EvaluationPlanRequestV1,
) -> Result<Vec<insight_platform_contracts::AgentSlotBindingInputV1>, InvalidEvaluationEvidence> {
    use insight_platform_contracts::{AgentSlotBindingInputV1, AgentSlotTargetInputV1};
    request.manifest.validate()?;
    request
        .subject_selection_policy
        .validate()
        .map_err(|_| InvalidEvaluationEvidence)?;
    request
        .evaluator_selection_policy
        .validate()
        .map_err(|_| InvalidEvaluationEvidence)?;
    let evaluator_input = evaluation_evaluator_input_schema(
        &request.subject_input_schema,
        &request.subject_output_schema,
        request.expected_schema.as_ref(),
    )?;
    let mut bindings = Vec::new();
    for (name, target, policy, input_schema, output_schema) in [
        (
            "subject",
            &request.manifest.subject,
            &request.subject_selection_policy,
            &request.subject_input_schema,
            &request.subject_output_schema,
        ),
        (
            "evaluator",
            &request.manifest.evaluator,
            &request.evaluator_selection_policy,
            &evaluator_input,
            &request.manifest.metric_schema,
        ),
    ] {
        let requirement: Sha256Digest =
            crate::agent_interface_contract_digest(input_schema, output_schema)
                .map_err(|_| InvalidEvaluationEvidence)?;
        bindings.push(AgentSlotBindingInputV1 {
            slot_id: name.into(),
            requirement_digest: requirement,
            target: AgentSlotTargetInputV1::ChildAgent {
                candidates: vec![target.clone()],
                selection_policy: policy.clone(),
            },
        });
    }
    Ok(bindings)
}
/// AllSettled joins reuse kernel failure handling. A failed Subject never starts
/// its Evaluator; its absent score is reported as failure, never as a score of zero.
pub fn compile_evaluation_plan(
    request: EvaluationPlanRequestV1,
) -> Result<EvaluationPlanV1, InvalidEvaluationEvidence> {
    use insight_platform_contracts::{
        canonical_json, ClosedJsonValue, ClosedValueSchema, DataClassification,
    };
    use insight_platform_plan::{
        DataPortKey, ExactDataPortRef, ExpressionFieldName, ExpressionLimits, JoinPolicy,
        PlanNodeKey, PortAssignment, RuntimeDependencyKind, RuntimeDependencySlot, RuntimeNode,
        RuntimePlan, TypedExpressionProgram, TypedInstruction,
    };
    use std::collections::BTreeMap;
    request.manifest.validate()?;
    let manifest_digest = request.manifest.canonical_digest()?;
    if request.schema_version != 1
        || request.manifest_artifact.validate().is_err()
        || !matches_manifest_artifact(&request.manifest_artifact, &request.manifest)?
        || request.subject_input_schema.validate().is_err()
        || request.subject_output_schema.validate().is_err()
        || request
            .expected_schema
            .as_ref()
            .is_some_and(|schema| schema.validate().is_err())
        || request.subject_selection_policy.validate().is_err()
        || request.evaluator_selection_policy.validate().is_err()
        || request.deployment_features.is_empty()
        || request.deployment_features.len() > 2
        || serde_json::to_vec(&request)
            .map_err(|_| InvalidEvaluationEvidence)?
            .len()
            > MAX_EVALUATION_MANIFEST_BYTES
    {
        return Err(InvalidEvaluationEvidence);
    }
    if request.manifest.samples.iter().any(|sample| {
        sample.input_schema_digest != request.subject_input_schema.canonical_digest
            || sample
                .expected_schema_digest
                .as_ref()
                .is_some_and(|digest| {
                    request
                        .expected_schema
                        .as_ref()
                        .is_none_or(|schema| &schema.canonical_digest != digest)
                })
    }) {
        return Err(InvalidEvaluationEvidence);
    }
    let trials = request.manifest.trials()?;
    let mut defs = serde_json::Map::new();
    embed_schema(&request.subject_input_schema, "subject_input", &mut defs)?;
    embed_schema(&request.subject_output_schema, "subject_output", &mut defs)?;
    if let Some(expected) = &request.expected_schema {
        embed_schema(expected, "expected", &mut defs)?;
    }
    let reference = |name: &str| serde_json::json!({"$ref":format!("#/$defs/{name}")});
    let mut sample_properties = serde_json::Map::new();
    for sample in &request.manifest.samples {
        let mut fields = serde_json::Map::from_iter([("input".into(), reference("subject_input"))]);
        let mut required = vec!["input".to_owned()];
        if sample.expected.is_some() {
            fields.insert("expected".into(), reference("expected"));
            required.push("expected".into());
        }
        sample_properties.insert(sample.sample_id.clone(),serde_json::json!({"type":"object","additionalProperties":false,"properties":fields,"required":required}));
    }
    let parent_input = object_schema(
        serde_json::Map::from_iter([(
            "samples".into(),
            serde_json::json!({"type":"object","additionalProperties":false,"required":request.manifest.samples.iter().map(|s|s.sample_id.clone()).collect::<Vec<_>>(),"properties":sample_properties}),
        )]),
        vec!["samples".into()],
        defs.clone(),
    )?;
    let evaluator_input = evaluation_evaluator_input_schema(
        &request.subject_input_schema,
        &request.subject_output_schema,
        request.expected_schema.as_ref(),
    )?;
    for (target, input, output) in [
        (
            &request.manifest.subject,
            &request.subject_input_schema,
            &request.subject_output_schema,
        ),
        (
            &request.manifest.evaluator,
            &evaluator_input,
            &request.manifest.metric_schema,
        ),
    ] {
        let contract = crate::agent_interface_contract_digest(input, output)
            .map_err(|_| InvalidEvaluationEvidence)?;
        if !request.deployment_features.iter().any(|item| {
            item.validate().is_ok()
                && item.deployment == *target
                && item.interface_contract_digest == contract
        }) {
            return Err(InvalidEvaluationEvidence);
        }
    }
    let output_schema = object_schema(
        serde_json::Map::from_iter([(
            "manifest".into(),
            serde_json::json!({"$ref":insight_platform_contracts::pinned_nominal_reference("ArtifactRef").ok_or(InvalidEvaluationEvidence)?}),
        )]),
        vec!["manifest".into()],
        serde_json::Map::new(),
    )?;
    let mut schemas = BTreeMap::new();
    for schema in [
        &parent_input,
        &evaluator_input,
        &request.subject_input_schema,
        &request.subject_output_schema,
        &request.manifest.metric_schema,
        &output_schema,
    ] {
        schemas.insert(
            schema.canonical_digest.clone(),
            ClosedValueSchema::try_from(schema.clone()).map_err(|_| InvalidEvaluationEvidence)?,
        );
    }
    if let Some(schema) = &request.expected_schema {
        schemas.insert(
            schema.canonical_digest.clone(),
            ClosedValueSchema::try_from(schema.clone()).map_err(|_| InvalidEvaluationEvidence)?,
        );
    }
    let key = |name: &str| PlanNodeKey::new(name.into()).map_err(|_| InvalidEvaluationEvidence);
    let port = |node: PlanNodeKey, schema: &Sha256Digest| ExactDataPortRef::NodeOutput {
        producer_node_id: node,
        port_id: DataPortKey::new("out".into()).expect("static port"),
        schema_digest: schema.clone(),
    };
    let run_input = ExactDataPortRef::RunInput {
        schema_digest: parent_input.canonical_digest.clone(),
    };
    let field =
        |name: &str| ExpressionFieldName::new(name.into()).map_err(|_| InvalidEvaluationEvidence);
    let limits = insight_platform_plan::PlanLimits::from_profile(
        &insight_platform_contracts::checked_in_hard_limit_profile(),
    )
    .map_err(|_| InvalidEvaluationEvidence)?;
    let chunk_size = limits.maximum_fan_out.clamp(1, 16);
    let chunks = trials.chunks(chunk_size).collect::<Vec<_>>();
    let mut nodes = BTreeMap::new();
    nodes.insert(
        key("start")?,
        RuntimeNode::Start {
            next: key("batch-0")?,
        },
    );
    for (batch, chunk) in chunks.iter().enumerate() {
        let join = key(&format!("join-{batch}"))?;
        let mut legs = Vec::new();
        for trial in *chunk {
            let sample = request
                .manifest
                .samples
                .iter()
                .find(|sample| sample.sample_id == trial.sample_id)
                .ok_or(InvalidEvaluationEvidence)?;
            let input_node = trial_node(trial, "input");
            let subject = trial_subject_node(trial);
            let scoring = trial_node(trial, "scoring");
            let evaluator = trial_evaluator_node(trial);
            legs.push(input_node.clone());
            let extract = TypedExpressionProgram::build(
                vec![run_input.clone()],
                vec![
                    TypedInstruction::LoadPort {
                        port: run_input.clone(),
                    },
                    TypedInstruction::GetField {
                        field: field("samples")?,
                    },
                    TypedInstruction::GetField {
                        field: field(&sample.sample_id)?,
                    },
                    TypedInstruction::GetField {
                        field: field("input")?,
                    },
                ],
                request.subject_input_schema.canonical_digest.clone(),
                ExpressionLimits::ABSOLUTE,
            )
            .map_err(|_| InvalidEvaluationEvidence)?;
            nodes.insert(
                input_node.clone(),
                RuntimeNode::Compute {
                    assignments: vec![PortAssignment {
                        output_port: port(
                            input_node.clone(),
                            &request.subject_input_schema.canonical_digest,
                        ),
                        expression: extract,
                    }],
                    next: subject.clone(),
                },
            );
            nodes.insert(
                subject.clone(),
                RuntimeNode::ChildAgentCall {
                    child_agent_slot_id: "subject".into(),
                    input: port(
                        input_node.clone(),
                        &request.subject_input_schema.canonical_digest,
                    ),
                    candidate_route: None,
                    output: port(
                        subject.clone(),
                        &request.subject_output_schema.canonical_digest,
                    ),
                    budget: request.child_budget.clone(),
                    cancellation_policy:
                        insight_platform_plan::ChildCancellationPolicy::CascadeAndWait,
                    attempt_limit: 1,
                    retry_backoff_milliseconds: 100,
                    resume: scoring.clone(),
                },
            );
            // Metadata constants are frozen authoring data. Sample and expected bodies
            // come exclusively from the admitted parent Run input.
            let metadata_schema=ClosedValueSchema::build(serde_json::json!({"$schema":"https://json-schema.org/draft/2020-12/schema","type":"string","minLength":0,"maxLength":128,"x-platform-max-bytes":128})).map_err(|_|InvalidEvaluationEvidence)?;
            let integer_schema=ClosedValueSchema::build(serde_json::json!({"$schema":"https://json-schema.org/draft/2020-12/schema","type":"integer","minimum":0,"maximum":15})).map_err(|_|InvalidEvaluationEvidence)?;
            schemas.insert(
                metadata_schema.canonical_digest.clone(),
                metadata_schema.clone(),
            );
            schemas.insert(
                integer_schema.canonical_digest.clone(),
                integer_schema.clone(),
            );
            let mut instructions = vec![
                TypedInstruction::Literal {
                    value: ClosedJsonValue::build(
                        metadata_schema.canonical_digest.clone(),
                        serde_json::json!(trial.trial_digest),
                    )
                    .map_err(|_| InvalidEvaluationEvidence)?,
                },
                TypedInstruction::Literal {
                    value: ClosedJsonValue::build(
                        metadata_schema.canonical_digest,
                        serde_json::json!(sample.sample_id),
                    )
                    .map_err(|_| InvalidEvaluationEvidence)?,
                },
                TypedInstruction::Literal {
                    value: ClosedJsonValue::build(
                        integer_schema.canonical_digest,
                        serde_json::json!(trial.repetition),
                    )
                    .map_err(|_| InvalidEvaluationEvidence)?,
                },
                TypedInstruction::LoadPort {
                    port: port(
                        input_node.clone(),
                        &request.subject_input_schema.canonical_digest,
                    ),
                },
                TypedInstruction::LoadPort {
                    port: port(
                        subject.clone(),
                        &request.subject_output_schema.canonical_digest,
                    ),
                },
            ];
            let mut fields = vec![
                field("trial_digest")?,
                field("sample_id")?,
                field("repetition")?,
                field("input")?,
                field("output")?,
            ];
            let mut input_ports = vec![
                port(input_node, &request.subject_input_schema.canonical_digest),
                port(subject, &request.subject_output_schema.canonical_digest),
            ];
            if sample.expected.is_some() {
                input_ports.push(run_input.clone());
                instructions.extend([
                    TypedInstruction::LoadPort {
                        port: run_input.clone(),
                    },
                    TypedInstruction::GetField {
                        field: field("samples")?,
                    },
                    TypedInstruction::GetField {
                        field: field(&sample.sample_id)?,
                    },
                    TypedInstruction::GetField {
                        field: field("expected")?,
                    },
                ]);
                fields.push(field("expected")?);
            }
            instructions.push(TypedInstruction::MakeObject {
                ordered_fields: fields,
            });
            let expression = TypedExpressionProgram::build(
                input_ports,
                instructions,
                evaluator_input.canonical_digest.clone(),
                ExpressionLimits::ABSOLUTE,
            )
            .map_err(|_| InvalidEvaluationEvidence)?;
            nodes.insert(
                scoring.clone(),
                RuntimeNode::Compute {
                    assignments: vec![PortAssignment {
                        output_port: port(scoring.clone(), &evaluator_input.canonical_digest),
                        expression,
                    }],
                    next: evaluator.clone(),
                },
            );
            nodes.insert(
                evaluator.clone(),
                RuntimeNode::ChildAgentCall {
                    child_agent_slot_id: "evaluator".into(),
                    input: port(scoring, &evaluator_input.canonical_digest),
                    candidate_route: None,
                    output: port(evaluator, &request.manifest.metric_schema.canonical_digest),
                    budget: request.child_budget.clone(),
                    cancellation_policy:
                        insight_platform_plan::ChildCancellationPolicy::CascadeAndWait,
                    attempt_limit: 1,
                    retry_backoff_milliseconds: 100,
                    resume: join.clone(),
                },
            );
        }
        nodes.insert(
            key(&format!("batch-{batch}"))?,
            RuntimeNode::Fork {
                legs,
                join: join.clone(),
            },
        );
        nodes.insert(
            join,
            RuntimeNode::Join {
                policy: JoinPolicy::AllSettled,
                quorum: None,
                remainder: None,
                next: if batch + 1 < chunks.len() {
                    key(&format!("batch-{}", batch + 1))?
                } else {
                    key("summary")?
                },
            },
        );
    }
    let summary = ClosedJsonValue::build(
        output_schema.canonical_digest.clone(),
        serde_json::json!({"manifest":request.manifest_artifact}),
    )
    .map_err(|_| InvalidEvaluationEvidence)?;
    nodes.insert(
        key("summary")?,
        RuntimeNode::Compute {
            assignments: vec![PortAssignment {
                output_port: port(key("summary")?, &output_schema.canonical_digest),
                expression: TypedExpressionProgram::build(
                    vec![],
                    vec![TypedInstruction::Literal { value: summary }],
                    output_schema.canonical_digest.clone(),
                    ExpressionLimits::ABSOLUTE,
                )
                .map_err(|_| InvalidEvaluationEvidence)?,
            }],
            next: key("finish")?,
        },
    );
    nodes.insert(
        key("finish")?,
        RuntimeNode::Return {
            value: port(key("summary")?, &output_schema.canonical_digest),
        },
    );
    let bindings = evaluation_dependency_bindings(&request)?;
    let slots = bindings
        .iter()
        .map(|binding| {
            (
                binding.slot_id.clone(),
                RuntimeDependencySlot {
                    kind: RuntimeDependencyKind::ChildAgent,
                    requirement_digest: binding.requirement_digest.clone(),
                },
            )
        })
        .collect();
    let interface = crate::agent_interface_contract_digest(&parent_input, &output_schema)
        .map_err(|_| InvalidEvaluationEvidence)?;
    let plan = RuntimePlan {
        plan_version: 6,
        interface_contract_digest: interface,
        entry_node_id: key("start")?,
        nodes,
        dependency_slots: slots,
        schema_documents: schemas,
    };
    let manifest = serde_json::json!({"apiVersion":"insight.platform/v1","kind":"Agent","metadata":{"name":request.name,"displayName":request.display_name},"spec":{"execution":{"kind":"full_plan","plan":"plan.json"},"input":{"schema":"input.json","classification":DataClassification::Restricted},"output":{"schema":"output.json"}}});
    let json = |value: serde_json::Value| -> Result<String, InvalidEvaluationEvidence> {
        String::from_utf8(canonical_json(&value).map_err(|_| InvalidEvaluationEvidence)?)
            .map_err(|_| InvalidEvaluationEvidence)
    };
    let source_bundle = crate::AgentSourceBundleV1 {
        schema_version: 1,
        compiler_semantic_identity: crate::compiler_semantic_identity(),
        compile_policy_inputs_digest: crate::AgentSourceBundleV1::policy_digest(&request.profile),
        profile: request.profile,
        bindings: crate::ResolvedAgentBindings {
            model: None,
            slots: bindings,
            deployment_features: request.deployment_features,
        },
        sources: crate::AgentSourceFilesV1 {
            manifest_path: "agent.json".into(),
            files: BTreeMap::from([
                ("agent.json".into(), json(manifest)?),
                ("input.json".into(), json(parent_input.schema.clone())?),
                ("output.json".into(), json(output_schema.schema)?),
                (
                    "plan.json".into(),
                    json(serde_json::to_value(plan).map_err(|_| InvalidEvaluationEvidence)?)?,
                ),
            ]),
        },
    };
    if !matches!(
        crate::compile_source_bundle(source_bundle.clone()),
        crate::AgentCompileResponseV1::Compiled { .. }
    ) {
        return Err(InvalidEvaluationEvidence);
    }
    Ok(EvaluationPlanV1 {
        schema_version: 1,
        manifest_digest,
        parent_input_schema: parent_input,
        evaluator_input_schema: evaluator_input,
        source_bundle,
    })
}
