//! Evaluation authoring and exact Artifact input preparation. Execution uses the
//! existing Agent publish/run commands and the ordinary durable parent Plan.
#[cfg(test)]
#[path = "evaluation_tests.rs"]
mod tests;
use crate::{agent::AgentCommandError, public_client::PublicHttpClient};
use insight_platform_agent_compiler::evaluation::*;
use insight_platform_contracts::{canonical_json, parse_strict_json, ArtifactRef, JsonLimits};
use serde::{de::DeserializeOwned, Serialize};
use serde_json::{json, Value};
use std::{
    fs::{self, OpenOptions},
    io::{Read, Write},
    path::Path,
};
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EvaluationAction {
    Init,
    Input,
}
fn invalid(detail: &str) -> AgentCommandError {
    AgentCommandError::InvalidLocalState(detail.into())
}
fn file_error(path: &Path, error: impl std::fmt::Display) -> AgentCommandError {
    AgentCommandError::Io {
        path: path.display().to_string(),
        detail: error.to_string(),
    }
}
fn read_json<T: DeserializeOwned>(path: &Path) -> Result<T, AgentCommandError> {
    let mut bytes = Vec::new();
    fs::File::open(path)
        .map_err(|e| file_error(path, e))?
        .take((MAX_EVALUATION_MANIFEST_BYTES + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(|e| file_error(path, e))?;
    serde_json::from_value(strict(&bytes)?)
        .map_err(|_| invalid("evaluation input does not match its closed contract"))
}
fn strict(bytes: &[u8]) -> Result<Value, AgentCommandError> {
    parse_strict_json(
        bytes,
        JsonLimits {
            max_bytes: MAX_EVALUATION_MANIFEST_BYTES,
            max_depth: 32,
            max_properties_per_object: 1024,
            max_items_per_array: 4096,
            max_string_bytes: 65536,
        },
    )
    .map_err(|_| invalid("evaluation input is not bounded strict JSON"))
}
fn write_private(path: &Path, bytes: &[u8]) -> Result<(), AgentCommandError> {
    write_private_with(path, |file| file.write_all(bytes))
}
fn write_private_with(
    path: &Path,
    write: impl FnOnce(&mut fs::File) -> std::io::Result<()>,
) -> Result<(), AgentCommandError> {
    #[cfg(unix)]
    use std::os::unix::fs::OpenOptionsExt;
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let parent = fs::canonicalize(parent).map_err(|e| file_error(parent, e))?;
    let name = path
        .file_name()
        .ok_or_else(|| invalid("evaluation output filename is missing"))?;
    let destination = parent.join(name);
    let temporary = parent.join(format!(".evaluation-file-{}", uuid::Uuid::now_v7()));
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    options.mode(0o600);
    let mut file = options
        .open(&temporary)
        .map_err(|e| file_error(&temporary, e))?;
    let result = (|| {
        write(&mut file)
            .and_then(|_| file.sync_all())
            .map_err(|e| file_error(path, e))?;
        crate::agent_restore::atomic_publish_new(&temporary, &destination)
            .map_err(|e| file_error(path, e))?;
        sync_directory(&parent)
    })();
    if result.is_err() {
        // Never remove the destination: it may belong to a concurrent writer.
        // A sync error after publication leaves the complete file retryable.
        let _ = fs::remove_file(&temporary);
    }
    result
}
fn sync_directory(path: &Path) -> Result<(), AgentCommandError> {
    fs::File::open(path)
        .and_then(|file| file.sync_all())
        .map_err(|e| file_error(path, e))
}
fn encode(value: &impl Serialize) -> Result<Vec<u8>, AgentCommandError> {
    canonical_json(&serde_json::to_value(value).map_err(|_| invalid("evaluation serialization"))?)
        .map_err(|_| invalid("evaluation serialization"))
}
fn exact_artifact_json(
    client: &PublicHttpClient,
    reference: &ArtifactRef,
) -> Result<Value, AgentCommandError> {
    if reference.validate().is_err()
        || reference.byte_length() > MAX_EVALUATION_MANIFEST_BYTES as u64
        || reference.media_type() != "application/json"
    {
        return Err(invalid(
            "evaluation requires bounded canonical JSON Artifacts",
        ));
    }
    let metadata = crate::artifact::read_artifact(client, reference.artifact_id())
        .map_err(AgentCommandError::Artifact)?;
    if metadata.content.as_ref() != Some(reference) {
        return Err(AgentCommandError::InvalidAuthority(
            "evaluation Artifact exact reference differs".into(),
        ));
    }
    let mut bytes = Vec::new();
    let actual = client.get_binary_to_writer(
        &format!("/v1/artifacts/{}/content", reference.artifact_id()),
        reference.byte_length(),
        &mut bytes,
    )?;
    if actual.content_length != reference.byte_length()
        || actual.content_digest != *reference.content_digest()
        || actual.content_type != reference.media_type()
        || actual.etag != format!("\"{}\"", reference.content_digest())
    {
        return Err(AgentCommandError::InvalidAuthority(
            "evaluation Artifact content differs".into(),
        ));
    }
    let value = strict(&bytes)?;
    if encode(&value)? != bytes {
        return Err(invalid("evaluation sample Artifact JSON must be canonical"));
    }
    Ok(value)
}
pub fn execute(
    action: EvaluationAction,
    project_root: &Path,
    request_file: &Path,
    output: &Path,
    client: Option<&PublicHttpClient>,
) -> Result<Value, AgentCommandError> {
    let mut request: EvaluationPlanRequestV1 = read_json(request_file)?;
    if let Some(client) = client.filter(|_| action == EvaluationAction::Init) {
        let bindings = evaluation_dependency_bindings(&request)
            .map_err(|_| invalid("evaluation dependency schema or identity is invalid"))?;
        let query = insight_platform_registry::authoring::exact_feature_request(&bindings)
            .map_err(|_| invalid("evaluation exact feature query is invalid"))?
            .ok_or_else(|| invalid("evaluation exact targets are missing"))?;
        let resolved = client.resolve_agent_bindings(&query)?;
        request.deployment_features =
            insight_platform_registry::authoring::resolved_feature_evidence(&resolved, &query)
                .map_err(|_| invalid("evaluation exact deployment features are unavailable"))?;
    }
    let compiled = compile_evaluation_plan(request.clone())
        .map_err(|_| invalid("evaluation Plan schemas, targets, identity or limits are invalid"))?;
    match action {
        EvaluationAction::Init => {
            let project_root =
                fs::canonicalize(project_root).map_err(|e| file_error(project_root, e))?;
            let parent = output
                .parent()
                .ok_or_else(|| invalid("evaluation output parent is missing"))?;
            let parent = fs::canonicalize(parent).map_err(|e| file_error(parent, e))?;
            if !parent.is_dir() || !parent.starts_with(&project_root) {
                return Err(invalid(
                    "evaluation authoring output must be inside the project",
                ));
            }
            let name = output
                .file_name()
                .ok_or_else(|| invalid("evaluation output directory name is missing"))?;
            let output = parent.join(name);
            let relative = output
                .strip_prefix(&project_root)
                .map_err(|_| invalid("evaluation authoring output must be inside the project"))?;
            match fs::symlink_metadata(&output) {
                Ok(_) => {
                    return Err(invalid(
                        "evaluation authoring output must be a new directory",
                    ));
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(file_error(&output, error)),
            }
            if relative.as_os_str().is_empty() {
                return Err(invalid(
                    "evaluation authoring output must be a new directory",
                ));
            }
            let prefix = relative
                .to_str()
                .ok_or_else(|| invalid("evaluation output path must be UTF-8"))?;
            insight_platform_agent_compiler::validate_relative_reference(
                &format!("{prefix}/agent.json"),
                "evaluation",
            )
            .map_err(AgentCommandError::Compiler)?;
            let temporary = parent.join(format!(".evaluation-{}", uuid::Uuid::now_v7()));
            let mut builder = fs::DirBuilder::new();
            #[cfg(unix)]
            {
                use std::os::unix::fs::DirBuilderExt;
                builder.mode(0o700);
            }
            builder
                .create(&temporary)
                .map_err(|e| file_error(&temporary, e))?;
            let result = (|| {
                for (name, source) in &compiled.source_bundle.sources.files {
                    let bytes = if name == "agent.json" {
                        let mut manifest: Value = serde_json::from_str(source)
                            .map_err(|_| invalid("generated evaluation manifest"))?;
                        manifest["spec"]["input"]["schema"] = json!(format!("{prefix}/input.json"));
                        manifest["spec"]["output"]["schema"] =
                            json!(format!("{prefix}/output.json"));
                        manifest["spec"]["execution"]["plan"] =
                            json!(format!("{prefix}/plan.json"));
                        encode(&manifest)?
                    } else {
                        source.as_bytes().to_vec()
                    };
                    write_private(&temporary.join(name), &bytes)?;
                }
                builder
                    .create(temporary.join(".insight"))
                    .map_err(|e| file_error(&temporary, e))?;
                write_private(
                    &temporary.join(".insight/agent-exact-bindings.json"),
                    &encode(&compiled.source_bundle.bindings)?,
                )?;
                write_private(
                    &temporary.join("evaluator-input.schema.json"),
                    &encode(&compiled.evaluator_input_schema.schema)?,
                )?;
                sync_directory(&temporary.join(".insight"))?;
                sync_directory(&temporary)?;
                crate::agent_restore::atomic_publish_new(&temporary, &output)
                    .map_err(|e| file_error(&output, e))?;
                sync_directory(&parent)?;
                Ok::<_, AgentCommandError>(())
            })();
            if result.is_err() {
                let _ = fs::remove_dir_all(&temporary);
            }
            result?;
            Ok(
                json!({"schema_version":1,"manifest_digest":compiled.manifest_digest,"agent_manifest":output.join("agent.json"),"evaluator_input_schema":output.join("evaluator-input.schema.json"),"execution":"ordinary_parent_run"}),
            )
        }
        EvaluationAction::Input => {
            let client = client.ok_or_else(|| {
                invalid("evaluation input requires the authenticated public client")
            })?;
            let authoritative_manifest = exact_artifact_json(client, &request.manifest_artifact)?;
            if authoritative_manifest
                != serde_json::to_value(&request.manifest)
                    .map_err(|_| invalid("evaluation manifest"))?
            {
                return Err(AgentCommandError::InvalidAuthority(
                    "evaluation manifest Artifact differs".into(),
                ));
            }
            let mut samples = serde_json::Map::new();
            let mut total = 0usize;
            for sample in &request.manifest.samples {
                let input = exact_artifact_json(client, &sample.input)?;
                request
                    .subject_input_schema
                    .validate_instance(&input)
                    .map_err(|_| {
                        invalid("evaluation sample violates the frozen Subject input schema")
                    })?;
                let mut values = serde_json::Map::from_iter([("input".into(), input)]);
                if let Some(reference) = &sample.expected {
                    let expected = exact_artifact_json(client, reference)?;
                    request
                        .expected_schema
                        .as_ref()
                        .ok_or_else(|| invalid("expected schema is missing"))?
                        .validate_instance(&expected)
                        .map_err(|_| {
                            invalid("evaluation expected Artifact violates its frozen schema")
                        })?;
                    values.insert("expected".into(), expected);
                }
                total = total
                    .checked_add(encode(&values)?.len())
                    .ok_or_else(|| invalid("evaluation input exceeds its bound"))?;
                if total > MAX_EVALUATION_MANIFEST_BYTES {
                    return Err(invalid("evaluation input exceeds its total bound"));
                }
                samples.insert(sample.sample_id.clone(), Value::Object(values));
            }
            let input = json!({"samples":samples});
            compiled
                .parent_input_schema
                .validate_instance(&input)
                .map_err(|_| invalid("evaluation parent input violates its schema"))?;
            let bytes = encode(&input)?;
            if bytes.len() > MAX_EVALUATION_MANIFEST_BYTES {
                return Err(invalid("evaluation parent input exceeds its bound"));
            }
            write_private(output, &bytes)?;
            Ok(
                json!({"schema_version":1,"manifest_digest":compiled.manifest_digest,"output":output,"schema_digest":compiled.parent_input_schema.canonical_digest,"samples":request.manifest.samples.len(),"trials":request.manifest.trials().map_err(|_|invalid("invalid evaluation trials"))?.len()}),
            )
        }
    }
}

fn read_run(
    client: &PublicHttpClient,
    id: &insight_platform_contracts::ResourceId,
) -> Result<insight_platform_api::run::RunViewV1, AgentCommandError> {
    let response = client.get_json::<insight_platform_api::run::RunViewV1>(
        &format!("/v1/runs/{id}"),
        reqwest::StatusCode::OK,
    )?;
    if response.body.validate().is_err()
        || response.body.run_id != *id
        || response.body.etag != response.etag
    {
        return Err(AgentCommandError::InvalidAuthority(
            "Run identity or version differs".into(),
        ));
    }
    Ok(response.body)
}
fn read_value(
    client: &PublicHttpClient,
    run: &insight_platform_contracts::ResourceId,
    value_id: &insight_platform_contracts::ResourceId,
) -> Result<(EvaluationRunValueEvidenceV1, Value), AgentCommandError> {
    use insight_platform_contracts::ValueRef;
    let response = client
        .get_body_json::<insight_platform_api::run::RunResultViewV1>(
            &format!("/v1/runs/{run}/values/{value_id}/content"),
            reqwest::StatusCode::OK,
        )?
        .body;
    if response.validate().is_err() || response.run_id != *run || response.value_id != *value_id {
        return Err(AgentCommandError::InvalidAuthority(
            "Run value identity differs".into(),
        ));
    }
    let (value, artifact) = match response.value {
        ValueRef::Inline { value } => (value, None),
        ValueRef::Artifact { artifact } => {
            let value = exact_artifact_json(client, &artifact)?;
            (value, Some(artifact))
        }
    };
    let actual: insight_platform_contracts::Sha256Digest =
        insight_platform_contracts::canonical_digest(&value)
            .map_err(|_| invalid("invalid Run value JSON"))?
            .parse()
            .map_err(|_| invalid("invalid Run value digest"))?;
    if actual != response.content_digest {
        return Err(AgentCommandError::InvalidAuthority(
            "Run value content digest differs".into(),
        ));
    }
    Ok((
        EvaluationRunValueEvidenceV1 {
            run_id: run.clone(),
            value_id: value_id.clone(),
            schema_digest: response.schema_digest,
            content_digest: response.content_digest,
            artifact,
        },
        value,
    ))
}
fn checked_child_run(
    client: &PublicHttpClient,
    link: &insight_platform_api::run::ChildRunViewV1,
) -> Result<insight_platform_api::run::RunViewV1, AgentCommandError> {
    let current = read_run(client, &link.child_run_id)?;
    if current.agent_deployment_id != link.child_agent_deployment.deployment_id
        || current.version != link.child_version
        || current.state != link.child_state
        || current.input_value_id != link.input_value_id
        || current.output_value_id != link.output_value_id
    {
        return Err(AgentCommandError::InvalidAuthority(
            "evaluation Run evidence changed".into(),
        ));
    }
    Ok(current)
}
fn checked_subject_input(
    client: &PublicHttpClient,
    request: &EvaluationPlanRequestV1,
    sample: &EvaluationSampleV1,
    subject: &insight_platform_api::run::ChildRunViewV1,
) -> Result<(EvaluationRunValueEvidenceV1, Value), AgentCommandError> {
    let (evidence, input) = read_value(client, &subject.child_run_id, &subject.input_value_id)?;
    if evidence.schema_digest != sample.input_schema_digest
        || evidence.content_digest != *sample.input.content_digest()
    {
        return Err(AgentCommandError::InvalidAuthority(
            "Subject received a different sample".into(),
        ));
    }
    request
        .subject_input_schema
        .validate_instance(&input)
        .map_err(|_| invalid("Subject input schema differs"))?;
    Ok((evidence, input))
}
fn checked_evaluator_input(
    client: &PublicHttpClient,
    request: &EvaluationPlanRequestV1,
    compiled: &EvaluationPlanV1,
    trial: &EvaluationTrialIdentityV1,
    subject: &insight_platform_api::run::ChildRunViewV1,
    evaluator: &insight_platform_api::run::ChildRunViewV1,
) -> Result<
    (
        EvaluationRunValueEvidenceV1,
        EvaluationRunValueEvidenceV1,
        EvaluationRunValueEvidenceV1,
    ),
    AgentCommandError,
> {
    let sample = request
        .manifest
        .samples
        .iter()
        .find(|sample| sample.sample_id == trial.sample_id)
        .ok_or_else(|| invalid("sample missing"))?;
    let (subject_input, input) = checked_subject_input(client, request, sample, subject)?;
    let (output, subject_output) = read_value(
        client,
        &subject.child_run_id,
        subject
            .output_value_id
            .as_ref()
            .ok_or_else(|| invalid("successful Subject has no output"))?,
    )?;
    if output.schema_digest != request.subject_output_schema.canonical_digest {
        return Err(AgentCommandError::InvalidAuthority(
            "Subject output schema differs".into(),
        ));
    }
    request
        .subject_output_schema
        .validate_instance(&subject_output)
        .map_err(|_| invalid("Subject output schema differs"))?;
    let (evaluator_input, actual_scoring_input) =
        read_value(client, &evaluator.child_run_id, &evaluator.input_value_id)?;
    let mut expected = json!({"trial_digest":trial.trial_digest,"sample_id":sample.sample_id,"repetition":trial.repetition,"input":input,"output":subject_output});
    if let Some(reference) = &sample.expected {
        expected["expected"] = exact_artifact_json(client, reference)?;
    }
    if evaluator_input.schema_digest != compiled.evaluator_input_schema.canonical_digest
        || actual_scoring_input != expected
    {
        return Err(AgentCommandError::InvalidAuthority(
            "Evaluator did not receive the exact trial output and expected value".into(),
        ));
    }
    compiled
        .evaluator_input_schema
        .validate_instance(&actual_scoring_input)
        .map_err(|_| invalid("evaluator input schema differs"))?;
    Ok((subject_input, output, evaluator_input))
}
fn terminal_failure(
    client: &PublicHttpClient,
    current: insight_platform_api::run::RunViewV1,
    stage: EvaluationFailureStage,
    subject_run_id: Option<insight_platform_contracts::ResourceId>,
) -> Result<EvaluationTrialEvidenceV1, AgentCommandError> {
    let failure_value = current
        .output_value_id
        .as_ref()
        .map(|value_id| {
            read_value(client, &current.run_id, value_id).map(|(evidence, _)| Box::new(evidence))
        })
        .transpose()?;
    if read_run(client, &current.run_id)? != current {
        return Err(AgentCommandError::InvalidAuthority(
            "terminal Run changed during evaluation report".into(),
        ));
    }
    Ok(EvaluationTrialEvidenceV1::Failed {
        stage,
        failed_run_id: current.run_id,
        terminal_state: current.state,
        terminal_version: current.version,
        subject_run_id,
        failure_value,
    })
}
/// Report extraction accepts no caller-supplied child Run IDs. Every child is
/// selected from the server-owned parent/Node relation and its exact deployment.
pub fn report(
    client: &PublicHttpClient,
    request_file: &Path,
    parent_run_id: &insight_platform_contracts::ResourceId,
    output: &Path,
) -> Result<Value, AgentCommandError> {
    use insight_platform_api::{product::ListPageV1, run::ChildRunViewV1};
    use insight_platform_contracts::{ResourceKind, RunState};
    if parent_run_id.kind() != ResourceKind::Run {
        return Err(invalid("evaluation parent must be a Run"));
    }
    let request: EvaluationPlanRequestV1 = read_json(request_file)?;
    let compiled = compile_evaluation_plan(request.clone())
        .map_err(|_| invalid("invalid evaluation Plan request"))?;
    if exact_artifact_json(client, &request.manifest_artifact)?
        != serde_json::to_value(&request.manifest)
            .map_err(|_| invalid("invalid evaluation manifest"))?
    {
        return Err(AgentCommandError::InvalidAuthority(
            "evaluation manifest Artifact differs".into(),
        ));
    }
    let parent = read_run(client, parent_run_id)?;
    if !matches!(
        parent.state,
        RunState::Succeeded | RunState::Failed | RunState::TimedOut | RunState::Cancelled
    ) {
        return Err(invalid("evaluation report requires a terminal parent Run"));
    }
    let mut links = std::collections::BTreeMap::new();
    let mut cursor: Option<String> = None;
    let mut seen_cursors = std::collections::BTreeSet::new();
    loop {
        let mut query = vec![("page_size".to_owned(), "50".to_owned())];
        if let Some(cursor) = &cursor {
            query.push(("cursor".to_owned(), cursor.clone()));
        }
        let page: ListPageV1<ChildRunViewV1> = client
            .get_body_json_query(
                &format!("/v1/runs/{parent_run_id}/children"),
                &query,
                reqwest::StatusCode::OK,
            )?
            .body;
        if page.schema_version != 1 || page.items.len() > 50 {
            return Err(AgentCommandError::InvalidAuthority(
                "child Run page bounds differ".into(),
            ));
        }
        for link in page.items {
            if link.validate_for(parent_run_id, None).is_err()
                || links
                    .insert(link.parent_plan_node_key.clone(), link)
                    .is_some()
                || links.len() > MAX_EVALUATION_TRIALS * 2
            {
                return Err(AgentCommandError::InvalidAuthority(
                    "child Run ancestry is ambiguous or exceeds the evaluation manifest".into(),
                ));
            }
        }
        match page.next_cursor {
            Some(next) => {
                let next = serde_json::to_value(next)
                    .map_err(|_| invalid("child cursor"))?
                    .as_str()
                    .ok_or_else(|| invalid("child cursor"))?
                    .to_owned();
                if !seen_cursors.insert(next.clone())
                    || seen_cursors.len() > MAX_EVALUATION_TRIALS * 2
                {
                    return Err(AgentCommandError::InvalidAuthority(
                        "child Run cursor cycle".into(),
                    ));
                }
                cursor = Some(next)
            }
            None => break,
        }
    }
    let mut trials = Vec::new();
    let mut counts = (0u32, 0u32, 0u32);
    for trial in request
        .manifest
        .trials()
        .map_err(|_| invalid("invalid evaluation trials"))?
    {
        let sample = request
            .manifest
            .samples
            .iter()
            .find(|sample| sample.sample_id == trial.sample_id)
            .ok_or_else(|| invalid("sample missing"))?;
        let subject = links.get(&trial_subject_node(&trial));
        let evaluator = links.get(&trial_evaluator_node(&trial));
        if subject.is_some_and(|link| link.child_agent_deployment != request.manifest.subject)
            || evaluator
                .is_some_and(|link| link.child_agent_deployment != request.manifest.evaluator)
        {
            return Err(AgentCommandError::InvalidAuthority(
                "evaluation child exact deployment differs".into(),
            ));
        }
        let evidence = match (subject, evaluator) {
            (Some(subject), _)
                if matches!(
                    subject.child_state,
                    RunState::Failed | RunState::TimedOut | RunState::Cancelled
                ) =>
            {
                let current = checked_child_run(client, subject)?;
                checked_subject_input(client, &request, sample, subject)?;
                if evaluator.is_some() {
                    return Err(AgentCommandError::InvalidAuthority(
                        "Evaluator exists after failed Subject".into(),
                    ));
                }
                terminal_failure(
                    client,
                    current,
                    EvaluationFailureStage::Subject,
                    Some(subject.child_run_id.clone()),
                )?
            }
            (Some(subject), Some(evaluator))
                if subject.child_state == RunState::Succeeded
                    && matches!(
                        evaluator.child_state,
                        RunState::Failed | RunState::TimedOut | RunState::Cancelled
                    ) =>
            {
                let subject_run = checked_child_run(client, subject)?;
                let evaluator_run = checked_child_run(client, evaluator)?;
                checked_evaluator_input(client, &request, &compiled, &trial, subject, evaluator)?;
                if read_run(client, &subject.child_run_id)? != subject_run {
                    return Err(AgentCommandError::InvalidAuthority(
                        "Subject Run changed during evidence read".into(),
                    ));
                }
                terminal_failure(
                    client,
                    evaluator_run,
                    EvaluationFailureStage::Evaluator,
                    Some(subject.child_run_id.clone()),
                )?
            }
            (Some(subject), Some(evaluator))
                if subject.child_state == RunState::Succeeded
                    && evaluator.child_state == RunState::Succeeded =>
            {
                let subject_run = checked_child_run(client, subject)?;
                let evaluator_run = checked_child_run(client, evaluator)?;
                let (subject_input, output, evaluator_input) = checked_evaluator_input(
                    client, &request, &compiled, &trial, subject, evaluator,
                )?;
                let (score, metric) = read_value(
                    client,
                    &evaluator.child_run_id,
                    evaluator
                        .output_value_id
                        .as_ref()
                        .ok_or_else(|| invalid("successful Evaluator has no output"))?,
                )?;
                if score.schema_digest != request.manifest.metric_schema.canonical_digest {
                    return Err(AgentCommandError::InvalidAuthority(
                        "metric schema differs".into(),
                    ));
                }
                request
                    .manifest
                    .metric_schema
                    .validate_instance(&metric)
                    .map_err(|_| invalid("metric result violates its exact schema"))?;
                if read_run(client, &subject.child_run_id)? != subject_run
                    || read_run(client, &evaluator.child_run_id)? != evaluator_run
                {
                    return Err(AgentCommandError::InvalidAuthority(
                        "evaluation Run changed during evidence read".into(),
                    ));
                }
                EvaluationTrialEvidenceV1::Scored {
                    subject_input: Box::new(subject_input),
                    evaluator_input: Box::new(evaluator_input),
                    output: Box::new(output),
                    score: Box::new(score),
                }
            }
            (None, None) => EvaluationTrialEvidenceV1::Missing {
                reason: EvaluationMissingReason::NotStarted,
            },
            (None, Some(_)) => {
                return Err(AgentCommandError::InvalidAuthority(
                    "Evaluator exists without its Subject ancestry".into(),
                ));
            }
            _ => EvaluationTrialEvidenceV1::Missing {
                reason: EvaluationMissingReason::EvidenceUnavailable,
            },
        };
        match &evidence {
            EvaluationTrialEvidenceV1::Scored { .. } => counts.0 += 1,
            EvaluationTrialEvidenceV1::Failed { .. } => counts.1 += 1,
            EvaluationTrialEvidenceV1::Missing { .. } => counts.2 += 1,
        }
        trials.push(EvaluationTrialResultV1 {
            trial,
            input: sample.input.clone(),
            expected: sample.expected.clone(),
            evidence,
        });
    }
    let expected_keys = request
        .manifest
        .trials()
        .map_err(|_| invalid("trials"))?
        .into_iter()
        .flat_map(|trial| [trial_subject_node(&trial), trial_evaluator_node(&trial)])
        .collect::<std::collections::BTreeSet<_>>();
    if links.keys().any(|key| !expected_keys.contains(key))
        || read_run(client, parent_run_id)? != parent
    {
        return Err(AgentCommandError::InvalidAuthority(
            "parent Run or child topology differs from evaluation".into(),
        ));
    }
    let report = EvaluationReportV1 {
        schema_version: 1,
        manifest: request.manifest_artifact,
        parent_run_id: parent_run_id.clone(),
        trials,
        scored_trials: counts.0,
        failed_trials: counts.1,
        missing_trials: counts.2,
    };
    report
        .validate_for(&request.manifest)
        .map_err(|_| invalid("evaluation report evidence is inconsistent"))?;
    let report_bytes = encode(&report)?;
    if output.exists() {
        let existing: Value = read_json(output)?;
        if encode(&existing)? != report_bytes {
            return Err(invalid(
                "existing evaluation report differs; choose a new output path",
            ));
        }
    } else {
        write_private(output, &report_bytes)?;
    }
    Ok(
        json!({"schema_version":1,"output":output,"parent_run_id":parent_run_id,"scored_trials":counts.0,"failed_trials":counts.1,"missing_trials":counts.2}),
    )
}
