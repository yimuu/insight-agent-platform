//! Public HTTP consumer fixtures with real compiler and evidence validation.
use super::*;
use crate::{artifact::ArtifactViewV1, workspace_assets::workspace_path};
use insight_platform_api::run::{run_etag, ChildRunViewV1, RunResultViewV1, RunViewV1};
use insight_platform_contracts::{
    ApiProblem, ApiProblemCode, ArtifactPurpose, ArtifactState, ClosedJsonSchema,
    DataClassification, ExactDeploymentRef, ExactPolicyBinding, ExactVersionRef, ResourceId,
    ResourceKind, RunState, Sha256Digest, UtcTimestamp, ValueRef,
};
use sha2::{Digest as _, Sha256};
use std::{
    collections::BTreeMap,
    net::TcpListener,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    thread,
    time::Duration,
};
use tempfile::TempDir;

fn id(kind: ResourceKind) -> ResourceId {
    format!("{}_{}", kind.descriptor().prefix, uuid::Uuid::now_v7())
        .parse()
        .unwrap()
}
fn digest(bytes: &[u8]) -> Sha256Digest {
    let hex: String = Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    format!("sha256:{hex}").parse().unwrap()
}
fn now() -> UtcTimestamp {
    UtcTimestamp::from_datetime(chrono::Utc::now())
}
fn policy() -> ExactPolicyBinding {
    ExactPolicyBinding {
        deployment: ExactDeploymentRef::new(
            id(ResourceKind::PolicyDeployment),
            digest(b"policy deployment"),
        )
        .unwrap(),
        revision: ExactVersionRef::new(
            id(ResourceKind::PolicyRevision),
            digest(b"policy revision"),
        )
        .unwrap(),
    }
}
#[derive(Clone)]
struct Response {
    body: Vec<u8>,
    etag: String,
    binary: bool,
    denied: bool,
}
impl Response {
    fn json(value: &impl Serialize, etag: String) -> Self {
        Self {
            body: encode(value).unwrap(),
            etag,
            binary: false,
            denied: false,
        }
    }
    fn deny() -> Self {
        let problem = ApiProblem {
            type_uri: "https://insight.platform/problems/permission_denied".into(),
            title: "Access denied".into(),
            status: 403,
            code: ApiProblemCode::PermissionDenied,
            detail: None,
            request_id: id(ResourceKind::ServerRequest),
            trace_id: "0123456789abcdef0123456789abcdef".parse().unwrap(),
            retryable: false,
            retry_after_ms: None,
            field_errors: vec![],
        };
        Self {
            denied: true,
            ..Self::json(&problem, String::new())
        }
    }
}
type Routes = BTreeMap<String, Response>;
fn artifact_route(routes: &mut Routes, value: &impl Serialize) -> ArtifactRef {
    let bytes = encode(value).unwrap();
    let reference = ArtifactRef::new(
        id(ResourceKind::Artifact),
        digest(&bytes),
        bytes.len() as u64,
        "application/json",
        DataClassification::Restricted,
        Some("evaluation.json".into()),
    )
    .unwrap();
    let stamp = now();
    let metadata = ArtifactViewV1 {
        schema_version: 1,
        artifact_id: reference.artifact_id().clone(),
        purpose: ArtifactPurpose::RunInput,
        classification: reference.classification(),
        state: ArtifactState::Ready,
        version: 1,
        expected_size_bytes: reference.byte_length(),
        declared_media_type: Some("application/json".into()),
        verified_media_type: Some("application/json".into()),
        content: Some(reference.clone()),
        retain_until: stamp.clone(),
        created_at: stamp.clone(),
        updated_at: stamp,
        etag: format!("\"{}-1\"", reference.artifact_id()),
    };
    routes.insert(
        format!("/v1/artifacts/{}", reference.artifact_id()),
        Response::json(&metadata, metadata.etag.clone()),
    );
    routes.insert(
        format!("/v1/artifacts/{}/content", reference.artifact_id()),
        Response {
            body: bytes,
            etag: format!("\"{}\"", reference.content_digest()),
            binary: true,
            denied: false,
        },
    );
    reference
}
struct Fixture {
    routes: Routes,
    request: EvaluationPlanRequestV1,
    compiled: EvaluationPlanV1,
    input: Value,
    expected: Value,
}
impl Fixture {
    fn new(input: Value) -> Self {
        let mut routes = Routes::new();
        let schema = ClosedJsonSchema::build(
            serde_json::from_slice(
                &fs::read(workspace_path(
                    "contracts/product-experience/agent-compiler/v2/schema-message.json",
                ))
                .unwrap(),
            )
            .unwrap(),
        )
        .unwrap();
        let expected = json!({"message":"expected"});
        let input_ref = artifact_route(&mut routes, &input);
        let expected_ref = artifact_route(&mut routes, &expected);
        let manifest = EvaluationManifestV1 {
            schema_version: 1,
            dataset_id: "fixture-data".into(),
            samples: vec![EvaluationSampleV1 {
                sample_id: "one".into(),
                input: input_ref,
                expected: Some(expected_ref),
                input_schema_digest: schema.canonical_digest.clone(),
                expected_schema_digest: Some(schema.canonical_digest.clone()),
            }],
            repetitions: 1,
            subject: ExactDeploymentRef::new(id(ResourceKind::AgentDeployment), digest(b"subject"))
                .unwrap(),
            evaluator: ExactDeploymentRef::new(
                id(ResourceKind::AgentDeployment),
                digest(b"evaluator"),
            )
            .unwrap(),
            metric_schema: schema.clone(),
        };
        let manifest_artifact = artifact_route(&mut routes, &manifest);
        let corpus: Value = serde_json::from_slice(
            &fs::read(workspace_path(
                "contracts/product-experience/agent-compiler/v2/corpus.json",
            ))
            .unwrap(),
        )
        .unwrap();
        let mut request = EvaluationPlanRequestV1 {
            deployment_features: Vec::new(),
            schema_version: 1,
            name: "fixture-evaluation".into(),
            display_name: "Fixture evaluation".into(),
            manifest,
            manifest_artifact,
            subject_input_schema: schema.clone(),
            subject_output_schema: schema.clone(),
            expected_schema: Some(schema),
            subject_selection_policy: policy(),
            evaluator_selection_policy: policy(),
            child_budget: insight_platform_plan::ChildBudgetLimit {
                maximum_duration_milliseconds: 30000,
                maximum_model_tokens: 1024,
                maximum_capability_calls: 2,
                maximum_artifact_bytes: 65536,
                maximum_descendant_runs: 1,
            },
            profile: serde_json::from_value(corpus["profile"].clone()).unwrap(),
        };
        // Explicit synthetic exact references for pure authoring/protocol fixtures, not Registry authority.
        let evaluator_schema =
            insight_platform_agent_compiler::evaluation::evaluation_evaluator_input_schema(
                &request.subject_input_schema,
                &request.subject_output_schema,
                request.expected_schema.as_ref(),
            )
            .unwrap();
        request.deployment_features = [
            (
                &request.manifest.subject,
                &request.subject_input_schema,
                &request.subject_output_schema,
            ),
            (
                &request.manifest.evaluator,
                &evaluator_schema,
                &request.manifest.metric_schema,
            ),
        ]
        .into_iter()
        .map(
            |(target, input, output)| insight_platform_contracts::AgentDeploymentFeaturesV1 {
                schema_version: 1,
                deployment: target.clone(),
                interface_contract_digest:
                    insight_platform_agent_compiler::agent_interface_contract_digest(input, output)
                        .unwrap(),
                required_features: Vec::new(),
            },
        )
        .collect();
        request
            .deployment_features
            .sort_by(|a, b| a.deployment.deployment_id.cmp(&b.deployment.deployment_id));
        let compiled =
            compile_evaluation_plan(request.clone()).expect("real evaluation compiler fixture");
        Self {
            routes,
            request,
            compiled,
            input,
            expected,
        }
    }
    fn request_file(&self, root: &Path) -> std::path::PathBuf {
        let path = root.join("request.json");
        fs::write(&path, encode(&self.request).unwrap()).unwrap();
        path
    }
    fn input_paths(&self) -> Vec<String> {
        [
            &self.request.manifest_artifact,
            &self.request.manifest.samples[0].input,
            self.request.manifest.samples[0].expected.as_ref().unwrap(),
        ]
        .into_iter()
        .flat_map(|reference| {
            [
                format!("/v1/artifacts/{}", reference.artifact_id()),
                format!("/v1/artifacts/{}/content", reference.artifact_id()),
            ]
        })
        .collect()
    }
}
struct Server {
    client: PublicHttpClient,
    seen: Arc<Mutex<Vec<String>>>,
    stop: Arc<AtomicBool>,
    handle: Option<thread::JoinHandle<()>>,
}
impl Drop for Server {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}
impl Server {
    fn finish(mut self) -> Vec<String> {
        self.stop.store(true, Ordering::SeqCst);
        self.handle.take().unwrap().join().unwrap();
        self.seen.lock().unwrap().clone()
    }
}
fn serve(routes: Routes) -> Server {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let address = listener.local_addr().unwrap();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let captured = seen.clone();
    let stop = Arc::new(AtomicBool::new(false));
    let stopped = stop.clone();
    let handle = thread::spawn(move || {
        while !stopped.load(Ordering::SeqCst) {
            let (mut stream, _) = match listener.accept() {
                Ok(stream) => stream,
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(2));
                    continue;
                }
                Err(error) => panic!("fixture accept: {error}"),
            };
            stream.set_nonblocking(false).unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(10)))
                .unwrap();
            let mut header = Vec::new();
            let mut byte = [0u8; 1];
            while !header.ends_with(b"\r\n\r\n") {
                stream.read_exact(&mut byte).unwrap();
                header.push(byte[0]);
                assert!(header.len() < 16384);
            }
            let header = String::from_utf8(header).unwrap();
            if header.starts_with("POST ") {
                assert!(header.starts_with("POST /v1/agent-authoring-bindings:resolve "));
                assert!(!header.to_ascii_lowercase().contains("idempotency-key:"));
                assert!(!header.to_ascii_lowercase().contains("if-match:"));
                let length: usize = header
                    .lines()
                    .find_map(|line| {
                        line.to_ascii_lowercase()
                            .strip_prefix("content-length:")
                            .map(|value| value.trim().parse().unwrap())
                    })
                    .unwrap();
                assert!(length <= 262144);
                let mut body = vec![0; length];
                stream.read_exact(&mut body).unwrap();
                let query: insight_platform_registry::authoring::ResolveAgentBindingsRequestV1 =
                    serde_json::from_slice(&body).unwrap();
                query.validate().unwrap();
            } else {
                assert!(header.starts_with("GET "));
            }
            assert!(header
                .to_ascii_lowercase()
                .contains("authorization: bearer evaluation-fixture"));
            let path = header.split_whitespace().nth(1).unwrap();
            captured.lock().unwrap().push(path.into());
            let response = routes
                .get(path)
                .unwrap_or_else(|| panic!("unexpected request {path}"));
            let status = if response.denied {
                "403 Forbidden"
            } else {
                "200 OK"
            };
            let attachment = if response.binary {
                "content-disposition: attachment\r\n"
            } else {
                ""
            };
            write!(stream, "HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncache-control: no-store, private, max-age=0\r\ntrace-id: 0123456789abcdef0123456789abcdef\r\netag: {}\r\ncontent-length: {}\r\n{attachment}connection: close\r\n\r\n", response.etag, response.body.len()).unwrap();
            stream.write_all(&response.body).unwrap();
        }
    });
    Server {
        client: PublicHttpClient::new(
            format!("http://{address}"),
            "evaluation-fixture".into(),
            Duration::from_secs(10),
        )
        .unwrap(),
        seen,
        stop,
        handle: Some(handle),
    }
}
fn assert_no_output(root: &Path, output: &Path) {
    assert!(!output.exists());
    assert_eq!(
        fs::read_dir(root).unwrap().count(),
        1,
        "only caller request should exist"
    );
}
#[test]
fn input_uses_exact_authorized_artifacts_and_frozen_schema_before_private_write() {
    let fixture = Fixture::new(json!({"message":"hello"}));
    let directory = TempDir::new().unwrap();
    let request = fixture.request_file(directory.path());
    let output = directory.path().join("input.json");
    let expected_paths = fixture.input_paths();
    let server = serve(fixture.routes);
    let result = execute(
        EvaluationAction::Input,
        directory.path(),
        &request,
        &output,
        Some(&server.client),
    )
    .unwrap();
    assert_eq!(
        result["schema_digest"],
        json!(fixture.compiled.parent_input_schema.canonical_digest)
    );
    assert_eq!(
        fs::read(&output).unwrap(),
        encode(&json!({"samples":{"one":{"input":fixture.input,"expected":fixture.expected}}}))
            .unwrap()
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            fs::metadata(&output).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }
    assert_eq!(server.finish(), expected_paths);
}
#[test]
fn input_denied_at_each_metadata_or_content_read_writes_nothing() {
    for denied_stage in 0..6 {
        let mut fixture = Fixture::new(json!({"message":"hello"}));
        let paths = fixture.input_paths();
        fixture
            .routes
            .insert(paths[denied_stage].clone(), Response::deny());
        let directory = TempDir::new().unwrap();
        let request = fixture.request_file(directory.path());
        let output = directory.path().join("input.json");
        let server = serve(fixture.routes);
        let result = execute(
            EvaluationAction::Input,
            directory.path(),
            &request,
            &output,
            Some(&server.client),
        );
        assert!(result.unwrap_err().to_string().contains("403"));
        assert_no_output(directory.path(), &output);
        assert_eq!(server.finish(), paths[..=denied_stage]);
    }
}
#[test]
fn input_rejects_changed_exact_metadata_bytes_schema_and_noncanonical_json_without_write() {
    for mode in 0..4 {
        let mut fixture = Fixture::new(if mode == 2 {
            json!({"message":false})
        } else {
            json!({"message":"hello"})
        });
        let paths = fixture.input_paths();
        if mode == 0 {
            let response = fixture.routes.get_mut(&paths[2]).unwrap();
            let mut metadata: ArtifactViewV1 = serde_json::from_slice(&response.body).unwrap();
            let original = metadata.content.take().unwrap();
            metadata.content = Some(
                ArtifactRef::new(
                    original.artifact_id().clone(),
                    digest(b"foreign content"),
                    original.byte_length(),
                    original.media_type(),
                    original.classification(),
                    original.display_name().map(str::to_owned),
                )
                .unwrap(),
            );
            response.body = encode(&metadata).unwrap();
        } else if mode == 1 {
            fixture.routes.get_mut(&paths[3]).unwrap().body[0] ^= 1;
        } else if mode == 3 {
            // Preserve exact bytes/digest while violating canonical JSON, so transport integrity alone is insufficient.
            let old = fixture.request.manifest.samples[0].input.clone();
            let bytes = b"{ \"message\": \"hello\" }".to_vec();
            let replacement = ArtifactRef::new(
                old.artifact_id().clone(),
                digest(&bytes),
                bytes.len() as u64,
                old.media_type(),
                old.classification(),
                old.display_name().map(str::to_owned),
            )
            .unwrap();
            let response = fixture.routes.get_mut(&paths[2]).unwrap();
            let mut metadata: ArtifactViewV1 = serde_json::from_slice(&response.body).unwrap();
            metadata.content = Some(replacement.clone());
            metadata.expected_size_bytes = bytes.len() as u64;
            response.body = encode(&metadata).unwrap();
            let response = fixture.routes.get_mut(&paths[3]).unwrap();
            response.body = bytes;
            response.etag = format!("\"{}\"", replacement.content_digest());
            fixture.request.manifest.samples[0].input = replacement;
            fixture.request.manifest_artifact =
                artifact_route(&mut fixture.routes, &fixture.request.manifest);
        }
        let directory = TempDir::new().unwrap();
        let request = fixture.request_file(directory.path());
        let output = directory.path().join("input.json");
        let paths = fixture.input_paths();
        let server = serve(fixture.routes);
        assert!(execute(
            EvaluationAction::Input,
            directory.path(),
            &request,
            &output,
            Some(&server.client)
        )
        .is_err());
        assert_no_output(directory.path(), &output);
        assert_eq!(server.finish(), paths[..if mode == 0 { 3 } else { 4 }]);
    }
}

#[derive(Clone, Copy)]
enum Outcome {
    Scored,
    SubjectFailed,
    EvaluatorFailed,
    Missing,
}
struct ReportFixture {
    fixture: Fixture,
    parent: RunViewV1,
    subject: RunViewV1,
    evaluator: RunViewV1,
    links: Vec<ChildRunViewV1>,
    value_paths: Vec<String>,
    children_path: String,
}
fn run(deployment: &ExactDeploymentRef, state: RunState) -> RunViewV1 {
    let run_id = id(ResourceKind::Run);
    let stamp = now();
    RunViewV1 {
        schema_version: 1,
        etag: run_etag(&run_id, 3),
        run_id,
        agent_deployment_id: deployment.deployment_id.clone(),
        state,
        version: 3,
        input_value_id: id(ResourceKind::RunValue),
        output_value_id: (state == RunState::Succeeded).then(|| id(ResourceKind::RunValue)),
        pause_generation: 0,
        cancel_generation: 0,
        deadline: stamp.clone(),
        started_at: Some(stamp.clone()),
        terminal_at: Some(stamp.clone()),
        created_at: stamp.clone(),
        updated_at: stamp,
    }
}
fn value_route(
    routes: &mut Routes,
    run: &RunViewV1,
    value_id: &ResourceId,
    schema: &Sha256Digest,
    value: Value,
) -> String {
    let path = format!("/v1/runs/{}/values/{value_id}/content", run.run_id);
    let response = RunResultViewV1 {
        schema_version: 1,
        run_id: run.run_id.clone(),
        value_id: value_id.clone(),
        classification: DataClassification::Restricted,
        schema_digest: schema.clone(),
        content_digest: digest(&encode(&value).unwrap()),
        value: ValueRef::Inline { value },
    };
    routes.insert(path.clone(), Response::json(&response, String::new()));
    path
}
impl ReportFixture {
    fn new(outcome: Outcome) -> Self {
        let mut fixture = Fixture::new(json!({"message":"hello"}));
        let parent = run(
            &ExactDeploymentRef::new(id(ResourceKind::AgentDeployment), digest(b"parent")).unwrap(),
            RunState::Succeeded,
        );
        let subject = run(
            &fixture.request.manifest.subject,
            if matches!(outcome, Outcome::SubjectFailed) {
                RunState::Failed
            } else {
                RunState::Succeeded
            },
        );
        let evaluator = run(
            &fixture.request.manifest.evaluator,
            if matches!(outcome, Outcome::EvaluatorFailed) {
                RunState::Failed
            } else {
                RunState::Succeeded
            },
        );
        for current in [&parent, &subject, &evaluator] {
            fixture.routes.insert(
                format!("/v1/runs/{}", current.run_id),
                Response::json(current, current.etag.clone()),
            );
        }
        let trial = fixture.request.manifest.trials().unwrap().remove(0);
        let mut links = Vec::new();
        if !matches!(outcome, Outcome::Missing) {
            for (current, deployment, node) in [
                (
                    &subject,
                    &fixture.request.manifest.subject,
                    trial_subject_node(&trial),
                ),
                (
                    &evaluator,
                    &fixture.request.manifest.evaluator,
                    trial_evaluator_node(&trial),
                ),
            ] {
                if matches!(outcome, Outcome::SubjectFailed) && current.run_id == evaluator.run_id {
                    continue;
                }
                links.push(ChildRunViewV1 {
                    schema_version: 1,
                    parent_run_id: parent.run_id.clone(),
                    parent_node_id: id(ResourceKind::NodeExecution),
                    parent_plan_node_key: node,
                    child_run_id: current.run_id.clone(),
                    child_agent_deployment: deployment.clone(),
                    child_state: current.state,
                    child_version: current.version,
                    input_value_id: current.input_value_id.clone(),
                    output_value_id: current.output_value_id.clone(),
                    created_at: now(),
                });
            }
        }
        let mut value_paths = vec![value_route(
            &mut fixture.routes,
            &subject,
            &subject.input_value_id,
            &fixture.request.subject_input_schema.canonical_digest,
            fixture.input.clone(),
        )];
        let subject_output = json!({"message":"actual output"});
        if let Some(output) = &subject.output_value_id {
            value_paths.push(value_route(
                &mut fixture.routes,
                &subject,
                output,
                &fixture.request.subject_output_schema.canonical_digest,
                subject_output.clone(),
            ));
        }
        let evaluator_input = json!({"trial_digest":trial.trial_digest,"sample_id":trial.sample_id,"repetition":trial.repetition,
            "input":fixture.input,"output":subject_output,"expected":fixture.expected});
        value_paths.push(value_route(
            &mut fixture.routes,
            &evaluator,
            &evaluator.input_value_id,
            &fixture.compiled.evaluator_input_schema.canonical_digest,
            evaluator_input,
        ));
        if let Some(output) = &evaluator.output_value_id {
            value_paths.push(value_route(
                &mut fixture.routes,
                &evaluator,
                output,
                &fixture.request.manifest.metric_schema.canonical_digest,
                json!({"message":"score=0.9"}),
            ));
        }
        let children_path = format!("/v1/runs/{}/children?page_size=50", parent.run_id);
        let mut result = Self {
            fixture,
            parent,
            subject,
            evaluator,
            links,
            value_paths,
            children_path,
        };
        result.update_links();
        result
    }
    fn update_links(&mut self) {
        self.fixture.routes.insert(
            self.children_path.clone(),
            Response::json(
                &json!({"schema_version":1,"items":self.links,"next_cursor":null}),
                String::new(),
            ),
        );
    }
}
#[test]
fn report_distinguishes_scored_terminal_failure_without_value_and_missing() {
    for outcome in [
        Outcome::Scored,
        Outcome::SubjectFailed,
        Outcome::EvaluatorFailed,
        Outcome::Missing,
    ] {
        let fixture = ReportFixture::new(outcome);
        let directory = TempDir::new().unwrap();
        let request_file = fixture.fixture.request_file(directory.path());
        let output = directory.path().join("report.json");
        let manifest = fixture.fixture.request.manifest.clone();
        let server = serve(fixture.fixture.routes);
        let result = report(
            &server.client,
            &request_file,
            &fixture.parent.run_id,
            &output,
        )
        .unwrap();
        let actual: EvaluationReportV1 =
            serde_json::from_slice(&fs::read(&output).unwrap()).unwrap();
        actual.validate_for(&manifest).unwrap();
        match outcome {
            Outcome::Scored => {
                assert_eq!(result["scored_trials"], 1);
                let EvaluationTrialEvidenceV1::Scored {
                    subject_input,
                    evaluator_input,
                    output,
                    score,
                } = &actual.trials[0].evidence
                else {
                    panic!("expected scored evidence")
                };
                assert_eq!(subject_input.run_id, fixture.subject.run_id);
                assert_eq!(output.run_id, fixture.subject.run_id);
                assert_eq!(evaluator_input.run_id, fixture.evaluator.run_id);
                assert_eq!(score.run_id, fixture.evaluator.run_id);
                assert_eq!(
                    subject_input.content_digest,
                    *manifest.samples[0].input.content_digest()
                );
            }
            Outcome::SubjectFailed | Outcome::EvaluatorFailed => {
                assert_eq!(result["failed_trials"], 1);
                assert_eq!(result["scored_trials"], 0);
                let EvaluationTrialEvidenceV1::Failed {
                    stage,
                    failed_run_id,
                    terminal_state,
                    failure_value,
                    ..
                } = &actual.trials[0].evidence
                else {
                    panic!("expected explicit failure")
                };
                assert_eq!(*terminal_state, RunState::Failed);
                assert!(failure_value.is_none());
                let (expected_stage, expected_run) = if matches!(outcome, Outcome::SubjectFailed) {
                    (EvaluationFailureStage::Subject, &fixture.subject.run_id)
                } else {
                    (EvaluationFailureStage::Evaluator, &fixture.evaluator.run_id)
                };
                assert_eq!(*stage, expected_stage);
                assert_eq!(failed_run_id, expected_run);
            }
            Outcome::Missing => {
                assert_eq!(result["missing_trials"], 1);
                assert!(matches!(
                    actual.trials[0].evidence,
                    EvaluationTrialEvidenceV1::Missing {
                        reason: EvaluationMissingReason::NotStarted
                    }
                ));
            }
        }
        let seen = server.finish();
        assert_eq!(
            seen.iter()
                .filter(|path| **path == format!("/v1/runs/{}", fixture.parent.run_id))
                .count(),
            2
        );
        let expected_body_reads = match outcome {
            Outcome::Scored => 4,
            Outcome::SubjectFailed => 1,
            Outcome::EvaluatorFailed => 3,
            Outcome::Missing => 0,
        };
        assert_eq!(
            seen.iter().filter(|path| path.contains("/values/")).count(),
            expected_body_reads
        );
    }
}
#[test]
fn report_rejects_foreign_children_wrong_digest_and_denied_evidence_without_write() {
    for mode in 0..7 {
        let mut fixture = ReportFixture::new(Outcome::Scored);
        match mode {
            0 => {
                fixture.links[0].parent_run_id = id(ResourceKind::Run);
                fixture.update_links();
            }
            1 => {
                fixture.links[0].child_agent_deployment =
                    ExactDeploymentRef::new(id(ResourceKind::AgentDeployment), digest(b"foreign"))
                        .unwrap();
                fixture.update_links();
            }
            2 => {
                let response = fixture
                    .fixture
                    .routes
                    .get_mut(&fixture.value_paths[0])
                    .unwrap();
                let mut value: RunResultViewV1 = serde_json::from_slice(&response.body).unwrap();
                value.content_digest = digest(b"wrong input digest");
                response.body = encode(&value).unwrap();
            }
            3 => {
                let response = fixture
                    .fixture
                    .routes
                    .get_mut(&fixture.value_paths[2])
                    .unwrap();
                let mut value: RunResultViewV1 = serde_json::from_slice(&response.body).unwrap();
                let ValueRef::Inline { value: body } = &mut value.value else {
                    unreachable!()
                };
                body["output"] = json!({"message":"different output"});
                value.content_digest = digest(&encode(body).unwrap());
                response.body = encode(&value).unwrap();
            }
            4 => {
                let response = fixture
                    .fixture
                    .routes
                    .get_mut(&fixture.value_paths[3])
                    .unwrap();
                let mut value: RunResultViewV1 = serde_json::from_slice(&response.body).unwrap();
                value.content_digest = digest(b"wrong score digest");
                response.body = encode(&value).unwrap();
            }
            5 => {
                fixture
                    .fixture
                    .routes
                    .insert(fixture.value_paths[3].clone(), Response::deny());
            }
            6 => {
                let mut current = fixture.subject.clone();
                current.agent_deployment_id = id(ResourceKind::AgentDeployment);
                fixture.fixture.routes.insert(
                    format!("/v1/runs/{}", current.run_id),
                    Response::json(&current, current.etag.clone()),
                );
            }
            _ => unreachable!(),
        }
        let directory = TempDir::new().unwrap();
        let request_file = fixture.fixture.request_file(directory.path());
        let output = directory.path().join("report.json");
        let server = serve(fixture.fixture.routes);
        let error = report(
            &server.client,
            &request_file,
            &fixture.parent.run_id,
            &output,
        )
        .unwrap_err();
        let expected_error = match mode {
            0 => "child Run ancestry is ambiguous",
            1 => "child exact deployment differs",
            2 | 4 => "Run value content digest differs",
            3 => "Evaluator did not receive the exact trial output",
            5 => "403",
            6 => "evaluation Run evidence changed",
            _ => unreachable!(),
        };
        assert!(
            error.to_string().contains(expected_error),
            "mode {mode}: {error}"
        );
        assert_no_output(directory.path(), &output);
        let seen = server.finish();
        assert!(seen.iter().any(|path| path == &fixture.children_path));
    }
}
#[test]
fn failed_trial_still_requires_real_input_provenance() {
    for outcome in [Outcome::SubjectFailed, Outcome::EvaluatorFailed] {
        let mut fixture = ReportFixture::new(outcome);
        let response = fixture
            .fixture
            .routes
            .get_mut(&fixture.value_paths[0])
            .unwrap();
        let mut value: RunResultViewV1 = serde_json::from_slice(&response.body).unwrap();
        let wrong = json!({"message":"another sample"});
        value.content_digest = digest(&encode(&wrong).unwrap());
        value.value = ValueRef::Inline { value: wrong };
        response.body = encode(&value).unwrap();
        let directory = TempDir::new().unwrap();
        let request_file = fixture.fixture.request_file(directory.path());
        let output = directory.path().join("report.json");
        let server = serve(fixture.fixture.routes);
        assert!(report(
            &server.client,
            &request_file,
            &fixture.parent.run_id,
            &output
        )
        .unwrap_err()
        .to_string()
        .contains("different sample"));
        assert_no_output(directory.path(), &output);
        assert!(server
            .finish()
            .iter()
            .any(|path| path == &fixture.value_paths[0]));
    }
}

#[test]
fn child_pagination_follows_an_empty_page_and_rejects_a_cursor_cycle() {
    for cycle in [false, true] {
        let mut fixture = ReportFixture::new(Outcome::Missing);
        let next_path = format!("{}&cursor=opaque-next", fixture.children_path);
        let first = Response::json(
            &json!({"schema_version":1,"items":[],"next_cursor":"opaque-next"}),
            String::new(),
        );
        fixture
            .fixture
            .routes
            .insert(fixture.children_path.clone(), first.clone());
        fixture.fixture.routes.insert(
            next_path.clone(),
            if cycle {
                first
            } else {
                Response::json(
                    &json!({"schema_version":1,"items":[],"next_cursor":null}),
                    String::new(),
                )
            },
        );
        let directory = TempDir::new().unwrap();
        let request_file = fixture.fixture.request_file(directory.path());
        let output = directory.path().join("report.json");
        let server = serve(fixture.fixture.routes);
        let result = report(
            &server.client,
            &request_file,
            &fixture.parent.run_id,
            &output,
        );
        if cycle {
            assert!(result.unwrap_err().to_string().contains("cursor cycle"));
            assert_no_output(directory.path(), &output);
        } else {
            assert_eq!(result.unwrap()["missing_trials"], 1);
        }
        let seen = server.finish();
        assert_eq!(
            seen.iter()
                .filter(|path| path.contains("/children?"))
                .cloned()
                .collect::<Vec<_>>(),
            vec![fixture.children_path, next_path]
        );
    }
}

#[test]
fn scored_artifact_value_requires_current_authorization_and_preserves_exact_evidence() {
    for denied in [false, true] {
        let mut fixture = ReportFixture::new(Outcome::Scored);
        let score_path = fixture.value_paths[3].clone();
        let mut score: RunResultViewV1 =
            serde_json::from_slice(&fixture.fixture.routes[&score_path].body).unwrap();
        let ValueRef::Inline { value } = &score.value else {
            unreachable!()
        };
        let artifact = artifact_route(&mut fixture.fixture.routes, value);
        assert_eq!(artifact.content_digest(), &score.content_digest);
        score.value = ValueRef::Artifact {
            artifact: artifact.clone(),
        };
        fixture
            .fixture
            .routes
            .insert(score_path, Response::json(&score, String::new()));
        let metadata_path = format!("/v1/artifacts/{}", artifact.artifact_id());
        let content_path = format!("{metadata_path}/content");
        if denied {
            fixture
                .fixture
                .routes
                .insert(content_path.clone(), Response::deny());
        }
        let directory = TempDir::new().unwrap();
        let request_file = fixture.fixture.request_file(directory.path());
        let output = directory.path().join("report.json");
        let server = serve(fixture.fixture.routes);
        let result = report(
            &server.client,
            &request_file,
            &fixture.parent.run_id,
            &output,
        );
        if denied {
            assert!(result.unwrap_err().to_string().contains("403"));
            assert_no_output(directory.path(), &output);
        } else {
            assert_eq!(result.unwrap()["scored_trials"], 1);
            let actual: EvaluationReportV1 =
                serde_json::from_slice(&fs::read(&output).unwrap()).unwrap();
            let EvaluationTrialEvidenceV1::Scored { score, .. } = &actual.trials[0].evidence else {
                panic!("expected score")
            };
            assert_eq!(score.artifact.as_ref(), Some(&artifact));
        }
        let seen = server.finish();
        assert!(seen.contains(&metadata_path));
        assert!(seen.contains(&content_path));
    }
}

#[test]
fn get_query_encodes_opaque_cursor_and_rejects_duplicate_or_unbounded_input_before_http() {
    let parent = id(ResourceKind::Run);
    let path = format!("/v1/runs/{parent}/children");
    let encoded = format!("{path}?page_size=50&cursor=opaque%2B%2F%26%3F%3D");
    let server = serve(BTreeMap::from([(
        encoded.clone(),
        Response::json(
            &json!({"schema_version":1,"items":[],"next_cursor":null}),
            String::new(),
        ),
    )]));
    for (request_path, pairs) in [
        (
            path.clone(),
            vec![
                ("cursor".into(), "one".into()),
                ("cursor".into(), "two".into()),
            ],
        ),
        (path.clone(), vec![("cursor".into(), "x".repeat(8193))]),
        (format!("{path}?cursor=injected"), vec![]),
        ("https://foreign.invalid/v1/runs".into(), vec![]),
    ] {
        let result = server.client.get_body_json_query::<Value>(
            &request_path,
            &pairs,
            reqwest::StatusCode::OK,
        );
        assert!(matches!(
            result,
            Err(crate::public_client::PublicClientError::InvalidConfiguration(_))
        ));
    }
    assert!(server.seen.lock().unwrap().is_empty());
    let response = server
        .client
        .get_body_json_query::<Value>(
            &path,
            &[
                ("page_size".into(), "50".into()),
                ("cursor".into(), "opaque+/&?=".into()),
            ],
            reqwest::StatusCode::OK,
        )
        .unwrap();
    assert_eq!(response.body["items"], json!([]));
    assert_eq!(server.finish(), vec![encoded]);
}

fn directory_entries(path: &Path) -> Vec<std::ffi::OsString> {
    let mut names = fs::read_dir(path)
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect::<Vec<_>>();
    names.sort();
    names
}

#[test]
fn private_output_is_complete_private_and_never_clobbers_existing_entries() {
    let directory = TempDir::new().unwrap();
    let output = directory.path().join("input.json");
    write_private(&output, b"{\"complete\":true}").unwrap();
    let before = directory_entries(directory.path());
    assert!(write_private(&output, b"replacement").is_err());
    assert_eq!(fs::read(&output).unwrap(), b"{\"complete\":true}");
    assert_eq!(directory_entries(directory.path()), before);
    #[cfg(unix)]
    {
        use std::os::unix::fs::{symlink, PermissionsExt};
        assert_eq!(
            fs::metadata(&output).unwrap().permissions().mode() & 0o777,
            0o600
        );
        let dangling = directory.path().join("dangling.json");
        let target = directory.path().join("missing.json");
        symlink(&target, &dangling).unwrap();
        assert!(write_private(&dangling, b"replacement").is_err());
        assert_eq!(fs::read_link(&dangling).unwrap(), target);
        assert!(!target.exists());
    }
    let empty = directory.path().join("empty.json");
    fs::create_dir(&empty).unwrap();
    let before = directory_entries(directory.path());
    assert!(write_private(&empty, b"replacement").is_err());
    assert!(empty.is_dir());
    assert!(directory_entries(&empty).is_empty());
    assert_eq!(directory_entries(directory.path()), before);
}

#[test]
fn partial_write_failure_leaves_no_final_file_and_can_retry() {
    let directory = TempDir::new().unwrap();
    let output = directory.path().join("report.json");
    let unrelated = directory.path().join(".evaluation-unrelated");
    fs::write(&unrelated, b"preserve").unwrap();
    let before = directory_entries(directory.path());
    let result = write_private_with(&output, |file| {
        file.write_all(b"{\"partial\":")?;
        file.sync_all()?;
        assert!(
            !output.exists(),
            "partial bytes must remain private staging"
        );
        Err(std::io::Error::new(
            std::io::ErrorKind::WriteZero,
            "injected write failure",
        ))
    });
    assert!(result.is_err());
    assert!(!output.exists());
    assert_eq!(directory_entries(directory.path()), before);
    assert_eq!(fs::read(&unrelated).unwrap(), b"preserve");
    write_private(&output, b"{\"complete\":true}").unwrap();
    assert_eq!(fs::read(&output).unwrap(), b"{\"complete\":true}");
}

#[test]
fn atomic_publication_preserves_a_destination_created_during_the_write() {
    let directory = TempDir::new().unwrap();
    let output = directory.path().join("report.json");
    let result = write_private_with(&output, |file| {
        file.write_all(b"our complete report")?;
        fs::write(&output, b"concurrent report")?;
        Ok(())
    });
    assert!(result.is_err());
    assert_eq!(fs::read(&output).unwrap(), b"concurrent report");
    assert_eq!(
        directory_entries(directory.path()),
        vec![std::ffi::OsString::from("report.json")]
    );
}

#[test]
#[cfg(unix)]
fn evaluation_init_checks_real_parent_containment_and_preserves_existing_output() {
    use std::os::unix::fs::{symlink, PermissionsExt};
    let fixture = Fixture::new(json!({"message":"sample"}));
    let project = TempDir::new().unwrap();
    let external = TempDir::new().unwrap();
    let request_file = fixture.request_file(project.path());
    let escape = project.path().join("escape");
    symlink(external.path(), &escape).unwrap();
    let before = directory_entries(project.path());
    let error = execute(
        EvaluationAction::Init,
        project.path(),
        &request_file,
        &escape.join("generated"),
        None,
    )
    .unwrap_err();
    assert!(error.to_string().contains("inside the project"));
    assert!(directory_entries(external.path()).is_empty());
    assert_eq!(directory_entries(project.path()), before);

    let actual_parent = project.path().join("actual");
    fs::create_dir(&actual_parent).unwrap();
    let alias = project.path().join("alias");
    symlink(&actual_parent, &alias).unwrap();
    let output = alias.join("generated");
    let result = execute(
        EvaluationAction::Init,
        project.path(),
        &request_file,
        &output,
        None,
    )
    .unwrap();
    let canonical_output = fs::canonicalize(&output).unwrap();
    assert_eq!(
        result["agent_manifest"],
        json!(canonical_output.join("agent.json"))
    );
    assert_eq!(
        fs::metadata(&output).unwrap().permissions().mode() & 0o777,
        0o700
    );
    assert_eq!(
        fs::metadata(output.join(".insight"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o700
    );
    let original = fs::read(output.join("agent.json")).unwrap();
    let compilation = crate::agent::compile_project(
        project.path(),
        crate::agent::capture_project_sources(
            project.path(),
            Path::new("actual/generated/agent.json"),
        )
        .unwrap(),
        fixture.request.profile.clone(),
    )
    .unwrap();
    let source_bundle: insight_platform_agent_compiler::AgentSourceBundleV1 =
        serde_json::from_slice(&compilation.source_bundle_bytes).unwrap();
    assert_eq!(
        source_bundle.bindings,
        fixture.compiled.source_bundle.bindings
    );
    assert!(execute(
        EvaluationAction::Init,
        project.path(),
        &request_file,
        &output,
        None
    )
    .is_err());
    assert_eq!(fs::read(output.join("agent.json")).unwrap(), original);
    assert_eq!(
        directory_entries(&actual_parent),
        vec![std::ffi::OsString::from("generated")]
    );

    let dangling = actual_parent.join("dangling");
    symlink(external.path().join("missing"), &dangling).unwrap();
    assert!(execute(
        EvaluationAction::Init,
        project.path(),
        &request_file,
        &dangling,
        None
    )
    .is_err());
    assert!(fs::symlink_metadata(&dangling)
        .unwrap()
        .file_type()
        .is_symlink());
    assert!(directory_entries(external.path()).is_empty());
}

#[test]
fn evaluation_init_resolves_exact_server_features_before_compiling_private_sources() {
    use insight_platform_registry::authoring::*;
    let mut fixture = Fixture::new(json!({"message":"actual protocol"}));
    let bindings = evaluation_dependency_bindings(&fixture.request).unwrap();
    let query = exact_feature_request(&bindings).unwrap().unwrap();
    let response = ResolveAgentBindingsResponseV1 {
        schema_version: 1,
        slots: bindings
            .into_iter()
            .map(|binding| {
                let insight_platform_contracts::AgentSlotTargetInputV1::ChildAgent {
                    candidates,
                    ..
                } = &binding.target
                else {
                    panic!("evaluation child")
                };
                let evidence = fixture
                    .request
                    .deployment_features
                    .iter()
                    .find(|item| item.deployment == candidates[0])
                    .unwrap()
                    .clone();
                AuthoringSlotResolutionV1 {
                    slot_id: binding.slot_id.clone(),
                    resolution: AuthoringResolutionV1::Resolved {
                        binding: Box::new(binding),
                        observed_contract_digests: vec![evidence.interface_contract_digest.clone()],
                        contract_match: None,
                        call_authorized: false,
                        deployment_features: vec![evidence],
                    },
                }
            })
            .collect(),
    };
    response.validate_for(&query).unwrap();
    fixture.routes.insert(
        "/v1/agent-authoring-bindings:resolve".into(),
        Response::json(&response, r#""query""#.into()),
    );
    // Client declarations cannot replace the actual resolver projection.
    fixture.request.deployment_features.clear();
    let directory = TempDir::new().unwrap();
    let path = fixture.request_file(directory.path());
    let output = directory.path().join("generated");
    let server = serve(fixture.routes);
    execute(
        EvaluationAction::Init,
        directory.path(),
        &path,
        &output,
        Some(&server.client),
    )
    .unwrap();
    let exact: insight_platform_agent_compiler::ResolvedAgentBindings = serde_json::from_slice(
        &fs::read(output.join(".insight/agent-exact-bindings.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(exact.deployment_features.len(), 2);
    assert_eq!(
        server.finish(),
        vec!["/v1/agent-authoring-bindings:resolve"]
    );
}
