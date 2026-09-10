//! Restore an exact published authoring bundle into a new private directory.
//! Authorization and integrity validation complete before filesystem staging begins.
use crate::{
    agent::AgentCommandError,
    artifact,
    public_client::{PublicHttpClient, PublicJsonResponse},
};
use insight_platform_agent_compiler::{
    self as compiler, AgentCompileResponseV1, AgentSourceBundleV1, ArtifactAuthority,
};
use insight_platform_api::resource::ResourceVersionViewV1;
use insight_platform_contracts::{
    canonical_json, parse_strict_json, ArtifactPurpose, ArtifactState, JsonLimits,
    RegistryResourceKind, ResourceDocument, ResourceId, ResourceKind, Sha256Digest,
};
use reqwest::StatusCode;
use serde::Serialize;
use std::{
    collections::BTreeMap,
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};

#[derive(Debug, Clone, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AgentRestoreReportV1 {
    pub schema_version: u32,
    pub agent_id: ResourceId,
    pub published_version_id: ResourceId,
    pub output_directory: PathBuf,
    pub manifest_path: PathBuf,
    pub compiler_profile_path: PathBuf,
    pub exact_bindings_path: PathBuf,
    pub source_bundle_digest: Sha256Digest,
    pub manifest_digest: Sha256Digest,
    pub typed_plan_digest: Sha256Digest,
    pub source_map_digest: Sha256Digest,
    pub source_map_path: PathBuf,
}
fn invalid(detail: &str) -> AgentCommandError {
    AgentCommandError::InvalidAuthority(detail.into())
}
fn local(detail: &str) -> AgentCommandError {
    AgentCommandError::InvalidLocalState(detail.into())
}
fn io(path: &Path, error: impl std::fmt::Display) -> AgentCommandError {
    AgentCommandError::Io {
        path: path.display().to_string(),
        detail: error.to_string(),
    }
}
fn json(value: &impl Serialize) -> Result<Vec<u8>, AgentCommandError> {
    canonical_json(
        &serde_json::to_value(value)
            .map_err(|_| invalid("restored authoring inputs cannot be encoded"))?,
    )
    .map_err(|_| invalid("restored authoring inputs cannot be encoded"))
}

pub fn restore_published_agent(
    client: &PublicHttpClient,
    agent_id: &ResourceId,
    published_version_id: &ResourceId,
    output_new_directory: &Path,
) -> Result<AgentRestoreReportV1, AgentCommandError> {
    if agent_id.kind() != ResourceKind::Agent
        || !matches!(
            published_version_id.kind(),
            ResourceKind::AgentInterfaceRevision | ResourceKind::AgentPlanRevision
        )
    {
        return Err(local(
            "restore requires exact Agent and published Agent revision IDs",
        ));
    }
    let output = checked_destination(output_new_directory)?;
    let response: PublicJsonResponse<ResourceVersionViewV1> = client.get_json(
        &format!("/v1/agents/{agent_id}/versions/{published_version_id}"),
        StatusCode::OK,
    )?;
    let version = response.body;
    version
        .validate()
        .map_err(|_| invalid("published Agent Version violates its owning contract"))?;
    if version.resource_id != *agent_id
        || version.resource_version_id != *published_version_id
        || version.resource_kind != RegistryResourceKind::Agent
        || version.etag != response.etag
    {
        return Err(invalid(
            "published Agent Version identity or ETag differs from the exact request",
        ));
    }
    let ResourceDocument::Agent(spec) = &version.payload.document else {
        return Err(invalid(
            "published Version does not contain an Agent document",
        ));
    };
    if published_version_id.kind() == ResourceKind::AgentPlanRevision
        && (version.content_digest != spec.typed_plan_digest
            || version.artifact_id.as_ref() != Some(&spec.typed_plan_artifact_id))
    {
        return Err(invalid(
            "published Plan revision does not bind its exact Plan Artifact",
        ));
    }
    let reference = &spec.authoring_package.artifact;
    if reference.byte_length() == 0
        || reference.byte_length() > compiler::MAX_AGENT_COMPILER_REQUEST_BYTES as u64
        || reference.media_type() != "application/json"
    {
        return Err(invalid(
            "authoring bundle is outside the bounded JSON Artifact contract",
        ));
    }
    let metadata = artifact::read_artifact(client, reference.artifact_id())?;
    if metadata.purpose != ArtifactPurpose::AuthoringDocument
        || metadata.state != ArtifactState::Ready
        || metadata.content.as_ref() != Some(reference)
    {
        return Err(invalid(
            "current authoring Artifact does not match the frozen exact reference",
        ));
    }
    let mut bytes = Vec::new();
    let downloaded = client.get_binary_to_writer(
        &format!("/v1/artifacts/{}/content", reference.artifact_id()),
        reference.byte_length(),
        &mut bytes,
    )?;
    if downloaded.content_length != reference.byte_length()
        || downloaded.content_digest != *reference.content_digest()
        || downloaded.content_type != reference.media_type()
        || downloaded.etag != format!("\"{}\"", reference.content_digest())
    {
        return Err(invalid(
            "authoring Artifact content differs from its exact length, digest, media type or ETag",
        ));
    }
    let value = parse_strict_json(
        &bytes,
        JsonLimits {
            max_bytes: compiler::MAX_AGENT_COMPILER_REQUEST_BYTES,
            max_depth: 32,
            max_properties_per_object: 1024,
            max_items_per_array: 4096,
            max_string_bytes: compiler::MAX_AGENT_SOURCE_FILE_BYTES,
        },
    )
    .map_err(|_| invalid("authoring bundle is not bounded strict JSON"))?;
    let bundle: AgentSourceBundleV1 = serde_json::from_value(value)
        .map_err(|_| invalid("authoring bundle violates the closed compiler input contract"))?;
    let compilation = match compiler::compile_source_bundle(bundle.clone()) {
        AgentCompileResponseV1::Compiled { compilation } => compilation,
        AgentCompileResponseV1::Rejected { .. } => {
            return Err(invalid(
                "authoring bundle is rejected by the current compiler semantics",
            ));
        }
    };
    if compilation.source_bundle_bytes != bytes
        || compilation.source_bundle_digest != *reference.content_digest()
        || compilation.compiled.manifest_digest != spec.authoring_package.manifest_digest
        || compilation.compiled.typed_plan_digest != spec.typed_plan_digest
    {
        return Err(invalid(
            "compiled authoring bundle differs from its published source, manifest or Plan identity",
        ));
    }
    // Reuse materialization's complete owning comparison with actual current Artifact metadata.
    // No speculative ArtifactRef or future owner identity is constructed for restore.
    let plan = artifact::read_artifact(client, &spec.typed_plan_artifact_id)?;
    let plan_reference = plan
        .content
        .ok_or_else(|| invalid("published Plan Artifact has no current exact content"))?;
    let restored = compilation.materialize(
        &ArtifactAuthority {
            purpose: metadata.purpose,
            state: metadata.state,
            artifact: reference.clone(),
        },
        &ArtifactAuthority {
            purpose: plan.purpose,
            state: plan.state,
            artifact: plan_reference,
        },
    )?;
    if restored != version.payload.document {
        return Err(invalid(
            "recompiled sources differ from the complete published Agent document",
        ));
    }
    let manifest = PathBuf::from(&bundle.sources.manifest_path);
    let private = manifest
        .parent()
        .unwrap_or_else(|| Path::new(""))
        .join(".insight");
    let profile_path = private.join("agent-compiler-profile.json");
    let bindings_path = private.join("agent-exact-bindings.json");
    let mut files: BTreeMap<PathBuf, Vec<u8>> = bundle
        .sources
        .files
        .iter()
        .map(|(name, text)| (PathBuf::from(name), text.as_bytes().to_vec()))
        .collect();
    for (path, content) in [
        (private.join("agent-source-bundle.json"), bytes),
        (
            private.join("source-map.json"),
            compilation.source_map_bytes.clone(),
        ),
        (profile_path.clone(), json(&bundle.profile)?),
        (bindings_path.clone(), json(&bundle.bindings)?),
    ] {
        if files.insert(path, content).is_some() {
            return Err(invalid(
                "source files overlap reserved restore context files",
            ));
        }
    }
    if let Some(model) = &bundle.bindings.model {
        let path = private.join("agent-model-bindings.json");
        let content = json(
            &serde_json::json!({"schema_version":1,"kind":"insight.platform.agent-model-bindings/v1","models":{model.manifest_ref.clone():model}}),
        )?;
        if files.insert(path, content).is_some() {
            return Err(invalid("source files overlap reserved model bindings"));
        }
    }
    let paths: Vec<_> = files.keys().collect();
    if paths.windows(2).any(|pair| pair[1].starts_with(pair[0])) {
        return Err(invalid("source file and directory paths overlap"));
    }
    write_new_directory(&output, &files)?;
    Ok(AgentRestoreReportV1 {
        schema_version: 1,
        agent_id: agent_id.clone(),
        published_version_id: published_version_id.clone(),
        output_directory: output,
        manifest_path: manifest,
        compiler_profile_path: profile_path,
        exact_bindings_path: bindings_path,
        source_bundle_digest: compilation.source_bundle_digest,
        manifest_digest: compilation.compiled.manifest_digest,
        typed_plan_digest: compilation.compiled.typed_plan_digest,
        source_map_digest: compilation.source_map_digest,
        source_map_path: private.join("source-map.json"),
    })
}

fn checked_destination(path: &Path) -> Result<PathBuf, AgentCommandError> {
    let name = path
        .file_name()
        .filter(|name| !name.is_empty())
        .ok_or_else(|| local("restore output must name a new directory"))?;
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let parent = fs::canonicalize(parent).map_err(|error| io(parent, error))?;
    if !parent.is_dir() {
        return Err(local("restore output parent must be an existing directory"));
    }
    let output = parent.join(name);
    match fs::symlink_metadata(&output) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(output),
        Ok(_) => Err(local("restore output already exists")),
        Err(error) => Err(io(&output, error)),
    }
}
fn write_new_directory(
    output: &Path,
    files: &BTreeMap<PathBuf, Vec<u8>>,
) -> Result<(), AgentCommandError> {
    let parent = output
        .parent()
        .ok_or_else(|| local("restore parent is missing"))?;
    let stage = parent.join(format!(".insight-agent-restore-{}", uuid::Uuid::now_v7()));
    let mut builder = fs::DirBuilder::new();
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
    }
    builder.create(&stage).map_err(|error| io(&stage, error))?;
    let result = (|| {
        let mut directories = std::collections::BTreeSet::from([stage.clone()]);
        for (relative, bytes) in files {
            let target = stage.join(relative);
            let directory = target
                .parent()
                .ok_or_else(|| local("source parent is missing"))?;
            builder
                .recursive(true)
                .create(directory)
                .map_err(|error| io(directory, error))?;
            let mut current = directory;
            while current.starts_with(&stage) {
                directories.insert(current.to_path_buf());
                if current == stage {
                    break;
                }
                current = current
                    .parent()
                    .ok_or_else(|| local("source path escaped staging"))?;
            }
            let mut options = OpenOptions::new();
            options.write(true).create_new(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                options.mode(0o600);
            }
            let mut file = options.open(&target).map_err(|error| io(&target, error))?;
            file.write_all(bytes)
                .and_then(|_| file.sync_all())
                .map_err(|error| io(&target, error))?;
        }
        for directory in directories.iter().rev() {
            fs::File::open(directory)
                .and_then(|file| file.sync_all())
                .map_err(|error| io(directory, error))?;
        }
        atomic_publish_new(&stage, output).map_err(|error| io(output, error))?;
        fs::File::open(parent)
            .and_then(|file| file.sync_all())
            .map_err(|error| io(parent, error))?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_dir_all(&stage);
    }
    result
}
#[cfg(any(target_os = "macos", target_os = "linux"))]
pub(crate) fn atomic_publish_new(source: &Path, destination: &Path) -> std::io::Result<()> {
    use std::os::unix::ffi::OsStrExt;
    let source = std::ffi::CString::new(source.as_os_str().as_bytes())
        .map_err(|_| std::io::Error::from(std::io::ErrorKind::InvalidInput))?;
    let destination = std::ffi::CString::new(destination.as_os_str().as_bytes())
        .map_err(|_| std::io::Error::from(std::io::ErrorKind::InvalidInput))?;
    // Both platform primitives atomically fail if *any* destination entry exists,
    // including an empty directory or a dangling symbolic link.
    #[cfg(target_os = "macos")]
    let result = unsafe {
        libc::renameatx_np(
            libc::AT_FDCWD,
            source.as_ptr(),
            libc::AT_FDCWD,
            destination.as_ptr(),
            libc::RENAME_EXCL,
        )
    };
    #[cfg(target_os = "linux")]
    let result = unsafe {
        libc::renameat2(
            libc::AT_FDCWD,
            source.as_ptr(),
            libc::AT_FDCWD,
            destination.as_ptr(),
            libc::RENAME_NOREPLACE,
        )
    };
    if result == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}
#[cfg(not(any(target_os = "macos", target_os = "linux")))]
pub(crate) fn atomic_publish_new(_source: &Path, _destination: &Path) -> std::io::Result<()> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "atomic directory restore is unsupported on this platform",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use compiler::{AgentCompilationV1, ArtifactIntent};
    use insight_platform_contracts::{
        ApiProblem, ApiProblemCode, ArtifactRef, PublishedVersionPayload, UtcTimestamp,
        ValidationSummary,
    };
    use sha2::{Digest as _, Sha256};
    use std::{
        io::Read,
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
        ResourceId::from_uuid_v7(kind, uuid::Uuid::now_v7()).unwrap()
    }
    fn digest(bytes: &[u8]) -> Sha256Digest {
        let mut text = String::from("sha256:");
        for byte in Sha256::digest(bytes) {
            text.push_str(&format!("{byte:02x}"))
        }
        text.parse().unwrap()
    }
    fn authority(intent: &ArtifactIntent) -> ArtifactAuthority {
        ArtifactAuthority {
            purpose: intent.purpose,
            state: ArtifactState::Ready,
            artifact: ArtifactRef::new(
                id(ResourceKind::Artifact),
                intent.content_digest.clone(),
                intent.byte_length,
                intent.media_type.clone(),
                intent.classification,
                intent.display_name.clone(),
            )
            .unwrap(),
        }
    }
    fn metadata(authority: &ArtifactAuthority) -> artifact::ArtifactViewV1 {
        let reference = &authority.artifact;
        let now = UtcTimestamp::from_datetime(chrono::Utc::now());
        artifact::ArtifactViewV1 {
            schema_version: 1,
            artifact_id: reference.artifact_id().clone(),
            purpose: authority.purpose,
            classification: reference.classification(),
            state: authority.state,
            version: 1,
            expected_size_bytes: reference.byte_length(),
            declared_media_type: Some(reference.media_type().into()),
            verified_media_type: Some(reference.media_type().into()),
            content: Some(reference.clone()),
            retain_until: now.clone(),
            created_at: now.clone(),
            updated_at: now,
            etag: format!("\"{}-1\"", reference.artifact_id()),
        }
    }
    struct Fixture {
        compilation: Box<AgentCompilationV1>,
        bundle: AgentSourceBundleV1,
        version: ResourceVersionViewV1,
        source: ArtifactAuthority,
        plan: ArtifactAuthority,
        bytes: Vec<u8>,
        deny_stage: Option<u8>,
        tamper: bool,
    }
    impl Fixture {
        fn new(model: bool) -> Self {
            let asset = |name: &str| {
                fs::read_to_string(crate::workspace_assets::workspace_path(format!(
                    "contracts/product-experience/agent-compiler/v2/{name}"
                )))
                .unwrap()
            };
            let corpus: serde_json::Value = serde_json::from_str(&asset("corpus.json")).unwrap();
            let profile = serde_json::from_value(corpus["profile"].clone()).unwrap();
            let mut bundle = AgentSourceBundleV1 {
                schema_version: 1,
                compiler_semantic_identity: compiler::compiler_semantic_identity(),
                compile_policy_inputs_digest: AgentSourceBundleV1::policy_digest(&profile),
                sources: compiler::AgentSourceFilesV1 {
                    manifest_path: "team/agent.yaml".into(),
                    files: BTreeMap::from([
                        (
                            "team/agent.yaml".into(),
                            asset(if model {
                                "model-chat.yaml"
                            } else {
                                "deterministic.yaml"
                            }),
                        ),
                        ("schema-message.json".into(), asset("schema-message.json")),
                    ]),
                },
                profile,
                bindings: if model {
                    serde_json::from_value(
                        corpus["cases"]
                            .as_array()
                            .unwrap()
                            .iter()
                            .find(|case| case["case_id"] == "model-chat-yaml")
                            .unwrap()["bindings"]
                            .clone(),
                    )
                    .unwrap()
                } else {
                    Default::default()
                },
            };
            if model {
                bundle
                    .sources
                    .files
                    .insert("schema-answer.json".into(), asset("schema-answer.json"));
            }
            // Exercise non-template Plan restoration while preserving its original source paths.
            if !model {
                let AgentCompileResponseV1::Compiled { compilation } =
                    compiler::compile_source_bundle(bundle.clone())
                else {
                    panic!("seed compilation")
                };
                let mut manifest: serde_json::Value =
                    serde_json::from_slice(&compilation.compiled.canonical_manifest_bytes).unwrap();
                manifest["spec"]["execution"] =
                    serde_json::json!({"kind":"full_plan","plan":"team/plan.json"});
                bundle.sources.files.insert(
                    "team/agent.yaml".into(),
                    serde_json::to_string(&manifest).unwrap(),
                );
                bundle.sources.files.insert(
                    "team/plan.json".into(),
                    String::from_utf8(compilation.compiled.typed_plan_bytes).unwrap(),
                );
            }
            let AgentCompileResponseV1::Compiled { compilation } =
                compiler::compile_source_bundle(bundle.clone())
            else {
                panic!("restoration fixture compilation")
            };
            let source = authority(&compilation.compiled.resource_intent.authoring_artifact);
            let plan = authority(&compilation.compiled.resource_intent.typed_plan_artifact);
            let document = compilation.materialize(&source, &plan).unwrap();
            let evidence = compiler::validate_frozen_agent_artifacts(
                &document,
                &source,
                &compilation.source_bundle_bytes,
                &plan,
                &compilation.compiled.typed_plan_bytes,
            )
            .unwrap();
            let version_id = id(ResourceKind::AgentPlanRevision);
            let content_digest = compilation.compiled.typed_plan_digest.clone();
            let version = ResourceVersionViewV1 {
                schema_version: 1,
                resource_id: id(ResourceKind::Agent),
                resource_kind: RegistryResourceKind::Agent,
                resource_version_id: version_id.clone(),
                revision_no: 1,
                content_digest: content_digest.clone(),
                artifact_id: Some(plan.artifact.artifact_id().clone()),
                payload: PublishedVersionPayload {
                    document,
                    validation: ValidationSummary {
                        program_requirement: Some(evidence.program_requirement),
                        validator_digest: digest(b"validator"),
                        validated_draft_digest: digest(b"draft"),
                        dependency_closure_digest: digest(b"closure"),
                        security_evidence_digest: digest(b"security"),
                        warnings: vec![],
                    },
                },
                created_at: UtcTimestamp::from_datetime(chrono::Utc::now()),
                etag: insight_platform_api::resource::resource_version_etag(
                    &version_id,
                    &content_digest,
                ),
            };
            version.validate().unwrap();
            let bytes = compilation.source_bundle_bytes.clone();
            Self {
                compilation,
                bundle,
                version,
                source,
                plan,
                bytes,
                deny_stage: None,
                tamper: false,
            }
        }
        fn replace_source(&mut self, bytes: Vec<u8>) {
            let old = &self.source.artifact;
            self.source.artifact = ArtifactRef::new(
                old.artifact_id().clone(),
                digest(&bytes),
                bytes.len() as u64,
                old.media_type(),
                old.classification(),
                old.display_name().map(str::to_owned),
            )
            .unwrap();
            let ResourceDocument::Agent(spec) = &mut self.version.payload.document else {
                unreachable!()
            };
            spec.authoring_package.artifact = self.source.artifact.clone();
            self.bytes = bytes;
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
        fn finish(mut self, expected_requests: usize) {
            self.stop.store(true, Ordering::SeqCst);
            self.handle.take().unwrap().join().unwrap();
            assert_eq!(self.seen.lock().unwrap().len(), expected_requests);
        }
    }
    fn serve(fixture: Fixture) -> Server {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let address = listener.local_addr().unwrap();
        let seen = Arc::new(Mutex::new(Vec::new()));
        let captured = seen.clone();
        let stop = Arc::new(AtomicBool::new(false));
        let shutdown = stop.clone();
        let handle = thread::spawn(move || {
            while !shutdown.load(Ordering::SeqCst) {
                let (mut stream, _) = match listener.accept() {
                    Ok(connection) => connection,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(2));
                        continue;
                    }
                    Err(error) => panic!("{error}"),
                };
                stream.set_nonblocking(false).unwrap();
                stream
                    .set_read_timeout(Some(Duration::from_secs(3)))
                    .unwrap();
                let mut header = Vec::new();
                let mut byte = [0; 1];
                while !header.ends_with(b"\r\n\r\n") {
                    stream.read_exact(&mut byte).unwrap();
                    header.push(byte[0]);
                    assert!(header.len() < 16384)
                }
                let head = String::from_utf8(header).unwrap();
                assert!(head.starts_with("GET "));
                assert!(head
                    .to_ascii_lowercase()
                    .contains("authorization: bearer restore-fixture"));
                let path = head.split_whitespace().nth(1).unwrap().to_owned();
                captured.lock().unwrap().push(path.clone());
                let stage = if path
                    == format!(
                        "/v1/agents/{}/versions/{}",
                        fixture.version.resource_id, fixture.version.resource_version_id
                    ) {
                    1
                } else if path == format!("/v1/artifacts/{}", fixture.source.artifact.artifact_id())
                {
                    2
                } else if path
                    == format!(
                        "/v1/artifacts/{}/content",
                        fixture.source.artifact.artifact_id()
                    )
                {
                    3
                } else if path == format!("/v1/artifacts/{}", fixture.plan.artifact.artifact_id()) {
                    4
                } else {
                    panic!("unexpected fixture path {path}")
                };
                let denied = fixture.deny_stage == Some(stage);
                let trace = "0123456789abcdef0123456789abcdef";
                let (body, etag, binary) = if denied {
                    let problem = ApiProblem {
                        type_uri: "https://insight.platform/problems/permission_denied".into(),
                        title: "Access denied".into(),
                        status: 403,
                        code: ApiProblemCode::PermissionDenied,
                        detail: None,
                        request_id: id(ResourceKind::ServerRequest),
                        trace_id: trace.parse().unwrap(),
                        retryable: false,
                        retry_after_ms: None,
                        field_errors: vec![],
                    };
                    (serde_json::to_vec(&problem).unwrap(), String::new(), false)
                } else {
                    match stage {
                        1 => (
                            serde_json::to_vec(&fixture.version).unwrap(),
                            fixture.version.etag.clone(),
                            false,
                        ),
                        2 => {
                            let view = metadata(&fixture.source);
                            (serde_json::to_vec(&view).unwrap(), view.etag, false)
                        }
                        3 => {
                            let mut bytes = fixture.bytes.clone();
                            if fixture.tamper {
                                bytes[0] ^= 1;
                            }
                            (
                                bytes,
                                format!("\"{}\"", fixture.source.artifact.content_digest()),
                                true,
                            )
                        }
                        4 => {
                            let view = metadata(&fixture.plan);
                            (serde_json::to_vec(&view).unwrap(), view.etag, false)
                        }
                        _ => unreachable!(),
                    }
                };
                let extra = if binary {
                    "content-disposition: attachment\r\n"
                } else {
                    ""
                };
                let status = if denied { "403 Forbidden" } else { "200 OK" };
                write!(stream,"HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\ncache-control: no-store, private, max-age=0\r\ntrace-id: {trace}\r\netag: {etag}\r\n{extra}connection: close\r\n\r\n",body.len()).unwrap();
                stream.write_all(&body).unwrap();
            }
        });
        Server {
            client: PublicHttpClient::new(
                format!("http://{address}"),
                "restore-fixture".into(),
                Duration::from_secs(3),
            )
            .unwrap(),
            seen,
            stop,
            handle: Some(handle),
        }
    }
    #[test]
    fn exact_full_plan_and_model_sources_restore_private_nested_context_and_recompile_identically()
    {
        for model in [false, true] {
            let fixture = Fixture::new(model);
            let agent = fixture.version.resource_id.clone();
            let version = fixture.version.resource_version_id.clone();
            let expected = fixture.bundle.clone();
            let compiled = fixture.compilation.clone();
            let server = serve(fixture);
            let directory = TempDir::new().unwrap();
            let output = directory.path().join("restored");
            let report =
                restore_published_agent(&server.client, &agent, &version, &output).unwrap();
            assert_eq!(report.manifest_path, Path::new("team/agent.yaml"));
            assert_eq!(server.seen.lock().unwrap().len(), 4);
            for (path, text) in &expected.sources.files {
                assert_eq!(fs::read(output.join(path)).unwrap(), text.as_bytes());
            }
            let frozen: AgentSourceBundleV1 = serde_json::from_slice(
                &fs::read(output.join("team/.insight/agent-source-bundle.json")).unwrap(),
            )
            .unwrap();
            assert_eq!(frozen, expected);
            let profile: compiler::AgentCompilerProfile = serde_json::from_slice(
                &fs::read(output.join(&report.compiler_profile_path)).unwrap(),
            )
            .unwrap();
            assert_eq!(profile, expected.profile);
            let bindings: compiler::ResolvedAgentBindings = serde_json::from_slice(
                &fs::read(output.join(&report.exact_bindings_path)).unwrap(),
            )
            .unwrap();
            assert_eq!(bindings, expected.bindings);
            let rebuilt = crate::agent::compile_project(
                &output,
                crate::agent::capture_project_sources(&output, &report.manifest_path).unwrap(),
                profile,
            )
            .unwrap();
            assert_eq!(rebuilt, *compiled);
            assert!(!output.join(".insight/runtime").exists());
            crate::validate_local_agent(
                &output,
                &report.manifest_path,
                false,
                crate::agent::AgentOutputOptions::default(),
            )
            .expect(
                "default offline validate must use the restored profile without runtime bootstrap",
            );
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                assert_eq!(
                    fs::metadata(&output).unwrap().permissions().mode() & 0o777,
                    0o700
                );
                for path in expected.sources.files.keys() {
                    assert_eq!(
                        fs::metadata(output.join(path))
                            .unwrap()
                            .permissions()
                            .mode()
                            & 0o777,
                        0o600
                    );
                }
                assert_eq!(
                    fs::metadata(output.join(report.exact_bindings_path))
                        .unwrap()
                        .permissions()
                        .mode()
                        & 0o777,
                    0o600
                );
            }
            let profile_path = output.join(&report.compiler_profile_path);
            let mut altered: serde_json::Value =
                serde_json::from_slice(&fs::read(&profile_path).unwrap()).unwrap();
            altered["unrecognized_restore_authority"] = serde_json::json!(true);
            fs::write(&profile_path, json(&altered).unwrap()).unwrap();
            let invalid = crate::validate_local_agent(
                &output,
                &report.manifest_path,
                false,
                crate::agent::AgentOutputOptions::default(),
            )
            .unwrap_err();
            assert!(invalid
                .to_string()
                .contains("compiler profile violates its closed contract"));
            server.finish(4);
        }
    }
    #[test]
    fn denied_version_metadata_content_or_plan_leaves_no_output_or_staging() {
        for stage in 1..=4 {
            let mut fixture = Fixture::new(false);
            fixture.deny_stage = Some(stage);
            let agent = fixture.version.resource_id.clone();
            let version = fixture.version.resource_version_id.clone();
            let server = serve(fixture);
            let directory = TempDir::new().unwrap();
            let result = restore_published_agent(
                &server.client,
                &agent,
                &version,
                &directory.path().join("out"),
            );
            assert!(result.is_err());
            assert!(result.unwrap_err().to_string().contains("403"));
            assert_eq!(fs::read_dir(directory.path()).unwrap().count(), 0);
            assert_eq!(server.seen.lock().unwrap().len(), stage as usize);
            server.finish(stage as usize);
        }
    }
    #[test]
    fn changed_content_duplicate_keys_and_stale_compiler_are_rejected_before_any_write() {
        for mode in 0..3 {
            let mut fixture = Fixture::new(false);
            if mode == 0 {
                fixture.tamper = true
            } else if mode == 1 {
                let text = String::from_utf8(fixture.bytes.clone()).unwrap();
                fixture.replace_source(
                    text.replacen(
                        "\"schema_version\":1",
                        "\"schema_version\":1,\"schema_version\":1",
                        1,
                    )
                    .into_bytes(),
                )
            } else {
                let mut bundle = fixture.bundle.clone();
                bundle.compiler_semantic_identity = digest(b"unsupported compiler");
                fixture.replace_source(json(&bundle).unwrap())
            }
            let agent = fixture.version.resource_id.clone();
            let version = fixture.version.resource_version_id.clone();
            let server = serve(fixture);
            let directory = TempDir::new().unwrap();
            assert!(restore_published_agent(
                &server.client,
                &agent,
                &version,
                &directory.path().join("out")
            )
            .is_err());
            assert_eq!(fs::read_dir(directory.path()).unwrap().count(), 0);
            assert_eq!(server.seen.lock().unwrap().len(), 3);
            server.finish(3);
        }
    }
    #[test]
    fn source_traversal_normalized_duplicates_and_file_bounds_are_rejected_before_staging() {
        for mode in 0..7 {
            let mut fixture = Fixture::new(false);
            let mut bundle = fixture.bundle.clone();
            match mode {
                0 => {
                    bundle
                        .sources
                        .files
                        .insert("../escaped.json".into(), "{}".into());
                }
                1 => {
                    bundle
                        .sources
                        .files
                        .insert("/absolute.json".into(), "{}".into());
                }
                2 => {
                    bundle.sources.files.insert(
                        "team/./agent.yaml".into(),
                        bundle.sources.files["team/agent.yaml"].clone(),
                    );
                }
                3 => {
                    bundle
                        .sources
                        .files
                        .insert("team\\agent.yaml".into(), "{}".into());
                }
                4 => {
                    for index in 0..=compiler::MAX_AGENT_SOURCE_FILES {
                        bundle
                            .sources
                            .files
                            .insert(format!("extra-{index}.json"), "{}".into());
                    }
                }
                5 => {
                    bundle.sources.files.insert(
                        "large.json".into(),
                        "x".repeat(compiler::MAX_AGENT_SOURCE_FILE_BYTES + 1),
                    );
                }
                6 => {
                    for index in 0..5 {
                        bundle.sources.files.insert(
                            format!("large-{index}.json"),
                            "x".repeat(compiler::MAX_AGENT_SOURCE_FILE_BYTES),
                        );
                    }
                }
                _ => unreachable!(),
            }
            assert!(bundle.validate().is_err());
            fixture.replace_source(json(&bundle).unwrap());
            let agent = fixture.version.resource_id.clone();
            let version = fixture.version.resource_version_id.clone();
            let server = serve(fixture);
            let directory = TempDir::new().unwrap();
            assert!(restore_published_agent(
                &server.client,
                &agent,
                &version,
                &directory.path().join("out")
            )
            .is_err());
            assert_eq!(fs::read_dir(directory.path()).unwrap().count(), 0);
            assert_eq!(server.seen.lock().unwrap().len(), 3);
            server.finish(3);
        }
    }
    #[test]
    fn oversized_authoring_artifact_is_rejected_before_content_or_filesystem_access() {
        let mut fixture = Fixture::new(false);
        let reference = &fixture.source.artifact;
        let oversized = ArtifactRef::new(
            reference.artifact_id().clone(),
            reference.content_digest().clone(),
            compiler::MAX_AGENT_COMPILER_REQUEST_BYTES as u64 + 1,
            reference.media_type(),
            reference.classification(),
            None,
        )
        .unwrap();
        let ResourceDocument::Agent(spec) = &mut fixture.version.payload.document else {
            unreachable!()
        };
        spec.authoring_package.artifact = oversized;
        let agent = fixture.version.resource_id.clone();
        let version = fixture.version.resource_version_id.clone();
        let server = serve(fixture);
        let directory = TempDir::new().unwrap();
        assert!(restore_published_agent(
            &server.client,
            &agent,
            &version,
            &directory.path().join("out")
        )
        .is_err());
        assert_eq!(server.seen.lock().unwrap().len(), 1);
        assert_eq!(fs::read_dir(directory.path()).unwrap().count(), 0);
        server.finish(1);
    }
    #[test]
    fn published_identity_and_compiled_document_mismatch_do_not_write() {
        for wrong_identity in [true, false] {
            let mut fixture = Fixture::new(false);
            let agent = fixture.version.resource_id.clone();
            let version = fixture.version.resource_version_id.clone();
            if wrong_identity {
                fixture.version.resource_kind = RegistryResourceKind::Policy;
            } else {
                let ResourceDocument::Agent(spec) = &mut fixture.version.payload.document else {
                    unreachable!()
                };
                spec.default_deadline_seconds += 1;
            }
            let server = serve(fixture);
            let directory = TempDir::new().unwrap();
            assert!(restore_published_agent(
                &server.client,
                &agent,
                &version,
                &directory.path().join("out")
            )
            .is_err());
            assert_eq!(fs::read_dir(directory.path()).unwrap().count(), 0);
            server.finish(if wrong_identity { 1 } else { 4 });
        }
    }
    #[test]
    fn existing_directory_and_atomic_publish_race_never_replace_even_empty_targets() {
        let directory = TempDir::new().unwrap();
        let target = directory.path().join("existing");
        fs::create_dir(&target).unwrap();
        assert!(checked_destination(&target).is_err());
        let files = BTreeMap::from([(PathBuf::from("agent.yaml"), b"private".to_vec())]);
        assert!(write_new_directory(&target, &files).is_err());
        assert_eq!(fs::read_dir(&target).unwrap().count(), 0);
        assert_eq!(fs::read_dir(directory.path()).unwrap().count(), 1);
        #[cfg(unix)]
        {
            let dangling = directory.path().join("link");
            std::os::unix::fs::symlink(directory.path().join("missing"), &dangling).unwrap();
            assert!(checked_destination(&dangling).is_err());
        }
    }
    #[test]
    fn agent_run_defaults_read_exact_published_version_and_send_active_condition_without_retry() {
        use insight_platform_api::resource::{
            deployment_closure_digest, deployment_etag, resource_etag, DeploymentViewV1,
            ResourceViewV1,
        };
        use insight_platform_contracts::{
            AdministrativeGate, AgentDeploymentClosure, DeploymentClosure, EntityLifecycle,
            ExactDeploymentRef, ExactVersionRef, ResourceDraftPayload,
        };
        let fixture = Fixture::new(false);
        let agent = fixture.version.resource_id.clone();
        let deployment_id = id(ResourceKind::AgentDeployment);
        let intent = &fixture.compilation.compiled.deployment_intent;
        let closure = DeploymentClosure::Agent(AgentDeploymentClosure {
            interface: ExactVersionRef::new(
                id(ResourceKind::AgentInterfaceRevision),
                fixture
                    .compilation
                    .compiled
                    .resource_intent
                    .contract_digest
                    .clone(),
            )
            .unwrap(),
            plan: ExactVersionRef::new(
                fixture.version.resource_version_id.clone(),
                fixture.version.content_digest.clone(),
            )
            .unwrap(),
            entry_node_id: intent.entry_node_id.clone(),
            entry_node_kind: intent.entry_node_kind,
            slots: vec![],
            policies: intent.policies.clone(),
            execution_profile: intent.execution_profile.clone(),
        });
        let closure_digest = deployment_closure_digest(&closure).unwrap();
        let exact = ExactDeploymentRef::new(deployment_id.clone(), closure_digest.clone()).unwrap();
        let deployment = DeploymentViewV1 {
            schema_version: 1,
            deployment_id: deployment_id.clone(),
            resource_id: agent.clone(),
            resource_kind: RegistryResourceKind::Agent,
            resource_version_id: fixture.version.resource_version_id.clone(),
            environment: "development".into(),
            closure_digest: closure_digest.clone(),
            closure,
            created_at: fixture.version.created_at.clone(),
            etag: deployment_etag(&deployment_id, &closure_digest),
        };
        deployment.validate().unwrap();
        let ResourceDocument::Agent(published_spec) = &fixture.version.payload.document else {
            panic!("Agent")
        };
        let expected_schema = published_spec.input_schema.canonical_digest.clone();
        let expected_deadline = published_spec.default_deadline_seconds;
        let mut draft = fixture.version.payload.document.clone();
        let ResourceDocument::Agent(draft_spec) = &mut draft else {
            unreachable!()
        };
        draft_spec.default_deadline_seconds += 1;
        let resource = ResourceViewV1 {
            schema_version: 1,
            resource_id: agent.clone(),
            active_deployment_id: Some(deployment_id.clone()),
            resource_kind: RegistryResourceKind::Agent,
            lifecycle_state: EntityLifecycle::Active,
            gate_state: AdministrativeGate::Enabled,
            draft_generation: 2,
            version: 2,
            draft: ResourceDraftPayload {
                alias: None,
                display_name: "edited draft".into(),
                document: draft,
                validation: None,
            },
            etag: resource_etag(&agent, 2),
        };
        let routes = [
            (
                format!("/v1/agents/{agent}"),
                serde_json::to_vec(&resource).unwrap(),
                resource.etag,
            ),
            (
                format!("/v1/agents/{agent}/deployments/{deployment_id}"),
                serde_json::to_vec(&deployment).unwrap(),
                deployment.etag,
            ),
            (
                format!(
                    "/v1/agents/{agent}/versions/{}",
                    fixture.version.resource_version_id
                ),
                serde_json::to_vec(&fixture.version).unwrap(),
                fixture.version.etag.clone(),
            ),
        ];
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let address = listener.local_addr().unwrap();
        let expected_exact = exact.clone();
        let server = thread::spawn(move || {
            let until = std::time::Instant::now() + Duration::from_secs(10);
            let mut seen = 0;
            while seen < 4 && std::time::Instant::now() < until {
                let (mut stream, _) = match listener.accept() {
                    Ok(v) => v,
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(2));
                        continue;
                    }
                    Err(e) => panic!("HTTP accept {e}"),
                };
                stream.set_nonblocking(false).unwrap();
                stream
                    .set_read_timeout(Some(Duration::from_secs(3)))
                    .unwrap();
                let mut bytes = Vec::new();
                let mut byte = [0];
                while !bytes.ends_with(b"\r\n\r\n") {
                    stream.read_exact(&mut byte).unwrap();
                    bytes.push(byte[0]);
                    assert!(bytes.len() < 16384)
                }
                let headers = String::from_utf8(bytes).unwrap();
                assert!(headers
                    .to_ascii_lowercase()
                    .contains("authorization: bearer published-defaults"));
                let (status, body, etag) = if seen < 3 {
                    let (path, body, etag) = &routes[seen];
                    assert!(headers.starts_with(&format!("GET {path} ")));
                    ("200 OK", body.clone(), etag.clone())
                } else {
                    assert!(headers.starts_with("POST /v1/runs "));
                    assert!(headers.to_ascii_lowercase().contains("idempotency-key:"));
                    let length: usize = headers
                        .lines()
                        .find_map(|line| {
                            line.to_ascii_lowercase()
                                .strip_prefix("content-length:")
                                .map(|n| n.trim().parse().unwrap())
                        })
                        .unwrap();
                    let mut body = vec![0; length];
                    stream.read_exact(&mut body).unwrap();
                    let request: crate::run::CreateRunRequestV1 =
                        serde_json::from_slice(&body).unwrap();
                    assert_eq!(
                        request.expected_agent_deployment,
                        Some(expected_exact.clone())
                    );
                    (
                        "409 Conflict",
                        serde_json::to_vec(&ApiProblem {
                            type_uri: "https://insight.platform/problems/invalid_state_transition"
                                .into(),
                            title: "Active deployment changed".into(),
                            status: 409,
                            code: ApiProblemCode::InvalidStateTransition,
                            detail: Some("Active deployment changed; refresh explicitly.".into()),
                            request_id: id(ResourceKind::ServerRequest),
                            trace_id: "0123456789abcdef0123456789abcdef".parse().unwrap(),
                            retryable: false,
                            retry_after_ms: None,
                            field_errors: vec![],
                        })
                        .unwrap(),
                        "\"conflict\"".into(),
                    )
                };
                write!(stream,"HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncache-control: no-store, private, max-age=0\r\ntrace-id: 0123456789abcdef0123456789abcdef\r\netag: {etag}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",body.len()).unwrap();
                stream.write_all(&body).unwrap();
                seen += 1;
            }
            assert_eq!(seen, 4);
        });
        let client = PublicHttpClient::new(
            format!("http://{address}"),
            "published-defaults".into(),
            Duration::from_secs(5),
        )
        .unwrap();
        let (actual, spec) = crate::agent::published_run_defaults(&client, &agent).unwrap();
        assert_eq!(actual, exact);
        assert_eq!(spec.default_deadline_seconds, expected_deadline);
        assert_eq!(spec.input_schema.canonical_digest, expected_schema);
        let request = crate::run::CreateRunRequestV1 {
            agent_id: agent,
            expected_agent_deployment: Some(actual),
            input: crate::run::CreateRunInputV1 {
                classification: spec.input_classification,
                schema_digest: spec.input_schema.canonical_digest,
                value: insight_platform_contracts::ValueRef::Inline {
                    value: serde_json::json!({"message":"hello"}),
                },
            },
            deadline: UtcTimestamp::from_datetime(
                chrono::Utc::now() + chrono::Duration::minutes(1),
            ),
        };
        let error =
            crate::run::create_run(&client, &serde_json::to_vec(&request).unwrap()).unwrap_err();
        assert!(
            matches!(error, crate::run::RunClientError::Public(crate::public_client::PublicClientError::Problem(ref problem)) if problem.status == 409 && problem.code == ApiProblemCode::InvalidStateTransition && !problem.retryable),
            "unexpected error: {error:?}"
        );
        server.join().unwrap();
    }
    #[test]
    fn invalid_local_sources_reject_before_any_dependency_http_or_publication_journal() {
        use insight_platform_registry::authoring::{
            AuthoringDeploymentSelectorV1, AuthoringSlotSelectionV1, AuthoringSlotTargetV1,
            ResolveAgentBindingsRequestV1,
        };
        let fixture = Fixture::new(false);
        let request = ResolveAgentBindingsRequestV1 {
            schema_version: 1,
            slots: vec![AuthoringSlotSelectionV1 {
                slot_id: "subject".into(),
                requirement_digest: digest(b"fixture requirement"),
                interface_contract_digest: None,
                target: AuthoringSlotTargetV1::ChildAgent {
                    candidates: vec![AuthoringDeploymentSelectorV1::Exact {
                        deployment: insight_platform_contracts::ExactDeploymentRef::new(
                            id(ResourceKind::AgentDeployment),
                            digest(b"exact immutable fixture deployment"),
                        )
                        .unwrap(),
                    }],
                    selection_policy: fixture.bundle.profile.deployment_policies[0].clone(),
                },
            }],
        };
        request.validate().unwrap();
        for corrupt_path in ["schema-message.json", "team/plan.json", "team/agent.yaml"] {
            let directory = TempDir::new().unwrap();
            for (path, text) in &fixture.bundle.sources.files {
                let path = directory.path().join(path);
                fs::create_dir_all(path.parent().unwrap()).unwrap();
                fs::write(path, text).unwrap();
            }
            fs::write(
                directory.path().join(corrupt_path),
                b"{ invalid local source",
            )
            .unwrap();
            let private = directory.path().join("team/.insight");
            fs::create_dir(&private).unwrap();
            let selections = serde_json::to_vec(&request).unwrap();
            fs::write(private.join("agent-binding-selections.json"), &selections).unwrap();
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            listener.set_nonblocking(true).unwrap();
            let client = PublicHttpClient::new(
                format!("http://{}", listener.local_addr().unwrap()),
                "inspection-fixture".into(),
                Duration::from_millis(200),
            )
            .unwrap();
            let error = crate::agent::capture_project_sources(
                directory.path(),
                Path::new("team/agent.yaml"),
            )
            .and_then(|captured| {
                crate::agent::compile_project_online(
                    directory.path(),
                    captured,
                    fixture.bundle.profile.clone(),
                    &client,
                )
            })
            .unwrap_err();
            assert!(
                !matches!(error, crate::agent::AgentCommandError::Public(_)),
                "local failure must precede network: {error}"
            );
            assert_eq!(
                listener.accept().unwrap_err().kind(),
                std::io::ErrorKind::WouldBlock,
                "no dependency query may begin for {corrupt_path}"
            );
            assert_eq!(
                fs::read_dir(&private).unwrap().count(),
                1,
                "inspection must not create a journal"
            );
            assert_eq!(
                fs::read(private.join("agent-binding-selections.json")).unwrap(),
                selections
            );
        }
    }
}
