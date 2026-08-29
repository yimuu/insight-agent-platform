#[cfg(test)]
mod tests {
    use super::*;
    use insight_platform_api::resource::{deployment_etag, resource_etag, resource_version_etag};
    use insight_platform_contracts::{
        operation_etag, ApiProblem, ApiProblemCode, AuthoringPackage,
        CapabilityEndpointScheme, CanonicalHttpEndpoint, ClosedJsonValue, DataClassification,
        ExactDeploymentRef, ExactVersionRef, PolicyKind, PolicyResourceSpec, SafeJobFailure,
        SafeJobResult, TraceId, ValidationSummary,
    };
    use std::{
        io::{ErrorKind, Read as _, Write as _},
        net::{TcpListener, TcpStream},
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc,
        },
        thread,
        time::Instant,
    };
    use tempfile::TempDir;
    use uuid::Uuid;

    fn id(kind: ResourceKind) -> ResourceId {
        ResourceId::from_uuid_v7(kind, Uuid::now_v7()).unwrap()
    }

    fn digest(character: char) -> Sha256Digest {
        format!("sha256:{}", character.to_string().repeat(64))
            .parse()
            .unwrap()
    }

    fn exact_version(kind: ResourceKind, marker: char) -> ExactVersionRef {
        ExactVersionRef::new(id(kind), digest(marker)).unwrap()
    }

    fn exact_deployment(kind: ResourceKind, marker: char) -> ExactDeploymentRef {
        ExactDeploymentRef::new(id(kind), digest(marker)).unwrap()
    }

    fn evidence(marker: char) -> ArtifactRef {
        ArtifactRef::new(
            id(ResourceKind::Artifact),
            digest(marker),
            128,
            "application/json",
            DataClassification::Internal,
            Some("qualification.json".to_owned()),
        )
        .unwrap()
    }

    fn policy_binding(marker: char) -> ExactPolicyBinding {
        ExactPolicyBinding {
            deployment: exact_deployment(ResourceKind::PolicyDeployment, marker),
            revision: exact_version(ResourceKind::PolicyRevision, marker),
        }
    }

    fn published(kind: ResourceKind, marker: char) -> PublishedResourceVersionSummaryV1 {
        let resource_version_id = id(kind);
        let content_digest = digest(marker);
        PublishedResourceVersionSummaryV1 {
            etag: resource_version_etag(&resource_version_id, &content_digest),
            resource_version_id,
            revision_no: 1,
            content_digest,
            artifact_id: None,
        }
    }

    fn policy_manifest() -> serde_json::Value {
        let authoring_artifact = ArtifactRef::new(
            id(ResourceKind::Artifact),
            digest('1'),
            128,
            "application/json",
            DataClassification::Internal,
            Some("policy authoring package".to_owned()),
        )
        .unwrap();
        let document = ResourceDocument::Policy(Box::new(PolicyResourceSpec {
            authoring_package: AuthoringPackage {
                artifact: authoring_artifact,
                manifest_digest: digest('2'),
            },
            contract_digest: digest('3'),
            dependency_versions: Vec::new(),
            policy_versions: Vec::new(),
            policy_kind: PolicyKind::Authorization,
            rules_digest: digest('4'),
            selection: None,
            scheduling: None,
            retention: None,
            model_safety: None,
            model_budget: None,
            model_public_projection: None,
            mcp_protocol: None,
            mcp_auth: None,
            sandbox_isolation: None,
            sandbox_resource: None,
            sandbox_network: None,
            sandbox_artifact_io: None,
            sandbox_secret_resolution: None,
        }));
        let qualification_evidence = ArtifactRef::new(
            id(ResourceKind::Artifact),
            digest('5'),
            128,
            "application/json",
            DataClassification::Internal,
            Some("policy qualification".to_owned()),
        )
        .unwrap();
        serde_json::json!({
            "schema_version": 1,
            "kind": APPLY_MANIFEST_KIND,
            "resource_noun": "policies",
            "create": {
                "display_name": "local execution policy",
                "document": document
            },
            "publish": {
                "kind": "single",
                "revision_no": 1,
                "content_digest": digest('a'),
                "artifact_id": null
            },
            "deployment": {
                "environment": "local",
                "closure": {
                    "resource_kind": "policy",
                    "bindings": {
                        "applicability_digest": digest('b'),
                        "qualification_evidence": qualification_evidence
                    }
                }
            }
        })
    }

    struct ScriptedResponse {
        method: &'static str,
        path: String,
        expected_if_match: Option<String>,
        status: &'static str,
        etag: String,
        location: Option<String>,
        body: Vec<u8>,
        assert_deployment_version: Option<ResourceId>,
    }

    fn read_request(stream: &mut TcpStream) -> (String, Vec<u8>) {
        let mut bytes = Vec::new();
        let mut buffer = [0_u8; 4096];
        let header_end = loop {
            let read = stream.read(&mut buffer).unwrap();
            assert!(read > 0, "connection closed before request headers");
            bytes.extend_from_slice(&buffer[..read]);
            if let Some(index) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
                break index + 4;
            }
            assert!(bytes.len() <= MAX_APPLY_MANIFEST_BYTES);
        };
        let head = String::from_utf8(bytes[..header_end].to_vec()).unwrap();
        let content_length = header_value(&head, "content-length")
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(0);
        while bytes.len() < header_end + content_length {
            let read = stream.read(&mut buffer).unwrap();
            assert!(read > 0, "connection closed before request body");
            bytes.extend_from_slice(&buffer[..read]);
        }
        (
            head,
            bytes[header_end..header_end + content_length].to_vec(),
        )
    }

    fn header_value<'a>(head: &'a str, name: &str) -> Option<&'a str> {
        head.lines().find_map(|line| {
            let (candidate, value) = line.split_once(':')?;
            candidate.eq_ignore_ascii_case(name).then_some(value.trim())
        })
    }

    fn request_trace_id(head: &str) -> String {
        let traceparent = header_value(head, "traceparent").expect("mutation traceparent");
        let mut fields = traceparent.split('-');
        assert_eq!(fields.next(), Some("00"));
        let trace_id = fields.next().unwrap();
        assert_eq!(trace_id.len(), 32);
        trace_id.to_owned()
    }

    fn write_response(stream: &mut TcpStream, response: &ScriptedResponse, trace_id: &str) {
        write!(
            stream,
            "HTTP/1.1 {}\r\ncontent-type: application/json\r\ncache-control: no-store, private, max-age=0\r\ntrace-id: {trace_id}\r\netag: {}\r\n",
            response.status, response.etag
        )
        .unwrap();
        if let Some(location) = &response.location {
            write!(stream, "location: {location}\r\n").unwrap();
        }
        write!(
            stream,
            "content-length: {}\r\nconnection: close\r\n\r\n",
            response.body.len()
        )
        .unwrap();
        stream.write_all(&response.body).unwrap();
    }

    fn problem(status: u16, code: ApiProblemCode, trace_id: &str) -> ApiProblem {
        ApiProblem {
            type_uri: format!("https://insight.platform/problems/{}", code.as_str()),
            title: code.as_str().replace('_', " "),
            status,
            code,
            detail: Some("bounded public failure detail".to_owned()),
            request_id: id(ResourceKind::ServerRequest),
            trace_id: trace_id.parse::<TraceId>().unwrap(),
            retryable: matches!(status, 429 | 503),
            retry_after_ms: matches!(status, 429 | 503).then_some(250),
            field_errors: Vec::new(),
        }
    }

    fn write_problem(stream: &mut TcpStream, status: &str, problem: &ApiProblem) {
        let body = serde_json::to_vec(problem).unwrap();
        write!(
            stream,
            "HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncache-control: no-store, private, max-age=0\r\ntrace-id: {}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
            problem.trace_id,
            body.len()
        )
        .unwrap();
        stream.write_all(&body).unwrap();
    }

    fn created_policy(
        manifest: &ApplyManifestV1,
        resource_id: &ResourceId,
        version: u64,
    ) -> ResourceViewV1 {
        ResourceViewV1 {
            schema_version: 1,
            resource_id: resource_id.clone(),
            resource_kind: RegistryResourceKind::Policy,
            lifecycle_state: EntityLifecycle::Active,
            gate_state: AdministrativeGate::Enabled,
            draft_generation: 1,
            version,
            draft: ResourceDraftPayload {
                display_name: manifest.create.display_name.clone(),
                document: manifest.create.document.clone(),
                validation: None,
            },
            etag: resource_etag(resource_id, version),
        }
    }

    fn validation_operation(
        tenant_id: &ResourceId,
        resource_id: &ResourceId,
        operation_id: &ResourceId,
        state: PublicJobState,
    ) -> OperationViewV1 {
        OperationViewV1 {
            operation_id: operation_id.clone(),
            tenant_id: tenant_id.clone(),
            kind: PublicJobKind::ResourceValidation,
            target: PublicJobTarget::ResourceVersion {
                resource_id: resource_id.clone(),
                resource_version: 1,
            },
            state,
            progress: None,
            result: None,
            error: (state == PublicJobState::Failed).then(|| SafeJobFailure {
                code: "validation_failed".to_owned(),
                message: "draft failed policy validation".to_owned(),
            }),
            created_at: "2026-08-29T00:00:00.000000Z".parse().unwrap(),
            updated_at: "2026-08-29T00:00:01.000000Z".parse().unwrap(),
            etag: operation_etag(&operation_id.to_string(), 2),
        }
    }

    fn scripted_create(
        manifest: &ApplyManifestV1,
        resource_id: &ResourceId,
    ) -> ScriptedResponse {
        let body = created_policy(manifest, resource_id, 1);
        ScriptedResponse {
            method: "POST",
            path: "/v1/policies".to_owned(),
            expected_if_match: None,
            status: "201 Created",
            etag: body.etag.clone(),
            location: Some(format!("/v1/policies/{resource_id}")),
            body: serde_json::to_vec(&body).unwrap(),
            assert_deployment_version: None,
        }
    }

    fn scripted_validation(
        tenant_id: &ResourceId,
        resource_id: &ResourceId,
        operation_id: &ResourceId,
    ) -> ScriptedResponse {
        let body = validation_operation(
            tenant_id,
            resource_id,
            operation_id,
            PublicJobState::Queued,
        );
        ScriptedResponse {
            method: "POST",
            path: format!("/v1/policies/{resource_id}/draft:validate"),
            expected_if_match: Some(resource_etag(resource_id, 1)),
            status: "202 Accepted",
            etag: body.etag.clone(),
            location: Some(format!("/v1/operations/{operation_id}")),
            body: serde_json::to_vec(&body).unwrap(),
            assert_deployment_version: None,
        }
    }

    #[test]
    fn manifest_wrapper_is_closed_and_kind_bound() {
        let manifest = policy_manifest();
        let bytes = serde_json::to_vec(&manifest).unwrap();
        let parsed = parse_manifest(&bytes);
        assert!(parsed.is_ok(), "{parsed:?}");

        let mut open = manifest;
        open.as_object_mut()
            .unwrap()
            .insert("unknown".to_owned(), serde_json::Value::Bool(true));
        assert!(parse_manifest(&serde_json::to_vec(&open).unwrap()).is_err());
    }

    #[test]
    fn apply_executes_the_public_policy_lifecycle_and_resolves_self_version() {
        let manifest_value = policy_manifest();
        let manifest_bytes = serde_json::to_vec(&manifest_value).unwrap();
        let (manifest, _) = parse_manifest(&manifest_bytes).unwrap();
        let tenant_id = id(ResourceKind::Tenant);
        let resource_id = id(ResourceKind::Policy);
        let operation_id = id(ResourceKind::Job);
        let version_id = id(ResourceKind::PolicyRevision);
        let deployment_id = id(ResourceKind::PolicyDeployment);
        let validation = ValidationSummary {
            validator_digest: digest('6'),
            validated_draft_digest: digest('7'),
            dependency_closure_digest: digest('8'),
            security_evidence_digest: digest('9'),
            warnings: Vec::new(),
        };
        let created_draft = ResourceDraftPayload {
            display_name: manifest.create.display_name.clone(),
            document: manifest.create.document.clone(),
            validation: None,
        };
        let validated_draft = ResourceDraftPayload {
            validation: Some(validation),
            ..created_draft.clone()
        };
        let create_etag = resource_etag(&resource_id, 1);
        let validated_etag = resource_etag(&resource_id, 2);
        let publish_etag = resource_etag(&resource_id, 3);
        let deployed_resource_etag = resource_etag(&resource_id, 4);
        let activated_etag = resource_etag(&resource_id, 5);
        let version_digest = digest('a');
        let version_etag = resource_version_etag(&version_id, &version_digest);
        let published = vec![PublishedResourceVersionSummaryV1 {
            resource_version_id: version_id.clone(),
            revision_no: 1,
            content_digest: version_digest,
            artifact_id: None,
            etag: version_etag,
        }];
        let closure = manifest
            .deployment
            .closure
            .clone()
            .resolve(&published)
            .unwrap();
        let closure_digest = deployment_closure_digest_v1(&closure).unwrap();
        let deployment_etag = deployment_etag(&deployment_id, &closure_digest);
        let operation_etag = operation_etag(&operation_id.to_string(), 2);
        let responses = vec![
            ScriptedResponse {
                method: "POST",
                path: "/v1/policies".to_owned(),
                expected_if_match: None,
                status: "201 Created",
                etag: create_etag.clone(),
                location: Some(format!("/v1/policies/{resource_id}")),
                body: serde_json::to_vec(&ResourceViewV1 {
                    schema_version: 1,
                    resource_id: resource_id.clone(),
                    resource_kind: RegistryResourceKind::Policy,
                    lifecycle_state: EntityLifecycle::Active,
                    gate_state: AdministrativeGate::Enabled,
                    draft_generation: 1,
                    version: 1,
                    draft: created_draft,
                    etag: create_etag.clone(),
                })
                .unwrap(),
                assert_deployment_version: None,
            },
            ScriptedResponse {
                method: "POST",
                path: format!("/v1/policies/{resource_id}/draft:validate"),
                expected_if_match: Some(create_etag),
                status: "202 Accepted",
                etag: operation_etag.clone(),
                location: Some(format!("/v1/operations/{operation_id}")),
                body: serde_json::to_vec(&OperationViewV1 {
                    operation_id: operation_id.clone(),
                    tenant_id: tenant_id.clone(),
                    kind: PublicJobKind::ResourceValidation,
                    target: PublicJobTarget::ResourceVersion {
                        resource_id: resource_id.clone(),
                        resource_version: 1,
                    },
                    state: PublicJobState::Queued,
                    progress: None,
                    result: None,
                    error: None,
                    created_at: "2026-08-29T00:00:00.000000Z".parse().unwrap(),
                    updated_at: "2026-08-29T00:00:00.000000Z".parse().unwrap(),
                    etag: operation_etag.clone(),
                })
                .unwrap(),
                assert_deployment_version: None,
            },
            ScriptedResponse {
                method: "GET",
                path: format!("/v1/operations/{operation_id}"),
                expected_if_match: None,
                status: "200 OK",
                etag: operation_etag.clone(),
                location: None,
                body: serde_json::to_vec(&OperationViewV1 {
                    operation_id: operation_id.clone(),
                    tenant_id: tenant_id.clone(),
                    kind: PublicJobKind::ResourceValidation,
                    target: PublicJobTarget::ResourceVersion {
                        resource_id: resource_id.clone(),
                        resource_version: 1,
                    },
                    state: PublicJobState::Succeeded,
                    progress: None,
                    result: Some(SafeJobResult {
                        result_digest: digest('d'),
                    }),
                    error: None,
                    created_at: "2026-08-29T00:00:00.000000Z".parse().unwrap(),
                    updated_at: "2026-08-29T00:00:01.000000Z".parse().unwrap(),
                    etag: operation_etag.clone(),
                })
                .unwrap(),
                assert_deployment_version: None,
            },
            ScriptedResponse {
                method: "GET",
                path: format!("/v1/policies/{resource_id}"),
                expected_if_match: None,
                status: "200 OK",
                etag: validated_etag.clone(),
                location: None,
                body: serde_json::to_vec(&ResourceViewV1 {
                    schema_version: 1,
                    resource_id: resource_id.clone(),
                    resource_kind: RegistryResourceKind::Policy,
                    lifecycle_state: EntityLifecycle::Active,
                    gate_state: AdministrativeGate::Enabled,
                    draft_generation: 1,
                    version: 2,
                    draft: validated_draft.clone(),
                    etag: validated_etag.clone(),
                })
                .unwrap(),
                assert_deployment_version: None,
            },
            ScriptedResponse {
                method: "POST",
                path: format!("/v1/policies/{resource_id}/draft:publish"),
                expected_if_match: Some(validated_etag),
                status: "200 OK",
                etag: publish_etag.clone(),
                location: None,
                body: serde_json::to_vec(&PublishResourceDraftResponseV1 {
                    schema_version: 1,
                    resource_id: resource_id.clone(),
                    resource_kind: RegistryResourceKind::Policy,
                    draft_generation: 1,
                    version: 3,
                    published_versions: published.clone(),
                    etag: publish_etag.clone(),
                })
                .unwrap(),
                assert_deployment_version: None,
            },
            ScriptedResponse {
                method: "POST",
                path: format!("/v1/policies/{resource_id}/deployments"),
                expected_if_match: Some(publish_etag.clone()),
                status: "201 Created",
                etag: deployment_etag.clone(),
                location: Some(format!(
                    "/v1/policies/{resource_id}/deployments/{deployment_id}"
                )),
                body: serde_json::to_vec(&DeploymentViewV1 {
                    schema_version: 1,
                    deployment_id: deployment_id.clone(),
                    resource_id: resource_id.clone(),
                    resource_kind: RegistryResourceKind::Policy,
                    resource_version_id: version_id.clone(),
                    environment: "local".to_owned(),
                    closure: closure.clone(),
                    closure_digest,
                    created_at: "2026-08-29T00:00:02.000000Z".parse().unwrap(),
                    etag: deployment_etag,
                })
                .unwrap(),
                assert_deployment_version: Some(version_id.clone()),
            },
            ScriptedResponse {
                method: "GET",
                path: format!("/v1/policies/{resource_id}"),
                expected_if_match: None,
                status: "200 OK",
                etag: deployed_resource_etag.clone(),
                location: None,
                body: serde_json::to_vec(&ResourceViewV1 {
                    schema_version: 1,
                    resource_id: resource_id.clone(),
                    resource_kind: RegistryResourceKind::Policy,
                    lifecycle_state: EntityLifecycle::Active,
                    gate_state: AdministrativeGate::Enabled,
                    draft_generation: 1,
                    version: 4,
                    draft: validated_draft.clone(),
                    etag: deployed_resource_etag.clone(),
                })
                .unwrap(),
                assert_deployment_version: None,
            },
            ScriptedResponse {
                method: "POST",
                path: format!("/v1/policies/{resource_id}/deployments/{deployment_id}:activate"),
                expected_if_match: Some(deployed_resource_etag),
                status: "200 OK",
                etag: activated_etag.clone(),
                location: None,
                body: serde_json::to_vec(&ResourceViewV1 {
                    schema_version: 1,
                    resource_id: resource_id.clone(),
                    resource_kind: RegistryResourceKind::Policy,
                    lifecycle_state: EntityLifecycle::Active,
                    gate_state: AdministrativeGate::Enabled,
                    draft_generation: 1,
                    version: 5,
                    draft: validated_draft,
                    etag: activated_etag.clone(),
                })
                .unwrap(),
                assert_deployment_version: None,
            },
        ];

        let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = thread::spawn(move || {
            let (mut dropped, _) = listener.accept().unwrap();
            let (dropped_head, _) = read_request(&mut dropped);
            assert!(dropped_head.starts_with("POST /v1/policies HTTP/1.1"));
            let dropped_receipt = header_value(&dropped_head, "idempotency-key")
                .unwrap()
                .to_owned();
            drop(dropped);

            for (index, response) in responses.into_iter().enumerate() {
                let (mut stream, _) = listener.accept().unwrap();
                let (head, body) = read_request(&mut stream);
                assert!(
                    head.starts_with(&format!("{} {} HTTP/1.1", response.method, response.path))
                );
                assert_eq!(
                    header_value(&head, "authorization"),
                    Some("Bearer test-token")
                );
                assert_eq!(
                    header_value(&head, "if-match"),
                    response.expected_if_match.as_deref()
                );
                let trace_id = if response.method == "POST" {
                    let receipt = header_value(&head, "idempotency-key")
                        .filter(|value| value.starts_with("insight-apply-v1-"))
                        .expect("bounded deterministic Receipt");
                    if index == 0 {
                        assert_eq!(receipt, dropped_receipt);
                    }
                    request_trace_id(&head)
                } else {
                    "33333333333333333333333333333333".to_owned()
                };
                if let Some(expected_version) = &response.assert_deployment_version {
                    let request: serde_json::Value = serde_json::from_slice(&body).unwrap();
                    assert_eq!(request["resource_version_id"], expected_version.to_string());
                    assert_eq!(
                        request["closure"]["bindings"]["policy_revision"]["revision_id"],
                        expected_version.to_string()
                    );
                }
                write_response(&mut stream, &response, &trace_id);
            }
        });
        let client = PublicHttpClient::new(
            format!("http://127.0.0.1:{port}"),
            "test-token".to_owned(),
            Duration::from_secs(2),
        )
        .unwrap();
        let journals = TempDir::new().unwrap();
        let interrupted = apply_manifest(
            &client,
            &tenant_id,
            &manifest_bytes,
            Duration::from_secs(2),
            journals.path(),
        );
        assert!(matches!(
            interrupted,
            Err(ApplyError::Public(PublicClientError::Transport(_)))
        ));
        let report = apply_manifest(
            &client,
            &tenant_id,
            &manifest_bytes,
            Duration::from_secs(2),
            journals.path(),
        )
        .unwrap();
        assert_eq!(report.resource_id, resource_id.to_string());
        assert_eq!(report.validation_operation_id, operation_id.to_string());
        assert_eq!(report.deployment_id, deployment_id.to_string());
        assert_eq!(report.active_deployment_id, deployment_id.to_string());
        assert_eq!(report.final_resource_etag, activated_etag);
        assert_eq!(report.published_versions.len(), 1);
        assert_eq!(
            report.published_versions[0].resource_version_id,
            version_id.to_string()
        );
        server.join().unwrap();

        let resumed = apply_manifest(
            &client,
            &tenant_id,
            &manifest_bytes,
            Duration::from_secs(2),
            journals.path(),
        )
        .unwrap();
        assert_eq!(resumed, report);
    }

    #[test]
    fn apply_preserves_create_conflict_and_rate_limit_problems() {
        for (status, status_line, code) in [
            (409, "409 Conflict", ApiProblemCode::IdempotencyConflict),
            (429, "429 Too Many Requests", ApiProblemCode::RateLimited),
        ] {
            let tenant_id = id(ResourceKind::Tenant);
            let manifest_bytes = serde_json::to_vec(&policy_manifest()).unwrap();
            let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
            let port = listener.local_addr().unwrap().port();
            let server = thread::spawn(move || {
                let (mut stream, _) = listener.accept().unwrap();
                let (head, _) = read_request(&mut stream);
                assert!(head.starts_with("POST /v1/policies HTTP/1.1"));
                assert_eq!(header_value(&head, "authorization"), Some("Bearer test-token"));
                assert!(header_value(&head, "if-match").is_none());
                let trace_id = request_trace_id(&head);
                let response = problem(status, code, &trace_id);
                write_problem(&mut stream, status_line, &response);
            });
            let client = PublicHttpClient::new(
                format!("http://127.0.0.1:{port}"),
                "test-token".to_owned(),
                Duration::from_secs(2),
            )
            .unwrap();
            let journals = TempDir::new().unwrap();
            let error = apply_manifest(
                &client,
                &tenant_id,
                &manifest_bytes,
                Duration::from_secs(2),
                journals.path(),
            )
            .unwrap_err();
            let ApplyError::Public(PublicClientError::Problem(actual)) = error else {
                panic!("unexpected apply error: {error}");
            };
            assert_eq!(actual.status, status);
            assert_eq!(actual.code, code);
            assert_eq!(actual.retryable, status == 429);
            assert_eq!(actual.retry_after_ms, (status == 429).then_some(250));
            server.join().unwrap();
        }
    }

    #[test]
    fn apply_preserves_validation_precondition_failure_after_exact_create() {
        let manifest_bytes = serde_json::to_vec(&policy_manifest()).unwrap();
        let (manifest, _) = parse_manifest(&manifest_bytes).unwrap();
        let tenant_id = id(ResourceKind::Tenant);
        let resource_id = id(ResourceKind::Policy);
        let create = scripted_create(&manifest, &resource_id);
        let expected_etag = create.etag.clone();
        let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let (head, _) = read_request(&mut stream);
            assert!(head.starts_with("POST /v1/policies HTTP/1.1"));
            write_response(&mut stream, &create, &request_trace_id(&head));

            let (mut stream, _) = listener.accept().unwrap();
            let (head, body) = read_request(&mut stream);
            assert!(head.starts_with(&format!(
                "POST /v1/policies/{resource_id}/draft:validate HTTP/1.1"
            )));
            assert!(body.is_empty());
            assert_eq!(header_value(&head, "if-match"), Some(expected_etag.as_str()));
            assert!(header_value(&head, "idempotency-key")
                .is_some_and(|value| value.ends_with("-validate")));
            let trace_id = request_trace_id(&head);
            write_problem(
                &mut stream,
                "412 Precondition Failed",
                &problem(412, ApiProblemCode::PreconditionFailed, &trace_id),
            );
        });
        let client = PublicHttpClient::new(
            format!("http://127.0.0.1:{port}"),
            "test-token".to_owned(),
            Duration::from_secs(2),
        )
        .unwrap();
        let journals = TempDir::new().unwrap();
        let error = apply_manifest(
            &client,
            &tenant_id,
            &manifest_bytes,
            Duration::from_secs(2),
            journals.path(),
        )
        .unwrap_err();
        let ApplyError::Public(PublicClientError::Problem(actual)) = error else {
            panic!("unexpected apply error: {error}");
        };
        assert_eq!(actual.status, 412);
        assert_eq!(actual.code, ApiProblemCode::PreconditionFailed);
        server.join().unwrap();
    }

    #[test]
    fn apply_reports_failed_validation_operation_with_safe_detail() {
        let manifest_bytes = serde_json::to_vec(&policy_manifest()).unwrap();
        let (manifest, _) = parse_manifest(&manifest_bytes).unwrap();
        let tenant_id = id(ResourceKind::Tenant);
        let resource_id = id(ResourceKind::Policy);
        let operation_id = id(ResourceKind::Job);
        let create = scripted_create(&manifest, &resource_id);
        let validation = scripted_validation(&tenant_id, &resource_id, &operation_id);
        let failed = validation_operation(
            &tenant_id,
            &resource_id,
            &operation_id,
            PublicJobState::Failed,
        );
        let failed_etag = failed.etag.clone();
        let server_operation_id = operation_id.clone();
        let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = thread::spawn(move || {
            for response in [create, validation] {
                let (mut stream, _) = listener.accept().unwrap();
                let (head, _) = read_request(&mut stream);
                write_response(&mut stream, &response, &request_trace_id(&head));
            }
            let (mut stream, _) = listener.accept().unwrap();
            let (head, _) = read_request(&mut stream);
            assert!(head.starts_with(&format!(
                "GET /v1/operations/{server_operation_id} HTTP/1.1"
            )));
            write_response(
                &mut stream,
                &ScriptedResponse {
                    method: "GET",
                    path: format!("/v1/operations/{server_operation_id}"),
                    expected_if_match: None,
                    status: "200 OK",
                    etag: failed_etag,
                    location: None,
                    body: serde_json::to_vec(&failed).unwrap(),
                    assert_deployment_version: None,
                },
                "33333333333333333333333333333333",
            );
        });
        let client = PublicHttpClient::new(
            format!("http://127.0.0.1:{port}"),
            "test-token".to_owned(),
            Duration::from_secs(2),
        )
        .unwrap();
        let journals = TempDir::new().unwrap();
        let error = apply_manifest(
            &client,
            &tenant_id,
            &manifest_bytes,
            Duration::from_secs(2),
            journals.path(),
        )
        .unwrap_err();
        match error {
            ApplyError::OperationTerminal {
                operation_id: actual_id,
                state,
                detail,
            } => {
                assert_eq!(actual_id, operation_id.to_string());
                assert_eq!(state, "failed");
                assert_eq!(
                    detail,
                    "code=validation_failed message=draft failed policy validation"
                );
            }
            other => panic!("unexpected apply error: {other}"),
        }
        server.join().unwrap();
    }

    #[test]
    fn apply_times_out_without_mutating_after_a_queued_validation() {
        let manifest_bytes = serde_json::to_vec(&policy_manifest()).unwrap();
        let (manifest, _) = parse_manifest(&manifest_bytes).unwrap();
        let tenant_id = id(ResourceKind::Tenant);
        let resource_id = id(ResourceKind::Policy);
        let operation_id = id(ResourceKind::Job);
        let create = scripted_create(&manifest, &resource_id);
        let validation = scripted_validation(&tenant_id, &resource_id, &operation_id);
        let queued = validation_operation(
            &tenant_id,
            &resource_id,
            &operation_id,
            PublicJobState::Queued,
        );
        let queued_etag = queued.etag.clone();
        let server_operation_id = operation_id.clone();
        let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let port = listener.local_addr().unwrap().port();
        let done = Arc::new(AtomicBool::new(false));
        let server_done = Arc::clone(&done);
        let server = thread::spawn(move || {
            for response in [create, validation] {
                let (mut stream, _) = listener.accept().unwrap();
                let (head, _) = read_request(&mut stream);
                write_response(&mut stream, &response, &request_trace_id(&head));
            }
            listener.set_nonblocking(true).unwrap();
            let started = Instant::now();
            while !server_done.load(Ordering::Relaxed) && started.elapsed() < Duration::from_secs(3)
            {
                match listener.accept() {
                    Ok((mut stream, _)) => {
                        stream.set_nonblocking(false).unwrap();
                        let (head, _) = read_request(&mut stream);
                        assert!(head.starts_with(&format!(
                            "GET /v1/operations/{server_operation_id} HTTP/1.1"
                        )));
                        write_response(
                            &mut stream,
                            &ScriptedResponse {
                                method: "GET",
                                path: format!("/v1/operations/{server_operation_id}"),
                                expected_if_match: None,
                                status: "200 OK",
                                etag: queued_etag.clone(),
                                location: None,
                                body: serde_json::to_vec(&queued).unwrap(),
                                assert_deployment_version: None,
                            },
                            "33333333333333333333333333333333",
                        );
                    }
                    Err(error) if error.kind() == ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(5));
                    }
                    Err(error) => panic!("accept failed: {error}"),
                }
            }
        });
        let client = PublicHttpClient::new(
            format!("http://127.0.0.1:{port}"),
            "test-token".to_owned(),
            Duration::from_secs(2),
        )
        .unwrap();
        let journals = TempDir::new().unwrap();
        let error = apply_manifest(
            &client,
            &tenant_id,
            &manifest_bytes,
            Duration::from_secs(1),
            journals.path(),
        )
        .unwrap_err();
        done.store(true, Ordering::Relaxed);
        assert!(matches!(
            error,
            ApplyError::Public(PublicClientError::OperationTimeout {
                operation_id: actual_id,
                timeout_seconds: 1,
            }) if actual_id == operation_id.to_string()
        ));
        server.join().unwrap();
    }

    #[test]
    fn all_unsuccessful_terminal_operation_states_fail_closed() {
        let tenant_id = id(ResourceKind::Tenant);
        let resource_id = id(ResourceKind::Policy);
        let operation_id = id(ResourceKind::Job);
        for (state, expected_name) in [
            (PublicJobState::Failed, "failed"),
            (PublicJobState::Cancelled, "cancelled"),
            (PublicJobState::TimedOut, "timed_out"),
            (
                PublicJobState::ReconciliationRequired,
                "reconciliation_required",
            ),
        ] {
            let operation = validation_operation(&tenant_id, &resource_id, &operation_id, state);
            let error = require_succeeded_operation(&operation).unwrap_err();
            assert!(matches!(
                error,
                ApplyError::OperationTerminal { state, .. } if state == expected_name
            ));
        }
    }

    #[test]
    fn every_apply_resource_closure_resolves_only_its_published_self_versions() {
        let agent_interface = published(ResourceKind::AgentInterfaceRevision, '1');
        let agent_plan = published(ResourceKind::AgentPlanRevision, '2');
        let resolved = ApplyDeploymentClosure::Agent(ApplyAgentDeploymentBindings {
            entry_node_id: "start".to_owned(),
            entry_node_kind: PlanNodeKind::Start,
            slots: Vec::new(),
            policies: vec![policy_binding('3')],
            execution_profile: policy_binding('4'),
        })
        .resolve(&[agent_interface.clone(), agent_plan.clone()])
        .unwrap();
        let DeploymentClosure::Agent(closure) = resolved else {
            panic!("Agent closure resolved to another Resource kind");
        };
        assert_eq!(closure.interface.revision_id, agent_interface.resource_version_id);
        assert_eq!(closure.interface.semantic_digest, agent_interface.content_digest);
        assert_eq!(closure.plan.revision_id, agent_plan.resource_version_id);
        assert_eq!(closure.plan.semantic_digest, agent_plan.content_digest);

        let skill_revision = published(ResourceKind::SkillRevision, '5');
        let resolved = ApplyDeploymentClosure::Skill(ApplySkillDeploymentBindings {
            requirements: Vec::new(),
            selection_policy: policy_binding('6'),
            qualification_evidence: evidence('7'),
        })
        .resolve(std::slice::from_ref(&skill_revision))
        .unwrap();
        let DeploymentClosure::Skill(closure) = resolved else {
            panic!("Skill closure resolved to another Resource kind");
        };
        assert_eq!(closure.skill_revision.revision_id, skill_revision.resource_version_id);
        assert_eq!(closure.skill_revision.semantic_digest, skill_revision.content_digest);

        let capability_revision =
            published(ResourceKind::CapabilityInterfaceRevision, '8');
        let resolved = ApplyDeploymentClosure::CapabilityInterface(
            ApplyCapabilityDeploymentBindings {
                implementation: exact_version(
                    ResourceKind::CapabilityImplementationRevision,
                    '9',
                ),
                backend: CapabilityBackendBinding::Native {
                    worker_manifest_digest: digest('a'),
                    adapter_module_digest: digest('b'),
                },
                secret_bindings: Vec::new(),
                policies: Vec::new(),
                conformance_evidence: evidence('c'),
            },
        )
        .resolve(std::slice::from_ref(&capability_revision))
        .unwrap();
        let DeploymentClosure::CapabilityInterface(closure) = resolved else {
            panic!("Capability closure resolved to another Resource kind");
        };
        assert_eq!(closure.interface.revision_id, capability_revision.resource_version_id);
        assert_eq!(closure.interface.semantic_digest, capability_revision.content_digest);

        let context_revision =
            published(ResourceKind::ContextSourceInterfaceRevision, 'd');
        let resolved = ApplyDeploymentClosure::ContextSourceInterface(
            ApplyContextDeploymentBindings {
                implementation: exact_version(
                    ResourceKind::ContextSourceImplementationRevision,
                    'e',
                ),
                required_worker_manifest_digest: digest('f'),
                backend: ContextBackendBinding::NativeCatalog {
                    installed_adapter_digest: digest('1'),
                },
                secret_bindings: Vec::new(),
                network_policy: None,
                tls_policy: None,
                trust_policy: None,
                parser_policy: exact_version(ResourceKind::PolicyRevision, '2'),
                chunker_policy: exact_version(ResourceKind::PolicyRevision, '3'),
                embedding_model_deployment: None,
                ranking_policy: exact_version(ResourceKind::PolicyRevision, '4'),
                data_policy: exact_version(ResourceKind::PolicyRevision, '5'),
                conformance_evidence: evidence('6'),
            },
        )
        .resolve(std::slice::from_ref(&context_revision))
        .unwrap();
        let DeploymentClosure::ContextSourceInterface(closure) = resolved else {
            panic!("Context closure resolved to another Resource kind");
        };
        assert_eq!(closure.interface.revision_id, context_revision.resource_version_id);
        assert_eq!(closure.interface.semantic_digest, context_revision.content_digest);

        let endpoint = CanonicalHttpEndpoint {
            scheme: CapabilityEndpointScheme::Https,
            host: "mcp.example.test".to_owned(),
            port: 443,
            base_path: "/mcp".to_owned(),
        };
        let mcp_revision = published(ResourceKind::McpServerRevision, '7');
        let resolved = ApplyDeploymentClosure::McpServer(ApplyMcpDeploymentBindings {
            server_identity_digest: digest('8'),
            transport: McpTransportBinding::StreamableHttp {
                endpoint_identity_digest: endpoint.canonical_digest().unwrap(),
                endpoint,
                network_policy: exact_version(ResourceKind::PolicyRevision, '9'),
                tls_policy: exact_version(ResourceKind::PolicyRevision, 'a'),
            },
            protocol_policy: exact_version(ResourceKind::PolicyRevision, 'b'),
            trust_policy: exact_version(ResourceKind::PolicyRevision, 'c'),
            auth_policy: None,
            secret_bindings: Vec::new(),
            conformance_evidence: evidence('d'),
        })
        .resolve(std::slice::from_ref(&mcp_revision))
        .unwrap();
        let DeploymentClosure::McpServer(closure) = resolved else {
            panic!("MCP closure resolved to another Resource kind");
        };
        assert_eq!(closure.server_revision.revision_id, mcp_revision.resource_version_id);
        assert_eq!(closure.server_revision.semantic_digest, mcp_revision.content_digest);

        let model_revision = published(ResourceKind::ModelProfileRevision, 'e');
        let resolved = ApplyDeploymentClosure::ModelProfile(ApplyModelDeploymentBindings {
            provider_deployment: exact_deployment(
                ResourceKind::ModelProviderDeployment,
                'f',
            ),
            data_policy: exact_version(ResourceKind::PolicyRevision, '1'),
            safety_policy: exact_version(ResourceKind::PolicyRevision, '2'),
            budget_policy: exact_version(ResourceKind::PolicyRevision, '3'),
            public_projection_policy: exact_version(ResourceKind::PolicyRevision, '4'),
            generation_defaults: ClosedJsonValue::build(
                digest('5'),
                serde_json::json!({"temperature_millis": 0}),
            )
            .unwrap(),
        })
        .resolve(std::slice::from_ref(&model_revision))
        .unwrap();
        let DeploymentClosure::ModelProfile(closure) = resolved else {
            panic!("Model closure resolved to another Resource kind");
        };
        assert_eq!(closure.profile_revision.revision_id, model_revision.resource_version_id);
        assert_eq!(closure.profile_revision.semantic_digest, model_revision.content_digest);

        let policy_revision = published(ResourceKind::PolicyRevision, '6');
        let resolved = ApplyDeploymentClosure::Policy(ApplyPolicyDeploymentBindings {
            applicability_digest: digest('7'),
            qualification_evidence: evidence('8'),
        })
        .resolve(std::slice::from_ref(&policy_revision))
        .unwrap();
        let DeploymentClosure::Policy(closure) = resolved else {
            panic!("Policy closure resolved to another Resource kind");
        };
        assert_eq!(closure.policy_revision.revision_id, policy_revision.resource_version_id);
        assert_eq!(closure.policy_revision.semantic_digest, policy_revision.content_digest);

        let sandbox_revision = published(ResourceKind::SandboxProfileRevision, '9');
        let resolved = ApplyDeploymentClosure::SandboxProfile(ApplySandboxDeploymentBindings {
            runtime_revision: exact_version(ResourceKind::SandboxRuntimeRevision, 'a'),
            policy_bindings: Vec::new(),
            qualification_evidence: evidence('b'),
        })
        .resolve(std::slice::from_ref(&sandbox_revision))
        .unwrap();
        let DeploymentClosure::SandboxProfile(closure) = resolved else {
            panic!("Sandbox closure resolved to another Resource kind");
        };
        assert_eq!(closure.profile_revision.revision_id, sandbox_revision.resource_version_id);
        assert_eq!(closure.profile_revision.semantic_digest, sandbox_revision.content_digest);

        let error = ApplyDeploymentClosure::Policy(ApplyPolicyDeploymentBindings {
            applicability_digest: digest('c'),
            qualification_evidence: evidence('d'),
        })
        .resolve(std::slice::from_ref(&skill_revision))
        .unwrap_err();
        assert!(matches!(error, ApplyError::InvalidResponse(_)));
    }
}
