//! Example constructors over the existing Registry owner. These files are authoring data, not a new API.
use super::{fields_schema, input_schema, observation_schema, MAX_RETRIEVED_ITEMS};
use insight_platform_contracts::*;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(super) enum Request {
    Policy {
        display_name: String,
        policy_kind: PolicyKind,
        rules: Value,
    },
    Interface {
        display_name: String,
        region: DataRegion,
        entitlement_policy: ExactVersionRef,
        cache_policy: ExactVersionRef,
    },
    Implementation {
        display_name: String,
        interface_revision: ExactVersionRef,
    },
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Source {
    schema_version: u32,
    display_name: String,
    resource_noun: String,
    resource_kind: RegistryResourceKind,
    definition: Value,
}
fn hash(value: &Value) -> Result<Sha256Digest, &'static str> {
    canonical_digest(value)
        .map_err(|_| "invalid declaration")?
        .parse()
        .map_err(|_| "invalid declaration")
}
fn policy(exact: &ExactVersionRef) -> Result<(), &'static str> {
    if exact.resource_kind != ResourceKind::PolicyRevision || exact.validate().is_err() {
        return Err("actual PolicyRevision required");
    }
    Ok(())
}
fn common(dependencies: Vec<ExactVersionRef>, policies: Vec<ExactVersionRef>) -> Value {
    json!({"dependency_versions":dependencies,"policy_versions":policies})
}
pub(super) fn source(request: Request) -> Result<Source, &'static str> {
    let (display_name, resource_noun, resource_kind, definition) = match request {
        Request::Policy {
            display_name,
            policy_kind,
            rules,
        } => {
            if !matches!(
                policy_kind,
                PolicyKind::Authorization
                    | PolicyKind::Ranking
                    | PolicyKind::Parser
                    | PolicyKind::Chunker
                    | PolicyKind::DataFlow
                    | PolicyKind::Network
                    | PolicyKind::Tls
                    | PolicyKind::Trust
            ) || !rules.is_object()
            {
                return Err("Context policy declaration required");
            }
            let mut value = common(vec![], vec![]);
            value["policy_kind"] = json!(policy_kind);
            value["rules_digest"] = json!(hash(&rules)?);
            for name in [
                "selection",
                "scheduling",
                "retention",
                "model_safety",
                "model_budget",
                "model_public_projection",
                "mcp_protocol",
                "mcp_auth",
                "sandbox_isolation",
                "sandbox_resource",
                "sandbox_network",
                "sandbox_artifact_io",
                "sandbox_secret_resolution",
            ] {
                value[name] = Value::Null;
            }
            // Full actual rule bytes remain in the uploaded authoring declaration.
            (
                display_name,
                "policies",
                RegistryResourceKind::Policy,
                json!({"spec":value,"rules":rules}),
            )
        }
        Request::Interface {
            display_name,
            region,
            entitlement_policy,
            cache_policy,
        } => {
            policy(&entitlement_policy)?;
            policy(&cache_policy)?;
            if entitlement_policy == cache_policy {
                return Err("distinct policies required");
            }
            let mut policies = vec![entitlement_policy.clone(), cache_policy.clone()];
            policies.sort_by(|a, b| a.revision_id.cmp(&b.revision_id));
            let mut value = common(vec![], policies);
            let filter=ClosedJsonSchema::build(json!({"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object","additionalProperties":false,"properties":{},"required":[]})).map_err(|_|"filter schema")?;
            let item = fields_schema();
            let query = input_schema();
            let observation = observation_schema();
            value["query_schema_digest"] = json!(query.canonical_digest);
            value["filter_schema_digest"] = json!(filter.canonical_digest);
            value["item_schema_digest"] = json!(item.canonical_digest);
            value["observation_schema_digest"] = json!(observation.canonical_digest);
            value["allowed_consistency"] = json!([ContextConsistencyMode::ExternalObservation]);
            value["citation"] = json!(ContextCitationContract {
                allowed_strengths: vec![ContextCitationStrength::ObservationOnly],
                locator_kinds: vec![ContextLocatorKind::RemoteOpaque],
                require_content_digest: true,
                maximum_display_label_bytes: 256
            });
            value["pagination"] = json!(ContextPaginationContract {
                maximum_page_size: MAX_RETRIEVED_ITEMS,
                maximum_cursor_bytes: 1024,
                cursor_ttl_milliseconds: 60_000
            });
            value["ranking"] = json!(ContextRankingContract {
                score_domain_digest: hash(
                    &json!({"algorithm":"distinct_query_term_overlap_fraction","scale":1_000_000,"tie_break":"locator_ascending","version":1})
                )?,
                reranker_contract_digest: None,
                maximum_candidates: 256
            });
            value["data_policy"] = json!(ContextDataPolicyContract {
                maximum_classification: DataClassification::Internal,
                allowed_regions: vec![region],
                entitlement_policy,
                cache_policy,
                maximum_retention_milliseconds: 3_600_000
            });
            value["limits"] = json!(ContextInterfaceLimits {
                maximum_query_bytes: 2048,
                maximum_filter_bytes: 1024,
                maximum_item_bytes: 16_384,
                maximum_total_bytes: 65_536,
                maximum_items: MAX_RETRIEVED_ITEMS,
                maximum_fan_out: 1
            });
            (
                display_name,
                "contexts",
                RegistryResourceKind::ContextSourceInterface,
                json!({"spec":value,"schemas":{"query":query,"filter":filter,"item":item,"observation":observation}}),
            )
        }
        Request::Implementation {
            display_name,
            interface_revision,
        } => {
            if interface_revision.resource_kind != ResourceKind::ContextSourceInterfaceRevision
                || interface_revision.validate().is_err()
            {
                return Err("actual Context InterfaceRevision required");
            }
            let mut value = common(vec![interface_revision.clone()], vec![]);
            value["interface_revision"] = json!(interface_revision);
            value["backend_kind"] = json!(ContextBackendKind::RemoteSearch);
            value["contract"] = json!(ContextImplementationContract {
                backend: ContextBackendContract::RemoteSearch {
                    protocol_contract_digest: remote_context_protocol_contract_digest(),
                    result_mapping_digest: remote_context_result_mapping_digest()
                },
                credential_requirements: vec![],
                limits: ContextBackendLimits {
                    maximum_request_bytes: 8192,
                    maximum_response_bytes: 65_536,
                    maximum_candidates: 256,
                    maximum_remote_state_bytes: 0,
                    maximum_poll_count: 0,
                    total_timeout_milliseconds: 15_000
                }
            });
            (
                display_name,
                "context-implementations",
                RegistryResourceKind::ContextSourceImplementation,
                json!({"spec":value}),
            )
        }
    };
    if display_name.is_empty()
        || display_name.len() > 128
        || display_name.chars().any(char::is_control)
    {
        return Err("bounded display name required");
    }
    Ok(Source {
        schema_version: 1,
        display_name,
        resource_noun: resource_noun.into(),
        resource_kind,
        definition,
    })
}
pub(super) fn publication(source: Source, artifact: ArtifactRef) -> Result<Value, &'static str> {
    if source.schema_version != 1
        || artifact.validate().is_err()
        || artifact.media_type() != "application/json"
    {
        return Err("source Artifact invalid");
    }
    let value = serde_json::to_value(&source).map_err(|_| "invalid source")?;
    let bytes = canonical_json(&value).map_err(|_| "invalid source")?;
    let digest = hash(&value)?;
    if artifact.content_digest() != &digest || artifact.byte_length() != bytes.len() as u64 {
        return Err("Artifact must bind these actual canonical source bytes");
    }
    let expected = match source.resource_kind {
        RegistryResourceKind::Policy => "policies",
        RegistryResourceKind::ContextSourceInterface => "contexts",
        RegistryResourceKind::ContextSourceImplementation => "context-implementations",
        _ => return Err("unsupported example resource"),
    };
    if source.resource_noun != expected {
        return Err("resource identity mismatch");
    }
    match source.resource_kind {
        RegistryResourceKind::Policy => {
            let rules = source.definition.get("rules").ok_or("rules missing")?;
            if !rules.is_object()
                || source.definition["spec"]["rules_digest"] != json!(hash(rules)?)
            {
                return Err("policy declaration digest mismatch");
            }
        }
        RegistryResourceKind::ContextSourceInterface => {
            for name in ["query", "filter", "item", "observation"] {
                let schema: ClosedJsonSchema =
                    serde_json::from_value(source.definition["schemas"][name].clone())
                        .map_err(|_| "schema missing")?;
                let actual =
                    ClosedJsonSchema::build(schema.schema).map_err(|_| "invalid schema")?;
                if actual.canonical_digest != schema.canonical_digest
                    || source.definition["spec"][format!("{name}_schema_digest")]
                        != json!(actual.canonical_digest)
                {
                    return Err("schema declaration digest mismatch");
                }
            }
        }
        _ => {}
    }
    let mut spec = source
        .definition
        .get("spec")
        .filter(|spec| spec.is_object())
        .ok_or("source definition missing")?
        .clone();
    if spec.get("authoring_package").is_some() || spec.get("contract_digest").is_some() {
        return Err("source cannot override Artifact binding");
    }
    spec["authoring_package"] = json!(AuthoringPackage {
        artifact: artifact.clone(),
        manifest_digest: digest.clone()
    });
    spec["contract_digest"] = json!(digest);
    let document: ResourceDocument =
        serde_json::from_value(json!({"resource_kind":source.resource_kind,"spec":spec}))
            .map_err(|_| "typed resource document rejected")?;
    document
        .validate()
        .map_err(|_| "resource document invalid")?;
    Ok(
        json!({"schema_version":1,"resource_noun":expected,"create":{"alias":null,"display_name":source.display_name,"document":document},"publish":{"kind":"single","revision_no":1,"content_digest":digest,"artifact_id":artifact.artifact_id()}}),
    )
}

/// The existing public deployment DTO, retaining the already published InterfaceRevision.
pub(super) fn deployment(
    mut bindings: Value,
    manifest: WorkerManifest,
    environment: &str,
) -> Result<Value, &'static str> {
    if environment != "development" {
        return Err("this sample targets development");
    }
    let capability = remote_context_adapter_execution_capability(
        &remote_context_protocol_contract_digest(),
        &remote_context_result_mapping_digest(),
    )
    .map_err(|_| "invalid Remote Context capability")?;
    if manifest.work_class != WorkClass::Context
        || !manifest
            .execution_capabilities
            .capabilities
            .contains(&capability)
    {
        return Err("current Remote Context worker manifest required");
    }
    let digest = manifest
        .canonical_digest()
        .map_err(|_| "invalid worker manifest")?;
    let object = bindings
        .as_object_mut()
        .ok_or("Context bindings object required")?;
    if object.contains_key("required_worker_manifest_digest") {
        return Err("worker digest must come only from the supplied actual manifest");
    }
    object.insert("required_worker_manifest_digest".into(), json!(digest));
    let closure = DeploymentClosure::ContextSourceInterface(
        serde_json::from_value(bindings).map_err(|_| "invalid Context bindings")?,
    );
    closure.validate().map_err(|_| "invalid Context closure")?;
    let DeploymentClosure::ContextSourceInterface(bindings) = closure else {
        unreachable!()
    };
    Ok(
        json!({"resource_version_id":bindings.interface.revision_id,"environment":environment,"closure":{"resource_kind":"context_source_interface","bindings":bindings}}),
    )
}

pub(super) fn destination(
    endpoint: CanonicalHttpEndpoint,
    region: DataRegion,
    trusted_root_pem: String,
) -> Result<InstalledRemoteContextDestinationV1, &'static str> {
    let pem = trusted_root_pem.trim();
    if endpoint.base_path != "/v1/query"
        || !pem.starts_with("-----BEGIN CERTIFICATE-----")
        || !pem.ends_with("-----END CERTIFICATE-----")
        || pem.matches("-----BEGIN ").count() != 1
        || pem.matches("-----END ").count() != 1
    {
        return Err("sample endpoint and one public certificate required");
    }
    let destination = InstalledRemoteContextDestinationV1 {
        schema_version: 1,
        protocol_contract_digest: remote_context_protocol_contract_digest(),
        result_mapping_digest: remote_context_result_mapping_digest(),
        endpoint_identity_digest: endpoint
            .canonical_digest()
            .map_err(|_| "endpoint invalid")?,
        endpoint,
        region,
        credential_injections: vec![],
        trusted_root_pem,
        maximum_request_bytes: 8192,
        maximum_response_bytes: 65_536,
    };
    if !destination.validate_shape() {
        return Err("destination shape invalid");
    }
    Ok(destination)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn version(n: u16) -> ExactVersionRef {
        ExactVersionRef::new(
            format!(
                "{}_018f3e20-0000-7000-8000-{n:012x}",
                ResourceKind::PolicyRevision.descriptor().prefix
            )
            .parse()
            .unwrap(),
            hash(&json!({"test_policy":n})).unwrap(),
        )
        .unwrap()
    }
    fn artifact(source: &Source) -> ArtifactRef {
        let value = serde_json::to_value(source).unwrap();
        ArtifactRef::new(
            "art_018f3e20-0000-7000-8000-000000000099".parse().unwrap(),
            hash(&value).unwrap(),
            canonical_json(&value).unwrap().len() as u64,
            "application/json",
            DataClassification::Internal,
            None,
        )
        .unwrap()
    }
    #[test]
    fn actual_source_digest_and_existing_typed_resource_contract_bind_publication() {
        let declaration = source(Request::Interface {
            display_name: "Document corpus".into(),
            region: "global".parse().unwrap(),
            entitlement_policy: version(1),
            cache_policy: version(2),
        })
        .unwrap();
        let proof = artifact(&declaration);
        let manifest = publication(declaration, proof).unwrap();
        let document: ResourceDocument =
            serde_json::from_value(manifest["create"]["document"].clone()).unwrap();
        document.validate().unwrap();
        let ResourceDocument::ContextSourceInterface(interface) = document else {
            panic!("interface")
        };
        assert_eq!(
            interface.query_schema_digest,
            input_schema().canonical_digest
        );
        assert_eq!(
            interface.observation_schema_digest,
            observation_schema().canonical_digest
        );
        assert!(manifest.get("deployment").is_none());
        assert!(
            manifest.get("kind").is_none(),
            "not an ApplyManifest requiring deployment"
        );
        let policy = source(Request::Policy {
            display_name: "Context parser".into(),
            policy_kind: PolicyKind::Parser,
            rules: json!({"mode":"frozen_utf8_paragraphs","version":1}),
        })
        .unwrap();
        let mut proof = serde_json::to_value(artifact(&policy)).unwrap();
        proof["content_digest"] = json!(hash(&json!({"other":true})).unwrap());
        assert!(publication(policy, serde_json::from_value(proof).unwrap()).is_err());
        let mut changed = source(Request::Policy {
            display_name: "Context parser".into(),
            policy_kind: PolicyKind::Parser,
            rules: json!({"mode":"frozen_utf8_paragraphs","version":1}),
        })
        .unwrap();
        changed.definition["rules"]["version"] = json!(2);
        let proof = artifact(&changed);
        assert!(
            publication(changed, proof).is_err(),
            "a matching Artifact cannot legitimize contradictory rule bytes"
        );
        let mut changed = source(Request::Interface {
            display_name: "Context schema".into(),
            region: "global".parse().unwrap(),
            entitlement_policy: version(1),
            cache_policy: version(2),
        })
        .unwrap();
        changed.definition["schemas"]["query"]["schema"]["properties"]["question"]["maxLength"] =
            json!(1000);
        let proof = artifact(&changed);
        assert!(
            publication(changed, proof).is_err(),
            "uploaded schema bytes must retain their declared digest"
        );
    }

    #[test]
    fn physical_destination_and_deployment_reuse_exact_owners_without_republishing_interface() {
        let endpoint = CanonicalHttpEndpoint {
            scheme: CapabilityEndpointScheme::Https,
            host: "documents.example.test".into(),
            port: 443,
            base_path: "/v1/query".into(),
        };
        // Shape fixture only; the installer and Egress separately parse actual certificates.
        let root = "-----BEGIN CERTIFICATE-----\nMAA=\n-----END CERTIFICATE-----\n".to_owned();
        let grant = destination(endpoint.clone(), "global".parse().unwrap(), root).unwrap();
        assert_eq!(
            grant.endpoint_identity_digest,
            endpoint.canonical_digest().unwrap()
        );
        assert_eq!(
            grant.protocol_contract_digest,
            remote_context_protocol_contract_digest()
        );
        assert!(destination(
            endpoint.clone(),
            "global".parse().unwrap(),
            "-----BEGIN PRIVATE KEY-----\nsecret\n-----END PRIVATE KEY-----".into()
        )
        .is_err());
        let exact = |kind: ResourceKind, n: u16| {
            ExactVersionRef::new(
                format!(
                    "{}_018f3e20-0000-7000-8000-{n:012x}",
                    kind.descriptor().prefix
                )
                .parse()
                .unwrap(),
                hash(&json!({"test_revision":n})).unwrap(),
            )
            .unwrap()
        };
        let declaration = source(Request::Policy {
            display_name: "test evidence".into(),
            policy_kind: PolicyKind::Parser,
            rules: json!({"fixture":true}),
        })
        .unwrap();
        let closure = ContextDeploymentClosure {
            implementation: exact(ResourceKind::ContextSourceImplementationRevision, 20),
            interface: exact(ResourceKind::ContextSourceInterfaceRevision, 21),
            required_worker_manifest_digest: hash(&json!({"test_manifest":true})).unwrap(),
            backend: ContextBackendBinding::RemoteSearch {
                endpoint,
                endpoint_identity_digest: grant.endpoint_identity_digest,
                region: grant.region,
            },
            secret_bindings: vec![],
            network_policy: Some(version(1)),
            tls_policy: Some(version(2)),
            trust_policy: Some(version(3)),
            parser_policy: version(4),
            chunker_policy: version(5),
            embedding_model_deployment: None,
            ranking_policy: version(6),
            data_policy: version(7),
            conformance_evidence: artifact(&declaration),
        };
        let manifest = WorkerManifest {
            manifest_version: WORKER_MANIFEST_VERSION,
            worker_role: "context-worker".into(),
            work_class: WorkClass::Context,
            adapter_runtime_digest: hash(&json!({"test_adapter":true})).unwrap(),
            worker_build_digest: hash(&json!({"test_binary":true})).unwrap(),
            execution_capabilities: WorkerExecutionCapabilities {
                schema_version: EXECUTION_REQUIREMENT_VERSION,
                capabilities: vec![remote_context_adapter_execution_capability(
                    &remote_context_protocol_contract_digest(),
                    &remote_context_result_mapping_digest(),
                )
                .unwrap()],
            },
            protocol_version: WORKER_PROTOCOL_VERSION,
            max_concurrency: 4,
            critical_control_reserved_slots: 1,
        };
        let mut input = json!(closure);
        input
            .as_object_mut()
            .unwrap()
            .remove("required_worker_manifest_digest");
        let body = deployment(input.clone(), manifest.clone(), "development").unwrap();
        assert_eq!(
            body["resource_version_id"],
            json!(closure.interface.revision_id)
        );
        let mut expected = json!(closure);
        expected["required_worker_manifest_digest"] = json!(manifest.canonical_digest().unwrap());
        assert_eq!(body["closure"]["bindings"], expected);
        assert!(body.get("publish").is_none());
        assert!(deployment(expected, manifest.clone(), "development").is_err());
        input["trust_policy"] = Value::Null;
        assert!(deployment(input, manifest, "development").is_err());
    }
}
