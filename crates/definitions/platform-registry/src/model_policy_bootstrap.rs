//! Pure installer declaration builder. Its output is the ordinary Registry/Artifact closure.
use crate::model_configuration::BASIC_MODEL_PLATFORM_INSTRUCTION;
use insight_platform_contracts::*;
use serde::Serialize;
use serde_json::{json, Value};
use sha2::{Digest as _, Sha256};

#[derive(Debug, Clone)]
pub struct ModelBootstrapPolicyMaterial {
    pub identity: ModelBootstrapPolicyIdentityV1,
    pub resource: TypedPayload,
    pub published: TypedPayload,
    pub deployment: TypedPayload,
    pub exact: ExactPolicyBinding,
}
#[derive(Debug, Clone)]
pub struct ModelPolicyBootstrapMaterial {
    pub declaration_bytes: Vec<u8>,
    pub content_digest: Sha256Digest,
    pub authoring_artifact: ArtifactRef,
    pub policies: Vec<ModelBootstrapPolicyMaterial>,
}
impl ModelPolicyBootstrapMaterial {
    pub fn policy(&self, role: ModelBootstrapPolicyRole) -> &ModelBootstrapPolicyMaterial {
        self.policies
            .iter()
            .find(|item| item.identity.role == role)
            .expect("builder emits exact roles")
    }
    pub fn configuration_policies(&self) -> ModelConfigurationPoliciesV1 {
        use ModelBootstrapPolicyRole::*;
        ModelConfigurationPoliciesV1 {
            protocol: self.policy(Protocol).exact.revision.clone(),
            safety: self.policy(Safety).exact.revision.clone(),
            budget: self.policy(Budget).exact.revision.clone(),
            public_projection: self.policy(PublicProjection).exact.revision.clone(),
            selection: self.policy(Selection).exact.clone(),
            execution: self.policy(Execution).exact.clone(),
        }
    }
}
fn raw_digest(bytes: &[u8]) -> Result<Sha256Digest, ModelPolicyBootstrapError> {
    let hex = Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    format!("sha256:{hex}")
        .parse()
        .map_err(|_| ModelPolicyBootstrapError)
}
fn digest(value: &impl Serialize) -> Result<Sha256Digest, ModelPolicyBootstrapError> {
    canonical_digest(&serde_json::to_value(value).map_err(|_| ModelPolicyBootstrapError)?)
        .map_err(|_| ModelPolicyBootstrapError)?
        .parse()
        .map_err(|_| ModelPolicyBootstrapError)
}
fn payload(value: &impl Serialize) -> Result<TypedPayload, ModelPolicyBootstrapError> {
    TypedPayload::with_limit(1, value, MAX_MODEL_POLICY_DECLARATION_BYTES)
        .map_err(|_| ModelPolicyBootstrapError)
}
fn rules(role: ModelBootstrapPolicyRole) -> Result<Value, ModelPolicyBootstrapError> {
    use ModelBootstrapPolicyRole::*;
    Ok(match role {
        Protocol => json!({"schema_version":1,"authority":"installed_model_adapter",
            "protocols":[ModelProviderWireProtocol::OpenAiResponses,ModelProviderWireProtocol::AnthropicMessages],
            "protocol_versions":["responses-v1","2023-06-01"]}),
        Network => json!({"schema_version":1,"authority":"installed_destination_grant",
            "require_exact_endpoint":true,"authorize_current_dispatch_before_dns":true,"redirects":false,
            "public_dns_only":true,"development_loopback_requires_explicit_grant":true}),
        Tls => json!({"schema_version":1,"https_required":true,"verify_server_identity":true,
            "trust_roots":"installed_destination_grant_or_system"}),
        Trust => json!({"schema_version":1,"authority":"current_security_dispatch",
            "require_exact_frozen_closure":true,"require_active_principal":true,
            "credential_purpose":MODEL_API_KEY_PURPOSE}),
        Data => json!({"schema_version":1,"maximum_classification":DataClassification::Internal,
            "allowed_regions":"installed_destination_grant","training":"unspecified",
            "provider_retention":"unknown","require_current_data_authorization":true}),
        Safety => serde_json::to_value(ModelSafetyPolicyDocument {
            schema_version: 1,
            contract_id: "platform.model_safety.v1".to_owned(),
            platform_instruction: BASIC_MODEL_PLATFORM_INSTRUCTION.to_owned(),
            instruction_content_digest: raw_digest(BASIC_MODEL_PLATFORM_INSTRUCTION.as_bytes())?,
            instruction_byte_budget: 1024,
            instruction_token_budget: 256,
            pre_dispatch_rules_digest: digest(
                &json!({"schema_version":1,"current_dispatch_authorization":true,
                "canonical_instruction_accounting":true,"exact_data_policy":true}),
            )?,
            post_response_rules_digest: digest(
                &json!({"schema_version":1,"exact_output_schema":true,
                "json_repair":false,"undeclared_tool_intents":false}),
            )?,
        })
        .map_err(|_| ModelPolicyBootstrapError)?,
        Budget => serde_json::to_value(ModelBudgetPolicyDocument {
            schema_version: 1,
            maximum_attempts_per_turn: 1,
            maximum_input_tokens_per_turn: 8192,
            maximum_output_tokens_per_turn: 2048,
            maximum_total_tokens_per_turn: 10240,
            cost_ceiling_microunits_per_turn: 1_000_000,
        })
        .map_err(|_| ModelPolicyBootstrapError)?,
        PublicProjection => serde_json::to_value(ModelPublicProjectionPolicyDocument {
            schema_version: 1,
            reject_prompt_overflow: true,
            retain_source_map: true,
            retain_sensitive_prompt_body: false,
        })
        .map_err(|_| ModelPolicyBootstrapError)?,
        Selection => serde_json::to_value(CandidateSelectionPolicyDocument {
            schema_version: 1,
            mode: CandidateSelectionMode::OnlyCandidate,
            route_schema_digest: None,
        })
        .map_err(|_| ModelPolicyBootstrapError)?,
        Execution => json!({"schema_version":1,"authority":"durable_plan_run_job",
            "exact_deployment_bindings":true,"typed_plan_validation":true,
            "current_lease_and_quota_fencing":true,"additional_execution_limits":null}),
    })
}

pub fn build_model_policy_bootstrap(
    seed: &ModelPolicyBootstrapSeedV1,
) -> Result<ModelPolicyBootstrapMaterial, ModelPolicyBootstrapError> {
    seed.validate()?;
    let declarations=seed.policies.iter().map(|identity|Ok(json!({
        "identity":identity,"policy_kind":identity.role.policy_kind(),"rules":rules(identity.role)?,
    }))).collect::<Result<Vec<Value>,ModelPolicyBootstrapError>>()?;
    let declaration = json!({"schema_version":1,"kind":"insight.installation-model-policies/v1",
        "basis":"installation_declaration","seed":seed,"policies":declarations,
        "validation":{"scope":"owning_policy_structural_validation","provider_conformance":false}});
    let declaration_bytes = canonical_json(&declaration).map_err(|_| ModelPolicyBootstrapError)?;
    if declaration_bytes.len() > MAX_MODEL_POLICY_DECLARATION_BYTES {
        return Err(ModelPolicyBootstrapError);
    }
    let content_digest = raw_digest(&declaration_bytes)?;
    let authoring_artifact = ArtifactRef::new(
        seed.authoring_artifact_id.clone(),
        content_digest.clone(),
        declaration_bytes.len() as u64,
        "application/json",
        DataClassification::Internal,
        Some("installation-model-policies.json".to_owned()),
    )
    .map_err(|_| ModelPolicyBootstrapError)?;
    let authoring = AuthoringPackage {
        artifact: authoring_artifact.clone(),
        manifest_digest: content_digest.clone(),
    };
    let mut policies = Vec::with_capacity(10);
    for (identity, decl) in seed.policies.iter().zip(&declarations) {
        let role = identity.role;
        let rule = rules(role)?;
        let mut spec = PolicyResourceSpec {
            authoring_package: authoring.clone(),
            contract_digest: digest(decl)?,
            dependency_versions: Vec::new(),
            policy_versions: Vec::new(),
            policy_kind: role.policy_kind(),
            rules_digest: digest(&rule)?,
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
        };
        match role {
            ModelBootstrapPolicyRole::Safety => {
                spec.model_safety =
                    Some(serde_json::from_value(rule).map_err(|_| ModelPolicyBootstrapError)?)
            }
            ModelBootstrapPolicyRole::Budget => {
                spec.model_budget =
                    Some(serde_json::from_value(rule).map_err(|_| ModelPolicyBootstrapError)?)
            }
            ModelBootstrapPolicyRole::PublicProjection => {
                spec.model_public_projection =
                    Some(serde_json::from_value(rule).map_err(|_| ModelPolicyBootstrapError)?)
            }
            ModelBootstrapPolicyRole::Selection => {
                spec.selection =
                    Some(serde_json::from_value(rule).map_err(|_| ModelPolicyBootstrapError)?)
            }
            _ => (),
        }
        let document = ResourceDocument::Policy(Box::new(spec));
        document.validate().map_err(|_| ModelPolicyBootstrapError)?;
        let draft = ResourceDraftPayload {
            alias: None,
            display_name: format!("Installation model {}", role.name()),
            document: document.clone(),
            validation: None,
        };
        draft.validate().map_err(|_| ModelPolicyBootstrapError)?;
        let validation = ValidationSummary {
            program_requirement: None,
            validator_digest: digest(
                &json!({"schema_version":1,"owner":"PolicyResourceSpec::validate",
                "scope":"installation_declaration"}),
            )?,
            validated_draft_digest: draft
                .document_digest()
                .map_err(|_| ModelPolicyBootstrapError)?,
            dependency_closure_digest: digest(&json!({"versions":[],"policies":[]}))?,
            security_evidence_digest: digest(&json!({"tenant_id":seed.tenant_id,
                "installation_principal_id":seed.installation_principal_id,"basis":"installation_declaration"}))?,
            warnings: Vec::new(),
        };
        validation
            .validate()
            .map_err(|_| ModelPolicyBootstrapError)?;
        let published = payload(&PublishedVersionPayload {
            document,
            validation,
        })?;
        let revision = ExactVersionRef::new(
            identity.revision_id.clone(),
            published
                .digest
                .parse()
                .map_err(|_| ModelPolicyBootstrapError)?,
        )
        .map_err(|_| ModelPolicyBootstrapError)?;
        let closure = DeploymentClosure::Policy(PolicyDeploymentClosure {
            policy_revision: revision.clone(),
            applicability_digest: digest(&json!({"tenant_id":seed.tenant_id,
                "environment":seed.environment,"role":role}))?,
            qualification_evidence: authoring_artifact.clone(),
        });
        closure.validate().map_err(|_| ModelPolicyBootstrapError)?;
        let deployment = payload(&closure)?;
        let exact = ExactPolicyBinding {
            revision,
            deployment: ExactDeploymentRef::new(
                identity.deployment_id.clone(),
                deployment
                    .digest
                    .parse()
                    .map_err(|_| ModelPolicyBootstrapError)?,
            )
            .map_err(|_| ModelPolicyBootstrapError)?,
        };
        policies.push(ModelBootstrapPolicyMaterial {
            identity: identity.clone(),
            resource: payload(&draft)?,
            published,
            deployment,
            exact,
        });
    }
    Ok(ModelPolicyBootstrapMaterial {
        declaration_bytes,
        content_digest,
        authoring_artifact,
        policies,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    fn id(kind: ResourceKind, n: u16) -> ResourceId {
        format!(
            "{}_0198f1c9-32e4-75e1-a9e8-d95ca0f4{n:04x}",
            kind.descriptor().prefix
        )
        .parse()
        .unwrap()
    }
    fn seed() -> ModelPolicyBootstrapSeedV1 {
        ModelPolicyBootstrapSeedV1 {
            schema_version: 1,
            tenant_id: id(ResourceKind::Tenant, 1),
            installation_principal_id: id(ResourceKind::Principal, 2),
            created_by: id(ResourceKind::Principal, 3),
            request_id: id(ResourceKind::ServerRequest, 4),
            environment: "development".into(),
            authoring_artifact_id: id(ResourceKind::Artifact, 5),
            authoring_blob_id: id(ResourceKind::InternalBlob, 6),
            model_quota_account_id: id(ResourceKind::QuotaAccount, 9),
            encryption_domain_id: id(ResourceKind::EncryptionDomain, 7),
            retention_policy: ExactVersionRef::new(
                id(ResourceKind::PolicyRevision, 8),
                raw_digest(b"retention").unwrap(),
            )
            .unwrap(),
            retain_until: "2027-01-01T00:00:00.000000Z".parse().unwrap(),
            policies: ModelBootstrapPolicyRole::ALL.map(|role| {
                let n = 100 + role as u16 * 4;
                ModelBootstrapPolicyIdentityV1 {
                    role,
                    resource_id: id(ResourceKind::Policy, n),
                    revision_id: id(ResourceKind::PolicyRevision, n + 1),
                    deployment_id: id(ResourceKind::PolicyDeployment, n + 2),
                    artifact_reference_id: id(ResourceKind::ArtifactLink, n + 3),
                }
            }),
        }
    }
    fn unwrapped<T: serde::de::DeserializeOwned>(payload: &TypedPayload) -> T {
        let mut value = payload.value.clone();
        assert_eq!(
            value.as_object_mut().unwrap().remove("schema_version"),
            Some(json!(1))
        );
        serde_json::from_value(value).unwrap()
    }
    #[test]
    fn complete_declaration_binds_actual_rules_and_normal_owning_payloads() {
        let seed = seed();
        let built = build_model_policy_bootstrap(&seed).unwrap();
        assert!(built.declaration_bytes.len() <= MAX_MODEL_POLICY_DECLARATION_BYTES);
        let declaration: Value = serde_json::from_slice(&built.declaration_bytes).unwrap();
        assert_eq!(
            canonical_json(&declaration).unwrap(),
            built.declaration_bytes
        );
        assert_eq!(
            raw_digest(&built.declaration_bytes).unwrap(),
            built.content_digest
        );
        assert_eq!(declaration["basis"], "installation_declaration");
        assert_eq!(declaration["validation"]["provider_conformance"], false);
        for (position, item) in built.policies.iter().enumerate() {
            let draft: ResourceDraftPayload = unwrapped(&item.resource);
            draft.validate().unwrap();
            let published: PublishedVersionPayload = unwrapped(&item.published);
            published.document.validate().unwrap();
            published.validation.validate().unwrap();
            assert_eq!(published.document, draft.document);
            assert_eq!(
                published.validation.validated_draft_digest,
                draft.document_digest().unwrap()
            );
            assert_eq!(
                item.exact.revision.semantic_digest.as_str(),
                item.published.digest
            );
            assert_eq!(
                digest(&item.published.value).unwrap().as_str(),
                item.published.digest
            );
            assert_eq!(
                item.exact.deployment.deployment_digest.as_str(),
                item.deployment.digest
            );
            let closure: DeploymentClosure = unwrapped(&item.deployment);
            closure.validate().unwrap();
            let ResourceDocument::Policy(spec) = published.document else {
                panic!("not a Policy")
            };
            assert_eq!(spec.authoring_package.artifact, built.authoring_artifact);
            assert_eq!(
                spec.rules_digest,
                digest(&declaration["policies"][position]["rules"]).unwrap()
            );
        }
        assert!(built.configuration_policies().validate());
        let safety: PublishedVersionPayload =
            unwrapped(&built.policy(ModelBootstrapPolicyRole::Safety).published);
        let ResourceDocument::Policy(safety) = safety.document else {
            unreachable!()
        };
        let safety = safety.model_safety.unwrap();
        assert_eq!(
            safety.platform_instruction,
            BASIC_MODEL_PLATFORM_INSTRUCTION
        );
        assert_eq!(
            safety.instruction_content_digest,
            raw_digest(BASIC_MODEL_PLATFORM_INSTRUCTION.as_bytes()).unwrap()
        );
        let budget: PublishedVersionPayload =
            unwrapped(&built.policy(ModelBootstrapPolicyRole::Budget).published);
        let ResourceDocument::Policy(budget) = budget.document else {
            unreachable!()
        };
        let budget = budget.model_budget.unwrap();
        assert_eq!(
            (
                budget.maximum_input_tokens_per_turn,
                budget.maximum_output_tokens_per_turn
            ),
            (8192, 2048)
        );
        let selection: PublishedVersionPayload =
            unwrapped(&built.policy(ModelBootstrapPolicyRole::Selection).published);
        let ResourceDocument::Policy(selection) = selection.document else {
            unreachable!()
        };
        assert_eq!(
            selection.selection.unwrap().mode,
            CandidateSelectionMode::OnlyCandidate
        );
    }
    #[test]
    fn frozen_identity_changes_cannot_reuse_material_or_catalog() {
        let seed = seed();
        let first = build_model_policy_bootstrap(&seed).unwrap();
        let again = build_model_policy_bootstrap(&seed).unwrap();
        assert_eq!(first.declaration_bytes, again.declaration_bytes);
        assert_eq!(
            first.configuration_policies(),
            again.configuration_policies()
        );
        let mut other = seed.clone();
        other.tenant_id = id(ResourceKind::Tenant, 999);
        let changed = build_model_policy_bootstrap(&other).unwrap();
        assert_ne!(first.content_digest, changed.content_digest);
        for (left, right) in first.policies.iter().zip(changed.policies) {
            assert_ne!(left.exact, right.exact);
        }
        other.policies[1].revision_id = other.policies[0].revision_id.clone();
        assert!(build_model_policy_bootstrap(&other).is_err());
    }
}
