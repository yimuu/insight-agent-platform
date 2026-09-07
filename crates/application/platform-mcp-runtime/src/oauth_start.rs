use chrono::{DateTime, Utc};
use insight_platform_contracts::{
    ExactDeploymentRef, ExactVersionRef, McpAuthPolicyDocument, McpOAuthEndpoint, PrincipalKind,
    ResourceId, SecretPurpose, Sha256Digest,
};
use serde::Serialize;
use std::sync::Arc;
use url::Url;

use insight_platform_mcp_host::*;

pub struct McpOAuthAuthorizationStartService {
    config: McpOAuthAuthorizationStartConfig,
    authority: Arc<dyn McpOAuthAuthorizationStartAuthority>,
    preparation: Arc<dyn McpOAuthAuthorizationPreparationBroker>,
}

impl McpOAuthAuthorizationStartService {
    pub fn new(
        config: McpOAuthAuthorizationStartConfig,
        authority: Arc<dyn McpOAuthAuthorizationStartAuthority>,
        preparation: Arc<dyn McpOAuthAuthorizationPreparationBroker>,
    ) -> Self {
        Self {
            config,
            authority,
            preparation,
        }
    }

    pub async fn start(
        &self,
        intent: McpOAuthAuthorizationStartIntent,
        now: DateTime<Utc>,
    ) -> Result<McpOAuthAuthorizationStartOutcome, McpOAuthAuthorizationStartError> {
        intent.validate_at(now)?;
        let resolved = self
            .authority
            .resolve_authorization_start(&intent, &self.config.callback_binding_digest)
            .await
            .map_err(map_authority_error)?;
        resolved.validate_for(&intent, &self.config.callback_binding_digest)?;
        let preparation_request = McpOAuthAuthorizationPreparationRequest {
            schema_version: 1,
            tenant_id: intent.audit.tenant_id.clone(),
            task_id: intent.task_id.clone(),
            authorization_binding_id: intent.authorization_binding_id.clone(),
            mcp_deployment: intent.mcp_deployment.clone(),
            pkce_secret_provider_id: resolved.auth_profile.pkce_secret_provider_id.clone(),
            preparation_digest: preparation_digest(
                &intent,
                &resolved,
                &self.config.callback_binding_digest,
            )?,
            callback_binding_digest: self.config.callback_binding_digest.clone(),
            expires_at: intent.deadline,
        };
        preparation_request.validate_at(now)?;
        let prepared = self
            .preparation
            .prepare_or_load(&preparation_request, now)
            .await
            .map_err(map_preparation_error)?;
        prepared.validate_for(&preparation_request)?;
        let state_digest = mcp_oauth_state_digest(&prepared.state)
            .map_err(|_| rejected("mcp_oauth_start_state_invalid"))?;
        let nonce_digest = mcp_oauth_nonce_digest(&prepared.nonce)?;
        let authorization_url =
            build_authorization_url(&resolved.auth_profile, &intent.requested_scopes, &prepared)?;
        let command = BeginMcpOAuthAuthorization {
            audit: intent.audit,
            task_id: intent.task_id.clone(),
            authorization_binding_id: intent.authorization_binding_id,
            mcp_deployment: intent.mcp_deployment,
            expected_principal_binding_generation: intent.expected_principal_binding_generation,
            requested_scopes: intent.requested_scopes,
            state_digest,
            nonce_digest,
            callback_binding_digest: self.config.callback_binding_digest.clone(),
            pkce_secret_binding: prepared.pkce_secret_binding,
            reauthorization: intent.reauthorization,
            safe_prompt_key: intent.safe_prompt_key,
            deadline: intent.deadline,
        };
        let outcome = self
            .authority
            .commit_authorization_start(command)
            .await
            .map_err(map_authority_error)?;
        Ok(McpOAuthAuthorizationStartOutcome {
            disposition: outcome.disposition,
            authorization_url,
            task_id: intent.task_id,
            deadline: intent.deadline,
        })
    }
}

fn preparation_digest(
    intent: &McpOAuthAuthorizationStartIntent,
    resolved: &ResolvedMcpOAuthAuthorizationStart,
    callback_binding_digest: &Sha256Digest,
) -> Result<Sha256Digest, McpOAuthAuthorizationStartError> {
    #[derive(Serialize)]
    struct PreparationDigestInput<'a> {
        schema_version: u32,
        tenant_id: &'a ResourceId,
        principal_id: &'a ResourceId,
        principal_kind: PrincipalKind,
        task_id: &'a ResourceId,
        authorization_binding_id: &'a ResourceId,
        mcp_deployment: &'a ExactDeploymentRef,
        expected_principal_binding_generation: u64,
        requested_scopes: &'a [String],
        auth_policy: &'a ExactVersionRef,
        audience_identity_digest: &'a Sha256Digest,
        token_credential_purpose: &'a SecretPurpose,
        pkce_secret_provider_id: &'a ResourceId,
        token_secret_provider_id: &'a ResourceId,
        callback_binding_digest: &'a Sha256Digest,
        idempotency_key_digest: &'a Sha256Digest,
        request_digest: &'a Sha256Digest,
        deadline: DateTime<Utc>,
    }
    digest(&PreparationDigestInput {
        schema_version: 1,
        tenant_id: &intent.audit.tenant_id,
        principal_id: &intent.audit.principal_id,
        principal_kind: intent.audit.principal_kind,
        task_id: &intent.task_id,
        authorization_binding_id: &intent.authorization_binding_id,
        mcp_deployment: &intent.mcp_deployment,
        expected_principal_binding_generation: intent.expected_principal_binding_generation,
        requested_scopes: &intent.requested_scopes,
        auth_policy: &resolved.auth_policy,
        audience_identity_digest: &resolved.audience_identity_digest,
        token_credential_purpose: &resolved.token_credential_purpose,
        pkce_secret_provider_id: &resolved.auth_profile.pkce_secret_provider_id,
        token_secret_provider_id: &resolved.auth_profile.token_secret_provider_id,
        callback_binding_digest,
        idempotency_key_digest: &intent.audit.idempotency_key_digest,
        request_digest: &intent.audit.request_digest,
        deadline: intent.deadline,
    })
    .map_err(|_| rejected("mcp_oauth_start_digest_failed"))
}

fn build_authorization_url(
    profile: &McpAuthPolicyDocument,
    requested_scopes: &[String],
    prepared: &PreparedMcpOAuthAuthorization,
) -> Result<SensitiveMcpOAuthAuthorizationUrl, McpOAuthAuthorizationStartError> {
    let mut authorization = endpoint_url(&profile.authorization_endpoint)?;
    let redirect = endpoint_url(&profile.redirect_uri)?.to_string();
    let resource = endpoint_url(&profile.resource_indicator)?.to_string();
    let state = std::str::from_utf8(prepared.state.as_bytes())
        .map_err(|_| rejected("mcp_oauth_start_state_invalid"))?;
    let nonce = std::str::from_utf8(prepared.nonce.as_bytes())
        .map_err(|_| rejected("mcp_oauth_start_nonce_invalid"))?;
    let scope = requested_scopes.join(" ");
    authorization
        .query_pairs_mut()
        .append_pair("response_type", "code")
        .append_pair("client_id", &profile.client_id)
        .append_pair("redirect_uri", &redirect)
        .append_pair("scope", &scope)
        .append_pair("state", state)
        .append_pair("nonce", nonce)
        .append_pair("code_challenge", &prepared.pkce_challenge)
        .append_pair("code_challenge_method", "S256")
        .append_pair("resource", &resource);
    SensitiveMcpOAuthAuthorizationUrl::new(authorization.to_string())
}

fn endpoint_url(endpoint: &McpOAuthEndpoint) -> Result<Url, McpOAuthAuthorizationStartError> {
    endpoint
        .validate()
        .map_err(|_| rejected("mcp_oauth_start_endpoint_invalid"))?;
    let raw = format!(
        "https://{}:{}{}",
        endpoint.endpoint.host, endpoint.endpoint.port, endpoint.endpoint.base_path
    );
    let url = Url::parse(&raw).map_err(|_| rejected("mcp_oauth_start_endpoint_invalid"))?;
    if url.scheme() != "https"
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || url.port_or_known_default() != Some(endpoint.endpoint.port)
    {
        return Err(rejected("mcp_oauth_start_endpoint_invalid"));
    }
    Ok(url)
}

fn map_authority_error(
    error: McpOAuthAuthorizationStartAuthorityError,
) -> McpOAuthAuthorizationStartError {
    match error {
        McpOAuthAuthorizationStartAuthorityError::NotFoundOrChanged => {
            rejected("mcp_oauth_start_not_found")
        }
        McpOAuthAuthorizationStartAuthorityError::Unavailable => {
            McpOAuthAuthorizationStartError::TemporarilyUnavailable(
                "mcp_oauth_start_authority_unavailable",
            )
        }
        McpOAuthAuthorizationStartAuthorityError::CommitUncertain => {
            McpOAuthAuthorizationStartError::CommitUncertain("mcp_oauth_start_commit_uncertain")
        }
    }
}

fn map_preparation_error(
    error: McpOAuthAuthorizationPreparationError,
) -> McpOAuthAuthorizationStartError {
    match error {
        McpOAuthAuthorizationPreparationError::Rejected => {
            rejected("mcp_oauth_start_preparation_rejected")
        }
        McpOAuthAuthorizationPreparationError::TemporarilyUnavailable => {
            McpOAuthAuthorizationStartError::TemporarilyUnavailable(
                "mcp_oauth_start_preparation_unavailable",
            )
        }
        McpOAuthAuthorizationPreparationError::WriteUncertain => {
            McpOAuthAuthorizationStartError::CommitUncertain(
                "mcp_oauth_start_preparation_uncertain",
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use chrono::Duration;
    use insight_platform_contracts::{CapabilityEndpointScheme, McpOAuthClientAuthenticationKind};
    use insight_platform_contracts::{CommandAudit, ExactSecretBindingRef, SecretResolutionPolicy};
    use std::sync::Mutex;

    fn id(value: &str) -> ResourceId {
        value.parse().unwrap()
    }

    fn sha(character: char) -> Sha256Digest {
        let hexadecimal = char::from_digit((character as u32) % 16, 16).unwrap();
        format!("sha256:{}", hexadecimal.to_string().repeat(64))
            .parse()
            .unwrap()
    }

    fn endpoint(host: &str, path: &str) -> McpOAuthEndpoint {
        let endpoint = insight_platform_contracts::CanonicalHttpEndpoint {
            scheme: CapabilityEndpointScheme::Https,
            host: host.to_owned(),
            port: 443,
            base_path: path.to_owned(),
        };
        let actual = endpoint.canonical_digest().unwrap();
        McpOAuthEndpoint {
            endpoint,
            endpoint_identity_digest: actual,
        }
    }

    fn exact_policy(profile: &McpAuthPolicyDocument) -> ExactVersionRef {
        ExactVersionRef::new(
            id("prev_0198f1c3-8f49-7c3e-b1f3-773c28367ba0"),
            profile.canonical_digest().unwrap(),
        )
        .unwrap()
    }

    fn deployment() -> ExactDeploymentRef {
        ExactDeploymentRef::new(id("mcdep_0198f1c3-8f49-7c3e-b1f3-773c28367ba1"), sha('d')).unwrap()
    }

    fn profile() -> McpAuthPolicyDocument {
        McpAuthPolicyDocument {
            schema_version: 1,
            issuer: endpoint("issuer.example", "/"),
            authorization_endpoint: endpoint("issuer.example", "/authorize"),
            token_endpoint: endpoint("issuer.example", "/token"),
            client_id: "platform-client".to_owned(),
            client_authentication: McpOAuthClientAuthenticationKind::None,
            client_credential_purpose: None,
            pkce_secret_provider_id: id("spr_0198f1c3-8f49-7c3e-b1f3-773c28367b9e"),
            token_secret_provider_id: id("spr_0198f1c3-8f49-7c3e-b1f3-773c28367b9f"),
            redirect_uri: endpoint("platform.example", "/v1/mcp/oauth/callback"),
            resource_indicator: endpoint("mcp.example", "/mcp"),
            allowed_scopes: vec!["read".to_owned(), "write".to_owned()],
            maximum_token_response_bytes: 65_536,
            connect_timeout_milliseconds: 1_000,
            total_timeout_milliseconds: 5_000,
            maximum_clock_skew_seconds: 30,
        }
    }

    fn audit(now: DateTime<Utc>) -> CommandAudit {
        CommandAudit {
            trace: insight_platform_contracts::TraceIdentityV1::generate(),
            tenant_id: id("ten_0198f1c3-8f49-7c3e-b1f3-773c28367ba2"),
            principal_id: id("prn_0198f1c3-8f49-7c3e-b1f3-773c28367ba3"),
            principal_kind: PrincipalKind::AgentRunner,
            receipt_id: id("rcp_0198f1c3-8f49-7c3e-b1f3-773c28367ba4"),
            event_id: id("evt_0198f1c3-8f49-7c3e-b1f3-773c28367ba5"),
            outbox_id: id("obx_0198f1c3-8f49-7c3e-b1f3-773c28367ba6"),
            idempotency_key_digest: sha('i'),
            request_digest: sha('r'),
            receipt_expires_at: now + Duration::minutes(20),
        }
    }

    fn intent(now: DateTime<Utc>) -> McpOAuthAuthorizationStartIntent {
        McpOAuthAuthorizationStartIntent {
            audit: audit(now),
            task_id: id("int_0198f1c3-8f49-7c3e-b1f3-773c28367ba7"),
            authorization_binding_id: id("mab_0198f1c3-8f49-7c3e-b1f3-773c28367ba8"),
            mcp_deployment: deployment(),
            expected_principal_binding_generation: 7,
            requested_scopes: vec!["read".to_owned(), "write".to_owned()],
            reauthorization: None,
            safe_prompt_key: "mcp_oauth_authorize".to_owned(),
            deadline: now + Duration::minutes(10),
        }
    }

    fn pkce_binding() -> ExactSecretBindingRef {
        ExactSecretBindingRef::build(
            id("sbd_0198f1c3-8f49-7c3e-b1f3-773c28367ba9"),
            3,
            profile().pkce_secret_provider_id,
            MCP_OAUTH_PKCE_SECRET_PURPOSE.parse().unwrap(),
            SecretResolutionPolicy::Pinned {
                opaque_version_identity_digest: sha('v'),
            },
        )
        .unwrap()
    }

    struct Authority {
        resolved: ResolvedMcpOAuthAuthorizationStart,
        command: Mutex<Option<BeginMcpOAuthAuthorization>>,
    }

    #[async_trait]
    impl McpOAuthAuthorizationStartAuthority for Authority {
        async fn resolve_authorization_start(
            &self,
            _intent: &McpOAuthAuthorizationStartIntent,
            _callback_binding_digest: &Sha256Digest,
        ) -> Result<ResolvedMcpOAuthAuthorizationStart, McpOAuthAuthorizationStartAuthorityError>
        {
            Ok(self.resolved.clone())
        }

        async fn commit_authorization_start(
            &self,
            command: BeginMcpOAuthAuthorization,
        ) -> Result<McpOAuthAuthorizationStartCommitOutcome, McpOAuthAuthorizationStartAuthorityError>
        {
            *self.command.lock().unwrap() = Some(command);
            Ok(McpOAuthAuthorizationStartCommitOutcome {
                disposition: McpOAuthAuthorizationStartCommitDisposition::Applied,
            })
        }
    }

    struct Preparation;

    #[async_trait]
    impl McpOAuthAuthorizationPreparationBroker for Preparation {
        async fn prepare_or_load(
            &self,
            request: &McpOAuthAuthorizationPreparationRequest,
            _now: DateTime<Utc>,
        ) -> Result<PreparedMcpOAuthAuthorization, McpOAuthAuthorizationPreparationError> {
            Ok(PreparedMcpOAuthAuthorization {
                preparation_digest: request.preparation_digest.clone(),
                state: SensitiveOAuthValue::from_decoded(
                    b"sealed.state".to_vec(),
                    insight_platform_mcp_host::MAX_MCP_OAUTH_STATE_BYTES,
                )
                .unwrap(),
                nonce: SensitiveMcpOAuthNonce::new(b"n".repeat(43)).unwrap(),
                pkce_challenge: "a".repeat(43),
                pkce_secret_binding: pkce_binding(),
                storage_evidence_digest: sha('s'),
            })
        }
    }

    #[tokio::test]
    async fn start_builds_canonical_url_and_commits_only_digests_and_exact_secret() {
        let now = Utc::now();
        let profile = profile();
        let callback_digest = profile.redirect_uri.endpoint_identity_digest.clone();
        let authority = Arc::new(Authority {
            resolved: ResolvedMcpOAuthAuthorizationStart {
                tenant_id: audit(now).tenant_id,
                mcp_deployment: deployment(),
                audience_identity_digest: profile
                    .resource_indicator
                    .endpoint_identity_digest
                    .clone(),
                token_credential_purpose: "mcp.oauth.token".parse().unwrap(),
                auth_policy: exact_policy(&profile),
                auth_profile: profile,
            },
            command: Mutex::new(None),
        });
        let service = McpOAuthAuthorizationStartService::new(
            McpOAuthAuthorizationStartConfig {
                callback_binding_digest: callback_digest,
            },
            authority.clone(),
            Arc::new(Preparation),
        );

        let outcome = service.start(intent(now), now).await.unwrap();
        let url = Url::parse(outcome.authorization_url.as_str()).unwrap();
        let query = url.query_pairs().collect::<Vec<_>>();
        assert_eq!(url.path(), "/authorize");
        assert_eq!(query[0], ("response_type".into(), "code".into()));
        assert!(query
            .iter()
            .any(|pair| pair == &("state".into(), "sealed.state".into())));
        assert!(query
            .iter()
            .any(|pair| pair == &("nonce".into(), "n".repeat(43).into())));
        assert!(query
            .iter()
            .any(|pair| pair == &("code_challenge_method".into(), "S256".into())));
        let command = authority.command.lock().unwrap();
        let command = command.as_ref().unwrap();
        assert_eq!(command.pkce_secret_binding, pkce_binding());
        assert_ne!(command.state_digest, sha('r'));
        assert_ne!(command.nonce_digest, sha('r'));
        let debug = format!("{outcome:?}");
        assert!(!debug.contains("sealed.state"));
        assert!(!debug.contains(&"n".repeat(43)));
    }

    #[test]
    fn intent_rejects_unbounded_lifetime_and_service_principal() {
        let now = Utc::now();
        let mut invalid = intent(now);
        invalid.deadline = now + Duration::seconds(MAX_MCP_OAUTH_STATE_LIFETIME_SECONDS + 1);
        invalid.audit.receipt_expires_at = invalid.deadline;
        assert!(invalid.validate_at(now).is_err());
        invalid = intent(now);
        invalid.audit.principal_kind = PrincipalKind::ServiceIdentity;
        assert!(invalid.validate_at(now).is_err());
    }

    #[test]
    fn prepared_values_and_url_are_redacted() {
        let prepared = PreparedMcpOAuthAuthorization {
            preparation_digest: sha('a'),
            state: SensitiveOAuthValue::from_decoded(
                b"secret-state".to_vec(),
                insight_platform_mcp_host::MAX_MCP_OAUTH_STATE_BYTES,
            )
            .unwrap(),
            nonce: SensitiveMcpOAuthNonce::new(b"n".repeat(43)).unwrap(),
            pkce_challenge: "c".repeat(43),
            pkce_secret_binding: pkce_binding(),
            storage_evidence_digest: sha('b'),
        };
        let debug = format!("{prepared:?}");
        assert!(!debug.contains("secret-state"));
        assert!(!debug.contains(&"n".repeat(43)));
        assert!(!debug.contains(&"c".repeat(43)));
        let url = SensitiveMcpOAuthAuthorizationUrl::new(
            "https://issuer.example/authorize?state=secret-state".to_owned(),
        )
        .unwrap();
        assert!(!format!("{url:?}").contains("secret-state"));
    }
}
