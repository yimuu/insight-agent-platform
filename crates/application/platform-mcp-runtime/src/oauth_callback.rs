use chrono::{DateTime, Duration, Utc};
use insight_platform_contracts::{
    ResourceId, ResourceKind, Sha256Digest, TraceFlags, TraceIdentityV1,
};
use insight_platform_execution_context::{scope_trace, ExecutionTraceContext};

use std::sync::Arc;
use uuid::Uuid;

use insight_platform_mcp_host::*;

/// Production UUIDv7 allocator for callback Receipt/Event/Outbox row identities. Callback replay
/// remains governed by the request-bound Receipt unique key; these values do not become a second
/// idempotency authority.
#[derive(Debug, Clone, Copy, Default)]
pub struct UuidMcpOAuthCallbackIdentityFactory;

impl UuidMcpOAuthCallbackIdentityFactory {
    fn next_id(kind: ResourceKind) -> Result<ResourceId, McpOAuthCallbackAuthorityError> {
        ResourceId::from_uuid_v7(kind, Uuid::now_v7())
            .map_err(|_| McpOAuthCallbackAuthorityError::Unavailable)
    }
}

impl McpOAuthCallbackIdentityFactory for UuidMcpOAuthCallbackIdentityFactory {
    fn next_ids(
        &self,
        _tenant_id: &ResourceId,
        _task_id: &ResourceId,
        _request_digest: &Sha256Digest,
    ) -> Result<McpOAuthCallbackCommitIds, McpOAuthCallbackAuthorityError> {
        Ok(McpOAuthCallbackCommitIds {
            receipt_id: Self::next_id(ResourceKind::Receipt)?,
            event_id: Self::next_id(ResourceKind::Event)?,
            outbox_id: Self::next_id(ResourceKind::OutboxEvent)?,
        })
    }
}

pub struct McpOAuthCallbackIngress {
    config: McpOAuthCallbackIngressConfig,
    states: Arc<dyn McpOAuthStateAuthenticator>,
    identities: Arc<dyn McpOAuthCallbackIdentityFactory>,
    authority: Arc<dyn McpOAuthCallbackAuthority>,
    broker: Arc<dyn McpOAuthCredentialBroker>,
}

impl McpOAuthCallbackIngress {
    pub fn new(
        config: McpOAuthCallbackIngressConfig,
        states: Arc<dyn McpOAuthStateAuthenticator>,
        identities: Arc<dyn McpOAuthCallbackIdentityFactory>,
        authority: Arc<dyn McpOAuthCallbackAuthority>,
        broker: Arc<dyn McpOAuthCredentialBroker>,
    ) -> Result<Self, McpOAuthCallbackError> {
        config.validate()?;
        Ok(Self {
            config,
            states,
            identities,
            authority,
            broker,
        })
    }

    pub async fn handle_query(
        &self,
        raw_query: &[u8],
        now: DateTime<Utc>,
    ) -> Result<McpOAuthCallbackIngressOutcome, McpOAuthCallbackError> {
        let trace = TraceIdentityV1::generate();
        let parsed = parse_mcp_oauth_callback_query(raw_query)?;
        let state_digest = mcp_oauth_state_digest(&parsed.state)?;
        let identity = self
            .states
            .authenticate(&parsed.state, now)
            .await
            .map_err(map_authority_error)?;
        identity.validate()?;
        let contract = self
            .authority
            .resolve_exchange_contract(&identity)
            .await
            .map_err(map_authority_error)?;
        contract.validate_at(now)?;
        if contract.tenant_id != identity.tenant_id
            || contract.task_id != identity.task_id
            || contract.binding.state_digest != state_digest
            || contract.binding.callback_binding_digest != self.config.callback_binding_digest
            || parsed.issuer.as_ref().is_some_and(|issuer| {
                mcp_oauth_issuer_identity_digest(issuer).ok().as_ref()
                    != Some(&contract.auth_profile.issuer.endpoint_identity_digest)
            })
        {
            return Err(McpOAuthCallbackError::Rejected(
                "mcp_oauth_callback_binding_mismatch",
            ));
        }

        let (resolution, wire_outcome_digest) = match parsed.outcome {
            McpOAuthCallbackWireOutcome::AuthorizationCode(code) => {
                let code_digest = sensitive_digest("mcp_oauth_code_v1", &code)?;
                let rpc_trace = ExecutionTraceContext::start(trace, TraceFlags::NotSampled)
                    .map_err(|_| McpOAuthCallbackError::Rejected("mcp_oauth_trace_invalid"))?;
                let grant = scope_trace(
                    rpc_trace,
                    self.broker
                        .exchange_authorization_code(&contract, code, now),
                )
                .await
                .map_err(map_broker_error)?;
                grant
                    .validate_for_binding(&contract.binding, now)
                    .map_err(|_| McpOAuthCallbackError::Rejected("mcp_oauth_grant_invalid"))?;
                (
                    McpOAuthCallbackResolution::Authorized(Box::new(grant)),
                    code_digest,
                )
            }
            McpOAuthCallbackWireOutcome::Declined { safe_reason_code } => {
                let evidence_digest = digest(&serde_json::json!({
                    "domain": "mcp_oauth_callback_declined",
                    "safe_reason_code": safe_reason_code,
                    "schema_version": 1,
                }))
                .map_err(|_| McpOAuthCallbackError::Rejected("mcp_oauth_callback_digest_failed"))?;
                (
                    McpOAuthCallbackResolution::Declined {
                        safe_reason_code: safe_reason_code.to_owned(),
                        evidence_digest: evidence_digest.clone(),
                    },
                    evidence_digest,
                )
            }
        };
        let request_digest = digest(&serde_json::json!({
            "callback_binding_digest": self.config.callback_binding_digest,
            "state_digest": state_digest,
            "task_id": contract.task_id,
            "wire_outcome_digest": wire_outcome_digest,
        }))
        .map_err(|_| McpOAuthCallbackError::Rejected("mcp_oauth_callback_digest_failed"))?;
        let idempotency_key_digest = digest(&serde_json::json!({
            "domain": "mcp_oauth_callback_idempotency_v1",
            "request_digest": request_digest,
        }))
        .map_err(|_| McpOAuthCallbackError::Rejected("mcp_oauth_callback_digest_failed"))?;
        let ids = self
            .identities
            .next_ids(&contract.tenant_id, &contract.task_id, &request_digest)
            .map_err(map_authority_error)?;
        ids.validate()?;
        let receipt_expires_at = now
            .checked_add_signed(Duration::seconds(self.config.receipt_ttl_seconds))
            .ok_or(McpOAuthCallbackError::Rejected(
                "mcp_oauth_callback_configuration_invalid",
            ))?;
        let command = CompleteMcpOAuthCallback {
            audit: McpOAuthCallbackAudit {
                trace,
                tenant_id: contract.tenant_id,
                callback_ingress_generation_id: self.config.callback_ingress_generation_id.clone(),
                receipt_id: ids.receipt_id,
                event_id: ids.event_id,
                outbox_id: ids.outbox_id,
                idempotency_key_digest,
                request_digest,
                callback_binding_digest: self.config.callback_binding_digest.clone(),
                receipt_expires_at,
            },
            task_id: contract.task_id,
            authorization_binding_id: contract.binding.authorization_binding_id.clone(),
            expected_task_generation: contract.task_generation,
            expected_task_version: contract.task_version,
            state_digest,
            resolution,
        };
        let outcome = self
            .authority
            .commit_callback(command)
            .await
            .map_err(map_authority_error)?;
        Ok(McpOAuthCallbackIngressOutcome {
            disposition: outcome.disposition,
            no_store: true,
        })
    }
}

fn map_authority_error(error: McpOAuthCallbackAuthorityError) -> McpOAuthCallbackError {
    match error {
        McpOAuthCallbackAuthorityError::NotFoundOrChanged => {
            McpOAuthCallbackError::Rejected("mcp_oauth_callback_not_found")
        }
        McpOAuthCallbackAuthorityError::Unavailable => {
            McpOAuthCallbackError::TemporarilyUnavailable(
                "mcp_oauth_callback_authority_unavailable",
            )
        }
        McpOAuthCallbackAuthorityError::CommitUncertain => {
            McpOAuthCallbackError::CommitUncertain("mcp_oauth_callback_commit_uncertain")
        }
    }
}

fn map_broker_error(error: McpOAuthCredentialBrokerError) -> McpOAuthCallbackError {
    match error {
        McpOAuthCredentialBrokerError::Rejected => {
            McpOAuthCallbackError::Rejected("mcp_oauth_exchange_rejected")
        }
        McpOAuthCredentialBrokerError::TemporarilyUnavailable => {
            McpOAuthCallbackError::TemporarilyUnavailable("mcp_oauth_exchange_unavailable")
        }
        McpOAuthCredentialBrokerError::ExchangeUncertain => {
            McpOAuthCallbackError::CommitUncertain("mcp_oauth_exchange_uncertain")
        }
    }
}
