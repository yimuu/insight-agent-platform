use async_trait::async_trait;
use chrono::{DateTime, Utc};
use insight_platform_contracts::{
    CanonicalHttpEndpoint, DataClassification, DataRegion, ExactDeploymentRef,
    ExactSecretBindingRef, ExactVersionRef, ResourceId, ResourceKind, Sha256Digest, ValueRef,
};
use serde::{Deserialize, Serialize};
use std::{error::Error, fmt};

pub const REMOTE_CONTEXT_PROTOCOL_VERSION: u32 = 1;
pub const MAX_REMOTE_CONTEXT_ITEMS: usize = 1_000;
pub const MAX_REMOTE_CONTEXT_PROJECTION_FIELDS: usize = 256;
pub const MAX_REMOTE_CONTEXT_LABEL_BYTES: usize = 256;
pub const MAX_REMOTE_CONTEXT_FAILURE_CODE_BYTES: usize = 128;
pub const MAX_REMOTE_CONTEXT_SAFE_MESSAGE_BYTES: usize = 1_024;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RemoteContextSearchRequest {
    pub schema_version: u32,
    pub tenant_id: ResourceId,
    pub context_query_id: ResourceId,
    pub job_id: ResourceId,
    pub physical_attempt: u32,
    pub lease_generation: u64,
    pub context_deployment: ExactDeploymentRef,
    pub implementation_revision: ExactVersionRef,
    pub protocol_contract_digest: Sha256Digest,
    pub result_mapping_digest: Sha256Digest,
    pub endpoint: CanonicalHttpEndpoint,
    pub endpoint_identity_digest: Sha256Digest,
    pub region: DataRegion,
    pub secret_bindings: Vec<ExactSecretBindingRef>,
    pub network_policy: ExactVersionRef,
    pub tls_policy: ExactVersionRef,
    pub trust_policy: ExactVersionRef,
    pub query_input: ValueRef,
    pub normalized_query_digest: Sha256Digest,
    pub normalized_filter_digest: Sha256Digest,
    pub requested_projection: Vec<String>,
    pub maximum_classification: DataClassification,
    pub page_size: u32,
    pub cursor_digest: Option<Sha256Digest>,
    pub maximum_response_bytes: u32,
    pub deadline: DateTime<Utc>,
}

impl RemoteContextSearchRequest {
    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), RemoteContextContractError> {
        self.context_deployment
            .validate()
            .map_err(|_| RemoteContextContractError::InvalidRequest)?;
        self.implementation_revision
            .validate()
            .map_err(|_| RemoteContextContractError::InvalidRequest)?;
        self.endpoint
            .validate()
            .map_err(|_| RemoteContextContractError::InvalidRequest)?;
        if self.schema_version != REMOTE_CONTEXT_PROTOCOL_VERSION
            || self.tenant_id.kind() != ResourceKind::Tenant
            || self.context_query_id.kind() != ResourceKind::ContextQuery
            || self.job_id.kind() != ResourceKind::Job
            || self.physical_attempt == 0
            || self.lease_generation == 0
            || self.context_deployment.resource_kind != ResourceKind::ContextDeployment
            || self.implementation_revision.resource_kind
                != ResourceKind::ContextSourceImplementationRevision
            || self.endpoint.canonical_digest().as_ref() != Ok(&self.endpoint_identity_digest)
            || self.secret_bindings.len() > 16
            || !self
                .secret_bindings
                .windows(2)
                .all(|pair| pair[0].purpose < pair[1].purpose)
            || self
                .secret_bindings
                .iter()
                .any(|binding| binding.validate().is_err())
            || !exact_transport_policies_are_distinct(self)
            || !matches!(self.query_input, ValueRef::Inline { .. })
            || self.requested_projection.len() > MAX_REMOTE_CONTEXT_PROJECTION_FIELDS
            || !self
                .requested_projection
                .windows(2)
                .all(|pair| pair[0] < pair[1])
            || self.page_size == 0
            || usize::try_from(self.page_size).map_or(true, |size| size > MAX_REMOTE_CONTEXT_ITEMS)
            || self.maximum_response_bytes == 0
            || self.maximum_response_bytes > 64 * 1_048_576
            || self.deadline <= now
        {
            return Err(RemoteContextContractError::InvalidRequest);
        }
        Ok(())
    }
}

fn exact_transport_policies_are_distinct(request: &RemoteContextSearchRequest) -> bool {
    let policies = [
        &request.network_policy,
        &request.tls_policy,
        &request.trust_policy,
    ];
    policies.iter().all(|policy| {
        policy.validate().is_ok() && policy.resource_kind == ResourceKind::PolicyRevision
    }) && policies
        .iter()
        .enumerate()
        .all(|(index, policy)| policies[..index].iter().all(|prior| prior != policy))
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RemoteContextItem {
    pub source_item_identity_digest: Sha256Digest,
    pub content: serde_json::Value,
    pub structured_fields: serde_json::Value,
    pub score_millionths: Option<i32>,
    pub locator_digest: Sha256Digest,
    pub authorization_evidence_digest: Sha256Digest,
    pub display_label: String,
    pub classification: DataClassification,
}

impl RemoteContextItem {
    pub fn validate(&self) -> Result<(), RemoteContextContractError> {
        if self.display_label.is_empty()
            || self.display_label.len() > MAX_REMOTE_CONTEXT_LABEL_BYTES
            || self.display_label.chars().any(char::is_control)
            || self
                .score_millionths
                .is_some_and(|score| !(-1_000_000..=1_000_000).contains(&score))
            || !self.structured_fields.is_object()
        {
            return Err(RemoteContextContractError::InvalidResponse);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RemoteContextSearchResponse {
    pub schema_version: u32,
    pub items: Vec<RemoteContextItem>,
    pub next_cursor_digest: Option<Sha256Digest>,
    pub backend_request_digest: Sha256Digest,
    pub backend_response_digest: Sha256Digest,
    pub ranking_evidence_digest: Sha256Digest,
    pub remote_revision_digest: Option<Sha256Digest>,
    pub observed_at: DateTime<Utc>,
}

impl RemoteContextSearchResponse {
    pub fn validate_for(
        &self,
        request: &RemoteContextSearchRequest,
        now: DateTime<Utc>,
    ) -> Result<(), RemoteContextContractError> {
        if self.schema_version != REMOTE_CONTEXT_PROTOCOL_VERSION
            || self.items.len() > usize::try_from(request.page_size).unwrap_or(usize::MAX)
            || self.items.len() > MAX_REMOTE_CONTEXT_ITEMS
            || self.items.iter().any(|item| item.validate().is_err())
            || self.backend_request_digest != request.normalized_query_digest
            || self.observed_at > now
            || self.observed_at > request.deadline
        {
            return Err(RemoteContextContractError::InvalidResponse);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RemoteContextFailureClass {
    RejectedBeforeDispatch,
    RetryableBeforeDispatch,
    RetryableAfterDispatch,
    PermanentAfterDispatch,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RemoteContextFailure {
    pub code: String,
    pub class: RemoteContextFailureClass,
    pub safe_message: String,
    pub dispatch_evidence_digest: Option<Sha256Digest>,
}

impl RemoteContextFailure {
    pub fn validate(&self) -> Result<(), RemoteContextContractError> {
        let code_valid = !self.code.is_empty()
            && self.code.len() <= MAX_REMOTE_CONTEXT_FAILURE_CODE_BYTES
            && self
                .code
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_');
        let dispatched = matches!(
            self.class,
            RemoteContextFailureClass::RetryableAfterDispatch
                | RemoteContextFailureClass::PermanentAfterDispatch
        );
        if !code_valid
            || self.safe_message.is_empty()
            || self.safe_message.len() > MAX_REMOTE_CONTEXT_SAFE_MESSAGE_BYTES
            || self.safe_message.chars().any(char::is_control)
            || dispatched != self.dispatch_evidence_digest.is_some()
        {
            return Err(RemoteContextContractError::InvalidFailure);
        }
        Ok(())
    }
}

#[async_trait]
pub trait RemoteContextSearchConnector: Send + Sync {
    async fn query(
        &self,
        request: RemoteContextSearchRequest,
    ) -> Result<RemoteContextSearchResponse, RemoteContextFailure>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RemoteContextContractError {
    InvalidRequest,
    InvalidResponse,
    InvalidFailure,
}

impl fmt::Display for RemoteContextContractError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidRequest => "remote Context request is invalid",
            Self::InvalidResponse => "remote Context response is invalid",
            Self::InvalidFailure => "remote Context failure is invalid",
        })
    }
}

impl Error for RemoteContextContractError {}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;
    use insight_platform_contracts::{
        CapabilityEndpointScheme, ExactVersionRef, ResourceId, ResourceKind,
    };

    fn id(kind: ResourceKind, suffix: u16) -> ResourceId {
        format!(
            "{}_0198f1c9-32e4-75e1-a9e8-d95ca0f6{suffix:04x}",
            kind.descriptor().prefix
        )
        .parse()
        .unwrap()
    }

    fn digest(marker: char) -> Sha256Digest {
        format!("sha256:{}", marker.to_string().repeat(64))
            .parse()
            .unwrap()
    }

    fn exact(kind: ResourceKind, suffix: u16, marker: char) -> ExactVersionRef {
        ExactVersionRef::new(id(kind, suffix), digest(marker)).unwrap()
    }

    fn request() -> RemoteContextSearchRequest {
        let endpoint = CanonicalHttpEndpoint {
            scheme: CapabilityEndpointScheme::Https,
            host: "search.example.test".to_owned(),
            port: 443,
            base_path: "/v1/query".to_owned(),
        };
        RemoteContextSearchRequest {
            schema_version: REMOTE_CONTEXT_PROTOCOL_VERSION,
            tenant_id: id(ResourceKind::Tenant, 1),
            context_query_id: id(ResourceKind::ContextQuery, 2),
            job_id: id(ResourceKind::Job, 3),
            physical_attempt: 1,
            lease_generation: 1,
            context_deployment: ExactDeploymentRef::new(
                id(ResourceKind::ContextDeployment, 4),
                digest('4'),
            )
            .unwrap(),
            implementation_revision: exact(
                ResourceKind::ContextSourceImplementationRevision,
                5,
                '5',
            ),
            protocol_contract_digest: digest('6'),
            result_mapping_digest: digest('7'),
            endpoint_identity_digest: endpoint.canonical_digest().unwrap(),
            endpoint,
            region: "cn-east-1".parse().unwrap(),
            secret_bindings: vec![],
            network_policy: exact(ResourceKind::PolicyRevision, 8, '8'),
            tls_policy: exact(ResourceKind::PolicyRevision, 9, '9'),
            trust_policy: exact(ResourceKind::PolicyRevision, 10, 'a'),
            query_input: ValueRef::Inline {
                value: serde_json::json!({"query": "bounded"}),
            },
            normalized_query_digest: digest('b'),
            normalized_filter_digest: digest('c'),
            requested_projection: vec!["title".to_owned()],
            maximum_classification: DataClassification::Confidential,
            page_size: 10,
            cursor_digest: None,
            maximum_response_bytes: 65_536,
            deadline: Utc::now() + Duration::minutes(1),
        }
    }

    #[test]
    fn remote_request_and_response_are_exact_and_fail_closed() {
        let request = request();
        request.validate_at(Utc::now()).unwrap();
        let response = RemoteContextSearchResponse {
            schema_version: REMOTE_CONTEXT_PROTOCOL_VERSION,
            items: vec![],
            next_cursor_digest: None,
            backend_request_digest: request.normalized_query_digest.clone(),
            backend_response_digest: digest('d'),
            ranking_evidence_digest: digest('e'),
            remote_revision_digest: None,
            observed_at: Utc::now(),
        };
        response.validate_for(&request, Utc::now()).unwrap();

        let mut wrong_endpoint = request.clone();
        wrong_endpoint.endpoint_identity_digest = digest('f');
        assert_eq!(
            wrong_endpoint.validate_at(Utc::now()),
            Err(RemoteContextContractError::InvalidRequest)
        );
        let mut wrong_response = response;
        wrong_response.backend_request_digest = digest('0');
        assert_eq!(
            wrong_response.validate_for(&request, Utc::now()),
            Err(RemoteContextContractError::InvalidResponse)
        );
    }
}
