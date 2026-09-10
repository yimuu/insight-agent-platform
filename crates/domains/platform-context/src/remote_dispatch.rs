//! One projection from a frozen admission to Remote Context transport and authorization.
use crate::{ContextAdmissionSnapshot, RemoteContextContractError, RemoteContextSearchRequest};
use insight_platform_contracts::{
    canonical_digest, ContextBackendBinding, ContextBackendContract,
    ContextDispatchAuthorizationV1, ResourceId, Sha256Digest, ValueRef,
    REMOTE_CONTEXT_EXECUTION_SCHEMA_VERSION,
};
use insight_platform_jobs::JobFence;

impl RemoteContextSearchRequest {
    pub fn from_admission(
        tenant_id: ResourceId,
        job_id: ResourceId,
        physical_attempt: u32,
        fence: &JobFence,
        admission: &ContextAdmissionSnapshot,
        query_input: ValueRef,
    ) -> Result<Self, RemoteContextContractError> {
        let ContextBackendContract::RemoteSearch {
            protocol_contract_digest,
            result_mapping_digest,
        } = &admission.implementation.contract.backend
        else {
            return Err(RemoteContextContractError::InvalidRequest);
        };
        let ContextBackendBinding::RemoteSearch {
            endpoint,
            endpoint_identity_digest,
            region,
        } = &admission.context_closure.backend
        else {
            return Err(RemoteContextContractError::InvalidRequest);
        };
        let closure = &admission.context_closure;
        Ok(Self {
            schema_version: REMOTE_CONTEXT_EXECUTION_SCHEMA_VERSION,
            tenant_id,
            context_query_id: admission.context_query_id.clone(),
            job_id,
            worker_process_generation_id: fence.worker_process_generation_id.clone(),
            physical_attempt,
            lease_generation: fence.lease_generation,
            lease_token_digest: fence.token_digest.clone(),
            admission_digest: admission.canonical_digest.clone(),
            context_deployment: admission.binding.context_deployment.clone(),
            implementation_revision: admission.implementation_revision.clone(),
            protocol_contract_digest: protocol_contract_digest.clone(),
            result_mapping_digest: result_mapping_digest.clone(),
            endpoint: endpoint.clone(),
            endpoint_identity_digest: endpoint_identity_digest.clone(),
            region: region.clone(),
            secret_bindings: closure.secret_bindings.clone(),
            network_policy: closure
                .network_policy
                .clone()
                .ok_or(RemoteContextContractError::InvalidRequest)?,
            tls_policy: closure
                .tls_policy
                .clone()
                .ok_or(RemoteContextContractError::InvalidRequest)?,
            trust_policy: closure
                .trust_policy
                .clone()
                .ok_or(RemoteContextContractError::InvalidRequest)?,
            query_input,
            normalized_query_digest: admission.request.normalized_query_digest.clone(),
            normalized_filter_digest: admission.request.normalized_filter_digest.clone(),
            requested_projection: admission.request.requested_projection.clone(),
            maximum_classification: admission.grant.maximum_classification,
            page_size: admission.request.page_size,
            cursor_digest: admission.request.cursor_digest.clone(),
            maximum_request_bytes: admission
                .implementation
                .contract
                .limits
                .maximum_request_bytes,
            maximum_response_bytes: admission
                .implementation
                .contract
                .limits
                .maximum_response_bytes,
            deadline: admission.deadline,
        })
    }

    /// Hash every owned transport field except the separately bound input body.
    pub fn metadata_digest(&self) -> Result<Sha256Digest, RemoteContextContractError> {
        let mut value =
            serde_json::to_value(self).map_err(|_| RemoteContextContractError::InvalidRequest)?;
        value
            .as_object_mut()
            .ok_or(RemoteContextContractError::InvalidRequest)?
            .remove("query_input")
            .ok_or(RemoteContextContractError::InvalidRequest)?;
        canonical_digest(&value)
            .map_err(|_| RemoteContextContractError::InvalidRequest)?
            .parse()
            .map_err(|_| RemoteContextContractError::InvalidRequest)
    }

    pub fn dispatch_authorization(
        &self,
    ) -> Result<ContextDispatchAuthorizationV1, RemoteContextContractError> {
        let ValueRef::Inline { value } = &self.query_input else {
            return Err(RemoteContextContractError::InvalidRequest);
        };
        let input_content_digest = canonical_digest(value)
            .map_err(|_| RemoteContextContractError::InvalidRequest)?
            .parse()
            .map_err(|_| RemoteContextContractError::InvalidRequest)?;
        Ok(ContextDispatchAuthorizationV1 {
            schema_version: 1,
            tenant_id: self.tenant_id.clone(),
            context_query_id: self.context_query_id.clone(),
            job_id: self.job_id.clone(),
            worker_process_generation_id: self.worker_process_generation_id.clone(),
            physical_attempt: self.physical_attempt,
            lease_generation: self.lease_generation,
            lease_token_digest: self.lease_token_digest.clone(),
            admission_digest: self.admission_digest.clone(),
            request_metadata_digest: self.metadata_digest()?,
            input_content_digest,
            deadline: self.deadline,
        })
    }
}
