use crate::{ScopedArtifactGrant, WasiArtifactReadPurpose, WasiArtifactReadRequest};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use insight_platform_contracts::{
    canonical_digest, ArtifactGrantOperation, ArtifactRef, ResourceId, ResourceKind,
    SandboxEntrypointKind, SandboxRuntimeFamily, Sha256Digest, ValueRef,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

const MAX_KUBERNETES_NAME_BYTES: usize = 253;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GvisorGuestPodIdentity {
    pub namespace: String,
    pub pod_name: String,
    pub pod_uid: String,
    pub service_account_name: String,
}

impl GvisorGuestPodIdentity {
    pub fn validate(&self) -> Result<(), GvisorGuestBootstrapError> {
        if !dns_name(&self.namespace)
            || !dns_name(&self.pod_name)
            || !dns_name(&self.service_account_name)
            || self.pod_uid.parse::<uuid::Uuid>().is_err()
        {
            return Err(GvisorGuestBootstrapError::Denied);
        }
        Ok(())
    }

    pub fn sandbox_identity_digest(
        &self,
        request_digest: &Sha256Digest,
        worker_process_generation_id: &ResourceId,
    ) -> Result<Sha256Digest, GvisorGuestBootstrapError> {
        self.validate()?;
        if worker_process_generation_id.kind() != ResourceKind::WorkerProcessGeneration {
            return Err(GvisorGuestBootstrapError::Denied);
        }
        gvisor_pod_identity_digest(
            &self.namespace,
            &self.pod_name,
            &self.pod_uid,
            request_digest,
            worker_process_generation_id,
        )
    }
}

pub fn gvisor_pod_identity_digest(
    namespace: &str,
    pod_name: &str,
    pod_uid: &str,
    request_digest: &Sha256Digest,
    worker_process_generation_id: &ResourceId,
) -> Result<Sha256Digest, GvisorGuestBootstrapError> {
    if !dns_name(namespace)
        || !dns_name(pod_name)
        || pod_uid.parse::<uuid::Uuid>().is_err()
        || worker_process_generation_id.kind() != ResourceKind::WorkerProcessGeneration
    {
        return Err(GvisorGuestBootstrapError::InvalidEvidence);
    }
    canonical_digest(&serde_json::json!({
        "schema_version": 1,
        "kind": "gvisor_pod_identity",
        "namespace": namespace,
        "name": pod_name,
        "uid": pod_uid,
        "request_digest": request_digest,
        "worker_process_generation_id": worker_process_generation_id,
    }))
    .map_err(|_| GvisorGuestBootstrapError::InvalidEvidence)?
    .parse()
    .map_err(|_| GvisorGuestBootstrapError::InvalidEvidence)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GvisorGuestBootstrapRequest {
    pub schema_version: u32,
    pub tenant_id: ResourceId,
    pub sandbox_job_id: ResourceId,
    pub request_digest: Sha256Digest,
    pub pod: GvisorGuestPodIdentity,
}

impl GvisorGuestBootstrapRequest {
    pub fn validate(&self) -> Result<(), GvisorGuestBootstrapError> {
        self.pod.validate()?;
        if self.schema_version != 1
            || self.tenant_id.kind() != ResourceKind::Tenant
            || self.sandbox_job_id.kind() != ResourceKind::Job
        {
            return Err(GvisorGuestBootstrapError::Denied);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GvisorGuestExecutionPlan {
    pub schema_version: u32,
    pub request_digest: Sha256Digest,
    pub runtime_family: SandboxRuntimeFamily,
    pub entrypoint_kind: SandboxEntrypointKind,
    pub entrypoint: String,
    pub runtime_bundle_artifact: ArtifactRef,
    pub input_ref: ValueRef,
    pub resources: crate::SandboxResourceEnvelope,
    pub deadline: DateTime<Utc>,
    pub plan_digest: Sha256Digest,
}

impl GvisorGuestExecutionPlan {
    pub fn seal(mut self) -> Result<Self, GvisorGuestBootstrapError> {
        self.plan_digest = self.digest_without_plan_digest()?;
        Ok(self)
    }

    pub fn validate(&self) -> Result<(), GvisorGuestBootstrapError> {
        let entrypoint_shape_valid = match (self.runtime_family, self.entrypoint_kind) {
            (SandboxRuntimeFamily::Python, SandboxEntrypointKind::PythonModule) => self
                .entrypoint
                .split('.')
                .all(|part| valid_language_identifier(part)),
            (SandboxRuntimeFamily::NodeJs, SandboxEntrypointKind::NodeModule)
            | (SandboxRuntimeFamily::ReviewedShell, SandboxEntrypointKind::ReviewedExecutable) => {
                self.entrypoint.split('/').all(valid_path_part)
            }
            _ => false,
        };
        if self.schema_version != 1
            || self.entrypoint.is_empty()
            || self.entrypoint.len() > 1_024
            || self.entrypoint.starts_with('/')
            || !entrypoint_shape_valid
            || self.runtime_bundle_artifact.validate().is_err()
            || self.deadline <= Utc::now()
            || self.digest_without_plan_digest()? != self.plan_digest
        {
            return Err(GvisorGuestBootstrapError::InvalidEvidence);
        }
        Ok(())
    }

    fn digest_without_plan_digest(&self) -> Result<Sha256Digest, GvisorGuestBootstrapError> {
        let mut value =
            serde_json::to_value(self).map_err(|_| GvisorGuestBootstrapError::InvalidEvidence)?;
        value
            .as_object_mut()
            .ok_or(GvisorGuestBootstrapError::InvalidEvidence)?
            .remove("plan_digest");
        canonical_digest(&value)
            .map_err(|_| GvisorGuestBootstrapError::InvalidEvidence)?
            .parse()
            .map_err(|_| GvisorGuestBootstrapError::InvalidEvidence)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct AuthorizedGvisorGuestBootstrap {
    pub plan: GvisorGuestExecutionPlan,
    pub package_read: WasiArtifactReadRequest,
    pub input_read: Option<WasiArtifactReadRequest>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GvisorGuestBootstrapError {
    Unavailable,
    Denied,
    NotFound,
    InvalidEvidence,
}

#[async_trait]
pub trait GvisorGuestBootstrapAuthority: Send + Sync {
    async fn authorize_guest_bootstrap(
        &self,
        request: &GvisorGuestBootstrapRequest,
    ) -> Result<AuthorizedGvisorGuestBootstrap, GvisorGuestBootstrapError>;
}

pub fn gvisor_guest_pod_name(
    request_digest: &Sha256Digest,
    attempt_no: u32,
    lease_generation: u64,
    worker_process_generation_id: &ResourceId,
) -> Result<String, GvisorGuestBootstrapError> {
    if attempt_no == 0
        || lease_generation == 0
        || worker_process_generation_id.kind() != ResourceKind::WorkerProcessGeneration
    {
        return Err(GvisorGuestBootstrapError::InvalidEvidence);
    }
    let digest = Sha256::digest(format!(
        "{request_digest}:{attempt_no}:{lease_generation}:{worker_process_generation_id}"
    ));
    Ok(format!("insight-gv-{}", lower_hex(&digest[..16])))
}

pub fn guest_read_requests(
    plan: &GvisorGuestExecutionPlan,
    tenant_id: &ResourceId,
    sandbox_job_id: &ResourceId,
    worker_process_generation_id: &ResourceId,
    lease_generation: u64,
    artifact_grants: &[ScopedArtifactGrant],
) -> Result<(WasiArtifactReadRequest, Option<WasiArtifactReadRequest>), GvisorGuestBootstrapError> {
    let maximum_package = usize::try_from(plan.runtime_bundle_artifact.byte_length())
        .map_err(|_| GvisorGuestBootstrapError::InvalidEvidence)?;
    let package = WasiArtifactReadRequest {
        tenant_id: tenant_id.clone(),
        sandbox_job_id: sandbox_job_id.clone(),
        request_digest: plan.request_digest.clone(),
        worker_process_generation_id: worker_process_generation_id.clone(),
        lease_generation,
        artifact: plan.runtime_bundle_artifact.clone(),
        purpose: WasiArtifactReadPurpose::RuntimeBundle,
        read_grant: None,
        maximum_bytes: maximum_package,
        deadline: plan.deadline,
    };
    let input = match &plan.input_ref {
        ValueRef::Inline { .. } => None,
        ValueRef::Artifact { artifact } => {
            let grant = artifact_grants
                .iter()
                .find(|grant| {
                    grant.operation == ArtifactGrantOperation::ReadWhole
                        && grant.artifact.as_ref() == Some(artifact)
                        && grant.byte_range.is_none()
                        && grant.maximum_bytes >= artifact.byte_length()
                })
                .cloned()
                .ok_or(GvisorGuestBootstrapError::Denied)?;
            Some(WasiArtifactReadRequest {
                tenant_id: tenant_id.clone(),
                sandbox_job_id: sandbox_job_id.clone(),
                request_digest: plan.request_digest.clone(),
                worker_process_generation_id: worker_process_generation_id.clone(),
                lease_generation,
                artifact: artifact.clone(),
                purpose: WasiArtifactReadPurpose::InputValue,
                read_grant: Some(grant),
                maximum_bytes: usize::try_from(artifact.byte_length())
                    .map_err(|_| GvisorGuestBootstrapError::InvalidEvidence)?,
                deadline: plan.deadline,
            })
        }
    };
    Ok((package, input))
}

fn dns_name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_KUBERNETES_NAME_BYTES
        && value.split('.').all(|label| {
            !label.is_empty()
                && label.len() <= 63
                && label.bytes().enumerate().all(|(index, byte)| match byte {
                    b'a'..=b'z' | b'0'..=b'9' => true,
                    b'-' => index > 0 && index + 1 < label.len(),
                    _ => false,
                })
        })
}

fn valid_language_identifier(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value.bytes().enumerate().all(|(index, byte)| {
            byte == b'_' || byte.is_ascii_alphabetic() || (index > 0 && byte.is_ascii_digit())
        })
}

fn valid_path_part(value: &str) -> bool {
    !value.is_empty()
        && value != "."
        && value != ".."
        && value.len() <= 255
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
}

fn lower_hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut value = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        value.push(char::from(DIGITS[usize::from(byte >> 4)]));
        value.push(char::from(DIGITS[usize::from(byte & 0x0f)]));
    }
    value
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    fn id(kind: ResourceKind) -> ResourceId {
        ResourceId::from_uuid_v7(kind, Uuid::now_v7()).unwrap()
    }

    fn digest(character: char) -> Sha256Digest {
        format!("sha256:{}", character.to_string().repeat(64))
            .parse()
            .unwrap()
    }

    #[test]
    fn pod_name_and_identity_are_fence_bound_and_closed() {
        let worker = id(ResourceKind::WorkerProcessGeneration);
        let request = digest('a');
        let name = gvisor_guest_pod_name(&request, 2, 7, &worker).unwrap();
        assert!(name.starts_with("insight-gv-"));
        assert_eq!(name.len(), "insight-gv-".len() + 32);
        assert_ne!(
            name,
            gvisor_guest_pod_name(&request, 2, 8, &worker).unwrap()
        );

        let identity = GvisorGuestPodIdentity {
            namespace: "platform-sandbox-guests".to_owned(),
            pod_name: name,
            pod_uid: Uuid::now_v7().to_string(),
            service_account_name: "sandbox-guest".to_owned(),
        };
        let first = identity.sandbox_identity_digest(&request, &worker).unwrap();
        let mut changed = identity;
        changed.pod_uid = Uuid::now_v7().to_string();
        assert_ne!(
            first,
            changed.sandbox_identity_digest(&request, &worker).unwrap()
        );
        changed.namespace = "Platform_Guests".to_owned();
        assert_eq!(changed.validate(), Err(GvisorGuestBootstrapError::Denied));
    }
}
