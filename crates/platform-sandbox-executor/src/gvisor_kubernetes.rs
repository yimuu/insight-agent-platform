use async_trait::async_trait;
use insight_platform_contracts::{canonical_digest, ResourceId, ResourceKind, Sha256Digest};
use insight_platform_sandbox::{SandboxExecutionRequest, SandboxResourceEnvelope};
use k8s_openapi::{
    api::core::v1::{
        Capabilities, Container, EmptyDirVolumeSource, EnvVar, Pod, PodSecurityContext, PodSpec,
        ProjectedVolumeSource, ResourceRequirements, SeccompProfile, SecurityContext,
        ServiceAccountTokenProjection, Volume, VolumeMount, VolumeProjection,
    },
    apimachinery::pkg::{api::resource::Quantity, apis::meta::v1::ObjectMeta},
};
use kube::{
    api::{DeleteParams, PostParams, Preconditions},
    Api, Client,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use std::{collections::BTreeMap, time::Duration};

const GVISOR_GUEST_CONTAINER: &str = "sandbox-guest";
const BOOTSTRAP_TOKEN_VOLUME: &str = "bootstrap-token";
const SCRATCH_VOLUME: &str = "scratch";
const BOOTSTRAP_TOKEN_PATH: &str = "/var/run/secrets/insight.platform/token";
const TERMINATION_MESSAGE_PATH: &str = "/dev/termination-log";
const MAX_TERMINATION_MESSAGE_BYTES: usize = 4_096;

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KubernetesGvisorRuntimeConfig {
    pub namespace: String,
    pub runtime_class_name: String,
    pub guest_service_account_name: String,
    pub guest_image_repository: String,
    pub guest_command: String,
    pub bootstrap_endpoint: String,
    pub bootstrap_token_audience: String,
    pub bootstrap_token_expiration_seconds: i64,
    pub observation_poll_milliseconds: u64,
}

impl KubernetesGvisorRuntimeConfig {
    pub fn validate(&self) -> Result<(), KubernetesGvisorError> {
        if !dns_label(&self.namespace)
            || !dns_label(&self.runtime_class_name)
            || !dns_label(&self.guest_service_account_name)
            || !closed_image_repository(&self.guest_image_repository)
            || !absolute_guest_command(&self.guest_command)
            || !closed_https_endpoint(&self.bootstrap_endpoint)
            || !stable_audience(&self.bootstrap_token_audience)
            || !(60..=3_600).contains(&self.bootstrap_token_expiration_seconds)
            || !(10..=10_000).contains(&self.observation_poll_milliseconds)
        {
            return Err(KubernetesGvisorError::InvalidContract);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LaunchedGvisorPod {
    pub namespace: String,
    pub name: String,
    pub uid: String,
    pub request_digest: Sha256Digest,
    pub worker_process_generation_id: ResourceId,
    pub launch_evidence_digest: Sha256Digest,
}

impl LaunchedGvisorPod {
    fn validate_for(
        &self,
        request: &SandboxExecutionRequest,
        worker_process_generation_id: &ResourceId,
        expected_namespace: &str,
    ) -> Result<(), KubernetesGvisorError> {
        if self.namespace != expected_namespace
            || !dns_label(&self.name)
            || self.uid.is_empty()
            || self.uid.len() > 128
            || self.uid.chars().any(char::is_control)
            || self.request_digest != request.request_digest
            || &self.worker_process_generation_id != worker_process_generation_id
        {
            return Err(KubernetesGvisorError::Integrity);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObservedGvisorPodExit {
    pub exit_code: i32,
    pub reason: Option<String>,
    pub termination_message: Vec<u8>,
    pub observation_evidence_digest: Sha256Digest,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KubernetesGvisorError {
    InvalidContract,
    Denied,
    Conflict,
    NotFound,
    TimedOut,
    Integrity,
    Unavailable,
}

#[async_trait]
pub trait KubernetesGvisorPodRuntime: Send + Sync {
    async fn create(
        &self,
        request: &SandboxExecutionRequest,
        worker_process_generation_id: &ResourceId,
    ) -> Result<LaunchedGvisorPod, KubernetesGvisorError>;

    async fn wait_for_exit(
        &self,
        pod: &LaunchedGvisorPod,
        timeout: Duration,
    ) -> Result<ObservedGvisorPodExit, KubernetesGvisorError>;

    async fn delete(&self, pod: &LaunchedGvisorPod) -> Result<(), KubernetesGvisorError>;

    async fn is_absent(&self, pod: &LaunchedGvisorPod) -> Result<bool, KubernetesGvisorError>;
}

pub struct SystemKubernetesGvisorPodRuntime {
    config: KubernetesGvisorRuntimeConfig,
    pods: Api<Pod>,
}

struct GvisorPodContract<'a> {
    request_digest: &'a Sha256Digest,
    sandbox_job_id: &'a ResourceId,
    attempt_no: u32,
    lease_generation: u64,
    runtime_digest: &'a Sha256Digest,
    package_digest: &'a Sha256Digest,
    resources: &'a SandboxResourceEnvelope,
}

impl<'a> From<&'a SandboxExecutionRequest> for GvisorPodContract<'a> {
    fn from(request: &'a SandboxExecutionRequest) -> Self {
        Self {
            request_digest: &request.request_digest,
            sandbox_job_id: &request.sandbox_job_id,
            attempt_no: request.attempt_no,
            lease_generation: request.lease_generation,
            runtime_digest: &request.runtime.image_or_module_digest,
            package_digest: &request.package.package_digest,
            resources: &request.resources,
        }
    }
}

impl SystemKubernetesGvisorPodRuntime {
    pub async fn in_cluster(
        config: KubernetesGvisorRuntimeConfig,
    ) -> Result<Self, KubernetesGvisorError> {
        config.validate()?;
        let client = Client::try_default()
            .await
            .map_err(|_| KubernetesGvisorError::Unavailable)?;
        Ok(Self {
            pods: Api::namespaced(client, &config.namespace),
            config,
        })
    }
}

#[async_trait]
impl KubernetesGvisorPodRuntime for SystemKubernetesGvisorPodRuntime {
    async fn create(
        &self,
        request: &SandboxExecutionRequest,
        worker_process_generation_id: &ResourceId,
    ) -> Result<LaunchedGvisorPod, KubernetesGvisorError> {
        let planned = build_guest_pod(&self.config, request, worker_process_generation_id)?;
        let created = self
            .pods
            .create(
                &PostParams {
                    field_manager: Some("insight-platform-gvisor-launcher".to_owned()),
                    ..PostParams::default()
                },
                &planned,
            )
            .await
            .map_err(map_kube_error)?;
        validate_returned_pod(&planned, &created)?;
        let name = created
            .metadata
            .name
            .clone()
            .ok_or(KubernetesGvisorError::Integrity)?;
        let uid = created
            .metadata
            .uid
            .clone()
            .ok_or(KubernetesGvisorError::Integrity)?;
        let launch_evidence_digest = digest(&serde_json::json!({
            "schema_version": 1,
            "kind": "gvisor_pod_launched",
            "namespace": self.config.namespace,
            "name": name,
            "uid": uid,
            "request_digest": request.request_digest,
            "worker_process_generation_id": worker_process_generation_id,
            "pod_contract_digest": digest(&planned)?,
        }))?;
        let launched = LaunchedGvisorPod {
            namespace: self.config.namespace.clone(),
            name,
            uid,
            request_digest: request.request_digest.clone(),
            worker_process_generation_id: worker_process_generation_id.clone(),
            launch_evidence_digest,
        };
        launched.validate_for(
            request,
            worker_process_generation_id,
            &self.config.namespace,
        )?;
        Ok(launched)
    }

    async fn wait_for_exit(
        &self,
        pod: &LaunchedGvisorPod,
        timeout: Duration,
    ) -> Result<ObservedGvisorPodExit, KubernetesGvisorError> {
        let deadline = tokio::time::Instant::now() + timeout;
        let poll = Duration::from_millis(self.config.observation_poll_milliseconds);
        loop {
            let current = self
                .pods
                .get_opt(&pod.name)
                .await
                .map_err(map_kube_error)?
                .ok_or(KubernetesGvisorError::NotFound)?;
            require_uid(&current, &pod.uid)?;
            if let Some(terminated) = current
                .status
                .as_ref()
                .and_then(|status| status.container_statuses.as_ref())
                .and_then(|statuses| {
                    statuses
                        .iter()
                        .find(|status| status.name == GVISOR_GUEST_CONTAINER)
                })
                .and_then(|status| status.state.as_ref())
                .and_then(|state| state.terminated.as_ref())
            {
                let message = terminated.message.clone().unwrap_or_default().into_bytes();
                if message.len() > MAX_TERMINATION_MESSAGE_BYTES {
                    return Err(KubernetesGvisorError::Integrity);
                }
                let reason = terminated.reason.clone();
                let exit_code = terminated.exit_code;
                let observation_evidence_digest = digest(&serde_json::json!({
                    "schema_version": 1,
                    "kind": "gvisor_pod_exit_observed",
                    "pod_uid": pod.uid,
                    "request_digest": pod.request_digest,
                    "exit_code": exit_code,
                    "reason": reason,
                    "termination_message_digest": digest(&message)?,
                }))?;
                return Ok(ObservedGvisorPodExit {
                    exit_code,
                    reason,
                    termination_message: message,
                    observation_evidence_digest,
                });
            }
            let now = tokio::time::Instant::now();
            if now >= deadline {
                return Err(KubernetesGvisorError::TimedOut);
            }
            tokio::time::sleep(poll.min(deadline - now)).await;
        }
    }

    async fn delete(&self, pod: &LaunchedGvisorPod) -> Result<(), KubernetesGvisorError> {
        let params = DeleteParams {
            grace_period_seconds: Some(0),
            preconditions: Some(Preconditions {
                resource_version: None,
                uid: Some(pod.uid.clone()),
            }),
            ..DeleteParams::default()
        };
        match self.pods.delete(&pod.name, &params).await {
            Ok(_) => Ok(()),
            Err(kube::Error::Api(response)) if response.code == 404 => Ok(()),
            Err(error) => Err(map_kube_error(error)),
        }
    }

    async fn is_absent(&self, pod: &LaunchedGvisorPod) -> Result<bool, KubernetesGvisorError> {
        let Some(current) = self.pods.get_opt(&pod.name).await.map_err(map_kube_error)? else {
            return Ok(true);
        };
        let current_uid = current
            .metadata
            .uid
            .ok_or(KubernetesGvisorError::Integrity)?;
        if current_uid != pod.uid {
            return Err(KubernetesGvisorError::Conflict);
        }
        Ok(false)
    }
}

fn build_guest_pod(
    config: &KubernetesGvisorRuntimeConfig,
    request: &SandboxExecutionRequest,
    worker_process_generation_id: &ResourceId,
) -> Result<Pod, KubernetesGvisorError> {
    build_guest_pod_contract(config, &request.into(), worker_process_generation_id)
}

fn build_guest_pod_contract(
    config: &KubernetesGvisorRuntimeConfig,
    request: &GvisorPodContract<'_>,
    worker_process_generation_id: &ResourceId,
) -> Result<Pod, KubernetesGvisorError> {
    config.validate()?;
    if worker_process_generation_id.kind() != ResourceKind::WorkerProcessGeneration {
        return Err(KubernetesGvisorError::InvalidContract);
    }
    let name = pod_name(request, worker_process_generation_id);
    let image = format!(
        "{}@{}",
        config.guest_image_repository, request.runtime_digest
    );
    let mut labels = BTreeMap::new();
    labels.insert(
        "app.kubernetes.io/name".to_owned(),
        "insight-platform-sandbox-guest".to_owned(),
    );
    labels.insert(
        "app.kubernetes.io/component".to_owned(),
        "gvisor-single-job".to_owned(),
    );
    labels.insert(
        "app.kubernetes.io/managed-by".to_owned(),
        "insight-platform-gvisor-launcher".to_owned(),
    );
    let mut annotations = BTreeMap::new();
    annotations.insert(
        "insight.platform/request-digest".to_owned(),
        request.request_digest.to_string(),
    );
    annotations.insert(
        "insight.platform/job-id".to_owned(),
        request.sandbox_job_id.to_string(),
    );
    annotations.insert(
        "insight.platform/worker-process-generation-id".to_owned(),
        worker_process_generation_id.to_string(),
    );
    annotations.insert(
        "insight.platform/runtime-digest".to_owned(),
        request.runtime_digest.to_string(),
    );
    annotations.insert(
        "insight.platform/package-digest".to_owned(),
        request.package_digest.to_string(),
    );
    annotations.insert(
        "insight.platform/attempt-no".to_owned(),
        request.attempt_no.to_string(),
    );
    annotations.insert(
        "insight.platform/lease-generation".to_owned(),
        request.lease_generation.to_string(),
    );

    let resources = pod_resources(request.resources)?;
    let security_context = SecurityContext {
        allow_privilege_escalation: Some(false),
        capabilities: Some(Capabilities {
            add: None,
            drop: Some(vec!["ALL".to_owned()]),
        }),
        privileged: Some(false),
        read_only_root_filesystem: Some(true),
        run_as_group: Some(65_532),
        run_as_non_root: Some(true),
        run_as_user: Some(65_532),
        seccomp_profile: Some(SeccompProfile {
            localhost_profile: None,
            type_: "RuntimeDefault".to_owned(),
        }),
        ..SecurityContext::default()
    };
    let container = Container {
        name: GVISOR_GUEST_CONTAINER.to_owned(),
        image: Some(image),
        image_pull_policy: Some("IfNotPresent".to_owned()),
        command: Some(vec![config.guest_command.clone()]),
        args: Some(vec!["run".to_owned()]),
        env: Some(vec![
            EnvVar {
                name: "INSIGHT_SANDBOX_BOOTSTRAP_ENDPOINT".to_owned(),
                value: Some(config.bootstrap_endpoint.clone()),
                ..EnvVar::default()
            },
            EnvVar {
                name: "INSIGHT_SANDBOX_BOOTSTRAP_TOKEN_PATH".to_owned(),
                value: Some(BOOTSTRAP_TOKEN_PATH.to_owned()),
                ..EnvVar::default()
            },
            EnvVar {
                name: "INSIGHT_SANDBOX_REQUEST_DIGEST".to_owned(),
                value: Some(request.request_digest.to_string()),
                ..EnvVar::default()
            },
        ]),
        resources: Some(resources),
        security_context: Some(security_context),
        termination_message_path: Some(TERMINATION_MESSAGE_PATH.to_owned()),
        termination_message_policy: Some("File".to_owned()),
        volume_mounts: Some(vec![
            VolumeMount {
                mount_path: "/var/run/secrets/insight.platform".to_owned(),
                name: BOOTSTRAP_TOKEN_VOLUME.to_owned(),
                read_only: Some(true),
                ..VolumeMount::default()
            },
            VolumeMount {
                mount_path: "/scratch".to_owned(),
                name: SCRATCH_VOLUME.to_owned(),
                read_only: Some(false),
                ..VolumeMount::default()
            },
        ]),
        ..Container::default()
    };
    let wall_seconds = request.resources.wall_milliseconds.div_ceil(1_000);
    let cleanup_seconds = request.resources.cleanup_milliseconds.div_ceil(1_000);
    let pod = Pod {
        metadata: ObjectMeta {
            name: Some(name),
            namespace: Some(config.namespace.clone()),
            labels: Some(labels),
            annotations: Some(annotations),
            ..ObjectMeta::default()
        },
        spec: Some(PodSpec {
            active_deadline_seconds: Some(
                i64::try_from(wall_seconds).map_err(|_| KubernetesGvisorError::InvalidContract)?,
            ),
            automount_service_account_token: Some(false),
            containers: vec![container],
            dns_policy: Some("ClusterFirst".to_owned()),
            enable_service_links: Some(false),
            host_ipc: Some(false),
            host_network: Some(false),
            host_pid: Some(false),
            restart_policy: Some("Never".to_owned()),
            runtime_class_name: Some(config.runtime_class_name.clone()),
            security_context: Some(PodSecurityContext {
                run_as_group: Some(65_532),
                run_as_non_root: Some(true),
                run_as_user: Some(65_532),
                seccomp_profile: Some(SeccompProfile {
                    localhost_profile: None,
                    type_: "RuntimeDefault".to_owned(),
                }),
                ..PodSecurityContext::default()
            }),
            service_account_name: Some(config.guest_service_account_name.clone()),
            termination_grace_period_seconds: Some(
                i64::try_from(cleanup_seconds.max(1))
                    .map_err(|_| KubernetesGvisorError::InvalidContract)?,
            ),
            volumes: Some(vec![
                Volume {
                    name: BOOTSTRAP_TOKEN_VOLUME.to_owned(),
                    projected: Some(ProjectedVolumeSource {
                        default_mode: Some(0o400),
                        sources: Some(vec![VolumeProjection {
                            service_account_token: Some(ServiceAccountTokenProjection {
                                audience: Some(config.bootstrap_token_audience.clone()),
                                expiration_seconds: Some(config.bootstrap_token_expiration_seconds),
                                path: "token".to_owned(),
                            }),
                            ..VolumeProjection::default()
                        }]),
                    }),
                    ..Volume::default()
                },
                Volume {
                    name: SCRATCH_VOLUME.to_owned(),
                    empty_dir: Some(EmptyDirVolumeSource {
                        medium: Some("Memory".to_owned()),
                        size_limit: Some(Quantity(request.resources.io_bytes.to_string())),
                    }),
                    ..Volume::default()
                },
            ]),
            ..PodSpec::default()
        }),
        status: None,
    };
    validate_planned_pod(&pod, config, request)?;
    Ok(pod)
}

fn pod_resources(
    resources: &SandboxResourceEnvelope,
) -> Result<ResourceRequirements, KubernetesGvisorError> {
    if resources.cpu_millicores == 0 || resources.memory_mebibytes == 0 || resources.io_bytes == 0 {
        return Err(KubernetesGvisorError::InvalidContract);
    }
    let values = BTreeMap::from([
        (
            "cpu".to_owned(),
            Quantity(format!("{}m", resources.cpu_millicores)),
        ),
        (
            "memory".to_owned(),
            Quantity(format!("{}Mi", resources.memory_mebibytes)),
        ),
        (
            "ephemeral-storage".to_owned(),
            Quantity(resources.io_bytes.to_string()),
        ),
    ]);
    Ok(ResourceRequirements {
        claims: None,
        limits: Some(values.clone()),
        requests: Some(values),
    })
}

fn validate_planned_pod(
    pod: &Pod,
    config: &KubernetesGvisorRuntimeConfig,
    request: &GvisorPodContract<'_>,
) -> Result<(), KubernetesGvisorError> {
    let spec = pod.spec.as_ref().ok_or(KubernetesGvisorError::Integrity)?;
    let container = spec
        .containers
        .first()
        .filter(|_| spec.containers.len() == 1)
        .ok_or(KubernetesGvisorError::Integrity)?;
    let security = container
        .security_context
        .as_ref()
        .ok_or(KubernetesGvisorError::Integrity)?;
    if pod.metadata.namespace.as_deref() != Some(config.namespace.as_str())
        || spec.runtime_class_name.as_deref() != Some(config.runtime_class_name.as_str())
        || spec.service_account_name.as_deref() != Some(config.guest_service_account_name.as_str())
        || spec.automount_service_account_token != Some(false)
        || spec.host_ipc != Some(false)
        || spec.host_network != Some(false)
        || spec.host_pid != Some(false)
        || spec.restart_policy.as_deref() != Some("Never")
        || container.name != GVISOR_GUEST_CONTAINER
        || container.image.as_deref()
            != Some(
                format!(
                    "{}@{}",
                    config.guest_image_repository, request.runtime_digest
                )
                .as_str(),
            )
        || security.privileged != Some(false)
        || security.allow_privilege_escalation != Some(false)
        || security.read_only_root_filesystem != Some(true)
        || security.run_as_non_root != Some(true)
        || spec.volumes.as_ref().is_none_or(|volumes| {
            volumes.len() != 2
                || volumes.iter().any(|volume| {
                    volume.host_path.is_some() || volume.persistent_volume_claim.is_some()
                })
        })
    {
        return Err(KubernetesGvisorError::Integrity);
    }
    Ok(())
}

fn validate_returned_pod(expected: &Pod, actual: &Pod) -> Result<(), KubernetesGvisorError> {
    let expected_spec = expected
        .spec
        .as_ref()
        .ok_or(KubernetesGvisorError::Integrity)?;
    let actual_spec = actual
        .spec
        .as_ref()
        .ok_or(KubernetesGvisorError::Integrity)?;
    let expected_container = expected_spec
        .containers
        .first()
        .filter(|_| expected_spec.containers.len() == 1)
        .ok_or(KubernetesGvisorError::Integrity)?;
    let actual_container = actual_spec
        .containers
        .first()
        .filter(|_| actual_spec.containers.len() == 1)
        .ok_or(KubernetesGvisorError::Integrity)?;
    if actual.metadata.name != expected.metadata.name
        || actual.metadata.namespace != expected.metadata.namespace
        || actual.metadata.labels != expected.metadata.labels
        || actual.metadata.annotations != expected.metadata.annotations
        || actual_spec.runtime_class_name != expected_spec.runtime_class_name
        || actual_spec.service_account_name != expected_spec.service_account_name
        || actual_spec.automount_service_account_token
            != expected_spec.automount_service_account_token
        || actual_spec.host_ipc != expected_spec.host_ipc
        || actual_spec.host_network != expected_spec.host_network
        || actual_spec.host_pid != expected_spec.host_pid
        || actual_spec.restart_policy != expected_spec.restart_policy
        || actual_spec.active_deadline_seconds != expected_spec.active_deadline_seconds
        || actual_spec.termination_grace_period_seconds
            != expected_spec.termination_grace_period_seconds
        || actual_spec.security_context != expected_spec.security_context
        || actual_spec.volumes != expected_spec.volumes
        || actual_spec
            .init_containers
            .as_ref()
            .is_some_and(|items| !items.is_empty())
        || actual_spec
            .ephemeral_containers
            .as_ref()
            .is_some_and(|items| !items.is_empty())
        || actual_spec
            .host_aliases
            .as_ref()
            .is_some_and(|items| !items.is_empty())
        || actual_spec.host_users == Some(true)
        || actual_container.name != expected_container.name
        || actual_container.image != expected_container.image
        || actual_container.command != expected_container.command
        || actual_container.args != expected_container.args
        || actual_container.env != expected_container.env
        || actual_container.env_from != expected_container.env_from
        || actual_container.resources != expected_container.resources
        || actual_container.security_context != expected_container.security_context
        || actual_container.volume_mounts != expected_container.volume_mounts
        || actual_container.volume_devices != expected_container.volume_devices
        || actual_container
            .ports
            .as_ref()
            .is_some_and(|ports| !ports.is_empty())
        || actual_container.lifecycle.is_some()
        || actual_container.stdin == Some(true)
        || actual_container.tty == Some(true)
    {
        return Err(KubernetesGvisorError::Integrity);
    }
    Ok(())
}

fn require_uid(pod: &Pod, expected: &str) -> Result<(), KubernetesGvisorError> {
    if pod.metadata.uid.as_deref() != Some(expected) {
        return Err(KubernetesGvisorError::Conflict);
    }
    Ok(())
}

fn pod_name(request: &GvisorPodContract<'_>, worker_process_generation_id: &ResourceId) -> String {
    let digest = Sha256::digest(format!(
        "{}:{}:{}:{}",
        request.request_digest,
        request.attempt_no,
        request.lease_generation,
        worker_process_generation_id
    ));
    format!("insight-gv-{}", lower_hex(&digest[..16]))
}

fn map_kube_error(error: kube::Error) -> KubernetesGvisorError {
    match error {
        kube::Error::Api(response) if response.code == 401 || response.code == 403 => {
            KubernetesGvisorError::Denied
        }
        kube::Error::Api(response) if response.code == 404 => KubernetesGvisorError::NotFound,
        kube::Error::Api(response) if response.code == 409 => KubernetesGvisorError::Conflict,
        _ => KubernetesGvisorError::Unavailable,
    }
}

fn digest(value: &impl Serialize) -> Result<Sha256Digest, KubernetesGvisorError> {
    let value = serde_json::to_value(value).map_err(|_| KubernetesGvisorError::Integrity)?;
    canonical_digest(&value)
        .map_err(|_| KubernetesGvisorError::Integrity)?
        .parse()
        .map_err(|_| KubernetesGvisorError::Integrity)
}

fn dns_label(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 63
        && value.bytes().enumerate().all(|(index, byte)| match byte {
            b'a'..=b'z' | b'0'..=b'9' => true,
            b'-' => index > 0 && index + 1 < value.len(),
            _ => false,
        })
}

fn closed_image_repository(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 255
        && value.contains('/')
        && !value.contains('@')
        && !value.contains("..")
        && value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'/' | b':' | b'_' | b'-')
        })
}

fn absolute_guest_command(value: &str) -> bool {
    value.starts_with('/')
        && value.len() <= 255
        && !value.contains("..")
        && !value.chars().any(char::is_control)
}

fn closed_https_endpoint(value: &str) -> bool {
    url::Url::parse(value).is_ok_and(|url| {
        url.scheme() == "https"
            && url.username().is_empty()
            && url.password().is_none()
            && url.host_str().is_some()
            && url.path() == "/"
            && url.query().is_none()
            && url.fragment().is_none()
    })
}

fn stable_audience(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 255
        && value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'/' | b':' | b'_' | b'-')
        })
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
    use insight_platform_contracts::ResourceKind;
    use uuid::Uuid;

    fn id(kind: ResourceKind) -> ResourceId {
        ResourceId::from_uuid_v7(kind, Uuid::now_v7()).unwrap()
    }

    fn sha(character: char) -> Sha256Digest {
        format!("sha256:{}", character.to_string().repeat(64))
            .parse()
            .unwrap()
    }

    fn config() -> KubernetesGvisorRuntimeConfig {
        KubernetesGvisorRuntimeConfig {
            namespace: "platform-sandbox-guests".to_owned(),
            runtime_class_name: "runsc".to_owned(),
            guest_service_account_name: "sandbox-guest".to_owned(),
            guest_image_repository: "registry.example/insight/sandbox-guest".to_owned(),
            guest_command: "/opt/insight/bin/sandbox-guest".to_owned(),
            bootstrap_endpoint: "https://sandbox-bootstrap.platform.svc:7445".to_owned(),
            bootstrap_token_audience: "insight.platform/sandbox-guest".to_owned(),
            bootstrap_token_expiration_seconds: 600,
            observation_poll_milliseconds: 100,
        }
    }

    struct TestPodContract {
        request_digest: Sha256Digest,
        sandbox_job_id: ResourceId,
        runtime_digest: Sha256Digest,
        package_digest: Sha256Digest,
        resources: SandboxResourceEnvelope,
    }

    impl TestPodContract {
        fn view(&self) -> GvisorPodContract<'_> {
            GvisorPodContract {
                request_digest: &self.request_digest,
                sandbox_job_id: &self.sandbox_job_id,
                attempt_no: 1,
                lease_generation: 7,
                runtime_digest: &self.runtime_digest,
                package_digest: &self.package_digest,
                resources: &self.resources,
            }
        }
    }

    fn contract() -> TestPodContract {
        TestPodContract {
            request_digest: sha('d'),
            sandbox_job_id: id(ResourceKind::Job),
            runtime_digest: sha('a'),
            package_digest: sha('b'),
            resources: SandboxResourceEnvelope {
                cpu_millicores: 500,
                memory_mebibytes: 128,
                pids: 32,
                files: 64,
                io_bytes: 1_048_576,
                stdout_bytes: 65_536,
                stderr_bytes: 65_536,
                result_bytes: 65_536,
                artifact_output_bytes: 1_048_576,
                network_connections: 0,
                network_request_bytes: 0,
                network_response_bytes: 0,
                startup_milliseconds: 5_000,
                idle_milliseconds: 5_000,
                wall_milliseconds: 30_000,
                cleanup_milliseconds: 5_000,
                wasm_fuel: None,
                wasm_memory_pages: None,
            },
        }
    }

    #[test]
    fn manifest_is_locked_to_runsc_digest_resources_and_no_host_authority() {
        let request = contract();
        let worker = id(ResourceKind::WorkerProcessGeneration);
        let pod = build_guest_pod_contract(&config(), &request.view(), &worker).unwrap();
        let spec = pod.spec.as_ref().unwrap();
        let container = &spec.containers[0];
        assert_eq!(spec.runtime_class_name.as_deref(), Some("runsc"));
        assert_eq!(spec.automount_service_account_token, Some(false));
        assert_eq!(
            container.security_context.as_ref().unwrap().privileged,
            Some(false)
        );
        assert_eq!(
            container.image.as_deref(),
            Some(format!("registry.example/insight/sandbox-guest@{}", sha('a')).as_str())
        );
        assert!(spec.volumes.as_ref().unwrap().iter().all(|volume| {
            volume.host_path.is_none()
                && volume.persistent_volume_claim.is_none()
                && volume.secret.is_none()
                && volume.config_map.is_none()
        }));
        let canonical = serde_json::to_value(&pod).unwrap();
        assert!(!canonical.to_string().contains("hello"));
    }

    #[test]
    fn invalid_runtime_or_mutated_returned_contract_fails_closed() {
        let request = contract();
        let worker = id(ResourceKind::WorkerProcessGeneration);
        let planned = build_guest_pod_contract(&config(), &request.view(), &worker).unwrap();
        let mut returned = planned.clone();
        returned.metadata.uid = Some(Uuid::now_v7().to_string());
        returned.spec.as_mut().unwrap().runtime_class_name = Some("runc".to_owned());
        assert_eq!(
            validate_returned_pod(&planned, &returned),
            Err(KubernetesGvisorError::Integrity)
        );

        let mut invalid = config();
        invalid.guest_image_repository = "registry.example/guest:latest@sha256:bad".to_owned();
        assert_eq!(
            invalid.validate(),
            Err(KubernetesGvisorError::InvalidContract)
        );
    }
}
