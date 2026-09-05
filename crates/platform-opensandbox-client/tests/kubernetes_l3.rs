//! Real OpenSandbox Server + BatchSandbox Controller + Kubernetes/containerd L3 qualification.
//!
//! The test is environment-gated and is orchestrated by
//! `scripts/qualify-platform-sandbox-l3.sh`. It never imports OpenSandbox implementation code;
//! all interaction uses the published lifecycle API and the immutable runner protocol.

use chrono::{Duration as ChronoDuration, TimeZone as _, Utc};
use insight_platform_contracts::{
    DataClassification, ResourceId, ResourceKind, Sha256Digest, TraceIdentityV1,
};
use insight_platform_opensandbox_client::{
    OpenSandboxApiKey, OpenSandboxHttpClient, OpenSandboxHttpClientConfig,
    OpenSandboxReadinessProbeConfig,
};
use insight_platform_sandbox::opensandbox::{
    parse_result_frame, ActivationSignature, CandidateCursorV1, OpaqueActivationToken,
    OpenSandboxCreateV1, OpenSandboxProvider, SandboxActivationFrameV1, SandboxExecutionRequestV1,
    SandboxNetworkMode, SandboxPhysicalEvidenceV1, SandboxProviderError,
    SandboxProvisioningLimitsV1, SandboxResourceLimitsV1, SandboxRunnerOutcomeV1,
    SandboxRunnerPhaseV1, SandboxRunnerRequestProofV1, SANDBOX_CONTRACT_SCHEMA_VERSION,
    SANDBOX_PACKAGE_UID,
};
use serde_json::{json, Value};
use std::{env, time::Duration};
use tokio::time::{sleep, Instant};
use url::Url;
use uuid::Uuid;

const L3_PHASE_ENV: &str = "PLATFORM_OPENSANDBOX_L3_PHASE";
const L3_URL_ENV: &str = "PLATFORM_OPENSANDBOX_L3_URL";
const L3_API_KEY_ENV: &str = "PLATFORM_OPENSANDBOX_L3_API_KEY";
const L3_IMAGE_ENV: &str = "PLATFORM_OPENSANDBOX_L3_IMAGE";
const L3_RUNTIME_DIGEST_ENV: &str = "PLATFORM_OPENSANDBOX_L3_RUNTIME_DIGEST";
const L3_PROBE_ADDRESS_ENV: &str = "PLATFORM_OPENSANDBOX_L3_PROBE_ADDRESS";
const L3_PROBE_PORT_ENV: &str = "PLATFORM_OPENSANDBOX_L3_PROBE_PORT";
const L3_WORKLOADS_NAMESPACE_ENV: &str = "PLATFORM_OPENSANDBOX_L3_WORKLOADS_NAMESPACE";

#[derive(Clone)]
struct L3Context {
    phase: String,
    lifecycle_base_url: Url,
    api_key: OpenSandboxApiKey,
    image_uri: String,
    runtime_contract_digest: Sha256Digest,
    probe_address: String,
    probe_port: String,
    workloads_namespace: String,
}

impl L3Context {
    fn load(expected_phase: &str) -> Option<Self> {
        let Ok(phase) = env::var(L3_PHASE_ENV) else {
            eprintln!("{L3_PHASE_ENV} is unset; real OpenSandbox Kubernetes L3 skipped");
            return None;
        };
        if phase != expected_phase {
            return None;
        }
        let lifecycle_base_url = env::var(L3_URL_ENV)
            .expect("L3 lifecycle URL")
            .parse()
            .expect("valid L3 lifecycle URL");
        let api_key = OpenSandboxApiKey::parse(env::var(L3_API_KEY_ENV).expect("L3 API key"))
            .expect("valid L3 API key");
        let image_uri = env::var(L3_IMAGE_ENV).expect("L3 exact Package image");
        let runtime_contract_digest = env::var(L3_RUNTIME_DIGEST_ENV)
            .expect("L3 runtime contract digest")
            .parse()
            .expect("valid L3 runtime contract digest");
        let probe_address = env::var(L3_PROBE_ADDRESS_ENV).expect("L3 external probe IP");
        assert!(
            probe_address.parse::<std::net::IpAddr>().is_ok(),
            "L3 external probe address must be a literal IP"
        );
        let probe_port = env::var(L3_PROBE_PORT_ENV).expect("L3 external probe port");
        assert!(
            probe_port.parse::<u16>().is_ok_and(|port| port != 0),
            "L3 external probe port must be non-zero"
        );
        let workloads_namespace = env::var(L3_WORKLOADS_NAMESPACE_ENV)
            .unwrap_or_else(|_| "platform-sandbox-workloads".to_owned());
        assert!(
            !workloads_namespace.is_empty() && workloads_namespace.len() <= 63,
            "L3 workloads namespace is invalid"
        );
        Some(Self {
            phase,
            lifecycle_base_url,
            api_key,
            image_uri,
            runtime_contract_digest,
            probe_address,
            probe_port,
            workloads_namespace,
        })
    }

    fn client(&self, request_timeout_milliseconds: u32) -> OpenSandboxHttpClient {
        OpenSandboxHttpClient::new(OpenSandboxHttpClientConfig {
            lifecycle_base_url: self.lifecycle_base_url.clone(),
            api_key: self.api_key.clone(),
            request_timeout_milliseconds,
            connect_timeout_milliseconds: request_timeout_milliseconds.min(1_000),
            candidate_page_items: 4,
            orphan_page_items: 20,
        })
        .expect("valid L3 client configuration")
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn opensandbox_kubernetes_l3_concurrent_response_loss_and_network_modes() {
    let Some(context) = L3Context::load("core") else {
        return;
    };
    assert_eq!(context.phase, "core");
    let client = context.client(10_000);
    client.lifecycle_probe().await.unwrap();
    let wrong_credential_client = OpenSandboxHttpClient::new(OpenSandboxHttpClientConfig {
        lifecycle_base_url: context.lifecycle_base_url.clone(),
        api_key: OpenSandboxApiKey::parse("wrong-opensandbox-api-key-0000001").unwrap(),
        request_timeout_milliseconds: 2_000,
        connect_timeout_milliseconds: 1_000,
        candidate_page_items: 4,
        orphan_page_items: 20,
    })
    .unwrap();
    assert_eq!(
        wrong_credential_client.lifecycle_probe().await,
        Err(insight_platform_sandbox::opensandbox::SandboxProviderError::Unauthorized)
    );

    let direct = request(
        &context,
        10,
        SandboxNetworkMode::Direct,
        vec![
            "/opt/insight/package".to_owned(),
            "probe".to_owned(),
            context.probe_address.clone(),
            context.probe_port.clone(),
        ],
    );
    assert_eq!(
        execute(&client, &direct, 'a').await,
        json!({"network_reachable":true})
    );

    let disabled = request(
        &context,
        11,
        SandboxNetworkMode::Disabled,
        vec![
            "/opt/insight/package".to_owned(),
            "probe".to_owned(),
            context.probe_address.clone(),
            context.probe_port.clone(),
        ],
    );
    assert_eq!(
        execute(&client, &disabled, 'b').await,
        json!({"network_reachable":false})
    );

    let concurrent = request(
        &context,
        12,
        SandboxNetworkMode::Disabled,
        vec!["/opt/insight/package".to_owned(), "echo".to_owned()],
    );
    let (first, second, token_digest, signing_seed) = two_authorized_creates(&concurrent, 'c', 60);
    let (first_result, second_result) = tokio::join!(
        client.create_candidate(first),
        client.create_candidate(second),
    );
    let first_id = first_result.unwrap().sandbox_id;
    let second_id = second_result.unwrap().sandbox_id;
    assert_ne!(first_id, second_id);
    let candidates = wait_for_candidates(&client, token_digest, 2).await;
    assert_eq!(candidates.len(), 2);
    for sandbox_id in candidates {
        assert_eq!(
            wait_for_runner(
                &client,
                &sandbox_id,
                &signing_seed,
                &concurrent.request_digest,
            )
            .await
            .phase,
            SandboxRunnerPhaseV1::Armed
        );
        delete_and_wait(&client, &sandbox_id).await;
    }

    let response_loss = request(
        &context,
        13,
        SandboxNetworkMode::Disabled,
        vec!["/opt/insight/package".to_owned(), "echo".to_owned()],
    );
    let (create, token_digest, evidence) = authorized_create(&response_loss, 'd', 1, 60);
    let short_client = context.client(100);
    assert!(short_client.create_candidate(create).await.is_err());
    let recovered = wait_for_candidates(&client, token_digest, 1).await;
    assert_eq!(recovered.len(), 1);
    assert_eq!(
        wait_for_runner(
            &client,
            &recovered[0],
            &evidence.activation_token,
            &response_loss.request_digest,
        )
        .await
        .phase,
        SandboxRunnerPhaseV1::Armed
    );
    delete_and_wait(&client, &recovered[0]).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn opensandbox_kubernetes_l3_full_readiness_probe() {
    let Some(context) = L3Context::load("readiness") else {
        return;
    };
    let client = context.client(90_000);
    client
        .readiness_probe(&OpenSandboxReadinessProbeConfig {
            image_uri: context.image_uri,
            runtime_contract_digest: context.runtime_contract_digest,
            profile_deployment_digest: digest('9'),
            ttl_seconds: 60,
        })
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn opensandbox_kubernetes_l3_signed_activation_is_candidate_bound() {
    let Some(context) = L3Context::load("activation-boundary") else {
        return;
    };
    let client = context.client(10_000);
    let request = request(
        &context,
        15,
        SandboxNetworkMode::Disabled,
        vec!["/opt/insight/package".to_owned(), "echo".to_owned()],
    );
    let (first_create, token_digest, evidence) = authorized_create(&request, '5', 1, 60);
    let (second_create, second_token_digest, _) = authorized_create(&request, '5', 2, 60);
    assert_eq!(token_digest, second_token_digest);
    for sandbox_id in list_candidates(&client, token_digest).await {
        delete_and_wait(&client, &sandbox_id).await;
    }
    let (first, second) = tokio::join!(
        client.create_candidate(first_create),
        client.create_candidate(second_create),
    );
    let first = first.unwrap();
    let second = second.unwrap();
    let first_armed = wait_for_runner(
        &client,
        &first.sandbox_id,
        &evidence.activation_token,
        &request.request_digest,
    )
    .await;
    let second_armed = wait_for_runner(
        &client,
        &second.sandbox_id,
        &evidence.activation_token,
        &request.request_digest,
    )
    .await;
    let activation = SandboxActivationFrameV1 {
        magic: String::new(),
        schema_version: SANDBOX_CONTRACT_SCHEMA_VERSION,
        sandbox_id: first.sandbox_id.clone(),
        boot_id: first_armed.boot_id.clone(),
        execution_request_digest: request.request_digest.clone(),
        input_schema_digest: request.input_schema_digest.clone(),
        input_digest: request.input_digest.clone(),
        declared_input_bytes: 0,
        input: request.input.clone(),
        activation_signature: ActivationSignature::parse("0".repeat(128)).unwrap(),
        frame_digest: zero_digest(),
    }
    .seal_with(&evidence.activation_token)
    .unwrap();
    let activation_proof = SandboxRunnerRequestProofV1::for_activate(
        &evidence.activation_token,
        &second.sandbox_id,
        &request.request_digest,
        &activation.canonical_bytes().unwrap(),
    )
    .unwrap();
    assert_eq!(
        client
            .activate(&second.sandbox_id, activation, activation_proof)
            .await,
        Err(SandboxProviderError::InvalidResponse)
    );
    let second_after = client
        .runner_state(
            &second.sandbox_id,
            state_proof(
                &evidence.activation_token,
                &second.sandbox_id,
                &request.request_digest,
            ),
        )
        .await
        .unwrap();
    assert_eq!(second_after.phase, SandboxRunnerPhaseV1::Armed);
    assert_eq!(second_after.boot_id, second_armed.boot_id);
    assert_eq!(
        client
            .runner_state(
                &first.sandbox_id,
                state_proof(
                    &evidence.activation_token,
                    &first.sandbox_id,
                    &request.request_digest,
                ),
            )
            .await
            .unwrap()
            .phase,
        SandboxRunnerPhaseV1::Armed
    );
    delete_and_wait(&client, &first.sandbox_id).await;
    delete_and_wait(&client, &second.sandbox_id).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn opensandbox_kubernetes_l3_package_cannot_cross_runner_boundary_or_survive() {
    let Some(context) = L3Context::load("package-boundary") else {
        return;
    };
    let client = context.client(10_000);
    let marker = "/tmp/insight-boundary-daemon-survived";
    let request = request(
        &context,
        16,
        SandboxNetworkMode::Disabled,
        vec![
            "/opt/insight/package".to_owned(),
            "boundary".to_owned(),
            marker.to_owned(),
        ],
    );
    let (sandbox_id, output) = execute_retained(&client, &request, '6').await;
    assert_eq!(
        output,
        json!({
            "effective_capabilities": 0,
            "effective_uid": SANDBOX_PACKAGE_UID,
            "runner_setuid_allowed": false,
            "runner_signal_allowed": false,
            "state_write_allowed": false,
        })
    );
    sleep(Duration::from_secs(3)).await;
    let pod_name = workload_pod_name(&context.workloads_namespace, &sandbox_id)
        .await
        .expect("boundary workload Pod name");
    kubectl(&[
        "exec",
        "-n",
        &context.workloads_namespace,
        &pod_name,
        "--",
        "/usr/bin/test",
        "!",
        "-e",
        marker,
    ])
    .await;
    delete_and_wait(&client, &sandbox_id).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn opensandbox_kubernetes_l3_runner_boot_changes_after_workload_pod_recreation() {
    let Some(context) = L3Context::load("boot-rollover") else {
        return;
    };
    let client = context.client(10_000);
    let request = request(
        &context,
        14,
        SandboxNetworkMode::Disabled,
        vec![
            "/opt/insight/package".to_owned(),
            "sleep-echo".to_owned(),
            "90000".to_owned(),
        ],
    );
    let (create, token_digest, evidence) = authorized_create(&request, '4', 1, 120);
    for sandbox_id in list_candidates(&client, token_digest).await {
        delete_and_wait(&client, &sandbox_id).await;
    }
    let candidate = client.create_candidate(create).await.unwrap();
    let armed = wait_for_runner(
        &client,
        &candidate.sandbox_id,
        &evidence.activation_token,
        &request.request_digest,
    )
    .await;
    assert_eq!(armed.phase, SandboxRunnerPhaseV1::Armed);
    let signing_seed = evidence.activation_token;
    let activation = SandboxActivationFrameV1 {
        magic: String::new(),
        schema_version: SANDBOX_CONTRACT_SCHEMA_VERSION,
        sandbox_id: candidate.sandbox_id.clone(),
        boot_id: armed.boot_id.clone(),
        execution_request_digest: request.request_digest.clone(),
        input_schema_digest: request.input_schema_digest.clone(),
        input_digest: request.input_digest.clone(),
        declared_input_bytes: 0,
        input: request.input.clone(),
        activation_signature: ActivationSignature::parse("0".repeat(128)).unwrap(),
        frame_digest: zero_digest(),
    }
    .seal_with(&signing_seed)
    .unwrap();
    let activation_proof = SandboxRunnerRequestProofV1::for_activate(
        &signing_seed,
        &candidate.sandbox_id,
        &request.request_digest,
        &activation.canonical_bytes().unwrap(),
    )
    .unwrap();
    client
        .activate(&candidate.sandbox_id, activation, activation_proof)
        .await
        .unwrap();
    wait_for_runner_phase(
        &client,
        &candidate.sandbox_id,
        &armed.boot_id,
        SandboxRunnerPhaseV1::Started,
        &signing_seed,
        &request.request_digest,
    )
    .await;

    let old_pod_uid = workload_pod_uid(&context.workloads_namespace, &candidate.sandbox_id)
        .await
        .expect("running workload Pod UID");
    kubectl(&[
        "delete",
        "pod",
        "-n",
        &context.workloads_namespace,
        "-l",
        &format!("opensandbox.io/id={}", candidate.sandbox_id.as_str()),
        "--wait=false",
    ])
    .await;
    let recreated = wait_for_recreated_runner(
        &client,
        &context.workloads_namespace,
        &candidate.sandbox_id,
        &old_pod_uid,
        &armed.boot_id,
        &signing_seed,
        &request.request_digest,
    )
    .await;
    assert_eq!(recreated.phase, SandboxRunnerPhaseV1::Armed);
    assert_eq!(recreated.sandbox_id, candidate.sandbox_id);
    assert_eq!(recreated.execution_request_digest, request.request_digest);
    delete_and_wait(&client, &candidate.sandbox_id).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn opensandbox_kubernetes_l3_persistent_candidate_create() {
    let Some(context) = L3Context::load("persistent-create") else {
        return;
    };
    let client = context.client(10_000);
    let request = request(
        &context,
        20,
        SandboxNetworkMode::Disabled,
        vec!["/opt/insight/package".to_owned(), "echo".to_owned()],
    );
    let (create, token_digest, evidence) = authorized_create(&request, 'e', 1, 120);
    for sandbox_id in list_candidates(&client, token_digest.clone()).await {
        delete_and_wait(&client, &sandbox_id).await;
    }
    let candidate = client.create_candidate(create).await.unwrap();
    assert_eq!(
        wait_for_runner(
            &client,
            &candidate.sandbox_id,
            &evidence.activation_token,
            &request.request_digest,
        )
        .await
        .phase,
        SandboxRunnerPhaseV1::Armed
    );
    assert_eq!(list_candidates(&client, token_digest).await.len(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn opensandbox_kubernetes_l3_persistent_candidate_recovers_after_provider_restart() {
    let Some(context) = L3Context::load("persistent-recover") else {
        return;
    };
    let client = context.client(10_000);
    client.lifecycle_probe().await.unwrap();
    let request = request(
        &context,
        20,
        SandboxNetworkMode::Disabled,
        vec!["/opt/insight/package".to_owned(), "echo".to_owned()],
    );
    let (_, token_digest, evidence) = authorized_create(&request, 'e', 1, 120);
    let candidates = wait_for_candidates(&client, token_digest, 1).await;
    assert_eq!(candidates.len(), 1);
    assert_eq!(
        wait_for_runner(
            &client,
            &candidates[0],
            &evidence.activation_token,
            &request.request_digest,
        )
        .await
        .phase,
        SandboxRunnerPhaseV1::Armed
    );
    delete_and_wait(&client, &candidates[0]).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn opensandbox_kubernetes_l3_ttl_removes_candidate() {
    let Some(context) = L3Context::load("ttl") else {
        return;
    };
    let client = context.client(10_000);
    let request = request(
        &context,
        30,
        SandboxNetworkMode::Disabled,
        vec!["/opt/insight/package".to_owned(), "echo".to_owned()],
    );
    let (create, _, _) = authorized_create(&request, 'f', 1, 60);
    let candidate = client.create_candidate(create).await.unwrap();
    wait_for_absence(&client, &candidate.sandbox_id, Duration::from_secs(90)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn opensandbox_kubernetes_l3_orphan_candidate_create() {
    let Some(context) = L3Context::load("orphan-create") else {
        return;
    };
    let client = context.client(10_000);
    let request = request(
        &context,
        40,
        SandboxNetworkMode::Disabled,
        vec!["/opt/insight/package".to_owned(), "echo".to_owned()],
    );
    let (create, token_digest, _) = authorized_create(&request, '1', 1, 120);
    for sandbox_id in list_candidates(&client, token_digest.clone()).await {
        delete_and_wait(&client, &sandbox_id).await;
    }
    client.create_candidate(create).await.unwrap();
    assert_eq!(wait_for_candidates(&client, token_digest, 1).await.len(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn opensandbox_kubernetes_l3_orphan_candidate_was_deleted() {
    let Some(context) = L3Context::load("orphan-verify") else {
        return;
    };
    let client = context.client(10_000);
    let request = request(
        &context,
        40,
        SandboxNetworkMode::Disabled,
        vec!["/opt/insight/package".to_owned(), "echo".to_owned()],
    );
    let (_, token_digest, _) = authorized_create(&request, '1', 1, 120);
    let deadline = Instant::now() + Duration::from_secs(45);
    loop {
        if list_candidates(&client, token_digest.clone())
            .await
            .is_empty()
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "orphan candidate was not deleted"
        );
        sleep(Duration::from_millis(250)).await;
    }
}

async fn execute(
    client: &OpenSandboxHttpClient,
    request: &SandboxExecutionRequestV1,
    token_character: char,
) -> Value {
    let (sandbox_id, output) = execute_retained(client, request, token_character).await;
    delete_and_wait(client, &sandbox_id).await;
    output
}

async fn execute_retained(
    client: &OpenSandboxHttpClient,
    request: &SandboxExecutionRequestV1,
    token_character: char,
) -> (insight_platform_sandbox::opensandbox::OpenSandboxId, Value) {
    let (create, _, evidence) = authorized_create(request, token_character, 1, 60);
    let candidate = client.create_candidate(create).await.unwrap();
    let armed = wait_for_runner(
        client,
        &candidate.sandbox_id,
        &evidence.activation_token,
        &request.request_digest,
    )
    .await;
    assert_eq!(armed.phase, SandboxRunnerPhaseV1::Armed);
    let signing_seed = evidence.activation_token;
    let activation = SandboxActivationFrameV1 {
        magic: String::new(),
        schema_version: SANDBOX_CONTRACT_SCHEMA_VERSION,
        sandbox_id: candidate.sandbox_id.clone(),
        boot_id: armed.boot_id.clone(),
        execution_request_digest: request.request_digest.clone(),
        input_schema_digest: request.input_schema_digest.clone(),
        input_digest: request.input_digest.clone(),
        declared_input_bytes: 0,
        input: request.input.clone(),
        activation_signature: ActivationSignature::parse("0".repeat(128)).unwrap(),
        frame_digest: zero_digest(),
    }
    .seal_with(&signing_seed)
    .unwrap();
    let activation_proof = SandboxRunnerRequestProofV1::for_activate(
        &signing_seed,
        &candidate.sandbox_id,
        &request.request_digest,
        &activation.canonical_bytes().unwrap(),
    )
    .unwrap();
    let activated = client
        .activate(&candidate.sandbox_id, activation, activation_proof)
        .await
        .unwrap();
    assert!(matches!(
        activated.phase,
        SandboxRunnerPhaseV1::ActivationLatched
            | SandboxRunnerPhaseV1::Started
            | SandboxRunnerPhaseV1::Succeeded
    ));
    let result = wait_for_result(
        client,
        &candidate.sandbox_id,
        &signing_seed,
        &request.request_digest,
    )
    .await;
    let frame = parse_result_frame(&result, request, &armed.boot_id).unwrap();
    let output = match frame.result {
        SandboxRunnerOutcomeV1::Succeeded { output, .. } => output,
        SandboxRunnerOutcomeV1::Failed {
            failure_class,
            diagnostic_digest,
            diagnostic_bytes,
        } => {
            panic!(
                "L3 Package failed: class={failure_class:?} diagnostic_digest={diagnostic_digest} diagnostic_bytes={diagnostic_bytes} sandbox_id={:?}",
                candidate.sandbox_id
            )
        }
    };
    (candidate.sandbox_id, output)
}

fn request(
    context: &L3Context,
    sequence: u128,
    network_mode: SandboxNetworkMode,
    package_argv: Vec<String>,
) -> SandboxExecutionRequestV1 {
    SandboxExecutionRequestV1 {
        schema_version: SANDBOX_CONTRACT_SCHEMA_VERSION,
        tenant_id: id(ResourceKind::Tenant, sequence * 100 + 1),
        invocation_id: id(ResourceKind::CapabilityInvocation, sequence * 100 + 2),
        job_id: id(ResourceKind::Job, sequence * 100 + 3),
        lease_generation: 1,
        physical_attempt: 1,
        worker_process_generation_id: id(ResourceKind::WorkerProcessGeneration, sequence * 100 + 4),
        package_version_id: id(ResourceKind::SandboxPackageRevision, sequence * 100 + 5),
        image_uri: context.image_uri.clone(),
        runtime_version_id: id(ResourceKind::SandboxRuntimeRevision, sequence * 100 + 6),
        runtime_contract_digest: context.runtime_contract_digest.clone(),
        sandbox_profile_deployment_id: id(
            ResourceKind::SandboxProfileDeployment,
            sequence * 100 + 7,
        ),
        profile_deployment_digest: digest('9'),
        runner_argv: vec!["/usr/local/bin/platform-sandbox-runner".to_owned()],
        package_argv,
        input_value_id: id(ResourceKind::RunValue, sequence * 100 + 8),
        output_value_id: id(ResourceKind::RunValue, sequence * 100 + 9),
        classification: DataClassification::Internal,
        input: json!({"sequence":sequence}),
        input_schema_digest: digest('7'),
        input_digest: zero_digest(),
        output_schema_digest: digest('8'),
        network_mode,
        limits: resource_limits(),
        provisioning_limits: provisioning_limits(),
        deadline_at: Utc.with_ymd_and_hms(2099, 1, 1, 0, 0, 0).unwrap(),
        trace: TraceIdentityV1::generate(),
        request_digest: zero_digest(),
    }
    .seal()
    .unwrap()
}

fn authorized_create(
    request: &SandboxExecutionRequestV1,
    token_character: char,
    ordinal: u8,
    ttl_seconds: u32,
) -> (OpenSandboxCreateV1, Sha256Digest, SandboxPhysicalEvidenceV1) {
    let start = Utc.with_ymd_and_hms(2098, 12, 31, 23, 0, 0).unwrap();
    let token = OpaqueActivationToken::parse(token_character.to_string().repeat(64)).unwrap();
    let mut evidence = SandboxPhysicalEvidenceV1::begin(request, token, start).unwrap();
    for next in 1..=ordinal {
        evidence = evidence
            .authorize_candidate_create(
                request,
                &request.provisioning_limits,
                start + ChronoDuration::milliseconds(i64::from(next) * 1_000),
                next,
            )
            .unwrap()
            .into_inner();
    }
    let create =
        OpenSandboxCreateV1::from_authorization(request, &evidence, ordinal, ttl_seconds).unwrap();
    (create, evidence.provisioning_token_digest.clone(), evidence)
}

fn two_authorized_creates(
    request: &SandboxExecutionRequestV1,
    token_character: char,
    ttl_seconds: u32,
) -> (
    OpenSandboxCreateV1,
    OpenSandboxCreateV1,
    Sha256Digest,
    OpaqueActivationToken,
) {
    let (first, token_digest, evidence) =
        authorized_create(request, token_character, 1, ttl_seconds);
    let (second, second_token_digest, _) =
        authorized_create(request, token_character, 2, ttl_seconds);
    assert_eq!(token_digest, second_token_digest);
    (first, second, token_digest, evidence.activation_token)
}

fn state_proof(
    signing_seed: &OpaqueActivationToken,
    sandbox_id: &insight_platform_sandbox::opensandbox::OpenSandboxId,
    execution_request_digest: &Sha256Digest,
) -> SandboxRunnerRequestProofV1 {
    SandboxRunnerRequestProofV1::for_state(signing_seed, sandbox_id, execution_request_digest)
        .unwrap()
}

async fn wait_for_runner(
    client: &OpenSandboxHttpClient,
    sandbox_id: &insight_platform_sandbox::opensandbox::OpenSandboxId,
    signing_seed: &OpaqueActivationToken,
    execution_request_digest: &Sha256Digest,
) -> insight_platform_sandbox::opensandbox::SandboxRunnerStateFrameV1 {
    let deadline = Instant::now() + Duration::from_secs(60);
    let proof = state_proof(signing_seed, sandbox_id, execution_request_digest);
    loop {
        if let Ok(frame) = client.runner_state(sandbox_id, proof.clone()).await {
            return frame;
        }
        assert!(Instant::now() < deadline, "runner did not become reachable");
        sleep(Duration::from_millis(200)).await;
    }
}

async fn wait_for_runner_phase(
    client: &OpenSandboxHttpClient,
    sandbox_id: &insight_platform_sandbox::opensandbox::OpenSandboxId,
    boot_id: &insight_platform_sandbox::opensandbox::RunnerBootId,
    expected_phase: SandboxRunnerPhaseV1,
    signing_seed: &OpaqueActivationToken,
    execution_request_digest: &Sha256Digest,
) {
    let deadline = Instant::now() + Duration::from_secs(30);
    let proof = state_proof(signing_seed, sandbox_id, execution_request_digest);
    loop {
        if client
            .runner_state(sandbox_id, proof.clone())
            .await
            .is_ok_and(|frame| frame.boot_id == *boot_id && frame.phase == expected_phase)
        {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "runner did not reach the expected phase"
        );
        sleep(Duration::from_millis(100)).await;
    }
}

async fn wait_for_recreated_runner(
    client: &OpenSandboxHttpClient,
    workloads_namespace: &str,
    sandbox_id: &insight_platform_sandbox::opensandbox::OpenSandboxId,
    old_pod_uid: &str,
    old_boot_id: &insight_platform_sandbox::opensandbox::RunnerBootId,
    signing_seed: &OpaqueActivationToken,
    execution_request_digest: &Sha256Digest,
) -> insight_platform_sandbox::opensandbox::SandboxRunnerStateFrameV1 {
    let deadline = Instant::now() + Duration::from_secs(90);
    let proof = state_proof(signing_seed, sandbox_id, execution_request_digest);
    loop {
        let pod_was_recreated = workload_pod_uid(workloads_namespace, sandbox_id)
            .await
            .is_some_and(|uid| uid != old_pod_uid);
        if pod_was_recreated {
            if let Ok(frame) = client.runner_state(sandbox_id, proof.clone()).await {
                if frame.boot_id != *old_boot_id {
                    return frame;
                }
            }
        }
        assert!(
            Instant::now() < deadline,
            "workload Pod did not return with a new runner boot"
        );
        sleep(Duration::from_millis(200)).await;
    }
}

async fn workload_pod_uid(
    workloads_namespace: &str,
    sandbox_id: &insight_platform_sandbox::opensandbox::OpenSandboxId,
) -> Option<String> {
    let output = kubectl(&[
        "get",
        "pods",
        "-n",
        workloads_namespace,
        "-l",
        &format!("opensandbox.io/id={}", sandbox_id.as_str()),
        "-o",
        "json",
    ])
    .await;
    serde_json::from_slice::<Value>(&output)
        .unwrap()
        .get("items")
        .and_then(Value::as_array)
        .and_then(|items| items.first())
        .and_then(|pod| pod.pointer("/metadata/uid"))
        .and_then(Value::as_str)
        .map(str::to_owned)
}

async fn workload_pod_name(
    workloads_namespace: &str,
    sandbox_id: &insight_platform_sandbox::opensandbox::OpenSandboxId,
) -> Option<String> {
    let output = kubectl(&[
        "get",
        "pods",
        "-n",
        workloads_namespace,
        "-l",
        &format!("opensandbox.io/id={}", sandbox_id.as_str()),
        "-o",
        "json",
    ])
    .await;
    serde_json::from_slice::<Value>(&output)
        .unwrap()
        .get("items")
        .and_then(Value::as_array)
        .and_then(|items| items.first())
        .and_then(|pod| pod.pointer("/metadata/name"))
        .and_then(Value::as_str)
        .map(str::to_owned)
}

async fn kubectl(arguments: &[&str]) -> Vec<u8> {
    let output = tokio::process::Command::new("kubectl")
        .args(arguments)
        .output()
        .await
        .unwrap();
    assert!(
        output.status.success(),
        "kubectl failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    output.stdout
}

async fn wait_for_result(
    client: &OpenSandboxHttpClient,
    sandbox_id: &insight_platform_sandbox::opensandbox::OpenSandboxId,
    signing_seed: &OpaqueActivationToken,
    execution_request_digest: &Sha256Digest,
) -> Vec<u8> {
    let deadline = Instant::now() + Duration::from_secs(60);
    let proof =
        SandboxRunnerRequestProofV1::for_result(signing_seed, sandbox_id, execution_request_digest)
            .unwrap();
    loop {
        if let Ok(result) = client
            .read_result(sandbox_id, 1_114_112, proof.clone())
            .await
        {
            return result;
        }
        assert!(
            Instant::now() < deadline,
            "runner result did not become ready"
        );
        sleep(Duration::from_millis(100)).await;
    }
}

async fn wait_for_candidates(
    client: &OpenSandboxHttpClient,
    token_digest: Sha256Digest,
    expected: usize,
) -> Vec<insight_platform_sandbox::opensandbox::OpenSandboxId> {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let candidates = list_candidates(client, token_digest.clone()).await;
        if candidates.len() == expected {
            return candidates;
        }
        assert!(
            candidates.len() < expected && Instant::now() < deadline,
            "candidate count did not converge: expected {expected}, got {}",
            candidates.len()
        );
        sleep(Duration::from_millis(200)).await;
    }
}

async fn list_candidates(
    client: &OpenSandboxHttpClient,
    token_digest: Sha256Digest,
) -> Vec<insight_platform_sandbox::opensandbox::OpenSandboxId> {
    let mut cursor = CandidateCursorV1 {
        schema_version: SANDBOX_CONTRACT_SCHEMA_VERSION,
        opaque: None,
    };
    let mut candidates = Vec::new();
    loop {
        let page = client
            .list_candidates(token_digest.clone(), cursor)
            .await
            .unwrap();
        candidates.extend(page.items.into_iter().map(|candidate| candidate.sandbox_id));
        let Some(next) = page.next else {
            return candidates;
        };
        cursor = next;
    }
}

async fn delete_and_wait(
    client: &OpenSandboxHttpClient,
    sandbox_id: &insight_platform_sandbox::opensandbox::OpenSandboxId,
) {
    let _ = client.terminate(sandbox_id).await;
    wait_for_absence(client, sandbox_id, Duration::from_secs(30)).await;
}

async fn wait_for_absence(
    client: &OpenSandboxHttpClient,
    sandbox_id: &insight_platform_sandbox::opensandbox::OpenSandboxId,
    timeout: Duration,
) {
    let deadline = Instant::now() + timeout;
    loop {
        if client
            .prove_absent(sandbox_id)
            .await
            .is_ok_and(|observation| !observation.present)
        {
            return;
        }
        assert!(Instant::now() < deadline, "sandbox did not become absent");
        sleep(Duration::from_millis(200)).await;
    }
}

fn resource_limits() -> SandboxResourceLimitsV1 {
    SandboxResourceLimitsV1 {
        maximum_input_bytes: 65_536,
        maximum_output_bytes: 65_536,
        cpu_millicores: 500,
        memory_mebibytes: 128,
        pids: 64,
        ephemeral_storage_bytes: 67_108_864,
        wall_milliseconds: 120_000,
        cleanup_milliseconds: 10_000,
    }
}

fn provisioning_limits() -> SandboxProvisioningLimitsV1 {
    SandboxProvisioningLimitsV1 {
        maximum_candidates: 2,
        candidate_page_items: 4,
        candidate_quiescence_milliseconds: 500,
        provisioning_timeout_milliseconds: 10_000,
        orphan_page_items: 20,
        runner_header_bytes: 8_192,
        diagnostic_bytes: 8_192,
    }
}

fn id(kind: ResourceKind, sequence: u128) -> ResourceId {
    let raw = (sequence & ((1_u128 << 74) - 1)) | (7_u128 << 76) | (2_u128 << 62);
    ResourceId::from_uuid_v7(kind, Uuid::from_u128(raw)).unwrap()
}

fn digest(character: char) -> Sha256Digest {
    format!("sha256:{}", character.to_string().repeat(64))
        .parse()
        .unwrap()
}

fn zero_digest() -> Sha256Digest {
    digest('0')
}
