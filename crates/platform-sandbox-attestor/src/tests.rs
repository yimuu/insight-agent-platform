use super::*;
use insight_platform_contracts::{ResourceId, ResourceKind};
use std::sync::atomic::{AtomicU8, Ordering};

fn id(kind: ResourceKind, suffix: u16) -> ResourceId {
    format!(
        "{}_0198f1d0-32e4-75e1-a9e8-d95ca0f4{suffix:04x}",
        kind.descriptor().prefix
    )
    .parse()
    .unwrap()
}

fn digest(character: char) -> Sha256Digest {
    format!("sha256:{}", character.to_string().repeat(64))
        .parse()
        .unwrap()
}

fn request(suffix: u16) -> RegisterWasiExecutorProcessGeneration {
    RegisterWasiExecutorProcessGeneration {
        worker_process_generation_id: id(ResourceKind::WorkerProcessGeneration, suffix),
        worker_manifest_digest: digest('a'),
        isolation_backend_contract_digest: digest('b'),
    }
}

fn peer() -> WasiExecutorRegistrationPeer {
    WasiExecutorRegistrationPeer {
        host_process_id: 42,
        host_user_id: 1000,
        host_group_id: 1000,
    }
}

fn observed(start_ticks: u64) -> ObservedExecutorProcessIdentity {
    ObservedExecutorProcessIdentity {
        schema_version: 1,
        node_uid: "0198f1d0-32e4-75e1-a9e8-d95ca0f40001".to_owned(),
        pod_uid: "0198f1d0-32e4-75e1-a9e8-d95ca0f40002".to_owned(),
        runtime_cgroup_locator:
            "0::/kubepods.slice/pod0198f1d0_32e4_75e1_a9e8_d95ca0f40002/executor.scope".to_owned(),
        boot_id: "0198f1d0-32e4-75e1-a9e8-d95ca0f40003".to_owned(),
        pid_namespace_inode: 73,
        host_process_id: 42,
        host_user_id: 1000,
        host_group_id: 1000,
        process_start_ticks: start_ticks,
    }
}

struct SwitchingObserver {
    state: AtomicU8,
}

impl SwitchingObserver {
    fn live() -> Self {
        Self {
            state: AtomicU8::new(0),
        }
    }

    fn set_reused(&self) {
        self.state.store(1, Ordering::Release);
    }

    fn set_absent(&self) {
        self.state.store(2, Ordering::Release);
    }
}

#[async_trait]
impl ExecutorProcessObserver for SwitchingObserver {
    async fn observe(
        &self,
        _peer: WasiExecutorRegistrationPeer,
    ) -> Result<ObservedExecutorProcessIdentity, ProcessObservationError> {
        match self.state.load(Ordering::Acquire) {
            0 => Ok(observed(91)),
            1 => Ok(observed(92)),
            _ => Err(ProcessObservationError::Missing),
        }
    }
}

fn config(path: PathBuf) -> NodeAttestorConfig {
    NodeAttestorConfig {
        attestor_identity_digest: digest('c'),
        attestor_route: "https://10.0.0.7:9443".parse().unwrap(),
        registry_path: path,
        maximum_registrations: 4,
        absent_retention: std::time::Duration::from_secs(3_900),
    }
}

#[tokio::test]
async fn registration_is_observed_replayable_and_survives_attestor_restart() {
    let directory = tempfile::tempdir().unwrap();
    let registry_path = directory.path().join("registrations.json");
    let observer = Arc::new(SwitchingObserver::live());
    let attestor =
        NodeProcessAttestor::open(Arc::clone(&observer), config(registry_path.clone())).unwrap();
    let request = request(1);
    let first = attestor
        .register_observed(request.clone(), peer())
        .await
        .unwrap();
    let replay = attestor
        .register_observed(request.clone(), peer())
        .await
        .unwrap();
    assert_eq!(first, replay);
    assert_ne!(
        first.executor_identity_digest,
        request.worker_manifest_digest
    );

    let restarted = NodeProcessAttestor::open(observer, config(registry_path)).unwrap();
    let verified = restarted
        .verify_registered(VerifyWasiExecutorProcessGeneration {
            worker_process_generation_id: request.worker_process_generation_id,
            worker_manifest_digest: request.worker_manifest_digest,
            isolation_backend_contract_digest: request.isolation_backend_contract_digest,
            executor_identity_digest: first.executor_identity_digest.clone(),
            attestor_route: first.attestor_route.clone(),
        })
        .await
        .unwrap();
    assert_eq!(
        verified.executor_identity_digest,
        first.executor_identity_digest
    );
}

#[tokio::test]
async fn same_observed_process_cannot_bind_two_generations() {
    let directory = tempfile::tempdir().unwrap();
    let observer = Arc::new(SwitchingObserver::live());
    let attestor = NodeProcessAttestor::open(
        observer,
        config(directory.path().join("registrations.json")),
    )
    .unwrap();
    attestor
        .register_observed(request(1), peer())
        .await
        .unwrap();
    assert_eq!(
        attestor.register_observed(request(2), peer()).await,
        Err(WasiExecutorProcessRegistrationError::Rejected)
    );
}

#[tokio::test]
async fn pid_reuse_is_absence_but_never_a_live_registration() {
    let directory = tempfile::tempdir().unwrap();
    let observer = Arc::new(SwitchingObserver::live());
    let attestor = NodeProcessAttestor::open(
        Arc::clone(&observer),
        config(directory.path().join("registrations.json")),
    )
    .unwrap();
    let registration_request = request(1);
    let registered = attestor
        .register_observed(registration_request.clone(), peer())
        .await
        .unwrap();
    let verify = VerifyWasiExecutorProcessGeneration {
        worker_process_generation_id: registration_request.worker_process_generation_id.clone(),
        worker_manifest_digest: registration_request.worker_manifest_digest,
        isolation_backend_contract_digest: registration_request.isolation_backend_contract_digest,
        executor_identity_digest: registered.executor_identity_digest.clone(),
        attestor_route: registered.attestor_route.clone(),
    };
    let absence_request = ProveSandboxProcessGenerationAbsent {
        tenant_id: id(ResourceKind::Tenant, 10),
        sandbox_job_id: id(ResourceKind::SandboxJob, 11),
        request_digest: digest('d'),
        previous_worker_process_generation_id: registration_request.worker_process_generation_id,
        executor_identity_digest: registered.executor_identity_digest,
        attestor_route: registered.attestor_route,
    };
    assert_eq!(
        attestor.prove_absent(absence_request.clone()).await,
        Err(SandboxProcessGenerationIsolationError::StillLive)
    );

    observer.set_reused();
    assert_eq!(
        attestor.verify_registered(verify).await,
        Err(WasiExecutorProcessRegistrationError::Rejected)
    );
    let evidence = attestor
        .prove_absent(absence_request.clone())
        .await
        .unwrap();
    evidence.validate_for(&absence_request, Utc::now()).unwrap();
    assert_eq!(
        evidence.disposition,
        SandboxProcessGenerationIsolationDisposition::ProcessAbsent
    );

    observer.set_absent();
    let replay = attestor.prove_absent(absence_request).await.unwrap();
    assert_eq!(replay, evidence);
}

#[tokio::test]
async fn persisted_registry_is_bound_to_the_configured_attestor_identity() {
    let directory = tempfile::tempdir().unwrap();
    let registry_path = directory.path().join("registrations.json");
    let observer = Arc::new(SwitchingObserver::live());
    let attestor =
        NodeProcessAttestor::open(Arc::clone(&observer), config(registry_path.clone())).unwrap();
    attestor
        .register_observed(request(1), peer())
        .await
        .unwrap();
    drop(attestor);

    let mut changed_identity = config(registry_path);
    changed_identity.attestor_identity_digest = digest('d');
    assert!(matches!(
        NodeProcessAttestor::open(observer, changed_identity),
        Err(NodeAttestorError::RegistryCorrupt)
    ));
}

#[test]
fn procfs_parser_binds_start_ticks_cgroup_pod_and_pid_namespace() {
    let stat = "42 (executor with spaces) S 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 424242 20";
    assert_eq!(parse_process_start_ticks(stat).unwrap(), 424242);
    assert_eq!(
        parse_process_credentials(
            "Name:\texecutor\nUid:\t1000\t1000\t1000\t1000\nGid:\t1001\t1001\t1001\t1001\n"
        )
        .unwrap(),
        (1000, 1001)
    );
    let cgroup = "0::/kubepods.slice/pod0198f1d0_32e4_75e1_a9e8_d95ca0f40002/executor.scope\n";
    let locator = parse_unified_cgroup(cgroup).unwrap();
    assert_eq!(
        extract_pod_uid(&locator).unwrap(),
        "0198f1d0-32e4-75e1-a9e8-d95ca0f40002"
    );
    assert_eq!(
        parse_namespace_inode(Path::new("pid:[4026531836]")).unwrap(),
        4_026_531_836
    );
}

#[test]
fn corrupt_registry_fails_closed() {
    let directory = tempfile::tempdir().unwrap();
    let registry_path = directory.path().join("registrations.json");
    std::fs::write(&registry_path, b"{\"schema_version\":1}").unwrap();
    assert!(matches!(
        NodeProcessAttestor::open(Arc::new(SwitchingObserver::live()), config(registry_path)),
        Err(NodeAttestorError::RegistryCorrupt)
    ));
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn linux_observer_tracks_a_real_process_start_identity_and_then_proves_it_missing() {
    use std::os::unix::fs::symlink;
    use std::process::Command;

    let mut child = Command::new("/bin/sleep").arg("30").spawn().unwrap();
    let process_id = child.id();
    let real_process_root = PathBuf::from(format!("/proc/{process_id}"));
    let status = std::fs::read_to_string(real_process_root.join("status")).unwrap();
    let (user_id, group_id) = parse_process_credentials(&status).unwrap();
    let namespace_target = std::fs::read_link(real_process_root.join("ns/pid")).unwrap();

    let directory = tempfile::tempdir().unwrap();
    let proc_root = directory.path().join("proc");
    let fixture_process_root = proc_root.join(process_id.to_string());
    std::fs::create_dir_all(fixture_process_root.join("ns")).unwrap();
    std::fs::create_dir_all(proc_root.join("sys/kernel/random")).unwrap();
    symlink(
        real_process_root.join("stat"),
        fixture_process_root.join("stat"),
    )
    .unwrap();
    symlink(
        real_process_root.join("status"),
        fixture_process_root.join("status"),
    )
    .unwrap();
    symlink(namespace_target, fixture_process_root.join("ns/pid")).unwrap();
    std::fs::write(
        fixture_process_root.join("cgroup"),
        "0::/kubepods.slice/pod0198f1d0_32e4_75e1_a9e8_d95ca0f40002/executor.scope\n",
    )
    .unwrap();
    std::fs::write(
        proc_root.join("sys/kernel/random/boot_id"),
        "0198f1d0-32e4-75e1-a9e8-d95ca0f40003\n",
    )
    .unwrap();
    let node_uid = directory.path().join("node-uid");
    std::fs::write(&node_uid, "0198f1d0-32e4-75e1-a9e8-d95ca0f40004\n").unwrap();

    let observer = LinuxProcfsExecutorProcessObserver::new(proc_root, node_uid).unwrap();
    let peer = WasiExecutorRegistrationPeer {
        host_process_id: process_id,
        host_user_id: user_id,
        host_group_id: group_id,
    };
    let observed = observer.observe(peer).await.unwrap();
    assert_eq!(observed.host_process_id, process_id);
    assert!(observed.process_start_ticks > 0);

    child.kill().unwrap();
    child.wait().unwrap();
    assert_eq!(
        observer.observe(peer).await,
        Err(ProcessObservationError::Missing)
    );
}
