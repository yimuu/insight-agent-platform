use super::*;
use insight_platform_deployment_tooling::installation::{compose_input, PreparedInstallation};
use std::{cell::RefCell, collections::VecDeque};

fn fixture() -> (tempfile::TempDir, InstallationInputV1, PreparedInstallation) {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().canonicalize().unwrap().join("installation");
    let input = compose_input(
        "s3-recovery-test",
        format!("sha256:{}", "a".repeat(64)).parse().unwrap(),
    )
    .unwrap();
    let prepared = PreparedInstallation::prepare(&input, &root).unwrap();
    (temp, input, prepared)
}
fn credential_digest() -> Sha256Digest {
    bytes_digest(b"unit-test-only-credential-evidence").unwrap()
}
fn journal(directory: &InstallationDirectory) -> Journal {
    serde_json::from_slice(
        &directory
            .read(JOURNAL_FILE, JOURNAL_LIMIT)
            .unwrap()
            .unwrap(),
    )
    .unwrap()
}
#[derive(Default)]
struct Memory {
    bucket: Option<Bucket>,
    calls: Vec<&'static str>,
    fault: Option<(&'static str, bool)>,
    known_failure: bool,
    read_faults: VecDeque<Option<Error>>,
    read_delay: Duration,
}
struct Fake<'a> {
    directory: &'a InstallationDirectory,
    memory: RefCell<Memory>,
}
impl<'a> Fake<'a> {
    fn new(directory: &'a InstallationDirectory) -> Self {
        Self {
            directory,
            memory: RefCell::new(Memory::default()),
        }
    }
    fn before(&self, operation: &'static str, phase: Phase) -> Option<bool> {
        assert_eq!(
            journal(self.directory).phase,
            phase,
            "write must follow durable exact intent"
        );
        let mut memory = self.memory.borrow_mut();
        memory.calls.push(operation);
        if memory.fault.is_some_and(|fault| fault.0 == operation) {
            memory.fault.take().map(|fault| fault.1)
        } else {
            None
        }
    }
    fn writes(&self) -> Vec<&'static str> {
        self.memory
            .borrow()
            .calls
            .iter()
            .copied()
            .filter(|call| *call != "read")
            .collect()
    }
    fn fail(&self, operation: &'static str, after: bool) {
        self.memory.borrow_mut().fault = Some((operation, after));
    }
    fn fault(&self) -> Error {
        if self.memory.borrow().known_failure {
            Error::ForeignState
        } else {
            Error::ExternalOutcomeUnknown
        }
    }
}
impl Api for Fake<'_> {
    async fn bucket(&self, _: &str) -> Result<Option<Bucket>, Error> {
        let (delay, fault) = {
            let mut memory = self.memory.borrow_mut();
            memory.calls.push("read");
            (memory.read_delay, memory.read_faults.pop_front().flatten())
        };
        if !delay.is_zero() {
            tokio::time::sleep(delay).await;
        }
        if let Some(error) = fault {
            return Err(error);
        }
        Ok(self.memory.borrow().bucket.clone())
    }
    async fn create_bucket(&self, _: &str) -> Result<(), Error> {
        let fault = self.before("create", Phase::CreateRequested);
        if fault == Some(false) {
            return Err(self.fault());
        }
        self.memory.borrow_mut().bucket = Some(Bucket {
            tags: None,
            version: None,
            cors: None,
        });
        if fault.is_some() {
            return Err(self.fault());
        }
        Ok(())
    }
    async fn tag_bucket(&self, _: &str, tags: &BTreeMap<String, String>) -> Result<(), Error> {
        let fault = self.before("tag", Phase::TagRequested);
        if fault == Some(false) {
            return Err(self.fault());
        }
        self.memory.borrow_mut().bucket.as_mut().unwrap().tags = Some(tags.clone());
        if fault.is_some() {
            return Err(self.fault());
        }
        Ok(())
    }
    async fn version_bucket(&self, _: &str) -> Result<(), Error> {
        let fault = self.before("version", Phase::VersionRequested);
        if fault == Some(false) {
            return Err(self.fault());
        }
        self.memory.borrow_mut().bucket.as_mut().unwrap().version = Some("Enabled".into());
        if fault.is_some() {
            return Err(self.fault());
        }
        Ok(())
    }
    async fn cors_bucket(&self, _: &str, cors: &Cors) -> Result<(), Error> {
        let fault = self.before("cors", Phase::CorsRequested);
        if fault == Some(false) {
            return Err(self.fault());
        }
        self.memory.borrow_mut().bucket.as_mut().unwrap().cors = Some(vec![cors.clone()]);
        if fault.is_some() {
            return Err(self.fault());
        }
        Ok(())
    }
}
async fn provision(
    input: &InstallationInputV1,
    prepared: &PreparedInstallation,
    fake: &Fake<'_>,
) -> Result<String, Error> {
    ensure_with_api(
        input,
        prepared.identity(),
        prepared.directory(),
        credential_digest(),
        Mode::Provision,
        fake,
    )
    .await
}

#[tokio::test]
async fn complete_replay_and_verify_are_readonly_and_preserve_journal_bytes() {
    let (_temp, input, prepared) = fixture();
    let fake = Fake::new(prepared.directory());
    let bucket = provision(&input, &prepared, &fake).await.unwrap();
    assert_eq!(
        bucket,
        format!(
            "insight-platform-artifacts-{}",
            prepared.identity().installation_id.uuid().simple()
        )
    );
    assert_eq!(fake.writes(), ["create", "tag", "version", "cors"]);
    let before = prepared
        .directory()
        .read(JOURNAL_FILE, JOURNAL_LIMIT)
        .unwrap();
    for mode in [Mode::Provision, Mode::Verify] {
        assert_eq!(
            ensure_with_api(
                &input,
                prepared.identity(),
                prepared.directory(),
                credential_digest(),
                mode,
                &fake
            )
            .await
            .unwrap(),
            bucket
        );
        assert_eq!(fake.writes(), ["create", "tag", "version", "cors"]);
        assert_eq!(
            prepared
                .directory()
                .read(JOURNAL_FILE, JOURNAL_LIMIT)
                .unwrap(),
            before
        );
    }
}

#[tokio::test(start_paused = true)]
async fn delayed_initial_readiness_preserves_one_shot_writes_and_completed_journals() {
    let (_temp, input, prepared) = fixture();
    let fake = Fake::new(prepared.directory());
    fake.memory.borrow_mut().read_faults =
        VecDeque::from([Some(Error::PrerequisiteUnavailable); 2]);
    let started = tokio::time::Instant::now();
    provision(&input, &prepared, &fake).await.unwrap();
    assert_eq!(started.elapsed(), Duration::from_secs(2));
    assert_eq!(fake.writes(), ["create", "tag", "version", "cors"]);
    let before = prepared
        .directory()
        .read(JOURNAL_FILE, JOURNAL_LIMIT)
        .unwrap();
    for mode in [Mode::Provision, Mode::Verify] {
        {
            let mut memory = fake.memory.borrow_mut();
            memory.calls.clear();
            memory.read_faults = VecDeque::from([Some(Error::PrerequisiteUnavailable); 2]);
        }
        ensure_with_api(
            &input,
            prepared.identity(),
            prepared.directory(),
            credential_digest(),
            mode,
            &fake,
        )
        .await
        .unwrap();
        assert!(fake.writes().is_empty());
        assert_eq!(
            prepared
                .directory()
                .read(JOURNAL_FILE, JOURNAL_LIMIT)
                .unwrap(),
            before
        );
    }
}

#[tokio::test(start_paused = true)]
async fn unavailable_or_hanging_initial_read_has_one_deadline_and_no_external_effects() {
    for hanging in [false, true] {
        let (_temp, input, prepared) = fixture();
        let fake = Fake::new(prepared.directory());
        {
            let mut memory = fake.memory.borrow_mut();
            if hanging {
                memory.read_delay = Duration::from_secs(60);
            } else {
                memory.read_faults = VecDeque::from([Some(Error::PrerequisiteUnavailable); 64]);
            }
        }
        let started = tokio::time::Instant::now();
        assert_eq!(
            provision(&input, &prepared, &fake).await,
            Err(Error::PrerequisiteUnavailable)
        );
        assert_eq!(started.elapsed(), Duration::from_secs(30));
        assert_eq!(journal(prepared.directory()).phase, Phase::Planned);
        assert!(fake.writes().is_empty());
        if hanging {
            assert_eq!(fake.memory.borrow().calls, ["read"]);
        }
    }
}

#[tokio::test(start_paused = true)]
async fn fatal_initial_read_and_post_write_unavailability_are_not_reobserved() {
    for failure in [
        Error::CredentialInvalid,
        Error::IdentityDrift,
        Error::ForeignState,
        Error::ConfigurationDrift,
        Error::ExternalOutcomeUnknown,
    ] {
        let (_temp, input, prepared) = fixture();
        let fake = Fake::new(prepared.directory());
        fake.memory
            .borrow_mut()
            .read_faults
            .push_back(Some(failure));
        assert_eq!(provision(&input, &prepared, &fake).await, Err(failure));
        assert_eq!(fake.memory.borrow().calls, ["read"]);
        assert!(fake.writes().is_empty());
    }
    let (_temp, input, prepared) = fixture();
    let fake = Fake::new(prepared.directory());
    fake.memory.borrow_mut().read_faults =
        VecDeque::from([None, Some(Error::PrerequisiteUnavailable)]);
    let started = tokio::time::Instant::now();
    assert_eq!(
        provision(&input, &prepared, &fake).await,
        Err(Error::PrerequisiteUnavailable)
    );
    assert!(started.elapsed().is_zero());
    assert_eq!(fake.memory.borrow().calls, ["read", "create", "read"]);
    assert_eq!(journal(prepared.directory()).phase, Phase::Created);
}

#[tokio::test(start_paused = true)]
async fn delayed_observation_does_not_renew_an_uncertain_create_intent() {
    for after in [false, true] {
        let (_temp, input, prepared) = fixture();
        let fake = Fake::new(prepared.directory());
        fake.fail("create", after);
        assert_eq!(
            provision(&input, &prepared, &fake).await,
            Err(Error::ExternalOutcomeUnknown)
        );
        let before = prepared
            .directory()
            .read(JOURNAL_FILE, JOURNAL_LIMIT)
            .unwrap();
        fake.memory.borrow_mut().read_faults =
            VecDeque::from([Some(Error::PrerequisiteUnavailable); 2]);
        assert_eq!(
            provision(&input, &prepared, &fake).await,
            Err(Error::ExternalOutcomeUnknown)
        );
        assert_eq!(fake.writes(), ["create"]);
        assert_eq!(
            prepared
                .directory()
                .read(JOURNAL_FILE, JOURNAL_LIMIT)
                .unwrap(),
            before
        );
    }
}
#[tokio::test]
async fn create_response_loss_never_adopts_untagged_bucket_or_recreates_missing_bucket() {
    for after in [false, true] {
        let (_temp, input, prepared) = fixture();
        let fake = Fake::new(prepared.directory());
        fake.fail("create", after);
        assert!(matches!(
            provision(&input, &prepared, &fake).await,
            Err(Error::ExternalOutcomeUnknown)
        ));
        assert_eq!(journal(prepared.directory()).phase, Phase::CreateRequested);
        assert!(matches!(
            provision(&input, &prepared, &fake).await,
            Err(Error::ExternalOutcomeUnknown)
        ));
        assert_eq!(fake.writes(), ["create"]);
    }
}
#[tokio::test]
async fn interrupted_puts_require_exact_readback_and_never_repeat_missing_effects() {
    for operation in ["tag", "version", "cors"] {
        for after in [false, true] {
            let (_temp, input, prepared) = fixture();
            let fake = Fake::new(prepared.directory());
            fake.fail(operation, after);
            assert!(matches!(
                provision(&input, &prepared, &fake).await,
                Err(Error::ExternalOutcomeUnknown)
            ));
            let result = provision(&input, &prepared, &fake).await;
            if after {
                assert!(result.is_ok());
            } else {
                assert!(matches!(result, Err(Error::ExternalOutcomeUnknown)));
            }
            assert_eq!(
                fake.writes()
                    .iter()
                    .filter(|call| **call == operation)
                    .count(),
                1
            );
        }
    }
}
#[tokio::test]
async fn foreign_bucket_and_credential_drift_are_not_adopted() {
    let (_temp, input, prepared) = fixture();
    let fake = Fake::new(prepared.directory());
    fake.memory.borrow_mut().bucket = Some(Bucket {
        tags: None,
        version: None,
        cors: None,
    });
    assert!(matches!(
        provision(&input, &prepared, &fake).await,
        Err(Error::ForeignState)
    ));
    assert!(fake.writes().is_empty());
    fake.memory.borrow_mut().bucket = None;
    provision(&input, &prepared, &fake).await.unwrap();
    let calls = fake.memory.borrow().calls.clone();
    assert!(matches!(
        ensure_with_api(
            &input,
            prepared.identity(),
            prepared.directory(),
            bytes_digest(b"changed").unwrap(),
            Mode::Provision,
            &fake
        )
        .await,
        Err(Error::IdentityDrift)
    ));
    assert_eq!(fake.memory.borrow().calls, calls);
}
#[tokio::test]
async fn configured_missing_or_changed_provider_state_never_repairs() {
    for drift in ["missing", "tags", "version", "cors"] {
        let (_temp, input, prepared) = fixture();
        let fake = Fake::new(prepared.directory());
        provision(&input, &prepared, &fake).await.unwrap();
        {
            let mut memory = fake.memory.borrow_mut();
            match drift {
                "missing" => memory.bucket = None,
                "tags" => memory.bucket.as_mut().unwrap().tags = None,
                "version" => memory.bucket.as_mut().unwrap().version = Some("Suspended".into()),
                _ => memory.bucket.as_mut().unwrap().cors = None,
            }
        }
        let before = prepared
            .directory()
            .read(JOURNAL_FILE, JOURNAL_LIMIT)
            .unwrap();
        for mode in [Mode::Provision, Mode::Verify] {
            assert!(ensure_with_api(
                &input,
                prepared.identity(),
                prepared.directory(),
                credential_digest(),
                mode,
                &fake
            )
            .await
            .is_err());
            assert_eq!(fake.writes(), ["create", "tag", "version", "cors"]);
            assert_eq!(
                prepared
                    .directory()
                    .read(JOURNAL_FILE, JOURNAL_LIMIT)
                    .unwrap(),
                before
            );
        }
    }
}
#[tokio::test]
async fn verify_missing_journal_and_known_write_failure_never_become_ready() {
    let (_temp, input, prepared) = fixture();
    let fake = Fake::new(prepared.directory());
    assert!(matches!(
        ensure_with_api(
            &input,
            prepared.identity(),
            prepared.directory(),
            credential_digest(),
            Mode::Verify,
            &fake
        )
        .await,
        Err(Error::Incomplete)
    ));
    assert!(fake.memory.borrow().calls.is_empty());
    assert!(prepared
        .directory()
        .read(JOURNAL_FILE, JOURNAL_LIMIT)
        .unwrap()
        .is_none());
    fake.memory.borrow_mut().known_failure = true;
    fake.fail("create", false);
    assert!(matches!(
        provision(&input, &prepared, &fake).await,
        Err(Error::ForeignState)
    ));
    assert_eq!(journal(prepared.directory()).phase, Phase::CreateRequested);
    assert!(matches!(
        provision(&input, &prepared, &fake).await,
        Err(Error::ExternalOutcomeUnknown)
    ));
    assert_eq!(fake.writes(), ["create"]);
}
#[test]
fn tls_factory_accepts_only_exact_frozen_ca_and_closed_environment_paths() {
    let (_temp, _input, prepared) = fixture();
    let directory = prepared.directory();
    let ca = directory.path("ca.pem").unwrap();
    assert!(tls_environment(
        directory,
        prepared.identity(),
        Some(ca.as_os_str()),
        Some(OsStr::new("/etc/ssl/certs"))
    )
    .is_ok());
    for (file, roots) in [
        (None, Some(OsStr::new("/etc/ssl/certs"))),
        (
            Some(OsStr::new("/tmp/foreign-ca.pem")),
            Some(OsStr::new("/etc/ssl/certs")),
        ),
        (Some(ca.as_os_str()), None),
    ] {
        assert!(tls_environment(directory, prepared.identity(), file, roots).is_err());
    }
    directory
        .replace("ca.pem", b"different certificate")
        .unwrap();
    assert!(matches!(
        tls_environment(
            directory,
            prepared.identity(),
            Some(ca.as_os_str()),
            Some(OsStr::new("/etc/ssl/certs"))
        ),
        Err(Error::IdentityDrift)
    ));
}
#[test]
fn diagnostics_never_format_sdk_error_or_change_outcome() {
    use aws_sdk_s3::error::{ConnectorError, SdkError};
    const CANARY: &str = "credential https://user:password@secret.invalid";
    type Failure = SdkError<String, String>;
    for (error, class) in [
        (
            Failure::construction_failure(std::io::Error::other(CANARY)),
            "unknown",
        ),
        (
            Failure::timeout_error(std::io::Error::other(CANARY)),
            "timeout",
        ),
        (
            Failure::dispatch_failure(ConnectorError::io(Box::new(std::io::Error::other(CANARY)))),
            "dispatch",
        ),
        (
            Failure::dispatch_failure(ConnectorError::timeout(Box::new(std::io::Error::other(
                CANARY,
            )))),
            "timeout",
        ),
        (
            Failure::response_error(std::io::Error::other(CANARY), CANARY.into()),
            "invalid_response",
        ),
        (
            Failure::service_error(CANARY.into(), CANARY.into()),
            "service",
        ),
    ] {
        assert_eq!(
            diagnostic(Operation::CreateBucket, &error),
            format!("installation_s3 operation=s3_create_bucket failure={class}")
        );
        assert!(matches!(
            failure(
                Operation::CreateBucket,
                &error,
                Error::ExternalOutcomeUnknown
            ),
            Error::ExternalOutcomeUnknown
        ));
        assert!(!diagnostic(Operation::CreateBucket, &error).contains(CANARY));
    }
}

#[tokio::test]
async fn sdk_factory_signs_with_only_explicit_initializer_credentials_and_bounded_exact_endpoint() {
    use std::sync::{Arc, Mutex};
    #[derive(Debug, Clone)]
    struct BeforeNetwork(Arc<Mutex<Vec<(String, String)>>>);
    impl aws_sdk_s3::config::Intercept for BeforeNetwork {
        fn name(&self) -> &'static str {
            "s3-qualification-before-network"
        }
        fn read_before_transmit(
            &self,
            context: &aws_sdk_s3::config::interceptors::BeforeTransmitInterceptorContextRef<'_>,
            _runtime: &aws_sdk_s3::config::RuntimeComponents,
            _config: &mut aws_sdk_s3::config::ConfigBag,
        ) -> Result<(), aws_sdk_s3::error::BoxError> {
            self.0.lock().unwrap().push((
                context.request().uri().to_string(),
                context
                    .request()
                    .headers()
                    .get("authorization")
                    .unwrap()
                    .to_owned(),
            ));
            Err(std::io::Error::other("test stops before external network").into())
        }
    }
    let (_temp, input, _prepared) = fixture();
    let raw = format!(
        "[default]\naws_access_key_id={}\naws_secret_access_key={}\n",
        "a".repeat(32),
        "b".repeat(64)
    );
    let credentials = S3RoleCredentials::decode(raw.as_bytes()).unwrap();
    let sdk = Sdk::new(&input, &credentials);
    assert_eq!(
        sdk.client.config().retry_config().unwrap().max_attempts(),
        1
    );
    let timeouts = sdk.client.config().timeout_config().unwrap();
    assert_eq!(timeouts.connect_timeout(), Some(Duration::from_secs(3)));
    assert_eq!(timeouts.operation_timeout(), Some(Duration::from_secs(10)));
    assert_eq!(
        timeouts.operation_attempt_timeout(),
        Some(Duration::from_secs(10))
    );
    let observed = Arc::new(Mutex::new(Vec::new()));
    assert!(sdk
        .client
        .head_bucket()
        .bucket("installation-probe")
        .customize()
        .interceptor(BeforeNetwork(Arc::clone(&observed)))
        .send()
        .await
        .is_err());
    let observed = observed.lock().unwrap();
    assert_eq!(observed.len(), 1);
    assert_eq!(
        observed[0].0,
        format!(
            "{}/installation-probe/",
            input.network.providers.artifact().as_str()
        )
    );
    assert!(observed[0]
        .1
        .contains(&format!("Credential={}/", "a".repeat(32))));
    assert!(observed[0].1.contains("/us-east-1/s3/aws4_request"));
}
