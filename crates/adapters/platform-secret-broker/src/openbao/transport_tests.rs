//! Synthetic HTTPS recovery evidence only; no claim of actual Bao or PostgreSQL qualification.
use super::*;
use crate::provider_config::tests::bao_config;
use insight_platform_contracts::{ModelCredentialImportError, ModelCredentialImportIdentityV1};
use insight_platform_security::{ModelCredentialImportAuthority, ModelCredentialImporter};
use std::{
    fs,
    os::unix::fs::{DirBuilderExt, PermissionsExt},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
};

struct Fixture {
    directory: PathBuf,
    child: Option<Child>,
}

impl Drop for Fixture {
    fn drop(&mut self) {
        if let Some(child) = &mut self.child {
            let _ = child.kill();
            let _ = child.wait();
        }
        let _ = fs::remove_dir_all(&self.directory);
    }
}

fn openssl(path: &Path, args: &[&str]) {
    assert!(Command::new("openssl")
        .args(args)
        .current_dir(path)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .expect("fixture openssl available")
        .success());
}

impl Fixture {
    async fn start() -> Self {
        let directory =
            std::env::temp_dir().join(format!("insight-bao-readback-{}", Uuid::new_v4()));
        fs::DirBuilder::new()
            .mode(0o700)
            .create(&directory)
            .unwrap();
        let mut fixture = Self {
            directory,
            child: None,
        };
        let path = &fixture.directory;
        openssl(
            path,
            &[
                "req",
                "-x509",
                "-newkey",
                "rsa:2048",
                "-nodes",
                "-days",
                "1",
                "-subj",
                "/CN=readback-fixture-ca",
                "-keyout",
                "ca-key.pem",
                "-out",
                "ca.pem",
            ],
        );
        for name in ["server", "client"] {
            openssl(
                path,
                &[
                    "req",
                    "-newkey",
                    "rsa:2048",
                    "-nodes",
                    "-subj",
                    &format!("/CN={name}"),
                    "-keyout",
                    &format!("{name}-key.pem"),
                    "-out",
                    &format!("{name}.csr"),
                ],
            );
            fs::write(
                path.join("extensions"),
                if name == "server" {
                    "subjectAltName=DNS:localhost\nextendedKeyUsage=serverAuth\n"
                } else {
                    "extendedKeyUsage=clientAuth\n"
                },
            )
            .unwrap();
            openssl(
                path,
                &[
                    "x509",
                    "-req",
                    "-in",
                    &format!("{name}.csr"),
                    "-CA",
                    "ca.pem",
                    "-CAkey",
                    "ca-key.pem",
                    "-CAcreateserial",
                    "-days",
                    "1",
                    "-extfile",
                    "extensions",
                    "-out",
                    &format!("{name}.pem"),
                ],
            );
            fs::set_permissions(
                path.join(format!("{name}-key.pem")),
                fs::Permissions::from_mode(0o600),
            )
            .unwrap();
        }
        fs::write(
            path.join("server.py"),
            include_str!("test_support/readback_faults.py"),
        )
        .unwrap();
        fs::write(path.join("mode"), "healthy").unwrap();
        fixture.child = Some(
            Command::new("python3")
                .arg(path.join("server.py"))
                .arg(path)
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .unwrap(),
        );
        let deadline = Instant::now() + Duration::from_secs(5);
        while !fixture.directory.join("port").exists() {
            assert!(
                Instant::now() < deadline,
                "private HTTPS fixture did not start"
            );
            assert!(fixture
                .child
                .as_mut()
                .unwrap()
                .try_wait()
                .unwrap()
                .is_none());
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        fixture
    }

    fn provider(&self) -> OpenBaoProvider {
        let mut config = bao_config();
        config.client.endpoint = format!(
            "https://localhost:{}",
            fs::read_to_string(self.directory.join("port")).unwrap()
        );
        config.client.ca_file = self.directory.join("ca.pem").display().to_string();
        config.client.client_certificate_file =
            self.directory.join("client.pem").display().to_string();
        config.client.client_private_key_file =
            self.directory.join("client-key.pem").display().to_string();
        config.provider_config_digest = config.calculated_digest().unwrap();
        OpenBaoProvider::install(config, Arc::new(NoopSecretExternalDependencyObserver)).unwrap()
    }

    fn mode(&self, mode: &str) {
        fs::write(self.directory.join("mode.next"), mode).unwrap();
        fs::rename(
            self.directory.join("mode.next"),
            self.directory.join("mode"),
        )
        .unwrap();
    }

    fn creates(&self) -> u64 {
        let value: serde_json::Value =
            serde_json::from_slice(&fs::read(self.directory.join("counts.json")).unwrap()).unwrap();
        value["create"].as_u64().unwrap()
    }
}

struct Authority;
#[async_trait]
impl ModelCredentialImportAuthority for Authority {
    async fn authorize_model_credential_import(
        &self,
        request: &ModelCredentialImportAuthorizationV1,
    ) -> Result<ModelCredentialImportPermitV1, ModelCredentialImportError> {
        Ok(ModelCredentialImportPermitV1 {
            schema_version: 1,
            request_digest: request.canonical_digest()?,
            valid_until: request.deadline,
        })
    }
}

struct Unreached;
#[async_trait]
impl PreparedSecretBindingAuthority for Unreached {
    async fn register_prepared(
        &self,
        _: RegisterPreparedSecretBinding,
    ) -> Result<
        insight_platform_security::PreparedSecretBindingRegistrationOutcome,
        PreparedSecretBindingRegistrationError,
    > {
        panic!("unknown physical write must not reach PostgreSQL registration")
    }
}
#[async_trait]
impl SecretReferenceSealer for Unreached {
    async fn seal(
        &self,
        _: &ResourceId,
        _: &ResourceId,
        _: &ResourceId,
        _: u64,
        _: &OpaqueSecretReference,
    ) -> Result<SealedSecretReference, SecretReferenceSealError> {
        panic!("unknown physical write must not reach reference sealing")
    }
}

#[tokio::test]
async fn possible_create_with_failed_readback_remains_unknown_and_replays_same_exact_write() {
    let fixture = Fixture::start().await;
    let provider = Arc::new(fixture.provider());
    let store = BrokeredPreparedSecretStore::new(
        Arc::new(Unreached),
        Arc::new(Unreached),
        InstalledSecretProviderCatalog::new(vec![provider.clone()]).unwrap(),
        "prn_0198f1c3-9a00-7c3e-b1f3-773c2836ae07".parse().unwrap(),
        SecretBrokerLimits::default(),
    )
    .unwrap()
    .with_model_credential_authority(Arc::new(Authority));
    for mode in [
        "denied",
        "malformed",
        "metadata-denied",
        "absent",
        "unknown-denied",
    ] {
        let request = ModelCredentialImportAuthorizationV1 {
            schema_version: 1,
            deadline: Utc::now() + ChronoDuration::seconds(25),
            identity: ModelCredentialImportIdentityV1 {
                schema_version: 1,
                operation_id: Uuid::new_v4().to_string().parse().unwrap(),
                tenant_id: "ten_0198f1c3-9a00-7c3e-b1f3-773c2836ae05".parse().unwrap(),
                principal_id: "prn_0198f1c3-9a00-7c3e-b1f3-773c2836ae06".parse().unwrap(),
                principal_kind: PrincipalKind::TenantAdmin,
                provider_id: provider.config.provider_id.clone(),
                purpose: "model_api_key".parse().unwrap(),
            },
        };
        let original = fixture.creates();
        fixture.mode("always-denied");
        assert_eq!(
            store
                .import_model_credential(
                    request.clone(),
                    SensitiveModelApiKey::new(b"synthetic-readback-canary".to_vec()).unwrap()
                )
                .await,
            Err(ModelCredentialImportError::Rejected),
            "pre-write rejection remains definite"
        );
        assert_eq!(
            fixture.creates(),
            original,
            "denied preflight must not create"
        );
        fixture.mode(mode);
        assert_eq!(
            store
                .import_model_credential(
                    request.clone(),
                    SensitiveModelApiKey::new(b"synthetic-readback-canary".to_vec()).unwrap()
                )
                .await,
            Err(ModelCredentialImportError::OutcomeUnknown),
            "mode {mode}"
        );
        assert_eq!(fixture.creates(), original + 1, "create must not retry");
        assert_eq!(
            store.capacity_snapshot().available,
            store.capacity_snapshot().maximum_in_flight
        );
        fixture.mode("healthy");
        let permit = Authority
            .authorize_model_credential_import(&request)
            .await
            .unwrap();
        let key = SensitiveModelApiKey::new(b"synthetic-readback-canary".to_vec()).unwrap();
        let first = provider
            .prepare_or_load_model_credential(&request, &permit, &key)
            .await
            .unwrap();
        let repeated = provider
            .prepare_or_load_model_credential(&request, &permit, &key)
            .await
            .unwrap();
        assert_eq!(
            first.secret_binding_id,
            request.identity.secret_binding_id().unwrap()
        );
        assert!(first.opaque_reference.expose() == repeated.opaque_reference.expose());
        assert_eq!(
            first.opaque_version_identity_digest,
            repeated.opaque_version_identity_digest
        );
        let different = SensitiveModelApiKey::new(b"different-synthetic-canary".to_vec()).unwrap();
        assert!(matches!(
            provider
                .prepare_or_load_model_credential(&request, &permit, &different)
                .await,
            Err(SecretProviderPrepareError::Rejected)
        ));
        assert_eq!(
            fixture.creates(),
            original + 1,
            "all recovery uses the original version"
        );
    }
}
