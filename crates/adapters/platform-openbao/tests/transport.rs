//! Actual HTTPS tests of the client only. The wire server is deliberately synthetic.
use insight_platform_openbao::{
    BaoClient, BaoClientConfigV1, BaoError, BaoSecretPath, KvV2BindingV1,
};
use std::{
    fs,
    os::unix::fs::PermissionsExt,
    path::Path,
    process::{Child, Command, Stdio},
    time::Duration,
};
use tokio::time::Instant;

struct Server {
    child: Child,
    directory: tempfile::TempDir,
}
impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn openssl(directory: &Path, args: &[&str]) {
    let result = Command::new("openssl")
        .args(args)
        .current_dir(directory)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .expect("fixture openssl available");
    assert!(result.success(), "fixture certificate generation");
}

impl Server {
    async fn start() -> Self {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path();
        fs::set_permissions(path, fs::Permissions::from_mode(0o700)).unwrap();
        for name in ["ca", "wrong-ca"] {
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
                    "/CN=wire-fixture",
                    "-keyout",
                    &format!("{name}-key.pem"),
                    "-out",
                    &format!("{name}.pem"),
                ],
            );
        }
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
            let extension = if name == "server" {
                "subjectAltName=DNS:localhost\nextendedKeyUsage=serverAuth\n"
            } else {
                "extendedKeyUsage=clientAuth\n"
            };
            fs::write(path.join("extensions"), extension).unwrap();
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
            include_str!("support/protocol_server.py"),
        )
        .unwrap();
        fs::write(path.join("mode"), "healthy").unwrap();
        let child = Command::new("python3")
            .arg(path.join("server.py"))
            .arg(path)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let server = Self { child, directory };
        let deadline = Instant::now() + Duration::from_secs(5);
        while !server.directory.path().join("port").exists() {
            assert!(Instant::now() < deadline, "HTTPS fixture failed to start");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        server
    }

    fn config(&self) -> BaoClientConfigV1 {
        let path = self.directory.path();
        let port = fs::read_to_string(path.join("port")).unwrap();
        BaoClientConfigV1 {
            schema_version: 1,
            endpoint: format!("https://localhost:{port}"),
            expected_cluster_id: "092036e7-f9ab-41fd-8122-077ea91db8b0".into(),
            auth_mount: "insight-cert".into(),
            auth_mount_accessor: "auth_cert_abc".into(),
            auth_role: "fixture".into(),
            expected_token_policies: vec!["fixture".into()],
            ca_file: path.join("ca.pem").display().to_string(),
            client_certificate_file: path.join("client.pem").display().to_string(),
            client_private_key_file: path.join("client-key.pem").display().to_string(),
            connect_timeout_milliseconds: 500,
            operation_timeout_milliseconds: 3000,
            maximum_response_bytes: 131072,
        }
    }
    fn mode(&self, mode: &str) {
        fs::write(self.directory.path().join("mode"), mode).unwrap();
    }
    fn count(&self, key: &str) -> u64 {
        let value: serde_json::Value =
            serde_json::from_slice(&fs::read(self.directory.path().join("counts.json")).unwrap())
                .unwrap();
        value[key].as_u64().unwrap()
    }
}

fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(3)
}

#[tokio::test]
async fn actual_mtls_bounds_unknown_write_exact_readback_and_tombstone() {
    let server = Server::start().await;
    let config = server.config();
    let mut kv = KvV2BindingV1 {
        schema_version: 1,
        mount: "secrets".into(),
        mount_accessor: "kv_abc".into(),
        identity_digest: format!("sha256:{}", "a".repeat(64)).parse().unwrap(),
    };
    kv.identity_digest = kv.calculated_digest(&config.expected_cluster_id).unwrap();
    let client = BaoClient::install(config.clone()).unwrap();
    client.check_kv(&kv, deadline()).await.unwrap();
    assert_eq!(server.count("login"), 1);
    let path = BaoSecretPath::parse("prepared/canary").unwrap();
    assert_eq!(
        client
            .read_exact(&kv, &path, 1, deadline())
            .await
            .unwrap_err(),
        BaoError::NotFound
    );

    for (mode, expected) in [
        ("wrong-cluster", BaoError::InvalidEvidence),
        ("wrong-mount", BaoError::InvalidEvidence),
        ("redirect", BaoError::InvalidEvidence),
        ("duplicate", BaoError::InvalidEvidence),
        ("oversized", BaoError::InvalidEvidence),
    ] {
        server.mode(mode);
        assert_eq!(
            client
                .read_exact(&kv, &path, 1, deadline())
                .await
                .unwrap_err(),
            expected,
            "{mode}"
        );
    }
    server.mode("healthy");
    for change in ["ca", "san", "role"] {
        let mut rejected = config.clone();
        match change {
            "ca" => {
                rejected.ca_file = server
                    .directory
                    .path()
                    .join("wrong-ca.pem")
                    .display()
                    .to_string()
            }
            "san" => rejected.endpoint = rejected.endpoint.replace("localhost", "127.0.0.1"),
            _ => rejected.auth_role = "foreign".into(),
        }
        let rejected = BaoClient::install(rejected).unwrap();
        assert!(
            rejected.check_kv(&kv, deadline()).await.is_err(),
            "{change}"
        );
    }
    assert_eq!(server.count("create"), 0);
    server.mode("lost-write-response");
    assert_eq!(
        client
            .create_only(
                &kv,
                &path,
                br#"{"wire_canary":"not-a-provider-secret"}"#,
                Instant::now() + Duration::from_millis(400)
            )
            .await,
        Err(BaoError::UnknownOutcome)
    );
    assert_eq!(
        server.count("create"),
        1,
        "possible write must never be retried"
    );
    server.mode("healthy");
    let observed = client.read_exact(&kv, &path, 1, deadline()).await.unwrap();
    assert_eq!(observed.version, 1);
    assert_eq!(
        observed.bytes.as_bytes(),
        br#"{"wire_canary":"not-a-provider-secret"}"#
    );
    assert!(!format!("{observed:?}").contains("not-a-provider-secret"));
    assert_eq!(
        server.count("login"),
        1,
        "short token cache must be bounded and reusable"
    );
    assert_eq!(
        client
            .create_only(&kv, &path, br#"{"wire_canary":"different"}"#, deadline())
            .await,
        Err(BaoError::UnknownOutcome),
        "generic 400 is not exact CAS evidence"
    );
    assert_eq!(server.count("create"), 2);
    client
        .destroy_exact(&kv, &path, 1, deadline())
        .await
        .unwrap();
    assert!(
        client
            .metadata(&kv, &path, deadline())
            .await
            .unwrap()
            .versions[&1]
            .destroyed
    );
    client
        .destroy_exact(&kv, &path, 1, deadline())
        .await
        .unwrap();
    assert_eq!(server.count("destroy"), 1);
    assert_eq!(
        client
            .read_exact(&kv, &path, 1, deadline())
            .await
            .unwrap_err(),
        BaoError::NotFound
    );
}
