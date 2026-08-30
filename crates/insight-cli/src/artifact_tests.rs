#[cfg(test)]
mod tests {
    use super::*;
    use insight_platform_contracts::{
        operation_etag, ApiProblem, ApiProblemCode, SafeJobResult, TraceId,
    };
    use rcgen::{CertificateParams, KeyPair};
    use rustls::{
        pki_types::{CertificateDer, PrivatePkcs8KeyDer},
        ServerConfig, ServerConnection, StreamOwned,
    };
    use sha2::Sha256;
    use std::{
        io::Read,
        net::{TcpListener, TcpStream},
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc, Mutex,
        },
        thread,
        time::Duration,
    };
    use tempfile::TempDir;

    static PROXY_ENVIRONMENT_LOCK: Mutex<()> = Mutex::new(());

    struct ScopedProxyEnvironment(Vec<(&'static str, Option<std::ffi::OsString>)>);

    impl ScopedProxyEnvironment {
        fn install(proxy: &str) -> Self {
            let names = ["HTTPS_PROXY", "https_proxy", "ALL_PROXY", "all_proxy"];
            let previous = names
                .into_iter()
                .map(|name| (name, std::env::var_os(name)))
                .collect::<Vec<_>>();
            for name in names {
                // SAFETY: this fixture serializes its proxy environment mutation and restores all
                // values before releasing the lock. Every HTTP client in this test binary is also
                // configured with `no_proxy`, so concurrent request behavior cannot consume it.
                unsafe { std::env::set_var(name, proxy) };
            }
            Self(previous)
        }
    }

    impl Drop for ScopedProxyEnvironment {
        fn drop(&mut self) {
            for (name, value) in &self.0 {
                // SAFETY: see `install`; the same serialized scope restores the prior process
                // environment before another proxy fixture may start.
                unsafe {
                    if let Some(value) = value {
                        std::env::set_var(name, value);
                    } else {
                        std::env::remove_var(name);
                    }
                }
            }
        }
    }

    fn id(kind: ResourceKind) -> ResourceId {
        ResourceId::from_uuid_v7(kind, Uuid::now_v7()).unwrap()
    }

    fn digest(bytes: &[u8]) -> Sha256Digest {
        let mut encoded = String::from("sha256:");
        for byte in Sha256::digest(bytes) {
            use std::fmt::Write as _;
            write!(&mut encoded, "{byte:02x}").unwrap();
        }
        encoded.parse().unwrap()
    }

    fn ready_artifact_view(
        artifact_id: ResourceId,
        content_digest: Sha256Digest,
        byte_length: u64,
    ) -> ArtifactViewV1 {
        let content = ArtifactRef::new(
            artifact_id.clone(),
            content_digest,
            byte_length,
            "text/plain",
            DataClassification::Internal,
            Some("result.txt".to_owned()),
        )
        .unwrap();
        ArtifactViewV1 {
            schema_version: 1,
            artifact_id: artifact_id.clone(),
            purpose: ArtifactPurpose::RunOutput,
            classification: DataClassification::Internal,
            state: ArtifactState::Ready,
            version: 4,
            expected_size_bytes: byte_length,
            declared_media_type: Some("text/plain".to_owned()),
            verified_media_type: Some("text/plain".to_owned()),
            content: Some(content),
            retain_until: "2026-09-29T00:00:00.000000Z".parse().unwrap(),
            created_at: "2026-08-29T00:00:00.000000Z".parse().unwrap(),
            updated_at: "2026-08-29T00:00:01.000000Z".parse().unwrap(),
            etag: format!("\"{artifact_id}-4\""),
        }
    }

    fn non_ready_artifact_view(artifact_id: ResourceId, state: ArtifactState) -> ArtifactViewV1 {
        ArtifactViewV1 {
            schema_version: 1,
            artifact_id: artifact_id.clone(),
            purpose: ArtifactPurpose::RunOutput,
            classification: DataClassification::Internal,
            state,
            version: 5,
            expected_size_bytes: 8,
            declared_media_type: Some("text/plain".to_owned()),
            verified_media_type: None,
            content: None,
            retain_until: "2026-09-29T00:00:00.000000Z".parse().unwrap(),
            created_at: "2026-08-29T00:00:00.000000Z".parse().unwrap(),
            updated_at: "2026-08-29T00:00:01.000000Z".parse().unwrap(),
            etag: format!("\"{artifact_id}-5\""),
        }
    }

    fn upload_options() -> ArtifactUploadOptions {
        ArtifactUploadOptions {
            purpose: ArtifactPurpose::RunInput,
            classification: DataClassification::Internal,
            declared_media_type: Some("text/plain".to_owned()),
            display_name: Some("input.txt".to_owned()),
            operation_timeout: Duration::from_secs(2),
        }
    }

    fn prepared_upload_response() -> PrepareArtifactUploadResponseV1 {
        let artifact_id = id(ResourceKind::Artifact);
        PrepareArtifactUploadResponseV1 {
            schema_version: 1,
            artifact_etag: format!("\"{artifact_id}-1\""),
            artifact_id,
            operation_id: id(ResourceKind::Job),
            upload_grant_id: id(ResourceKind::ArtifactGrant),
            upload_target: SecretBearingUploadTargetV1 {
                url: "https://uploads.example/object?signature=secret".to_owned(),
                completion_proof: OpaqueUploadCompletionProof("proof_123.safe".to_owned()),
            },
            upload_expires_at: UtcTimestamp::from_datetime(Utc::now() + chrono::Duration::hours(1)),
        }
    }

    fn problem(
        status: u16,
        code: ApiProblemCode,
        retryable: bool,
        retry_after_ms: Option<u64>,
        trace_id: TraceId,
    ) -> ApiProblem {
        ApiProblem {
            type_uri: format!("urn:insight:problem:{}", code.as_str()),
            title: "Artifact request rejected".to_owned(),
            status,
            code,
            detail: Some("safe public diagnostic".to_owned()),
            request_id: id(ResourceKind::ServerRequest),
            trace_id,
            retryable,
            retry_after_ms,
            field_errors: Vec::new(),
        }
    }

    fn local_tls_server_config() -> (Arc<ServerConfig>, Certificate) {
        let params = CertificateParams::new(vec!["127.0.0.1".to_owned()]).unwrap();
        let key = KeyPair::generate().unwrap();
        let certificate = params.self_signed(&key).unwrap();
        let root = Certificate::from_pem(certificate.pem().as_bytes()).unwrap();
        let certificate_der = CertificateDer::from(certificate.der().to_vec());
        let private_key = PrivatePkcs8KeyDer::from(key.serialize_der());
        let config = ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(vec![certificate_der], private_key.into())
            .unwrap();
        (Arc::new(config), root)
    }

    #[test]
    fn metadata_and_content_download_are_authority_and_digest_bound() {
        let bytes = b"verified artifact body".to_vec();
        let content_digest = digest(&bytes);
        let artifact_id = id(ResourceKind::Artifact);
        let content = ArtifactRef::new(
            artifact_id.clone(),
            content_digest.clone(),
            bytes.len() as u64,
            "text/plain",
            DataClassification::Internal,
            Some("result.txt".to_owned()),
        )
        .unwrap();
        let view = ArtifactViewV1 {
            schema_version: 1,
            artifact_id: artifact_id.clone(),
            purpose: ArtifactPurpose::RunOutput,
            classification: DataClassification::Internal,
            state: ArtifactState::Ready,
            version: 4,
            expected_size_bytes: bytes.len() as u64,
            declared_media_type: Some("text/plain".to_owned()),
            verified_media_type: Some("text/plain".to_owned()),
            content: Some(content),
            retain_until: "2026-09-29T00:00:00.000000Z".parse().unwrap(),
            created_at: "2026-08-29T00:00:00.000000Z".parse().unwrap(),
            updated_at: "2026-08-29T00:00:01.000000Z".parse().unwrap(),
            etag: format!("\"{artifact_id}-4\""),
        };
        let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let port = listener.local_addr().unwrap().port();
        let server_view = view.clone();
        let server_digest = content_digest.clone();
        let server_id = artifact_id.clone();
        let server = thread::spawn(move || {
            for step in 0..2 {
                let (mut stream, _) = listener.accept().unwrap();
                let head = read_request_head(&mut stream);
                assert_eq!(header_value(&head, "authorization"), Some("Bearer token"));
                if step == 0 {
                    assert!(head.starts_with(&format!("GET /v1/artifacts/{server_id} HTTP/1.1")));
                    write_json_response(&mut stream, &server_view);
                } else {
                    assert!(head
                        .starts_with(&format!("GET /v1/artifacts/{server_id}/content HTTP/1.1")));
                    write!(
                        stream,
                        "HTTP/1.1 200 OK\r\ncontent-type: text/plain\r\ncontent-length: {}\r\ncontent-disposition: attachment\r\ncache-control: no-store, private, max-age=0\r\netag: \"{}\"\r\ntrace-id: 22222222222222222222222222222222\r\nconnection: close\r\n\r\n",
                        bytes.len(), server_digest
                    )
                    .unwrap();
                    stream.write_all(&bytes).unwrap();
                }
            }
        });
        let client = PublicHttpClient::new(
            format!("http://127.0.0.1:{port}"),
            "token".to_owned(),
            Duration::from_secs(2),
        )
        .unwrap();
        let directory = TempDir::new().unwrap();
        let output = directory.path().join("result.txt");
        let report = download_artifact(&client, &artifact_id, &output).unwrap();
        assert_eq!(fs::read(&output).unwrap(), b"verified artifact body");
        assert_eq!(report.content_digest, content_digest.to_string());
        assert!(download_artifact(&client, &artifact_id, &output).is_err());
        server.join().unwrap();
    }

    #[test]
    fn download_rejects_non_ready_authority_states() {
        for state in [
            ArtifactState::Quarantined,
            ArtifactState::Rejected,
            ArtifactState::Deleted,
        ] {
            let artifact_id = id(ResourceKind::Artifact);
            let view = non_ready_artifact_view(artifact_id.clone(), state);
            let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
            let port = listener.local_addr().unwrap().port();
            let server_id = artifact_id.clone();
            let server_view = view.clone();
            let server = thread::spawn(move || {
                let (mut stream, _) = listener.accept().unwrap();
                let head = read_request_head(&mut stream);
                assert!(head.starts_with(&format!("GET /v1/artifacts/{server_id} HTTP/1.1")));
                write_json_response(&mut stream, &server_view);
            });
            let client = PublicHttpClient::new(
                format!("http://127.0.0.1:{port}"),
                "token".to_owned(),
                Duration::from_secs(2),
            )
            .unwrap();
            let directory = TempDir::new().unwrap();
            let output = directory.path().join("result.txt");
            assert!(matches!(
                download_artifact(&client, &artifact_id, &output),
                Err(ArtifactClientError::InvalidResponse(_))
            ));
            assert!(!output.exists());
            assert_eq!(fs::read_dir(directory.path()).unwrap().count(), 0);
            server.join().unwrap();
        }
    }

    #[test]
    fn download_rejects_truncated_oversized_and_digest_mismatched_streams() {
        let expected = b"expected";
        let cases: [(&str, &[u8], u64); 3] = [
            ("truncated", b"short", expected.len() as u64),
            ("oversized", b"too-large", (expected.len() + 1) as u64),
            ("digest-mismatch", b"mismatch", expected.len() as u64),
        ];
        for (name, body, declared_length) in cases {
            let artifact_id = id(ResourceKind::Artifact);
            let expected_digest = digest(expected);
            let view = ready_artifact_view(
                artifact_id.clone(),
                expected_digest.clone(),
                expected.len() as u64,
            );
            let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
            let port = listener.local_addr().unwrap().port();
            let server_id = artifact_id.clone();
            let server_view = view.clone();
            let server_body = body.to_vec();
            let server = thread::spawn(move || {
                let (mut metadata, _) = listener.accept().unwrap();
                let head = read_request_head(&mut metadata);
                assert!(head.starts_with(&format!("GET /v1/artifacts/{server_id} HTTP/1.1")));
                write_json_response(&mut metadata, &server_view);

                let (mut content, _) = listener.accept().unwrap();
                let head = read_request_head(&mut content);
                assert!(
                    head.starts_with(&format!("GET /v1/artifacts/{server_id}/content HTTP/1.1"))
                );
                write!(
                    content,
                    "HTTP/1.1 200 OK\r\ncontent-type: text/plain\r\ncontent-length: {declared_length}\r\ncontent-disposition: attachment\r\ncache-control: no-store, private, max-age=0\r\netag: \"{expected_digest}\"\r\ntrace-id: 22222222222222222222222222222222\r\nconnection: close\r\n\r\n"
                )
                .unwrap();
                content.write_all(&server_body).unwrap();
            });
            let client = PublicHttpClient::new(
                format!("http://127.0.0.1:{port}"),
                "token".to_owned(),
                Duration::from_secs(2),
            )
            .unwrap();
            let directory = TempDir::new().unwrap();
            let output = directory.path().join(format!("{name}.txt"));
            assert!(download_artifact(&client, &artifact_id, &output).is_err());
            assert!(!output.exists());
            assert_eq!(fs::read_dir(directory.path()).unwrap().count(), 0);
            server.join().unwrap();
        }
    }

    #[test]
    fn upload_preserves_prepare_conflict_and_backpressure_problems() {
        for (status_line, status, code, retryable, retry_after_ms) in [
            (
                "409 Conflict",
                409,
                ApiProblemCode::IdempotencyConflict,
                false,
                None,
            ),
            (
                "429 Too Many Requests",
                429,
                ApiProblemCode::RateLimited,
                true,
                Some(250),
            ),
            (
                "503 Service Unavailable",
                503,
                ApiProblemCode::TemporarilyUnavailable,
                true,
                Some(500),
            ),
        ] {
            let directory = TempDir::new().unwrap();
            let source = directory.path().join("input.txt");
            fs::write(&source, b"upload body").unwrap();
            let tenant_id = id(ResourceKind::Tenant);
            let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
            let port = listener.local_addr().unwrap().port();
            let server_code = code;
            let server = thread::spawn(move || {
                let (mut stream, _) = listener.accept().unwrap();
                let (head, _) = read_request(&mut stream);
                assert!(head.starts_with("POST /v1/artifacts:prepare-upload HTTP/1.1"));
                assert!(header_value(&head, "idempotency-key")
                    .is_some_and(|value| value.ends_with("-prepare")));
                let trace_id = request_trace_id(&head).parse().unwrap();
                let problem = problem(status, server_code, retryable, retry_after_ms, trace_id);
                write_problem_response(&mut stream, status_line, &problem);
            });
            let client = PublicHttpClient::new(
                format!("http://127.0.0.1:{port}"),
                "token".to_owned(),
                Duration::from_secs(2),
            )
            .unwrap();
            let puts = Arc::new(AtomicUsize::new(0));
            let uploader = CountingUploader(Arc::clone(&puts));
            match upload_artifact(
                &client,
                &uploader,
                &tenant_id,
                &source,
                upload_options(),
                &directory.path().join("journals"),
            )
            .unwrap_err()
            {
                ArtifactClientError::Public(PublicClientError::Problem(actual)) => {
                    assert_eq!(actual.status, status);
                    assert_eq!(actual.code, code);
                    assert_eq!(actual.retryable, retryable);
                    assert_eq!(actual.retry_after_ms, retry_after_ms);
                }
                other => panic!("expected closed Artifact Problem, got {other:?}"),
            }
            assert_eq!(puts.load(Ordering::SeqCst), 0);
            server.join().unwrap();
        }
    }

    #[test]
    fn complete_precondition_failure_happens_after_exactly_one_object_put() {
        let directory = TempDir::new().unwrap();
        let source = directory.path().join("input.txt");
        fs::write(&source, b"upload body").unwrap();
        let tenant_id = id(ResourceKind::Tenant);
        let prepared = prepared_upload_response();
        let prepared_etag = prepared.artifact_etag.clone();
        let artifact_id = prepared.artifact_id.clone();
        let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = thread::spawn(move || {
            let (mut prepare, _) = listener.accept().unwrap();
            let (head, _) = read_request(&mut prepare);
            let trace_id = request_trace_id(&head);
            write_json_envelope(
                &mut prepare,
                "201 Created",
                &trace_id,
                Some(&prepared_etag),
                Some(&format!("/v1/artifacts/{artifact_id}")),
                &prepared,
            );

            let (mut complete, _) = listener.accept().unwrap();
            let (head, _) = read_request(&mut complete);
            assert!(head.starts_with(&format!(
                "POST /v1/artifacts/{artifact_id}:complete-upload HTTP/1.1"
            )));
            assert_eq!(
                header_value(&head, "if-match"),
                Some(prepared_etag.as_str())
            );
            let trace_id = request_trace_id(&head).parse().unwrap();
            let problem = problem(
                412,
                ApiProblemCode::PreconditionFailed,
                false,
                None,
                trace_id,
            );
            write_problem_response(&mut complete, "412 Precondition Failed", &problem);
        });
        let client = PublicHttpClient::new(
            format!("http://127.0.0.1:{port}"),
            "token".to_owned(),
            Duration::from_secs(2),
        )
        .unwrap();
        let puts = Arc::new(AtomicUsize::new(0));
        let uploader = CountingUploader(Arc::clone(&puts));
        match upload_artifact(
            &client,
            &uploader,
            &tenant_id,
            &source,
            upload_options(),
            &directory.path().join("journals"),
        )
        .unwrap_err()
        {
            ArtifactClientError::Public(PublicClientError::Problem(actual)) => {
                assert_eq!(actual.status, 412);
                assert_eq!(actual.code, ApiProblemCode::PreconditionFailed);
            }
            other => panic!("expected complete precondition Problem, got {other:?}"),
        }
        assert_eq!(puts.load(Ordering::SeqCst), 1);
        let journal = fs::read_to_string(
            fs::read_dir(directory.path().join("journals"))
                .unwrap()
                .next()
                .unwrap()
                .unwrap()
                .path(),
        )
        .unwrap();
        assert!(journal.contains("\"object_uploaded\": true"));
        server.join().unwrap();
    }

    #[test]
    fn prepare_rejects_expired_or_non_https_secret_targets() {
        let prepared = prepared_upload_response();
        let envelope = |body: PrepareArtifactUploadResponseV1| PublicJsonResponse {
            etag: body.artifact_etag.clone(),
            location: Some(format!("/v1/artifacts/{}", body.artifact_id)),
            trace_id: TraceId::new(),
            body,
        };
        let mut expired = prepared.clone();
        expired.upload_expires_at =
            UtcTimestamp::from_datetime(Utc::now() - chrono::Duration::seconds(1));
        assert!(validate_prepare_response(&envelope(expired)).is_err());

        let mut plaintext = prepared;
        plaintext.upload_target.url = "http://127.0.0.1/object?signature=secret".to_owned();
        assert!(validate_prepare_response(&envelope(plaintext)).is_err());
    }

    #[test]
    fn isolated_https_uploader_rejects_redirect_and_non_200_without_platform_token() {
        for (status_line, extra_headers) in [
            (
                "307 Temporary Redirect",
                "location: https://redirect.invalid/forbidden\r\n",
            ),
            ("500 Internal Server Error", ""),
        ] {
            let directory = TempDir::new().unwrap();
            let source = directory.path().join("input.txt");
            fs::write(&source, b"upload body").unwrap();
            let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
            let port = listener.local_addr().unwrap().port();
            let (server_config, root) = local_tls_server_config();
            let server = thread::spawn(move || {
                let (tcp, _) = listener.accept().unwrap();
                let connection = ServerConnection::new(server_config).unwrap();
                let mut stream = StreamOwned::new(connection, tcp);
                let (head, body) = read_request(&mut stream);
                assert!(head.starts_with("PUT /object?signature=secret HTTP/1.1"));
                assert_eq!(header_value(&head, "authorization"), None);
                assert_eq!(header_value(&head, "proxy-authorization"), None);
                assert_eq!(header_value(&head, "content-type"), Some("text/plain"));
                assert_eq!(body, b"upload body");
                write!(
                    stream,
                    "HTTP/1.1 {status_line}\r\n{extra_headers}content-length: 0\r\nconnection: close\r\n\r\n"
                )
                .unwrap();
                stream.flush().unwrap();
            });
            let uploader = HttpsArtifactObjectUploader::with_additional_root(Some(root)).unwrap();
            let error = uploader
                .put(
                    &format!("https://127.0.0.1:{port}/object?signature=secret"),
                    &source,
                    11,
                    Some("text/plain"),
                )
                .unwrap_err();
            assert!(matches!(error, ArtifactClientError::InvalidResponse(_)));
            assert!(error.to_string().contains("was not accepted"));
            server.join().unwrap();
        }
    }

    #[test]
    fn isolated_https_uploader_fails_closed_on_tls_handshake_error() {
        let directory = TempDir::new().unwrap();
        let source = directory.path().join("input.txt");
        fs::write(&source, b"upload body").unwrap();
        let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            drop(stream);
        });
        let uploader = HttpsArtifactObjectUploader::new().unwrap();
        let error = uploader
            .put(
                &format!("https://127.0.0.1:{port}/object?signature=secret"),
                &source,
                11,
                Some("text/plain"),
            )
            .unwrap_err();
        assert!(matches!(error, ArtifactClientError::InvalidResponse(_)));
        assert!(error.to_string().contains("transport failed"));
        server.join().unwrap();
    }

    #[test]
    fn isolated_https_uploader_ignores_process_proxy_environment() {
        let _environment_lock = PROXY_ENVIRONMENT_LOCK.lock().unwrap();
        let directory = TempDir::new().unwrap();
        let source = directory.path().join("input.txt");
        fs::write(&source, b"upload body").unwrap();
        let proxy_listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        proxy_listener.set_nonblocking(true).unwrap();
        let proxy_port = proxy_listener.local_addr().unwrap().port();
        let target_listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let target_port = target_listener.local_addr().unwrap().port();
        let (server_config, root) = local_tls_server_config();
        let target = thread::spawn(move || {
            let (tcp, _) = target_listener.accept().unwrap();
            let connection = ServerConnection::new(server_config).unwrap();
            let mut stream = StreamOwned::new(connection, tcp);
            let (head, body) = read_request(&mut stream);
            assert!(head.starts_with("PUT /object?signature=secret HTTP/1.1"));
            assert_eq!(header_value(&head, "authorization"), None);
            assert_eq!(header_value(&head, "proxy-authorization"), None);
            assert_eq!(body, b"upload body");
            write!(
                stream,
                "HTTP/1.1 200 OK\r\ncontent-length: 0\r\nconnection: close\r\n\r\n"
            )
            .unwrap();
            stream.flush().unwrap();
        });
        let _proxy_environment =
            ScopedProxyEnvironment::install(&format!("http://127.0.0.1:{proxy_port}"));
        let uploader = HttpsArtifactObjectUploader::with_additional_root(Some(root)).unwrap();
        uploader
            .put(
                &format!("https://127.0.0.1:{target_port}/object?signature=secret"),
                &source,
                11,
                Some("text/plain"),
            )
            .unwrap();
        target.join().unwrap();
        assert!(matches!(
            proxy_listener.accept(),
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock
        ));
    }

    struct FixtureUploader;

    impl ArtifactObjectUploader for FixtureUploader {
        fn put(
            &self,
            target_url: &str,
            source: &Path,
            content_length: u64,
            content_type: Option<&str>,
        ) -> Result<(), ArtifactClientError> {
            assert_eq!(
                target_url,
                "https://uploads.example/object?signature=secret"
            );
            assert_eq!(fs::read(source).unwrap(), b"upload body");
            assert_eq!(content_length, 11);
            assert_eq!(content_type, Some("text/plain"));
            Ok(())
        }
    }

    struct CountingUploader(Arc<AtomicUsize>);

    impl ArtifactObjectUploader for CountingUploader {
        fn put(
            &self,
            target_url: &str,
            source: &Path,
            content_length: u64,
            content_type: Option<&str>,
        ) -> Result<(), ArtifactClientError> {
            assert_eq!(
                target_url,
                "https://uploads.example/object?signature=secret"
            );
            assert_eq!(fs::read(source).unwrap(), b"upload body");
            assert_eq!(content_length, 11);
            assert_eq!(content_type, Some("text/plain"));
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    #[test]
    fn upload_uses_isolated_target_then_waits_for_exact_ready_artifact() {
        let directory = TempDir::new().unwrap();
        let source = directory.path().join("input.txt");
        fs::write(&source, b"upload body").unwrap();
        let content_digest = digest(b"upload body");
        let tenant_id = id(ResourceKind::Tenant);
        let artifact_id = id(ResourceKind::Artifact);
        let operation_id = id(ResourceKind::Job);
        let grant_id = id(ResourceKind::ArtifactGrant);
        let prepared_etag = format!("\"{artifact_id}-1\"");
        let completed_etag = format!("\"{artifact_id}-2\"");
        let ready_etag = format!("\"{artifact_id}-4\"");
        let operation_etag = operation_etag(&operation_id.to_string(), 3);
        let prepared = PrepareArtifactUploadResponseV1 {
            schema_version: 1,
            artifact_id: artifact_id.clone(),
            operation_id: operation_id.clone(),
            upload_grant_id: grant_id.clone(),
            artifact_etag: prepared_etag.clone(),
            upload_target: SecretBearingUploadTargetV1 {
                url: "https://uploads.example/object?signature=secret".to_owned(),
                completion_proof: OpaqueUploadCompletionProof("proof_123.safe".to_owned()),
            },
            upload_expires_at: UtcTimestamp::from_datetime(Utc::now() + chrono::Duration::hours(1)),
        };
        let completed = ArtifactMutationAcceptedV1 {
            schema_version: 1,
            artifact_id: artifact_id.clone(),
            artifact_etag: completed_etag,
            operation_id: operation_id.clone(),
        };
        let operation = OperationViewV1 {
            operation_id: operation_id.clone(),
            tenant_id: tenant_id.clone(),
            kind: PublicJobKind::ArtifactVerify,
            target: PublicJobTarget::Artifact {
                artifact_id: artifact_id.clone(),
            },
            state: PublicJobState::Succeeded,
            progress: None,
            result: Some(SafeJobResult::Digest {
                result_digest: digest(b"verification"),
            }),
            error: None,
            created_at: "2026-08-29T00:00:00.000000Z".parse().unwrap(),
            updated_at: "2026-08-29T00:00:01.000000Z".parse().unwrap(),
            etag: operation_etag.clone(),
        };
        let content = ArtifactRef::new(
            artifact_id.clone(),
            content_digest.clone(),
            11,
            "text/plain",
            DataClassification::Internal,
            Some("input.txt".to_owned()),
        )
        .unwrap();
        let ready = ArtifactViewV1 {
            schema_version: 1,
            artifact_id: artifact_id.clone(),
            purpose: ArtifactPurpose::RunInput,
            classification: DataClassification::Internal,
            state: ArtifactState::Ready,
            version: 4,
            expected_size_bytes: 11,
            declared_media_type: Some("text/plain".to_owned()),
            verified_media_type: Some("text/plain".to_owned()),
            content: Some(content),
            retain_until: "2026-09-29T00:00:00.000000Z".parse().unwrap(),
            created_at: "2026-08-29T00:00:00.000000Z".parse().unwrap(),
            updated_at: "2026-08-29T00:00:02.000000Z".parse().unwrap(),
            etag: ready_etag.clone(),
        };

        let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let port = listener.local_addr().unwrap().port();
        let server_artifact_id = artifact_id.clone();
        let server_operation_id = operation_id.clone();
        let server_digest = content_digest.clone();
        let server = thread::spawn(move || {
            for step in 0..4 {
                let (mut stream, _) = listener.accept().unwrap();
                let (head, body) = read_request(&mut stream);
                assert_eq!(header_value(&head, "authorization"), Some("Bearer token"));
                match step {
                    0 => {
                        assert!(head.starts_with("POST /v1/artifacts:prepare-upload HTTP/1.1"));
                        assert!(header_value(&head, "idempotency-key")
                            .is_some_and(|value| value.ends_with("-prepare")));
                        let request: serde_json::Value = serde_json::from_slice(&body).unwrap();
                        assert_eq!(request["expected_digest"], server_digest.to_string());
                        let trace = request_trace_id(&head);
                        write_json_envelope(
                            &mut stream,
                            "201 Created",
                            &trace,
                            Some(&prepared_etag),
                            Some(&format!("/v1/artifacts/{server_artifact_id}")),
                            &prepared,
                        );
                    }
                    1 => {
                        assert!(head.starts_with(&format!(
                            "POST /v1/artifacts/{server_artifact_id}:complete-upload HTTP/1.1"
                        )));
                        assert_eq!(
                            header_value(&head, "if-match"),
                            Some(prepared_etag.as_str())
                        );
                        assert!(header_value(&head, "idempotency-key")
                            .is_some_and(|value| value.ends_with("-complete")));
                        assert_eq!(
                            serde_json::from_slice::<serde_json::Value>(&body).unwrap()
                                ["completion_proof"],
                            "proof_123.safe"
                        );
                        let trace = request_trace_id(&head);
                        write_json_envelope(
                            &mut stream,
                            "202 Accepted",
                            &trace,
                            Some(&completed.artifact_etag),
                            Some(&format!("/v1/operations/{server_operation_id}")),
                            &completed,
                        );
                    }
                    2 => {
                        assert!(head.starts_with(&format!(
                            "GET /v1/operations/{server_operation_id} HTTP/1.1"
                        )));
                        write_json_envelope(
                            &mut stream,
                            "200 OK",
                            "33333333333333333333333333333333",
                            Some(&operation_etag),
                            None,
                            &operation,
                        );
                    }
                    3 => {
                        assert!(head.starts_with(&format!(
                            "GET /v1/artifacts/{server_artifact_id} HTTP/1.1"
                        )));
                        write_json_envelope(
                            &mut stream,
                            "200 OK",
                            "44444444444444444444444444444444",
                            Some(&ready_etag),
                            None,
                            &ready,
                        );
                    }
                    _ => unreachable!(),
                }
            }
        });
        let client = PublicHttpClient::new(
            format!("http://127.0.0.1:{port}"),
            "token".to_owned(),
            Duration::from_secs(2),
        )
        .unwrap();
        let report = upload_artifact(
            &client,
            &FixtureUploader,
            &tenant_id,
            &source,
            ArtifactUploadOptions {
                purpose: ArtifactPurpose::RunInput,
                classification: DataClassification::Internal,
                declared_media_type: Some("text/plain".to_owned()),
                display_name: Some("input.txt".to_owned()),
                operation_timeout: Duration::from_secs(2),
            },
            &directory.path().join("journals"),
        )
        .unwrap();
        assert_eq!(report.artifact_id, artifact_id.to_string());
        assert_eq!(report.operation_id, operation_id.to_string());
        assert_eq!(report.upload_grant_id, grant_id.to_string());
        assert_eq!(report.content_digest, content_digest.to_string());
        let serialized = serde_json::to_string(&report).unwrap();
        assert!(!serialized.contains("signature=secret"));
        assert!(!serialized.contains("proof_123"));
        assert!(!serialized.contains("Bearer"));
        server.join().unwrap();
    }

    #[test]
    fn upload_replays_complete_after_response_loss_without_second_object_put() {
        let directory = TempDir::new().unwrap();
        let source = directory.path().join("input.txt");
        fs::write(&source, b"upload body").unwrap();
        let content_digest = digest(b"upload body");
        let tenant_id = id(ResourceKind::Tenant);
        let artifact_id = id(ResourceKind::Artifact);
        let operation_id = id(ResourceKind::Job);
        let grant_id = id(ResourceKind::ArtifactGrant);
        let prepared_etag = format!("\"{artifact_id}-1\"");
        let completed_etag = format!("\"{artifact_id}-2\"");
        let ready_etag = format!("\"{artifact_id}-4\"");
        let operation_etag = operation_etag(&operation_id.to_string(), 3);
        let prepared = PrepareArtifactUploadResponseV1 {
            schema_version: 1,
            artifact_id: artifact_id.clone(),
            operation_id: operation_id.clone(),
            upload_grant_id: grant_id.clone(),
            artifact_etag: prepared_etag.clone(),
            upload_target: SecretBearingUploadTargetV1 {
                url: "https://uploads.example/object?signature=secret".to_owned(),
                completion_proof: OpaqueUploadCompletionProof("proof_123.safe".to_owned()),
            },
            upload_expires_at: UtcTimestamp::from_datetime(Utc::now() + chrono::Duration::hours(1)),
        };
        let completed = ArtifactMutationAcceptedV1 {
            schema_version: 1,
            artifact_id: artifact_id.clone(),
            artifact_etag: completed_etag,
            operation_id: operation_id.clone(),
        };
        let operation = OperationViewV1 {
            operation_id: operation_id.clone(),
            tenant_id: tenant_id.clone(),
            kind: PublicJobKind::ArtifactVerify,
            target: PublicJobTarget::Artifact {
                artifact_id: artifact_id.clone(),
            },
            state: PublicJobState::Succeeded,
            progress: None,
            result: Some(SafeJobResult::Digest {
                result_digest: digest(b"verification"),
            }),
            error: None,
            created_at: "2026-08-29T00:00:00.000000Z".parse().unwrap(),
            updated_at: "2026-08-29T00:00:01.000000Z".parse().unwrap(),
            etag: operation_etag.clone(),
        };
        let content = ArtifactRef::new(
            artifact_id.clone(),
            content_digest.clone(),
            11,
            "text/plain",
            DataClassification::Internal,
            Some("input.txt".to_owned()),
        )
        .unwrap();
        let ready = ArtifactViewV1 {
            schema_version: 1,
            artifact_id: artifact_id.clone(),
            purpose: ArtifactPurpose::RunInput,
            classification: DataClassification::Internal,
            state: ArtifactState::Ready,
            version: 4,
            expected_size_bytes: 11,
            declared_media_type: Some("text/plain".to_owned()),
            verified_media_type: Some("text/plain".to_owned()),
            content: Some(content),
            retain_until: "2026-09-29T00:00:00.000000Z".parse().unwrap(),
            created_at: "2026-08-29T00:00:00.000000Z".parse().unwrap(),
            updated_at: "2026-08-29T00:00:02.000000Z".parse().unwrap(),
            etag: ready_etag.clone(),
        };

        let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let port = listener.local_addr().unwrap().port();
        let server_artifact_id = artifact_id.clone();
        let server_operation_id = operation_id.clone();
        let server = thread::spawn(move || {
            let mut first_complete_receipt = None;
            for step in 0..6 {
                let (mut stream, _) = listener.accept().unwrap();
                let (head, body) = read_request(&mut stream);
                assert_eq!(header_value(&head, "authorization"), Some("Bearer token"));
                match step {
                    0 => {
                        assert!(head.starts_with("POST /v1/artifacts:prepare-upload HTTP/1.1"));
                        let trace = request_trace_id(&head);
                        write_json_envelope(
                            &mut stream,
                            "201 Created",
                            &trace,
                            Some(&prepared_etag),
                            Some(&format!("/v1/artifacts/{server_artifact_id}")),
                            &prepared,
                        );
                    }
                    1 | 2 => {
                        assert!(head.starts_with(&format!(
                            "POST /v1/artifacts/{server_artifact_id}:complete-upload HTTP/1.1"
                        )));
                        assert_eq!(
                            header_value(&head, "if-match"),
                            Some(prepared_etag.as_str())
                        );
                        let receipt = header_value(&head, "idempotency-key")
                            .expect("complete Receipt")
                            .to_owned();
                        if step == 1 {
                            first_complete_receipt = Some(receipt);
                            drop(stream);
                        } else {
                            assert_eq!(Some(receipt), first_complete_receipt);
                            assert_eq!(
                                serde_json::from_slice::<serde_json::Value>(&body).unwrap()
                                    ["completion_proof"],
                                "proof_123.safe"
                            );
                            let trace = request_trace_id(&head);
                            write_json_envelope(
                                &mut stream,
                                "202 Accepted",
                                &trace,
                                Some(&completed.artifact_etag),
                                Some(&format!("/v1/operations/{server_operation_id}")),
                                &completed,
                            );
                        }
                    }
                    3 => write_json_envelope(
                        &mut stream,
                        "200 OK",
                        "33333333333333333333333333333333",
                        Some(&operation_etag),
                        None,
                        &operation,
                    ),
                    4 | 5 => write_json_envelope(
                        &mut stream,
                        "200 OK",
                        "44444444444444444444444444444444",
                        Some(&ready_etag),
                        None,
                        &ready,
                    ),
                    _ => unreachable!(),
                }
            }
        });
        let client = PublicHttpClient::new(
            format!("http://127.0.0.1:{port}"),
            "token".to_owned(),
            Duration::from_secs(2),
        )
        .unwrap();
        let puts = Arc::new(AtomicUsize::new(0));
        let uploader = CountingUploader(Arc::clone(&puts));
        let journals = directory.path().join("journals");
        let options = ArtifactUploadOptions {
            purpose: ArtifactPurpose::RunInput,
            classification: DataClassification::Internal,
            declared_media_type: Some("text/plain".to_owned()),
            display_name: Some("input.txt".to_owned()),
            operation_timeout: Duration::from_secs(2),
        };
        let first = upload_artifact(
            &client,
            &uploader,
            &tenant_id,
            &source,
            options.clone(),
            &journals,
        );
        assert!(matches!(first, Err(ArtifactClientError::Public(_))));
        assert_eq!(puts.load(Ordering::SeqCst), 1);

        let report =
            upload_artifact(&client, &uploader, &tenant_id, &source, options, &journals).unwrap();
        assert_eq!(report.artifact_id, artifact_id.to_string());
        assert_eq!(report.operation_id, operation_id.to_string());
        assert_eq!(puts.load(Ordering::SeqCst), 1);
        let replayed = upload_artifact(
            &client,
            &uploader,
            &tenant_id,
            &source,
            ArtifactUploadOptions {
                purpose: ArtifactPurpose::RunInput,
                classification: DataClassification::Internal,
                declared_media_type: Some("text/plain".to_owned()),
                display_name: Some("input.txt".to_owned()),
                operation_timeout: Duration::from_secs(2),
            },
            &journals,
        )
        .unwrap();
        assert_eq!(replayed, report);
        assert_eq!(puts.load(Ordering::SeqCst), 1);
        let journal = fs::read_to_string(
            fs::read_dir(&journals)
                .unwrap()
                .next()
                .unwrap()
                .unwrap()
                .path(),
        )
        .unwrap();
        assert!(journal.contains("\"object_uploaded\": true"));
        assert!(journal.contains("insight.platform.artifact-upload-report/v1"));
        server.join().unwrap();
    }

    fn read_request_head<R: Read>(stream: &mut R) -> String {
        let mut bytes = Vec::new();
        let mut buffer = [0_u8; 2048];
        loop {
            let read = stream.read(&mut buffer).unwrap();
            assert!(read > 0);
            bytes.extend_from_slice(&buffer[..read]);
            if let Some(index) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
                return String::from_utf8(bytes[..index + 4].to_vec()).unwrap();
            }
        }
    }

    fn read_request<R: Read>(stream: &mut R) -> (String, Vec<u8>) {
        let mut bytes = Vec::new();
        let mut buffer = [0_u8; 4096];
        let header_end = loop {
            let read = stream.read(&mut buffer).unwrap();
            assert!(read > 0);
            bytes.extend_from_slice(&buffer[..read]);
            if let Some(index) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
                break index + 4;
            }
        };
        let head = String::from_utf8(bytes[..header_end].to_vec()).unwrap();
        let content_length = header_value(&head, "content-length")
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(0);
        while bytes.len() < header_end + content_length {
            let read = stream.read(&mut buffer).unwrap();
            assert!(read > 0);
            bytes.extend_from_slice(&buffer[..read]);
        }
        (
            head,
            bytes[header_end..header_end + content_length].to_vec(),
        )
    }

    fn header_value<'a>(head: &'a str, name: &str) -> Option<&'a str> {
        head.lines().find_map(|line| {
            let (candidate, value) = line.split_once(':')?;
            candidate.eq_ignore_ascii_case(name).then_some(value.trim())
        })
    }

    fn write_json_response(stream: &mut TcpStream, view: &ArtifactViewV1) {
        let body = serde_json::to_vec(view).unwrap();
        write!(
            stream,
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\ncache-control: no-store, private, max-age=0\r\netag: {}\r\ntrace-id: 11111111111111111111111111111111\r\nconnection: close\r\n\r\n",
            body.len(), view.etag
        )
        .unwrap();
        stream.write_all(&body).unwrap();
    }

    fn request_trace_id(head: &str) -> String {
        header_value(head, "traceparent")
            .unwrap()
            .split('-')
            .nth(1)
            .unwrap()
            .to_owned()
    }

    fn write_json_envelope<T: Serialize>(
        stream: &mut TcpStream,
        status: &str,
        trace_id: &str,
        etag: Option<&str>,
        location: Option<&str>,
        value: &T,
    ) {
        let body = serde_json::to_vec(value).unwrap();
        write!(
            stream,
            "HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\ncache-control: no-store, private, max-age=0\r\ntrace-id: {trace_id}\r\n",
            body.len()
        )
        .unwrap();
        if let Some(etag) = etag {
            write!(stream, "etag: {etag}\r\n").unwrap();
        }
        if let Some(location) = location {
            write!(stream, "location: {location}\r\n").unwrap();
        }
        write!(stream, "connection: close\r\n\r\n").unwrap();
        stream.write_all(&body).unwrap();
    }

    fn write_problem_response(stream: &mut TcpStream, status: &str, problem: &ApiProblem) {
        let body = serde_json::to_vec(problem).unwrap();
        write!(
            stream,
            "HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncache-control: no-store, private, max-age=0\r\ntrace-id: {}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
            problem.trace_id,
            body.len()
        )
        .unwrap();
        stream.write_all(&body).unwrap();
    }
}
