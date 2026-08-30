use super::*;
use insight_platform_artifacts::{
    ArtifactObjectReadAuthority, ArtifactObjectReadAuthorityError, GatewayArtifactReadRequest,
};
use insight_platform_contracts::{
    ArtifactRef, DataClassification, PrincipalKind, ResourceId, ResourceKind, Sha256Digest,
};
use insight_platform_postgres::repository::PgRepository;
use reqwest::{blocking::Client, Certificate};
use sqlx::{postgres::PgPoolOptions, Row};
use uuid::Uuid;

const DATABASE_URL: &str = "postgres://insight:insight@127.0.0.1:5432/insight_platform";

pub(super) struct ArtifactLifecycleEvidence {
    artifact_id: String,
    console_passed: bool,
    started_at: chrono::DateTime<Utc>,
    finished_at: chrono::DateTime<Utc>,
}

impl ArtifactLifecycleEvidence {
    pub(super) fn report(&self, revision: &str) -> Value {
        let check = |id: &str, status: &str, evidence: &str| json!({"id": id, "status": status, "evidence": evidence});
        let console = if self.console_passed {
            check(
                "console",
                "passed",
                "the same real headless Chromium session opened Artifacts, read this exact Artifact ID from the fresh Gateway/PostgreSQL authority, verified Ready metadata and exposed the controlled-download action",
            )
        } else {
            check(
                "console",
                "not_run",
                "set PLATFORM_PRODUCTIZATION_CONSOLE_BROWSER=true to include the real Console entrypoint",
            )
        };
        let status = if self.console_passed {
            "passed"
        } else {
            "incomplete"
        };
        json!({
            "schema_version": 1,
            "report_kind": "insight.productization.scenario-report/v1",
            "scenario_id": "artifact-lifecycle-and-rejection",
            "contract_profile": "insight.platform/v1",
            "profile": "full",
            "automation_layer": "P3",
            "source_revision": revision,
            "environment": {
                "os": env::consts::OS,
                "architecture": env::consts::ARCH,
                "fresh_profile": true,
            },
            "started_at": self.started_at.to_rfc3339_opts(SecondsFormat::Micros, true),
            "finished_at": self.finished_at.to_rfc3339_opts(SecondsFormat::Micros, true),
            "status": status,
            "entrypoints": [
                check("cli", "passed", "public insight operation/artifact get/read commands completed for the exact Ready Artifact and verified the downloaded bytes"),
                check("http_fixture", "passed", "raw public /v1 prepare, secret-bearing HTTPS PUT, complete, operation polling, metadata read and controlled content read exercised the real Artifact plane"),
                console,
            ],
            "assertions": [
                check("artifact_ready", "passed", &format!("Artifact {} reached Ready after the exact verification Operation succeeded", self.artifact_id)),
                check("typed_link", "passed", "the PostgreSQL authority retained exactly one active typed reference link for the exact Artifact generation"),
                check("controlled_download", "passed", "CLI and independent raw HTTP reads returned the exact canonical bytes, Content-Length, media type and sha256 digest"),
            ],
            "failure_probes": [
                check("digest_mismatch", "passed", "a real upload whose bytes differed from expected_digest failed verification, entered the non-readable quarantined safety gate and exposed no content"),
                check("wrong_tenant_read", "passed", "the Artifact read authority rejected the exact valid ArtifactRef when only tenant_id was replaced by another valid tenant identifier"),
            ],
        })
    }
}

pub(super) fn run(
    insight: &Path,
    project: &Path,
    fixture: &Path,
    ready_upload: &Value,
    ready_document: &Value,
    console_passed: bool,
) -> ArtifactLifecycleEvidence {
    let started_at = Utc::now();
    let artifact_id = ready_upload["artifact_id"]
        .as_str()
        .expect("Ready Artifact ID");
    let operation_id = ready_upload["operation_id"]
        .as_str()
        .expect("Ready Artifact Operation ID");
    let operation = run_json(
        insight,
        &[
            "operation",
            "wait",
            operation_id,
            "--timeout-seconds",
            "120",
            "--path",
            project.to_str().unwrap(),
        ],
    );
    assert_eq!(operation["state"], "succeeded");
    let artifact = run_json(
        insight,
        &[
            "artifact",
            "get",
            artifact_id,
            "--path",
            project.to_str().unwrap(),
        ],
    );
    assert_eq!(artifact["state"], "ready");
    assert_eq!(
        artifact["content"]["content_digest"],
        ready_upload["content_digest"]
    );

    let output = fixture.join("artifact-lifecycle-download.json");
    let download = run_json(
        insight,
        &[
            "artifact",
            "read",
            artifact_id,
            "--output",
            output.to_str().unwrap(),
            "--path",
            project.to_str().unwrap(),
        ],
    );
    let expected_bytes = canonical_bytes(ready_document);
    assert_eq!(
        fs::read(&output).expect("Artifact download is readable"),
        expected_bytes
    );
    assert_eq!(download["content_digest"], ready_upload["content_digest"]);

    prove_raw_controlled_read(project, artifact_id, ready_upload, &expected_bytes);
    prove_typed_link_and_wrong_tenant(project, ready_upload);
    prove_digest_mismatch(
        project,
        &canonical_bytes(&json!({
            "schema_version": 1,
            "kind": "deliberate-artifact-digest-mismatch"
        })),
    );

    ArtifactLifecycleEvidence {
        artifact_id: artifact_id.to_owned(),
        console_passed,
        started_at,
        finished_at: Utc::now(),
    }
}

fn prove_raw_controlled_read(
    project: &Path,
    artifact_id: &str,
    upload: &Value,
    expected_bytes: &[u8],
) {
    let (client, base_url, token) = raw_runtime_client(project);
    let metadata = client
        .get(format!("{base_url}/v1/artifacts/{artifact_id}"))
        .bearer_auth(&token)
        .header("accept", "application/json")
        .send()
        .expect("raw Artifact metadata read completes");
    assert_eq!(metadata.status(), StatusCode::OK);
    assert!(metadata.headers().contains_key("trace-id"));
    let metadata: Value = metadata.json().expect("Artifact metadata is JSON");
    assert_eq!(metadata["state"], "ready");
    let content = client
        .get(format!("{base_url}/v1/artifacts/{artifact_id}/content"))
        .bearer_auth(token)
        .header("accept", "application/json")
        .send()
        .expect("raw controlled Artifact read completes");
    assert_eq!(content.status(), StatusCode::OK);
    assert_eq!(
        content.content_length(),
        Some(u64::try_from(expected_bytes.len()).expect("bounded fixture length"))
    );
    assert_eq!(
        content
            .headers()
            .get("content-type")
            .and_then(|value| value.to_str().ok()),
        upload["media_type"].as_str()
    );
    let bytes = content.bytes().expect("raw controlled Artifact bytes");
    assert_eq!(bytes.as_ref(), expected_bytes);
    assert_eq!(
        canonical_digest_bytes(bytes.as_ref()),
        upload["content_digest"]
    );
}

fn prove_typed_link_and_wrong_tenant(project: &Path, upload: &Value) {
    let identity: Value = serde_json::from_slice(
        &fs::read(project.join(".insight/project.json")).expect("project identity is readable"),
    )
    .expect("project identity is closed JSON");
    let tenant_id = identity["identity"]["tenant_id"]
        .as_str()
        .expect("development tenant ID")
        .parse::<ResourceId>()
        .expect("development tenant ID is valid");
    let principal_id = identity["identity"]["developer_principal_id"]
        .as_str()
        .expect("developer principal ID")
        .parse::<ResourceId>()
        .expect("developer principal ID is valid");
    let artifact_id = upload["artifact_id"]
        .as_str()
        .expect("Artifact ID")
        .parse::<ResourceId>()
        .expect("Artifact ID is valid");
    let digest = upload["content_digest"]
        .as_str()
        .expect("Artifact digest")
        .parse::<Sha256Digest>()
        .expect("Artifact digest is valid");
    let byte_length = upload["byte_length"]
        .as_u64()
        .expect("Artifact byte length");
    let media_type = upload["media_type"].as_str().expect("Artifact media type");
    let artifact = ArtifactRef::new(
        artifact_id.clone(),
        digest,
        byte_length,
        media_type,
        DataClassification::Internal,
        Some("typed-plan.json".to_owned()),
    )
    .expect("exact ArtifactRef is valid");
    let runtime = tokio::runtime::Runtime::new().expect("Artifact authority runtime");
    runtime.block_on(async move {
        let pool = PgPoolOptions::new()
            .max_connections(2)
            .connect(DATABASE_URL)
            .await
            .expect("fresh PostgreSQL authority is reachable");
        let active_links: i64 = sqlx::query(
            "SELECT count(*) AS count FROM insight_platform.artifact_links WHERE tenant_id = $1 AND target_artifact_id = $2 AND link_kind = 'reference' AND state = 'active'",
        )
        .bind(tenant_id.to_string())
        .bind(artifact_id.to_string())
        .fetch_one(&pool)
        .await
        .expect("typed Artifact links are readable")
        .try_get("count")
        .expect("typed Artifact link count");
        assert_eq!(active_links, 1, "exactly one active typed Artifact reference");
        let repository = PgRepository::new(pool);
        let wrong_tenant = ResourceId::from_uuid_v7(ResourceKind::Tenant, Uuid::now_v7())
            .expect("fresh wrong tenant ID");
        let request = GatewayArtifactReadRequest {
            tenant_id: wrong_tenant,
            principal_id,
            principal_kind: PrincipalKind::AgentAuthor,
            artifact,
            request_digest: canonical_digest(&json!({
                "schema_version": 1,
                "probe": "wrong_tenant_artifact_read"
            }))
            .parse()
            .expect("request digest is valid"),
            maximum_bytes: usize::try_from(byte_length).expect("bounded Artifact length"),
            deadline: Utc::now() + Duration::minutes(1),
        };
        assert!(matches!(
            repository.authorize_object_read(&request).await,
            Err(ArtifactObjectReadAuthorityError::Denied)
                | Err(ArtifactObjectReadAuthorityError::NotFound)
        ));
    });
}

fn prove_digest_mismatch(project: &Path, bytes: &[u8]) {
    let (client, base_url, token) = raw_runtime_client(project);
    let prepare = client
        .post(format!("{base_url}/v1/artifacts:prepare-upload"))
        .bearer_auth(&token)
        .header("accept", "application/json")
        .header("content-type", "application/json")
        .header("idempotency-key", format!("artifact-mismatch-{}", Uuid::now_v7()))
        .json(&json!({
            "schema_version": 1,
            "purpose": "diagnostic",
            "classification": "internal",
            "expected_size_bytes": bytes.len(),
            "expected_digest": "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            "declared_media_type": "application/json",
            "display_name": "deliberate-digest-mismatch.json"
        }))
        .send()
        .expect("digest mismatch prepare completes");
    assert_eq!(prepare.status(), StatusCode::CREATED);
    let etag = prepare
        .headers()
        .get("etag")
        .and_then(|value| value.to_str().ok())
        .expect("prepared Artifact ETag")
        .to_owned();
    let prepared: Value = prepare.json().expect("prepared Artifact response is JSON");
    let artifact_id = prepared["artifact_id"]
        .as_str()
        .expect("prepared Artifact ID");
    let operation_id = prepared["operation_id"]
        .as_str()
        .expect("verification Operation ID");
    let upload_url = prepared["upload_target"]["url"]
        .as_str()
        .expect("secret-bearing upload URL");
    let completion_proof = prepared["upload_target"]["completion_proof"]
        .as_str()
        .expect("upload completion proof");
    let root = Certificate::from_pem(
        &fs::read(project.join(".insight/runtime/tls/ca.pem"))
            .expect("local Artifact CA is readable"),
    )
    .expect("local Artifact CA is valid");
    let upload_client = Client::builder()
        .redirect(Policy::none())
        .no_proxy()
        .add_root_certificate(root)
        .timeout(StdDuration::from_secs(30))
        .build()
        .expect("isolated Artifact upload client builds");
    let uploaded = upload_client
        .put(upload_url)
        .header("content-length", bytes.len())
        .header("content-type", "application/json")
        .body(bytes.to_vec())
        .send()
        .expect("deliberate mismatch object upload completes");
    assert_eq!(uploaded.status(), StatusCode::OK);
    let completed = client
        .post(format!(
            "{base_url}/v1/artifacts/{artifact_id}:complete-upload"
        ))
        .bearer_auth(&token)
        .header("accept", "application/json")
        .header("content-type", "application/json")
        .header(
            "idempotency-key",
            format!("artifact-mismatch-complete-{}", Uuid::now_v7()),
        )
        .header("if-match", etag)
        .json(&json!({"schema_version": 1, "completion_proof": completion_proof}))
        .send()
        .expect("digest mismatch completion is accepted");
    assert_eq!(completed.status(), StatusCode::ACCEPTED);

    let deadline = Instant::now() + StdDuration::from_secs(30);
    loop {
        let operation = client
            .get(format!("{base_url}/v1/operations/{operation_id}"))
            .bearer_auth(&token)
            .header("accept", "application/json")
            .send()
            .expect("verification Operation polling completes");
        assert_eq!(operation.status(), StatusCode::OK);
        let operation: Value = operation.json().expect("verification Operation is JSON");
        if operation["state"] == "failed" {
            break;
        }
        assert!(
            matches!(
                operation["state"].as_str(),
                Some("queued" | "running" | "waiting")
            ),
            "digest mismatch reached unexpected Operation state: {operation}"
        );
        assert!(
            Instant::now() < deadline,
            "digest mismatch Operation did not terminate"
        );
        thread::sleep(StdDuration::from_millis(100));
    }
    let quarantined = client
        .get(format!("{base_url}/v1/artifacts/{artifact_id}"))
        .bearer_auth(token)
        .header("accept", "application/json")
        .send()
        .expect("quarantined Artifact read completes");
    assert_eq!(quarantined.status(), StatusCode::OK);
    let quarantined: Value = quarantined.json().expect("quarantined Artifact is JSON");
    assert_eq!(quarantined["state"], "quarantined");
    assert!(quarantined["content"].is_null());
}

fn canonical_digest_bytes(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    let mut digest = String::from("sha256:");
    for byte in hasher.finalize() {
        write!(&mut digest, "{byte:02x}").expect("writing to String cannot fail");
    }
    digest
}
