//! Actual development dependency checks; CORS never supplies upload authorization.
use super::*;
use insight_platform_contracts::{ResourceId, ResourceKind};

fn localstack_container(profile: &Value, processes: &Value) -> Result<String, &'static str> {
    let tenant = profile["tenant_id"]
        .as_str()
        .and_then(|id| ResourceId::parse_expected(id, ResourceKind::Tenant).ok())
        .ok_or("runtime profile must contain a valid tenant identity")?;
    let expected = format!("insight-{}", tenant.uuid().simple());
    if processes["compose_project"].as_str() != Some(expected.as_str()) {
        return Err("Compose project must match the runtime profile's complete tenant identity");
    }
    Ok(format!("{expected}-localstack-1"))
}

#[test]
fn localstack_probe_requires_the_exact_current_tenant_namespace() {
    let profile = json!({"tenant_id": "ten_019a7c80-0000-7000-8000-000000000001"});
    let processes = json!({"compose_project": "insight-019a7c80000070008000000000000001"});
    assert_eq!(
        localstack_container(&profile, &processes).unwrap(),
        "insight-019a7c80000070008000000000000001-localstack-1"
    );
    for wrong in [
        json!({"compose_project": "insight-019a7c80000070008000000000000002"}),
        json!({"compose_project": "insight-019a7c80"}),
        json!({"compose_project": "insight-019A7C80000070008000000000000001"}),
        json!({"compose_project": null}),
        json!({}),
    ] {
        assert!(localstack_container(&profile, &wrong).is_err(), "{wrong}");
    }
    for invalid in [
        json!("ten_019a7c80"),
        json!("agt_019a7c80-0000-7000-8000-000000000001"),
        json!("ten_019a7c80-0000-4000-8000-000000000001"),
        json!("ten_019A7C80-0000-7000-8000-000000000001"),
        Value::Null,
    ] {
        assert!(
            localstack_container(&json!({"tenant_id": invalid}), &processes).is_err(),
            "{invalid}"
        );
    }
    assert!(localstack_container(&json!({}), &processes).is_err());
}

fn aws(container: &str, args: &[&str]) -> Result<Value, String> {
    let output = Command::new("docker")
        .args(["exec", container, "awslocal", "s3api"])
        .args(args)
        .output()
        .map_err(|e| e.to_string())?;
    if !output.status.success() || output.stdout.len() > 65_536 {
        return Err(format!(
            "bounded local S3 command failed: {}",
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    if output.stdout.is_empty() {
        return Ok(Value::Null);
    }
    serde_json::from_slice(&output.stdout).map_err(|e| e.to_string())
}
fn cli(insight: &Path, project: &Path, action: &str) -> Result<std::process::Output, String> {
    Command::new(insight)
        .current_dir(workspace_root())
        .args([action, "--path", project.to_str().unwrap()])
        .output()
        .map_err(|e| e.to_string())
}
pub(super) fn verify(insight: &Path, project: &Path) {
    let runtime = project.join(".insight/runtime");
    let profile_bytes = fs::read(runtime.join("profile.json")).unwrap();
    let bootstrap_bytes = fs::read(runtime.join("config/artifact-bootstrap.json")).unwrap();
    let profile: Value = serde_json::from_slice(&profile_bytes).unwrap();
    let processes: Value =
        serde_json::from_slice(&fs::read(runtime.join("processes.json")).unwrap()).unwrap();
    let container = localstack_container(&profile, &processes).unwrap();
    let bucket = profile["s3_bucket"].as_str().unwrap();
    let expected = json!({"CORSRules":[{"ID":"insight-loopback-upload-v1","AllowedOrigins":["http://127.0.0.1:*"],"AllowedMethods":["PUT"],"AllowedHeaders":["content-type"],"MaxAgeSeconds":0}]});
    let observed = aws(&container, &["get-bucket-cors", "--bucket", bucket]).unwrap();
    assert_eq!(
        observed, expected,
        "actual initial provisioning installs only the reviewed local upload rule"
    );
    let config: Value =
        serde_json::from_slice(&fs::read(runtime.join("config/artifact-gateway.json")).unwrap())
            .unwrap();
    let endpoint = config["artifact_provider_catalog"]["s3_storage_bindings"][0]["endpoint"]
        .as_str()
        .unwrap();
    let target = format!(
        "{}/{bucket}/cors-transport-probe",
        endpoint.trim_end_matches('/')
    );
    let client = Client::builder()
        .timeout(StdDuration::from_secs(10))
        .redirect(Policy::none())
        .build()
        .unwrap();
    let allowed_origin = "http://127.0.0.1:50999";
    for (origin, method, headers, allowed) in [
        (allowed_origin, "PUT", "content-type", true),
        ("https://foreign.invalid", "PUT", "content-type", false),
        (allowed_origin, "GET", "content-type", false),
        (allowed_origin, "PUT", "x-unknown-header", false),
    ] {
        let response = client
            .request(reqwest::Method::OPTIONS, &target)
            .header("Origin", origin)
            .header("Access-Control-Request-Method", method)
            .header("Access-Control-Request-Headers", headers)
            .send()
            .unwrap();
        assert_eq!(
            response.status().is_success(),
            allowed,
            "actual S3 preflight {origin}/{method}/{headers}"
        );
        if allowed {
            assert_eq!(
                response
                    .headers()
                    .get("access-control-allow-origin")
                    .unwrap(),
                allowed_origin
            );
            assert!(response
                .headers()
                .get("access-control-allow-methods")
                .unwrap()
                .to_str()
                .unwrap()
                .split(',')
                .any(|method| method.trim() == "PUT"));
            // S3 may return Allow-Credentials:true. The request's signed upload capability
            // and the browser client's credentials:omit remain the authorization boundary.
        } else {
            assert!(response
                .headers()
                .get("access-control-allow-origin")
                .is_none());
        }
    }
    let running_state = fs::read(runtime.join("processes.json")).unwrap();
    let mut narrowed = expected.clone();
    narrowed["CORSRules"][0]["AllowedOrigins"] = json!(["http://127.0.0.1:1"]);
    let changed = narrowed.to_string();
    let negative = (|| -> Result<(), String> {
        aws(
            &container,
            &[
                "put-bucket-cors",
                "--bucket",
                bucket,
                "--cors-configuration",
                &changed,
            ],
        )?;
        let running = cli(insight, project, "start")?;
        let stderr = String::from_utf8_lossy(&running.stderr);
        if running.status.success() || !stderr.contains("CORS drifted") {
            return Err(format!("running start must reject CORS drift: {stderr}"));
        }
        if fs::read(runtime.join("processes.json")).map_err(|e| e.to_string())? != running_state {
            return Err("running CORS rejection changed owned role identities".into());
        }
        if aws(&container, &["get-bucket-cors", "--bucket", bucket])? != narrowed {
            return Err("running start silently repaired CORS drift".into());
        }
        let stopped = cli(insight, project, "stop")?;
        if !stopped.status.success() {
            return Err(format!(
                "CORS fixture stop failed: {}",
                String::from_utf8_lossy(&stopped.stderr)
            ));
        }
        let failed = cli(insight, project, "start")?;
        let stderr = String::from_utf8_lossy(&failed.stderr);
        if failed.status.success() || !stderr.contains("CORS drifted") {
            return Err(format!(
                "drift must reject startup at the CORS boundary: {stderr}"
            ));
        }
        if aws(&container, &["get-bucket-cors", "--bucket", bucket])? != narrowed {
            return Err("CLI silently repaired CORS drift".into());
        }
        match fs::read(runtime.join("processes.json")) {
            Ok(bytes) => {
                let state: Value = serde_json::from_slice(&bytes).map_err(|e| e.to_string())?;
                if !state["processes"].as_object().is_some_and(|p| p.is_empty()) {
                    return Err("rejected start launched owned roles".into());
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.to_string()),
        }
        for process in processes["processes"].as_object().unwrap().values() {
            if process_is_running(process["pid"].as_u64().unwrap()) {
                return Err("a previously owned process survived rejected stopped startup".into());
            }
        }
        if runtime.join("config-transition.json").exists() {
            return Err("rejected start left a transition journal".into());
        }
        if fs::read(runtime.join("profile.json")).map_err(|e| e.to_string())? != profile_bytes {
            return Err("CORS rejection changed the frozen runtime profile".into());
        }
        Ok(())
    })();
    // Restore explicitly as test-owned dependency cleanup, regardless of the negative assertion.
    let restore = aws(
        &container,
        &[
            "put-bucket-cors",
            "--bucket",
            bucket,
            "--cors-configuration",
            &expected.to_string(),
        ],
    );
    if let Err(error) = restore {
        panic!("CORS fixture restoration failed: {error}; negative result: {negative:?}");
    }
    if let Err(error) = negative {
        panic!("CORS drift behavior failed after restoration: {error}");
    }
    assert_eq!(
        aws(&container, &["get-bucket-cors", "--bucket", bucket]).unwrap(),
        expected
    );
    let restarted = cli(insight, project, "start").unwrap();
    assert!(
        restarted.status.success(),
        "restored CORS permits ordinary startup: {}",
        String::from_utf8_lossy(&restarted.stderr)
    );
    assert_eq!(
        fs::read(runtime.join("profile.json")).unwrap(),
        profile_bytes
    );
    assert_eq!(
        fs::read(runtime.join("config/artifact-bootstrap.json")).unwrap(),
        bootstrap_bytes,
        "normal restart preserves the exact durable bootstrap identities and policies"
    );
    fs::write(runtime.join("logs/native-artifact-cors.json"),serde_json::to_vec_pretty(&json!({"kind":"insight.local-artifact-cors-probe/v1","allowed_preflight":true,"foreign_origin_rejected":true,"get_rejected":true,"unknown_header_rejected":true,"drift_start_rejected":true,"running_drift_start_rejected":true,"automatic_repair":false,"profile_preserved":true,"bootstrap_seed_bytes_preserved":true,"restored_start_succeeded":true,"browser_upload":"requires_separate_actual_chrome_journey"})).unwrap()).unwrap();
}

#[test]
#[ignore = "targeted actual development CORS lifecycle; also invoked by the full native journey"]
fn actual_development_artifact_cors_lifecycle() {
    let project = env::var(PROJECT_ENV).expect("explicit task-owned native project");
    let insight = env::var(INSIGHT_BIN_ENV).expect("actual built CLI");
    verify(Path::new(&insight), Path::new(&project));
}
