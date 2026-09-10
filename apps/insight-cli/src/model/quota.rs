use super::*;
use crate::public_client::PublicJsonResponse;
use insight_platform_contracts::*;
use insight_platform_deployment_tooling::private_state::InstallationDirectory;
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Intent {
    schema_version: u16,
    endpoint: String,
    tenant: ResourceId,
    selector: Option<String>,
    nonce: uuid::Uuid,
    request: SetModelQuotaRequestV1,
    etag: String,
    completed: bool,
}
impl Intent {
    fn validate(&self, client: &PublicHttpClient, tenant: &ResourceId) -> Result<(), String> {
        if self.schema_version != 1
            || self.endpoint != client.origin()
            || &self.tenant != tenant
            || self.nonce.get_version_num() != 4
            || self.request.validate().is_err()
            || validate_model_quota_etag(&self.etag).is_err()
        {
            return Err("quota recovery belongs to another installation or is invalid".to_owned());
        }
        Ok(())
    }
}
pub(super) fn read_quota(
    client: &PublicHttpClient,
    tenant: &ResourceId,
    target: &ResourceId,
) -> Result<ModelQuotaViewV1, String> {
    if target.kind() != ResourceKind::ModelDeployment {
        return Err("invalid quota deployment".to_owned());
    }
    let value: PublicJsonResponse<ModelQuotaViewV1> = client
        .get_json(&format!("/v1/model-quotas/{target}"), StatusCode::OK)
        .map_err(|error| error.to_string())?;
    if value.body.validate().is_err()
        || &value.body.tenant_id != tenant
        || &value.body.model_deployment.deployment_id != target
        || value.etag != value.body.etag
    {
        return Err("quota response has an invalid authority".to_owned());
    }
    Ok(value.body)
}
fn apply(
    client: &PublicHttpClient,
    tenant: &ResourceId,
    state: &InstallationDirectory,
    name: &str,
    mut intent: Intent,
) -> Result<ModelQuotaViewV1, String> {
    intent.validate(client, tenant)?;
    let matches = |value: &ModelQuotaViewV1| {
        value.validate().is_ok()
            && &value.tenant_id == tenant
            && value.model_deployment == intent.request.model_deployment
            && value
                .allocation
                .as_ref()
                .is_some_and(|allocation| allocation.limits == intent.request.limits)
    };
    if !intent.completed {
        let result: PublicJsonResponse<ModelQuotaViewV1> = client
            .put_json(
                &format!(
                    "/v1/model-quotas/{}",
                    intent.request.model_deployment.deployment_id
                ),
                &intent.request,
                StatusCode::OK,
                &format!("model-quota-{}", intent.nonce),
                &intent.etag,
            )
            .map_err(|error| error.to_string())?;
        if !matches(&result.body) || result.etag != result.body.etag {
            return Err("quota response differs from the original allocation".to_owned());
        }
    }
    let current = read_quota(
        client,
        tenant,
        &intent.request.model_deployment.deployment_id,
    )?;
    if !matches(&current) {
        return Err(
            "current quota differs from the allocation; reconcile before a new attempt".to_owned(),
        );
    }
    intent.completed = true;
    workflow::save(state, name, &intent)?;
    Ok(current)
}
fn new_intent(
    client: &PublicHttpClient,
    tenant: &ResourceId,
    target: &ExactDeploymentRef,
    limits: ModelQuotaLimitsV1,
    selector: Option<String>,
) -> Result<Intent, String> {
    let current = read_quota(client, tenant, &target.deployment_id)?;
    if current.model_deployment != *target {
        return Err("quota target digest differs from the selected deployment".to_owned());
    }
    let intent = Intent {
        schema_version: 1,
        endpoint: client.origin().to_owned(),
        tenant: tenant.clone(),
        selector,
        nonce: uuid::Uuid::new_v4(),
        request: SetModelQuotaRequestV1 {
            schema_version: 1,
            model_deployment: target.clone(),
            limits,
        },
        etag: current.etag,
        completed: false,
    };
    intent.validate(client, tenant)?;
    Ok(intent)
}
/// A configure step freezes allocation independently of changing usage or the Resource's head.
pub(super) fn configure(
    client: &PublicHttpClient,
    tenant: &ResourceId,
    state: &InstallationDirectory,
    name: &str,
    target: &ExactDeploymentRef,
    limits: ModelQuotaLimitsV1,
    completed: bool,
) -> Result<ModelQuotaViewV1, String> {
    limits
        .validate()
        .map_err(|_| "quota limits must be finite safe integers")?;
    let intent: Intent = match workflow::read(state, name)? {
        Some(intent) => intent,
        None if completed => {
            return Err("completed model configuration is missing its quota intent".to_owned());
        }
        None => {
            let intent = new_intent(client, tenant, target, limits, None)?;
            workflow::save(state, name, &intent)?;
            intent
        }
    };
    if intent.request.model_deployment != *target
        || intent.request.limits != limits
        || intent.selector.is_some()
    {
        return Err("quota recovery differs from the original configuration".to_owned());
    }
    apply(client, tenant, state, name, intent)
}
fn target(
    client: &PublicHttpClient,
    tenant: &ResourceId,
    selector: &str,
) -> Result<ExactDeploymentRef, String> {
    match ResourceId::parse_expected(selector, ResourceKind::ModelDeployment) {
        Ok(id) => Ok(read_quota(client, tenant, &id)?.model_deployment),
        Err(_) => management::model(client, selector),
    }
}
pub(super) fn execute(
    client: &PublicHttpClient,
    command: &Command,
    cwd: &Path,
) -> Result<Value, String> {
    let selector = command
        .model
        .as_deref()
        .ok_or("quota model selector is required")?;
    let Some(file) = &command.file else {
        let target = target(client, &command.tenant, selector)?;
        return serde_json::to_value(read_quota(client, &command.tenant, &target.deployment_id)?)
            .map_err(|_| "invalid quota report".to_owned());
    };
    let path = absolute(cwd, file);
    let metadata = std::fs::symlink_metadata(&path).map_err(|_| "cannot read quota limits file")?;
    if !metadata.is_file() || metadata.file_type().is_symlink() || metadata.len() > 2048 {
        return Err("quota limits must be a bounded regular JSON file".to_owned());
    }
    let bytes = std::fs::read(&path).map_err(|_| "cannot read quota limits file")?;
    let limits = parse_limits(&bytes)?;
    let state = InstallationDirectory::open(&absolute(cwd, &command.state_dir), true)
        .map_err(|_| "quota state directory must be private and unlocked")?;
    let saved: Option<Intent> = workflow::read(&state, "model-quota.json")?;
    if let Some(intent) = &saved {
        intent.validate(client, &command.tenant)?;
    }
    let intent = match saved {
        Some(intent)
            if intent.selector.as_deref() == Some(selector)
                && intent.request.limits == limits
                && !command.new_attempt =>
        {
            intent
        }
        Some(intent) if !intent.completed => {
            return Err(
                "resume the pending quota allocation with its original arguments".to_owned(),
            );
        }
        _ => {
            let exact = target(client, &command.tenant, selector)?;
            let intent = new_intent(
                client,
                &command.tenant,
                &exact,
                limits,
                Some(selector.to_owned()),
            )?;
            workflow::save(&state, "model-quota.json", &intent)?;
            intent
        }
    };
    serde_json::to_value(apply(
        client,
        &command.tenant,
        &state,
        "model-quota.json",
        intent,
    )?)
    .map_err(|_| "invalid quota report".to_owned())
}

fn parse_limits(bytes: &[u8]) -> Result<ModelQuotaLimitsV1, String> {
    let value = parse_strict_json(
        bytes,
        JsonLimits {
            max_bytes: 2048,
            max_depth: 2,
            max_items_per_array: 1,
            max_properties_per_object: 3,
            max_string_bytes: 32,
        },
    )
    .map_err(|_| "invalid quota limits JSON")?;
    let limits: ModelQuotaLimitsV1 = serde_json::from_value(value)
        .map_err(|_| "quota limits require requests, tokens and cost_microunits")?;
    limits
        .validate()
        .map_err(|_| "quota limits must be finite safe integers")?;
    Ok(limits)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        collections::BTreeMap,
        io::{BufRead, Read, Write},
        net::TcpListener,
    };
    #[test]
    fn standalone_quota_file_decodes_exact_flat_integer_limits() {
        let limits =
            parse_limits(br#"{"requests":20,"tokens":204800,"cost_microunits":20000000}"#).unwrap();
        assert_eq!(limits.values(), [20, 204800, 20000000]);
        for bad in [
            br#"{"requests":20,"tokens":1,"cost_microunits":0,"extra":0}"#.as_slice(),
            br#"{"requests":20,"requests":21,"tokens":1,"cost_microunits":0}"#,
            br#"{"requests":{"value":20},"tokens":1,"cost_microunits":0}"#,
            br#"{"requests":-1,"tokens":1,"cost_microunits":0}"#,
        ] {
            assert!(parse_limits(bad).is_err());
        }
    }
    #[test]
    fn quota_response_loss_replays_original_target_receipt_and_cas_then_checks_current_limits() {
        let tenant: ResourceId = "ten_0198f1cc-32e4-75e1-a9e8-000000000001".parse().unwrap();
        let target = ExactDeploymentRef::new(
            "mdep_0198f1cc-32e4-75e1-a9e8-000000000002".parse().unwrap(),
            format!("sha256:{}", "a".repeat(64)).parse().unwrap(),
        )
        .unwrap();
        let limits = ModelQuotaLimitsV1 {
            requests: 20,
            tokens: 204800,
            cost_microunits: 20000000,
        };
        let zero = ModelQuotaLimitsV1::from_values([0; 3]).unwrap();
        let original = ModelQuotaViewV1 {
            schema_version: 1,
            tenant_id: tenant.clone(),
            model_deployment: target.clone(),
            allocation: None,
            tenant_concurrency: ModelQuotaCounterV1 {
                limit: 8,
                reserved: 0,
                used: 0,
            },
            etag: format!("\"model-quota-{}\"", "a".repeat(64)),
        };
        let allocated = ModelQuotaViewV1 {
            allocation: Some(ModelQuotaAllocationV1 {
                limits,
                reserved: zero,
                used: zero,
            }),
            etag: format!("\"model-quota-{}\"", "b".repeat(64)),
            ..original.clone()
        };
        let mut consumed = allocated.clone();
        consumed.allocation.as_mut().unwrap().used.requests = 2;
        consumed.etag = format!("\"model-quota-{}\"", "c".repeat(64));
        let mut drift = consumed.clone();
        drift.allocation.as_mut().unwrap().limits.requests = 30;
        drift.etag = format!("\"model-quota-{}\"", "d".repeat(64));
        let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap());
        let route = format!("/v1/model-quotas/{}", target.deployment_id);
        let expected = serde_json::to_value(SetModelQuotaRequestV1 {
            schema_version: 1,
            model_deployment: target.clone(),
            limits,
        })
        .unwrap();
        let worker = std::thread::spawn(move || {
            let mut receipt = None;
            for step in 0..5 {
                let (mut stream, _) = listener.accept().unwrap();
                stream
                    .set_read_timeout(Some(Duration::from_secs(3)))
                    .unwrap();
                let mut reader = std::io::BufReader::new(&mut stream);
                let mut line = String::new();
                reader.read_line(&mut line).unwrap();
                assert_eq!(
                    line,
                    format!(
                        "{} {route} HTTP/1.1\r\n",
                        if step == 1 || step == 2 { "PUT" } else { "GET" }
                    )
                );
                let mut headers = BTreeMap::new();
                loop {
                    line.clear();
                    reader.read_line(&mut line).unwrap();
                    if line == "\r\n" {
                        break;
                    }
                    let (name, value) = line.split_once(':').unwrap();
                    headers.insert(name.to_ascii_lowercase(), value.trim().to_owned());
                }
                if step == 1 || step == 2 {
                    let len: usize = headers["content-length"].parse().unwrap();
                    assert!(len < 2048);
                    let mut bytes = vec![0; len];
                    reader.read_exact(&mut bytes).unwrap();
                    assert_eq!(serde_json::from_slice::<Value>(&bytes).unwrap(), expected);
                    assert_eq!(headers["if-match"], original.etag);
                    if step == 1 {
                        receipt = Some(headers["idempotency-key"].clone());
                        continue;
                    }
                    assert_eq!(receipt.as_ref(), headers.get("idempotency-key"));
                }
                let view = match step {
                    0 => &original,
                    2 => &allocated,
                    3 => &consumed,
                    4 => &drift,
                    _ => unreachable!(),
                };
                let body = serde_json::to_vec(view).unwrap();
                let trace = headers
                    .get("traceparent")
                    .and_then(|value| value.split('-').nth(1))
                    .unwrap_or("11111111111111111111111111111111");
                write!(stream, "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncache-control: no-store, private, max-age=0\r\ntrace-id: {trace}\r\netag: {}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n", view.etag, body.len()).unwrap();
                stream.write_all(&body).unwrap();
            }
        });
        let client = PublicHttpClient::new(
            origin,
            "private-test-session".to_owned(),
            Duration::from_secs(3),
        )
        .unwrap();
        let root = tempfile::TempDir::new().unwrap();
        let path = root.path().canonicalize().unwrap().join("state");
        let state = InstallationDirectory::open(&path, true).unwrap();
        assert!(configure(
            &client,
            &tenant,
            &state,
            "quota.json",
            &target,
            limits,
            false
        )
        .is_err());
        let pending = state.read("quota.json", 4096).unwrap().unwrap();
        let mut changed = limits;
        changed.requests += 1;
        assert!(configure(
            &client,
            &tenant,
            &state,
            "quota.json",
            &target,
            changed,
            false
        )
        .is_err());
        assert_eq!(pending, state.read("quota.json", 4096).unwrap().unwrap());
        assert_eq!(
            configure(
                &client,
                &tenant,
                &state,
                "quota.json",
                &target,
                limits,
                false
            )
            .unwrap()
            .allocation
            .unwrap()
            .used
            .requests,
            2
        );
        assert!(configure(
            &client,
            &tenant,
            &state,
            "quota.json",
            &target,
            limits,
            true
        )
        .unwrap_err()
        .contains("current quota differs"));
        assert!(configure(
            &client,
            &tenant,
            &state,
            "missing.json",
            &target,
            limits,
            true
        )
        .unwrap_err()
        .contains("missing"));
        assert!(!String::from_utf8(pending)
            .unwrap()
            .contains("private-test-session"));
        worker.join().unwrap();
    }
}
