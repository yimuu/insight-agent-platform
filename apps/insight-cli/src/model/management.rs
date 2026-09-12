use super::*;
use crate::public_client::PublicJsonResponse;
use insight_platform_api::{
    model_configuration::ModelConfigurationCatalogViewV2,
    model_credential_management::{ModelCredentialMetadataViewV1, RevokeModelCredentialRequestV1},
    resource::{DeploymentViewV1, ModelDefaultViewV1, ResourceViewV1},
};
use insight_platform_contracts::*;
use insight_platform_deployment_tooling::private_state::InstallationDirectory;
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Intent {
    schema_version: u16,
    endpoint: String,
    tenant: ResourceId,
    nonce: uuid::Uuid,
    completed: bool,
    operation: Operation,
}
#[derive(Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum Operation {
    Default {
        selector: Option<String>,
        model: Option<ExactDeploymentRef>,
        version: u64,
    },
    Revoke {
        credential: ModelCredentialMetadataViewV1,
    },
}
impl Intent {
    fn validate(&self, command: &Command) -> Result<(), String> {
        if self.schema_version != 1
            || self.endpoint != command.endpoint
            || self.tenant != command.tenant
            || self.nonce.get_version_num() != 4
        {
            return Err(
                "model management recovery belongs to another installation or is invalid"
                    .to_owned(),
            );
        }
        match &self.operation {
            Operation::Default {
                selector,
                model,
                version,
            } if *version > 0
                && *version <= i64::MAX as u64
                && selector.is_some() == model.is_some()
                && model.as_ref().is_none_or(|model| {
                    model.validate().is_ok() && model.resource_kind == ResourceKind::ModelDeployment
                }) =>
            {
                Ok(())
            }
            Operation::Revoke { credential }
                if credential.validate()
                    && credential.tenant_id == command.tenant
                    && credential.state == SecretBindingState::Active =>
            {
                Ok(())
            }
            _ => Err("model management recovery is invalid".to_owned()),
        }
    }
}
pub fn resource(
    client: &PublicHttpClient,
    kind: RegistryResourceKind,
    selector: &str,
) -> Result<ResourceViewV1, String> {
    let noun = match kind {
        RegistryResourceKind::ModelProvider => "model-providers",
        RegistryResourceKind::ModelProfile => "models",
        _ => return Err("invalid model kind".to_owned()),
    };
    let resource_id = match ResourceId::parse_expected(selector, kind.id_kind()) {
        Ok(id) => id,
        Err(_) => {
            let alias = selector
                .parse::<ResourceAlias>()
                .map_err(|_| "invalid model selector")?;
            let mut matches = workflow::list(client, kind)?
                .into_iter()
                .filter(|item| item.alias.as_ref() == Some(&alias));
            let found = matches.next().ok_or("model alias was not found")?;
            if matches.next().is_some() {
                return Err("model alias has multiple authorities".to_owned());
            }
            found.resource_id
        }
    };
    let response: PublicJsonResponse<ResourceViewV1> = client
        .get_json(&format!("/v1/{noun}/{resource_id}"), StatusCode::OK)
        .map_err(|e| e.to_string())?;
    if response.body.validate().is_err()
        || response.body.resource_id != resource_id
        || response.body.resource_kind != kind
        || response.etag != response.body.etag
    {
        return Err("model resource authority is invalid".to_owned());
    }
    Ok(response.body)
}
pub(super) fn model(
    client: &PublicHttpClient,
    selector: &str,
) -> Result<ExactDeploymentRef, String> {
    let resource = resource(client, RegistryResourceKind::ModelProfile, selector)?;
    if resource.gate_state != AdministrativeGate::Enabled
        || resource.lifecycle_state != EntityLifecycle::Active
    {
        return Err("model must be active and enabled".to_owned());
    }
    let id = resource
        .active_deployment_id
        .ok_or("model has no active deployment")?;
    let result: PublicJsonResponse<DeploymentViewV1> = client
        .get_json(
            &format!("/v1/models/{}/deployments/{id}", resource.resource_id),
            StatusCode::OK,
        )
        .map_err(|e| e.to_string())?;
    if result.body.validate().is_err()
        || result.body.resource_id != resource.resource_id
        || result.body.deployment_id != id
        || result.body.resource_kind != RegistryResourceKind::ModelProfile
        || result.etag != result.body.etag
    {
        return Err("model deployment authority is invalid".to_owned());
    }
    ExactDeploymentRef::new(id, result.body.closure_digest)
        .map_err(|_| "invalid model deployment".to_owned())
}
pub fn probe(client: &PublicHttpClient, command: &Command) -> Result<Value, String> {
    let target = model(
        client,
        command
            .model
            .as_deref()
            .ok_or("model selector is required")?,
    )?;
    let catalog: ModelConfigurationCatalogViewV2 = client
        .get_body_json("/v1/model-configuration", StatusCode::OK)
        .map_err(|e| e.to_string())?
        .body;
    if catalog.schema_version != 1 {
        return Err("invalid model installation catalog".to_owned());
    }
    let result = client
        .probe_model_connection(&ModelConnectionProbeRequestV1 {
            schema_version: 1,
            installation_digest: catalog.installation_digest,
            model_deployment: target,
        })
        .map_err(|e| e.to_string())?;
    serde_json::to_value(result).map_err(|_| "invalid model observation".to_owned())
}
pub fn credential(
    client: &PublicHttpClient,
    tenant: &ResourceId,
    binding: &ResourceId,
) -> Result<ModelCredentialMetadataViewV1, String> {
    if binding.kind() != ResourceKind::SecretBinding {
        return Err("invalid credential identity".to_owned());
    }
    let result: PublicJsonResponse<ModelCredentialMetadataViewV1> = client
        .get_json(&format!("/v1/model-credentials/{binding}"), StatusCode::OK)
        .map_err(|e| e.to_string())?;
    if !result.body.validate()
        || &result.body.tenant_id != tenant
        || &result.body.secret_binding_id != binding
        || result.etag != result.body.etag
    {
        return Err("invalid credential metadata authority".to_owned());
    }
    Ok(result.body)
}
pub fn set_default(
    client: &PublicHttpClient,
    command: &Command,
    path: &Path,
) -> Result<Value, String> {
    let state = InstallationDirectory::open(path, true)
        .map_err(|_| "model state directory must be private and unlocked")?;
    let current = workflow::default(client, &command.tenant)?;
    let saved: Option<Intent> = workflow::read(&state, "model-default.json")?;
    if let Some(value) = &saved {
        value.validate(command)?;
    }
    let mut intent = match saved {
        Some(intent)
            if matches!(&intent.operation,Operation::Default {selector,..} if selector==&command.model)
                && !command.new_attempt =>
        {
            intent
        }
        Some(intent) if !intent.completed => {
            return Err(
                "resume the unfinished default selection with its original arguments".to_owned(),
            )
        }
        _ => {
            let model = command
                .model
                .as_deref()
                .map(|selector| model(client, selector))
                .transpose()?;
            let intent = Intent {
                schema_version: 1,
                endpoint: command.endpoint.clone(),
                tenant: command.tenant.clone(),
                nonce: uuid::Uuid::new_v4(),
                completed: false,
                operation: Operation::Default {
                    selector: command.model.clone(),
                    model,
                    version: current.body.version,
                },
            };
            workflow::save(&state, "model-default.json", &intent)?;
            intent
        }
    };
    let Operation::Default { model, version, .. } = &intent.operation else {
        return Err("invalid default recovery operation".to_owned());
    };
    if !intent.completed {
        let result: PublicJsonResponse<ModelDefaultViewV1> = client
            .put_json(
                "/v1/model-default",
                &json!({"schema_version":1,"default_model":model}),
                StatusCode::OK,
                &format!("model-default-{}", intent.nonce),
                &insight_platform_api::resource::resource_etag(&command.tenant, *version),
            )
            .map_err(|e| e.to_string())?;
        if result.body.validate().is_err()
            || result.body.tenant_id != command.tenant
            || result.etag != result.body.etag
            || &result.body.default_model != model
        {
            return Err("default mutation does not match its original intent".to_owned());
        }
    }
    let result = workflow::default(client, &command.tenant)?;
    if &result.body.default_model != model {
        return Err("current default differs from this selection; use --new-attempt for a new explicit selection".to_owned());
    }
    intent.completed = true;
    workflow::save(&state, "model-default.json", &intent)?;
    serde_json::to_value(result.body).map_err(|_| "invalid default report".to_owned())
}
pub fn revoke(client: &PublicHttpClient, command: &Command, path: &Path) -> Result<Value, String> {
    let binding = command.binding.as_ref().ok_or("binding is required")?;
    let state = InstallationDirectory::open(path, true)
        .map_err(|_| "model state directory must be private and unlocked")?;
    let saved: Option<Intent> = workflow::read(&state, "model-revoke.json")?;
    if let Some(value) = &saved {
        value.validate(command)?;
    }
    let mut intent = match saved {
        Some(intent) if matches!(&intent.operation,Operation::Revoke {credential} if &credential.secret_binding_id==binding) => {
            intent
        }
        Some(intent) if !intent.completed => {
            return Err("resume the unfinished credential revocation first".to_owned())
        }
        _ => {
            let value = credential(client, &command.tenant, binding)?;
            if value.state == SecretBindingState::Revoked {
                return serde_json::to_value(value)
                    .map_err(|_| "invalid credential report".to_owned());
            }
            let intent = Intent {
                schema_version: 1,
                endpoint: command.endpoint.clone(),
                tenant: command.tenant.clone(),
                nonce: uuid::Uuid::new_v4(),
                completed: false,
                operation: Operation::Revoke { credential: value },
            };
            workflow::save(&state, "model-revoke.json", &intent)?;
            intent
        }
    };
    let Operation::Revoke {
        credential: original,
    } = &intent.operation
    else {
        return Err("invalid credential recovery operation".to_owned());
    };
    let verify = |value: &ModelCredentialMetadataViewV1| {
        value.validate()
            && value.tenant_id == command.tenant
            && value.secret_binding_id == original.secret_binding_id
            && value.provider_id == original.provider_id
            && value.state == SecretBindingState::Revoked
            && Some(value.generation) == original.generation.checked_add(1)
            && Some(value.version) == original.version.checked_add(1)
    };
    if !intent.completed {
        let result: PublicJsonResponse<ModelCredentialMetadataViewV1> = client
            .post_json(
                &format!("/v1/model-credentials/{binding}:revoke"),
                &RevokeModelCredentialRequestV1 {
                    schema_version: 1,
                    expected_generation: original.generation,
                },
                StatusCode::OK,
                &format!("model-revoke-{}", intent.nonce),
                Some(&original.etag),
            )
            .map_err(|e| e.to_string())?;
        if !verify(&result.body) || result.etag != result.body.etag {
            return Err("revocation does not match its original intent".to_owned());
        }
    }
    let value = credential(client, &command.tenant, binding)?;
    if !verify(&value) {
        return Err("current credential differs from the revocation".to_owned());
    }
    intent.completed = true;
    workflow::save(&state, "model-revoke.json", &intent)?;
    serde_json::to_value(value).map_err(|_| "invalid credential report".to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        collections::BTreeMap,
        io::{BufRead, Read, Write},
        net::{TcpListener, TcpStream},
    };
    fn receive(stream: &mut TcpStream) -> (String, BTreeMap<String, String>, Value) {
        stream
            .set_read_timeout(Some(Duration::from_secs(3)))
            .unwrap();
        let mut reader = std::io::BufReader::new(stream);
        let mut line = String::new();
        reader.read_line(&mut line).unwrap();
        let first = line.trim().to_owned();
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
        let length = headers
            .get("content-length")
            .map(|v| v.parse::<usize>().unwrap())
            .unwrap_or(0);
        assert!(length < 4096);
        let mut bytes = vec![0; length];
        reader.read_exact(&mut bytes).unwrap();
        (
            first,
            headers,
            if bytes.is_empty() {
                Value::Null
            } else {
                serde_json::from_slice(&bytes).unwrap()
            },
        )
    }
    fn send(
        stream: &mut TcpStream,
        headers: &BTreeMap<String, String>,
        value: &ModelDefaultViewV1,
    ) {
        let body = serde_json::to_vec(value).unwrap();
        let trace = headers
            .get("traceparent")
            .and_then(|value| value.split('-').nth(1))
            .unwrap_or("11111111111111111111111111111111");
        write!(stream,"HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncache-control: no-store, private, max-age=0\r\ntrace-id: {trace}\r\netag: {}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",value.etag,body.len()).unwrap();
        stream.write_all(&body).unwrap();
    }
    #[test]
    fn default_clear_retries_original_cas_after_response_loss_and_completed_state_detects_drift() {
        let tenant: ResourceId = "ten_0198f1cc-32e4-75e1-a9e8-000000000001".parse().unwrap();
        let model = ExactDeploymentRef::new(
            "mdep_0198f1cc-32e4-75e1-a9e8-000000000002".parse().unwrap(),
            format!("sha256:{}", "a".repeat(64)).parse().unwrap(),
        )
        .unwrap();
        let view = |version, model| ModelDefaultViewV1 {
            schema_version: 1,
            tenant_id: tenant.clone(),
            default_model: model,
            version,
            etag: insight_platform_api::resource::resource_etag(&tenant, version),
        };
        let original = view(9, Some(model.clone()));
        let cleared = view(10, None);
        let drift = view(11, Some(model));
        let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let server = std::thread::spawn(move || {
            let mut intent = None;
            for step in 0..7 {
                let (mut stream, _) = listener.accept().unwrap();
                let (route, headers, body) = receive(&mut stream);
                if step == 1 || step == 3 {
                    assert!(route.starts_with("PUT /v1/model-default "));
                    assert_eq!(body, json!({"schema_version":1,"default_model":null}));
                    assert_eq!(headers.get("if-match"), Some(&original.etag));
                    let current = (
                        headers["idempotency-key"].clone(),
                        headers["if-match"].clone(),
                        body,
                    );
                    if step == 1 {
                        intent = Some(current);
                        continue;
                    }
                    assert_eq!(intent.as_ref(), Some(&current));
                    send(&mut stream, &headers, &cleared);
                } else {
                    assert!(route.starts_with("GET /v1/model-default "));
                    send(
                        &mut stream,
                        &headers,
                        if step == 0 {
                            &original
                        } else if step >= 5 {
                            &drift
                        } else {
                            &cleared
                        },
                    );
                }
            }
        });
        let client = PublicHttpClient::new(
            endpoint.clone(),
            "private-test-session".into(),
            Duration::from_secs(3),
        )
        .unwrap();
        let command = Command {
            action: Action::Default,
            endpoint,
            tenant,
            token_file: "unused-session".into(),
            ca_file: None,
            state_dir: "unused-state".into(),
            file: None,
            new_attempt: false,
            model: None,
            source: None,
            binding: None,
            clear: true,
        };
        let directory = tempfile::TempDir::new().unwrap();
        let path = directory.path().canonicalize().unwrap().join("state");
        assert!(set_default(&client, &command, &path).is_err());
        assert_eq!(
            set_default(&client, &command, &path).unwrap()["default_model"],
            Value::Null
        );
        assert!(set_default(&client, &command, &path)
            .unwrap_err()
            .contains("current default differs"));
        server.join().unwrap();
        let state = std::fs::read_to_string(path.join("model-default.json")).unwrap();
        assert!(!state.contains("private-test-session"));
    }
}
