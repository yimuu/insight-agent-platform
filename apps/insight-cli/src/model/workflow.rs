use super::*;
use crate::{
    apply, artifact,
    public_client::{PublicHttpClient, PublicJsonResponse},
};
use configuration::{ConfigurationFileV1, ResolvedSource};
use insight_platform_api::{
    model_configuration::*,
    resource::{DeploymentViewV1, ModelDefaultViewV1, ResourceViewV1},
};
use insight_platform_contracts::*;
use insight_platform_deployment_tooling::private_state::InstallationDirectory;
use insight_platform_registry::model_configuration::*;
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{path::Path, time::Duration};

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Attempt {
    schema_version: u16,
    nonce: String,
    input_digest: Sha256Digest,
    declared_at: chrono::DateTime<chrono::Utc>,
    completed: bool,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CredentialAttempt {
    schema_version: u16,
    operation_id: ModelCredentialOperationId,
}

pub(super) fn save(
    state: &InstallationDirectory,
    name: &str,
    value: &impl Serialize,
) -> Result<(), String> {
    let bytes = serde_json::to_vec(value).map_err(|_| "cannot encode model recovery state")?;
    state
        .replace(name, &bytes)
        .map_err(|_| "cannot durably persist model recovery state".to_owned())
}
pub(super) fn read<T: serde::de::DeserializeOwned>(
    state: &InstallationDirectory,
    name: &str,
) -> Result<Option<T>, String> {
    state
        .read(name, 262_144)
        .map_err(|_| "model recovery state is not a private regular file")?
        .map(|bytes| {
            serde_json::from_slice(&bytes).map_err(|_| "model recovery state is invalid".to_owned())
        })
        .transpose()
}
pub fn list(
    client: &PublicHttpClient,
    kind: RegistryResourceKind,
) -> Result<Vec<ModelConfigurationResourceSummaryV1>, String> {
    let mut items = Vec::new();
    let mut after: Option<ResourceId> = None;
    // A bounded interactive/configuration scan. Users can use the API's explicit `after` filter
    // for a larger registry; the command fails rather than treating a partial list as complete.
    for _ in 0..128 {
        let mut query = vec![("kind".to_owned(), kind.as_str().to_owned())];
        if let Some(id) = &after {
            query.push(("after".to_owned(), id.to_string()));
        }
        let page: ModelConfigurationResourcePageV1 = client
            .get_body_json_query("/v1/model-configuration/resources", &query, StatusCode::OK)
            .map_err(|error| error.to_string())?
            .body;
        if page.schema_version != 1
            || page.items.len() > 25
            || page.items.iter().any(|item| {
                item.resource_kind != kind
                    || item.resource_id.kind() != kind.id_kind()
                    || item.version == 0
                    || item.etag
                        != insight_platform_api::resource::resource_etag(
                            &item.resource_id,
                            item.version,
                        )
            })
        {
            return Err("model list response is invalid".to_owned());
        }
        let mut boundary = after.clone();
        for item in &page.items {
            if boundary
                .as_ref()
                .is_some_and(|last| last >= &item.resource_id)
            {
                return Err("model page order is invalid".to_owned());
            }
            boundary = Some(item.resource_id.clone());
        }
        if page.next_after.is_some() && (page.items.len() != 25 || page.next_after != boundary) {
            return Err("model page continuation is invalid".to_owned());
        }
        items.extend(page.items);
        after = page.next_after;
        if after.is_none() {
            return Ok(items);
        }
    }
    Err("model registry exceeds the bounded command scan; use paginated API queries".to_owned())
}
pub fn default(
    client: &PublicHttpClient,
    tenant: &ResourceId,
) -> Result<PublicJsonResponse<ModelDefaultViewV1>, String> {
    let result: PublicJsonResponse<ModelDefaultViewV1> = client
        .get_json("/v1/model-default", StatusCode::OK)
        .map_err(|e| e.to_string())?;
    if result.body.validate().is_err()
        || &result.body.tenant_id != tenant
        || result.etag != result.body.etag
    {
        return Err("session does not match the requested tenant or default authority".to_owned());
    }
    Ok(result)
}
pub fn configure(
    client: &PublicHttpClient,
    command: &Command,
    file: &ConfigurationFileV1,
    state_path: &Path,
) -> Result<Value, String> {
    default(client, &command.tenant)?;
    let (sources, environment) = file.resolve(
        |name| std::env::var(name).map_err(|_| "mapped input unavailable".to_owned()),
        |path| super::read_private_key_file(Path::new(path)),
    )?;
    let catalog: ModelConfigurationCatalogViewV1 = client
        .get_body_json("/v1/model-configuration", StatusCode::OK)
        .map_err(|e| e.to_string())?
        .body;
    if catalog.schema_version != 1
        || catalog.secret_provider_id.kind() != ResourceKind::SecretProvider
        || catalog.destinations.is_empty()
        || catalog.destinations.len() > 64
    {
        return Err("installed model choices are invalid".to_owned());
    }
    // Resolve every physical destination before importing any credential.
    let destinations = sources
        .iter()
        .map(|source| destination(source, &catalog))
        .collect::<Result<Vec<_>, _>>()?;
    let identity:Sha256Digest=canonical_digest(&json!({"schema_version":1,"endpoint":command.endpoint,"tenant":command.tenant,"sources":sources,"default_model":file.default_model,"installation_digest":catalog.installation_digest}))
        .map_err(|_|"cannot canonicalize model configuration")?.parse().map_err(|_|"invalid configuration digest")?;
    let root = InstallationDirectory::open(state_path, true).map_err(|_| {
        "model state directory must be private, unlocked and have an existing parent"
    })?;
    let current: Option<Attempt> = read(&root, "model-attempt.json")?;
    if let Some(attempt) = &current {
        validate_attempt(attempt, chrono::Utc::now())?;
    }
    let attempt=match current {
        Some(attempt) if attempt.schema_version==1 && attempt.input_digest==identity && !command.new_attempt=>attempt,
        Some(attempt) if !attempt.completed=>return Err("an unfinished model configuration must be resumed with the same inputs before another attempt".to_owned()),
        _=>{let attempt=Attempt {schema_version:1,nonce:uuid::Uuid::new_v4().to_string(),input_digest:identity,declared_at:chrono::Utc::now(),completed:false};save(&root,"model-attempt.json",&attempt)?;attempt}
    };
    validate_attempt(&attempt, chrono::Utc::now())?;
    let work_path = root
        .path(&format!("model-{}", attempt.nonce))
        .map_err(|_| "invalid model attempt path")?;
    let state = InstallationDirectory::open(&work_path, true)
        .map_err(|_| "cannot open private model attempt directory")?;
    let mut reports = Vec::new();
    let mut selected = None;
    for (index, (source, destination)) in sources.iter().zip(destinations).enumerate() {
        let credential_name = format!("source-{index}-credential.json");
        let credential: CredentialAttempt = match read(&state, &credential_name)? {
            Some(value) => value,
            None => {
                let value = CredentialAttempt {
                    schema_version: 1,
                    operation_id: uuid::Uuid::new_v4()
                        .to_string()
                        .parse()
                        .map_err(|_| "invalid credential operation")?,
                };
                save(&state, &credential_name, &value)?;
                value
            }
        };
        if credential.schema_version != 1 {
            return Err("credential attempt version is invalid".to_owned());
        }
        let imported = client
            .import_model_credential(
                &credential.operation_id,
                &catalog.secret_provider_id,
                environment.key(&source.credential)?,
            )
            .map_err(|e| e.to_string())?;
        let input = ModelConfigurationInputV1::Source(ModelSourceConfigurationV1 {
            schema_version: 1,
            alias: source.alias.clone(),
            display_name: source.display_name.clone(),
            destination_digest: destination,
            credential: imported.binding,
        });
        let (report, source_deployment) = register(
            client,
            &command.tenant,
            &catalog,
            &state,
            &work_path,
            &format!("source-{index}"),
            input,
        )?;
        reports.push(serde_json::to_value(report).map_err(|_| "invalid source report")?);
        for (model_index, model) in source.models.iter().enumerate() {
            let input = ModelConfigurationInputV1::Model(BasicModelConfigurationV1 {
                schema_version: 1,
                alias: model.alias.clone(),
                display_name: model.display_name.clone(),
                source: source_deployment.clone(),
                model: model.model.clone(),
                maximum_input_tokens: model.maximum_input_tokens,
                maximum_output_tokens: model.maximum_output_tokens,
                declared_at: attempt.declared_at,
            });
            let (report, deployment) = register(
                client,
                &command.tenant,
                &catalog,
                &state,
                &work_path,
                &format!("model-{index}-{model_index}"),
                input,
            )?;
            let quota = super::quota::configure(
                client,
                &command.tenant,
                &state,
                &format!("model-{index}-{model_index}-quota.json"),
                &deployment,
                model.quota,
                attempt.completed,
            )?;
            reports.push(json!({"model_alias":model.alias,"quota":quota}));
            if file.default_model.as_ref() == Some(&model.alias) {
                selected = Some(deployment);
            }
            reports.push(serde_json::to_value(report).map_err(|_| "invalid model report")?);
        }
    }
    if let Some(model) = selected {
        #[derive(Serialize, Deserialize)]
        #[serde(deny_unknown_fields)]
        struct DefaultIntent {
            etag: String,
            model: ExactDeploymentRef,
        }
        let persisted: Option<DefaultIntent> = read(&state, "default-intent.json")?;
        let current = default(client, &command.tenant)?;
        if attempt.completed {
            if current.body.default_model.as_ref() != Some(&model) {
                return Err("the selected default changed after this completed attempt; use --new-attempt to explicitly configure again".to_owned());
            }
        } else if current.body.default_model.as_ref() != Some(&model) || persisted.is_some() {
            let intent = match persisted {
                Some(intent) if intent.model == model => intent,
                Some(_) => {
                    return Err("default recovery intent differs from the compiled model".to_owned())
                }
                None => {
                    let intent = DefaultIntent {
                        etag: current.etag,
                        model,
                    };
                    save(&state, "default-intent.json", &intent)?;
                    intent
                }
            };
            let result: PublicJsonResponse<ModelDefaultViewV1> = client
                .put_json(
                    "/v1/model-default",
                    &json!({"schema_version":1,"default_model":intent.model}),
                    StatusCode::OK,
                    &format!("model-{}-default", attempt.nonce),
                    &intent.etag,
                )
                .map_err(|e| e.to_string())?;
            if result.body.validate().is_err()
                || result.body.tenant_id != command.tenant
                || result.body.etag != result.etag
                || result.body.default_model.as_ref() != Some(&intent.model)
            {
                return Err("default response does not match this configuration".to_owned());
            }
            if default(client, &command.tenant)?
                .body
                .default_model
                .as_ref()
                != Some(&intent.model)
            {
                return Err("current default differs from this configuration".to_owned());
            }
        }
    }
    save(
        &root,
        "model-attempt.json",
        &Attempt {
            completed: true,
            ..attempt
        },
    )?;
    Ok(
        json!({"schema_version":1,"tenant_id":command.tenant,"configured":reports,"default_model":file.default_model}),
    )
}
fn destination(
    source: &ResolvedSource,
    catalog: &ModelConfigurationCatalogViewV1,
) -> Result<Sha256Digest, String> {
    let candidates = catalog
        .destinations
        .iter()
        .filter(|choice| {
            choice.protocol == source.protocol
                && normalize_model_base_url(&choice.base_url).ok().as_ref()
                    == Some(&source.endpoint)
        })
        .collect::<Vec<_>>();
    if candidates.len() != 1 {
        return Err(format!("source {} requires exactly one matching installed destination; update installation configuration before importing its key",source.alias.as_str()));
    }
    Ok(candidates[0].destination_digest.clone())
}
fn register(
    client: &PublicHttpClient,
    tenant: &ResourceId,
    catalog: &ModelConfigurationCatalogViewV1,
    state: &InstallationDirectory,
    path: &Path,
    step: &str,
    input: ModelConfigurationInputV1,
) -> Result<(apply::ApplyReportV1, ExactDeploymentRef), String> {
    let declaration = client
        .declare_model_configuration(&DeclareModelConfigurationRequestV1 {
            schema_version: 1,
            installation_digest: catalog.installation_digest.clone(),
            input: input.clone(),
        })
        .map_err(|e| e.to_string())?;
    let bytes =
        canonical_json(&declaration.content).map_err(|_| "invalid model declaration JSON")?;
    if declaration.schema_version != 1
        || bytes.len() != declaration.size_bytes as usize
        || canonical_digest(&declaration.content).map_err(|_| "invalid declaration digest")?
            != declaration.content_digest.as_str()
    {
        return Err("model declaration does not match its bytes".to_owned());
    }
    let source_name = format!("{step}-declaration.json");
    state
        .write_immutable(&source_name, &bytes)
        .map_err(|_| "model declaration changed during the same attempt")?;
    let uploader =
        artifact::HttpsArtifactObjectUploader::with_additional_roots(client.additional_roots())
            .map_err(|e| e.to_string())?;
    let uploaded = artifact::upload_artifact(
        client,
        &uploader,
        tenant,
        &state
            .path(&source_name)
            .map_err(|_| "invalid declaration path")?,
        artifact::ArtifactUploadOptions {
            purpose: ArtifactPurpose::AuthoringDocument,
            classification: DataClassification::Internal,
            declared_media_type: Some("application/json".to_owned()),
            display_name: None,
            operation_timeout: Duration::from_secs(120),
        },
        path,
    )
    .map_err(|e| e.to_string())?;
    let artifact = ArtifactRef::new(
        uploaded
            .artifact_id
            .parse()
            .map_err(|_| "invalid declaration Artifact ID")?,
        uploaded
            .content_digest
            .parse()
            .map_err(|_| "invalid declaration Artifact digest")?,
        uploaded.byte_length,
        uploaded.media_type,
        DataClassification::Internal,
        None,
    )
    .map_err(|_| "invalid declaration Artifact")?;
    let compiled = client
        .compile_model_configuration(&CompileModelConfigurationRequestV1 {
            schema_version: 1,
            installation_digest: catalog.installation_digest.clone(),
            input: input.clone(),
            artifact,
        })
        .map_err(|e| e.to_string())?;
    compiled
        .draft
        .validate()
        .map_err(|_| "compiled model draft is invalid")?;
    let (kind, noun, alias) = match &input {
        ModelConfigurationInputV1::Source(source) => (
            RegistryResourceKind::ModelProvider,
            "model-providers",
            &source.alias,
        ),
        ModelConfigurationInputV1::Model(model) => {
            (RegistryResourceKind::ModelProfile, "models", &model.alias)
        }
    };
    if compiled.schema_version != 1
        || compiled.draft.alias.as_ref() != Some(alias)
        || compiled.draft.document.kind() != kind
        || compiled.environment != catalog.environment
        || compiled.declaration != declaration
    {
        return Err("compiled model does not match the requested declaration".to_owned());
    }
    let manifest_name = format!("{step}-manifest.json");
    let manifest: Value = if let Some(manifest) = read(state, &manifest_name)? {
        manifest
    } else {
        let matches = list(client, kind)?
            .into_iter()
            .filter(|item| item.alias.as_ref() == Some(alias))
            .collect::<Vec<_>>();
        if matches.len() > 1 {
            return Err("model alias has multiple authorities".to_owned());
        }
        let existing = if let Some(item) = matches.first() {
            let current: PublicJsonResponse<ResourceViewV1> = client
                .get_json(&format!("/v1/{noun}/{}", item.resource_id), StatusCode::OK)
                .map_err(|e| e.to_string())?;
            if current.body.validate().is_err()
                || current.body.resource_id != item.resource_id
                || current.body.draft.alias.as_ref() != Some(alias)
                || current.body.resource_kind != kind
                || current.etag != current.body.etag
            {
                return Err("model update authority is invalid".to_owned());
            }
            Some((
                current.body.resource_id,
                current.etag,
                current
                    .body
                    .draft_generation
                    .checked_add(1)
                    .ok_or("model draft generation overflow")?,
            ))
        } else {
            None
        };
        let value = json!({"schema_version":1,"kind":"insight.platform.apply/v1","resource_noun":noun,"existing_resource":existing.as_ref().map(|(id,etag,_)|json!({"resource_id":id,"etag":etag})),
            "create":{"alias":compiled.draft.alias,"display_name":compiled.draft.display_name,"document":compiled.draft.document},
            "publish":{"kind":"single","revision_no":existing.as_ref().map(|(_,_,generation)|*generation).unwrap_or(1),"content_digest":declaration.content_digest,"artifact_id":compiled.draft.document.authoring_package().artifact.artifact_id()},
            "deployment":{"environment":compiled.environment,"closure":compiled.deployment}});
        save(state, &manifest_name, &value)?;
        value
    };
    let expected_create = json!({"alias":compiled.draft.alias,"display_name":compiled.draft.display_name,"document":compiled.draft.document});
    if manifest["schema_version"] != 1
        || manifest["kind"] != "insight.platform.apply/v1"
        || manifest["resource_noun"] != noun
        || manifest["create"] != expected_create
        || manifest["deployment"]
            != json!({"environment":compiled.environment,"closure":compiled.deployment})
        || manifest["publish"]["kind"] != "single"
        || manifest["publish"]["content_digest"] != json!(declaration.content_digest)
        || manifest["publish"]["artifact_id"]
            != json!(compiled
                .draft
                .document
                .authoring_package()
                .artifact
                .artifact_id())
    {
        return Err("model recovery manifest differs from current compiler output".to_owned());
    }
    let report = apply::apply_manifest(
        client,
        tenant,
        &serde_json::to_vec(&manifest).map_err(|_| "invalid model manifest")?,
        Duration::from_secs(120),
        path,
    )
    .map_err(|e| e.to_string())?;
    let id = report
        .deployment_id
        .as_ref()
        .ok_or("model publication did not return a deployment")?;
    let deployment: PublicJsonResponse<DeploymentViewV1> = client
        .get_json(
            &format!("/v1/{noun}/{}/deployments/{id}", report.resource_id),
            StatusCode::OK,
        )
        .map_err(|e| e.to_string())?;
    if deployment.body.validate().is_err()
        || deployment.body.deployment_id.to_string() != *id
        || deployment.body.resource_id.to_string() != report.resource_id
        || deployment.body.resource_kind != kind
    {
        return Err("model deployment authority is invalid".to_owned());
    }
    let exact = ExactDeploymentRef::new(
        deployment.body.deployment_id,
        deployment.body.closure_digest,
    )
    .map_err(|_| "invalid exact model deployment")?;
    Ok((report, exact))
}

fn validate_attempt(attempt: &Attempt, now: chrono::DateTime<chrono::Utc>) -> Result<(), String> {
    let nonce =
        uuid::Uuid::parse_str(&attempt.nonce).map_err(|_| "invalid model attempt identity")?;
    if attempt.schema_version != 1
        || nonce.get_version_num() != 4
        || nonce.to_string() != attempt.nonce
        || attempt.declared_at > now + chrono::Duration::minutes(5)
    {
        return Err("invalid model attempt identity".to_owned());
    }
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn completed_recovery_state_is_validated_before_replacement() {
        let now = chrono::Utc::now();
        let mut attempt = Attempt {
            schema_version: 1,
            nonce: uuid::Uuid::new_v4().to_string(),
            input_digest: format!("sha256:{}", "a".repeat(64)).parse().unwrap(),
            declared_at: now,
            completed: true,
        };
        validate_attempt(&attempt, now).unwrap();
        attempt.schema_version = 2;
        assert!(validate_attempt(&attempt, now).is_err());
        attempt.schema_version = 1;
        attempt.nonce = uuid::Uuid::now_v7().to_string();
        assert!(validate_attempt(&attempt, now).is_err());
        attempt.nonce = "../../outside".to_owned();
        assert!(validate_attempt(&attempt, now).is_err());
    }
}
