use std::{collections::BTreeMap, env, fs, path::PathBuf, time::Duration};

use insight_agent_platform::config::{
    ModelInputModality, PlatformConfig, ProviderExtensionConfig, ProviderExtensionSource,
};
use reqwest::{header, Client, Method, StatusCode};
use serde::Deserialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Debug)]
struct ImportOptions {
    server: String,
    token_env: String,
    platform_config: PathBuf,
    catalog: PathBuf,
    provider: Option<String>,
    activate: bool,
    dry_run: bool,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Catalog {
    catalog_version: u32,
    providers: BTreeMap<String, CatalogProvider>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct CatalogProvider {
    adapter: String,
    endpoint: String,
    credential: CatalogCredential,
    models: BTreeMap<String, CatalogModel>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct CatalogCredential {
    #[serde(rename = "type")]
    credential_type: String,
    env: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct CatalogModel {
    input: Vec<String>,
}

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("providerctl: {error}");
        std::process::exit(1);
    }
}

async fn run() -> Result<()> {
    let options = parse_options()?;
    // Provider extension parsing retains credential environment-variable names
    // as references; it does not resolve their values. PlatformConfig may
    // still consume documented platform-level override/auth variables.
    let platform = PlatformConfig::load(&options.platform_config)?;
    let catalog: Catalog = yaml_serde::from_str(&fs::read_to_string(&options.catalog)?)?;
    if catalog.catalog_version != 1 {
        return Err("provider catalog version is unsupported".into());
    }
    let selected = platform
        .providers
        .extensions
        .iter()
        .filter(|(provider_id, _)| {
            options
                .provider
                .as_ref()
                .is_none_or(|selected| selected == *provider_id)
        })
        .collect::<Vec<_>>();
    if selected.is_empty() {
        return Err("no matching Provider extension was found".into());
    }
    let drafts = selected
        .into_iter()
        .map(|(provider_id, extension)| {
            build_import_draft(provider_id, extension, &catalog)
                .map(|draft| (provider_id.clone(), draft))
        })
        .collect::<Result<Vec<_>>>()?;
    let previews = drafts
        .iter()
        .map(|(provider_id, draft)| {
            Ok(json!({
                "provider_id":provider_id,
                "provider_input_hash":canonical_hash(draft)?,
                "model_count":draft["models"].as_array().map(Vec::len).unwrap_or(0),
            }))
        })
        .collect::<Result<Vec<_>>>()?;
    if options.dry_run {
        println!(
            "{}",
            serde_json::to_string_pretty(&json!({
                "version":1,
                "operation":"provider_extension_import",
                "dry_run":true,
                "activate_requested":options.activate,
                "providers":previews,
            }))?
        );
        return Ok(());
    }

    let token = env::var(&options.token_env).map_err(|_| {
        format!(
            "token environment variable '{}' is unavailable",
            options.token_env
        )
    })?;
    if token.trim().is_empty() {
        return Err("management token is empty".into());
    }
    let client = Client::builder().build()?;
    let mut reports = Vec::new();
    for (provider_id, draft) in drafts {
        reports.push(import_provider(&client, &options, &token, &provider_id, draft).await?);
    }
    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "version":1,
            "operation":"provider_extension_import",
            "dry_run":false,
            "providers":reports,
        }))?
    );
    Ok(())
}

fn build_import_draft(
    provider_id: &str,
    extension: &ProviderExtensionConfig,
    catalog: &Catalog,
) -> Result<Value> {
    let (mut endpoint, mut credential_env, mut models, provenance) = match &extension.source {
        ProviderExtensionSource::Extends { provider } => {
            let parent = catalog
                .providers
                .get(provider)
                .ok_or_else(|| format!("Provider '{provider_id}' extends a missing template"))?;
            if parent.adapter != "open_ai_chat" || parent.credential.credential_type != "bearer" {
                return Err(
                    format!("Provider '{provider_id}' uses an unsupported template").into(),
                );
            }
            (
                parent.endpoint.clone(),
                Some(parent.credential.env.clone()),
                parent
                    .models
                    .iter()
                    .map(|(model_id, model)| {
                        (
                            model_id.clone(),
                            (model.input.clone(), "template_verified".to_owned()),
                        )
                    })
                    .collect::<BTreeMap<_, _>>(),
                "operator_asserted",
            )
        }
        ProviderExtensionSource::OpenAiCompatible => (
            extension
                .endpoint
                .clone()
                .ok_or_else(|| format!("Provider '{provider_id}' has no endpoint"))?,
            extension.credential_env.clone(),
            BTreeMap::new(),
            "operator_asserted",
        ),
    };
    if let Some(value) = &extension.endpoint {
        endpoint = value.clone();
    }
    if let Some(value) = &extension.credential_env {
        credential_env = Some(value.clone());
    }
    for (model_id, profile) in &extension.models {
        if models
            .insert(
                model_id.clone(),
                (
                    profile
                        .input
                        .iter()
                        .map(|input| match input {
                            ModelInputModality::Text => "text".to_owned(),
                            ModelInputModality::Image => "image".to_owned(),
                        })
                        .collect(),
                    provenance.to_owned(),
                ),
            )
            .is_some()
        {
            return Err(format!("Provider '{provider_id}' overrides an inherited model").into());
        }
    }
    let credential = credential_env.map_or_else(
        || json!({"type":"none"}),
        |name| json!({"type":"bearer","reference":format!("secret://environment/{name}")}),
    );
    let model_documents = models
        .into_iter()
        .map(|(model_id, (input, provenance))| {
            json!({
                "id":model_id,
                "input":input,
                "capabilities":["complete","streaming"],
                "provenance":{"type":provenance},
            })
        })
        .collect::<Vec<_>>();
    Ok(json!({
        "adapter":{"type":"open_ai_compatible"},
        "endpoint":endpoint,
        "credential":credential,
        "transport":{
            "tls":"required",
            "redirects":"deny",
            "connect_timeout_ms":duration_millis(extension.connect_timeout)?,
            "request_timeout_ms":duration_millis(extension.request_timeout)?,
        },
        "models":model_documents,
        "operator_note":"Imported from a static Provider extension; secret values were not read.",
    }))
}

async fn import_provider(
    client: &Client,
    options: &ImportOptions,
    token: &str,
    provider_id: &str,
    draft: Value,
) -> Result<Value> {
    let input_hash = canonical_hash(&draft)?;
    let prefix = format!(
        "provider-import-{provider_id}-{}",
        &input_hash.trim_start_matches("sha256:")[..16]
    );
    let base = options.server.trim_end_matches('/');
    let created = request_json(
        client,
        Method::POST,
        &format!("{base}/v1/admin/providers"),
        token,
        &format!("{prefix}-create"),
        None,
        Some("*"),
        json!({
            "provider_id":provider_id,
            "display_name":provider_id,
            "adapter_type":"open_ai_compatible",
            "draft":draft,
        }),
        &[StatusCode::CREATED],
    )
    .await?;
    let draft_version = required_u64(&created, "draft_version")?;
    let provider_version = required_u64(&created, "provider_version")?;
    let mut published_revision = false;
    let result = async {
        let validation = request_json(
            client,
            Method::POST,
            &format!("{base}/v1/admin/providers/{provider_id}/validations"),
            token,
            &format!("{prefix}-validate"),
            None,
            None,
            json!({"draft_version":draft_version}),
            &[StatusCode::ACCEPTED],
        )
        .await?;
        let validation_id = required_str(&validation, "validation_id")?;
        let revision = request_json(
            client,
            Method::POST,
            &format!("{base}/v1/admin/providers/{provider_id}/revisions"),
            token,
            &format!("{prefix}-publish"),
            None,
            None,
            json!({"draft_version":draft_version,"validation_id":validation_id}),
            &[StatusCode::CREATED],
        )
        .await?;
        published_revision = true;
        let revision_id = required_str(&revision, "revision_id")?;
        if options.activate {
            request_json(
                client,
                Method::PUT,
                &format!("{base}/v1/admin/providers/{provider_id}/active-revision"),
                token,
                &format!("{prefix}-activate"),
                Some(&format!("\"provider-{provider_version}\"")),
                None,
                json!({"revision_id":revision_id}),
                &[StatusCode::OK],
            )
            .await?;
        }
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(json!({
            "provider_id":provider_id,
            "provider_input_hash":input_hash,
            "revision_id":revision_id,
            "activated":options.activate,
        }))
    }
    .await;
    match result {
        Ok(report) => Ok(report),
        Err(error) => {
            if !published_revision {
                let _ = request_empty(
                    client,
                    Method::DELETE,
                    &format!("{base}/v1/admin/providers/{provider_id}"),
                    token,
                    &format!("{prefix}-rollback"),
                    &format!("\"provider-{provider_version}\""),
                )
                .await;
            }
            Err(error)
        }
    }
}

fn parse_options() -> Result<ImportOptions> {
    let mut args = env::args().skip(1);
    if args.next().as_deref() != Some("import-extensions") {
        return Err(usage().into());
    }
    let mut values = BTreeMap::new();
    let mut activate = false;
    let mut dry_run = false;
    while let Some(argument) = args.next() {
        match argument.as_str() {
            "--activate" => activate = true,
            "--dry-run" => dry_run = true,
            "--server" | "--token-env" | "--platform-config" | "--catalog" | "--provider" => {
                let value = args.next().ok_or_else(usage)?;
                values.insert(argument, value);
            }
            _ => return Err(usage().into()),
        }
    }
    Ok(ImportOptions {
        server: values.remove("--server").ok_or_else(usage)?,
        token_env: values.remove("--token-env").ok_or_else(usage)?,
        platform_config: PathBuf::from(values.remove("--platform-config").ok_or_else(usage)?),
        catalog: PathBuf::from(values.remove("--catalog").ok_or_else(usage)?),
        provider: values.remove("--provider"),
        activate,
        dry_run,
    })
}

fn usage() -> String {
    "usage: providerctl import-extensions --server URL --token-env ENV --platform-config FILE --catalog FILE [--provider ID] [--dry-run] [--activate]".to_owned()
}

#[allow(clippy::too_many_arguments)]
async fn request_json(
    client: &Client,
    method: Method,
    url: &str,
    token: &str,
    request_id: &str,
    if_match: Option<&str>,
    if_none_match: Option<&str>,
    body: Value,
    expected: &[StatusCode],
) -> Result<Value> {
    let operation = format!("{} {}", method, management_path(url));
    let mut request = client
        .request(method, url)
        .bearer_auth(token)
        .header("x-request-id", request_id)
        .json(&body);
    if let Some(value) = if_match {
        request = request.header(header::IF_MATCH, value);
    }
    if let Some(value) = if_none_match {
        request = request.header(header::IF_NONE_MATCH, value);
    }
    let response = request.send().await?;
    let status = response.status();
    let envelope: Value = response.json().await?;
    if !expected.contains(&status) {
        let code = envelope
            .get("code")
            .and_then(Value::as_str)
            .unwrap_or("MANAGEMENT_IMPORT_FAILED");
        return Err(format!("management API {operation} returned {status} ({code})").into());
    }
    Ok(envelope.get("data").cloned().unwrap_or(Value::Null))
}

fn management_path(url: &str) -> &str {
    url.find("/v1/admin/").map_or(url, |index| &url[index..])
}

async fn request_empty(
    client: &Client,
    method: Method,
    url: &str,
    token: &str,
    request_id: &str,
    if_match: &str,
) -> Result<()> {
    let response = client
        .request(method, url)
        .bearer_auth(token)
        .header("x-request-id", request_id)
        .header(header::IF_MATCH, if_match)
        .send()
        .await?;
    if response.status().is_success() {
        Ok(())
    } else {
        Err(format!("rollback returned {}", response.status()).into())
    }
}

fn required_str<'a>(value: &'a Value, field: &str) -> Result<&'a str> {
    value
        .get(field)
        .and_then(Value::as_str)
        .ok_or_else(|| format!("management response omitted '{field}'").into())
}

fn required_u64(value: &Value, field: &str) -> Result<u64> {
    value
        .get(field)
        .and_then(Value::as_u64)
        .ok_or_else(|| format!("management response omitted '{field}'").into())
}

fn duration_millis(value: Duration) -> Result<u64> {
    u64::try_from(value.as_millis()).map_err(|_| "duration is too large".into())
}

fn canonical_hash(value: &Value) -> Result<String> {
    let bytes = serde_jcs::to_vec(value)?;
    let mut output = String::from("sha256:");
    for byte in Sha256::digest(bytes) {
        use std::fmt::Write as _;
        write!(&mut output, "{byte:02x}")?;
    }
    Ok(output)
}
