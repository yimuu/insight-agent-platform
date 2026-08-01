use std::{collections::BTreeMap, env, path::PathBuf};

use insight_agent_platform::catalog::compile_agent_dir;
use reqwest::{header, Client, Method, StatusCode};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Debug)]
struct ImportOptions {
    server: String,
    token_env: String,
    agent_dir: PathBuf,
    activate: bool,
    dry_run: bool,
}

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("agentctl: {error}");
        std::process::exit(1);
    }
}

async fn run() -> Result<()> {
    let options = parse_options()?;
    let published = compile_agent_dir(&options.agent_dir)?;
    let agent_id = published.metadata().id.clone();
    let source = published.author_source().to_owned();
    let prompt_files = published
        .prompt_files()
        .iter()
        .map(|(path, content)| json!({"path":path,"content":content}))
        .collect::<Vec<_>>();
    let author_hash = canonical_hash(&json!({
        "source":source,
        "prompt_files":prompt_files,
    }))?;
    let report = json!({
        "version":1,
        "operation":"agent_import",
        "agent_id":agent_id,
        "author_hash":author_hash,
        "prompt_file_count":published.prompt_files().len(),
        "prompt_bytes":published.prompt_files().values().map(String::len).sum::<usize>(),
        "activate_requested":options.activate,
        "dry_run":options.dry_run,
    });
    if options.dry_run {
        println!("{}", serde_json::to_string_pretty(&report)?);
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
    let request_prefix = format!(
        "agent-import-{}-{}",
        agent_id,
        &author_hash.trim_start_matches("sha256:")[..16]
    );
    let create = request_json(
        &client,
        Method::POST,
        &format!("{}/v1/admin/agents", options.server.trim_end_matches('/')),
        &token,
        &format!("{request_prefix}-create"),
        None,
        Some("*"),
        json!({
            "agent_id":agent_id,
            "authoring_mode":"yaml_package",
            "labels":{"import_source":"file_package"},
            "draft":{"source":{
                "type":"yaml_package",
                "agent_yaml":published.author_source(),
                "prompt_files":prompt_files,
            }}
        }),
        &[StatusCode::CREATED],
    )
    .await?;
    let draft_version = required_u64(&create.body, "draft_version")?;
    let entity_version = required_u64(&create.body, "entity_version")?;
    let mut published_revision = false;
    let result = async {
        let validation = request_json(
            &client,
            Method::POST,
            &format!(
                "{}/v1/admin/agents/{agent_id}/validations",
                options.server.trim_end_matches('/')
            ),
            &token,
            &format!("{request_prefix}-validate"),
            Some(&format!("\"draft-{draft_version}\"")),
            None,
            json!({}),
            &[StatusCode::ACCEPTED],
        )
        .await?;
        let validation_id = required_str(&validation.body, "validation_id")?;
        let revision = request_json(
            &client,
            Method::POST,
            &format!(
                "{}/v1/admin/agents/{agent_id}/revisions",
                options.server.trim_end_matches('/')
            ),
            &token,
            &format!("{request_prefix}-publish"),
            Some(&format!("\"draft-{draft_version}\"")),
            None,
            json!({"validation_id":validation_id}),
            &[StatusCode::CREATED],
        )
        .await?;
        published_revision = true;
        let definition_revision_id = required_str(&revision.body, "definition_revision_id")?;
        let resolution = request_json(
            &client,
            Method::POST,
            &format!(
                "{}/v1/admin/agents/{agent_id}/deployment-resolutions",
                options.server.trim_end_matches('/')
            ),
            &token,
            &format!("{request_prefix}-resolve"),
            None,
            None,
            json!({"definition_revision_id":definition_revision_id}),
            &[StatusCode::ACCEPTED],
        )
        .await?;
        let resolution_id = required_str(&resolution.body, "resolution_id")?;
        let deployment = request_json(
            &client,
            Method::POST,
            &format!(
                "{}/v1/admin/agents/{agent_id}/deployments",
                options.server.trim_end_matches('/')
            ),
            &token,
            &format!("{request_prefix}-deploy"),
            None,
            None,
            json!({"resolution_id":resolution_id}),
            &[StatusCode::CREATED],
        )
        .await?;
        let deployment_revision_id = required_str(&deployment.body, "deployment_revision_id")?;
        if options.activate {
            request_json(
                &client,
                Method::PUT,
                &format!(
                    "{}/v1/admin/agents/{agent_id}/active-deployment",
                    options.server.trim_end_matches('/')
                ),
                &token,
                &format!("{request_prefix}-activate"),
                Some(&format!("\"agent-{entity_version}\"")),
                None,
                json!({"deployment_revision_id":deployment_revision_id}),
                &[StatusCode::OK],
            )
            .await?;
        }
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(json!({
            "version":1,
            "operation":"agent_import",
            "agent_id":agent_id,
            "author_hash":author_hash,
            "definition_revision_id":definition_revision_id,
            "deployment_revision_id":deployment_revision_id,
            "activated":options.activate,
            "dry_run":false,
        }))
    }
    .await;

    match result {
        Ok(report) => println!("{}", serde_json::to_string_pretty(&report)?),
        Err(error) => {
            if !published_revision {
                let _ = request_empty(
                    &client,
                    Method::DELETE,
                    &format!(
                        "{}/v1/admin/agents/{agent_id}",
                        options.server.trim_end_matches('/')
                    ),
                    &token,
                    &format!("{request_prefix}-rollback"),
                    &format!("\"agent-{entity_version}\""),
                )
                .await;
            }
            return Err(error);
        }
    }
    Ok(())
}

fn parse_options() -> Result<ImportOptions> {
    let mut args = env::args().skip(1);
    if args.next().as_deref() != Some("import") {
        return Err(usage().into());
    }
    let mut values = BTreeMap::new();
    let mut activate = false;
    let mut dry_run = false;
    while let Some(argument) = args.next() {
        match argument.as_str() {
            "--activate" => activate = true,
            "--dry-run" => dry_run = true,
            "--server" | "--token-env" | "--agent-dir" => {
                let value = args.next().ok_or_else(usage)?;
                values.insert(argument, value);
            }
            _ => return Err(usage().into()),
        }
    }
    Ok(ImportOptions {
        server: values.remove("--server").ok_or_else(usage)?,
        token_env: values.remove("--token-env").ok_or_else(usage)?,
        agent_dir: PathBuf::from(values.remove("--agent-dir").ok_or_else(usage)?),
        activate,
        dry_run,
    })
}

fn usage() -> String {
    "usage: agentctl import --server URL --token-env ENV --agent-dir DIR [--dry-run] [--activate]"
        .to_owned()
}

struct JsonResponse {
    body: Value,
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
) -> Result<JsonResponse> {
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
        return Err(format!("management API returned {status} ({code})").into());
    }
    Ok(JsonResponse {
        body: envelope.get("data").cloned().unwrap_or(Value::Null),
    })
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

fn canonical_hash(value: &Value) -> Result<String> {
    let bytes = serde_jcs::to_vec(value)?;
    let mut output = String::from("sha256:");
    for byte in Sha256::digest(bytes) {
        use std::fmt::Write as _;
        write!(&mut output, "{byte:02x}")?;
    }
    Ok(output)
}
