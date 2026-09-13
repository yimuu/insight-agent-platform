//! Declarative local composition. Only the one-shot process sees installation private state.
use insight_platform_contracts::Sha256Digest;
use insight_platform_deployment_contracts::installation::*;
use serde_json::{json, Value};
use std::path::Path;

fn invalid() -> InstallationError {
    InstallationError::InvalidInput
}
fn image_digest(image: &str) -> Result<Sha256Digest, InstallationError> {
    if image.starts_with("sha256:") {
        return image.parse().map_err(|_| invalid());
    }
    let (name, digest) = image.rsplit_once('@').ok_or_else(invalid)?;
    if name.is_empty()
        || name.len() > 256
        || !name.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'_' | b'/' | b':')
        })
    {
        return Err(invalid());
    }
    digest.parse().map_err(|_| invalid())
}
fn volume(source: &str, target: &str, readonly: bool) -> Value {
    json!({"type":"volume","source":source,"target":target,"read_only":readonly,"volume":{"nocopy":true}})
}

/// Returns Compose's JSON-compatible YAML representation, without evaluating arbitrary templates.
pub fn compose_document(
    input: &InstallationInputV1,
    input_file: &Path,
    runtime_image: &str,
    console_image: &str,
) -> Result<Value, InstallationError> {
    if image_digest(runtime_image)? != input.package_digest {
        return Err(invalid());
    }
    image_digest(console_image)?;
    document(input, input_file, runtime_image, console_image)
}

fn document(
    input: &InstallationInputV1,
    input_file: &Path,
    runtime_image: &str,
    console_image: &str,
) -> Result<Value, InstallationError> {
    input.validate()?;
    if input.network.topology != InstallationTopology::Compose {
        return Err(invalid());
    }
    if !input_file.is_absolute()
        || input_file.components().any(|part| {
            matches!(
                part,
                std::path::Component::ParentDir | std::path::Component::CurDir
            )
        })
    {
        return Err(InstallationError::InvalidPath);
    }
    let mut expected =
        crate::installation::compose_input(&input.name, input.package_digest.clone())?;
    expected.network.console_origin = input.network.console_origin.clone();
    expected = crate::installation::with_remote_context_destinations(
        expected,
        input.remote_context_destinations.clone(),
    )?;
    if expected.digest()? != input.digest()? {
        return Err(InstallationError::ConfigurationDrift);
    }
    let console = input
        .network
        .console_origin
        .as_str()
        .strip_prefix("http://127.0.0.1:")
        .ok_or(InstallationError::InvalidEndpoint)?;
    let console_port: u16 = console
        .parse()
        .map_err(|_| InstallationError::InvalidEndpoint)?;
    if console_port < 1024 {
        return Err(InstallationError::InvalidEndpoint);
    }
    let dependencies = crate::dependency_profile::composition(
        input,
        crate::dependency_profile::DependencyLayout::Compose,
    )?;
    let mut services = dependencies.services;
    for service in services.values_mut() {
        service["depends_on"] =
            json!({"installation-prepare":{"condition":"service_completed_successfully"}});
    }
    let mut volumes = dependencies.volumes;
    for name in [
        "installation-private",
        "role-console",
        "role-local-identity",
    ] {
        volumes.insert(
            name.into(),
            json!({"labels":{"insight.installation.input":input.digest()?}}),
        );
    }
    let mut output_mounts = vec![
        volume("installation-private", "/installation", false),
        volume("role-console", "/output/roles/console", false),
        volume("role-local-identity", "/output/roles/local-identity", false),
        json!({"type":"bind","source":input_file,"target":"/installation-input/input.json","read_only":true}),
    ];
    output_mounts.extend(dependencies.prepare_mounts);
    for process in &input.network.processes {
        let name = format!("role-{}", process.process.name());
        volumes.insert(
            name.clone(),
            json!({"labels":{"insight.installation.input":input.digest()?}}),
        );
        output_mounts.push(volume(
            &name,
            &format!("/output/roles/{}", process.process.name()),
            false,
        ));
    }
    let command = |phase: &str| {
        let mut args = vec![
            phase,
            "--input",
            "/installation-input/input.json",
            "--state",
            "/installation/private",
            "--output",
            "/output",
        ];
        if phase != "prepare" {
            args.extend(["--binaries", "/usr/local/bin"]);
        }
        args.into_iter().map(str::to_owned).collect::<Vec<_>>()
    };
    for phase in ["prepare", "provision", "verify"] {
        let mut service = json!({"image":runtime_image,"user":"0:0","entrypoint":["/usr/local/bin/platform-installation"],"command":command(phase),"restart":"no","read_only":true,"tmpfs":["/tmp:mode=0700"],"volumes":output_mounts,"environment":{"AWS_EC2_METADATA_DISABLED":"true","AWS_SHARED_CREDENTIALS_FILE":"/installation/private/s3-artifact-gateway-credentials","AWS_PROFILE":"default","AWS_CONFIG_FILE":"/dev/null","SSL_CERT_FILE":"/installation/private/ca.pem","SSL_CERT_DIR":"/etc/ssl/certs"},"security_opt":["no-new-privileges:true"],"cap_drop":["ALL"],"cap_add":["CHOWN","DAC_OVERRIDE","FOWNER"]});
        if phase != "prepare" {
            service["depends_on"] = json!({"postgres":{"condition":"service_healthy"},"s3":{"condition":"service_started"},"installation-bootstrap":{"condition":"service_completed_successfully"},"nats":{"condition":"service_started"}});
        }
        if phase == "verify" {
            service["profiles"] = json!(["operations"]);
        }
        services.insert(format!("installation-{phase}"), service);
    }
    services.insert("installation-ready".into(),json!({"image":runtime_image,"user":"10001:10001","entrypoint":["/usr/local/bin/platform-installation"],"command":["ready","--input","/installation-input/input.json"],"profiles":["operations"],"restart":"no","read_only":true,"volumes":[json!({"type":"bind","source":input_file,"target":"/installation-input/input.json","read_only":true})],"cap_drop":["ALL"],"security_opt":["no-new-privileges:true"]}));
    services.insert("installation-session".into(),json!({"image":runtime_image,"user":"0:0","entrypoint":["/usr/local/bin/platform-installation"],"command":["session","--input","/installation-input/input.json","--state","/installation/private"],"profiles":["operations"],"restart":"no","read_only":true,"volumes":[volume("installation-private","/installation",false),json!({"type":"bind","source":input_file,"target":"/installation-input/input.json","read_only":true})],"cap_drop":["ALL"],"security_opt":["no-new-privileges:true"]}));
    services.insert("installation-public-trust".into(),json!({"image":runtime_image,"user":"0:0","entrypoint":["/usr/local/bin/platform-installation"],"command":["public-trust","--input","/installation-input/input.json","--state","/installation/private"],"profiles":["operations"],"restart":"no","read_only":true,"network_mode":"none","volumes":[volume("installation-private","/installation",true),json!({"type":"bind","source":input_file,"target":"/installation-input/input.json","read_only":true})],"cap_drop":["ALL"],"security_opt":["no-new-privileges:true"]}));
    for phase in ["bootstrap", "provider-observe"] {
        services.insert(format!("installation-{phase}"),json!({
            "image":runtime_image,"user":"0:0","entrypoint":["/usr/local/bin/platform-installation"],
            "command":[phase,"--input","/installation-input/input.json","--state","/installation/private"],
            "restart":"no","read_only":true,
            "volumes":[volume("installation-private","/installation",false),json!({"type":"bind","source":input_file,"target":"/installation-input/input.json","read_only":true})],
            "cap_drop":["ALL"],"security_opt":["no-new-privileges:true"]
        }));
        if phase == "bootstrap" {
            services.get_mut("installation-bootstrap").unwrap()["depends_on"] =
                json!({"openbao":{"condition":"service_started"}});
        } else {
            services.get_mut("installation-provider-observe").unwrap()["profiles"] =
                json!(["operations"]);
        }
    }
    for process in &input.network.processes {
        services.insert(process.process.name().into(),json!({"image":runtime_image,"user":"10001:10001","entrypoint":["/bin/sh","-ec"],"command":[format!("set -a; . /run/insight/role/environment; set +a; exec /usr/local/bin/{}",process.process.binary())],"depends_on":{"installation-provision":{"condition":"service_completed_successfully"}},"volumes":[volume(&format!("role-{}",process.process.name()),"/run/insight/role",true)],"read_only":true,"tmpfs":["/tmp:uid=10001,gid=10001,mode=0700","/var/lib/insight:uid=10001,gid=10001,mode=0700"],"cap_drop":["ALL"],"security_opt":["no-new-privileges:true"],"restart":"unless-stopped","stop_grace_period":"35s"}));
    }
    services.insert("console".into(),json!({"image":console_image,"user":"1000:1000","command":["--config","/run/insight/console/config.json"],"ports":[format!("127.0.0.1:{console_port}:8080")],"volumes":[volume("role-console","/run/insight/console",true)],"depends_on":{"gateway-management":{"condition":"service_started"},"gateway-runtime":{"condition":"service_started"}},"read_only":true,"cap_drop":["ALL"],"security_opt":["no-new-privileges:true"],"restart":"unless-stopped"}));
    services.insert("local-identity".into(), json!({
        "image":console_image,"user":"1000:1000",
        "entrypoint":["/usr/local/bin/node","/console/server-dist/identity-main.js"],
        "command":["--config","/run/insight/identity/config.json"],
        "depends_on":{"installation-provision":{"condition":"service_completed_successfully"}},
        "volumes":[volume("role-local-identity","/run/insight/identity",true)],
        "healthcheck":{"test":["CMD","node","-e","fetch('http://127.0.0.1:8081/health/ready').then(r=>{if(!r.ok)process.exit(1)}).catch(()=>process.exit(1))"],"interval":"5s","timeout":"3s","retries":20},
        "read_only":true,"cap_drop":["ALL"],"security_opt":["no-new-privileges:true"],"restart":"unless-stopped"
    }));
    services.get_mut("console").ok_or_else(invalid)?["depends_on"]["local-identity"] =
        json!({"condition":"service_healthy"});
    for service in services.values_mut() {
        service["labels"] = json!({"insight.installation.input":input.digest()?});
    }
    Ok(json!({"name":input.name,"services":services,"volumes":volumes,"networks":{"default":{}}}))
}

/// Repository artifact: all role/dependency details still come from the existing owning builder.
pub fn repository_document() -> Result<Value, InstallationError> {
    let input = crate::installation::compose_input(
        "my-platform",
        format!("sha256:{}", "0".repeat(64))
            .parse()
            .map_err(|_| invalid())?,
    )?;
    let mut result = document(
        &input,
        Path::new("/installation-input/input.json"),
        "${INSIGHT_RUNTIME_IMAGE:-insight-runtime:local}",
        "${INSIGHT_CONSOLE_IMAGE:-insight-console:local}",
    )?;
    result["name"] = json!("${INSIGHT_NAME:-my-platform}");
    result["volumes"]["installation-input"] = json!({});
    for item in result["volumes"]
        .as_object_mut()
        .ok_or_else(invalid)?
        .values_mut()
    {
        item.as_object_mut().ok_or_else(invalid)?.remove("labels");
    }
    for (name, service) in result["services"].as_object_mut().ok_or_else(invalid)? {
        service["labels"] = json!({"insight.installation.managed":"true"});
        if let Some(command) = service["command"].as_array_mut() {
            for argument in command {
                if argument == "/installation-input/input.json" {
                    *argument = json!("/installation-input/prepared/input.json");
                }
            }
        }
        if let Some(mounts) = service["volumes"].as_array_mut() {
            for mount in mounts {
                if mount["type"] == "bind" && mount["target"] == "/installation-input/input.json" {
                    *mount = volume(
                        "installation-input",
                        "/installation-input",
                        name != "installation-prepare",
                    );
                }
            }
        }
    }
    let services = result["services"].as_object_mut().ok_or_else(invalid)?;
    let prepare = services
        .get_mut("installation-prepare")
        .ok_or_else(invalid)?;
    prepare["command"] = json!(["compose-prepare"]);
    prepare["environment"]["INSIGHT_NAME"] = json!("${INSIGHT_NAME:-my-platform}");
    prepare["environment"]["INSIGHT_HTTP_PORT"] = json!("${INSIGHT_HTTP_PORT:-8088}");
    prepare["build"] =
        json!({"context":".","dockerfile":"deploy/images/platform.Dockerfile","target":"runtime"});
    services.get_mut("installation-ready").ok_or_else(invalid)?["user"] = json!("0:0");
    let console = services.get_mut("console").ok_or_else(invalid)?;
    console["ports"] = json!(["127.0.0.1:${INSIGHT_HTTP_PORT:-8088}:8080"]);
    console["build"] =
        json!({"context":".","dockerfile":"deploy/images/console-source.Dockerfile"});
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn repository_entry_point_matches_the_owning_topology() {
        let path = crate::workspace_assets::workspace_path("compose.yaml");
        let actual: Value = yaml_serde::from_slice(&std::fs::read(path).unwrap()).unwrap();
        assert_eq!(actual, repository_document().unwrap());
    }
    #[test]
    fn complete_base_is_direct_and_every_serving_mount_is_private() {
        let digest = format!("sha256:{}", "a".repeat(64));
        let image = format!("example/platform@{digest}");
        let input =
            crate::installation::compose_input("compose-test", digest.parse().unwrap()).unwrap();
        let document =
            compose_document(&input, Path::new("/safe/input.json"), &image, &image).unwrap();
        for process in InstallationProcess::BASE {
            let service = &document["services"][process.name()];
            assert_eq!(service["volumes"].as_array().unwrap().len(), 1);
            assert_eq!(
                service["volumes"][0]["source"],
                format!("role-{}", process.name())
            );
            assert_eq!(service["volumes"][0]["read_only"], true);
            assert!(service["command"][0]
                .as_str()
                .unwrap()
                .ends_with(process.binary()));
        }
        let text = serde_json::to_string(&document).unwrap();
        assert!(!text.contains("docker.sock") && !text.contains("insight dev"));
        assert_eq!(
            document["services"]["postgres"]["environment"]["POSTGRES_USER"],
            "insight_installation_admin"
        );
        assert_eq!(document["services"]["nats"]["user"], "10001:10001");
        assert_eq!(document["services"]["nats"]["stop_signal"], "SIGINT");
        assert_eq!(document["services"]["s3"]["stop_grace_period"], "45s");
        let trust = &document["services"]["installation-public-trust"];
        assert_eq!(trust["network_mode"], "none");
        assert_eq!(trust["command"][0], "public-trust");
        assert_eq!(trust["volumes"].as_array().unwrap().len(), 2);
        assert!(trust["volumes"]
            .as_array()
            .unwrap()
            .iter()
            .all(|mount| mount["read_only"] == true));
        assert!(compose_document(
            &input,
            Path::new("/safe/input.json"),
            "example/platform:latest",
            &image
        )
        .is_err());
        let mut wrong = input.clone();
        wrong.network.database.host = "other-database".into();
        assert!(compose_document(&wrong, Path::new("/safe/input.json"), &image, &image).is_err());
    }
}
