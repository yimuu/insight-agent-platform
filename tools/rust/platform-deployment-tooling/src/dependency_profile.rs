//! One shared dependency composition for the native and container installation adapters.
use insight_platform_deployment_contracts::installation::*;
use serde_json::{json, Map, Value};
use std::path::Path;

pub enum DependencyLayout<'a> {
    Compose,
    Native {
        output: &'a Path,
        uid: u32,
        gid: u32,
    },
}
pub struct DependencyComposition {
    pub services: Map<String, Value>,
    pub volumes: Map<String, Value>,
    pub prepare_mounts: Vec<Value>,
}
fn volume(source: &str, target: &str, readonly: bool) -> Value {
    json!({"type":"volume","source":source,"target":target,"read_only":readonly,"volume":{"nocopy":true}})
}
fn port(origin: &ServiceOrigin) -> Result<u16, InstallationError> {
    origin
        .as_str()
        .rsplit(':')
        .next()
        .ok_or(InstallationError::InvalidEndpoint)?
        .parse()
        .map_err(|_| InstallationError::InvalidEndpoint)
}
pub fn composition(
    input: &InstallationInputV1,
    layout: DependencyLayout<'_>,
) -> Result<DependencyComposition, InstallationError> {
    input.validate()?;
    let (output, user) = match layout {
        DependencyLayout::Compose if input.network.topology == InstallationTopology::Compose => {
            (None, "10001:10001".to_owned())
        }
        DependencyLayout::Native { output, uid, gid }
            if input.network.topology == InstallationTopology::Native
                && output.is_absolute()
                && uid != 0
                && gid != 0
                && uid <= i32::MAX as u32
                && gid <= i32::MAX as u32
                && !output.components().any(|part| {
                    matches!(
                        part,
                        std::path::Component::CurDir | std::path::Component::ParentDir
                    )
                }) =>
        {
            (Some(output), format!("{uid}:{gid}"))
        }
        _ => return Err(InstallationError::UnsupportedTopology),
    };
    let images = crate::kubernetes::dependency_images()?;
    let mut volumes = Map::new();
    let mut prepare_mounts = Vec::new();
    let mut mount = |name: &str, target: &str, readonly: bool, prepared: Option<&str>| {
        if let Some(output) = output {
            let relative = prepared.unwrap_or(name);
            json!({"type":"bind","source":output.join(relative),"target":target,"read_only":readonly})
        } else {
            volumes.insert(name.into(),json!({"labels":{"insight.installation.input":input.digest().expect("validated input")}}));
            if let Some(relative) = prepared {
                prepare_mounts.push(volume(name, &format!("/output/{relative}"), false));
            }
            volume(name, target, readonly)
        }
    };
    let pg_config = mount(
        "dependency-postgres",
        "/run/insight-postgres",
        true,
        Some("dependencies/postgres"),
    );
    let pg_data = mount("postgres-data", "/var/lib/postgresql/data", false, None);
    let nats_config = mount(
        "dependency-nats",
        "/etc/nats",
        true,
        Some("dependencies/nats"),
    );
    let nats_data = mount("nats-data", "/data/jetstream", false, Some("nats-data"));
    let s3_config = mount(
        "dependency-s3",
        crate::s3_profile::S3_DIRECTORY,
        true,
        Some("dependencies/s3"),
    );
    let s3_data = mount(
        "s3-data",
        crate::s3_profile::S3_DATA_DIRECTORY,
        false,
        Some("s3-data"),
    );
    let bao_config = mount(
        "dependency-openbao",
        crate::openbao_profile::OPENBAO_DIRECTORY,
        true,
        Some("dependencies/openbao"),
    );
    let bao_data = mount(
        "openbao-data",
        crate::openbao_profile::OPENBAO_DATA_DIRECTORY,
        false,
        Some("openbao-data"),
    );
    let mut services = Map::new();
    let mut postgres = json!({"image":images["postgres"],"environment":{"POSTGRES_DB":"insight_platform","POSTGRES_USER":"insight_installation_admin","POSTGRES_PASSWORD_FILE":"/run/insight-postgres/admin-password"},"volumes":[pg_config,pg_data],"healthcheck":{"test":["CMD-SHELL","pg_isready -U insight_installation_admin -d insight_platform"],"interval":"2s","timeout":"2s","retries":30},"restart":"unless-stopped"});
    if output.is_some() {
        postgres["user"] = json!(user);
        postgres["ports"] = json!([format!("127.0.0.1:{}:5432", input.network.database.port)]);
    }
    services.insert("postgres".into(), postgres);
    let mut nats = json!({"image":images["nats"],"user":user,"command":["--config=/etc/nats/nats.conf"],"volumes":[nats_config,nats_data],"read_only":true,"cap_drop":["ALL"],"security_opt":["no-new-privileges:true"],"restart":"unless-stopped"});
    // The pinned server's SIGINT branch performs Shutdown/Wait and returns success. Its SIGTERM
    // branch completes the same shutdown but deliberately exits 1; do not reinterpret that code.
    nats["stop_signal"] = json!("SIGINT");
    if output.is_some() {
        nats["ports"] = json!([format!("127.0.0.1:{}:4222", input.network.nats_port)]);
    }
    services.insert("nats".into(), nats);
    let s3_host = input.network.providers.artifact().host()?;
    let mut s3 = json!({"image":images["s3"],"user":user,"entrypoint":["/usr/bin/weed"],"command":crate::s3_profile::server_arguments(),"volumes":[s3_config,s3_data],"ports":[format!("127.0.0.1:{}:8333",port(input.network.providers.artifact())?)],"networks":{"default":{"aliases":[s3_host]}},"read_only":true,"tmpfs":[format!("/tmp:uid={},gid={},mode=0700",user.split(':').next().unwrap(),user.split(':').nth(1).unwrap())],"cap_drop":["ALL"],"security_opt":["no-new-privileges:true"],"restart":"unless-stopped"});
    s3["stop_grace_period"] = json!(format!("{}s", crate::s3_profile::S3_STOP_GRACE_SECONDS));
    if output.is_some() {
        s3.as_object_mut().unwrap().remove("networks");
    }
    services.insert("s3".into(), s3);
    for (name, configuration) in [
        ("openbao-initialize", "initialize.json"),
        ("openbao", "serve.json"),
    ] {
        let mut service = json!({"image":images["openbao"],"user":user,"entrypoint":["/usr/bin/bao"],"command":["server",format!("-config={}/{configuration}",crate::openbao_profile::OPENBAO_DIRECTORY)],"volumes":[bao_config,bao_data],"read_only":true,"tmpfs":[format!("/tmp:uid={},gid={},mode=0700",user.split(':').next().unwrap(),user.split(':').nth(1).unwrap())],"cap_drop":["ALL"],"security_opt":["no-new-privileges:true"],"restart":if name=="openbao-initialize"{"no"}else{"unless-stopped"},"networks":{"default":{"aliases":[input.network.providers.openbao()?.host()?]}}});
        if name == "openbao-initialize" {
            service["profiles"] = json!(["initialize"]);
        }
        if output.is_some() {
            service["ports"] = json!([format!(
                "127.0.0.1:{}:8200",
                port(input.network.providers.openbao()?)?
            )]);
        }
        if output.is_some() {
            service.as_object_mut().unwrap().remove("networks");
        }
        services.insert(name.into(), service);
    }
    for service in services.values_mut() {
        service["labels"] = json!({"insight.installation.input":input.digest()?});
    }
    Ok(DependencyComposition {
        services,
        volumes,
        prepare_mounts,
    })
}

pub fn native_document(
    input: &InstallationInputV1,
    output: &Path,
    uid: u32,
    gid: u32,
) -> Result<Value, InstallationError> {
    let composed = composition(input, DependencyLayout::Native { output, uid, gid })?;
    Ok(
        json!({"name":input.name,"services":composed.services,"volumes":composed.volumes,"networks":{"default":{}}}),
    )
}
