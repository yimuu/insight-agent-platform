//! Local Kubernetes physical composition. Process configuration remains owned by the renderer.
use insight_platform_contracts::Sha256Digest;
use insight_platform_deployment_contracts::installation::*;
use serde_json::{json, Value};
use std::collections::BTreeMap;

/// One installation owns one namespace and its newly created, exclusive PVCs.
pub fn kubernetes_input(
    name: &str,
    package_digest: Sha256Digest,
) -> Result<InstallationInputV1, InstallationError> {
    if name.is_empty()
        || name.len() > 40
        || !name
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        || name.starts_with('-')
        || name.ends_with('-')
    {
        return Err(InstallationError::InvalidInput);
    }
    let mut input = crate::installation::compose_input(name, package_digest)?;
    input.network.topology = InstallationTopology::KubernetesLocal;
    input.network.database.host = format!("postgres.{name}.svc.cluster.local");
    input.network.nats_host = format!("nats.{name}.svc.cluster.local");
    input.network.providers = ProviderNetworkV1::S3OpenBao {
        artifact: ServiceOrigin::parse(&format!("https://s3.{name}.svc.cluster.local:8333"))?,
        openbao: ServiceOrigin::parse(&format!("https://openbao.{name}.svc.cluster.local:8200"))?,
    };
    for entry in &mut input.network.processes {
        if let Some(origin) = &entry.service_origin {
            let listen = entry
                .listen_address
                .ok_or(InstallationError::InvalidEndpoint)?;
            entry.service_origin = Some(ServiceOrigin::parse(&format!(
                "{}://{}.{name}.svc.cluster.local:{}",
                if origin.is_tls() { "https" } else { "http" },
                entry.process.name(),
                listen.port()
            ))?);
        }
    }
    input.credentials = crate::role_material::credentials(&input.network);
    input.validate()?;
    Ok(input)
}

fn image_digest(image: &str) -> Result<Sha256Digest, InstallationError> {
    let (name, digest) = image
        .rsplit_once('@')
        .ok_or(InstallationError::InvalidInput)?;
    if name.is_empty()
        || name.len() > 256
        || !name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'-' | b'_' | b'/' | b':'))
    {
        return Err(InstallationError::InvalidInput);
    }
    digest.parse().map_err(|_| InstallationError::InvalidInput)
}

pub(crate) fn dependency_images() -> Result<BTreeMap<String, String>, InstallationError> {
    let profile: Value = serde_json::from_str(include_str!(
        "../../../../deploy/release/development-profile-v1.json"
    ))
    .map_err(|_| InstallationError::InvalidInput)?;
    let mut dependencies = BTreeMap::new();
    for dependency in profile["dependencies"]
        .as_array()
        .ok_or(InstallationError::InvalidInput)?
    {
        let name = match dependency["name"].as_str() {
            Some("postgresql") => "postgres",
            Some("nats") => "nats",
            _ => continue,
        };
        let image = dependency["image"]
            .as_str()
            .ok_or(InstallationError::InvalidInput)?;
        image_digest(image)?;
        if dependencies
            .insert(name.to_owned(), image.to_owned())
            .is_some()
        {
            return Err(InstallationError::InvalidInput);
        }
    }
    if dependencies.len() != 2 {
        return Err(InstallationError::InvalidInput);
    }
    dependencies.insert(
        "openbao".into(),
        crate::openbao_profile::OPENBAO_IMAGE.into(),
    );
    dependencies.insert("s3".into(), crate::s3_profile::S3_IMAGE.into());
    Ok(dependencies)
}

/// A safe, bounded physical plan consumed by Helm. It contains no generated config or secret bytes.
pub fn helm_plan(
    input: &InstallationInputV1,
    runtime_image: &str,
    console_image: &str,
) -> Result<Value, InstallationError> {
    input.validate()?;
    let mut expected = kubernetes_input(&input.name, input.package_digest.clone())?;
    expected.network.console_origin = input.network.console_origin.clone();
    expected = crate::installation::with_remote_context_destinations(
        expected,
        input.remote_context_destinations.clone(),
    )?;
    if expected.digest()? != input.digest()? || image_digest(runtime_image)? != input.package_digest
    {
        return Err(InstallationError::ConfigurationDrift);
    }
    image_digest(console_image)?;
    let dependencies = dependency_images()?;
    let processes = input.network.processes.iter().map(|entry| json!({
        "name": entry.process.name(), "binary": entry.process.binary(), "uid": 10001,
        "port": entry.listen_address.map(|listen| listen.port()),
        "observability_port": entry.observability_address.port(),
        "paths": input.paths.iter().find(|paths| paths.process == entry.process).expect("validated process paths"),
    })).collect::<Vec<_>>();
    Ok(
        json!({"schema_version": 1, "namespace": input.name, "input": input,
        "input_digest": input.digest()?, "runtime_image": runtime_image, "console_image": console_image,
        "dependencies": dependencies, "dependency_commands": {"s3": crate::s3_profile::server_arguments()},
        "dependency_stop_grace_seconds": {"s3": crate::s3_profile::S3_STOP_GRACE_SECONDS}, "processes": processes}),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn exact_namespace_dns_and_images_are_bound_before_rendering() {
        let digest: Sha256Digest = format!("sha256:{}", "a".repeat(64)).parse().unwrap();
        let input = kubernetes_input("local-test", digest.clone()).unwrap();
        let image = format!("example/platform@{digest}");
        let plan = helm_plan(&input, &image, &image).unwrap();
        assert_eq!(
            plan["dependency_commands"],
            json!({"s3": crate::s3_profile::server_arguments()})
        );
        assert_eq!(
            plan["dependency_stop_grace_seconds"],
            json!({"s3": crate::s3_profile::S3_STOP_GRACE_SECONDS})
        );
        assert_eq!(
            plan["processes"].as_array().unwrap().len(),
            InstallationProcess::BASE.len()
        );
        assert_eq!(
            input.network.database.host,
            "postgres.local-test.svc.cluster.local"
        );
        assert_eq!(
            input.network.providers.artifact().as_str(),
            "https://s3.local-test.svc.cluster.local:8333"
        );
        assert_eq!(
            input
                .network
                .tls_server_name(InstallationProcess::ArtifactGateway)
                .unwrap(),
            "artifact-gateway.local-test.svc.cluster.local"
        );
        assert!(helm_plan(&input, digest.as_str(), &image).is_err());
        let mut foreign = input.clone();
        foreign.network.database.host = "postgres.other.svc.cluster.local".into();
        assert!(helm_plan(&foreign, &image, &image).is_err());
        for bad in ["a.b", "Other", "-bad", "bad-", ""] {
            assert!(kubernetes_input(bad, digest.clone()).is_err());
        }
        assert!(!serde_json::to_string(&plan)
            .unwrap()
            .contains("PRIVATE KEY"));
    }
}
