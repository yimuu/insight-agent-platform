//! Bounded observation of the installed physical processes; no retries of business operations.
use insight_platform_deployment_contracts::installation::{
    InstallationError as Error, InstallationInputV1, InstallationTopology,
};
use std::time::Duration;

fn selected_endpoints(
    input: &InstallationInputV1,
    selected: Option<&str>,
) -> Result<Vec<String>, Error> {
    input.validate()?;
    if selected.is_some_and(|name| {
        !input
            .network
            .processes
            .iter()
            .any(|entry| entry.process.name() == name)
    }) {
        return Err(Error::InvalidRoleClosure);
    }
    let mut endpoints = input
        .network
        .processes
        .iter()
        .filter(|process| selected.is_none_or(|name| process.process.name() == name))
        .map(|process| {
            let host = match input.network.topology {
                InstallationTopology::Native => process.observability_address.ip().to_string(),
                InstallationTopology::Compose => process.process.name().to_owned(),
                InstallationTopology::KubernetesLocal => format!(
                    "{}.{}.svc.cluster.local",
                    process.process.name(),
                    input.name
                ),
            };
            format!(
                "http://{host}:{}/readyz",
                process.observability_address.port()
            )
        })
        .collect::<Vec<_>>();
    if selected.is_some() {
        return Ok(endpoints);
    }
    // This also checks the serving Console's same-origin transport into Runtime Gateway.
    let console = match input.network.topology {
        InstallationTopology::Native => input.network.console_origin.as_str().to_owned(),
        InstallationTopology::Compose => "http://console:8080".to_owned(),
        InstallationTopology::KubernetesLocal => {
            format!("http://console.{}.svc.cluster.local:8080", input.name)
        }
    };
    endpoints.push(format!("{console}/readyz"));
    Ok(endpoints)
}

async fn probe(client: reqwest::Client, endpoint: String) -> bool {
    let Ok(mut response) = client.get(endpoint).send().await else {
        return false;
    };
    if response.status() != reqwest::StatusCode::OK {
        return false;
    }
    let mut bytes = Vec::new();
    loop {
        match response.chunk().await {
            Ok(Some(chunk)) if bytes.len() + chunk.len() <= 4096 => bytes.extend_from_slice(&chunk),
            Ok(None) => break,
            _ => return false,
        }
    }
    bytes == b"ready"
}

async fn wait_for(endpoints: Vec<String>, budget: Duration) -> Result<(), Error> {
    let client = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(1))
        .timeout(Duration::from_secs(3))
        .redirect(reqwest::redirect::Policy::none())
        .no_proxy()
        .build()
        .map_err(|_| Error::InvalidInput)?;
    tokio::time::timeout(budget, async {
        loop {
            let mut probes = tokio::task::JoinSet::new();
            for endpoint in &endpoints {
                probes.spawn(probe(client.clone(), endpoint.clone()));
            }
            let mut ready = true;
            while let Some(result) = probes.join_next().await {
                ready &= result.unwrap_or(false);
            }
            if ready {
                return;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    })
    .await
    .map_err(|_| Error::PrerequisiteUnavailable)
}

pub async fn wait(input: &InstallationInputV1) -> Result<(), Error> {
    wait_for(selected_endpoints(input, None)?, Duration::from_secs(120)).await
}

pub async fn wait_process(input: &InstallationInputV1, name: &str) -> Result<(), Error> {
    wait_for(
        selected_endpoints(input, Some(name))?,
        Duration::from_secs(120),
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    #[test]
    fn container_probe_targets_include_every_selected_role_and_console_transport() {
        let digest = format!("sha256:{}", "a".repeat(64)).parse().unwrap();
        let input =
            insight_platform_deployment_tooling::installation::compose_input("readiness", digest)
                .unwrap();
        let targets = selected_endpoints(&input, None).unwrap();
        assert_eq!(targets.len(), input.network.processes.len() + 1);
        assert!(targets.contains(&"http://console:8080/readyz".to_owned()));
        assert!(!targets.iter().any(|target| target.contains("127.0.0.1")));
        assert_eq!(
            selected_endpoints(&input, Some("egress-broker"))
                .unwrap()
                .len(),
            1
        );
        assert!(
            selected_endpoints(&input, Some("egress-broker")).unwrap()[0]
                .starts_with("http://egress-broker:")
        );
        for absent in ["console", "mcp-host", "foreign"] {
            assert!(matches!(
                selected_endpoints(&input, Some(absent)),
                Err(Error::InvalidRoleClosure)
            ));
        }
    }

    #[tokio::test]
    async fn actual_http_checks_reject_redirects_wrong_body_and_slow_incomplete_response() {
        for (status, body, expected) in [
            ("200 OK", "ready", true),
            ("200 OK", "not-ready", false),
            ("302 Found", "ready", false),
        ] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let endpoint = format!("http://{}/readyz", listener.local_addr().unwrap());
            let server = tokio::spawn(async move {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut request = [0; 4096];
                assert!(socket.read(&mut request).await.unwrap() > 0);
                socket.write_all(format!("HTTP/1.1 {status}\r\nContent-Length: {}\r\nLocation: /redirect\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes()).await.unwrap();
            });
            assert_eq!(
                wait_for(vec![endpoint], Duration::from_millis(100))
                    .await
                    .is_ok(),
                expected
            );
            server.await.unwrap();
        }
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}/readyz", listener.local_addr().unwrap());
        let result = wait_for(vec![endpoint], Duration::from_millis(50)).await;
        assert!(matches!(result, Err(Error::PrerequisiteUnavailable)));
    }
}
