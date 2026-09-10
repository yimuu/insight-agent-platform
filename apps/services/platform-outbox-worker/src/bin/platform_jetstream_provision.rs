//! Explicit provisioning utility. Never invoked by the Outbox worker and uses separate credentials.
use insight_platform_contracts::{parse_strict_json, JsonLimits};
use insight_platform_deployment_contracts::outbox::OutboxJetStreamContractV1;
use insight_platform_outbox_worker::{stream_configuration, JetStreamCommittedEventPublisher};
use std::{io::Read as _, path::PathBuf, time::Duration};
#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("Outbox provisioning failed: {error}");
        std::process::exit(1);
    }
}
async fn run() -> Result<(), &'static str> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    let [command, config_path] = args.as_slice() else {
        return Err("usage: platform-jetstream-provision <create|verify> <stream-contract.json>");
    };
    if !matches!(command.as_str(), "create" | "verify") {
        return Err("only create or read-only verify is supported; stream upgrades require deployment review");
    }
    let mut bytes = Vec::new();
    std::fs::File::open(config_path)
        .map_err(|_| "contract unreadable")?
        .take(16_385)
        .read_to_end(&mut bytes)
        .map_err(|_| "contract unreadable")?;
    let value = parse_strict_json(
        &bytes,
        JsonLimits {
            max_bytes: 16_384,
            max_depth: 4,
            max_items_per_array: 8,
            max_properties_per_object: 16,
            max_string_bytes: 2048,
        },
    )
    .map_err(|_| "contract invalid")?;
    let contract: OutboxJetStreamContractV1 =
        serde_json::from_value(value).map_err(|_| "contract invalid")?;
    let config = stream_configuration(&contract).map_err(|_| "contract invalid")?;
    let required = |name| {
        std::env::var(name)
            .ok()
            .filter(|s| !s.is_empty())
            .ok_or("provisioning credential/configuration missing")
    };
    let server = required("PLATFORM_OUTBOX_PROVISION_NATS_URL")?;
    if !server.starts_with("tls://") || server.contains('@') {
        return Err("provisioning requires a credential-free TLS endpoint");
    }
    let connect = async_nats::ConnectOptions::new()
        .require_tls(true)
        .add_root_certificates(PathBuf::from(required(
            "PLATFORM_OUTBOX_PROVISION_CA_PATH",
        )?))
        .add_client_certificate(
            PathBuf::from(required("PLATFORM_OUTBOX_PROVISION_CERT_PATH")?),
            PathBuf::from(required("PLATFORM_OUTBOX_PROVISION_KEY_PATH")?),
        )
        .connect(server);
    let client = tokio::time::timeout(Duration::from_secs(10), connect)
        .await
        .map_err(|_| "NATS unavailable")?
        .map_err(|_| "NATS unavailable")?;
    if command == "create" {
        tokio::time::timeout(
            Duration::from_secs(10),
            async_nats::jetstream::new(client.clone()).create_stream(config),
        )
        .await
        .map_err(|_| "stream creation outcome unknown")?
        .map_err(|_| "stream creation rejected")?;
    }
    JetStreamCommittedEventPublisher::from_client(client, &contract, Duration::from_secs(10))
        .await
        .map_err(|_| "installed stream contract verification failed")?;
    println!("Committed-event stream matches the deployment contract");
    Ok(())
}
