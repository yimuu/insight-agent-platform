use insight_platform_contracts::{ResourceId, ResourceKind, Sha256Digest};
use insight_platform_postgres::{
    repository::{BootstrapInstallationOperator, BootstrapOutcome, PgRepository},
    verify_schema,
};
use sqlx::postgres::PgPoolOptions;

#[tokio::main]
async fn main() {
    if std::env::args().len() != 1 {
        eprintln!("usage: platform-bootstrap-operator");
        std::process::exit(2);
    }
    let database_url = required("PLATFORM_DATABASE_URL");
    let principal_id = required_id("PLATFORM_BOOTSTRAP_PRINCIPAL_ID", ResourceKind::Principal);
    let request_id = required_id("PLATFORM_BOOTSTRAP_REQUEST_ID", ResourceKind::ServerRequest);
    let authentication_authority_digest =
        required_digest("PLATFORM_BOOTSTRAP_AUTHENTICATION_AUTHORITY_DIGEST");
    let subject_digest = required_digest("PLATFORM_BOOTSTRAP_SUBJECT_DIGEST");
    let evidence_digest = required_digest("PLATFORM_BOOTSTRAP_EVIDENCE_DIGEST");
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&database_url)
        .await
        .unwrap_or_else(|failure| {
            eprintln!("cannot connect to PostgreSQL authority: {failure}");
            std::process::exit(1);
        });
    verify_schema(&pool).await.unwrap_or_else(|failure| {
        eprintln!("PostgreSQL schema is not verified: {failure}");
        std::process::exit(1);
    });
    let repository = PgRepository::new(pool);
    let outcome = repository
        .bootstrap_installation_operator(BootstrapInstallationOperator {
            principal_id: principal_id.clone(),
            request_id,
            authentication_authority_digest,
            subject_digest,
            evidence_digest,
        })
        .await
        .unwrap_or_else(|failure| {
            eprintln!("installation operator bootstrap failed: {failure}");
            std::process::exit(1);
        });
    println!(
        "installation operator {principal_id} {}",
        match outcome {
            BootstrapOutcome::Created => "created",
            BootstrapOutcome::Replayed => "replayed",
        }
    );
}

fn required(name: &'static str) -> String {
    match std::env::var(name) {
        Ok(value) if !value.is_empty() => value,
        _ => {
            eprintln!("{name} is required");
            std::process::exit(2);
        }
    }
}

fn required_id(name: &'static str, kind: ResourceKind) -> ResourceId {
    let value = required(name);
    ResourceId::parse_expected(&value, kind).unwrap_or_else(|failure| {
        eprintln!("{name} is invalid: {failure}");
        std::process::exit(2);
    })
}

fn required_digest(name: &'static str) -> Sha256Digest {
    required(name).parse().unwrap_or_else(|failure| {
        eprintln!("{name} is invalid: {failure}");
        std::process::exit(2);
    })
}
