use insight_platform_postgres::{capture_schema_inventory, verify_schema};
use sqlx::postgres::PgPoolOptions;

#[tokio::main]
async fn main() {
    let arguments = std::env::args().skip(1).collect::<Vec<_>>();
    let valid = matches!(
        arguments.as_slice(),
        [command] if matches!(command.as_str(), "verify" | "inventory")
    );
    if !valid {
        eprintln!("usage: platform-schema <verify|inventory>");
        std::process::exit(2);
    }
    let command = arguments.first().map(String::as_str);
    let database_url = match std::env::var("PLATFORM_DATABASE_URL") {
        Ok(value) if !value.is_empty() => value,
        _ => {
            eprintln!("PLATFORM_DATABASE_URL is required");
            std::process::exit(2);
        }
    };
    let pool = match PgPoolOptions::new()
        .max_connections(2)
        .connect(&database_url)
        .await
    {
        Ok(pool) => pool,
        Err(failure) => {
            eprintln!("cannot connect to candidate PostgreSQL authority: {failure}");
            std::process::exit(1);
        }
    };
    if command == Some("inventory") {
        match capture_schema_inventory(&pool).await {
            Ok(inventory) => print!("{}", String::from_utf8_lossy(&inventory)),
            Err(failure) => {
                eprintln!("{failure}");
                std::process::exit(1);
            }
        }
        return;
    }
    let result = verify_schema(&pool).await;
    match result {
        Ok(verification) => println!(
            "insight.platform/v1 schema contract {} verified (migrations {}, inventory {}, tables {})",
            verification.contract_version,
            verification.migration_set_digest,
            verification.schema_inventory_digest,
            verification.table_count
        ),
        Err(failure) => {
            eprintln!("{failure}");
            std::process::exit(1);
        }
    }
}
