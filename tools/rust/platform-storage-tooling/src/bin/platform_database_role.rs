//! Development-only role provisioning. Runtime processes never acquire DDL credentials.
use std::{
    io::Read as _,
    path::{Path, PathBuf},
};

struct Role {
    name: &'static str,
    marker: &'static str,
    variable: &'static str,
    password_file: &'static str,
}
const OUTBOX: &[Role] = &[Role {
    name: "insight_outbox_dev",
    marker: "Insight development Outbox v1",
    variable: ":'outbox_worker_role'",
    password_file: "outbox-password",
}];
const HISTORY: &[Role] = &[Role {
    name: "insight_history_dev",
    marker: "Insight development History v1",
    variable: ":'history_maintenance_role'",
    password_file: "history-password",
}];
const SECURITY: &[Role] = &[Role {
    name: "insight_security_authority_dev",
    marker: "Insight development Security Authority v1",
    variable: ":'security_authority_role'",
    password_file: "security-authority-password",
}];
const ARTIFACT: &[Role] = &[
    Role {
        name: "insight_artifact_gateway_dev",
        marker: "Insight development Artifact Gateway v1",
        variable: ":'artifact_gateway_role'",
        password_file: "artifact-gateway-password",
    },
    Role {
        name: "insight_artifact_data_reader_dev",
        marker: "Insight development Artifact Data Reader v1",
        variable: ":'artifact_data_reader_role'",
        password_file: "artifact-data-reader-password",
    },
    Role {
        name: "insight_artifact_data_worker_dev",
        marker: "Insight development Artifact Data Worker v1",
        variable: ":'artifact_data_worker_role'",
        password_file: "artifact-data-worker-password",
    },
    Role {
        name: "insight_artifact_maintenance_dev",
        marker: "Insight development Artifact Maintenance v1",
        variable: ":'artifact_maintenance_role'",
        password_file: "artifact-maintenance-password",
    },
];

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("Database role provisioning failed: {error}");
        std::process::exit(1);
    }
}
async fn run() -> Result<(), &'static str> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    let (flag, purpose, credential_path, kind_port) = match args.as_slice() {
        [flag, purpose, credential_path] => (flag, purpose, credential_path, None),
        [profile, name, port, flag, purpose, credential_path] if profile == "--profile" && name == "kind-local" =>
            (flag, purpose, credential_path, Some(kind_local_port(port)?)),
        _ => return Err("usage: platform-database-role [--profile kind-local <port>] --purpose <outbox|history|security-authority|artifact> <private-password-file|artifact-private-directory>"),
    };
    if flag != "--purpose" {
        return Err("explicit closed role purpose is required");
    }
    let (roles, grants) = match purpose.as_str() {
        "outbox" => (
            OUTBOX,
            insight_platform_postgres::outbox_repository::outbox_role_grants_sql(),
        ),
        "history" => (
            HISTORY,
            insight_platform_postgres::history_repository::history_role_grants_sql(),
        ),
        "security-authority" => (
            SECURITY,
            insight_platform_postgres::security_authority_role_grants_sql(),
        ),
        "artifact" => (
            ARTIFACT,
            insight_platform_postgres::artifact_repository::artifact_role_grants_sql(),
        ),
        _ => return Err("unknown database role purpose"),
    };
    let root = Path::new(credential_path);
    if purpose == "artifact" {
        check_private_path(root, true)?;
    }
    let credentials = roles
        .iter()
        .map(|role| {
            let password_path = if purpose == "artifact" {
                root.join(role.password_file)
            } else {
                PathBuf::from(root)
            };
            Ok((role, read_password(&password_path)?))
        })
        .collect::<Result<Vec<_>, &'static str>>()?;
    let url = std::env::var("PLATFORM_DATABASE_ROLE_ADMIN_URL")
        .map_err(|_| "development database URL missing")?;
    let expected = match kind_port {
        Some(port) => format!("postgresql://insight:insight-local-only@127.0.0.1:{port}/insight"),
        None => "postgres://insight:insight@127.0.0.1:5432/insight_platform".to_owned(),
    };
    if url != expected {
        return Err("role provisioning requires the exact local development authority");
    }
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(&url)
        .await
        .map_err(|_| "database unavailable")?;
    let mut tx = pool.begin().await.map_err(|_| "database unavailable")?;
    // Create and check the entire Artifact cohort before its shared owning SQL is applied.
    for (role, password) in credentials {
        sqlx::query("SELECT set_config('insight_platform.development_role', $1, true), set_config('insight_platform.development_role_marker', $2, true), set_config('insight_platform.development_password', $3, true)")
            .bind(role.name).bind(role.marker).bind(password).execute(&mut *tx).await.map_err(|_| "role provisioning unavailable")?;
        sqlx::raw_sql(r#"DO $role$
    DECLARE target_role text := current_setting('insight_platform.development_role');
            marker text := current_setting('insight_platform.development_role_marker');
    BEGIN
        IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname=target_role) THEN
            IF (SELECT shobj_description(oid,'pg_authid') FROM pg_roles WHERE rolname=target_role) IS DISTINCT FROM marker THEN
                RAISE EXCEPTION 'existing role is not owned by the development profile';
            END IF;
            IF EXISTS (SELECT 1 FROM pg_auth_members WHERE member=(SELECT oid FROM pg_roles WHERE rolname=target_role)) THEN
                RAISE EXCEPTION 'development role has unexpected inherited authority';
            END IF;
            EXECUTE format('ALTER ROLE %I LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS NOINHERIT PASSWORD %L', target_role, current_setting('insight_platform.development_password'));
        ELSE
            EXECUTE format('CREATE ROLE %I LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS NOINHERIT PASSWORD %L', target_role, current_setting('insight_platform.development_password'));
            EXECUTE format('COMMENT ON ROLE %I IS %L', target_role, marker);
        END IF;
    END $role$;"#).execute(&mut *tx).await.map_err(|_| "development role ownership check failed")?;
    }
    // Every composed fragment comes from a closed role and the PostgreSQL grants owner.
    let mut grants = grants
        .replace("\\set ON_ERROR_STOP on", "")
        .replace("BEGIN;", "")
        .replace("COMMIT;", "");
    for role in roles {
        grants = grants.replace(role.variable, &format!("'{}'", role.name));
    }
    sqlx::raw_sql(sqlx::AssertSqlSafe(grants))
        .execute(&mut *tx)
        .await
        .map_err(|_| "role grants rejected")?;
    tx.commit()
        .await
        .map_err(|_| "role provisioning unavailable")?;
    println!("Development database roles provisioned for {purpose}");
    Ok(())
}
fn check_private_path(path: &Path, directory: bool) -> Result<(), &'static str> {
    let metadata = std::fs::symlink_metadata(path).map_err(|_| "credential unreadable")?;
    if (directory && !metadata.is_dir()) || (!directory && !metadata.is_file()) {
        return Err("credential must be a private regular file or physical directory");
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        let expected = if directory { 0o700 } else { 0o600 };
        if metadata.permissions().mode() & 0o777 != expected {
            return Err("credential file permissions are not private");
        }
    }
    Ok(())
}
fn read_password(path: &Path) -> Result<String, &'static str> {
    check_private_path(path, false)?;
    let mut bytes = Vec::new();
    std::fs::File::open(path)
        .map_err(|_| "credential unreadable")?
        .take(33)
        .read_to_end(&mut bytes)
        .map_err(|_| "credential unreadable")?;
    if bytes.len() != 32 || !bytes.iter().all(u8::is_ascii_hexdigit) {
        return Err("development credential invalid");
    }
    String::from_utf8(bytes).map_err(|_| "development credential invalid")
}
fn kind_local_port(value: &str) -> Result<u16, &'static str> {
    let port: u16 = value.parse().map_err(|_| "Kind loopback port invalid")?;
    if port < 1024 || port.to_string() != value {
        return Err("Kind loopback port invalid");
    }
    Ok(port)
}
#[cfg(test)]
mod tests {
    #[test]
    fn kind_profile_rejects_noncanonical_or_privileged_ports() {
        for value in [
            "0",
            "1023",
            "65536",
            "015432",
            "+15432",
            "15432/insight",
            "localhost:15432",
            "15432?sslmode=disable",
        ] {
            assert!(super::kind_local_port(value).is_err(), "{value}");
        }
        assert_eq!(super::kind_local_port("15432"), Ok(15432));
        assert_eq!(super::kind_local_port("65535"), Ok(65535));
    }
}
