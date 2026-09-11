//! Explicit installation-only administration. Verify performs no role or grant mutation.
use super::{Role, ARTIFACT, HISTORY, OUTBOX, SECURITY};
use insight_platform_contracts::{canonical_digest, parse_strict_json, Sha256Digest};
use insight_platform_deployment_contracts::installation::{
    InstallationDatabaseEvidenceV1, InstallationDatabasePurpose as Purpose,
    InstallationDatabaseRoleEvidenceV1, InstallationInputV1, InstallationTopology,
    INSTALLATION_LIMITS, INSTALLATION_MAX_BYTES,
};
use sqlx::{postgres::PgPoolOptions, Row as _};
use std::{
    io::{Read as _, Write as _},
    path::Path,
};
const RUNTIME: &[Role] = &[Role {
    name: "insight_runtime_dev",
    marker: "Insight development runtime DML v1",
    variable: ":'development_runtime_role'",
    password_file: "runtime-password",
}];

pub(super) async fn run(args: &[String]) -> Result<(), &'static str> {
    let [input_path, mode, flag, purpose, credentials_path, evidence_path] = args else {
        return Err("installation role arguments invalid");
    };
    if flag != "--purpose" || !matches!(mode.as_str(), "create" | "verify") {
        return Err("installation role operation invalid");
    }
    let mode = if mode == "create"
        && Path::new(evidence_path)
            .try_exists()
            .map_err(|_| "installation evidence unavailable")?
    {
        "verify"
    } else {
        mode.as_str()
    };
    let input = InstallationInputV1::decode(&read_private(
        Path::new(input_path),
        INSTALLATION_MAX_BYTES,
    )?)
    .map_err(|_| "installation input invalid")?;
    let input_digest = input.digest().map_err(|_| "installation input invalid")?;
    let expected_input: Sha256Digest = std::env::var("PLATFORM_INSTALLATION_INPUT_DIGEST")
        .map_err(|_| "installation input digest missing")?
        .parse()
        .map_err(|_| "installation input digest invalid")?;
    let identity_digest: Sha256Digest = std::env::var("PLATFORM_INSTALLATION_IDENTITY_DIGEST")
        .map_err(|_| "installation identity digest missing")?
        .parse()
        .map_err(|_| "installation identity digest invalid")?;
    if input_digest != expected_input {
        return Err("installation input drift");
    }
    let purpose: Purpose = serde_json::from_value(serde_json::Value::String(purpose.clone()))
        .map_err(|_| "installation role purpose invalid")?;
    let (roles, grants) = match purpose {
        Purpose::Runtime => (
            RUNTIME,
            insight_platform_postgres::development_runtime_role_grants_sql(),
        ),
        Purpose::Artifact => (
            ARTIFACT,
            insight_platform_postgres::artifact_repository::artifact_role_grants_sql(),
        ),
        Purpose::History => (
            HISTORY,
            insight_platform_postgres::history_repository::history_role_grants_sql(),
        ),
        Purpose::Outbox => (
            OUTBOX,
            insight_platform_postgres::outbox_repository::outbox_role_grants_sql(),
        ),
        Purpose::SecurityAuthority => (
            SECURITY,
            insight_platform_postgres::security_authority_role_grants_sql(),
        ),
    };
    let credentials = Path::new(credentials_path);
    check_directory(credentials)?;
    let passwords = roles
        .iter()
        .map(|role| {
            Ok((
                role,
                super::read_password(&credentials.join(role.password_file))?,
            ))
        })
        .collect::<Result<Vec<_>, &'static str>>()?;
    let raw_url = std::env::var("PLATFORM_DATABASE_ROLE_ADMIN_URL")
        .map_err(|_| "installation database authority missing")?;
    let admin_url = checked_admin_url(&input, &raw_url)?;
    let expected = if mode == "verify" {
        Some(decode_evidence(&read_private(
            Path::new(evidence_path),
            INSTALLATION_MAX_BYTES,
        )?)?)
    } else {
        None
    };
    if let Some(expected) = &expected {
        if expected.input_digest != input_digest
            || expected.identity_digest != identity_digest
            || expected.purpose != purpose
        {
            return Err("installation grant evidence mismatch");
        }
    }
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(admin_url.as_str())
        .await
        .map_err(|_| "installation database unavailable")?;
    insight_platform_postgres::verify_schema(&pool)
        .await
        .map_err(|_| "installation schema mismatch")?;
    let mut tx = pool
        .begin()
        .await
        .map_err(|_| "installation database unavailable")?;
    if mode == "verify" {
        sqlx::query("SET TRANSACTION READ ONLY")
            .execute(&mut *tx)
            .await
            .map_err(|_| "installation verification unavailable")?;
    }
    if mode == "create" {
        // These are installation-dedicated databases; default PUBLIC temporary/DDL authority is not serving authority.
        sqlx::raw_sql("REVOKE CREATE ON SCHEMA public FROM PUBLIC; REVOKE CREATE ON SCHEMA insight_platform FROM PUBLIC;").execute(&mut *tx).await.map_err(|_|"installation schema privileges rejected")?;
        sqlx::query(
            "SELECT set_config('insight_platform.installation_database',current_database(),true)",
        )
        .execute(&mut *tx)
        .await
        .map_err(|_| "installation privileges unavailable")?;
        sqlx::raw_sql("DO $revoke$ BEGIN EXECUTE pg_catalog.format('REVOKE CREATE, TEMPORARY ON DATABASE %I FROM PUBLIC',current_database()); END $revoke$;").execute(&mut *tx).await.map_err(|_|"installation database privileges rejected")?;
        for (role, password) in &passwords {
            let marker = format!("{} {}", role.marker, identity_digest);
            sqlx::query("SELECT set_config('insight_platform.installation_role',$1,true),set_config('insight_platform.installation_marker',$2,true),set_config('insight_platform.installation_password',$3,true)").bind(role.name).bind(marker).bind(password).execute(&mut *tx).await.map_err(|_|"installation role unavailable")?;
            sqlx::raw_sql(r#"DO $role$
DECLARE target text:=current_setting('insight_platform.installation_role'); marker text:=current_setting('insight_platform.installation_marker'); target_oid oid;
BEGIN
 SELECT oid INTO target_oid FROM pg_catalog.pg_roles WHERE rolname=target;
 IF target_oid IS NOT NULL THEN
   IF pg_catalog.shobj_description(target_oid,'pg_authid') IS DISTINCT FROM marker
      OR EXISTS(SELECT 1 FROM pg_catalog.pg_auth_members WHERE member=target_oid)
      OR EXISTS(SELECT 1 FROM pg_catalog.pg_class WHERE relowner=target_oid)
      OR EXISTS(SELECT 1 FROM pg_catalog.pg_namespace WHERE nspowner=target_oid)
      OR EXISTS(SELECT 1 FROM pg_catalog.pg_database WHERE datdba=target_oid) THEN
     RAISE EXCEPTION 'installation role ownership differs';
   END IF;
   EXECUTE pg_catalog.format('ALTER ROLE %I LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS NOINHERIT PASSWORD %L',target,current_setting('insight_platform.installation_password'));
 ELSE
   EXECUTE pg_catalog.format('CREATE ROLE %I LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS NOINHERIT PASSWORD %L',target,current_setting('insight_platform.installation_password'));
   EXECUTE pg_catalog.format('COMMENT ON ROLE %I IS %L',target,marker);
 END IF;
END $role$;"#).execute(&mut *tx).await.map_err(|_|"installation role ownership rejected")?;
        }
        let mut sql = grants
            .replace("\\set ON_ERROR_STOP on", "")
            .replace("BEGIN;", "")
            .replace("COMMIT;", "");
        for role in roles {
            sql = sql.replace(role.variable, &format!("'{}'", role.name));
        }
        sqlx::raw_sql(sqlx::AssertSqlSafe(sql))
            .execute(&mut *tx)
            .await
            .map_err(|_| "installation role grants rejected")?;
    }
    let mut evidence = InstallationDatabaseEvidenceV1 {
        schema_version: 1,
        input_digest,
        identity_digest,
        purpose,
        roles: Vec::new(),
    };
    for (role, _) in &passwords {
        let expected_marker = format!("{} {}", role.marker, evidence.identity_digest);
        validate_role(&mut tx, role.name, &expected_marker).await?;
        let privileges = effective_privileges(&mut tx, role.name).await?;
        let effective_privileges_digest = canonical_digest(&privileges)
            .map_err(|_| "installation privilege evidence invalid")?
            .parse()
            .map_err(|_| "installation privilege evidence invalid")?;
        evidence.roles.push(InstallationDatabaseRoleEvidenceV1 {
            role_name: role.name.into(),
            effective_privileges_digest,
        });
    }
    evidence
        .validate()
        .map_err(|_| "installation role closure invalid")?;
    if let Some(expected) = expected {
        if serde_json::to_value(expected).map_err(|_| "installation evidence invalid")?
            != serde_json::to_value(&evidence).map_err(|_| "installation evidence invalid")?
        {
            return Err("installation role privileges drifted");
        }
    }
    tx.commit()
        .await
        .map_err(|_| "installation role transaction unavailable")?;
    // Authenticate each actual serving credential. Never disclose or store the password in evidence.
    for (role, password) in passwords {
        let mut url = admin_url.clone();
        url.set_username(role.name)
            .map_err(|_| "installation role invalid")?;
        url.set_password(Some(&password))
            .map_err(|_| "installation credential invalid")?;
        let connection = PgPoolOptions::new()
            .max_connections(1)
            .connect(url.as_str())
            .await
            .map_err(|_| "installation serving credential rejected")?;
        insight_platform_postgres::verify_schema(&connection)
            .await
            .map_err(|_| "installation serving schema verification rejected")?;
        connection.close().await;
    }
    if mode == "create" {
        write_new_evidence(Path::new(evidence_path), &evidence)?;
    }
    println!(
        "{}",
        serde_json::to_string(&evidence).map_err(|_| "installation evidence invalid")?
    );
    Ok(())
}
fn checked_admin_url(input: &InstallationInputV1, value: &str) -> Result<url::Url, &'static str> {
    let expected_host = match input.network.topology {
        InstallationTopology::Compose => "postgres".to_owned(),
        InstallationTopology::KubernetesLocal => {
            format!("postgres.{}.svc.cluster.local", input.name)
        }
        InstallationTopology::Native => "127.0.0.1".to_owned(),
    };
    let expected_port = if input.network.topology == InstallationTopology::Native {
        if input.network.database.port < 1024 {
            return Err("installation native database port invalid");
        }
        input.network.database.port
    } else {
        5432
    };
    if input.network.database.host != expected_host
        || input.network.database.port != expected_port
        || input.network.database.database != "insight_platform"
    {
        return Err("installation database target invalid");
    }
    let url = url::Url::parse(value).map_err(|_| "installation database authority invalid")?;
    if !matches!(url.scheme(), "postgres" | "postgresql")
        || url.username() != "insight_installation_admin"
        || url
            .password()
            .is_none_or(|p| p.len() != 32 || !p.bytes().all(|b| b.is_ascii_hexdigit()))
        || url.host_str() != Some(expected_host.as_str())
        || url.port() != Some(expected_port)
        || url.path() != "/insight_platform"
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err("installation database authority differs from declared topology");
    }
    Ok(url)
}
async fn validate_role(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    role: &str,
    marker: &str,
) -> Result<(), &'static str> {
    let valid:Option<bool>=sqlx::query_scalar(r#"SELECT r.rolcanlogin AND NOT(r.rolsuper OR r.rolcreaterole OR r.rolcreatedb OR r.rolreplication OR r.rolbypassrls OR r.rolinherit)
 AND pg_catalog.shobj_description(r.oid,'pg_authid')=$2
 AND NOT EXISTS(SELECT 1 FROM pg_catalog.pg_auth_members WHERE member=r.oid)
 AND NOT EXISTS(SELECT 1 FROM pg_catalog.pg_class WHERE relowner=r.oid)
 AND NOT EXISTS(SELECT 1 FROM pg_catalog.pg_namespace WHERE nspowner=r.oid)
 AND NOT EXISTS(SELECT 1 FROM pg_catalog.pg_database WHERE datdba=r.oid)
 AND NOT pg_catalog.has_database_privilege(r.oid,current_database(),'CREATE')
 AND NOT pg_catalog.has_database_privilege(r.oid,current_database(),'TEMPORARY')
 AND NOT pg_catalog.has_schema_privilege(r.oid,'public','CREATE')
 AND NOT pg_catalog.has_schema_privilege(r.oid,'insight_platform','CREATE')
 FROM pg_catalog.pg_roles r WHERE r.rolname=$1"#).bind(role).bind(marker).fetch_optional(&mut **tx).await.map_err(|_|"installation role verification unavailable")?;
    if valid != Some(true) {
        return Err("installation role has unexpected authority");
    }
    Ok(())
}
async fn effective_privileges(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    role: &str,
) -> Result<serde_json::Value, &'static str> {
    let rows=sqlx::query(r#"SELECT c.relname,a.attname,p.privilege,pg_catalog.has_column_privilege($1,c.oid,a.attnum,p.privilege) AS allowed
 FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
 JOIN pg_catalog.pg_attribute a ON a.attrelid=c.oid AND a.attnum>0 AND NOT a.attisdropped
 CROSS JOIN (VALUES('SELECT'),('INSERT'),('UPDATE'),('REFERENCES')) p(privilege)
 WHERE n.nspname='insight_platform' AND c.relkind='r' ORDER BY c.relname,a.attnum,p.privilege"#).bind(role).fetch_all(&mut **tx).await.map_err(|_|"installation column privileges unavailable")?;
    let mut columns = Vec::new();
    for row in rows {
        columns.push(serde_json::json!([
            row.try_get::<String, _>("relname")
                .map_err(|_| "privilege evidence invalid")?,
            row.try_get::<String, _>("attname")
                .map_err(|_| "privilege evidence invalid")?,
            row.try_get::<String, _>("privilege")
                .map_err(|_| "privilege evidence invalid")?,
            row.try_get::<bool, _>("allowed")
                .map_err(|_| "privilege evidence invalid")?
        ]));
    }
    let rows=sqlx::query(r#"SELECT c.relname,p.privilege,pg_catalog.has_table_privilege($1,c.oid,p.privilege) AS allowed FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace CROSS JOIN (VALUES('DELETE'),('TRUNCATE'),('TRIGGER')) p(privilege) WHERE n.nspname='insight_platform' AND c.relkind='r' ORDER BY c.relname,p.privilege"#).bind(role).fetch_all(&mut **tx).await.map_err(|_|"installation table privileges unavailable")?;
    let mut tables = Vec::new();
    for row in rows {
        tables.push(serde_json::json!([
            row.try_get::<String, _>("relname")
                .map_err(|_| "privilege evidence invalid")?,
            row.try_get::<String, _>("privilege")
                .map_err(|_| "privilege evidence invalid")?,
            row.try_get::<bool, _>("allowed")
                .map_err(|_| "privilege evidence invalid")?
        ]));
    }
    let rows=sqlx::query(r#"SELECT p.oid::regprocedure::text AS signature,pg_catalog.has_function_privilege($1,p.oid,'EXECUTE') AS allowed FROM pg_catalog.pg_proc p JOIN pg_catalog.pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='insight_platform' ORDER BY p.oid::regprocedure::text"#).bind(role).fetch_all(&mut **tx).await.map_err(|_|"installation function privileges unavailable")?;
    let mut functions = Vec::new();
    for row in rows {
        functions.push(serde_json::json!([
            row.try_get::<String, _>("signature")
                .map_err(|_| "privilege evidence invalid")?,
            row.try_get::<bool, _>("allowed")
                .map_err(|_| "privilege evidence invalid")?
        ]));
    }
    Ok(
        serde_json::json!({"schema_version":1,"columns":columns,"tables":tables,"functions":functions}),
    )
}
fn check_directory(path: &Path) -> Result<(), &'static str> {
    if !path.is_absolute() {
        return Err("private installation path must be absolute");
    }
    for ancestor in path.ancestors() {
        let meta = std::fs::symlink_metadata(ancestor)
            .map_err(|_| "private installation directory missing")?;
        if !meta.is_dir() || meta.file_type().is_symlink() {
            return Err("private installation directory invalid");
        }
    }
    super::check_private_path(path, true)
}
fn read_private(path: &Path, limit: usize) -> Result<Vec<u8>, &'static str> {
    check_directory(path.parent().ok_or("private installation path invalid")?)?;
    super::check_private_path(path, false)?;
    let mut options = std::fs::OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
    }
    let file = options
        .open(path)
        .map_err(|_| "installation file unreadable")?;
    let metadata = file
        .metadata()
        .map_err(|_| "installation file unreadable")?;
    if metadata.len() == 0 || metadata.len() > limit as u64 {
        return Err("installation file size invalid");
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt as _;
        if metadata.nlink() != 1 || metadata.mode() & 0o777 != 0o600 {
            return Err("installation file is not private");
        }
    }
    let mut bytes = Vec::new();
    file.take(limit as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| "installation file unreadable")?;
    if bytes.len() > limit {
        return Err("installation file too large");
    }
    Ok(bytes)
}
fn decode_evidence(bytes: &[u8]) -> Result<InstallationDatabaseEvidenceV1, &'static str> {
    let value = parse_strict_json(bytes, INSTALLATION_LIMITS)
        .map_err(|_| "installation evidence invalid")?;
    let evidence: InstallationDatabaseEvidenceV1 =
        serde_json::from_value(value).map_err(|_| "installation evidence invalid")?;
    evidence
        .validate()
        .map_err(|_| "installation evidence invalid")?;
    Ok(evidence)
}
fn write_new_evidence(
    path: &Path,
    evidence: &InstallationDatabaseEvidenceV1,
) -> Result<(), &'static str> {
    check_directory(path.parent().ok_or("installation evidence path invalid")?)?;
    let bytes = serde_json::to_vec_pretty(evidence).map_err(|_| "installation evidence invalid")?;
    if path
        .try_exists()
        .map_err(|_| "installation evidence unavailable")?
    {
        if read_private(path, INSTALLATION_MAX_BYTES)? == bytes {
            return Ok(());
        }
        return Err("installation evidence drift");
    }
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
    }
    let temporary = path.with_extension(format!("pending-{}", std::process::id()));
    let mut file = options
        .open(&temporary)
        .map_err(|_| "installation evidence unavailable")?;
    file.write_all(&bytes)
        .and_then(|()| file.sync_all())
        .map_err(|_| "installation evidence unavailable")?;
    std::fs::rename(&temporary, path).map_err(|_| "installation evidence unavailable")?;
    std::fs::File::open(path.parent().ok_or("installation evidence path invalid")?)
        .and_then(|file| file.sync_all())
        .map_err(|_| "installation evidence unavailable")
}
