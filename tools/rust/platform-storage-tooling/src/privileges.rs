use sqlx::Row as _;
pub async fn effective_privileges(
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
