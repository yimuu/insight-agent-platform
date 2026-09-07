//! Canonical physical inventory. Only provisioning tools write DDL;
//! runtime verification compares this read-only view with generated expected data.

use serde_json::{json, Value};
use sqlx::{PgConnection, Row};

pub(crate) async fn capture(connection: &mut PgConnection) -> Result<Value, sqlx::Error> {
    // pg_get_* printers are search-path sensitive. The caller owns a transaction,
    // so this canonicalization cannot leak into later pool users.
    sqlx::query("SELECT set_config('search_path', 'pg_catalog', true)")
        .execute(&mut *connection)
        .await?;
    let columns = sqlx::query(
        r#"SELECT c.relname AS table_name, a.attnum AS ordinal, a.attname AS column_name,
            pg_catalog.format_type(a.atttypid, a.atttypmod) AS data_type,
            a.attnotnull AS not_null, a.attidentity::text AS identity_kind,
            a.attgenerated::text AS generated_kind,
            pg_get_expr(d.adbin, d.adrelid, false) AS default_expression
        FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid
        LEFT JOIN pg_catalog.pg_attrdef d ON d.adrelid = c.oid AND d.adnum = a.attnum
        WHERE n.nspname = $1 AND c.relkind IN ('r', 'p')
          AND a.attnum > 0 AND NOT a.attisdropped
        ORDER BY c.relname, a.attnum"#,
    ).bind(crate::AUTHORITY_SCHEMA).fetch_all(&mut *connection).await?.into_iter().map(|row| json!({
        "table": row.get::<String,_>("table_name"), "ordinal": row.get::<i16,_>("ordinal"),
        "column": row.get::<String,_>("column_name"), "type": row.get::<String,_>("data_type"),
        "not_null": row.get::<bool,_>("not_null"), "identity": row.get::<String,_>("identity_kind"),
        "generated": row.get::<String,_>("generated_kind"), "default": row.get::<Option<String>,_>("default_expression"),
    })).collect::<Vec<_>>();
    let constraints = sqlx::query(
        r#"SELECT relation.relname AS table_name, con.conname AS name,
            con.contype::text AS kind, con.convalidated AS validated,
            con.condeferrable AS deferrable, con.condeferred AS initially_deferred,
            pg_get_constraintdef(con.oid, false) AS definition
        FROM pg_catalog.pg_constraint con
        JOIN pg_catalog.pg_class relation ON relation.oid = con.conrelid
        JOIN pg_catalog.pg_namespace n ON n.oid = relation.relnamespace
        WHERE n.nspname = $1 ORDER BY relation.relname, con.conname"#,
    ).bind(crate::AUTHORITY_SCHEMA).fetch_all(&mut *connection).await?.into_iter().map(|row| json!({
        "table":row.get::<String,_>("table_name"), "name":row.get::<String,_>("name"),
        "kind":row.get::<String,_>("kind"), "validated":row.get::<bool,_>("validated"),
        "deferrable":row.get::<bool,_>("deferrable"), "initially_deferred":row.get::<bool,_>("initially_deferred"),
        "definition":row.get::<String,_>("definition"),
    })).collect::<Vec<_>>();
    let indexes = sqlx::query(
        r#"SELECT relation.relname AS table_name, index_relation.relname AS name,
            idx.indisvalid AS valid, idx.indisready AS ready, pg_get_indexdef(idx.indexrelid) AS definition
        FROM pg_catalog.pg_index idx
        JOIN pg_catalog.pg_class relation ON relation.oid = idx.indrelid
        JOIN pg_catalog.pg_class index_relation ON index_relation.oid = idx.indexrelid
        JOIN pg_catalog.pg_namespace n ON n.oid = relation.relnamespace
        WHERE n.nspname = $1 ORDER BY relation.relname, index_relation.relname"#,
    ).bind(crate::AUTHORITY_SCHEMA).fetch_all(&mut *connection).await?.into_iter().map(|row| json!({
        "table":row.get::<String,_>("table_name"), "name":row.get::<String,_>("name"),
        "valid":row.get::<bool,_>("valid"), "ready":row.get::<bool,_>("ready"),
        "definition":row.get::<String,_>("definition"),
    })).collect::<Vec<_>>();
    let functions = sqlx::query(
        r#"SELECT p.proname || '(' || pg_catalog.oidvectortypes(p.proargtypes) || ')' AS identity,
            pg_get_functiondef(p.oid) AS definition
        FROM pg_catalog.pg_proc p JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
        WHERE n.nspname = $1 ORDER BY identity"#,
    ).bind(crate::AUTHORITY_SCHEMA).fetch_all(&mut *connection).await?.into_iter().map(|row| json!({
        "identity":row.get::<String,_>("identity"), "definition":row.get::<String,_>("definition"),
    })).collect::<Vec<_>>();
    let triggers = sqlx::query(
        r#"SELECT relation.relname AS table_name, trigger.tgname AS name,
            trigger.tgenabled::text AS enabled, pg_get_triggerdef(trigger.oid, false) AS definition
        FROM pg_catalog.pg_trigger trigger
        JOIN pg_catalog.pg_class relation ON relation.oid = trigger.tgrelid
        JOIN pg_catalog.pg_namespace n ON n.oid = relation.relnamespace
        WHERE n.nspname = $1 AND NOT trigger.tgisinternal ORDER BY relation.relname, trigger.tgname"#,
    ).bind(crate::AUTHORITY_SCHEMA).fetch_all(&mut *connection).await?.into_iter().map(|row| json!({
        "table":row.get::<String,_>("table_name"), "name":row.get::<String,_>("name"),
        "enabled":row.get::<String,_>("enabled"), "definition":row.get::<String,_>("definition"),
    })).collect::<Vec<_>>();
    let relations = sqlx::query(
        r#"SELECT c.relname AS name, c.relkind::text AS kind, c.relpersistence::text AS persistence,
            c.relrowsecurity AS row_security, c.relforcerowsecurity AS force_row_security,
            CASE WHEN c.relkind IN ('v','m') THEN pg_get_viewdef(c.oid, false) END AS view_definition,
            pg_get_partkeydef(c.oid) AS partition_key
        FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname=$1 AND c.relkind IN ('r','p','v','m','S') ORDER BY c.relname"#,
    ).bind(crate::AUTHORITY_SCHEMA).fetch_all(&mut *connection).await?.into_iter().map(|row| json!({
        "name":row.get::<String,_>("name"), "kind":row.get::<String,_>("kind"),
        "persistence":row.get::<String,_>("persistence"), "row_security":row.get::<bool,_>("row_security"),
        "force_row_security":row.get::<bool,_>("force_row_security"),
        "view_definition":row.get::<Option<String>,_>("view_definition"),
        "partition_key":row.get::<Option<String>,_>("partition_key"),
    })).collect::<Vec<_>>();
    Ok(
        json!({ "inventory_version":1, "schema":crate::AUTHORITY_SCHEMA,
        "postgres_major":crate::POSTGRES_MAJOR_VERSION, "relations":relations,
        "columns":columns, "constraints":constraints, "indexes":indexes,
        "functions":functions, "triggers":triggers }),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn physical_constraint_and_index_drift_is_rejected() {
        let database_url = std::env::var("PLATFORM_TEST_SCHEMA_INVENTORY_DATABASE_URL").expect(
            "PLATFORM_TEST_SCHEMA_INVENTORY_DATABASE_URL is required for this PostgreSQL test",
        );
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .connect(&database_url)
            .await
            .unwrap();
        let mut tx = pool.begin().await.unwrap();
        sqlx::raw_sql(crate::CURRENT_SCHEMA_SQL)
            .execute(&mut *tx)
            .await
            .unwrap();
        crate::verify_inventory_value(&capture(&mut tx).await.unwrap()).unwrap();
        for mutation in [
            "DROP INDEX insight_platform.artifact_blobs_content_uq",
            "ALTER TABLE insight_platform.artifact_blobs DROP CONSTRAINT artifact_blobs_backend_ck",
            "ALTER TABLE insight_platform.artifact_blobs ALTER COLUMN created_at DROP NOT NULL",
        ] {
            sqlx::query("SAVEPOINT physical_drift")
                .execute(&mut *tx)
                .await
                .unwrap();
            sqlx::query(mutation).execute(&mut *tx).await.unwrap();
            assert!(matches!(
                crate::verify_inventory_value(&capture(&mut tx).await.unwrap()),
                Err(crate::AuthoritySchemaError::SchemaInventoryMismatch { .. })
            ));
            sqlx::query("ROLLBACK TO SAVEPOINT physical_drift")
                .execute(&mut *tx)
                .await
                .unwrap();
        }
        tx.rollback().await.unwrap();
    }
}
