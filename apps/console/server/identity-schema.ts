import { readFileSync } from 'node:fs'
import type { Pool } from 'pg'

type Inventory = Record<string, Array<Record<string, unknown>>>
function stable(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(stable).join(',')}]`
  if (value && typeof value === 'object')
    return `{${Object.entries(value)
      .sort(([a], [b]) => a.localeCompare(b))
      .map(([key, item]) => `${JSON.stringify(key)}:${stable(item)}`)
      .join(',')}}`
  return JSON.stringify(value)
}
export function identitySchemaInventory(): Inventory {
  return JSON.parse(readFileSync(new URL('./schema-inventory.json', import.meta.url), 'utf8'))
}
/** Read-only comparison against the PostgreSQL owner's generated physical inventory. */
export async function verifyIdentitySchema(pool: Pool, expected: Inventory) {
  const connection = await pool.connect()
  const names = ['local_console_owner', 'local_console_sessions']
  try {
    await connection.query('BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY')
    await connection.query("SELECT set_config('search_path','pg_catalog',true)")
    const queries: Record<string, string> = {
      columns: `SELECT c.relname AS "table", a.attnum AS ordinal,a.attname AS "column",format_type(a.atttypid,a.atttypmod) AS type,a.attnotnull AS not_null,a.attidentity::text AS identity,a.attgenerated::text AS generated,pg_get_expr(d.adbin,d.adrelid,false) AS "default"
        FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace JOIN pg_attribute a ON a.attrelid=c.oid LEFT JOIN pg_attrdef d ON d.adrelid=c.oid AND d.adnum=a.attnum
        WHERE n.nspname='insight_platform' AND c.relname=ANY($1) AND a.attnum>0 AND NOT a.attisdropped ORDER BY c.relname,a.attnum`,
      constraints: `SELECT c.relname AS "table",con.conname AS name,con.contype::text AS kind,con.convalidated AS validated,con.condeferrable AS deferrable,con.condeferred AS initially_deferred,pg_get_constraintdef(con.oid,false) AS definition
        FROM pg_constraint con JOIN pg_class c ON c.oid=con.conrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='insight_platform' AND c.relname=ANY($1) ORDER BY c.relname,con.conname`,
      indexes: `SELECT c.relname AS "table",i.relname AS name,idx.indisvalid AS valid,idx.indisready AS ready,pg_get_indexdef(idx.indexrelid) AS definition
        FROM pg_index idx JOIN pg_class c ON c.oid=idx.indrelid JOIN pg_class i ON i.oid=idx.indexrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='insight_platform' AND c.relname=ANY($1) ORDER BY c.relname,i.relname`,
      relations: `SELECT c.relname AS name,c.relkind::text AS kind,c.relpersistence::text AS persistence,c.relrowsecurity AS row_security,c.relforcerowsecurity AS force_row_security,CASE WHEN c.relkind IN ('v','m') THEN pg_get_viewdef(c.oid,false) END AS view_definition,pg_get_partkeydef(c.oid) AS partition_key
        FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='insight_platform' AND c.relname=ANY($1) ORDER BY c.relname`,
      triggers: `SELECT c.relname AS "table",t.tgname AS name,t.tgenabled::text AS enabled,pg_get_triggerdef(t.oid,false) AS definition
        FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='insight_platform' AND c.relname=ANY($1) AND NOT t.tgisinternal ORDER BY c.relname,t.tgname`,
    }
    for (const [kind, sql] of Object.entries(queries)) {
      const rows = expected[kind]?.filter((row) =>
        names.includes(String(row[kind === 'relations' ? 'name' : 'table'])),
      )
      if (!rows || stable((await connection.query(sql, [names])).rows) !== stable(rows))
        throw new Error('Local identity schema incompatible')
    }
    await connection.query('COMMIT')
  } catch (error) {
    await connection.query('ROLLBACK').catch(() => {})
    throw error
  } finally {
    connection.release()
  }
}
