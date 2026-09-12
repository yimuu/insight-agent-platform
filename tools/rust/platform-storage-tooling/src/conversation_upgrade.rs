use insight_platform_contracts::canonical_digest;
use insight_platform_deployment_contracts::installation_release::InstallationReleaseV1;
use insight_platform_postgres::capture_schema_inventory_in_transaction;
use sqlx::PgPool;

pub fn target_inventory_digest() -> String {
    insight_platform_postgres::expected_schema_inventory_digest()
}

/// Deployment evidence CAS for a package-only rollout. Executes no schema or grant statements.
pub async fn rollout_package(
    pool: &PgPool,
    intent: &insight_platform_deployment_contracts::installation_release::PackageRolloutIntentV1,
) -> Result<(), String> {
    intent
        .validate_transition()
        .map_err(|_| "invalid package transition")?;
    if intent.target_release.to_schema_version != 17
        || intent.target_release.to_inventory_digest.as_str() != target_inventory_digest()
    {
        return Err("package rollout requires the complete current schema".into());
    }
    let mut tx = pool
        .begin()
        .await
        .map_err(|_| "cannot begin package rollout")?;
    sqlx::query("SET LOCAL lock_timeout = '5s'")
        .execute(&mut *tx)
        .await
        .map_err(|_| "cannot bound rollout lock")?;
    sqlx::query("SELECT pg_advisory_xact_lock(184739, 17)")
        .execute(&mut *tx)
        .await
        .map_err(|_| "cannot lock package rollout")?;
    let serving:i64=sqlx::query_scalar("SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid() AND usename LIKE 'insight_%' AND usename<>'insight_installation_admin'").fetch_one(&mut *tx).await.map_err(|_|"cannot verify serving stopped")?;
    if serving != 0 {
        return Err("stop all serving processes before rollout".into());
    }
    let actual = capture_schema_inventory_in_transaction(&mut tx)
        .await
        .map_err(|_| "cannot verify rollout schema")?;
    if canonical_digest(&actual).map_err(|_| "cannot digest rollout schema")?
        != target_inventory_digest()
    {
        return Err("installed schema differs from rollout target".into());
    }
    let rows: Vec<serde_json::Value> = sqlx::query_scalar(
        "SELECT receipt FROM public.insight_installation_upgrade_receipt FOR UPDATE",
    )
    .fetch_all(&mut *tx)
    .await
    .map_err(|_| "cannot lock deployment receipt")?;
    let previous =
        serde_json::to_value(&intent.previous_release).map_err(|_| "invalid previous release")?;
    let target =
        serde_json::to_value(&intent.target_release).map_err(|_| "invalid target release")?;
    if rows != [target.clone()] {
        if rows != [previous.clone()] {
            return Err("published database release differs from expected previous".into());
        }
        let changed=sqlx::query("UPDATE public.insight_installation_upgrade_receipt SET receipt=$1 WHERE singleton=true AND receipt=$2").bind(target).bind(previous).execute(&mut *tx).await.map_err(|_|"cannot advance deployment receipt")?;
        if changed.rows_affected() != 1 {
            return Err("deployment receipt CAS failed".into());
        }
    }
    tx.commit()
        .await
        .map_err(|_| "package rollout commit requires verification")?;
    Ok(())
}

/// A single reviewed source-to-current transition, committed with its exact installation receipt.
/// A retry is accepted only for the same receipt and the complete target inventory.
pub async fn upgrade(pool: &PgPool, release: &InstallationReleaseV1) -> Result<(), String> {
    use insight_platform_deployment_contracts::installation_release::*;
    if release.from_schema_version != SOURCE_SCHEMA_VERSION
        || release.to_schema_version != TARGET_SCHEMA_VERSION
        || release.from_inventory_digest.as_str() != SOURCE_INVENTORY_DIGEST
        || release.to_inventory_digest.as_str() != target_inventory_digest()
    {
        return Err("unsupported schema upgrade identity".into());
    }
    let mut tx = pool
        .begin()
        .await
        .map_err(|_| "cannot begin schema upgrade")?;
    sqlx::query("SET LOCAL lock_timeout = '5s'")
        .execute(&mut *tx)
        .await
        .map_err(|_| "cannot set upgrade lock timeout")?;
    sqlx::query("SELECT pg_advisory_xact_lock(184739, 17)")
        .execute(&mut *tx)
        .await
        .map_err(|_| "cannot lock schema upgrade")?;
    let serving: i64 = sqlx::query_scalar("SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() AND pid <> pg_backend_pid() AND usename LIKE 'insight_%' AND usename <> 'insight_installation_admin'")
        .fetch_one(&mut *tx).await.map_err(|_| "cannot verify serving processes stopped")?;
    if serving != 0 {
        return Err("stop all serving processes before upgrading".into());
    }
    let actual = capture_schema_inventory_in_transaction(&mut tx)
        .await
        .map_err(|_| "cannot inspect installed schema")?;
    let actual = canonical_digest(&actual).map_err(|_| "cannot digest installed schema")?;
    let receipt_exists: bool = sqlx::query_scalar(
        "SELECT to_regclass('public.insight_installation_upgrade_receipt') IS NOT NULL",
    )
    .fetch_one(&mut *tx)
    .await
    .map_err(|_| "cannot inspect upgrade receipt")?;
    let wanted = serde_json::to_value(release).map_err(|_| "cannot encode upgrade receipt")?;
    if actual == release.to_inventory_digest.as_str() {
        if !receipt_exists {
            return Err("target schema has no matching upgrade receipt".into());
        }
        let rows: Vec<serde_json::Value> =
            sqlx::query_scalar("SELECT receipt FROM public.insight_installation_upgrade_receipt")
                .fetch_all(&mut *tx)
                .await
                .map_err(|_| "cannot read upgrade receipt")?;
        if rows != [wanted] {
            return Err("upgrade receipt differs".into());
        }
        tx.commit()
            .await
            .map_err(|_| "cannot complete upgrade verification")?;
        return Ok(());
    }
    if actual != release.from_inventory_digest.as_str() || receipt_exists {
        return Err("installed schema is not the exact supported source".into());
    }
    sqlx::raw_sql(include_str!("../schema-conversation-upgrade.sql"))
        .execute(&mut *tx)
        .await
        .map_err(|_| "conversation schema upgrade failed")?;
    sqlx::raw_sql("GRANT SELECT, INSERT, UPDATE, DELETE ON insight_platform.conversations, insight_platform.conversation_turns TO insight_runtime_dev;
        GRANT SELECT ON insight_platform.conversation_turns TO insight_artifact_data_reader_dev;
        GRANT SELECT (principal_id, state, version) ON insight_platform.principals TO insight_artifact_data_reader_dev;
        GRANT SELECT (tenant_id, principal_id, principal_kind, state, generation, version, permissions_schema_version, permissions, permissions_digest) ON insight_platform.tenant_principals TO insight_artifact_data_reader_dev;
        GRANT SELECT (input_value_id, output_value_id) ON insight_platform.runs TO insight_artifact_data_reader_dev;
        GRANT SELECT (tenant_id, run_id, artifact_id) ON insight_platform.run_values TO insight_artifact_gateway_dev;
        GRANT SELECT (tenant_id, run_id) ON insight_platform.conversation_turns TO insight_artifact_gateway_dev")
        .execute(&mut *tx).await.map_err(|_| "conversation role grants failed")?;
    let actual = capture_schema_inventory_in_transaction(&mut tx)
        .await
        .map_err(|_| "cannot inspect upgraded schema")?;
    if canonical_digest(&actual).map_err(|_| "cannot digest upgraded schema")?
        != release.to_inventory_digest.as_str()
    {
        return Err("upgraded schema differs from complete current inventory".into());
    }
    sqlx::raw_sql("CREATE TABLE public.insight_installation_upgrade_receipt (singleton boolean PRIMARY KEY CHECK (singleton), receipt jsonb NOT NULL); REVOKE ALL ON public.insight_installation_upgrade_receipt FROM PUBLIC;")
        .execute(&mut *tx).await.map_err(|_| "cannot create upgrade receipt")?;
    sqlx::query("INSERT INTO public.insight_installation_upgrade_receipt VALUES (true, $1)")
        .bind(wanted)
        .execute(&mut *tx)
        .await
        .map_err(|_| "cannot write upgrade receipt")?;
    tx.commit()
        .await
        .map_err(|_| "schema upgrade commit requires verification")?;
    Ok(())
}

/// Rebind evidence only after proving every pre-existing privilege is unchanged. Newly added
/// tables must have exactly the reviewed runtime DML privileges, and no other role gains access.
pub async fn refresh_role_evidence(
    pool: &PgPool,
    old: &insight_platform_deployment_contracts::installation::InstallationDatabaseEvidenceV1,
) -> Result<
    insight_platform_deployment_contracts::installation::InstallationDatabaseEvidenceV1,
    String,
> {
    old.validate().map_err(|_| "invalid role evidence")?;
    let mut next = old.clone();
    let mut tx = pool
        .begin()
        .await
        .map_err(|_| "cannot verify role evidence")?;
    for role in &mut next.roles {
        let current = crate::privileges::effective_privileges(&mut tx, &role.role_name).await?;
        let mut previous = current.clone();
        let upgraded = current["columns"]
            .as_array()
            .ok_or("invalid privilege columns")?
            .iter()
            .any(|row| row[0] == "conversations");
        if upgraded && role.role_name == "insight_artifact_gateway_dev" {
            for row in previous["columns"]
                .as_array_mut()
                .ok_or("invalid privilege columns")?
            {
                if row[0] == "run_values"
                    && matches!(
                        row[1].as_str(),
                        Some("tenant_id" | "run_id" | "artifact_id")
                    )
                    && row[2] == "SELECT"
                {
                    if row[3] != true {
                        return Err("conversation retention grants missing".into());
                    }
                    row[3] = serde_json::Value::Bool(false);
                }
            }
        }
        if upgraded && role.role_name == "insight_artifact_data_reader_dev" {
            for row in previous["columns"]
                .as_array_mut()
                .ok_or("invalid privilege columns")?
            {
                let table = row[0].as_str().ok_or("invalid table")?;
                let column = row[1].as_str().ok_or("invalid column")?;
                let added = match table {
                    "principals" => matches!(column, "principal_id" | "state" | "version"),
                    "tenant_principals" => matches!(
                        column,
                        "tenant_id"
                            | "principal_id"
                            | "principal_kind"
                            | "state"
                            | "generation"
                            | "version"
                            | "permissions_schema_version"
                            | "permissions"
                            | "permissions_digest"
                    ),
                    "runs" => matches!(column, "input_value_id" | "output_value_id"),
                    _ => false,
                };
                if added && row[2] == "SELECT" {
                    if row[3] != true {
                        return Err("conversation disclosure grants missing".into());
                    }
                    row[3] = serde_json::Value::Bool(false);
                }
            }
        }
        for key in ["columns", "tables"] {
            let rows = previous[key]
                .as_array_mut()
                .ok_or("invalid role privileges")?;
            for row in rows.iter().filter(|row| {
                matches!(
                    row[0].as_str(),
                    Some("conversations" | "conversation_turns")
                )
            }) {
                let privilege = row.as_array().ok_or("invalid privilege row")?;
                let action = privilege[privilege.len() - 2]
                    .as_str()
                    .ok_or("invalid privilege action")?;
                let expected = (role.role_name == "insight_runtime_dev"
                    && matches!(action, "SELECT" | "INSERT" | "UPDATE" | "DELETE"))
                    || (role.role_name == "insight_artifact_data_reader_dev"
                        && row[0] == "conversation_turns"
                        && action == "SELECT")
                    || (role.role_name == "insight_artifact_gateway_dev"
                        && key == "columns"
                        && row[0] == "conversation_turns"
                        && matches!(row[1].as_str(), Some("tenant_id" | "run_id"))
                        && action == "SELECT");
                if privilege.last().and_then(|v| v.as_bool()) != Some(expected) {
                    return Err("new conversation privileges differ".into());
                }
            }
            rows.retain(|row| {
                !matches!(
                    row[0].as_str(),
                    Some("conversations" | "conversation_turns")
                )
            });
        }
        let actual = canonical_digest(&current).map_err(|_| "invalid privilege digest")?;
        let prior = canonical_digest(&previous).map_err(|_| "invalid privilege digest")?;
        if role.effective_privileges_digest.as_str() != prior
            && role.effective_privileges_digest.as_str() != actual
        {
            return Err("pre-existing privileges drifted".into());
        }
        role.effective_privileges_digest =
            actual.parse().map_err(|_| "invalid privilege digest")?;
    }
    tx.commit()
        .await
        .map_err(|_| "cannot verify role evidence")?;
    Ok(next)
}
