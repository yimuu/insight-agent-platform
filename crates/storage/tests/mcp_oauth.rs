mod support;

use chrono::{DateTime, Duration, Utc};
use insight_durable::{
    ClaimMcpOAuthRefreshCommand, CompleteMcpOAuthCallbackCommand,
    ConsumeMcpOAuthTransactionCommand, CreateMcpOAuthTransactionCommand, McpInteractionPrincipal,
    McpOAuthDurableRepository, McpOAuthTransactionId, McpOAuthTransactionState,
    McpSecretCiphertext, StoreMcpOAuthCredentialCommand,
};
use insight_engine::TransitionOutcome;
use insight_storage::{PostgresDurableRepository, SqliteDurableRepository};
use sqlx::{postgres::PgPoolOptions, AssertSqlSafe, PgPool};
use uuid::Uuid;

fn now() -> DateTime<Utc> {
    "2026-07-30T12:00:00Z".parse().unwrap()
}

fn principal() -> McpInteractionPrincipal {
    McpInteractionPrincipal::new("tenant-a", "user-a").unwrap()
}

fn transaction(label: &str) -> CreateMcpOAuthTransactionCommand {
    CreateMcpOAuthTransactionCommand::new(
        McpOAuthTransactionId::new(format!("oauth-{label}")).unwrap(),
        principal(),
        "calendar",
        "https://issuer.example",
        "https://mcp.example",
        "client-a",
        "https://platform.example/v1/mcp/oauth/callback",
        vec!["calendar.read".to_owned(), "calendar.write".to_owned()],
        "a".repeat(64),
        McpSecretCiphertext::new(format!("enc:v1:transaction-{label}")).unwrap(),
        "b".repeat(64),
        now() + Duration::minutes(10),
        now(),
    )
    .unwrap()
}

fn credential(
    generation: u64,
    expected_generation: Option<u64>,
    request_id: &str,
) -> StoreMcpOAuthCredentialCommand {
    StoreMcpOAuthCredentialCommand::new(
        principal(),
        "calendar",
        "https://issuer.example",
        "client-a",
        "https://mcp.example",
        vec!["calendar.read".to_owned(), "calendar.write".to_owned()],
        "Bearer",
        generation,
        expected_generation,
        request_id,
        McpSecretCiphertext::new(format!("enc:v1:access-{generation}")).unwrap(),
        "c".repeat(64),
        Some(McpSecretCiphertext::new(format!("enc:v1:refresh-{generation}")).unwrap()),
        Some("d".repeat(64)),
        Some(now() + Duration::hours(1)),
        now() + Duration::seconds(generation as i64),
    )
    .unwrap()
}

async fn exercise_repository<R>(repository: R)
where
    R: McpOAuthDurableRepository,
{
    let create = transaction("contract");
    let transaction_id = create.transaction().transaction_id().clone();
    assert!(matches!(
        repository
            .create_mcp_oauth_transaction(create.clone())
            .await
            .unwrap(),
        TransitionOutcome::Committed { .. }
    ));
    assert!(matches!(
        repository
            .create_mcp_oauth_transaction(create)
            .await
            .unwrap(),
        TransitionOutcome::ExactReplay { .. }
    ));
    let secret = repository
        .load_mcp_oauth_transaction_secret(&transaction_id)
        .await
        .unwrap()
        .unwrap();
    assert!(!format!("{secret:?}").contains("transaction-contract"));

    let current = repository
        .load_mcp_oauth_transaction(&transaction_id)
        .await
        .unwrap()
        .unwrap();
    let wrong_state = ConsumeMcpOAuthTransactionCommand::new(
        transaction_id.clone(),
        principal(),
        "wrong-state",
        current.version(),
        "0".repeat(64),
        now() + Duration::seconds(1),
    )
    .unwrap();
    assert!(matches!(
        repository
            .consume_mcp_oauth_transaction(wrong_state)
            .await
            .unwrap(),
        TransitionOutcome::StateConflict
    ));
    let wrong_principal = ConsumeMcpOAuthTransactionCommand::new(
        transaction_id.clone(),
        McpInteractionPrincipal::new("tenant-a", "user-b").unwrap(),
        "wrong-principal",
        current.version(),
        "a".repeat(64),
        now() + Duration::seconds(1),
    )
    .unwrap();
    assert!(matches!(
        repository
            .consume_mcp_oauth_transaction(wrong_principal)
            .await
            .unwrap(),
        TransitionOutcome::StateConflict
    ));
    let consume = ConsumeMcpOAuthTransactionCommand::new(
        transaction_id,
        principal(),
        "consume",
        current.version(),
        "a".repeat(64),
        now() + Duration::seconds(1),
    )
    .unwrap();
    let consumed = repository
        .consume_mcp_oauth_transaction(consume.clone())
        .await
        .unwrap();
    assert!(matches!(consumed, TransitionOutcome::Committed { .. }));
    assert!(matches!(
        repository
            .consume_mcp_oauth_transaction(consume)
            .await
            .unwrap(),
        TransitionOutcome::ExactReplay { .. }
    ));
    let consumed = repository
        .load_mcp_oauth_transaction(&McpOAuthTransactionId::new("oauth-contract").unwrap())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(consumed.state(), McpOAuthTransactionState::Consumed);
    assert!(repository
        .load_mcp_oauth_transaction_secret(&McpOAuthTransactionId::new("oauth-contract").unwrap())
        .await
        .unwrap()
        .is_none());

    let first = credential(1, None, "store-1");
    assert!(matches!(
        repository
            .store_mcp_oauth_credential(first.clone())
            .await
            .unwrap(),
        TransitionOutcome::Committed { .. }
    ));
    assert!(matches!(
        repository.store_mcp_oauth_credential(first).await.unwrap(),
        TransitionOutcome::ExactReplay { .. }
    ));
    assert!(matches!(
        repository
            .store_mcp_oauth_credential(credential(2, Some(99), "stale"))
            .await
            .unwrap(),
        TransitionOutcome::StateConflict
    ));
    assert!(matches!(
        repository
            .store_mcp_oauth_credential(credential(2, Some(1), "store-2"))
            .await
            .unwrap(),
        TransitionOutcome::Committed { .. }
    ));
    let credential_secret = repository
        .load_mcp_oauth_credential_secret(&principal(), "calendar")
        .await
        .unwrap()
        .unwrap();
    assert!(!format!("{credential_secret:?}").contains("access-2"));
    assert!(!format!("{credential_secret:?}").contains("refresh-2"));

    let refresh_claim = |owner: &str| {
        ClaimMcpOAuthRefreshCommand::new(
            principal(),
            "calendar",
            2,
            owner,
            now() + Duration::seconds(2),
            now() + Duration::seconds(32),
        )
        .unwrap()
    };
    assert!(repository
        .claim_mcp_oauth_refresh(refresh_claim("refresh-worker-a"))
        .await
        .unwrap());
    assert!(!repository
        .claim_mcp_oauth_refresh(refresh_claim("refresh-worker-b"))
        .await
        .unwrap());
    assert!(!repository
        .release_mcp_oauth_refresh(&principal(), "calendar", 2, "refresh-worker-b")
        .await
        .unwrap());
    assert!(repository
        .release_mcp_oauth_refresh(&principal(), "calendar", 2, "refresh-worker-a")
        .await
        .unwrap());
    assert!(repository
        .claim_mcp_oauth_refresh(refresh_claim("refresh-worker-b"))
        .await
        .unwrap());
    assert!(repository
        .mark_mcp_oauth_refresh_dispatched(
            &principal(),
            "calendar",
            2,
            "refresh-worker-b",
            now() + Duration::seconds(3),
        )
        .await
        .unwrap());
    assert!(!repository
        .release_mcp_oauth_refresh(&principal(), "calendar", 2, "refresh-worker-b")
        .await
        .unwrap());
    assert!(matches!(
        repository
            .store_mcp_oauth_credential(
                credential(3, Some(2), "refresh-store-3")
                    .with_refresh_lease_owner("refresh-worker-b")
                    .unwrap()
            )
            .await
            .unwrap(),
        TransitionOutcome::Committed { .. }
    ));
    assert!(!repository
        .release_mcp_oauth_refresh(&principal(), "calendar", 2, "refresh-worker-b")
        .await
        .unwrap());

    let crash_claim = ClaimMcpOAuthRefreshCommand::new(
        principal(),
        "calendar",
        3,
        "refresh-worker-crash",
        now() + Duration::seconds(4),
        now() + Duration::seconds(34),
    )
    .unwrap();
    assert!(repository
        .claim_mcp_oauth_refresh(crash_claim)
        .await
        .unwrap());
    assert!(repository
        .mark_mcp_oauth_refresh_dispatched(
            &principal(),
            "calendar",
            3,
            "refresh-worker-crash",
            now() + Duration::seconds(5),
        )
        .await
        .unwrap());
    assert!(!repository
        .release_mcp_oauth_refresh(&principal(), "calendar", 3, "refresh-worker-crash")
        .await
        .unwrap());
    let recovery_claim = ClaimMcpOAuthRefreshCommand::new(
        principal(),
        "calendar",
        3,
        "refresh-worker-recovery",
        now() + Duration::seconds(35),
        now() + Duration::seconds(65),
    )
    .unwrap();
    assert!(!repository
        .claim_mcp_oauth_refresh(recovery_claim)
        .await
        .unwrap());
    let quarantined = repository
        .load_mcp_oauth_credential(&principal(), "calendar")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(quarantined.generation(), 4);
    assert!(quarantined.revoked_at().is_some());
    assert!(repository
        .load_mcp_oauth_credential_secret(&principal(), "calendar")
        .await
        .unwrap()
        .is_none());

    assert!(matches!(
        repository
            .delete_mcp_oauth_credential(
                &principal(),
                "calendar",
                "disconnect",
                now() + Duration::seconds(3)
            )
            .await
            .unwrap(),
        TransitionOutcome::Committed { .. }
    ));
    assert!(repository
        .load_mcp_oauth_credential_secret(&principal(), "calendar")
        .await
        .unwrap()
        .is_none());
    let revoked = repository
        .load_mcp_oauth_credential(&principal(), "calendar")
        .await
        .unwrap()
        .unwrap();
    assert!(revoked.revoked_at().is_some());
    assert!(!repository
        .release_mcp_oauth_refresh(&principal(), "calendar", 2, "refresh-worker-b")
        .await
        .unwrap());

    let atomic_create = transaction("atomic");
    let atomic_id = atomic_create.transaction().transaction_id().clone();
    repository
        .create_mcp_oauth_transaction(atomic_create)
        .await
        .unwrap();
    let atomic_transaction = repository
        .load_mcp_oauth_transaction(&atomic_id)
        .await
        .unwrap()
        .unwrap();
    let atomic_consume = ConsumeMcpOAuthTransactionCommand::new(
        atomic_id,
        principal(),
        "atomic-consume",
        atomic_transaction.version(),
        "a".repeat(64),
        now() + Duration::seconds((revoked.generation() + 1) as i64),
    )
    .unwrap();
    let atomic = CompleteMcpOAuthCallbackCommand::new(
        atomic_consume,
        credential(
            revoked.generation() + 1,
            Some(revoked.generation()),
            "atomic-store",
        ),
    )
    .unwrap();
    let committed = repository
        .complete_mcp_oauth_callback(atomic.clone())
        .await
        .unwrap();
    let TransitionOutcome::Committed { result } = committed else {
        panic!("OAuth callback must commit both authorities atomically");
    };
    assert_eq!(
        result.transaction.state(),
        McpOAuthTransactionState::Consumed
    );
    assert_eq!(result.credential.generation(), revoked.generation() + 1);
    assert!(result.credential.revoked_at().is_none());
    assert!(matches!(
        repository
            .complete_mcp_oauth_callback(atomic)
            .await
            .unwrap(),
        TransitionOutcome::ExactReplay { .. }
    ));
    assert!(repository
        .load_mcp_oauth_transaction_secret(&McpOAuthTransactionId::new("oauth-atomic").unwrap())
        .await
        .unwrap()
        .is_none());

    let expiring = transaction("expired");
    let expiring_id = expiring.transaction().transaction_id().clone();
    repository
        .create_mcp_oauth_transaction(expiring)
        .await
        .unwrap();
    assert_eq!(
        repository
            .expire_mcp_oauth_transactions(now() + Duration::minutes(11), 16)
            .await
            .unwrap(),
        1
    );
    let expired = repository
        .load_mcp_oauth_transaction(&expiring_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(expired.state(), McpOAuthTransactionState::Expired);
    assert_eq!(expired.version(), 2);
    assert!(repository
        .load_mcp_oauth_transaction_secret(&expiring_id)
        .await
        .unwrap()
        .is_none());
    assert_eq!(
        repository
            .expire_mcp_oauth_transactions(now() + Duration::minutes(12), 16)
            .await
            .unwrap(),
        0
    );
}

async fn isolated_postgres_repository(
) -> Option<(PostgresDurableRepository, PgPool, PgPool, String)> {
    let database_url = std::env::var("TEST_POSTGRES_URL").ok()?;
    let schema = format!("mcp_oauth_{}", Uuid::new_v4().simple());
    let admin = PgPoolOptions::new()
        .max_connections(4)
        .connect(&database_url)
        .await
        .unwrap();
    sqlx::query(AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
        .execute(&admin)
        .await
        .unwrap();
    let separator = if database_url.contains('?') { '&' } else { '?' };
    let scoped_url = format!("{database_url}{separator}options=-csearch_path%3D{schema}");
    let control = PgPoolOptions::new()
        .max_connections(4)
        .connect(&scoped_url)
        .await
        .unwrap();
    support::provision_postgres_schema(&control).await;
    let repository = PostgresDurableRepository::connect(&scoped_url)
        .await
        .unwrap();
    Some((repository, control, admin, schema))
}

#[tokio::test]
async fn sqlite_mcp_oauth_transactions_and_credentials_are_durable_and_fenced() {
    let (_temporary, repository): (_, SqliteDurableRepository) =
        support::temporary_sqlite_repository().await;
    exercise_repository(repository).await;
}

#[tokio::test]
async fn postgres_mcp_oauth_transactions_and_credentials_are_durable_and_fenced() {
    let Some((repository, control, admin, schema)) = isolated_postgres_repository().await else {
        assert!(
            std::env::var_os("CI").is_none(),
            "CI must set TEST_POSTGRES_URL for PostgreSQL MCP OAuth conformance"
        );
        return;
    };
    exercise_repository(repository).await;
    drop(control);
    sqlx::query(AssertSqlSafe(format!("DROP SCHEMA {schema} CASCADE")))
        .execute(&admin)
        .await
        .unwrap();
    admin.close().await;
}
