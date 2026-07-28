mod support;

use std::{collections::BTreeSet, time::Duration as StdDuration};

use chrono::{Duration, Utc};
use insight_engine::{
    repository::{REPOSITORY_CONSTRAINT_CONFLICT, REPOSITORY_DATA_INVALID},
    ContentHash, DefinitionRevisionId, DeploymentRevisionId, PersistenceMode, RunId,
};
use insight_storage::{
    AckContentDeletionJob, AckTerminalArtifactStage, AdmissionConversation, ArchiveOutcome,
    BoundedRetention, ClaimContentDeletionJobs, ClaimConversationSummaryJob,
    ClaimTerminalArtifactStages, CommitConversationTurn, ContentDeletionSourceKind,
    ConversationContent, ConversationContextQuery, ConversationQuery, ConversationRole,
    ConversationStore, HeartbeatRuntimeInstance, MessagePageQuery, NewConversation,
    NewConversationMessage, NewConversationSummary, NewConversationTurn, NewTerminalArtifactStage,
    NewTerminalRunAdmission, NewTerminalRunResult, OwnerLeaseHeartbeat, OwnerLeaseQuery,
    OwnerLeaseStatus, PostgresDurableRepository, PrivacyDeleteOutcome,
    ReleaseConversationSummaryJob, ResolveTerminalArtifactStage, RuntimeInstanceLease,
    RuntimeOwner, SqliteDurableRepository, TerminalArtifactSourceKind,
    TerminalArtifactStageDisposition, TerminalArtifactStagingStore, TerminalContentDeletionStore,
    TerminalRunDerivedState, TerminalRunQuery, TerminalRunStore, TerminalState,
    CONVERSATION_OWNERSHIP_MISMATCH, TERMINAL_RUN_OWNER_LEASE_LOST,
    TERMINAL_SCOPED_ARTIFACT_MEDIA_TYPE,
};
use serde_json::json;
use sqlx::{
    postgres::PgPoolOptions,
    sqlite::{SqliteConnectOptions, SqlitePoolOptions},
    AssertSqlSafe, PgPool,
};
use uuid::Uuid;

async fn sleep_past(deadline: chrono::DateTime<Utc>) {
    if let Ok(remaining) = (deadline - Utc::now()).to_std() {
        tokio::time::sleep(remaining + StdDuration::from_millis(50)).await;
    }
}

fn scoped_artifact_ref(label: &str) -> (String, ContentHash) {
    let content_hash = ContentHash::from_bytes(label.as_bytes());
    let artifact_id = insight_engine::ArtifactId::new(format!(
        "artifact_{}",
        content_hash.as_str().trim_start_matches("sha256:")
    ))
    .unwrap();
    let artifact = insight_engine::ArtifactRef::new(
        artifact_id,
        content_hash.clone(),
        u64::try_from(label.len()).unwrap(),
        Some(TERMINAL_SCOPED_ARTIFACT_MEDIA_TYPE.to_owned()),
    )
    .unwrap();
    (serde_json::to_string(&artifact).unwrap(), content_hash)
}

async fn stage_artifact<S>(
    store: &S,
    tenant_id: &str,
    content_ref: &str,
    content_hash: &ContentHash,
    source_kind: TerminalArtifactSourceKind,
    source_id: &str,
    now: chrono::DateTime<Utc>,
) where
    S: TerminalArtifactStagingStore,
{
    store
        .stage_terminal_artifact(NewTerminalArtifactStage {
            tenant_id: tenant_id.to_owned(),
            content_ref: content_ref.to_owned(),
            content_hash: content_hash.clone(),
            source_kind,
            source_id: source_id.to_owned(),
            available_at: now,
            created_at: now,
        })
        .await
        .unwrap();
}

fn run_id(label: &str) -> RunId {
    RunId::new(format!("run_{label}")).unwrap()
}

fn lease(label: &str, epoch: i64, now: chrono::DateTime<Utc>) -> RuntimeInstanceLease {
    RuntimeInstanceLease {
        owner: RuntimeOwner {
            instance_id: format!("runtime_{label}"),
            owner_epoch: epoch,
        },
        endpoint: format!("http://runtime-{label}.internal"),
        lease_expires_at: now + Duration::hours(1),
        started_at: now,
    }
}

fn admission(
    label: &str,
    tenant_id: &str,
    request_id: &str,
    agent_id: &str,
    owner: &RuntimeOwner,
    now: chrono::DateTime<Utc>,
    conversation: Option<AdmissionConversation>,
) -> NewTerminalRunAdmission {
    NewTerminalRunAdmission {
        run_id: run_id(label),
        tenant_id: tenant_id.to_owned(),
        request_id: request_id.to_owned(),
        agent_id: agent_id.to_owned(),
        definition_revision_id: DefinitionRevisionId::new(format!("definition_{label}")).unwrap(),
        deployment_revision_id: DeploymentRevisionId::new(format!("deployment_{label}")).unwrap(),
        conversation,
        input_ref: None,
        input_hash: ContentHash::from_bytes(format!("input:{label}").as_bytes()),
        selected_context_hash: Some(ContentHash::from_bytes(
            format!("context:{label}").as_bytes(),
        )),
        owner: owner.clone(),
        accepted_at: now,
    }
}

fn result(
    run_id: RunId,
    owner: &RuntimeOwner,
    label: &str,
    now: chrono::DateTime<Utc>,
) -> NewTerminalRunResult {
    NewTerminalRunResult {
        run_id,
        owner: owner.clone(),
        terminal_state: TerminalState::Succeeded,
        response_id: format!("response_{label}"),
        output_ref: Some(format!("object://outputs/{label}")),
        output_hash: Some(ContentHash::from_bytes(
            format!("output:{label}").as_bytes(),
        )),
        error_code: None,
        usage_json: Some(json!({"input_tokens": 2, "output_tokens": 3})),
        started_at: now,
        terminal_at: now + Duration::seconds(1),
    }
}

fn message_ref(label: &str, reference: &str, now: chrono::DateTime<Utc>) -> NewConversationMessage {
    NewConversationMessage {
        message_id: format!("message_{label}"),
        content: ConversationContent::Ref(reference.to_owned()),
        content_hash: ContentHash::from_bytes(reference.as_bytes()),
        created_at: now,
    }
}

fn message_inline(
    label: &str,
    value: serde_json::Value,
    now: chrono::DateTime<Utc>,
) -> NewConversationMessage {
    NewConversationMessage {
        message_id: format!("message_{label}"),
        content_hash: ContentHash::from_bytes(serde_json::to_string(&value).unwrap().as_bytes()),
        content: ConversationContent::Inline(value),
        created_at: now,
    }
}

async fn exercise_store<S>(store: &S, label: &str)
where
    S: TerminalRunStore
        + ConversationStore
        + TerminalContentDeletionStore
        + TerminalArtifactStagingStore,
{
    let now = Utc::now();
    let tenant = format!("tenant_{label}");
    let user = format!("user_{label}");
    let agent = format!("agent_{label}");

    let lease_a = lease(&format!("{label}_a"), 1, now);
    let lease_b = lease(&format!("{label}_b"), 1, now);
    let (registered_a, registered_b) = tokio::join!(
        store.register_runtime_instance(lease_a.clone()),
        store.register_runtime_instance(lease_b.clone())
    );
    assert_ne!(
        registered_a.is_ok(),
        registered_b.is_ok(),
        "exactly one runtime instance may own terminal-only admission"
    );
    let first_owner = match (registered_a, registered_b) {
        (Ok(lease), Err(error)) | (Err(error), Ok(lease)) => {
            assert_eq!(error.code(), TERMINAL_RUN_OWNER_LEASE_LOST);
            lease.owner
        }
        _ => unreachable!(),
    };
    let owner_lease = lease(
        first_owner.instance_id.strip_prefix("runtime_").unwrap(),
        2,
        now,
    );
    let owner = store
        .register_runtime_instance(owner_lease.clone())
        .await
        .unwrap()
        .owner;
    assert_eq!(
        store
            .heartbeat_runtime_instance(HeartbeatRuntimeInstance {
                owner: first_owner,
                lease_expires_at: now + Duration::hours(2),
            })
            .await
            .unwrap(),
        OwnerLeaseHeartbeat::Lost
    );
    assert!(matches!(
        store
            .check_runtime_owner(OwnerLeaseQuery {
                owner: owner.clone(),
                observed_at: now,
            })
            .await
            .unwrap(),
        OwnerLeaseStatus::Active { .. }
    ));

    let standalone = admission(
        &format!("{label}_standalone"),
        &tenant,
        &format!("request_{label}_standalone"),
        &agent,
        &owner,
        now,
        None,
    );
    let admitted = store.admit_terminal_run(standalone.clone()).await.unwrap();
    assert!(!admitted.replayed);
    let mut replay = standalone.clone();
    replay.run_id = run_id(&format!("{label}_standalone_retry_generated"));
    replay.accepted_at = now + Duration::seconds(2);
    let replayed = store.admit_terminal_run(replay).await.unwrap();
    assert!(replayed.replayed);
    assert_eq!(replayed.admission.run_id, standalone.run_id);

    let mut mismatched_replay = standalone.clone();
    mismatched_replay.input_hash = ContentHash::from_bytes(b"different request body");
    assert_eq!(
        store
            .admit_terminal_run(mismatched_replay)
            .await
            .unwrap_err()
            .code(),
        REPOSITORY_CONSTRAINT_CONFLICT
    );

    let active = store
        .get_terminal_run(TerminalRunQuery {
            tenant_id: tenant.clone(),
            run_id: standalone.run_id.clone(),
            observed_at: now,
        })
        .await
        .unwrap()
        .unwrap();
    assert_eq!(active.state, TerminalRunDerivedState::Active);
    assert!(store
        .get_terminal_run(TerminalRunQuery {
            tenant_id: format!("{tenant}_other"),
            run_id: standalone.run_id.clone(),
            observed_at: now,
        })
        .await
        .unwrap()
        .is_none());

    let standalone_result = result(
        standalone.run_id.clone(),
        &owner,
        &format!("{label}_standalone"),
        now,
    );
    let mut missing_output_ref = standalone_result.clone();
    missing_output_ref.output_ref = None;
    assert_eq!(
        store
            .commit_terminal_result(missing_output_ref)
            .await
            .unwrap_err()
            .code(),
        REPOSITORY_DATA_INVALID
    );
    let mut missing_output_hash = standalone_result.clone();
    missing_output_hash.output_hash = None;
    assert_eq!(
        store
            .commit_terminal_result(missing_output_hash)
            .await
            .unwrap_err()
            .code(),
        REPOSITORY_DATA_INVALID
    );
    assert!(
        !store
            .commit_terminal_result(standalone_result.clone())
            .await
            .unwrap()
            .replayed
    );
    assert!(
        store
            .commit_terminal_result(standalone_result.clone())
            .await
            .unwrap()
            .replayed
    );
    let mut conflicting_result = standalone_result.clone();
    conflicting_result.response_id.push_str("_different");
    assert_eq!(
        store
            .commit_terminal_result(conflicting_result)
            .await
            .unwrap_err()
            .code(),
        REPOSITORY_CONSTRAINT_CONFLICT
    );

    let interrupted_admission = admission(
        &format!("{label}_interrupted"),
        &tenant,
        &format!("request_{label}_interrupted"),
        &agent,
        &owner,
        now,
        None,
    );
    store
        .admit_terminal_run(interrupted_admission.clone())
        .await
        .unwrap();
    assert!(store
        .unregister_runtime_instance(owner.clone())
        .await
        .unwrap());
    assert_eq!(
        store
            .get_terminal_run(TerminalRunQuery {
                tenant_id: tenant.clone(),
                run_id: interrupted_admission.run_id.clone(),
                observed_at: now,
            })
            .await
            .unwrap()
            .unwrap()
            .state,
        TerminalRunDerivedState::Interrupted
    );
    assert_eq!(
        store
            .commit_terminal_result(result(
                interrupted_admission.run_id,
                &owner,
                &format!("{label}_late"),
                now,
            ))
            .await
            .unwrap_err()
            .code(),
        TERMINAL_RUN_OWNER_LEASE_LOST
    );

    let owner = store
        .register_runtime_instance(lease(
            owner.instance_id.strip_prefix("runtime_").unwrap(),
            owner.owner_epoch + 1,
            now,
        ))
        .await
        .unwrap()
        .owner;

    let conversation_id = format!("conversation_{label}");
    let conversation = NewConversation {
        conversation_id: conversation_id.clone(),
        tenant_id: tenant.clone(),
        user_id: user.clone(),
        agent_id: agent.clone(),
        persistence_mode: PersistenceMode::TerminalOnly,
        deployment_revision_id: DeploymentRevisionId::new(format!("deployment_{label}")).unwrap(),
        created_at: now,
    };
    assert!(
        !store
            .create_conversation(conversation.clone())
            .await
            .unwrap()
            .replayed
    );
    assert!(
        store
            .create_conversation(conversation.clone())
            .await
            .unwrap()
            .replayed
    );
    let query = ConversationQuery {
        conversation_id: conversation_id.clone(),
        tenant_id: tenant.clone(),
        user_id: user.clone(),
    };
    assert!(store
        .get_conversation(query.clone())
        .await
        .unwrap()
        .is_some());
    assert!(store
        .get_conversation(ConversationQuery {
            tenant_id: format!("{tenant}_other"),
            ..query.clone()
        })
        .await
        .unwrap()
        .is_none());
    assert!(store
        .get_conversation(ConversationQuery {
            user_id: format!("{user}_other"),
            ..query.clone()
        })
        .await
        .unwrap()
        .is_none());
    let summary_claim = ClaimConversationSummaryJob {
        conversation: query.clone(),
        claim_token: format!("summary_claim_{label}"),
        claimed_by: format!("summary_worker_{label}"),
        claim_expires_at: now + Duration::minutes(10),
        created_at: now,
    };
    assert!(store
        .try_claim_conversation_summary_job(summary_claim.clone())
        .await
        .unwrap());
    assert!(
        store
            .try_claim_conversation_summary_job(summary_claim.clone())
            .await
            .unwrap(),
        "an exact summary claim retry must be idempotent"
    );
    let mut competing_summary_claim = summary_claim.clone();
    competing_summary_claim.claim_token = format!("summary_claim_competing_{label}");
    competing_summary_claim.claimed_by = format!("summary_worker_competing_{label}");
    assert!(
        !store
            .try_claim_conversation_summary_job(competing_summary_claim)
            .await
            .unwrap(),
        "an unexpired summary claim must coalesce work across runtime instances"
    );
    assert!(!store
        .release_conversation_summary_job(ReleaseConversationSummaryJob {
            conversation_id: conversation_id.clone(),
            claim_token: summary_claim.claim_token.clone(),
            claimed_by: format!("wrong_summary_worker_{label}"),
        })
        .await
        .unwrap());
    assert!(store
        .release_conversation_summary_job(ReleaseConversationSummaryJob {
            conversation_id: conversation_id.clone(),
            claim_token: summary_claim.claim_token,
            claimed_by: summary_claim.claimed_by,
        })
        .await
        .unwrap());
    let expired_summary_claim = ClaimConversationSummaryJob {
        conversation: query.clone(),
        claim_token: format!("summary_claim_expired_{label}"),
        claimed_by: format!("summary_worker_expired_{label}"),
        claim_expires_at: now - Duration::minutes(1),
        created_at: now - Duration::minutes(2),
    };
    assert!(store
        .try_claim_conversation_summary_job(expired_summary_claim.clone())
        .await
        .unwrap());
    let takeover_summary_claim = ClaimConversationSummaryJob {
        conversation: query.clone(),
        claim_token: format!("summary_claim_takeover_{label}"),
        claimed_by: format!("summary_worker_takeover_{label}"),
        claim_expires_at: now + Duration::minutes(10),
        created_at: now,
    };
    assert!(
        store
            .try_claim_conversation_summary_job(takeover_summary_claim.clone())
            .await
            .unwrap(),
        "an expired summary claim must be atomically claimable by a new runtime"
    );
    assert!(
        !store
            .release_conversation_summary_job(ReleaseConversationSummaryJob {
                conversation_id: conversation_id.clone(),
                claim_token: expired_summary_claim.claim_token,
                claimed_by: expired_summary_claim.claimed_by,
            })
            .await
            .unwrap(),
        "a superseded summary worker must not release the new runtime's claim"
    );
    assert!(store
        .release_conversation_summary_job(ReleaseConversationSummaryJob {
            conversation_id: conversation_id.clone(),
            claim_token: takeover_summary_claim.claim_token,
            claimed_by: takeover_summary_claim.claimed_by,
        })
        .await
        .unwrap());

    let first_user = message_ref(
        &format!("{label}_user_1"),
        &format!("object://messages/{label}/user-1"),
        now,
    );
    let first_admission = admission(
        &format!("{label}_turn_1"),
        &tenant,
        &format!("request_{label}_turn_1"),
        &agent,
        &owner,
        now,
        Some(AdmissionConversation {
            conversation_id: conversation_id.clone(),
            user_message_id: first_user.message_id.clone(),
        }),
    );
    let first_turn = NewConversationTurn {
        user_id: user.clone(),
        message: first_user.clone(),
        admission: first_admission.clone(),
    };
    let first_turn_outcome = store
        .create_conversation_turn(first_turn.clone())
        .await
        .unwrap();
    assert!(!first_turn_outcome.replayed);
    assert_eq!(first_turn_outcome.user_message.role, ConversationRole::User);

    let mut replay_turn = first_turn.clone();
    replay_turn.message.message_id = format!("message_{label}_retry_generated");
    replay_turn.admission.run_id = run_id(&format!("{label}_turn_1_retry_generated"));
    replay_turn.admission.input_ref = Some(format!("object://recomputed-input/{label}/turn-1"));
    replay_turn.admission.input_hash = ContentHash::from_bytes(b"recomputed prompt input");
    replay_turn.admission.selected_context_hash =
        Some(ContentHash::from_bytes(b"recomputed conversation context"));
    replay_turn
        .admission
        .conversation
        .as_mut()
        .unwrap()
        .user_message_id = replay_turn.message.message_id.clone();
    let replayed_turn = store.create_conversation_turn(replay_turn).await.unwrap();
    assert!(replayed_turn.replayed);
    assert_eq!(replayed_turn.user_message.message_id, first_user.message_id);

    let mut mismatched_turn = first_turn.clone();
    mismatched_turn.message.content = ConversationContent::Inline(json!({"different": true}));
    mismatched_turn.message.content_hash = ContentHash::from_bytes(b"different");
    assert_eq!(
        store
            .create_conversation_turn(mismatched_turn)
            .await
            .unwrap_err()
            .code(),
        REPOSITORY_CONSTRAINT_CONFLICT
    );

    let wrong_agent_admission = admission(
        &format!("{label}_wrong_agent"),
        &tenant,
        &format!("request_{label}_wrong_agent"),
        &format!("{agent}_other"),
        &owner,
        now,
        Some(AdmissionConversation {
            conversation_id: conversation_id.clone(),
            user_message_id: format!("message_{label}_wrong_agent"),
        }),
    );
    assert_eq!(
        store
            .create_conversation_turn(NewConversationTurn {
                user_id: user.clone(),
                message: message_inline(
                    &format!("{label}_wrong_agent"),
                    json!("wrong agent"),
                    now,
                ),
                admission: wrong_agent_admission.clone(),
            })
            .await
            .unwrap_err()
            .code(),
        CONVERSATION_OWNERSHIP_MISMATCH
    );
    assert!(store
        .get_terminal_run(TerminalRunQuery {
            tenant_id: tenant.clone(),
            run_id: wrong_agent_admission.run_id,
            observed_at: now,
        })
        .await
        .unwrap()
        .is_none());

    let second_admission = admission(
        &format!("{label}_turn_2"),
        &tenant,
        &format!("request_{label}_turn_2"),
        &agent,
        &owner,
        now + Duration::seconds(2),
        Some(AdmissionConversation {
            conversation_id: conversation_id.clone(),
            user_message_id: first_user.message_id.clone(),
        }),
    );
    let atomic_message_failure = store
        .create_conversation_turn(NewConversationTurn {
            user_id: user.clone(),
            message: NewConversationMessage {
                message_id: first_user.message_id.clone(),
                content: ConversationContent::Inline(json!("second user")),
                content_hash: ContentHash::from_bytes(b"second user"),
                created_at: now + Duration::seconds(2),
            },
            admission: second_admission.clone(),
        })
        .await
        .unwrap_err();
    assert_eq!(
        atomic_message_failure.code(),
        REPOSITORY_CONSTRAINT_CONFLICT
    );
    assert!(store
        .get_terminal_run(TerminalRunQuery {
            tenant_id: tenant.clone(),
            run_id: second_admission.run_id.clone(),
            observed_at: now,
        })
        .await
        .unwrap()
        .is_none());

    let second_user = message_inline(
        &format!("{label}_user_2"),
        json!("second user"),
        now + Duration::seconds(2),
    );
    let mut second_admission = second_admission;
    second_admission
        .conversation
        .as_mut()
        .unwrap()
        .user_message_id = second_user.message_id.clone();
    let second_turn = store
        .create_conversation_turn(NewConversationTurn {
            user_id: user.clone(),
            message: second_user.clone(),
            admission: second_admission.clone(),
        })
        .await
        .unwrap();

    let first_assistant = message_inline(
        &format!("{label}_assistant_1"),
        json!({"answer": 1}),
        now + Duration::seconds(1),
    );
    let first_commit = CommitConversationTurn {
        result: result(
            first_admission.run_id.clone(),
            &owner,
            &format!("{label}_turn_1"),
            now,
        ),
        assistant_message: first_assistant.clone(),
    };
    let committed_first = store
        .commit_conversation_turn(first_commit.clone())
        .await
        .unwrap();
    assert!(!committed_first.replayed);
    assert_eq!(
        committed_first.assistant_message.role,
        ConversationRole::Assistant
    );
    let mut replay_commit = first_commit.clone();
    replay_commit.assistant_message.message_id =
        format!("message_{label}_assistant_retry_generated");
    assert!(
        store
            .commit_conversation_turn(replay_commit)
            .await
            .unwrap()
            .replayed
    );
    let mut mismatched_commit = first_commit;
    mismatched_commit.assistant_message.content = ConversationContent::Inline(json!("different"));
    mismatched_commit.assistant_message.content_hash = ContentHash::from_bytes(b"different");
    assert_eq!(
        store
            .commit_conversation_turn(mismatched_commit)
            .await
            .unwrap_err()
            .code(),
        REPOSITORY_CONSTRAINT_CONFLICT
    );

    let second_result = result(
        second_admission.run_id.clone(),
        &owner,
        &format!("{label}_turn_2"),
        now + Duration::seconds(2),
    );
    let result_atomic_failure = store
        .commit_conversation_turn(CommitConversationTurn {
            result: second_result.clone(),
            assistant_message: NewConversationMessage {
                message_id: first_assistant.message_id.clone(),
                content: ConversationContent::Inline(json!({"answer": 2})),
                content_hash: ContentHash::from_bytes(b"answer 2"),
                created_at: now + Duration::seconds(3),
            },
        })
        .await
        .unwrap_err();
    assert_eq!(result_atomic_failure.code(), REPOSITORY_CONSTRAINT_CONFLICT);
    let still_active = store
        .get_terminal_run(TerminalRunQuery {
            tenant_id: tenant.clone(),
            run_id: second_admission.run_id.clone(),
            observed_at: now,
        })
        .await
        .unwrap()
        .unwrap();
    assert!(still_active.result.is_none());
    assert_eq!(still_active.state, TerminalRunDerivedState::Active);
    let second_assistant = message_inline(
        &format!("{label}_assistant_2"),
        json!({"answer": 2}),
        now + Duration::seconds(3),
    );
    let committed_second = store
        .commit_conversation_turn(CommitConversationTurn {
            result: second_result,
            assistant_message: second_assistant,
        })
        .await
        .unwrap();

    let mut cursor = None;
    let mut message_ids = Vec::new();
    loop {
        let page = store
            .page_conversation_messages(MessagePageQuery {
                conversation: query.clone(),
                before: cursor,
                limit: 2,
            })
            .await
            .unwrap()
            .unwrap();
        message_ids.extend(
            page.messages
                .iter()
                .map(|message| message.message_id.clone()),
        );
        let Some(next) = page.next_cursor else {
            break;
        };
        cursor = Some(next);
    }
    assert_eq!(message_ids.len(), 4);
    assert_eq!(
        message_ids.iter().collect::<BTreeSet<_>>().len(),
        message_ids.len(),
        "cursor pagination must not duplicate messages"
    );

    let summary = store
        .put_conversation_summary(NewConversationSummary {
            conversation: query.clone(),
            through_message_order: first_turn_outcome.user_message.message_order,
            summary_ref: format!("object://summaries/{label}/1"),
            summary_hash: ContentHash::from_bytes(b"summary one"),
            model_revision: "summary-model-v1".to_owned(),
            created_at: now + Duration::seconds(4),
        })
        .await
        .unwrap();
    assert!(!summary.replayed);
    let context = store
        .load_conversation_context(ConversationContextQuery {
            conversation: query.clone(),
            recent_message_limit: 20,
        })
        .await
        .unwrap()
        .unwrap();
    assert_eq!(context.summary, Some(summary.summary.clone()));
    assert_eq!(
        context
            .messages
            .iter()
            .map(|message| message.message_id.as_str())
            .collect::<Vec<_>>(),
        vec![
            second_turn.user_message.message_id.as_str(),
            committed_first.assistant_message.message_id.as_str(),
            committed_second.assistant_message.message_id.as_str()
        ]
    );

    assert!(matches!(
        store
            .archive_conversation(query.clone(), now + Duration::minutes(1))
            .await
            .unwrap(),
        ArchiveOutcome::Archived { changed: true, .. }
    ));
    assert!(matches!(
        store
            .archive_conversation(query.clone(), now + Duration::minutes(2))
            .await
            .unwrap(),
        ArchiveOutcome::Archived { changed: false, .. }
    ));
    let archived_turn_message = message_inline(
        &format!("{label}_archived"),
        json!("archived"),
        now + Duration::minutes(2),
    );
    let archived_admission = admission(
        &format!("{label}_archived"),
        &tenant,
        &format!("request_{label}_archived"),
        &agent,
        &owner,
        now + Duration::minutes(2),
        Some(AdmissionConversation {
            conversation_id: conversation_id.clone(),
            user_message_id: archived_turn_message.message_id.clone(),
        }),
    );
    assert!(store
        .create_conversation_turn(NewConversationTurn {
            user_id: user.clone(),
            message: archived_turn_message,
            admission: archived_admission,
        })
        .await
        .is_err());

    let deleted = store.delete_conversation(query.clone()).await.unwrap();
    let PrivacyDeleteOutcome::Deleted { content } = deleted else {
        panic!("conversation must be privacy-deleted");
    };
    assert!(
        store.create_conversation(conversation).await.is_err(),
        "a privacy-deleted deterministic Conversation id must be permanently tombstoned"
    );
    assert!(
        store
            .get_conversation(query.clone())
            .await
            .unwrap()
            .is_none(),
        "privacy deletion must physically remove Conversation ownership metadata"
    );
    assert_eq!(
        content.content_refs,
        vec![
            format!("object://messages/{label}/user-1"),
            format!("object://outputs/{label}_turn_1"),
            format!("object://outputs/{label}_turn_2"),
        ]
    );
    assert_eq!(
        content.summary_refs,
        vec![format!("object://summaries/{label}/1")]
    );
    let privacy_claims = store
        .claim_content_deletion_jobs(ClaimContentDeletionJobs {
            claimed_by: format!("privacy_worker_{label}"),
            observed_at: now + Duration::hours(1),
            claim_expires_at: now + Duration::hours(2),
            limit: 100,
        })
        .await
        .unwrap();
    assert_eq!(privacy_claims.len(), 4);
    assert!(privacy_claims.iter().all(|claim| {
        claim.job.source_kind == ContentDeletionSourceKind::ConversationPrivacy
            && claim.job.source_id == conversation_id
            && claim.job.attempts == 1
    }));
    assert_eq!(
        privacy_claims
            .iter()
            .map(|claim| claim.job.content_ref.clone())
            .collect::<BTreeSet<_>>(),
        [
            format!("object://messages/{label}/user-1"),
            format!("object://outputs/{label}_turn_1"),
            format!("object://outputs/{label}_turn_2"),
            format!("object://summaries/{label}/1"),
        ]
        .into_iter()
        .collect(),
    );
    let unacked_privacy_claim = privacy_claims[0].clone();
    for claim in privacy_claims.iter().skip(1) {
        assert!(store
            .ack_content_deletion_job(AckContentDeletionJob {
                deletion_job_id: claim.job.deletion_job_id.clone(),
                claim_token: claim.claim_token.clone(),
            })
            .await
            .unwrap());
    }
    assert!(
        store
            .claim_content_deletion_jobs(ClaimContentDeletionJobs {
                claimed_by: format!("early_worker_{label}"),
                observed_at: now + Duration::minutes(90),
                claim_expires_at: now + Duration::minutes(110),
                limit: 100,
            })
            .await
            .unwrap()
            .is_empty(),
        "an unexpired content-deletion claim must not be stolen"
    );
    let reclaimed_privacy = store
        .claim_content_deletion_jobs(ClaimContentDeletionJobs {
            claimed_by: format!("retry_worker_{label}"),
            observed_at: now + Duration::hours(3),
            claim_expires_at: now + Duration::hours(4),
            limit: 100,
        })
        .await
        .unwrap();
    assert_eq!(reclaimed_privacy.len(), 1);
    assert_eq!(
        reclaimed_privacy[0].job.deletion_job_id,
        unacked_privacy_claim.job.deletion_job_id
    );
    assert_eq!(reclaimed_privacy[0].job.attempts, 2);
    assert_ne!(
        reclaimed_privacy[0].claim_token,
        unacked_privacy_claim.claim_token
    );
    assert!(
        !store
            .ack_content_deletion_job(AckContentDeletionJob {
                deletion_job_id: unacked_privacy_claim.job.deletion_job_id,
                claim_token: unacked_privacy_claim.claim_token,
            })
            .await
            .unwrap(),
        "a stale claim token must not acknowledge a reclaimed job"
    );
    assert!(store
        .ack_content_deletion_job(AckContentDeletionJob {
            deletion_job_id: reclaimed_privacy[0].job.deletion_job_id.clone(),
            claim_token: reclaimed_privacy[0].claim_token.clone(),
        })
        .await
        .unwrap());
    assert!(
        !store
            .ack_content_deletion_job(AckContentDeletionJob {
                deletion_job_id: reclaimed_privacy[0].job.deletion_job_id.clone(),
                claim_token: reclaimed_privacy[0].claim_token.clone(),
            })
            .await
            .unwrap(),
        "acknowledgement is idempotent after the job is gone"
    );
    assert!(store
        .get_conversation(query.clone())
        .await
        .unwrap()
        .is_none());
    assert!(store
        .page_conversation_messages(MessagePageQuery {
            conversation: query.clone(),
            before: None,
            limit: 50,
        })
        .await
        .unwrap()
        .is_none());
    assert!(matches!(
        store.delete_conversation(query).await.unwrap(),
        PrivacyDeleteOutcome::NotFound
    ));
    let retained_terminal = store
        .get_terminal_run(TerminalRunQuery {
            tenant_id: tenant.clone(),
            run_id: first_admission.run_id,
            observed_at: now,
        })
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        retained_terminal.state,
        TerminalRunDerivedState::Succeeded,
        "privacy deletion must not remove the terminal result authority"
    );
    assert!(
        retained_terminal
            .result
            .as_ref()
            .is_some_and(|result| result.output_ref.is_none() && result.output_hash.is_none()),
        "privacy-deleted Conversation output content must not remain observable via Run GET"
    );

    let active_retention_query = ConversationQuery {
        conversation_id: format!("conversation_{label}_retention_active_terminal"),
        tenant_id: tenant.clone(),
        user_id: user.clone(),
    };
    store
        .create_conversation(NewConversation {
            conversation_id: active_retention_query.conversation_id.clone(),
            tenant_id: tenant.clone(),
            user_id: user.clone(),
            agent_id: agent.clone(),
            persistence_mode: PersistenceMode::TerminalOnly,
            deployment_revision_id: DeploymentRevisionId::new(format!("deployment_{label}"))
                .unwrap(),
            created_at: now - Duration::hours(5),
        })
        .await
        .unwrap();
    let active_retention_user = message_inline(
        &format!("{label}_retention_active_terminal_user"),
        json!({"active": true}),
        now - Duration::hours(2),
    );
    let active_retention_admission = admission(
        &format!("{label}_retention_active_terminal_turn"),
        &tenant,
        &format!("request_{label}_retention_active_terminal_turn"),
        &agent,
        &owner,
        now - Duration::hours(2),
        Some(AdmissionConversation {
            conversation_id: active_retention_query.conversation_id.clone(),
            user_message_id: active_retention_user.message_id.clone(),
        }),
    );
    store
        .create_conversation_turn(NewConversationTurn {
            user_id: user.clone(),
            admission: active_retention_admission.clone(),
            message: active_retention_user,
        })
        .await
        .unwrap();
    let active_retention = store
        .delete_conversations_before(BoundedRetention {
            before: now - Duration::hours(1),
            limit: 100,
        })
        .await
        .unwrap();
    assert_eq!(
        active_retention.deleted, 0,
        "retention must not delete a Conversation while its terminal owner lease is active"
    );
    assert!(store
        .get_conversation(active_retention_query.clone())
        .await
        .unwrap()
        .is_some());

    let mut active_retention_result = result(
        active_retention_admission.run_id.clone(),
        &owner,
        &format!("{label}_retention_active_terminal_turn"),
        now - Duration::hours(2),
    );
    active_retention_result.output_ref = None;
    active_retention_result.output_hash = None;
    let active_retention_commit = store
        .commit_conversation_turn(CommitConversationTurn {
            result: active_retention_result,
            assistant_message: message_inline(
                &format!("{label}_retention_active_terminal_assistant"),
                json!({"committed": true}),
                now - Duration::hours(2) + Duration::seconds(1),
            ),
        })
        .await
        .unwrap();
    assert!(
        !active_retention_commit.replayed,
        "the terminal commit must retain a live Conversation FK target"
    );
    let completed_retention = store
        .delete_conversations_before(BoundedRetention {
            before: now - Duration::hours(1),
            limit: 100,
        })
        .await
        .unwrap();
    assert_eq!(
        completed_retention.deleted, 1,
        "a terminal Conversation becomes eligible after its result commits"
    );
    assert!(completed_retention.content_refs.is_empty());
    assert!(completed_retention.summary_refs.is_empty());

    let expired_unarchived_query = ConversationQuery {
        conversation_id: format!("conversation_{label}_retention_unarchived"),
        tenant_id: tenant.clone(),
        user_id: user.clone(),
    };
    let expired_archived_query = ConversationQuery {
        conversation_id: format!("conversation_{label}_retention_archived"),
        tenant_id: tenant.clone(),
        user_id: user.clone(),
    };
    let fresh_archived_query = ConversationQuery {
        conversation_id: format!("conversation_{label}_retention_fresh"),
        tenant_id: tenant.clone(),
        user_id: user.clone(),
    };
    for (query, created_at) in [
        (&expired_unarchived_query, now - Duration::hours(4)),
        (&expired_archived_query, now - Duration::hours(3)),
        (&fresh_archived_query, now - Duration::minutes(30)),
    ] {
        store
            .create_conversation(NewConversation {
                conversation_id: query.conversation_id.clone(),
                tenant_id: tenant.clone(),
                user_id: user.clone(),
                agent_id: agent.clone(),
                persistence_mode: PersistenceMode::TerminalOnly,
                deployment_revision_id: DeploymentRevisionId::new(format!("deployment_{label}"))
                    .unwrap(),
                created_at,
            })
            .await
            .unwrap();
    }
    assert!(matches!(
        store
            .archive_conversation(expired_archived_query.clone(), now - Duration::hours(2))
            .await
            .unwrap(),
        ArchiveOutcome::Archived { changed: true, .. }
    ));
    assert!(matches!(
        store
            .archive_conversation(fresh_archived_query.clone(), now - Duration::minutes(10))
            .await
            .unwrap(),
        ArchiveOutcome::Archived { changed: true, .. }
    ));

    let retention_user = message_ref(
        &format!("{label}_retention_user"),
        &format!("object://messages/{label}/retention-user"),
        now - Duration::hours(2),
    );
    let retention_admission = admission(
        &format!("{label}_retention_turn"),
        &tenant,
        &format!("request_{label}_retention_turn"),
        &agent,
        &owner,
        now - Duration::hours(2),
        Some(AdmissionConversation {
            conversation_id: expired_unarchived_query.conversation_id.clone(),
            user_message_id: retention_user.message_id.clone(),
        }),
    );
    let retention_turn = store
        .create_conversation_turn(NewConversationTurn {
            user_id: user.clone(),
            admission: retention_admission.clone(),
            message: retention_user,
        })
        .await
        .unwrap();
    store
        .commit_conversation_turn(CommitConversationTurn {
            result: result(
                retention_admission.run_id.clone(),
                &owner,
                &format!("{label}_retention_turn"),
                now - Duration::hours(2),
            ),
            assistant_message: message_inline(
                &format!("{label}_retention_assistant"),
                json!({"retained": true}),
                now - Duration::hours(2) + Duration::seconds(1),
            ),
        })
        .await
        .unwrap();
    store
        .put_conversation_summary(NewConversationSummary {
            conversation: expired_unarchived_query.clone(),
            through_message_order: retention_turn.user_message.message_order,
            summary_ref: format!("object://summaries/{label}/retention"),
            summary_hash: ContentHash::from_bytes(b"retention summary"),
            model_revision: "summary-model-v1".to_owned(),
            created_at: now - Duration::hours(2) + Duration::seconds(2),
        })
        .await
        .unwrap();

    let first_retention = store
        .delete_conversations_before(BoundedRetention {
            before: now - Duration::hours(1),
            limit: 1,
        })
        .await
        .unwrap();
    assert_eq!(first_retention.deleted, 1);
    assert_eq!(
        first_retention
            .content_refs
            .into_iter()
            .collect::<BTreeSet<_>>(),
        [
            format!("object://messages/{label}/retention-user"),
            format!("object://outputs/{label}_retention_turn"),
        ]
        .into_iter()
        .collect()
    );
    assert_eq!(
        first_retention.summary_refs,
        vec![format!("object://summaries/{label}/retention")]
    );
    assert!(
        store
            .get_conversation(expired_unarchived_query.clone())
            .await
            .unwrap()
            .is_none(),
        "created_at expiry must apply even when a Conversation was never archived"
    );
    assert!(
        store
            .get_conversation(expired_archived_query.clone())
            .await
            .unwrap()
            .is_some(),
        "a batch limit of one must leave the next expired Conversation untouched"
    );
    assert!(
        store
            .get_conversation(fresh_archived_query.clone())
            .await
            .unwrap()
            .is_some(),
        "archive age must not override the independent created_at cutoff"
    );
    let retained_conversation_run = store
        .get_terminal_run(TerminalRunQuery {
            tenant_id: tenant.clone(),
            run_id: retention_admission.run_id.clone(),
            observed_at: now,
        })
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        retained_conversation_run.state,
        TerminalRunDerivedState::Succeeded
    );
    assert!(
        retained_conversation_run
            .result
            .as_ref()
            .is_some_and(|result| result.output_ref.is_none() && result.output_hash.is_none()),
        "Conversation retention must mask retained terminal output authority"
    );

    let second_retention = store
        .delete_conversations_before(BoundedRetention {
            before: now - Duration::hours(1),
            limit: 1,
        })
        .await
        .unwrap();
    assert_eq!(second_retention.deleted, 1);
    assert!(second_retention.content_refs.is_empty());
    assert!(second_retention.summary_refs.is_empty());
    assert!(store
        .get_conversation(expired_archived_query)
        .await
        .unwrap()
        .is_none());
    let exhausted_retention = store
        .delete_conversations_before(BoundedRetention {
            before: now - Duration::hours(1),
            limit: 1,
        })
        .await
        .unwrap();
    assert_eq!(exhausted_retention.deleted, 0);
    assert!(
        store
            .get_conversation(fresh_archived_query)
            .await
            .unwrap()
            .is_some(),
        "fresh Conversations must survive regardless of archive state"
    );
    let conversation_retention_claims = store
        .claim_content_deletion_jobs(ClaimContentDeletionJobs {
            claimed_by: format!("conversation_retention_worker_{label}"),
            observed_at: now + Duration::hours(5),
            claim_expires_at: now + Duration::hours(6),
            limit: 100,
        })
        .await
        .unwrap();
    assert_eq!(conversation_retention_claims.len(), 3);
    assert!(conversation_retention_claims.iter().all(|claim| {
        claim.job.source_kind == ContentDeletionSourceKind::ConversationRetention
            && claim.job.source_id == expired_unarchived_query.conversation_id
            && claim.job.attempts == 1
    }));
    assert_eq!(
        conversation_retention_claims
            .iter()
            .map(|claim| claim.job.content_ref.clone())
            .collect::<BTreeSet<_>>(),
        [
            format!("object://messages/{label}/retention-user"),
            format!("object://outputs/{label}_retention_turn"),
            format!("object://summaries/{label}/retention"),
        ]
        .into_iter()
        .collect(),
        "message, summary and linked terminal output deletion must be queued atomically"
    );
    for claim in conversation_retention_claims {
        assert!(store
            .ack_content_deletion_job(AckContentDeletionJob {
                deletion_job_id: claim.job.deletion_job_id,
                claim_token: claim.claim_token,
            })
            .await
            .unwrap());
    }

    let mut retained_output_refs = Vec::new();
    loop {
        let outcome = store
            .delete_terminal_runs_before(BoundedRetention {
                before: now + Duration::hours(2),
                limit: 1,
            })
            .await
            .unwrap();
        assert!(outcome.deleted <= 1, "retention must honor its batch limit");
        assert!(outcome.input_refs.is_empty());
        retained_output_refs.extend(outcome.output_refs);
        if outcome.deleted == 0 {
            break;
        }
    }
    assert_eq!(
        retained_output_refs.into_iter().collect::<BTreeSet<_>>(),
        [
            format!("object://outputs/{label}_standalone"),
            format!("object://outputs/{label}_retention_turn"),
            format!("object://outputs/{label}_turn_1"),
            format!("object://outputs/{label}_turn_2"),
        ]
        .into_iter()
        .collect(),
        "object-store output refs must be returned before bounded row deletion"
    );
    let run_retention_claims = store
        .claim_content_deletion_jobs(ClaimContentDeletionJobs {
            claimed_by: format!("run_retention_worker_{label}"),
            observed_at: now + Duration::hours(7),
            claim_expires_at: now + Duration::hours(8),
            limit: 100,
        })
        .await
        .unwrap();
    assert_eq!(run_retention_claims.len(), 4);
    assert!(run_retention_claims.iter().all(|claim| {
        claim.job.source_kind == ContentDeletionSourceKind::TerminalRunRetention
            && claim.job.attempts == 1
    }));
    assert_eq!(
        run_retention_claims
            .iter()
            .map(|claim| claim.job.content_ref.clone())
            .collect::<BTreeSet<_>>(),
        [
            format!("object://outputs/{label}_standalone"),
            format!("object://outputs/{label}_retention_turn"),
            format!("object://outputs/{label}_turn_1"),
            format!("object://outputs/{label}_turn_2"),
        ]
        .into_iter()
        .collect(),
    );
    for claim in run_retention_claims {
        assert!(store
            .ack_content_deletion_job(AckContentDeletionJob {
                deletion_job_id: claim.job.deletion_job_id,
                claim_token: claim.claim_token,
            })
            .await
            .unwrap());
    }

    let (orphan_ref, orphan_hash) = scoped_artifact_ref(&format!("{label}:orphan"));
    let orphan_command = NewTerminalArtifactStage {
        tenant_id: tenant.clone(),
        content_ref: orphan_ref.clone(),
        content_hash: orphan_hash.clone(),
        source_kind: TerminalArtifactSourceKind::UserMessage,
        source_id: format!("orphan_message_{label}"),
        available_at: now,
        created_at: now,
    };
    assert!(
        !store
            .stage_terminal_artifact(orphan_command.clone())
            .await
            .unwrap()
            .replayed
    );
    let exact_stage_replay = store
        .stage_terminal_artifact(orphan_command.clone())
        .await
        .unwrap();
    assert!(exact_stage_replay.replayed);
    let mut drifted_stage_replay = orphan_command.clone();
    drifted_stage_replay.created_at = now + Duration::minutes(1);
    drifted_stage_replay.available_at = now + Duration::minutes(2);
    let drifted_stage_replay = store
        .stage_terminal_artifact(drifted_stage_replay)
        .await
        .unwrap();
    assert!(
        drifted_stage_replay.replayed,
        "producer retry timestamps must not change staging identity"
    );
    assert_eq!(
        drifted_stage_replay.stage.available_at,
        exact_stage_replay.stage.available_at
    );
    assert_eq!(
        drifted_stage_replay.stage.created_at, exact_stage_replay.stage.created_at,
        "the first successful staging timestamps remain authoritative"
    );
    let mut cross_tenant = orphan_command.clone();
    cross_tenant.tenant_id = format!("{tenant}_other");
    cross_tenant.source_id = format!("other_orphan_message_{label}");
    assert_eq!(
        store
            .stage_terminal_artifact(cross_tenant)
            .await
            .unwrap_err()
            .code(),
        REPOSITORY_CONSTRAINT_CONFLICT,
        "one scoped object must never be staged across tenant authorities"
    );
    let orphan_claims = store
        .claim_terminal_artifact_stages(ClaimTerminalArtifactStages {
            claimed_by: format!("orphan_worker_{label}"),
            observed_at: now + Duration::seconds(1),
            claim_expires_at: now + Duration::minutes(1),
            limit: 1,
        })
        .await
        .unwrap();
    assert_eq!(orphan_claims.len(), 1);
    let orphan_claim = &orphan_claims[0];
    assert_eq!(orphan_claim.stage.content_ref, orphan_ref);
    assert_eq!(orphan_claim.stage.attempts, 1);
    assert_eq!(
        store
            .resolve_terminal_artifact_stage(ResolveTerminalArtifactStage {
                staging_id: orphan_claim.stage.staging_id.clone(),
                claim_token: orphan_claim.claim_token.clone(),
            })
            .await
            .unwrap(),
        TerminalArtifactStageDisposition::DeleteOrphan
    );
    assert!(
        store.stage_terminal_artifact(orphan_command).await.is_err(),
        "a producer cannot race a claimed orphan collector"
    );
    assert!(store
        .claim_terminal_artifact_stages(ClaimTerminalArtifactStages {
            claimed_by: format!("early_orphan_worker_{label}"),
            observed_at: now + Duration::seconds(30),
            claim_expires_at: now + Duration::seconds(45),
            limit: 1,
        })
        .await
        .unwrap()
        .is_empty());
    let reclaimed_orphan = store
        .claim_terminal_artifact_stages(ClaimTerminalArtifactStages {
            claimed_by: format!("retry_orphan_worker_{label}"),
            observed_at: now + Duration::minutes(2),
            claim_expires_at: now + Duration::minutes(3),
            limit: 1,
        })
        .await
        .unwrap();
    assert_eq!(reclaimed_orphan.len(), 1);
    assert_eq!(
        reclaimed_orphan[0].stage.staging_id,
        orphan_claim.stage.staging_id
    );
    assert_eq!(reclaimed_orphan[0].stage.attempts, 2);
    assert_eq!(
        store
            .resolve_terminal_artifact_stage(ResolveTerminalArtifactStage {
                staging_id: reclaimed_orphan[0].stage.staging_id.clone(),
                claim_token: reclaimed_orphan[0].claim_token.clone(),
            })
            .await
            .unwrap(),
        TerminalArtifactStageDisposition::DeleteOrphan
    );
    assert!(!store
        .ack_terminal_artifact_stage(AckTerminalArtifactStage {
            staging_id: orphan_claim.stage.staging_id.clone(),
            claim_token: orphan_claim.claim_token.clone(),
        })
        .await
        .unwrap());
    assert!(store
        .ack_terminal_artifact_stage(AckTerminalArtifactStage {
            staging_id: reclaimed_orphan[0].stage.staging_id.clone(),
            claim_token: reclaimed_orphan[0].claim_token.clone(),
        })
        .await
        .unwrap());

    let staged_conversation_id = format!("conversation_{label}_staged_authority");
    let staged_query = ConversationQuery {
        conversation_id: staged_conversation_id.clone(),
        tenant_id: tenant.clone(),
        user_id: user.clone(),
    };
    store
        .create_conversation(NewConversation {
            conversation_id: staged_conversation_id.clone(),
            tenant_id: tenant.clone(),
            user_id: user.clone(),
            agent_id: agent.clone(),
            persistence_mode: PersistenceMode::TerminalOnly,
            deployment_revision_id: DeploymentRevisionId::new(format!("deployment_{label}"))
                .unwrap(),
            created_at: now,
        })
        .await
        .unwrap();
    let staged_user_id = format!("message_{label}_staged_user");
    let (staged_user_ref, staged_user_hash) = scoped_artifact_ref(&format!("{label}:staged-user"));
    stage_artifact(
        store,
        &tenant,
        &staged_user_ref,
        &staged_user_hash,
        TerminalArtifactSourceKind::UserMessage,
        &staged_user_id,
        now,
    )
    .await;
    let staged_admission = admission(
        &format!("{label}_staged_turn"),
        &tenant,
        &format!("request_{label}_staged_turn"),
        &agent,
        &owner,
        now,
        Some(AdmissionConversation {
            conversation_id: staged_conversation_id,
            user_message_id: staged_user_id.clone(),
        }),
    );
    let staged_turn = store
        .create_conversation_turn(NewConversationTurn {
            user_id: user.clone(),
            message: NewConversationMessage {
                message_id: staged_user_id,
                content: ConversationContent::Ref(staged_user_ref),
                content_hash: ContentHash::from_bytes(b"staged user value"),
                created_at: now,
            },
            admission: staged_admission.clone(),
        })
        .await
        .unwrap();

    let (staged_output_ref, staged_output_hash) =
        scoped_artifact_ref(&format!("{label}:staged-output"));
    stage_artifact(
        store,
        &tenant,
        &staged_output_ref,
        &staged_output_hash,
        TerminalArtifactSourceKind::RunOutput,
        staged_admission.run_id.as_str(),
        now,
    )
    .await;
    let staged_assistant_id = format!("message_{label}_staged_assistant");
    let (staged_assistant_ref, staged_assistant_hash) =
        scoped_artifact_ref(&format!("{label}:staged-assistant"));
    stage_artifact(
        store,
        &tenant,
        &staged_assistant_ref,
        &staged_assistant_hash,
        TerminalArtifactSourceKind::AssistantMessage,
        &staged_assistant_id,
        now,
    )
    .await;
    let mut staged_result = result(
        staged_admission.run_id.clone(),
        &owner,
        &format!("{label}_staged_turn"),
        now,
    );
    staged_result.output_ref = Some(staged_output_ref);
    staged_result.output_hash = Some(staged_output_hash);
    store
        .commit_conversation_turn(CommitConversationTurn {
            result: staged_result,
            assistant_message: NewConversationMessage {
                message_id: staged_assistant_id,
                content: ConversationContent::Ref(staged_assistant_ref),
                content_hash: ContentHash::from_bytes(b"staged assistant value"),
                created_at: now + Duration::seconds(1),
            },
        })
        .await
        .unwrap();

    let staged_summary_source = format!(
        "{}:{}",
        staged_query.conversation_id, staged_turn.user_message.message_order
    );
    let (staged_summary_ref, staged_summary_hash) =
        scoped_artifact_ref(&format!("{label}:staged-summary"));
    stage_artifact(
        store,
        &tenant,
        &staged_summary_ref,
        &staged_summary_hash,
        TerminalArtifactSourceKind::ConversationSummary,
        &staged_summary_source,
        now,
    )
    .await;
    store
        .put_conversation_summary(NewConversationSummary {
            conversation: staged_query,
            through_message_order: staged_turn.user_message.message_order,
            summary_ref: staged_summary_ref.clone(),
            summary_hash: staged_summary_hash.clone(),
            model_revision: "staging-contract-v1".to_owned(),
            created_at: now + Duration::seconds(2),
        })
        .await
        .unwrap();
    stage_artifact(
        store,
        &tenant,
        &staged_summary_ref,
        &staged_summary_hash,
        TerminalArtifactSourceKind::ConversationSummary,
        &staged_summary_source,
        now,
    )
    .await;
    let authoritative_claim = store
        .claim_terminal_artifact_stages(ClaimTerminalArtifactStages {
            claimed_by: format!("authoritative_worker_{label}"),
            observed_at: now + Duration::hours(1),
            claim_expires_at: now + Duration::hours(2),
            limit: 100,
        })
        .await
        .unwrap();
    assert_eq!(authoritative_claim.len(), 1);
    assert_eq!(
        store
            .resolve_terminal_artifact_stage(ResolveTerminalArtifactStage {
                staging_id: authoritative_claim[0].stage.staging_id.clone(),
                claim_token: authoritative_claim[0].claim_token.clone(),
            })
            .await
            .unwrap(),
        TerminalArtifactStageDisposition::Authoritative,
        "ambiguous post-commit staging must preserve a referenced object"
    );
    assert!(
        store
            .claim_terminal_artifact_stages(ClaimTerminalArtifactStages {
                claimed_by: format!("post_commit_worker_{label}"),
                observed_at: now + Duration::hours(1),
                claim_expires_at: now + Duration::hours(2),
                limit: 100,
            })
            .await
            .unwrap()
            .is_empty(),
        "user, assistant/output and summary staging rows must be consumed by metadata commits"
    );
}

async fn exercise_conversation_tombstone_races<S>(store: S, label: &str)
where
    S: ConversationStore + Clone + Send + Sync + 'static,
{
    let now = Utc::now();
    for (suffix, retention) in [("privacy", false), ("retention", true)] {
        let conversation = NewConversation {
            conversation_id: format!("conversation_tombstone_race_{label}_{suffix}"),
            tenant_id: format!("tenant_tombstone_race_{label}"),
            user_id: format!("user_tombstone_race_{label}"),
            agent_id: format!("agent_tombstone_race_{label}"),
            persistence_mode: PersistenceMode::TerminalOnly,
            deployment_revision_id: DeploymentRevisionId::new(format!(
                "deployment_tombstone_race_{label}"
            ))
            .unwrap(),
            created_at: now - Duration::hours(2),
        };
        store
            .create_conversation(conversation.clone())
            .await
            .unwrap();
        let query = ConversationQuery {
            conversation_id: conversation.conversation_id.clone(),
            tenant_id: conversation.tenant_id.clone(),
            user_id: conversation.user_id.clone(),
        };
        let create_store = store.clone();
        let create = conversation.clone();
        let create_task =
            tokio::spawn(async move { create_store.create_conversation(create).await });
        let delete_store = store.clone();
        let query_for_delete = query.clone();
        let delete_task = tokio::spawn(async move {
            if retention {
                delete_store
                    .delete_conversations_before(BoundedRetention {
                        before: now + Duration::hours(1),
                        limit: 1,
                    })
                    .await
                    .map(|outcome| outcome.deleted)
            } else {
                delete_store
                    .delete_conversation(query_for_delete)
                    .await
                    .map(|outcome| {
                        u64::from(matches!(outcome, PrivacyDeleteOutcome::Deleted { .. }))
                    })
            }
        });
        let (_create_result, deleted) = tokio::join!(create_task, delete_task);
        assert_eq!(deleted.unwrap().unwrap(), 1);
        assert!(store.get_conversation(query).await.unwrap().is_none());
        assert!(
            store.create_conversation(conversation).await.is_err(),
            "{suffix} deletion must win permanently over same-id creation"
        );
    }
}

#[tokio::test]
async fn sqlite_terminal_and_conversation_contract() {
    let (_temporary, repository) = support::temporary_sqlite_repository().await;
    exercise_store(&repository, &Uuid::new_v4().simple().to_string()).await;
    exercise_conversation_tombstone_races(repository, &Uuid::new_v4().simple().to_string()).await;
}

#[tokio::test]
async fn sqlite_expired_conversation_commit_cannot_reverse_interrupted() {
    let temporary = tempfile::tempdir().unwrap();
    let database = temporary.path().join("owner-expiry-race.sqlite3");
    support::provision_sqlite_database(&database).await;
    let repository_pool = SqlitePoolOptions::new()
        .max_connections(1)
        .min_connections(1)
        .connect_with(
            SqliteConnectOptions::new()
                .filename(&database)
                .create_if_missing(false)
                .foreign_keys(true)
                .busy_timeout(StdDuration::from_secs(2)),
        )
        .await
        .unwrap();
    let repository = SqliteDurableRepository::from_pool(repository_pool)
        .await
        .unwrap();
    let now = Utc::now();
    let mut short_lease = lease("sqlite_expiry_race", 1, now);
    short_lease.lease_expires_at = now + Duration::milliseconds(350);
    let owner = repository
        .register_runtime_instance(short_lease.clone())
        .await
        .unwrap()
        .owner;
    let query = ConversationQuery {
        conversation_id: "conversation_sqlite_expiry_race".to_owned(),
        tenant_id: "tenant_sqlite_expiry_race".to_owned(),
        user_id: "user_sqlite_expiry_race".to_owned(),
    };
    repository
        .create_conversation(NewConversation {
            conversation_id: query.conversation_id.clone(),
            tenant_id: query.tenant_id.clone(),
            user_id: query.user_id.clone(),
            agent_id: "agent_sqlite_expiry_race".to_owned(),
            persistence_mode: PersistenceMode::TerminalOnly,
            deployment_revision_id: DeploymentRevisionId::new("deployment_sqlite_expiry_race")
                .unwrap(),
            created_at: now,
        })
        .await
        .unwrap();
    let user_message = message_inline("sqlite_expiry_race_user", json!({"value": "user"}), now);
    let command = admission(
        "sqlite_expiry_race",
        &query.tenant_id,
        "request_sqlite_expiry_race",
        "agent_sqlite_expiry_race",
        &owner,
        now,
        Some(AdmissionConversation {
            conversation_id: query.conversation_id.clone(),
            user_message_id: user_message.message_id.clone(),
        }),
    );
    repository
        .create_conversation_turn(NewConversationTurn {
            user_id: query.user_id.clone(),
            admission: command.clone(),
            message: user_message,
        })
        .await
        .unwrap();
    let mut terminal_result = result(command.run_id.clone(), &owner, "sqlite_expiry_race", now);
    terminal_result.output_ref = None;
    terminal_result.output_hash = None;
    let terminal_command = CommitConversationTurn {
        result: terminal_result,
        assistant_message: message_inline(
            "sqlite_expiry_race_assistant",
            json!({"value": "assistant"}),
            now + Duration::milliseconds(1),
        ),
    };

    let blocker_pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(
            SqliteConnectOptions::new()
                .filename(&database)
                .create_if_missing(false)
                .foreign_keys(true),
        )
        .await
        .unwrap();
    let mut blocker = blocker_pool.acquire().await.unwrap();
    sqlx::query("BEGIN IMMEDIATE")
        .execute(&mut *blocker)
        .await
        .unwrap();

    let commit_repository = repository.clone();
    let mut commit_task = tokio::spawn(async move {
        commit_repository
            .commit_conversation_turn(terminal_command)
            .await
    });
    tokio::time::sleep(StdDuration::from_millis(50)).await;
    assert!(
        !commit_task.is_finished(),
        "the fixture must block terminal commit before its owner check"
    );
    sleep_past(short_lease.lease_expires_at).await;

    let get_repository = repository.clone();
    let run_id = command.run_id.clone();
    let tenant_id = query.tenant_id.clone();
    let mut get_task = tokio::spawn(async move {
        get_repository
            .get_terminal_run(TerminalRunQuery {
                tenant_id,
                run_id,
                observed_at: Utc::now(),
            })
            .await
    });
    assert!(
        tokio::time::timeout(StdDuration::from_millis(50), &mut get_task)
            .await
            .is_err(),
        "GET must wait behind the SQLite terminal writer"
    );
    sqlx::query("COMMIT").execute(&mut *blocker).await.unwrap();
    let commit_error = tokio::time::timeout(StdDuration::from_secs(2), &mut commit_task)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert_eq!(commit_error.code(), TERMINAL_RUN_OWNER_LEASE_LOST);
    let view = get_task.await.unwrap().unwrap().unwrap();
    assert_eq!(view.state, TerminalRunDerivedState::Interrupted);
    assert!(view.result.is_none());
    let messages = repository
        .page_conversation_messages(MessagePageQuery {
            conversation: query,
            before: None,
            limit: 10,
        })
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        messages.messages.len(),
        1,
        "the expired terminal transaction must roll back its assistant message"
    );
    drop(blocker);
    blocker_pool.close().await;
}

#[tokio::test]
async fn sqlite_repository_open_clears_owner_and_derives_interrupted() {
    let temporary = tempfile::tempdir().unwrap();
    let database = temporary.path().join("owner-reset.sqlite3");
    support::provision_sqlite_database(&database).await;
    let repository = SqliteDurableRepository::connect_path(&database)
        .await
        .unwrap();
    let now = Utc::now();
    let owner = repository
        .register_runtime_instance(lease("sqlite_reset", 1, now))
        .await
        .unwrap()
        .owner;
    let command = admission(
        "sqlite_reset",
        "tenant_reset",
        "request_reset",
        "agent_reset",
        &owner,
        now,
        None,
    );
    repository
        .admit_terminal_run(command.clone())
        .await
        .unwrap();
    drop(repository);

    let restarted = SqliteDurableRepository::connect_path(&database)
        .await
        .unwrap();
    assert_eq!(
        restarted
            .get_terminal_run(TerminalRunQuery {
                tenant_id: command.tenant_id,
                run_id: command.run_id,
                observed_at: now,
            })
            .await
            .unwrap()
            .unwrap()
            .state,
        TerminalRunDerivedState::Interrupted
    );
}

#[tokio::test]
async fn sqlite_terminal_staging_preserves_a_full_runtime_artifact_authority() {
    let temporary = tempfile::tempdir().unwrap();
    let database = temporary.path().join("full-authority.sqlite3");
    support::provision_sqlite_database(&database).await;
    let repository = SqliteDurableRepository::connect_path(&database)
        .await
        .unwrap();
    let now = Utc::now();
    let (content_ref, content_hash) = scoped_artifact_ref("shared-with-full-authority");
    stage_artifact(
        &repository,
        "tenant-terminal",
        &content_ref,
        &content_hash,
        TerminalArtifactSourceKind::RunOutput,
        "run_terminal_stage",
        now,
    )
    .await;

    // Inject a representative full-runtime Artifact authority through a
    // fixture-only connection. The production full repository establishes
    // the workflow Run FK before this row; disabling FK checks here keeps this
    // test focused on the cross-runtime content-hash fence.
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(
            SqliteConnectOptions::new()
                .filename(&database)
                .create_if_missing(false)
                .foreign_keys(false),
        )
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO artifacts (
             run_id,artifact_id,content_hash,size_bytes,media_type,storage_uri,
             artifact_state,created_at
         ) VALUES (?,?,?,?,?,?,'staged',?)",
    )
    .bind("run_full_authority")
    .bind("artifact_full_authority")
    .bind(content_hash.as_str())
    .bind(1_i64)
    .bind("application/octet-stream")
    .bind("content-addressed:v1/sha256/full-authority")
    .bind(now)
    .execute(&pool)
    .await
    .unwrap();
    pool.close().await;

    let claim = repository
        .claim_terminal_artifact_stages(ClaimTerminalArtifactStages {
            claimed_by: "full-authority-check".to_owned(),
            observed_at: now + Duration::seconds(1),
            claim_expires_at: now + Duration::minutes(1),
            limit: 1,
        })
        .await
        .unwrap();
    assert_eq!(claim.len(), 1);
    assert_eq!(
        repository
            .resolve_terminal_artifact_stage(ResolveTerminalArtifactStage {
                staging_id: claim[0].stage.staging_id.clone(),
                claim_token: claim[0].claim_token.clone(),
            })
            .await
            .unwrap(),
        TerminalArtifactStageDisposition::Authoritative,
        "terminal orphan GC must not delete bytes owned by the full runtime"
    );
}

struct IsolatedPostgresSchema {
    admin: PgPool,
    control: PgPool,
    scoped_url: String,
    schema: String,
}

fn postgres_test_url() -> Option<String> {
    match std::env::var("TEST_POSTGRES_URL") {
        Ok(value) => Some(value),
        Err(error) if std::env::var_os("CI").is_some() => {
            panic!("CI must set TEST_POSTGRES_URL for terminal store contract tests: {error}")
        }
        Err(_) => None,
    }
}

async fn isolated_postgres_schema() -> Option<IsolatedPostgresSchema> {
    let database_url = postgres_test_url()?;
    let schema = format!("terminal_store_{}", Uuid::new_v4().simple());
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
        .max_connections(8)
        .connect(&scoped_url)
        .await
        .unwrap();
    Some(IsolatedPostgresSchema {
        admin,
        control,
        scoped_url,
        schema,
    })
}

async fn cleanup_postgres(schema: IsolatedPostgresSchema) {
    schema.control.close().await;
    sqlx::query(AssertSqlSafe(format!(
        "DROP SCHEMA {} CASCADE",
        schema.schema
    )))
    .execute(&schema.admin)
    .await
    .unwrap();
    schema.admin.close().await;
}

async fn wait_for_postgres_lock_wait(pool: &PgPool, query_fragment: &str) {
    let pattern = format!("%{query_fragment}%");
    tokio::time::timeout(StdDuration::from_secs(2), async {
        loop {
            let waiting = sqlx::query_scalar::<_, bool>(
                "SELECT EXISTS(
                     SELECT 1
                     FROM pg_stat_activity
                     WHERE pid<>pg_backend_pid()
                       AND datname=current_database()
                       AND wait_event_type='Lock'
                       AND query LIKE $1
                 )",
            )
            .bind(&pattern)
            .fetch_one(pool)
            .await
            .unwrap();
            if waiting {
                break;
            }
            tokio::time::sleep(StdDuration::from_millis(5)).await;
        }
    })
    .await
    .expect("terminal commit did not reach the expected PostgreSQL lock wait");
}

#[tokio::test]
async fn postgres_terminal_and_conversation_contract() {
    let Some(schema) = isolated_postgres_schema().await else {
        return;
    };
    support::provision_postgres_schema(&schema.control).await;
    let repository = PostgresDurableRepository::connect(&schema.scoped_url)
        .await
        .unwrap();
    exercise_store(&repository, &Uuid::new_v4().simple().to_string()).await;
    exercise_conversation_tombstone_races(repository.clone(), &Uuid::new_v4().simple().to_string())
        .await;
    drop(repository);
    cleanup_postgres(schema).await;
}

#[tokio::test]
async fn postgres_expired_terminal_commit_cannot_reverse_interrupted() {
    let Some(schema) = isolated_postgres_schema().await else {
        return;
    };
    support::provision_postgres_schema(&schema.control).await;
    let repository = PostgresDurableRepository::connect(&schema.scoped_url)
        .await
        .unwrap();
    let now = Utc::now();
    let mut short_lease = lease("postgres_expiry_race", 1, now);
    short_lease.lease_expires_at = now + Duration::milliseconds(450);
    let owner = repository
        .register_runtime_instance(short_lease.clone())
        .await
        .unwrap()
        .owner;
    let command = admission(
        "postgres_expiry_race",
        "tenant_postgres_expiry_race",
        "request_postgres_expiry_race",
        "agent_postgres_expiry_race",
        &owner,
        now,
        None,
    );
    repository
        .admit_terminal_run(command.clone())
        .await
        .unwrap();
    let (output_ref, output_hash) = scoped_artifact_ref("postgres-expiry-race-output");
    stage_artifact(
        &repository,
        &command.tenant_id,
        &output_ref,
        &output_hash,
        TerminalArtifactSourceKind::RunOutput,
        command.run_id.as_str(),
        now,
    )
    .await;
    let mut terminal_result = result(command.run_id.clone(), &owner, "postgres_expiry_race", now);
    terminal_result.output_ref = Some(output_ref.clone());
    terminal_result.output_hash = Some(output_hash);

    let mut blocker = schema.control.begin().await.unwrap();
    sqlx::query(
        "SELECT staging_id
         FROM terminal_artifact_staging
         WHERE content_ref=$1
         FOR UPDATE",
    )
    .bind(&output_ref)
    .fetch_one(&mut *blocker)
    .await
    .unwrap();

    let commit_repository = repository.clone();
    let mut commit_task = tokio::spawn(async move {
        commit_repository
            .commit_terminal_result(terminal_result)
            .await
    });
    wait_for_postgres_lock_wait(&schema.control, "DELETE FROM terminal_artifact_staging").await;
    sleep_past(short_lease.lease_expires_at).await;

    let get_repository = repository.clone();
    let tenant_id = command.tenant_id.clone();
    let run_id = command.run_id.clone();
    let mut get_task = tokio::spawn(async move {
        get_repository
            .get_terminal_run(TerminalRunQuery {
                tenant_id,
                run_id,
                observed_at: Utc::now(),
            })
            .await
    });
    assert!(
        tokio::time::timeout(StdDuration::from_millis(50), &mut get_task)
            .await
            .is_err(),
        "GET must wait on the commit's exclusive admission lock"
    );
    blocker.commit().await.unwrap();

    let commit_error = tokio::time::timeout(StdDuration::from_secs(2), &mut commit_task)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert_eq!(commit_error.code(), TERMINAL_RUN_OWNER_LEASE_LOST);
    let view = get_task.await.unwrap().unwrap().unwrap();
    assert_eq!(view.state, TerminalRunDerivedState::Interrupted);
    assert!(view.result.is_none());

    let success_now = Utc::now();
    let success_lease = lease("postgres_commit_visibility", 1, success_now);
    let success_owner = repository
        .register_runtime_instance(success_lease)
        .await
        .unwrap()
        .owner;
    let success_command = admission(
        "postgres_commit_visibility",
        "tenant_postgres_expiry_race",
        "request_postgres_commit_visibility",
        "agent_postgres_expiry_race",
        &success_owner,
        success_now,
        None,
    );
    repository
        .admit_terminal_run(success_command.clone())
        .await
        .unwrap();
    let (success_output_ref, success_output_hash) =
        scoped_artifact_ref("postgres-commit-visibility-output");
    stage_artifact(
        &repository,
        &success_command.tenant_id,
        &success_output_ref,
        &success_output_hash,
        TerminalArtifactSourceKind::RunOutput,
        success_command.run_id.as_str(),
        success_now,
    )
    .await;
    let mut success_result = result(
        success_command.run_id.clone(),
        &success_owner,
        "postgres_commit_visibility",
        success_now,
    );
    success_result.output_ref = Some(success_output_ref.clone());
    success_result.output_hash = Some(success_output_hash);

    let mut success_blocker = schema.control.begin().await.unwrap();
    sqlx::query(
        "SELECT staging_id
         FROM terminal_artifact_staging
         WHERE content_ref=$1
         FOR UPDATE",
    )
    .bind(&success_output_ref)
    .fetch_one(&mut *success_blocker)
    .await
    .unwrap();
    let success_commit_repository = repository.clone();
    let success_commit_task = tokio::spawn(async move {
        success_commit_repository
            .commit_terminal_result(success_result)
            .await
    });
    wait_for_postgres_lock_wait(&schema.control, "DELETE FROM terminal_artifact_staging").await;
    let success_get_repository = repository.clone();
    let success_tenant_id = success_command.tenant_id.clone();
    let success_run_id = success_command.run_id.clone();
    let mut success_get_task = tokio::spawn(async move {
        success_get_repository
            .get_terminal_run(TerminalRunQuery {
                tenant_id: success_tenant_id,
                run_id: success_run_id,
                observed_at: Utc::now(),
            })
            .await
    });
    assert!(
        tokio::time::timeout(StdDuration::from_millis(50), &mut success_get_task)
            .await
            .is_err(),
        "GET must wait for an in-flight successful terminal commit"
    );
    success_blocker.commit().await.unwrap();
    let success_commit = success_commit_task.await.unwrap().unwrap();
    assert!(!success_commit.replayed);
    let success_view = success_get_task.await.unwrap().unwrap().unwrap();
    assert_eq!(
        success_view.state,
        TerminalRunDerivedState::Succeeded,
        "GET must take a fresh result snapshot after acquiring the admission lock"
    );
    assert!(success_view.result.is_some());

    let rolled_back_stage = repository
        .claim_terminal_artifact_stages(ClaimTerminalArtifactStages {
            claimed_by: "postgres_expiry_race_gc".to_owned(),
            observed_at: Utc::now(),
            claim_expires_at: Utc::now() + Duration::minutes(1),
            limit: 1,
        })
        .await
        .unwrap();
    assert_eq!(rolled_back_stage.len(), 1);
    assert_eq!(rolled_back_stage[0].stage.content_ref, output_ref);

    drop(repository);
    cleanup_postgres(schema).await;
}
