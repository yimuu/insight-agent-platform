use insight_engine::control::{
    BranchDecision, ChildRequirement, ControlFrame, ControlLedger, ForkGroup, ForkLeg, MergeState,
    OwnedControlToken, PersistedControlTokenRow, ScopeTracker, CONTROL_HANDLE_FOREIGN,
    CONTROL_INTENT_CONFLICT, CONTROL_RUN_MISMATCH, FORK_ATOMIC_ADMISSION_CONFLICT,
    MERGE_CORRELATION_MISMATCH,
};
use insight_engine::{
    ActivationId, ForkGroupId, LegId, PortId, RunId, ScopeInstanceId, TransitionKey,
    TransitionOutcome,
};

fn run(value: &str) -> RunId {
    RunId::new(value).unwrap()
}

fn activation(value: &str) -> ActivationId {
    ActivationId::new(value).unwrap()
}

fn port(value: &str) -> PortId {
    PortId::new(value).unwrap()
}

fn leg(value: &str) -> LegId {
    LegId::new(value).unwrap()
}

fn scope(value: &str) -> ScopeInstanceId {
    ScopeInstanceId::new(value).unwrap()
}

fn key(domain: &str, part: &str) -> TransitionKey {
    TransitionKey::derive(domain, &[part]).unwrap()
}

fn committed<T: Clone>(outcome: &TransitionOutcome<T>) -> T {
    outcome.committed_result().unwrap().clone()
}

fn branch(run_id: &RunId, activation_id: &str, scope_id: &str, ports: &[&str]) -> BranchDecision {
    BranchDecision::new(
        run_id.clone(),
        activation(activation_id),
        scope(scope_id),
        ports.iter().map(|value| port(value)).collect(),
    )
    .unwrap()
}

fn fork_members(run_id: &RunId) -> Vec<ForkLeg> {
    vec![
        ForkLeg::new(
            run_id.clone(),
            leg("beta"),
            port("beta"),
            scope("scope_beta"),
            activation("activation_beta"),
            ChildRequirement::Required,
        ),
        ForkLeg::new(
            run_id.clone(),
            leg("alpha"),
            port("alpha"),
            scope("scope_alpha"),
            activation("activation_alpha"),
            ChildRequirement::Optional,
        ),
        ForkLeg::new(
            run_id.clone(),
            leg("gamma"),
            port("gamma"),
            scope("scope_gamma"),
            activation("activation_gamma"),
            ChildRequirement::Required,
        ),
    ]
}

#[test]
fn persisted_rows_are_inert_until_the_owning_ledger_loads_a_handle() {
    let run_id = run("run_serde_row");
    let mut ledger = ControlLedger::new(run_id.clone());
    let mut source = branch(
        &run_id,
        "activation_source",
        "scope_root",
        &["then", "else"],
    );
    let original = committed(
        &source
            .select_and_emit(
                &mut ledger,
                key("branch.emit", "source"),
                port("then"),
                None,
            )
            .unwrap(),
    );

    let encoded = serde_json::to_value(ledger.row(&original).unwrap()).unwrap();
    let decoded: PersistedControlTokenRow = serde_json::from_value(encoded).unwrap();
    assert_eq!(decoded.run_id(), &run_id);
    assert_eq!(decoded.token_id(), original.token_id());

    // A persisted row is data, not an execution capability. There is no public
    // row-to-handle constructor; even another ledger for the same run rejects
    // the old authority-bound handle.
    let restored = ControlLedger::new(run_id);
    assert_eq!(
        restored.row(&original).unwrap_err().code(),
        CONTROL_HANDLE_FOREIGN
    );
}

#[test]
fn branch_emission_is_first_winner_and_exact_replay_returns_the_first_token() {
    let run_id = run("run_branch_winner");
    let mut ledger = ControlLedger::new(run_id.clone());
    let mut decision = branch(
        &run_id,
        "activation_branch",
        "scope_root",
        &["then", "else"],
    );
    let transition = key("branch.emit", "winner");

    let first = decision
        .select_and_emit(&mut ledger, transition.clone(), port("then"), None)
        .unwrap();
    assert!(matches!(first, TransitionOutcome::Committed { .. }));
    let first_token = committed(&first);

    let replay = decision
        .select_and_emit(&mut ledger, transition.clone(), port("then"), None)
        .unwrap();
    assert!(matches!(replay, TransitionOutcome::ExactReplay { .. }));
    assert_eq!(committed(&replay).token_id(), first_token.token_id());

    assert_eq!(
        decision
            .select_and_emit(&mut ledger, transition, port("else"), None)
            .unwrap_err()
            .code(),
        CONTROL_INTENT_CONFLICT
    );
    assert_eq!(decision.selected_port(), Some(&port("then")));
    assert_eq!(ledger.len(), 1);
}

#[test]
fn aggregates_and_handles_are_run_scoped() {
    let run_a = run("run_a");
    let run_b = run("run_b");
    let mut ledger_b = ControlLedger::new(run_b);
    let mut branch_a = branch(&run_a, "activation_branch", "scope_root", &["then", "else"]);
    assert_eq!(
        branch_a
            .select_and_emit(
                &mut ledger_b,
                key("branch.emit", "cross_run"),
                port("then"),
                None,
            )
            .unwrap_err()
            .code(),
        CONTROL_RUN_MISMATCH
    );
    assert!(ledger_b.is_empty());
}

#[test]
fn one_token_has_one_first_winner_consumer() {
    let run_id = run("run_single_consumer");
    let mut ledger = ControlLedger::new(run_id.clone());
    let mut source = branch(
        &run_id,
        "activation_branch",
        "scope_root",
        &["then", "else"],
    );
    let branch_token = committed(
        &source
            .select_and_emit(
                &mut ledger,
                key("branch.emit", "single_consumer"),
                port("then"),
                None,
            )
            .unwrap(),
    );

    let mut merge_a = MergeState::new(
        run_id.clone(),
        activation("activation_merge_a"),
        port("merged_a"),
        activation("activation_branch"),
        scope("scope_root"),
        vec![port("then"), port("else")],
    )
    .unwrap();
    assert!(matches!(
        merge_a
            .arrive_and_emit(
                &mut ledger,
                key("merge.consume", "consumer_a"),
                &branch_token,
            )
            .unwrap(),
        TransitionOutcome::Committed { .. }
    ));
    let consumption = ledger.consumption(&branch_token).unwrap().unwrap();
    assert_eq!(consumption.run_id(), &run_id);
    assert_eq!(consumption.consumer_kind(), "merge");
    assert_eq!(
        consumption.consumer_activation_id(),
        &activation("activation_merge_a")
    );

    let mut merge_b = MergeState::new(
        run_id,
        activation("activation_merge_b"),
        port("merged_b"),
        activation("activation_branch"),
        scope("scope_root"),
        vec![port("then"), port("else")],
    )
    .unwrap();
    assert!(matches!(
        merge_b
            .arrive_and_emit(
                &mut ledger,
                key("merge.consume", "consumer_b"),
                &branch_token,
            )
            .unwrap(),
        TransitionOutcome::StateConflict
    ));
    assert!(merge_b.arrival().is_none());
}

#[test]
fn same_transition_key_with_a_changed_authoritative_input_is_an_error() {
    let run_id = run("run_intent_hash");
    let mut ledger = ControlLedger::new(run_id.clone());
    let mut source_a = branch(&run_id, "activation_source_a", "scope_root", &["a"]);
    let token_a = committed(
        &source_a
            .select_and_emit(&mut ledger, key("branch.emit", "source_a"), port("a"), None)
            .unwrap(),
    );
    let mut source_b = branch(&run_id, "activation_source_b", "scope_root", &["b"]);
    let token_b = committed(
        &source_b
            .select_and_emit(&mut ledger, key("branch.emit", "source_b"), port("b"), None)
            .unwrap(),
    );

    let transition = key("branch.emit", "nested");
    let mut nested = branch(&run_id, "activation_nested", "scope_root", &["chosen"]);
    assert!(matches!(
        nested
            .select_and_emit(
                &mut ledger,
                transition.clone(),
                port("chosen"),
                Some(&token_a),
            )
            .unwrap(),
        TransitionOutcome::Committed { .. }
    ));

    // Reconstructing the same aggregate identity does not weaken the ledger:
    // the alternate source row and frame stack change the canonical intent.
    let mut reconstructed = branch(&run_id, "activation_nested", "scope_root", &["chosen"]);
    assert_eq!(
        reconstructed
            .select_and_emit(&mut ledger, transition, port("chosen"), Some(&token_b),)
            .unwrap_err()
            .code(),
        CONTROL_INTENT_CONFLICT
    );
    assert!(ledger.consumption(&token_b).unwrap().is_none());
}

#[test]
fn fork_creation_atomically_admits_every_member_and_mints_only_their_first_tokens() {
    let run_id = run("run_fork_atomic");
    let mut ledger = ControlLedger::new(run_id.clone());
    let mut parent = ScopeTracker::new(run_id.clone(), scope("scope_parent"));
    let transition = key("fork.create", "atomic");

    let applied = ForkGroup::create(
        run_id.clone(),
        ForkGroupId::new("fork_atomic").unwrap(),
        activation("activation_fork"),
        scope("scope_parent"),
        fork_members(&run_id),
        &mut ledger,
        &mut parent,
        transition.clone(),
        None,
    )
    .unwrap();
    assert!(matches!(applied, TransitionOutcome::Committed { .. }));
    let creation = committed(&applied);
    assert_eq!(creation.run_id(), &run_id);
    assert_eq!(parent.child_count(), 3);
    assert_eq!(ledger.len(), 3);
    assert_eq!(creation.tokens().len(), 3);
    assert_eq!(
        creation
            .group()
            .members()
            .iter()
            .map(|member| member.leg_id().as_str())
            .collect::<Vec<_>>(),
        vec!["beta", "alpha", "gamma"]
    );

    let replay = ForkGroup::create(
        run_id.clone(),
        ForkGroupId::new("fork_atomic").unwrap(),
        activation("activation_fork"),
        scope("scope_parent"),
        fork_members(&run_id),
        &mut ledger,
        &mut parent,
        key("fork.create", "atomic_after_restart"),
        None,
    )
    .unwrap();
    assert!(matches!(replay, TransitionOutcome::ExactReplay { .. }));
    assert_eq!(parent.child_count(), 3);
    assert_eq!(ledger.len(), 3);
    assert_eq!(
        committed(&replay)
            .tokens()
            .iter()
            .map(|entry| entry.token().token_id().clone())
            .collect::<Vec<_>>(),
        creation
            .tokens()
            .iter()
            .map(|entry| entry.token().token_id().clone())
            .collect::<Vec<_>>()
    );

    let mut changed_members = fork_members(&run_id);
    changed_members[0] = ForkLeg::new(
        run_id.clone(),
        leg("beta"),
        port("changed"),
        scope("scope_beta"),
        activation("activation_beta"),
        ChildRequirement::Required,
    );
    assert_eq!(
        ForkGroup::create(
            run_id,
            ForkGroupId::new("fork_atomic").unwrap(),
            activation("activation_fork"),
            scope("scope_parent"),
            changed_members,
            &mut ledger,
            &mut parent,
            transition,
            None,
        )
        .unwrap_err()
        .code(),
        CONTROL_INTENT_CONFLICT
    );
    assert_eq!(parent.child_count(), 3);
    assert_eq!(ledger.len(), 3);
}

#[test]
fn fork_rejects_pre_admitted_or_cross_run_members_without_partial_rows() {
    let run_id = run("run_fork_reject");
    let mut ledger = ControlLedger::new(run_id.clone());
    let mut parent = ScopeTracker::new(run_id.clone(), scope("scope_parent"));
    parent
        .admit_child(
            activation("activation_beta"),
            scope("scope_beta"),
            ChildRequirement::Required,
        )
        .unwrap();
    assert_eq!(
        ForkGroup::create(
            run_id.clone(),
            ForkGroupId::new("fork_pre_admitted").unwrap(),
            activation("activation_fork"),
            scope("scope_parent"),
            fork_members(&run_id),
            &mut ledger,
            &mut parent,
            key("fork.create", "pre_admitted"),
            None,
        )
        .unwrap_err()
        .code(),
        FORK_ATOMIC_ADMISSION_CONFLICT
    );
    assert!(ledger.is_empty());
    assert_eq!(parent.child_count(), 1);

    let mut clean_parent = ScopeTracker::new(run_id.clone(), scope("scope_clean"));
    let mut wrong_members = fork_members(&run_id);
    wrong_members[1] = ForkLeg::new(
        run("run_other"),
        leg("alpha"),
        port("alpha"),
        scope("scope_alpha"),
        activation("activation_alpha"),
        ChildRequirement::Optional,
    );
    assert_eq!(
        ForkGroup::create(
            run_id,
            ForkGroupId::new("fork_cross_run").unwrap(),
            activation("activation_fork_cross"),
            scope("scope_clean"),
            wrong_members,
            &mut ledger,
            &mut clean_parent,
            key("fork.create", "cross_run"),
            None,
        )
        .unwrap_err()
        .code(),
        CONTROL_RUN_MISMATCH
    );
    assert!(ledger.is_empty());
    assert_eq!(clean_parent.child_count(), 0);
}

#[test]
fn unified_frames_enforce_true_lifo_for_branch_and_fork_interleavings() {
    let run_id = run("run_lifo");
    let mut ledger = ControlLedger::new(run_id.clone());
    let mut outer_branch = branch(
        &run_id,
        "activation_outer_branch",
        "scope_parent",
        &["then", "else"],
    );
    let outer = committed(
        &outer_branch
            .select_and_emit(&mut ledger, key("branch.emit", "outer"), port("then"), None)
            .unwrap(),
    );
    let mut parent = ScopeTracker::new(run_id.clone(), scope("scope_parent"));
    let creation = committed(
        &ForkGroup::create(
            run_id.clone(),
            ForkGroupId::new("fork_nested").unwrap(),
            activation("activation_fork"),
            scope("scope_parent"),
            fork_members(&run_id),
            &mut ledger,
            &mut parent,
            key("fork.create", "nested"),
            Some(&outer),
        )
        .unwrap(),
    );
    let beta = creation.token(&leg("beta")).unwrap();
    let row = ledger.row(beta).unwrap();
    assert!(matches!(
        row.provenance().frames(),
        [ControlFrame::Branch(_), ControlFrame::ForkLeg(_)]
    ));

    // The Branch frame exists, but it is not the true top frame. Merge must
    // not search below the Fork frame.
    let mut outer_merge = MergeState::new(
        run_id.clone(),
        activation("activation_outer_merge"),
        port("merged"),
        activation("activation_outer_branch"),
        scope("scope_parent"),
        vec![port("then"), port("else")],
    )
    .unwrap();
    assert_eq!(
        outer_merge
            .arrive_and_emit(&mut ledger, key("merge.consume", "too_early"), beta,)
            .unwrap_err()
            .code(),
        MERGE_CORRELATION_MISMATCH
    );
    assert!(ledger.consumption(beta).unwrap().is_none());

    // The opposite nesting direction produces Fork -> Branch. Consuming the
    // inner Branch pops exactly one frame and exposes the Fork frame.
    let mut inner_branch = branch(&run_id, "activation_inner_branch", "scope_beta", &["inner"]);
    let inner = committed(
        &inner_branch
            .select_and_emit(
                &mut ledger,
                key("branch.emit", "inner"),
                port("inner"),
                Some(beta),
            )
            .unwrap(),
    );
    assert!(matches!(
        ledger.row(&inner).unwrap().provenance().frames(),
        [
            ControlFrame::Branch(_),
            ControlFrame::ForkLeg(_),
            ControlFrame::Branch(_)
        ]
    ));
    let mut inner_merge = MergeState::new(
        run_id,
        activation("activation_inner_merge"),
        port("inner_merged"),
        activation("activation_inner_branch"),
        scope("scope_beta"),
        vec![port("inner")],
    )
    .unwrap();
    let after_inner = committed(
        &inner_merge
            .arrive_and_emit(&mut ledger, key("merge.consume", "inner"), &inner)
            .unwrap(),
    );
    assert!(matches!(
        ledger.row(&after_inner).unwrap().provenance().frames(),
        [ControlFrame::Branch(_), ControlFrame::ForkLeg(_)]
    ));
}

// This helper makes the public execution boundary explicit at compile time:
// callers can hold a serde row, but control aggregates consume only an owned
// ledger handle.
#[allow(dead_code)]
fn merge_execution_requires_owned_handle(_: &OwnedControlToken) {}
