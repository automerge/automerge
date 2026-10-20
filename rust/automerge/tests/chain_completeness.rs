use automerge::{
    transaction::Transactable, ActorId, Automerge, Change, ExpandedChange, PatchLog, ReadDoc, ROOT,
};

/// Every noninitial change must directly depend on its actor's previous change.
fn assert_chain_complete(doc: &Automerge) {
    let changes = doc.get_changes(&[]);
    for change in &changes {
        if change.seq() > 1 {
            let predecessor = changes
                .iter()
                .find(|candidate| {
                    candidate.actor_id() == change.actor_id() && candidate.seq() == change.seq() - 1
                })
                .expect("actor predecessor exists");
            assert!(
                change.deps().contains(&predecessor.hash()),
                "change {:?} (seq {}) does not list actor predecessor {:?}; deps = {:?}",
                change.hash(),
                change.seq(),
                predecessor.hash(),
                change.deps(),
            );
        }
    }
}

#[test]
fn transaction_at_lists_actor_predecessor() {
    let actor_x = ActorId::from(vec![1]);
    let actor_y = ActorId::from(vec![2]);
    let mut doc = Automerge::new().with_actor(actor_x.clone());

    let mut tx = doc.transaction();
    tx.put(ROOT, "a", 1).unwrap();
    let x1 = tx.commit().0.unwrap();

    // Y:1 depends on X:1, making X:1 an ancestor but not a head.
    let mut other = doc.fork().with_actor(actor_y);
    let mut tx = other.transaction();
    tx.put(ROOT, "b", 2).unwrap();
    let y1 = tx.commit().0.unwrap();
    doc.merge(&mut other).unwrap();
    assert_eq!(doc.get_heads(), vec![y1]);
    assert!(doc.get_change_by_hash(&y1).unwrap().deps().contains(&x1));

    // X continues at Y:1. Its predecessor must be an explicit dependency,
    // even though it is already reachable through Y:1.
    let mut tx = doc.transaction_at(PatchLog::inactive(), &[y1]).unwrap();
    tx.put(ROOT, "c", 3).unwrap();
    let x2 = tx.commit().0.unwrap();

    let change = doc.get_change_by_hash(&x2).unwrap();
    assert_eq!(change.actor_id(), &actor_x, "isolation must reuse X");
    assert_eq!(change.seq(), 2);
    assert!(change.deps().contains(&y1), "retain the requested head");
    assert!(
        change.deps().contains(&x1),
        "X:2 must list X:1 directly; deps = {:?}",
        change.deps(),
    );
    assert_chain_complete(&doc);
}

#[test]
fn isolation_after_empty_change_does_not_reuse_actor_outside_ancestry() {
    let actor_x = ActorId::from(vec![1]);
    let actor_y = ActorId::from(vec![2]);
    let mut doc = Automerge::new().with_actor(actor_x.clone());
    let x1 = doc.empty_commit(Default::default());
    assert!(doc.get_change_by_hash(&x1).unwrap().is_empty());

    // Y starts independently: Y:1 has no ancestry linking it to X:1.
    let mut other = Automerge::new().with_actor(actor_y);
    let mut tx = other.transaction();
    tx.put(ROOT, "b", 2).unwrap();
    let y1 = tx.commit().0.unwrap();
    assert!(other.get_change_by_hash(&y1).unwrap().deps().is_empty());
    doc.merge(&mut other).unwrap();

    let mut tx = doc.transaction_at(PatchLog::inactive(), &[y1]).unwrap();
    tx.put(ROOT, "c", 3).unwrap();
    let hash = tx.commit().0.unwrap();

    let change = doc.get_change_by_hash(&hash).unwrap();
    assert_ne!(
        change.actor_id(),
        &actor_x,
        "isolation must not reuse X when X:1 is outside the requested ancestry",
    );
    assert_eq!(change.seq(), 1);
    assert_eq!(
        change.deps(),
        &[y1],
        "must not introduce X:1 into the scope"
    );
    assert_chain_complete(&doc);
}

#[test]
fn isolation_reuses_actor_when_latest_change_is_ancestor() {
    let actor_x = ActorId::from(vec![1]);
    let actor_y = ActorId::from(vec![2]);
    let mut doc = Automerge::new().with_actor(actor_x.clone());
    let x1 = doc.empty_commit(Default::default());

    // An empty latest change does not by itself prevent reuse: here Y:1
    // descends from X:1, so X's whole chain is in the requested ancestry.
    let mut other = doc.fork().with_actor(actor_y);
    let mut tx = other.transaction();
    tx.put(ROOT, "b", 2).unwrap();
    let y1 = tx.commit().0.unwrap();
    assert!(other.get_change_by_hash(&y1).unwrap().deps().contains(&x1));
    doc.merge(&mut other).unwrap();

    let mut tx = doc.transaction_at(PatchLog::inactive(), &[y1]).unwrap();
    tx.put(ROOT, "c", 3).unwrap();
    let hash = tx.commit().0.unwrap();

    let change = doc.get_change_by_hash(&hash).unwrap();
    assert_eq!(change.actor_id(), &actor_x, "isolation should reuse X");
    assert_eq!(change.seq(), 2);
    assert!(change.deps().contains(&x1));
    assert!(change.deps().contains(&y1));
    assert_chain_complete(&doc);
}

#[test]
fn isolation_reuses_derived_actor_and_lists_its_predecessor() {
    let actor_x = ActorId::from(vec![1]);
    let actor_y = ActorId::from(vec![2]);
    let mut doc = Automerge::new().with_actor(actor_x.clone());

    let mut tx = doc.transaction();
    tx.put(ROOT, "a", 1).unwrap();
    let x1 = tx.commit().0.unwrap();

    // Y starts independently, so X:1 is outside the ancestry of Y:1.
    let mut other = Automerge::new().with_actor(actor_y.clone());
    let mut tx = other.transaction();
    tx.put(ROOT, "b", 2).unwrap();
    let y1 = tx.commit().0.unwrap();
    assert!(other.get_change_by_hash(&y1).unwrap().deps().is_empty());
    doc.merge(&mut other).unwrap();

    // X cannot be reused at Y:1, so a concurrency actor is derived.
    let mut tx = doc.transaction_at(PatchLog::inactive(), &[y1]).unwrap();
    tx.put(ROOT, "c", 3).unwrap();
    let derived1 = tx.commit().0.unwrap();
    let first = doc.get_change_by_hash(&derived1).unwrap();
    let derived_actor = first.actor_id().clone();
    assert_ne!(&derived_actor, &actor_x, "must not reuse X");
    assert_ne!(&derived_actor, &actor_y, "must not reuse Y");
    assert_eq!(first.seq(), 1);

    // Z descends from the derived change but not from X:1, so the derived
    // actor's predecessor becomes a transitive ancestor rather than a head.
    let mut third = doc.fork().with_actor(ActorId::from(vec![3]));
    let mut tx = third
        .transaction_at(PatchLog::inactive(), &[derived1])
        .unwrap();
    tx.put(ROOT, "d", 4).unwrap();
    let z1 = tx.commit().0.unwrap();
    assert_eq!(third.get_change_by_hash(&z1).unwrap().deps(), &[derived1]);
    doc.merge(&mut third).unwrap();

    // Isolating at Z:1 reuses the derived actor rather than deriving another:
    // X is still outside the ancestry, but the derived actor's whole chain is
    // within it.
    let mut tx = doc.transaction_at(PatchLog::inactive(), &[z1]).unwrap();
    tx.put(ROOT, "e", 5).unwrap();
    let derived2 = tx.commit().0.unwrap();

    let change = doc.get_change_by_hash(&derived2).unwrap();
    assert_eq!(
        change.actor_id(),
        &derived_actor,
        "isolation must reuse the derived actor",
    );
    assert_eq!(change.seq(), 2);
    assert!(change.deps().contains(&z1), "retain the requested head");
    assert!(
        change.deps().contains(&derived1),
        "seq 2 must list its actor predecessor directly, even though it is \
         already reachable through Z:1; deps = {:?}",
        change.deps(),
    );
    assert!(
        !change.deps().contains(&x1),
        "must not introduce X:1 into the scope; deps = {:?}",
        change.deps(),
    );
    assert_chain_complete(&doc);
}

#[test]
fn legacy_transitive_actor_predecessor_loads_and_round_trips() {
    let mut doc = Automerge::new().with_actor(ActorId::from(vec![1]));
    let mut tx = doc.transaction();
    tx.put(ROOT, "a", 1).unwrap();
    let x1 = tx.commit().0.unwrap();

    let mut other = doc.fork().with_actor(ActorId::from(vec![2]));
    let mut tx = other.transaction();
    tx.put(ROOT, "b", 2).unwrap();
    let y1 = tx.commit().0.unwrap();
    doc.merge(&mut other).unwrap();

    let mut tx = doc.transaction_at(PatchLog::inactive(), &[y1]).unwrap();
    tx.put(ROOT, "c", 3).unwrap();
    let x2 = tx.commit().0.unwrap();

    // Recreate the legacy producer's shape independently of transaction_args:
    // X:2 depends on Y:1, which depends on X:1, but omits the direct X:1 edge.
    let mut expanded = ExpandedChange::from(&doc.get_change_by_hash(&x2).unwrap());
    expanded.deps = vec![y1];
    expanded.hash = None;
    let legacy_x2 = Change::from(expanded);
    let first = doc.get_change_by_hash(&x1).unwrap();
    let second = doc.get_change_by_hash(&y1).unwrap();
    assert_eq!(legacy_x2.actor_id(), first.actor_id());
    assert_eq!(legacy_x2.seq(), 2);
    assert_eq!(second.deps(), &[x1]);
    assert!(!legacy_x2.deps().contains(&x1));

    let bytes = [first.raw_bytes(), second.raw_bytes(), legacy_x2.raw_bytes()].concat();
    let loaded = Automerge::load(&bytes).expect("legacy change chunks must load");
    let reloaded = Automerge::load(&loaded.save()).expect("legacy snapshot must reload");

    for document in [&loaded, &reloaded] {
        assert_eq!(document.get_changes(&[]).len(), 3);
        assert_eq!(document.get_heads(), vec![legacy_x2.hash()]);
        let preserved = document.get_change_by_hash(&legacy_x2.hash()).unwrap();
        assert_eq!(preserved.deps(), &[y1]);
        assert_eq!(preserved.raw_bytes(), legacy_x2.raw_bytes());
        for (key, expected) in [("a", 1i64), ("b", 2), ("c", 3)] {
            let (value, _) = document.get(ROOT, key).unwrap().unwrap();
            assert_eq!(value, expected.into(), "value of {key} survives loading");
        }
    }
}

#[test]
fn legacy_fixtures_load_and_report_chain_violations() {
    // These fixtures are required to load by tests/test.rs. The other fixtures
    // are deliberately malformed or exercise 64-bit IDs whose rejection is
    // permitted by their existing tests; they are not compatibility baselines.
    let fixtures: &[(&str, &[u8])] = &[
        (
            "counter_value_is_ok",
            include_bytes!("fixtures/counter_value_is_ok.automerge"),
        ),
        (
            "two_change_chunks",
            include_bytes!("fixtures/two_change_chunks.automerge"),
        ),
        (
            "two_change_chunks_compressed",
            include_bytes!("fixtures/two_change_chunks_compressed.automerge"),
        ),
        (
            "two_change_chunks_out_of_order",
            include_bytes!("fixtures/two_change_chunks_out_of_order.automerge"),
        ),
    ];
    let mut violations = 0;
    for (name, bytes) in fixtures {
        let doc = Automerge::load(bytes)
            .unwrap_or_else(|err| panic!("legacy fixture {name} must load: {err}"));
        let changes = doc.get_changes(&[]);
        assert!(!changes.is_empty(), "fixture {name} must contain changes");
        for change in &changes {
            if change.seq() > 1 {
                let predecessor = changes
                    .iter()
                    .find(|candidate| {
                        candidate.actor_id() == change.actor_id()
                            && candidate.seq() == change.seq() - 1
                    })
                    .unwrap_or_else(|| {
                        panic!("fixture {name}: missing predecessor of {:?}", change.hash())
                    });
                if !change.deps().contains(&predecessor.hash()) {
                    violations += 1;
                    eprintln!(
                        "fixture {name}: change {:?} omits actor predecessor {:?}",
                        change.hash(),
                        predecessor.hash(),
                    );
                }
            }
        }
    }
    // Loading legacy documents must not enforce the new producer invariant.
    eprintln!(
        "checked {} fixtures; {violations} chain-incomplete changes (allowed on load)",
        fixtures.len(),
    );
    assert!(
        !fixtures.is_empty(),
        "compatibility corpus must not be empty"
    );
}

mod properties {
    use super::assert_chain_complete;
    use automerge::{transaction::Transactable, ActorId, AutoCommit, ChangeHash, ROOT};
    use proptest::prelude::*;

    #[derive(Debug, Clone)]
    enum Action {
        Put(u8),
        Empty,
        Isolate(usize),
        Integrate,
        ForkMergeBack,
    }

    fn actions() -> impl Strategy<Value = Vec<Action>> {
        proptest::collection::vec(
            prop_oneof![
                4 => any::<u8>().prop_map(Action::Put),
                1 => Just(Action::Empty),
                2 => any::<usize>().prop_map(Action::Isolate),
                1 => Just(Action::Integrate),
                1 => Just(Action::ForkMergeBack),
            ],
            1..40,
        )
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(200))]

        #[test]
        fn producer_always_emits_actor_predecessor(actions in actions()) {
            let mut doc = AutoCommit::new().with_actor(ActorId::from(vec![0]));
            let mut historical_heads: Vec<ChangeHash> = Vec::new();
            let mut next_actor = 1u64;

            for action in actions {
                match action {
                    Action::Put(value) => {
                        doc.put(ROOT, "k", i64::from(value)).unwrap();
                    }
                    Action::Empty => {
                        doc.empty_change(Default::default());
                    }
                    Action::Isolate(index) => {
                        // Include the empty history as well as every recorded change.
                        let index = index % (historical_heads.len() + 1);
                        if index == historical_heads.len() {
                            doc.isolate(&[]);
                        } else {
                            doc.isolate(&[historical_heads[index]]);
                        }
                    }
                    Action::Integrate => doc.integrate(),
                    Action::ForkMergeBack => {
                        // Override fork's random actor so shrinking replays the
                        // same actor ordering and index shifts.
                        let actor = ActorId::from(next_actor.to_be_bytes().to_vec());
                        next_actor += 1;
                        let mut fork = doc.fork().with_actor(actor);
                        fork.put(ROOT, "fork", 1).unwrap();
                        doc.merge(&mut fork).unwrap();
                    }
                }
                doc.commit();

                // Check after every action, including changes hidden by isolation.
                let document = doc.document();
                assert_chain_complete(document);
                historical_heads = document
                    .get_changes(&[])
                    .iter()
                    .map(|change| change.hash())
                    .collect();
                historical_heads.sort();
            }

            doc.integrate();
            assert_chain_complete(doc.document());
        }
    }
}
