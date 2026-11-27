//! Manual actor selection cannot change the author of an established chain.

use automerge::transaction::Transactable;
use automerge::{ActorId, Author, AutoCommit, Automerge, Config, ROOT};

#[test]
#[allow(deprecated)]
fn unattested_actor_reuse_checks_named_and_anonymous_identity() {
    for original in [None, Some(Author::from(vec![1]))] {
        let actor = ActorId::from([1]);
        let mut source = Automerge::from_config(Config::default().author(original.clone()));
        source.set_actor(actor.clone()).unwrap();
        let mut tx = source.transaction();
        tx.put(ROOT, "v", 1).unwrap();
        tx.commit();
        for requested in [
            None,
            Some(Author::from(vec![1])),
            Some(Author::from(vec![2])),
        ] {
            let mut fork = source.fork_with_author(requested.clone()).unwrap();
            let before = fork.get_actor().clone();
            assert_eq!(fork.set_actor(actor.clone()).is_ok(), requested == original);
            if requested != original {
                assert_eq!(fork.get_actor(), &before);
            }
        }
    }
}

#[test]
#[allow(deprecated)]
fn imported_identity_blocks_an_unattested_actor_chosen_while_unbound() {
    let actor = ActorId::from([1]);
    let mut source = AutoCommit::from_config(Config::default().author(Some(Author::from(vec![1]))));
    source.set_actor(actor.clone()).unwrap();
    source.put(ROOT, "source", 1).unwrap();
    let bytes = source.save();

    let mut incremental = Automerge::new();
    incremental.set_actor(actor.clone()).unwrap();
    let before = format!("{incremental:?}");
    assert!(matches!(
        incremental.load_incremental(&bytes),
        Err(automerge::AutomergeError::ActorAuthorMismatch { .. })
    ));
    assert_eq!(format!("{incremental:?}"), before);

    // Ordinary apply can bind a previously selected unused actor. The later
    // infallible transaction entry must not continue another identity's chain.
    // This test observes the panic, but promises no reusable state after it.
    let mut imported = Automerge::new();
    imported.set_actor(actor).unwrap();
    imported.apply_changes(source.get_changes(&[])).unwrap();
    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let mut tx = imported.transaction();
        tx.put(ROOT, "forbidden", 2).unwrap();
        tx.commit();
    }))
    .is_err());
}

#[test]
#[allow(deprecated)]
fn rejected_actor_selection_preserves_autocommit_pending_work() {
    let actor = ActorId::from([1]);
    let mut source = AutoCommit::from_config(Config::default().author(Some(Author::from(vec![1]))));
    source.set_actor(actor.clone()).unwrap();
    source.put(ROOT, "v", 1).unwrap();
    source.commit();
    let mut fork = source
        .fork_with_author(Some(Author::from(vec![2])))
        .unwrap();
    fork.put(ROOT, "pending", 1).unwrap();
    assert!(fork.set_actor(actor).is_err());
    assert_eq!(fork.pending_ops(), 1);
}
