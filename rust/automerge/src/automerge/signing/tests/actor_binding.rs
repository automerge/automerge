//! The fixed local author may only write through actors it owns.
//!
//! In required mode an actor whose recorded author differs from the document's
//! fixed author must never be selected for local commits: that would continue
//! another author's chain with a draft nobody can sign.

use std::panic::{catch_unwind, AssertUnwindSafe};

use crate::signature::error::SigningError;
use crate::{AutoCommit, AutomergeError, LoadOptions};

use super::super::{RequiredSigning, SigningState};
use super::*;

fn drafts(doc: &mut Automerge) -> Vec<ChangeHash> {
    doc.try_signing()
        .unwrap()
        .state
        .local_drafts
        .iter()
        .collect()
}

/// Author A publishes `a` under actor `[1]`.
fn published_source() -> (Automerge, SigningKey, ChangeHash) {
    let k = key(7);
    let mut doc = Automerge::signing(author(&k))
        .unwrap()
        .with_actor(ActorId::from([1]));
    let a = append_at_level(&mut doc, None, 1, 0);
    doc.insert_signatures([(a, sig(&k, &a))]);
    assert!(!doc.has_unpublished_changes());
    (doc, k, a)
}

#[test]
fn new_author_fork_cannot_reuse_the_old_authors_actor() {
    let (doc, _k, _a) = published_source();
    let k2 = key(8);
    let mut fork = doc.fork_with_author(Some(author(&k2))).unwrap();
    let fresh = fork.get_actor().clone();
    assert_ne!(fresh, ActorId::from([1]));

    assert!(matches!(
        fork.try_set_actor(ActorId::from([1])),
        Err(AutomergeError::Signing(SigningError::ActorBoundToOtherAuthor { actor })) if actor == ActorId::from([1])
    ));
    assert_eq!(
        fork.get_actor(),
        &fresh,
        "rejected request leaves the actor unchanged"
    );

    // A brand-new actor is fine for the new author, and a local commit
    // records B as its author.
    fork.try_set_actor(ActorId::from([9])).unwrap();
    let b = append_at_level(&mut fork, None, 2, 0);
    assert_eq!(
        fork.get_change_by_hash(&b).unwrap().author(),
        Some(author(&k2))
    );
    assert_eq!(requested_hashes(&mut fork), [b]);
    assert_eq!(drafts(&mut fork), [b]);

    // Now actor [9] belongs to B; A's original document can't take it either.
    let b_wire = {
        fork.insert_signatures([(b, sig(&k2, &b))]);
        fork.bundle_fragments([fork.get_fragment(b).unwrap()])
            .concat()
    };
    let (mut source, _, _) = published_source();
    source.load_incremental(&b_wire).unwrap();
    assert!(source.try_set_actor(ActorId::from([9])).is_err());
    assert_eq!(source.get_actor(), &ActorId::from([1]));
}

#[test]
fn same_author_actor_reuse_and_unsigned_documents_are_unchanged() {
    let (mut doc, k, _a) = published_source();
    // Switch away and back to our own historical actor: allowed, and the
    // continued chain is signable by the fixed author.
    doc.try_set_actor(ActorId::from([2])).unwrap();
    let c = append_at_level(&mut doc, None, 2, 0);
    doc.try_set_actor(ActorId::from([1])).unwrap();
    let d = append_at_level(&mut doc, None, 3, 0);
    assert_eq!(doc.get_change_by_hash(&d).unwrap().seq(), 2);
    assert_eq!(requested_hashes(&mut doc), {
        let mut v = vec![c, d];
        v.sort_unstable();
        v
    });
    doc.insert_signatures([(c, sig(&k, &c)), (d, sig(&k, &d))]);
    assert!(!doc.has_unpublished_changes());

    // Same-author fork may reuse the actor explicitly.
    let mut same = doc.fork_with_author(Some(author(&k))).unwrap();
    same.try_set_actor(ActorId::from([1])).unwrap();
    assert_eq!(same.get_actor(), &ActorId::from([1]));

    // Unsigned documents keep the historical free actor choice.
    let mut unsigned =
        Automerge::new_with_author(Some(Author::from(vec![1]))).with_actor(ActorId::from([1]));
    append_at_level(&mut unsigned, None, 1, 0);
    let mut other = unsigned
        .fork_with_author(Some(Author::from(vec![2])))
        .unwrap();
    other.try_set_actor(ActorId::from([1])).unwrap();
    other.set_actor(ActorId::from([1]));
    assert_eq!(other.get_actor(), &ActorId::from([1]));
}

#[test]
#[should_panic(expected = "ActorBoundToOtherAuthor")]
fn legacy_set_actor_panics_on_required_foreign_actor_reuse() {
    let (doc, _k, _a) = published_source();
    let mut fork = doc.fork_with_author(Some(author(&key(8)))).unwrap();
    fork.set_actor(ActorId::from([1]));
}

#[test]
fn autocommit_checked_setter_rejects_before_closing_the_transaction() {
    let (source, _k, _a) = published_source();
    let mut fork = AutoCommit::load_with_options(
        &source.save(),
        LoadOptions::new().signing().author(author(&key(8))),
    )
    .unwrap();
    fork.put(ROOT, "v", 1).unwrap();
    assert_eq!(fork.pending_ops(), 1);
    assert!(fork.try_set_actor(ActorId::from([1])).is_err());
    assert_eq!(
        fork.pending_ops(),
        1,
        "rejection does not close the transaction"
    );
    fork.try_set_actor(ActorId::from([5])).unwrap();
    assert_eq!(fork.pending_ops(), 0);
    assert_eq!(fork.get_actor(), &ActorId::from([5]));
}

#[test]
fn imported_collision_on_an_unused_configured_actor_fails_before_any_mutation() {
    let (source, _k, a) = published_source();
    let wire = source.bundle_fragments(source.fragments(..)).concat();

    // B configures actor [1] while it is still unused, then imports A's
    // published chain, which binds [1] to A.
    let k2 = key(8);
    let mut doc = Automerge::signing(author(&k2))
        .unwrap()
        .with_actor(ActorId::from([1]));
    doc.load_incremental(&wire).unwrap();
    assert_eq!(
        doc.get_author_for_actor(&ActorId::from([1])),
        Some(author(&key(7)))
    );
    let before = format!("{:?}", doc.get_heads());

    let outcome = catch_unwind(AssertUnwindSafe(|| {
        let mut tx = doc.transaction();
        tx.put(ROOT, "v", 1).unwrap();
        tx.commit();
    }));
    assert!(
        outcome.is_err(),
        "continuing A's chain as B must not succeed"
    );
    assert_eq!(format!("{:?}", doc.get_heads()), before);
    assert_eq!(doc.get_heads(), [a]);
    assert!(drafts(&mut doc).is_empty());
    assert!(!doc.has_unpublished_changes());
    assert!(doc.get(ROOT, "v").unwrap().is_none());

    // The same document works once it picks its own actor, and the loaded
    // actor override path is checked up front.
    doc.try_set_actor(ActorId::from([2])).unwrap();
    let b = append_at_level(&mut doc, None, 2, 0);
    assert_eq!(drafts(&mut doc), [b]);
    assert!(matches!(
        Automerge::load_with_options(&wire, LoadOptions::new().signing().author(author(&k2)))
            .unwrap()
            .try_set_actor(ActorId::from([1])),
        Err(AutomergeError::Signing(
            SigningError::ActorBoundToOtherAuthor { .. }
        ))
    ));
}

#[test]
fn authorless_applied_history_is_not_the_fixed_authors_to_continue() {
    // Valid required-mode ingress always records an author, so reach this
    // state by injection: unsigned history under actors [1] and [2] with no
    // recorded author, then the document is switched to required mode.
    let mut doc = Automerge::new().with_actor(ActorId::from([1]));
    append_at_level(&mut doc, None, 1, 0);
    doc.set_actor(ActorId::from([2]));
    doc.author = Some(author(&key(7)));
    doc.signing = SigningState::Required(RequiredSigning::new(doc.actors()));
    assert_eq!(doc.get_author_for_actor(&ActorId::from([1])), None);

    // Applied history without an author is not ours to continue.
    assert!(matches!(
        doc.check_actor_binding(&ActorId::from([1])),
        Err(AutomergeError::Signing(SigningError::ActorBoundToOtherAuthor { actor })) if actor == ActorId::from([1])
    ));
    assert!(doc.try_set_actor(ActorId::from([1])).is_err());
    assert_eq!(doc.get_actor(), &ActorId::from([2]));

    // An unused actor slot in the table (no applied history) is still free.
    assert_eq!(doc.get_author_for_actor(&ActorId::from([2])), None);
    doc.check_actor_binding(&ActorId::from([2])).unwrap();
    doc.try_set_actor(ActorId::from([2])).unwrap();
    doc.try_set_actor(ActorId::from([3])).unwrap();
}
