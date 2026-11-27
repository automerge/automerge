//! Immutable construction-time authors and the guarded new-author fork.

use crate::signature::error::AttestationError;
use crate::{AutoCommit, AutomergeError, LoadOptions, Signature};

use super::*;

fn weak_author() -> Author<'static> {
    let mut weak = vec![0; 32];
    weak[0] = 1;
    Author::from(weak)
}

fn invalid_authors() -> Vec<Author<'static>> {
    vec![Author::from(vec![1, 2, 3]), weak_author()]
}

fn unattested_doc(author: Author<'static>) -> Automerge {
    Automerge::from_config(crate::Config::default().author(Some(author)))
        .with_actor(ActorId::from([1]))
        .unwrap()
}

#[test]
fn unattested_commits_carry_the_configured_author() {
    let arbitrary = Author::from(vec![1, 2, 3]);
    let mut doc = unattested_doc(arbitrary.clone());
    let first = append_at_level(&mut doc, None, 1, 0);
    assert_eq!(
        doc.get_change_by_hash(&first).unwrap().author(),
        Some(arbitrary)
    );
}

#[test]
fn load_options_author_overrides_the_saved_author() {
    let mut doc = unattested_doc(Author::from(vec![1, 2, 3]));
    append_at_level(&mut doc, None, 1, 0);
    let bytes = doc.save();

    let other = Author::from(vec![9, 9]);
    let loaded =
        Automerge::load_with_options(&bytes, LoadOptions::new().author(other.clone())).unwrap();
    assert_eq!(loaded.get_author(), Some(&other));
    let empty =
        Automerge::load_with_options(&[], LoadOptions::new().author(other.clone())).unwrap();
    assert_eq!(empty.get_author(), Some(&other));
    assert!(Automerge::load(&bytes).unwrap().get_author().is_none());
}

#[test]
fn fork_inherits_the_author_with_a_fresh_actor() {
    let k = key(7);
    let mut doc = signing_doc(&k, 1);
    append_at_level(&mut doc, None, 1, 0);

    let fork = doc.fork();
    assert_eq!(fork.get_author(), Some(&author(&k)));
    assert_ne!(fork.get_actor(), doc.get_actor());
    assert!(fork.attestation.is_attested());
}

#[test]
fn clone_is_exact() {
    let k = key(7);
    let mut doc = signing_doc(&k, 1);
    append_at_level(&mut doc, None, 1, 0);

    let clone = doc.clone();
    assert_eq!(clone.get_actor(), doc.get_actor());
    assert_eq!(format!("{clone:?}"), format!("{doc:?}"));
}

#[test]
fn same_author_fork_copies_drafts_with_independent_provenance() {
    let k = key(7);
    let mut doc = signing_doc(&k, 1);
    let a = append_at_level(&mut doc, None, 1, 0);

    let mut same = doc.fork_with_author(Some(author(&k))).unwrap();
    assert_eq!(same.get_author(), Some(&author(&k)));
    assert_ne!(same.get_actor(), doc.get_actor());
    assert_eq!(drafts(&mut same), [a]);
    assert_eq!(drafts(&mut doc), [a]);
    // Independent provenance: publishing in one copy does not touch the other.
    sign_doc(&mut same, a, &k);
    assert!(drafts(&mut same).is_empty());
    assert_eq!(drafts(&mut doc), [a]);
}

#[test]
fn different_author_fork_is_rejected_while_an_unsigned_draft_remains() {
    let k = key(7);
    let mut doc = signing_doc(&k, 1);
    append_at_level(&mut doc, None, 1, 0);
    let before = format!("{doc:?}");

    assert!(matches!(
        doc.fork_with_author(Some(author(&key(8)))),
        Err(AutomergeError::Signing(
            AttestationError::UnpublishedLocalChanges
        ))
    ));
    assert_eq!(format!("{doc:?}"), before);
}

#[test]
fn different_author_fork_is_rejected_while_a_signed_but_unpublished_draft_remains() {
    let k = key(7);
    let mut doc = signing_doc(&k, 1);
    // With the loose gap `a` unsigned, signing `b` stores its proof but cannot
    // publish it, so a draft remains.
    let a = append_at_level(&mut doc, None, 1, 0);
    let b = append_at_level(&mut doc, None, 2, 0);
    doc.insert_signatures([(b, sig(&k, &b))]);
    assert!(!is_published(&mut doc, &b));
    assert_eq!(drafts(&mut doc), [a, b]);
    let before = format!("{doc:?}");

    assert!(matches!(
        doc.fork_with_author(Some(author(&key(8)))),
        Err(AutomergeError::Signing(
            AttestationError::UnpublishedLocalChanges
        ))
    ));
    assert_eq!(format!("{doc:?}"), before);
}

#[test]
fn attested_fork_never_removes_the_author() {
    let k = key(7);
    let mut doc = signing_doc(&k, 1);
    append_at_level(&mut doc, None, 1, 0);
    let before = format!("{doc:?}");

    assert!(matches!(
        doc.fork_with_author(None),
        Err(AutomergeError::SigningAuthor(_))
    ));
    assert_eq!(format!("{doc:?}"), before);
}

#[test]
fn fork_with_an_unusable_author_is_a_key_error_before_the_draft_gate() {
    let k = key(7);
    let mut doc = signing_doc(&k, 1);
    append_at_level(&mut doc, None, 1, 0);
    let before = format!("{doc:?}");

    for invalid in invalid_authors() {
        assert!(matches!(
            doc.fork_with_author(Some(invalid)),
            Err(AutomergeError::SigningAuthor(_))
        ));
    }
    assert_eq!(format!("{doc:?}"), before);
}

#[test]
fn fully_published_fork_with_a_new_author_keeps_the_old_proofs() {
    let k = key(7);
    let k2 = key(8);
    let mut doc = signing_doc(&k, 1);
    let a = append_at_level(&mut doc, None, 1, 0);
    doc.insert_signatures([(a, sig(&k, &a))]);
    assert!(is_published(&mut doc, &a));

    let mut fork = doc.fork_with_author(Some(author(&k2))).unwrap();
    assert_eq!(fork.get_author(), Some(&author(&k2)));
    assert_eq!(doc.get_author(), Some(&author(&k)));
    assert!(fork.attestation.is_attested());
    assert!(is_published(&mut fork, &a));
    assert_eq!(
        fork.get_change_by_hash(&a).unwrap().author(),
        Some(author(&k))
    );
    assert!(drafts(&mut fork).is_empty());
}

#[test]
fn fully_published_fork_with_a_new_author_uses_a_fresh_actor() {
    let k = key(7);
    let k2 = key(8);
    let mut doc = signing_doc(&k, 1);
    let a = append_at_level(&mut doc, None, 1, 0);
    doc.insert_signatures([(a, sig(&k, &a))]);

    let mut fork = doc.fork_with_author(Some(author(&k2))).unwrap();
    assert_ne!(fork.get_actor(), doc.get_actor());

    let b = append_at_level(&mut fork, None, 2, 0);
    let change = fork.get_change_by_hash(&b).unwrap();
    assert_eq!(change.author(), Some(author(&k2)));
    assert_ne!(change.actor_id(), &ActorId::from([1]));
    assert_eq!(drafts(&mut fork), [b]);
    assert!(fork.has_unpublished_changes());
}

#[test]
fn forked_author_work_is_signed_only_by_the_new_author() {
    let k = key(7);
    let k2 = key(8);
    let mut doc = signing_doc(&k, 1);
    let a = append_at_level(&mut doc, None, 1, 0);
    doc.insert_signatures([(a, sig(&k, &a))]);
    let mut fork = doc.fork_with_author(Some(author(&k2))).unwrap();
    let b = append_at_level(&mut fork, None, 2, 0);

    // The old author cannot sign the new author's change.
    assert!(insert_one(&mut fork, b, &sig(&k, &b)).is_err());
    fork.insert_signatures([(b, sig(&k2, &b))]);
    assert!(is_published(&mut fork, &b));
}

#[test]
fn both_authors_proofs_verify_on_reload_of_the_published_fork() {
    let k = key(7);
    let k2 = key(8);
    let mut doc = signing_doc(&k, 1);
    let a = append_at_level(&mut doc, None, 1, 0);
    doc.insert_signatures([(a, sig(&k, &a))]);
    let mut fork = doc.fork_with_author(Some(author(&k2))).unwrap();
    let b = append_at_level(&mut fork, None, 2, 0);
    fork.insert_signatures([(b, sig(&k2, &b))]);

    let bytes = fork.bundle_fragments(fork.fragments(..)).concat();
    let mut reloaded = Automerge::load_with_options(
        &bytes,
        LoadOptions::new().attested(author(&key(200))).unwrap(),
    )
    .unwrap();
    assert_eq!(reloaded.get_heads(), fork.get_heads());
    assert!(is_published(&mut reloaded, &a));
    assert!(is_published(&mut reloaded, &b));
}

#[test]
fn the_source_admits_the_new_authors_work_when_it_is_delivered_later() {
    let k = key(7);
    let k2 = key(8);
    let mut doc = signing_doc(&k, 1);
    let a = append_at_level(&mut doc, None, 1, 0);
    doc.insert_signatures([(a, sig(&k, &a))]);
    let mut fork = doc.fork_with_author(Some(author(&k2))).unwrap();
    let b = append_at_level(&mut fork, None, 2, 0);
    fork.insert_signatures([(b, sig(&k2, &b))]);

    let b_bytes = attach(
        fork.get_change_by_hash(&b).unwrap().raw_bytes(),
        [(b, Signature::ed25519(sig(&k2, &b)))],
    );
    doc.load_incremental(&b_bytes).unwrap();
    assert!(is_published(&mut doc, &b));
    assert_eq!(doc.get_heads(), fork.get_heads());
}

#[test]
fn unattested_fork_removes_the_author_freely() {
    let mut doc = unattested_doc(Author::from(vec![1, 2, 3]));
    append_at_level(&mut doc, None, 1, 0);

    let removed = doc.fork_with_author(None).unwrap();
    assert!(removed.get_author().is_none());
    assert!(!removed.attestation.is_attested());
}

#[test]
fn unattested_fork_replaces_the_author_freely() {
    let arbitrary = Author::from(vec![1, 2, 3]);
    let mut doc = unattested_doc(arbitrary.clone());
    append_at_level(&mut doc, None, 1, 0);

    let replaced = doc.fork_with_author(Some(weak_author())).unwrap();
    assert_eq!(replaced.get_author(), Some(&weak_author()));
    assert_ne!(replaced.get_actor(), doc.get_actor());
    assert_eq!(doc.get_author(), Some(&arbitrary));
}

#[test]
fn incoming_pending_groups_do_not_block_a_new_author_fork() {
    let k = key(7);
    let k2 = key(8);
    let mut doc = signing_doc(&k, 1);
    let a = append_at_level(&mut doc, None, 1, 0);
    doc.insert_signatures([(a, sig(&k, &a))]);

    // A foreign author extends `a` twice on an unsigned replica of the same
    // history; only the tip is delivered, so it must wait for `b`.
    let mut remote = Automerge::from_config(crate::Config::default().author(Some(author(&k2))))
        .with_actor(ActorId::from([2]))
        .unwrap();
    remote
        .apply_changes([doc.get_change_by_hash(&a).unwrap()])
        .unwrap();
    let b = append_at_level(&mut remote, None, 2, 0);
    let c = append_at_level(&mut remote, None, 3, 0);
    let c_bytes = attach(
        remote.get_change_by_hash(&c).unwrap().raw_bytes(),
        [(c, Signature::ed25519(sig(&k2, &c)))],
    );
    assert_eq!(doc.load_incremental(&c_bytes).unwrap(), 0);
    assert!(doc.attestation.has_pending_groups());
    assert!(drafts(&mut doc).is_empty());

    let k3 = key(9);
    let mut fork = doc.fork_with_author(Some(author(&k3))).unwrap();
    assert_eq!(fork.get_author(), Some(&author(&k3)));
    assert!(fork.attestation.has_pending_groups());
    // The retained group still completes once `b` arrives.
    let b_bytes = attach(
        remote.get_change_by_hash(&b).unwrap().raw_bytes(),
        [(b, Signature::ed25519(sig(&k2, &b)))],
    );
    fork.load_incremental(&b_bytes).unwrap();
    assert!(is_published(&mut fork, &b));
    assert!(is_published(&mut fork, &c));
}

#[test]
fn attested_automerge_fork_at_is_unsupported() {
    let k = key(7);
    let mut doc = Automerge::from_config(crate::Config::default().attested(author(&k)).unwrap());
    let a = append_at_level(&mut doc, None, 1, 0);
    doc.insert_signatures([(a, sig(&k, &a))]);
    assert!(matches!(
        doc.fork_at(&[a]),
        Err(AutomergeError::Signing(
            AttestationError::ForkAtNotSupported
        ))
    ));
}

#[test]
fn attested_autocommit_fork_at_is_unsupported_and_keeps_the_transaction_open() {
    let k = key(7);
    let mut auto = AutoCommit::from_config(crate::Config::default().attested(author(&k)).unwrap());
    auto.put(ROOT, "pending", 1).unwrap();
    assert!(matches!(
        auto.fork_at(&[]),
        Err(AutomergeError::Signing(
            AttestationError::ForkAtNotSupported
        ))
    ));
    assert_eq!(auto.pending_ops(), 1, "transaction stays open");
    assert!(!auto.has_unpublished_changes(), "nothing was committed");
}

#[test]
fn unattested_autocommit_fork_at_commits_pending_edits_first() {
    let mut unsigned = AutoCommit::new();
    unsigned.put(ROOT, "pending", 1).unwrap();
    let mut fork = unsigned.fork_at(&[]).unwrap();
    assert_eq!(unsigned.pending_ops(), 0);
    assert_eq!(unsigned.get_heads().len(), 1);
    assert!(fork.get_heads().is_empty());
}
