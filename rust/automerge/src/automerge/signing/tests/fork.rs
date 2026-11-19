//! Immutable construction-time authors and the guarded new-author fork.

use crate::signature::error::SigningError;
use crate::storage::signatures::{HashEntries, SignaturesChunk};
use crate::{AutoCommit, AutomergeError, LoadOptions, Signature, TextEncoding};

use super::*;

fn drafts(doc: &mut Automerge) -> Vec<ChangeHash> {
    doc.try_signing()
        .unwrap()
        .state
        .local_drafts
        .iter()
        .collect()
}

fn is_accepted(doc: &mut Automerge, hash: &ChangeHash) -> bool {
    doc.try_signing().unwrap().is_accepted(hash)
}

fn attach(content: &[u8], entries: impl IntoIterator<Item = (ChangeHash, Signature)>) -> Vec<u8> {
    let chunk = SignaturesChunk::encode(HashEntries::canonicalize(entries).unwrap());
    [content, chunk.bytes()].concat()
}

fn weak_author() -> Author<'static> {
    let mut weak = vec![0; 32];
    weak[0] = 1;
    Author::from(weak)
}

fn invalid_authors() -> Vec<Author<'static>> {
    vec![Author::from(vec![1, 2, 3]), weak_author()]
}

#[test]
fn unsigned_constructors_fix_any_author_and_required_constructors_validate() {
    let arbitrary = Author::from(vec![1, 2, 3]);
    let doc = Automerge::new_with_author(Some(arbitrary.clone()));
    assert_eq!(doc.get_author(), Some(&arbitrary));
    assert!(!doc.signing.is_required());
    assert!(Automerge::new_with_author(None).get_author().is_none());
    assert!(Automerge::new().get_author().is_none());
    assert!(Automerge::new_with_encoding(TextEncoding::Utf8CodeUnit)
        .get_author()
        .is_none());
    let encoded =
        Automerge::new_with_author_and_encoding(Some(weak_author()), TextEncoding::Utf8CodeUnit);
    assert_eq!(encoded.get_author(), Some(&weak_author()));
    assert_eq!(encoded.text_encoding(), TextEncoding::Utf8CodeUnit);

    let auto = AutoCommit::new_with_author(Some(arbitrary.clone()));
    assert_eq!(auto.get_author(), Some(&arbitrary));
    let auto = AutoCommit::new_with_author_and_encoding(None, TextEncoding::Utf8CodeUnit);
    assert!(auto.get_author().is_none());

    for invalid in invalid_authors() {
        assert!(Automerge::signing(invalid.clone()).is_err());
        assert!(AutoCommit::signing(invalid).is_err());
    }
    let k = key(7);
    assert_eq!(
        Automerge::signing(author(&k)).unwrap().get_author(),
        Some(&author(&k))
    );
}

#[test]
fn unsigned_commits_carry_the_fixed_author_and_load_options_fix_the_author() {
    let arbitrary = Author::from(vec![1, 2, 3]);
    let mut doc =
        Automerge::new_with_author(Some(arbitrary.clone())).with_actor(ActorId::from([1]));
    let first = append_at_level(&mut doc, None, 1, 0);
    assert_eq!(
        doc.get_change_by_hash(&first).unwrap().author(),
        Some(arbitrary.clone())
    );
    let bytes = doc.save();

    let other = Author::from(vec![9, 9]);
    let loaded =
        Automerge::load_with_options(&bytes, LoadOptions::new().author(other.clone())).unwrap();
    assert_eq!(loaded.get_author(), Some(&other));
    let empty =
        Automerge::load_with_options(&[], LoadOptions::new().author(other.clone())).unwrap();
    assert_eq!(empty.get_author(), Some(&other));
    assert!(Automerge::load(&bytes).unwrap().get_author().is_none());

    // Required-mode loads keep validating.
    for invalid in invalid_authors() {
        assert!(
            Automerge::load_with_options(&[], LoadOptions::new().signing().author(invalid))
                .is_err()
        );
    }
    assert!(Automerge::load_with_options(&[], LoadOptions::new().signing()).is_err());
}

#[test]
fn plain_fork_inherits_author_with_fresh_actor_and_clone_is_exact() {
    let k = key(7);
    let mut doc = Automerge::signing(author(&k))
        .unwrap()
        .with_actor(ActorId::from([1]));
    let a = append_at_level(&mut doc, None, 1, 0);

    let fork = doc.fork();
    assert_eq!(fork.get_author(), Some(&author(&k)));
    assert_ne!(fork.get_actor(), doc.get_actor());
    assert!(fork.signing.is_required());

    let clone = doc.clone();
    assert_eq!(clone.get_actor(), doc.get_actor());
    assert_eq!(format!("{clone:?}"), format!("{doc:?}"));

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
fn different_author_fork_is_rejected_while_local_drafts_remain() {
    let k = key(7);
    let k2 = key(8);
    let mut doc = Automerge::signing(author(&k))
        .unwrap()
        .with_actor(ActorId::from([1]));
    let a = append_at_level(&mut doc, None, 1, 0);
    let before = format!("{doc:?}");

    assert!(matches!(
        doc.fork_with_author(Some(author(&k2))),
        Err(AutomergeError::Signing(
            SigningError::UnpublishedLocalChanges
        ))
    ));
    assert_eq!(format!("{doc:?}"), before);

    // Signed-but-unpublished drafts still reject: with the loose gap `a`
    // unsigned, signing `b` stores its proof but cannot publish it.
    let b = append_at_level(&mut doc, None, 2, 0);
    doc.insert_signatures([(b, sig(&k, &b))]);
    assert!(!is_accepted(&mut doc, &b));
    assert_eq!(drafts(&mut doc), [a, b]);
    let before = format!("{doc:?}");
    assert!(matches!(
        doc.fork_with_author(Some(author(&k2))),
        Err(AutomergeError::Signing(
            SigningError::UnpublishedLocalChanges
        ))
    ));
    assert_eq!(format!("{doc:?}"), before);

    // Required mode never permits removing the author.
    assert!(matches!(
        doc.fork_with_author(None),
        Err(AutomergeError::SigningAuthor(_))
    ));
    // An invalid key is reported as a key error even before the draft gate.
    for invalid in invalid_authors() {
        assert!(matches!(
            doc.fork_with_author(Some(invalid)),
            Err(AutomergeError::SigningAuthor(_))
        ));
    }
    assert_eq!(format!("{doc:?}"), before);
}

#[test]
fn fully_published_fork_with_new_author_keeps_old_proofs_and_uses_fresh_actor() {
    let k = key(7);
    let k2 = key(8);
    let mut doc = Automerge::signing(author(&k))
        .unwrap()
        .with_actor(ActorId::from([1]));
    let a = append_at_level(&mut doc, None, 1, 0);
    doc.insert_signatures([(a, sig(&k, &a))]);
    assert!(is_accepted(&mut doc, &a));

    let mut fork = doc.fork_with_author(Some(author(&k2))).unwrap();
    assert_eq!(fork.get_author(), Some(&author(&k2)));
    assert_eq!(doc.get_author(), Some(&author(&k)));
    assert_ne!(fork.get_actor(), doc.get_actor());
    assert!(fork.signing.is_required());
    assert!(is_accepted(&mut fork, &a));
    assert_eq!(
        fork.get_change_by_hash(&a).unwrap().author(),
        Some(author(&k))
    );
    assert!(drafts(&mut fork).is_empty());

    let b = append_at_level(&mut fork, None, 2, 0);
    let change = fork.get_change_by_hash(&b).unwrap();
    assert_eq!(change.author(), Some(author(&k2)));
    assert_ne!(change.actor_id(), &ActorId::from([1]));
    assert_eq!(drafts(&mut fork), [b]);
    assert!(fork.has_unpublished_changes());
    // The old author cannot sign the new author's change.
    assert!(insert_one(&mut fork, b, sig(&k, &b)).is_err());
    fork.insert_signatures([(b, sig(&k2, &b))]);
    assert!(is_accepted(&mut fork, &b));

    // Reloading the published projection verifies both authors' proofs.
    let bytes = fork.bundle_fragments(fork.fragments(..)).concat();
    let mut reloaded = Automerge::load_with_options(
        &bytes,
        LoadOptions::new().signing().author(author(&key(200))),
    )
    .unwrap();
    assert_eq!(reloaded.get_heads(), fork.get_heads());
    assert!(is_accepted(&mut reloaded, &a));
    assert!(is_accepted(&mut reloaded, &b));

    // Later delivery to the original document admits the new author's work.
    let b_bytes = attach(
        fork.get_change_by_hash(&b).unwrap().raw_bytes(),
        [(b, Signature::ed25519(sig(&k2, &b)))],
    );
    doc.load_incremental(&b_bytes).unwrap();
    assert!(is_accepted(&mut doc, &b));
    assert_eq!(doc.get_heads(), fork.get_heads());
}

#[test]
fn unsigned_fork_with_author_changes_or_removes_the_author_freely() {
    let arbitrary = Author::from(vec![1, 2, 3]);
    let mut doc =
        Automerge::new_with_author(Some(arbitrary.clone())).with_actor(ActorId::from([1]));
    append_at_level(&mut doc, None, 1, 0);
    let removed = doc.fork_with_author(None).unwrap();
    assert!(removed.get_author().is_none());
    assert!(!removed.signing.is_required());
    let replaced = doc.fork_with_author(Some(weak_author())).unwrap();
    assert_eq!(replaced.get_author(), Some(&weak_author()));
    assert_ne!(replaced.get_actor(), doc.get_actor());
    assert_eq!(doc.get_author(), Some(&arbitrary));
}

#[test]
fn incoming_pending_groups_do_not_block_a_new_author_fork() {
    let k = key(7);
    let k2 = key(8);
    let mut doc = Automerge::signing(author(&k))
        .unwrap()
        .with_actor(ActorId::from([1]));
    let a = append_at_level(&mut doc, None, 1, 0);
    doc.insert_signatures([(a, sig(&k, &a))]);

    // A foreign author extends `a` twice on an unsigned replica of the same
    // history; only the tip is delivered, so it must wait for `b`.
    let mut remote = Automerge::new_with_author(Some(author(&k2))).with_actor(ActorId::from([2]));
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
    assert!(doc.signing.has_pending_groups());
    assert!(drafts(&mut doc).is_empty());

    let k3 = key(9);
    let mut fork = doc.fork_with_author(Some(author(&k3))).unwrap();
    assert_eq!(fork.get_author(), Some(&author(&k3)));
    assert!(fork.signing.has_pending_groups());
    // The retained group still completes once `b` arrives.
    let b_bytes = attach(
        remote.get_change_by_hash(&b).unwrap().raw_bytes(),
        [(b, Signature::ed25519(sig(&k2, &b)))],
    );
    fork.load_incremental(&b_bytes).unwrap();
    assert!(is_accepted(&mut fork, &b));
    assert!(is_accepted(&mut fork, &c));
}

#[test]
fn required_fork_at_remains_unsupported_in_both_apis() {
    let k = key(7);
    let mut doc = Automerge::signing(author(&k)).unwrap();
    let a = append_at_level(&mut doc, None, 1, 0);
    doc.insert_signatures([(a, sig(&k, &a))]);
    assert!(matches!(
        doc.fork_at(&[a]),
        Err(AutomergeError::Signing(SigningError::ForkAtNotSupported))
    ));
    // AutoCommit rejects before closing its pending transaction.
    let mut auto = AutoCommit::signing(author(&k)).unwrap();
    auto.put(ROOT, "pending", 1).unwrap();
    assert!(matches!(
        auto.fork_at(&[]),
        Err(AutomergeError::Signing(SigningError::ForkAtNotSupported))
    ));
    assert_eq!(auto.pending_ops(), 1, "transaction stays open");
    assert!(!auto.has_unpublished_changes(), "nothing was committed");
    // Unsigned fork_at still commits pending edits first.
    let mut unsigned = AutoCommit::new();
    unsigned.put(ROOT, "pending", 1).unwrap();
    let mut fork = unsigned.fork_at(&[]).unwrap();
    assert_eq!(unsigned.pending_ops(), 0);
    assert_eq!(unsigned.get_heads().len(), 1);
    assert!(fork.get_heads().is_empty());
}

#[test]
fn autocommit_rejects_different_author_fork_before_closing_pending_edits() {
    let k = key(7);
    let k2 = key(8);
    let mut doc = AutoCommit::signing(author(&k)).unwrap();
    doc.put(ROOT, "pending", 1).unwrap();
    assert_eq!(doc.pending_ops(), 1);
    assert!(matches!(
        doc.fork_with_author(Some(author(&k2))),
        Err(AutomergeError::Signing(
            SigningError::UnpublishedLocalChanges
        ))
    ));
    assert_eq!(doc.pending_ops(), 1, "transaction stays open");
    assert!(doc.doc.get_heads().is_empty());
    assert_eq!(doc.get_author(), Some(&author(&k)));

    // Same author: normal fork semantics, transaction closed as usual.
    let mut fork = doc.fork_with_author(Some(author(&k))).unwrap();
    assert_eq!(doc.pending_ops(), 0);
    assert_eq!(fork.get_heads(), doc.get_heads());
    assert_eq!(fork.get_author(), Some(&author(&k)));
    assert_ne!(fork.get_actor(), doc.get_actor());

    // Unsigned: pending edits are committed into the source, then forked.
    let mut unsigned = AutoCommit::new_with_author(Some(Author::from(vec![1])));
    unsigned.put(ROOT, "pending", 1).unwrap();
    let mut fork = unsigned
        .fork_with_author(Some(Author::from(vec![2])))
        .unwrap();
    assert_eq!(unsigned.pending_ops(), 0);
    assert_eq!(fork.get_heads(), unsigned.get_heads());
    assert_eq!(fork.get_author(), Some(&Author::from(vec![2])));

    // Published required document: fork proceeds with a fresh actor.
    let mut published = AutoCommit::signing(author(&k)).unwrap();
    published.put(ROOT, "x", 1).unwrap();
    let hash = published.commit().unwrap();
    published.insert_signatures([(hash, sig(&k, &hash))]);
    assert!(!published.has_unpublished_changes());
    published.put(ROOT, "y", 2).unwrap();
    assert!(matches!(
        published.fork_with_author(Some(author(&k2))),
        Err(AutomergeError::Signing(
            SigningError::UnpublishedLocalChanges
        ))
    ));
    assert_eq!(published.pending_ops(), 1);
    published.rollback();
    let fork = published.fork_with_author(Some(author(&k2))).unwrap();
    assert_eq!(fork.get_author(), Some(&author(&k2)));
    assert_ne!(fork.get_actor(), published.get_actor());
}
