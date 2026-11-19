//! Batch signature receipt, coordinated completion, and error-boundary tests.

use crate::storage::signatures::{HashEntries, SignaturesChunk};
use crate::{
    AutoCommit, PublicationError, Signature, SignatureBatchOutcome, SignatureInsertError,
    SignatureInsertOutcome, SignatureRejection,
};

use super::*;

fn attach(content: &[u8], entries: impl IntoIterator<Item = (ChangeHash, Signature)>) -> Vec<u8> {
    let chunk = SignaturesChunk::encode(HashEntries::canonicalize(entries).unwrap());
    [content, chunk.bytes()].concat()
}

fn insertions(outcome: &SignatureBatchOutcome) -> &[SignatureInsertOutcome] {
    match outcome {
        SignatureBatchOutcome::Completed { insertions }
        | SignatureBatchOutcome::PublicationFailed { insertions, .. } => insertions,
        SignatureBatchOutcome::Rejected { error } => panic!("unexpected rejection: {error}"),
    }
}

fn is_accepted(doc: &mut Automerge, hash: &ChangeHash) -> bool {
    doc.try_signing().unwrap().is_accepted(hash)
}

/// Local author `k1` (actor 1) with unsigned loose `a`, plus a signed loose `b`
/// by author `k2` (actor 2) depending on `a`, delivered as an incoming group.
fn local_a_with_pending_b() -> (Automerge, SigningKey, [ChangeHash; 2]) {
    let k1 = key(1);
    let k2 = key(2);
    let mut doc = Automerge::signing(author(&k1))
        .unwrap()
        .with_actor(ActorId::from([1]));
    let a = append_at_level(&mut doc, None, 1, 0);

    let mut remote = doc.clone();
    remote.set_author(Some(author(&k2))).unwrap();
    remote.set_actor(ActorId::from([2]));
    let b = append_at_level(&mut remote, Some(&[a]), 2, 0);
    let b_bytes = attach(
        remote.get_change_by_hash(&b).unwrap().raw_bytes(),
        [(b, Signature::ed25519(sig(&k2, &b)))],
    );
    assert_eq!(doc.load_incremental(&b_bytes).unwrap(), 0);
    assert!(doc.signing.has_pending_groups());
    assert_eq!(doc.get_heads(), [a]);
    (doc, k1, [a, b])
}

#[test]
fn mixed_batch_reports_every_owned_pair_and_continues_after_errors() {
    let k = key(7);
    let mut doc = Automerge::signing(author(&k))
        .unwrap()
        .with_actor(ActorId::from([1]));
    let a = append_at_level(&mut doc, None, 1, 0);
    let b = append_at_level(&mut doc, None, 2, 0);
    let c = append_at_level(&mut doc, None, 3, 0);
    let unknown = ChangeHash([0xff; 32]);
    let bad = sig(&key(9), &b);

    let inputs = vec![
        (unknown, sig(&k, &unknown)),
        (b, bad),
        (a, sig(&k, &a)),
        (a, sig(&k, &a)),
        (c, sig(&k, &c)),
    ];
    let outcome = doc.insert_signatures(inputs.clone());
    let results = insertions(&outcome);
    assert_eq!(results.len(), inputs.len());
    for (result, (hash, signature)) in results.iter().zip(&inputs) {
        assert_eq!(result.hash, *hash);
        assert_eq!(result.signature, *signature);
    }
    assert!(matches!(
        results[0].result,
        Err(SignatureInsertError::UnknownChange { change }) if change == unknown
    ));
    assert!(matches!(
        results[1].result,
        Err(SignatureInsertError::Verification { change, .. }) if change == b
    ));
    assert!(matches!(results[2].result, Ok(Insert::Stored)));
    assert!(matches!(results[3].result, Ok(Insert::Duplicate)));
    assert!(matches!(results[4].result, Ok(Insert::Stored)));
    assert!(matches!(outcome, SignatureBatchOutcome::Completed { .. }));

    // Completion ran once: a is published, b/c wait on the gap at b.
    assert!(is_accepted(&mut doc, &a));
    assert!(!is_accepted(&mut doc, &b));
    assert!(!is_accepted(&mut doc, &c));
    // c is signed but unpublished: still tracked, no longer requested.
    assert_eq!(doc.try_signing().unwrap().required_signatures(), [b]);
    assert_eq!(doc.try_signing().unwrap().state.local_unsigned, [b, c]);

    // Closing the gap publishes both in one operation.
    let outcome = doc.insert_signatures([(b, sig(&k, &b))]);
    assert!(matches!(insertions(&outcome)[0].result, Ok(Insert::Stored)));
    assert!(is_accepted(&mut doc, &b));
    assert!(is_accepted(&mut doc, &c));
    assert!(doc.try_signing().unwrap().required_signatures().is_empty());
}

#[test]
fn wrong_mode_rejects_without_consuming_inputs_or_mutating() {
    let mut doc = Automerge::new().with_actor(ActorId::from([1]));
    let a = append_at_level(&mut doc, None, 1, 0);
    let before = doc.save_nocompress();
    let mut consumed = 0usize;
    let inputs = std::iter::from_fn(|| {
        consumed += 1;
        Some((a, sig(&key(1), &a)))
    })
    .take(3);
    let outcome = doc.insert_signatures(inputs);
    assert!(matches!(
        outcome,
        SignatureBatchOutcome::Rejected {
            error: SignatureRejection::Mode(_)
        }
    ));
    assert_eq!(consumed, 0);
    assert_eq!(doc.save_nocompress(), before);
}

#[test]
fn foreign_patch_log_rejects_before_processing_and_preserves_log() {
    let k = key(7);
    let mut doc = Automerge::signing(author(&k))
        .unwrap()
        .with_actor(ActorId::from([1]));
    let a = append_at_level(&mut doc, None, 1, 0);
    let before = doc.save_nocompress();

    let mut foreign = Automerge::new().with_actor(ActorId::from([99]));
    let mut log = PatchLog::active();
    let mut tx = foreign.transaction_log_patches(log).unwrap();
    tx.put(ROOT, "foreign", 9).unwrap();
    log = tx.commit().1;
    let expected = foreign.make_patches(&mut log.clone());
    assert!(!expected.is_empty());

    let mut consumed = 0usize;
    let inputs = std::iter::from_fn(|| {
        consumed += 1;
        Some((a, sig(&k, &a)))
    })
    .take(1);
    let outcome = doc.insert_signatures_log_patches(inputs, &mut log);
    assert!(matches!(
        outcome,
        SignatureBatchOutcome::Rejected {
            error: SignatureRejection::PatchLog(_)
        }
    ));
    assert_eq!(consumed, 0);
    assert_eq!(doc.save_nocompress(), before);
    assert!(!is_accepted(&mut doc, &a));
    assert_eq!(foreign.make_patches(&mut log), expected);
}

#[test]
fn autocommit_checks_mode_before_closing_pending_transaction() {
    let mut unsigned = AutoCommit::new().with_actor(ActorId::from([1]));
    unsigned.put(ROOT, "value", 1).unwrap();
    let a = unsigned.commit().unwrap();
    unsigned.put(ROOT, "value", 2).unwrap();
    assert_eq!(unsigned.pending_ops(), 1);
    let outcome = unsigned.insert_signatures([(a, sig(&key(1), &a))]);
    assert!(matches!(
        outcome,
        SignatureBatchOutcome::Rejected {
            error: SignatureRejection::Mode(_)
        }
    ));
    assert_eq!(unsigned.pending_ops(), 1, "pending operations stay open");
    // `get_heads` closes the pending transaction, so check it afterwards.
    assert_eq!(unsigned.get_heads().len(), 1);
    assert_ne!(unsigned.get_heads(), [a]);

    // Required mode: the pending transaction is closed, then the batch runs
    // and patches for incoming completion are collected normally.
    let k = key(7);
    let mut signed = AutoCommit::signing(author(&k))
        .unwrap()
        .with_actor(ActorId::from([1]));
    signed.put(ROOT, "value", 1).unwrap();
    let a = signed.commit().unwrap();
    signed.put(ROOT, "value", 2).unwrap();
    let outcome = signed.insert_signatures([(a, sig(&k, &a))]);
    assert!(matches!(outcome, SignatureBatchOutcome::Completed { .. }));
    assert_eq!(signed.pending_ops(), 0);
    assert_eq!(signed.get_heads().len(), 1);
    assert_ne!(signed.get_heads(), [a]);
    assert!(signed.try_signing().unwrap().is_accepted(&a));
}

#[test]
fn local_publication_applies_causally_enabled_pending_group_with_patches() {
    let (mut doc, k1, [a, b]) = local_a_with_pending_b();

    // An unrelated pending group (actor 3, depends on unknown history) must survive.
    let k3 = key(3);
    let mut other = Automerge::signing(author(&k3))
        .unwrap()
        .with_actor(ActorId::from([3]));
    let o1 = append_at_level(&mut other, None, 10, 0);
    let o2 = append_at_level(&mut other, None, 11, 0);
    let o2_bytes = attach(
        other.get_change_by_hash(&o2).unwrap().raw_bytes(),
        [(o2, Signature::ed25519(sig(&k3, &o2)))],
    );
    assert_eq!(doc.load_incremental(&o2_bytes).unwrap(), 0);
    assert_eq!(doc.signing.queued_changes().count(), 2);

    let mut log = PatchLog::active();
    let outcome = doc.insert_signatures_log_patches([(a, sig(&k1, &a))], &mut log);
    assert!(matches!(outcome, SignatureBatchOutcome::Completed { .. }));
    assert!(matches!(insertions(&outcome)[0].result, Ok(Insert::Stored)));
    assert!(is_accepted(&mut doc, &a));
    assert!(is_accepted(&mut doc, &b));
    assert_eq!(doc.get_heads(), [b]);
    assert_eq!(doc.get(ROOT, "value").unwrap().unwrap().0, 2.into());
    let patches = doc.make_patches(&mut log);
    assert_eq!(
        patches.len(),
        1,
        "b's application is observed in the same operation"
    );
    assert_eq!(
        doc.signing
            .queued_changes()
            .map(Change::hash)
            .collect::<Vec<_>>(),
        [o2]
    );
    assert_eq!(doc.get_missing_deps(&[]), [o1]);
    assert!(doc.try_signing().unwrap().required_signatures().is_empty());

    // Repeated and empty batches make no further progress and reapply nothing.
    let mut log = PatchLog::active();
    let heads = doc.get_heads();
    let saved = doc.save_nocompress();
    let outcome = doc.insert_signatures_log_patches([(a, sig(&k1, &a))], &mut log);
    assert!(matches!(
        insertions(&outcome)[0].result,
        Ok(Insert::Duplicate)
    ));
    let outcome = doc.insert_signatures_log_patches(std::iter::empty(), &mut log);
    assert!(matches!(
        outcome,
        SignatureBatchOutcome::Completed { insertions } if insertions.is_empty()
    ));
    assert_eq!(doc.get_heads(), heads);
    assert_eq!(doc.save_nocompress(), saved);
    let again = doc.make_patches(&mut log);
    assert!(again.is_empty(), "no reapply patches: {again:?}");
    assert_eq!(doc.signing.queued_changes().count(), 1);
}

#[test]
fn empty_and_all_error_batches_still_attempt_completion() {
    // A pending group whose dependency is published *locally* before any
    // batch call: the next batch, even with no usable signature, completes it.
    let (mut doc, k1, [a, b]) = local_a_with_pending_b();
    sign_doc(&mut doc, a, &k1);
    assert!(is_accepted(&mut doc, &a));
    // The scalar adapter publishes locally but cannot apply operations; the
    // next batch call, even empty, completes the enabled group.
    assert!(!is_accepted(&mut doc, &b));
    let outcome = doc.insert_signatures(std::iter::empty());
    assert!(matches!(outcome, SignatureBatchOutcome::Completed { .. }));
    assert!(is_accepted(&mut doc, &b));
    assert_eq!(doc.get_heads(), [b]);

    let (mut doc, _k1, [a, b]) = local_a_with_pending_b();
    let unknown = ChangeHash([0xee; 32]);
    let outcome = doc.insert_signatures([(unknown, sig(&key(5), &unknown))]);
    assert!(matches!(
        insertions(&outcome)[0].result,
        Err(SignatureInsertError::UnknownChange { .. })
    ));
    assert!(!is_accepted(&mut doc, &a));
    assert!(!is_accepted(&mut doc, &b));
    assert!(doc.signing.has_pending_groups());
    let outcome = doc.insert_signatures(std::iter::empty());
    assert!(matches!(outcome, SignatureBatchOutcome::Completed { .. }));
    assert!(doc.signing.has_pending_groups());
    assert_eq!(doc.get_heads(), [a]);
}

#[test]
fn historical_author_is_verified_not_current_author() {
    let k1 = key(1);
    let k2 = key(2);
    let mut doc = Automerge::signing(author(&k1))
        .unwrap()
        .with_actor(ActorId::from([1]));
    let a = append_at_level(&mut doc, None, 1, 0);
    doc.set_author(Some(author(&k2))).unwrap();
    doc.set_actor(ActorId::from([2]));
    let b = append_at_level(&mut doc, None, 2, 0);

    // Current author is k2; a was authored by k1 and must verify under k1 only.
    let outcome = doc.insert_signatures([(a, sig(&k2, &a)), (a, sig(&k1, &a)), (b, sig(&k2, &b))]);
    let results = insertions(&outcome);
    assert!(matches!(
        results[0].result,
        Err(SignatureInsertError::Verification { change, .. }) if change == a
    ));
    assert!(matches!(results[1].result, Ok(Insert::Stored)));
    assert!(matches!(results[2].result, Ok(Insert::Stored)));
    assert!(is_accepted(&mut doc, &a));
    assert!(is_accepted(&mut doc, &b));
}

#[test]
fn missing_or_invalid_historical_author_is_a_per_entry_error() {
    let k = key(7);
    let mut receiver = Automerge::signing(author(&k))
        .unwrap()
        .with_actor(ActorId::from([1]));
    let mine = append_at_level(&mut receiver, None, 1, 0);

    let mut authorless = Automerge::new().with_actor(ActorId::from([3]));
    let none = append_at_level(&mut authorless, None, 2, 0);
    let mut bad_key = Automerge::new()
        .with_author(Some(Author::from(vec![1, 2, 3])))
        .unwrap()
        .with_actor(ActorId::from([4]));
    let bad = append_at_level(&mut bad_key, None, 3, 0);
    for (doc, hash) in [(&authorless, none), (&bad_key, bad)] {
        // Inject applied history directly, as imported changes with these
        // authors could never pass admission.
        let change = doc.get_change_by_hash(&hash).unwrap();
        receiver.put_actor(change.actor_id().clone());
        let _ = crate::automerge::history::History::new(
            &mut receiver.change_graph,
            &mut receiver.deps,
            &mut receiver.authors,
            &receiver.ops.actors,
            &receiver.visibility,
            &mut receiver.signing,
        )
        .update(&change);
    }

    let outcome = receiver.insert_signatures([
        (none, sig(&k, &none)),
        (bad, sig(&k, &bad)),
        (mine, sig(&k, &mine)),
    ]);
    let results = insertions(&outcome);
    assert!(matches!(
        results[0].result,
        Err(SignatureInsertError::MissingAuthor { change, .. }) if change == none
    ));
    assert!(matches!(
        results[1].result,
        Err(SignatureInsertError::InvalidAuthorKey { change, .. }) if change == bad
    ));
    assert!(matches!(results[2].result, Ok(Insert::Stored)));
    assert!(is_accepted(&mut receiver, &mine));
}

#[test]
fn simultaneously_admissible_pending_equivocation_fails_publication_but_retains_state() {
    // Local a unsigned. Two honest-looking pending groups from the same
    // actor/seq (equivocation) both depend on a. Each is individually valid
    // and stays pending until a publishes; then both become admissible at once.
    let k1 = key(1);
    let k2 = key(2);
    let mut doc = Automerge::signing(author(&k1))
        .unwrap()
        .with_actor(ActorId::from([1]));
    let a = append_at_level(&mut doc, None, 1, 0);

    let mut branch = |value: i64| {
        let mut remote = doc.clone();
        remote.set_author(Some(author(&k2))).unwrap();
        remote.set_actor(ActorId::from([2]));
        let h = append_at_level(&mut remote, Some(&[a]), value, 0);
        attach(
            remote.get_change_by_hash(&h).unwrap().raw_bytes(),
            [(h, Signature::ed25519(sig(&k2, &h)))],
        )
    };
    let left = branch(2);
    let right = branch(3);
    assert_eq!(doc.load_incremental(&left).unwrap(), 0);
    assert_eq!(doc.load_incremental(&right).unwrap(), 0);
    assert_eq!(doc.signing.queued_changes().count(), 2);

    let mut log = PatchLog::active();
    let outcome = doc.insert_signatures_log_patches([(a, sig(&k1, &a))], &mut log);
    let SignatureBatchOutcome::PublicationFailed { insertions, error } = &outcome else {
        panic!("expected PublicationFailed, got {outcome:?}");
    };
    assert_eq!(insertions.len(), 1);
    assert!(matches!(insertions[0].result, Ok(Insert::Stored)));
    assert!(matches!(error, PublicationError::Admission(_)));
    // Evidence and local publication retained; no half-application; pending kept.
    assert!(is_accepted(&mut doc, &a));
    assert_eq!(doc.get_heads(), [a]);
    assert_eq!(doc.signing.queued_changes().count(), 2);
    assert!(doc.make_patches(&mut log).is_empty());
    assert!(doc.try_signing().unwrap().required_signatures().is_empty());

    // Unchanged retry fails the same way without choosing a winner.
    let outcome = doc.insert_signatures(std::iter::empty());
    assert!(matches!(
        outcome,
        SignatureBatchOutcome::PublicationFailed { .. }
    ));
    assert_eq!(doc.get_heads(), [a]);
    assert_eq!(doc.signing.queued_changes().count(), 2);
}

#[test]
fn published_frontier_with_reclaimed_signature_does_not_prove_a_new_candidate() {
    // Published F = {a} (loose, level 0) whose own signature has been reclaimed
    // by a published cover G = {a, c} ⊢ b. A later hard fragment H headed by d
    // with frontiers {d, x} where x is an unsigned local loose change in a
    // *different* actor must not publish just because a is accepted.
    let ka = key(7);
    let kb = key(8);
    let mut doc = Automerge::signing(author(&ka))
        .unwrap()
        .with_actor(ActorId::from([1]));
    let a = append_at_level(&mut doc, None, 1, 0);
    sign_doc(&mut doc, a, &ka);
    doc.set_author(Some(author(&kb))).unwrap();
    doc.set_actor(ActorId::from([2]));
    let b = append_at_level(&mut doc, Some(&[]), 2, 1);
    doc.set_author(Some(author(&ka))).unwrap();
    doc.set_actor(ActorId::from([1]));
    let c = append_at_level(&mut doc, Some(&[a, b]), 3, 1);
    sign_doc(&mut doc, c, &ka);
    sign_doc(&mut doc, b, &kb);
    assert!(is_accepted(&mut doc, &c));
    assert_eq!(signature_count(&mut doc, &a), 0, "a's proof was reclaimed");

    // New incomplete candidate: x (actor 2, loose, unsigned) then d (actor 1,
    // level 1) depending on [c, x]. H = {d, x} needs sig(d) and sig(x).
    doc.set_author(Some(author(&kb))).unwrap();
    doc.set_actor(ActorId::from([2]));
    let x = append_at_level(&mut doc, Some(&[b]), 4, 0);
    doc.set_author(Some(author(&ka))).unwrap();
    doc.set_actor(ActorId::from([1]));
    let d = append_at_level(&mut doc, Some(&[c, x]), 5, 1);
    let h = doc.change_graph.get_historical_fragment(d).unwrap();
    assert!(h.members.contains(&x));

    let outcome = doc.insert_signatures([(d, sig(&ka, &d))]);
    assert!(matches!(insertions(&outcome)[0].result, Ok(Insert::Stored)));
    assert!(
        !is_accepted(&mut doc, &d),
        "unproved group must not publish"
    );
    assert!(!is_accepted(&mut doc, &x));
    assert!(doc.get_fragment(d).is_none());
    // Supplying x's exact frontier signature completes H.
    let outcome = doc.insert_signatures([(x, sig(&kb, &x))]);
    assert!(matches!(insertions(&outcome)[0].result, Ok(Insert::Stored)));
    assert!(is_accepted(&mut doc, &d));
    assert!(is_accepted(&mut doc, &x));
}

fn signature_count(doc: &mut Automerge, hash: &ChangeHash) -> usize {
    doc.try_signing()
        .unwrap()
        .state
        .signatures
        .ed25519_entries(hash)
        .count()
}
