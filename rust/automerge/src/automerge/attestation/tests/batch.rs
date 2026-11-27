//! Batch signature receipt, coordinated completion, and error-boundary tests.

use crate::{
    AutoCommit, PublicationError, Signature, SignatureBatchOutcome, SignatureInsertError,
    SignatureInsertOutcome, SignatureRejection,
};

use super::*;

fn insertions(outcome: &SignatureBatchOutcome) -> &[SignatureInsertOutcome] {
    match outcome {
        SignatureBatchOutcome::Completed { insertions }
        | SignatureBatchOutcome::PublicationFailed { insertions, .. } => insertions,
        SignatureBatchOutcome::Rejected { error } => panic!("unexpected rejection: {error}"),
    }
}

fn local_drafts(doc: &mut Automerge) -> Vec<ChangeHash> {
    doc.try_attested()
        .unwrap()
        .state
        .local_drafts
        .iter()
        .collect()
}

/// Three unsigned loose changes `a -> b -> c` authored locally by `key`.
fn three_loose_drafts(key: &SigningKey) -> (Automerge, [ChangeHash; 3]) {
    let mut doc = Automerge::from_config(crate::Config::default().attested(author(key)).unwrap())
        .with_actor(ActorId::from([1]))
        .unwrap();
    let a = append_at_level(&mut doc, None, 1, 0);
    let b = append_at_level(&mut doc, None, 2, 0);
    let c = append_at_level(&mut doc, None, 3, 0);
    (doc, [a, b, c])
}

/// Local author `k1` (actor 1) with unsigned loose `a`, plus a signed loose `b`
/// by author `k2` (actor 2) depending on `a`, delivered as an incoming group.
fn local_a_with_pending_b() -> (Automerge, SigningKey, [ChangeHash; 2]) {
    let k1 = key(1);
    let k2 = key(2);
    let mut doc = Automerge::from_config(crate::Config::default().attested(author(&k1)).unwrap())
        .with_actor(ActorId::from([1]))
        .unwrap();
    let a = append_at_level(&mut doc, None, 1, 0);

    let mut remote = replica(&doc, author(&k2), ActorId::from([2]));
    let b = append_at_level(&mut remote, Some(&[a]), 2, 0);
    let b_bytes = attach(
        remote.get_change_by_hash(&b).unwrap().raw_bytes(),
        [(b, Signature::ed25519(sig(&k2, &b)))],
    );
    assert_eq!(doc.load_incremental(&b_bytes).unwrap(), 0);
    assert!(doc.attestation.has_pending_groups());
    assert_eq!(doc.get_heads(), [a]);
    (doc, k1, [a, b])
}

/// [`local_a_with_pending_b`] plus an unrelated pending group `{o2}` from
/// author `k3` (actor 3) whose dependency `o1` is unknown to the document, so
/// it can never be enabled by local publication.
fn local_a_with_pending_b_and_unrelated_group(
) -> (Automerge, SigningKey, [ChangeHash; 2], [ChangeHash; 2]) {
    let (mut doc, k1, [a, b]) = local_a_with_pending_b();
    let k3 = key(3);
    let mut other = Automerge::from_config(crate::Config::default().attested(author(&k3)).unwrap())
        .with_actor(ActorId::from([3]))
        .unwrap();
    let o1 = append_at_level(&mut other, None, 10, 0);
    let o2 = append_at_level(&mut other, None, 11, 0);
    let o2_bytes = attach(
        other.get_change_by_hash(&o2).unwrap().raw_bytes(),
        [(o2, Signature::ed25519(sig(&k3, &o2)))],
    );
    assert_eq!(doc.load_incremental(&o2_bytes).unwrap(), 0);
    assert_eq!(doc.attestation.queued_changes().count(), 2);
    (doc, k1, [a, b], [o1, o2])
}

/// Injects applied history for `hash` from `source`, bypassing admission:
/// changes with a missing or malformed author could never be imported.
fn graft_applied_change(receiver: &mut Automerge, source: &Automerge, hash: &ChangeHash) {
    let change = source.get_change_by_hash(hash).unwrap();
    receiver.put_actor(change.actor_id().clone());
    let _ = crate::automerge::history::History::new(
        &mut receiver.change_graph,
        &mut receiver.deps,
        &mut receiver.authors,
        &receiver.ops.actors,
        &receiver.visibility,
        &mut receiver.attestation,
    )
    .update(&change);
}

#[test]
fn batch_outcome_reports_one_result_per_input_in_input_order() {
    let k = key(7);
    let (mut doc, [a, b, c]) = three_loose_drafts(&k);
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
}

#[test]
fn batch_completion_publishes_the_signed_prefix_and_keeps_signed_but_blocked_drafts_tracked() {
    let k = key(7);
    let (mut doc, [a, b, c]) = three_loose_drafts(&k);

    doc.insert_signatures([(a, sig(&k, &a)), (c, sig(&k, &c))]);

    // Completion ran once: a is published, b/c wait on the gap at b.
    assert!(is_published(&mut doc, &a));
    assert!(!is_published(&mut doc, &b));
    assert!(!is_published(&mut doc, &c));
    // c is signed but unpublished: still tracked, no longer requested.
    assert_eq!(requested_hashes(&mut doc), [b]);
    assert_eq!(local_drafts(&mut doc), [b, c]);
}

#[test]
fn closing_the_gap_publishes_every_signed_successor_in_one_batch() {
    let k = key(7);
    let (mut doc, [a, b, c]) = three_loose_drafts(&k);
    doc.insert_signatures([(a, sig(&k, &a)), (c, sig(&k, &c))]);

    let outcome = doc.insert_signatures([(b, sig(&k, &b))]);
    assert!(matches!(insertions(&outcome)[0].result, Ok(Insert::Stored)));
    assert!(is_published(&mut doc, &b));
    assert!(is_published(&mut doc, &c));
    assert!(requested_hashes(&mut doc).is_empty());
}

#[test]
fn wrong_mode_rejects_without_consuming_inputs_or_mutating() {
    let mut doc = Automerge::new().with_actor(ActorId::from([1])).unwrap();
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
    let mut doc = Automerge::from_config(crate::Config::default().attested(author(&k)).unwrap())
        .with_actor(ActorId::from([1]))
        .unwrap();
    let a = append_at_level(&mut doc, None, 1, 0);
    let before = doc.save_nocompress();

    let mut foreign = Automerge::new().with_actor(ActorId::from([99])).unwrap();
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
    assert!(!is_published(&mut doc, &a));
    assert_eq!(foreign.make_patches(&mut log), expected);
}

#[test]
fn autocommit_checks_mode_before_closing_pending_transaction() {
    let mut unsigned = AutoCommit::new().with_actor(ActorId::from([1])).unwrap();
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

    // An attested document: the pending transaction is closed, then the batch runs
    // and patches for incoming completion are collected normally.
    let k = key(7);
    let mut signed =
        AutoCommit::from_config(crate::Config::default().attested(author(&k)).unwrap())
            .with_actor(ActorId::from([1]))
            .unwrap();
    signed.put(ROOT, "value", 1).unwrap();
    let a = signed.commit().unwrap();
    signed.put(ROOT, "value", 2).unwrap();
    let outcome = signed.insert_signatures([(a, sig(&k, &a))]);
    assert!(matches!(outcome, SignatureBatchOutcome::Completed { .. }));
    assert_eq!(signed.pending_ops(), 0);
    assert_eq!(signed.get_heads().len(), 1);
    assert_ne!(signed.get_heads(), [a]);
    assert!(signed.doc.try_attested().unwrap().is_published(&a));
}

#[test]
fn local_publication_applies_causally_enabled_pending_group_with_patches() {
    let (mut doc, k1, [a, b], [o1, o2]) = local_a_with_pending_b_and_unrelated_group();

    let mut log = PatchLog::active();
    let outcome = doc.insert_signatures_log_patches([(a, sig(&k1, &a))], &mut log);
    assert!(matches!(outcome, SignatureBatchOutcome::Completed { .. }));
    assert!(matches!(insertions(&outcome)[0].result, Ok(Insert::Stored)));
    assert!(is_published(&mut doc, &a));
    assert!(is_published(&mut doc, &b));
    assert_eq!(doc.get_heads(), [b]);
    assert_eq!(doc.get(ROOT, "value").unwrap().unwrap().0, 2.into());
    let patches = doc.make_patches(&mut log);
    assert_eq!(
        patches.len(),
        1,
        "b's application is observed in the same operation"
    );
    // The unrelated group is untouched: still queued, still missing its dep.
    assert_eq!(
        doc.attestation
            .queued_changes()
            .map(Change::hash)
            .collect::<Vec<_>>(),
        [o2]
    );
    assert_eq!(doc.get_missing_deps(&[]), [o1]);
    assert!(requested_hashes(&mut doc).is_empty());
}

#[test]
fn repeating_a_published_signature_reports_duplicate_without_reapplying() {
    let (mut doc, k1, [a, _b], _unrelated) = local_a_with_pending_b_and_unrelated_group();
    doc.insert_signatures([(a, sig(&k1, &a))]);
    let heads = doc.get_heads();
    let saved = doc.save_nocompress();

    let mut log = PatchLog::active();
    let outcome = doc.insert_signatures_log_patches([(a, sig(&k1, &a))], &mut log);
    assert!(matches!(
        insertions(&outcome)[0].result,
        Ok(Insert::Duplicate)
    ));
    assert_eq!(doc.get_heads(), heads);
    assert_eq!(doc.save_nocompress(), saved);
    let again = doc.make_patches(&mut log);
    assert!(again.is_empty(), "no reapply patches: {again:?}");
    assert_eq!(doc.attestation.queued_changes().count(), 1);
}

#[test]
fn empty_batch_after_publication_makes_no_further_progress() {
    let (mut doc, k1, [a, _b], _unrelated) = local_a_with_pending_b_and_unrelated_group();
    doc.insert_signatures([(a, sig(&k1, &a))]);
    let heads = doc.get_heads();
    let saved = doc.save_nocompress();

    let mut log = PatchLog::active();
    let outcome = doc.insert_signatures_log_patches(std::iter::empty(), &mut log);
    assert!(matches!(
        outcome,
        SignatureBatchOutcome::Completed { insertions } if insertions.is_empty()
    ));
    assert_eq!(doc.get_heads(), heads);
    assert_eq!(doc.save_nocompress(), saved);
    let again = doc.make_patches(&mut log);
    assert!(again.is_empty(), "no reapply patches: {again:?}");
    assert_eq!(doc.attestation.queued_changes().count(), 1);
}

#[test]
fn empty_batch_still_attempts_completion() {
    // A pending group whose dependency is published *locally* before any batch
    // call: internal receipt and local publication without the completion pass
    // (the state a non-batch publication path would leave behind). The next
    // batch call, even empty, completes the enabled group.
    let (mut doc, k1, [a, b]) = local_a_with_pending_b();
    {
        let mut signing = doc.try_attested().unwrap();
        signing.receive_signature(&a, &sig(&k1, &a)).unwrap();
        signing.publish_local();
    }
    assert!(is_published(&mut doc, &a));
    assert!(!is_published(&mut doc, &b));

    let outcome = doc.insert_signatures(std::iter::empty());
    assert!(matches!(outcome, SignatureBatchOutcome::Completed { .. }));
    assert!(is_published(&mut doc, &b));
    assert_eq!(doc.get_heads(), [b]);
}

#[test]
fn all_error_batch_still_attempts_completion() {
    let (mut doc, _k1, [a, b]) = local_a_with_pending_b();
    let unknown = ChangeHash([0xee; 32]);

    let outcome = doc.insert_signatures([(unknown, sig(&key(5), &unknown))]);
    assert!(matches!(
        insertions(&outcome)[0].result,
        Err(SignatureInsertError::UnknownChange { .. })
    ));
    assert!(matches!(outcome, SignatureBatchOutcome::Completed { .. }));
    // Nothing was publishable, so the completion pass changed nothing.
    assert!(!is_published(&mut doc, &a));
    assert!(!is_published(&mut doc, &b));
    assert!(doc.attestation.has_pending_groups());
    assert_eq!(doc.get_heads(), [a]);
}

#[test]
#[allow(deprecated)]
fn historical_author_is_verified_not_current_author() {
    let k1 = key(1);
    let k2 = key(2);
    let mut doc = Automerge::from_config(crate::Config::default().attested(author(&k1)).unwrap())
        .with_actor(ActorId::from([1]))
        .unwrap();
    let a = append_at_level(&mut doc, None, 1, 0);
    doc.insert_signatures([(a, sig(&k1, &a))]);
    assert!(is_published(&mut doc, &a));

    // Once fully published the history is forked for author k2, whose change
    // `b` is independent of `a`. Its signature was dropped in transit and is
    // supplied afterwards.
    let mut doc = doc.fork_with_author(Some(author(&k2))).unwrap();
    doc.set_actor(ActorId::from([2])).unwrap();
    let b = append_at_level(&mut doc, Some(&[]), 2, 0);
    // Keep only `b` published-pending: `a` is already published.
    let c = append_at_level(&mut doc, Some(&[a]), 3, 0);

    // Current author is k2; a was authored by k1 and must verify under k1 only,
    // and k2's own changes verify under k2 only.
    let outcome = doc.insert_signatures([(c, sig(&k1, &c)), (b, sig(&k2, &b)), (c, sig(&k2, &c))]);
    let results = insertions(&outcome);
    assert!(matches!(
        results[0].result,
        Err(SignatureInsertError::Verification { change, .. }) if change == c
    ));
    assert!(matches!(results[1].result, Ok(Insert::Stored)));
    assert!(matches!(results[2].result, Ok(Insert::Stored)));
    assert!(is_published(&mut doc, &a));
    assert!(is_published(&mut doc, &b));
    assert!(is_published(&mut doc, &c));
    // Historical `a` is still bound to k1: a k2 signature for it is rejected.
    let outcome = doc.insert_signatures([(a, sig(&k2, &a))]);
    assert!(matches!(
        insertions(&outcome)[0].result,
        Err(SignatureInsertError::Verification { change, .. }) if change == a
    ));
}

#[test]
fn batch_resolves_initial_and_footerless_changes_under_their_historical_author() {
    let alice = key(7);
    let bob = key(8);
    let mut source =
        Automerge::from_config(crate::Config::default().attested(author(&alice)).unwrap());
    let first = append_at_level(&mut source, None, 1, 0);
    let later = append_at_level(&mut source, None, 2, 0);
    assert_eq!(
        source.get_change_by_hash(&first).unwrap().author(),
        Some(author(&alice))
    );
    assert!(source
        .get_change_by_hash(&later)
        .unwrap()
        .author()
        .is_none());
    source.insert_signatures([(first, sig(&alice, &first)), (later, sig(&alice, &later))]);
    assert!(!source.has_unpublished_changes());

    let mut doc = source.fork_with_author(Some(author(&bob))).unwrap();
    let local = append_at_level(&mut doc, None, 3, 0);
    let inputs = [
        (later, sig(&bob, &later)),
        (later, sig(&alice, &later)),
        (local, sig(&alice, &local)),
        (first, sig(&alice, &first)),
        (local, sig(&bob, &local)),
        (later, sig(&alice, &later)),
    ];
    let outcome = doc.insert_signatures(inputs);
    let results = insertions(&outcome);
    assert_eq!(results.len(), inputs.len());
    for (entry, (hash, signature)) in results.iter().zip(inputs) {
        assert_eq!((entry.hash, entry.signature), (hash, signature));
    }
    for index in [0, 2] {
        assert!(matches!(results[index].result,
            Err(SignatureInsertError::Verification { change, .. }) if change == inputs[index].0));
    }
    for index in [1, 3, 5] {
        assert!(matches!(results[index].result, Ok(Insert::Duplicate)));
    }
    assert!(matches!(results[4].result, Ok(Insert::Stored)));
    assert!(!doc.has_unpublished_changes());
    assert_eq!(doc.get_author(), Some(&author(&bob)));
}

#[test]
fn missing_historical_author_is_a_per_entry_error() {
    let k = key(7);
    let mut receiver =
        Automerge::from_config(crate::Config::default().attested(author(&k)).unwrap())
            .with_actor(ActorId::from([1]))
            .unwrap();
    let mine = append_at_level(&mut receiver, None, 1, 0);

    let mut authorless = Automerge::new().with_actor(ActorId::from([3])).unwrap();
    let none = append_at_level(&mut authorless, None, 2, 0);
    graft_applied_change(&mut receiver, &authorless, &none);

    let outcome = receiver.insert_signatures([(none, sig(&k, &none)), (mine, sig(&k, &mine))]);
    let results = insertions(&outcome);
    assert!(matches!(
        results[0].result,
        Err(SignatureInsertError::MissingAuthor { change, .. }) if change == none
    ));
    assert!(matches!(results[1].result, Ok(Insert::Stored)));
    assert!(is_published(&mut receiver, &mine));
}

#[test]
fn invalid_historical_author_is_a_per_entry_error() {
    let k = key(7);
    let mut receiver =
        Automerge::from_config(crate::Config::default().attested(author(&k)).unwrap())
            .with_actor(ActorId::from([1]))
            .unwrap();

    let mut bad_key =
        Automerge::from_config(crate::Config::default().author(Some(Author::from(vec![1, 2, 3]))))
            .with_actor(ActorId::from([4]))
            .unwrap();
    let bad = append_at_level(&mut bad_key, None, 3, 0);
    graft_applied_change(&mut receiver, &bad_key, &bad);

    let outcome = receiver.insert_signatures([(bad, sig(&k, &bad))]);
    assert!(matches!(
        insertions(&outcome)[0].result,
        Err(SignatureInsertError::InvalidAuthorKey { change, .. }) if change == bad
    ));
}

#[test]
fn simultaneously_admissible_pending_equivocation_fails_publication_but_retains_state() {
    // Local a unsigned. Two honest-looking pending groups from the same
    // actor/seq (equivocation) both depend on a. Each is individually valid
    // and stays pending until a publishes; then both become admissible at once.
    let k1 = key(1);
    let k2 = key(2);
    let mut doc = Automerge::from_config(crate::Config::default().attested(author(&k1)).unwrap())
        .with_actor(ActorId::from([1]))
        .unwrap();
    let a = append_at_level(&mut doc, None, 1, 0);

    let branch = |value: i64| {
        let mut remote = replica(&doc, author(&k2), ActorId::from([2]));
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
    assert_eq!(doc.attestation.queued_changes().count(), 2);

    let mut log = PatchLog::active();
    let outcome = doc.insert_signatures_log_patches([(a, sig(&k1, &a))], &mut log);
    let SignatureBatchOutcome::PublicationFailed { insertions, error } = &outcome else {
        panic!("expected PublicationFailed, got {outcome:?}");
    };
    assert_eq!(insertions.len(), 1);
    assert!(matches!(insertions[0].result, Ok(Insert::Stored)));
    assert!(matches!(error, PublicationError::Admission(_)));
    // Evidence and local publication retained; no half-application; pending kept.
    assert!(is_published(&mut doc, &a));
    assert_eq!(doc.get_heads(), [a]);
    assert_eq!(doc.attestation.queued_changes().count(), 2);
    assert!(doc.make_patches(&mut log).is_empty());
    assert!(requested_hashes(&mut doc).is_empty());

    // Unchanged retry fails the same way without choosing a winner.
    let outcome = doc.insert_signatures(std::iter::empty());
    assert!(matches!(
        outcome,
        SignatureBatchOutcome::PublicationFailed { .. }
    ));
    assert_eq!(doc.get_heads(), [a]);
    assert_eq!(doc.attestation.queued_changes().count(), 2);
}

#[test]
#[allow(deprecated)]
fn published_frontier_with_reclaimed_signature_does_not_prove_a_new_candidate() {
    // Published loose a (actor 1) whose own signature has been reclaimed by
    // the published same-actor cover G = {a, c}. A different actor's level-1
    // d built at [a] (not c) has canonical H = {a, d} with frontiers {a, d}:
    // a is published but carries no signature, so H must stay unproved until
    // the exact sig(a) is supplied, even though every frontier is applied and
    // a is published.
    let ka = key(7);
    let mut doc = Automerge::from_config(crate::Config::default().attested(author(&ka)).unwrap())
        .with_actor(ActorId::from([1]))
        .unwrap();
    let a = append_at_level(&mut doc, None, 1, 0);
    sign_doc(&mut doc, a, &ka);
    assert!(is_published(&mut doc, &a));
    let c = append_at_level(&mut doc, Some(&[a]), 2, 1);
    let g = doc.change_graph.get_historical_fragment(c).unwrap();
    assert_eq!(sorted(&g.members), sorted(&[a, c]));
    sign_doc(&mut doc, c, &ka);
    assert!(is_published(&mut doc, &c));
    assert_eq!(signature_count(&mut doc, &a), 0, "a's proof was reclaimed");

    doc.set_actor(ActorId::from([2])).unwrap();
    let d = append_at_level(&mut doc, Some(&[a]), 3, 1);
    let h = doc.change_graph.get_historical_fragment(d).unwrap();
    assert_eq!(sorted(&h.members), sorted(&[a, d]));
    let frontiers: Vec<ChangeHash> = doc
        .change_graph
        .fragment_requirements(d)
        .unwrap()
        .hashes()
        .copied()
        .collect();
    assert_eq!(sorted(&frontiers), sorted(&[a, d]));

    let outcome = doc.insert_signatures([(d, sig(&ka, &d))]);
    assert!(matches!(insertions(&outcome)[0].result, Ok(Insert::Stored)));
    assert!(
        !is_published(&mut doc, &d),
        "published membership of frontier a is not proof for H"
    );
    assert!(doc.get_fragment(d).is_none());

    // Supplying a's exact frontier signature again completes H.
    let outcome = doc.insert_signatures([(a, sig(&ka, &a))]);
    assert!(matches!(insertions(&outcome)[0].result, Ok(Insert::Stored)));
    assert!(is_published(&mut doc, &d));
}

#[test]
#[allow(deprecated)]
fn completion_continues_after_publication_only_progress_of_pending_group() {
    // l (actor 1) is local and unpublished. a (actor 2, same author) was committed
    // locally on this document too, so its contents are applied; the same a
    // then arrives as a signed incoming group, which stays pending on l. c
    // (actor 1) depends on a and already has its own signature retained.
    // Signing l publishes l, then the retry publishes pending a without
    // appending any graph node, which in turn enables c: all of this must
    // happen in one call.
    let k1 = key(1);
    let mut doc = Automerge::from_config(crate::Config::default().attested(author(&k1)).unwrap())
        .with_actor(ActorId::from([1]))
        .unwrap();
    let l = append_at_level(&mut doc, None, 1, 0);
    doc.set_actor(ActorId::from([2])).unwrap();
    let a = append_at_level(&mut doc, Some(&[l]), 2, 0);
    doc.set_actor(ActorId::from([1])).unwrap();
    let c = append_at_level(&mut doc, Some(&[a]), 3, 0);

    let a_bytes = attach(
        doc.get_change_by_hash(&a).unwrap().raw_bytes(),
        [(a, Signature::ed25519(sig(&k1, &a)))],
    );
    assert_eq!(doc.load_incremental(&a_bytes).unwrap(), 0);
    assert_eq!(
        doc.attestation
            .queued_changes()
            .map(Change::hash)
            .collect::<Vec<_>>(),
        [a],
        "a's contents are applied but its proof waits on l"
    );
    let outcome = doc.insert_signatures([(c, sig(&k1, &c))]);
    assert!(matches!(insertions(&outcome)[0].result, Ok(Insert::Stored)));
    assert!(!is_published(&mut doc, &c));
    // a was committed on this document too, so it is tracked alongside l and c.
    assert_eq!(local_drafts(&mut doc), [l, a, c]);
    let graph_len = doc.change_graph.len();

    let mut log = PatchLog::active();
    let outcome = doc.insert_signatures_log_patches([(l, sig(&k1, &l))], &mut log);
    assert!(matches!(outcome, SignatureBatchOutcome::Completed { .. }));
    assert!(is_published(&mut doc, &l));
    assert!(is_published(&mut doc, &a));
    assert!(
        is_published(&mut doc, &c),
        "c is enabled by a's publication within the same call"
    );
    assert_eq!(doc.change_graph.len(), graph_len, "no new history applied");
    assert!(!doc.attestation.has_pending_groups());
    assert!(
        local_drafts(&mut doc).is_empty(),
        "published local tracking retired in the same call"
    );
    assert!(doc.make_patches(&mut log).is_empty());

    // Stable: an empty follow-up changes nothing.
    let saved = doc.save_nocompress();
    let outcome = doc.insert_signatures(std::iter::empty());
    assert!(matches!(outcome, SignatureBatchOutcome::Completed { .. }));
    assert_eq!(doc.save_nocompress(), saved);
}
