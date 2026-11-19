//! Independent end-to-end lifecycle coverage for required signing mode.
//!
//! These tests drive only the public API and judge outcomes against
//! externally observable contracts rather than the planner or admission
//! algorithms. The publication oracle is the *published projection*: a
//! required-mode `save` reloaded into a fresh required-mode document. A change
//! is published iff that projection contains it; the projection must itself
//! be fully published, draft-free and re-exportable to another empty peer.

use std::collections::BTreeSet;

use automerge::signature::error::SigningError;
use automerge::signature::{ed25519, parse_signing_message, signing_message, Insert};
use automerge::transaction::{CommitOptions, Transactable};
use automerge::{
    ActorId, Author, AutoCommit, Automerge, AutomergeError, Change, ChangeHash, LoadOptions,
    PatchLog, ReadDoc, SignatureBatchOutcome, SigningRequests, ROOT,
};
use ed25519_dalek::{Signer, SigningKey};
use proptest::prelude::*;

fn key(seed: u8) -> SigningKey {
    SigningKey::from_bytes(&[seed; 32])
}

fn author(key: &SigningKey) -> Author<'static> {
    Author::from(key.verifying_key().to_bytes().to_vec())
}

fn sign(key: &SigningKey, hash: &ChangeHash) -> ed25519::Signature {
    ed25519::Signature::from_bytes(key.sign(&signing_message(hash)).to_bytes())
}

/// Sign every request with `key`, asserting each request is well formed for
/// the expected author, and submit them in one batch.
fn answer(
    doc: &mut Automerge,
    requests: &SigningRequests,
    key: &SigningKey,
) -> SignatureBatchOutcome {
    let inputs: Vec<_> = requests
        .iter()
        .map(|request| {
            assert_eq!(request.author(), &author(key));
            assert_eq!(parse_signing_message(request.message()), Ok(request.hash()));
            let signature = ed25519::Signature::from_bytes(key.sign(request.message()).to_bytes());
            (request.hash(), signature)
        })
        .collect();
    doc.insert_signatures(inputs)
}

fn requested(doc: &mut Automerge) -> Vec<ChangeHash> {
    doc.request_signatures()
        .unwrap()
        .iter()
        .map(|r| r.hash())
        .collect()
}

fn hashes(doc: &Automerge) -> BTreeSet<ChangeHash> {
    doc.get_changes(&[]).iter().map(Change::hash).collect()
}

fn observer() -> LoadOptions<'static> {
    LoadOptions::new().signing().author(author(&key(250)))
}

/// The published projection of `doc`, checked for the properties every
/// signed save must have: it loads only in required mode, is fully
/// published, has no drafts or requests, and round-trips to another empty
/// peer with identical bytes per change.
fn published(doc: &Automerge) -> Automerge {
    let bytes = doc.save();
    let mut projection = Automerge::load_with_options(&bytes, observer()).unwrap();
    assert!(
        Automerge::load(&bytes).is_err() || hashes(&projection).is_empty(),
        "a non-empty signed save is not a bare snapshot"
    );
    assert!(!projection.has_unpublished_changes());
    assert!(requested(&mut projection).is_empty());
    let again = Automerge::load_with_options(&projection.save(), observer()).unwrap();
    assert_eq!(hashes(&again), hashes(&projection), "re-export is stable");
    assert!(!again.has_unpublished_changes());
    for hash in hashes(&projection) {
        assert_eq!(
            projection.get_change_by_hash(&hash).unwrap().raw_bytes(),
            doc.get_change_by_hash(&hash).unwrap().raw_bytes()
        );
    }
    // Save/merge agree on the published set.
    let mut merged = Automerge::signing(author(&key(251))).unwrap();
    merged.merge(&mut doc.clone()).unwrap();
    assert_eq!(hashes(&merged), hashes(&projection), "merge == save");
    projection
}

fn commit(doc: &mut Automerge, heads: Option<&[ChangeHash]>, key: &str, value: i64) -> ChangeHash {
    let mut tx = match heads {
        Some(heads) => doc.transaction_at(PatchLog::inactive(), heads).unwrap(),
        None => doc.transaction(),
    };
    tx.put(ROOT, key, value).unwrap();
    tx.commit_with(CommitOptions::default().with_time(value))
        .0
        .unwrap()
}

/// Commit at `heads` until the hash is at least level `level` (leading zero
/// bytes), varying the commit time.
fn commit_at_level(doc: &mut Automerge, heads: Option<&[ChangeHash]>, level: usize) -> ChangeHash {
    for time in 0..100_000 {
        let mut next = doc.clone();
        let mut tx = match heads {
            Some(heads) => next.transaction_at(PatchLog::inactive(), heads).unwrap(),
            None => next.transaction(),
        };
        // Distinct value per attempt and per call site, so the put is never
        // a no-op against the state at `heads`.
        tx.put(ROOT, "v", time * 1_000 + i64::from(level as u8) * 7 + 1)
            .unwrap();
        tx.put(ROOT, "w", level as i64).unwrap();
        let hash = tx
            .commit_with(CommitOptions::default().with_time(time))
            .0
            .unwrap();
        if hash.0.iter().take_while(|b| **b == 0).count() == level {
            *doc = next;
            return hash;
        }
    }
    panic!("level search exhausted")
}

/// Invariants that must hold for any required-mode document at any time.
fn check_invariants(doc: &mut Automerge, locally_committed: &BTreeSet<ChangeHash>) {
    let applied = hashes(doc);
    let projection = published(doc);
    let published_set = hashes(&projection);
    assert!(published_set.is_subset(&applied));
    assert_eq!(
        doc.has_unpublished_changes(),
        published_set != applied,
        "status == some applied history unpublished"
    );
    let requests = requested(doc);
    let unique: BTreeSet<_> = requests.iter().copied().collect();
    assert_eq!(unique.len(), requests.len(), "deduplicated");
    assert!(requests.windows(2).all(|w| w[0] < w[1]), "hash ordered");
    assert_eq!(requests, requested(doc), "deterministic");
    for hash in &requests {
        assert!(
            locally_committed.contains(hash),
            "only local commits requested"
        );
        assert!(!published_set.contains(hash), "published never requested");
    }
    // Published history is causally closed.
    for hash in &published_set {
        for dep in doc.get_change_by_hash(hash).unwrap().deps() {
            assert!(published_set.contains(dep), "published deps are published");
        }
    }
}

#[test]
fn lifecycle_local_drafts_delayed_duplicate_invalid_responses_and_publication() {
    let k = key(1);
    let mut doc = Automerge::signing(author(&k))
        .unwrap()
        .with_actor(ActorId::from([1]));
    let mut local = BTreeSet::new();
    check_invariants(&mut doc, &local);

    let a = commit(&mut doc, None, "a", 1);
    let b = commit(&mut doc, None, "b", 2);
    local.extend([a, b]);
    check_invariants(&mut doc, &local);
    assert_eq!(requested(&mut doc), {
        let mut v = vec![a, b];
        v.sort_unstable();
        v
    });
    let early = doc.request_signatures().unwrap();

    // Later commits do not disturb the owned snapshot.
    let c = commit(&mut doc, None, "c", 3);
    local.insert(c);
    assert_eq!(early.len(), 2);
    check_invariants(&mut doc, &local);

    // Delayed response to the early snapshot, with an invalid and a duplicate
    // entry mixed in: every entry owned, bad ones isolated, a and b publish.
    let bad_key = key(9);
    let mut inputs: Vec<_> = early
        .iter()
        .map(|r| (r.hash(), sign(&k, &r.hash())))
        .collect();
    inputs.insert(0, (a, sign(&bad_key, &a)));
    inputs.push((a, sign(&k, &a)));
    let SignatureBatchOutcome::Completed { insertions } = doc.insert_signatures(inputs.clone())
    else {
        panic!("completed");
    };
    assert_eq!(insertions.len(), inputs.len());
    assert!(insertions[0].result.is_err());
    assert!(matches!(insertions[1].result, Ok(Insert::Stored)));
    assert!(matches!(insertions[2].result, Ok(Insert::Stored)));
    assert!(matches!(insertions[3].result, Ok(Insert::Duplicate)));
    let projection = published(&doc);
    assert_eq!(hashes(&projection), [a, b].into());
    assert!(doc.has_unpublished_changes());
    assert_eq!(requested(&mut doc), [c]);
    check_invariants(&mut doc, &local);

    // Completed != all published: an empty batch is harmless.
    assert!(matches!(
        doc.insert_signatures(std::iter::empty()),
        SignatureBatchOutcome::Completed { insertions } if insertions.is_empty()
    ));
    assert_eq!(hashes(&published(&doc)), [a, b].into());

    // Published-only save keeps the draft live.
    let saved = doc.save();
    assert!(doc.get_change_by_hash(&c).is_some());
    assert_eq!(doc.get(ROOT, "c").unwrap().unwrap().0.as_i64(), Some(3));
    let mut reloaded =
        Automerge::load_with_options(&saved, LoadOptions::new().signing().author(author(&k)))
            .unwrap();
    assert!(
        reloaded.get_change_by_hash(&c).is_none(),
        "drafts are not persisted"
    );
    assert!(
        requested(&mut reloaded).is_empty(),
        "reload creates no eligibility"
    );

    // Finishing the draft publishes everything; a stale duplicate afterwards
    // is reported but changes nothing.
    let requests = doc.request_signatures().unwrap();
    answer(&mut doc, &requests, &k);
    assert!(!doc.has_unpublished_changes());
    assert_eq!(hashes(&published(&doc)), [a, b, c].into());
    let SignatureBatchOutcome::Completed { insertions } =
        doc.insert_signatures([(c, sign(&k, &c))])
    else {
        panic!()
    };
    assert!(insertions[0].result.is_ok());
    check_invariants(&mut doc, &local);
}

#[test]
fn lifecycle_pending_delivery_causal_progress_and_atomic_rejection_of_new_invalid_input() {
    let ka = key(1);
    let kb = key(2);
    let mut alice = Automerge::signing(author(&ka))
        .unwrap()
        .with_actor(ActorId::from([1]));
    let a = commit(&mut alice, None, "a", 1);

    // Bob builds on Alice's draft (exported via inspection, not transport) and
    // signs b1, b2. Bob's groups reach Alice before `a` is published.
    let mut bob = Automerge::signing(author(&kb))
        .unwrap()
        .with_actor(ActorId::from([2]));
    let unsigned_a = {
        let mut copy = Automerge::new();
        copy.apply_changes([alice.get_change_by_hash(&a).unwrap()])
            .unwrap();
        copy
    };
    // Bob cannot import an unsigned draft; he must wait for publication. So
    // Bob's history branches independently and later depends on nothing.
    assert!(matches!(
        bob.apply_changes(unsigned_a.get_changes(&[])),
        Err(AutomergeError::Signing(
            SigningError::UnauthenticatedChanges
        ))
    ));
    let b1 = commit(&mut bob, None, "b1", 10);
    let b2 = commit(&mut bob, None, "b2", 11);
    let requests = bob.request_signatures().unwrap();
    answer(&mut bob, &requests, &kb);
    assert!(!bob.has_unpublished_changes());
    let b1_wire = bob
        .bundle_fragments([bob.get_fragment(b1).unwrap()])
        .concat();
    let b2_wire = bob
        .bundle_fragments([bob.get_fragment(b2).unwrap()])
        .concat();

    // Out-of-order delivery: b2 waits (missing b1), nothing applied; a
    // duplicate delivery is harmless; then b1 completes both with patches.
    let mut log = PatchLog::active();
    assert_eq!(
        alice
            .load_incremental_log_patches(&b2_wire, &mut log)
            .unwrap(),
        0
    );
    assert_eq!(
        alice
            .load_incremental_log_patches(&b2_wire, &mut log)
            .unwrap(),
        0
    );
    assert_eq!(alice.get_heads(), [a]);
    assert!(alice.make_patches(&mut log).is_empty());
    assert_eq!(alice.get_missing_deps(&[]), vec![b1]);
    let mut log = PatchLog::active();
    assert_eq!(
        alice
            .load_incremental_log_patches(&b1_wire, &mut log)
            .unwrap(),
        2
    );
    let patches = alice.make_patches(&mut log);
    assert_eq!(patches.len(), 2, "one patch per applied op, no duplicates");
    assert_eq!(hashes(&published(&alice)), [b1, b2].into());
    assert!(alice.has_unpublished_changes(), "a is still a draft");
    assert_eq!(requested(&mut alice), [a]);
    // Replaying already-applied groups neither reapplies nor patches.
    let mut log = PatchLog::active();
    assert_eq!(
        alice
            .load_incremental_log_patches(&b1_wire, &mut log)
            .unwrap(),
        0
    );
    assert!(alice.make_patches(&mut log).is_empty());

    // A new delivery containing an invalid group is rejected atomically: the
    // valid group in the same delivery is not retained either.
    let b3 = commit(&mut bob, None, "b3", 12);
    let b4 = commit(&mut bob, None, "b4", 13);
    let requests = bob.request_signatures().unwrap();
    answer(&mut bob, &requests, &kb);
    let b3_wire = bob
        .bundle_fragments([bob.get_fragment(b3).unwrap()])
        .concat();
    let mut b4_wire = bob
        .bundle_fragments([bob.get_fragment(b4).unwrap()])
        .concat();
    // Corrupt b4's trailing signature bytes.
    let n = b4_wire.len();
    b4_wire[n - 1] ^= 0xff;
    let before = alice.save();
    assert!(alice
        .load_incremental(&[b3_wire.clone(), b4_wire].concat())
        .is_err());
    assert_eq!(alice.save(), before);
    assert_eq!(alice.get_missing_deps(&[]), Vec::<ChangeHash>::new());
    assert!(
        alice.get_change_by_hash(&b3).is_none(),
        "valid sibling not retained"
    );
    // Delivered alone, b3 applies at once.
    assert_eq!(alice.load_incremental(&b3_wire).unwrap(), 1);

    // Alice's own draft publishes after a signature; Bob merges only the
    // published set and never gains eligibility for Alice's hashes.
    let d = commit(&mut alice, None, "d", 2);
    let requests = alice.request_signatures().unwrap();
    assert_eq!(requests.len(), 2);
    answer(&mut alice, &requests, &ka);
    assert!(!alice.has_unpublished_changes());
    bob.merge(&mut alice).unwrap();
    assert!(bob.get_change_by_hash(&a).is_some());
    assert!(bob.get_change_by_hash(&d).is_some());
    assert!(
        requested(&mut bob).is_empty(),
        "imported hashes are never Bob's drafts"
    );
    assert!(!bob.has_unpublished_changes());
    check_invariants(&mut alice, &[a, d].into());
    check_invariants(&mut bob, &[b1, b2, b3, b4].into());
}

#[test]
fn lifecycle_old_invalid_pending_is_discarded_while_new_valid_delivery_applies() {
    // An orphan group from a forged actor chain is retained speculatively;
    // once the real actor history arrives, the old pending group is disproved
    // and dropped while the new delivery applies.
    let ka = key(1);
    let mallory = key(99);
    let mut alice = Automerge::signing(author(&ka))
        .unwrap()
        .with_actor(ActorId::from([1]));
    let a1 = commit(&mut alice, None, "a", 1);
    let a2 = commit(&mut alice, None, "a", 2);
    let requests = alice.request_signatures().unwrap();
    answer(&mut alice, &requests, &ka);
    let a1_wire = alice
        .bundle_fragments([alice.get_fragment(a1).unwrap()])
        .concat();

    // Mallory forges actor [1] seq 2 under her own author.
    let mut forged = Automerge::signing(author(&mallory))
        .unwrap()
        .with_actor(ActorId::from([1]));
    commit(&mut forged, None, "f", 1);
    let f2 = commit(&mut forged, None, "f", 2);
    let requests = forged.request_signatures().unwrap();
    answer(&mut forged, &requests, &mallory);
    let f2_wire = forged
        .bundle_fragments([forged.get_fragment(f2).unwrap()])
        .concat();

    let mut receiver = Automerge::signing(author(&key(3))).unwrap();
    assert_eq!(
        receiver.load_incremental(&f2_wire).unwrap(),
        0,
        "orphan retained pending"
    );
    assert!(receiver.get_heads().is_empty());
    // Real actor [1] seq 1 arrives: applies; the orphan is now disproved and
    // discarded rather than poisoning the delivery.
    assert_eq!(receiver.load_incremental(&a1_wire).unwrap(), 1);
    assert_eq!(receiver.get_heads(), [a1]);
    assert!(
        receiver.get_missing_deps(&[]).is_empty(),
        "orphan no longer waits"
    );
    assert!(receiver.get_change_by_hash(&f2).is_none());
    // Alice's real seq 2 continues normally.
    let a2_wire = alice
        .bundle_fragments([alice.get_fragment(a2).unwrap()])
        .concat();
    assert_eq!(receiver.load_incremental(&a2_wire).unwrap(), 1);
    assert_eq!(hashes(&published(&receiver)), [a1, a2].into());
    check_invariants(&mut receiver, &BTreeSet::new());
}

#[test]
fn lifecycle_reclamation_requires_publication_and_keeps_transferable_proof() {
    // Loose `a` published; foreign unpublished hard `b`; local hard `c` at
    // [a, b] forms a cover {a, c} with boundary {b}. Signing `c` makes the
    // cover ready but it cannot publish; `a`'s proof must survive so the
    // published projection stays transferable. Publishing `b` later retires
    // the fallback proof: publication-only progress, no redelivery.
    let ka = key(1);
    let kb = key(2);
    let mut doc = Automerge::signing(author(&ka))
        .unwrap()
        .with_actor(ActorId::from([1]));
    let a = commit_at_level(&mut doc, None, 0);
    doc.insert_signatures([(a, sign(&ka, &a))]);
    assert_eq!(hashes(&published(&doc)), [a].into());
    let early_save = doc.save();

    // `b` by another author, imported as a *signed* group so it is applied.
    // To make it unpublished we deliver it blocked: b depends on an
    // unpublished local change `x` of ours.
    let x = commit(&mut doc, Some(&[]), "x", 5);
    let mut bob = Automerge::signing(author(&kb))
        .unwrap()
        .with_actor(ActorId::from([2]));
    // Bob gets `a` (published) via merge, then builds hard `b` at [a].
    bob.merge(&mut doc).unwrap();
    let b = commit_at_level(&mut bob, Some(&[a]), 1);
    let requests = bob.request_signatures().unwrap();
    answer(&mut bob, &requests, &kb);
    let b_wire = bob
        .bundle_fragments([bob.get_fragment(b).unwrap()])
        .concat();
    assert!(doc.load_incremental(&b_wire).unwrap() > 0);
    assert_eq!(hashes(&published(&doc)), [a, b].into());

    // Local hard `c` at [b, x]: its cover is blocked on our unpublished `x`.
    let c = commit_at_level(&mut doc, Some(&[b, x]), 1);
    let requests = doc.request_signatures().unwrap();
    // Planning: x is a loose local draft and c's cover needs x published.
    assert!(requests.iter().any(|r| r.hash() == x));
    // Sign only c: ready but unpublished. a and b's proofs stay transferable.
    doc.insert_signatures([(c, sign(&ka, &c))]);
    assert_eq!(hashes(&published(&doc)), [a, b].into());
    assert!(doc.has_unpublished_changes());
    assert_eq!(requested(&mut doc), [x]);
    // Publishing x publishes c too without re-signing c.
    doc.insert_signatures([(x, sign(&ka, &x))]);
    assert!(!doc.has_unpublished_changes());
    let projection = published(&doc);
    assert_eq!(hashes(&projection), [a, b, x, c].into());
    assert!(requested(&mut doc).is_empty());
    // Everything an empty peer needs is in the save, even if interior
    // fallback proofs were reclaimed by the published cover.
    let peer = Automerge::load_with_options(&doc.save(), observer()).unwrap();
    assert_eq!(peer.get_heads(), doc.get_heads());
    assert!(!peer.has_unpublished_changes());
    // Incremental transfer after a known cut resends complete groups.
    let inc = doc.save_after(&[a]);
    let mut stale = Automerge::load_with_options(&early_save, observer()).unwrap();
    assert_eq!(hashes(&stale), [a].into());
    stale.load_incremental(&inc).unwrap();
    assert_eq!(hashes(&stale), hashes(&projection));
    assert_eq!(stale.load_incremental(&inc).unwrap(), 0, "idempotent");
    check_invariants(&mut doc, &[a, x, c].into());
}

#[test]
fn lifecycle_historical_branches_and_overlapping_fragments_publish_deterministically() {
    // Two actors of the same author with a historical transaction_at branch
    // off `a` and a hard merge spanning both actors. Equivalent histories
    // (same applied change set and provenance, different current actor and
    // a same-author fork in between) yield identical request sets, and
    // publishing either yields the same projection.
    let k = key(1);
    let mut one = Automerge::signing(author(&k))
        .unwrap()
        .with_actor(ActorId::from([1]));
    let a = commit(&mut one, None, "a", 1);
    let right_first = commit(&mut one, None, "r", 2);
    one.set_actor(ActorId::from([2]));
    let left = commit(&mut one, Some(&[a]), "l", 3);
    let merge = commit_at_level(&mut one, Some(&[left, right_first]), 1);
    let h1 = [a, right_first, left, merge];
    assert_eq!(hashes(&one), h1.into());
    let mut two = one.fork_with_author(Some(author(&k))).unwrap();
    two.set_actor(ActorId::from([1]));
    one.set_actor(ActorId::from([7]));
    assert_eq!(hashes(&two), h1.into());
    let r1 = requested(&mut one);
    let r2 = requested(&mut two);
    assert_eq!(r1, r2, "deterministic under equivalent histories");
    assert!(!r1.is_empty());
    for hash in &r1 {
        assert!(h1.contains(hash));
    }
    let requests = one.request_signatures().unwrap();
    answer(&mut one, &requests, &k);
    assert!(!one.has_unpublished_changes());
    assert_eq!(hashes(&published(&one)), h1.into());
    let requests = two.request_signatures().unwrap();
    answer(&mut two, &requests, &k);
    assert_eq!(hashes(&published(&two)), h1.into());
    // The merge cover spans both actors: its exported group includes the
    // whole branch and loads into an empty peer.
    let wire = one.bundle_fragments(one.fragments(..)).concat();
    let peer = Automerge::load_with_options(&wire, observer()).unwrap();
    assert_eq!(hashes(&peer), h1.into());
    check_invariants(&mut one, &h1.into());
}

#[test]
fn lifecycle_guarded_clone_fixed_author_and_actor_ownership() {
    let ka = key(1);
    let kb = key(2);
    let mut doc = AutoCommit::signing(author(&ka))
        .unwrap()
        .with_actor(ActorId::from([1]));
    doc.put(ROOT, "a", 1).unwrap();
    let a = doc.commit().unwrap();
    // Different author with a draft: rejected; same author: allowed and
    // independent (copies eligibility, does not move it).
    assert!(matches!(
        doc.fork_with_author(Some(author(&kb))),
        Err(AutomergeError::Signing(
            SigningError::UnpublishedLocalChanges
        ))
    ));
    let mut same = doc.fork_with_author(Some(author(&ka))).unwrap();
    let requests = same.request_signatures().unwrap();
    assert_eq!(requests.iter().map(|r| r.hash()).collect::<Vec<_>>(), [a]);
    same.insert_signatures(
        requests
            .iter()
            .map(|r| (r.hash(), sign(&ka, &r.hash())))
            .collect::<Vec<_>>(),
    );
    assert!(!same.has_unpublished_changes());
    assert!(doc.has_unpublished_changes(), "source keeps its draft");

    // Publish in the source, then Bob forks with a fresh actor; Bob cannot
    // adopt Alice's actor, and Alice's key cannot sign Bob's change.
    let outcome = doc.insert_signatures([(a, sign(&ka, &a))]);
    assert!(matches!(outcome, SignatureBatchOutcome::Completed { .. }));
    let mut bobs = doc.fork_with_author(Some(author(&kb))).unwrap();
    assert_ne!(bobs.get_actor(), &ActorId::from([1]));
    assert!(matches!(
        bobs.try_set_actor(ActorId::from([1])),
        Err(AutomergeError::Signing(
            SigningError::ActorBoundToOtherAuthor { .. }
        ))
    ));
    bobs.put(ROOT, "b", 2).unwrap();
    let b = bobs.commit().unwrap();
    let SignatureBatchOutcome::Completed { insertions } =
        bobs.insert_signatures([(b, sign(&ka, &b))])
    else {
        panic!()
    };
    assert!(insertions[0].result.is_err(), "historical author is Bob");
    bobs.insert_signatures([(b, sign(&kb, &b))]);
    assert!(!bobs.has_unpublished_changes());
    // Alice's document receives Bob's published change through merge and
    // still has nothing to sign.
    doc.merge(&mut bobs).unwrap();
    assert!(doc.get_change_by_hash(&b).is_some());
    assert!(doc.request_signatures().unwrap().is_empty());
    assert!(!doc.has_unpublished_changes());
    assert_eq!(
        doc.get_author_for_actor(bobs.get_actor()),
        Some(author(&kb))
    );
    assert_eq!(
        doc.get_author_for_actor(&ActorId::from([1])),
        Some(author(&ka))
    );
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(16))]

    /// Random schedules of commits (two actors, occasional branch), partial
    /// signing responses (including stale and foreign-key ones), saves and
    /// reloads: the lifecycle invariants hold after every step and the
    /// history drains completely once every request is honoured.
    #[test]
    fn lifecycle_invariants_hold_under_random_schedules(
        steps in prop::collection::vec(0u8..10, 1..14)
    ) {
        let k = key(1);
        let mut doc = Automerge::signing(author(&k)).unwrap().with_actor(ActorId::from([1]));
        let mut local = BTreeSet::new();
        let mut stale: Vec<ChangeHash> = Vec::new();
        for (i, step) in steps.iter().enumerate() {
            match step {
                0..=3 => {
                    doc.set_actor(ActorId::from([1 + (step % 2)]));
                    let heads = doc.get_heads();
                    let base = if *step == 3 && heads.len() == 1 && local.len() > 1 {
                        Some(vec![*local.iter().next().unwrap()])
                    } else {
                        None
                    };
                    let hash = commit(&mut doc, base.as_deref(), "k", i as i64);
                    local.insert(hash);
                }
                4..=6 => {
                    let requests = requested(&mut doc);
                    if let Some(hash) = requests.get(usize::from(*step) % requests.len().max(1)) {
                        stale.push(*hash);
                        let outcome = doc.insert_signatures([(*hash, sign(&k, hash)), (*hash, sign(&key(9), hash))]);
                        let SignatureBatchOutcome::Completed { insertions } = outcome else { panic!() };
                        prop_assert!(insertions[0].result.is_ok());
                        prop_assert!(insertions[1].result.is_err());
                    }
                }
                7 => {
                    // Stale duplicates never hurt.
                    let inputs: Vec<_> = stale.iter().map(|h| (*h, sign(&k, h))).collect();
                    let completed = matches!(doc.insert_signatures(inputs), SignatureBatchOutcome::Completed { .. });
                    prop_assert!(completed);
                }
                _ => {
                    // Reload the published projection with the same author:
                    // no eligibility, nothing to request.
                    let mut reloaded = Automerge::load_with_options(
                        &doc.save(),
                        LoadOptions::new().signing().author(author(&k)),
                    ).unwrap();
                    prop_assert!(requested(&mut reloaded).is_empty());
                    prop_assert!(!reloaded.has_unpublished_changes());
                }
            }
            check_invariants(&mut doc, &local);
        }
        // Drain.
        let mut rounds = 0;
        loop {
            let requests = doc.request_signatures().unwrap();
            if requests.is_empty() {
                break;
            }
            answer(&mut doc, &requests, &k);
            rounds += 1;
            prop_assert!(rounds <= local.len() + 1);
        }
        prop_assert!(!doc.has_unpublished_changes());
        prop_assert_eq!(hashes(&published(&doc)), hashes(&doc));
    }
}
