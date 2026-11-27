//! Independent end-to-end lifecycle coverage for an attested document.
//!
//! These tests drive only the public API and judge outcomes against
//! externally observable contracts rather than the planner or admission
//! algorithms. Saves expose publication, but save/merge agreement alone is
//! not an independent oracle: both share cover selection. Example scenarios
//! assert exact expected hashes; the loose-chain schedule property derives its
//! expected prefix solely from submitted evidence and the known causal chain.

use std::collections::BTreeSet;

use automerge::signature::{ed25519, parse_signing_message, signing_message, Insert};
use automerge::transaction::{CommitOptions, Transactable};
use automerge::{
    ActorId, Author, Automerge, Change, ChangeHash, LoadOptions, PatchLog, ReadDoc,
    SignatureBatchOutcome, SigningRequests, ROOT,
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
            assert_eq!(parse_signing_message(request.message()), Ok(request.hash()));
            let signature = ed25519::Signature::from_bytes(key.sign(request.message()).to_bytes());
            (request.hash(), signature)
        })
        .collect();
    doc.insert_signatures(inputs)
}

fn requested(doc: &mut Automerge) -> Vec<ChangeHash> {
    doc.signing_requests()
        .unwrap()
        .iter()
        .map(|r| r.hash())
        .collect()
}

fn hashes(doc: &Automerge) -> BTreeSet<ChangeHash> {
    doc.get_changes(&[]).iter().map(Change::hash).collect()
}

fn observer() -> LoadOptions<'static> {
    LoadOptions::new().attested(author(&key(250))).unwrap()
}

/// The published projection of `doc`, checked for the properties every
/// signed save must have: it loads only in an attested document, is fully
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
    // Cross-surface consistency only; callers provide expected-state oracles.
    let mut merged = Automerge::from_config(
        automerge::Config::default()
            .attested(author(&key(251)))
            .unwrap(),
    );
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
        tx.put(
            ROOT,
            format!("level-{level}-{}", doc.get_changes(&[]).len()),
            time,
        )
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

/// Invariants that must hold for any attested documents at any time.
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
fn owned_request_snapshot_answers_late_with_mixed_entries() {
    let k = key(1);
    let mut doc =
        Automerge::from_config(automerge::Config::default().attested(author(&k)).unwrap())
            .with_actor(ActorId::from([1]))
            .unwrap();
    let a = commit(&mut doc, None, "a", 1);
    let b = commit(&mut doc, None, "b", 2);
    let early = doc.signing_requests().unwrap();
    assert_eq!(early.len(), 2);

    // A later commit does not disturb the already-owned snapshot.
    let c = commit(&mut doc, None, "c", 3);
    assert_eq!(early.len(), 2);

    // Delayed response to the early snapshot with a wrong-key entry and a
    // duplicate mixed in: every entry is reported, the bad one stays
    // isolated, and a and b publish while c remains requested.
    let mut inputs: Vec<_> = early
        .iter()
        .map(|r| (r.hash(), sign(&k, &r.hash())))
        .collect();
    inputs.insert(0, (a, sign(&key(9), &a)));
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
    assert_eq!(hashes(&published(&doc)), [a, b].into());
    assert_eq!(requested(&mut doc), [c]);
    check_invariants(&mut doc, &[a, b, c].into());
}

#[test]
#[allow(deprecated)]
fn equivalent_histories_yield_identical_requests_and_projections() {
    // Two documents with the same applied change set and provenance but a
    // different current actor (and a same-author fork in between) must ask
    // for the same signatures and publish to the same projection.
    let k = key(1);
    let mut one =
        Automerge::from_config(automerge::Config::default().attested(author(&k)).unwrap())
            .with_actor(ActorId::from([1]))
            .unwrap();
    let a = commit(&mut one, None, "a", 1);
    let right = commit(&mut one, None, "r", 2);
    one.set_actor(ActorId::from([2])).unwrap();
    let left = commit(&mut one, Some(&[a]), "l", 3);
    let merge = commit_at_level(&mut one, Some(&[left, right]), 1);
    let all = [a, right, left, merge];
    assert_eq!(hashes(&one), all.into());

    let mut two = one.fork_with_author(Some(author(&k))).unwrap();
    two.set_actor(ActorId::from([1])).unwrap();
    one.set_actor(ActorId::from([7])).unwrap();
    assert_eq!(hashes(&two), all.into());

    let shared = requested(&mut one);
    assert_eq!(shared, requested(&mut two));
    assert!(!shared.is_empty());
    for hash in &shared {
        assert!(all.contains(hash));
    }

    let requests = one.signing_requests().unwrap();
    answer(&mut one, &requests, &k);
    let requests = two.signing_requests().unwrap();
    answer(&mut two, &requests, &k);
    assert_eq!(hashes(&published(&one)), all.into());
    assert_eq!(hashes(&published(&two)), all.into());
    check_invariants(&mut one, &all.into());
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(16))]

    /// For a chain of loose changes, the published set is exactly the longest
    /// causally contiguous signed prefix. Derive it from external input events,
    /// not from save, merge, readiness, fragment metadata or published clocks.
    #[test]
    fn loose_chain_publication_matches_an_external_signed_prefix_model(
        length in 1usize..7,
        responses in prop::collection::vec((0usize..6, any::<bool>()), 0..20),
    ) {
        let k = key(42);
        let wrong_key = key(99);
        let mut doc = Automerge::from_config(automerge::Config::default().attested(author(&k)).unwrap())
            .with_actor(ActorId::from([1])).unwrap();
        let chain: Vec<_> = (0..length).map(|_| commit_at_level(&mut doc, None, 0)).collect();
        let mut signed = vec![false; length];
        // Include the initial empty-evidence state and a final complete drain.
        let events = std::iter::once(None)
            .chain(responses.into_iter().map(|(index, valid)| Some((index % length, valid))))
            .chain((0..length).map(|index| Some((index, true))));
        for event in events {
            let inputs: Vec<_> = event.into_iter().map(|(index, valid)| {
                signed[index] |= valid;
                (chain[index], sign(if valid { &k } else { &wrong_key }, &chain[index]))
            }).collect();
            let SignatureBatchOutcome::Completed { insertions } = doc.insert_signatures(inputs) else { panic!("chain completion cannot conflict") };
            if let Some((_, valid)) = event {
                prop_assert_eq!(insertions[0].result.is_ok(), valid);
            }
            let prefix_len = signed.iter().take_while(|signed| **signed).count();
            let expected: BTreeSet<_> = chain[..prefix_len].iter().copied().collect();
            prop_assert_eq!(hashes(&published(&doc)), expected);
            prop_assert_eq!(doc.has_unpublished_changes(), prefix_len < length);
            let expected_requests: BTreeSet<_> = chain.iter().zip(&signed)
                .filter_map(|(hash, signed)| (!signed).then_some(*hash)).collect();
            prop_assert_eq!(requested(&mut doc).into_iter().collect::<BTreeSet<_>>(), expected_requests);
        }
    }

    /// Random schedules of commits (two actors, occasional branch), partial
    /// signing responses (including stale and foreign-key ones), saves and
    /// reloads: the lifecycle invariants hold after every step and the
    /// history drains completely once every request is honoured.
    #[test]
    #[allow(deprecated)]
    fn lifecycle_invariants_hold_under_random_schedules(
        steps in prop::collection::vec(0u8..10, 1..14)
    ) {
        let k = key(1);
        let mut doc = Automerge::from_config(automerge::Config::default().attested(author(&k)).unwrap()).with_actor(ActorId::from([1])).unwrap();
        let mut local = BTreeSet::new();
        let mut stale: Vec<ChangeHash> = Vec::new();
        for (i, step) in steps.iter().enumerate() {
            match step {
                0..=3 => {
                    doc.set_actor(ActorId::from([1 + (step % 2)])).unwrap();
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
                        LoadOptions::new().attested(author(&k)).unwrap(),
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
            let requests = doc.signing_requests().unwrap();
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
