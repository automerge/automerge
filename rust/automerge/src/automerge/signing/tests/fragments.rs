//! Evidence gates availability, never the graph-derived fragment descriptors.

use std::collections::{BTreeMap, HashSet};

use super::super::has_signature;
use super::*;
use crate::admission::{self, AcceptedHistory};
use crate::change_graph::FragmentReadiness;
use crate::transaction::CommitOptions;
use crate::{Fragment, PatchLog, Signature};

fn document() -> (Automerge, SigningKey) {
    let key = SigningKey::from_bytes(&[7; 32]);
    let author = Author::from(key.verifying_key().to_bytes().to_vec());
    let doc = Automerge::signing()
        .with_author(Some(author))
        .with_actor(ActorId::from([1]));
    (doc, key)
}

fn append(doc: &mut Automerge, heads: Option<&[ChangeHash]>, level: usize) -> ChangeHash {
    let value = doc.get_changes(&[]).len() as i64 + 1;
    for time in 0..10_000 {
        let mut next = doc.clone();
        let mut tx = match heads {
            Some(heads) => next.transaction_at(PatchLog::inactive(), heads).unwrap(),
            None => next.transaction(),
        };
        tx.put(crate::ROOT, "value", value).unwrap();
        let (hash, _) = tx.commit_with(CommitOptions::default().with_time(time));
        let hash = hash.unwrap();
        if hash.fragment_level() == level {
            *doc = next;
            return hash;
        }
    }
    panic!("fixture level search exhausted");
}

fn signature(hash: ChangeHash, key: &SigningKey) -> ed25519::Signature {
    ed25519::Signature::from_bytes(key.sign(&signing_message(&hash)).to_bytes())
}

fn sign(doc: &mut Automerge, hash: ChangeHash, key: &SigningKey) {
    doc.try_signing()
        .unwrap()
        .insert_signature(&hash, &signature(hash, key))
        .unwrap();
}

fn canonical_fragments(doc: &Automerge) -> Vec<Fragment> {
    let mut fragments: Vec<_> = doc.change_graph.fragments(..).collect();
    fragments.reverse();
    fragments
}

fn assert_readiness_matches_graph(doc: &mut Automerge) {
    let signing = doc.try_signing().unwrap();
    let expected = FragmentReadiness::new(signing.graph, |hash| {
        has_signature(
            signing.graph,
            signing.authors,
            &signing.state.signatures,
            hash,
        )
    });
    assert_eq!(signing.state.fragment_readiness, expected);
}

struct EmptyHistory;

impl AcceptedHistory for EmptyHistory {
    fn contains(&self, _: &ChangeHash) -> bool {
        false
    }
}

fn assert_independently_attested(doc: &mut Automerge, fragment: &Fragment) {
    assert!(fragment.boundary.is_empty());
    let members: Vec<_> = fragment
        .members
        .iter()
        .map(|hash| doc.get_change_by_hash(hash).unwrap())
        .collect();
    let candidate = [CandidateHistory::CanonicalFragment {
        fragment,
        members: members.iter().collect(),
    }];
    let signing = doc.try_signing().unwrap();
    let selection = admission::select(&EmptyHistory, &candidate, &signing.state.signatures);
    assert_eq!(selection.selected().len(), 1);
    assert!(selection.pending().is_empty());
}

#[test]
fn fragments_wait_for_frontier_evidence_not_interior_signatures() {
    let (mut doc, key) = document();
    let a1 = append(&mut doc, None, 0);
    let a2 = append(&mut doc, None, 0);
    let a3 = append(&mut doc, None, 1);
    let canonical = canonical_fragments(&doc);
    assert_eq!(canonical.len(), 1);
    assert!(doc.fragments(..).is_empty());
    assert!(doc.get_fragment(a3).is_none());

    sign(&mut doc, a2, &key);
    // a2 is a member of the fixed a3 fragment, not loose history just because
    // a3 cannot yet be materialized. No singleton fallback changes the partition.
    assert!(doc.fragments(..).is_empty());
    assert_eq!(canonical_fragments(&doc), canonical);

    sign(&mut doc, a3, &key);
    assert_readiness_matches_graph(&mut doc);
    let fragments = doc.fragments(..);
    assert_eq!(fragments, canonical);
    let fragment = &fragments[0];
    assert_eq!(fragment.head, a3);
    assert_eq!(fragment.members.len(), 3);
    assert!([a1, a2, a3].iter().all(|h| fragment.members.contains(h)));
    assert!(doc.fragments(0..=0).is_empty());
    assert_eq!(doc.get_fragment(a3).as_ref(), Some(fragment));
    assert_independently_attested(&mut doc, fragment);
    assert!(!doc.try_signing().unwrap().is_accepted(&a3));
}

// A's signed a2 attests a1, but is not in the branch fragment rooted at a1.
// B has the same author as A but is a distinct actor chain.
fn historical_branch() -> (Automerge, SigningKey, ChangeHash, ChangeHash, ChangeHash) {
    let (mut doc, key) = document();
    let a1 = append(&mut doc, None, 0);
    let a2 = append(&mut doc, None, 1);
    sign(&mut doc, a2, &key);
    let fragment = doc.get_fragment(a2).unwrap();
    let members = doc.get_changes(&[]);
    doc.try_signing()
        .unwrap()
        .accept_applied(&[CandidateHistory::CanonicalFragment {
            fragment: &fragment,
            members: members.iter().collect(),
        }])
        .unwrap();
    doc.set_actor(ActorId::from([3]));
    let b1 = append(&mut doc, Some(&[a1]), 1);
    sign(&mut doc, b1, &key);
    (doc, key, a1, a2, b1)
}

#[test]
fn transaction_at_withholds_a_fragment_requiring_external_attestation() {
    let (mut doc, key, a1, a2, b1) = historical_branch();
    let canonical = canonical_fragments(&doc);
    assert_eq!(canonical.len(), 2);
    assert!(doc.try_signing().unwrap().is_accepted(&a1));
    assert_eq!(doc.fragments(..), [canonical[0].clone()]);
    assert_eq!(canonical[0].head, a2);
    assert_eq!(canonical[1].head, b1);
    assert!(doc.get_fragment(b1).is_none());
    assert!(doc.fragments(0..=0).is_empty()); // no hard-head-to-loose fallback

    sign(&mut doc, a1, &key);
    assert_readiness_matches_graph(&mut doc);
    assert_eq!(doc.fragments(..), canonical);
    assert_eq!(doc.get_fragment(b1), Some(canonical[1].clone()));
    assert_independently_attested(&mut doc, &canonical[1]);
    let mut loaded = Automerge::new();
    for bytes in doc.bundle_fragments(doc.fragments(..)) {
        loaded.load_incremental(&bytes).unwrap();
    }
    assert_eq!(loaded.get_heads(), doc.get_heads());
    assert_eq!(loaded.get_changes(&[]).len(), 3);
}

#[test]
fn delayed_evidence_changes_only_availability_and_does_not_affect_a_clone() {
    let (mut doc, key, a1, _, b1) = historical_branch();
    let canonical = canonical_fragments(&doc);
    let before = doc.fragments(..);
    let mut clone = doc.clone();
    sign(&mut doc, a1, &key);
    assert_eq!(doc.fragments(..), canonical);
    assert_eq!(doc.get_fragment(b1), Some(canonical[1].clone()));
    assert_eq!(clone.fragments(..), before);
    sign(&mut clone, a1, &key);
    assert_eq!(clone.fragments(..), canonical);
}

#[test]
fn later_fragment_keeps_an_unavailable_earlier_fragment_as_its_boundary() {
    let (mut doc, key, a1, a2, b1) = historical_branch();
    doc.set_actor(ActorId::from([1]));
    let a3 = append(&mut doc, None, 1); // joins both branches
    let canonical = canonical_fragments(&doc);
    let joined = canonical.iter().find(|f| f.head == a3).unwrap().clone();
    assert_eq!(joined.members, [a3]);
    assert!(joined.checkpoints.is_empty());
    assert_eq!(joined.boundary.len(), 2);
    assert!(joined.boundary.contains(&a2));
    assert!(joined.boundary.contains(&b1));
    assert!(doc.get_fragment(b1).is_none());
    assert!(doc.get_fragment(a3).is_none());

    sign(&mut doc, a3, &key);
    assert_eq!(doc.get_fragment(a3), Some(joined.clone()));
    let bytes_before = doc.bundle_fragments([joined.clone()]);
    assert!(doc.get_fragment(b1).is_none());
    // It can wait causally on b1 without borrowing b1's attestation or absorbing
    // its members. Making b1 available later must not repartition a3.
    sign(&mut doc, a1, &key);
    assert_eq!(doc.fragments(..), canonical);
    assert_eq!(doc.get_fragment(a3), Some(joined.clone()));
    assert_eq!(doc.bundle_fragments([joined]), bytes_before);
}

#[test]
fn delayed_signature_on_a_linear_boundary_does_not_change_the_next_fragment() {
    let (mut doc, key) = document();
    append(&mut doc, None, 0);
    let b = append(&mut doc, None, 1);
    let c = append(&mut doc, None, 0);
    let d = append(&mut doc, None, 1);
    let canonical = canonical_fragments(&doc);
    assert_eq!(canonical.len(), 2);
    let later = canonical[1].clone();
    assert_eq!(later.head, d);
    assert_eq!(later.boundary, [b]);
    assert_eq!(later.members.len(), 2);
    assert!(later.members.contains(&c));
    assert!(later.members.contains(&d));

    sign(&mut doc, d, &key);
    assert_eq!(doc.fragments(..).as_slice(), std::slice::from_ref(&later));
    assert!(doc.get_fragment(b).is_none());
    let bytes_before = doc.bundle_fragments([later.clone()]);
    sign(&mut doc, b, &key);
    assert_readiness_matches_graph(&mut doc);
    assert_eq!(doc.fragments(..), canonical);
    assert_eq!(doc.get_fragment(d), Some(later.clone()));
    assert_eq!(doc.bundle_fragments([later]), bytes_before);
}

#[test]
fn only_matching_verified_evidence_unlocks_a_fragment() {
    let (mut doc, key) = document();
    append(&mut doc, None, 0);
    let tip = append(&mut doc, None, 1);
    doc.try_signing()
        .unwrap()
        .state
        .signatures
        .insert_unchecked(tip, Signature::ed25519(signature(tip, &key)));
    assert!(doc.fragments(..).is_empty());
    let wrong_key = SigningKey::from_bytes(&[8; 32]);
    assert!(doc
        .try_signing()
        .unwrap()
        .insert_signature(&tip, &signature(tip, &wrong_key))
        .is_err());
    assert_readiness_matches_graph(&mut doc);
    assert!(doc.fragments(..).is_empty());
    sign(&mut doc, tip, &key); // promotes the unchecked entry
    assert_readiness_matches_graph(&mut doc);
    assert_eq!(doc.fragments(1..).len(), 1);

    let (mut other, _) = document();
    append(&mut other, None, 0);
    assert_eq!(append(&mut other, None, 1), tip);
    let author = Author::from(wrong_key.verifying_key().to_bytes().to_vec());
    let verifier = ed25519::VerifyingAuthor::new(author).unwrap();
    other
        .try_signing()
        .unwrap()
        .state
        .signatures
        .insert_ed25519(
            verifier
                .verify_hash(&tip, &signature(tip, &wrong_key))
                .unwrap(),
        )
        .unwrap();
    assert!(other.fragments(..).is_empty());
}

#[test]
fn readiness_survives_signature_arrival_order_and_actor_shifts() {
    let (mut doc, key) = document();
    let a = append(&mut doc, None, 0);
    doc.set_actor(ActorId::from([3]));
    let b = append(&mut doc, None, 1);
    let mut reversed = doc.clone();
    sign(&mut doc, a, &key);
    assert!(doc.fragments(1..).is_empty());
    sign(&mut doc, b, &key);
    sign(&mut reversed, b, &key);
    assert!(reversed.fragments(..).is_empty());
    assert!(reversed.get_fragment(b).is_none());
    sign(&mut reversed, a, &key);
    let expected = doc.fragments(..);
    assert_eq!(expected, canonical_fragments(&doc));
    assert_eq!(expected, reversed.fragments(..));
    assert_independently_attested(&mut doc, &expected[0]);

    doc.set_actor(ActorId::from([0]));
    {
        let mut tx = doc.transaction();
        tx.put(crate::ROOT, "rolled-back", true).unwrap();
        tx.rollback();
    }
    assert_readiness_matches_graph(&mut doc);
    assert_eq!(doc.fragments(..), expected);
    let c = append(&mut doc, None, 0);
    assert_readiness_matches_graph(&mut doc);
    sign(&mut doc, c, &key);
    let fragments = doc.fragments(..);
    assert_eq!(fragments[0], expected[0]);
    assert_eq!(fragments[1].head, c);
    assert_eq!(fragments[1].level, 0);
}

#[test]
fn signatures_update_readiness_in_place_and_leave_loose_classification_unchanged() {
    let (mut doc, key) = document();
    let interior = append(&mut doc, None, 0);
    let head = append(&mut doc, None, 1);
    let loose = append(&mut doc, None, 0);
    let canonical = canonical_fragments(&doc);
    assert!(doc.fragments(..).is_empty());
    assert_readiness_matches_graph(&mut doc);
    sign(&mut doc, interior, &key);
    assert_readiness_matches_graph(&mut doc);
    assert!(doc.fragments(..).is_empty()); // interior is not loose
    sign(&mut doc, head, &key);
    assert_readiness_matches_graph(&mut doc);
    assert_eq!(doc.fragments(..), [canonical[0].clone()]);
    sign(&mut doc, head, &key); // duplicate evidence does not decrement twice
    assert_readiness_matches_graph(&mut doc);
    sign(&mut doc, loose, &key);
    assert_readiness_matches_graph(&mut doc);
    assert_eq!(doc.fragments(..), canonical);
}

#[test]
fn multiple_missing_frontiers_are_resolved_in_place_in_either_order() {
    let (mut original, key) = document();
    let a = append(&mut original, None, 0);
    original.set_actor(ActorId::from([2]));
    let b = append(&mut original, None, 0);
    original.set_actor(ActorId::from([3]));
    let head = append(&mut original, None, 1);
    let canonical = canonical_fragments(&original);
    sign(&mut original, head, &key);
    assert!(original.fragments(..).is_empty());
    for order in [[a, b], [b, a]] {
        for query_between in [false, true] {
            let mut doc = original.clone();
            sign(&mut doc, order[0], &key);
            assert_readiness_matches_graph(&mut doc);
            if query_between {
                assert!(doc.fragments(..).is_empty());
            }
            sign(&mut doc, order[0], &key); // duplicate while still pending
            sign(&mut doc, order[1], &key);
            assert_readiness_matches_graph(&mut doc);
            assert_eq!(doc.fragments(..), canonical);
            assert_independently_attested(&mut doc, &canonical[0]);
        }
    }
}

#[test]
fn every_signature_order_emits_only_the_original_graph_derived_descriptors() {
    use itertools::Itertools;

    let (mut original, key) = document();
    let a1 = append(&mut original, None, 0);
    let a2 = append(&mut original, None, 1);
    original.set_actor(ActorId::from([3]));
    let b1 = append(&mut original, Some(&[a1]), 1);
    original.set_actor(ActorId::from([1]));
    let a3 = append(&mut original, None, 1);
    original.set_actor(ActorId::from([4]));
    let loose = append(&mut original, None, 0);
    // Freeze the oracle before any signatures exist. Its shape must not be
    // recomputed against evidence, even for fragments not yet materializable.
    let canonical = canonical_fragments(&original);
    let frontier_sets: Vec<HashSet<ChangeHash>> = canonical
        .iter()
        .map(|fragment| {
            let mut frontiers: BTreeMap<ActorId, (u64, ChangeHash)> = BTreeMap::new();
            for hash in &fragment.members {
                let change = original.get_change_by_hash(hash).unwrap();
                let entry = frontiers
                    .entry(change.actor_id().clone())
                    .or_insert((0, *hash));
                if change.seq() > entry.0 {
                    *entry = (change.seq(), *hash);
                }
            }
            frontiers.values().map(|(_, hash)| *hash).collect()
        })
        .collect();

    for (index, order) in [a1, a2, b1, a3, loose]
        .into_iter()
        .permutations(5)
        .enumerate()
    {
        let mut doc = original.clone();
        if index % 2 == 0 {
            assert!(doc.fragments(..).is_empty()); // queries do not drive readiness updates
        }
        let mut signed = HashSet::new();
        let mut previously_emitted = BTreeMap::new();
        for hash in order {
            sign(&mut doc, hash, &key);
            assert_readiness_matches_graph(&mut doc);
            signed.insert(hash);
            let expected: Vec<_> = canonical
                .iter()
                .zip(&frontier_sets)
                .filter(|(_, frontiers)| frontiers.is_subset(&signed))
                .map(|(fragment, _)| fragment.clone())
                .collect();
            let actual = doc.fragments(..);
            assert_eq!(actual, expected);
            for fragment in actual {
                assert_eq!(doc.get_fragment(fragment.head), Some(fragment.clone()));
                let bytes = doc.bundle_fragments([fragment.clone()]);
                let entry = previously_emitted
                    .entry(fragment.head)
                    .or_insert_with(|| (fragment.clone(), bytes.clone()));
                assert_eq!(*entry, (fragment, bytes));
            }
            assert_eq!(canonical_fragments(&doc), canonical);
        }
    }
}

#[test]
fn one_frontier_signature_makes_all_fragments_requiring_it_available() {
    let (mut doc, key) = document();
    let root = append(&mut doc, None, 0);
    doc.set_actor(ActorId::from([3]));
    let left = append(&mut doc, Some(&[root]), 1);
    doc.set_actor(ActorId::from([4]));
    let right = append(&mut doc, Some(&[root]), 1);
    let canonical = canonical_fragments(&doc);
    assert_eq!(canonical.len(), 2);
    sign(&mut doc, left, &key);
    sign(&mut doc, right, &key);
    assert!(doc.fragments(..).is_empty());
    sign(&mut doc, root, &key);
    assert_readiness_matches_graph(&mut doc);
    assert_eq!(doc.fragments(..), canonical);
    for fragment in &canonical {
        assert_independently_attested(&mut doc, fragment);
    }
}

#[test]
fn history_growth_updates_readiness_before_any_fragment_query() {
    let (mut doc, key) = document();
    assert_readiness_matches_graph(&mut doc);
    let first = append(&mut doc, None, 1);
    let loose = append(&mut doc, None, 0);
    let next = append(&mut doc, None, 1);
    assert_readiness_matches_graph(&mut doc);
    sign(&mut doc, first, &key);
    assert_readiness_matches_graph(&mut doc);
    assert_eq!(doc.fragments(..).len(), 1);

    // Neither loose history nor a new fragment discards existing readiness. The new
    // fragment needs only its own frontier, not the signature on its boundary.
    append(&mut doc, None, 0);
    assert_readiness_matches_graph(&mut doc);
    let latest = append(&mut doc, None, 1);
    assert_readiness_matches_graph(&mut doc);
    sign(&mut doc, loose, &key);
    assert!(doc.get_fragment(next).is_none());
    sign(&mut doc, latest, &key);
    assert!(doc.get_fragment(latest).is_some());
    sign(&mut doc, next, &key);
    assert_eq!(doc.fragments(..), canonical_fragments(&doc));
}

#[test]
fn pending_readiness_survives_actor_insertion_and_rollback() {
    let (mut doc, key) = document();
    let root = append(&mut doc, None, 0);
    doc.set_actor(ActorId::from([3]));
    let head = append(&mut doc, None, 1);
    sign(&mut doc, head, &key);
    assert!(doc.fragments(..).is_empty());

    doc.set_actor(ActorId::from([0]));
    {
        let mut tx = doc.transaction();
        tx.put(crate::ROOT, "rolled-back", true).unwrap();
        tx.rollback();
    }
    assert_readiness_matches_graph(&mut doc);
    let loose = append(&mut doc, None, 0);
    assert_readiness_matches_graph(&mut doc);
    assert!(doc.fragments(..).is_empty());
    sign(&mut doc, root, &key);
    assert!(doc.get_fragment(head).is_some());
    sign(&mut doc, loose, &key);
    assert_eq!(doc.fragments(..), canonical_fragments(&doc));
}

#[test]
fn applying_a_batch_on_an_existing_actor_reconciles_fragment_readiness() {
    let (mut doc, key) = document();
    let first = append(&mut doc, None, 1);
    sign(&mut doc, first, &key);
    let before = doc.fragments(..);
    assert_eq!(before.len(), 1);
    assert_readiness_matches_graph(&mut doc);
    let actors_before = doc.ops.actors.len();
    assert_eq!(doc.change_graph.actor_ids().count(), actors_before);

    let mut sender = doc.clone();
    let next = append(&mut sender, None, 1);
    let change = sender.get_change_by_hash(&next).unwrap();
    doc.apply_changes_batch([change]).unwrap();
    assert_eq!(doc.ops.actors.len(), actors_before);
    // The shared history update must index the new head without discarding the
    // ready first fragment, even though no actor insertion occurred.
    assert_readiness_matches_graph(&mut doc);
    assert_eq!(doc.fragments(..), before);
    assert!(doc.get_fragment(next).is_none());

    sign(&mut doc, next, &key);
    let canonical = canonical_fragments(&doc);
    assert_eq!(canonical.len(), 2);
    assert_eq!(doc.fragments(..), canonical);
    assert_eq!(doc.get_fragment(next), Some(canonical[1].clone()));
    assert_readiness_matches_graph(&mut doc);
}
