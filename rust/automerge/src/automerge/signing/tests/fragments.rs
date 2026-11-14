//! Outgoing fragment readiness, candidate/signature GC, and proof-bearing export tests.

use std::collections::{BTreeMap, HashSet};

use itertools::Itertools;

use super::super::has_signature;
use super::super::readiness::FragmentReadiness;
use super::*;
use crate::storage::parse::Input;
use crate::storage::signatures::SignaturesChunk;
use crate::storage::{Bundle, Chunk};
use crate::Fragment;

fn document() -> (Automerge, SigningKey) {
    let k = key(7);
    let doc = Automerge::signing()
        .with_author(Some(author(&k)))
        .with_actor(ActorId::from([1]));
    (doc, k)
}

fn append(doc: &mut Automerge, heads: Option<&[ChangeHash]>, level: usize) -> ChangeHash {
    let value = doc.get_changes(&[]).len() as i64 + 1;
    append_at_level(doc, heads, value, level)
}

fn commit_with_time(
    doc: &mut Automerge,
    heads: Option<&[ChangeHash]>,
    prop: &str,
    value: i64,
    time: i64,
) -> ChangeHash {
    let mut tx = match heads {
        Some(heads) => doc.transaction_at(PatchLog::inactive(), heads).unwrap(),
        None => doc.transaction(),
    };
    tx.put(ROOT, prop, value).unwrap();
    tx.commit_with(CommitOptions::default().with_time(time))
        .0
        .unwrap()
}

fn canonical_fragments(doc: &Automerge) -> Vec<Fragment> {
    let mut fragments: Vec<_> = doc.change_graph.fragments(..).collect();
    fragments.reverse();
    fragments
}

fn assert_readiness_matches_graph(doc: &mut Automerge) {
    let signing = doc.try_signing().unwrap();
    let mut expected = FragmentReadiness::from_graph(signing.graph, |hash| {
        has_signature(
            signing.graph,
            signing.authors,
            &signing.state.signatures,
            hash,
        )
    });
    expected.prune_covered(signing.graph);
    assert_eq!(signing.state.fragment_readiness, expected);
}

fn decode_group(bytes: &[u8]) -> (Vec<Change>, SignaturesChunk<'_>) {
    let (remaining, content) = Chunk::parse(Input::new(bytes)).unwrap();
    assert!(content.checksum_valid());
    let members = match content {
        Chunk::Change(change) => {
            vec![Change::new_from_unverified(change.into_owned(), None).unwrap()]
        }
        Chunk::CompressedChange(change, compressed) => {
            vec![Change::new_from_unverified(change, Some(compressed.into_owned())).unwrap()]
        }
        Chunk::Bundle(bundle) => Bundle::new_from_unverified(bundle.into_owned())
            .unwrap()
            .to_changes()
            .unwrap(),
        _ => panic!("fragment must start with its content chunk"),
    };
    let (remaining, proof) = Chunk::parse(remaining.reset()).unwrap();
    assert!(remaining.is_empty(), "exactly one attached signature chunk");
    assert!(proof.checksum_valid());
    let Chunk::Signatures(proof) = proof else {
        panic!("proof must immediately follow content")
    };
    (members, proof)
}

#[test]
fn every_signature_order_emits_only_graph_derived_descriptors_and_valid_proofs() {
    let (mut original, k) = document();
    let a1 = append(&mut original, None, 0);
    let a2 = append(&mut original, None, 1);
    original.set_actor(ActorId::from([3]));
    let b1 = append(&mut original, Some(&[a1]), 1);
    original.set_actor(ActorId::from([1]));
    let a3 = append(&mut original, None, 1);
    original.set_actor(ActorId::from([4]));
    let loose = append(&mut original, None, 0);

    let canonical = canonical_fragments(&original);
    let changes = original.get_changes(&[]);
    let positions: BTreeMap<_, _> = changes
        .iter()
        .enumerate()
        .map(|(i, change)| (change.hash(), i))
        .collect();
    let singletons: Vec<_> = changes
        .iter()
        .filter(|change| change.hash().fragment_level() == 0)
        .map(|change| original.change_graph.get_fragment(change.hash()).unwrap())
        .collect();
    let frontier_sets: Vec<HashSet<ChangeHash>> = canonical
        .iter()
        .map(|fragment| {
            original
                .change_graph
                .fragment_requirements(fragment.head)
                .unwrap()
                .hashes()
                .copied()
                .collect()
        })
        .collect();

    for order in [a1, a2, b1, a3, loose].into_iter().permutations(5) {
        let mut doc = original.clone();
        let mut signed = HashSet::new();
        let mut previously_emitted = BTreeMap::new();
        for hash in order {
            sign_doc(&mut doc, hash, &k);
            assert_readiness_matches_graph(&mut doc);
            signed.insert(hash);
            let mut expected: Vec<_> = canonical
                .iter()
                .zip(&frontier_sets)
                .filter(|(fragment, frontiers)| fragment.level > 0 && frontiers.is_subset(&signed))
                .map(|(fragment, _)| fragment.clone())
                .collect();
            let represented: HashSet<_> = expected
                .iter()
                .flat_map(|fragment| fragment.members.iter().copied())
                .collect();
            expected.extend(
                singletons
                    .iter()
                    .filter(|fragment| {
                        signed.contains(&fragment.head) && !represented.contains(&fragment.head)
                    })
                    .cloned(),
            );
            expected.sort_unstable_by_key(|fragment| positions[&fragment.head]);
            let actual = doc.fragments(..);
            assert_eq!(actual, expected);
            for fragment in actual {
                assert_eq!(doc.get_fragment(fragment.head), Some(fragment.clone()));
                let bytes = doc.bundle_fragments([fragment.clone()]);
                assert_eq!(bytes.len(), 1);
                let (decoded_members, proof) = decode_group(&bytes[0]);
                assert_eq!(
                    decoded_members
                        .iter()
                        .map(Change::hash)
                        .collect::<HashSet<_>>(),
                    fragment.members.iter().copied().collect(),
                );
                let reqs = doc
                    .change_graph
                    .fragment_requirements(fragment.head)
                    .unwrap();
                assert_eq!(
                    proof.iter().map(|(h, _)| h).collect::<Vec<_>>(),
                    reqs.hashes().copied().collect::<Vec<_>>(),
                );
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
fn supersession_and_branch_gc_reclaim_covered_candidates_and_signatures() {
    // 1. Linear level-2 supersession: keeps fallback until new head is signed,
    //    then reclaims old candidate and signature bytes while preserving DAG lookup.
    let (mut doc, k) = document();
    append(&mut doc, None, 0);
    let old = append(&mut doc, None, 1);
    let loose = append(&mut doc, None, 0);
    sign_doc(&mut doc, old, &k);
    sign_doc(&mut doc, loose, &k);
    let before = doc.fragments(..);
    assert_eq!(before.len(), 2);

    // Precomputed level-2 commit timestamp for this deterministic fixture.
    let new = commit_with_time(&mut doc, None, "value", 4, 45_260);
    assert_eq!(new.fragment_level(), 2);
    assert!(doc.change_graph.get_fragment(old).is_none());
    assert_eq!(doc.fragments(..), before);
    assert_eq!(doc.fragments(1..), [before[0].clone()]);
    assert_eq!(doc.fragments(0..=0), [before[1].clone()]);

    sign_doc(&mut doc, new, &k);
    let replacement = doc.get_fragment(new).unwrap();
    assert_eq!(doc.fragments(..), [replacement.clone()]);
    assert!(doc.fragments(0..=1).is_empty());
    assert_eq!(
        doc.change_graph.get_historical_fragment(old),
        Some(before[0].clone()),
    );
    assert!(doc.get_fragment(old).is_none());
    assert!(doc.bundle_fragments([before[0].clone()]).is_empty());
    assert_eq!(doc.try_signing().unwrap().state.signatures.len(), 1);
    assert_readiness_matches_graph(&mut doc);

    // 2. Shared frontier a1 stays alive across an incomplete sibling branch c1
    //    until c1 is also covered!
    let k1 = key(1);
    let k2 = key(2);
    let k3 = key(3);
    let mut branched = Automerge::signing()
        .with_author(Some(author(&k1)))
        .with_actor(ActorId::from([1]));
    let a1 = append_at_level(&mut branched, None, 1, 0);
    branched.set_author(Some(author(&k2)));
    branched.set_actor(ActorId::from([2]));
    let b1 = append_at_level(&mut branched, Some(&[a1]), 2, 1);
    branched.set_author(Some(author(&k3)));
    branched.set_actor(ActorId::from([3]));
    let c1 = append_at_level(&mut branched, Some(&[a1]), 3, 1);

    sign_doc(&mut branched, a1, &k1);
    sign_doc(&mut branched, b1, &k2);

    branched.set_author(Some(author(&k1)));
    branched.set_actor(ActorId::from([1]));
    let a2 = append_at_level(&mut branched, Some(&[b1]), 4, 0);
    branched.set_author(Some(author(&k2)));
    branched.set_actor(ActorId::from([2]));
    let b2 = commit_with_time(&mut branched, Some(&[a2]), "b2", 5, 7_197);
    assert_eq!(b2.fragment_level(), 2);
    sign_doc(&mut branched, a2, &k1);
    sign_doc(&mut branched, b2, &k2);

    assert_eq!(
        branched
            .try_signing()
            .unwrap()
            .state
            .signatures
            .ed25519_entries(&a1)
            .count(),
        1,
        "shared frontier a1 must stay retained for incomplete branch c1",
    );
    sign_doc(&mut branched, c1, &k3);
    assert!(branched.get_fragment(c1).is_some());

    // Joining above b2 and c1 at level 2 covers c1 via b2's causal past and
    // drops the last obligation on a1!
    branched.set_author(Some(author(&k3)));
    branched.set_actor(ActorId::from([3]));
    let c2 = commit_with_time(&mut branched, Some(&[b2, c1]), "c2", 6, 32_269);
    assert_eq!(c2.fragment_level(), 2);
    sign_doc(&mut branched, c2, &k3);
    assert_eq!(
        branched
            .try_signing()
            .unwrap()
            .state
            .signatures
            .ed25519_entries(&a1)
            .count(),
        0,
    );
}

#[test]
fn bundle_fragments_validates_descriptors_and_preserves_unsigned_encoding() {
    let (mut doc, k) = document();
    append(&mut doc, None, 0);
    doc.set_actor(ActorId::from([2]));
    let other = append(&mut doc, None, 0);
    doc.set_actor(ActorId::from([1]));
    let head = append(&mut doc, None, 1);
    let fragment = doc.change_graph.get_historical_fragment(head).unwrap();

    // Missing proof or partial shared-key proof withholds the whole group.
    assert!(doc.bundle_fragments([fragment.clone()]).is_empty());
    sign_doc(&mut doc, head, &k);
    assert!(doc.bundle_fragments([fragment.clone()]).is_empty());
    sign_doc(&mut doc, other, &k);
    assert_eq!(doc.bundle_fragments([fragment.clone()]).len(), 1);

    // Malformed descriptors are withheld; reordered member hashes still match.
    let mut missing = fragment.clone();
    missing.members.pop();
    let mut duplicated = fragment.clone();
    duplicated.members.push(head);
    let mut bad_level = fragment.clone();
    bad_level.level = 0;
    assert!(doc
        .bundle_fragments([missing, duplicated, bad_level])
        .is_empty());

    let mut reordered = fragment;
    reordered.members.reverse();
    assert_eq!(doc.bundle_fragments([reordered]).len(), 1);

    // Unsigned documents continue to emit bare content chunks without proofs.
    let unsigned = Automerge::load(&doc.save()).unwrap();
    for bytes in unsigned.bundle_fragments(unsigned.fragments(..)) {
        let (remaining, content) = Chunk::parse(Input::new(&bytes)).unwrap();
        assert!(content.checksum_valid());
        assert!(remaining.is_empty());
    }
}
