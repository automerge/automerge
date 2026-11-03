//! Nested fragments, pruned accepted history, and larger history fixtures.
//! Membership is constructed for simple linear/branch shapes, not validated
//! by the selector. Hash levels are real and checked when constructing fixtures.

use std::collections::BTreeSet;
use std::num::NonZeroU64;
use std::sync::OnceLock;

use ed25519_dalek::SigningKey;

use crate::admission::tests::AcceptedHistoryArray;
use crate::admission::{select, CandidateHistory};
use crate::change::encode_author_footer;
use crate::signature::SignatureStore;
use crate::{ActorId, Author, Automerge, Change, ChangeHash, ExpandedChange, Fragment};

use super::super::retain_signature;
use super::assert_matches;

mod propagation;
mod pruned;
mod scaling;

fn key(actor: u8) -> SigningKey {
    SigningKey::from_bytes(&[7 + actor; 32])
}

fn empty_change(actor: u8, seq: u64, deps: &[ChangeHash], time: i64) -> Change {
    let author = (seq == 1).then(|| Author::from(key(actor).verifying_key().to_bytes().to_vec()));
    let mut deps = deps.to_vec();
    deps.sort();
    Change::from(ExpandedChange {
        operations: Vec::new(),
        actor_id: ActorId::from([actor + 1]),
        hash: None,
        seq,
        start_op: NonZeroU64::new(1).unwrap(),
        time,
        message: None,
        deps,
        extra_bytes: encode_author_footer(&author).into_owned(),
        author,
    })
}

fn at_level(actor: u8, seq: u64, deps: &[ChangeHash], level: usize) -> Change {
    (0..2_000_000)
        .map(|time| empty_change(actor, seq, deps, time))
        .find(|change| change.hash().fragment_level() == level)
        .expect("bounded fixture level search exhausted")
}

fn complete_fragment(changes: &[Change]) -> Fragment {
    let head = changes.last().unwrap().hash();
    let level = head.fragment_level();
    assert!(level > 0);
    assert!(changes[..changes.len() - 1]
        .iter()
        .all(|c| c.hash().fragment_level() < level));
    Fragment {
        head,
        level,
        boundary: Vec::new(),
        checkpoints: changes[..changes.len() - 1]
            .iter()
            .map(Change::hash)
            .filter(|hash| hash.fragment_level() > 0)
            .collect(),
        members: changes.iter().map(Change::hash).collect(),
    }
}

fn valid_history(changes: &[Change]) {
    let mut doc = Automerge::new();
    doc.apply_changes(changes.to_vec()).unwrap();
}

// F={a,f}, G={a,f,x,g}. A writes a,f,g; B writes x. G's level-2
// head absorbs the level-1 F. Keeping this in OnceLock avoids repeated search.
struct Nested {
    changes: Vec<Change>,
    f: Fragment,
    g: Fragment,
}

fn nested() -> &'static Nested {
    static FIXTURE: OnceLock<Nested> = OnceLock::new();
    FIXTURE.get_or_init(|| {
        // Parameters found by bounded timestamp search, then pinned to avoid
        // repeating the level-2 search in ordinary tests. Hashes are not forged.
        let a = empty_change(0, 1, &[], 0);
        let f = empty_change(0, 2, &[a.hash()], 595);
        let x = empty_change(1, 1, &[f.hash()], 0);
        let g = empty_change(0, 3, &[f.hash(), x.hash()], 105_563);
        let changes = vec![a, f, x, g];
        assert_eq!(
            changes
                .iter()
                .map(|c| c.hash().fragment_level())
                .collect::<Vec<_>>(),
            [0, 1, 0, 2]
        );
        valid_history(&changes);
        let f = complete_fragment(&changes[..2]);
        let g = complete_fragment(&changes);
        assert_eq!(g.checkpoints, [f.head]);
        assert!(f.members.iter().all(|hash| g.members.contains(hash)));
        Nested { changes, f, g }
    })
}

fn evidence(changes: &[Change], indices: &[usize]) -> (SignatureStore, BTreeSet<ChangeHash>) {
    let mut store = SignatureStore::default();
    let mut signed = BTreeSet::new();
    for &i in indices {
        let change = &changes[i];
        let actor = change.actor_id().to_bytes()[0] - 1;
        retain_signature(&mut store, change, &key(actor));
        signed.insert(change.hash());
    }
    (store, signed)
}

#[test]
fn nested_level_two_fragment_matches_oracle_for_all_signature_sets() {
    let data = nested();
    for mask in 0usize..16 {
        let indices: Vec<_> = (0..4).filter(|i| mask & (1 << i) != 0).collect();
        let (store, signed) = evidence(&data.changes, &indices);
        for supplied in 0usize..4 {
            for reverse in [false, true] {
                let mut candidates = Vec::new();
                if supplied & 1 != 0 {
                    candidates.push(CandidateHistory::CanonicalFragment {
                        fragment: &data.f,
                        members: data.changes[..2].iter().collect(),
                    });
                }
                if supplied & 2 != 0 {
                    candidates.push(CandidateHistory::CanonicalFragment {
                        fragment: &data.g,
                        members: data.changes.iter().collect(),
                    });
                }
                if reverse {
                    candidates.reverse();
                }
                assert_matches(
                    &[],
                    &candidates,
                    &store,
                    &signed,
                    &format!("nested mask={mask:b}, supplied={supplied:b}, reverse={reverse}"),
                );
            }
        }
    }
}

#[test]
fn blocked_container_does_not_hide_an_independently_attested_inner_fragment() {
    let data = nested();
    let (store, signed) = evidence(&data.changes, &[1, 3]);
    let candidates = [
        CandidateHistory::CanonicalFragment {
            fragment: &data.f,
            members: data.changes[..2].iter().collect(),
        },
        CandidateHistory::CanonicalFragment {
            fragment: &data.g,
            members: data.changes.iter().collect(),
        },
    ];
    assert_matches(
        &[],
        &candidates,
        &store,
        &signed,
        "blocked container with sig(f)",
    );
    let accepted = AcceptedHistoryArray::default();
    let result = select(&accepted, &candidates, &store);
    assert_eq!(result.selected().len(), 1);
    assert!(std::ptr::eq(result.selected()[0], &candidates[0]));
    assert_eq!(result.pending().len(), 1);
    assert!(std::ptr::eq(result.pending()[0], &candidates[1]));
}

#[test]
fn nested_descendant_evidence_requires_its_container_history() {
    let data = nested();
    let candidates = [
        CandidateHistory::CanonicalFragment {
            fragment: &data.f,
            members: data.changes[..2].iter().collect(),
        },
        CandidateHistory::CanonicalFragment {
            fragment: &data.g,
            members: data.changes.iter().collect(),
        },
    ];
    let accepted = AcceptedHistoryArray::default();
    let (store, signed) = evidence(&data.changes, &[3]);
    assert_matches(&[], &candidates, &store, &signed, "container blocked on x");
    let result = select(&accepted, &candidates, &store);
    assert!(result.selected().is_empty());
    assert_eq!(result.pending().len(), 2);

    let (store, signed) = evidence(&data.changes, &[2, 3]);
    assert_matches(
        &[],
        &candidates,
        &store,
        &signed,
        "container fully attested",
    );
    let result = select(&accepted, &candidates, &store);
    assert_eq!(result.selected().len(), 2);
    assert!(result.pending().is_empty());
    // sig(g) remains stored, but F alone cannot use the unavailable g.
    let result = select(&accepted, &candidates[..1], &store);
    assert!(result.selected().is_empty());
    assert_eq!(result.pending().len(), 1);
}
