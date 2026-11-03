//! Accepted history exposes membership only, never change bytes or iteration.

use std::cell::RefCell;
use std::collections::HashSet;

use ed25519_dalek::SigningKey;

use crate::admission::{select, AcceptedHistory, CandidateHistory};
use crate::signature::SignatureStore;
use crate::transaction::{CommitOptions, Transactable};
use crate::{ActorId, Author, AutoCommit, Change, ChangeHash, Fragment, ROOT};

use super::{append_at_level, retain_signature};

struct Membership {
    hashes: HashSet<ChangeHash>,
    queries: RefCell<Vec<ChangeHash>>,
}

impl Membership {
    fn new(hashes: impl IntoIterator<Item = ChangeHash>) -> Self {
        Self {
            hashes: hashes.into_iter().collect(),
            queries: RefCell::new(Vec::new()),
        }
    }
}

impl AcceptedHistory for Membership {
    fn contains(&self, hash: &ChangeHash) -> bool {
        self.queries.borrow_mut().push(*hash);
        self.hashes.contains(hash)
    }
}

fn chain() -> (Vec<Change>, SigningKey) {
    let key = SigningKey::from_bytes(&[7; 32]);
    let author = Author::from(key.verifying_key().to_bytes().to_vec());
    let mut doc = AutoCommit::new()
        .with_author(Some(author))
        .with_actor(ActorId::from([1]));
    for value in 1..=3 {
        append_at_level(&mut doc, value, 0);
    }
    let changes = doc.get_changes(&[]);
    assert_eq!(changes[1].deps(), &[changes[0].hash()]);
    assert_eq!(changes[2].deps(), &[changes[1].hash()]);
    (changes, key)
}

#[test]
fn accepted_dependency_needs_no_accepted_change_bytes() {
    let (changes, key) = chain();
    let accepted = Membership::new(changes[..2].iter().map(Change::hash));
    let mut store = SignatureStore::default();
    for change in &changes {
        retain_signature(&mut store, change, &key);
    }
    let candidates = [CandidateHistory::Loose(&changes[2])];

    let result = select(&accepted, &candidates, &store);

    assert_eq!(result.selected().len(), 1);
    assert!(std::ptr::eq(result.selected()[0], &candidates[0]));
    assert!(result.pending().is_empty());
    // Only the candidate and its immediate accepted dependency can be queried.
    assert!(accepted
        .queries
        .borrow()
        .iter()
        .all(|hash| *hash == changes[1].hash() || *hash == changes[2].hash()));
    assert!(!accepted.queries.borrow().contains(&changes[0].hash()));
}

#[test]
fn present_signature_does_not_turn_an_unaccepted_dependency_into_accepted_history() {
    let (changes, key) = chain();
    let accepted = Membership::new([changes[0].hash()]);
    let mut store = SignatureStore::default();
    // All changes exist in the originating document and all signatures are held,
    // but a2 is neither accepted nor supplied as a candidate in this call.
    for change in &changes {
        retain_signature(&mut store, change, &key);
    }
    let candidates = [CandidateHistory::Loose(&changes[2])];

    let result = select(&accepted, &candidates, &store);

    assert!(result.selected().is_empty());
    assert_eq!(result.pending().len(), 1);
    assert!(std::ptr::eq(result.pending()[0], &candidates[0]));

    let complete = [
        CandidateHistory::Loose(&changes[1]),
        CandidateHistory::Loose(&changes[2]),
    ];
    let result = select(&accepted, &complete, &store);
    assert_eq!(result.selected().len(), 2);
    assert!(result.pending().is_empty());
}

#[test]
fn overlapping_accepted_member_stops_traversal_at_the_accepted_boundary() {
    // a0 (level1) is F's boundary; F={a1,a2}, both levels 0 except head a2=1.
    // a0,a1 are already accepted. Supplying a1 again must not make the round
    // walk or re-establish attestation for the accepted prefix.
    let key = SigningKey::from_bytes(&[7; 32]);
    let author = Author::from(key.verifying_key().to_bytes().to_vec());
    let mut doc = AutoCommit::new()
        .with_author(Some(author))
        .with_actor(ActorId::from([1]));
    let a0 = append_at_level(&mut doc, 1, 1);
    let a1 = append_at_level(&mut doc, 2, 0);
    let a2 = append_at_level(&mut doc, 3, 1);
    let changes = doc.get_changes(&[]);
    let fragment: Fragment = doc.get_fragment(a2).unwrap();
    assert_eq!(fragment.boundary, [a0]);
    assert_eq!(
        fragment.members.iter().copied().collect::<HashSet<_>>(),
        HashSet::from([a1, a2])
    );
    let accepted = Membership::new([a0, a1]);
    let mut store = SignatureStore::default();
    for change in &changes {
        retain_signature(&mut store, change, &key);
    }
    let candidates = [CandidateHistory::CanonicalFragment {
        fragment: &fragment,
        members: changes[1..].iter().collect(),
    }];

    let result = select(&accepted, &candidates, &store);

    assert_eq!(result.selected().len(), 1);
    assert!(result.pending().is_empty());
    assert_eq!(store.ed25519_entries(&a1).count(), 1);

    // Initialization can query an accepted member's direct dependencies while
    // constructing the static index. Check the round itself for traversal cutoff.
    let round_membership = Membership::new([a0, a1]);
    let round = crate::admission::Round::new(changes[1..].iter(), &round_membership, &store);
    assert!(round.meets_requirements(&candidates[0], &round_membership, &store));
    assert!(!round.available.contains_key(&a1));
    assert!(!round.attested.contains(&a1));
    assert!(!round_membership.queries.borrow().contains(&a0));
}

#[test]
fn unrelated_accepted_history_does_not_increase_membership_queries() {
    let (changes, key) = chain();
    let mut store = SignatureStore::default();
    retain_signature(&mut store, &changes[2], &key);
    let candidates = [CandidateHistory::Loose(&changes[2])];
    let small = Membership::new(changes[..2].iter().map(Change::hash));

    // Real independent history, not fabricated hashes falsely labelled accepted.
    let unrelated_key = SigningKey::from_bytes(&[9; 32]);
    let author = Author::from(unrelated_key.verifying_key().to_bytes().to_vec());
    let mut unrelated = AutoCommit::new()
        .with_author(Some(author))
        .with_actor(ActorId::from([9]));
    for value in 0..256 {
        unrelated.put(ROOT, "unrelated", value).unwrap();
        unrelated.commit_with(CommitOptions::default().with_time(value));
    }
    let unrelated = unrelated.get_changes(&[]);
    for change in changes[..2].iter() {
        retain_signature(&mut store, change, &key);
    }
    for change in &unrelated {
        retain_signature(&mut store, change, &unrelated_key);
    }
    let large = Membership::new(changes[..2].iter().chain(&unrelated).map(Change::hash));
    // Every accepted change is directly signed and its dependencies are accepted.
    // No unrelated change bytes are passed to select.
    let result = select(&small, &candidates, &store);
    assert_eq!(result.selected().len(), 1);
    let result = select(&large, &candidates, &store);
    assert_eq!(result.selected().len(), 1);
    assert_eq!(*small.queries.borrow(), *large.queries.borrow());
}
