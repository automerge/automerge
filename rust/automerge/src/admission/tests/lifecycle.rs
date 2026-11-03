use std::collections::BTreeMap;

use ed25519_dalek::{Signer, SigningKey};

use crate::signature::{ed25519, signing_message, SignatureStore};
use crate::{ActorId, Author, AutoCommit, Change, Signature};

use super::{append_at_level, retain_signature};
use crate::admission::tests::AcceptedHistoryArray;
use crate::admission::{select, CandidateHistory};

// a1 is incorporated into the fragment headed by a2. A retained signature
// on a2 supports the admitted prefix even though sig(a1) is no longer held.
struct AdmittedPrefix {
    changes: Vec<Change>,
    signatures: SignatureStore,
}

fn admitted_prefix() -> AdmittedPrefix {
    let key = SigningKey::from_bytes(&[7; 32]);
    let author = Author::from(key.verifying_key().to_bytes().to_vec());
    let mut doc = AutoCommit::new()
        .with_author(Some(author))
        .with_actor(ActorId::from([1]));
    let a1 = append_at_level(&mut doc, 1, 0);
    let a2 = append_at_level(&mut doc, 2, 1);
    let fragment = doc.get_fragment(a2).unwrap();
    let a3 = append_at_level(&mut doc, 3, 0);
    let changes = doc.get_changes(&[]);
    assert_eq!(
        changes.iter().map(Change::hash).collect::<Vec<_>>(),
        [a1, a2, a3]
    );
    assert_eq!(changes[1].deps(), &[a1]);
    assert_eq!(changes[2].deps(), &[a2]);
    assert!(fragment.boundary.is_empty());
    assert_eq!(fragment.members.len(), 2);
    assert!([a1, a2].iter().all(|hash| fragment.members.contains(hash)));

    let mut signatures = SignatureStore::default();
    retain_signature(&mut signatures, &changes[1], &key);
    retain_signature(&mut signatures, &changes[2], &key);
    assert_eq!(signatures.ed25519_entries(&a1).count(), 0);
    assert_eq!(signatures.ed25519_entries(&a2).count(), 1);
    assert_eq!(signatures.ed25519_entries(&a3).count(), 1);

    AdmittedPrefix {
        changes,
        signatures,
    }
}

#[test]
fn accepted_history_supplies_a_signed_loose_changes_dependency() {
    let fixture = admitted_prefix();
    let accepted = AcceptedHistoryArray::from_admitted(&fixture.changes[..2]);
    let candidates = [CandidateHistory::Loose(&fixture.changes[2])];

    let selection = select(&accepted, &candidates, &fixture.signatures);

    assert_eq!(selection.selected().len(), 1);
    assert!(std::ptr::eq(selection.selected()[0], &candidates[0]));
    assert!(selection.pending().is_empty());

    // The same signed change cannot be admitted without its admitted prefix.
    let selection = select(
        &AcceptedHistoryArray::default(),
        &candidates,
        &fixture.signatures,
    );
    assert!(selection.selected().is_empty());
    assert_eq!(selection.pending().len(), 1);
}

#[test]
fn receiving_an_accepted_change_again_does_not_require_its_pruned_signature() {
    let fixture = admitted_prefix();
    let accepted = AcceptedHistoryArray::from_admitted(&fixture.changes[..2]);
    // The incoming level-0 change is already incorporated in accepted history.
    // Its direct signature need not be restored just to process this delivery.
    let candidates = [CandidateHistory::Loose(&fixture.changes[0])];

    let selection = select(&accepted, &candidates, &fixture.signatures);

    assert_eq!(selection.selected().len(), 1);
    assert!(std::ptr::eq(selection.selected()[0], &candidates[0]));
    assert!(selection.pending().is_empty());
    assert_eq!(
        fixture
            .signatures
            .ed25519_entries(&fixture.changes[0].hash())
            .count(),
        0
    );

    // Without prior admission it is a novel loose change and needs sig(a1).
    let selection = select(
        &AcceptedHistoryArray::default(),
        &candidates,
        &fixture.signatures,
    );
    assert!(selection.selected().is_empty());
    assert_eq!(selection.pending().len(), 1);
}

#[test]
fn empty_candidates_return_empty_partitions_with_or_without_accepted_history() {
    let fixture = admitted_prefix();
    for accepted in [
        AcceptedHistoryArray::default(),
        AcceptedHistoryArray::from_admitted(&fixture.changes[..2]),
    ] {
        let selection = select(&accepted, &[], &fixture.signatures);
        assert!(selection.selected().is_empty());
        assert!(selection.pending().is_empty());
    }
}

#[test]
fn a_signature_does_not_supply_an_absent_dependency() {
    let key = SigningKey::from_bytes(&[9; 32]);
    let author = Author::from(key.verifying_key().to_bytes().to_vec());
    let mut doc = AutoCommit::new()
        .with_author(Some(author))
        .with_actor(ActorId::from([1]));
    let a1 = append_at_level(&mut doc, 1, 0);
    let a2 = append_at_level(&mut doc, 2, 0);
    let changes = doc.get_changes(&[]);
    assert_eq!(changes[1].deps(), &[a1]);
    assert_eq!(changes[1].hash(), a2);
    let mut signatures = SignatureStore::default();
    // Even sig(a1) cannot supply a1's missing history to the selector.
    for change in &changes {
        retain_signature(&mut signatures, change, &key);
    }
    let candidates = [CandidateHistory::Loose(&changes[1])];
    let accepted = AcceptedHistoryArray::default();

    let selection = select(&accepted, &candidates, &signatures);

    assert!(selection.selected().is_empty());
    assert_eq!(selection.pending().len(), 1);
    assert!(std::ptr::eq(selection.pending()[0], &candidates[0]));

    let complete = [
        CandidateHistory::Loose(&changes[0]),
        CandidateHistory::Loose(&changes[1]),
    ];
    let selection = select(&accepted, &complete, &signatures);
    assert_eq!(selection.selected().len(), 2);
    assert!(selection.pending().is_empty());
}

#[test]
fn unchecked_signature_bytes_do_not_count_until_evidence_is_established() {
    let key = SigningKey::from_bytes(&[7; 32]);
    let author = Author::from(key.verifying_key().to_bytes().to_vec());
    let actor = ActorId::from([1]);
    let mut doc = AutoCommit::new()
        .with_author(Some(author.clone()))
        .with_actor(actor.clone());
    let hash = append_at_level(&mut doc, 1, 0);
    let changes = doc.get_changes(&[]);
    assert!(changes[0].deps().is_empty());
    let signature = ed25519::Signature::from_bytes(key.sign(&signing_message(&hash)).to_bytes());
    let mut signatures = SignatureStore::default();
    signatures.insert_unchecked(hash, Signature::ed25519(signature));
    assert_eq!(signatures.unchecked_entries(&hash).count(), 1);
    assert_eq!(signatures.ed25519_entries(&hash).count(), 0);
    let candidates = [CandidateHistory::Loose(&changes[0])];
    let accepted = AcceptedHistoryArray::default();

    let selection = select(&accepted, &candidates, &signatures);

    assert!(selection.selected().is_empty());
    assert_eq!(selection.pending().len(), 1);
    // Selection must not perform verification or mutate the unchecked entry.
    assert_eq!(signatures.unchecked_entries(&hash).count(), 1);
    assert_eq!(signatures.ed25519_entries(&hash).count(), 0);

    let authors = BTreeMap::from([(actor, ed25519::VerifyingAuthor::new(author).unwrap())]);
    let verifier = ed25519::AuthorVerifier::new(&authors);
    signatures
        .insert_ed25519(verifier.verify_change(&changes[0], &signature).unwrap())
        .unwrap();
    let selection = select(&accepted, &candidates, &signatures);
    assert_eq!(selection.selected().len(), 1);
    assert!(std::ptr::eq(selection.selected()[0], &candidates[0]));
    assert!(selection.pending().is_empty());
    assert_eq!(signatures.unchecked_entries(&hash).count(), 0);
    assert_eq!(signatures.ed25519_entries(&hash).count(), 1);
}

#[test]
fn mutually_supporting_fragments_are_selected_together_but_not_individually() {
    let a_key = SigningKey::from_bytes(&[7; 32]);
    let b_key = SigningKey::from_bytes(&[8; 32]);
    let a_author = Author::from(a_key.verifying_key().to_bytes().to_vec());
    let b_author = Author::from(b_key.verifying_key().to_bytes().to_vec());
    let a_actor = ActorId::from([1]);
    let mut doc = AutoCommit::new()
        .with_author(Some(a_author.clone()))
        .with_actor(a_actor.clone());
    let a = append_at_level(&mut doc, 1, 0);
    doc.set_author(Some(b_author));
    doc.set_actor(ActorId::from([2]));
    let b = append_at_level(&mut doc, 2, 1);
    let f = doc.get_fragment(b).unwrap();
    doc.set_author(Some(a_author));
    doc.set_actor(a_actor);
    let c = append_at_level(&mut doc, 3, 1);
    let g = doc.get_fragment(c).unwrap();
    let changes = doc.get_changes(&[]);
    assert_eq!(
        changes.iter().map(Change::hash).collect::<Vec<_>>(),
        [a, b, c]
    );
    assert_eq!(changes[1].deps(), &[a]);
    assert!(changes[2].deps().contains(&a));
    assert!(changes[2].deps().contains(&b));
    assert_eq!(changes[0].actor_id(), changes[2].actor_id());

    // Equal-level b is G's boundary, excluding b and a from G's membership.
    // F needs sig(c) to attest a. G needs F to supply its dependencies a,b.
    assert!(f.boundary.is_empty());
    assert_eq!(f.members.len(), 2);
    assert!([a, b].iter().all(|hash| f.members.contains(hash)));
    assert_eq!(g.boundary, [b]);
    assert_eq!(g.members, [c]);
    let mut signatures = SignatureStore::default();
    retain_signature(&mut signatures, &changes[1], &b_key);
    retain_signature(&mut signatures, &changes[2], &a_key);
    assert_eq!(signatures.ed25519_entries(&a).count(), 0);
    let accepted = AcceptedHistoryArray::default();

    for reverse in [false, true] {
        let mut candidates = [
            CandidateHistory::CanonicalFragment {
                fragment: &f,
                members: changes[..2].iter().collect(),
            },
            CandidateHistory::CanonicalFragment {
                fragment: &g,
                members: changes[2..].iter().collect(),
            },
        ];
        if reverse {
            candidates.reverse();
        }
        for candidate in &candidates {
            let selection = select(&accepted, std::slice::from_ref(candidate), &signatures);
            assert!(selection.selected().is_empty());
            assert_eq!(selection.pending().len(), 1);
        }

        let selection = select(&accepted, &candidates, &signatures);
        assert_eq!(selection.selected().len(), 2);
        assert!(selection.pending().is_empty());
        for candidate in &candidates {
            assert!(selection
                .selected()
                .iter()
                .any(|selected| std::ptr::eq(*selected, candidate)));
        }
    }
}
