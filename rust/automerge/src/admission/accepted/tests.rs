use std::collections::BTreeMap;

use ed25519_dalek::{Signer, SigningKey};

use crate::actor::ActorTable;
use crate::admission::CandidateHistory;
use crate::signature::{ed25519, signing_message, SignatureStore};
use crate::transaction::{CommitOptions, Transactable};
use crate::{ActorId, Author, AutoCommit, Change, ChangeHash, ReadDoc, ROOT};

use super::{error, AcceptedClock, AppliedAcceptance};

fn append_at_level(doc: &mut AutoCommit, value: i64, level: usize) -> ChangeHash {
    for time in 0..10_000 {
        let mut next = doc.clone();
        next.put(ROOT, "value", value).unwrap();
        let hash = next
            .commit_with(CommitOptions::default().with_time(time))
            .unwrap();
        if hash.fragment_level() == level {
            *doc = next;
            return hash;
        }
    }
    panic!("fixture level search exhausted");
}

fn fixture(levels: &[usize]) -> (AutoCommit, Vec<Change>, SigningKey) {
    let key = SigningKey::from_bytes(&[7; 32]);
    let author = Author::from(key.verifying_key().to_bytes().to_vec());
    let mut doc = AutoCommit::new()
        .with_author(Some(author))
        .with_actor(ActorId::from([1]));
    for (index, &level) in levels.iter().enumerate() {
        append_at_level(&mut doc, index as i64 + 1, level);
    }
    let changes = doc.get_changes(&[]);
    (doc, changes, key)
}

fn retain_signature(store: &mut SignatureStore, change: &Change, key: &SigningKey) {
    let author = Author::from(key.verifying_key().to_bytes().to_vec());
    let authors = BTreeMap::from([(
        change.actor_id().clone(),
        ed25519::VerifyingAuthor::new(author).unwrap(),
    )]);
    let signature =
        ed25519::Signature::from_bytes(key.sign(&signing_message(&change.hash())).to_bytes());
    let verifier = ed25519::AuthorVerifier::new(&authors);
    store
        .insert_ed25519(verifier.verify_change(change, &signature).unwrap())
        .unwrap();
}

#[test]
fn clock_with_no_accepted_prefix_rejects_every_sequence() {
    let actor = ActorId::from([1]);
    let actors = ActorTable::from_actors([actor.clone()]);
    let index = actors.lookup(&actor).unwrap();
    let accepted = AcceptedClock::new(&actors);

    for seq in [1, 2, 3] {
        assert!(!accepted.is_accepted_actor_seq(index, seq));
    }
}

#[test]
fn clock_membership_includes_the_accepted_limit_but_not_later_sequences() {
    let actor_a = ActorId::from([1]);
    let actor_b = ActorId::from([2]);
    let actors = ActorTable::from_actors([actor_a.clone(), actor_b.clone()]);
    let a = actors.lookup(&actor_a).unwrap();
    let b = actors.lookup(&actor_b).unwrap();
    let mut accepted = AcceptedClock::new(&actors);
    // Private fixture setup isolates the membership comparison from admission.
    // This is not a caller-facing clock advancement path.
    accepted.clock.include(a, Some(2));

    assert!(accepted.is_accepted_actor_seq(a, 1));
    assert!(accepted.is_accepted_actor_seq(a, 2));
    assert!(!accepted.is_accepted_actor_seq(a, 3));
    assert!(!accepted.is_accepted_actor_seq(b, 1));
    assert!(!accepted.is_accepted_actor_seq(b, 2));
}

#[test]
fn applied_local_changes_are_not_initially_accepted() {
    let (mut doc, changes, _) = fixture(&[0, 0, 0]);
    let document = doc.document();
    let acceptance = AppliedAcceptance::new(document);

    assert_eq!(document.get_heads(), [changes[2].hash()]);
    assert_eq!(document.get(ROOT, "value").unwrap().unwrap().0, 3.into());
    for change in &changes {
        assert!(!acceptance.is_accepted(&change.hash()));
    }
    assert!(!acceptance.is_accepted(&ChangeHash([0xff; 32])));
}

#[test]
fn acceptance_waits_for_a_gap_in_loose_signatures_without_reapplying_changes() {
    let (mut doc, changes, key) = fixture(&[0, 0, 0]);
    let document = doc.document();
    let heads_before = document.get_heads();
    let value_before = document.get(ROOT, "value").unwrap();
    let mut acceptance = AppliedAcceptance::new(document);
    let candidates: Vec<_> = changes.iter().map(CandidateHistory::Loose).collect();
    let mut signatures = SignatureStore::default();

    // Merely having an unchecked, well-formed signature must not advance acceptance.
    let signature =
        ed25519::Signature::from_bytes(key.sign(&signing_message(&changes[0].hash())).to_bytes());
    signatures.insert_unchecked(changes[0].hash(), crate::Signature::ed25519(signature));
    acceptance.accept_applied(&candidates, &signatures).unwrap();
    assert!(changes.iter().all(|c| !acceptance.is_accepted(&c.hash())));

    retain_signature(&mut signatures, &changes[0], &key);
    retain_signature(&mut signatures, &changes[2], &key);
    acceptance.accept_applied(&candidates, &signatures).unwrap();
    assert!(acceptance.is_accepted(&changes[0].hash()));
    assert!(!acceptance.is_accepted(&changes[1].hash()));
    assert!(!acceptance.is_accepted(&changes[2].hash()));
    assert_eq!(signatures.ed25519_entries(&changes[2].hash()).count(), 1);

    retain_signature(&mut signatures, &changes[1], &key);
    acceptance.accept_applied(&candidates, &signatures).unwrap();
    assert!(changes.iter().all(|c| acceptance.is_accepted(&c.hash())));
    // Idempotence and the exact-hash check, even after this actor's clock advances.
    acceptance.accept_applied(&candidates, &signatures).unwrap();
    assert!(!acceptance.is_accepted(&ChangeHash([0xff; 32])));
    assert_eq!(document.get_heads(), heads_before);
    assert_eq!(document.get(ROOT, "value").unwrap(), value_before);
}

#[test]
fn accepting_a_fragment_covers_interior_changes_without_direct_signatures() {
    let (mut doc, changes, key) = fixture(&[0, 0, 1]);
    let fragment = doc.get_fragment(changes[2].hash()).unwrap();
    assert!(fragment.boundary.is_empty());
    assert_eq!(fragment.members.len(), 3);
    let document = doc.document();
    let mut acceptance = AppliedAcceptance::new(document);
    let candidates = [CandidateHistory::CanonicalFragment {
        fragment: &fragment,
        members: changes.iter().collect(),
    }];
    let mut signatures = SignatureStore::default();
    retain_signature(&mut signatures, &changes[2], &key);

    acceptance.accept_applied(&candidates, &signatures).unwrap();

    assert!(changes.iter().all(|c| acceptance.is_accepted(&c.hash())));
    assert_eq!(signatures.ed25519_entries(&changes[0].hash()).count(), 0);
    assert_eq!(signatures.ed25519_entries(&changes[1].hash()).count(), 0);
    assert_eq!(document.get_heads(), [changes[2].hash()]);
    assert_eq!(document.get(ROOT, "value").unwrap().unwrap().0, 3.into());
}

#[test]
fn unapplied_candidate_does_not_partially_advance_acceptance() {
    let (mut doc, changes, key) = fixture(&[0]);
    let mut other = doc.clone();
    let missing = append_at_level(&mut other, 2, 0);
    let later = other.get_change_by_hash(&missing).unwrap();
    let document = doc.document();
    let mut acceptance = AppliedAcceptance::new(document);
    let mut signatures = SignatureStore::default();
    retain_signature(&mut signatures, &changes[0], &key);
    retain_signature(&mut signatures, &later, &key);
    let candidates = [
        CandidateHistory::Loose(&changes[0]),
        CandidateHistory::Loose(&later),
    ];

    assert!(
        matches!(acceptance.accept_applied(&candidates, &signatures),
        Err(error::AcceptApplied::UnappliedChange { change }) if change == missing)
    );
    assert!(!acceptance.is_accepted(&changes[0].hash()));
    assert!(!acceptance.is_accepted(&missing));
    assert_eq!(document.get_heads(), [changes[0].hash()]);
}

#[test]
fn dropping_a_prepared_admission_does_not_advance_acceptance() {
    let (mut doc, changes, key) = fixture(&[0]);
    let document = doc.document();
    let heads_before = document.get_heads();
    let value_before = document.get(ROOT, "value").unwrap();
    let mut acceptance = AppliedAcceptance::new(document);
    let mut signatures = SignatureStore::default();
    retain_signature(&mut signatures, &changes[0], &key);
    let candidates = [CandidateHistory::Loose(&changes[0])];

    // Let the proof go out of scope without recording its proposed update.
    {
        let _proof = acceptance.prepare(&candidates, &signatures).unwrap();
    }
    assert!(!acceptance.is_accepted(&changes[0].hash()));
    assert_eq!(signatures.ed25519_entries(&changes[0].hash()).count(), 1);

    acceptance
        .prepare(&candidates, &signatures)
        .unwrap()
        .record();
    assert!(acceptance.is_accepted(&changes[0].hash()));
    assert_eq!(document.get_heads(), heads_before);
    assert_eq!(document.get(ROOT, "value").unwrap(), value_before);
}

#[test]
fn unapplied_input_preserves_existing_acceptance_without_partial_advancement() {
    let (mut doc, changes, key) = fixture(&[0, 0]);
    let mut other = doc.clone();
    let missing = append_at_level(&mut other, 3, 0);
    let unapplied = other.get_change_by_hash(&missing).unwrap();
    let document = doc.document();
    let heads_before = document.get_heads();
    let value_before = document.get(ROOT, "value").unwrap();
    let mut acceptance = AppliedAcceptance::new(document);
    let mut signatures = SignatureStore::default();
    for change in changes.iter().chain(std::iter::once(&unapplied)) {
        retain_signature(&mut signatures, change, &key);
    }
    acceptance
        .accept_applied(&[CandidateHistory::Loose(&changes[0])], &signatures)
        .unwrap();

    // The valid a2 precedes invalid input to expose eager partial advancement.
    let candidates = [
        CandidateHistory::Loose(&changes[1]),
        CandidateHistory::Loose(&unapplied),
    ];
    assert!(matches!(
        acceptance.accept_applied(&candidates, &signatures),
        Err(error::AcceptApplied::UnappliedChange { change }) if change == missing
    ));
    assert!(acceptance.is_accepted(&changes[0].hash()));
    assert!(!acceptance.is_accepted(&changes[1].hash()));
    assert!(!acceptance.is_accepted(&missing));

    // Retrying only the applied candidate can still complete acceptance.
    acceptance
        .accept_applied(&candidates[..1], &signatures)
        .unwrap();
    assert!(acceptance.is_accepted(&changes[0].hash()));
    assert!(acceptance.is_accepted(&changes[1].hash()));
    assert_eq!(document.get_heads(), heads_before);
    assert_eq!(document.get(ROOT, "value").unwrap(), value_before);
}

#[test]
fn signed_change_waits_for_an_unaccepted_dependency_from_another_actor() {
    let (mut doc, mut changes, a_key) = fixture(&[0]);
    let b_key = SigningKey::from_bytes(&[8; 32]);
    let b_author = Author::from(b_key.verifying_key().to_bytes().to_vec());
    // Changing author generates an actor. Set the deterministic fixture actor afterward.
    doc.set_author(Some(b_author));
    doc.set_actor(ActorId::from([2]));
    let b1 = append_at_level(&mut doc, 2, 0);
    changes.push(doc.get_change_by_hash(&b1).unwrap());
    assert_ne!(changes[0].actor_id(), changes[1].actor_id());
    assert_eq!(changes[1].seq(), 1);
    assert_eq!(changes[1].deps(), &[changes[0].hash()]);
    let document = doc.document();
    let heads_before = document.get_heads();
    let value_before = document.get(ROOT, "value").unwrap();
    let mut acceptance = AppliedAcceptance::new(document);
    let candidates: Vec<_> = changes.iter().map(CandidateHistory::Loose).collect();
    let mut signatures = SignatureStore::default();
    retain_signature(&mut signatures, &changes[1], &b_key);

    acceptance.accept_applied(&candidates, &signatures).unwrap();
    assert!(!acceptance.is_accepted(&changes[0].hash()));
    assert!(!acceptance.is_accepted(&b1));
    assert_eq!(signatures.ed25519_entries(&b1).count(), 1);

    retain_signature(&mut signatures, &changes[0], &a_key);
    acceptance.accept_applied(&candidates, &signatures).unwrap();
    assert!(acceptance.is_accepted(&changes[0].hash()));
    assert!(acceptance.is_accepted(&b1));
    assert_eq!(document.get_heads(), heads_before);
    assert_eq!(document.get(ROOT, "value").unwrap(), value_before);
}

#[test]
fn advancing_one_actor_preserves_other_actors_accepted_prefixes() {
    let (mut doc, mut changes, a_key) = fixture(&[0]);
    let b_key = SigningKey::from_bytes(&[8; 32]);
    doc.set_author(Some(Author::from(
        b_key.verifying_key().to_bytes().to_vec(),
    )));
    doc.set_actor(ActorId::from([2]));
    let b1 = append_at_level(&mut doc, 2, 0);
    changes.push(doc.get_change_by_hash(&b1).unwrap());
    doc.set_author(Some(Author::from(
        a_key.verifying_key().to_bytes().to_vec(),
    )));
    doc.set_actor(changes[0].actor_id().clone());
    let a2 = append_at_level(&mut doc, 3, 0);
    changes.push(doc.get_change_by_hash(&a2).unwrap());
    assert_eq!(changes[2].actor_id(), changes[0].actor_id());
    assert_eq!(changes[2].seq(), 2);
    assert!(changes[2].deps().contains(&changes[0].hash()));
    assert!(changes[2].deps().contains(&b1));
    let document = doc.document();
    let heads_before = document.get_heads();
    let value_before = document.get(ROOT, "value").unwrap();
    let mut acceptance = AppliedAcceptance::new(document);
    let mut signatures = SignatureStore::default();
    retain_signature(&mut signatures, &changes[0], &a_key);
    retain_signature(&mut signatures, &changes[1], &b_key);
    let prefix: Vec<_> = changes[..2].iter().map(CandidateHistory::Loose).collect();
    acceptance.accept_applied(&prefix, &signatures).unwrap();
    assert!(acceptance.is_accepted(&changes[0].hash()));
    assert!(acceptance.is_accepted(&b1));
    assert!(!acceptance.is_accepted(&a2));

    retain_signature(&mut signatures, &changes[2], &a_key);
    // B is absent from this input. Preparing the next clock must preserve its slot.
    acceptance
        .accept_applied(&[CandidateHistory::Loose(&changes[2])], &signatures)
        .unwrap();
    assert!(acceptance.is_accepted(&changes[0].hash()));
    assert!(acceptance.is_accepted(&a2));
    assert!(acceptance.is_accepted(&b1));
    assert_eq!(document.get_heads(), heads_before);
    assert_eq!(document.get(ROOT, "value").unwrap(), value_before);
}

#[test]
fn empty_local_change_needs_acceptance_even_without_new_operations() {
    let (mut doc, mut changes, key) = fixture(&[0]);
    let value_before_empty = doc
        .get(ROOT, "value")
        .unwrap()
        .map(|(value, object)| (value.into_owned(), object));
    // Require a loose empty change so its direct-signature obligation is explicit.
    let (next, empty_hash) = (0..10_000)
        .find_map(|time| {
            let mut next = doc.clone();
            let hash = next.empty_change(CommitOptions::default().with_time(time));
            (hash.fragment_level() == 0).then_some((next, hash))
        })
        .expect("fixture search for loose empty change exhausted");
    doc = next;
    changes.push(doc.get_change_by_hash(&empty_hash).unwrap());
    assert!(changes[1].is_empty());
    assert_eq!(changes[1].seq(), changes[0].seq() + 1);
    assert_eq!(changes[1].max_op(), changes[0].max_op());
    assert_eq!(changes[1].deps(), &[changes[0].hash()]);
    let document = doc.document();
    let heads_before = document.get_heads();
    assert_eq!(heads_before, [empty_hash]);
    assert_eq!(
        document
            .get(ROOT, "value")
            .unwrap()
            .map(|(value, object)| (value.into_owned(), object)),
        value_before_empty
    );
    let mut acceptance = AppliedAcceptance::new(document);
    let mut signatures = SignatureStore::default();
    retain_signature(&mut signatures, &changes[0], &key);
    acceptance
        .accept_applied(&[CandidateHistory::Loose(&changes[0])], &signatures)
        .unwrap();
    assert!(acceptance.is_accepted(&changes[0].hash()));
    assert!(!acceptance.is_accepted(&empty_hash));

    let candidates = [CandidateHistory::Loose(&changes[1])];
    acceptance.accept_applied(&candidates, &signatures).unwrap();
    assert!(!acceptance.is_accepted(&empty_hash));
    retain_signature(&mut signatures, &changes[1], &key);
    acceptance.accept_applied(&candidates, &signatures).unwrap();
    assert!(acceptance.is_accepted(&empty_hash));
    assert!(acceptance.is_accepted(&changes[0].hash()));
    assert_eq!(document.get_heads(), heads_before);
    assert_eq!(
        document
            .get(ROOT, "value")
            .unwrap()
            .map(|(value, object)| (value.into_owned(), object)),
        value_before_empty
    );
}

#[test]
fn acceptance_requires_store_authorship_to_match_the_document() {
    let (mut doc, changes, _) = fixture(&[0]);
    let document = doc.document();
    let mut acceptance = AppliedAcceptance::new(document);
    let mut signatures = SignatureStore::default();
    // This really verifies under a different key, but its caller-supplied
    // author association is not this document's established binding.
    retain_signature(
        &mut signatures,
        &changes[0],
        &SigningKey::from_bytes(&[8; 32]),
    );
    let candidates = [CandidateHistory::Loose(&changes[0])];

    assert!(
        matches!(acceptance.accept_applied(&candidates, &signatures),
        Err(error::AcceptApplied::AuthorMismatch { change }) if change == changes[0].hash())
    );
    assert!(!acceptance.is_accepted(&changes[0].hash()));
}
