use ed25519_dalek::{Signer, SigningKey};

use crate::actor::ActorTable;
use crate::admission::CandidateHistory;
use crate::signature::{ed25519, signing_message};
use crate::transaction::{CommitOptions, Transactable};
use crate::{ActorId, Author, Automerge, Change, ChangeHash, ReadDoc, Value, ROOT};

use super::{error, AcceptedClock, SigningDocument};

mod lifecycle;
mod local;
mod local_contents;

fn append_at_level(doc: &mut Automerge, value: i64, level: usize) -> ChangeHash {
    for time in 0..10_000 {
        let mut next = doc.clone();
        let mut tx = next.transaction();
        tx.put(ROOT, "value", value).unwrap();
        let (hash, _) = tx.commit_with(CommitOptions::default().with_time(time));
        let hash = hash.unwrap();
        if hash.fragment_level() == level {
            *doc = next;
            return hash;
        }
    }
    panic!("fixture level search exhausted");
}

fn fixture(levels: &[usize]) -> (Automerge, Vec<Change>, SigningKey) {
    let key = SigningKey::from_bytes(&[7; 32]);
    let author = Author::from(key.verifying_key().to_bytes().to_vec());
    let mut doc = Automerge::signing()
        .with_author(Some(author))
        .with_actor(ActorId::from([1]));
    for (index, &level) in levels.iter().enumerate() {
        append_at_level(&mut doc, index as i64 + 1, level);
    }
    let changes = doc.get_changes(&[]);
    (doc, changes, key)
}

// Own the observed value so a later signing handle can borrow the document mutably.
fn value(doc: &Automerge) -> Value<'static> {
    doc.get(ROOT, "value").unwrap().unwrap().0.into_owned()
}

fn retain_signature(signing: &mut SigningDocument<'_>, change: &Change, key: &SigningKey) {
    let signature =
        ed25519::Signature::from_bytes(key.sign(&signing_message(&change.hash())).to_bytes());
    signing
        .insert_signature(&change.hash(), &signature)
        .unwrap();
}

// Deliberately bypass document insertion only to exercise acceptance's checks
// for unapplied history and mismatched authors. Normal fixtures use the handle.
fn inject_evidence(signing: &mut SigningDocument<'_>, change: &Change, key: &SigningKey) {
    let author = Author::from(key.verifying_key().to_bytes().to_vec());
    let verifier = ed25519::VerifyingAuthor::new(author).unwrap();
    let signature =
        ed25519::Signature::from_bytes(key.sign(&signing_message(&change.hash())).to_bytes());
    signing
        .state
        .signatures
        .insert_ed25519(verifier.verify_hash(&change.hash(), &signature).unwrap())
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
    let (mut document, changes, _) = fixture(&[0, 0, 0]);
    assert_eq!(document.get_heads(), [changes[2].hash()]);
    assert_eq!(value(&document), 3.into());

    let signing = document.try_signing().unwrap();
    for change in &changes {
        assert!(!signing.is_accepted(&change.hash()));
    }
    assert!(!signing.is_accepted(&ChangeHash([0xff; 32])));
}

#[test]
fn acceptance_waits_for_a_gap_in_loose_signatures_without_reapplying_changes() {
    let (mut document, changes, key) = fixture(&[0, 0, 0]);
    let heads_before = document.get_heads();
    let value_before = value(&document);
    let candidates: Vec<_> = changes.iter().map(CandidateHistory::Loose).collect();

    {
        let mut signing = document.try_signing().unwrap();
        // Merely having an unchecked, well-formed signature must not advance acceptance.
        let signature = ed25519::Signature::from_bytes(
            key.sign(&signing_message(&changes[0].hash())).to_bytes(),
        );
        signing
            .state
            .signatures
            .insert_unchecked(changes[0].hash(), crate::Signature::ed25519(signature));
        signing.accept_applied(&candidates).unwrap();
        assert!(changes.iter().all(|c| !signing.is_accepted(&c.hash())));

        retain_signature(&mut signing, &changes[0], &key);
        retain_signature(&mut signing, &changes[2], &key);
        signing.accept_applied(&candidates).unwrap();
        assert!(signing.is_accepted(&changes[0].hash()));
        assert!(!signing.is_accepted(&changes[1].hash()));
        assert!(!signing.is_accepted(&changes[2].hash()));
    }
    {
        let mut signing = document.try_signing().unwrap();
        assert!(signing.is_accepted(&changes[0].hash()));
        assert!(!signing.is_accepted(&changes[1].hash()));
        assert!(!signing.is_accepted(&changes[2].hash()));
        assert_eq!(
            signing
                .state
                .signatures
                .ed25519_entries(&changes[2].hash())
                .count(),
            1
        );

        retain_signature(&mut signing, &changes[1], &key);
        signing.accept_applied(&candidates).unwrap();
        assert!(changes.iter().all(|c| signing.is_accepted(&c.hash())));
        // Idempotence and the exact-hash check, even after this actor's clock advances.
        signing.accept_applied(&candidates).unwrap();
        assert!(changes.iter().all(|c| signing.is_accepted(&c.hash())));
        assert!(!signing.is_accepted(&ChangeHash([0xff; 32])));
    }
    assert_eq!(document.get_heads(), heads_before);
    assert_eq!(value(&document), value_before);
}

#[test]
fn accepting_a_fragment_covers_interior_changes_without_direct_signatures() {
    let (mut document, changes, key) = fixture(&[0, 0, 1]);
    // Required-mode discovery waits until the fragment can attest itself.
    assert!(document.get_fragment(changes[2].hash()).is_none());
    retain_signature(&mut document.try_signing().unwrap(), &changes[2], &key);
    let fragment = document.get_fragment(changes[2].hash()).unwrap();
    assert!(fragment.boundary.is_empty());
    assert_eq!(fragment.members.len(), 3);
    let candidates = [CandidateHistory::CanonicalFragment {
        fragment: &fragment,
        members: changes.iter().collect(),
    }];
    {
        let mut signing = document.try_signing().unwrap();
        retain_signature(&mut signing, &changes[2], &key);
        signing.accept_applied(&candidates).unwrap();

        assert!(changes.iter().all(|c| signing.is_accepted(&c.hash())));
        assert_eq!(
            signing
                .state
                .signatures
                .ed25519_entries(&changes[0].hash())
                .count(),
            0
        );
        assert_eq!(
            signing
                .state
                .signatures
                .ed25519_entries(&changes[1].hash())
                .count(),
            0
        );
    }
    assert_eq!(document.get_heads(), [changes[2].hash()]);
    assert_eq!(value(&document), 3.into());
}

#[test]
fn unapplied_candidate_does_not_partially_advance_acceptance() {
    let (mut document, changes, key) = fixture(&[0]);
    let mut other = document.clone();
    let missing = append_at_level(&mut other, 2, 0);
    let later = other.get_change_by_hash(&missing).unwrap();
    let candidates = [
        CandidateHistory::Loose(&changes[0]),
        CandidateHistory::Loose(&later),
    ];
    {
        let mut signing = document.try_signing().unwrap();
        retain_signature(&mut signing, &changes[0], &key);
        inject_evidence(&mut signing, &later, &key);
        assert!(matches!(
            signing.accept_applied(&candidates),
            Err(error::AcceptApplied::UnappliedChange { change }) if change == missing
        ));
    }
    {
        let signing = document.try_signing().unwrap();
        assert!(!signing.is_accepted(&changes[0].hash()));
        assert!(!signing.is_accepted(&missing));
    }
    assert_eq!(document.get_heads(), [changes[0].hash()]);
}

#[test]
fn dropping_a_prepared_admission_does_not_advance_acceptance() {
    let (mut document, changes, key) = fixture(&[0]);
    let change = &changes[0];
    let hash = change.hash();
    let heads_before = document.get_heads();
    let value_before = value(&document);
    let candidates = [CandidateHistory::Loose(change)];

    {
        let mut signing = document.try_signing().unwrap();
        retain_signature(&mut signing, change, &key);
        // Let the proof go out of scope without recording its proposed update.
        {
            let _proof = signing.prepare(&candidates).unwrap();
        }
        assert!(!signing.is_accepted(&hash));
    }
    {
        let mut signing = document.try_signing().unwrap();
        assert!(!signing.is_accepted(&hash));
        assert_eq!(signing.state.signatures.ed25519_entries(&hash).count(), 1);

        // Recording a fresh proof must advance the same document-owned clock.
        signing.prepare(&candidates).unwrap().record();
        assert!(signing.is_accepted(&hash));
    }
    assert!(document.try_signing().unwrap().is_accepted(&hash));
    assert_eq!(document.get_heads(), heads_before);
    assert_eq!(value(&document), value_before);
}

#[test]
fn unapplied_input_preserves_existing_acceptance_without_partial_advancement() {
    let (mut document, changes, key) = fixture(&[0, 0]);
    let mut other = document.clone();
    let missing = append_at_level(&mut other, 3, 0);
    let unapplied = other.get_change_by_hash(&missing).unwrap();
    let heads_before = document.get_heads();
    let value_before = value(&document);

    // The valid a2 precedes invalid input to expose eager partial advancement.
    let candidates = [
        CandidateHistory::Loose(&changes[1]),
        CandidateHistory::Loose(&unapplied),
    ];
    {
        let mut signing = document.try_signing().unwrap();
        for change in &changes {
            retain_signature(&mut signing, change, &key);
        }
        inject_evidence(&mut signing, &unapplied, &key);
        signing
            .accept_applied(&[CandidateHistory::Loose(&changes[0])])
            .unwrap();
        assert!(matches!(
            signing.accept_applied(&candidates),
            Err(error::AcceptApplied::UnappliedChange { change }) if change == missing
        ));
    }
    {
        let mut signing = document.try_signing().unwrap();
        assert!(signing.is_accepted(&changes[0].hash()));
        assert!(!signing.is_accepted(&changes[1].hash()));
        assert!(!signing.is_accepted(&missing));

        // Retrying only the applied candidate can still complete acceptance.
        signing.accept_applied(&candidates[..1]).unwrap();
        assert!(signing.is_accepted(&changes[0].hash()));
        assert!(signing.is_accepted(&changes[1].hash()));
    }
    assert_eq!(document.get_heads(), heads_before);
    assert_eq!(value(&document), value_before);
}

#[test]
fn signed_change_waits_for_an_unaccepted_dependency_from_another_actor() {
    let (mut document, mut changes, a_key) = fixture(&[0]);
    let b_key = SigningKey::from_bytes(&[8; 32]);
    let b_author = Author::from(b_key.verifying_key().to_bytes().to_vec());
    // Changing author generates an actor. Set the deterministic fixture actor afterward.
    document.set_author(Some(b_author));
    document.set_actor(ActorId::from([2]));
    let b1 = append_at_level(&mut document, 2, 0);
    changes.push(document.get_change_by_hash(&b1).unwrap());
    assert_ne!(changes[0].actor_id(), changes[1].actor_id());
    assert_eq!(changes[1].seq(), 1);
    assert_eq!(changes[1].deps(), &[changes[0].hash()]);
    let heads_before = document.get_heads();
    let value_before = value(&document);
    let candidates: Vec<_> = changes.iter().map(CandidateHistory::Loose).collect();

    {
        let mut signing = document.try_signing().unwrap();
        retain_signature(&mut signing, &changes[1], &b_key);
        signing.accept_applied(&candidates).unwrap();
        assert!(!signing.is_accepted(&changes[0].hash()));
        assert!(!signing.is_accepted(&b1));
        assert_eq!(signing.state.signatures.ed25519_entries(&b1).count(), 1);
    }
    {
        let mut signing = document.try_signing().unwrap();
        retain_signature(&mut signing, &changes[0], &a_key);
        signing.accept_applied(&candidates).unwrap();
        assert!(signing.is_accepted(&changes[0].hash()));
        assert!(signing.is_accepted(&b1));
    }
    assert_eq!(document.get_heads(), heads_before);
    assert_eq!(value(&document), value_before);
}

#[test]
fn advancing_one_actor_preserves_other_actors_accepted_prefixes() {
    let (mut document, mut changes, a_key) = fixture(&[0]);
    let b_key = SigningKey::from_bytes(&[8; 32]);
    document.set_author(Some(Author::from(
        b_key.verifying_key().to_bytes().to_vec(),
    )));
    document.set_actor(ActorId::from([2]));
    let b1 = append_at_level(&mut document, 2, 0);
    changes.push(document.get_change_by_hash(&b1).unwrap());
    document.set_author(Some(Author::from(
        a_key.verifying_key().to_bytes().to_vec(),
    )));
    document.set_actor(changes[0].actor_id().clone());
    let a2 = append_at_level(&mut document, 3, 0);
    changes.push(document.get_change_by_hash(&a2).unwrap());
    assert_eq!(changes[2].actor_id(), changes[0].actor_id());
    assert_eq!(changes[2].seq(), 2);
    assert!(changes[2].deps().contains(&changes[0].hash()));
    assert!(changes[2].deps().contains(&b1));
    let heads_before = document.get_heads();
    let value_before = value(&document);
    let prefix: Vec<_> = changes[..2].iter().map(CandidateHistory::Loose).collect();

    {
        let mut signing = document.try_signing().unwrap();
        retain_signature(&mut signing, &changes[0], &a_key);
        retain_signature(&mut signing, &changes[1], &b_key);
        signing.accept_applied(&prefix).unwrap();
        assert!(signing.is_accepted(&changes[0].hash()));
        assert!(signing.is_accepted(&b1));
        assert!(!signing.is_accepted(&a2));
    }
    {
        let mut signing = document.try_signing().unwrap();
        retain_signature(&mut signing, &changes[2], &a_key);
        // B is absent from this input. Preparing the next clock must preserve its slot.
        signing
            .accept_applied(&[CandidateHistory::Loose(&changes[2])])
            .unwrap();
        assert!(signing.is_accepted(&changes[0].hash()));
        assert!(signing.is_accepted(&a2));
        assert!(signing.is_accepted(&b1));
    }
    assert_eq!(document.get_heads(), heads_before);
    assert_eq!(value(&document), value_before);
}

#[test]
fn empty_local_change_needs_acceptance_even_without_new_operations() {
    let (mut document, mut changes, key) = fixture(&[0]);
    let value_before_empty = value(&document);
    // Require a loose empty change so its direct-signature obligation is explicit.
    let (next, empty_hash) = (0..10_000)
        .find_map(|time| {
            let mut next = document.clone();
            let hash = next.empty_commit(CommitOptions::default().with_time(time));
            (hash.fragment_level() == 0).then_some((next, hash))
        })
        .expect("fixture search for loose empty change exhausted");
    document = next;
    changes.push(document.get_change_by_hash(&empty_hash).unwrap());
    assert!(changes[1].is_empty());
    assert_eq!(changes[1].seq(), changes[0].seq() + 1);
    assert_eq!(changes[1].max_op(), changes[0].max_op());
    assert_eq!(changes[1].deps(), &[changes[0].hash()]);
    let heads_before = document.get_heads();
    assert_eq!(heads_before, [empty_hash]);
    assert_eq!(value(&document), value_before_empty);

    {
        let mut signing = document.try_signing().unwrap();
        retain_signature(&mut signing, &changes[0], &key);
        signing
            .accept_applied(&[CandidateHistory::Loose(&changes[0])])
            .unwrap();
        assert!(signing.is_accepted(&changes[0].hash()));
        assert!(!signing.is_accepted(&empty_hash));

        let candidates = [CandidateHistory::Loose(&changes[1])];
        signing.accept_applied(&candidates).unwrap();
        assert!(!signing.is_accepted(&empty_hash));
        retain_signature(&mut signing, &changes[1], &key);
        signing.accept_applied(&candidates).unwrap();
        assert!(signing.is_accepted(&empty_hash));
        assert!(signing.is_accepted(&changes[0].hash()));
    }
    assert_eq!(document.get_heads(), heads_before);
    assert_eq!(value(&document), value_before_empty);
}

#[test]
fn acceptance_requires_store_authorship_to_match_the_document() {
    let (mut document, changes, _) = fixture(&[0]);
    let candidates = [CandidateHistory::Loose(&changes[0])];
    {
        let mut signing = document.try_signing().unwrap();
        // This really verifies under a different key, but its caller-supplied
        // author association is not this document's established binding.
        inject_evidence(&mut signing, &changes[0], &SigningKey::from_bytes(&[8; 32]));
        assert!(matches!(
            signing.accept_applied(&candidates),
            Err(error::AcceptApplied::AuthorMismatch { change }) if change == changes[0].hash()
        ));
    }
    assert!(!document
        .try_signing()
        .unwrap()
        .is_accepted(&changes[0].hash()));
}
