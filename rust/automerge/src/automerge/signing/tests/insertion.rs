//! Document-level insertion contract. Deduplication, capacity, and store author
//! conflicts remain covered by SignatureStore's tests.

use std::error::Error;

use ed25519_dalek::{Signer, SigningKey};

use crate::admission::CandidateHistory;
use crate::signature::{ed25519, signing_message, Insert};
use crate::transaction::Transactable;
use crate::{ActorId, Author, Automerge, Change, ChangeHash, ReadDoc, ROOT};

use super::super::{error, SigningDocument};

fn author(key: &SigningKey) -> Author<'static> {
    Author::from(key.verifying_key().to_bytes().to_vec())
}

fn document(author: Option<Author<'static>>) -> Automerge {
    Automerge::signing()
        .with_author(author)
        .with_actor(ActorId::from([1]))
}

fn append(document: &mut Automerge, value: i64) -> Change {
    let mut tx = document.transaction();
    tx.put(ROOT, "value", value).unwrap();
    let (hash, _) = tx.commit();
    document.get_change_by_hash(&hash.unwrap()).unwrap()
}

fn sign(key: &SigningKey, hash: &ChangeHash) -> ed25519::Signature {
    ed25519::Signature::from_bytes(key.sign(&signing_message(hash)).to_bytes())
}

// Observe only the target entry and acceptance, without exposing a store accessor.
// This catches wrappers that insert unchecked bytes or associate an author before
// their own fallible validation completes.
fn assert_not_inserted(signing: &SigningDocument<'_>, hash: &ChangeHash) {
    assert_eq!(signing.state.signatures.author(hash), None);
    assert_eq!(signing.state.signatures.unchecked_entries(hash).count(), 0);
    assert_eq!(signing.state.signatures.ed25519_entries(hash).count(), 0);
    assert!(!signing.is_accepted(hash));
}

#[test]
fn insertion_supplies_evidence_without_accepting_or_changing_local_history() {
    let key = SigningKey::from_bytes(&[7; 32]);
    let mut document = document(Some(author(&key)));
    let change = append(&mut document, 1);
    let hash = change.hash();
    let signature = sign(&key, &hash);
    let heads_before = document.get_heads();
    let value_before = document.get(ROOT, "value").unwrap().unwrap().0.into_owned();
    {
        let mut signing = document.try_signing().unwrap();
        assert_eq!(
            signing.insert_signature(&hash, &signature).unwrap(),
            Insert::Stored
        );
        assert!(!signing.is_accepted(&hash));
    }
    assert_eq!(document.get_heads(), heads_before);
    assert_eq!(
        document.get(ROOT, "value").unwrap().unwrap().0,
        value_before
    );
    {
        let mut signing = document.try_signing().unwrap();
        assert!(!signing.is_accepted(&hash));
        // The later handle must see the evidence inserted by the earlier one.
        signing
            .accept_applied(&[CandidateHistory::Loose(&change)])
            .unwrap();
        assert!(signing.is_accepted(&hash));
    }
    assert_eq!(document.get_heads(), heads_before);
    assert_eq!(
        document.get(ROOT, "value").unwrap().unwrap().0,
        value_before
    );
}

#[test]
fn insertion_uses_the_changes_author_after_the_current_author_changes() {
    let alice = SigningKey::from_bytes(&[7; 32]);
    let bob = SigningKey::from_bytes(&[8; 32]);
    let mut document = document(Some(author(&alice)));
    let change = append(&mut document, 1);
    let hash = change.hash();
    let delayed_signature = sign(&alice, &hash);

    document.set_author(Some(author(&bob)));
    document.set_actor(ActorId::from([2]));
    let later = append(&mut document, 2);
    assert_ne!(change.actor_id(), later.actor_id());
    assert_eq!(document.get_author(), Some(&author(&bob)));
    let heads_before = document.get_heads();
    {
        let mut signing = document.try_signing().unwrap();
        // A signature from the current author cannot attest Alice's old change.
        let error = signing
            .insert_signature(&hash, &sign(&bob, &hash))
            .unwrap_err();
        assert!(
            matches!(error, error::InsertSignature::Verification { change, .. } if change == hash)
        );
        assert_not_inserted(&signing, &hash);

        assert_eq!(
            signing.insert_signature(&hash, &delayed_signature).unwrap(),
            Insert::Stored
        );
        assert!(!signing.is_accepted(&hash));
        signing
            .accept_applied(&[CandidateHistory::Loose(&change)])
            .unwrap();
        assert!(signing.is_accepted(&hash));
        assert!(!signing.is_accepted(&later.hash()));
    }
    assert_eq!(document.get_heads(), heads_before);
}

#[test]
fn insertion_rejects_an_unknown_hash_even_when_its_actor_is_known() {
    let key = SigningKey::from_bytes(&[7; 32]);
    let mut document = document(Some(author(&key)));
    let known = append(&mut document, 1);
    let mut other = document.clone();
    let unknown = append(&mut other, 2);
    assert_eq!(known.actor_id(), unknown.actor_id());
    let hash = unknown.hash();
    let heads_before = document.get_heads();
    {
        let mut signing = document.try_signing().unwrap();
        let error = signing
            .insert_signature(&hash, &sign(&key, &hash))
            .unwrap_err();
        assert!(
            matches!(error, error::InsertSignature::UnknownChange { change } if change == hash)
        );
        assert_not_inserted(&signing, &hash);
        assert_not_inserted(&signing, &known.hash());
    }
    assert_eq!(document.get_heads(), heads_before);
}

#[test]
fn insertion_does_not_use_the_current_author_for_an_authorless_change() {
    let key = SigningKey::from_bytes(&[7; 32]);
    let mut document = document(None);
    let change = append(&mut document, 1);
    let hash = change.hash();
    document.set_author(Some(author(&key)));
    let mut signing = document.try_signing().unwrap();
    let error = signing
        .insert_signature(&hash, &sign(&key, &hash))
        .unwrap_err();
    assert!(
        matches!(error, error::InsertSignature::MissingAuthor { change: h, actor }
        if h == hash && &actor == change.actor_id())
    );
    assert_not_inserted(&signing, &hash);
}

#[test]
fn insertion_reports_the_changes_unusable_author_key_without_storing_bytes() {
    let key = SigningKey::from_bytes(&[7; 32]);
    // Authors are general identifiers. These bytes cannot encode an Ed25519 key.
    let mut document = document(Some(Author::from(vec![1, 2, 3])));
    let change = append(&mut document, 1);
    let hash = change.hash();
    let mut signing = document.try_signing().unwrap();
    let error = signing
        .insert_signature(&hash, &sign(&key, &hash))
        .unwrap_err();
    assert!(
        matches!(&error, error::InsertSignature::InvalidAuthorKey { change: h, actor, .. }
        if *h == hash && actor == change.actor_id())
    );
    assert!(error.source().is_some());
    assert_not_inserted(&signing, &hash);
}

#[test]
fn insertion_verifies_the_exact_hash_and_preserves_existing_acceptance_on_error() {
    let key = SigningKey::from_bytes(&[7; 32]);
    let mut document = document(Some(author(&key)));
    let first = append(&mut document, 1);
    let second = append(&mut document, 2);
    assert_eq!(first.actor_id(), second.actor_id());
    let signature = sign(&key, &first.hash());
    let heads_before = document.get_heads();
    {
        let mut signing = document.try_signing().unwrap();
        signing.insert_signature(&first.hash(), &signature).unwrap();
        signing
            .accept_applied(&[CandidateHistory::Loose(&first)])
            .unwrap();

        let error = signing
            .insert_signature(&second.hash(), &signature)
            .unwrap_err();
        assert!(
            matches!(&error, error::InsertSignature::Verification { change, .. }
            if *change == second.hash())
        );
        assert!(error.source().is_some());
        assert_not_inserted(&signing, &second.hash());
        assert!(signing.is_accepted(&first.hash()));
    }
    let signing = document.try_signing().unwrap();
    assert!(signing.is_accepted(&first.hash()));
    assert_not_inserted(&signing, &second.hash());
    assert_eq!(document.get_heads(), heads_before);
}
