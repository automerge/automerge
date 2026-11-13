use ed25519_dalek::{Signer, SigningKey};

use crate::admission::CandidateHistory;
use crate::signature::{ed25519, signing_message};
use crate::transaction::Transactable;
use crate::{ActorId, Author, Automerge, Change, ChangeHash, ReadDoc};

use super::error;

mod fragments;
mod insertion;

#[test]
fn unsigned_document_cannot_produce_a_signing_handle() {
    let mut document = Automerge::new();
    assert!(matches!(
        document.try_signing(),
        Err(error::SigningMode::Required)
    ));
    assert!(document.get_heads().is_empty());
}

#[test]
fn failed_signing_access_does_not_convert_or_change_existing_history() {
    let mut document = Automerge::new().with_actor(ActorId::from([1]));
    let mut tx = document.transaction();
    tx.put(crate::ROOT, "value", 1).unwrap();
    tx.commit();
    let heads = document.get_heads();
    let bytes = document.save();

    assert!(matches!(
        document.try_signing(),
        Err(error::SigningMode::Required)
    ));
    assert!(matches!(
        document.try_signing(),
        Err(error::SigningMode::Required)
    ));
    assert_eq!(document.get_heads(), heads);
    assert_eq!(document.save(), bytes);
}

#[test]
fn signing_constructor_starts_with_empty_history_acceptance_and_evidence() {
    let mut document = Automerge::signing();
    assert!(document.get_heads().is_empty());
    assert!(document.get_changes(&[]).is_empty());
    let signing = document.try_signing().unwrap();

    assert!(signing.graph.is_empty());
    assert_eq!(signing.ops.actors.iter().len(), 0);
    assert!(signing.authors.get_authors().is_empty());
    assert_eq!(
        signing.state.fragment_readiness,
        crate::change_graph::FragmentReadiness::default()
    );
    assert!(!signing.state.accepted.is_accepted_actor_seq(0, 1));
    assert_eq!(
        signing
            .state
            .signatures
            .unchecked_entries(&ChangeHash([0; 32]))
            .count(),
        0
    );
    assert_eq!(
        signing
            .state
            .signatures
            .ed25519_entries(&ChangeHash([0; 32]))
            .count(),
        0
    );
}

#[test]
fn local_edit_is_visible_before_it_is_accepted() {
    let key = SigningKey::from_bytes(&[7; 32]);
    let author = Author::from(key.verifying_key().to_bytes().to_vec());
    let mut document = Automerge::signing()
        .with_author(Some(author))
        .with_actor(ActorId::from([1]));

    let mut tx = document.transaction();
    tx.put(crate::ROOT, "value", 1).unwrap();
    let (hash, _) = tx.commit();
    let hash = hash.unwrap();

    // Local application and heads are available without waiting for signing.
    assert_eq!(document.get_heads(), [hash]);
    assert_eq!(
        document.get(crate::ROOT, "value").unwrap().unwrap().0,
        1.into(),
    );
    {
        let signing = document.try_signing().unwrap();
        assert!(signing.graph.has_change(&hash));
        assert!(!signing.is_accepted(&hash));
        assert!(!signing.is_accepted(&ChangeHash([0xff; 32])));
    }

    // Merely acquiring the handle does not establish acceptance.
    assert!(!document.try_signing().unwrap().is_accepted(&hash));
    assert_eq!(document.get_heads(), [hash]);
}

#[test]
fn acceptance_survives_dropping_and_reacquiring_the_signing_handle() {
    let key = SigningKey::from_bytes(&[7; 32]);
    let author = Author::from(key.verifying_key().to_bytes().to_vec());
    let actor = ActorId::from([1]);
    let mut document = Automerge::signing()
        .with_author(Some(author))
        .with_actor(actor);

    let mut tx = document.transaction();
    tx.put(crate::ROOT, "value", 1).unwrap();
    tx.commit();

    let change = document.get_changes(&[]).pop().unwrap();
    let hash = change.hash();
    let candidates = [CandidateHistory::Loose(&change)];
    let signature = ed25519::Signature::from_bytes(key.sign(&signing_message(&hash)).to_bytes());

    {
        let mut signing = document.try_signing().unwrap();
        signing.insert_signature(&hash, &signature).unwrap();

        // Evidence alone does not advance acceptance.
        assert!(!signing.is_accepted(&hash));
        signing.accept_applied(&candidates).unwrap();
        assert!(signing.is_accepted(&hash));
    }

    assert!(document.try_signing().unwrap().is_accepted(&hash));
    assert_eq!(document.get_heads(), [hash]);
}

fn signed_fixture() -> (Automerge, Change, ed25519::Signature, Author<'static>) {
    let key = SigningKey::from_bytes(&[7; 32]);
    let author = Author::from(key.verifying_key().to_bytes().to_vec());
    let mut source = Automerge::signing()
        .with_author(Some(author.clone()))
        .with_actor(ActorId::from([1]));
    let mut tx = source.transaction();
    tx.put(crate::ROOT, "value", 1).unwrap();
    tx.commit();
    let change = source.get_changes(&[]).pop().unwrap();
    let signature =
        ed25519::Signature::from_bytes(key.sign(&signing_message(&change.hash())).to_bytes());
    (source, change, signature, author)
}

#[test]
fn dropping_and_reacquiring_signing_handle_preserves_retained_evidence() {
    let (mut document, change, signature, author) = signed_fixture();
    let heads_before = document.get_heads();
    {
        let mut signing = document.try_signing().unwrap();
        signing
            .insert_signature(&change.hash(), &signature)
            .unwrap();
    }

    let signing = document.try_signing().unwrap();
    assert_eq!(
        signing.state.signatures.author(&change.hash()),
        Some(&author)
    );
    let entries: Vec<_> = signing
        .state
        .signatures
        .ed25519_entries(&change.hash())
        .collect();
    assert_eq!(entries, vec![(&author, &signature)]);
    assert!(!signing.is_accepted(&change.hash()));
    assert_eq!(document.get_heads(), heads_before);
}

#[test]
fn clone_preserves_required_mode_and_owns_its_signature_entries() {
    let (mut original, first, signature, author) = signed_fixture();
    original
        .try_signing()
        .unwrap()
        .insert_signature(&first.hash(), &signature)
        .unwrap();
    let mut tx = original.transaction();
    tx.put(crate::ROOT, "value", 2).unwrap();
    let (second, _) = tx.commit();
    let second = second.unwrap();
    let key = SigningKey::from_bytes(&[7; 32]);
    let second_signature =
        ed25519::Signature::from_bytes(key.sign(&signing_message(&second)).to_bytes());

    let mut cloned = original.clone();
    {
        let mut signing = cloned.try_signing().unwrap();
        assert_eq!(
            signing
                .state
                .signatures
                .ed25519_entries(&first.hash())
                .collect::<Vec<_>>(),
            vec![(&author, &signature)]
        );
        signing
            .insert_signature(&second, &second_signature)
            .unwrap();
        assert_eq!(
            signing
                .state
                .signatures
                .ed25519_entries(&second)
                .collect::<Vec<_>>(),
            vec![(&author, &second_signature)]
        );
    }
    let signing = original.try_signing().unwrap();
    assert_eq!(signing.state.signatures.author(&second), None);
    assert_eq!(signing.state.signatures.ed25519_entries(&second).count(), 0);
    assert_eq!(
        signing
            .state
            .signatures
            .ed25519_entries(&first.hash())
            .collect::<Vec<_>>(),
        vec![(&author, &signature)]
    );
}
