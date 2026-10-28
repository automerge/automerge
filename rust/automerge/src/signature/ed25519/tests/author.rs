use std::collections::BTreeMap;

use ed25519_dalek::{Signer, SigningKey};

use crate::signature::ed25519::{error, AuthorVerifier, Signature, VerifyingAuthor};
use crate::signature::signing_message;
use crate::transaction::Transactable;
use crate::{ActorId, Author, AutoCommit, Change, ROOT};

pub(super) fn changes(actor: u8, author: Author<'static>) -> Vec<Change> {
    let mut doc = AutoCommit::new()
        .with_author(Some(author))
        .with_actor(ActorId::from([actor]));
    for value in [1, 2] {
        doc.put(ROOT, "value", value).unwrap();
        doc.commit();
    }
    doc.get_changes(&[])
}

pub(super) fn fixture() -> (
    SigningKey,
    Vec<Change>,
    BTreeMap<ActorId, VerifyingAuthor<'static>>,
) {
    let key = SigningKey::from_bytes(&[7; 32]);
    let author = Author::from(key.verifying_key().to_bytes().to_vec());
    let changes = changes(2, author.clone());
    let prepared = BTreeMap::from([
        (
            changes[0].actor_id().clone(),
            VerifyingAuthor::new(author).unwrap(),
        ),
        (
            ActorId::from([0]),
            VerifyingAuthor::new(Author::from(
                SigningKey::from_bytes(&[8; 32])
                    .verifying_key()
                    .to_bytes()
                    .to_vec(),
            ))
            .unwrap(),
        ),
    ]);
    (key, changes, prepared)
}

pub(super) fn sign(key: &SigningKey, change: &Change) -> Signature {
    Signature::from_bytes(key.sign(&signing_message(&change.hash())).to_bytes())
}

#[test]
fn lookup_borrows_the_existing_prepared_value_without_rebuilding_it() {
    let (_, changes, prepared) = fixture();
    let verifier = AuthorVerifier::new(&prepared);
    for change in &changes {
        let selected = verifier.verifying_author(change).unwrap();
        let expected = prepared.get(change.actor_id()).unwrap();
        assert!(std::ptr::eq(selected, expected));
    }
}

#[test]
fn lookup_reports_the_missing_actor_id_and_change_hash() {
    let (_, changes, mut prepared) = fixture();
    prepared.remove(changes[0].actor_id());
    let verifier = AuthorVerifier::new(&prepared);
    for change in &changes {
        assert!(matches!(verifier.verifying_author(change),
            Err(error::VerifiedSignature::MissingVerifyingAuthor { change: hash, actor })
                if hash == change.hash() && &actor == change.actor_id()));
    }
}

#[test]
fn initial_change_uses_prepared_author_and_its_own_hash() {
    let (key, changes, prepared) = fixture();
    let change = &changes[0];
    assert_eq!(change.seq(), 1);
    let expected_author = change.author().expect("initial author footer");
    let signature = sign(&key, change);
    let verifier = AuthorVerifier::new(&prepared);
    let witness = verifier.verify_change(change, &signature).unwrap();

    assert_eq!(witness.author(), &expected_author);
    assert_eq!(witness.hash(), &change.hash());
    assert_eq!(witness.signature(), &signature);
}

#[test]
fn later_change_uses_prepared_author_without_a_footer() {
    let (key, changes, prepared) = fixture();
    let change = &changes[1];
    assert_eq!(change.seq(), 2);
    assert!(change.author().is_none());
    let signature = sign(&key, change);
    let verifier = AuthorVerifier::new(&prepared);
    let witness = verifier.verify_change(change, &signature).unwrap();

    assert_eq!(witness.author().as_bytes(), key.verifying_key().as_bytes());
    assert_eq!(witness.hash(), &change.hash());
    assert_eq!(witness.signature(), &signature);
}

#[test]
fn missing_prepared_author_is_not_a_fallback_to_the_footer_or_another_actor() {
    let (key, changes, mut prepared) = fixture();
    prepared.remove(changes[0].actor_id());
    let verifier = AuthorVerifier::new(&prepared);
    for change in &changes {
        let signature = sign(&key, change);
        assert!(matches!(verifier.verify_change(change, &signature),
            Err(error::VerifiedSignature::MissingVerifyingAuthor { change: hash, actor })
                if hash == change.hash() && &actor == change.actor_id()));
    }
}

#[test]
fn another_authors_signature_does_not_verify_for_the_prepared_author() {
    let (_, changes, prepared) = fixture();
    let signature = sign(&SigningKey::from_bytes(&[8; 32]), &changes[1]);
    let error = AuthorVerifier::new(&prepared)
        .verify_change(&changes[1], &signature)
        .unwrap_err();

    assert!(
        matches!(&error, error::VerifiedSignature::Verification { change, .. }
        if *change == changes[1].hash())
    );
    assert!(std::error::Error::source(&error)
        .unwrap()
        .is::<error::Verification>());
}

#[test]
fn signature_for_another_change_of_the_same_actor_is_rejected() {
    let (key, changes, prepared) = fixture();
    let signature = sign(&key, &changes[0]);
    assert!(
        matches!(AuthorVerifier::new(&prepared).verify_change(&changes[1], &signature),
        Err(error::VerifiedSignature::Verification { change, .. }) if change == changes[1].hash())
    );
}

#[test]
fn modified_signature_is_rejected() {
    let (key, changes, prepared) = fixture();
    let mut bytes = sign(&key, &changes[1]).to_bytes();
    bytes[32] ^= 1;
    let signature = Signature::from_bytes(bytes);
    assert!(
        matches!(AuthorVerifier::new(&prepared).verify_change(&changes[1], &signature),
        Err(error::VerifiedSignature::Verification { change, .. }) if change == changes[1].hash())
    );
}

#[test]
fn signing_the_raw_hash_without_the_domain_prefix_is_rejected() {
    let (key, changes, prepared) = fixture();
    let signature = Signature::from_bytes(key.sign(&changes[1].hash().0).to_bytes());
    assert!(
        matches!(AuthorVerifier::new(&prepared).verify_change(&changes[1], &signature),
        Err(error::VerifiedSignature::Verification { change, .. }) if change == changes[1].hash())
    );
}

#[test]
fn witness_outlives_the_map_verifier_and_change_but_borrows_author_bytes() {
    let key = SigningKey::from_bytes(&[7; 32]);
    let author_bytes = key.verifying_key().to_bytes();
    let changes = changes(2, Author::from(author_bytes.to_vec()));
    let hash = changes[1].hash();
    let signature = sign(&key, &changes[1]);
    let witness = {
        let prepared = BTreeMap::from([(
            changes[1].actor_id().clone(),
            VerifyingAuthor::new(Author::from(author_bytes.as_slice())).unwrap(),
        )]);
        AuthorVerifier::new(&prepared)
            .verify_change(&changes[1], &signature)
            .unwrap()
    };
    drop(changes);

    assert_eq!(witness.hash(), &hash);
    assert_eq!(witness.signature(), &signature);
    assert!(std::ptr::eq(
        witness.author().as_bytes(),
        author_bytes.as_slice()
    ));
}

#[test]
fn caller_preparation_uses_the_document_actor_to_author_association() {
    use crate::actor::ActorTable;
    use crate::author::Authors;

    let (key, changes, _) = fixture();
    let table = ActorTable::from_actors([ActorId::from([0]), changes[1].actor_id().clone()]);
    let mut authors = Authors::new(&table);
    authors.assign_author(
        Author::from(key.verifying_key().to_bytes().to_vec()),
        table.lookup(changes[1].actor_id()).unwrap(),
    );
    let prepared: BTreeMap<_, _> = table
        .iter()
        .enumerate()
        .filter_map(|(index, actor)| {
            authors
                .get_author_for_actor(index)
                .map(|author| (actor.clone(), VerifyingAuthor::new(author).unwrap()))
        })
        .collect();
    let signature = sign(&key, &changes[1]);
    let witness = AuthorVerifier::new(&prepared)
        .verify_change(&changes[1], &signature)
        .unwrap();
    assert_eq!(witness.hash(), &changes[1].hash());
    assert_eq!(witness.author().as_bytes(), key.verifying_key().as_bytes());
}
