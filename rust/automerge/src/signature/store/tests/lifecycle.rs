use std::collections::BTreeMap;

use crate::signature::ed25519::AuthorVerifier;
use crate::signature::store::{
    AssociationError, ResolvedChangeAuthor, SignatureStore, VerifyEd25519Error,
};
use crate::signature::{Signature, SignatureBytes, SignatureTag};
use crate::Author;

use super::{evidence_entries, fixture, prepared_authors, sign};

fn unchecked(tag: u8, bytes: &[u8]) -> Signature {
    Signature::new(
        SignatureTag::new(tag),
        SignatureBytes::new(bytes.to_vec()).unwrap(),
    )
}

#[test]
fn identity_resolution_needs_no_ed25519_key_or_signature() {
    let (changes, _, _, _) = fixture([7; 32]);
    let hash = changes[1].hash();
    let alice = Author::from(b"alice".as_slice());
    let authors = BTreeMap::from([(changes[1].actor_id().clone(), alice.clone())]);
    let mut store = SignatureStore::default();
    store.insert_unchecked(hash, unchecked(42, &[9]));
    assert!(store.author(&hash).is_none());

    store
        .associate_author(ResolvedChangeAuthor::resolve(&changes[1], &authors).unwrap())
        .unwrap();
    assert_eq!(store.author(&hash), Some(&alice));
    assert_eq!(store.unchecked_entries(&hash).count(), 1);
    assert!(evidence_entries(&store, &hash).is_empty());
    assert_eq!(
        store.verify_ed25519(&hash).unwrap().skipped_other_schemes,
        1
    );
}

#[test]
fn missing_association_reports_actor_and_hash_without_using_the_footer() {
    let (changes, _, _, _) = fixture([7; 32]);
    assert!(changes[0].author().is_some());
    let error = ResolvedChangeAuthor::resolve(&changes[0], &BTreeMap::new()).unwrap_err();
    assert!(
        matches!(error, AssociationError::MissingAuthor { change, actor }
        if change == changes[0].hash() && &actor == changes[0].actor_id())
    );
}

#[test]
fn resolved_context_and_evidence_outlive_the_prepared_map() {
    let (mut store, hash, expected) = {
        let (changes, table, authors, key) = fixture([7; 32]);
        let prepared = prepared_authors(&table, &authors);
        let verifier = AuthorVerifier::new(&prepared);
        let association = ResolvedChangeAuthor::from_ed25519(&changes[1], &verifier).unwrap();
        let signature = sign(&key, &changes[1]);
        let expected = authors
            .get_author_for_actor(table.lookup(changes[1].actor_id()).unwrap())
            .unwrap()
            .into_owned();
        let mut store = SignatureStore::default();
        store.insert_unchecked(changes[1].hash(), Signature::ed25519(signature));
        store.associate_author(association).unwrap();
        (store, changes[1].hash(), expected)
    };
    assert_eq!(store.author(&hash), Some(&expected));
    assert_eq!(
        store
            .verify_ed25519(&hash)
            .unwrap()
            .established_ed25519_evidence,
        1
    );
    assert_eq!(store.ed25519_entries(&hash).count(), 1);
}

#[test]
fn identity_and_prepared_resolution_agree_and_preparation_never_downgrades() {
    let (changes, table, authors, _) = fixture([7; 32]);
    let hash = changes[1].hash();
    let prepared = prepared_authors(&table, &authors);
    let author = prepared
        .get(changes[1].actor_id())
        .unwrap()
        .author()
        .clone();
    let identities = BTreeMap::from([(changes[1].actor_id().clone(), author)]);
    let mut store = SignatureStore::default();
    store
        .associate_author(ResolvedChangeAuthor::resolve(&changes[1], &identities).unwrap())
        .unwrap();
    store
        .associate_author(
            ResolvedChangeAuthor::from_ed25519(&changes[1], &AuthorVerifier::new(&prepared))
                .unwrap(),
        )
        .unwrap();
    store
        .associate_author(ResolvedChangeAuthor::resolve(&changes[1], &identities).unwrap())
        .unwrap();

    // Internal state check: re-association with identity-only data must retain
    // the already prepared key, not request key decoding again on the next pass.
    let super::super::ChangeSignatures::Resolved(resolved) = store.by_hash.get(&hash).unwrap()
    else {
        panic!("resolved")
    };
    assert!(matches!(
        resolved.author,
        super::super::AuthorContext::WithEd25519Key(_)
    ));
    assert!(evidence_entries(&store, &hash).is_empty());
}

#[test]
fn failed_key_preparation_preserves_resolved_identity_and_all_entries() {
    let (changes, _, _, _) = fixture([7; 32]);
    let hash = changes[1].hash();
    let alice = Author::from(b"alice".as_slice());
    let authors = BTreeMap::from([(changes[1].actor_id().clone(), alice.clone())]);
    let signature = unchecked(0, &[7]);
    let mut store = SignatureStore::default();
    store.insert_unchecked(hash, signature.clone());
    store
        .associate_author(ResolvedChangeAuthor::resolve(&changes[1], &authors).unwrap())
        .unwrap();

    assert!(
        matches!(store.verify_ed25519(&hash), Err(VerifyEd25519Error::InvalidAuthorKey { change, .. }) if change == hash)
    );
    assert_eq!(store.author(&hash), Some(&alice));
    assert_eq!(
        store.unchecked_entries(&hash).collect::<Vec<_>>(),
        vec![&signature]
    );
    assert!(evidence_entries(&store, &hash).is_empty());
}

#[test]
fn failed_signatures_leave_an_empty_resolved_record_and_block_reassociation() {
    let (changes, table, authors, _) = fixture([7; 32]);
    let hash = changes[1].hash();
    let prepared = prepared_authors(&table, &authors);
    let expected = prepared
        .get(changes[1].actor_id())
        .unwrap()
        .author()
        .clone();
    let mut store = SignatureStore::default();
    store.insert_unchecked(hash, unchecked(0, &[7]));
    store
        .associate_author(
            ResolvedChangeAuthor::from_ed25519(&changes[1], &AuthorVerifier::new(&prepared))
                .unwrap(),
        )
        .unwrap();
    assert_eq!(store.verify_ed25519(&hash).unwrap().discarded_invalid, 1);
    assert_eq!(store.author(&hash), Some(&expected));
    assert_eq!(store.unchecked_entries(&hash).count(), 0);
    assert_eq!(store.ed25519_entries(&hash).count(), 0);

    let others = BTreeMap::from([(
        changes[1].actor_id().clone(),
        Author::from(b"bob".as_slice()),
    )]);
    assert!(
        matches!(store.associate_author(ResolvedChangeAuthor::resolve(&changes[1], &others).unwrap()),
        Err(AssociationError::AuthorConflict { change }) if change == hash)
    );
    assert_eq!(store.author(&hash), Some(&expected));
}

#[test]
fn an_empty_resolved_record_rejects_evidence_for_another_author() {
    let (changes, table, authors, _) = fixture([7; 32]);
    let hash = changes[1].hash();
    let prepared = prepared_authors(&table, &authors);
    let mut store = SignatureStore::default();
    store
        .associate_author(
            ResolvedChangeAuthor::from_ed25519(&changes[1], &AuthorVerifier::new(&prepared))
                .unwrap(),
        )
        .unwrap();
    let expected = store.author(&hash).unwrap().clone();
    let other_key = ed25519_dalek::SigningKey::from_bytes(&[8; 32]);
    let other_author = crate::signature::ed25519::VerifyingAuthor::new(Author::from(
        other_key.verifying_key().to_bytes().to_vec(),
    ))
    .unwrap();
    let other_map = BTreeMap::from([(changes[1].actor_id().clone(), other_author)]);
    let signature = sign(&other_key, &changes[1]);
    let witness = AuthorVerifier::new(&other_map)
        .verify_change(&changes[1], &signature)
        .unwrap();

    assert!(matches!(store.insert_ed25519(witness),
        Err(crate::signature::store::InsertError::AuthorConflict { change }) if change == hash));
    assert_eq!(store.author(&hash), Some(&expected));
    assert_eq!(store.unchecked_entries(&hash).count(), 0);
    assert_eq!(store.ed25519_entries(&hash).count(), 0);
}

#[test]
fn resolution_is_idempotent_and_does_not_touch_other_hashes() {
    let (changes, _, _, key) = fixture([7; 32]);
    let author = Author::from(key.verifying_key().to_bytes().to_vec());
    let identities = BTreeMap::from([(changes[1].actor_id().clone(), author.clone())]);
    let mut store = SignatureStore::default();
    let signature = unchecked(42, &[5]);
    store.insert_unchecked(changes[0].hash(), signature.clone());
    for _ in 0..2 {
        store
            .associate_author(ResolvedChangeAuthor::resolve(&changes[1], &identities).unwrap())
            .unwrap();
    }
    assert_eq!(store.author(&changes[1].hash()), Some(&author));
    assert!(store.author(&changes[0].hash()).is_none());
    assert_eq!(
        store
            .unchecked_entries(&changes[0].hash())
            .collect::<Vec<_>>(),
        vec![&signature]
    );
    assert_eq!(store.unchecked_entries(&changes[1].hash()).count(), 0);
}

#[test]
fn inserting_evidence_into_identity_only_context_retains_the_witness_key() {
    let (changes, table, authors, key) = fixture([7; 32]);
    let hash = changes[1].hash();
    let prepared = prepared_authors(&table, &authors);
    let verifier = AuthorVerifier::new(&prepared);
    let author = prepared
        .get(changes[1].actor_id())
        .unwrap()
        .author()
        .clone();
    let identities = BTreeMap::from([(changes[1].actor_id().clone(), author)]);
    let signature = sign(&key, &changes[1]);
    let witness = verifier.verify_change(&changes[1], &signature).unwrap();
    let mut store = SignatureStore::default();
    store
        .associate_author(ResolvedChangeAuthor::resolve(&changes[1], &identities).unwrap())
        .unwrap();
    store.insert_unchecked(hash, Signature::ed25519(signature));
    let other_scheme = unchecked(42, &[9]);
    store.insert_unchecked(hash, other_scheme.clone());

    assert!(matches!(
        store.insert_ed25519(witness.clone()),
        Ok(crate::signature::store::Insert::EstablishedEd25519Evidence)
    ));
    assert!(matches!(
        store.insert_ed25519(witness),
        Ok(crate::signature::store::Insert::Duplicate)
    ));
    assert_eq!(
        store.unchecked_entries(&hash).collect::<Vec<_>>(),
        vec![&other_scheme]
    );
    let super::super::ChangeSignatures::Resolved(resolved) = store.by_hash.get(&hash).unwrap()
    else {
        panic!("expected resolved record");
    };
    assert!(matches!(
        resolved.author,
        super::super::AuthorContext::WithEd25519Key(_)
    ));
    assert_eq!(resolved.ed25519_evidence, vec![signature]);
}

#[test]
fn identity_only_record_prepares_once_and_reuses_key_for_later_entries() {
    let (changes, _, _, key) = fixture([7; 32]);
    let hash = changes[1].hash();
    let identities = BTreeMap::from([(
        changes[1].actor_id().clone(),
        Author::from(key.verifying_key().to_bytes().to_vec()),
    )]);
    let mut store = SignatureStore::default();
    store
        .associate_author(ResolvedChangeAuthor::resolve(&changes[1], &identities).unwrap())
        .unwrap();
    store.insert_unchecked(hash, Signature::ed25519(sign(&key, &changes[1])));
    assert_eq!(
        store
            .verify_ed25519(&hash)
            .unwrap()
            .established_ed25519_evidence,
        1
    );
    store.insert_unchecked(hash, unchecked(0, &[8]));
    assert_eq!(store.verify_ed25519(&hash).unwrap().discarded_invalid, 1);

    let super::super::ChangeSignatures::Resolved(resolved) = store.by_hash.get(&hash).unwrap()
    else {
        panic!("resolved")
    };
    assert!(matches!(
        resolved.author,
        super::super::AuthorContext::WithEd25519Key(_)
    ));
    assert_eq!(resolved.ed25519_evidence.len(), 1);
}
