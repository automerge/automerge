use std::collections::BTreeMap;

use ed25519_dalek::SigningKey;

use crate::signature::ed25519::{
    error, AuthorVerifier, Signature, VerifiedSignature, VerifyingAuthor,
};
use crate::{ActorId, Author, Change};

use super::author::{changes, fixture, sign};

#[test]
fn empty_batch_returns_no_results() {
    assert!(AuthorVerifier::new(&BTreeMap::new())
        .verify_changes([])
        .is_empty());
}

#[test]
fn repeated_actor_and_hash_inputs_preserve_order_and_signature_identity() {
    let (key, changes, prepared) = fixture();
    let first = sign(&key, &changes[0]);
    let second = sign(&key, &changes[1]);
    let wrong = sign(&SigningKey::from_bytes(&[8; 32]), &changes[1]);
    let inputs = [
        (&changes[1], &second),
        (&changes[1], &wrong),
        (&changes[0], &first),
        (&changes[1], &second),
    ];
    let results = AuthorVerifier::new(&prepared).verify_changes(inputs);
    assert_eq!(results.len(), inputs.len());
    for index in [0, 2, 3] {
        let witness = results[index].as_ref().unwrap();
        assert_eq!(*witness.hash(), inputs[index].0.hash());
        assert_eq!(witness.signature(), inputs[index].1);
        assert_eq!(witness.author().as_bytes(), key.verifying_key().as_bytes());
    }
    assert!(
        matches!(&results[1], Err(error::VerifiedSignature::Verification { change, .. })
        if *change == changes[1].hash())
    );
}

#[test]
fn different_actors_use_their_own_prepared_authors_and_do_not_fail_fast() {
    let (key, first_changes, mut prepared) = fixture();
    let other_key = SigningKey::from_bytes(&[8; 32]);
    let other_author = Author::from(other_key.verifying_key().to_bytes().to_vec());
    let other_changes = changes(1, other_author.clone());
    prepared.insert(
        other_changes[0].actor_id().clone(),
        VerifyingAuthor::new(other_author.clone()).unwrap(),
    );
    let first_signature = sign(&key, &first_changes[1]);
    let second_signature = sign(&other_key, &other_changes[1]);
    let results = AuthorVerifier::new(&prepared).verify_changes([
        (&first_changes[1], &second_signature),
        (&other_changes[1], &second_signature),
        (&first_changes[1], &first_signature),
    ]);
    assert_eq!(results.len(), 3);
    assert!(
        matches!(&results[0], Err(error::VerifiedSignature::Verification { change, .. })
        if *change == first_changes[1].hash())
    );
    assert_eq!(results[1].as_ref().unwrap().author(), &other_author);
    assert_eq!(
        results[2].as_ref().unwrap().author().as_bytes(),
        key.verifying_key().as_bytes()
    );
}

#[test]
fn missing_prepared_actors_get_per_input_errors_without_filtering_the_batch() {
    let (key, known, prepared) = fixture();
    let unknown = changes(9, Author::from(key.verifying_key().to_bytes().to_vec()));
    let signatures = [
        sign(&key, &unknown[0]),
        sign(&key, &known[1]),
        sign(&key, &unknown[1]),
    ];
    let results = AuthorVerifier::new(&prepared).verify_changes([
        (&unknown[0], &signatures[0]),
        (&known[1], &signatures[1]),
        (&unknown[1], &signatures[2]),
    ]);
    assert_eq!(results.len(), 3);
    assert!(
        matches!(&results[0], Err(error::VerifiedSignature::MissingVerifyingAuthor { change, actor })
        if *change == unknown[0].hash() && actor == unknown[0].actor_id())
    );
    assert!(results[1].is_ok());
    assert!(
        matches!(&results[2], Err(error::VerifiedSignature::MissingVerifyingAuthor { change, actor })
        if *change == unknown[1].hash() && actor == unknown[1].actor_id())
    );
}

#[test]
fn preparation_retains_invalid_key_diagnostics_outside_the_batch() {
    let (key, known, prepared) = fixture();
    let mut identity = vec![0; 32];
    identity[0] = 1;
    for (bytes, weak) in [(vec![7; 31], false), (identity, true)] {
        let omitted = changes(9, Author::from(bytes.clone()));
        let error = VerifyingAuthor::new(Author::from(bytes)).unwrap_err();
        match (&error.0, weak) {
            (error::VerifyingAuthorInner::WeakKey, true) => {}
            (error::VerifyingAuthorInner::Key(_), false) => {}
            _ => panic!("wrong key failure reason: {error:?}"),
        }
        // The preparation error stays with the caller. Omitting the key from
        // the final map does not erase inputs or mark their signatures invalid.
        let diagnostic: &error::VerifyingAuthor = &error;
        assert!(std::error::Error::source(diagnostic).is_some());
        let signatures = [
            sign(&key, &omitted[0]),
            sign(&key, &known[1]),
            sign(&key, &omitted[1]),
        ];
        let results = AuthorVerifier::new(&prepared).verify_changes([
            (&omitted[0], &signatures[0]),
            (&known[1], &signatures[1]),
            (&omitted[1], &signatures[2]),
        ]);
        assert_eq!(results.len(), 3);
        assert!(results[1].is_ok());
        for (index, change) in [(0, &omitted[0]), (2, &omitted[1])] {
            assert!(
                matches!(&results[index], Err(error::VerifiedSignature::MissingVerifyingAuthor {
                change: hash, actor,
            }) if *hash == change.hash() && actor == change.actor_id())
            );
        }
    }
}

#[test]
fn one_item_batch_agrees_with_singular_verification() {
    let (key, changes, prepared) = fixture();
    let signature = sign(&key, &changes[1]);
    let invalid = Signature::from_bytes([0; 64]);
    let verifier = AuthorVerifier::new(&prepared);
    let single = verifier.verify_change(&changes[1], &signature).unwrap();
    let batch = verifier.verify_changes([(&changes[1], &signature)]);
    let batched = batch[0].as_ref().unwrap();
    assert_eq!(single.author(), batched.author());
    assert_eq!(single.hash(), batched.hash());
    assert_eq!(single.signature(), batched.signature());
    assert!(matches!(
        verifier.verify_change(&changes[1], &invalid),
        Err(error::VerifiedSignature::Verification { .. })
    ));
    assert!(matches!(
        &verifier.verify_changes([(&changes[1], &invalid)])[0],
        Err(error::VerifiedSignature::Verification { .. })
    ));
}

fn verify_temporary_changes<'author, 'signature>(
    prepared: BTreeMap<ActorId, VerifyingAuthor<'author>>,
    changes: Vec<Change>,
    signatures: &'signature [Signature],
) -> Vec<Result<VerifiedSignature<'author, 'signature>, error::VerifiedSignature>> {
    AuthorVerifier::new(&prepared).verify_changes(changes.iter().zip(signatures))
}

#[test]
fn batch_witnesses_outlive_owned_map_changes_and_temporary_verifier() {
    let (key, changes, prepared) = fixture();
    let hashes: Vec<_> = changes.iter().map(Change::hash).collect();
    let signatures: Vec<_> = changes.iter().map(|change| sign(&key, change)).collect();
    let results = verify_temporary_changes(prepared, changes, &signatures);
    assert_eq!(results.len(), hashes.len());
    for ((result, hash), signature) in results.iter().zip(hashes).zip(&signatures) {
        let witness = result.as_ref().unwrap();
        assert_eq!(*witness.hash(), hash);
        assert_eq!(witness.signature(), signature);
        assert_eq!(witness.author().as_bytes(), key.verifying_key().as_bytes());
    }
}
