use std::collections::BTreeMap;

use ed25519_dalek::{Signer, SigningKey};
use proptest::prelude::*;

use crate::actor::ActorTable;
use crate::author::Authors;
use crate::signature::ed25519::AuthorVerifier;
use crate::signature::{ed25519, signing_message};
use crate::transaction::Transactable;
use crate::{ActorId, Author, AutoCommit, Change, ChangeHash, ROOT};

use super::{Insert, InsertError, SignatureStore};

mod capacity;
mod ed25519_check;
mod lifecycle;
mod unchecked;

fn fixture(seed: [u8; 32]) -> (Vec<Change>, ActorTable, Authors, SigningKey) {
    let key = SigningKey::from_bytes(&seed);
    let author = Author::from(key.verifying_key().to_bytes().to_vec());
    let mut doc = AutoCommit::new()
        .with_author(Some(author.clone()))
        .with_actor(ActorId::from([2]));
    for value in [1, 2] {
        doc.put(ROOT, "value", value).unwrap();
        doc.commit();
    }
    let changes = doc.get_changes(&[]);
    let table = ActorTable::from_actors([changes[0].actor_id().clone()]);
    let mut authors = Authors::new(&table);
    authors.assign_author(author, table.lookup(changes[0].actor_id()).unwrap());
    (changes, table, authors, key)
}

// Caller-side preparation for these fixtures. Tests deliberately fail rather
// than discard an invalid-key diagnostic. Missing mappings simply have no slot.
fn prepared_authors<'a>(
    table: &ActorTable,
    authors: &'a Authors,
) -> BTreeMap<ActorId, ed25519::VerifyingAuthor<'a>> {
    table
        .iter()
        .enumerate()
        .filter_map(|(index, actor)| {
            authors.get_author_for_actor(index).map(|author| {
                (
                    actor.clone(),
                    ed25519::VerifyingAuthor::new(author).expect("valid fixture author"),
                )
            })
        })
        .collect()
}

fn sign(key: &SigningKey, change: &Change) -> ed25519::Signature {
    ed25519::Signature::from_bytes(key.sign(&signing_message(&change.hash())).to_bytes())
}

fn evidence_entries(
    store: &SignatureStore,
    hash: &ChangeHash,
) -> Vec<(Author<'static>, ed25519::Signature)> {
    store
        .ed25519_entries(hash)
        .map(|(author, signature)| (author.clone(), *signature))
        .collect()
}

#[test]
fn empty_store_has_no_entries() {
    let store = SignatureStore::default();

    assert_eq!(store.ed25519_entries(&ChangeHash([0; 32])).count(), 0);
    assert_eq!(store.unchecked_entries(&ChangeHash([0; 32])).count(), 0);
}

#[test]
fn inserted_evidence_outlives_all_borrowed_inputs() {
    let mut store = SignatureStore::default();
    let (hash, expected_author, expected_signature) = {
        let (changes, table, authors, key) = fixture([7; 32]);
        let change = &changes[1];
        let signature = sign(&key, change);
        let prepared = prepared_authors(&table, &authors);
        let verifier = AuthorVerifier::new(&prepared);
        let witness = verifier.verify_change(change, &signature).unwrap();
        let expected_author = witness.author().clone().into_owned();

        assert!(matches!(store.insert_ed25519(witness), Ok(Insert::Stored)));
        (change.hash(), expected_author, signature)
    };

    // The document, tables, signature input and witness are all gone.
    assert_eq!(
        evidence_entries(&store, &hash),
        vec![(expected_author, expected_signature)]
    );
    assert_eq!(store.ed25519_entries(&ChangeHash([0; 32])).count(), 0);
}

#[test]
fn reinserting_the_same_ed25519_evidence_is_a_duplicate() {
    let (changes, table, authors, key) = fixture([7; 32]);
    let signature = sign(&key, &changes[1]);
    let prepared = prepared_authors(&table, &authors);
    let verifier = AuthorVerifier::new(&prepared);
    let witness = verifier.verify_change(&changes[1], &signature).unwrap();
    let mut store = SignatureStore::default();
    assert!(matches!(
        store.insert_ed25519(witness.clone()),
        Ok(Insert::Stored)
    ));
    let before = evidence_entries(&store, &changes[1].hash());

    assert!(matches!(
        store.insert_ed25519(witness),
        Ok(Insert::Duplicate)
    ));
    assert_eq!(evidence_entries(&store, &changes[1].hash()), before);
    assert_eq!(store.ed25519_entries(&changes[1].hash()).count(), 1);
}

#[test]
fn different_changes_keep_their_signatures_separate() {
    let (changes, table, authors, key) = fixture([7; 32]);
    let first = sign(&key, &changes[0]);
    let second = sign(&key, &changes[1]);
    let prepared = prepared_authors(&table, &authors);
    let verifier = AuthorVerifier::new(&prepared);
    let mut store = SignatureStore::default();
    let expected_author = Author::from(key.verifying_key().to_bytes().to_vec());

    for (change, signature) in [(&changes[0], &first), (&changes[1], &second)] {
        let witness = verifier.verify_change(change, signature).unwrap();
        assert!(matches!(store.insert_ed25519(witness), Ok(Insert::Stored)));
    }

    assert_eq!(
        evidence_entries(&store, &changes[0].hash()),
        vec![(expected_author.clone(), first)]
    );
    assert_eq!(
        evidence_entries(&store, &changes[1].hash()),
        vec![(expected_author, second)]
    );
}

#[test]
fn conflicting_author_is_rejected_without_changing_existing_entries() {
    let (changes, table, authors, key) = fixture([7; 32]);
    let prepared = prepared_authors(&table, &authors);
    let verifier = AuthorVerifier::new(&prepared);
    let first = sign(&key, &changes[0]);
    let second = sign(&key, &changes[1]);
    let mut store = SignatureStore::default();
    let witness = verifier.verify_change(&changes[1], &second).unwrap();
    store
        .insert_ed25519(verifier.verify_change(&changes[0], &first).unwrap())
        .unwrap();
    store.insert_ed25519(witness.clone()).unwrap();
    let first_before = evidence_entries(&store, &changes[0].hash());
    let second_before = evidence_entries(&store, &changes[1].hash());

    // Simulate a caller supplying an inconsistent author context. The other
    // key really signs the same change hash, so this is cryptographically valid
    // evidence, not a forged witness. It must not replace the stored binding.
    let other_key = SigningKey::from_bytes(&[8; 32]);
    let mut other_authors = Authors::new(&table);
    other_authors.assign_author(
        Author::from(other_key.verifying_key().to_bytes().to_vec()),
        table.lookup(changes[1].actor_id()).unwrap(),
    );
    let other_signature = sign(&other_key, &changes[1]);
    let other_prepared = prepared_authors(&table, &other_authors);
    let other_verifier = AuthorVerifier::new(&other_prepared);
    let conflicting = other_verifier
        .verify_change(&changes[1], &other_signature)
        .unwrap();

    assert!(matches!(
        store.insert_ed25519(conflicting),
        Err(InsertError::AuthorConflict { change }) if change == changes[1].hash()
    ));
    assert_eq!(evidence_entries(&store, &changes[0].hash()), first_before);
    assert_eq!(evidence_entries(&store, &changes[1].hash()), second_before);
    assert!(matches!(
        store.insert_ed25519(witness),
        Ok(Insert::Duplicate)
    ));
}

#[test]
fn different_authors_can_have_entries_for_different_hashes() {
    let mut store = SignatureStore::default();
    let mut expected = Vec::new();
    for seed in [[7; 32], [8; 32]] {
        let (changes, table, authors, key) = fixture(seed);
        let signature = sign(&key, &changes[1]);
        let prepared = prepared_authors(&table, &authors);
        let verifier = AuthorVerifier::new(&prepared);
        let witness = verifier.verify_change(&changes[1], &signature).unwrap();
        expected.push((
            changes[1].hash(),
            witness.author().clone().into_owned(),
            signature,
        ));
        assert!(matches!(store.insert_ed25519(witness), Ok(Insert::Stored)));
    }
    assert_ne!(expected[0].0, expected[1].0);
    for (hash, author, signature) in expected {
        assert_eq!(evidence_entries(&store, &hash), vec![(author, signature)]);
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]

    #[test]
    fn repeated_ed25519_insertion_is_idempotent(seed in any::<[u8; 32]>(), repeats in 1usize..=16) {
        let (changes, table, authors, key) = fixture(seed);
        let signature = sign(&key, &changes[1]);
        let prepared = prepared_authors(&table, &authors);
        let verifier = AuthorVerifier::new(&prepared);
        let witness = verifier.verify_change(&changes[1], &signature).unwrap();
        let mut store = SignatureStore::default();
        prop_assert!(matches!(store.insert_ed25519(witness.clone()), Ok(Insert::Stored)));
        let before = evidence_entries(&store, &changes[1].hash());

        for _ in 0..repeats {
            prop_assert!(matches!(store.insert_ed25519(witness.clone()), Ok(Insert::Duplicate)));
            prop_assert_eq!(&evidence_entries(&store, &changes[1].hash()), &before);
        }
    }
}
