use ed25519_dalek::SigningKey;

use crate::author::Authors;
use crate::signature::ed25519::AuthorVerifier;
use crate::signature::store::{Insert, InsertError, SignatureStore};
use crate::signature::{ed25519, Signature, SignatureBytes, SignatureTag};
use crate::{Author, ChangeHash};

use super::{evidence_entries, fixture, prepared_authors, sign};

fn opaque(tag: u8, byte: u8) -> Signature {
    Signature::new(
        SignatureTag::new(tag),
        SignatureBytes::new(vec![byte]).unwrap(),
    )
}

fn unchecked(store: &SignatureStore, hash: &ChangeHash) -> Vec<(u8, Vec<u8>)> {
    let mut entries: Vec<_> = store
        .unchecked_entries(hash)
        .map(|s| (s.tag().inner_tag(), s.to_bytes().as_bytes().to_vec()))
        .collect();
    entries.sort();
    entries
}

fn total(store: &SignatureStore, hash: &ChangeHash) -> usize {
    store.ed25519_entries(hash).count() + store.unchecked_entries(hash).count()
}

#[test]
fn typed_malformed_and_unknown_signatures_remain_unchecked() {
    let (changes, _, _, key) = fixture([7; 32]);
    let hash = changes[1].hash();
    let typed = Signature::ed25519(sign(&key, &changes[1]));
    let unknown = opaque(255, 3);
    let malformed = opaque(0, 4);
    let mut store = SignatureStore::default();
    for signature in [typed, unknown, malformed] {
        assert_eq!(
            store.insert_unchecked(hash, signature.clone()),
            Insert::Stored
        );
        assert_eq!(store.insert_unchecked(hash, signature), Insert::Duplicate);
    }
    assert_eq!(total(&store, &hash), 3);
    assert!(evidence_entries(&store, &hash).is_empty());
}

#[test]
fn unchecked_only_overflow_preserves_the_first_sixteen_entries() {
    let hash = ChangeHash([7; 32]);
    let mut store = SignatureStore::default();
    for byte in 0..16 {
        assert_eq!(
            store.insert_unchecked(hash, opaque(255, byte)),
            Insert::Stored
        );
    }
    let before = unchecked(&store, &hash);
    assert_eq!(
        store.insert_unchecked(hash, opaque(255, 0)),
        Insert::Duplicate
    );
    assert_eq!(
        store.insert_unchecked(hash, opaque(255, 16)),
        Insert::DroppedOverflow
    );
    assert_eq!(unchecked(&store, &hash), before);
    assert!(evidence_entries(&store, &hash).is_empty());
}

#[test]
fn first_ed25519_evidence_below_capacity_keeps_all_unchecked_entries() {
    let (changes, table, authors, key) = fixture([7; 32]);
    let hash = changes[1].hash();
    let signature = sign(&key, &changes[1]);
    let mut store = SignatureStore::default();
    store.insert_unchecked(hash, opaque(255, 9));
    let prepared = prepared_authors(&table, &authors);
    let verifier = AuthorVerifier::new(&prepared);
    let witness = verifier.verify_change(&changes[1], &signature).unwrap();

    assert!(matches!(store.insert_ed25519(witness), Ok(Insert::Stored)));
    assert_eq!(unchecked(&store, &hash), vec![(255, vec![9])]);
    assert_eq!(evidence_entries(&store, &hash).len(), 1);
    assert_eq!(total(&store, &hash), 2);
}

#[test]
fn establishing_ed25519_evidence_preserves_bytes_and_establishes_author() {
    let (changes, table, authors, key) = fixture([7; 32]);
    let change = &changes[1];
    let hash = change.hash();
    let signature = sign(&key, change);
    let mut store = SignatureStore::default();
    store.insert_unchecked(hash, opaque(255, 9));
    store.insert_unchecked(hash, Signature::ed25519(signature));
    let prepared = prepared_authors(&table, &authors);
    let verifier = AuthorVerifier::new(&prepared);
    let witness = verifier.verify_change(change, &signature).unwrap();
    let expected = witness.author().clone().into_owned();

    assert!(matches!(
        store.insert_ed25519(witness.clone()),
        Ok(Insert::EstablishedEd25519Evidence)
    ));
    assert_eq!(evidence_entries(&store, &hash), vec![(expected, signature)]);
    assert_eq!(unchecked(&store, &hash), vec![(255, vec![9])]);
    assert_eq!(total(&store, &hash), 2);
    assert!(matches!(
        store.insert_ed25519(witness),
        Ok(Insert::Duplicate)
    ));
    assert_eq!(
        store.insert_unchecked(hash, Signature::ed25519(signature)),
        Insert::Duplicate
    );
    assert_eq!(store.unchecked_entries(&hash).count(), 1);
}

#[test]
fn first_ed25519_evidence_at_capacity_does_not_evict_matching_or_other_entries() {
    let (changes, table, authors, key) = fixture([7; 32]);
    let hash = changes[1].hash();
    let signature = sign(&key, &changes[1]);
    let mut store = SignatureStore::default();
    store.insert_unchecked(hash, Signature::ed25519(signature));
    for byte in 0..15 {
        store.insert_unchecked(hash, opaque(255, byte));
    }
    let prepared = prepared_authors(&table, &authors);
    let verifier = AuthorVerifier::new(&prepared);

    assert!(matches!(
        store.insert_ed25519(verifier.verify_change(&changes[1], &signature).unwrap()),
        Ok(Insert::EstablishedEd25519Evidence)
    ));
    assert_eq!(total(&store, &hash), 16);
    assert_eq!(
        unchecked(&store, &hash),
        (0..15).map(|b| (255, vec![b])).collect::<Vec<_>>()
    );
    assert_eq!(
        store.insert_unchecked(hash, opaque(255, 0)),
        Insert::Duplicate
    );
    assert_eq!(
        store.insert_unchecked(hash, opaque(255, 99)),
        Insert::DroppedOverflow
    );
}

#[test]
fn first_ed25519_evidence_displaces_largest_unchecked_entry_regardless_of_arrival_order() {
    let (changes, table, authors, key) = fixture([7; 32]);
    let hash = changes[1].hash();
    let signature = sign(&key, &changes[1]);
    let prepared = prepared_authors(&table, &authors);
    let verifier = AuthorVerifier::new(&prepared);
    let mut inputs: Vec<_> = (0..14).map(|b| opaque(42, b)).collect();
    inputs.extend([opaque(254, 255), opaque(255, 0)]);
    for reverse in [false, true] {
        let mut store = SignatureStore::default();
        let mut order = inputs.clone();
        if reverse {
            order.reverse();
        }
        for input in order {
            assert_eq!(store.insert_unchecked(hash, input), Insert::Stored);
        }
        assert!(matches!(
            store.insert_ed25519(verifier.verify_change(&changes[1], &signature).unwrap()),
            Ok(Insert::ReplacedUnchecked)
        ));
        assert_eq!(total(&store, &hash), 16);
        assert_eq!(evidence_entries(&store, &hash).len(), 1);
        let mut expected: Vec<_> = (0..14).map(|b| (42, vec![b])).collect();
        expected.push((254, vec![255]));
        assert_eq!(unchecked(&store, &hash), expected);
        assert_eq!(
            store.insert_unchecked(hash, Signature::ed25519(signature)),
            Insert::Duplicate
        );
    }
}

#[test]
fn replacement_uses_byte_order_within_a_tag_not_representation_order() {
    let (changes, table, authors, key) = fixture([7; 32]);
    let hash = changes[1].hash();
    let signature = sign(&key, &changes[1]);
    let mut store = SignatureStore::default();
    // All tag 0: one typed but unchecked value between short opaque values.
    for b in 0..14 {
        store.insert_unchecked(hash, opaque(0, b));
    }
    store.insert_unchecked(
        hash,
        Signature::ed25519(ed25519::Signature::from_bytes([14; 64])),
    );
    store.insert_unchecked(hash, opaque(0, 255));
    let prepared = prepared_authors(&table, &authors);
    let verifier = AuthorVerifier::new(&prepared);

    assert!(matches!(
        store.insert_ed25519(verifier.verify_change(&changes[1], &signature).unwrap()),
        Ok(Insert::ReplacedUnchecked)
    ));
    assert!(!unchecked(&store, &hash).contains(&(0, vec![255])));
    assert!(unchecked(&store, &hash).contains(&(0, vec![14; 64])));
}

#[test]
fn ed25519_evidence_and_unchecked_entries_share_one_budget() {
    let (changes, table, authors, key) = fixture([7; 32]);
    let hash = changes[1].hash();
    let signature = sign(&key, &changes[1]);
    let prepared = prepared_authors(&table, &authors);
    let verifier = AuthorVerifier::new(&prepared);
    let witness = verifier.verify_change(&changes[1], &signature).unwrap();
    let mut store = SignatureStore::default();
    store.insert_ed25519(witness.clone()).unwrap();
    for b in 0..15 {
        assert_eq!(store.insert_unchecked(hash, opaque(255, b)), Insert::Stored);
    }
    let before = evidence_entries(&store, &hash);
    for b in 15..32 {
        assert_eq!(
            store.insert_unchecked(hash, opaque(255, b)),
            Insert::DroppedOverflow
        );
    }
    assert_eq!(total(&store, &hash), 16);
    assert_eq!(evidence_entries(&store, &hash), before);
    assert!(matches!(
        store.insert_ed25519(witness),
        Ok(Insert::Duplicate)
    ));
    let other_hash = changes[0].hash();
    assert_eq!(
        store.insert_unchecked(other_hash, opaque(255, 99)),
        Insert::Stored
    );
    assert_eq!(total(&store, &other_hash), 1);
}

#[test]
fn conflicting_author_cannot_establish_evidence_for_a_matching_unchecked_entry() {
    let (changes, table, authors, key) = fixture([7; 32]);
    let hash = changes[1].hash();
    let signature = sign(&key, &changes[1]);
    let mut store = SignatureStore::default();
    store
        .insert_ed25519(
            AuthorVerifier::new(&prepared_authors(&table, &authors))
                .verify_change(&changes[1], &signature)
                .unwrap(),
        )
        .unwrap();
    let other_key = SigningKey::from_bytes(&[8; 32]);
    let mut other_authors = Authors::new(&table);
    other_authors.assign_author(
        Author::from(other_key.verifying_key().to_bytes().to_vec()),
        table.lookup(changes[1].actor_id()).unwrap(),
    );
    let other_signature = sign(&other_key, &changes[1]);
    store.insert_unchecked(hash, Signature::ed25519(other_signature));
    for b in 0..14 {
        store.insert_unchecked(hash, opaque(255, b));
    }
    let before = (evidence_entries(&store, &hash), unchecked(&store, &hash));

    let error = store.insert_ed25519(
        AuthorVerifier::new(&prepared_authors(&table, &other_authors))
            .verify_change(&changes[1], &other_signature)
            .unwrap(),
    );
    assert!(matches!(error, Err(InsertError::AuthorConflict { change }) if change == hash));
    assert_eq!(
        (evidence_entries(&store, &hash), unchecked(&store, &hash)),
        before
    );
}
