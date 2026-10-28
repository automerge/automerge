use std::collections::BTreeMap;

use ed25519_dalek::SigningKey;
use proptest::prelude::*;

use crate::signature::ed25519::AuthorVerifier;
use crate::signature::store::{
    AssociationError, Ed25519VerificationReport, ResolvedChangeAuthor, SignatureStore,
    VerifyEd25519Error,
};
use crate::signature::{ed25519, Signature, SignatureBytes, SignatureTag};
use crate::{Author, ChangeHash};

use super::{evidence_entries, fixture, prepared_authors, sign};

fn opaque(tag: u8, bytes: &[u8]) -> Signature {
    Signature::new(
        SignatureTag::new(tag),
        SignatureBytes::new(bytes.to_vec()).unwrap(),
    )
}
fn unchecked(store: &SignatureStore, hash: &ChangeHash) -> Vec<(u8, Vec<u8>)> {
    let mut result: Vec<_> = store
        .unchecked_entries(hash)
        .map(|s| (s.tag().inner_tag(), s.to_bytes().as_bytes().to_vec()))
        .collect();
    result.sort();
    result
}

#[test]
fn signatures_wait_for_resolution_then_gain_evidence_without_another_lookup() {
    let (changes, table, authors, key) = fixture([7; 32]);
    let change = &changes[1];
    let hash = change.hash();
    let signature = sign(&key, change);
    let mut store = SignatureStore::default();
    store.insert_unchecked(hash, Signature::ed25519(signature));
    let before = unchecked(&store, &hash);
    assert!(
        matches!(store.verify_ed25519(&hash), Err(VerifyEd25519Error::UnresolvedAuthor { change }) if change == hash)
    );
    assert_eq!(unchecked(&store, &hash), before);
    let prepared = prepared_authors(&table, &authors);
    store
        .associate_author(
            ResolvedChangeAuthor::from_ed25519(change, &AuthorVerifier::new(&prepared)).unwrap(),
        )
        .unwrap();
    assert_eq!(
        store.verify_ed25519(&hash).unwrap(),
        Ed25519VerificationReport {
            established_ed25519_evidence: 1,
            discarded_invalid: 0,
            skipped_other_schemes: 0,
        }
    );
    assert_eq!(
        evidence_entries(&store, &hash),
        vec![(
            Author::from(key.verifying_key().to_bytes().to_vec()),
            signature
        )]
    );
    assert!(unchecked(&store, &hash).is_empty());
}

#[test]
fn unresolved_author_leaves_malformed_tag_zero_data_untouched() {
    let hash = ChangeHash([1; 32]);
    let mut store = SignatureStore::default();
    store.insert_unchecked(hash, opaque(0, &[1]));
    let before = unchecked(&store, &hash);
    assert!(
        matches!(store.verify_ed25519(&hash), Err(VerifyEd25519Error::UnresolvedAuthor { change }) if change == hash)
    );
    assert_eq!(unchecked(&store, &hash), before);
}

#[test]
fn mixed_entries_gain_evidence_discard_invalid_and_skip_other_schemes() {
    let (changes, table, authors, key) = fixture([7; 32]);
    let hash = changes[1].hash();
    let signature = sign(&key, &changes[1]);
    let mut changed = signature.to_bytes();
    changed[32] ^= 1;
    let mut store = SignatureStore::default();
    for input in [
        Signature::ed25519(signature),
        opaque(0, &[]),
        Signature::ed25519(ed25519::Signature::from_bytes(changed)),
        opaque(255, &[9, 8]),
    ] {
        store.insert_unchecked(hash, input);
    }
    store.insert_unchecked(hash, Signature::ed25519(sign(&key, &changes[0])));
    store.insert_unchecked(changes[0].hash(), opaque(0, &[5]));
    let other_before = unchecked(&store, &changes[0].hash());
    let prepared = prepared_authors(&table, &authors);
    store
        .associate_author(
            ResolvedChangeAuthor::from_ed25519(&changes[1], &AuthorVerifier::new(&prepared))
                .unwrap(),
        )
        .unwrap();
    assert_eq!(
        store.verify_ed25519(&hash).unwrap(),
        Ed25519VerificationReport {
            established_ed25519_evidence: 1,
            discarded_invalid: 3,
            skipped_other_schemes: 1,
        }
    );
    assert_eq!(unchecked(&store, &hash), vec![(255, vec![9, 8])]);
    assert_eq!(evidence_entries(&store, &hash).len(), 1);
    assert_eq!(unchecked(&store, &changes[0].hash()), other_before);
    assert_eq!(
        store.verify_ed25519(&hash).unwrap(),
        Ed25519VerificationReport {
            established_ed25519_evidence: 0,
            discarded_invalid: 0,
            skipped_other_schemes: 1,
        }
    );
}

#[test]
fn other_schemes_need_no_author_or_ed25519_capability() {
    let hash = ChangeHash([2; 32]);
    let mut store = SignatureStore::default();
    store.insert_unchecked(hash, opaque(42, &[]));
    store.insert_unchecked(hash, opaque(255, &[7; 64]));
    let before = unchecked(&store, &hash);
    assert_eq!(
        store.verify_ed25519(&hash).unwrap(),
        Ed25519VerificationReport {
            established_ed25519_evidence: 0,
            discarded_invalid: 0,
            skipped_other_schemes: 2,
        }
    );
    assert_eq!(unchecked(&store, &hash), before);
}

#[test]
fn failed_key_preparation_preserves_the_known_author_and_entries() {
    let (changes, _, _, key) = fixture([7; 32]);
    let hash = changes[1].hash();
    let mut identity = vec![0; 32];
    identity[0] = 1;
    for bytes in [vec![7; 31], identity] {
        let author = Author::from(bytes);
        let identities = BTreeMap::from([(changes[1].actor_id().clone(), author.clone())]);
        let mut store = SignatureStore::default();
        store
            .associate_author(ResolvedChangeAuthor::resolve(&changes[1], &identities).unwrap())
            .unwrap();
        store.insert_unchecked(hash, opaque(0, &[1]));
        store.insert_unchecked(hash, Signature::ed25519(sign(&key, &changes[1])));
        store.insert_unchecked(hash, opaque(255, &[8]));
        let before = unchecked(&store, &hash);
        assert!(
            matches!(store.verify_ed25519(&hash), Err(VerifyEd25519Error::InvalidAuthorKey { change, .. }) if change == hash)
        );
        assert_eq!(store.author(&hash), Some(&author));
        assert_eq!(unchecked(&store, &hash), before);
    }
}

#[test]
fn conflicting_association_cannot_change_any_entries_or_existing_key() {
    let (changes, table, authors, key) = fixture([7; 32]);
    let hash = changes[1].hash();
    let signature = sign(&key, &changes[1]);
    let prepared = prepared_authors(&table, &authors);
    let mut store = SignatureStore::default();
    store
        .insert_ed25519(
            AuthorVerifier::new(&prepared)
                .verify_change(&changes[1], &signature)
                .unwrap(),
        )
        .unwrap();
    store.insert_unchecked(hash, opaque(0, &[0]));
    store.insert_unchecked(hash, opaque(255, &[9]));
    let before = (evidence_entries(&store, &hash), unchecked(&store, &hash));
    let other_key = SigningKey::from_bytes(&[8; 32]);
    let others = BTreeMap::from([(
        changes[1].actor_id().clone(),
        Author::from(other_key.verifying_key().to_bytes().to_vec()),
    )]);
    assert!(
        matches!(store.associate_author(ResolvedChangeAuthor::resolve(&changes[1], &others).unwrap()),
        Err(AssociationError::AuthorConflict { change }) if change == hash)
    );
    assert_eq!(
        (evidence_entries(&store, &hash), unchecked(&store, &hash)),
        before
    );
}

#[test]
fn cleanup_frees_capacity_without_touching_existing_evidence() {
    let (changes, table, authors, key) = fixture([7; 32]);
    let hash = changes[1].hash();
    let signature = sign(&key, &changes[1]);
    let prepared = prepared_authors(&table, &authors);
    let mut store = SignatureStore::default();
    store
        .insert_ed25519(
            AuthorVerifier::new(&prepared)
                .verify_change(&changes[1], &signature)
                .unwrap(),
        )
        .unwrap();
    for byte in 0..15 {
        store.insert_unchecked(hash, opaque(0, &[byte]));
    }
    let before = evidence_entries(&store, &hash);
    assert_eq!(
        store.verify_ed25519(&hash).unwrap(),
        Ed25519VerificationReport {
            established_ed25519_evidence: 0,
            discarded_invalid: 15,
            skipped_other_schemes: 0,
        }
    );
    assert_eq!(evidence_entries(&store, &hash), before);
    assert!(unchecked(&store, &hash).is_empty());
}

#[test]
fn establishing_evidence_at_capacity_preserves_other_schemes() {
    let (changes, table, authors, key) = fixture([7; 32]);
    let hash = changes[1].hash();
    let mut store = SignatureStore::default();
    store.insert_unchecked(hash, Signature::ed25519(sign(&key, &changes[1])));
    for byte in 0..15 {
        store.insert_unchecked(hash, opaque(255, &[byte]));
    }
    let prepared = prepared_authors(&table, &authors);
    store
        .associate_author(
            ResolvedChangeAuthor::from_ed25519(&changes[1], &AuthorVerifier::new(&prepared))
                .unwrap(),
        )
        .unwrap();
    assert_eq!(
        store.verify_ed25519(&hash).unwrap(),
        Ed25519VerificationReport {
            established_ed25519_evidence: 1,
            discarded_invalid: 0,
            skipped_other_schemes: 15,
        }
    );
    assert_eq!(store.unchecked_entries(&hash).count(), 15);
    assert_eq!(store.ed25519_entries(&hash).count(), 1);
}

#[test]
fn empty_and_evidence_only_records_report_no_work() {
    let (changes, table, authors, key) = fixture([7; 32]);
    let hash = changes[1].hash();
    let mut store = SignatureStore::default();
    assert_eq!(
        store.verify_ed25519(&hash).unwrap(),
        Ed25519VerificationReport::default()
    );
    let prepared = prepared_authors(&table, &authors);
    store
        .associate_author(
            ResolvedChangeAuthor::from_ed25519(&changes[1], &AuthorVerifier::new(&prepared))
                .unwrap(),
        )
        .unwrap();
    assert_eq!(
        store.verify_ed25519(&hash).unwrap(),
        Ed25519VerificationReport::default()
    );
    let signature = sign(&key, &changes[1]);
    store
        .insert_ed25519(
            AuthorVerifier::new(&prepared)
                .verify_change(&changes[1], &signature)
                .unwrap(),
        )
        .unwrap();
    assert_eq!(
        store.verify_ed25519(&hash).unwrap(),
        Ed25519VerificationReport::default()
    );
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]

    #[test]
    fn checking_preserves_other_schemes_and_identity_across_repeated_passes(
        seed in any::<[u8; 32]>(),
        malformed in proptest::collection::btree_set(any::<u8>(), 0..=6),
        opaque_bytes in proptest::collection::btree_set(any::<u8>(), 0..=6),
        include_valid in any::<bool>(),
    ) {
        let (changes, _, _, key) = fixture(seed);
        let hash = changes[1].hash();
        let expected_author = Author::from(key.verifying_key().to_bytes().to_vec());
        let identities = BTreeMap::from([(changes[1].actor_id().clone(), expected_author.clone())]);
        let mut store = SignatureStore::default();
        store.associate_author(ResolvedChangeAuthor::resolve(&changes[1], &identities).unwrap()).unwrap();
        for byte in &malformed { store.insert_unchecked(hash, opaque(0, &[*byte])); }
        for byte in &opaque_bytes { store.insert_unchecked(hash, opaque(42, &[*byte])); }
        let valid = sign(&key, &changes[1]);
        if include_valid { store.insert_unchecked(hash, Signature::ed25519(valid)); }

        let report = store.verify_ed25519(&hash).unwrap();
        prop_assert_eq!(report.established_ed25519_evidence, usize::from(include_valid));
        prop_assert_eq!(report.discarded_invalid, malformed.len());
        prop_assert_eq!(report.skipped_other_schemes, opaque_bytes.len());
        prop_assert_eq!(store.author(&hash), Some(&expected_author));
        let expected: Vec<_> = opaque_bytes.iter().map(|b| (42, vec![*b])).collect();
        prop_assert_eq!(unchecked(&store, &hash), expected);
        prop_assert_eq!(store.ed25519_entries(&hash).count(), usize::from(include_valid));
        let second = store.verify_ed25519(&hash).unwrap();
        prop_assert_eq!(second.established_ed25519_evidence, 0);
        prop_assert_eq!(second.discarded_invalid, 0);
        prop_assert_eq!(second.skipped_other_schemes, opaque_bytes.len());
    }
}

#[test]
fn other_schemes_alongside_evidence_are_skipped() {
    let (changes, table, authors, key) = fixture([7; 32]);
    let hash = changes[1].hash();
    let signature = sign(&key, &changes[1]);
    let prepared = prepared_authors(&table, &authors);
    let mut store = SignatureStore::default();
    store
        .insert_ed25519(
            AuthorVerifier::new(&prepared)
                .verify_change(&changes[1], &signature)
                .unwrap(),
        )
        .unwrap();
    store.insert_unchecked(hash, opaque(42, &[5]));
    let before = (evidence_entries(&store, &hash), unchecked(&store, &hash));
    assert_eq!(
        store.verify_ed25519(&hash).unwrap(),
        Ed25519VerificationReport {
            established_ed25519_evidence: 0,
            discarded_invalid: 0,
            skipped_other_schemes: 1,
        }
    );
    assert_eq!(
        (evidence_entries(&store, &hash), unchecked(&store, &hash)),
        before
    );
}
