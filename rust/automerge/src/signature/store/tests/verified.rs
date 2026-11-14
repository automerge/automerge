//! Exercise the same witness-only store interface used by receipt and admission.

use std::collections::BTreeMap;

use proptest::prelude::*;

use super::*;

#[test]
fn merging_evidence_preserves_existing_entries_and_is_idempotent() {
    let (changes, key) = fixture([7; 32]);
    let verifier = verifying_author(&key);
    let first = sign(&key, &changes[0]);
    let later = sign(&key, &changes[1]);
    let mut target = SignatureStore::default();
    target
        .insert_ed25519(verifier.verify_hash(&changes[0].hash(), &first).unwrap())
        .unwrap();
    let mut source = SignatureStore::default();
    for (change, signature) in changes.iter().zip([first, later]) {
        source
            .insert_ed25519(verifier.verify_hash(&change.hash(), &signature).unwrap())
            .unwrap();
    }
    let before_source: Vec<_> = source
        .verified_hashes()
        .map(|h| (*h, evidence_entries(&source, h)))
        .collect();
    for _ in 0..2 {
        target.merge_verified(&source).unwrap();
        assert_eq!(target.verified_hashes().count(), 2);
        assert_eq!(
            evidence_entries(&target, &changes[0].hash()),
            vec![(verifier.author().clone(), first)]
        );
        assert_eq!(
            evidence_entries(&target, &changes[1].hash()),
            vec![(verifier.author().clone(), later)]
        );
    }
    assert_eq!(
        source
            .verified_hashes()
            .map(|h| (*h, evidence_entries(&source, h)))
            .collect::<Vec<_>>(),
        before_source
    );
    target.merge_verified(&SignatureStore::default()).unwrap();
    assert_eq!(target.verified_hashes().count(), 2);
}

#[test]
fn a_late_merge_conflict_leaves_even_earlier_nonconflicting_hashes_unmodified() {
    let first_hash = ChangeHash([1; 32]);
    let conflict_hash = ChangeHash([2; 32]);
    let key = SigningKey::from_bytes(&[7; 32]);
    let other = SigningKey::from_bytes(&[8; 32]);
    let signature =
        ed25519::Signature::from_bytes(key.sign(&signing_message(&conflict_hash)).to_bytes());
    let mut target = SignatureStore::default();
    target
        .insert_ed25519(
            verifying_author(&key)
                .verify_hash(&conflict_hash, &signature)
                .unwrap(),
        )
        .unwrap();
    let before = evidence_entries(&target, &conflict_hash);
    let mut source = SignatureStore::default();
    for hash in [first_hash, conflict_hash] {
        let signature =
            ed25519::Signature::from_bytes(other.sign(&signing_message(&hash)).to_bytes());
        source
            .insert_ed25519(
                verifying_author(&other)
                    .verify_hash(&hash, &signature)
                    .unwrap(),
            )
            .unwrap();
    }
    assert!(
        matches!(target.merge_verified(&source), Err(InsertError::AuthorConflict { change }) if change == conflict_hash)
    );
    assert_eq!(
        target.verified_hashes().copied().collect::<Vec<_>>(),
        [conflict_hash]
    );
    assert_eq!(evidence_entries(&target, &conflict_hash), before);
    assert_eq!(
        source.verified_hashes().copied().collect::<Vec<_>>(),
        [first_hash, conflict_hash]
    );
}

#[test]
fn retention_keeps_evidence_for_selected_hashes_and_removes_whole_records() {
    let (changes, key) = fixture([7; 32]);
    let verifier = verifying_author(&key);
    let mut store = SignatureStore::default();
    for change in &changes {
        let signature = sign(&key, change);
        store
            .insert_ed25519(verifier.verify_hash(&change.hash(), &signature).unwrap())
            .unwrap();
    }
    let kept = changes[1].hash();
    let before = evidence_entries(&store, &kept);
    store.retain_hashes(|hash| *hash == kept);
    assert_eq!(store.verified_hashes().copied().collect::<Vec<_>>(), [kept]);
    assert_eq!(evidence_entries(&store, &kept), before);
    assert!(evidence_entries(&store, &changes[0].hash()).is_empty());
    store.retain_hashes(|_| false);
    assert!(store.verified_hashes().next().is_none());
    assert!(evidence_entries(&store, &kept).is_empty());
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]

    #[test]
    fn insertion_merge_and_retention_match_an_author_bound_map(
        operations in prop::collection::vec((0u8..4, 0u8..4, any::<bool>()), 0..24)
    ) {
        let keys = [SigningKey::from_bytes(&[7; 32]), SigningKey::from_bytes(&[8; 32])];
        let authors = keys.each_ref().map(|key| Author::from(key.verifying_key().to_bytes().to_vec()));
        let signatures: Vec<_> = keys.iter().map(|key| (0u8..4).map(|hash| {
            ed25519::Signature::from_bytes(key.sign(&signing_message(&ChangeHash([hash; 32]))).to_bytes())
        }).collect::<Vec<_>>()).collect();
        let mut model = BTreeMap::<u8, usize>::new();
        let mut store = SignatureStore::default();
        for (operation, hash, second_author) in operations {
            let author = usize::from(second_author);
            let change = ChangeHash([hash; 32]);
            if operation == 3 {
                model.retain(|h, _| *h != hash);
                store.retain_hashes(|h| *h != change);
            } else {
                let signature = signatures[author][hash as usize];
                let witness = verifying_author(&keys[author]).verify_hash(&change, &signature).unwrap();
                let conflict = model.get(&hash).is_some_and(|old| *old != author);
                let duplicate = model.contains_key(&hash);
                if operation == 2 {
                    let mut source = SignatureStore::default();
                    source.insert_ed25519(witness).unwrap();
                    prop_assert_eq!(store.merge_verified(&source).is_err(), conflict);
                } else {
                    let result = store.insert_ed25519(witness);
                    if conflict {
                        prop_assert!(matches!(result, Err(InsertError::AuthorConflict { change: h }) if h == change), "conflicting author must reject the exact hash");
                    } else {
                        prop_assert_eq!(result.unwrap(), if duplicate { Insert::Duplicate } else { Insert::Stored });
                    }
                }
                if !conflict { model.insert(hash, author); }
            }
            prop_assert_eq!(store.verified_hashes().copied().collect::<Vec<_>>(), model.keys().map(|hash| ChangeHash([*hash; 32])).collect::<Vec<_>>());
            for hash in 0u8..4 {
                let expected = model.get(&hash).map(|author| vec![(authors[*author].clone(), signatures[*author][hash as usize])]).unwrap_or_default();
                prop_assert_eq!(evidence_entries(&store, &ChangeHash([hash; 32])), expected);
            }
        }
    }
}
